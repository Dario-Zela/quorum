// Package wal is a segmented write-ahead log for raft hard state.
//
// Layout: numbered segment files (wal-000001.log, wal-000002.log, …) whose
// names are monotonic so recovery order is lexicographic. One active
// segment receives appends; at SegmentLimit it is fsynced, closed, and
// becomes immutable. Records are framed [len:u32][crc32c:u32][protobuf
// WalRecord] with little-endian integers and a Castagnoli CRC over the
// payload.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/Dario-Zela/quorum/proto/quorumpb"
	"github.com/Dario-Zela/quorum/raft"
	"github.com/Dario-Zela/quorum/storage"
)

const (
	// DefaultSegmentLimit rolls the active segment at 64MB.
	DefaultSegmentLimit = 64 << 20
	headerSize          = 8
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Options tunes a WAL; the zero value takes defaults.
type Options struct {
	SegmentLimit int64
}

// WAL implements storage.Store over segment files.
type WAL struct {
	dir      string
	limit    int64
	active   *os.File
	size     int64
	seq      int
	lastIdx  uint64 // last log index, for contiguity validation
	firstIdx uint64 // == 1 until compaction lands (weekend 6)
}

// Open recovers the WAL in dir (creating it if empty) and returns the
// recovered state.
//
// CRC policy, two cases: a failed record at the tail of the FINAL segment
// is a torn write — truncate it and continue, safe because an unpersisted
// record was never acknowledged anywhere. A failed record anywhere else is
// corruption — refuse to start. Guessing here converts a detectable fault
// into silent divergence.
func Open(dir string, opts Options) (*WAL, storage.State, error) {
	if opts.SegmentLimit == 0 {
		opts.SegmentLimit = DefaultSegmentLimit
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, storage.State{}, err
	}
	names, err := segmentNames(dir)
	if err != nil {
		return nil, storage.State{}, err
	}

	w := &WAL{dir: dir, limit: opts.SegmentLimit, firstIdx: 1}
	var st storage.State

	if len(names) == 0 {
		if err := w.createSegment(1); err != nil {
			return nil, storage.State{}, err
		}
		return w, st, nil
	}

	for i, name := range names {
		final := i == len(names)-1
		if err := w.replaySegment(filepath.Join(dir, name), final, &st); err != nil {
			return nil, storage.State{}, err
		}
	}
	w.lastIdx = 0
	if n := len(st.Entries); n > 0 {
		w.lastIdx = st.Entries[n-1].Index
	}

	// Reopen the final segment for appending.
	last := filepath.Join(dir, names[len(names)-1])
	f, err := os.OpenFile(last, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return nil, storage.State{}, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, storage.State{}, err
	}
	w.active = f
	w.size = info.Size()
	fmt.Sscanf(names[len(names)-1], "wal-%06d.log", &w.seq)
	return w, st, nil
}

func segmentNames(dir string) ([]string, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, de := range des {
		if strings.HasPrefix(de.Name(), "wal-") && strings.HasSuffix(de.Name(), ".log") {
			names = append(names, de.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// replaySegment reads one segment, applying records to st. In the final
// segment a torn tail is truncated away; anywhere else it is corruption.
func (w *WAL) replaySegment(path string, final bool, st *storage.State) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	off := 0
	for off < len(data) {
		rest := data[off:]
		torn := ""
		var payload []byte
		if len(rest) < headerSize {
			torn = "short header"
		} else {
			length := binary.LittleEndian.Uint32(rest[0:4])
			crc := binary.LittleEndian.Uint32(rest[4:8])
			if int(length) > len(rest)-headerSize {
				torn = "short payload"
			} else {
				payload = rest[headerSize : headerSize+int(length)]
				if crc32.Checksum(payload, castagnoli) != crc {
					// A bad CRC is a torn write only when this is the last
					// record on disk; bytes following it prove otherwise.
					if final && off+headerSize+int(length) == len(data) {
						torn = "crc mismatch at tail"
					} else {
						return fmt.Errorf("wal: corrupt record in %s at offset %d (crc mismatch mid-log)", path, off)
					}
				}
			}
		}
		if torn != "" {
			if !final {
				return fmt.Errorf("wal: corrupt record in %s at offset %d (%s in non-final segment)", path, off, torn)
			}
			// Torn write: the record never finished, so it was never
			// acknowledged. Truncate and carry on.
			if err := os.Truncate(path, int64(off)); err != nil {
				return err
			}
			return syncPath(path)
		}
		var rec quorumpb.WalRecord
		if err := proto.Unmarshal(payload, &rec); err != nil {
			return fmt.Errorf("wal: undecodable record in %s at offset %d: %w", path, off, err)
		}
		if err := applyRecord(&rec, st); err != nil {
			return fmt.Errorf("wal: %s at offset %d: %w", path, off, err)
		}
		off += headerSize + len(payload)
	}
	return nil
}

func applyRecord(rec *quorumpb.WalRecord, st *storage.State) error {
	switch r := rec.Rec.(type) {
	case *quorumpb.WalRecord_HardState:
		st.Term = r.HardState.Term
		st.Vote = raft.NodeID(r.HardState.VotedFor)
	case *quorumpb.WalRecord_Truncate:
		from := r.Truncate.FromIndex
		keep := st.Entries[:0]
		for _, e := range st.Entries {
			if e.Index < from {
				keep = append(keep, e)
			}
		}
		st.Entries = keep
	case *quorumpb.WalRecord_Entries:
		for _, pe := range r.Entries.Entries {
			next := uint64(1)
			if n := len(st.Entries); n > 0 {
				next = st.Entries[n-1].Index + 1
			}
			if pe.Index != next {
				return fmt.Errorf("log gap: expected index %d, record holds %d", next, pe.Index)
			}
			st.Entries = append(st.Entries, raft.Entry{Term: pe.Term, Index: pe.Index, Data: pe.Data})
		}
	default:
		return fmt.Errorf("unknown record type %T", rec.Rec)
	}
	return nil
}

// Persist implements storage.Store: one batch, one write, one fsync.
func (w *WAL) Persist(hs *raft.HardState) error {
	if hs.TruncateFrom > 0 {
		if hs.TruncateFrom > w.lastIdx+1 {
			return fmt.Errorf("wal: truncate from %d beyond last index %d", hs.TruncateFrom, w.lastIdx)
		}
	}
	var batch []byte
	batch = appendRecord(batch, &quorumpb.WalRecord{Rec: &quorumpb.WalRecord_HardState{
		HardState: &quorumpb.HardStateRec{Term: hs.Term, VotedFor: uint64(hs.VotedFor)},
	}})
	if hs.TruncateFrom > 0 {
		batch = appendRecord(batch, &quorumpb.WalRecord{Rec: &quorumpb.WalRecord_Truncate{
			Truncate: &quorumpb.TruncateRec{FromIndex: hs.TruncateFrom},
		}})
		if hs.TruncateFrom <= w.lastIdx {
			w.lastIdx = hs.TruncateFrom - 1
		}
	}
	if len(hs.Append) > 0 {
		if hs.Append[0].Index != w.lastIdx+1 {
			return fmt.Errorf("wal: non-contiguous append: last %d, appending %d", w.lastIdx, hs.Append[0].Index)
		}
		rec := &quorumpb.EntriesRec{}
		for _, e := range hs.Append {
			rec.Entries = append(rec.Entries, &quorumpb.Entry{Term: e.Term, Index: e.Index, Data: e.Data})
		}
		batch = appendRecord(batch, &quorumpb.WalRecord{Rec: &quorumpb.WalRecord_Entries{Entries: rec}})
		w.lastIdx = hs.Append[len(hs.Append)-1].Index
	}

	if w.size > 0 && w.size+int64(len(batch)) > w.limit {
		if err := w.roll(); err != nil {
			return err
		}
	}
	if _, err := w.active.Write(batch); err != nil {
		return err
	}
	if err := w.active.Sync(); err != nil {
		return err
	}
	w.size += int64(len(batch))
	return nil
}

func appendRecord(buf []byte, rec *quorumpb.WalRecord) []byte {
	payload, err := proto.Marshal(rec)
	if err != nil {
		panic(fmt.Sprintf("wal: marshal: %v", err)) // structurally impossible
	}
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, castagnoli))
	return append(append(buf, hdr[:]...), payload...)
}

// roll closes the active segment (now immutable) and opens the next. The
// directory is fsynced: a new file's name is a directory-entry write and is
// not durable until the directory is.
func (w *WAL) roll() error {
	if err := w.active.Sync(); err != nil {
		return err
	}
	if err := w.active.Close(); err != nil {
		return err
	}
	return w.createSegment(w.seq + 1)
}

func (w *WAL) createSegment(seq int) error {
	path := filepath.Join(w.dir, fmt.Sprintf("wal-%06d.log", seq))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := syncPath(w.dir); err != nil {
		f.Close()
		return err
	}
	w.active, w.size, w.seq = f, 0, seq
	return nil
}

func syncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// Close syncs and closes the active segment.
func (w *WAL) Close() error {
	if w.active == nil {
		return nil
	}
	if err := w.active.Sync(); err != nil {
		return err
	}
	return w.active.Close()
}
