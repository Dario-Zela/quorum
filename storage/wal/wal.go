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

	// UnsafeNoSync skips the per-Persist fsync (segment rolls and
	// snapshots still sync). Exists ONLY for the benchmark contrast table:
	// running with it is cheating — an acknowledged write can vanish in a
	// crash, which is precisely the guarantee this project exists to keep.
	// Enabled by QUORUM_UNSAFE_NO_FSYNC=1.
	UnsafeNoSync bool
}

// WAL implements storage.Store over segment files, plus snapshots.
type WAL struct {
	dir     string
	limit   int64
	active  *os.File
	size    int64
	seq     int
	lastIdx uint64 // last log index, for contiguity validation

	lastTerm  uint64      // last persisted hard state, rewritten into a
	lastVote  raft.NodeID // fresh segment after snapshot-driven segment GC
	snapIndex uint64
	segMax    map[int]uint64 // closed segment seq → max entry index it holds
	noSync    bool           // Options.UnsafeNoSync
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

	w := &WAL{dir: dir, limit: opts.SegmentLimit, segMax: make(map[int]uint64), noSync: opts.UnsafeNoSync}
	var st storage.State

	// (1) Load the newest snapshot passing its checksum.
	if err := w.loadSnapshot(&st); err != nil {
		return nil, storage.State{}, err
	}
	w.snapIndex = st.SnapIndex

	if len(names) == 0 {
		if err := w.createSegment(1); err != nil {
			return nil, storage.State{}, err
		}
		w.lastIdx = st.SnapIndex
		return w, st, nil
	}

	// (2) Replay the segments. Entries accumulate from wherever the oldest
	// retained segment starts (segments wholly below the snapshot were
	// deleted at snapshot time); the snapshot trim happens after, because
	// Truncate records must replay against the full retained sequence.
	for i, name := range names {
		final := i == len(names)-1
		var seq int
		fmt.Sscanf(name, "wal-%06d.log", &seq)
		maxIdx, err := w.replaySegment(filepath.Join(dir, name), final, &st)
		if err != nil {
			return nil, storage.State{}, err
		}
		if !final {
			w.segMax[seq] = maxIdx
		}
	}
	// (3) Trim to the snapshot boundary and validate contiguity.
	keep := st.Entries[:0]
	for _, e := range st.Entries {
		if e.Index > st.SnapIndex {
			keep = append(keep, e)
		}
	}
	st.Entries = keep
	if len(st.Entries) > 0 && st.Entries[0].Index != st.SnapIndex+1 {
		return nil, storage.State{}, fmt.Errorf("wal: gap between snapshot %d and first log entry %d", st.SnapIndex, st.Entries[0].Index)
	}
	w.lastIdx = st.SnapIndex
	if n := len(st.Entries); n > 0 {
		w.lastIdx = st.Entries[n-1].Index
	}
	w.lastTerm, w.lastVote = st.Term, st.Vote

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

// replaySegment reads one segment, applying records to st and reporting
// the max entry index it held. In the final segment a torn tail is
// truncated away; anywhere else it is corruption.
func (w *WAL) replaySegment(path string, final bool, st *storage.State) (uint64, error) {
	var maxIdx uint64
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
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
						return 0, fmt.Errorf("wal: corrupt record in %s at offset %d (crc mismatch mid-log)", path, off)
					}
				}
			}
		}
		if torn != "" {
			if !final {
				return 0, fmt.Errorf("wal: corrupt record in %s at offset %d (%s in non-final segment)", path, off, torn)
			}
			// Torn write: the record never finished, so it was never
			// acknowledged. Truncate and carry on.
			if err := os.Truncate(path, int64(off)); err != nil {
				return 0, err
			}
			return maxIdx, syncPath(path)
		}
		var rec quorumpb.WalRecord
		if err := proto.Unmarshal(payload, &rec); err != nil {
			return 0, fmt.Errorf("wal: undecodable record in %s at offset %d: %w", path, off, err)
		}
		if err := applyRecord(&rec, st); err != nil {
			return 0, fmt.Errorf("wal: %s at offset %d: %w", path, off, err)
		}
		if er, ok := rec.Rec.(*quorumpb.WalRecord_Entries); ok {
			for _, pe := range er.Entries.Entries {
				if pe.Index > maxIdx {
					maxIdx = pe.Index
				}
			}
		}
		off += headerSize + len(payload)
	}
	return maxIdx, nil
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
			if n := len(st.Entries); n > 0 {
				if next := st.Entries[n-1].Index + 1; pe.Index != next {
					return fmt.Errorf("log gap: expected index %d, record holds %d", next, pe.Index)
				}
			}
			// An empty accumulator accepts any starting index: segments
			// wholly below a snapshot get deleted, so the retained stream
			// starts mid-log; the snapshot trim after replay reconciles.
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
	w.lastTerm, w.lastVote = hs.Term, hs.VotedFor
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
	if !w.noSync {
		if err := w.active.Sync(); err != nil {
			return err
		}
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
	w.segMax[w.seq] = w.lastIdx
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

// --- snapshots ---

func snapName(index, term uint64) string {
	return fmt.Sprintf("snap-%016d-%016d.db", index, term)
}

// loadSnapshot finds the newest snapshot file passing its checksum.
// Corrupt newer snapshots fall back to older ones (the previous snapshot
// is always retained until the new one is fully durable, so at least one
// valid pair exists); if snapshot files exist but NONE parse, that is
// corruption — refuse to start.
func (w *WAL) loadSnapshot(st *storage.State) error {
	des, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}
	var names []string
	for _, de := range des {
		if strings.HasPrefix(de.Name(), "snap-") && strings.HasSuffix(de.Name(), ".db") {
			names = append(names, de.Name())
		}
	}
	if len(names) == 0 {
		return nil
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(w.dir, name))
		if err != nil || len(data) < headerSize {
			continue
		}
		length := binary.LittleEndian.Uint32(data[0:4])
		crc := binary.LittleEndian.Uint32(data[4:8])
		if int(length) != len(data)-headerSize {
			continue
		}
		payload := data[headerSize:]
		if crc32.Checksum(payload, castagnoli) != crc {
			continue
		}
		var sf quorumpb.SnapshotFile
		if err := proto.Unmarshal(payload, &sf); err != nil {
			continue
		}
		st.SnapIndex, st.SnapTerm, st.SnapData = sf.LastIncludedIndex, sf.LastIncludedTerm, sf.Kv
		return nil
	}
	return fmt.Errorf("wal: %d snapshot file(s) present but none pass their checksum", len(names))
}

// SaveSnapshot makes a snapshot durable and garbage-collects the log
// beneath it. Durability order: write temp → fsync file → rename → fsync
// dir → only then delete WAL segments entirely below the snapshot index
// and any older snapshot — so a crash at any instant leaves at least one
// valid (snapshot, WAL-suffix) pair on disk.
func (w *WAL) SaveSnapshot(index, term uint64, data []byte) error {
	if index <= w.snapIndex {
		return nil
	}
	payload, err := proto.Marshal(&quorumpb.SnapshotFile{
		LastIncludedIndex: index, LastIncludedTerm: term, Kv: data,
	})
	if err != nil {
		return err
	}
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, castagnoli))

	tmp := filepath.Join(w.dir, "snap.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(hdr[:], payload...)); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	final := filepath.Join(w.dir, snapName(index, term))
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	// A rename is a directory-entry write and is not durable until the
	// directory is.
	if err := syncPath(w.dir); err != nil {
		return err
	}
	prevSnap := w.snapIndex
	w.snapIndex = index

	// Roll to a fresh segment carrying the current hard state, so deleting
	// old segments cannot lose the latest term/vote.
	if err := w.roll(); err != nil {
		return err
	}
	hs := appendRecord(nil, &quorumpb.WalRecord{Rec: &quorumpb.WalRecord_HardState{
		HardState: &quorumpb.HardStateRec{Term: w.lastTerm, VotedFor: uint64(w.lastVote)},
	}})
	if _, err := w.active.Write(hs); err != nil {
		return err
	}
	if err := w.active.Sync(); err != nil {
		return err
	}
	w.size += int64(len(hs))

	// GC: a PREFIX of closed segments whose every entry is covered by the
	// snapshot. Prefix only — truncation can rewrite lower indexes into
	// later segments, and deleting a middle segment would delete the
	// Truncate record that disowned an earlier segment's entries. Also GC
	// snapshots older than the previous one (the previous is kept until
	// the next SaveSnapshot).
	var seqs []int
	for seq := range w.segMax {
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)
	for _, seq := range seqs {
		if w.segMax[seq] > index {
			break
		}
		os.Remove(filepath.Join(w.dir, fmt.Sprintf("wal-%06d.log", seq)))
		delete(w.segMax, seq)
	}
	des, err := os.ReadDir(w.dir)
	if err == nil {
		for _, de := range des {
			name := de.Name()
			if !strings.HasPrefix(name, "snap-") || !strings.HasSuffix(name, ".db") {
				continue
			}
			var i, t uint64
			if _, err := fmt.Sscanf(name, "snap-%016d-%016d.db", &i, &t); err == nil {
				if i < prevSnap {
					os.Remove(filepath.Join(w.dir, name))
				}
			}
		}
	}
	return syncPath(w.dir)
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
