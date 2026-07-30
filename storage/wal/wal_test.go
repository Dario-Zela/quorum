package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Dario-Zela/quorum/raft"
	"github.com/Dario-Zela/quorum/storage"
)

func mustOpen(t *testing.T, dir string, opts Options) (*WAL, storage.State) {
	t.Helper()
	w, st, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return w, st
}

func persist(t *testing.T, w *WAL, hs raft.HardState) {
	t.Helper()
	if err := w.Persist(&hs); err != nil {
		t.Fatalf("Persist: %v", err)
	}
}

func entries(idx uint64, terms ...uint64) []raft.Entry {
	var es []raft.Entry
	for i, tm := range terms {
		es = append(es, raft.Entry{Term: tm, Index: idx + uint64(i), Data: []byte{byte(tm)}})
	}
	return es
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, st := mustOpen(t, dir, Options{})
	if st.Term != 0 || len(st.Entries) != 0 {
		t.Fatalf("fresh WAL should recover empty, got %+v", st)
	}
	persist(t, w, raft.HardState{Term: 1, VotedFor: 2})
	persist(t, w, raft.HardState{Term: 1, VotedFor: 2, Append: entries(1, 1, 1)})
	persist(t, w, raft.HardState{Term: 3, VotedFor: 1, Append: entries(3, 3)})
	w.Close()

	_, st = mustOpen(t, dir, Options{})
	if st.Term != 3 || st.Vote != 1 {
		t.Fatalf("recovered hard state = term %d vote %d, want 3/1", st.Term, st.Vote)
	}
	if len(st.Entries) != 3 || st.Entries[2].Term != 3 || st.Entries[2].Index != 3 {
		t.Fatalf("recovered entries %+v", st.Entries)
	}
}

// The Truncate record: without it, a restart resurrects entries the node
// already disowned to its leader.
func TestTruncateReplay(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	persist(t, w, raft.HardState{Term: 2, Append: entries(1, 1, 1, 2, 2, 2)})
	// A new leader overwrites from index 3.
	persist(t, w, raft.HardState{Term: 4, TruncateFrom: 3, Append: entries(3, 4, 4)})
	w.Close()

	_, st := mustOpen(t, dir, Options{})
	if len(st.Entries) != 4 {
		t.Fatalf("want 4 entries after truncate replay, got %+v", st.Entries)
	}
	if st.Entries[2].Term != 4 || st.Entries[3].Term != 4 {
		t.Fatalf("truncated suffix resurrected: %+v", st.Entries)
	}
}

func TestSegmentRolling(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentLimit: 256})
	idx := uint64(1)
	for i := 0; i < 50; i++ {
		persist(t, w, raft.HardState{Term: 1, Append: entries(idx, 1)})
		idx++
	}
	w.Close()

	names, _ := segmentNames(dir)
	if len(names) < 2 {
		t.Fatalf("expected multiple segments at 256-byte limit, got %v", names)
	}
	_, st := mustOpen(t, dir, Options{SegmentLimit: 256})
	if len(st.Entries) != 50 {
		t.Fatalf("recovered %d entries across segments, want 50", len(st.Entries))
	}
}

// Torn write at the tail of the final segment: truncate and continue.
func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	persist(t, w, raft.HardState{Term: 1, Append: entries(1, 1, 1)})
	persist(t, w, raft.HardState{Term: 1, Append: entries(3, 1)})
	w.Close()

	names, _ := segmentNames(dir)
	path := filepath.Join(dir, names[len(names)-1])
	info, _ := os.Stat(path)
	// Chop mid-way through the final record.
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	_, st := mustOpen(t, dir, Options{})
	if len(st.Entries) != 2 {
		t.Fatalf("want the first persist to survive (2 entries), got %+v", st.Entries)
	}
	// The torn bytes must be gone from disk so appends resume cleanly.
	info2, _ := os.Stat(path)
	if info2.Size() >= info.Size()-3 {
		t.Fatalf("torn tail not truncated: size %d", info2.Size())
	}
}

// A flipped byte mid-log is corruption, not a torn write: refuse to start.
func TestMidLogCorruptionRefusesToStart(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	persist(t, w, raft.HardState{Term: 1, Append: entries(1, 1, 1, 1)})
	persist(t, w, raft.HardState{Term: 2, VotedFor: 3})
	w.Close()

	names, _ := segmentNames(dir)
	path := filepath.Join(dir, names[0])
	data, _ := os.ReadFile(path)
	data[headerSize+2] ^= 0xFF // corrupt the first record's payload
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, _, err := Open(dir, Options{}); err == nil {
		t.Fatal("mid-log corruption must refuse to start — guessing converts a detectable fault into silent divergence")
	}
}

// Same flip on the FINAL record is a torn write and must be tolerated.
func TestCorruptFinalRecordIsTorn(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	persist(t, w, raft.HardState{Term: 1, Append: entries(1, 1)})
	persist(t, w, raft.HardState{Term: 2, VotedFor: 3})
	w.Close()

	names, _ := segmentNames(dir)
	path := filepath.Join(dir, names[len(names)-1])
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, st := mustOpen(t, dir, Options{})
	if st.Term != 1 || st.Vote != 0 {
		t.Fatalf("state should roll back to the last intact record, got term=%d vote=%d", st.Term, st.Vote)
	}
}

// Torn writes in a non-final segment are corruption: later segments prove
// the log continued past them.
func TestTornNonFinalSegmentRefusesToStart(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentLimit: 128})
	idx := uint64(1)
	for i := 0; i < 20; i++ {
		persist(t, w, raft.HardState{Term: 1, Append: entries(idx, 1)})
		idx++
	}
	w.Close()
	names, _ := segmentNames(dir)
	if len(names) < 2 {
		t.Fatal("setup: need multiple segments")
	}
	path := filepath.Join(dir, names[0])
	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(dir, Options{}); err == nil {
		t.Fatal("short non-final segment must refuse to start")
	}
}

// Appending after a torn-tail recovery must produce a clean, replayable log.
func TestAppendAfterTornRecovery(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	persist(t, w, raft.HardState{Term: 1, Append: entries(1, 1, 1)})
	persist(t, w, raft.HardState{Term: 1, Append: entries(3, 1)})
	w.Close()
	names, _ := segmentNames(dir)
	path := filepath.Join(dir, names[0])
	info, _ := os.Stat(path)
	os.Truncate(path, info.Size()-1)

	w, st := mustOpen(t, dir, Options{})
	if len(st.Entries) != 2 {
		t.Fatalf("setup: want 2 entries, got %d", len(st.Entries))
	}
	persist(t, w, raft.HardState{Term: 2, Append: entries(3, 2, 2)})
	w.Close()

	_, st = mustOpen(t, dir, Options{})
	if len(st.Entries) != 4 || st.Entries[3].Term != 2 {
		t.Fatalf("post-tear appends lost: %+v", st.Entries)
	}
}
