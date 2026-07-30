package wal

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/Dario-Zela/quorum/raft"
)

func TestSnapshotRoundTripAndSegmentGC(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{SegmentLimit: 128})
	idx := uint64(1)
	for i := 0; i < 30; i++ {
		persist(t, w, raft.HardState{Term: 2, VotedFor: 1, Append: entries(idx, 2)})
		idx++
	}
	if err := w.SaveSnapshot(20, 2, []byte("kv-at-20")); err != nil {
		t.Fatal(err)
	}
	// More writes after the snapshot.
	for i := 0; i < 5; i++ {
		persist(t, w, raft.HardState{Term: 2, VotedFor: 1, Append: entries(idx, 2)})
		idx++
	}
	w.Close()

	// A prefix of segments wholly below the snapshot is gone.
	names, _ := segmentNames(dir)
	if len(names) == 0 || names[0] == "wal-000001.log" {
		t.Fatalf("expected a prefix of old segments GC'd, still have %v", names)
	}

	_, st := mustOpen(t, dir, Options{SegmentLimit: 128})
	if st.SnapIndex != 20 || st.SnapTerm != 2 || !bytes.Equal(st.SnapData, []byte("kv-at-20")) {
		t.Fatalf("snapshot not recovered: idx=%d term=%d data=%q", st.SnapIndex, st.SnapTerm, st.SnapData)
	}
	if st.Term != 2 || st.Vote != 1 {
		t.Fatalf("hard state lost across segment GC: term=%d vote=%d", st.Term, st.Vote)
	}
	if len(st.Entries) == 0 || st.Entries[0].Index != 21 {
		t.Fatalf("entries must resume at snapIndex+1: %+v", st.Entries[:min(3, len(st.Entries))])
	}
	if last := st.Entries[len(st.Entries)-1].Index; last != 35 {
		t.Fatalf("tail = %d, want 35", last)
	}
}

func TestCorruptNewestSnapshotFallsBack(t *testing.T) {
	// Large segment limit: the pre-snapshot segment holds entries 1..20 and
	// survives GC (its max index exceeds both snapshot points), so the
	// fallback snapshot still has its WAL suffix.
	dir := t.TempDir()
	w, _ := mustOpen(t, dir, Options{})
	idx := uint64(1)
	for i := 0; i < 20; i++ {
		persist(t, w, raft.HardState{Term: 1, Append: entries(idx, 1)})
		idx++
	}
	if err := w.SaveSnapshot(5, 1, []byte("snap5")); err != nil {
		t.Fatal(err)
	}
	if err := w.SaveSnapshot(10, 1, []byte("snap10")); err != nil {
		t.Fatal(err)
	}
	w.Close()

	corruptSnap := func(marker string) {
		des, _ := os.ReadDir(dir)
		for _, de := range des {
			if strings.HasPrefix(de.Name(), "snap-") && strings.Contains(de.Name(), marker) {
				data, _ := os.ReadFile(dir + "/" + de.Name())
				data[len(data)-1] ^= 0xFF
				os.WriteFile(dir+"/"+de.Name(), data, 0o644)
			}
		}
	}
	corruptSnap("0000000000000010")

	_, st := mustOpen(t, dir, Options{})
	if st.SnapIndex != 5 || !bytes.Equal(st.SnapData, []byte("snap5")) {
		t.Fatalf("should fall back to snapshot 5, got idx=%d", st.SnapIndex)
	}
	if len(st.Entries) == 0 || st.Entries[0].Index != 6 || st.Entries[len(st.Entries)-1].Index != 20 {
		t.Fatalf("entries must cover 6..20: %d entries", len(st.Entries))
	}

	// Both snapshots corrupt: refuse to start — guessing converts a
	// detectable fault into silent divergence.
	corruptSnap("0000000000000005")
	if _, _, err := Open(dir, Options{}); err == nil {
		t.Fatal("all snapshots corrupt must refuse to start")
	}
}
