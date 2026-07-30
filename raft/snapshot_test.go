package raft

import (
	"bytes"
	"testing"
)

// leaderWithLog builds a 3-node leader holding entries 1..n (its no-op at
// 1, synthetic client entries after), all committed.
func leaderWithLog(t *testing.T, n uint64) *Raft {
	t.Helper()
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	electTestLeader(t, r)
	for i := uint64(2); i <= n; i++ {
		r.Step(Propose{Data: []byte{byte(i)}})
	}
	r.Step(MsgRecv{From: 2, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: n}})
	if r.Status().CommitIndex != n || r.Status().LastApplied != n {
		t.Fatalf("setup: commit/applied = %d/%d, want %d", r.Status().CommitIndex, r.Status().LastApplied, n)
	}
	return r
}

func TestCompactDropsCoveredEntriesAndKeepsBoundaryTerm(t *testing.T) {
	r := leaderWithLog(t, 5)
	out := r.Step(Compact{Index: 3, Data: []byte("snap3")})
	if out.Snapshot == nil || out.Snapshot.Index != 3 || out.Snapshot.FromLeader {
		t.Fatalf("compact must emit a snapshot op: %+v", out.Snapshot)
	}
	if r.log.first != 4 {
		t.Fatalf("firstIndex = %d, want 4", r.log.first)
	}
	// Term(firstIndex-1) must answer from snapshot metadata: it is the
	// PrevLogIndex of the first append after compaction.
	if tm, ok := r.log.Term(3); !ok || tm != 1 {
		t.Fatalf("Term(3) after compaction = (%d,%v), want (1,true)", tm, ok)
	}
	// Replication to a caught-up follower still works.
	var probed bool
	out = Output{}
	r.sendAppend(2, &out)
	for _, s := range out.Send {
		if ae, ok := s.Msg.(AppendEntries); ok && ae.PrevLogIndex >= 3 {
			probed = true
		}
	}
	if !probed {
		t.Fatal("post-compaction append should anchor at or above the boundary")
	}
}

func TestLaggardGetsInstallSnapshot(t *testing.T) {
	r := leaderWithLog(t, 6)
	r.Step(Compact{Index: 5, Data: []byte("snap5")})
	// Node 3 never acked anything: nextIndex backtracks below firstIndex.
	out := r.Step(MsgRecv{From: 3, Msg: AppendEntriesReply{Term: 1, Success: false, ConflictIndex: 1}})
	var is *InstallSnapshot
	for _, s := range out.Send {
		if m, ok := s.Msg.(InstallSnapshot); ok && s.To == 3 {
			is = &m
		}
	}
	if is == nil {
		t.Fatalf("laggard below firstIndex must get InstallSnapshot, sends: %+v", out.Send)
	}
	if is.LastIncludedIndex != 5 || !bytes.Equal(is.Data, []byte("snap5")) {
		t.Fatalf("wrong snapshot shipped: %+v", is)
	}
	// The reply advances match/next so appends resume after the snapshot.
	r.Step(MsgRecv{From: 3, Msg: InstallSnapshotReply{Term: 1, MatchIndex: 5}})
	if r.matchIndex[3] != 5 || r.nextIndex[3] != 6 {
		t.Fatalf("match/next = %d/%d, want 5/6", r.matchIndex[3], r.nextIndex[3])
	}
}

func TestInstallSnapshotDiscardOrRetainSuffix(t *testing.T) {
	// Case A: follower log has a matching (index, term) entry → suffix
	// after it survives.
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	feedAppend(r, 2, 1, 0, 0, mkEntries(1, 1, 1, 1, 1), 0) // idx 1..4 term 1
	out := r.Step(MsgRecv{From: 2, Msg: InstallSnapshot{
		Term: 1, LeaderID: 2, LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte("s"),
	}})
	if r.Status().LastIndex != 4 {
		t.Fatalf("matching suffix must survive: lastIndex = %d, want 4", r.Status().LastIndex)
	}
	if r.Status().CommitIndex != 2 || out.Snapshot == nil || !out.Snapshot.FromLeader {
		t.Fatalf("install bookkeeping wrong: commit=%d snap=%+v", r.Status().CommitIndex, out.Snapshot)
	}

	// Case B: mismatched term at LastIncludedIndex → whole log discarded,
	// and the disowning is logged for recovery.
	r2 := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	feedAppend(r2, 2, 1, 0, 0, mkEntries(1, 1, 1, 1, 1), 0)
	out = r2.Step(MsgRecv{From: 3, Msg: InstallSnapshot{
		Term: 2, LeaderID: 3, LastIncludedIndex: 3, LastIncludedTerm: 2, Data: []byte("s2"),
	}})
	if r2.Status().LastIndex != 3 {
		t.Fatalf("mismatched suffix must be discarded: lastIndex = %d, want 3 (the snapshot boundary)", r2.Status().LastIndex)
	}
	if out.PersistHard == nil || out.PersistHard.TruncateFrom != 4 {
		t.Fatalf("the discard must be logged (TruncateFrom=4), got %+v", out.PersistHard)
	}

	// Case C: stale snapshot (≤ commitIndex) is ignored entirely.
	r3 := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	feedAppend(r3, 2, 1, 0, 0, mkEntries(1, 1, 1, 1, 1), 3) // idx 1..4, commit 3
	out = r3.Step(MsgRecv{From: 2, Msg: InstallSnapshot{
		Term: 1, LeaderID: 2, LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte("old"),
	}})
	if out.Snapshot != nil {
		t.Fatal("stale snapshot must be ignored")
	}
	rep, ok := out.Send[0].Msg.(InstallSnapshotReply)
	if !ok || rep.MatchIndex != 3 {
		t.Fatalf("stale-snapshot reply should report our commit (3): %+v", out.Send[0].Msg)
	}
	if r3.Status().LastIndex != 4 {
		t.Fatalf("stale snapshot truncated live entries: lastIndex=%d", r3.Status().LastIndex)
	}
}
