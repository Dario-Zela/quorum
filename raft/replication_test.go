package raft

import (
	"bytes"
	"testing"
)

// mkEntries builds entries idx..idx+len(terms)-1 with the given terms.
func mkEntries(idx uint64, terms ...uint64) []Entry {
	es := make([]Entry, len(terms))
	for i, t := range terms {
		es[i] = Entry{Term: t, Index: idx + uint64(i), Data: []byte{byte(t)}}
	}
	return es
}

// feedAppend drives one AppendEntries into r from a synthetic leader.
func feedAppend(r *Raft, leader NodeID, term, prevIdx, prevTerm uint64, entries []Entry, commit uint64) (AppendEntriesReply, Output) {
	out := r.Step(MsgRecv{From: leader, Msg: AppendEntries{
		Term: term, LeaderID: leader,
		PrevLogIndex: prevIdx, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: commit,
	}})
	for _, s := range out.Send {
		if rep, ok := s.Msg.(AppendEntriesReply); ok {
			return rep, out
		}
	}
	panic("no AppendEntriesReply in output")
}

func TestFollowerAppendsAndPersistsEntries(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	rep, out := feedAppend(r, 2, 1, 0, 0, mkEntries(1, 1, 1), 0)
	if !rep.Success || rep.MatchIndex != 2 {
		t.Fatalf("append rejected: %+v", rep)
	}
	if out.PersistHard == nil || len(out.PersistHard.Append) != 2 {
		t.Fatalf("appended entries must be in PersistHard, got %+v", out.PersistHard)
	}
	if r.Status().LastIndex != 2 {
		t.Fatalf("lastIndex = %d, want 2", r.Status().LastIndex)
	}
}

func TestFollowerRejectsMissingPrev(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	feedAppend(r, 2, 1, 0, 0, mkEntries(1, 1), 0)
	// Leader probes at index 5; our log ends at 1.
	rep, _ := feedAppend(r, 2, 1, 5, 1, nil, 0)
	if rep.Success {
		t.Fatal("append beyond log end must fail")
	}
	if rep.ConflictIndex != 2 || rep.ConflictTerm != 0 {
		t.Fatalf("want ConflictIndex=lastIndex+1=2, ConflictTerm=0; got %+v", rep)
	}
}

func TestFollowerConflictTermReporting(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	// Log: idx1..4 with terms 1,2,2,2.
	feedAppend(r, 2, 2, 0, 0, mkEntries(1, 1, 2, 2, 2), 0)
	// New leader at term 3 probes (4, 3): we have term 2 there.
	rep, _ := feedAppend(r, 3, 3, 4, 3, nil, 0)
	if rep.Success {
		t.Fatal("term mismatch at prev must fail")
	}
	if rep.ConflictTerm != 2 || rep.ConflictIndex != 2 {
		t.Fatalf("want ConflictTerm=2 with first index 2, got %+v", rep)
	}
}

func TestFollowerTruncatesConflictingSuffix(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	feedAppend(r, 2, 2, 0, 0, mkEntries(1, 1, 2, 2), 0)
	// Leader term 3 overwrites from index 2.
	newE := mkEntries(2, 3, 3)
	rep, out := feedAppend(r, 3, 3, 1, 1, newE, 0)
	if !rep.Success || rep.MatchIndex != 3 {
		t.Fatalf("overwrite append failed: %+v", rep)
	}
	if out.PersistHard == nil || out.PersistHard.TruncateFrom != 2 {
		t.Fatalf("truncation must be persisted as a logged event, got %+v", out.PersistHard)
	}
	if got, _ := r.log.Term(2); got != 3 {
		t.Fatalf("index 2 should now be term 3, got %d", got)
	}
	if r.Status().LastIndex != 3 {
		t.Fatalf("lastIndex = %d, want 3", r.Status().LastIndex)
	}
}

// A reordered older append that matches a prefix must NOT truncate newer
// entries — matching entries are skipped, never re-truncated.
func TestReorderedOldAppendDoesNotTruncate(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	feedAppend(r, 2, 2, 0, 0, mkEntries(1, 1, 2, 2, 2), 0)
	// Stale duplicate: just entries 1..2, all matching our prefix.
	rep, out := feedAppend(r, 2, 2, 0, 0, mkEntries(1, 1, 2), 0)
	if !rep.Success || rep.MatchIndex != 2 {
		t.Fatalf("prefix duplicate should succeed with MatchIndex=2: %+v", rep)
	}
	if out.PersistHard != nil && out.PersistHard.TruncateFrom != 0 {
		t.Fatalf("prefix duplicate must not truncate, got %+v", out.PersistHard)
	}
	if r.Status().LastIndex != 4 {
		t.Fatalf("newer entries lost: lastIndex = %d, want 4", r.Status().LastIndex)
	}
}

func TestFollowerAdvancesCommitAndApplies(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	feedAppend(r, 2, 1, 0, 0, mkEntries(1, 1, 1, 1), 0)
	// Heartbeat carrying LeaderCommit=2.
	rep, out := feedAppend(r, 2, 1, 3, 1, nil, 2)
	if !rep.Success {
		t.Fatalf("heartbeat failed: %+v", rep)
	}
	if len(out.ApplyEntries) != 2 || out.ApplyEntries[0].Index != 1 || out.ApplyEntries[1].Index != 2 {
		t.Fatalf("want entries 1,2 applied, got %+v", out.ApplyEntries)
	}
	if st := r.Status(); st.CommitIndex != 2 || st.LastApplied != 2 {
		t.Fatalf("commit/applied = %d/%d, want 2/2", st.CommitIndex, st.LastApplied)
	}
	// Commit is capped at the last entry the message covered, even if the
	// leader's commit is ahead.
	rep, _ = feedAppend(r, 2, 1, 3, 1, nil, 99)
	if r.Status().CommitIndex != 3 {
		t.Fatalf("commit must cap at lastNew=3, got %d", r.Status().CommitIndex)
	}
}

func TestLeaderProposeEmitsReceiptAndReplicates(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	tickUntilCandidate(t, r)
	r.Step(MsgRecv{From: 2, Msg: RequestVoteReply{Term: 1, Granted: true}})
	if r.Status().Role != Leader {
		t.Fatal("setup: should be leader")
	}
	// The no-op occupies index 1.
	out := r.Step(Propose{Data: []byte("x")})
	if out.Proposed == nil || out.Proposed.Index != 2 || out.Proposed.Term != 1 {
		t.Fatalf("want Receipt{2,1}, got %+v", out.Proposed)
	}
	if out.PersistHard == nil || len(out.PersistHard.Append) != 1 {
		t.Fatalf("proposal must persist its entry, got %+v", out.PersistHard)
	}
	sent := 0
	for _, s := range out.Send {
		ae := s.Msg.(AppendEntries)
		if len(ae.Entries) == 0 || !bytes.Equal(ae.Entries[len(ae.Entries)-1].Data, []byte("x")) {
			t.Fatalf("replication must carry the proposal, got %+v", ae)
		}
		sent++
	}
	if sent != 2 {
		t.Fatalf("expected replication to 2 peers, got %d", sent)
	}
}

func TestProposeOnNonLeaderYieldsNoReceipt(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	out := r.Step(Propose{Data: []byte("x")})
	if out.Proposed != nil || len(out.Send) != 0 {
		t.Fatalf("follower must drop proposals, got %+v", out)
	}
}

func TestCommitRuleRequiresCurrentTermEntry(t *testing.T) {
	// Leader at term 3 holds an uncommitted entry from term 1 that is
	// majority-replicated: it must NOT commit until the term-3 no-op is
	// also majority-replicated — then both commit together.
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	feedAppend(r, 2, 1, 0, 0, mkEntries(1, 1), 0) // index 1, term 1 from an old leader
	// Win an election at term 3 (term 2 burns in a failed attempt).
	for r.Status().Role != Candidate {
		r.Step(Tick{})
	}
	for r.Status().Term < 3 {
		r.Step(Tick{})
	}
	out := r.Step(MsgRecv{From: 2, Msg: RequestVoteReply{Term: 3, Granted: true}})
	if r.Status().Role != Leader {
		t.Fatalf("setup: should be leader at term 3, got %+v", r.Status())
	}
	_ = out
	// Follower 2 acks the old entry only (index 1): majority for index 1.
	r.Step(MsgRecv{From: 2, Msg: AppendEntriesReply{Term: 3, Success: true, MatchIndex: 1}})
	if r.Status().CommitIndex != 0 {
		t.Fatalf("prior-term entry committed by counting — Figure 8 bug! commit=%d", r.Status().CommitIndex)
	}
	// Follower 2 acks through the no-op (index 2): both commit.
	out = r.Step(MsgRecv{From: 2, Msg: AppendEntriesReply{Term: 3, Success: true, MatchIndex: 2}})
	if r.Status().CommitIndex != 2 {
		t.Fatalf("commit = %d, want 2 (no-op pulls the prior-term entry in)", r.Status().CommitIndex)
	}
	if len(out.ApplyEntries) != 2 {
		t.Fatalf("both entries apply together, got %+v", out.ApplyEntries)
	}
}

func TestLeaderFastBacktrackSkipsTerm(t *testing.T) {
	// Leader's log: idx1..3 terms 1,1,1 (via a synthetic old leader), then
	// it wins at term 3. A follower reporting ConflictTerm=1/ConflictIndex=1
	// makes the leader resume after ITS last entry of term 1 — index 4 — in
	// one hop, not one index per round trip.
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	feedAppend(r, 2, 1, 0, 0, mkEntries(1, 1, 1, 1), 0)
	for r.Status().Role != Candidate {
		r.Step(Tick{})
	}
	for r.Status().Term < 3 {
		r.Step(Tick{})
	}
	r.Step(MsgRecv{From: 2, Msg: RequestVoteReply{Term: 3, Granted: true}})
	// Follower 3's log diverged: it reports its term-1 block starts at 1.
	out := r.Step(MsgRecv{From: 3, Msg: AppendEntriesReply{
		Term: 3, Success: false, ConflictIndex: 1, ConflictTerm: 1,
	}})
	var resent *AppendEntries
	for _, s := range out.Send {
		if ae, ok := s.Msg.(AppendEntries); ok && s.To == 3 {
			resent = &ae
		}
	}
	if resent == nil {
		t.Fatal("leader must retry immediately after a rejection")
	}
	// Leader has term-1 entries through index 3 → next = 4 → prev = 3.
	if resent.PrevLogIndex != 3 {
		t.Fatalf("fast backtrack: want PrevLogIndex=3, got %d", resent.PrevLogIndex)
	}
	// Unknown conflict term: jump straight to ConflictIndex.
	out = r.Step(MsgRecv{From: 3, Msg: AppendEntriesReply{
		Term: 3, Success: false, ConflictIndex: 2, ConflictTerm: 9,
	}})
	for _, s := range out.Send {
		if ae, ok := s.Msg.(AppendEntries); ok && s.To == 3 {
			if ae.PrevLogIndex != 1 {
				t.Fatalf("unknown ConflictTerm: want PrevLogIndex=ConflictIndex-1=1, got %d", ae.PrevLogIndex)
			}
		}
	}
}
