package raft

import (
	"math/rand"
	"testing"
)

func newTestRaft(t *testing.T, id NodeID, peers []NodeID, seed int64) *Raft {
	t.Helper()
	return New(Config{ID: id, Peers: peers, Rand: rand.New(rand.NewSource(seed))})
}

// tickUntilCandidate ticks until the node starts an election, returning the
// Output of the tick that fired it.
func tickUntilCandidate(t *testing.T, r *Raft) Output {
	t.Helper()
	for i := 0; i < 100; i++ {
		out := r.Step(Tick{})
		if r.Status().Role == Candidate {
			return out
		}
	}
	t.Fatal("node never started an election within 100 ticks")
	return Output{}
}

func TestSingleNodeElectsItself(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1}, 1)
	for i := 0; i < 100 && r.Status().Role != Leader; i++ {
		r.Step(Tick{})
	}
	if got := r.Status(); got.Role != Leader || got.Term != 1 {
		t.Fatalf("single node should elect itself at term 1, got %+v", got)
	}
}

func TestElectionRequestsVotesAndPersistsSelfVote(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	out := tickUntilCandidate(t, r)

	if out.PersistHard == nil {
		t.Fatal("starting an election must persist term/votedFor")
	}
	if out.PersistHard.Term != 1 || out.PersistHard.VotedFor != 1 {
		t.Fatalf("expected hard state {term:1 vote:1}, got %+v", out.PersistHard)
	}
	if len(out.Send) != 2 {
		t.Fatalf("expected RequestVote to 2 peers, got %d sends", len(out.Send))
	}
	for _, s := range out.Send {
		rv, ok := s.Msg.(RequestVote)
		if !ok || rv.Term != 1 || rv.CandidateID != 1 {
			t.Fatalf("unexpected message %+v", s)
		}
	}
}

func TestMajorityVotesWinElection(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3, 4, 5}, 1)
	tickUntilCandidate(t, r)

	out := r.Step(MsgRecv{From: 2, Msg: RequestVoteReply{Term: 1, Granted: true}})
	if r.Status().Role != Follower && r.Status().Role == Leader {
		t.Fatal("2/5 votes must not win")
	}
	out = r.Step(MsgRecv{From: 3, Msg: RequestVoteReply{Term: 1, Granted: true}})
	if r.Status().Role != Leader {
		t.Fatalf("3/5 votes should win, still %v", r.Status().Role)
	}
	// Winning sends immediate heartbeats to assert leadership.
	if len(out.Send) != 4 {
		t.Fatalf("new leader should heartbeat all 4 peers, sent %d", len(out.Send))
	}
}

func TestDuplicateVoteRepliesDoNotWin(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3, 4, 5}, 1)
	tickUntilCandidate(t, r)
	for i := 0; i < 5; i++ {
		r.Step(MsgRecv{From: 2, Msg: RequestVoteReply{Term: 1, Granted: true}})
	}
	if r.Status().Role == Leader {
		t.Fatal("five copies of one vote are one vote")
	}
}

func TestVoteGrantedOncePerTerm(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	out := r.Step(MsgRecv{From: 2, Msg: RequestVote{Term: 1, CandidateID: 2}})
	if rep := out.Send[0].Msg.(RequestVoteReply); !rep.Granted {
		t.Fatal("first vote in a term should be granted")
	}
	if out.PersistHard == nil || out.PersistHard.VotedFor != 2 {
		t.Fatal("the grant must be persisted before the reply leaves")
	}
	// Same term, different candidate: refused.
	out = r.Step(MsgRecv{From: 3, Msg: RequestVote{Term: 1, CandidateID: 3}})
	if rep := out.Send[0].Msg.(RequestVoteReply); rep.Granted {
		t.Fatal("second candidate in the same term must be refused")
	}
	// Same term, same candidate (retransmitted request): re-granted.
	out = r.Step(MsgRecv{From: 2, Msg: RequestVote{Term: 1, CandidateID: 2}})
	if rep := out.Send[0].Msg.(RequestVoteReply); !rep.Granted {
		t.Fatal("re-request from the voted-for candidate should be re-granted")
	}
}

func TestHigherTermConvertsToFollower(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	tickUntilCandidate(t, r) // term 1 candidate
	out := r.Step(MsgRecv{From: 2, Msg: RequestVote{Term: 5, CandidateID: 2}})
	st := r.Status()
	if st.Role != Follower || st.Term != 5 {
		t.Fatalf("higher term must convert to follower at that term, got %+v", st)
	}
	if out.PersistHard == nil || out.PersistHard.Term != 5 {
		t.Fatal("adopted term must be persisted")
	}
	// The vote itself should also have been granted (empty logs are equal).
	if rep := out.Send[0].Msg.(RequestVoteReply); !rep.Granted {
		t.Fatal("vote should be granted after adopting the higher term")
	}
}

// The reply term rule: a stale leader learns it is deposed from a reply.
func TestHigherTermInReplyConvertsLeader(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	tickUntilCandidate(t, r)
	r.Step(MsgRecv{From: 2, Msg: RequestVoteReply{Term: 1, Granted: true}})
	if r.Status().Role != Leader {
		t.Fatal("setup: should be leader")
	}
	r.Step(MsgRecv{From: 3, Msg: AppendEntriesReply{Term: 9, Success: false}})
	if st := r.Status(); st.Role != Follower || st.Term != 9 {
		t.Fatalf("reply with higher term must depose the leader, got %+v", st)
	}
}

func TestCandidateConcedesToSameTermLeader(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	tickUntilCandidate(t, r) // candidate at term 1
	out := r.Step(MsgRecv{From: 2, Msg: AppendEntries{Term: 1, LeaderID: 2}})
	st := r.Status()
	if st.Role != Follower || st.LeaderHint != 2 {
		t.Fatalf("candidate must concede to a same-term leader, got %+v", st)
	}
	if rep := out.Send[0].Msg.(AppendEntriesReply); !rep.Success {
		t.Fatal("heartbeat on an empty log should succeed")
	}
}

func TestStaleTermMessagesRejected(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	tickUntilCandidate(t, r)
	for i := 0; i < 100 && r.Status().Term < 2; i++ {
		r.Step(Tick{}) // let the first election time out; term 2 follows
	}
	if r.Status().Term != 2 {
		t.Fatalf("setup: wanted term 2, got %d", r.Status().Term)
	}

	out := r.Step(MsgRecv{From: 2, Msg: RequestVote{Term: 1, CandidateID: 2}})
	if rep := out.Send[0].Msg.(RequestVoteReply); rep.Granted || rep.Term != 2 {
		t.Fatalf("stale RequestVote must be refused with current term, got %+v", rep)
	}
	out = r.Step(MsgRecv{From: 2, Msg: AppendEntries{Term: 1, LeaderID: 2}})
	if rep := out.Send[0].Msg.(AppendEntriesReply); rep.Success || rep.Term != 2 {
		t.Fatalf("stale AppendEntries must be refused with current term, got %+v", rep)
	}
}

// A rejected RequestVote reply must not reset the election timer (liveness:
// a stale-logged node could otherwise suppress elections indefinitely).
func TestRejectedVoteDoesNotResetTimer(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	tickUntilCandidate(t, r)
	// Feed rejections forever; the candidate must eventually re-elect
	// (timeout fires again) rather than stall.
	startTerm := r.Status().Term
	for i := 0; i < 100 && r.Status().Term == startTerm; i++ {
		r.Step(MsgRecv{From: 2, Msg: RequestVoteReply{Term: startTerm, Granted: false}})
		r.Step(Tick{})
	}
	if r.Status().Term == startTerm {
		t.Fatal("candidate never retried the election despite rejections")
	}
}

func TestElectionTimeoutRedrawnEachReset(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	seen := map[int]bool{}
	for i := 0; i < 50; i++ {
		r.resetElectionTimer()
		if r.electionTimeout < 10 || r.electionTimeout >= 20 {
			t.Fatalf("timeout %d outside [10,20)", r.electionTimeout)
		}
		seen[r.electionTimeout] = true
	}
	if len(seen) < 5 {
		t.Fatalf("50 redraws produced only %d distinct timeouts — not randomized?", len(seen))
	}
}

func TestLogTermBoundaries(t *testing.T) {
	l := newLog()
	if got, ok := l.Term(0); !ok || got != 0 {
		t.Fatalf("Term(0) of empty log = (%d,%v), want (0,true)", got, ok)
	}
	if _, ok := l.Term(1); ok {
		t.Fatal("Term(1) of empty log should be unavailable")
	}
	l.Append(Entry{Term: 1, Index: 1}, Entry{Term: 2, Index: 2})
	if l.LastIndex() != 2 || l.LastTerm() != 2 {
		t.Fatalf("last = (%d,%d), want (2,2)", l.LastIndex(), l.LastTerm())
	}
	l.TruncateFrom(2)
	if l.LastIndex() != 1 || l.LastTerm() != 1 {
		t.Fatalf("after truncate last = (%d,%d), want (1,1)", l.LastIndex(), l.LastTerm())
	}
}
