package raft

import (
	"math/rand"
	"testing"
)

func newPreVoteRaft(t *testing.T, id NodeID, peers []NodeID, seed int64) *Raft {
	t.Helper()
	return New(Config{ID: id, Peers: peers, Rand: rand.New(rand.NewSource(seed))})
}

// A timeout starts a PRE-vote round: no term bump, no role change, no
// persisted state — just a question.
func TestPreVoteRoundIsFree(t *testing.T) {
	r := newPreVoteRaft(t, 1, []NodeID{1, 2, 3}, 1)
	var out Output
	for i := 0; i < 100; i++ {
		out = r.Step(Tick{})
		if len(out.Send) > 0 {
			break
		}
	}
	if len(out.Send) != 2 {
		t.Fatalf("expected PreVote to 2 peers, got %+v", out.Send)
	}
	pv, ok := out.Send[0].Msg.(PreVote)
	if !ok || pv.Term != 1 {
		t.Fatalf("expected PreVote for hypothetical term 1, got %+v", out.Send[0].Msg)
	}
	st := r.Status()
	if st.Role != Follower || st.Term != 0 {
		t.Fatalf("pre-vote must not change role or term: %+v", st)
	}
	if out.PersistHard != nil {
		t.Fatal("pre-vote must not persist anything")
	}
}

// A majority of pre-grants converts into a real election.
func TestPreVoteMajorityStartsRealElection(t *testing.T) {
	r := newPreVoteRaft(t, 1, []NodeID{1, 2, 3}, 1)
	for i := 0; i < 100 && r.preVotesGranted == nil; i++ {
		r.Step(Tick{})
	}
	out := r.Step(MsgRecv{From: 2, Msg: PreVoteReply{Term: 0, Granted: true}})
	st := r.Status()
	if st.Role != Candidate || st.Term != 1 {
		t.Fatalf("2/3 pre-grants should start a real term-1 election, got %+v", st)
	}
	if out.PersistHard == nil || out.PersistHard.VotedFor != 1 {
		t.Fatal("the real election must persist the self-vote")
	}
}

// A node being actively led refuses pre-votes — the rule that stops a
// rejoining node from campaigning against a healthy leader.
func TestPreVoteRefusedWhileBeingLed(t *testing.T) {
	r := newPreVoteRaft(t, 1, []NodeID{1, 2, 3}, 1)
	// Leader 2 establishes itself at term 3.
	r.Step(MsgRecv{From: 2, Msg: AppendEntries{Term: 3, LeaderID: 2}})
	out := r.Step(MsgRecv{From: 3, Msg: PreVote{Term: 4, CandidateID: 3}})
	rep := out.Send[0].Msg.(PreVoteReply)
	if rep.Granted {
		t.Fatal("a node hearing from a live leader must refuse pre-votes")
	}
	if r.Status().Term != 3 {
		t.Fatalf("the pre-vote must not have disturbed the term: %d", r.Status().Term)
	}
	// Once the leader goes quiet past the minimum timeout, grants resume.
	for i := 0; i < r.etMin; i++ {
		r.electionElapsed++ // silence without ticking a full round
	}
	out = r.Step(MsgRecv{From: 3, Msg: PreVote{Term: 4, CandidateID: 3}})
	if rep := out.Send[0].Msg.(PreVoteReply); !rep.Granted {
		t.Fatal("after leader silence the pre-vote should be granted")
	}
}

// A stale pre-vote round must not fire an election after a real leader
// makes contact.
func TestLeaderContactAbandonsPreVoteRound(t *testing.T) {
	r := newPreVoteRaft(t, 1, []NodeID{1, 2, 3}, 1)
	for i := 0; i < 100 && r.preVotesGranted == nil; i++ {
		r.Step(Tick{})
	}
	r.Step(MsgRecv{From: 2, Msg: AppendEntries{Term: 1, LeaderID: 2}})
	// A late pre-grant arrives for the abandoned round.
	r.Step(MsgRecv{From: 3, Msg: PreVoteReply{Term: 0, Granted: true}})
	if st := r.Status(); st.Role != Follower || st.Term != 1 {
		t.Fatalf("late pre-grants must not fire an election under a leader: %+v", st)
	}
}

// CheckQuorum: a leader that cannot reach a majority for a full election
// window steps down rather than serving a partitioned minority.
func TestCheckQuorumStepsDownIsolatedLeader(t *testing.T) {
	r := newPreVoteRaft(t, 1, []NodeID{1, 2, 3}, 1)
	// Win a real election the pre-vote way.
	for i := 0; i < 100 && r.preVotesGranted == nil; i++ {
		r.Step(Tick{})
	}
	r.Step(MsgRecv{From: 2, Msg: PreVoteReply{Term: 0, Granted: true}})
	r.Step(MsgRecv{From: 2, Msg: RequestVoteReply{Term: 1, Granted: true}})
	if r.Status().Role != Leader {
		t.Fatal("setup: should be leader")
	}
	// Total silence: after one full etMax window the leader steps down.
	for i := 0; i < r.etMax+1 && r.Status().Role == Leader; i++ {
		r.Step(Tick{})
	}
	if st := r.Status(); st.Role != Follower {
		t.Fatalf("isolated leader must step down via CheckQuorum, got %+v", st)
	}
	// With regular follower contact it stays leader indefinitely.
	r2 := newPreVoteRaft(t, 1, []NodeID{1, 2, 3}, 2)
	for i := 0; i < 100 && r2.preVotesGranted == nil; i++ {
		r2.Step(Tick{})
	}
	r2.Step(MsgRecv{From: 2, Msg: PreVoteReply{Term: 0, Granted: true}})
	r2.Step(MsgRecv{From: 2, Msg: RequestVoteReply{Term: 1, Granted: true}})
	for i := 0; i < 3*r2.etMax; i++ {
		r2.Step(Tick{})
		if i%2 == 0 {
			r2.Step(MsgRecv{From: 2, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1}})
			r2.Step(MsgRecv{From: 3, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1}})
		}
	}
	if r2.Status().Role != Leader {
		t.Fatal("a leader with majority contact must not step down")
	}
}
