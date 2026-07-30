package raft

import "testing"

// electTestLeader hand-drives r to leadership of a 3-node cluster.
func electTestLeader(t *testing.T, r *Raft) {
	t.Helper()
	tickUntilCandidate(t, r)
	r.Step(MsgRecv{From: 2, Msg: RequestVoteReply{Term: r.Status().Term, Granted: true}})
	if r.Status().Role != Leader {
		t.Fatal("setup: expected leadership")
	}
}

// ackNoop makes follower 2 acknowledge everything, committing the no-op.
func ackNoop(t *testing.T, r *Raft) {
	t.Helper()
	r.Step(MsgRecv{From: 2, Msg: AppendEntriesReply{Term: r.Status().Term, Success: true, MatchIndex: r.Status().LastIndex}})
	if r.Status().CommitIndex < r.Status().LastIndex {
		t.Fatal("setup: no-op did not commit")
	}
}

// The gate: a new leader serves no reads until its own-term no-op commits —
// its commitIndex can be behind entries the previous leader committed.
func TestReadIndexGatedOnNoopCommit(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	electTestLeader(t, r)

	out := r.Step(ReadIndexReq{Ctx: 41})
	if len(out.ReadReady) != 0 {
		t.Fatalf("read served before the no-op committed: %+v", out.ReadReady)
	}
	// Committing the no-op opens the gate and dispatches the queued round.
	out = r.Step(MsgRecv{From: 2, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1}})
	round := uint64(0)
	for _, s := range out.Send {
		if ae, ok := s.Msg.(AppendEntries); ok && ae.Round != 0 {
			round = ae.Round
		}
	}
	if round == 0 {
		t.Fatal("no tagged confirmation round dispatched after the gate opened")
	}
	// The round's ack completes the read at the recorded index.
	out = r.Step(MsgRecv{From: 2, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1, Round: round}})
	if len(out.ReadReady) != 1 || !out.ReadReady[0].OK || out.ReadReady[0].Ctx != 41 || out.ReadReady[0].Index != 1 {
		t.Fatalf("read not completed correctly: %+v", out.ReadReady)
	}
}

// Acks from earlier heartbeats must not count toward a later round —
// response reordering would sneak the deposed-leader race back in.
func TestReadIndexIgnoresStaleRoundAcks(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3, 4, 5}, 1)
	tickUntilCandidate(t, r)
	r.Step(MsgRecv{From: 2, Msg: RequestVoteReply{Term: 1, Granted: true}})
	r.Step(MsgRecv{From: 3, Msg: RequestVoteReply{Term: 1, Granted: true}})
	// Commit the no-op with acks from 2 and 3.
	r.Step(MsgRecv{From: 2, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1}})
	r.Step(MsgRecv{From: 3, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1}})

	out := r.Step(ReadIndexReq{Ctx: 7})
	var round uint64
	for _, s := range out.Send {
		if ae, ok := s.Msg.(AppendEntries); ok {
			round = ae.Round
		}
	}
	if round == 0 {
		t.Fatal("round should dispatch immediately (gate already open)")
	}
	// Stale echoes: Round 0 (pre-round heartbeats) and a made-up old round.
	out = r.Step(MsgRecv{From: 2, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1, Round: 0}})
	if len(out.ReadReady) != 0 {
		t.Fatal("round-0 echo must not confirm the read")
	}
	out = r.Step(MsgRecv{From: 3, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1, Round: round + 99}})
	if len(out.ReadReady) != 0 {
		t.Fatal("foreign round echo must not confirm the read")
	}
	// Two genuine echoes (majority of 5 = 3 incl. self) complete it.
	r.Step(MsgRecv{From: 2, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1, Round: round}})
	out = r.Step(MsgRecv{From: 3, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1, Round: round}})
	if len(out.ReadReady) != 1 || !out.ReadReady[0].OK {
		t.Fatalf("genuine round echoes should complete the read: %+v", out.ReadReady)
	}
}

// A failed consistency check still confirms leadership: the follower
// processed our current-term append. Its echo counts.
func TestReadIndexRejectionEchoStillConfirms(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	electTestLeader(t, r)
	ackNoop(t, r)

	out := r.Step(ReadIndexReq{Ctx: 9})
	var round uint64
	for _, s := range out.Send {
		if ae, ok := s.Msg.(AppendEntries); ok {
			round = ae.Round
		}
	}
	out = r.Step(MsgRecv{From: 3, Msg: AppendEntriesReply{
		Term: 1, Success: false, ConflictIndex: 1, Round: round,
	}})
	if len(out.ReadReady) != 1 || !out.ReadReady[0].OK {
		t.Fatalf("a same-term rejection echoing the round must confirm: %+v", out.ReadReady)
	}
}

// Deposed mid-round: every pending read fails immediately — a stale leader
// must never serve them.
func TestReadIndexFailsOnStepDown(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	electTestLeader(t, r)
	ackNoop(t, r)

	r.Step(ReadIndexReq{Ctx: 1})
	r.Step(ReadIndexReq{Ctx: 2}) // joins the queue behind the in-flight round
	out := r.Step(MsgRecv{From: 3, Msg: RequestVote{Term: 9, CandidateID: 3, LastLogIndex: 5, LastLogTerm: 8}})
	if len(out.ReadReady) != 2 {
		t.Fatalf("step-down must fail all pending reads, got %+v", out.ReadReady)
	}
	for _, rs := range out.ReadReady {
		if rs.OK {
			t.Fatalf("read %d served by a deposed leader", rs.Ctx)
		}
	}
}

// Reads on a non-leader fail immediately (the server redirects).
func TestReadIndexOnFollowerFails(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	out := r.Step(ReadIndexReq{Ctx: 3})
	if len(out.ReadReady) != 1 || out.ReadReady[0].OK {
		t.Fatalf("follower must fail reads: %+v", out.ReadReady)
	}
}

// Reads arriving mid-round batch into the NEXT round at a fresh readIndex.
func TestReadIndexBatchesBehindInflightRound(t *testing.T) {
	r := newTestRaft(t, 1, []NodeID{1, 2, 3}, 1)
	electTestLeader(t, r)
	ackNoop(t, r)

	out := r.Step(ReadIndexReq{Ctx: 1})
	var round1 uint64
	for _, s := range out.Send {
		if ae, ok := s.Msg.(AppendEntries); ok {
			round1 = ae.Round
		}
	}
	r.Step(ReadIndexReq{Ctx: 2})
	r.Step(ReadIndexReq{Ctx: 3})
	// Completing round 1 answers ctx 1 and dispatches one round for 2+3.
	out = r.Step(MsgRecv{From: 2, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1, Round: round1}})
	if len(out.ReadReady) != 1 || out.ReadReady[0].Ctx != 1 {
		t.Fatalf("round 1 should answer only ctx 1: %+v", out.ReadReady)
	}
	var round2 uint64
	for _, s := range out.Send {
		if ae, ok := s.Msg.(AppendEntries); ok && ae.Round > round1 {
			round2 = ae.Round
		}
	}
	if round2 == 0 {
		t.Fatal("queued reads should dispatch a fresh round")
	}
	out = r.Step(MsgRecv{From: 3, Msg: AppendEntriesReply{Term: 1, Success: true, MatchIndex: 1, Round: round2}})
	if len(out.ReadReady) != 2 {
		t.Fatalf("round 2 should answer ctx 2 and 3 together: %+v", out.ReadReady)
	}
}
