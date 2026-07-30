package raft

import (
	"fmt"
	"math/rand"
	"sort"
)

// Config configures a Raft core. Peers must contain every cluster member,
// including ID. Rand is the core's only entropy source — seed it in tests.
type Config struct {
	ID    NodeID
	Peers []NodeID
	Rand  *rand.Rand

	// Tick counts. Election timeout is redrawn uniformly from
	// [ElectionTickMin, ElectionTickMax) at every timer reset — a fixed
	// per-node draw re-collides forever. Zero values take the defaults.
	ElectionTickMin int // default 10
	ElectionTickMax int // default 20
	HeartbeatTicks  int // default 3

	// DisablePreVote reverts to paper-basic elections: no pre-vote round
	// and no CheckQuorum. On by default because together they stop a
	// rejoining partitioned/restarted node from bumping the term and
	// deposing a healthy leader, and make an isolated leader step down.
	// Tests that hand-drive raw election machinery (and the Figure 8
	// scenario, which deliberately stages the old dynamics) disable them.
	DisablePreVote bool
}

// Raft is the pure consensus core. Not safe for concurrent use: Step is
// single-threaded and non-reentrant, and each Output must be fully processed
// before the next Step.
type Raft struct {
	id    NodeID
	peers []NodeID // sorted, includes self; a slice, never a map, so iteration order is deterministic
	rng   *rand.Rand

	role     Role
	term     uint64
	votedFor NodeID
	log      *raftLog

	commitIndex uint64
	lastApplied uint64
	leaderHint  NodeID

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	etMin, etMax     int
	heartbeatTicks   int

	// Candidate state: which nodes granted us a vote this term. Only len()
	// is ever taken — iteration order can never leak into an Output.
	votesGranted map[NodeID]bool

	// Pre-vote round state (nil = no round in flight), and CheckQuorum
	// contact tracking (leader only).
	preVoteOn       bool
	preVotesGranted map[NodeID]bool
	contacted       map[NodeID]bool
	quorumElapsed   int

	// Leader state, keyed by NodeID but only ever iterated via r.peers.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	// ReadIndex state (leader only). The gate: no reads are served until
	// this leader's own-term no-op (noopIndex) commits — before that its
	// commitIndex can be BEHIND entries the previous leader committed (it
	// holds them but cannot count them), so readIndex := commitIndex would
	// miss committed writes. The most-omitted line in ReadIndex writeups.
	noopIndex    uint64
	readQueue    []uint64 // ctxs accumulating for the next round
	roundCounter uint64
	curRound     uint64 // 0 = no confirmation round in flight
	roundIndex   uint64 // readIndex recorded when curRound dispatched
	roundReads   []uint64
	roundAcks    map[NodeID]bool

	// Latest retained snapshot, for shipping to laggards whose nextIndex
	// fell below our first retained entry.
	snapIndex uint64
	snapTerm  uint64
	snapData  []byte

	// Per-Step scratch for building Output.PersistHard.
	hardDirty    bool
	truncateFrom uint64
	appended     []Entry

	// unsafeCommitRule reverts the core to naive Raft: no term clause in
	// the commit rule and no leader no-op. Exists only so the simulator can
	// prove the harness catches the Figure 8 bug — see
	// EnableUnsafeCommitRuleForTesting.
	unsafeCommitRule bool
}

// EnableUnsafeCommitRuleForTesting switches this core to the buggy commit
// rule (count any majority-replicated entry, no §5.4.2 term clause) and
// disables the new-leader no-op. Poor-man's mutation testing: scenario
// tests flip this on and assert the invariant checkers DO fire — the
// harness is itself tested. Never call it outside tests.
func (r *Raft) EnableUnsafeCommitRuleForTesting() {
	r.unsafeCommitRule = true
}

// New creates a follower at term 0 with an empty log.
func New(cfg Config) *Raft {
	if cfg.ID == None {
		panic("raft: ID must be non-zero")
	}
	if cfg.Rand == nil {
		panic("raft: Config.Rand is required (the core has no entropy of its own)")
	}
	peers := append([]NodeID(nil), cfg.Peers...)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	found := false
	for _, p := range peers {
		found = found || p == cfg.ID
	}
	if !found {
		panic(fmt.Sprintf("raft: ID %d not in Peers %v", cfg.ID, peers))
	}
	if cfg.ElectionTickMin == 0 {
		cfg.ElectionTickMin = 10
	}
	if cfg.ElectionTickMax == 0 {
		cfg.ElectionTickMax = 20
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 3
	}
	if cfg.ElectionTickMax <= cfg.ElectionTickMin {
		panic("raft: ElectionTickMax must exceed ElectionTickMin")
	}
	r := &Raft{
		id:             cfg.ID,
		peers:          peers,
		rng:            cfg.Rand,
		role:           Follower,
		log:            newLog(),
		etMin:          cfg.ElectionTickMin,
		etMax:          cfg.ElectionTickMax,
		heartbeatTicks: cfg.HeartbeatTicks,
		preVoteOn:      !cfg.DisablePreVote,
	}
	r.resetElectionTimer()
	return r
}

// RestoredState is what the storage layer recovered at startup. Entries
// must be contiguous from SnapIndex+1.
type RestoredState struct {
	Term      uint64
	VotedFor  NodeID
	SnapIndex uint64
	SnapTerm  uint64
	SnapData  []byte
	Entries   []Entry
}

// Restore rebuilds a core from persisted state after a crash-restart.
// Volatile role state is deliberately not restored, and commitIndex
// restarts at the snapshot boundary — everything above it is rediscovered
// through the first AppendEntries exchange, and the state machine rebuilds
// from the snapshot plus replayed applies.
func Restore(cfg Config, s RestoredState) *Raft {
	r := New(cfg)
	r.term = s.Term
	r.votedFor = s.VotedFor
	for i, e := range s.Entries {
		if e.Index != s.SnapIndex+uint64(i)+1 {
			panic(fmt.Sprintf("raft: restored log not contiguous from %d: position %d holds index %d", s.SnapIndex+1, i, e.Index))
		}
	}
	r.log.first = s.SnapIndex + 1
	r.log.snapIndex, r.log.snapTerm = s.SnapIndex, s.SnapTerm
	r.log.entries = append([]Entry(nil), s.Entries...)
	r.snapIndex, r.snapTerm, r.snapData = s.SnapIndex, s.SnapTerm, s.SnapData
	r.commitIndex, r.lastApplied = s.SnapIndex, s.SnapIndex
	return r
}

// Status returns a read-only snapshot of the core.
func (r *Raft) Status() Status {
	return Status{
		ID:          r.id,
		Role:        r.role,
		Term:        r.term,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
		LastIndex:   r.log.LastIndex(),
		LeaderHint:  r.leaderHint,
	}
}

// Step consumes one input event and returns the commands it produced.
// See Output for the ordering contract the caller must honour.
func (r *Raft) Step(in Input) Output {
	r.hardDirty, r.truncateFrom, r.appended = false, 0, nil
	var out Output
	switch v := in.(type) {
	case Tick:
		r.tick(&out)
	case MsgRecv:
		r.recv(v.From, v.Msg, &out)
	case Propose:
		r.propose(v.Data, &out)
	case ReadIndexReq:
		r.readIndex(v.Ctx, &out)
	case Compact:
		r.compact(v.Index, v.Data, &out)
	default:
		panic(fmt.Sprintf("raft: unknown input %T", in))
	}
	// Losing leadership fails every pending read immediately; the server
	// layer turns that into a retryable redirect.
	if r.role != Leader && (len(r.readQueue) > 0 || r.curRound != 0) {
		for _, ctx := range r.readQueue {
			out.ReadReady = append(out.ReadReady, ReadState{Ctx: ctx})
		}
		for _, ctx := range r.roundReads {
			out.ReadReady = append(out.ReadReady, ReadState{Ctx: ctx})
		}
		r.readQueue, r.roundReads, r.roundAcks = nil, nil, nil
		r.curRound = 0
	}
	if r.hardDirty || r.truncateFrom != 0 || len(r.appended) > 0 {
		out.PersistHard = &HardState{
			Term:         r.term,
			VotedFor:     r.votedFor,
			TruncateFrom: r.truncateFrom,
			Append:       r.appended,
		}
	}
	return out
}

// --- timers ---

func (r *Raft) tick(out *Output) {
	if r.role == Leader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTicks {
			r.heartbeatElapsed = 0
			r.broadcastAppend(out)
		}
		// CheckQuorum: a leader that cannot reach a majority within a full
		// election-timeout window steps down instead of uselessly serving
		// a partitioned minority (its reads would already fail, but its
		// waiters and clients learn faster this way).
		if r.preVoteOn {
			r.quorumElapsed++
			if r.quorumElapsed >= r.etMax {
				r.quorumElapsed = 0
				count := 1 // self
				for _, p := range r.peers {
					if p != r.id && r.contacted[p] {
						count++
					}
				}
				r.contacted = make(map[NodeID]bool)
				if !r.hasMajority(count) {
					r.becomeFollower(r.term, None)
					return
				}
			}
		}
		return
	}
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		if r.preVoteOn {
			r.startPreVote(out)
		} else {
			r.startElection(out)
		}
	}
}

// startPreVote canvasses for a hypothetical election at term+1 without
// touching term, vote, or role. Only a majority of willing granters
// converts it into a real election — a node that partitions back into the
// cluster can no longer bump terms and depose a healthy leader.
func (r *Raft) startPreVote(out *Output) {
	r.resetElectionTimer()
	r.preVotesGranted = map[NodeID]bool{r.id: true}
	if r.hasMajority(len(r.preVotesGranted)) { // single-node cluster
		r.startElection(out)
		return
	}
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		out.Send = append(out.Send, AddressedMsg{To: p, Msg: PreVote{
			Term:         r.term + 1,
			CandidateID:  r.id,
			LastLogIndex: r.log.LastIndex(),
			LastLogTerm:  r.log.LastTerm(),
		}})
	}
}

// resetElectionTimer redraws the timeout. The timer resets on exactly three
// events: granting a vote, a valid AppendEntries from the current-term
// leader, and starting an election. Resetting anywhere else (e.g. on
// rejected RequestVotes) lets a stale-logged node suppress legitimate
// elections indefinitely.
func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	r.electionTimeout = r.etMin + r.rng.Intn(r.etMax-r.etMin)
}

// --- role transitions ---

func (r *Raft) startElection(out *Output) {
	r.role = Candidate
	r.term++
	r.votedFor = r.id
	r.hardDirty = true
	r.leaderHint = None
	r.votesGranted = map[NodeID]bool{r.id: true}
	r.preVotesGranted = nil
	r.resetElectionTimer()
	if r.hasMajority(len(r.votesGranted)) { // single-node cluster
		r.becomeLeader(out)
		return
	}
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		out.Send = append(out.Send, AddressedMsg{To: p, Msg: RequestVote{
			Term:         r.term,
			CandidateID:  r.id,
			LastLogIndex: r.log.LastIndex(),
			LastLogTerm:  r.log.LastTerm(),
		}})
	}
}

func (r *Raft) hasMajority(n int) bool {
	return n > len(r.peers)/2
}

func (r *Raft) becomeLeader(out *Output) {
	r.role = Leader
	r.leaderHint = r.id
	r.heartbeatElapsed = 0
	r.nextIndex = make(map[NodeID]uint64, len(r.peers))
	r.matchIndex = make(map[NodeID]uint64, len(r.peers))
	for _, p := range r.peers {
		r.nextIndex[p] = r.log.LastIndex() + 1
		r.matchIndex[p] = 0
	}
	// matchIndex[self] tracks the leader's own last index so it counts
	// toward its majority; forgetting this stalls commits and is invisible
	// in happy-path tests with fast followers.
	r.matchIndex[r.id] = r.log.LastIndex()
	r.contacted = make(map[NodeID]bool)
	r.quorumElapsed = 0
	r.noopIndex = 0
	r.readQueue, r.roundReads, r.roundAcks = nil, nil, nil
	r.curRound = 0
	// The no-op is not an optimization: under the §5.4.2 commit rule it is
	// the only way prior-term entries ever commit, and ReadIndex is
	// unserviceable until it commits.
	if !r.unsafeCommitRule {
		e := r.appendToOwnLog(nil)
		r.noopIndex = e.Index
		r.maybeCommit(out) // single-node cluster commits immediately
	}
	r.broadcastAppend(out)
}

// appendToOwnLog appends one entry (nil data = the leadership no-op) to the
// leader's log, records it for persistence, and keeps matchIndex[self] true.
func (r *Raft) appendToOwnLog(data []byte) Entry {
	e := Entry{Term: r.term, Index: r.log.LastIndex() + 1, Data: data}
	r.log.Append(e)
	r.appended = append(r.appended, e)
	r.matchIndex[r.id] = e.Index
	return e
}

func (r *Raft) becomeFollower(term uint64, leader NodeID) {
	if term > r.term {
		r.term = term
		r.votedFor = None
		r.hardDirty = true
	}
	r.role = Follower
	r.leaderHint = leader
	r.votesGranted = nil
	r.preVotesGranted = nil
	r.nextIndex, r.matchIndex = nil, nil
	r.contacted = nil
}

// --- proposals ---

// propose is the client write path. On a non-leader it yields no Receipt —
// just the leader hint in Status, which the server layer turns into a
// redirect. On a leader the entry is appended, a Receipt emitted for the
// write path's waiter, and replication starts immediately.
func (r *Raft) propose(data []byte, out *Output) {
	if r.role != Leader {
		return
	}
	e := r.appendToOwnLog(data)
	out.Proposed = &Receipt{Index: e.Index, Term: e.Term}
	r.maybeCommit(out) // single-node cluster
	r.broadcastAppend(out)
}

// --- replication (leader side) ---

// broadcastAppend sends AppendEntries to every peer, each from its own
// nextIndex; a caught-up peer gets an empty append — the heartbeat.
func (r *Raft) broadcastAppend(out *Output) {
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.sendAppend(p, out)
	}
}

func (r *Raft) sendAppend(p NodeID, out *Output) {
	next := r.nextIndex[p]
	prevTerm, ok := r.log.Term(next - 1)
	if !ok {
		// nextIndex fell below our first retained entry: ship the snapshot.
		// Optimistically advance nextIndex so heartbeats don't re-send the
		// whole snapshot every interval; if the follower didn't install it,
		// its conflict replies walk us back here and we send it again.
		if r.snapIndex == 0 {
			panic("raft: peer below first entry but no snapshot retained")
		}
		out.Send = append(out.Send, AddressedMsg{To: p, Msg: InstallSnapshot{
			Term: r.term, LeaderID: r.id,
			LastIncludedIndex: r.snapIndex, LastIncludedTerm: r.snapTerm,
			Data: r.snapData,
		}})
		r.nextIndex[p] = r.snapIndex + 1
		return
	}
	out.Send = append(out.Send, AddressedMsg{To: p, Msg: AppendEntries{
		Term:         r.term,
		LeaderID:     r.id,
		PrevLogIndex: next - 1,
		PrevLogTerm:  prevTerm,
		Entries:      r.log.Slice(next, r.log.LastIndex()+1),
		LeaderCommit: r.commitIndex,
		Round:        r.curRound,
	}})
}

// --- linearizable reads (ReadIndex) ---

func (r *Raft) readIndex(ctx uint64, out *Output) {
	if r.role != Leader {
		out.ReadReady = append(out.ReadReady, ReadState{Ctx: ctx})
		return
	}
	r.readQueue = append(r.readQueue, ctx)
	r.tryDispatchRound(out)
}

// tryDispatchRound starts a confirmation round for the queued reads: the
// round number is issued AFTER readIndex is recorded, and reads arriving
// while a round is in flight join the next one. Blocked until the gate
// (own-term no-op committed) passes.
func (r *Raft) tryDispatchRound(out *Output) {
	if r.curRound != 0 || len(r.readQueue) == 0 || r.noopIndex == 0 || r.commitIndex < r.noopIndex {
		return
	}
	r.roundIndex = r.commitIndex
	r.roundCounter++
	r.curRound = r.roundCounter
	r.roundReads = r.readQueue
	r.readQueue = nil
	r.roundAcks = map[NodeID]bool{r.id: true}
	if r.hasMajority(len(r.roundAcks)) { // single-node cluster
		r.completeRound(out)
		return
	}
	r.broadcastAppend(out)
}

func (r *Raft) completeRound(out *Output) {
	for _, ctx := range r.roundReads {
		out.ReadReady = append(out.ReadReady, ReadState{Ctx: ctx, Index: r.roundIndex, OK: true})
	}
	r.curRound = 0
	r.roundReads, r.roundAcks = nil, nil
	r.tryDispatchRound(out) // reads that arrived mid-round go next
}

func (r *Raft) handleAppendEntriesReply(from NodeID, m AppendEntriesReply, out *Output) {
	if r.role != Leader || m.Term < r.term {
		return // stale reply
	}
	// ReadIndex confirmation: ANY same-term reply echoing the current round
	// proves the follower recognizes our leadership now — a failed
	// consistency check confirms leadership just as well as a success.
	// Earlier rounds' echoes must not count (response reordering).
	if r.curRound != 0 && m.Round == r.curRound {
		r.roundAcks[from] = true
		if r.hasMajority(len(r.roundAcks)) {
			r.completeRound(out)
		}
	}
	if m.Success {
		// Replies reorder in flight: only ever advance.
		if m.MatchIndex > r.matchIndex[from] {
			r.matchIndex[from] = m.MatchIndex
			r.nextIndex[from] = m.MatchIndex + 1
			r.maybeCommit(out)
		}
		return
	}
	// Fast backtracking: if we have entries in ConflictTerm, resume after
	// our last entry of that term; otherwise jump to ConflictIndex.
	next := m.ConflictIndex
	if m.ConflictTerm != 0 {
		if li, ok := r.log.lastIndexOfTerm(m.ConflictTerm); ok {
			next = li + 1
		}
	}
	if next < 1 {
		next = 1
	}
	// A reordered stale rejection must never drag nextIndex below what the
	// follower has already acknowledged.
	if next <= r.matchIndex[from] {
		next = r.matchIndex[from] + 1
	}
	r.nextIndex[from] = next
	r.sendAppend(from, out)
}

// maybeCommit advances commitIndex to the highest N where a majority of
// matchIndex >= N AND log[N].Term == currentTerm — the §5.4.2 subtlety:
// never commit prior-term entries by counting replicas (Figure 8). They
// still replicate eagerly; they just can't be counted until the election
// no-op commits above them.
func (r *Raft) maybeCommit(out *Output) {
	for n := r.log.LastIndex(); n > r.commitIndex; n-- {
		t, ok := r.log.Term(n)
		if !ok {
			return
		}
		if t < r.term && !r.unsafeCommitRule {
			return // everything below is older still — nothing can commit
		}
		if t > r.term {
			panic("raft: log entry from the future")
		}
		count := 0
		for _, p := range r.peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if r.hasMajority(count) {
			r.commitIndex = n
			r.emitApply(out)
			// Commit advancing can open the read gate (no-op committed).
			r.tryDispatchRound(out)
			return
		}
	}
}

// emitApply hands newly committed entries to the caller for the state
// machine. Apply may lag arbitrarily behind commit — that only delays
// reads, never safety.
func (r *Raft) emitApply(out *Output) {
	if r.commitIndex > r.lastApplied {
		out.ApplyEntries = append(out.ApplyEntries, r.log.Slice(r.lastApplied+1, r.commitIndex+1)...)
		r.lastApplied = r.commitIndex
	}
}

// --- message handling (follower side) ---

func msgTerm(m Message) uint64 {
	switch v := m.(type) {
	case RequestVote:
		return v.Term
	case RequestVoteReply:
		return v.Term
	case AppendEntries:
		return v.Term
	case AppendEntriesReply:
		return v.Term
	case InstallSnapshot:
		return v.Term
	case InstallSnapshotReply:
		return v.Term
	case PreVote:
		return v.Term
	case PreVoteReply:
		return v.Term
	}
	panic(fmt.Sprintf("raft: unknown message %T", m))
}

func (r *Raft) recv(from NodeID, m Message, out *Output) {
	// Pre-vote traffic is exempt from the uniform term rule below: its
	// entire purpose is to probe a future term WITHOUT disturbing anyone's
	// current one. (A higher term inside a PreVoteReply still teaches the
	// asker — handled in its own handler.)
	switch v := m.(type) {
	case PreVote:
		r.handlePreVote(from, v, out)
		return
	case PreVoteReply:
		r.handlePreVoteReply(from, v, out)
		return
	}

	// Term rule, applied uniformly to messages and replies: a higher term
	// means adopt it, revert to follower, clear votedFor. Step builds
	// PersistHard at the end, and the caller fsyncs it before Send — so the
	// adoption is durable before anything referencing it leaves the node.
	if t := msgTerm(m); t > r.term {
		leader := None
		switch v := m.(type) {
		case AppendEntries:
			leader = v.LeaderID
		case InstallSnapshot:
			leader = v.LeaderID
		}
		r.becomeFollower(t, leader)
	}

	// CheckQuorum bookkeeping: any same-term message from a peer counts as
	// contact for the current window.
	if r.role == Leader && msgTerm(m) == r.term {
		r.contacted[from] = true
	}

	switch v := m.(type) {
	case RequestVote:
		r.handleRequestVote(from, v, out)
	case RequestVoteReply:
		r.handleRequestVoteReply(from, v, out)
	case AppendEntries:
		r.handleAppendEntries(from, v, out)
	case AppendEntriesReply:
		r.handleAppendEntriesReply(from, v, out)
	case InstallSnapshot:
		r.handleInstallSnapshot(from, v, out)
	case InstallSnapshotReply:
		r.handleInstallSnapshotReply(from, v)
	}
}

// handlePreVote grants iff (a) the hypothetical term is ahead of ours,
// (b) the candidate's log passes the same up-to-date test as a real vote,
// and (c) we are not being led — we're a leader ourselves, or we've heard
// from one within the minimum election timeout, means refuse. Grants are
// not persisted, don't set votedFor, and don't reset timers.
func (r *Raft) handlePreVote(from NodeID, m PreVote, out *Output) {
	upToDate := m.LastLogTerm > r.log.LastTerm() ||
		(m.LastLogTerm == r.log.LastTerm() && m.LastLogIndex >= r.log.LastIndex())
	beingLed := r.role == Leader || (r.leaderHint != None && r.electionElapsed < r.etMin)
	grant := m.Term > r.term && upToDate && !beingLed
	out.Send = append(out.Send, AddressedMsg{To: from, Msg: PreVoteReply{Term: r.term, Granted: grant}})
}

func (r *Raft) handlePreVoteReply(from NodeID, m PreVoteReply, out *Output) {
	if m.Term > r.term {
		// Someone is ahead of us; catch up quietly and abandon the round.
		r.becomeFollower(m.Term, None)
		return
	}
	if r.preVotesGranted == nil || r.role != Follower {
		return // no round in flight (or it already converted)
	}
	if m.Granted {
		r.preVotesGranted[from] = true
		if r.hasMajority(len(r.preVotesGranted)) {
			r.preVotesGranted = nil
			r.startElection(out) // the real thing: term++, persisted self-vote
		}
	}
}

// handleInstallSnapshot implements the §4.1 edge cases: a stale snapshot
// (LastIncludedIndex <= commitIndex) is ignored — it can arrive late
// through a reordering network; a matching (index, term) entry in our log
// means the suffix after it survives; otherwise the whole log is discarded.
func (r *Raft) handleInstallSnapshot(from NodeID, m InstallSnapshot, out *Output) {
	if m.Term < r.term {
		out.Send = append(out.Send, AddressedMsg{To: from, Msg: InstallSnapshotReply{Term: r.term}})
		return
	}
	if r.role != Follower {
		r.becomeFollower(m.Term, m.LeaderID)
	}
	r.leaderHint = m.LeaderID
	r.preVotesGranted = nil // leader contact abandons any pre-vote round
	r.resetElectionTimer() // valid InstallSnapshot from the current-term leader

	if m.LastIncludedIndex <= r.commitIndex {
		// Stale: everything it contains is already committed here. Tell the
		// leader where we actually are so it resumes appends.
		out.Send = append(out.Send, AddressedMsg{To: from, Msg: InstallSnapshotReply{
			Term: r.term, MatchIndex: r.commitIndex,
		}})
		return
	}
	if t, ok := r.log.Term(m.LastIncludedIndex); ok && t == m.LastIncludedTerm {
		r.log.CompactTo(m.LastIncludedIndex, m.LastIncludedTerm) // retain the suffix
	} else {
		r.log.ResetToSnapshot(m.LastIncludedIndex, m.LastIncludedTerm)
		// The WAL may hold entries above the snapshot that we just
		// disowned; log the truncation so recovery cannot resurrect them.
		r.truncateFrom = m.LastIncludedIndex + 1
	}
	r.snapIndex, r.snapTerm, r.snapData = m.LastIncludedIndex, m.LastIncludedTerm, m.Data
	r.commitIndex = m.LastIncludedIndex
	r.lastApplied = m.LastIncludedIndex
	out.Snapshot = &SnapshotOp{Index: m.LastIncludedIndex, Term: m.LastIncludedTerm, Data: m.Data, FromLeader: true}
	out.Send = append(out.Send, AddressedMsg{To: from, Msg: InstallSnapshotReply{
		Term: r.term, MatchIndex: m.LastIncludedIndex,
	}})
}

func (r *Raft) handleInstallSnapshotReply(from NodeID, m InstallSnapshotReply) {
	if r.role != Leader || m.Term < r.term {
		return
	}
	if m.MatchIndex > r.matchIndex[from] {
		r.matchIndex[from] = m.MatchIndex
		r.nextIndex[from] = m.MatchIndex + 1
	}
}

// compact handles the state machine's own snapshot: drop covered entries,
// keep the snapshot for laggards.
func (r *Raft) compact(index uint64, data []byte, out *Output) {
	if index <= r.log.first-1 {
		return // already compacted past here
	}
	if index > r.lastApplied {
		panic(fmt.Sprintf("raft: compacting at %d beyond lastApplied %d", index, r.lastApplied))
	}
	t, ok := r.log.Term(index)
	if !ok {
		panic("raft: compaction point missing from log")
	}
	r.snapIndex, r.snapTerm, r.snapData = index, t, data
	r.log.CompactTo(index, t)
	out.Snapshot = &SnapshotOp{Index: index, Term: t, Data: data}
}

func (r *Raft) handleRequestVote(from NodeID, m RequestVote, out *Output) {
	if m.Term < r.term {
		out.Send = append(out.Send, AddressedMsg{To: from, Msg: RequestVoteReply{Term: r.term, Granted: false}})
		return
	}
	// Election Restriction (§5.4.1): a term-first comparison, not a length
	// comparison — "candidate's log is at least as up-to-date as mine".
	upToDate := m.LastLogTerm > r.log.LastTerm() ||
		(m.LastLogTerm == r.log.LastTerm() && m.LastLogIndex >= r.log.LastIndex())
	grant := (r.votedFor == None || r.votedFor == m.CandidateID) && upToDate
	if grant {
		r.votedFor = m.CandidateID
		r.hardDirty = true // the grant is persisted before the reply leaves
		r.preVotesGranted = nil
		r.resetElectionTimer()
	}
	out.Send = append(out.Send, AddressedMsg{To: from, Msg: RequestVoteReply{Term: r.term, Granted: grant}})
}

func (r *Raft) handleRequestVoteReply(from NodeID, m RequestVoteReply, out *Output) {
	if r.role != Candidate || m.Term < r.term {
		return // stale reply from an earlier election
	}
	if m.Granted {
		r.votesGranted[from] = true
	}
	// No timer reset on rejection — see resetElectionTimer.
	if r.hasMajority(len(r.votesGranted)) {
		r.becomeLeader(out)
	}
}

func (r *Raft) handleAppendEntries(from NodeID, m AppendEntries, out *Output) {
	if m.Term < r.term {
		out.Send = append(out.Send, AddressedMsg{To: from, Msg: AppendEntriesReply{Term: r.term, Success: false}})
		return
	}
	// Same term: a candidate that hears from this term's leader concedes.
	if r.role != Follower {
		r.becomeFollower(m.Term, m.LeaderID)
	}
	r.leaderHint = m.LeaderID
	r.preVotesGranted = nil // leader contact abandons any pre-vote round
	r.resetElectionTimer() // valid AppendEntries from the current-term leader

	// Consistency check: our log must contain an entry at PrevLogIndex with
	// PrevLogTerm.
	prevTerm, ok := r.log.Term(m.PrevLogIndex)
	if !ok {
		// Log too short: tell the leader where our log ends so it can skip
		// straight there instead of decrementing one index per round trip.
		out.Send = append(out.Send, AddressedMsg{To: from, Msg: AppendEntriesReply{
			Term: r.term, Success: false,
			ConflictIndex: r.log.LastIndex() + 1,
			Round:         m.Round,
		}})
		return
	}
	if prevTerm != m.PrevLogTerm {
		// Term mismatch at PrevLogIndex: report that term and its first
		// index so the leader can skip the whole term in one round trip.
		out.Send = append(out.Send, AddressedMsg{To: from, Msg: AppendEntriesReply{
			Term: r.term, Success: false,
			ConflictIndex: r.log.firstIndexOfTerm(m.PrevLogIndex, prevTerm),
			ConflictTerm:  prevTerm,
			Round:         m.Round,
		}})
		return
	}

	// Append, truncating at the first conflict. Entries that already match
	// are skipped, never re-truncated — a reordered older append must not
	// chop off newer entries it doesn't know about.
	insert := m.PrevLogIndex + 1
	i := 0
	for ; i < len(m.Entries); i++ {
		idx := insert + uint64(i)
		t, ok := r.log.Term(idx)
		if !ok {
			break // our log ends here; append the rest
		}
		if t != m.Entries[i].Term {
			r.log.TruncateFrom(idx)
			// Truncation is itself a logged event: an append-only WAL
			// cannot physically delete the conflicting suffix, so recovery
			// must replay the disowning or the node resurrects entries it
			// already disowned to its leader.
			r.truncateFrom = idx
			break
		}
	}
	if rest := m.Entries[i:]; len(rest) > 0 {
		r.log.Append(rest...)
		r.appended = append(r.appended, rest...)
	}

	lastNew := m.PrevLogIndex + uint64(len(m.Entries))
	if m.LeaderCommit > r.commitIndex {
		r.commitIndex = min(m.LeaderCommit, lastNew)
		r.emitApply(out)
	}
	out.Send = append(out.Send, AddressedMsg{To: from, Msg: AppendEntriesReply{
		Term: r.term, Success: true,
		MatchIndex: lastNew,
		Round:      m.Round,
	}})
}
