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

	// Leader state, keyed by NodeID but only ever iterated via r.peers.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

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
	}
	r.resetElectionTimer()
	return r
}

// Restore rebuilds a core from persisted state after a crash-restart: the
// hard state (term, vote) and the durable log suffix, as recovered by the
// storage layer. Volatile state — role, commitIndex, lastApplied — is
// deliberately not restored: commitIndex is rediscovered through the first
// AppendEntries exchange, and the state machine rebuilds by replay.
func Restore(cfg Config, term uint64, votedFor NodeID, entries []Entry) *Raft {
	r := New(cfg)
	r.term = term
	r.votedFor = votedFor
	for i, e := range entries {
		if e.Index != uint64(i)+1 {
			panic(fmt.Sprintf("raft: restored log not contiguous from 1: position %d holds index %d", i, e.Index))
		}
	}
	r.log.entries = append([]Entry(nil), entries...)
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
	default:
		panic(fmt.Sprintf("raft: unknown input %T", in))
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
		return
	}
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.startElection(out)
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
	// The no-op is not an optimization: under the §5.4.2 commit rule it is
	// the only way prior-term entries ever commit, and ReadIndex is
	// unserviceable until it commits.
	if !r.unsafeCommitRule {
		r.appendToOwnLog(nil)
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
	r.nextIndex, r.matchIndex = nil, nil
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
		// nextIndex has fallen below our first retained entry — only
		// possible after compaction. InstallSnapshot lands in weekend 6.
		panic("raft: peer needs a snapshot; compaction lands in weekend 6")
	}
	out.Send = append(out.Send, AddressedMsg{To: p, Msg: AppendEntries{
		Term:         r.term,
		LeaderID:     r.id,
		PrevLogIndex: next - 1,
		PrevLogTerm:  prevTerm,
		Entries:      r.log.Slice(next, r.log.LastIndex()+1),
		LeaderCommit: r.commitIndex,
	}})
}

func (r *Raft) handleAppendEntriesReply(from NodeID, m AppendEntriesReply, out *Output) {
	if r.role != Leader || m.Term < r.term {
		return // stale reply
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
	}
	panic(fmt.Sprintf("raft: unknown message %T", m))
}

func (r *Raft) recv(from NodeID, m Message, out *Output) {
	// Term rule, applied uniformly to messages and replies: a higher term
	// means adopt it, revert to follower, clear votedFor. Step builds
	// PersistHard at the end, and the caller fsyncs it before Send — so the
	// adoption is durable before anything referencing it leaves the node.
	if t := msgTerm(m); t > r.term {
		leader := None
		if ae, ok := m.(AppendEntries); ok {
			leader = ae.LeaderID
		}
		r.becomeFollower(t, leader)
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
	}
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
	}})
}
