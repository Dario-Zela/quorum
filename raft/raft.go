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
	// TODO(weekend 2): append the no-op entry for the new term — under the
	// §5.4.2 commit rule it is the only way prior-term entries ever commit.
	r.broadcastAppend(out)
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

// propose is the client write path.
// TODO(weekend 2): on a leader, append the entry, emit a Receipt, and
// replicate. For now proposals are dropped; non-leaders will always redirect
// via the leader hint in Status.
func (r *Raft) propose(data []byte, out *Output) {
	_ = data
	_ = out
}

// --- replication (leader side) ---

// broadcastAppend sends AppendEntries to every peer. Until replication
// lands (weekend 2) these are pure heartbeats anchored at the leader's last
// entry; afterwards each peer gets entries from its nextIndex.
func (r *Raft) broadcastAppend(out *Output) {
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		out.Send = append(out.Send, AddressedMsg{To: p, Msg: AppendEntries{
			Term:         r.term,
			LeaderID:     r.id,
			PrevLogIndex: r.log.LastIndex(),
			PrevLogTerm:  r.log.LastTerm(),
			LeaderCommit: r.commitIndex,
		}})
	}
}

func (r *Raft) handleAppendEntriesReply(from NodeID, m AppendEntriesReply, out *Output) {
	if r.role != Leader || m.Term < r.term {
		return // stale reply
	}
	// TODO(weekend 2): advance matchIndex/nextIndex on success, back off via
	// ConflictIndex/ConflictTerm on failure, and advance commitIndex per the
	// §5.4.2 rule (majority match AND current-term entry).
	_ = from
	_ = out
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
		// TODO(weekend 2): report ConflictTerm and its first index for
		// term-granular backtracking.
		out.Send = append(out.Send, AddressedMsg{To: from, Msg: AppendEntriesReply{
			Term: r.term, Success: false,
			ConflictIndex: m.PrevLogIndex, ConflictTerm: prevTerm,
		}})
		return
	}
	if len(m.Entries) > 0 {
		// TODO(weekend 2): append with conflict-truncation.
		panic("raft: entry replication lands in weekend 2")
	}
	// TODO(weekend 2): advance commitIndex from LeaderCommit and emit
	// ApplyEntries.
	out.Send = append(out.Send, AddressedMsg{To: from, Msg: AppendEntriesReply{
		Term: r.term, Success: true,
		MatchIndex: m.PrevLogIndex + uint64(len(m.Entries)),
	}})
}
