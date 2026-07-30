// Package raft implements the Raft consensus algorithm as a pure,
// I/O-free state machine. The core performs no I/O whatsoever: it consumes
// Input events via Step and returns Output commands; callers do the
// sending, persisting, and applying. Time is logical (Tick events) and the
// only entropy is an injected *rand.Rand, so the same inputs always produce
// the same outputs — which is what lets the simulator drive it
// deterministically. See ADR-001.
package raft

// NodeID identifies a cluster member. 0 is reserved as None.
type NodeID uint64

// None is the zero NodeID, used for "no vote" and "no known leader".
const None NodeID = 0

// Entry is a single log entry.
type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

// Role is the node's current Raft role.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// Message is an RPC or RPC response exchanged between nodes. The core sees
// these internal structs; transports map them to and from the wire format.
type Message interface{ isMessage() }

// RequestVote solicits a vote for CandidateID in Term.
type RequestVote struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply answers a RequestVote.
type RequestVoteReply struct {
	Term    uint64
	Granted bool
}

// AppendEntries replicates log entries; with no entries it is a heartbeat.
// Round tags the append with the leader's current ReadIndex confirmation
// round (0 = none); followers echo it so acks from earlier rounds cannot
// satisfy a later confirmation — response reordering would otherwise sneak
// the deposed-leader race back in.
type AppendEntries struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
	Round        uint64
}

// AppendEntriesReply answers an AppendEntries. On success MatchIndex is the
// last log index the follower now knows matches the leader — replies can be
// reordered in flight, so the leader must not infer it from request context.
// On failure ConflictIndex/ConflictTerm implement fast log backtracking
// (skip a term per round trip, not one entry): ConflictTerm is the
// follower's term at PrevLogIndex (0 if its log is too short), ConflictIndex
// is the first index of that term (or lastIndex+1 when too short).
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	MatchIndex    uint64
	ConflictIndex uint64
	ConflictTerm  uint64
	Round         uint64 // echoed from the AppendEntries being answered
}

func (RequestVote) isMessage()        {}
func (RequestVoteReply) isMessage()   {}
func (AppendEntries) isMessage()      {}
func (AppendEntriesReply) isMessage() {}

// Input is an event fed to the core via Step.
type Input interface{ isInput() }

// MsgRecv delivers a message (RPC or reply) from another node.
type MsgRecv struct {
	From NodeID
	Msg  Message
}

// Tick advances the core's logical clock by one tick. Production feeds it
// from a wall-clock ticker; the simulator from virtual time. The core never
// reads a clock.
type Tick struct{}

// Propose asks the leader to append a client command to the log.
type Propose struct {
	Data []byte
}

// ReadIndexReq asks the leader for a linearizable read point. Ctx is a
// caller-chosen id returned in the matching ReadState. Reads never enter
// the log; they cost one confirmation round of heartbeats, batched.
type ReadIndexReq struct {
	Ctx uint64
}

func (MsgRecv) isInput()      {}
func (Tick) isInput()         {}
func (Propose) isInput()      {}
func (ReadIndexReq) isInput() {}

// AddressedMsg is a message the caller must transmit to To.
type AddressedMsg struct {
	To  NodeID
	Msg Message
}

// HardState is everything that must be durable before any message in the
// same Output leaves the node: current term, vote, and the log change made
// by this Step. TruncateFrom, when non-zero, discards entries with
// Index >= TruncateFrom and applies before Append.
type HardState struct {
	Term         uint64
	VotedFor     NodeID
	TruncateFrom uint64
	Append       []Entry
}

// Receipt identifies the log slot assigned to a Propose. Waiters key on
// {Index, Term}: if a different term's entry commits at Index, leadership
// changed and the waiter must fail with a retryable error.
type Receipt struct {
	Index uint64
	Term  uint64
}

// ReadState answers a ReadIndexReq. When OK, the caller may serve the read
// from local state once lastApplied >= Index. When !OK the node is not (or
// no longer) a leader able to confirm — retry via the current leader.
type ReadState struct {
	Ctx   uint64
	Index uint64
	OK    bool
}

// Output is the result of one Step. The caller must process it fully,
// in this order, before the next Step:
//
//	1. PersistHard — fsync term/vote/log-suffix. MUST complete before Send:
//	   a vote or append-ack that leaves the node before it is durable can be
//	   forgotten by a crash and contradicted after restart.
//	2. Send — transmit messages (order within Send is irrelevant).
//	3. ApplyEntries — feed committed entries to the state machine (may lag).
//
// The simulator asserts this ordering on every step.
type Output struct {
	Send         []AddressedMsg
	PersistHard  *HardState
	ApplyEntries []Entry
	Proposed     *Receipt
	ReadReady    []ReadState
}

// Status is a read-only snapshot of the core for observability, redirects,
// and the simulator's invariant checkers.
type Status struct {
	ID          NodeID
	Role        Role
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
	LeaderHint  NodeID
}
