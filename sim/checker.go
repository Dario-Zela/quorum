package sim

import (
	"bytes"
	"fmt"

	"github.com/Dario-Zela/quorum/raft"
)

// Violation is an invariant breach: the simulation stops at the first one,
// which is always earlier (and therefore more debuggable) than the eventual
// client-visible symptom.
type Violation struct {
	Time LogicalTime
	Seq  uint64
	Desc string
}

func (v Violation) Error() string {
	return fmt.Sprintf("t=%d seq=%d: %s", v.Time, v.Seq, v.Desc)
}

// checker holds the always-on invariant state, fed after every step.
//
// Invariants (design §7.3):
//  1. Election Safety — at most one leader per term.
//  2. Log Matching — same (index, term) on two nodes ⇒ identical entries
//     and identical prefixes.
//  3. Leader Completeness — every committed entry present in every later
//     leader's log.
//  4. State-Machine Safety — applied sequences are prefix-consistent.
//
// Plus: monotonic terms per node, contiguous applies, and commit never
// outrunning the durable log.
type checker struct {
	now LogicalTime
	seq uint64

	leaderByTerm     map[uint64]raft.NodeID
	candidatesByTerm map[uint64]map[raft.NodeID]bool
	lastTerm         map[raft.NodeID]uint64

	// committed[i] is the entry the cluster as a whole committed at index i
	// (recorded the first time any node's commitIndex covers i). Once here,
	// it may never change. commitTerm is the observer's term at recording —
	// a sound upper bound on the term the commit happened in, which Leader
	// Completeness needs: a leader is only obliged to hold entries
	// committed in terms BELOW its own. A stale candidacy that completes
	// after a pause can legally produce a leader of an older term lacking
	// newer commits (the GC-stall scenario) — that is not a violation.
	committed    map[uint64]committedEntry
	maxCommitted uint64
	lastCommit   map[raft.NodeID]uint64
	appliedUpTo  map[raft.NodeID]uint64

	// matchedUpTo[{a,b}] (a<b): log prefixes of a and b are verified
	// identical up to this index. Reset downward when either truncates.
	matchedUpTo map[[2]raft.NodeID]uint64
}

type committedEntry struct {
	e          raft.Entry
	commitTerm uint64
}

func newChecker() *checker {
	return &checker{
		leaderByTerm:     make(map[uint64]raft.NodeID),
		candidatesByTerm: make(map[uint64]map[raft.NodeID]bool),
		lastTerm:         make(map[raft.NodeID]uint64),
		committed:        make(map[uint64]committedEntry),
		lastCommit:       make(map[raft.NodeID]uint64),
		appliedUpTo:      make(map[raft.NodeID]uint64),
		matchedUpTo:      make(map[[2]raft.NodeID]uint64),
	}
}

func (c *checker) at(now LogicalTime, seq uint64) {
	c.now, c.seq = now, seq
}

func (c *checker) fail(format string, args ...any) *Violation {
	return &Violation{c.now, c.seq, fmt.Sprintf(format, args...)}
}

// CommittedEntry returns the entry the cluster committed at index i.
func (c *checker) CommittedEntry(i uint64) (raft.Entry, bool) {
	ce, ok := c.committed[i]
	return ce.e, ok
}

// record notes entry e as cluster-committed at index e.Index, observed by a
// node currently at observerTerm. Returns a violation if it contradicts an
// earlier recording.
func (c *checker) record(node raft.NodeID, e raft.Entry, observerTerm uint64, how string) *Violation {
	if prev, seen := c.committed[e.Index]; seen {
		if !entriesEqual(prev.e, e) {
			return c.fail("state-machine safety: node %d %s %+v at index %d but the cluster committed %+v",
				node, how, e, e.Index, prev.e)
		}
		return nil
	}
	c.committed[e.Index] = committedEntry{e: e, commitTerm: observerTerm}
	if e.Index > c.maxCommitted {
		c.maxCommitted = e.Index
	}
	return nil
}

// SplitVoteTerms counts terms in which more than one candidate stood.
func (c *checker) SplitVoteTerms() int {
	n := 0
	for _, cands := range c.candidatesByTerm {
		if len(cands) > 1 {
			n++
		}
	}
	return n
}

func entriesEqual(a, b raft.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && bytes.Equal(a.Data, b.Data)
}

// entryAt fetches index i from a shell's durable log (contiguous from 1
// until compaction lands).
func entryAt(s *NodeShell, i uint64) (raft.Entry, bool) {
	st := s.Store.State()
	if i <= st.SnapIndex {
		return raft.Entry{}, false // compacted away
	}
	pos := i - st.SnapIndex - 1
	if pos >= uint64(len(st.Entries)) {
		return raft.Entry{}, false
	}
	e := st.Entries[pos]
	if e.Index != i {
		panic(fmt.Sprintf("sim: durable log of node %d not contiguous: position %d holds index %d", s.ID, pos, e.Index))
	}
	return e, true
}

// lastDurableIndex is the durable tail (snapshot boundary if log empty).
func lastDurableIndex(s *NodeShell) uint64 {
	st := s.Store.State()
	if n := len(st.Entries); n > 0 {
		return st.Entries[n-1].Index
	}
	return st.SnapIndex
}

// observeRestart resets per-node volatile expectations after a
// crash-restart: the state machine rebuilds by replay, so applies begin
// again from index 1 (State-Machine Safety still holds them to the same
// committed entries), and the commit watermark is rediscovered.
func (c *checker) observeRestart(node raft.NodeID, snapIndex uint64) {
	c.appliedUpTo[node] = snapIndex
	c.lastCommit[node] = snapIndex
}

// observeSnapshotInstall records a follower jumping its applied state to a
// leader's snapshot. The skipped entries' contents were verified when the
// SENDER (and every other node) applied them; State-Machine Safety resumes
// from the boundary.
func (c *checker) observeSnapshotInstall(node raft.NodeID, index uint64) {
	if c.appliedUpTo[node] < index {
		c.appliedUpTo[node] = index
	}
	if c.lastCommit[node] < index {
		c.lastCommit[node] = index
	}
}

// observeTruncate must be called whenever node's durable log was truncated
// from index i: any verified-match watermark involving it above i-1 is void.
func (c *checker) observeTruncate(node raft.NodeID, i uint64) {
	for pair, upTo := range c.matchedUpTo {
		if (pair[0] == node || pair[1] == node) && upTo >= i {
			c.matchedUpTo[pair] = i - 1
		}
	}
}

// observeApply checks State-Machine Safety for one applied entry.
func (c *checker) observeApply(node raft.NodeID, observerTerm uint64, e raft.Entry) *Violation {
	if e.Index != c.appliedUpTo[node]+1 {
		return c.fail("apply order: node %d applied index %d after %d", node, e.Index, c.appliedUpTo[node])
	}
	c.appliedUpTo[node] = e.Index
	return c.record(node, e, observerTerm, "applied")
}

// observeStatus checks Election Safety, Leader Completeness, monotonic
// terms, and records commits from the node's advancing commitIndex.
func (c *checker) observeStatus(s *NodeShell, st raft.Status) *Violation {
	if st.Term < c.lastTerm[st.ID] {
		return c.fail("monotonic terms: node %d went from term %d back to %d", st.ID, c.lastTerm[st.ID], st.Term)
	}
	c.lastTerm[st.ID] = st.Term

	if st.Role == raft.Candidate {
		if c.candidatesByTerm[st.Term] == nil {
			c.candidatesByTerm[st.Term] = make(map[raft.NodeID]bool)
		}
		c.candidatesByTerm[st.Term][st.ID] = true
	}

	// Commit watermark: everything newly covered by commitIndex is now
	// cluster-committed and immutable.
	if st.CommitIndex > c.lastCommit[st.ID] {
		for i := c.lastCommit[st.ID] + 1; i <= st.CommitIndex; i++ {
			e, ok := entryAt(s, i)
			if !ok {
				if i <= s.Store.State().SnapIndex {
					continue // compacted: verified before compaction
				}
				return c.fail("node %d committed index %d beyond its durable tail %d", st.ID, i, lastDurableIndex(s))
			}
			if v := c.record(st.ID, e, st.Term, "committed"); v != nil {
				return v
			}
		}
		c.lastCommit[st.ID] = st.CommitIndex
	}

	if st.Role == raft.Leader {
		if prev, claimed := c.leaderByTerm[st.Term]; claimed {
			if prev != st.ID {
				return c.fail("election safety: term %d has two leaders, %d and %d", st.Term, prev, st.ID)
			}
		} else {
			c.leaderByTerm[st.Term] = st.ID
			// Leader Completeness, checked the instant the claim is
			// recorded — weekends before a client would notice a lost
			// write: every entry committed in a term BELOW the claimed one
			// must be in the new leader's log. Entries committed at or
			// above it are exempt: a stale candidacy completing late (the
			// pause scenario) may legally lack them.
			for i := uint64(1); i <= c.maxCommitted; i++ {
				want, ok := c.committed[i]
				if !ok || want.commitTerm >= st.Term {
					continue
				}
				got, ok := entryAt(s, i)
				if !ok {
					if i <= s.Store.State().SnapIndex {
						continue // committed-then-compacted: it held them
					}
					return c.fail("leader completeness: node %d elected leader of term %d without entry %+v committed in term %d (log too short)",
						st.ID, st.Term, want.e, want.commitTerm)
				}
				if !entriesEqual(want.e, got) {
					return c.fail("leader completeness: node %d elected leader of term %d without entry %+v committed in term %d (has %+v)",
						st.ID, st.Term, want.e, want.commitTerm, got)
				}
			}
		}
	}
	return nil
}

// observeLogPair checks Log Matching between two nodes, incrementally: it
// finds the highest index below both tails where terms agree, then verifies
// every entry from the last verified watermark up to it.
func (c *checker) observeLogPair(a, b *NodeShell) *Violation {
	if a.ID == b.ID {
		return nil
	}
	key := [2]raft.NodeID{a.ID, b.ID}
	if key[0] > key[1] {
		key[0], key[1] = key[1], key[0]
	}
	limit := min(lastDurableIndex(a), lastDurableIndex(b))
	// Below either snapshot boundary the entries are gone; the floor is the
	// highest of (verified watermark, both snapshot boundaries).
	floor := c.matchedUpTo[key]
	if sa := a.Store.State().SnapIndex; sa > floor {
		floor = sa
	}
	if sb := b.Store.State().SnapIndex; sb > floor {
		floor = sb
	}

	// Highest index where the two logs claim the same term.
	agree := uint64(0)
	for i := limit; i > floor; i-- {
		ea, okA := entryAt(a, i)
		eb, okB := entryAt(b, i)
		if okA && okB && ea.Term == eb.Term {
			agree = i
			break
		}
	}
	if agree == 0 {
		return nil
	}
	// Same (index, term) ⇒ identical entries and identical prefixes.
	for i := floor + 1; i <= agree; i++ {
		ea, _ := entryAt(a, i)
		eb, _ := entryAt(b, i)
		if !entriesEqual(ea, eb) {
			return c.fail("log matching: nodes %d and %d agree on term at index %d but diverge at index %d: %+v vs %+v",
				a.ID, b.ID, agree, i, ea, eb)
		}
	}
	if agree > c.matchedUpTo[key] {
		c.matchedUpTo[key] = agree
	}
	return nil
}
