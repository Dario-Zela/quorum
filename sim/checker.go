package sim

import (
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
type checker struct {
	// Election Safety (invariant 1): at most one leader per term. Records
	// every (term → leaderID) claim ever observed; a second claimant for the
	// same term is a violation.
	leaderByTerm map[uint64]raft.NodeID

	// Monotonic terms per node.
	lastTerm map[raft.NodeID]uint64
}

func newChecker() *checker {
	return &checker{
		leaderByTerm: make(map[uint64]raft.NodeID),
		lastTerm:     make(map[raft.NodeID]uint64),
	}
}

// observe checks invariants against one node's post-step status. It returns
// the first violation found, or nil.
func (c *checker) observe(now LogicalTime, seq uint64, st raft.Status) *Violation {
	if st.Term < c.lastTerm[st.ID] {
		return &Violation{now, seq, fmt.Sprintf(
			"monotonic terms: node %d went from term %d back to %d",
			st.ID, c.lastTerm[st.ID], st.Term)}
	}
	c.lastTerm[st.ID] = st.Term

	if st.Role == raft.Leader {
		if prev, claimed := c.leaderByTerm[st.Term]; claimed && prev != st.ID {
			return &Violation{now, seq, fmt.Sprintf(
				"election safety: term %d has two leaders, %d and %d",
				st.Term, prev, st.ID)}
		}
		c.leaderByTerm[st.Term] = st.ID
	}
	return nil
}
