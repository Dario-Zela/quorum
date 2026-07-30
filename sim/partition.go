package sim

import "github.com/Dario-Zela/quorum/raft"

// PartitionMatrix records directed reachability: Reach(from, to) reports
// whether a message from → to is deliverable *at delivery time*. Directed,
// so asymmetric partitions are the natural case, not a special one. A
// partition that forms while messages are in flight eats them too — the
// matrix is consulted when an EvDeliver pops, never at send time — and
// healing never resurrects a dropped message.
type PartitionMatrix struct {
	blocked map[[2]raft.NodeID]bool
}

func newPartitionMatrix() *PartitionMatrix {
	return &PartitionMatrix{blocked: make(map[[2]raft.NodeID]bool)}
}

// Reach reports whether from can currently deliver to to. Reach(i, i) is
// always true.
func (p *PartitionMatrix) Reach(from, to raft.NodeID) bool {
	if from == to {
		return true
	}
	return !p.blocked[[2]raft.NodeID{from, to}]
}

// Block cuts the directed link from → to.
func (p *PartitionMatrix) Block(from, to raft.NodeID) {
	p.blocked[[2]raft.NodeID{from, to}] = true
}

// BlockBoth cuts both directions between a and b.
func (p *PartitionMatrix) BlockBoth(a, b raft.NodeID) {
	p.Block(a, b)
	p.Block(b, a)
}

// Isolate cuts every link to and from node.
func (p *PartitionMatrix) Isolate(node raft.NodeID, all []raft.NodeID) {
	for _, other := range all {
		if other != node {
			p.BlockBoth(node, other)
		}
	}
}

// Heal restores full reachability.
func (p *PartitionMatrix) Heal() {
	p.blocked = make(map[[2]raft.NodeID]bool)
}
