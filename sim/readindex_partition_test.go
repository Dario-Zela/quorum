package sim

import (
	"testing"

	"github.com/Dario-Zela/quorum/raft"
)

// probeRead issues a ReadIndex directly on node id and registers a probe
// that captures the core's eventual answer.
func (w *World) probeRead(id raft.NodeID, ctx uint64) {
	if w.readProbe == nil {
		w.readProbe = make(map[uint64]*raft.ReadState)
	}
	w.readProbe[ctx] = nil
	e := Event{At: w.now, Seq: w.seq, Kind: EvClientOp, Node: id}
	w.seq++
	out := w.stepNode(e, w.Node(id), raft.ReadIndexReq{Ctx: ctx})
	// Synchronous answers (non-leader rejection, single-node) surface in
	// this step's own output; stepNode has already routed them to the probe.
	_ = out
}

// TestReadIndexDuringPartition is the design's §7.5 scenario, directed: a
// leader cut off from the majority MUST fail linearizable reads rather
// than serve from its (about-to-be-stale) state — the whole point of the
// confirmation round. The majority side moves on and commits new writes
// the old leader cannot see; any read it served would be a linearizability
// violation.
func TestReadIndexDuringPartition(t *testing.T) {
	for seed := int64(0); seed < 15; seed++ {
		w := New(Config{Seed: seed})
		old := electLeader(t, w, seed)

		// Healthy baseline: once the no-op gate opens, a probed read
		// confirms via a majority round.
		if err := w.Run(maxEvents, func(w *World) bool {
			return w.Node(old).R.Status().CommitIndex >= 1
		}); err != nil {
			t.Fatalf("REPRO: seed=%d — opening the gate: %v", seed, err)
		}
		w.probeRead(old, 1)
		if err := w.Run(maxEvents, func(w *World) bool { return w.readProbe[1] != nil }); err != nil {
			t.Fatalf("REPRO: seed=%d — healthy read: %v", seed, err)
		}
		if rs := w.readProbe[1]; !rs.OK || rs.Index < 1 {
			t.Fatalf("REPRO: seed=%d — healthy leader failed a read: %+v", seed, rs)
		}

		// Partition the leader away and ask it for another linearizable
		// read. It cannot gather a confirmation round; the read must fail —
		// at the latest when CheckQuorum makes it abdicate.
		w.Net.Isolate(old, w.IDs())
		w.probeRead(old, 2)
		if err := w.Run(maxEvents, func(w *World) bool { return w.readProbe[2] != nil }); err != nil {
			t.Fatalf("REPRO: seed=%d — partitioned read: %v", seed, err)
		}
		if rs := w.readProbe[2]; rs.OK {
			t.Fatalf("REPRO: seed=%d — stale leader SERVED a read during partition: %+v", seed, rs)
		}

		// Meanwhile the majority elects a successor and commits fresh
		// writes the old leader never saw — the state the failed read
		// would have missed.
		var succ raft.NodeID
		if err := w.Run(maxEvents, func(w *World) bool {
			for _, id := range w.IDs() {
				if id == old {
					continue
				}
				if st := w.Node(id).R.Status(); st.Role == raft.Leader && st.CommitIndex > w.Node(old).R.Status().CommitIndex {
					succ = id
					return true
				}
			}
			return false
		}); err != nil {
			t.Fatalf("REPRO: seed=%d — electing successor: %v", seed, err)
		}

		// And the successor serves reads at the new frontier.
		w.probeRead(succ, 3)
		if err := w.Run(maxEvents, func(w *World) bool { return w.readProbe[3] != nil }); err != nil {
			t.Fatalf("REPRO: seed=%d — successor read: %v", seed, err)
		}
		if rs := w.readProbe[3]; !rs.OK || rs.Index <= w.readProbe[1].Index {
			t.Fatalf("REPRO: seed=%d — successor read not at the new frontier: %+v", seed, rs)
		}
	}
}
