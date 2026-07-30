package sim

import (
	"testing"

	"github.com/Dario-Zela/quorum/raft"
)

const maxEvents = 200_000

// TestLeaderElectedAcrossSeeds: with no faults, every seed elects a leader,
// and Election Safety holds throughout.
func TestLeaderElectedAcrossSeeds(t *testing.T) {
	for seed := int64(0); seed < 100; seed++ {
		w := New(Config{Seed: seed})
		err := w.Run(maxEvents, func(w *World) bool {
			_, ok := w.AnyLeader()
			return ok
		})
		if err != nil {
			t.Fatalf("REPRO: seed=%d — invariant violated: %v", seed, err)
		}
		if _, ok := w.AnyLeader(); !ok {
			t.Fatalf("REPRO: seed=%d — no leader elected within %d events", seed, maxEvents)
		}
	}
}

// TestDeterministicReplay: the same seed must produce a byte-identical
// event trace. This is the check that catches nondeterminism leaks
// (map-iteration order, stray time.Now(), unseeded randomness) on the day
// they are introduced.
func TestDeterministicReplay(t *testing.T) {
	for _, seed := range []int64{7, 42, 1234} {
		run := func() (string, int) {
			w := New(Config{Seed: seed, Trace: true})
			if err := w.Run(5_000, nil); err != nil {
				t.Fatalf("REPRO: seed=%d — %v", seed, err)
			}
			return w.Trace(), w.events
		}
		t1, e1 := run()
		t2, e2 := run()
		if e1 != e2 {
			t.Fatalf("seed=%d: event counts differ across replays: %d vs %d", seed, e1, e2)
		}
		if t1 != t2 {
			t.Fatalf("seed=%d: traces differ across replays (lengths %d vs %d)", seed, len(t1), len(t2))
		}
		if len(t1) == 0 {
			t.Fatalf("seed=%d: empty trace — tracing is broken", seed)
		}
	}
}

// TestReElectionAfterLeaderPartition: isolate the leader; the remaining
// majority elects a successor at a higher term; after healing, the old
// leader steps down and the cluster converges on one leader.
func TestReElectionAfterLeaderPartition(t *testing.T) {
	for seed := int64(0); seed < 25; seed++ {
		w := New(Config{Seed: seed})
		if err := w.Run(maxEvents, func(w *World) bool { _, ok := w.AnyLeader(); return ok }); err != nil {
			t.Fatalf("REPRO: seed=%d — %v", seed, err)
		}
		old, _ := w.AnyLeader()
		oldTerm := w.Node(old).R.Status().Term

		w.Net.Isolate(old, w.IDs())
		err := w.Run(maxEvents, func(w *World) bool {
			l, ok := w.AnyLeader()
			return ok && l != old && w.Node(l).R.Status().Term > oldTerm
		})
		if err != nil {
			t.Fatalf("REPRO: seed=%d — during partition: %v", seed, err)
		}
		succ, ok := w.AnyLeader()
		if !ok || succ == old {
			t.Fatalf("REPRO: seed=%d — no successor elected while leader %d isolated", seed, old)
		}

		w.Net.Heal()
		err = w.Run(maxEvents, func(w *World) bool {
			// Converged: exactly one node in the Leader role, and the old
			// leader has adopted the successor's term.
			leaders := 0
			for _, id := range w.IDs() {
				if w.Node(id).R.Status().Role == raft.Leader {
					leaders++
				}
			}
			return leaders == 1 && w.Node(old).R.Status().Role == raft.Follower
		})
		if err != nil {
			t.Fatalf("REPRO: seed=%d — after heal: %v", seed, err)
		}
		if w.Node(old).R.Status().Role != raft.Follower {
			t.Fatalf("REPRO: seed=%d — deposed leader %d never stepped down after heal", seed, old)
		}
	}
}

// TestElectionSafetyUnderChurn: repeatedly partition random pairs and heal,
// checking the invariants the whole way. No liveness assertion — only
// safety under an adversarial-ish network.
func TestElectionSafetyUnderChurn(t *testing.T) {
	for seed := int64(0); seed < 25; seed++ {
		w := New(Config{Seed: seed})
		for round := 0; round < 20; round++ {
			a := raft.NodeID(1 + w.rng.Intn(len(w.nodes)))
			b := raft.NodeID(1 + w.rng.Intn(len(w.nodes)))
			if a != b {
				w.Net.BlockBoth(a, b)
			}
			if err := w.Run(w.events+2_000, nil); err != nil {
				t.Fatalf("REPRO: seed=%d round=%d — %v", seed, round, err)
			}
			w.Net.Heal()
			if err := w.Run(w.events+2_000, nil); err != nil {
				t.Fatalf("REPRO: seed=%d round=%d (healed) — %v", seed, round, err)
			}
		}
	}
}
