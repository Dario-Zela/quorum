package sim

import (
	"testing"

	"github.com/Dario-Zela/quorum/raft"
)

// TestRevivedNodeDoesNotDeposeLeader: the disease pre-vote cures, staged
// directly. A follower crashes and comes back; without pre-vote its
// election timeout bumps the term and deposes the healthy leader (the term
// churn visible in the v1.0 chaos demo). With pre-vote the cluster refuses
// its candidacy and the leader's term never moves.
func TestRevivedNodeDoesNotDeposeLeader(t *testing.T) {
	for seed := int64(0); seed < 20; seed++ {
		w := New(Config{Seed: seed, Clients: 2, ClientOps: 30})
		leader := electLeader(t, w, seed)
		term := w.Node(leader).R.Status().Term

		var victim raft.NodeID
		for _, id := range w.IDs() {
			if id != leader {
				victim = id
				break
			}
		}
		// Crash the follower long enough that it would certainly have timed
		// out several times over, then revive it mid-workload.
		w.Crash(victim)
		if err := w.Run(w.events+8_000, nil); err != nil {
			t.Fatalf("REPRO: seed=%d — %v", seed, err)
		}
		w.Restart(victim)
		if err := w.Run(maxEvents, func(w *World) bool { return w.ClientsDone() }); err != nil {
			t.Fatalf("REPRO: seed=%d — %v", seed, err)
		}
		// The workload may already be done; give the revived node a
		// convergence window regardless.
		if err := w.Run(w.events+20_000, nil); err != nil {
			t.Fatalf("REPRO: seed=%d — converging: %v", seed, err)
		}
		checkLinearizable(t, seed, w)

		st := w.Node(leader).R.Status()
		if st.Role != raft.Leader || st.Term != term {
			t.Fatalf("REPRO: seed=%d — revival disturbed leadership: leader %d went from term %d to %+v",
				seed, leader, term, st)
		}
		if got := w.Node(victim).R.Status().LastApplied; got < st.LastApplied-5 {
			t.Fatalf("REPRO: seed=%d — victim never caught back up (%d vs %d)", seed, got, st.LastApplied)
		}
	}
}

// TestIsolatedLeaderStepsDownInSim: CheckQuorum end to end — an isolated
// leader abdicates on its own, WITHOUT hearing a higher term from anyone.
func TestIsolatedLeaderStepsDownInSim(t *testing.T) {
	for seed := int64(0); seed < 20; seed++ {
		w := New(Config{Seed: seed})
		leader := electLeader(t, w, seed)
		w.Net.Isolate(leader, w.IDs())
		err := w.Run(maxEvents, func(w *World) bool {
			return w.Node(leader).R.Status().Role == raft.Follower
		})
		if err != nil {
			t.Fatalf("REPRO: seed=%d — %v", seed, err)
		}
		if w.Node(leader).R.Status().Role != raft.Follower {
			t.Fatalf("REPRO: seed=%d — isolated leader never stepped down", seed)
		}
		// It stepped down in isolation: nothing reached it, so its term can
		// only have moved by its own (pre-vote-gated) hand — and pre-vote
		// can't succeed alone. Term must be unchanged.
		if got := w.Node(leader).R.Status().Term; got != w.check.lastTerm[leader] {
			t.Fatalf("REPRO: seed=%d — isolated step-down changed term to %d", seed, got)
		}
	}
}
