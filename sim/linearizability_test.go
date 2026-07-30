package sim

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

// runClients drives a world with client actors to completion (or the event
// cap), quiets the nemesis, and lets stragglers finish.
func runClients(t *testing.T, seed int64, cfg Config) *World {
	t.Helper()
	w := New(cfg)
	if err := w.Run(400_000, func(w *World) bool { return w.ClientsDone() }); err != nil {
		t.Fatalf("REPRO: seed=%d — %v", seed, err)
	}
	if !w.ClientsDone() {
		// Fine: unfinished ops become outcome-unknown in the history. But
		// give the cluster a fault-free window so MOST ops complete.
		w.QuietNemesis()
		if err := w.Run(w.events+100_000, func(w *World) bool { return w.ClientsDone() }); err != nil {
			t.Fatalf("REPRO: seed=%d — after quiesce: %v", seed, err)
		}
	}
	return w
}

func checkLinearizable(t *testing.T, seed int64, w *World) []porcupine.Operation {
	t.Helper()
	res, ops := w.CheckLinearizable()
	if res != porcupine.Ok {
		for _, op := range ops {
			t.Logf("%s call=%d ret=%d client=%d", kvModel.DescribeOperation(op.Input, op.Output), op.Call, op.Return, op.ClientId)
		}
		t.Fatalf("REPRO: seed=%d — history is NOT linearizable (%d ops above)", seed, len(ops))
	}
	return ops
}

// TestLinearizableHealthy: clients on a fault-free cluster; every op
// completes and the history linearizes.
func TestLinearizableHealthy(t *testing.T) {
	for seed := int64(0); seed < 15; seed++ {
		w := runClients(t, seed, Config{Seed: seed, Clients: 4, ClientOps: 15})
		ops := checkLinearizable(t, seed, w)
		if len(ops) != 4*15 {
			t.Fatalf("REPRO: seed=%d — history holds %d ops, want 60: the harness lost operations", seed, len(ops))
		}
		for _, op := range ops {
			if op.Return == unknownReturn {
				t.Fatalf("REPRO: seed=%d — op never completed on a healthy cluster", seed)
			}
		}
	}
}

// TestLinearizableUnderChaos: the design's soak shape — client load under
// the full nemesis, porcupine as the judge. This is where
// acknowledged-then-lost and unacknowledged-but-applied writes would show.
func TestLinearizableUnderChaos(t *testing.T) {
	seeds := int64(20)
	if testing.Short() {
		seeds = 6
	}
	for seed := int64(0); seed < seeds; seed++ {
		w := runClients(t, seed, Config{
			Seed: seed, Clients: 4, ClientOps: 12,
			Nemesis: &NemesisConfig{Every: 30, DropRate: 0.03, DupRate: 0.05},
		})
		checkLinearizable(t, seed, w)
	}
}

// TestDedupAcrossLeaderChange is the design's §7.6 second trace, driven
// adversarially: a single client works through CAS-chained writes while we
// repeatedly crash the leader precisely when a write is in flight — the
// window where the entry may be majority-replicated but unacknowledged.
// The client's retry carries the same Seq; dedup must collapse it to one
// effect. A CAS chain makes any double-apply visible to porcupine: the
// checker model would find no linearization for the second application.
func TestDedupAcrossLeaderChange(t *testing.T) {
	for seed := int64(0); seed < 10; seed++ {
		w := New(Config{Seed: seed, Clients: 1, ClientOps: 10})
		crashes := 0
		for !w.ClientsDone() && w.events < 600_000 {
			if err := w.Run(w.events+30, nil); err != nil {
				t.Fatalf("REPRO: seed=%d — %v", seed, err)
			}
			// Strike exactly when the client has a write outstanding on a
			// live leader.
			a := w.clients[0]
			if l, ok := w.AnyLeader(); ok && !w.Node(l).down && a.pendingCmd != nil && crashes < 6 {
				if w.Node(l).Waiters.Len() > 0 {
					w.Crash(l)
					w.push(Event{At: w.now + LogicalTime(10+w.rng.Intn(20)), Kind: EvRestart, Node: l})
					crashes++
				}
			}
		}
		if !w.ClientsDone() {
			t.Fatalf("REPRO: seed=%d — client never finished (crashes=%d, events=%d)", seed, crashes, w.events)
		}
		if crashes == 0 {
			t.Fatalf("REPRO: seed=%d — no leader crash coincided with an in-flight write; test exercised nothing", seed)
		}
		checkLinearizable(t, seed, w)

		// The session must show exactly the client's op count — every seq
		// exactly once, no gaps, no doubles.
		a := w.clients[0]
		var maxSeq uint64
		for _, id := range w.IDs() {
			if s, ok := w.Node(id).KV.SessionSeq(a.clientID); ok && s > maxSeq {
				maxSeq = s
			}
		}
		if maxSeq != a.seq {
			t.Fatalf("REPRO: seed=%d — session lastSeq=%d, client seq=%d", seed, maxSeq, a.seq)
		}
		t.Logf("seed=%d: %d ops, %d adversarial leader crashes, linearizable", seed, a.opsDone, crashes)
	}
}
