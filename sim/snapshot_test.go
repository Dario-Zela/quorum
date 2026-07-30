package sim

import (
	"testing"

	"github.com/Dario-Zela/quorum/raft"
)

// TestSnapshotLaggingFollower: a follower sleeps through enough traffic
// that the leader compacts its log past the follower's tail; on heal, the
// only way back is InstallSnapshot — KV state AND session table must both
// arrive (restore one without the other and dedup silently breaks).
func TestSnapshotLaggingFollower(t *testing.T) {
	for seed := int64(0); seed < 10; seed++ {
		w := New(Config{Seed: seed, Clients: 2, ClientOps: 40, SnapshotEvery: 15})
		leader := electLeader(t, w, seed)

		// Pick a victim follower and cut it off.
		var victim raft.NodeID
		for _, id := range w.IDs() {
			if id != leader {
				victim = id
				break
			}
		}
		w.Net.Isolate(victim, w.IDs())

		// Let the clients push enough traffic that every connected node
		// compacts beyond the victim's tail.
		if err := w.Run(maxEvents, func(w *World) bool {
			if w.ClientsDone() {
				return true
			}
			l, ok := w.AnyLeader()
			return ok && w.Node(l).Store.State().SnapIndex > lastDurableIndex(w.Node(victim))+5
		}); err != nil {
			t.Fatalf("REPRO: seed=%d — %v", seed, err)
		}
		l, ok := w.AnyLeader()
		if !ok || w.Node(l).Store.State().SnapIndex <= lastDurableIndex(w.Node(victim)) {
			t.Fatalf("REPRO: seed=%d — leader never compacted past the victim (snap=%d victim tail=%d)",
				seed, w.Node(l).Store.State().SnapIndex, lastDurableIndex(w.Node(victim)))
		}

		// Heal: the victim can only catch up via InstallSnapshot.
		w.Net.Heal()
		err := w.Run(maxEvents, func(w *World) bool {
			vs := w.Node(victim).Store.State()
			return vs.SnapIndex > 0 && w.Node(victim).R.Status().LastApplied >= w.Node(l).Store.State().SnapIndex
		})
		if err != nil {
			t.Fatalf("REPRO: seed=%d — after heal: %v", seed, err)
		}
		if w.Node(victim).Store.State().SnapIndex == 0 {
			t.Fatalf("REPRO: seed=%d — victim caught up without a snapshot install?", seed)
		}

		// Let the workload finish and judge the histories.
		w.QuietNemesis()
		if err := w.Run(w.events+200_000, func(w *World) bool { return w.ClientsDone() }); err != nil {
			t.Fatalf("REPRO: seed=%d — finishing workload: %v", seed, err)
		}
		checkLinearizable(t, seed, w)
	}
}

// TestSoakWithSnapshots: the full nemesis with compaction running hot, so
// crash-recovery exercises snapshot+WAL-suffix restore and laggards force
// InstallSnapshot under fire.
func TestSoakWithSnapshots(t *testing.T) {
	seeds := int64(15)
	if testing.Short() {
		seeds = 5
	}
	for seed := int64(0); seed < seeds; seed++ {
		w := runClients(t, seed, Config{
			Seed: seed, Clients: 4, ClientOps: 25, SnapshotEvery: 10,
			Nemesis: &NemesisConfig{Every: 30, DropRate: 0.03, DupRate: 0.05},
		})
		checkLinearizable(t, seed, w)
		snapshots := 0
		for _, id := range w.IDs() {
			if w.Node(id).Store.State().SnapIndex > 0 {
				snapshots++
			}
		}
		if snapshots == 0 {
			t.Fatalf("REPRO: seed=%d — no node ever snapshotted; the test exercised nothing", seed)
		}
	}
}
