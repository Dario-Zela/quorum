package sim

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Dario-Zela/quorum/raft"
)

// TestNemesisSoakSafety runs the full nemesis — drops, duplicates,
// symmetric and asymmetric partitions, crash-restarts, pauses — under
// periodic client proposals, asserting only safety: the invariants must
// hold at every step. (Liveness under permanent chaos is not promised.)
// Then the nemesis is quieted and the cluster must converge: elect a
// leader, commit a fresh write, and apply it everywhere.
func TestNemesisSoakSafety(t *testing.T) {
	seeds := int64(30)
	if testing.Short() {
		seeds = 8
	}
	for seed := int64(0); seed < seeds; seed++ {
		w := New(Config{Seed: seed, Nemesis: &NemesisConfig{
			Every:    20,
			DropRate: 0.05,
			DupRate:  0.05,
		}})

		proposals := 0
		for w.events < 60_000 {
			if err := w.Run(w.events+500, nil); err != nil {
				t.Fatalf("REPRO: seed=%d — %v", seed, err)
			}
			if l, ok := w.AnyLeader(); ok && !w.Node(l).down {
				if _, ok := w.Propose(l, []byte(fmt.Sprintf("s%d-p%d", seed, proposals))); ok {
					proposals++
				}
			}
			if w.violation != nil {
				t.Fatalf("REPRO: seed=%d — %v", seed, w.violation)
			}
		}
		if proposals == 0 {
			t.Fatalf("REPRO: seed=%d — nemesis never allowed a single proposal; chaos too hot to test anything", seed)
		}

		// Chaos off: the cluster must recover and accept a fresh write.
		w.QuietNemesis()
		if w.violation != nil {
			t.Fatalf("REPRO: seed=%d — violation on quiesce: %v", seed, w.violation)
		}
		final := []byte(fmt.Sprintf("s%d-final", seed))
		var rcpt raft.Receipt
		err := w.Run(w.events+200_000, func(w *World) bool {
			if rcpt.Index == 0 {
				if l, ok := w.AnyLeader(); ok {
					if r, ok := w.Propose(l, final); ok {
						rcpt = r
					}
				}
				return false
			}
			// If leadership was lost after proposing, re-propose.
			if e, ok := w.CommittedEntry(rcpt.Index); ok && !bytes.Equal(e.Data, final) {
				rcpt = raft.Receipt{}
				return false
			}
			for _, id := range w.IDs() {
				if w.Node(id).R.Status().LastApplied < rcpt.Index {
					return false
				}
			}
			return true
		})
		if err != nil {
			t.Fatalf("REPRO: seed=%d — after quiesce: %v", seed, err)
		}
		if rcpt.Index == 0 {
			t.Fatalf("REPRO: seed=%d — no leader after chaos stopped", seed)
		}
		for _, id := range w.IDs() {
			if got := w.Node(id).R.Status().LastApplied; got < rcpt.Index {
				t.Fatalf("REPRO: seed=%d — node %d never converged (applied %d < %d)", seed, id, got, rcpt.Index)
			}
		}
		t.Logf("seed=%d: %d proposals under chaos, converged at index %d", seed, proposals, rcpt.Index)
	}
}

// Determinism must survive the nemesis: same seed, same chaos, same trace.
func TestNemesisDeterministicReplay(t *testing.T) {
	for _, seed := range []int64{3, 99} {
		run := func() string {
			w := New(Config{Seed: seed, Trace: true, Nemesis: &NemesisConfig{
				Every: 20, DropRate: 0.1, DupRate: 0.1,
			}})
			if err := w.Run(8_000, nil); err != nil {
				t.Fatalf("REPRO: seed=%d — %v", seed, err)
			}
			return w.Trace()
		}
		if a, b := run(), run(); a != b {
			t.Fatalf("seed=%d: nemesis traces differ across replays", seed)
		}
	}
}
