package sim

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Dario-Zela/quorum/raft"
)

// splitSets heals the network, then blocks every cross-set link (both
// directions), leaving intra-set links intact.
func splitSets(w *World, sets ...[]raft.NodeID) {
	w.Net.Heal()
	for i, a := range sets {
		for j, b := range sets {
			if i == j {
				continue
			}
			for _, x := range a {
				for _, y := range b {
					w.Net.Block(x, y)
				}
			}
		}
	}
}

func electLeader(t *testing.T, w *World, seed int64) raft.NodeID {
	t.Helper()
	if err := w.Run(maxEvents, func(w *World) bool { _, ok := w.AnyLeader(); return ok }); err != nil {
		t.Fatalf("REPRO: seed=%d — %v", seed, err)
	}
	l, ok := w.AnyLeader()
	if !ok {
		t.Fatalf("REPRO: seed=%d — no leader", seed)
	}
	return l
}

// TestReplicationAcrossSeeds: proposals on a healthy cluster commit, apply
// on every node, and survive the invariant gauntlet.
func TestReplicationAcrossSeeds(t *testing.T) {
	for seed := int64(0); seed < 50; seed++ {
		w := New(Config{Seed: seed})
		leader := electLeader(t, w, seed)

		var last raft.Receipt
		for k := 0; k < 5; k++ {
			rcpt, ok := w.Propose(leader, []byte(fmt.Sprintf("cmd-%d", k)))
			if !ok {
				t.Fatalf("REPRO: seed=%d — leader %d refused proposal", seed, leader)
			}
			last = rcpt
		}
		err := w.Run(maxEvents, func(w *World) bool {
			for _, id := range w.IDs() {
				if w.Node(id).R.Status().LastApplied < last.Index {
					return false
				}
			}
			return true
		})
		if err != nil {
			t.Fatalf("REPRO: seed=%d — %v", seed, err)
		}
		for _, id := range w.IDs() {
			if got := w.Node(id).R.Status().LastApplied; got < last.Index {
				t.Fatalf("REPRO: seed=%d — node %d applied only to %d, want %d", seed, id, got, last.Index)
			}
		}
		e, ok := w.CommittedEntry(last.Index)
		if !ok || !bytes.Equal(e.Data, []byte("cmd-4")) {
			t.Fatalf("REPRO: seed=%d — committed entry at %d is %+v", seed, last.Index, e)
		}
		// A follower must refuse proposals (no receipt — server layer
		// redirects via the leader hint).
		for _, id := range w.IDs() {
			if id != leader && w.Node(id).R.Status().Role == raft.Follower {
				if _, ok := w.Propose(id, []byte("nope")); ok {
					t.Fatalf("REPRO: seed=%d — follower %d accepted a proposal", seed, id)
				}
				break
			}
		}
	}
}

// TestSplitVotesHappenAndResolve: across seeds, contested elections (more
// than one candidate in a term) must actually occur — otherwise the sim
// isn't exercising contention — and every one of them must still end in a
// single elected leader.
func TestSplitVotesHappenAndResolve(t *testing.T) {
	storms := 0
	for seed := int64(0); seed < 50; seed++ {
		w := New(Config{Seed: seed})
		electLeader(t, w, seed)
		if w.SplitVoteTerms() > 0 {
			storms++
		}
	}
	if storms == 0 {
		t.Fatal("no split votes across 50 seeds — election contention untested")
	}
	t.Logf("split-vote elections resolved in %d/50 seeds", storms)
}

// figureEight replays the design's §7.6 trace through partitions:
//
//	L1 leads and replicates a client entry x to one follower only; a second
//	leader L2 rises elsewhere, accepts y locally, and is instantly cut off;
//	an x-holder W then wins, re-replicates x to a majority, and commits it.
//	Finally L2 — with its conflicting entry and an inflated term — rejoins.
//
// With the real §5.4.2 rule, x only commits under W's new-term no-op, after
// which L2 can never win an election (its last log term is stale) — the
// cluster converges with x intact. With the unsafe rule (no term clause, no
// no-op), x commits by bare counting, L2's y beats x on the term-first vote
// comparison, L2 wins, and an acknowledged write vanishes — which the
// checkers must catch the moment L2's leadership claim is recorded.
func figureEight(t *testing.T, seed int64, unsafe bool) *Violation {
	t.Helper()
	w := New(Config{Seed: seed})
	if unsafe {
		for _, id := range w.IDs() {
			w.Node(id).R.EnableUnsafeCommitRuleForTesting()
		}
	}
	fatalf := func(format string, args ...any) {
		t.Fatalf("REPRO: seed=%d unsafe=%v — "+format, append([]any{seed, unsafe}, args...)...)
	}

	// Phase A: elect L1; in real mode wait for its no-op to reach every log
	// so all logs share index 1 (paper-style alignment).
	l1 := electLeader(t, w, seed)
	if !unsafe {
		if err := w.Run(maxEvents, func(w *World) bool {
			for _, id := range w.IDs() {
				if len(w.Node(id).Log) < 1 {
					return false
				}
			}
			return true
		}); err != nil {
			fatalf("spreading L1 no-op: %v", err)
		}
	}
	t1 := w.Node(l1).R.Status().Term

	// Phase B: L1 can reach only F2. Propose x; it replicates to F2 alone.
	var f2 raft.NodeID
	for _, id := range w.IDs() {
		if id != l1 {
			f2 = id
			break
		}
	}
	var rest []raft.NodeID
	for _, id := range w.IDs() {
		if id != l1 && id != f2 {
			rest = append(rest, id)
		}
	}
	splitSets(w, []raft.NodeID{l1, f2}, rest)
	rx, ok := w.Propose(l1, []byte("x"))
	if !ok {
		fatalf("L1 refused x")
	}
	if err := w.Run(maxEvents, func(w *World) bool {
		e, ok := entryAt(w.Node(f2), rx.Index)
		return ok && e.Term == rx.Term
	}); err != nil {
		fatalf("replicating x to F2: %v", err)
	}
	if w.Node(l1).R.Status().CommitIndex >= rx.Index {
		fatalf("x committed with only 2/5 replicas")
	}

	// Phase C: cut L1 and F2 apart too ("S1 crashes"); the other three
	// elect L2 at a higher term.
	splitSets(w, []raft.NodeID{l1}, []raft.NodeID{f2}, rest)
	var l2 raft.NodeID
	if err := w.Run(maxEvents, func(w *World) bool {
		for _, id := range rest {
			st := w.Node(id).R.Status()
			if st.Role == raft.Leader && st.Term > t1 {
				l2 = id
				return true
			}
		}
		return false
	}); err != nil {
		fatalf("electing L2: %v", err)
	}

	// Phase D: L2 accepts y locally, then crashes before any Deliver
	// fires. Crashing alone isn't enough — its replication is already
	// queued on the heap ("on the wire"), so L2 is also partitioned off:
	// reachability is checked at delivery time, which eats those messages.
	// The survivors reconnect fully.
	if _, ok := w.Propose(l2, []byte("y")); !ok {
		fatalf("L2 refused y")
	}
	w.Crash(l2)
	var quorum4 []raft.NodeID
	for _, id := range w.IDs() {
		if id != l2 {
			quorum4 = append(quorum4, id)
		}
	}
	splitSets(w, quorum4, []raft.NodeID{l2})

	// Phase E: among the connected four, only x-holders (L1, F2) can win —
	// the Election Restriction refuses the two bare logs. Run until an
	// x-holder W leads and x is committed cluster-wide (minus L2).
	var wNode raft.NodeID
	if err := w.Run(maxEvents, func(w *World) bool {
		for _, id := range []raft.NodeID{l1, f2} {
			st := w.Node(id).R.Status()
			if st.Role == raft.Leader && st.CommitIndex >= rx.Index {
				wNode = id
				return true
			}
		}
		return false
	}); err != nil {
		if unsafe {
			// Under the unsafe rule the catastrophe can only strike after
			// L2 rejoins; any earlier violation is unexpected.
			fatalf("premature violation before L2 rejoined: %v", err)
		}
		fatalf("committing x under W: %v", err)
	}
	got, ok := w.CommittedEntry(rx.Index)
	if !ok || !bytes.Equal(got.Data, []byte("x")) {
		fatalf("committed entry at %d is %+v, want x", rx.Index, got)
	}

	// Phase F: the same nemesis schedule for both rules — kill W the moment
	// it reports x committed, then restart L2. What differs is what that
	// moment MEANS. Real rule: commit implied W's new-term no-op reached a
	// majority, so the survivors' last log term already exceeds L2's and
	// its candidacies are refused forever. Unsafe rule: x "committed" by
	// bare counting with nothing else spread — the survivors' last log term
	// is still T1 < T2, L2's ratcheting elections eventually win, and the
	// acknowledged write is overwritten cluster-wide.
	w.Crash(wNode)
	w.Net.Heal()
	w.Restart(l2)
	err := w.Run(w.events+150_000, nil)

	if err == nil {
		// Convergence check (real rule): L2's log must adopt x.
		e, ok := entryAt(w.Node(l2), rx.Index)
		if !ok || !bytes.Equal(e.Data, []byte("x")) || e.Term != rx.Term {
			fatalf("L2 never converged to x at %d: %+v", rx.Index, e)
		}
		if final, _ := w.CommittedEntry(rx.Index); !bytes.Equal(final.Data, []byte("x")) {
			fatalf("committed x was replaced by %+v", final)
		}
		return nil
	}
	return w.Violation()
}

func TestFigureEightRealRulePreservesCommit(t *testing.T) {
	for seed := int64(0); seed < 5; seed++ {
		if v := figureEight(t, seed, false); v != nil {
			t.Fatalf("REPRO: seed=%d — real commit rule violated an invariant: %v", seed, v)
		}
	}
}

// The mutation test: with the unsafe rule the harness MUST catch the lost
// write — a checker that stays silent here certifies buggy systems.
func TestFigureEightUnsafeRuleIsCaught(t *testing.T) {
	caught := 0
	for seed := int64(0); seed < 5; seed++ {
		if v := figureEight(t, seed, true); v != nil {
			caught++
			t.Logf("seed=%d caught: %v", seed, v)
		}
	}
	if caught == 0 {
		t.Fatal("unsafe commit rule survived all seeds — the checkers are blind to Figure 8")
	}
}
