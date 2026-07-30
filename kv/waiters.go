package kv

import "sort"

// Waiters is the write path's bridge from "proposed at index i, term t" to
// "here is your result": the server registers under the Receipt, and the
// apply loop completes by index — but only if the applied entry's term
// matches the receipt's. A different term at that index means a different
// entry won the slot after a leadership change: ErrLeadershipLost, client
// retries with the same Seq, dedup collapses it. Waiters and sessions are
// one mechanism seen from two ends.
type Waiters struct {
	m map[uint64]waiter
}

type waiter struct {
	term uint64
	done func(Outcome)
}

// Outcome is what a waiter learns.
type Outcome struct {
	Result         Result
	LeadershipLost bool
}

func NewWaiters() *Waiters { return &Waiters{m: make(map[uint64]waiter)} }

// Register arms a waiter for the given receipt. A second registration at
// the same index (possible only after an unreported step-down bug) fails
// the earlier one.
func (w *Waiters) Register(index, term uint64, done func(Outcome)) {
	if old, ok := w.m[index]; ok {
		old.done(Outcome{LeadershipLost: true})
	}
	w.m[index] = waiter{term: term, done: done}
}

// Applied completes the waiter at index, if any: with the result when the
// applied term matches the registered term, with LeadershipLost otherwise
// (the classic missed term-check bug).
func (w *Waiters) Applied(index, appliedTerm uint64, res Result) {
	wt, ok := w.m[index]
	if !ok {
		return
	}
	delete(w.m, index)
	if wt.term != appliedTerm {
		wt.done(Outcome{LeadershipLost: true})
		return
	}
	wt.done(Outcome{Result: res})
}

// StepDown fails ALL outstanding waiters immediately: the proposal may in
// fact still commit later, and that's fine — the retry carries the same
// {ClientID, Seq} and dedup collapses it. Failure order is by index, never
// map order — completion callbacks schedule work, and map iteration order
// leaking into behavior is the classic silent determinism leak.
func (w *Waiters) StepDown() {
	idxs := make([]uint64, 0, len(w.m))
	for idx := range w.m {
		idxs = append(idxs, idx)
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i] < idxs[j] })
	for _, idx := range idxs {
		wt := w.m[idx]
		delete(w.m, idx)
		wt.done(Outcome{LeadershipLost: true})
	}
}

// Len reports outstanding waiters (testing/observability).
func (w *Waiters) Len() int { return len(w.m) }
