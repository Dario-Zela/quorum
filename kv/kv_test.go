package kv

import (
	"testing"

	"github.com/Dario-Zela/quorum/proto/quorumpb"
	"github.com/Dario-Zela/quorum/raft"
)

func entry(idx uint64, cmd *quorumpb.Command) raft.Entry {
	return raft.Entry{Term: 1, Index: idx, Data: EncodeCommand(cmd)}
}

func put(client, seq uint64, k, v string) *quorumpb.Command {
	return &quorumpb.Command{Op: quorumpb.OpType_OP_PUT, ClientId: client, Seq: seq, Key: k, Value: v}
}

func TestRegisterAssignsLogIndexAsClientID(t *testing.T) {
	s := NewStore()
	res, ok := s.Apply(entry(7, &quorumpb.Command{Op: quorumpb.OpType_OP_REGISTER}))
	if !ok || res.ClientID != 7 || res.Err != ErrNone {
		t.Fatalf("register: %+v", res)
	}
}

func TestApplyOpsAndDedup(t *testing.T) {
	s := NewStore()
	s.Apply(entry(1, &quorumpb.Command{Op: quorumpb.OpType_OP_REGISTER}))

	res, _ := s.Apply(entry(2, put(1, 1, "x", "a")))
	if res.Err != ErrNone || s.Get("x") != "a" {
		t.Fatalf("put: %+v, x=%q", res, s.Get("x"))
	}

	// The same {ClientID, Seq} proposed again (a retry that actually
	// committed): cached response, no re-application.
	res, _ = s.Apply(entry(3, put(1, 1, "x", "a")))
	if res.Err != ErrNone {
		t.Fatalf("duplicate should return cached OK, got %+v", res)
	}
	if seq, _ := s.SessionSeq(1); seq != 1 {
		t.Fatalf("lastSeq = %d, want 1", seq)
	}

	// CAS caches its outcome: replaying the SAME seq must return the
	// ORIGINAL outcome even though the state has since moved on.
	res, _ = s.Apply(entry(4, &quorumpb.Command{Op: quorumpb.OpType_OP_CAS, ClientId: 1, Seq: 2, Key: "x", OldValue: "a", Value: "b"}))
	if !res.CASOk || s.Get("x") != "b" {
		t.Fatalf("cas: %+v, x=%q", res, s.Get("x"))
	}
	res, _ = s.Apply(entry(5, &quorumpb.Command{Op: quorumpb.OpType_OP_CAS, ClientId: 1, Seq: 2, Key: "x", OldValue: "a", Value: "b"}))
	if !res.CASOk {
		t.Fatalf("replayed CAS must return the cached success, got %+v", res)
	}
	if s.Get("x") != "b" {
		t.Fatalf("replayed CAS re-applied! x=%q", s.Get("x"))
	}

	// A ghost from before the last answered call.
	res, _ = s.Apply(entry(6, put(1, 1, "x", "zzz")))
	if res.Err != ErrStaleSeq {
		t.Fatalf("stale seq must be rejected, got %+v", res)
	}
	if s.Get("x") != "b" {
		t.Fatalf("stale seq mutated state: x=%q", s.Get("x"))
	}

	res, _ = s.Apply(entry(7, put(99, 1, "x", "boom")))
	if res.Err != ErrUnknownClient {
		t.Fatalf("unknown client must be rejected, got %+v", res)
	}
}

func TestDeleteAndCASOnAbsent(t *testing.T) {
	s := NewStore()
	s.Apply(entry(1, &quorumpb.Command{Op: quorumpb.OpType_OP_REGISTER}))
	s.Apply(entry(2, put(1, 1, "k", "v")))
	res, _ := s.Apply(entry(3, &quorumpb.Command{Op: quorumpb.OpType_OP_DELETE, ClientId: 1, Seq: 2, Key: "k"}))
	if res.Err != ErrNone || s.Get("k") != "" {
		t.Fatalf("delete: %+v, k=%q", res, s.Get("k"))
	}
	// CAS with old="" on an absent key: absent collapses to "" by design.
	res, _ = s.Apply(entry(4, &quorumpb.Command{Op: quorumpb.OpType_OP_CAS, ClientId: 1, Seq: 3, Key: "k", OldValue: "", Value: "w"}))
	if !res.CASOk || s.Get("k") != "w" {
		t.Fatalf("cas on absent: %+v, k=%q", res, s.Get("k"))
	}
}

func TestNoOpEntriesSkipped(t *testing.T) {
	s := NewStore()
	if _, ok := s.Apply(raft.Entry{Term: 1, Index: 1}); ok {
		t.Fatal("raft no-op must not produce a client result")
	}
}

func TestWaitersTermCheck(t *testing.T) {
	w := NewWaiters()
	var got []Outcome
	w.Register(5, 2, func(o Outcome) { got = append(got, o) })

	// A different term's entry won index 5 after a leadership change.
	w.Applied(5, 3, Result{Value: "v"})
	if len(got) != 1 || !got[0].LeadershipLost {
		t.Fatalf("term mismatch must fail the waiter: %+v", got)
	}

	got = nil
	w.Register(6, 3, func(o Outcome) { got = append(got, o) })
	w.Applied(6, 3, Result{CASOk: true})
	if len(got) != 1 || got[0].LeadershipLost || !got[0].Result.CASOk {
		t.Fatalf("matching term must deliver the result: %+v", got)
	}
}

func TestWaitersStepDownFailsAllInIndexOrder(t *testing.T) {
	w := NewWaiters()
	var order []uint64
	for _, idx := range []uint64{9, 3, 7, 1} {
		i := idx
		w.Register(i, 4, func(o Outcome) {
			if !o.LeadershipLost {
				t.Fatalf("step-down must report LeadershipLost")
			}
			order = append(order, i)
		})
	}
	w.StepDown()
	if w.Len() != 0 {
		t.Fatalf("%d waiters survived step-down", w.Len())
	}
	for i := 1; i < len(order); i++ {
		if order[i-1] > order[i] {
			t.Fatalf("failure order not deterministic by index: %v", order)
		}
	}
}
