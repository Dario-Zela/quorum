package sim

import (
	"fmt"
	"sort"

	"github.com/anishathalye/porcupine"
)

// The KV model, precisely: state is the value of ONE key (histories are
// partitioned per key — linearizability is a local, composable property,
// so per-key checking is sound and turns one exponential search into many
// trivial ones). Get returns the current value with absent collapsed to ""
// deliberately; Put/Delete overwrite/remove; CAS mutates iff current
// equals old and reports which way it went.

type histInput struct {
	Op       uint8
	Key      string
	Val, Old string
}

type histOutput struct {
	Unknown bool // outcome never observed: accept any output
	CASOk   bool
	Val     string // for future Get support
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		for _, op := range history {
			k := op.Input.(histInput).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys) // deterministic partition order
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(string)
		in := input.(histInput)
		out := output.(histOutput)
		switch in.Op {
		case opPut:
			return true, in.Val
		case opDelete:
			return true, ""
		case opCAS:
			ok := st == in.Old
			next := st
			if ok {
				next = in.Val
			}
			return out.Unknown || out.CASOk == ok, next
		case opGet:
			return out.Unknown || out.Val == st, st
		}
		return false, st
	},
	Equal: func(a, b interface{}) bool { return a == b },
	DescribeOperation: func(input, output interface{}) string {
		in := input.(histInput)
		out := output.(histOutput)
		switch in.Op {
		case opPut:
			return fmt.Sprintf("put(%s=%s)", in.Key, in.Val)
		case opDelete:
			return fmt.Sprintf("del(%s)", in.Key)
		case opCAS:
			return fmt.Sprintf("cas(%s: %s->%s) ok=%v unk=%v", in.Key, in.Old, in.Val, out.CASOk, out.Unknown)
		case opGet:
			return fmt.Sprintf("get(%s)=%q unk=%v", in.Key, out.Val, out.Unknown)
		}
		return "?"
	},
}

// History converts every actor's recorded ops into porcupine operations.
// Ops with unknown outcomes get Return = MaxInt64: porcupine is then free
// to linearize them at any later point (or effectively never).
func (w *World) History() []porcupine.Operation {
	var ops []porcupine.Operation
	for _, a := range w.clients {
		a.abandon()
		for _, h := range a.history {
			op := porcupine.Operation{
				ClientId: a.id,
				Input:    histInput{Op: h.Op, Key: h.Key, Val: h.Val, Old: h.Old},
				Call:     int64(h.invokedAt),
			}
			if h.returnedAt == 0 {
				op.Return = unknownReturn
				op.Output = histOutput{Unknown: true}
			} else {
				op.Return = int64(h.returnedAt)
				op.Output = histOutput{CASOk: h.casOK, Val: h.getVal}
			}
			ops = append(ops, op)
		}
	}
	return ops
}

// CheckLinearizable feeds the recorded history to porcupine.
func (w *World) CheckLinearizable() (porcupine.CheckResult, []porcupine.Operation) {
	ops := w.History()
	res := porcupine.CheckOperations(kvModel, ops)
	return boolToResult(res), ops
}

func boolToResult(ok bool) porcupine.CheckResult {
	if ok {
		return porcupine.Ok
	}
	return porcupine.Illegal
}
