package sim

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

// The KV model is itself load-bearing: a wrong model certifies buggy
// systems. Check it against hand-written histories before trusting it.

func op(client int, in histInput, out histOutput, call, ret int64) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret}
}

func TestModelAcceptsLegalHistory(t *testing.T) {
	ops := []porcupine.Operation{
		op(0, histInput{Op: opPut, Key: "a", Val: "1"}, histOutput{}, 0, 10),
		op(1, histInput{Op: opCAS, Key: "a", Old: "1", Val: "2"}, histOutput{CASOk: true}, 20, 30),
		op(0, histInput{Op: opCAS, Key: "a", Old: "1", Val: "3"}, histOutput{CASOk: false}, 40, 50),
		op(1, histInput{Op: opDelete, Key: "a"}, histOutput{}, 60, 70),
		op(0, histInput{Op: opCAS, Key: "a", Old: "", Val: "4"}, histOutput{CASOk: true}, 80, 90),
	}
	if !porcupine.CheckOperations(kvModel, ops) {
		t.Fatal("legal sequential history rejected")
	}
}

// An acknowledged-then-lost write: put succeeds, but a later CAS behaves as
// if the key were empty. No linearization exists.
func TestModelCatchesLostWrite(t *testing.T) {
	ops := []porcupine.Operation{
		op(0, histInput{Op: opPut, Key: "a", Val: "1"}, histOutput{}, 0, 10),
		op(1, histInput{Op: opCAS, Key: "a", Old: "", Val: "2"}, histOutput{CASOk: true}, 20, 30),
		op(0, histInput{Op: opCAS, Key: "a", Old: "1", Val: "3"}, histOutput{CASOk: true}, 40, 50),
	}
	if porcupine.CheckOperations(kvModel, ops) {
		t.Fatal("model certified a lost write")
	}
}

// A double-applied CAS (broken dedup): the same transition observed to
// succeed twice with no interleaving writer.
func TestModelCatchesDoubleApply(t *testing.T) {
	ops := []porcupine.Operation{
		op(0, histInput{Op: opPut, Key: "a", Val: "x"}, histOutput{}, 0, 10),
		op(0, histInput{Op: opCAS, Key: "a", Old: "x", Val: "y"}, histOutput{CASOk: true}, 20, 30),
		op(0, histInput{Op: opCAS, Key: "a", Old: "x", Val: "y"}, histOutput{CASOk: true}, 40, 50),
	}
	if porcupine.CheckOperations(kvModel, ops) {
		t.Fatal("model certified a double-applied CAS")
	}
}

// Unknown-outcome ops may linearize anywhere later — including "never
// visibly": a concurrent unknown Put must allow BOTH a read of old and new.
func TestModelUnknownOutcomeIsFlexible(t *testing.T) {
	base := []porcupine.Operation{
		op(0, histInput{Op: opPut, Key: "a", Val: "1"}, histOutput{}, 0, 10),
		op(1, histInput{Op: opPut, Key: "a", Val: "2"}, histOutput{Unknown: true}, 20, unknownReturn),
	}
	sawOld := append(append([]porcupine.Operation(nil), base...),
		op(0, histInput{Op: opCAS, Key: "a", Old: "1", Val: "z"}, histOutput{CASOk: true}, 30, 40))
	if !porcupine.CheckOperations(kvModel, sawOld) {
		t.Fatal("unknown write must be allowed to linearize after the CAS")
	}
	sawNew := append(append([]porcupine.Operation(nil), base...),
		op(0, histInput{Op: opCAS, Key: "a", Old: "2", Val: "z"}, histOutput{CASOk: true}, 30, 40))
	if !porcupine.CheckOperations(kvModel, sawNew) {
		t.Fatal("unknown write must be allowed to linearize before the CAS")
	}
}
