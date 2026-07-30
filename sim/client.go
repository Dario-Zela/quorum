package sim

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/Dario-Zela/quorum/kv"
	"github.com/Dario-Zela/quorum/proto/quorumpb"
	"github.com/Dario-Zela/quorum/raft"
)

// Simulated clients implement the design's retry state machine
// (DISCOVER → SEND → {done, retry}) as event-driven actors, and every op
// they run is recorded for the linearizability checker. The retry rules
// are the load-bearing part: on ErrNotLeader follow the hint; on
// leadership loss, timeout, or silence go back to DISCOVER and resend the
// SAME Seq — bumping Seq on retry reintroduces duplicate effects.

const (
	opPut = iota
	opDelete
	opCAS
	opGet // joins the workload in weekend 5 with ReadIndex
)

// histOp is one client-visible operation for the history.
type histOp struct {
	Op       uint8
	Key      string
	Val, Old string

	invokedAt  LogicalTime
	returnedAt LogicalTime // 0 = outcome unknown at end of run
	casOK      bool
	getVal     string
}

// clientOutcome is the sim-level RPC response.
type clientOutcome struct {
	notLeader bool
	hint      raft.NodeID
	lost      bool // ErrLeadershipLost
	res       kv.Result
}

type clientActor struct {
	id  int
	rng *rand.Rand

	clientID uint64 // session; 0 until registered
	seq      uint64 // seq of the CURRENT pending op (assigned at invoke)

	rr       int // round-robin cursor for DISCOVER
	hint     raft.NodeID
	attempt  uint64
	sweeps   int // consecutive full sweeps without an answer, for backoff
	pending  *histOp
	pendingCmd *quorumpb.Command

	history []histOp
	opsDone int
	maxOps  int
	lastCAS map[string]string // expected current value per key, for CAS chains
}

// clientTimeout is how long an actor waits for a response before it
// re-enters DISCOVER and resends the same Seq.
const clientTimeout = 40

func (w *World) newClientActor(id, maxOps int) *clientActor {
	return &clientActor{
		id:      id,
		rng:     rand.New(rand.NewSource(w.cfg.Seed*7919 + int64(id))),
		maxOps:  maxOps,
		lastCAS: make(map[string]string),
	}
}

// Done reports whether the actor finished its workload.
func (a *clientActor) Done() bool { return a.opsDone >= a.maxOps && a.pending == nil }

// nextTarget implements DISCOVER: the hint short-circuits; otherwise
// round-robin, with exponential backoff + jitter between full sweeps — an
// electing cluster has no leader to find, and hammering it prolongs the
// election.
func (a *clientActor) nextTarget(n int) (raft.NodeID, LogicalTime) {
	if a.hint != raft.None {
		t := a.hint
		a.hint = raft.None
		return t, 1
	}
	target := raft.NodeID(1 + a.rr%n)
	a.rr++
	var backoff LogicalTime
	if a.rr%n == 0 {
		a.sweeps++
		shift := a.sweeps
		if shift > 4 {
			shift = 4
		}
		backoff = LogicalTime(4<<shift) + LogicalTime(a.rng.Intn(4))
	}
	return target, 1 + backoff
}

// invoke starts the actor's next logical operation (Register first, then
// random KV writes) and returns the command to send. Reads join in
// weekend 5 with ReadIndex; until then CAS supplies the observation power.
func (a *clientActor) invoke(now LogicalTime) *quorumpb.Command {
	if a.clientID == 0 {
		a.pendingCmd = &quorumpb.Command{Op: quorumpb.OpType_OP_REGISTER}
		return a.pendingCmd
	}
	a.seq++
	key := fmt.Sprintf("k%d", a.rng.Intn(3))
	op := &histOp{Key: key, invokedAt: now}
	switch a.rng.Intn(3) {
	case 0:
		op.Op = opPut
		op.Val = fmt.Sprintf("c%d-s%d", a.id, a.seq)
		a.pendingCmd = &quorumpb.Command{Op: quorumpb.OpType_OP_PUT, ClientId: a.clientID, Seq: a.seq, Key: key, Value: op.Val}
	case 1:
		op.Op = opDelete
		a.pendingCmd = &quorumpb.Command{Op: quorumpb.OpType_OP_DELETE, ClientId: a.clientID, Seq: a.seq, Key: key}
	case 2:
		op.Op = opCAS
		op.Old = a.lastCAS[key] // best-effort guess; either outcome is legal
		op.Val = fmt.Sprintf("c%d-s%d", a.id, a.seq)
		a.pendingCmd = &quorumpb.Command{Op: quorumpb.OpType_OP_CAS, ClientId: a.clientID, Seq: a.seq, Key: key, OldValue: op.Old, Value: op.Val}
	}
	a.pending = op
	return a.pendingCmd
}

// complete finishes the pending op with a successful result.
func (a *clientActor) complete(now LogicalTime, res kv.Result) {
	if a.pendingCmd.Op == quorumpb.OpType_OP_REGISTER {
		a.clientID = res.ClientID
		a.pendingCmd = nil
		return
	}
	op := a.pending
	op.returnedAt = now
	op.casOK = res.CASOk
	if op.Op == opCAS && res.CASOk {
		a.lastCAS[op.Key] = op.Val
	} else if op.Op == opPut {
		a.lastCAS[op.Key] = op.Val
	} else if op.Op == opDelete {
		a.lastCAS[op.Key] = ""
	}
	a.history = append(a.history, *op)
	a.pending = nil
	a.pendingCmd = nil
	a.opsDone++
}

// abandon records the pending op as outcome-unknown (end of run). An op
// with unknown outcome must be left free to linearize at any later point
// or never — recording it as failed is the standard way to build a checker
// that certifies buggy systems.
func (a *clientActor) abandon() {
	if a.pending != nil {
		a.history = append(a.history, *a.pending)
		a.pending = nil
	}
}

const unknownReturn = int64(math.MaxInt64)
