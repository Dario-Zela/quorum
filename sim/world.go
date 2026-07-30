// Package sim is a deterministic simulation of a quorum cluster: a
// single-threaded event loop over virtual time, driving the pure raft cores
// with rng-scheduled deliveries and ticks. No goroutines, no real time, no
// data races possible — every bug is a logic bug and every run replays from
// its seed.
package sim

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/Dario-Zela/quorum/raft"
)

// Config parameterizes a World. The zero value plus a Seed is usable.
type Config struct {
	Nodes      int   // default 5
	Seed       int64 // THE seed — every run replayable from it
	MaxLatency int   // message delay drawn uniformly from [1, MaxLatency]; default 3
	Trace      bool  // record the full event trace (determinism diffs, debugging)
}

// NodeShell wraps one raft core with its "durable" state, standing in for
// the storage layer: what survives a crash-restart is exactly what passed
// through PersistHard. (A real memstore replaces this in weekend 3.)
type NodeShell struct {
	ID   raft.NodeID
	R    *raft.Raft
	Term uint64      // persisted
	Vote raft.NodeID // persisted
	Log  []raft.Entry

	down     bool
	restarts int
}

func (s *NodeShell) persist(hs *raft.HardState) {
	s.Term, s.Vote = hs.Term, hs.VotedFor
	if hs.TruncateFrom > 0 {
		keep := s.Log[:0]
		for _, e := range s.Log {
			if e.Index < hs.TruncateFrom {
				keep = append(keep, e)
			}
		}
		s.Log = keep
	}
	s.Log = append(s.Log, hs.Append...)
}

// World is the simulated universe.
type World struct {
	cfg   Config
	now   LogicalTime
	seq   uint64
	rng   *rand.Rand
	nodes []*NodeShell // index i holds node i+1
	heap  eventHeap
	Net   *PartitionMatrix
	check *checker

	events    int
	trace     strings.Builder
	violation *Violation
}

// New builds a world of cfg.Nodes fresh followers and schedules their ticks.
func New(cfg Config) *World {
	if cfg.Nodes == 0 {
		cfg.Nodes = 5
	}
	if cfg.MaxLatency == 0 {
		cfg.MaxLatency = 3
	}
	w := &World{
		cfg:   cfg,
		rng:   rand.New(rand.NewSource(cfg.Seed)),
		Net:   newPartitionMatrix(),
		check: newChecker(),
	}
	ids := make([]raft.NodeID, cfg.Nodes)
	for i := range ids {
		ids[i] = raft.NodeID(i + 1)
	}
	for _, id := range ids {
		w.nodes = append(w.nodes, &NodeShell{
			ID: id,
			R: raft.New(raft.Config{
				ID:    id,
				Peers: ids,
				// Distinct deterministic stream per node: two nodes sharing
				// a stream would correlate their election timeouts.
				Rand: rand.New(rand.NewSource(cfg.Seed<<8 | int64(id))),
			}),
		})
		w.push(Event{At: 1, Kind: EvTick, Node: id})
	}
	return w
}

// IDs lists the cluster members in sorted order.
func (w *World) IDs() []raft.NodeID {
	ids := make([]raft.NodeID, len(w.nodes))
	for i, s := range w.nodes {
		ids[i] = s.ID
	}
	return ids
}

// Node returns the shell for id.
func (w *World) Node(id raft.NodeID) *NodeShell { return w.nodes[id-1] }

// Now returns the current virtual time.
func (w *World) Now() LogicalTime { return w.now }

// Violation returns the first invariant breach, or nil.
func (w *World) Violation() *Violation { return w.violation }

// Trace returns the recorded event trace (empty unless cfg.Trace).
func (w *World) Trace() string { return w.trace.String() }

// Crash takes a node down: volatile state is lost, durable state (what
// passed through PersistHard) is kept. Its ticks stop and deliveries to it
// are dropped until Restart.
func (w *World) Crash(id raft.NodeID) {
	shell := w.Node(id)
	shell.down = true
	w.tracef("t=%d crash node=%d", w.now, id)
}

// Restart brings a crashed node back as a follower, rebuilt from its
// durable state, with a fresh (but deterministically derived) rng stream.
func (w *World) Restart(id raft.NodeID) {
	shell := w.Node(id)
	if !shell.down {
		panic("sim: restarting a node that is not down")
	}
	shell.down = false
	shell.restarts++
	shell.R = raft.Restore(raft.Config{
		ID:    id,
		Peers: w.IDs(),
		Rand:  rand.New(rand.NewSource(w.cfg.Seed<<8 | int64(id) + int64(shell.restarts)*100_003)),
	}, shell.Term, shell.Vote, append([]raft.Entry(nil), shell.Log...))
	w.check.observeRestart(id)
	w.tracef("t=%d restart node=%d term=%d vote=%d log=%d", w.now, id, shell.Term, shell.Vote, len(shell.Log))
	w.push(Event{At: w.now + 1, Kind: EvTick, Node: id})
}

// Propose injects a client proposal at the current virtual time, returning
// the Receipt if the node accepted it (i.e. believes itself leader).
func (w *World) Propose(id raft.NodeID, data []byte) (raft.Receipt, bool) {
	shell := w.Node(id)
	e := Event{At: w.now, Seq: w.seq, Kind: EvClientOp, Node: id}
	w.seq++
	w.tracef("t=%d #%d propose node=%d data=%q", e.At, e.Seq, id, data)
	out := w.stepNode(e, shell, raft.Propose{Data: data})
	if out.Proposed == nil {
		return raft.Receipt{}, false
	}
	return *out.Proposed, true
}

// CommittedEntry returns the entry the cluster committed at index i, as
// tracked by the invariant checker.
func (w *World) CommittedEntry(i uint64) (raft.Entry, bool) { return w.check.CommittedEntry(i) }

// SplitVoteTerms counts terms in which more than one candidate stood.
func (w *World) SplitVoteTerms() int { return w.check.SplitVoteTerms() }

// AnyLeader reports a node currently in the Leader role, preferring the
// highest term if several stale leaders coexist.
func (w *World) AnyLeader() (raft.NodeID, bool) {
	var best raft.NodeID
	var bestTerm uint64
	for _, s := range w.nodes {
		if st := s.R.Status(); st.Role == raft.Leader && (best == raft.None || st.Term > bestTerm) {
			best, bestTerm = st.ID, st.Term
		}
	}
	return best, best != raft.None
}

func (w *World) push(e Event) {
	e.Seq = w.seq
	w.seq++
	w.heap.Push(e)
}

func (w *World) tracef(format string, args ...any) {
	if w.cfg.Trace {
		fmt.Fprintf(&w.trace, format+"\n", args...)
	}
}

// Run processes events until stop returns true, the heap drains, maxEvents
// is hit, or an invariant is violated (returned as the error).
func (w *World) Run(maxEvents int, stop func(*World) bool) error {
	for w.heap.Len() > 0 && w.events < maxEvents && w.violation == nil {
		if stop != nil && stop(w) {
			return nil
		}
		w.stepOnce()
	}
	if w.violation != nil {
		return w.violation
	}
	return nil
}

func (w *World) stepOnce() {
	e := w.heap.Pop()
	w.now = e.At
	w.events++
	shell := w.Node(e.Node)

	switch e.Kind {
	case EvTick:
		if shell.down {
			return // ticks die with the node; Restart schedules a new one
		}
		w.stepNode(e, shell, raft.Tick{})
		w.push(Event{At: e.At + 1, Kind: EvTick, Node: e.Node})
	case EvDeliver:
		if shell.down {
			w.tracef("t=%d #%d drop(down) %d->%d %T%+v", e.At, e.Seq, e.From, e.Node, e.Msg, e.Msg)
			return
		}
		// Reachability is checked at delivery, not send: a partition that
		// forms while a message is in flight eats it, and healing never
		// resurrects it.
		if !w.Net.Reach(e.From, e.Node) {
			w.tracef("t=%d #%d drop %d->%d %T%+v", e.At, e.Seq, e.From, e.Node, e.Msg, e.Msg)
			return
		}
		w.tracef("t=%d #%d deliver %d->%d %T%+v", e.At, e.Seq, e.From, e.Node, e.Msg, e.Msg)
		w.stepNode(e, shell, raft.MsgRecv{From: e.From, Msg: e.Msg})
	}
}

// stepNode drives one Step and processes its Output in the contract order:
// PersistHard → Send → ApplyEntries. The persist-before-send assertion
// lives here: after persisting, every outbound message must be justified by
// durable state, so processing sends first would trip it.
func (w *World) stepNode(e Event, shell *NodeShell, in raft.Input) raft.Output {
	out := shell.R.Step(in)
	w.check.at(w.now, e.Seq)

	if out.PersistHard != nil {
		shell.persist(out.PersistHard)
		if out.PersistHard.TruncateFrom > 0 {
			w.check.observeTruncate(shell.ID, out.PersistHard.TruncateFrom)
		}
		w.tracef("t=%d #%d persist node=%d term=%d vote=%d trunc=%d app=%d",
			e.At, e.Seq, shell.ID, out.PersistHard.Term, out.PersistHard.VotedFor,
			out.PersistHard.TruncateFrom, len(out.PersistHard.Append))
	}

	for _, send := range out.Send {
		if v := w.assertSendDurable(e, shell, send); v != nil {
			w.violation = v
			return out
		}
		delay := LogicalTime(1 + w.rng.Intn(w.cfg.MaxLatency))
		w.tracef("t=%d #%d send %d->%d +%d %T%+v", e.At, e.Seq, shell.ID, send.To, delay, send.Msg, send.Msg)
		w.push(Event{At: e.At + delay, Kind: EvDeliver, Node: send.To, From: shell.ID, Msg: send.Msg})
	}

	for _, applied := range out.ApplyEntries {
		w.tracef("t=%d #%d apply node=%d %+v", e.At, e.Seq, shell.ID, applied)
		if w.violation == nil {
			w.violation = w.check.observeApply(shell.ID, applied)
		}
	}

	st := shell.R.Status()
	w.tracef("t=%d #%d status node=%d role=%v term=%d commit=%d last=%d",
		e.At, e.Seq, st.ID, st.Role, st.Term, st.CommitIndex, st.LastIndex)
	if w.violation == nil {
		w.violation = w.check.observeStatus(shell, st)
	}
	for _, other := range w.nodes {
		if w.violation != nil {
			break
		}
		if other.ID != shell.ID {
			w.violation = w.check.observeLogPair(shell, other)
		}
	}
	return out
}

// assertSendDurable machine-checks the persist-before-send rule: any
// message leaving the node must reference only state that is already
// durable. A vote or term adoption that is sent before it is fsynced can be
// forgotten by a crash and contradicted after restart.
func (w *World) assertSendDurable(e Event, shell *NodeShell, send raft.AddressedMsg) *Violation {
	var term uint64
	switch m := send.Msg.(type) {
	case raft.RequestVote:
		term = m.Term
	case raft.RequestVoteReply:
		term = m.Term
		if m.Granted && (shell.Vote != send.To || shell.Term != m.Term) {
			return &Violation{w.now, e.Seq, fmt.Sprintf(
				"persist-before-send: node %d sends vote grant for term %d to %d but durable state is term=%d vote=%d",
				shell.ID, m.Term, send.To, shell.Term, shell.Vote)}
		}
	case raft.AppendEntries:
		term = m.Term
	case raft.AppendEntriesReply:
		term = m.Term
	}
	if term > shell.Term {
		return &Violation{w.now, e.Seq, fmt.Sprintf(
			"persist-before-send: node %d sends %T at term %d but durable term is %d",
			shell.ID, send.Msg, term, shell.Term)}
	}
	return nil
}
