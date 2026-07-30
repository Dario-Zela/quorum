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

	"github.com/Dario-Zela/quorum/kv"
	"github.com/Dario-Zela/quorum/proto/quorumpb"
	"github.com/Dario-Zela/quorum/raft"
	"github.com/Dario-Zela/quorum/storage/memstore"
)

// NemesisConfig switches on rng-driven fault injection. Rates apply per
// send/delivery; one structural action (partition, asymmetric partition,
// crash, pause) is drawn every Every ticks. Byte-level torn writes are
// exercised in the wal package tests, where the bytes are real; the sim's
// crash-restart still replays the full record stream (including Truncate
// records) through memstore recovery.
type NemesisConfig struct {
	Every    LogicalTime // interval between structural actions; default 25
	DropRate float64     // per-delivery message loss
	DupRate  float64     // per-send duplication
}

// Config parameterizes a World. The zero value plus a Seed is usable.
type Config struct {
	Nodes      int   // default 5
	Seed       int64 // THE seed — every run replayable from it
	MaxLatency int   // message delay drawn uniformly from [1, MaxLatency]; default 3
	Trace      bool  // record the full event trace (determinism diffs, debugging)
	Nemesis    *NemesisConfig

	// Clients spawns simulated client actors running the retry state
	// machine, each performing ClientOps random KV writes; their histories
	// feed the linearizability checker.
	Clients   int
	ClientOps int
}

// NodeShell wraps one raft core with its durable storage: what survives a
// crash-restart is exactly what passed through PersistHard into the store.
type NodeShell struct {
	ID    raft.NodeID
	R     *raft.Raft
	Store *memstore.Store

	// The replicated state machine and the write path's waiters. Both are
	// volatile: a crash loses them, and recovery rebuilds the KV by
	// re-applying the log from the start.
	KV      *kv.Store
	Waiters *kv.Waiters

	down        bool
	pausedUntil LogicalTime
	restarts    int
}

// Term returns the durable term.
func (s *NodeShell) Term() uint64 { return s.Store.State().Term }

// Vote returns the durable vote.
func (s *NodeShell) Vote() raft.NodeID { return s.Store.State().Vote }

// Log returns the durable log entries (live view; do not retain).
func (s *NodeShell) Log() []raft.Entry { return s.Store.State().Entries }

func (s *NodeShell) paused(now LogicalTime) bool { return now < s.pausedUntil }

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

	events     int
	trace      strings.Builder
	violation  *Violation
	nemesisOff bool
	clients    []*clientActor

	// In-flight ReadIndex requests: ctx → who asked, for which key.
	readCtx      uint64
	pendingReads map[uint64]pendingRead
}

type pendingRead struct {
	client  int
	attempt uint64
	key     string
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
		cfg:          cfg,
		rng:          rand.New(rand.NewSource(cfg.Seed)),
		Net:          newPartitionMatrix(),
		check:        newChecker(),
		pendingReads: make(map[uint64]pendingRead),
	}
	ids := make([]raft.NodeID, cfg.Nodes)
	for i := range ids {
		ids[i] = raft.NodeID(i + 1)
	}
	for _, id := range ids {
		w.nodes = append(w.nodes, &NodeShell{
			ID:      id,
			Store:   memstore.New(),
			KV:      kv.NewStore(),
			Waiters: kv.NewWaiters(),
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
	if cfg.Nemesis != nil {
		if cfg.Nemesis.Every == 0 {
			cfg.Nemesis.Every = 25
		}
		w.push(Event{At: cfg.Nemesis.Every, Kind: EvNemesis})
	}
	for i := 0; i < cfg.Clients; i++ {
		w.clients = append(w.clients, w.newClientActor(i, cfg.ClientOps))
		w.push(Event{At: LogicalTime(2 + i), Kind: EvClientStart, Client: i})
	}
	return w
}

// ClientsDone reports whether every client actor finished its workload.
func (w *World) ClientsDone() bool {
	for _, a := range w.clients {
		if !a.Done() {
			return false
		}
	}
	return true
}

// QuietNemesis stops fault injection and repairs the world: partitions
// heal, crashed nodes restart, pauses lift. Used by soaks to assert the
// cluster converges once the chaos stops.
func (w *World) QuietNemesis() {
	w.nemesisOff = true
	w.Net.Heal()
	for _, s := range w.nodes {
		if s.down {
			w.Restart(s.ID)
		}
		s.pausedUntil = 0
	}
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

// Restart brings a crashed node back as a follower, rebuilt by replaying
// its durable record stream (recovery), with a fresh deterministic rng.
func (w *World) Restart(id raft.NodeID) {
	shell := w.Node(id)
	if !shell.down {
		panic("sim: restarting a node that is not down")
	}
	st, err := shell.Store.Reopen()
	if err != nil {
		w.violation = &Violation{w.now, w.seq, fmt.Sprintf("recovery replay on node %d: %v", id, err)}
		return
	}
	shell.down = false
	shell.pausedUntil = 0
	shell.restarts++
	shell.KV = kv.NewStore()        // volatile: rebuilt by replaying applies
	shell.Waiters = kv.NewWaiters() // volatile: clients re-learn via timeouts
	shell.R = raft.Restore(raft.Config{
		ID:    id,
		Peers: w.IDs(),
		Rand:  rand.New(rand.NewSource(w.cfg.Seed<<8 | int64(id) + int64(shell.restarts)*100_003)),
	}, st.Term, st.Vote, append([]raft.Entry(nil), st.Entries...))
	w.check.observeRestart(id)
	w.tracef("t=%d restart node=%d term=%d vote=%d log=%d", w.now, id, st.Term, st.Vote, len(st.Entries))
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
	var shell *NodeShell
	if e.Node != raft.None {
		shell = w.Node(e.Node)
	}

	switch e.Kind {
	case EvTick:
		if shell.down {
			return // ticks die with the node; Restart schedules a new one
		}
		if shell.paused(e.At) {
			// A paused node's clock is frozen: the tick chain continues but
			// this tick never reaches the core — it experiences less time,
			// which is exactly the GC-stall asymmetry.
			w.push(Event{At: e.At + 1, Kind: EvTick, Node: e.Node})
			return
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
		if nc := w.cfg.Nemesis; nc != nil && nc.DropRate > 0 && w.rng.Float64() < nc.DropRate {
			w.tracef("t=%d #%d drop(nemesis) %d->%d %T%+v", e.At, e.Seq, e.From, e.Node, e.Msg, e.Msg)
			return
		}
		if shell.paused(e.At) {
			// Frozen process: the packet sits in its socket buffer and is
			// processed in a burst on resume.
			w.tracef("t=%d #%d defer(paused) %d->%d until %d", e.At, e.Seq, e.From, e.Node, shell.pausedUntil)
			w.push(Event{At: shell.pausedUntil, Kind: EvDeliver, Node: e.Node, From: e.From, Msg: e.Msg})
			return
		}
		w.tracef("t=%d #%d deliver %d->%d %T%+v", e.At, e.Seq, e.From, e.Node, e.Msg, e.Msg)
		w.stepNode(e, shell, raft.MsgRecv{From: e.From, Msg: e.Msg})
	case EvNemesis:
		w.nemesisStrike(e)
	case EvHeal:
		w.tracef("t=%d #%d heal", e.At, e.Seq)
		w.Net.Heal()
	case EvRestart:
		if w.Node(e.Node).down {
			w.Restart(e.Node)
		}
	case EvClientStart:
		w.clientDispatch(e)
	case EvClientReq:
		w.serveClientReq(e, shell)
	case EvClientResp:
		w.clientOnResp(e)
	case EvClientTimeout:
		a := w.clients[e.Client]
		if a.pendingCmd != nil && a.attempt == e.Attempt {
			w.tracef("t=%d #%d client %d timeout attempt=%d", e.At, e.Seq, e.Client, e.Attempt)
			w.push(Event{At: e.At + 1, Kind: EvClientStart, Client: e.Client})
		}
	}
}

// clientDispatch sends the actor's next attempt: invoke a fresh op if none
// is pending, pick a target (hint or round-robin with backoff), and arm a
// timeout for this attempt.
func (w *World) clientDispatch(e Event) {
	a := w.clients[e.Client]
	if a.Done() {
		return
	}
	if a.pendingCmd == nil {
		a.invoke(e.At)
		w.tracef("t=%d #%d client %d invoke %v seq=%d", e.At, e.Seq, e.Client, a.pendingCmd.Op, a.pendingCmd.Seq)
	}
	target, delay := a.nextTarget(len(w.nodes))
	a.attempt++
	w.tracef("t=%d #%d client %d -> node %d attempt=%d", e.At, e.Seq, e.Client, target, a.attempt)
	w.push(Event{At: e.At + delay, Kind: EvClientReq, Node: target, Client: e.Client, Attempt: a.attempt, Cmd: kv.EncodeCommand(a.pendingCmd)})
	w.push(Event{At: e.At + delay + clientTimeout, Kind: EvClientTimeout, Client: e.Client, Attempt: a.attempt})
}

// serveClientReq is the sim's stand-in for the server's write path: redirect
// non-leaders via the hint, otherwise Propose and register a waiter keyed on
// the Receipt. The waiter's callback fires from the apply loop (or a
// step-down) and schedules the response back to the client.
func (w *World) serveClientReq(e Event, shell *NodeShell) {
	if shell.down {
		return // request lost; the client's timeout covers it
	}
	if shell.paused(e.At) {
		w.push(Event{At: shell.pausedUntil, Kind: EvClientReq, Node: e.Node, Client: e.Client, Attempt: e.Attempt, Cmd: e.Cmd})
		return
	}
	respond := func(out *clientOutcome) {
		delay := LogicalTime(1 + w.rng.Intn(w.cfg.MaxLatency))
		w.push(Event{At: w.now + delay, Kind: EvClientResp, Client: e.Client, Attempt: e.Attempt, Out: out})
	}
	if shell.R.Status().Role != raft.Leader {
		respond(&clientOutcome{notLeader: true, hint: shell.R.Status().LeaderHint})
		return
	}
	if cmd := decodeCommand(e.Cmd); cmd.Op == quorumpb.OpType_OP_GET {
		// Linearizable read: ReadIndex, never the log. The answer arrives
		// via Output.ReadReady on a later step.
		w.readCtx++
		w.pendingReads[w.readCtx] = pendingRead{client: e.Client, attempt: e.Attempt, key: cmd.Key}
		w.stepNode(e, shell, raft.ReadIndexReq{Ctx: w.readCtx})
		return
	}
	out := w.stepNode(e, shell, raft.Propose{Data: e.Cmd})
	if out.Proposed == nil {
		respond(&clientOutcome{notLeader: true})
		return
	}
	shell.Waiters.Register(out.Proposed.Index, out.Proposed.Term, func(o kv.Outcome) {
		if o.LeadershipLost {
			respond(&clientOutcome{lost: true})
			return
		}
		respond(&clientOutcome{res: o.Result})
	})
}

// clientOnResp advances the actor's retry state machine.
func (w *World) clientOnResp(e Event) {
	a := w.clients[e.Client]
	if a.attempt != e.Attempt || a.pendingCmd == nil {
		return // a stale attempt's answer; the current attempt supersedes it
	}
	out := e.Out
	switch {
	case out.notLeader:
		a.hint = out.hint
		w.push(Event{At: e.At + 1, Kind: EvClientStart, Client: e.Client})
	case out.lost:
		// Back to DISCOVER and resend the SAME Seq — the entire point of
		// sessions. The proposal may in fact still have committed; dedup
		// collapses the retry.
		w.push(Event{At: e.At + 1, Kind: EvClientStart, Client: e.Client})
	default:
		if out.res.Err != kv.ErrNone {
			// A well-behaved client (≤1 outstanding op, registered before
			// writing) can never legally see these.
			w.violation = &Violation{w.now, e.Seq, fmt.Sprintf(
				"client protocol: actor %d got err=%d for seq %d", e.Client, out.res.Err, a.seq)}
			return
		}
		a.sweeps = 0
		a.complete(e.At, out.res)
		w.tracef("t=%d #%d client %d done (%d/%d)", e.At, e.Seq, e.Client, a.opsDone, a.maxOps)
		if !a.Done() {
			w.push(Event{At: e.At + LogicalTime(1+a.rng.Intn(8)), Kind: EvClientStart, Client: e.Client})
		}
	}
}

// nemesisStrike draws one structural fault. All randomness comes from the
// world rng, so a nemesis schedule replays exactly with its seed.
func (w *World) nemesisStrike(e Event) {
	if w.nemesisOff {
		return
	}
	defer w.push(Event{At: e.At + w.cfg.Nemesis.Every, Kind: EvNemesis})

	pick := func() raft.NodeID { return raft.NodeID(1 + w.rng.Intn(len(w.nodes))) }
	switch w.rng.Intn(4) {
	case 0: // symmetric bipartition, healed after a while
		var a, b []raft.NodeID
		for _, id := range w.IDs() {
			if w.rng.Intn(2) == 0 {
				a = append(a, id)
			} else {
				b = append(b, id)
			}
		}
		if len(a) == 0 || len(b) == 0 {
			return
		}
		for _, x := range a {
			for _, y := range b {
				w.Net.BlockBoth(x, y)
			}
		}
		heal := e.At + LogicalTime(10+w.rng.Intn(40))
		w.tracef("t=%d #%d nemesis partition %v | %v heal=%d", e.At, e.Seq, a, b, heal)
		w.push(Event{At: heal, Kind: EvHeal})
	case 1: // asymmetric partition: one direction of one pair
		x, y := pick(), pick()
		if x == y {
			return
		}
		w.Net.Block(x, y)
		heal := e.At + LogicalTime(10+w.rng.Intn(40))
		w.tracef("t=%d #%d nemesis oneway-block %d->%d heal=%d", e.At, e.Seq, x, y, heal)
		w.push(Event{At: heal, Kind: EvHeal})
	case 2: // crash-restart: volatile state lost, storage kept
		n := pick()
		if w.Node(n).down {
			return
		}
		w.Crash(n)
		back := e.At + LogicalTime(5+w.rng.Intn(45))
		w.tracef("t=%d #%d nemesis crash %d restart=%d", e.At, e.Seq, n, back)
		w.push(Event{At: back, Kind: EvRestart, Node: n})
	case 3: // pause/resume: the GC/VM-stall that breaks naive implementations
		n := pick()
		if w.Node(n).down {
			return
		}
		w.Node(n).pausedUntil = e.At + LogicalTime(5+w.rng.Intn(25))
		w.tracef("t=%d #%d nemesis pause %d until %d", e.At, e.Seq, n, w.Node(n).pausedUntil)
	}
}

// stepNode drives one Step and processes its Output in the contract order:
// PersistHard → Send → ApplyEntries. The persist-before-send assertion
// lives here: after persisting, every outbound message must be justified by
// durable state, so processing sends first would trip it.
func (w *World) stepNode(e Event, shell *NodeShell, in raft.Input) raft.Output {
	wasLeader := shell.R.Status().Role == raft.Leader
	out := shell.R.Step(in)
	w.check.at(w.now, e.Seq)
	defer func() {
		// Step-down rule: the moment the core loses leadership, ALL
		// outstanding waiters fail with ErrLeadershipLost — after the
		// applies above completed their own waiters.
		if wasLeader && shell.R.Status().Role != raft.Leader {
			shell.Waiters.StepDown()
		}
	}()

	if out.PersistHard != nil {
		if err := shell.Store.Persist(out.PersistHard); err != nil {
			w.violation = &Violation{w.now, e.Seq, fmt.Sprintf("storage: %v", err)}
			return out
		}
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
		copies := 1
		if nc := w.cfg.Nemesis; nc != nil && nc.DupRate > 0 && w.rng.Float64() < nc.DupRate {
			copies = 2 // duplicate = scheduled twice with independent latencies
		}
		for c := 0; c < copies; c++ {
			delay := LogicalTime(1 + w.rng.Intn(w.cfg.MaxLatency))
			w.tracef("t=%d #%d send %d->%d +%d %T%+v", e.At, e.Seq, shell.ID, send.To, delay, send.Msg, send.Msg)
			w.push(Event{At: e.At + delay, Kind: EvDeliver, Node: send.To, From: shell.ID, Msg: send.Msg})
		}
	}

	for _, applied := range out.ApplyEntries {
		w.tracef("t=%d #%d apply node=%d %+v", e.At, e.Seq, shell.ID, applied)
		if w.violation == nil {
			w.violation = w.check.observeApply(shell.ID, shell.R.Status().Term, applied)
		}
		res, isCmd := shell.KV.Apply(applied)
		if isCmd {
			shell.Waiters.Applied(applied.Index, applied.Term, res)
		}
	}

	for _, rs := range out.ReadReady {
		pr, ok := w.pendingReads[rs.Ctx]
		if !ok {
			continue
		}
		delete(w.pendingReads, rs.Ctx)
		delay := LogicalTime(1 + w.rng.Intn(w.cfg.MaxLatency))
		if !rs.OK {
			w.tracef("t=%d #%d read-fail node=%d ctx=%d", e.At, e.Seq, shell.ID, rs.Ctx)
			w.push(Event{At: e.At + delay, Kind: EvClientResp, Client: pr.client, Attempt: pr.attempt, Out: &clientOutcome{lost: true}})
			continue
		}
		// Serve from local state once lastApplied covers the read index. In
		// the sim applies are synchronous with commit, so this must already
		// hold — machine-check it rather than assume it.
		if la := shell.R.Status().LastApplied; la < rs.Index {
			w.violation = &Violation{w.now, e.Seq, fmt.Sprintf(
				"readindex: node %d would serve read at index %d with lastApplied %d", shell.ID, rs.Index, la)}
			return out
		}
		val := shell.KV.Get(pr.key)
		w.tracef("t=%d #%d read-serve node=%d ctx=%d idx=%d %s=%q", e.At, e.Seq, shell.ID, rs.Ctx, rs.Index, pr.key, val)
		w.push(Event{At: e.At + delay, Kind: EvClientResp, Client: pr.client, Attempt: pr.attempt, Out: &clientOutcome{res: kv.Result{Value: val}}})
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
		if m.Granted && (shell.Vote() != send.To || shell.Term() != m.Term) {
			return &Violation{w.now, e.Seq, fmt.Sprintf(
				"persist-before-send: node %d sends vote grant for term %d to %d but durable state is term=%d vote=%d",
				shell.ID, m.Term, send.To, shell.Term(), shell.Vote())}
		}
	case raft.AppendEntries:
		term = m.Term
		// Every entry leaving the node must already be durable.
		durable := shell.Log()
		for _, sent := range m.Entries {
			if sent.Index > uint64(len(durable)) || durable[sent.Index-1].Term != sent.Term {
				return &Violation{w.now, e.Seq, fmt.Sprintf(
					"persist-before-send: node %d sends entry %d/%d that its durable log doesn't hold",
					shell.ID, sent.Index, sent.Term)}
			}
		}
	case raft.AppendEntriesReply:
		term = m.Term
	}
	if term > shell.Term() {
		return &Violation{w.now, e.Seq, fmt.Sprintf(
			"persist-before-send: node %d sends %T at term %d but durable term is %d",
			shell.ID, send.Msg, term, shell.Term())}
	}
	return nil
}
