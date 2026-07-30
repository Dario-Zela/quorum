// Package server is the production shell around the pure raft core: one
// goroutine owns the core (Step serialized via channels — concurrency at
// the edges, none in the core), a ticker feeds logical time, the transport
// pumps messages, and an apply-loop goroutine feeds the KV store and the
// write path's waiters.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/Dario-Zela/quorum/kv"
	"github.com/Dario-Zela/quorum/proto/quorumpb"
	"github.com/Dario-Zela/quorum/raft"
	"github.com/Dario-Zela/quorum/storage/wal"
	"github.com/Dario-Zela/quorum/transport"
	"github.com/Dario-Zela/quorum/transport/grpctransport"
)

// Node is a running quorum server.
type Node struct {
	cfg  Config
	log  *slog.Logger
	core *raft.Raft
	wal  *wal.WAL
	tr   transport.Transport

	kvMu    sync.Mutex
	kvStore *kv.Store
	waiters *lockedWaiters

	doCh      chan doReq
	applyCh   chan applyItem
	compactCh chan compactReq
	stop      chan struct{}
	done      sync.WaitGroup
	snapBase  atomic.Uint64

	status      atomic.Pointer[raft.Status]
	lastApplied atomic.Uint64
	readSeq     uint64
	pendingRead map[uint64]doReq

	peerSrv, clientSrv *grpc.Server
	metricsSrv         *http.Server
	metrics            *metrics

	prevRole raft.Role
	prevTerm uint64
}

type doReq struct {
	cmd  *quorumpb.Command
	resp chan *quorumpb.DoReply
}

type compactReq struct {
	index uint64
	data  []byte
}

// applyItem sequences work through the apply loop: entry batches, read
// serves (queued AFTER the entries covering their read index, so ordering
// guarantees lastApplied ≥ index by serve time), and step-down markers
// (queued after the applies that precede them, so waiters complete in the
// right order before the survivors fail).
type applyItem struct {
	entries  []raft.Entry
	read     *readServe
	stepDown bool
	restore  *raft.SnapshotOp // InstallSnapshot: rebuild the state machine
}

type readServe struct {
	key   string
	index uint64
	resp  chan *quorumpb.DoReply
}

// lockedWaiters makes kv.Waiters safe for the node loop (Register,
// StepDown-marker producer) and apply loop (Applied) to share.
type lockedWaiters struct {
	mu sync.Mutex
	w  *kv.Waiters
}

func (l *lockedWaiters) Register(index, term uint64, done func(kv.Outcome)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w.Register(index, term, done)
}
func (l *lockedWaiters) Applied(index, term uint64, res kv.Result) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w.Applied(index, term, res)
}
func (l *lockedWaiters) StepDown() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w.StepDown()
}

// Run starts a node from cfg and returns it once listeners are up.
func Run(cfg Config, logger *slog.Logger) (*Node, error) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("node", cfg.ID)

	ids, err := cfg.PeerIDs()
	if err != nil {
		return nil, err
	}
	w, st, err := wal.Open(cfg.DataDir, wal.Options{})
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}
	core := raft.Restore(raft.Config{
		ID:              raft.NodeID(cfg.ID),
		Peers:           ids,
		Rand:            rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(cfg.ID)<<32)),
		ElectionTickMin: cfg.ElectionTickMin,
		ElectionTickMax: cfg.ElectionTickMax,
		HeartbeatTicks:  cfg.HeartbeatTicks,
	}, raft.RestoredState{
		Term: st.Term, VotedFor: st.Vote,
		SnapIndex: st.SnapIndex, SnapTerm: st.SnapTerm, SnapData: st.SnapData,
		Entries: st.Entries,
	})
	logger.Info("recovered", "term", st.Term, "vote", st.Vote, "snap_index", st.SnapIndex, "log_entries", len(st.Entries))

	tr := grpctransport.New(raft.NodeID(cfg.ID), cfg.PeerAddrs(), logger)

	n := &Node{
		cfg:         cfg,
		log:         logger,
		core:        core,
		wal:         w,
		tr:          tr,
		kvStore:     kv.NewStore(),
		waiters:     &lockedWaiters{w: kv.NewWaiters()},
		doCh:        make(chan doReq, 256),
		applyCh:     make(chan applyItem, 1024),
		compactCh:   make(chan compactReq, 1),
		stop:        make(chan struct{}),
		pendingRead: make(map[uint64]doReq),
		metrics:     newMetrics(cfg.ID),
	}
	if st.SnapData != nil {
		n.kvStore.RestoreSnapshot(st.SnapData)
		n.lastApplied.Store(st.SnapIndex)
		n.snapBase.Store(st.SnapIndex)
	}
	stat := core.Status()
	n.status.Store(&stat)

	// Peer-facing gRPC (raft transport).
	peerLis, err := net.Listen("tcp", cfg.ListenPeer)
	if err != nil {
		return nil, err
	}
	n.peerSrv = grpc.NewServer()
	quorumpb.RegisterRaftTransportServer(n.peerSrv, tr)
	go n.peerSrv.Serve(peerLis)

	// Client-facing gRPC (KV + status).
	clientLis, err := net.Listen("tcp", cfg.ListenClient)
	if err != nil {
		return nil, err
	}
	n.clientSrv = grpc.NewServer()
	quorumpb.RegisterQuorumKVServer(n.clientSrv, &kvService{n: n})
	go n.clientSrv.Serve(clientLis)

	if cfg.ListenMetrics != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(n.metrics.reg, promhttp.HandlerOpts{}))
		n.metricsSrv = &http.Server{Addr: cfg.ListenMetrics, Handler: mux}
		go n.metricsSrv.ListenAndServe()
	}

	n.done.Add(2)
	go n.applyLoop()
	go n.nodeLoop()
	logger.Info("started", "peer_addr", cfg.ListenPeer, "client_addr", cfg.ListenClient)
	return n, nil
}

// Stop shuts the node down.
func (n *Node) Stop() {
	close(n.stop)
	n.peerSrv.Stop()
	n.clientSrv.Stop()
	if n.metricsSrv != nil {
		n.metricsSrv.Close()
	}
	n.tr.Close()
	n.done.Wait()
	n.wal.Close()
}

// nodeLoop is the single goroutine that owns the core.
func (n *Node) nodeLoop() {
	defer n.done.Done()
	ticker := time.NewTicker(time.Duration(n.cfg.TickMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		var out raft.Output
		select {
		case <-n.stop:
			close(n.applyCh)
			return
		case <-ticker.C:
			out = n.core.Step(raft.Tick{})
		case mr := <-n.tr.Recv():
			out = n.core.Step(mr)
		case req := <-n.doCh:
			out = n.handleDo(req)
		case cr := <-n.compactCh:
			out = n.core.Step(raft.Compact{Index: cr.index, Data: cr.data})
		}
		n.process(out)
	}
}

// handleDo runs inside the node loop.
func (n *Node) handleDo(req doReq) raft.Output {
	st := n.core.Status()
	if req.cmd.Op == quorumpb.OpType_OP_GET {
		if st.Role != raft.Leader {
			req.resp <- &quorumpb.DoReply{Status: quorumpb.DoStatus_DO_NOT_LEADER, LeaderHint: uint64(st.LeaderHint)}
			return raft.Output{}
		}
		n.readSeq++
		n.pendingRead[n.readSeq] = req
		return n.core.Step(raft.ReadIndexReq{Ctx: n.readSeq})
	}
	if st.Role != raft.Leader {
		req.resp <- &quorumpb.DoReply{Status: quorumpb.DoStatus_DO_NOT_LEADER, LeaderHint: uint64(st.LeaderHint)}
		return raft.Output{}
	}
	out := n.core.Step(raft.Propose{Data: kv.EncodeCommand(req.cmd)})
	if out.Proposed == nil {
		req.resp <- &quorumpb.DoReply{Status: quorumpb.DoStatus_DO_NOT_LEADER}
		return out
	}
	resp := req.resp
	n.waiters.Register(out.Proposed.Index, out.Proposed.Term, func(o kv.Outcome) {
		if o.LeadershipLost {
			resp <- &quorumpb.DoReply{Status: quorumpb.DoStatus_DO_RETRY}
			return
		}
		resp <- &quorumpb.DoReply{Result: &quorumpb.Result{
			Value: o.Result.Value, CasOk: o.Result.CASOk, ClientId: o.Result.ClientID, Err: o.Result.Err,
		}}
	})
	return out
}

// process honours the Output contract: PersistHard (fsync) → Send →
// ApplyEntries → reads, then bookkeeping.
func (n *Node) process(out raft.Output) {
	if out.PersistHard != nil {
		if err := n.wal.Persist(out.PersistHard); err != nil {
			// Fail-stop: a node that cannot persist must not keep talking.
			n.log.Error("wal persist failed; halting", "err", err)
			panic(fmt.Sprintf("wal persist failed: %v", err))
		}
	}
	if op := out.Snapshot; op != nil {
		// Durable before Send, like PersistHard: the reply acknowledging an
		// installed snapshot references it.
		if err := n.wal.SaveSnapshot(op.Index, op.Term, op.Data); err != nil {
			n.log.Error("snapshot persist failed; halting", "err", err)
			panic(fmt.Sprintf("snapshot persist failed: %v", err))
		}
		n.snapBase.Store(op.Index)
		if op.FromLeader {
			n.applyCh <- applyItem{restore: op}
		}
		n.log.Info("snapshot", "index", op.Index, "term", op.Term, "from_leader", op.FromLeader)
	}
	for _, send := range out.Send {
		n.tr.Send(send.To, send.Msg)
	}
	if len(out.ApplyEntries) > 0 {
		n.applyCh <- applyItem{entries: out.ApplyEntries}
	}
	for _, rs := range out.ReadReady {
		req, ok := n.pendingRead[rs.Ctx]
		if !ok {
			continue
		}
		delete(n.pendingRead, rs.Ctx)
		if !rs.OK {
			req.resp <- &quorumpb.DoReply{Status: quorumpb.DoStatus_DO_RETRY}
			continue
		}
		n.applyCh <- applyItem{read: &readServe{key: req.cmd.Key, index: rs.Index, resp: req.resp}}
	}

	st := n.core.Status()
	n.status.Store(&st)
	n.metrics.observe(st)
	if st.Role != n.prevRole || st.Term != n.prevTerm {
		n.log.Info("state transition",
			"role", st.Role.String(), "term", st.Term,
			"prev_role", n.prevRole.String(), "prev_term", n.prevTerm)
		if st.Role == raft.Candidate && n.prevRole != raft.Candidate {
			n.metrics.elections.Inc()
		}
		if n.prevRole == raft.Leader && st.Role != raft.Leader {
			// Sequenced through applyCh so waiters for already-queued
			// applies complete before the survivors fail.
			n.applyCh <- applyItem{stepDown: true}
		}
		n.prevRole, n.prevTerm = st.Role, st.Term
	}
}

// applyLoop owns the KV store.
func (n *Node) applyLoop() {
	defer n.done.Done()
	for item := range n.applyCh {
		switch {
		case item.stepDown:
			n.waiters.StepDown()
		case item.restore != nil:
			n.kvMu.Lock()
			n.kvStore.RestoreSnapshot(item.restore.Data)
			n.kvMu.Unlock()
			n.lastApplied.Store(item.restore.Index)
			n.metrics.applied.Set(float64(item.restore.Index))
		case item.read != nil:
			if got := n.lastApplied.Load(); got < item.read.index {
				// Impossible by queue ordering; a violation here is a bug.
				panic(fmt.Sprintf("read at index %d before lastApplied %d", item.read.index, got))
			}
			n.kvMu.Lock()
			val := n.kvStore.Get(item.read.key)
			n.kvMu.Unlock()
			item.read.resp <- &quorumpb.DoReply{Result: &quorumpb.Result{Value: val}}
		default:
			for _, e := range item.entries {
				n.kvMu.Lock()
				res, isCmd := n.kvStore.Apply(e)
				n.kvMu.Unlock()
				if isCmd {
					n.waiters.Applied(e.Index, e.Term, res)
				}
				n.lastApplied.Store(e.Index)
				n.metrics.applied.Set(float64(e.Index))
			}
			// Compaction trigger: snapshot the state machine once the log
			// outruns the last snapshot far enough. Non-blocking: skipping
			// a trigger is fine, the next batch re-fires it.
			if la := n.lastApplied.Load(); la >= n.snapBase.Load()+uint64(n.cfg.SnapshotEntries) {
				n.kvMu.Lock()
				data := n.kvStore.Snapshot()
				n.kvMu.Unlock()
				select {
				case n.compactCh <- compactReq{index: la, data: data}:
				default:
				}
			}
		}
	}
}

// --- client-facing gRPC service ---

type kvService struct {
	quorumpb.UnimplementedQuorumKVServer
	n *Node
}

func (s *kvService) Do(ctx context.Context, req *quorumpb.DoRequest) (*quorumpb.DoReply, error) {
	if req.Cmd == nil {
		return nil, fmt.Errorf("empty command")
	}
	// Stale read mode: any node, no protocol — for the bench contrast table.
	if req.StaleRead && req.Cmd.Op == quorumpb.OpType_OP_GET {
		s.n.kvMu.Lock()
		val := s.n.kvStore.Get(req.Cmd.Key)
		s.n.kvMu.Unlock()
		return &quorumpb.DoReply{Result: &quorumpb.Result{Value: val}}, nil
	}
	r := doReq{cmd: req.Cmd, resp: make(chan *quorumpb.DoReply, 1)}
	select {
	case s.n.doCh <- r:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case reply := <-r.resp:
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *kvService) Status(ctx context.Context, _ *quorumpb.StatusRequest) (*quorumpb.StatusReply, error) {
	st := s.n.status.Load()
	return &quorumpb.StatusReply{
		Id:          uint64(st.ID),
		Role:        st.Role.String(),
		Term:        st.Term,
		CommitIndex: st.CommitIndex,
		LastApplied: n_min(st.LastApplied, s.n.lastApplied.Load()),
		LastIndex:   st.LastIndex,
		LeaderHint:  uint64(st.LeaderHint),
	}, nil
}

// The core's LastApplied counts emitted entries; the apply loop's counter
// counts truly applied ones — report the smaller (honest) figure.
func n_min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// --- metrics ---

type metrics struct {
	reg       *prometheus.Registry
	term      prometheus.Gauge
	commit    prometheus.Gauge
	applied   prometheus.Gauge
	isLeader  prometheus.Gauge
	elections prometheus.Counter
}

func newMetrics(id uint64) *metrics {
	reg := prometheus.NewRegistry()
	labels := prometheus.Labels{"node": fmt.Sprint(id)}
	m := &metrics{
		reg:       reg,
		term:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "quorum_term", ConstLabels: labels}),
		commit:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "quorum_commit_index", ConstLabels: labels}),
		applied:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "quorum_last_applied", ConstLabels: labels}),
		isLeader:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "quorum_is_leader", ConstLabels: labels}),
		elections: prometheus.NewCounter(prometheus.CounterOpts{Name: "quorum_elections_total", ConstLabels: labels}),
	}
	reg.MustRegister(m.term, m.commit, m.applied, m.isLeader, m.elections)
	return m
}

func (m *metrics) observe(st raft.Status) {
	m.term.Set(float64(st.Term))
	m.commit.Set(float64(st.CommitIndex))
	if st.Role == raft.Leader {
		m.isLeader.Set(1)
	} else {
		m.isLeader.Set(0)
	}
}
