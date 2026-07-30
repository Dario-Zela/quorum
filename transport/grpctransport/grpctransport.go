// Package grpctransport moves raft messages over gRPC. Each node runs a
// RaftTransport server; for each peer it keeps one outbound goroutine
// holding a client stream, reconnecting with backoff. Messages are
// fire-and-forget: a slow or dead peer drops messages rather than blocking
// the node loop — raft's own retries provide delivery.
package grpctransport

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Dario-Zela/quorum/proto/quorumpb"
	"github.com/Dario-Zela/quorum/raft"
)

const (
	outboundBuffer   = 1024
	reconnectMin     = 100 * time.Millisecond
	reconnectMax     = 3 * time.Second
	inboundBuffer    = 1024
)

// Transport implements transport.Transport over gRPC streams.
type Transport struct {
	quorumpb.UnimplementedRaftTransportServer

	id     raft.NodeID
	log    *slog.Logger
	inbox  chan raft.MsgRecv
	peers  map[raft.NodeID]*peer
	ctx    context.Context
	cancel context.CancelFunc
}

type peer struct {
	addr string
	out  chan *quorumpb.Envelope
}

// New builds the transport. Register the returned Transport on the node's
// gRPC server (RegisterRaftTransportServer) and share the server with the
// KV service.
func New(id raft.NodeID, peerAddrs map[raft.NodeID]string, log *slog.Logger) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &Transport{
		id:     id,
		log:    log,
		inbox:  make(chan raft.MsgRecv, inboundBuffer),
		peers:  make(map[raft.NodeID]*peer),
		ctx:    ctx,
		cancel: cancel,
	}
	for pid, addr := range peerAddrs {
		if pid == id {
			continue
		}
		p := &peer{addr: addr, out: make(chan *quorumpb.Envelope, outboundBuffer)}
		t.peers[pid] = p
		go t.pump(pid, p)
	}
	return t
}

// Deliver implements the RaftTransport service: an inbound one-way stream.
func (t *Transport) Deliver(stream grpc.ClientStreamingServer[quorumpb.Envelope, quorumpb.DeliverAck]) error {
	for {
		env, err := stream.Recv()
		if err != nil {
			return stream.SendAndClose(&quorumpb.DeliverAck{})
		}
		from, msg, err := decode(env)
		if err != nil {
			t.log.Warn("transport: dropping undecodable envelope", "err", err)
			continue
		}
		select {
		case t.inbox <- raft.MsgRecv{From: from, Msg: msg}:
		case <-t.ctx.Done():
			return nil
		default:
			// Inbox full: drop. Raft retries; blocking would let one hot
			// peer wedge the node loop.
		}
	}
}

// Send implements transport.Transport. Never blocks.
func (t *Transport) Send(to raft.NodeID, m raft.Message) {
	p, ok := t.peers[to]
	if !ok {
		return
	}
	select {
	case p.out <- encode(t.id, m):
	default:
		// Peer backlogged (down or partitioned): drop, raft retries.
	}
}

// Recv implements transport.Transport.
func (t *Transport) Recv() <-chan raft.MsgRecv { return t.inbox }

// Close implements transport.Transport.
func (t *Transport) Close() error {
	t.cancel()
	return nil
}

// pump owns the outbound stream to one peer: dial with backoff, stream
// until error, repeat.
func (t *Transport) pump(pid raft.NodeID, p *peer) {
	backoff := reconnectMin
	for t.ctx.Err() == nil {
		conn, err := grpc.NewClient(p.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.sleep(&backoff)
			continue
		}
		client := quorumpb.NewRaftTransportClient(conn)
		stream, err := client.Deliver(t.ctx)
		if err != nil {
			conn.Close()
			t.sleep(&backoff)
			continue
		}
		backoff = reconnectMin
		for {
			var env *quorumpb.Envelope
			select {
			case env = <-p.out:
			case <-t.ctx.Done():
				stream.CloseAndRecv()
				conn.Close()
				return
			}
			if err := stream.Send(env); err != nil {
				t.log.Debug("transport: stream to peer broke", "peer", pid, "err", err)
				break
			}
		}
		conn.Close()
		t.sleep(&backoff)
	}
}

func (t *Transport) sleep(backoff *time.Duration) {
	select {
	case <-time.After(*backoff):
	case <-t.ctx.Done():
	}
	*backoff *= 2
	if *backoff > reconnectMax {
		*backoff = reconnectMax
	}
}
