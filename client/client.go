// Package client is the Go client library: leader discovery, retries, and
// sessions. The retry rules mirror the simulator's actors (the same state
// machine the linearizability checker exercises): DISCOVER round-robin
// with a leader-hint short-circuit and backoff between full sweeps; on
// leadership loss, timeout, or connection error, re-DISCOVER and resend
// the SAME Seq — bumping Seq on retry reintroduces duplicate effects.
//
// A Client holds one session and allows at most one outstanding write —
// that is the protocol constraint implied by the single cached response
// per session. It is NOT safe for concurrent use; open one Client per
// logical actor.
package client

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Dario-Zela/quorum/proto/quorumpb"
)

// Client is a session-holding KV client.
type Client struct {
	ids   []uint64
	addrs map[uint64]string
	conns map[uint64]quorumpb.QuorumKVClient

	clientID uint64
	seq      uint64
	hint     uint64
	rr       int
	rng      *rand.Rand

	// AttemptTimeout bounds one RPC attempt; the op as a whole is bounded
	// by the caller's context.
	AttemptTimeout time.Duration
}

// New builds a client for the given id → client-address map.
func New(addrs map[uint64]string) *Client {
	c := &Client{
		addrs:          addrs,
		conns:          make(map[uint64]quorumpb.QuorumKVClient),
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
		AttemptTimeout: 2 * time.Second,
	}
	for id := range addrs {
		c.ids = append(c.ids, id)
	}
	sort.Slice(c.ids, func(i, j int) bool { return c.ids[i] < c.ids[j] })
	return c
}

func (c *Client) conn(id uint64) (quorumpb.QuorumKVClient, error) {
	if kc, ok := c.conns[id]; ok {
		return kc, nil
	}
	cc, err := grpc.NewClient(c.addrs[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	kc := quorumpb.NewQuorumKVClient(cc)
	c.conns[id] = kc
	return kc, nil
}

// nextTarget implements DISCOVER.
func (c *Client) nextTarget(sweeps *int) uint64 {
	if c.hint != 0 {
		t := c.hint
		c.hint = 0
		if _, ok := c.addrs[t]; ok {
			return t
		}
	}
	t := c.ids[c.rr%len(c.ids)]
	c.rr++
	if c.rr%len(c.ids) == 0 {
		// A full sweep found no leader — an electing cluster has no leader
		// to find, and hammering it prolongs the election.
		*sweeps++
		shift := *sweeps
		if shift > 5 {
			shift = 5
		}
		backoff := time.Duration(10<<shift)*time.Millisecond + time.Duration(c.rng.Intn(20))*time.Millisecond
		time.Sleep(backoff)
	}
	return t
}

// do runs one command to completion under ctx.
func (c *Client) do(ctx context.Context, cmd *quorumpb.Command, stale bool) (*quorumpb.Result, error) {
	sweeps := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		target := c.nextTarget(&sweeps)
		kc, err := c.conn(target)
		if err != nil {
			continue
		}
		attemptCtx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
		reply, err := kc.Do(attemptCtx, &quorumpb.DoRequest{Cmd: cmd, StaleRead: stale})
		cancel()
		if err != nil {
			continue // timeout or connection error: re-DISCOVER, same Seq
		}
		switch reply.Status {
		case quorumpb.DoStatus_DO_NOT_LEADER:
			c.hint = reply.LeaderHint
			continue
		case quorumpb.DoStatus_DO_RETRY:
			continue // leadership lost mid-flight; dedup collapses the retry
		}
		sweeps = 0
		if reply.Result == nil {
			return nil, fmt.Errorf("quorum: empty result")
		}
		if reply.Result.Err != 0 {
			return nil, fmt.Errorf("quorum: server error code %d", reply.Result.Err)
		}
		return reply.Result, nil
	}
}

// Register opens the session. Called automatically by the first write.
func (c *Client) Register(ctx context.Context) error {
	if c.clientID != 0 {
		return nil
	}
	res, err := c.do(ctx, &quorumpb.Command{Op: quorumpb.OpType_OP_REGISTER}, false)
	if err != nil {
		return err
	}
	c.clientID = res.ClientId
	return nil
}

func (c *Client) write(ctx context.Context, cmd *quorumpb.Command) (*quorumpb.Result, error) {
	if err := c.Register(ctx); err != nil {
		return nil, err
	}
	c.seq++
	cmd.ClientId = c.clientID
	cmd.Seq = c.seq
	return c.do(ctx, cmd, false)
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.write(ctx, &quorumpb.Command{Op: quorumpb.OpType_OP_PUT, Key: key, Value: value})
	return err
}

// Delete removes key.
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.write(ctx, &quorumpb.Command{Op: quorumpb.OpType_OP_DELETE, Key: key})
	return err
}

// CAS sets key to value iff its current value equals old (absent = ""),
// reporting whether it did.
func (c *Client) CAS(ctx context.Context, key, old, value string) (bool, error) {
	res, err := c.write(ctx, &quorumpb.Command{Op: quorumpb.OpType_OP_CAS, Key: key, OldValue: old, Value: value})
	if err != nil {
		return false, err
	}
	return res.CasOk, nil
}

// Get performs a linearizable read (ReadIndex on the leader).
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	res, err := c.do(ctx, &quorumpb.Command{Op: quorumpb.OpType_OP_GET, Key: key}, false)
	if err != nil {
		return "", err
	}
	return res.Value, nil
}

// GetStale reads from whichever node answers first — no protocol, no
// linearizability. For the benchmark contrast table.
func (c *Client) GetStale(ctx context.Context, key string) (string, error) {
	res, err := c.do(ctx, &quorumpb.Command{Op: quorumpb.OpType_OP_GET, Key: key}, true)
	if err != nil {
		return "", err
	}
	return res.Value, nil
}

// Status queries one node's status RPC.
func (c *Client) Status(ctx context.Context, id uint64) (*quorumpb.StatusReply, error) {
	kc, err := c.conn(id)
	if err != nil {
		return nil, err
	}
	return kc.Status(ctx, &quorumpb.StatusRequest{})
}

// IDs lists the cluster members.
func (c *Client) IDs() []uint64 { return c.ids }
