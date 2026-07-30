// Package kv is the replicated state machine: a string→string store plus
// the client session table that makes writes exactly-once in effect over
// at-least-once delivery.
//
// Dedup lives HERE, in the apply path, never at propose time: a
// check-then-propose races with in-flight applies, while proposing a
// duplicate is harmless — it applies as a no-op returning the cached
// response. Sessions are part of replicated state (they snapshot with the
// store); restore one without the other and dedup silently breaks.
package kv

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/Dario-Zela/quorum/proto/quorumpb"
	"github.com/Dario-Zela/quorum/raft"
)

// Err codes carried in Result.Err. 0 means OK.
const (
	ErrNone uint32 = iota
	// ErrUnknownClient: no session — never registered, or (post-v1) expired.
	ErrUnknownClient
	// ErrStaleSeq: seq below the session's last — a ghost of an
	// already-answered call (the client keeps ≤1 write outstanding).
	ErrStaleSeq
)

// Result mirrors quorumpb.Result for in-process use.
type Result struct {
	Value    string
	CASOk    bool
	ClientID uint64
	Err      uint32
}

type session struct {
	lastSeq uint64
	cached  Result // the SINGLE cached response — implies ≤1 outstanding write per session
}

// Store is the state machine. Not safe for concurrent use; the apply loop
// owns it.
type Store struct {
	data     map[string]string
	sessions map[uint64]*session
}

func NewStore() *Store {
	return &Store{data: make(map[string]string), sessions: make(map[uint64]*session)}
}

// Get reads a key (absent ⇒ ""). Reads never enter the log; the caller is
// responsible for ReadIndex linearizability before asking.
func (s *Store) Get(key string) string { return s.data[key] }

// EncodeCommand serializes a command for Propose.
func EncodeCommand(cmd *quorumpb.Command) []byte {
	b, err := proto.Marshal(cmd)
	if err != nil {
		panic(fmt.Sprintf("kv: marshal command: %v", err))
	}
	if len(b) == 0 {
		// A raft no-op is nil Data; a real command must never encode empty.
		panic("kv: command encoded to zero bytes")
	}
	return b
}

// Apply feeds one committed entry through the state machine and returns
// the client-visible result. Raft no-ops (nil Data) return ok=false.
func (s *Store) Apply(e raft.Entry) (Result, bool) {
	if len(e.Data) == 0 {
		return Result{}, false
	}
	var cmd quorumpb.Command
	if err := proto.Unmarshal(e.Data, &cmd); err != nil {
		panic(fmt.Sprintf("kv: undecodable command at index %d: %v", e.Index, err))
	}

	if cmd.Op == quorumpb.OpType_OP_REGISTER {
		// The ClientID is the log index of the Register entry itself:
		// unique without coordination, restored from snapshots for free.
		id := e.Index
		if _, exists := s.sessions[id]; !exists {
			s.sessions[id] = &session{}
		}
		return Result{ClientID: id}, true
	}

	sess, ok := s.sessions[cmd.ClientId]
	if !ok {
		return Result{Err: ErrUnknownClient}, true
	}
	// Exactly-once effects: a retry that already committed returns the
	// cached response without re-applying.
	if cmd.Seq == sess.lastSeq && sess.lastSeq > 0 {
		return sess.cached, true
	}
	if cmd.Seq < sess.lastSeq {
		return Result{Err: ErrStaleSeq}, true
	}

	var res Result
	switch cmd.Op {
	case quorumpb.OpType_OP_PUT:
		s.data[cmd.Key] = cmd.Value
	case quorumpb.OpType_OP_DELETE:
		delete(s.data, cmd.Key)
	case quorumpb.OpType_OP_CAS:
		if s.data[cmd.Key] == cmd.OldValue {
			s.data[cmd.Key] = cmd.Value
			res.CASOk = true
		}
	default:
		panic(fmt.Sprintf("kv: unknown op %v at index %d", cmd.Op, e.Index))
	}
	sess.lastSeq = cmd.Seq
	sess.cached = res
	return res, true
}

// Snapshot serializes the store AND the session table. Marshaling is
// deterministic (map ordering fixed): snapshot bytes ride in messages the
// simulator byte-diffs across replays.
func (s *Store) Snapshot() []byte {
	snap := &quorumpb.KVSnapshot{
		Data:     make(map[string]string, len(s.data)),
		Sessions: make(map[uint64]*quorumpb.SessionRec, len(s.sessions)),
	}
	for k, v := range s.data {
		snap.Data[k] = v
	}
	for id, sess := range s.sessions {
		snap.Sessions[id] = &quorumpb.SessionRec{
			LastSeq: sess.lastSeq,
			Cached: &quorumpb.Result{
				Value: sess.cached.Value, CasOk: sess.cached.CASOk,
				ClientId: sess.cached.ClientID, Err: sess.cached.Err,
			},
		}
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(snap)
	if err != nil {
		panic(fmt.Sprintf("kv: marshal snapshot: %v", err))
	}
	return b
}

// RestoreSnapshot replaces the store's entire state with the snapshot's.
func (s *Store) RestoreSnapshot(b []byte) {
	var snap quorumpb.KVSnapshot
	if err := proto.Unmarshal(b, &snap); err != nil {
		panic(fmt.Sprintf("kv: undecodable snapshot: %v", err))
	}
	s.data = make(map[string]string, len(snap.Data))
	for k, v := range snap.Data {
		s.data[k] = v
	}
	s.sessions = make(map[uint64]*session, len(snap.Sessions))
	for id, rec := range snap.Sessions {
		sess := &session{lastSeq: rec.LastSeq}
		if rec.Cached != nil {
			sess.cached = Result{
				Value: rec.Cached.Value, CASOk: rec.Cached.CasOk,
				ClientID: rec.Cached.ClientId, Err: rec.Cached.Err,
			}
		}
		s.sessions[id] = sess
	}
}

// SessionSeq reports a session's lastSeq (testing/observability).
func (s *Store) SessionSeq(clientID uint64) (uint64, bool) {
	sess, ok := s.sessions[clientID]
	if !ok {
		return 0, false
	}
	return sess.lastSeq, true
}

// Len reports the number of keys (testing/observability).
func (s *Store) Len() int { return len(s.data) }
