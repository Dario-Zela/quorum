// Package memstore is the simulator's stand-in for the WAL: it stores the
// record stream in memory and "recovers" by replaying it, so crash-restart
// scenarios exercise the same record semantics (including Truncate replay)
// as the real log, without touching a filesystem.
package memstore

import (
	"fmt"

	"github.com/Dario-Zela/quorum/raft"
	"github.com/Dario-Zela/quorum/storage"
)

type record struct {
	term     uint64
	vote     raft.NodeID
	truncate uint64
	entries  []raft.Entry
}

// Store implements storage.Store in memory. It also keeps a materialized
// copy of the state so Reopen can verify that replaying the record stream
// reproduces it exactly — a free consistency check on the replay logic the
// real WAL shares.
type Store struct {
	records []record
	state   storage.State
}

func New() *Store { return &Store{} }

// Persist implements storage.Store.
func (s *Store) Persist(hs *raft.HardState) error {
	s.records = append(s.records, record{
		term:     hs.Term,
		vote:     hs.VotedFor,
		truncate: hs.TruncateFrom,
		entries:  append([]raft.Entry(nil), hs.Append...),
	})
	s.state.Term, s.state.Vote = hs.Term, hs.VotedFor
	if hs.TruncateFrom > 0 {
		keep := s.state.Entries[:0]
		for _, e := range s.state.Entries {
			if e.Index < hs.TruncateFrom {
				keep = append(keep, e)
			}
		}
		s.state.Entries = keep
	}
	s.state.Entries = append(s.state.Entries, hs.Append...)
	return nil
}

func (s *Store) Close() error { return nil }

// State returns the current materialized state (live view for checkers).
func (s *Store) State() storage.State { return s.state }

// Reopen replays the record stream as recovery would, verifies it matches
// the materialized state, and returns it.
func (s *Store) Reopen() (storage.State, error) {
	var st storage.State
	for i, r := range s.records {
		st.Term, st.Vote = r.term, r.vote
		if r.truncate > 0 {
			keep := st.Entries[:0]
			for _, e := range st.Entries {
				if e.Index < r.truncate {
					keep = append(keep, e)
				}
			}
			st.Entries = keep
		}
		for _, e := range r.entries {
			next := uint64(1)
			if n := len(st.Entries); n > 0 {
				next = st.Entries[n-1].Index + 1
			}
			if e.Index != next {
				return storage.State{}, fmt.Errorf("memstore: replay gap at record %d: expected %d, got %d", i, next, e.Index)
			}
			st.Entries = append(st.Entries, e)
		}
	}
	if st.Term != s.state.Term || st.Vote != s.state.Vote || len(st.Entries) != len(s.state.Entries) {
		return storage.State{}, fmt.Errorf("memstore: replay diverged from materialized state: %+v vs %+v", st, s.state)
	}
	for i := range st.Entries {
		if st.Entries[i].Index != s.state.Entries[i].Index || st.Entries[i].Term != s.state.Entries[i].Term {
			return storage.State{}, fmt.Errorf("memstore: replay diverged at entry %d", i)
		}
	}
	return st, nil
}
