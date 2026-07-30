// Package storage defines the durable-state contract between the raft
// core's PersistHard outputs and their on-disk (or in-memory) home.
package storage

import "github.com/Dario-Zela/quorum/raft"

// State is everything a store recovers at startup: the last hard state and
// the surviving log. Note what is absent: commitIndex is deliberately never
// persisted — it is volatile, rediscovered through the first AppendEntries
// exchange; persisting it buys nothing and costs an fsync.
type State struct {
	Term    uint64
	Vote    raft.NodeID
	Entries []raft.Entry // contiguous from SnapIndex+1

	// Snapshot boundary: the state machine's serialized state at SnapIndex.
	SnapIndex uint64
	SnapTerm  uint64
	SnapData  []byte
}

// Store persists raft hard state. Persist must complete durably before the
// caller transmits any message from the same Step — the persist-before-send
// rule.
type Store interface {
	// Persist applies one Step's hard state as a single group commit:
	// term/vote, the truncation (if any), then the appended entries, with
	// one fsync covering the batch.
	Persist(hs *raft.HardState) error
	Close() error
}
