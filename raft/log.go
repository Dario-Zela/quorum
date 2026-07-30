package raft

import "fmt"

// raftLog is the in-memory log: a slice with a firstIndex offset, because
// post-compaction logs don't start at 1. Index 0 is the "empty log" sentinel
// with term 0. After a snapshot, snapIndex/snapTerm remember the compaction
// point so Term(firstIndex-1) — the PrevLogIndex of the first append after a
// compaction — can still be answered.
type raftLog struct {
	first     uint64 // index of entries[0]
	entries   []Entry
	snapIndex uint64
	snapTerm  uint64
}

func newLog() *raftLog {
	return &raftLog{first: 1}
}

// LastIndex is the index of the last entry (snapIndex if the log is empty).
func (l *raftLog) LastIndex() uint64 {
	return l.first + uint64(len(l.entries)) - 1
}

// LastTerm is the term of the last entry (snapTerm if the log is empty).
func (l *raftLog) LastTerm() uint64 {
	t, ok := l.Term(l.LastIndex())
	if !ok {
		panic("raftLog: term of own last index unavailable")
	}
	return t
}

// Term reports the term of entry i. ok is false when i is compacted away or
// beyond the last index. Term(firstIndex-1) answers from snapshot metadata.
func (l *raftLog) Term(i uint64) (term uint64, ok bool) {
	if i == l.first-1 {
		return l.snapTerm, true // snapIndex's term; (0,true) for the empty-log sentinel
	}
	if i < l.first || i > l.LastIndex() {
		return 0, false
	}
	return l.entries[i-l.first].Term, true
}

// Slice returns entries in [lo, hi). Callers must stay within
// (firstIndex-1, LastIndex()+1); the result aliases the log's backing array.
func (l *raftLog) Slice(lo, hi uint64) []Entry {
	if lo < l.first || hi > l.LastIndex()+1 {
		panic(fmt.Sprintf("raftLog: slice [%d,%d) out of bounds [%d,%d]", lo, hi, l.first, l.LastIndex()))
	}
	if lo >= hi {
		return nil
	}
	return l.entries[lo-l.first : hi-l.first]
}

// Append adds entries at the tail. Entries must be contiguous with the log.
func (l *raftLog) Append(es ...Entry) {
	if len(es) == 0 {
		return
	}
	if es[0].Index != l.LastIndex()+1 {
		panic(fmt.Sprintf("raftLog: non-contiguous append: last=%d, appending index %d", l.LastIndex(), es[0].Index))
	}
	l.entries = append(l.entries, es...)
}

// TruncateFrom discards all entries with index >= i.
func (l *raftLog) TruncateFrom(i uint64) {
	if i < l.first {
		panic(fmt.Sprintf("raftLog: truncate at %d below firstIndex %d", i, l.first))
	}
	if i > l.LastIndex() {
		return
	}
	l.entries = l.entries[:i-l.first]
}
