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

// Slice returns a copy of entries in [lo, hi). Callers must stay within
// (firstIndex-1, LastIndex()+1). Copying matters: the result rides in
// messages and Outputs that outlive the next TruncateFrom/Append, which
// would otherwise scribble over the shared backing array.
func (l *raftLog) Slice(lo, hi uint64) []Entry {
	if lo < l.first || hi > l.LastIndex()+1 {
		panic(fmt.Sprintf("raftLog: slice [%d,%d) out of bounds [%d,%d]", lo, hi, l.first, l.LastIndex()))
	}
	if lo >= hi {
		return nil
	}
	return append([]Entry(nil), l.entries[lo-l.first:hi-l.first]...)
}

// firstIndexOfTerm walks back from i (which must hold term t) to the first
// index of that term still in the log. Used to fill ConflictIndex.
func (l *raftLog) firstIndexOfTerm(i uint64, t uint64) uint64 {
	for i > l.first {
		prev, ok := l.Term(i - 1)
		if !ok || prev != t {
			break
		}
		i--
	}
	return i
}

// lastIndexOfTerm finds the last entry with term t, scanning from the tail.
// ok is false when the log holds no entry of that term.
func (l *raftLog) lastIndexOfTerm(t uint64) (uint64, bool) {
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Term == t {
			return l.entries[i].Index, true
		}
		if l.entries[i].Term < t {
			break // terms are non-decreasing along the log
		}
	}
	return 0, false
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

// CompactTo discards entries with index <= i, remembering (i, term) as the
// snapshot boundary so Term(firstIndex-1) keeps answering. Entries above i
// are retained — call it only when they are known good.
func (l *raftLog) CompactTo(i, term uint64) {
	if i < l.first-1 {
		return // already compacted past here
	}
	if i > l.LastIndex() {
		l.entries = nil
	} else {
		l.entries = append([]Entry(nil), l.entries[i+1-l.first:]...)
	}
	l.first = i + 1
	l.snapIndex, l.snapTerm = i, term
}

// ResetToSnapshot discards the ENTIRE log and rebases it on (i, term) —
// the InstallSnapshot case where the local log does not contain a matching
// (i, term) entry: the whole thing is superseded. Getting this vs CompactTo
// wrong silently truncates live entries (or resurrects dead ones).
func (l *raftLog) ResetToSnapshot(i, term uint64) {
	l.entries = nil
	l.first = i + 1
	l.snapIndex, l.snapTerm = i, term
}
