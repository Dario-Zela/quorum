package sim

import "github.com/Dario-Zela/quorum/raft"

// LogicalTime is the simulator's virtual clock. Nothing in sim (or in the
// raft core it drives) ever reads a real clock.
type LogicalTime uint64

// EventKind discriminates scheduled events.
type EventKind uint8

const (
	// EvTick advances one node's logical clock.
	EvTick EventKind = iota
	// EvDeliver hands a message to a node — the heap is the network:
	// drop = never scheduled, duplicate = scheduled twice, reorder =
	// independent latency draws.
	EvDeliver
	// EvClientOp marks a client-injected input (Propose) in the trace; it is
	// synthesized by World.Propose rather than scheduled on the heap.
	EvClientOp
	// EvNemesis draws one structural fault (partition, crash, pause) and
	// reschedules itself.
	EvNemesis
	// EvHeal removes all partitions.
	EvHeal
	// EvRestart brings a crashed node back up.
	EvRestart
	// EvClientStart dispatches a client actor's next attempt (invoking a
	// fresh op if none is pending).
	EvClientStart
	// EvClientReq delivers a client request to a node's server logic.
	EvClientReq
	// EvClientResp delivers a server response back to a client actor.
	EvClientResp
	// EvClientTimeout re-enters DISCOVER if the attempt got no answer.
	EvClientTimeout
)

// Event is one scheduled occurrence.
type Event struct {
	At   LogicalTime
	Seq  uint64 // monotonic tiebreak: simultaneous events must pop deterministically
	Kind EventKind

	Node raft.NodeID // target
	From raft.NodeID // EvDeliver: sender
	Msg  raft.Message

	Client  int // client-protocol events: actor index
	Attempt uint64
	Cmd     []byte         // EvClientReq: encoded command
	Out     *clientOutcome // EvClientResp
}

// eventHeap is a min-heap keyed by (At, Seq). Implemented directly rather
// than via container/heap to keep the hot loop allocation-free and the
// ordering rule in one visible place.
type eventHeap struct {
	events []Event
}

func (h *eventHeap) Len() int { return len(h.events) }

func (h *eventHeap) less(i, j int) bool {
	if h.events[i].At != h.events[j].At {
		return h.events[i].At < h.events[j].At
	}
	return h.events[i].Seq < h.events[j].Seq
}

func (h *eventHeap) Push(e Event) {
	h.events = append(h.events, e)
	i := len(h.events) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			break
		}
		h.events[i], h.events[parent] = h.events[parent], h.events[i]
		i = parent
	}
}

func (h *eventHeap) Pop() Event {
	top := h.events[0]
	last := len(h.events) - 1
	h.events[0] = h.events[last]
	h.events = h.events[:last]
	i := 0
	for {
		l, r := 2*i+1, 2*i+2
		smallest := i
		if l < len(h.events) && h.less(l, smallest) {
			smallest = l
		}
		if r < len(h.events) && h.less(r, smallest) {
			smallest = r
		}
		if smallest == i {
			break
		}
		h.events[i], h.events[smallest] = h.events[smallest], h.events[i]
		i = smallest
	}
	return top
}
