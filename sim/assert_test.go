package sim

import (
	"testing"

	"github.com/Dario-Zela/quorum/raft"
)

// The durability assertion is itself load-bearing: prove it FIRES on the
// violation shapes it exists to catch, not just that correct code stays
// silent under it.
func TestDurabilityAssertionCatchesViolations(t *testing.T) {
	w := New(Config{Seed: 1})
	shell := w.Node(1) // fresh: durable log empty, term 0
	ev := Event{At: 1, Seq: 1}

	// A phantom append ack: claiming entries through index 5 with an empty
	// durable log — the archetypal persist-before-send violation.
	v := w.assertSendDurable(ev, shell, raft.AddressedMsg{To: 2, Msg: raft.AppendEntriesReply{
		Term: 0, Success: true, MatchIndex: 5,
	}})
	if v == nil {
		t.Fatal("phantom append ack not caught")
	}

	// A phantom snapshot ack.
	v = w.assertSendDurable(ev, shell, raft.AddressedMsg{To: 2, Msg: raft.InstallSnapshotReply{
		Term: 0, MatchIndex: 3,
	}})
	if v == nil {
		t.Fatal("phantom snapshot ack not caught")
	}

	// A vote grant whose durable state doesn't back it.
	v = w.assertSendDurable(ev, shell, raft.AddressedMsg{To: 2, Msg: raft.RequestVoteReply{
		Term: 4, Granted: true,
	}})
	if v == nil {
		t.Fatal("unbacked vote grant not caught")
	}

	// An entry shipped that the durable log doesn't hold.
	v = w.assertSendDurable(ev, shell, raft.AddressedMsg{To: 2, Msg: raft.AppendEntries{
		Term: 0, LeaderID: 1, Entries: []raft.Entry{{Term: 1, Index: 1, Data: []byte("ghost")}},
	}})
	if v == nil {
		t.Fatal("undurable shipped entry not caught")
	}

	// And the legitimate shapes pass: a failed ack carries no durability
	// claim, and a truthful ack at the durable tail is fine.
	if v := w.assertSendDurable(ev, shell, raft.AddressedMsg{To: 2, Msg: raft.AppendEntriesReply{
		Term: 0, Success: false, ConflictIndex: 1,
	}}); v != nil {
		t.Fatalf("failed ack wrongly flagged: %v", v)
	}
	if v := w.assertSendDurable(ev, shell, raft.AddressedMsg{To: 2, Msg: raft.AppendEntriesReply{
		Term: 0, Success: true, MatchIndex: 0,
	}}); v != nil {
		t.Fatalf("truthful empty-log ack wrongly flagged: %v", v)
	}
}
