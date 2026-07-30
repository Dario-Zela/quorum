package grpctransport

import (
	"fmt"

	"github.com/Dario-Zela/quorum/proto/quorumpb"
	"github.com/Dario-Zela/quorum/raft"
)

func entriesToPb(es []raft.Entry) []*quorumpb.Entry {
	out := make([]*quorumpb.Entry, len(es))
	for i, e := range es {
		out[i] = &quorumpb.Entry{Term: e.Term, Index: e.Index, Data: e.Data}
	}
	return out
}

func entriesFromPb(es []*quorumpb.Entry) []raft.Entry {
	out := make([]raft.Entry, len(es))
	for i, e := range es {
		out[i] = raft.Entry{Term: e.Term, Index: e.Index, Data: e.Data}
	}
	return out
}

// encode maps an internal raft message onto the wire envelope.
func encode(from raft.NodeID, m raft.Message) *quorumpb.Envelope {
	env := &quorumpb.Envelope{From: uint64(from)}
	switch v := m.(type) {
	case raft.RequestVote:
		env.Msg = &quorumpb.Envelope_RequestVote{RequestVote: &quorumpb.RequestVoteMsg{
			Term: v.Term, CandidateId: uint64(v.CandidateID),
			LastLogIndex: v.LastLogIndex, LastLogTerm: v.LastLogTerm,
		}}
	case raft.RequestVoteReply:
		env.Msg = &quorumpb.Envelope_RequestVoteReply{RequestVoteReply: &quorumpb.RequestVoteReplyMsg{
			Term: v.Term, Granted: v.Granted,
		}}
	case raft.AppendEntries:
		env.Msg = &quorumpb.Envelope_AppendEntries{AppendEntries: &quorumpb.AppendEntriesMsg{
			Term: v.Term, LeaderId: uint64(v.LeaderID),
			PrevLogIndex: v.PrevLogIndex, PrevLogTerm: v.PrevLogTerm,
			Entries: entriesToPb(v.Entries), LeaderCommit: v.LeaderCommit, Round: v.Round,
		}}
	case raft.AppendEntriesReply:
		env.Msg = &quorumpb.Envelope_AppendEntriesReply{AppendEntriesReply: &quorumpb.AppendEntriesReplyMsg{
			Term: v.Term, Success: v.Success, MatchIndex: v.MatchIndex,
			ConflictIndex: v.ConflictIndex, ConflictTerm: v.ConflictTerm, Round: v.Round,
		}}
	default:
		panic(fmt.Sprintf("grpctransport: unmapped message %T", m))
	}
	return env
}

// decode maps a wire envelope back to the internal message.
func decode(env *quorumpb.Envelope) (raft.NodeID, raft.Message, error) {
	from := raft.NodeID(env.From)
	switch v := env.Msg.(type) {
	case *quorumpb.Envelope_RequestVote:
		return from, raft.RequestVote{
			Term: v.RequestVote.Term, CandidateID: raft.NodeID(v.RequestVote.CandidateId),
			LastLogIndex: v.RequestVote.LastLogIndex, LastLogTerm: v.RequestVote.LastLogTerm,
		}, nil
	case *quorumpb.Envelope_RequestVoteReply:
		return from, raft.RequestVoteReply{Term: v.RequestVoteReply.Term, Granted: v.RequestVoteReply.Granted}, nil
	case *quorumpb.Envelope_AppendEntries:
		return from, raft.AppendEntries{
			Term: v.AppendEntries.Term, LeaderID: raft.NodeID(v.AppendEntries.LeaderId),
			PrevLogIndex: v.AppendEntries.PrevLogIndex, PrevLogTerm: v.AppendEntries.PrevLogTerm,
			Entries: entriesFromPb(v.AppendEntries.Entries), LeaderCommit: v.AppendEntries.LeaderCommit,
			Round: v.AppendEntries.Round,
		}, nil
	case *quorumpb.Envelope_AppendEntriesReply:
		r := v.AppendEntriesReply
		return from, raft.AppendEntriesReply{
			Term: r.Term, Success: r.Success, MatchIndex: r.MatchIndex,
			ConflictIndex: r.ConflictIndex, ConflictTerm: r.ConflictTerm, Round: r.Round,
		}, nil
	}
	return 0, nil, fmt.Errorf("grpctransport: empty or unknown envelope from %d", env.From)
}
