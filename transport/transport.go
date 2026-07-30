// Package transport defines how raft messages move between nodes. The core
// neither sends nor receives; the server shell pumps messages between the
// core's Outputs and a Transport.
package transport

import "github.com/Dario-Zela/quorum/raft"

// Transport is a best-effort, fire-and-forget message fabric. Send never
// blocks the caller on a slow peer (raft tolerates loss; the protocol
// retries), and inbound messages arrive on Recv.
type Transport interface {
	Send(to raft.NodeID, m raft.Message)
	Recv() <-chan raft.MsgRecv
	Close() error
}
