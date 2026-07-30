package sim

import (
	"github.com/Dario-Zela/quorum/kv"
	"github.com/Dario-Zela/quorum/proto/quorumpb"
)

// testPayload wraps a tag in a valid (but session-less) command so
// replication-level tests can propose raw data without tripping the KV
// decoder. It applies as ErrUnknownClient: deterministic, no mutation.
func testPayload(s string) []byte {
	return kv.EncodeCommand(&quorumpb.Command{
		Op: quorumpb.OpType_OP_PUT, ClientId: 999_999, Seq: 1, Key: "t", Value: s,
	})
}
