# Quorum

A distributed key-value store built on the Raft consensus algorithm, implemented from the
[extended paper](https://raft.github.io/raft.pdf) in Go — with a **deterministic simulation
harness** as the centerpiece: seeded, replayable failure injection; continuous invariant
checking; and linearizability verification with
[porcupine](https://github.com/anishathalye/porcupine).

> **Status: under construction.** Following the milestone plan in
> [`docs/design.md`](docs/design.md); this README grows with the project.

## Goals

- Raft from the paper: leader election, log replication, persistence, snapshots/log compaction.
- Linearizable KV service (Get/Put/Delete/CAS) over gRPC with client-session deduplication.
- Deterministic simulation: every failure is replayable from a seed; invariants checked after
  every step; linearizability checked with porcupine.
- Demo: a 5-node Docker Compose cluster surviving a leader-killing chaos loop under client
  load — re-run nightly on GitHub Actions and published to a status page, so the demo is
  continuously *verified*, not just recorded ([ADR-003](docs/adr/003-ci-verified-demo.md)).

## Non-goals (v1)

- **Cluster membership changes** (joint consensus) — the #1 scope trap; static 5-node config.
- Multi-raft/sharding, lease-based reads, pre-vote, learners, TLS/auth — each acknowledged with
  a line in [`FUTURE.md`](FUTURE.md).

## Architecture in one paragraph

The Raft core (`raft/`) is a single-threaded, message-passing state machine that performs **no
I/O whatsoever** — no goroutines, no clocks, no sockets. It consumes input events (`MsgRecv`,
`Tick`, `Propose`) and returns output commands (messages to send, state to fsync, entries to
apply); callers do the sending, persisting, and applying. The production server and the
simulator are two thin shells around identical logic, which is what makes the whole system
deterministically testable. See [ADR-001](docs/adr/001-pure-core.md).

## Layout

```
quorum/
├── raft/          # pure core: state machine, log, no I/O, no goroutines
├── transport/     # Transport interface; grpctransport/ (real); simtransport/ (test)
├── storage/       # Storage interface; wal/ (file WAL + snapshots); memstore/ (test)
├── kv/            # replicated state machine: store, sessions, dedup
├── server/        # node wiring: ticker, transport pump, apply loop, config
├── client/        # Go client lib: leader discovery, retries, sessions
├── sim/           # deterministic world: scheduler, virtual time, nemesis, checkers
├── cmd/           # quorum-server, quorum-cli, quorum-chaos
└── deploy/        # docker-compose.yml, chaos.sh, VM notes
```

Dependency rule (enforced in CI): `raft` imports only stdlib; `sim` never imports
`transport/grpctransport` or `storage/wal`.

## References

- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm* (extended version)
- Ongaro's PhD thesis, ch. 3–6 (log compaction; client sessions & linearizability)
- etcd/raft design docs · MIT 6.824 lab notes · Jepsen analyses · anishathalye/porcupine
