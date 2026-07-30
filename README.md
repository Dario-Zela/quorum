# Quorum

[![CI](https://github.com/Dario-Zela/quorum/actions/workflows/ci.yml/badge.svg)](https://github.com/Dario-Zela/quorum/actions/workflows/ci.yml)
[![Nightly soak](https://github.com/Dario-Zela/quorum/actions/workflows/soak.yml/badge.svg)](https://github.com/Dario-Zela/quorum/actions/workflows/soak.yml)
[![Chaos demo](https://github.com/Dario-Zela/quorum/actions/workflows/chaos-demo.yml/badge.svg)](https://github.com/Dario-Zela/quorum/actions/workflows/chaos-demo.yml)

A distributed key-value store built on the Raft consensus algorithm, implemented from the
[extended paper](https://raft.github.io/raft.pdf) in Go — with a **deterministic simulation
harness** as the centerpiece: seeded, replayable failure injection; invariant checking after
every step; and linearizability verification with
[porcupine](https://github.com/anishathalye/porcupine).

**Live chaos demo:** a 5-node cluster is re-built nightly on GitHub Actions, put under client
load, and survives a leader-killing loop — results, history, and full logs on the
[status page](https://dario-zela.github.io/quorum/). It's a CI job, not a hosted service, so
every claim is publicly auditable and reproducible by forking the repo
([ADR-003](docs/adr/003-ci-verified-demo.md)).

## What's inside

- **Raft from the paper**: leader election, log replication with fast conflict backtracking,
  the §5.4.2 commit rule, persistence with a segmented WAL, snapshots/log compaction with
  InstallSnapshot.
- **Linearizable KV** (Get/Put/Delete/CAS) over gRPC, with client sessions giving
  *exactly-once effects over at-least-once delivery*, and ReadIndex reads that never touch
  the log.
- **The simulator** (`sim/`): a single-threaded event loop over virtual time driving the
  *identical* core code production runs. No goroutines, no clocks, no sockets in the core —
  so every run replays byte-for-byte from its seed, and CI proves it by running seeds twice
  and diffing the traces.

## The central design decision

The Raft core is a **pure, I/O-free state machine** ([ADR-001](docs/adr/001-pure-core.md)):

```go
func (r *Raft) Step(in Input) Output
```

It consumes events (`MsgRecv`, `Tick`, `Propose`, `ReadIndexReq`, `Compact`) and returns
commands — messages to send, state to fsync, entries to apply. *Callers* do the I/O, in a
fixed order: **persist → send → apply**. Only the first arrow is a safety edge: a vote or
append-ack that leaves the node before it is durable can be forgotten by a crash and
contradicted after restart. The simulator machine-checks that ordering on every step — the
invariant is enforced, not remembered.

Time is logical (`Tick{}` events; production feeds a 10 ms ticker, the sim feeds virtual
time) and the only entropy is an injected `*rand.Rand`. That is the entire trick: a core
with no goroutines and no clocks can be driven deterministically through arbitrarily cruel
failure schedules, and every failure found is replayable with one command:

```
REPRO: seed=6 — t=1827 seq=13468: leader completeness: node 5 elected leader of term 50 ...
```

## What the harness checks, always

After **every** step of every simulated node:

1. **Election Safety** — at most one leader per term.
2. **Log Matching** — same (index, term) on two nodes ⇒ identical entries and prefixes.
3. **Leader Completeness** — every committed entry present in every higher-term leader.
4. **State-Machine Safety** — applied sequences are prefix-consistent.
5. **Persist-before-send** — no message may reference state that isn't durable yet.

On top of the always-on invariants, simulated clients run the real retry state machine
(same-`Seq` resends, leader-hint discovery, backoff) and every operation lands in a
**porcupine** history, partitioned per key. Operations with unknown outcomes are recorded
as invoked-but-never-returned — recording them as failed is the standard way to build a
checker that certifies buggy systems.

The nemesis injects: message drop · duplication · reordering · symmetric and asymmetric
partitions · crash-restart (volatile state lost, storage kept, recovery by record replay)
· pause/resume (the GC-stall that breaks naive implementations) · torn writes (exercised
byte-for-byte in the WAL tests).

**The harness is itself tested.** A test-only mutation hook reverts the core to naive Raft
(no commit-rule term clause, no leader no-op) and the Figure 8 scenario asserts the
checkers *do* fire — the instant the stale-logged node claims leadership, weekends before
any client would notice a lost write (`sim/scenario_test.go`).

## Two worked failure traces

**Figure 8 — why the commit rule carries a term clause.** A leader L1 replicates a client
entry *x* to one follower and dies; a second leader L2 rises elsewhere, accepts *y* into
the same slot, and crashes before replicating it; an *x*-holder W wins and re-replicates
*x* to a majority. May W count *x* committed? The paper's answer: **only after W's own
no-op commits above it**. The simulator runs the schedule both ways: with the real rule the
acknowledged write survives L2's return (its stale last-log term can never win an
election); with the buggy rule *x* "commits" by bare counting, L2's higher-term entry wins
the vote comparison, and an acknowledged write vanishes cluster-wide — caught by Leader
Completeness at claim time. (`TestFigureEightRealRulePreservesCommit`,
`TestFigureEightUnsafeRuleIsCaught`)

**Dedup across leader change — why sessions and waiters are one mechanism.** A client's
`Put` is majority-replicated when the leader dies *before responding*. The waiter fails
with a retryable error; the client resends the **same** `{ClientID, Seq}`; the new leader
proposes it *again* (dedup never happens at propose time — that races with in-flight
applies); the apply loop sees `Seq ≤ lastSeq` and returns the cached response. Two log
entries, one state mutation, one client-visible effect — porcupine sees a single `Put`.
The test crashes leaders precisely when a write is in flight, ten times per seed, and
CAS-chained workloads make any double-apply un-linearizable.
(`TestDedupAcrossLeaderChange`)

## Numbers (honest ones)

Apple M-series laptop, local 3–5 node clusters over loopback, 32 closed-loop clients, 8 s
runs. macOS `fsync` is a full flush (`F_FULLFSYNC`), which makes the write path brutally
honest. Indicative, not rigorous — the [status page](https://dario-zela.github.io/quorum/)
carries fresh CI numbers per run.

| workload | throughput | p50 | p99 |
|---|---:|---:|---:|
| writes, 1 node | 258 ops/s | 126 ms | 211 ms |
| writes, 3 nodes | 92 ops/s | 365 ms | 837 ms |
| writes, 5 nodes | 67 ops/s | 515 ms | 1.05 s |
| writes, 5 nodes, **fsync off** | 4,774 ops/s | 487 µs | 39 ms |
| linearizable reads (ReadIndex), 5 nodes | 4,884 ops/s | 533 µs | 39 ms |
| stale reads (no protocol), 5 nodes | 5,176 ops/s | 117 µs | 39 ms |

Why *fsync off* is cheating: the 70× is exactly the price of the persist-before-send rule.
Skip the fsync and an acknowledged write can be forgotten in a crash and contradicted after
restart — the precise failure mode this project exists to prevent. The row is there because
systems that benchmark without durability are quietly selling you this line of the table.
Why the read rows differ: ReadIndex costs one confirmation round of heartbeats (~4× p50)
but never touches the log; stale reads answer from any node with no protocol at all.

## Run it

```bash
cd deploy
docker compose up -d --build          # 5-node cluster
docker compose run --rm bench         # 32-client load generator
INTERVAL=30 ./chaos.sh                # kill the leader every 30s
docker compose exec node1 quorum-cli -cluster \
  "1=node1:7201,2=node2:7201,3=node3:7201,4=node4:7201,5=node5:7201" watch
```

`quorum-cli` speaks `put / get [-stale] / cas / del / status / watch / bench`. Each node
also serves Prometheus counters on `/metrics`.

## Layout

```
quorum/
├── raft/          # pure core: state machine, log, no I/O, no goroutines
├── transport/     # Transport interface; grpctransport (one-way gRPC streams)
├── storage/       # wal (segmented WAL + snapshots); memstore (sim twin)
├── kv/            # replicated state machine: store, sessions, dedup, waiters
├── server/        # node loop, apply loop, status RPC, metrics
├── client/        # Go client: discovery, retries, sessions
├── sim/           # deterministic world: scheduler, nemesis, checkers, porcupine
├── cmd/           # quorum-server, quorum-cli
└── deploy/        # docker-compose, chaos.sh, status page
```

Dependency rule, enforced in CI: `raft/` imports stdlib only; `sim/` never imports the
production transport or storage.

Deliberate non-goals (membership changes above all — the #1 scope trap) are recorded with
reasons in [FUTURE.md](FUTURE.md). The full design contract is
[docs/design.md](docs/design.md); decisions in [docs/adr/](docs/adr/).

## References

- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm* (extended)
- Ongaro's PhD thesis, ch. 3–6 (compaction; the client-session design the paper omits)
- etcd/raft design docs · MIT 6.824 · Jepsen analyses · anishathalye/porcupine
