# ADR-001: Pure, I/O-free Raft core

**Status:** accepted

## Context

Two well-trodden architectures exist for a Raft implementation in Go:

1. **6.824-style**: goroutine-per-peer with a big mutex around shared state. RPCs, timers, and
   appliers all run concurrently and synchronize on the lock.
2. **etcd/raft-style**: a single-threaded, message-passing state machine that performs no I/O.
   It consumes input events and returns output commands; callers do the sending, persisting,
   and applying.

## Decision

The etcd/raft-style pure core, for one reason that defines the whole project: **a core with no
goroutines, no clocks, and no sockets can be driven deterministically by a simulator.** Time is
logical (`Tick{}` events); randomness is an injected `*rand.Rand`; the same seed replays the
same execution byte-for-byte.

## Consequences

- The simulation harness (`sim/`) drives the identical code that production runs — every bug it
  finds is a real bug, and every failure replays from its seed.
- The lock-based style is easier to get started with; the pure core front-loads design work
  (explicit input/output types, an ordering contract on outputs) in exchange for testability.
- Concurrency lives at the edges (`server/`): one goroutine owns the core, and the transport,
  ticker, and apply loop feed it through channels.
- The persist-before-send rule becomes *structural*: `Output` carries `PersistHard` and `Send`
  as separate fields with a documented ordering contract, and the simulator asserts the
  ordering on every step — the invariant is machine-checked, not remembered.
