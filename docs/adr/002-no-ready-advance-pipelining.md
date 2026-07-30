# ADR-002: No Ready/Advance pipelining

**Status:** accepted

## Context

etcd/raft decouples the core from its caller with a two-phase async protocol: the caller reads
a `Ready` struct (messages, states to persist, entries to apply), acts on it concurrently, and
calls `Advance` when done — allowing the core to prepare the next batch while the previous one
is still being fsynced or transmitted. This pipelining is a real throughput win at etcd scale.

## Decision

Drop it. `Step` is single-threaded and non-reentrant: one caller, and each `Output` is fully
processed — in the fixed order `PersistHard → Send → ApplyEntries → SnapshotOps` — before the
next `Step`.

## Consequences

- The caller contract is one sentence instead of a state machine; there is no "in-flight
  Ready" state to reason about in either the server or the simulator.
- We give up overlap between fsync and message transmission. At this project's scale (5 nodes,
  demo workloads) batching within a single `Output` recovers most of the win: one `Step` can
  carry many entries, and the WAL fsyncs them as one group commit.
- If throughput ever matters, the escape hatch is the same one etcd took — but it would need
  the simulator to model the two-phase protocol too, which is exactly the complexity v1 buys
  its way out of.
