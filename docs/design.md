# Design Doc — Quorum (Raft Distributed KV Store)
*Deep project · 4–6 weekends · Go 1.22+, gRPC, Docker*

> **Amendment (2026-07-30):** the demo/deployment strategy in §1 and §9 ("free VM") is
> superseded by [ADR-003](adr/003-ci-verified-demo.md) — the chaos demo runs on GitHub
> Actions (nightly cron + manual dispatch) and publishes to a GitHub Pages status page;
> the free VM is a stretch goal for dedicated-hardware benchmarks only. The rest of this
> document is the v1 contract as written.

## 1. Goals & non-goals

**Goals**
- Raft from the paper: leader election, log replication, persistence, snapshots/log compaction.
- Linearizable KV service (Get/Put/Delete/CAS) over gRPC with client-session deduplication.
- **Deterministic simulation harness**: seeded, replayable failure injection; continuous invariant checking; linearizability verification with porcupine.
- Live demo: 5-node Docker Compose cluster on the free VM surviving a leader-killing chaos loop under client load.

**Non-goals (documented in README)**
- Cluster membership changes (joint consensus) — the #1 scope trap; static 5-node config.
- Multi-raft/sharding, lease-based reads, pre-vote, learners, TLS/auth. Each gets a line in `FUTURE.md` showing you know they exist.

## 2. The central design decision: a pure, I/O-free core

The Raft core is a **single-threaded, message-passing state machine** that performs no I/O whatsoever. It consumes input events and returns output commands; *callers* do the sending, persisting, and applying. This is the etcd/raft architecture, chosen over the 6.824-style "goroutine-per-peer with a big mutex" for one reason that defines the whole project: **a core with no goroutines, no clocks, and no sockets can be driven deterministically by a simulator.** (ADR-001 documents the trade-off: the lock-based style is easier to start; the pure core is testable and interview-superior.)

```go
// raft/raft.go — the entire surface of the core
type Raft struct { /* unexported: role, term, votedFor, log, commitIndex,
                      nextIndex[], matchIndex[], electionElapsed, ... */ }

type Input interface{ isInput() }
//  MsgRecv{From, Message}   — an RPC or response arrived
//  Tick{}                   — one logical clock tick elapsed
//  Propose{Data []byte}     — client wants an entry appended (leader only)

type Output struct {
    Send        []AddressedMsg  // messages to transmit (caller's job)
    PersistHard *HardState      // term/votedFor/log-suffix to fsync BEFORE Send
    ApplyEntries []Entry        // committed entries for the state machine
    Proposed    *Receipt        // {Index, Term} assigned to a Propose — waiters key on this (§6.1)
    SnapshotOps  ...            // install/compact instructions
}

func (r *Raft) Step(in Input) Output
```

**Time is logical:** the core never reads a clock. `Tick{}` arrives from outside (production: a 10ms ticker; simulation: the virtual scheduler). Election timeout = N ticks randomized in [10, 20); heartbeat = 3 ticks. Randomness comes from an injected `*rand.Rand` — seeded in tests.

**The persistence-before-send rule** (Raft's subtlest correctness requirement — you must fsync term/vote/log before any message referencing them leaves the node) is made structural: `Output` ordering contract says callers must complete `PersistHard` before `Send`. The simulator *asserts* this ordering; the invariant is machine-checked, not remembered.

**The Step contract (what callers must honour, exactly):**
1. `Step` is single-threaded and non-reentrant: one caller (the node loop in production, the scheduler in sim), and each `Output` is fully processed before the next `Step`. (etcd/raft's `Ready`/`Advance` pipelining is deliberately dropped — it buys throughput at the price of a two-phase async protocol; ADR-002 records the trade and why simplicity wins at this scale.)
2. Processing order within one `Output` is fixed: **PersistHard (fsync) → Send → ApplyEntries → SnapshotOps.** Only the first arrow is a safety edge: a vote or append-ack that leaves the node before it is durable can be forgotten by a crash and contradicted after restart (double vote in one term; acknowledged entries vanishing). Apply may lag arbitrarily — that only delays reads. Order *within* `Send` is irrelevant (the network reorders anyway; the sim exploits this).
3. `Propose` on a non-leader yields no `Receipt`, just a leader hint the server layer turns into a redirect. On a leader, `Receipt{Index, Term}` is what the write path's waiter registers under (§6.1); lose it and the client can only time out.
4. Determinism rules inside the core: the only entropy is the injected rng; **no iteration over a Go map anywhere order can affect `Output`** (map order is randomized per process — the classic silent determinism leak); peer sets live in sorted slices. Every replayed seed must produce a byte-identical trace, and the sim verifies exactly that (§7.1).

## 3. Package layout

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
Dependency rule (enforced by import graph, checked in CI with `depguard`): `raft` imports only stdlib; `sim` imports `raft`+`kv` but never `transport/grpctransport` or `storage/wal`. The production binary and the simulator are two thin shells around identical logic.

## 4. Raft core specifics

### 4.1 State & RPCs (per the paper, §5)
- Persistent: `currentTerm`, `votedFor`, `log[]` (entries carry `{Term, Index, Data}`).
- Volatile: `commitIndex`, `lastApplied`; leader: `nextIndex[]`, `matchIndex[]`.
- Messages (protobuf, but core sees internal structs — transport maps them):
  - `RequestVote{Term, CandidateId, LastLogIndex, LastLogTerm}` / reply `{Term, Granted}`
  - `AppendEntries{Term, LeaderId, PrevLogIndex, PrevLogTerm, Entries[], LeaderCommit}` / reply `{Term, Success, ConflictIndex, ConflictTerm}` — the conflict fields implement the fast log-backtracking optimization (skip a term per round-trip, not one entry).
  - `InstallSnapshot{Term, LastIncludedIndex, LastIncludedTerm, Data}` (single-shot in v1; chunking noted in FUTURE.md).
- Election: randomized timeout (redrawn from [10, 20) ticks at *every* reset — a fixed per-node draw re-collides forever) → candidate, self-vote, parallel RequestVotes; majority → leader; immediately send heartbeats to assert leadership **and append a no-op entry in the new term**. The no-op is not an optimization: under the commit rule below it is the only way prior-term entries ever commit, and ReadIndex is unserviceable until it commits (§6.2).
- The Election Restriction (§5.4.1), stated precisely because "candidate's log ≥ own" hides a trap: grant iff `votedFor ∈ {nil, candidate}` for this term **and** (`lastLogTerm_cand > lastLogTerm_mine`, or terms equal and `lastLogIndex_cand ≥ lastLogIndex_mine`). It is a term-first comparison, not a length comparison. The grant (`votedFor`) is persisted before the reply leaves — persist-before-send again.
- Election timer resets on exactly three events: granting a vote, a valid AppendEntries/InstallSnapshot from the current-term leader, and starting an election. Resetting on *rejected* RequestVotes lets a stale-logged but higher-term node suppress legitimate elections indefinitely — a liveness bug the split-vote scenario would mask if this rule were sloppy.
- Term rule, applied uniformly to messages *and replies*: `term > currentTerm` ⇒ adopt the term, revert to follower, clear `votedFor`, persist before anything else leaves the node.
- Commit rule: leader advances `commitIndex` to the highest N where a majority of `matchIndex ≥ N` **and** `log[N].Term == currentTerm` (the §5.4.2 subtlety — never commit prior-term entries by counting; it's the famous Figure 8, traced through this design's structures in §7.6). Bookkeeping detail that bites: `matchIndex[self]` is kept equal to the leader's own last index so the leader counts toward its majority — forgetting this stalls commits and is invisible in happy-path tests with fast followers. Prior-term entries still *replicate* eagerly; they just can't be counted until the election no-op commits above them.
- Conflict-backtracking semantics, since both sides must agree: follower missing `PrevLogIndex` entirely ⇒ `ConflictIndex = lastIndex+1`, `ConflictTerm = none`; term mismatch at `PrevLogIndex` ⇒ `ConflictTerm =` that entry's term, `ConflictIndex =` first index of that term in the follower's log. Leader on rejection: if its log contains entries in `ConflictTerm`, `nextIndex :=` (its last index in that term) + 1; otherwise `nextIndex := ConflictIndex`.
- InstallSnapshot edge cases: a snapshot with `LastIncludedIndex ≤ commitIndex` is stale — ignore it (it can arrive late through a reordering network). If the follower's log contains an entry matching `(LastIncludedIndex, LastIncludedTerm)`, retain the suffix after it; otherwise discard the whole log. Getting the second half wrong silently truncates live entries.

### 4.2 Log representation
In-memory: slice with a `firstIndex` offset (post-compaction logs don't start at 1). Interface hides it:
`Term(i)`, `Slice(lo, hi)`, `Append(entries)`, `TruncateFrom(i)`, `CompactTo(i, snapMeta)`.
Boundary case worth naming: `Term(firstIndex−1)` must answer from snapshot metadata (`lastIncludedTerm`) — that index is the `PrevLogIndex` of the first append after a compaction, and returning "unknown" there breaks replication to every freshly-snapshotted follower.

## 5. Storage layer (`storage/wal`)

- **Segment lifecycle:** one *active* segment receives appends; at 64MB it is fsynced, closed, and becomes immutable (`wal-000001.log`, `wal-000002.log`, … — names monotonic, recovery order lexicographic). Record = `[len:u32][crc32c:u32][protobuf]`; record types: `HardState`, `Entries` batch, and `Truncate{fromIndex}`.
- **Why a `Truncate` record exists:** an append-only file cannot physically delete the conflicting suffix a follower discards when its leader overwrites its log. Truncation is therefore itself a logged event; recovery replays it. Without it, a restart resurrects entries the node already disowned to its leader — it then diverges, and Log Matching fires only after the *next* crash. One of the nastiest bug shapes in this project class, so it's structural.
- **CRC policy, two cases:** a failed CRC on the *final* record of the *final* segment is a torn write ⇒ truncate it and continue — safe because an unpersisted record was never acknowledged anywhere. A failed CRC anywhere else is corruption ⇒ refuse to start. Guessing here converts a detectable fault into silent divergence.
- fsync policy: on every `PersistHard` (correctness), batched within one `Step` output (performance). Group commit across concurrent proposals happens naturally via the apply loop batching. Segment rolls and snapshot renames additionally fsync the *directory* — a rename is a directory-entry write and is not durable until the directory is.
- **Snapshots:** `snap-<index>-<term>.db` = serialized KV store **+ session table** (sessions are part of replicated state — restore without them and dedup silently breaks; classic bug, note in ADR) + `{lastIncludedIndex, lastIncludedTerm}` + checksum. Durability order: write temp → fsync file → rename → fsync dir → *only then* delete WAL segments entirely below the snapshot index and any older snapshot. The previous snapshot is retained until the new one is fully durable, so a crash at any instant leaves at least one valid (snapshot, WAL-suffix) pair on disk.
- **Startup:** (1) load the newest snapshot passing its checksum; restore KV + session table; `lastApplied := lastIncludedIndex`. (2) Replay WAL from there, applying `Truncate` records and keeping the last `HardState`. (3) Hand `HardState` + log to the core. `commitIndex` is deliberately *never* persisted: it is volatile state, rediscovered through the first AppendEntries exchange — persisting it buys nothing and costs an fsync.

## 6. KV service (`kv/`, `server/`)

### 6.1 Write path
```
client.Put ─gRPC→ leader server ─Propose→ raft core ─replicate→ majority
   ▲                                                        │ commit
   └── response ←─ waiter (by log index) ←─ apply loop ←────┘
```
- Server maintains `waiters: map[index]chan Result`, registered under the `Receipt{Index, Term}` from `Propose` (§2). On apply, complete the waiter **iff the applied entry's term matches the receipt term** — otherwise a different entry won that index after a leadership change → return `ErrLeadershipLost`, client retries. (This term-check is the classic missed bug; it gets a dedicated sim scenario.) Additionally, the moment the core steps down, **all** outstanding waiters fail with `ErrLeadershipLost` immediately — the proposal may in fact still commit later, and that's fine *because* the retry carries the same `{ClientID, Seq}` and dedup collapses it. Waiters and sessions are one mechanism seen from two ends, not two features.
- **Sessions & dedup:** commands carry `{ClientID, Seq}`; the replicated session table stores `lastSeq` + the **single** cached response per client. Duplicate `Seq` (a retry that actually committed) returns the cached response without re-applying — exactly-once *effects* over at-least-once delivery. One cached response implies a protocol constraint the client lib enforces: **at most one outstanding write per session** (the design from Ongaro's thesis ch. 6). `Seq < lastSeq` can then only be a ghost of an already-answered call ⇒ reject. Duplicate detection lives in the *apply loop*, never at propose time — check-then-propose races with in-flight applies; proposing a duplicate is harmless (it applies as a no-op returning the cache), and §7.6 traces exactly this.
- Client obtains `ClientID` via a `Register` command through the log; the ID **is the log index of its own Register entry** — unique without coordination, restored from snapshots for free. A timed-out Register retry can leak an orphan session; v1 accepts that (bounded clients, sessions never expire). Real systems expire sessions, and expiry must itself be driven through the log so all replicas agree — never by local clocks; that reasoning gets the FUTURE.md line, not just the feature name.

### 6.2 Linearizable reads (ReadIndex protocol)
Reads don't enter the log (throughput), but must not be served from a deposed leader's stale state:
1. **Gate:** a new leader serves no reads until its own-term no-op (§4.1) commits. Until then its `commitIndex` can be *behind* entries the previous leader committed — it holds them but cannot count them — so `readIndex := commitIndex` would miss committed writes: a linearizability violation porcupine catches within a few hundred seeds. This gate is the most-omitted line in ReadIndex writeups.
2. Leader records `readIndex := commitIndex`.
3. Confirms leadership with a majority round of heartbeats **tagged with a round number issued after the record** — acks from earlier heartbeats must not count, or the deposed-leader race sneaks back in through response reordering.
4. Waits until `lastApplied ≥ readIndex`, then serves from the local store.
Batch concurrent reads behind one confirmation round (every read arriving mid-round joins it). Follower reads forward to the leader in v1 (follower ReadIndex relay: FUTURE.md). `Stale` read mode (any node, no protocol) offered explicitly for the benchmark contrast table.

### 6.3 Client retry state machine (`client/`)
States: `DISCOVER → SEND → {done, retry}`.
- **DISCOVER:** try nodes round-robin; `ErrNotLeader{hint}` short-circuits to the hint; exponential backoff + jitter between full sweeps (an electing cluster has no leader to find — hammering it prolongs the election).
- **SEND (writes):** send `{ClientID, Seq, op}`. On success: deliver result, `Seq++`. On `ErrLeadershipLost`, timeout, or connection error: back to DISCOVER and **resend the same Seq** — this is the entire point of sessions; bumping Seq on retry reintroduces duplicate effects.
- **Reads** carry no Seq (ReadIndex reads are idempotent) and retry freely.
- The sim drives this exact client (simtransport instead of gRPC), so the retry logic itself sits under the linearizability checker — the client library is tested code, not demo glue.

## 7. Simulation harness (`sim/`) — the differentiator

### 7.1 The world
```go
type World struct {
    now      LogicalTime            // virtual clock; no time.Now() anywhere in sim
    rng      *rand.Rand             // THE seed — every run replayable
    nodes    [5]*NodeShell          // raft core + memstore + kv, no goroutines
    inflight EventHeap              // {deliverAt, msg} — the network
    partition PartitionMatrix       // who can reach whom
}
```
Single-threaded event loop: pop next event (message delivery or node tick), step the target node, collect outputs, schedule new events with rng-drawn delays. **No real goroutines, no real time, no data races possible in sim — every bug is a logic bug and every run replays from its seed.**

**Event model.** The heap is keyed by `(deliverAt, seq)` where `seq` is a monotonically assigned tiebreak — simultaneous events must pop in a deterministic order or replayability dies quietly. Event kinds: `Deliver{to, msg}`, `Tick{node}`, `ClientOp{...}`, `NemesisOp{...}`, `Restart{node}`. Popping an event mutates the world, steps the target node, and processes its `Output` inline: `PersistHard` goes to that node's memstore (the harness *asserts* persist-before-send right here), and each `Send` becomes a future `Deliver` at `now + latency(rng)`. Drop = never scheduled; duplicate = scheduled twice; reorder = independent latency draws — no special machinery, the heap *is* the network.

**Partition matrix semantics.** `reach[from][to] bool` — directed, so asymmetric partitions are the natural case, not a special one (the pause/GC scenarios need one-way reachability). Checked at **delivery** time, not send time: a partition that forms while messages are in flight eats them too, matching real networks where packets on the wire die with the link, and healing never resurrects a dropped message. `reach[i][i]` is always true. The nemesis edits the matrix and schedules the heal as a future event.

**Determinism is verified, not assumed:** CI replays a sample of seeds twice per run and byte-diffs the full event traces. This catches the leaks that silently destroy replayability — map-iteration order, a stray `time.Now()`, an unseeded shuffle in a dependency — on the day they are introduced, not the day a soak failure refuses to reproduce.

### 7.2 Nemesis (fault injection, all rng-driven with configurable rates)
Message drop · duplicate · reorder (delay jitter) · symmetric & asymmetric partitions (heal after k ticks) · node crash-restart (volatile state lost, storage kept) · node pause/resume (simulates GC/VM stall — the asymmetric-information scenario that breaks naive implementations) · torn-write on recovery (truncate the memstore WAL mid-record).

### 7.3 Always-on invariant checkers (assert after every step)
1. **Election Safety** — ≤1 leader per term (track all `(term, leaderID)` claims globally).
2. **Log Matching** — same `(index, term)` across any two nodes ⇒ identical entries and identical prefixes.
3. **Leader Completeness** — every entry committed in term T present in every leader of terms > T.
4. **State-Machine Safety** — applied sequences across nodes are prefix-consistent.
5. Persist-before-send ordering (§2) and monotonic terms per node.

### 7.4 Linearizability checking
Simulated clients issue concurrent Get/Put/Delete/CAS through the world; record history `{op, args, invokeTime, returnTime, result}`; feed **porcupine** (github.com/anishathalye/porcupine) with a KV model.

**The KV model, precisely:** state is `map[string]string`; `Get` returns the current value (absent ⇒ `""` — nil-vs-absent collapsed deliberately, see Risks); `Put`/`Delete` overwrite/remove; `CAS(k, old, new)` mutates iff current equals `old` and reports which way it went. Histories are **partitioned per key** via porcupine's partition function — linearizability is a local (composable) property, so per-key checking is sound and turns one exponential search into many trivial ones.

**The subtlety that makes or breaks the checker:** an operation with *unknown* outcome — client timed out, gave up after `ErrLeadershipLost` — must be recorded as invoked-but-never-returned, leaving porcupine free to linearize it at any later point *or never*. Recording such ops as failed is the standard way to build a checker that certifies buggy systems: acknowledged-then-lost and unacknowledged-but-applied writes are precisely what this distinction detects.

Run in CI: 500 short seeds every push, 50k-step nightly soak; any failure prints `REPRO: go test -run TestSim -seed=184467...`. A `t.Logf` event trace per node, gated on failure, makes the seed debuggable.

### 7.5 Scenario tests (directed, on top of random soak)
Figure-8 prior-term commit scenario · split-vote storms (timeouts aligned) · partitioned-leader-keeps-accepting (its uncommitted writes must error on client, disappear on heal) · snapshot-lagging-follower (InstallSnapshot path) · dedup-across-leader-change · ReadIndex-during-partition (stale leader must fail the read).

### 7.6 Two worked failure traces (README centerpieces)

**Figure 8, through this design's structures** — why the commit rule carries the term clause. Five nodes S1–S5; `⟨index:term⟩` denotes an entry.

1. S1 leads term 2, appends `⟨2:2⟩`, replicates it to S2 only, crashes. (Nemesis: `Crash(S1)` scheduled after one `Deliver`.)
2. S5 wins term 3 with votes from S3, S4, S5 — legal: `⟨2:2⟩` is uncommitted and none of its voters hold it. S5 appends `⟨2:3⟩` locally, crashes before any `Deliver` fires.
3. S1 restarts and wins term 4 (its last entry `⟨2:2⟩` beats S3/S4's bare logs on the term-first comparison). Per §4.1 it appends its no-op `⟨3:4⟩`, and its append pump re-replicates `⟨2:2⟩` — which now sits on S1, S2, S3: a majority.
4. **The buggy rule** (majority `matchIndex` alone) now sets `commitIndex = 2` and acks the client. Kill S1 before `⟨3:4⟩` spreads; S5 restarts and wins term 5 — its `lastLogTerm = 3` beats every voter's `2`. S5's appends overwrite index 2 with `⟨2:3⟩` cluster-wide. An acknowledged write is gone. In the sim, **Leader Completeness fires the instant S5's leadership claim is recorded** — weekends before any client would notice — and porcupine independently flags the lost write.
5. **The real rule** refuses to count `⟨2:2⟩` in term 4. Only when the no-op `⟨3:4⟩` reaches a majority do both entries commit — and from that moment S5 *cannot* win an election (Election Restriction: a majority now answers with `lastLogTerm = 4`). The same nemesis schedule passes clean.

The scenario runs both ways: a test-only mutation hook switches the core to the buggy rule and asserts the invariant **does** fire — the harness is itself tested (poor-man's mutation testing, and a great interview beat).

**Dedup across leader change** — why sessions and waiters are one mechanism.

1. Client (ClientID 7, Seq 12) sends `Put x=1` to leader S1. S1 appends `⟨40:6⟩`, replicates it to a majority, then crashes — *before responding*. The waiter for index 40 dies with `ErrLeadershipLost` (§6.1 step-down rule); the client sees a retryable error, not a hang.
2. S2 wins term 7. Because `⟨40:6⟩` sits on a majority and vote comparisons are term-then-length, no candidate lacking it can be elected — S2 necessarily holds it, but must not count it committed until its own no-op `⟨41:7⟩` commits (the rule the first trace just justified).
3. The no-op commits, pulling `⟨40:6⟩` in beneath it; the apply loop applies it: session 7 records `lastSeq = 12`, caches `OK`.
4. The client's retry `{7, 12}` reaches S2, which proposes it as `⟨42:7⟩` — no propose-time dedup check, per §6.1. On apply, `Seq ≤ lastSeq` ⇒ the entry is a no-op returning the cached `OK`. Two proposals, one state mutation, one client-visible effect — porcupine sees a single `Put`.

## 8. Production shell (`server/`, `transport/grpctransport`)

- Node loop: one goroutine owning the core (`Step` serialized via input channel — concurrency at the edges, none in the core); 10ms ticker; per-peer outbound goroutine with reconnect+backoff (gRPC streams); apply-loop goroutine feeding kv + waiters.
- Config: static TOML (`id`, `peers[]`, `dataDir`, timeout tick counts).
- Status RPC: `{id, role, term, commitIndex, lastApplied, leaderHint}` — powers `chaos.sh`'s leader discovery and the demo's cluster table; costs an afternoon, earns its keep twice.
- Observability: structured slog per state transition; `/metrics` Prometheus counters (elections, term, commitIndex, apply lag, RPC latency histograms) — a Grafana-ready screenshot is stretch, counters are v1.

## 9. Demo & benchmarks (`deploy/`)

- `docker compose up`: 5 nodes + a load-generator container (`quorum-cli bench --clients 32`).
- `chaos.sh`: every 30s `docker kill` the current leader (discovered via status RPC); client throughput graph shows dips + recoveries, no errors surfaced to completed writes. Terminal recording (asciinema) in the README, with `quorum-cli watch` (1s-refresh per-node role/term/commitIndex table from the status RPC) running beside the chaos loop — it makes each leader kill and re-election legible on screen.
- Bench table: write throughput/latency (p50/p99) vs cluster size {1,3,5} · linearizable vs stale reads · fsync on/off (with the "why off is cheating" paragraph). Run on the free VM; label the hardware honestly.

## 10. Milestones

| Weekend | Deliverable |
|---|---|
| 1 | Core: elections + heartbeats; sim v0 (scheduler, delivery, ticks); invariant 1 checking; determinism double-run diff; first seeds pass |
| 2 | Log replication + commit rule + leader no-op + conflict backtracking; invariants 2–4; directed scenarios (Figure 8 both ways via mutation hook, split vote) |
| 3 | Persistence: WAL incl. `Truncate` records + recovery + torn-write handling; crash-restart nemesis; persist-before-send assertion |
| 4 | KV + sessions + dedup + waiters (incl. step-down failure); porcupine wired with per-key partitioning; nightly soak job |
| 5 | ReadIndex reads (no-op gate, tagged rounds); gRPC transport + server shell + client lib retry SM; compose cluster boots |
| 6 | Snapshots/compaction + InstallSnapshot; chaos demo + `quorum-cli watch` + benchmarks + README (incl. §7.6 traces) + tag v1.0 |

### 10.1 Stretch, with honest estimates (post-v1, in value-per-hour order)
- **Grafana dashboard** over the existing Prometheus counters — ~½ weekend; the highest demo-value-per-hour item, do first if v1 lands early.
- **Chunked InstallSnapshot** — ~½ weekend; mechanical once the single-shot path works.
- **Leadership transfer** (thesis §3.10) — ~½ weekend; makes rolling restarts in the demo graceful instead of timeout-shaped.
- **Pre-vote + CheckQuorum** — ~1 weekend; the core change is small, the real value is the new sim scenarios (a rejoining partitioned node no longer bumps the term and deposes a healthy leader).
- **Lease-based reads** — ~1 weekend, but the honest cost is modeling bounded clock drift in the sim so the *unsafe* cases are testable; without that it's cargo cult.
- **Membership changes** (single-server changes, not joint consensus) — 2+ weekends; touches core, snapshot metadata, client discovery, and every invariant's definition of "majority". Exactly why it's a non-goal.
- **Jepsen run** — a week+ of harness work, deliberately skipped: porcupine-in-sim checks the same property with perfect reproducibility, the better trade for a solo project — and being able to say why is worth more than the run.

## 11. Risks
- **The Raft debugging spiral** → mitigated structurally: sim exists from weekend 1 (before gRPC, before disk); every failure is seed-replayable; invariants fire at the *first* violation, not at the eventual symptom.
- **Porcupine model subtleties** (CAS semantics, nil-vs-absent) → keep the KV model tiny (string→string, four ops); test the model itself against hand-written histories first.
- **Nondeterminism leaks** (map-iteration order, a stray `time.Now()`, unseeded rand in a dependency) silently destroy seed-replayability, which is the whole pitch → caught structurally by the CI double-run trace diff (§7.1) the day they appear, not the day a soak failure won't reproduce.
- **Scope** → membership changes are the siren song; the non-goals list is the contract.

## 12. References
- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm* (extended version — the one with Figure 8)
- Ongaro's PhD thesis ch. 3–6 (log compaction, and the ch. 6 client-session/linearizability design the paper omits)
- etcd/raft design docs (the pure-core precedent) · MIT 6.824 lab notes · Jepsen analyses (what real systems get wrong) · anishathalye/porcupine
