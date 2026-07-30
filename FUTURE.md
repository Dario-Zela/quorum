# FUTURE.md — deliberately out of scope for v1

Each item below is a real feature of production Raft systems, left out of v1 on purpose.
Knowing *why* each exists (and what it costs) is the point of this file.

- **Membership changes** (single-server changes, not joint consensus): touches the core, snapshot
  metadata, client discovery, and every invariant's definition of "majority" — exactly why it is
  v1's #1 non-goal. 2+ weekends.
- **Pre-vote + CheckQuorum**: prevents a rejoining partitioned node from bumping the term and
  deposing a healthy leader. Core change is small; the real value is the new sim scenarios.
- **Leadership transfer** (thesis §3.10): makes rolling restarts graceful instead of
  timeout-shaped.
- **Lease-based reads**: cheaper than ReadIndex, but only safe under bounded clock drift — the
  honest cost is modeling that drift in the simulator so the *unsafe* cases are testable;
  without that it's cargo cult.
- **Follower ReadIndex relay**: followers serve linearizable reads by asking the leader for a
  read index, instead of forwarding the whole read.
- **Chunked InstallSnapshot**: v1 sends snapshots single-shot; real systems chunk with offsets.
- **Session expiry**: v1 sessions never expire (bounded clients). Expiry must be driven through
  the log so all replicas agree on which sessions die — never by local clocks.
- **Multi-raft / sharding**: one group per key range; routing, split/merge, and rebalancing.
- **Learners / non-voting members**: catch-up replicas that don't count toward majorities.
- **TLS/auth on gRPC**: mechanical, but out of scope for a demo cluster.
- **Jepsen run**: a week+ of harness work, deliberately skipped — porcupine-in-sim checks the
  same property with perfect reproducibility, the better trade for a solo project.
- **Dedicated-hardware benchmarks**: the demo and its status page run in CI
  ([ADR-003](docs/adr/003-ci-verified-demo.md)); GitHub runners are shared hardware, so those
  numbers are indicative. A free VM (e.g. Oracle Always Free ARM) exists in the plan solely to
  re-run the bench table on dedicated hardware with honest labeling.
- **Public "run the demo" button** on the status page: rejected, not deferred — a static page
  can't hold a dispatch token safely, and a rate-limited token-holding middleman is machinery
  that "fork the repo and click Run workflow" already covers ([ADR-003](docs/adr/003-ci-verified-demo.md)).
