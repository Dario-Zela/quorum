# ADR-003: CI-verified demo instead of a VM-hosted live demo

**Status:** accepted (2026-07-30, supersedes the design doc's "free VM" demo plan)

## Context

The original design (docs/design.md §1, §9) runs the chaos demo on a free cloud VM. A live
status page on GitHub Pages was considered so visitors could see the cluster from the repo.
Both share a weakness: an unattended VM running a deliberate chaos loop is a demo designed to
hurt itself — free tiers get reclaimed, disks fill, and a stale or broken dashboard reads as
an abandoned project. A live page also can't prove anything: a visitor can't verify a status
table isn't hand-edited.

## Decision

The demo runs on **GitHub Actions**, not a VM:

- `chaos-demo.yml` (nightly cron + manual `workflow_dispatch`) boots the real 5-node Docker
  Compose cluster on a GitHub-hosted runner, runs the leader-killing chaos loop under client
  load, and publishes artifacts — throughput graph with kill markers, cluster watch output,
  bench table — to the `gh-pages` branch.
- A static GitHub Pages dashboard renders the latest run and its history, links to the full
  publicly-auditable Actions logs, and shows "reproduce this yourself: fork + Run workflow"
  instructions.
- **No public trigger button.** A static page cannot hold a dispatch token safely, and a
  token-holding middleman service is machinery serving a need that cron + fork-and-run
  already covers. Explicitly rejected.
- Local development uses colima (docker compose on macOS); the free VM is demoted to a
  stretch goal whose only purpose is **clean benchmark numbers on dedicated hardware** —
  CI numbers are labeled indicative (shared runners).

## Consequences

- The demo inherits the project's core pitch: *reproducible verification*. "GitHub's own
  infrastructure re-ran the full chaos demo last night; here are the logs" is a stronger
  claim than "trust me, it's running somewhere."
- The page can never go stale-and-broken: freshness is a cron, not an uptime obligation.
- Public repos get free unlimited Actions minutes on standard runners; a nightly ~15-minute
  demo run and the 50k-step soak cost nothing.
- Benchmark numbers in CI are noisy → the bench table states its hardware honestly, and the
  VM stretch exists solely to produce clean numbers.
