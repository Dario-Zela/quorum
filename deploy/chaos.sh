#!/usr/bin/env bash
# Kill the current leader every INTERVAL seconds, ROUNDS times, bringing
# each victim back a few seconds later (docker's restart policy ignores
# manual kills, so resurrection is explicit). The leader is discovered via
# the status RPC through whichever node answers first; run beside
# `quorum-cli watch` to see each kill and re-election.
#
#   INTERVAL=30 ROUNDS=10 ./chaos.sh
set -euo pipefail
cd "$(dirname "$0")"

INTERVAL="${INTERVAL:-30}"
ROUNDS="${ROUNDS:-10}"
REVIVE_AFTER="${REVIVE_AFTER:-5}"
CLUSTER="1=node1:7201,2=node2:7201,3=node3:7201,4=node4:7201,5=node5:7201"

leader() {
  # Fetch, THEN parse: under pipefail, awk exiting at the first match closes
  # the pipe early and the writer's EPIPE fails the whole pipeline.
  for n in 1 2 3 4 5; do
    local out l
    out=$(docker compose exec -T "node$n" quorum-cli -cluster "$CLUSTER" status 2>/dev/null) || continue
    l=$(awk '$2 == "leader" {print $1; exit}' <<<"$out")
    if [ -n "$l" ]; then
      echo "$l"
      return
    fi
  done
}

for round in $(seq 1 "$ROUNDS"); do
  sleep "$INTERVAL"
  L="$(leader || true)"
  if [ -z "$L" ]; then
    echo "[chaos] round $round: no leader visible (election in progress?)"
    continue
  fi
  echo "[chaos] round $round: killing leader node$L"
  docker kill "$(docker ps -qf "name=node${L}")" >/dev/null
  (sleep "$REVIVE_AFTER" && docker compose up -d "node$L" >/dev/null 2>&1 &&
     echo "[chaos] node$L revived") &
done
wait
echo "[chaos] done: $ROUNDS rounds"
