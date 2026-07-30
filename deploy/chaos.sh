#!/usr/bin/env bash
# Kill the current leader every INTERVAL seconds, ROUNDS times. The leader
# is discovered through the status RPC (via quorum-cli inside the compose
# network); the container's restart policy brings it back, and the cluster
# re-elects around the corpse. Run beside `quorum-cli watch` to see it.
#
#   INTERVAL=30 ROUNDS=10 ./chaos.sh
set -euo pipefail
cd "$(dirname "$0")"

INTERVAL="${INTERVAL:-30}"
ROUNDS="${ROUNDS:-10}"
CLUSTER="1=node1:7201,2=node2:7201,3=node3:7201,4=node4:7201,5=node5:7201"

leader() {
  docker compose exec -T node1 quorum-cli -cluster "$CLUSTER" status 2>/dev/null \
    | awk '$2 == "leader" {print $1; exit}'
}

for round in $(seq 1 "$ROUNDS"); do
  sleep "$INTERVAL"
  L="$(leader || true)"
  if [ -z "$L" ]; then
    echo "[chaos] round $round: no leader visible (election in progress?)"
    continue
  fi
  echo "[chaos] round $round: killing leader node$L"
  docker kill "deploy-node${L}-1" >/dev/null 2>&1 || docker kill "$(docker ps -qf name=node${L})" >/dev/null
done
echo "[chaos] done: $ROUNDS rounds"
