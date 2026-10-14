#!/usr/bin/env bash
# Every configuration in the README's load-test table, one fresh kwok cluster each.
set -euo pipefail
cd "$(dirname "$0")/.."
run() { # label shards workers rate instances [extra operator flags]
  LABEL=$1 SHARDS=$2 WORKERS=$3 RATE=$4 INSTANCES=$5 MANAGER_FLAGS=${6:-} ./hack/loadtest-kwok.sh >"results/raw/run-$1.out" 2>&1
  echo "done $1"
}
run shards4-w8-r50 4 8 50 19000
run shards4-w8-r100 4 8 100 19000
run shards4-w8-r200 4 8 200 19000
run shards1-w32-r100 1 32 100 19000
run shards4-w8-r100-clusterip-4k 4 8 100 4000 -cluster-ip-services
