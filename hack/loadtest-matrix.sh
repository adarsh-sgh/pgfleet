#!/usr/bin/env bash
# Every configuration in the README's load-test table, one fresh kwok cluster each.
set -euo pipefail
cd "$(dirname "$0")/.."
run() { LABEL=$1 SHARDS=$2 WORKERS=$3 MANAGER_FLAGS=${4:-} ./hack/loadtest-kwok.sh >"results/raw/run-$1.out" 2>&1; echo "done $1"; }
run shards4-w8 4 8
run shards1-w32 1 32
run shards1-w8 1 8
run shards4-w8-clusterip 4 8 -cluster-ip-services
