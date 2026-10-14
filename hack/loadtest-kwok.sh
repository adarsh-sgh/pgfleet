#!/usr/bin/env bash
# Fleet-scale load test on a kwok cluster: real etcd, kube-apiserver, scheduler and
# controller-manager; kwok fakes the kubelets, so pods go Running/Ready with no containers.
# RUNTIME=binary runs them as local processes (no Docker); RUNTIME=docker uses the upstream images.
#   SHARDS=4 WORKERS=8 ./hack/loadtest-kwok.sh
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=${CLUSTER:-pgfleet-load}
RUNTIME=${RUNTIME:-binary}
SHARDS=${SHARDS:-4}
WORKERS=${WORKERS:-8}        # per shard
QPS=${QPS:-2000}
INSTANCES=${INSTANCES:-19000}
NAMESPACES=${NAMESPACES:-10}
POOL=${POOL:-100}            # warm pods per namespace; total pods = INSTANCES + NAMESPACES*POOL
NODES=${NODES:-25}
RATE=${RATE:-200}
# MANAGER_FLAGS: extra operator flags, e.g. -cluster-ip-services
LABEL=${LABEL:-shards${SHARDS}-w${WORKERS}-qps${QPS}}
export KUBECONFIG=$PWD/results/raw/kwok.kubeconfig
mkdir -p results/raw

kwokctl delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kwokctl create cluster --name "$CLUSTER" --runtime "$RUNTIME" --kubeconfig "$KUBECONFIG" --disable-qps-limits \
  --extra-args kube-apiserver=service-cluster-ip-range=10.0.0.0/16
trap 'kwokctl delete cluster --name "$CLUSTER" >/dev/null 2>&1' EXIT

kwokctl scale node --name "$CLUSTER" --replicas "$NODES" \
  --param '.allocatable.pods=1000' --param '.allocatable.cpu="1000"' --param '.allocatable.memory="4Ti"'
kubectl apply -f config/crd >/dev/null

go build -o bin/manager ./cmd/manager
go run ./cmd/loadtest -manager ./bin/manager -manager-log "results/raw/manager-$LABEL.log" \
  -manager-args "-metrics-bind-address :19080 -health-probe-bind-address :19081 -rotate-credentials=false \
    -shards $SHARDS -workers-per-shard $WORKERS -kube-api-qps $QPS -kube-api-burst $((QPS * 2)) ${MANAGER_FLAGS:-}" \
  -instances "$INSTANCES" -namespaces "$NAMESPACES" -pool-size "$POOL" -rate "$RATE" -label "$LABEL" \
  -out "results/loadtest-$LABEL.json"
