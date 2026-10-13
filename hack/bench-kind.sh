#!/usr/bin/env bash
# Cold vs warm ready time on a real kind cluster (real kubelet, containerd, local-path volumes).
# First run: image preloaded, so neither mode pays for a registry pull. Second run: cold with pull.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=${CLUSTER:-pgfleet}
TRIALS=${TRIALS:-10}
IMAGE=postgres:17-alpine
mkdir -p results

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --wait 120s
kubectl config use-context "kind-$CLUSTER"
# Pull inside the node (kind load trips over multi-arch manifests with Docker's containerd store).
docker exec "$CLUSTER-control-plane" crictl pull "$IMAGE" >/dev/null
kubectl apply -f config/crd

go build -o bin/manager ./cmd/manager
./bin/manager -metrics-bind-address :18080 -health-probe-bind-address :18081 >results/bench-kind-manager.log 2>&1 &
MANAGER=$!
trap 'kill $MANAGER' EXIT

go run ./cmd/bench -n "$TRIALS" -out results/bench-kind.json | tee results/bench-kind.txt
# Cold on a node that has never seen the image (registry pull included): no pool, image evicted
# before every trial.
go run ./cmd/bench -n "${UNCACHED_TRIALS:-5}" -modes cold -pool-size 0 -out results/bench-kind-uncached.json \
  -before-each "docker exec $CLUSTER-control-plane crictl rmi docker.io/library/$IMAGE >/dev/null" | tee -a results/bench-kind.txt
{ echo; kubectl version | grep Server; kind version; } | tee -a results/bench-kind.txt
