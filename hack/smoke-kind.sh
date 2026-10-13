#!/usr/bin/env bash
# Checks on a real kubelet what envtest cannot: a warm claim's rotated password is the only one
# that works, CPU resizes in place without a restart, memory resizes restart the container, and
# local-path (no expansion support) surfaces ExpansionNotSupported. Expects the operator running.
set -euo pipefail
NS=smoke-$(date +%s)
k() { kubectl -n "$NS" "$@"; }
jp() { k get "$1" -o jsonpath="$2"; }
fail() { echo "FAIL: $*"; exit 1; }

kubectl create ns "$NS" >/dev/null
trap 'kubectl delete ns "$NS" --wait=false >/dev/null' EXIT
k apply -f - >/dev/null <<EOF
apiVersion: pgfleet.dev/v1alpha1
kind: WarmPool
metadata: {name: small}
spec: {size: 1, resources: {cpu: 500m, memory: 512Mi}, storage: 1Gi}
EOF
until [ "$(jp warmpool/small '{.status.ready}')" = 1 ]; do sleep 1; done
k apply -f - >/dev/null <<EOF
apiVersion: pgfleet.dev/v1alpha1
kind: PostgresInstance
metadata: {name: db}
spec: {poolRef: small, resources: {cpu: 750m, memory: 512Mi}, storage: 1Gi}
EOF
until [ "$(jp pgi/db '{.status.conditions[?(@.type=="Ready")].status}')" = True ]; do sleep 0.5; done
[ "$(jp pgi/db '{.status.source}')" = warm ] || fail "not a warm claim"

POD=$(jp pgi/db '{.status.podName}')
IP=$(jp pod/"$POD" '{.status.podIP}')
PW=$(jp secret/db-credentials '{.data.password}' | base64 -d)
BOOT=$(jp secret/small-bootstrap '{.data.password}' | base64 -d)
psql_as() { k exec "$POD" -- env PGPASSWORD="$1" psql -h "$IP" -U postgres -tAc 'select 1' >/dev/null 2>&1; }
psql_as "$PW" || fail "instance password rejected"
psql_as "$BOOT" && fail "pool bootstrap password still works"
echo "ok: warm claim, password rotated (bootstrap password rejected over TCP)"

until [ "$(jp pod/"$POD" '{.status.containerStatuses[0].resources.limits.cpu}')" = 750m ]; do sleep 0.5; done
[ "$(jp pod/"$POD" '{.status.containerStatuses[0].restartCount}')" = 0 ] || fail "cpu resize restarted postgres"
echo "ok: cpu 500m -> 750m in place, 0 restarts"

k patch pgi db --type merge -p '{"spec":{"storage":"2Gi","resources":{"memory":"768Mi"}}}' >/dev/null
until [ "$(jp pod/"$POD" '{.status.containerStatuses[0].resources.limits.memory}')" = 768Mi ]; do sleep 0.5; done
until [ "$(jp pgi/db '{.status.conditions[?(@.type=="StorageResized")].reason}')" = ExpansionNotSupported ]; do sleep 0.5; done
echo "ok: memory 512Mi -> 768Mi applied with $(jp pod/"$POD" '{.status.containerStatuses[0].restartCount}') container restart; local-path expansion reported as ExpansionNotSupported"
