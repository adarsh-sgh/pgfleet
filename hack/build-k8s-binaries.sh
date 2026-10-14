#!/usr/bin/env bash
# Builds darwin kube-apiserver/scheduler/controller-manager and kwok (etcd comes from envtest) from the Go module
# proxy into bin/k8s for `RUNTIME=binary ./hack/loadtest-kwok.sh`. kwokctl would otherwise
# download them from GitHub releases, which ran at ~30KB/s here.
set -euo pipefail
cd "$(dirname "$0")/.."
K8S=${K8S:-v1.36.1}
OUT=$PWD/bin/k8s
WORK=$(mktemp -d)
mkdir -p "$OUT"

GOFLAGS=-mod=mod go mod download -json "k8s.io/kubernetes@$K8S" >/dev/null
SRC=$(go env GOMODCACHE)/k8s.io/kubernetes@$K8S
cd "$WORK"
# k8s.io/kubernetes replaces its staging modules with local paths that are not in the module
# zip; point them at the published v0.x tags instead.
{
  echo "module k8sbuild"; echo "go 1.26"; echo "require k8s.io/kubernetes $K8S"; echo "replace ("
  grep -E '^\s+k8s.io/[a-z-]+ => ./staging' "$SRC/go.mod" | awk -v v="v0.${K8S#v1.}" '{print "\t" $1 " => " $1 " " v}'
  echo ")"
} >go.mod
printf '//go:build tools\npackage tools\nimport (\n_ "k8s.io/kubernetes/cmd/kube-apiserver"\n_ "k8s.io/kubernetes/cmd/kube-controller-manager"\n_ "k8s.io/kubernetes/cmd/kube-scheduler"\n)\n' >tools.go
GOFLAGS=-mod=mod go mod tidy
GOFLAGS=-mod=mod go build -o "$OUT/" k8s.io/kubernetes/cmd/kube-apiserver k8s.io/kubernetes/cmd/kube-controller-manager k8s.io/kubernetes/cmd/kube-scheduler
# etcd's go.mod has replace directives, so it cannot be go-installed; envtest ships one.
cp "$(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.25 use 1.37.0 -p path)/etcd" "$OUT/etcd"
GOBIN=$OUT go install sigs.k8s.io/kwok/cmd/kwok@v0.8.0
ls -la "$OUT"
