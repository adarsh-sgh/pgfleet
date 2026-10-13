CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.22.0
SETUP_ENVTEST  ?= go run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.25
ENVTEST_K8S    ?= 1.37.0
IMG            ?= pgfleet:dev

.PHONY: generate test run build image bench-kind loadtest-kwok

generate: ## deepcopy, CRDs, RBAC from kubebuilder markers
	$(CONTROLLER_GEN) object paths=./api/...
	$(CONTROLLER_GEN) crd rbac:roleName=pgfleet paths=./... output:crd:dir=config/crd output:rbac:dir=config/rbac

test: ## envtest e2e (real kube-apiserver + etcd, no kubelet)
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S) -p path)" go test -race -count=1 ./...

build:
	go build -o bin/manager ./cmd/manager

run: ## against the current kubeconfig
	kubectl apply -f config/crd
	go run ./cmd/manager

image:
	docker build -t $(IMG) .

bench-kind: ## warm vs cold ready time on a real kind cluster
	./hack/bench-kind.sh

loadtest-kwok: ## 20k simulated pods on a kwok cluster
	./hack/loadtest-kwok.sh
