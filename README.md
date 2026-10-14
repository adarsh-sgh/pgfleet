# pgfleet

A Kubernetes operator (Go, controller-runtime) that runs a fleet of single-node Postgres
servers. A `PostgresInstance` is created, resized in place (CPU/memory via the pod resize
subresource, storage via PVC expansion) and deleted with an ordered, policy-aware teardown. A
`WarmPool` keeps pods booted, initdb'd and passing readiness; an instance that names a pool
claims one of them, gets its own password and is Ready in a fraction of a second instead of
waiting for volume provisioning, container start and initdb.

```
                  ┌──────────── pgfleet manager ───────────────────────────────────────┐
 PostgresInstance │ instance controller: 1 queue x 32 workers (or N fnv-hashed shards)  │
 ──watch──────────┤   claim warm pod (CAS) or create PVC+pod │ Service │ password         │
 WarmPool ────────┤ warmpool controller: refill to size, roll on template change        │
                  │ informers: Pods/PVCs/Services with managed-by=pgfleet, indexed by   │
                  │ instance and by pool                                                │
                  └───────────────┬─────────────────────────────────────────────────────┘
                                  │ pods/resize, PVC patch, pods/exec (psql over stdin)
                                  ▼
   warm pod {pool=small, state=warm} ──claim──▶ {claimed-from=small, state=claimed,
                                                  instance=app-db}, owner = the instance
```

```yaml
apiVersion: pgfleet.dev/v1alpha1
kind: WarmPool
metadata: {name: small}
spec: {size: 2, version: "17", resources: {cpu: 500m, memory: 512Mi}, storage: 1Gi}
---
apiVersion: pgfleet.dev/v1alpha1
kind: PostgresInstance
metadata: {name: app-db}
spec:
  poolRef: small                        # claim a warm pod if one is ready, else create cold
  resources: {cpu: "1", memory: 512Mi}  # differs from the pool: resized in place after the claim
  storage: 5Gi                          # larger than the pool volume: expanded after the claim
  deletionPolicy: Retain                # keep the volume and password when the instance goes
```

Status carries `Ready`, `ComputeResized`, `StorageResized` and `CredentialsRotated` conditions
(each with `observedGeneration`), plus the pod, PVC, Secret and `host:port` endpoint.

## Design decisions

- **One pod per instance, no StatefulSet.** A warm claim adopts an existing pod, which a
  StatefulSet cannot do. The operator owns the pod directly and recreates it on the same volume
  if it is evicted.
- **A claim is a compare-and-swap on the pod.** The claim patch relabels the pod
  (`state=claimed`, `instance=<name>`, `claimed-from=<pool>`) and moves its owner reference, carrying the cached
  `resourceVersion`; of two instances racing for one pod, one gets a 409. Candidates are tried in
  random order so concurrent claimers spread out, and a 409 caused by a stale cache (say a
  kubelet status update) is retried once against a fresh read.
- **State is discovered from the cluster, not from status.** Each step is idempotent and finds
  earlier work by label or deterministic name, so a crash between any two side effects converges
  on restart. The one dangerous read is "this instance has no pod yet": the informer can lag a
  claim this process made a moment ago. Binds are remembered in memory and confirmed with an
  uncached GET by name; after a restart the map is empty but the cache was filled by a fresh
  LIST that already contains every earlier claim. (The first version did an uncached LIST here;
  in the 19k-instance load test it was the slowest call the operator made.)
- **Claimed pods get their own password.** Warm pods are initdb'd with the pool's bootstrap
  secret. The claim creates `<instance>-credentials` and runs `ALTER ROLE` through `pods/exec`
  with the SQL on stdin, so the password is never in a pod spec, exec URL or audit log. The
  instance is not Ready until that succeeded; `hack/smoke-kind.sh` checks that the bootstrap
  password is rejected afterwards.
- **Resize in place.** Containers run Guaranteed QoS (requests = limits); an in-place resize may
  not change the QoS class, so both always move together. CPU uses `RestartPolicy: NotRequired`.
  Memory uses `RestartContainer`: shrinking memory under a running Postgres risks an OOM kill
  mid-query, so the container restarts on the new limit. `ComputeResized` follows the
  kubelet's `PodResizePending/InProgress` conditions and `containerStatuses[].resources`. Storage
  is grow-only; the operator checks `allowVolumeExpansion` and reports `ExpansionNotSupported` or
  `ShrinkNotSupported` instead of failing silently.
- **The finalizer exists for ordering and Retain.** Teardown deletes the pod first (nothing
  writes to the volume), then, per `deletionPolicy`, deletes or orphans the PVC and Secret. With
  Retain the owner references are removed before the finalizer is dropped, otherwise owner-ref GC
  would delete the data anyway. Recreating an instance with the same name re-adopts the retained
  volume and password.
- **Warm pool refill uses expectations.** Pods the pool created but the cache has not shown yet
  are counted as warming (the ReplicaSet controller's trick), so a refill never double-creates;
  a claim settles the expectation directly. A warm pod's PVC is owned by the pod and re-ensured
  every reconcile, so a crash between the two creates leaves a pending pod, not an orphaned
  volume; the claim moves PVC ownership to the instance. Template changes (image, size) replace
  unclaimed pods and leave claimed ones alone. Status counts are written at most once a second
  per pool and the pool ignores its own status writes; before that, pool status was the largest
  source of writes in the load test.
- **Optional sharded work queues.** `-shards N` registers N instance controllers; each filters
  events to `fnv(namespace/name) % N` and has its own queue, rate limiter and workers, while all
  share one set of informers. An instance always lands on one shard, which keeps the queue's
  guarantee that a key is never reconciled concurrently. The load test found no consistent gain
  over one queue with the same number of workers, so the default is one queue.
- **Cache reads go through field indexes.** Pods and PVCs are indexed by instance, unclaimed
  pods by pool, and a claim swaps the pool label for `claimed-from` so a pool's index only ever
  holds its warm pods. Read-only lists skip the deep copy.
- **Headless Services.** Each instance gets a headless Service: DNS resolves straight to the
  pod, and a 20k-instance fleet does not put 20k virtual IPs and their kube-proxy rules on every
  node. `-cluster-ip-services` switches to ClusterIP. Run 5 of the load test shows ClusterIP
  creates are cheap on the apiserver side, so the choice rests on the kube-proxy cost.
- **The cache only holds what pgfleet created.** Pod/PVC/Service informers are filtered to
  `app.kubernetes.io/managed-by=pgfleet`; Secrets are read uncached on the rare paths that need
  them, so the operator never holds a cluster-wide Secret informer.
- **Status patches use optimistic locking.** A merge patch diffed against a stale cached status
  silently omits fields that only look unchanged; the lifecycle test caught `phase` and
  `conditions` disagreeing because of it.

## Tests

`make test` runs envtest (real kube-apiserver 1.37 + etcd, no kubelet/scheduler/GC). A fake
kubelet goroutine marks pods Running/Ready and binds PVCs at their requested size. Three broad
tests:

- `TestInstanceLifecycle`: cold create (PVC, pod, headless Service, Secret, status), CPU resize
  through `pods/resize`, storage expansion, shrink rejected, delete, Retain orphaning the volume,
  recreate re-adopting it.
- `TestWarmPoolClaims`: six concurrent claims against three warm pods. Every pod has exactly one
  owner, at least three claims are warm, claimed pods are resized and expanded and their password
  rotated, the pool refills, and a pool template change rolls only unclaimed pods.
- `TestSurvivesRestartMidReconcile`: for each of 11 side effects (finalizer added, PVC created,
  pod created, Service created, pod claimed, PVC adopted, password rotated, pod resized, volume
  expanded, pods deleting, children released) the manager is stopped while the reconcile is
  blocked right after that step, a fresh manager is started, and the test checks the instance
  converges with exactly one pod and one PVC, no second claim, and a full teardown for the
  deletion cases.

CI (`.github/workflows/ci.yaml`) checks generated code is current, vets, and runs the envtest
suite with `-race`.

## Benchmarks

Hardware: Apple M5 laptop (10 cores, 16 GB), macOS 26.3. One run per row; tables are generated
from the JSON in `results/` by `hack/results-table.py`.

### Warm claim vs cold create on kind (`make bench-kind`)

kind v0.32 (Kubernetes v1.36.1) in Docker Desktop; real kubelet, containerd and the local-path
provisioner; operator out of cluster. `cmd/bench` times each trial client-side, from just before
`Create` to the watch event showing `Ready=True`, then deletes the instance and waits for it to
go. Instances are 500m/512Mi with a 1Gi volume; warm trials start with a full pool.

| mode | trials | p50 | max |
|---|--:|--:|--:|
| cold, image on node | 10 | 4.97s | 6.94s |
| warm claim | 10 | 0.06s | 0.08s |
| cold, image pulled from Docker Hub | 5 | 102.11s | 119.26s |

Cold time is volume provisioning, container start, initdb and the first passing `pg_isready`;
the registry pull, when the node does not have the image, dominates everything else and depends
on the network. The warm path is a claim patch, a Service create, the `ALTER ROLE` exec and a
status write.

### Fleet load test on kwok (`make loadtest-kwok`)

kwok v0.8.0 simulates the kubelets; etcd, kube-apiserver, kube-scheduler and
kube-controller-manager (v1.36.1) run for real as local processes (`RUNTIME=binary`, no Docker).
`hack/build-k8s-binaries.sh` builds the darwin control-plane binaries from the Go module proxy,
because kwokctl's GitHub release downloads ran at ~30KB/s here. `cmd/loadtest` runs the operator
as a child process and drives four phases:

1. fill 10 warm pools of 100 (ephemeral storage: kwok has no volume provisioner),
2. create 19,000 instances at a fixed rate across 10 namespaces, all pointing at their pool,
   until every one is Ready: 20,000 pods,
3. `kill -9` the operator and start a new one, which re-lists and reconciles the whole fleet,
4. delete all 19,000 instances through the finalizer.

Per phase it diffs the operator's Prometheus histograms
(`controller_runtime_reconcile_time_seconds` and `workqueue_queue_duration_seconds`, summed over
the instance shards) and interpolates quantiles the way `histogram_quantile` does. The queue-wait
histogram has power-of-ten buckets, so its p99 is reported as the bucket bound ("≤ 0.1s").
Create-to-Ready is measured client-side from a watch. Credential rotation is off (kwok has no
exec) and in-place resize is not exercised (the apiserver only allows resizing pods whose
kubelet reports `containerStatuses[].resources`, which kwok does not); both are covered on kind.

Create phase (19,000 instances on top of 1,000 warm pods; `rN` is creates per second, `shardsS-wW`
is S queues of W workers):

| # | config | pods | phase | wall | reconciles | reconcile p50 | reconcile p99 | queue wait p99 | create→Ready p50 / p99 |
|--:|---|--:|---|--:|--:|--:|--:|--:|--:|
| 1 | shards4-w8-r50 | 20000 | create | 386s | 47343 | 3ms | 546ms | ≤ 1s | 6ms / 1.61s |
| 2 | shards4-w8-r100 | 20000 | create | 221s | 44752 | 3ms | 1.32s | ≤ 100s | 6ms / 29.68s |
| 3 | shards4-w8-r200 | 20000 | create | 187s | 45488 | 4ms | 1.59s | ≤ 100s | 12ms / 85.83s |
| 4 | shards1-w32-r100 | 20000 | create | 203s | 46441 | 3ms | 551ms | ≤ 10s | 5ms / 8.63s |
| 5 | shards4-w8-r100-clusterip-4k | 5000 | create | 43s | 9898 | 3ms | 16ms | ≤ 0.01s | 5ms / 18ms |
| 6 | shards1-w32-r50 | 20000 | create | 402s | 47515 | 4ms | 1.24s | ≤ 10s | 6ms / 29.44s |
| 7 | shards4-w8-r100-rep2 | 20000 | create | 306s | 44912 | 4ms | 2.21s | ≤ 100s | 8ms / 117.00s |
| 8 | shards1-w32-r100-rep2 | 20000 | create | 321s | 42776 | 4ms | 2.23s | ≤ 1000s | 274ms / 114.59s |

Restart and teardown phases of the same runs:

| # | config | pods | phase | wall | reconciles | reconcile p50 | reconcile p99 | queue wait p99 | create→Ready p50 / p99 |
|--:|---|--:|---|--:|--:|--:|--:|--:|--:|
| 1 | shards4-w8-r50 | 20000 | restart-resync | 3s | 19000 | 2ms | 5ms | ≤ 10s |  |
| 1 | shards4-w8-r50 | 20000 | delete | 383s | 107998 | 3ms | 408ms | ≤ 1s |  |
| 2 | shards4-w8-r100 | 20000 | restart-resync | 3s | 19000 | 2ms | 5ms | ≤ 10s |  |
| 2 | shards4-w8-r100 | 20000 | delete | 318s | 96251 | 5ms | 1.15s | ≤ 10s |  |
| 3 | shards4-w8-r200 | 20000 | restart-resync | 6s | 19000 | 2ms | 5ms | ≤ 10s |  |
| 3 | shards4-w8-r200 | 20000 | delete | 332s | 103757 | 4ms | 1.11s | ≤ 100s |  |
| 4 | shards1-w32-r100 | 20000 | restart-resync | 3s | 19000 | 2ms | 5ms | ≤ 10s |  |
| 4 | shards1-w32-r100 | 20000 | delete | 303s | 95578 | 6ms | 1.11s | ≤ 10s |  |
| 5 | shards4-w8-r100-clusterip-4k | 5000 | restart-resync | 3s | 4000 | 2ms | 5ms | ≤ 1s |  |
| 5 | shards4-w8-r100-clusterip-4k | 5000 | delete | 43s | 23105 | 2ms | 7ms | ≤ 0.01s |  |
| 6 | shards1-w32-r50 | 20000 | restart-resync | 3s | 19000 | 2ms | 5ms | ≤ 10s |  |
| 6 | shards1-w32-r50 | 20000 | delete | 389s | 104249 | 4ms | 937ms | ≤ 10s |  |
| 7 | shards4-w8-r100-rep2 | 20000 | restart-resync | 6s | 19000 | 2ms | 5ms | ≤ 10s |  |
| 7 | shards4-w8-r100-rep2 | 20000 | delete | 508s | 114727 | 4ms | 1.47s | ≤ 100s |  |
| 8 | shards1-w32-r100-rep2 | 20000 | restart-resync | 3s | 19000 | 2ms | 5ms | ≤ 10s |  |
| 8 | shards1-w32-r100-rep2 | 20000 | delete | 317s | 105881 | 5ms | 1.18s | ≤ 10s |  |

What the runs say:

- **20,000 pods, every one Running/Ready**, 17.6-19k of the 19k instances claimed warm. At 50
  creates/s the first run held reconcile p99 at 546ms, queue wait p99 under 1s and
  create-to-Ready p99 at 1.6s (p50 6ms). The same rate later in the session (run 6) gave 1.24s.
  At 100-200 creates/s the laptop's apiserver and etcd saturate: reconcile p99 is 0.55-2.2s and
  instances queue for tens of seconds. Rows 7 and 8 repeat row 2 and 4 an hour later on a
  hotter machine (lid closed) and are worse in both configurations.
- **Sharding did not measurably help.** At equal worker count (32), 4 queues beat 1 at 50/s and
  lost at 100/s, and the repeats moved more than either difference. The tail is set by the
  control plane's latency at 20k pods (the operator's apiserver calls: p50 1-2ms in most runs,
  p99 0.2-1.3s), not by queue contention, so the default is one queue of 32 workers and
  `-shards` stays as an option.
- **A kill -9 at 20k pods costs 3-6 seconds**: the new process lists the fleet into a cold
  cache and re-reconciles all 19,000 instances at p99 5ms; no instance needs a write.
- **Teardown is bounded by kwok**, which drains terminating pods at ~50/s; reconciles stay
  cache-only while the operator waits for pod delete events.
- **ClusterIP is not the problem on a native control plane**: run 5 (4,000 instances with
  ClusterIP Services) shows 1.7ms p50 Service creates. The headless default stands on the
  kube-proxy argument above, not on apiserver cost.

What moved the numbers (each step is in `results/history/`, same harness, r50 unless noted):

| change | reconcile p99 | create→Ready p99 |
|---|--:|--:|
| uncached pod LIST per new instance, 10s finalize poll (r200, `dcd05e1`) | 2.58s | 139s |
| bind expectations instead of the LIST (r200, `5a3d94b`) | 1.85s | 124s |
| label-selector cache lists, etcd fsync on (`94569e8`) | 2.33s | 157s |
| field indexes, claimed pods leave the pool label, no deep copy; etcd fsync off (`3e9263e`) | 1.23s | 8.0s |
| pool ignores its own status writes, 1 write/s per pool; finalize without uncached reads | 0.55s | 1.6s |

The index change is the one pprof pointed at: at ~5k pods the operator used 197% CPU, a third
of it `WarmPoolReconciler` deep-copying every pod that had ever come from its pool; after,
38% (`results/profiles/`, `go tool pprof -top bin/manager <file>`). The etcd change is a
property of the laptop, not the operator: Go's fsync on macOS is `F_FULLFSYNC`, and under this
write load etcd stalled long enough for requests to time out
(`results/history/loadtest-etcd-fsync-timeouts.txt`), so the kwok etcd runs with
`--unsafe-no-fsync` unless `ETCD_FSYNC=true`. The fourth row changes both at once.

## Run it

```sh
make test                     # envtest e2e (downloads apiserver/etcd via setup-envtest)
kind create cluster && make run   # CRDs + operator against the current kubeconfig
kubectl apply -f config/samples/instances.yaml
./hack/smoke-kind.sh          # password rotation, CPU/memory resize, expansion rejection on kind
make bench-kind               # warm vs cold table above
./hack/build-k8s-binaries.sh && make loadtest-kwok   # one load-test row
RUNTIME=binary ./hack/loadtest-matrix.sh             # every row (~90 min), then:
python3 hack/results-table.py                        # the benchmark tables, from results/*.json
```

`config/` holds the generated CRDs and RBAC and a Deployment (`--leader-elect`, two replicas).
Operator flags: `-shards`, `-workers-per-shard`, `-pool-workers`, `-kube-api-qps/-burst`,
`-rotate-credentials`, `-cluster-ip-services`, `-pprof-bind-address`. Metrics on `:8080`: controller-runtime's
reconcile/queue histograms, `pgfleet_instance_pod_source_total{result}` (warm, cold, conflict,
pool_empty, incompatible) and `pgfleet_api_request_seconds{verb,resource}`.

## Pulumi (EKS)

`deploy/pulumi-eks` is a TypeScript Pulumi program: VPC (3 AZs, one NAT), EKS with a managed node
group in private subnets, the EBS CSI driver add-on with an IRSA role, an encrypted gp3 default
StorageClass with `allowVolumeExpansion` (what storage resize needs), then the CRDs, RBAC and
operator Deployment from `config/`. It type-checks (`npm install && npm run typecheck`) and has
never been deployed.

## Gaps

- **Not deployed anywhere real.** The Pulumi program has never run against AWS, and no image is
  published (the Deployment references `ghcr.io/adarsh-sgh/pgfleet`). All numbers are from one
  laptop.
- **The load test is a simulation on one laptop.** kwok pods have no containers, volumes or
  exec, so the 20k-pod runs measure control-plane work only (claims, creates, status,
  teardown), with the operator, apiserver, etcd and kwok sharing 10 cores, etcd without fsync,
  and one run per configuration. p99 moved by 2x between identical configurations, so the
  sub-second figure holds at 50 creates/s in one of two runs, not as a guarantee; above that
  rate this control plane saturates.
- **Resize and password rotation are not load-tested**, only verified on kind
  (`hack/smoke-kind.sh`) and in envtest.
- **No HA story inside an instance.** One pod, no replicas, no backups, WAL archiving, PITR or
  connection pooling; a node loss means the pod is recreated on the same RWO volume once it can
  attach.
- **Sharding is in-process.** Shards split work queues inside one manager; there is no
  cross-replica sharding (each replica would still cache every pod). Leader election makes the
  second replica a standby only.
- **Memory resize restarts Postgres** (by design, see above), and `shared_buffers` and friends
  are not retuned for the new limit.
- **Warm pods share one bootstrap password until claimed**, so anyone who can read the pool
  Secret can reach unclaimed pods.
- Major version upgrades are rejected (`version` is immutable), not performed.
