package controller

import (
	"context"
	"fmt"
	"hash/fnv"
	"maps"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgv1 "github.com/adarsh-sgh/pgfleet/api/v1alpha1"
)

const (
	LabelManagedBy = "app.kubernetes.io/managed-by"
	managedBy      = "pgfleet"
)

var claims = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "pgfleet_instance_pod_source_total",
	Help: "How instances got their pod: warm claim, cold create, or why a claim was skipped.",
}, []string{"result"})

func init() { metrics.Registry.MustRegister(claims) }

// Options configures the controllers.
type Options struct {
	// Shards splits PostgresInstance reconciles over this many controllers, each with its own
	// work queue, rate limiter and workers; an instance always hashes to the same shard.
	Shards          int
	WorkersPerShard int
	PoolWorkers     int
	Creds           CredentialSetter
	ClusterIP       bool // ClusterIP instead of headless Services
	Step            func(ctx context.Context, step string) error
}

// CacheOptions limits the Pod/PVC/Service informers to objects pgfleet created, so memory scales
// with the fleet, not the cluster.
func CacheOptions() cache.Options {
	sel := labels.SelectorFromSet(labels.Set{LabelManagedBy: managedBy})
	return cache.Options{ByObject: map[client.Object]cache.ByObject{
		&corev1.Pod{}:                   {Label: sel},
		&corev1.PersistentVolumeClaim{}: {Label: sel},
		&corev1.Service{}:               {Label: sel},
	}}
}

// Cache indexes. Label-selector Lists against the cache scan and deep-copy every object in the
// namespace; at 2k pods per namespace that was a third of the operator's CPU.
const (
	idxInstance = "pgfleet.instance"  // pods and PVCs by instance
	idxWarmPool = "pgfleet.warm-pool" // unclaimed pods by pool
)

func byLabel(key string) client.IndexerFunc {
	return func(o client.Object) []string {
		if v := o.GetLabels()[key]; v != "" {
			return []string{v}
		}
		return nil
	}
}

func Setup(mgr ctrl.Manager, o Options) error {
	o.Shards, o.WorkersPerShard, o.PoolWorkers = max(o.Shards, 1), max(o.WorkersPerShard, 1), max(o.PoolWorkers, 1)
	ctx, idx := context.Background(), mgr.GetFieldIndexer()
	warm := func(o client.Object) []string {
		if o.GetLabels()[LabelState] != StateWarm {
			return nil
		}
		return byLabel(LabelPool)(o)
	}
	for _, err := range []error{
		idx.IndexField(ctx, &corev1.Pod{}, idxInstance, byLabel(LabelInstance)),
		idx.IndexField(ctx, &corev1.PersistentVolumeClaim{}, idxInstance, byLabel(LabelInstance)),
		idx.IndexField(ctx, &corev1.Pod{}, idxWarmPool, warm),
	} {
		if err != nil {
			return err
		}
	}
	expect := &poolExpectations{}
	for i := range o.Shards {
		owns := func(ns, name string) bool { return ShardOf(ns, name, o.Shards) == i }
		child := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			name := obj.GetLabels()[LabelInstance]
			if name == "" || !owns(obj.GetNamespace(), name) {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}}}
		})
		r := &InstanceReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Creds: o.Creds, Step: o.Step,
			ClusterIPServices: o.ClusterIP, PoolExpect: expect}
		err := ctrl.NewControllerManagedBy(mgr).
			Named(fmt.Sprintf("postgresinstance-%d", i)).
			For(&pgv1.PostgresInstance{}, builder.WithPredicates(
				predicate.NewPredicateFuncs(func(obj client.Object) bool { return owns(obj.GetNamespace(), obj.GetName()) }),
				// Our own status patches would otherwise requeue every instance once more.
				predicate.GenerationChangedPredicate{},
			)).
			Watches(&corev1.Pod{}, child).
			Watches(&corev1.PersistentVolumeClaim{}, child).
			Watches(&corev1.Service{}, child).
			WithOptions(controller.Options{MaxConcurrentReconciles: o.WorkersPerShard}).
			Complete(r)
		if err != nil {
			return err
		}
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("warmpool").
		For(&pgv1.WarmPool{}).
		// Owner handler enqueues the old owner too, so a claim (owner moves to the instance) refills.
		Owns(&corev1.Pod{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: o.PoolWorkers}).
		Complete(&WarmPoolReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Expect: expect})
}

// ShardOf maps an instance to exactly one shard, which keeps the work queue's guarantee that one
// key is never reconciled concurrently.
func ShardOf(ns, name string, shards int) int {
	h := fnv.New32a()
	h.Write([]byte(ns + "/" + name))
	return int(h.Sum32() % uint32(shards))
}

func withManaged(l map[string]string) map[string]string {
	out := maps.Clone(l)
	if out == nil {
		out = map[string]string{}
	}
	out[LabelManagedBy] = managedBy
	return out
}
