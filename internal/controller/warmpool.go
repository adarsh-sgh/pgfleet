package controller

import (
	"context"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgv1 "github.com/adarsh-sgh/pgfleet/api/v1alpha1"
)

// createBatch bounds the pods one pool reconcile creates, so a cold pool of thousands fills in
// short reconciles instead of one long one.
const createBatch = 50

// WarmPoolReconciler keeps spec.size unclaimed, current-template pods per pool. Claimed pods
// leave the pool by relabeling, which triggers a refill here.
type WarmPoolReconciler struct {
	client.Client
	APIReader client.Reader

	// created tracks pods this process created that the cache has not shown yet, the same
	// "expectations" trick the ReplicaSet controller uses to avoid double-creating on cache lag.
	mu      sync.Mutex
	created map[types.NamespacedName]map[string]time.Time
}

func (r *WarmPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pool := &pgv1.WarmPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !pool.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil // warm pods are owned by the pool; GC removes them
	}
	secret, err := r.ensureSecret(ctx, pool)
	if err != nil {
		return ctrl.Result{}, err
	}
	tmpl := poolTemplate(pool)
	hash := tmpl.hash()

	pods := &corev1.PodList{}
	// Claimed pods keep the pool label; listing them too lets expectations clear for a pod that
	// was claimed before the cache ever showed it warm.
	if err := r.List(ctx, pods, client.InNamespace(pool.Namespace), client.MatchingLabels{LabelPool: pool.Name}); err != nil {
		return ctrl.Result{}, err
	}
	seen := map[string]bool{}
	var ready, warming []*corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		seen[p.Name] = true
		switch {
		case p.Labels[LabelState] != StateWarm, p.DeletionTimestamp != nil:
		case p.Labels[LabelTemplate] != hash:
			// Spec changed: stale warm pods are replaced, claimed ones are left alone.
			if err := r.Delete(ctx, p); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
		default:
			if err := r.ensurePVC(ctx, pool, tmpl, p); err != nil {
				return ctrl.Result{}, err
			}
			if podReady(p) {
				ready = append(ready, p)
			} else {
				warming = append(warming, p)
			}
		}
	}
	inflight := r.inflight(req.NamespacedName, seen)
	have := len(ready) + len(warming) + inflight
	want := int(pool.Spec.Size)

	var res ctrl.Result
	switch {
	case have < want:
		n := min(want-have, createBatch)
		if n < want-have {
			res.RequeueAfter = 10 * time.Millisecond
		}
		if err := r.createPods(ctx, pool, tmpl, hash, secret, n); err != nil {
			return ctrl.Result{}, err
		}
	case have > want:
		// Shrink: drop booting pods before ready ones.
		extra := append(warming, ready...)[:min(have-want, len(warming)+len(ready))]
		for _, p := range extra {
			if err := r.Delete(ctx, p); client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
		}
	}

	st := pgv1.WarmPoolStatus{Ready: int32(len(ready)), Warming: int32(len(warming) + inflight), ObservedGeneration: pool.Generation}
	if st == pool.Status {
		return res, nil
	}
	patch := client.MergeFrom(pool.DeepCopy())
	pool.Status = st
	return res, client.IgnoreNotFound(r.Status().Patch(ctx, pool, patch))
}

func (r *WarmPoolReconciler) createPods(ctx context.Context, pool *pgv1.WarmPool, t template, hash, secret string, n int) error {
	errs := make(chan error, n)
	sem := make(chan struct{}, 10)
	var wg sync.WaitGroup
	for range n {
		name := pool.Name + "-" + utilrand.String(6)
		r.expect(client.ObjectKeyFromObject(pool), name)
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			pvc := ""
			if t.Storage != nil {
				pvc = name + "-data"
			}
			labels := map[string]string{LabelPool: pool.Name, LabelState: StateWarm, LabelTemplate: hash}
			// Pod first: its PVC is created next (owned by the pod) and re-ensured every
			// reconcile, so a crash in between leaves a pending pod, not an orphaned volume.
			pod := buildPod(name, pool.Namespace, t, pvc, secret, labels, ownerRef(pool, "WarmPool"))
			if err := r.Create(ctx, pod); err != nil {
				r.forget(client.ObjectKeyFromObject(pool), name)
				errs <- err
				return
			}
			errs <- r.ensurePVC(ctx, pool, t, pod)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *WarmPoolReconciler) ensurePVC(ctx context.Context, pool *pgv1.WarmPool, t template, pod *corev1.Pod) error {
	name := pvcOf(pod)
	if name == "" {
		return nil
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: name}, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		return err
	}
	owner := metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID, Controller: ptr(true), BlockOwnerDeletion: ptr(true)}
	err := r.Create(ctx, buildPVC(name, pod.Namespace, t, map[string]string{LabelPool: pool.Name}, owner))
	return client.IgnoreAlreadyExists(err)
}

func (r *WarmPoolReconciler) ensureSecret(ctx context.Context, pool *pgv1.WarmPool) (string, error) {
	name := pool.Name + "-bootstrap"
	err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: name}, &corev1.Secret{})
	if apierrors.IsNotFound(err) {
		err = client.IgnoreAlreadyExists(r.Create(ctx, buildSecret(name, pool.Namespace, map[string]string{LabelPool: pool.Name}, ownerRef(pool, "WarmPool"))))
	}
	return name, err
}

func (r *WarmPoolReconciler) expect(pool types.NamespacedName, pod string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.created == nil {
		r.created = map[types.NamespacedName]map[string]time.Time{}
	}
	if r.created[pool] == nil {
		r.created[pool] = map[string]time.Time{}
	}
	r.created[pool][pod] = time.Now()
}

func (r *WarmPoolReconciler) forget(pool types.NamespacedName, pod string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.created[pool], pod)
}

// inflight drops expectations the cache has caught up with (or that are too old to trust) and
// counts the rest.
func (r *WarmPoolReconciler) inflight(pool types.NamespacedName, seen map[string]bool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, at := range r.created[pool] {
		if seen[name] || time.Since(at) > 30*time.Second {
			delete(r.created[pool], name)
		}
	}
	return len(r.created[pool])
}
