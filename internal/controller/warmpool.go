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
	Expect    *poolExpectations
	secrets   sync.Map // pool UID -> bootstrap secret known to exist
	statusAt  sync.Map // pool UID -> last status write
}

// poolExpectations tracks pods a pool created that the cache has not shown yet, the trick the
// ReplicaSet controller uses to avoid double-creating on cache lag.
type poolExpectations struct {
	mu sync.Mutex
	m  map[types.NamespacedName]map[string]time.Time
}

func (e *poolExpectations) expect(pool types.NamespacedName, pod string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.m == nil {
		e.m = map[types.NamespacedName]map[string]time.Time{}
	}
	if e.m[pool] == nil {
		e.m[pool] = map[string]time.Time{}
	}
	e.m[pool][pod] = time.Now()
}

func (e *poolExpectations) observe(pool types.NamespacedName, pod string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.m[pool], pod)
}

// pending drops expectations the cache has caught up with (or that are too old to trust) and
// counts the rest.
func (e *poolExpectations) pending(pool types.NamespacedName, seen map[string]bool) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	for name, at := range e.m[pool] {
		if seen[name] || time.Since(at) > 30*time.Second {
			delete(e.m[pool], name)
		}
	}
	return len(e.m[pool])
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
	// Read-only: skip the per-pod deep copy, which was most of this reconcile's CPU.
	if err := r.List(ctx, pods, client.InNamespace(pool.Namespace), client.MatchingFields{idxWarmPool: pool.Name}, client.UnsafeDisableDeepCopy); err != nil {
		return ctrl.Result{}, err
	}
	seen := map[string]bool{}
	var ready, warming []*corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		seen[p.Name] = true
		switch {
		case p.DeletionTimestamp != nil:
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
	inflight := r.Expect.pending(req.NamespacedName, seen)
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
	// Counts change on every claim and every pod that turns ready; write them at most once a
	// second per pool (they were ~2.7 writes per instance created, all to the same 10 objects).
	if last, ok := r.statusAt.Load(pool.UID); ok && st.ObservedGeneration == pool.Status.ObservedGeneration {
		if wait := time.Second - time.Since(last.(time.Time)); wait > 0 {
			if res.RequeueAfter == 0 || wait < res.RequeueAfter {
				res.RequeueAfter = wait
			}
			return res, nil
		}
	}
	r.statusAt.Store(pool.UID, time.Now())
	patch := client.MergeFromWithOptions(pool.DeepCopy(), client.MergeFromWithOptimisticLock{})
	pool.Status = st
	if err := r.Status().Patch(ctx, pool, patch); apierrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		return res, client.IgnoreNotFound(err)
	}
	return res, nil
}

func (r *WarmPoolReconciler) createPods(ctx context.Context, pool *pgv1.WarmPool, t template, hash, secret string, n int) error {
	errs := make(chan error, n)
	sem := make(chan struct{}, 10)
	var wg sync.WaitGroup
	for range n {
		name := pool.Name + "-" + utilrand.String(6)
		r.Expect.expect(client.ObjectKeyFromObject(pool), name)
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
				r.Expect.observe(client.ObjectKeyFromObject(pool), name)
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

// ensureSecret checks the apiserver once per pool UID per process; pools reconcile on every claim.
func (r *WarmPoolReconciler) ensureSecret(ctx context.Context, pool *pgv1.WarmPool) (string, error) {
	name := pool.Name + "-bootstrap"
	if _, ok := r.secrets.Load(pool.UID); ok {
		return name, nil
	}
	err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: name}, &corev1.Secret{})
	if apierrors.IsNotFound(err) {
		err = client.IgnoreAlreadyExists(r.Create(ctx, buildSecret(name, pool.Namespace, map[string]string{LabelPool: pool.Name}, ownerRef(pool, "WarmPool"))))
	}
	if err == nil {
		r.secrets.Store(pool.UID, true)
	}
	return name, err
}
