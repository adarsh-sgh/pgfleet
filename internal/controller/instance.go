package controller

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pgv1 "github.com/adarsh-sgh/pgfleet/api/v1alpha1"
)

// InstanceReconciler drives a PostgresInstance to a single running pod (claimed warm or created
// cold), its data volume, Service and credentials. Every step is idempotent and discovers prior
// work from the cluster, not from status, so a crash between any two steps converges on restart.
type InstanceReconciler struct {
	client.Client
	// APIReader bypasses the cache where a stale read could create a duplicate pod.
	APIReader client.Reader
	// Creds rotates the password of claimed warm pods. Nil disables rotation (kwok has no exec).
	Creds CredentialSetter
	// Step, if set, runs after each side effect; tests use it to kill the reconciler mid-flight.
	Step func(ctx context.Context, step string) error
	// ClusterIPServices gives each instance a virtual IP instead of a headless Service.
	ClusterIPServices bool

	// PoolExpect is shared with the pool controller: a claim settles the pool's expectation for
	// a pod it created but never saw warm.
	PoolExpect *poolExpectations

	bound sync.Map // instance key -> pod name claimed/created by this process
}

// +kubebuilder:rbac:groups=pgfleet.dev,resources=postgresinstances;warmpools,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=pgfleet.dev,resources=postgresinstances/status;warmpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgfleet.dev,resources=postgresinstances/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods;persistentvolumeclaims;services;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/resize,verbs=patch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create;get
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *InstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	in := &pgv1.PostgresInstance{}
	if err := r.Get(ctx, req.NamespacedName, in); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !in.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, in)
	}
	if !controllerutil.ContainsFinalizer(in, Finalizer) {
		patch := client.MergeFrom(in.DeepCopy())
		controllerutil.AddFinalizer(in, Finalizer)
		if err := r.Patch(ctx, in, patch); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.step(ctx, "finalizer"); err != nil {
			return ctrl.Result{}, err
		}
	}

	orig := in.DeepCopy()
	res, err := r.converge(ctx, in)
	in.Status.ObservedGeneration = in.Generation
	in.Status.Phase = phase(in)
	if equality.Semantic.DeepEqual(orig.Status, in.Status) {
		return res, err
	}
	// Optimistic lock: a merge patch diffed against a stale cached status would silently skip
	// fields that only look unchanged, leaving e.g. phase and conditions out of sync.
	perr := r.Status().Patch(ctx, in, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
	switch {
	case apierrors.IsConflict(perr):
		return ctrl.Result{Requeue: true}, err // newer object is on its way through the cache
	case perr != nil && !apierrors.IsNotFound(perr):
		return ctrl.Result{}, errors.Join(err, perr)
	}
	return res, err
}

func (r *InstanceReconciler) converge(ctx context.Context, in *pgv1.PostgresInstance) (ctrl.Result, error) {
	st := &in.Status
	pod, err := r.findPod(ctx, in)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pod != nil && pod.DeletionTimestamp != nil {
		// Evicted or deleted out from under us; the replacement reuses the volume once it is gone.
		setCond(in, pgv1.CondReady, false, "PodTerminating", pod.Name)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	if pod == nil && st.Source == "" {
		if pod, err = r.claim(ctx, in); err != nil {
			return ctrl.Result{}, err
		}
	}
	if pod == nil {
		if pod, err = r.createCold(ctx, in); err != nil {
			return ctrl.Result{}, err
		}
	}
	if st.Source == "" {
		st.Source = pgv1.SourceCold
		if pod.Labels[LabelClaimedFrom] != "" {
			st.Source = pgv1.SourceWarm
		}
	}
	st.PodName = pod.Name

	if st.PVCName = pvcOf(pod); st.PVCName != "" {
		if err := r.adoptPVC(ctx, in, st.PVCName); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.ensureService(ctx, in); err != nil {
		return ctrl.Result{}, err
	}
	credsOK, err := r.ensureCredentials(ctx, in, pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	st.SecretName = podSecret(pod)
	if meta.IsStatusConditionTrue(st.Conditions, pgv1.CondCredentials) {
		st.SecretName = secretName(in)
	}
	if err := r.resizeCompute(ctx, in, pod); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.resizeStorage(ctx, in); err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case !podReady(pod):
		setCond(in, pgv1.CondReady, false, "PodNotReady", pod.Name)
	case !credsOK:
		setCond(in, pgv1.CondReady, false, "CredentialsPending", "")
	default:
		setCond(in, pgv1.CondReady, true, "Ready", "")
	}
	return ctrl.Result{}, nil
}

// findPod returns the pod bound to the instance. The cache can lag a claim this process made a
// moment ago, and trusting that miss would claim a second pod, so binds are remembered in
// r.bound and confirmed with an uncached GET by name. After a restart the map is empty, but the
// cache was filled by a fresh LIST that already contains every earlier claim.
func (r *InstanceReconciler) findPod(ctx context.Context, in *pgv1.PostgresInstance) (*corev1.Pod, error) {
	key := client.ObjectKeyFromObject(in)
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(in.Namespace), client.MatchingFields{idxInstance: in.Name}); err != nil {
		return nil, err
	}
	for i := range pods.Items {
		if p := &pods.Items[i]; metav1.IsControlledBy(p, in) {
			r.bound.Delete(key) // cache caught up
			return p, nil
		}
	}
	name, ok := r.bound.Load(key)
	if !ok {
		return nil, nil
	}
	pod := &corev1.Pod{}
	err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: name.(string)}, pod)
	switch {
	case apierrors.IsNotFound(err):
		r.bound.Delete(key)
		return nil, nil
	case err != nil:
		return nil, err
	case !metav1.IsControlledBy(pod, in):
		r.bound.Delete(key)
		return nil, nil
	}
	return pod, nil
}

// claim takes a ready warm pod from the instance's pool. The pod patch carries the cached
// resourceVersion, so two instances racing for one pod cannot both win.
func (r *InstanceReconciler) claim(ctx context.Context, in *pgv1.PostgresInstance) (*corev1.Pod, error) {
	if in.Spec.PoolRef == "" {
		return nil, nil
	}
	pool := &pgv1.WarmPool{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: in.Spec.PoolRef}, pool); err != nil {
		claims.WithLabelValues("pool_missing").Inc()
		return nil, client.IgnoreNotFound(err)
	}
	if reason := incompatible(pool, in); reason != "" {
		claims.WithLabelValues("incompatible").Inc()
		log.FromContext(ctx).Info("pool incompatible, creating cold", "pool", pool.Name, "reason", reason)
		return nil, nil
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(in.Namespace), client.MatchingFields{idxWarmPool: pool.Name}, client.UnsafeDisableDeepCopy); err != nil {
		return nil, err
	}
	hash := poolTemplate(pool).hash()
	var ready []*corev1.Pod
	for i := range pods.Items {
		if p := &pods.Items[i]; p.Labels[LabelTemplate] == hash && podReady(p) {
			ready = append(ready, p)
		}
	}
	// Random order spreads concurrent claimers over different pods instead of all CAS-ing the first.
	rand.Shuffle(len(ready), func(i, j int) { ready[i], ready[j] = ready[j], ready[i] })
	for _, cached := range ready[:min(len(ready), 5)] {
		p := cached.DeepCopy() // list skipped the copy; never mutate the cache's object
		ok, err := r.tryClaim(ctx, in, p)
		if err != nil {
			return nil, err
		}
		if ok {
			r.bound.Store(client.ObjectKeyFromObject(in), p.Name)
			r.PoolExpect.observe(types.NamespacedName{Namespace: in.Namespace, Name: pool.Name}, p.Name)
			claims.WithLabelValues("warm").Inc()
			return p, r.step(ctx, "claimed")
		}
		claims.WithLabelValues("conflict").Inc()
	}
	claims.WithLabelValues("pool_empty").Inc()
	return nil, nil
}

// tryClaim CASes one pod. A conflict from a stale cached copy (e.g. a kubelet status update)
// is retried once against a fresh read; a conflict because another instance won is not.
func (r *InstanceReconciler) tryClaim(ctx context.Context, in *pgv1.PostgresInstance, p *corev1.Pod) (bool, error) {
	for attempt := 0; ; attempt++ {
		patch := client.MergeFromWithOptions(p.DeepCopy(), client.MergeFromWithOptimisticLock{})
		p.Labels[LabelClaimedFrom] = p.Labels[LabelPool]
		delete(p.Labels, LabelPool)
		delete(p.Labels, LabelTemplate)
		p.Labels[LabelState] = StateClaimed
		p.Labels[LabelInstance] = in.Name
		p.OwnerReferences = []metav1.OwnerReference{ownerRef(in, "PostgresInstance")}
		err := r.Patch(ctx, p, patch)
		switch {
		case err == nil:
			return true, nil
		case apierrors.IsNotFound(err):
			return false, nil
		case !apierrors.IsConflict(err):
			return false, err
		case attempt > 0:
			return false, nil
		}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(p), p); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		if p.Labels[LabelState] != StateWarm || !podReady(p) {
			return false, nil
		}
	}
}

func incompatible(p *pgv1.WarmPool, in *pgv1.PostgresInstance) string {
	pt, it := poolTemplate(p), instanceTemplate(in)
	switch {
	case pt.Image != it.Image:
		return "image"
	case (pt.Storage == nil) != (it.Storage == nil):
		return "ephemeral vs persistent"
	case pt.Storage != nil && pt.Storage.Cmp(*it.Storage) > 0:
		return "pool volume larger than requested"
	case ptrVal(pt.StorageClassName) != ptrVal(it.StorageClassName):
		return "storage class"
	}
	return ""
}

func (r *InstanceReconciler) createCold(ctx context.Context, in *pgv1.PostgresInstance) (*corev1.Pod, error) {
	if _, err := r.ensureSecret(ctx, in); err != nil {
		return nil, err
	}
	owner := ownerRef(in, "PostgresInstance")
	labels := map[string]string{LabelInstance: in.Name}
	pvc := ""
	if in.Spec.Storage != nil {
		// Reuse a volume this instance already has (pod was evicted, or Retain + recreate).
		var err error
		if pvc, err = r.instancePVC(ctx, in); err != nil {
			return nil, err
		}
		if pvc == "" {
			pvc = in.Name + "-data"
			if err := r.Create(ctx, buildPVC(pvc, in.Namespace, instanceTemplate(in), labels, owner)); err != nil && !apierrors.IsAlreadyExists(err) {
				return nil, err
			}
			if err := r.step(ctx, "pvc"); err != nil {
				return nil, err
			}
		}
	}
	pod := buildPod(in.Name+"-pg", in.Namespace, instanceTemplate(in), pvc, secretName(in), labels, owner)
	if err := r.Create(ctx, pod); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
			return nil, err
		}
	}
	r.bound.Store(client.ObjectKeyFromObject(in), pod.Name)
	claims.WithLabelValues("cold").Inc()
	return pod, r.step(ctx, "pod")
}

func (r *InstanceReconciler) instancePVC(ctx context.Context, in *pgv1.PostgresInstance) (string, error) {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcs, client.InNamespace(in.Namespace), client.MatchingFields{idxInstance: in.Name}); err != nil {
		return "", err
	}
	for _, p := range pvcs.Items {
		if p.DeletionTimestamp == nil {
			return p.Name, nil
		}
	}
	return "", nil
}

// adoptPVC moves a volume's ownership to the instance: a claimed pool volume is still owned by
// its pod (whose deletion would garbage-collect the data), a retained one is owned by nobody.
func (r *InstanceReconciler) adoptPVC(ctx context.Context, in *pgv1.PostgresInstance, name string) error {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: name}, pvc); err != nil {
		return client.IgnoreNotFound(err)
	}
	if metav1.IsControlledBy(pvc, in) {
		return nil
	}
	patch := client.MergeFrom(pvc.DeepCopy())
	if pvc.Labels == nil {
		pvc.Labels = map[string]string{}
	}
	pvc.Labels[LabelInstance] = in.Name
	pvc.Labels[LabelState] = StateClaimed
	pvc.OwnerReferences = []metav1.OwnerReference{ownerRef(in, "PostgresInstance")}
	if err := r.Patch(ctx, pvc, patch); err != nil {
		return err
	}
	return r.step(ctx, "pvc-adopted")
}

func (r *InstanceReconciler) ensureService(ctx context.Context, in *pgv1.PostgresInstance) error {
	in.Status.Endpoint = fmt.Sprintf("%s.%s.svc:5432", in.Name, in.Namespace)
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: in.Name}, svc)
	if !apierrors.IsNotFound(err) {
		return err
	}
	if err := r.Create(ctx, buildService(in, ownerRef(in, "PostgresInstance"), r.ClusterIPServices)); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return r.step(ctx, "service")
}

func podSecret(p *corev1.Pod) string {
	if c := container(p); c != nil {
		for _, e := range c.Env {
			if e.Name == "POSTGRES_PASSWORD" && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
				return e.ValueFrom.SecretKeyRef.Name
			}
		}
	}
	return ""
}

func secretName(in *pgv1.PostgresInstance) string { return in.Name + "-credentials" }

// ensureSecret returns the instance's password, creating it once. Secrets are read uncached:
// the operator never needs a cluster-wide Secret informer.
func (r *InstanceReconciler) ensureSecret(ctx context.Context, in *pgv1.PostgresInstance) (string, error) {
	s := &corev1.Secret{}
	key := types.NamespacedName{Namespace: in.Namespace, Name: secretName(in)}
	err := r.APIReader.Get(ctx, key, s)
	if apierrors.IsNotFound(err) {
		s = buildSecret(key.Name, key.Namespace, map[string]string{LabelInstance: in.Name}, ownerRef(in, "PostgresInstance"))
		if err = r.Create(ctx, s); apierrors.IsAlreadyExists(err) {
			err = r.APIReader.Get(ctx, key, s)
		}
	}
	if err != nil {
		return "", err
	}
	if pw, ok := s.StringData["password"]; ok {
		return pw, nil
	}
	return string(s.Data["password"]), nil
}

// ensureCredentials replaces the pool's bootstrap password on a claimed pod. Cold pods were
// initdb'd with the instance's own secret and need nothing.
func (r *InstanceReconciler) ensureCredentials(ctx context.Context, in *pgv1.PostgresInstance, pod *corev1.Pod) (bool, error) {
	if in.Status.Source != pgv1.SourceWarm || r.Creds == nil || meta.IsStatusConditionTrue(in.Status.Conditions, pgv1.CondCredentials) {
		return true, nil
	}
	pw, err := r.ensureSecret(ctx, in)
	if err != nil {
		return false, err
	}
	if !podReady(pod) {
		setCond(in, pgv1.CondCredentials, false, "PodNotReady", "")
		return false, nil
	}
	if err := r.Creds.SetPassword(ctx, pod, pw); err != nil {
		setCond(in, pgv1.CondCredentials, false, "RotationFailed", err.Error())
		return false, err
	}
	setCond(in, pgv1.CondCredentials, true, "Rotated", "")
	return true, r.step(ctx, "rotated")
}

// resizeCompute patches the pods/resize subresource; the kubelet applies it without a reschedule.
func (r *InstanceReconciler) resizeCompute(ctx context.Context, in *pgv1.PostgresInstance, pod *corev1.Pod) error {
	want := instanceTemplate(in).resourceList()
	c := container(pod)
	if c == nil {
		return fmt.Errorf("pod %s has no %s container", pod.Name, containerName)
	}
	if !sameCompute(c.Resources.Limits, want) || !sameCompute(c.Resources.Requests, want) {
		patch := client.StrategicMergeFrom(pod.DeepCopy())
		c.Resources = corev1.ResourceRequirements{Requests: want, Limits: want}
		if err := r.SubResource("resize").Patch(ctx, pod, patch); err != nil {
			setCond(in, pgv1.CondComputeResized, false, "ResizeRejected", err.Error())
			if apierrors.IsInvalid(err) || apierrors.IsForbidden(err) {
				return nil // terminal until the spec changes
			}
			return client.IgnoreNotFound(err)
		}
		setCond(in, pgv1.CondComputeResized, false, "Resizing", "")
		return r.step(ctx, "resized")
	}
	for _, pc := range pod.Status.Conditions {
		if (pc.Type == corev1.PodResizePending || pc.Type == corev1.PodResizeInProgress) && pc.Status == corev1.ConditionTrue {
			setCond(in, pgv1.CondComputeResized, false, pc.Reason+string(pc.Type), pc.Message)
			return nil
		}
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == containerName && cs.Resources != nil && !sameCompute(cs.Resources.Limits, want) {
			setCond(in, pgv1.CondComputeResized, false, "Resizing", "kubelet has not applied the new limits")
			return nil
		}
	}
	setCond(in, pgv1.CondComputeResized, true, "Applied", "")
	return nil
}

// resizeStorage grows the PVC; the CSI driver and kubelet expand the volume and filesystem.
func (r *InstanceReconciler) resizeStorage(ctx context.Context, in *pgv1.PostgresInstance) error {
	if in.Spec.Storage == nil || in.Status.PVCName == "" {
		return nil
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: in.Status.PVCName}, pvc); err != nil {
		return client.IgnoreNotFound(err)
	}
	want, cur := *in.Spec.Storage, pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	switch want.Cmp(cur) {
	case -1:
		setCond(in, pgv1.CondStorageResized, false, "ShrinkNotSupported", fmt.Sprintf("volume is %s", cur.String()))
	case 1:
		if ok, err := r.expandable(ctx, pvc); err != nil || !ok {
			setCond(in, pgv1.CondStorageResized, false, "ExpansionNotSupported", "storage class does not allow volume expansion")
			return err
		}
		if pvc.Status.Phase != corev1.ClaimBound {
			setCond(in, pgv1.CondStorageResized, false, "WaitingForBind", "")
			return nil
		}
		patch := client.MergeFrom(pvc.DeepCopy())
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = want
		if err := r.Patch(ctx, pvc, patch); err != nil {
			return err
		}
		setCond(in, pgv1.CondStorageResized, false, "Expanding", "")
		return r.step(ctx, "expanded")
	default:
		capacity, ok := pvc.Status.Capacity[corev1.ResourceStorage]
		if !ok || capacity.Cmp(want) < 0 {
			setCond(in, pgv1.CondStorageResized, false, "Expanding", "")
		} else {
			setCond(in, pgv1.CondStorageResized, true, "Applied", "")
		}
	}
	return nil
}

func (r *InstanceReconciler) expandable(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	if pvc.Spec.StorageClassName == nil {
		return false, nil
	}
	sc := &storagev1.StorageClass{}
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: *pvc.Spec.StorageClassName}, sc); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return ptrVal(sc.AllowVolumeExpansion), nil
}

// finalize tears down in order: pod first (so nothing writes to the volume), then the volume
// per DeletionPolicy. With Retain the volume and secret are orphaned before the finalizer is
// dropped, otherwise owner-reference GC would delete them.
func (r *InstanceReconciler) finalize(ctx context.Context, in *pgv1.PostgresInstance) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(in, Finalizer) {
		return ctrl.Result{}, nil
	}
	// Pods come from the cache (their delete events requeue us) plus findPod's check for a bind
	// this process made that the cache has not shown yet. An uncached LIST here was the slowest
	// call of a 19k-instance teardown.
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(in.Namespace), client.MatchingFields{idxInstance: in.Name}); err != nil {
		return ctrl.Result{}, err
	}
	if len(pods.Items) == 0 {
		p, err := r.findPod(ctx, in)
		if err != nil {
			return ctrl.Result{}, err
		}
		if p != nil {
			pods.Items = append(pods.Items, *p)
		}
	}
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp != nil {
			continue
		}
		if err := r.Delete(ctx, &pods.Items[i]); client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
	}
	if len(pods.Items) > 0 {
		// The pod's delete event requeues us; this is only a safety net. A 10s poll here made
		// teardown of 19k instances cost ~21 reconciles each while kwok drained pods.
		return ctrl.Result{RequeueAfter: 2 * time.Minute}, r.step(ctx, "pods-deleting")
	}

	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: in.Namespace, Name: in.Name}}
	if err := r.Delete(ctx, svc); client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	children := []client.Object{&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: in.Namespace, Name: secretName(in)}}}
	if in.Spec.Storage != nil { // storage cannot be added later, so ephemeral instances never own a volume
		// Uncached: with Retain, a volume the cache missed would keep its owner reference and be
		// garbage-collected, data included.
		pvcs := &corev1.PersistentVolumeClaimList{}
		if err := r.APIReader.List(ctx, pvcs, client.InNamespace(in.Namespace), client.MatchingLabels{LabelInstance: in.Name}); err != nil {
			return ctrl.Result{}, err
		}
		for i := range pvcs.Items {
			children = append(children, &pvcs.Items[i])
		}
	}
	orphan := client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"ownerReferences":null}}`))
	for _, obj := range children {
		var err error
		if in.Spec.DeletionPolicy == pgv1.DeletionPolicyRetain {
			err = r.Patch(ctx, obj, orphan)
		} else {
			err = r.Delete(ctx, obj)
		}
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.step(ctx, "children-released"); err != nil {
		return ctrl.Result{}, err
	}
	r.bound.Delete(client.ObjectKeyFromObject(in))
	patch := client.MergeFrom(in.DeepCopy())
	controllerutil.RemoveFinalizer(in, Finalizer)
	return ctrl.Result{}, client.IgnoreNotFound(r.Patch(ctx, in, patch))
}

func (r *InstanceReconciler) step(ctx context.Context, name string) error {
	if r.Step == nil {
		return nil
	}
	return r.Step(ctx, name)
}

func setCond(in *pgv1.PostgresInstance, t string, ok bool, reason, msg string) {
	s := metav1.ConditionFalse
	if ok {
		s = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&in.Status.Conditions, metav1.Condition{
		Type: t, Status: s, Reason: reason, Message: msg, ObservedGeneration: in.Generation,
	})
}

func phase(in *pgv1.PostgresInstance) string {
	conds := in.Status.Conditions
	switch {
	case in.Status.PodName == "":
		return "Pending"
	case !meta.IsStatusConditionTrue(conds, pgv1.CondReady):
		return "Provisioning"
	case meta.IsStatusConditionFalse(conds, pgv1.CondComputeResized), meta.IsStatusConditionFalse(conds, pgv1.CondStorageResized):
		return "Resizing"
	}
	return "Ready"
}

func sameCompute(have, want corev1.ResourceList) bool {
	for _, k := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		h, w := have[k], want[k]
		if h.Cmp(w) != 0 {
			return false
		}
	}
	return true
}

func ptrVal[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}
