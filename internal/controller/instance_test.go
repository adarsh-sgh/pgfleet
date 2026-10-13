package controller_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"

	pgv1 "github.com/adarsh-sgh/pgfleet/api/v1alpha1"
	"github.com/adarsh-sgh/pgfleet/internal/controller"
)

var ctrlConfig = config.Controller{SkipNameValidation: ptr(true)}

const sc = "expandable"

func instance(ns, name, pool string, storage string) *pgv1.PostgresInstance {
	in := &pgv1.PostgresInstance{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: pgv1.PostgresInstanceSpec{
			Resources: pgv1.Compute{CPU: resource.MustParse("500m"), Memory: resource.MustParse("512Mi")},
			PoolRef:   pool,
		},
	}
	if storage != "" {
		in.Spec.Storage, in.Spec.StorageClassName = ptr(resource.MustParse(storage)), ptr(sc)
	}
	return in
}

func pool(ns, name string, size int32) *pgv1.WarmPool {
	return &pgv1.WarmPool{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: pgv1.WarmPoolSpec{
			Size:      size,
			Resources: pgv1.Compute{CPU: resource.MustParse("250m"), Memory: resource.MustParse("256Mi")},
			Storage:   ptr(resource.MustParse("1Gi")), StorageClassName: ptr(sc),
		},
	}
}

// TestInstanceLifecycle: cold create, compute + storage resize, shrink rejection, delete with
// both policies, and Retain + recreate reattaching the volume.
func TestInstanceLifecycle(t *testing.T) {
	ctx := context.Background()
	ns := newNamespace(t)
	startManager(t, controller.Options{})

	in := instance(ns, "db", "", "1Gi")
	must(t, k8s.Create(ctx, in))
	waitReady(t, in)
	if in.Status.Source != pgv1.SourceCold || in.Status.PodName != "db-pg" || in.Status.PVCName != "db-data" ||
		in.Status.SecretName != "db-credentials" || in.Status.Endpoint != "db."+ns+".svc:5432" {
		t.Fatalf("status: %+v", in.Status)
	}
	must(t, k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "db"}, &corev1.Service{}))
	must(t, k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "db-credentials"}, &corev1.Secret{}))

	// Resize: 1 CPU in place via pods/resize, volume grown to 2Gi.
	update(t, in, func(in *pgv1.PostgresInstance) {
		in.Spec.Resources.CPU = resource.MustParse("1")
		in.Spec.Storage = ptr(resource.MustParse("2Gi"))
	})
	eventually(t, "resized", func() error {
		pod, pvc := &corev1.Pod{}, &corev1.PersistentVolumeClaim{}
		must(t, k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "db-pg"}, pod))
		must(t, k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "db-data"}, pvc))
		if cpu := pod.Spec.Containers[0].Resources.Limits.Cpu(); cpu.Cmp(resource.MustParse("1")) != 0 {
			return fmt.Errorf("pod cpu %s", cpu)
		}
		if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(resource.MustParse("2Gi")) != 0 {
			return fmt.Errorf("pvc %s", got.String())
		}
		return wantConds(in, pgv1.CondComputeResized, pgv1.CondStorageResized, pgv1.CondReady)
	})
	if in.Status.Phase != "Ready" {
		t.Fatalf("phase %s", in.Status.Phase)
	}

	update(t, in, func(in *pgv1.PostgresInstance) { in.Spec.Storage = ptr(resource.MustParse("1Gi")) })
	eventually(t, "shrink rejected", func() error {
		must(t, k8s.Get(ctx, client.ObjectKeyFromObject(in), in))
		if c := meta.FindStatusCondition(in.Status.Conditions, pgv1.CondStorageResized); c == nil || c.Reason != "ShrinkNotSupported" {
			return fmt.Errorf("cond %+v", c)
		}
		return nil
	})

	// Delete policy: everything goes.
	must(t, k8s.Delete(ctx, in))
	waitGone(t, in)
	assertGone(t, ns, "db-pg", "db-data", "db", "db-credentials")

	// Retain: volume and secret survive unowned; recreating the name reattaches both.
	kept := instance(ns, "kept", "", "1Gi")
	kept.Spec.DeletionPolicy = pgv1.DeletionPolicyRetain
	must(t, k8s.Create(ctx, kept))
	waitReady(t, kept)
	must(t, k8s.Delete(ctx, kept))
	waitGone(t, kept)
	pvc := &corev1.PersistentVolumeClaim{}
	must(t, k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "kept-data"}, pvc))
	if len(pvc.OwnerReferences) != 0 || pvc.DeletionTimestamp != nil {
		t.Fatalf("retained pvc: owners %v deleting %v", pvc.OwnerReferences, pvc.DeletionTimestamp)
	}
	again := instance(ns, "kept", "", "1Gi")
	must(t, k8s.Create(ctx, again))
	waitReady(t, again)
	must(t, k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: "kept-data"}, pvc))
	if !metav1.IsControlledBy(pvc, again) || again.Status.PVCName != "kept-data" {
		t.Fatalf("volume not re-adopted: owners %v status %+v", pvc.OwnerReferences, again.Status)
	}
}

// TestWarmPoolClaims: more concurrent claimers than warm pods. Each pod goes to at most one
// instance, claimed pods get resized/expanded and their password rotated, and the pool refills.
func TestWarmPoolClaims(t *testing.T) {
	ctx := context.Background()
	ns := newNamespace(t)
	creds := &fakeCreds{}
	startManager(t, controller.Options{Creds: creds})

	p := pool(ns, "small", 3)
	must(t, k8s.Create(ctx, p))
	waitPool(t, p, 3)

	var ins []*pgv1.PostgresInstance
	for i := range 6 {
		in := instance(ns, fmt.Sprintf("app-%d", i), "small", "2Gi")
		ins = append(ins, in)
		must(t, k8s.Create(ctx, in))
	}
	owners := map[string]string{}
	warm := 0
	for _, in := range ins {
		waitReady(t, in)
		if prev, dup := owners[in.Status.PodName]; dup {
			t.Fatalf("pod %s owned by %s and %s", in.Status.PodName, prev, in.Name)
		}
		owners[in.Status.PodName] = in.Name
		pod := &corev1.Pod{}
		must(t, k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: in.Status.PodName}, pod))
		if !metav1.IsControlledBy(pod, in) || pod.Labels[controller.LabelInstance] != in.Name {
			t.Fatalf("pod %s not bound to %s: %v", pod.Name, in.Name, pod.OwnerReferences)
		}
		if cpu := pod.Spec.Containers[0].Resources.Limits.Cpu(); cpu.Cmp(resource.MustParse("500m")) != 0 {
			t.Fatalf("%s cpu %s", pod.Name, cpu)
		}
		if in.Status.Source != pgv1.SourceWarm {
			continue
		}
		warm++
		pvc := &corev1.PersistentVolumeClaim{}
		must(t, k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: in.Status.PVCName}, pvc))
		if !metav1.IsControlledBy(pvc, in) || pvc.Spec.Resources.Requests.Storage().Cmp(resource.MustParse("2Gi")) != 0 {
			t.Fatalf("claimed pvc %s: owners %v size %s", pvc.Name, pvc.OwnerReferences, pvc.Spec.Resources.Requests.Storage())
		}
		if creds.count(pod.Name) == 0 || in.Status.SecretName != in.Name+"-credentials" {
			t.Fatalf("%s: password not rotated (secret %s)", in.Name, in.Status.SecretName)
		}
	}
	if warm < 3 {
		t.Fatalf("only %d of 6 claims were warm with 3 warm pods ready", warm)
	}
	waitPool(t, p, 3)

	// Template change replaces warm pods, leaves claimed ones alone.
	update(t, p, func(p *pgv1.WarmPool) { p.Spec.Resources.CPU = resource.MustParse("300m") })
	eventually(t, "pool rolled", func() error {
		pods := &corev1.PodList{}
		must(t, k8s.List(ctx, pods, client.InNamespace(ns), client.MatchingLabels{controller.LabelState: controller.StateWarm}))
		live := 0
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp != nil {
				continue
			}
			live++
			if cpu := pod.Spec.Containers[0].Resources.Limits.Cpu(); cpu.Cmp(resource.MustParse("300m")) != 0 {
				return fmt.Errorf("stale warm pod %s", pod.Name)
			}
		}
		if live != 3 {
			return fmt.Errorf("%d warm pods", live)
		}
		return nil
	})
	for pod := range owners {
		must(t, k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: pod}, &corev1.Pod{}))
	}
}

// TestSurvivesRestartMidReconcile kills the manager right after each side effect, starts a fresh
// one, and checks the fleet converges with no duplicate or leaked pods, volumes or claims.
func TestSurvivesRestartMidReconcile(t *testing.T) {
	cases := []struct {
		step string
		warm bool
	}{
		{"finalizer", false}, {"pvc", false}, {"pod", false}, {"service", false},
		{"claimed", true}, {"pvc-adopted", true}, {"rotated", true}, {"resized", true}, {"expanded", true},
		{"pods-deleting", false}, {"children-released", true},
	}
	for _, tc := range cases {
		t.Run(tc.step, func(t *testing.T) {
			ctx := context.Background()
			ns := newNamespace(t)
			creds := &fakeCreds{}
			deleting := tc.step == "pods-deleting" || tc.step == "children-released"

			var p *pgv1.WarmPool
			if tc.warm {
				// Pool + its pods come up under a crash-free manager first.
				stop := startManager(t, controller.Options{Creds: creds})
				p = pool(ns, "pool", 2)
				must(t, k8s.Create(ctx, p))
				waitPool(t, p, 2)
				stop()
			}
			in := instance(ns, "db", map[bool]string{true: "pool"}[tc.warm], "2Gi")

			crashed := make(chan struct{})
			var once sync.Once
			hook := func(ctx context.Context, step string) error {
				if step != tc.step {
					return nil
				}
				once.Do(func() { close(crashed) })
				<-ctx.Done() // hang until the process is killed
				return ctx.Err()
			}
			if deleting {
				stop := startManager(t, controller.Options{Creds: creds})
				must(t, k8s.Create(ctx, in))
				waitReady(t, in)
				stop()
			}
			stop := startManager(t, controller.Options{Creds: creds, Step: hook})
			if deleting {
				must(t, k8s.Delete(ctx, in))
			} else {
				must(t, k8s.Create(ctx, in))
			}
			<-crashed
			stop()

			startManager(t, controller.Options{Creds: creds})
			if deleting {
				waitGone(t, in)
				assertGone(t, ns, "db-pg", "db", "db-credentials")
				pods := &corev1.PodList{}
				must(t, k8s.List(ctx, pods, client.InNamespace(ns), client.MatchingLabels{controller.LabelInstance: "db"}))
				if len(pods.Items) != 0 {
					t.Fatalf("pod %s leaked", pods.Items[0].Name)
				}
				pvcs := &corev1.PersistentVolumeClaimList{}
				must(t, k8s.List(ctx, pvcs, client.InNamespace(ns), client.MatchingLabels{controller.LabelInstance: "db"}))
				for _, c := range pvcs.Items {
					if c.DeletionTimestamp == nil {
						t.Fatalf("pvc %s leaked", c.Name)
					}
				}
				return
			}
			waitReady(t, in)
			eventually(t, "resize converged", func() error {
				return wantConds(in, pgv1.CondReady, pgv1.CondComputeResized, pgv1.CondStorageResized)
			})

			pods := &corev1.PodList{}
			must(t, k8s.List(ctx, pods, client.InNamespace(ns), client.MatchingLabels{controller.LabelInstance: "db"}))
			pvcs := &corev1.PersistentVolumeClaimList{}
			must(t, k8s.List(ctx, pvcs, client.InNamespace(ns), client.MatchingLabels{controller.LabelInstance: "db"}))
			if len(pods.Items) != 1 || len(pvcs.Items) != 1 {
				t.Fatalf("want 1 pod and 1 pvc, got %d pods %d pvcs", len(pods.Items), len(pvcs.Items))
			}
			if pods.Items[0].Name != in.Status.PodName || !metav1.IsControlledBy(&pvcs.Items[0], in) {
				t.Fatalf("status %+v does not match pod %s / pvc owners %v", in.Status, pods.Items[0].Name, pvcs.Items[0].OwnerReferences)
			}
			if tc.warm {
				if in.Status.Source != pgv1.SourceWarm || creds.count(in.Status.PodName) == 0 {
					t.Fatalf("warm claim lost: source %s rotations %d", in.Status.Source, creds.count(in.Status.PodName))
				}
				waitPool(t, p, 2)
			}
		})
	}
}

func waitReady(t *testing.T, in *pgv1.PostgresInstance) {
	t.Helper()
	eventually(t, in.Name+" ready", func() error { return wantConds(in, pgv1.CondReady) })
}

func wantConds(in *pgv1.PostgresInstance, types ...string) error {
	if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(in), in); err != nil {
		return err
	}
	if in.Status.ObservedGeneration != in.Generation {
		return fmt.Errorf("observed generation %d of %d", in.Status.ObservedGeneration, in.Generation)
	}
	for _, typ := range types {
		if !meta.IsStatusConditionTrue(in.Status.Conditions, typ) {
			return fmt.Errorf("%s not true: %+v", typ, in.Status.Conditions)
		}
	}
	return nil
}

func waitPool(t *testing.T, p *pgv1.WarmPool, ready int32) {
	t.Helper()
	eventually(t, "pool ready", func() error {
		if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(p), p); err != nil {
			return err
		}
		if p.Status.Ready != ready || p.Status.Warming != 0 {
			return fmt.Errorf("pool status %+v", p.Status)
		}
		return nil
	})
}

func waitGone(t *testing.T, obj client.Object) {
	t.Helper()
	eventually(t, obj.GetName()+" deleted", func() error {
		err := k8s.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Join(err, fmt.Errorf("still present, finalizers %v", obj.GetFinalizers()))
	})
}

// assertGone checks no pod/service/secret by these names remains; PVCs only need to be
// terminating (envtest has no pvc-protection controller to drop the finalizer).
func assertGone(t *testing.T, ns string, names ...string) {
	t.Helper()
	for _, n := range names {
		key := client.ObjectKey{Namespace: ns, Name: n}
		for _, obj := range []client.Object{&corev1.Pod{}, &corev1.Service{}, &corev1.Secret{}} {
			if err := k8s.Get(context.Background(), key, obj); !apierrors.IsNotFound(err) {
				t.Fatalf("%T %s still exists (err %v)", obj, n, err)
			}
		}
		pvc := &corev1.PersistentVolumeClaim{}
		if err := k8s.Get(context.Background(), key, pvc); err == nil && pvc.DeletionTimestamp == nil {
			t.Fatalf("pvc %s not deleted", n)
		}
	}
}

func update[T client.Object](t *testing.T, obj T, mutate func(T)) {
	t.Helper()
	eventually(t, "update", func() error {
		if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
			return err
		}
		mutate(obj)
		return k8s.Update(context.Background(), obj)
	})
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func ptr[T any](v T) *T { return &v }
