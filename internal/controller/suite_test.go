package controller_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	pgv1 "github.com/adarsh-sgh/pgfleet/api/v1alpha1"
	"github.com/adarsh-sgh/pgfleet/internal/controller"
)

var (
	cfg    *rest.Config
	k8s    client.Client
	scheme = runtime.NewScheme()
)

// TestMain runs a real kube-apiserver + etcd (envtest). There is no kubelet, scheduler or GC, so
// fakeKubelet plays node and CSI driver: it marks pods Running/Ready and binds PVCs at their
// requested size (which also completes expansions).
func TestMain(m *testing.M) {
	if os.Getenv("TEST_LOG") != "" {
		ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	} else {
		ctrl.SetLogger(logr.Discard())
	}
	_ = clientgoscheme.AddToScheme(scheme)
	_ = pgv1.AddToScheme(scheme)

	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd")}, ErrorIfCRDPathMissing: true}
	var err error
	if cfg, err = env.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "envtest:", err)
		os.Exit(1)
	}
	if k8s, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := k8s.Create(ctx, &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: sc}, Provisioner: "example.com/fake", AllowVolumeExpansion: ptr(true),
	}); err != nil {
		panic(err)
	}
	go fakeKubelet(ctx)
	code := m.Run()
	cancel()
	_ = env.Stop()
	os.Exit(code)
}

func fakeKubelet(ctx context.Context) {
	for ctx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
		pods := &corev1.PodList{}
		if k8s.List(ctx, pods) != nil {
			continue
		}
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.Status.Phase == corev1.PodRunning || p.DeletionTimestamp != nil {
				continue
			}
			p.Status.Phase = corev1.PodRunning
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			_ = k8s.Status().Update(ctx, p)
		}
		pvcs := &corev1.PersistentVolumeClaimList{}
		if k8s.List(ctx, pvcs) != nil {
			continue
		}
		for i := range pvcs.Items {
			c := &pvcs.Items[i]
			want := c.Spec.Resources.Requests[corev1.ResourceStorage]
			if have, ok := c.Status.Capacity[corev1.ResourceStorage]; ok && have.Cmp(want) == 0 && c.Status.Phase == corev1.ClaimBound {
				continue
			}
			c.Status.Phase = corev1.ClaimBound
			c.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: want}
			_ = k8s.Status().Update(ctx, c)
		}
	}
}

// startManager runs the controllers until the returned stop func is called; stop blocks until
// the manager has fully exited, like a killed process.
func startManager(t *testing.T, o controller.Options) (stop func()) {
	t.Helper()
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Cache:                  controller.CacheOptions(),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             ctrlConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	if o.Shards == 0 {
		o.Shards, o.WorkersPerShard = 2, 4
	}
	if err := controller.Setup(mgr, o); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("manager: %v", err)
		}
	}()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

// fakeCreds records password rotations instead of exec'ing psql (envtest has no kubelet).
type fakeCreds struct {
	mu    sync.Mutex
	calls map[string]int // pod -> rotations
}

func (f *fakeCreds) SetPassword(_ context.Context, pod *corev1.Pod, pw string) error {
	if pw == "" {
		return fmt.Errorf("empty password")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[pod.Name]++
	return nil
}

func (f *fakeCreds) count(pod string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[pod]
}

func newNamespace(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "t-"}}
	if err := k8s.Create(context.Background(), ns); err != nil {
		t.Fatal(err)
	}
	return ns.Name
}

func eventually(t *testing.T, what string, f func() error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if err = f(); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s: %v", what, err)
}
