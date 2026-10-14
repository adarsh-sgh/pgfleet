// Command loadtest drives the operator at fleet scale on a kwok cluster. It runs the operator as a
// child process, fills warm pools, creates instances until the cluster holds the target pod count,
// SIGKILLs and restarts the operator (the new process re-reconciles the whole fleet from a cold
// cache), then deletes every instance. Per phase it diffs the operator's controller-runtime
// histograms into reconcile-time and queue-wait quantiles, plus client-side create-to-Ready.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/flowcontrol"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgv1 "github.com/adarsh-sgh/pgfleet/api/v1alpha1"
)

var (
	instances   = flag.Int("instances", 19000, "")
	namespaces  = flag.Int("namespaces", 10, "instances and pools are spread over this many namespaces")
	poolSize    = flag.Int("pool-size", 100, "warm pods per namespace")
	rate        = flag.Float64("rate", 200, "instance creates (and deletes) per second")
	managerBin  = flag.String("manager", "./bin/manager", "operator binary, started and restarted by this tool")
	managerArgs = flag.String("manager-args", "", "")
	managerLog  = flag.String("manager-log", "results/raw/manager.log", "")
	metricsURL  = flag.String("metrics-url", "http://127.0.0.1:19080/metrics", "")
	readyzURL   = flag.String("readyz-url", "http://127.0.0.1:19081/readyz", "")
	label       = flag.String("label", "", "free-form run label stored in the result")
	out         = flag.String("out", "", "write JSON results here")
)

type quantiles struct {
	Count uint64  `json:"count"`
	P50   float64 `json:"p50_s"`
	P99   float64 `json:"p99_s"`
	// Upper bound of the bucket holding p99; the honest number when buckets are coarse
	// (workqueue_queue_duration_seconds buckets are powers of 10).
	P99Le float64 `json:"p99_bucket_le,omitempty"`
}

type phase struct {
	Name      string    `json:"name"`
	Seconds   float64   `json:"seconds"`
	Reconcile quantiles `json:"reconcile_time"` // controller_runtime_reconcile_time_seconds, all instance shards
	QueueWait quantiles `json:"queue_wait"`     // workqueue_queue_duration_seconds, all instance shards
	Pool      quantiles `json:"pool_reconcile_time"`
	Errors    float64   `json:"reconcile_errors"`
	EndToEnd  quantiles `json:"create_to_ready,omitempty"` // client-side, create phase only
	// Operator-to-apiserver latency by "VERB resource" (pgfleet_api_request_seconds).
	API map[string]quantiles `json:"api_requests"`
}

type report struct {
	Label       string             `json:"label"`
	ManagerArgs string             `json:"manager_args"`
	Instances   int                `json:"instances"`
	Pods        int                `json:"pods_at_peak"`
	Ready       int                `json:"pods_running_ready_at_peak"`
	Nodes       int                `json:"nodes"`
	Source      map[string]float64 `json:"pod_source"`
	Phases      []phase            `json:"phases"`
}

func main() {
	flag.Parse()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = pgv1.AddToScheme(scheme)
	cfg := ctrl.GetConfigOrDie()
	cfg.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(1000, 2000)
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	check(err)
	ctx := context.Background()
	compute := pgv1.Compute{CPU: resource.MustParse("100m"), Memory: resource.MustParse("128Mi")}
	rep := report{Label: *label, ManagerArgs: *managerArgs, Instances: *instances}
	name := func(i int) (string, string) { return fmt.Sprintf("load-%d", i%*namespaces), fmt.Sprintf("db-%05d", i) }

	mgr := startManager()

	// Phase 1: warm pools.
	before := scrape()
	t0 := time.Now()
	for i := range *namespaces {
		ns, _ := name(i)
		check(client.IgnoreAlreadyExists(c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})))
		check(c.Create(ctx, &pgv1.WarmPool{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pool"},
			Spec:       pgv1.WarmPoolSpec{Size: int32(*poolSize), Resources: compute},
		}))
	}
	waitFor("pools full", func() bool {
		pools := &pgv1.WarmPoolList{}
		check(c.List(ctx, pools))
		for _, p := range pools.Items {
			if p.Status.Ready != p.Spec.Size {
				return false
			}
		}
		return len(pools.Items) == *namespaces
	})
	rep.Phases = append(rep.Phases, diff("pools", time.Since(t0), before, scrape()))

	// Phase 2: instances, all pointing at their namespace's pool (warm when available, else cold).
	before = scrape()
	t0 = time.Now()
	created := sync.Map{} // ns/name -> time.Time
	var readyLat []float64
	var mu sync.Mutex
	watchCtx, stopWatch := context.WithCancel(ctx)
	readyCount := atomic.Int64{}
	seen := sync.Map{}
	go watchReady(watchCtx, c, func(in *pgv1.PostgresInstance) {
		key := in.Namespace + "/" + in.Name
		start, ok := created.Load(key)
		if _, dup := seen.LoadOrStore(key, true); !ok || dup {
			return
		}
		mu.Lock()
		readyLat = append(readyLat, time.Since(start.(time.Time)).Seconds())
		mu.Unlock()
		readyCount.Add(1)
	})
	paced(*instances, func(i int) {
		ns, n := name(i)
		in := &pgv1.PostgresInstance{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: n},
			Spec:       pgv1.PostgresInstanceSpec{Resources: compute, PoolRef: "pool"},
		}
		created.Store(ns+"/"+n, time.Now())
		check(c.Create(ctx, in))
	})
	fmt.Fprintf(os.Stderr, "created %d instances in %s\n", *instances, time.Since(t0).Round(time.Second))
	// The watch gives per-instance latency; completion is checked by listing, so an event lost
	// across a watch reconnect cannot stall the run (it only shrinks the latency sample).
	waitFor("instances ready", func() bool {
		if readyCount.Load() >= int64(*instances) {
			return true
		}
		list := &pgv1.PostgresInstanceList{}
		check(c.List(ctx, list))
		ready := 0
		for _, in := range list.Items {
			if meta.IsStatusConditionTrue(in.Status.Conditions, pgv1.CondReady) {
				ready++
			}
		}
		return ready >= *instances
	})
	stopWatch()
	p := diff("create", time.Since(t0), before, scrape())
	p.EndToEnd = sampleQuantiles(readyLat)
	rep.Phases = append(rep.Phases, p)
	rep.Pods, rep.Ready = countPods(ctx, c)
	nodes := &corev1.NodeList{}
	check(c.List(ctx, nodes))
	rep.Nodes = len(nodes.Items)
	rep.Source = counterByLabel(scrape(), "pgfleet_instance_pod_source_total", "result")

	// Phase 3: kill -9 the operator and start a fresh one; it lists the whole fleet into a cold
	// cache and reconciles every instance once.
	check(mgr.Process.Signal(syscall.SIGKILL))
	_ = mgr.Wait()
	t0 = time.Now()
	mgr = startManager()
	inst := isInstanceController
	waitFor("resync", func() bool {
		m := scrape()
		return counterSum(m, "controller_runtime_reconcile_total", inst) >= float64(*instances) &&
			gaugeSum(m, "workqueue_depth", inst) == 0
	})
	rep.Phases = append(rep.Phases, diff("restart-resync", time.Since(t0), nil, scrape()))

	// Phase 4: delete every instance (finalizer teardown) at the same rate.
	before = scrape()
	t0 = time.Now()
	paced(*instances, func(i int) {
		ns, n := name(i)
		check(client.IgnoreNotFound(c.Delete(ctx, &pgv1.PostgresInstance{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: n}})))
	})
	waitFor("instances deleted", func() bool {
		list := &metav1.PartialObjectMetadataList{}
		list.SetGroupVersionKind(pgv1.GroupVersion.WithKind("PostgresInstanceList"))
		check(c.List(ctx, list))
		return len(list.Items) == 0
	})
	rep.Phases = append(rep.Phases, diff("delete", time.Since(t0), before, scrape()))
	_ = mgr.Process.Signal(syscall.SIGTERM)
	_ = mgr.Wait()

	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		check(os.WriteFile(*out, b, 0o644))
	}
}

// watchReady calls f for every Ready instance event, re-watching from the last seen
// resourceVersion whenever the apiserver closes the stream.
func watchReady(ctx context.Context, c client.WithWatch, f func(*pgv1.PostgresInstance)) {
	rv := ""
	for ctx.Err() == nil {
		w, err := c.Watch(ctx, &pgv1.PostgresInstanceList{}, &client.ListOptions{Raw: &metav1.ListOptions{ResourceVersion: rv}})
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		for ev := range w.ResultChan() {
			in, ok := ev.Object.(*pgv1.PostgresInstance)
			if !ok {
				rv = "" // 410 Gone or similar: resume from now
				break
			}
			rv = in.ResourceVersion
			if meta.IsStatusConditionTrue(in.Status.Conditions, pgv1.CondReady) {
				f(in)
			}
		}
		w.Stop()
		if ctx.Err() == nil {
			fmt.Fprintln(os.Stderr, "ready watch closed, re-watching")
		}
	}
}

func startManager() *exec.Cmd {
	logf, err := os.OpenFile(*managerLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	check(err)
	cmd := exec.Command(*managerBin, strings.Fields(*managerArgs)...)
	cmd.Stdout, cmd.Stderr = logf, logf
	check(cmd.Start())
	waitFor("operator ready", func() bool {
		resp, err := http.Get(*readyzURL)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	return cmd
}

func countPods(ctx context.Context, c client.Client) (total, ready int) {
	pods := &corev1.PodList{}
	check(c.List(ctx, pods, client.MatchingLabels{"app.kubernetes.io/managed-by": "pgfleet"}))
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodRunning && slices.ContainsFunc(p.Status.Conditions, func(c corev1.PodCondition) bool {
			return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue
		}) {
			ready++
		}
	}
	return len(pods.Items), ready
}

func isInstanceController(m *dto.Metric) bool {
	return strings.HasPrefix(labelValue(m, "controller")+labelValue(m, "name"), "postgresinstance-")
}

// paced runs f(0..n-1) at *rate per second over 64 workers.
func paced(n int, f func(int)) {
	work := make(chan int)
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			for i := range work {
				f(i)
			}
		})
	}
	tick := time.NewTicker(time.Duration(float64(time.Second) / *rate))
	for i := range n {
		<-tick.C
		work <- i
	}
	tick.Stop()
	close(work)
	wg.Wait()
}

func waitFor(what string, ok func() bool) {
	start := time.Now()
	for i := 1; !ok(); i++ {
		if time.Since(start) > 15*time.Minute {
			check(fmt.Errorf("timed out waiting for %s", what))
		}
		if i%15 == 0 {
			fmt.Fprintf(os.Stderr, "waiting for %s (%s)\n", what, time.Since(start).Round(time.Second))
		}
		time.Sleep(time.Second)
	}
}

func scrape() map[string]*dto.MetricFamily {
	resp, err := http.Get(*metricsURL)
	check(err)
	defer resp.Body.Close()
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(resp.Body)
	check(err)
	return fams
}

func diff(name string, d time.Duration, before, after map[string]*dto.MetricFamily) phase {
	inst := isInstanceController
	pool := func(m *dto.Metric) bool { return labelValue(m, "controller") == "warmpool" }
	api := map[string]quantiles{}
	if f := after["pgfleet_api_request_seconds"]; f != nil {
		for _, m := range f.Metric {
			verb, res := labelValue(m, "verb"), labelValue(m, "resource")
			q := histQuantiles(before, after, f.GetName(), func(o *dto.Metric) bool {
				return labelValue(o, "verb") == verb && labelValue(o, "resource") == res
			})
			if q.Count > 0 {
				api[verb+" "+res] = q
			}
		}
	}
	return phase{
		Name:      name,
		API:       api,
		Seconds:   math.Round(d.Seconds()*10) / 10,
		Reconcile: histQuantiles(before, after, "controller_runtime_reconcile_time_seconds", inst),
		QueueWait: histQuantiles(before, after, "workqueue_queue_duration_seconds", inst),
		Pool:      histQuantiles(before, after, "controller_runtime_reconcile_time_seconds", pool),
		Errors:    counterSum(after, "controller_runtime_reconcile_errors_total", inst) - counterSum(before, "controller_runtime_reconcile_errors_total", inst),
	}
}

// histQuantiles merges matching histograms, subtracts the earlier snapshot, and interpolates
// within buckets the way PromQL's histogram_quantile does.
func histQuantiles(before, after map[string]*dto.MetricFamily, name string, match func(*dto.Metric) bool) quantiles {
	merge := func(fams map[string]*dto.MetricFamily) (map[float64]uint64, uint64) {
		buckets, total := map[float64]uint64{}, uint64(0)
		if f := fams[name]; f != nil {
			for _, m := range f.Metric {
				if !match(m) {
					continue
				}
				for _, b := range m.Histogram.Bucket {
					buckets[b.GetUpperBound()] += b.GetCumulativeCount()
				}
				total += m.Histogram.GetSampleCount()
			}
		}
		return buckets, total
	}
	a, at := merge(after)
	b, bt := merge(before)
	bounds := make([]float64, 0, len(a))
	for ub := range a {
		bounds = append(bounds, ub)
	}
	sort.Float64s(bounds)
	cum := make([]uint64, len(bounds))
	for i, ub := range bounds {
		cum[i] = a[ub] - b[ub]
	}
	count := at - bt
	le := func(p float64) float64 {
		for i, ub := range bounds {
			if float64(cum[i]) >= p*float64(count) {
				return ub
			}
		}
		return -1 // +Inf bucket; JSON cannot encode Inf
	}
	q := func(p float64) float64 {
		if count == 0 {
			return 0
		}
		rank := p * float64(count)
		lower, prev := 0.0, uint64(0)
		for i, ub := range bounds {
			if float64(cum[i]) >= rank {
				if math.IsInf(ub, 1) {
					return lower
				}
				return lower + (ub-lower)*(rank-float64(prev))/float64(cum[i]-prev)
			}
			lower, prev = ub, cum[i]
		}
		return lower
	}
	if count == 0 {
		return quantiles{}
	}
	return quantiles{Count: count, P50: round(q(0.5)), P99: round(q(0.99)), P99Le: le(0.99)}
}

func sampleQuantiles(s []float64) quantiles {
	if len(s) == 0 {
		return quantiles{}
	}
	sort.Float64s(s)
	at := func(p float64) float64 { return round(s[min(len(s)-1, int(p*float64(len(s))))]) }
	return quantiles{Count: uint64(len(s)), P50: at(0.5), P99: at(0.99)}
}

func counterSum(fams map[string]*dto.MetricFamily, name string, match func(*dto.Metric) bool) float64 {
	total := 0.0
	if f := fams[name]; f != nil {
		for _, m := range f.Metric {
			if match(m) {
				total += m.Counter.GetValue()
			}
		}
	}
	return total
}

func gaugeSum(fams map[string]*dto.MetricFamily, name string, match func(*dto.Metric) bool) float64 {
	total := 0.0
	if f := fams[name]; f != nil {
		for _, m := range f.Metric {
			if match(m) {
				total += m.Gauge.GetValue()
			}
		}
	}
	return total
}

func counterByLabel(fams map[string]*dto.MetricFamily, name, label string) map[string]float64 {
	out := map[string]float64{}
	if f := fams[name]; f != nil {
		for _, m := range f.Metric {
			out[labelValue(m, label)] += m.Counter.GetValue()
		}
	}
	return out
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.Label {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

func round(v float64) float64 { return math.Round(v*10000) / 10000 }

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
