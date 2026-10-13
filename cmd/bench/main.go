// Command bench measures PostgresInstance ready time, cold create vs warm-pool claim, against a
// running operator. Each trial is timed client-side: from just before Create to the watch event
// that shows Ready=True.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgv1 "github.com/adarsh-sgh/pgfleet/api/v1alpha1"
)

type result struct {
	Mode    string    `json:"mode"`
	Seconds []float64 `json:"seconds"`
	P50     float64   `json:"p50"`
	Max     float64   `json:"max"`
}

func main() {
	trials := flag.Int("n", 10, "trials per mode")
	poolSize := flag.Int("pool-size", 2, "")
	storage := flag.String("storage", "1Gi", "")
	sc := flag.String("storage-class", "standard", "")
	out := flag.String("out", "", "write JSON results here")
	modes := flag.String("modes", "cold,warm", "comma-separated: cold, warm")
	before := flag.String("before-each", "", "shell command run before every trial (e.g. evict the image from the node)")
	flag.Parse()

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = pgv1.AddToScheme(scheme)
	c, err := client.NewWithWatch(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	check(err)
	ctx := context.Background()

	ns := fmt.Sprintf("bench-%d", time.Now().Unix())
	check(c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	defer c.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})

	compute := pgv1.Compute{CPU: resource.MustParse("500m"), Memory: resource.MustParse("512Mi")}
	size := resource.MustParse(*storage)
	pool := &pgv1.WarmPool{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "bench"},
		Spec:       pgv1.WarmPoolSpec{Size: int32(*poolSize), Resources: compute, Storage: &size, StorageClassName: sc},
	}
	check(c.Create(ctx, pool))

	var results []result
	for _, mode := range strings.Split(*modes, ",") {
		r := result{Mode: mode}
		for i := range *trials {
			if mode == "warm" {
				waitPoolFull(ctx, c, pool)
			}
			if *before != "" {
				if b, err := exec.Command("sh", "-c", *before).CombinedOutput(); err != nil {
					check(fmt.Errorf("before-each: %v: %s", err, b))
				}
			}
			in := &pgv1.PostgresInstance{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: fmt.Sprintf("%s-%d", mode, i)},
				Spec:       pgv1.PostgresInstanceSpec{Resources: compute, Storage: &size, StorageClassName: sc},
			}
			if mode == "warm" {
				in.Spec.PoolRef = pool.Name
			}
			d, source := timeToReady(ctx, c, in)
			if string(source) != mode {
				fmt.Fprintf(os.Stderr, "%s: wanted %s, got %s\n", in.Name, mode, source)
				os.Exit(1)
			}
			fmt.Printf("%-7s %6.2fs\n", in.Name, d.Seconds())
			r.Seconds = append(r.Seconds, d.Seconds())
			check(c.Delete(ctx, in))
			waitDeleted(ctx, c, in)
		}
		s := slices.Sorted(slices.Values(r.Seconds))
		r.P50, r.Max = s[len(s)/2], s[len(s)-1]
		results = append(results, r)
	}

	fmt.Printf("\n| mode | trials | p50 | max |\n|---|--:|--:|--:|\n")
	for _, r := range results {
		fmt.Printf("| %s | %d | %.2fs | %.2fs |\n", r.Mode, len(r.Seconds), r.P50, r.Max)
	}
	if *out != "" {
		b, _ := json.MarshalIndent(results, "", "  ")
		check(os.WriteFile(*out, b, 0o644))
	}
}

func timeToReady(ctx context.Context, c client.WithWatch, in *pgv1.PostgresInstance) (time.Duration, pgv1.Source) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	// Watch from before Create so the Ready event cannot be missed.
	w, err := c.Watch(ctx, &pgv1.PostgresInstanceList{}, client.InNamespace(in.Namespace))
	check(err)
	defer w.Stop()
	start := time.Now()
	check(c.Create(ctx, in))
	for ev := range w.ResultChan() {
		got, ok := ev.Object.(*pgv1.PostgresInstance)
		if ok && got.Name == in.Name && meta.IsStatusConditionTrue(got.Status.Conditions, pgv1.CondReady) {
			return time.Since(start), got.Status.Source
		}
	}
	check(fmt.Errorf("%s: watch ended before Ready: %v", in.Name, ctx.Err()))
	return 0, ""
}

func waitPoolFull(ctx context.Context, c client.Client, p *pgv1.WarmPool) {
	for deadline := time.Now().Add(5 * time.Minute); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		check(c.Get(ctx, client.ObjectKeyFromObject(p), p))
		if p.Status.Ready == p.Spec.Size {
			return
		}
	}
	check(fmt.Errorf("pool never filled: %+v", p.Status))
}

func waitDeleted(ctx context.Context, c client.Client, in *pgv1.PostgresInstance) {
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(in), &pgv1.PostgresInstance{})) {
			return
		}
	}
	check(fmt.Errorf("%s not deleted", in.Name))
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
