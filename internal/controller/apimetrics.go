package controller

import (
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var apiLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "pgfleet_api_request_seconds",
	Help:    "Operator-to-apiserver request latency (watches excluded).",
	Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
}, []string{"verb", "resource"})

func init() { metrics.Registry.MustRegister(apiLatency) }

// InstrumentAPI records the latency of every non-watch request the operator makes, labelled by
// HTTP verb and resource, so a slow reconcile can be attributed to the call that made it slow.
func InstrumentAPI(cfg *rest.Config) {
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Query().Get("watch") == "true" {
				return rt.RoundTrip(r)
			}
			start := time.Now()
			resp, err := rt.RoundTrip(r)
			apiLatency.WithLabelValues(r.Method, resourceOf(r.URL.Path)).Observe(time.Since(start).Seconds())
			return resp, err
		})
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// resourceOf turns /api/v1/namespaces/ns/pods/name/status into "pods/status".
func resourceOf(path string) string {
	p := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case len(p) >= 2 && p[0] == "api":
		p = p[2:]
	case len(p) >= 3 && p[0] == "apis":
		p = p[3:]
	default:
		return "other"
	}
	if len(p) >= 2 && p[0] == "namespaces" {
		p = p[2:]
	}
	switch len(p) {
	case 0:
		return "namespaces"
	case 3:
		return p[0] + "/" + p[2]
	default:
		return p[0]
	}
}
