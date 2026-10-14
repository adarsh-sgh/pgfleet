// Command manager runs the pgfleet controllers.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	pgv1 "github.com/adarsh-sgh/pgfleet/api/v1alpha1"
	"github.com/adarsh-sgh/pgfleet/internal/controller"
)

func main() {
	var (
		metricsAddr, probeAddr string
		pprofAddr              string
		leaderElect, rotate    bool
		opts                   controller.Options
		qps                    float64
		burst                  int
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "")
	flag.StringVar(&pprofAddr, "pprof-bind-address", "", "serve net/http/pprof here, e.g. :6060")
	flag.BoolVar(&leaderElect, "leader-elect", false, "")
	flag.BoolVar(&rotate, "rotate-credentials", true, "replace the pool password on claimed pods (needs pods/exec)")
	flag.BoolVar(&opts.ClusterIP, "cluster-ip-services", false, "give each instance a ClusterIP Service instead of a headless one")
	flag.IntVar(&opts.Shards, "shards", 4, "PostgresInstance work queues")
	flag.IntVar(&opts.WorkersPerShard, "workers-per-shard", 8, "")
	flag.IntVar(&opts.PoolWorkers, "pool-workers", 4, "")
	flag.Float64Var(&qps, "kube-api-qps", 200, "client-side rate limit; controller-runtime's default is 20")
	flag.IntVar(&burst, "kube-api-burst", 400, "")
	zapOpts := zap.Options{}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	log := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	must(clientgoscheme.AddToScheme(scheme))
	must(pgv1.AddToScheme(scheme))

	cfg := ctrl.GetConfigOrDie()
	cfg.QPS, cfg.Burst = float32(qps), burst
	controller.InstrumentAPI(cfg)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Cache:                  controller.CacheOptions(),
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		PprofBindAddress:       pprofAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "pgfleet.dev",
	})
	must(err)
	if rotate {
		opts.Creds = &controller.ExecSetter{Config: cfg, Client: kubernetes.NewForConfigOrDie(cfg)}
	}
	must(controller.Setup(mgr, opts))
	must(mgr.AddHealthzCheck("healthz", healthz.Ping))
	must(mgr.AddReadyzCheck("readyz", healthz.Ping))

	log.Info("starting", "shards", opts.Shards, "workersPerShard", opts.WorkersPerShard, "qps", qps)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited")
		os.Exit(1)
	}
}

func must(err error) {
	if err != nil {
		ctrl.Log.Error(err, "setup")
		os.Exit(1)
	}
}
