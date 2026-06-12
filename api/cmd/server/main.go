// Command server is the CVS unified platform daemon.
//
// It serves three tiles (Test Execution, Cluster Monitor, Fleet Metrics) behind
// one binary. At S0 it serves the embedded React shell plus health/version.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/ROCm/cvs/api/internal/cluster"
	"github.com/ROCm/cvs/api/internal/clustermon"
	"github.com/ROCm/cvs/api/internal/inventory"
	"github.com/ROCm/cvs/api/internal/testexec"
	httptransport "github.com/ROCm/cvs/api/internal/transport/http"
	"github.com/ROCm/cvs/api/internal/transport/ws"
	"github.com/ROCm/cvs/api/internal/version"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	addr := envOr("CVS_LISTEN_ADDR", ":8080")
	cvsBin := envOr("CVS_BIN", "cvs")
	configDir := testexec.LocateConfigDir(os.Getenv("CVS_CONFIG_DIR"), cvsBin)
	logger.Info("config_dir_resolved", "dir", configDir)

	// Persistent data volume backing every FileStore plus uploaded SSH keys.
	dataDir := envOr("CVS_DATA_DIR", "/app/data")
	keyDir := envOr("CVS_SSH_KEY_DIR", filepath.Join(dataDir, "keys"))
	logger.Info("data_dir_resolved", "data_dir", dataDir, "key_dir", keyDir)

	invStore, err := inventory.NewFileStore(filepath.Join(dataDir, "inventory.json"))
	if err != nil {
		logger.Error("inventory_store_init_failed", "err", err)
		os.Exit(1)
	}
	keyStore := inventory.NewKeyStore(keyDir)

	// Saved-cluster catalog (S3b): generated cluster_json files live on the data
	// volume; the catalog index is a JSON collection beside them.
	clusterStore, err := cluster.NewFileStore(filepath.Join(dataDir, "clusters.json"))
	if err != nil {
		logger.Error("cluster_store_init_failed", "err", err)
		os.Exit(1)
	}
	clusterSvc := cluster.NewService(
		clusterStore,
		cluster.NewCLIGenerator(cvsBin),
		invAdapter{store: invStore, keys: keyStore},
		filepath.Join(dataDir, "clusters"),
	)

	// Test Execution runs (S3): persisted execution records + a bounded worker
	// pool. S3 uses a fake runner; S5 swaps in testexec.NewCLIRunner(cvsBin).
	execStore, err := testexec.NewFileExecutionStore(filepath.Join(dataDir, "executions.json"))
	if err != nil {
		logger.Error("execution_store_init_failed", "err", err)
		os.Exit(1)
	}
	// Test runner (S5): the real CLI runner shells out to `cvs run`. The fake
	// runner (used to build/test the lifecycle without real nodes) is still
	// available via CVS_RUNNER=fake for dev/demo.
	var runner testexec.Runner
	if envOr("CVS_RUNNER", "cli") == "fake" {
		runner = testexec.FakeRunner{Delay: 400 * time.Millisecond}
		logger.Info("runner_selected", "runner", "fake")
	} else {
		runner = testexec.NewCLIRunner(cvsBin)
		logger.Info("runner_selected", "runner", "cli", "bin", cvsBin)
	}

	// Live log/status streaming (S4): the executor publishes lifecycle events to
	// the WS hub, which fans them out to connected UI clients. The hub also
	// carries the S8 Cluster Monitor metrics broadcast.
	hub := ws.NewHub()

	// Cluster Monitor live metrics (S7 service + S8 poll loop). One shared
	// MetricsService caches the latest snapshot; the poller refreshes it on an
	// interval and broadcasts over the hub, and reprobes reachability with a
	// failure-threshold debounce.
	metricsSvc := clustermon.NewMetricsService(invStore, keyStore, logger)
	poller := clustermon.NewPoller(
		metricsSvc,
		invStore,
		inventory.NewSSHProber(keyStore),
		hub,
		clustermon.PollerConfig{
			PollInterval:     time.Duration(envOrInt("POLLING__INTERVAL", 60)) * time.Second,
			FailureThreshold: envOrInt("POLLING__FAILURE_THRESHOLD", 5),
		},
		logger,
	)
	executor := testexec.NewExecutor(
		execStore,
		runner,
		wsEvents{hub: hub},
		envOrInt("CVS_MAX_CONCURRENT", 2),
		1024,
		logger,
	)
	defer executor.Shutdown()

	handler := httptransport.NewRouter(httptransport.Options{
		Logger:         logger,
		CvsBin:         cvsBin,
		ConfigDir:      configDir,
		InventoryStore: invStore,
		KeyStore:       keyStore,
		ClusterService: clusterSvc,
		Execution: &testexec.ExecutionDeps{
			Store:    execStore,
			Executor: executor,
			Clusters: clusterStore,
			Dir:      filepath.Join(dataDir, "executions"),
		},
		WSHub:          hub,
		WSSnapshots:    execSnapshots{store: execStore},
		MetricsService: metricsSvc,
		WSMetrics:      metricsProvider{svc: metricsSvc},
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("server_starting",
			"addr", addr,
			"version", version.Version,
			"commit", version.Commit,
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server_error", "err", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Cluster Monitor poll loop (S8): refreshes + broadcasts GPU metrics and
	// debounced reachability until shutdown.
	go poller.Run(ctx)

	<-ctx.Done()

	logger.Info("server_shutting_down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server_shutdown_error", "err", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// invAdapter bridges the inventory store + key store to the cluster package's
// InventoryProvider, resolving the uploaded key name to an absolute path for
// the `cvs generate cluster_json --key_file` flag.
type invAdapter struct {
	store inventory.Store
	keys  *inventory.KeyStore
}

// wsEvents adapts the WS hub to testexec.Events so the executor can stream live
// log lines and lifecycle transitions without importing the ws package.
type wsEvents struct{ hub *ws.Hub }

func (e wsEvents) Log(id, line string)                     { e.hub.PublishLog(id, line) }
func (e wsEvents) Status(id string, ex testexec.Execution) { e.hub.PublishStatus(id, ex) }
func (e wsEvents) Complete(ex testexec.Execution)          { e.hub.PublishCompletion(ex.ID, ex) }

// metricsProvider adapts the Cluster Monitor metrics service to
// ws.MetricsProvider so a newly connected /ws/clustermon client gets the cached
// latest snapshot as its first frame.
type metricsProvider struct{ svc *clustermon.MetricsService }

func (m metricsProvider) LatestMetrics() (any, bool) {
	snap := m.svc.Latest()
	if snap == nil {
		return nil, false
	}
	return snap, true
}

// execSnapshots adapts the execution store to ws.SnapshotProvider, supplying the
// terminal/late-joiner fallback (persisted log + final status).
type execSnapshots struct{ store testexec.ExecutionStore }

func (s execSnapshots) Snapshot(id string) (ws.ExecutionSnapshot, bool) {
	ex, ok := s.store.Get(id)
	if !ok {
		return ws.ExecutionSnapshot{}, false
	}
	logs := ""
	if ex.LogPath != "" {
		if b, err := os.ReadFile(ex.LogPath); err == nil {
			logs = string(b)
		}
	}
	return ws.ExecutionSnapshot{Terminal: ex.Status.Terminal(), Logs: logs, Status: ex}, true
}

func (a invAdapter) Current() (cluster.Inventory, bool, error) {
	inv, ok, err := a.store.Get()
	if err != nil || !ok {
		return cluster.Inventory{}, ok, err
	}
	keyFile := ""
	if a.keys != nil && inv.KeyName != "" {
		if p, e := a.keys.Path(inv.KeyName); e == nil {
			keyFile = p
		}
	}
	return cluster.Inventory{Username: inv.Username, KeyFile: keyFile, Nodes: inv.Nodes}, true, nil
}
