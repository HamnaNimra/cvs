package clustermon

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/ROCm/cvs/api/internal/inventory"
)

// maxParallelCollect bounds concurrent SSH sessions during a collection.
// With a persistent-connection pool, this limits simultaneous in-flight
// commands (not TCP dials). For fleets ≤ 500 nodes, set this ≥ fleet size
// so all nodes run in parallel. Throttle at 256 to cap goroutine/fd pressure
// at very large scale (1000+ nodes).
const maxParallelCollect = 256

// MetricsSnapshot is the result of one fleet-wide live metrics sweep. As of S9
// it carries both the GPU and NIC collectors (the two live/critical collectors,
// mirroring the cluster-mon Python poll loop); software/logs collectors are
// on-demand with TTL caches and live elsewhere.
type MetricsSnapshot struct {
	CollectedAt time.Time        `json:"collected_at"`
	GPU         []NodeGPUMetrics `json:"gpu"`
	NIC         []NodeNICMetrics `json:"nic"`
}

// MetricsService collects live GPU + NIC metrics over the shared SSH pool and
// caches the latest snapshot. Collection runs on-demand (REST) and on the S8
// poll loop.
type MetricsService struct {
	store  inventory.Store
	keys   *inventory.KeyStore
	logger *slog.Logger

	mu     sync.RWMutex
	latest *MetricsSnapshot
}

// NewMetricsService wires the collector to the shared inventory + key stores.
func NewMetricsService(store inventory.Store, keys *inventory.KeyStore, logger *slog.Logger) *MetricsService {
	if logger == nil {
		logger = slog.Default()
	}
	return &MetricsService{store: store, keys: keys, logger: logger}
}

// Latest returns the most recent cached snapshot, or nil if none yet.
func (s *MetricsService) Latest() *MetricsSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latest
}

// Collect runs a GPU + NIC metrics sweep over the reachable nodes, caches it as
// the latest snapshot, and returns it. It requires an SSH-capable (key-auth)
// inventory.
func (s *MetricsService) Collect(ctx context.Context) (*MetricsSnapshot, error) {
	inv, ok, err := s.store.Get()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no inventory saved")
	}

	pool, err := inventory.NewSSHPool(inv, s.keys, maxParallelCollect)
	if err != nil {
		return nil, fmt.Errorf("build ssh pool: %w", err)
	}
	if pool == nil {
		return nil, fmt.Errorf("SSH not available: live metrics require key-based auth")
	}
	defer pool.Close()

	hosts := targetHosts(inv)
	s.logger.Info("metrics_collect_start", "nodes", len(hosts), "max_parallel", maxParallelCollect)
	t0 := time.Now()

	// GPU and NIC sweeps share one pool (one persistent connection per host).
	// They run concurrently — semaphore slots are shared but with maxParallelCollect=256
	// all 165 nodes run in a single wave so contention is negligible.
	var (
		gpuResults map[string]NodeGPUMetrics
		nicResults map[string]NodeNICMetrics
		wg         sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		t := time.Now()
		gpuResults = CollectGPU(ctx, pool, hosts)
		s.logger.Info("gpu_collect_done", "nodes", len(gpuResults), "elapsed_ms", time.Since(t).Milliseconds())
	}()
	go func() {
		defer wg.Done()
		t := time.Now()
		nicResults = CollectNIC(ctx, pool, hosts)
		s.logger.Info("nic_collect_done", "nodes", len(nicResults), "elapsed_ms", time.Since(t).Milliseconds())
	}()
	wg.Wait()
	s.logger.Info("metrics_collect_done", "elapsed_ms", time.Since(t0).Milliseconds())

	gpu := make([]NodeGPUMetrics, 0, len(gpuResults))
	nic := make([]NodeNICMetrics, 0, len(nicResults))
	for _, h := range hosts {
		if m, found := gpuResults[h]; found {
			gpu = append(gpu, m)
		}
		if m, found := nicResults[h]; found {
			nic = append(nic, m)
		}
	}
	sort.Slice(gpu, func(i, j int) bool { return gpu[i].Host < gpu[j].Host })
	sort.Slice(nic, func(i, j int) bool { return nic[i].Host < nic[j].Host })

	snap := &MetricsSnapshot{CollectedAt: time.Now().UTC(), GPU: gpu, NIC: nic}
	s.mu.Lock()
	s.latest = snap
	s.mu.Unlock()
	s.logger.Info("live_metrics_collected", "gpu_nodes", len(gpu), "nic_nodes", len(nic))
	return snap, nil
}

// targetHosts returns the nodes to collect from: the reachable subset per the
// last probe, or all nodes when no probe has run yet.
func targetHosts(inv inventory.Inventory) []string {
	if len(inv.Statuses) == 0 {
		return inv.Nodes
	}
	reachable := make(map[string]bool, len(inv.Statuses))
	for _, st := range inv.Statuses {
		reachable[st.Host] = st.Reachable
	}
	hosts := make([]string, 0, len(inv.Nodes))
	for _, h := range inv.Nodes {
		// Include unprobed-but-listed nodes too (reachable defaults false only
		// when an explicit unreachable status exists).
		if r, probed := reachable[h]; !probed || r {
			hosts = append(hosts, h)
		}
	}
	return hosts
}
