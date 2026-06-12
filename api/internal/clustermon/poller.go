package clustermon

import (
	"context"
	"log/slog"
	"time"

	"github.com/ROCm/cvs/api/internal/inventory"
)

// Default poll/reprobe cadence and reachability debounce. Overridable via env in
// main (POLLING__INTERVAL, POLLING__FAILURE_THRESHOLD).
const (
	defaultPollInterval     = 60 * time.Second
	defaultReprobeInterval  = 300 * time.Second
	defaultFailureThreshold = 5
	reprobeTimeout          = 60 * time.Second
)

// MetricsPublisher broadcasts a collected snapshot to live subscribers (the WS
// hub). Decoupled so the poller does not import the transport package.
type MetricsPublisher interface {
	PublishMetrics(data any)
}

// Poller drives the Cluster Monitor's live updates (S8): it runs the GPU metrics
// sweep on a fixed interval (caching + broadcasting each snapshot) and re-runs
// the F2 connectivity probe less often, applying a consecutive-failure debounce
// so a node only flips unreachable after FailureThreshold straight misses.
type Poller struct {
	metrics   *MetricsService
	store     inventory.Store
	prober    inventory.Prober
	publisher MetricsPublisher
	logger    *slog.Logger

	pollInterval     time.Duration
	reprobeInterval  time.Duration
	failureThreshold int

	// fails tracks consecutive reprobe misses per host for the debounce.
	fails map[string]int
}

// PollerConfig tunes the poller cadence. Zero values fall back to defaults.
type PollerConfig struct {
	PollInterval     time.Duration
	ReprobeInterval  time.Duration
	FailureThreshold int
}

// NewPoller wires the poller to the shared metrics service, inventory store, and
// connectivity prober, publishing each snapshot through publisher.
func NewPoller(
	metrics *MetricsService,
	store inventory.Store,
	prober inventory.Prober,
	publisher MetricsPublisher,
	cfg PollerConfig,
	logger *slog.Logger,
) *Poller {
	if logger == nil {
		logger = slog.Default()
	}
	p := &Poller{
		metrics:          metrics,
		store:            store,
		prober:           prober,
		publisher:        publisher,
		logger:           logger,
		pollInterval:     cfg.PollInterval,
		reprobeInterval:  cfg.ReprobeInterval,
		failureThreshold: cfg.FailureThreshold,
		fails:            map[string]int{},
	}
	if p.pollInterval <= 0 {
		p.pollInterval = defaultPollInterval
	}
	if p.reprobeInterval <= 0 {
		p.reprobeInterval = defaultReprobeInterval
	}
	if p.failureThreshold <= 0 {
		p.failureThreshold = defaultFailureThreshold
	}
	return p
}

// Run blocks until ctx is cancelled, collecting metrics every pollInterval and
// reprobing reachability every reprobeInterval. Intended to run in a goroutine.
func (p *Poller) Run(ctx context.Context) {
	p.logger.Info("clustermon_poller_started",
		"poll_interval", p.pollInterval.String(),
		"reprobe_interval", p.reprobeInterval.String(),
		"failure_threshold", p.failureThreshold,
	)
	// Prime the cache shortly after startup so the first WS client paints fast.
	p.collectOnce(ctx)

	pollT := time.NewTicker(p.pollInterval)
	reprobeT := time.NewTicker(p.reprobeInterval)
	defer pollT.Stop()
	defer reprobeT.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("clustermon_poller_stopped")
			return
		case <-pollT.C:
			p.collectOnce(ctx)
		case <-reprobeT.C:
			p.reprobe(ctx)
		}
	}
}

// collectOnce runs one fleet GPU sweep and broadcasts it. Errors (no inventory,
// no SSH creds) are non-fatal: the loop simply waits for the next tick.
func (p *Poller) collectOnce(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, collectTimeout)
	defer cancel()
	snap, err := p.metrics.Collect(cctx)
	if err != nil {
		p.logger.Debug("clustermon_metrics_poll_skipped", "err", err)
		return
	}
	if p.publisher != nil {
		p.publisher.PublishMetrics(snap)
	}
}

// reprobe re-runs the connectivity probe and persists a debounced reachability
// view back to the inventory store.
func (p *Poller) reprobe(ctx context.Context) {
	inv, ok, err := p.store.Get()
	if err != nil || !ok || len(inv.Nodes) == 0 {
		return
	}
	rctx, cancel := context.WithTimeout(ctx, reprobeTimeout)
	defer cancel()
	fresh, err := p.prober.Probe(rctx, inv)
	if err != nil {
		p.logger.Warn("clustermon_reprobe_failed", "err", err)
		return
	}

	inv.Statuses = p.debounce(inv.Statuses, fresh)
	if _, err := p.store.Save(inv); err != nil {
		p.logger.Error("clustermon_reprobe_save_failed", "err", err)
	}
}

// debounce merges a fresh probe result with the previous statuses: a reachable
// node is taken as-is (and its failure counter reset); an unreachable node is
// only persisted as unreachable once it has missed FailureThreshold consecutive
// reprobes — until then it retains its last-good status (with the new
// CheckedAt) so a single transient SSH/TCP blip does not flap the grid.
func (p *Poller) debounce(prev, fresh []inventory.NodeStatus) []inventory.NodeStatus {
	prevByHost := make(map[string]inventory.NodeStatus, len(prev))
	for _, s := range prev {
		prevByHost[s.Host] = s
	}

	merged := make([]inventory.NodeStatus, 0, len(fresh))
	for _, st := range fresh {
		if st.Reachable {
			p.fails[st.Host] = 0
			merged = append(merged, st)
			continue
		}

		p.fails[st.Host]++
		if p.fails[st.Host] >= p.failureThreshold {
			// Confirmed down after enough straight misses.
			merged = append(merged, st)
			continue
		}
		// Debounce window: keep the last-good status but refresh the timestamp.
		if last, found := prevByHost[st.Host]; found && last.Reachable {
			last.CheckedAt = st.CheckedAt
			merged = append(merged, last)
			continue
		}
		// No prior reachable status to fall back on.
		merged = append(merged, st)
	}
	return merged
}
