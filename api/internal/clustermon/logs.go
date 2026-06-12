package clustermon

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ROCm/cvs/api/internal/inventory"
	"github.com/ROCm/cvs/api/internal/ssh"
)

// logsCollectTimeout bounds a full dmesg log sweep (three commands per host).
const logsCollectTimeout = 90 * time.Second

// Logs collector commands (verbatim parity with the cluster-mon Python logs
// collector). Each pipes through `|| echo ""` so a missing tool yields an empty
// string rather than a shell error.
const (
	cmdAMDLogs = `bash -c 'sudo dmesg --decode -T -l emerg,alert,crit,err,warn 2>/dev/null | grep -iE "PCIe|XGMI|amdgpu|epyc|cpu|ionic|bnxt|mlnx|mellanox|Link|error|fail" 2>/dev/null | grep -iv "vital buffer"  2>/dev/null || echo ""'`

	cmdDmesgErrors = `bash -c 'sudo dmesg --decode -T -l emerg,alert,crit,err 2>/dev/null || echo ""'`

	cmdUserspaceErrors = `bash -c 'sudo dmesg --decode -T -l emerg,alert,crit,err,warn 2>/dev/null | egrep -i "oom|out of memory|killed process|segfault|general protection|call trace|bug:|hardware error|mce|stack trace|pytorch|torch|tensorflow|megatron|jax|vllm|sglang|triton.*error|triton.*exception|triton.*failed" 2>/dev/null || echo ""'`
)

// NodeLogs is one node's collected dmesg log buckets. Empty strings mean a clean
// node (no matching lines).
type NodeLogs struct {
	Host            string `json:"host"`
	AMDLogs         string `json:"amd_logs"`
	DmesgErrors     string `json:"dmesg_errors"`
	UserspaceErrors string `json:"userspace_errors"`
	Error           string `json:"error,omitempty"`
}

// LogsSnapshot is one fleet-wide log sweep.
type LogsSnapshot struct {
	CollectedAt time.Time  `json:"collected_at"`
	Nodes       []NodeLogs `json:"nodes"`
}

// SearchResult is one node's first matching lines for a grep search.
type SearchResult struct {
	Host   string `json:"host"`
	Output string `json:"output"`
}

// SearchResponse wraps an ad-hoc dmesg grep search across the fleet.
type SearchResponse struct {
	GrepCommand      string         `json:"grep_command"`
	Results          []SearchResult `json:"results"`
	TotalNodes       int            `json:"total_nodes_searched"`
	NodesWithResults int            `json:"nodes_with_results"`
}

// LogsService runs the dmesg log collectors + ad-hoc grep search over the shared
// SSH pool. Logs are point-in-time, so there is no cache.
type LogsService struct {
	store  inventory.Store
	keys   *inventory.KeyStore
	logger *slog.Logger
}

// NewLogsService wires the logs collectors to the shared stores.
func NewLogsService(store inventory.Store, keys *inventory.KeyStore, logger *slog.Logger) *LogsService {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogsService{store: store, keys: keys, logger: logger}
}

func (s *LogsService) poolForCollect() (*ssh.Pool, []string, error) {
	inv, ok, err := s.store.Get()
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("no inventory saved")
	}
	pool, err := inventory.NewSSHPool(inv, s.keys, maxParallelCollect)
	if err != nil {
		return nil, nil, fmt.Errorf("build ssh pool: %w", err)
	}
	if pool == nil {
		return nil, nil, fmt.Errorf("SSH not available: log collection requires key-based auth")
	}
	return pool, targetHosts(inv), nil
}

// Collect runs the three dmesg collectors over the reachable nodes.
func (s *LogsService) Collect(ctx context.Context) (*LogsSnapshot, error) {
	pool, hosts, err := s.poolForCollect()
	if err != nil {
		return nil, err
	}
	defer pool.Close()

	results := collectLogsAll(ctx, pool, hosts)
	nodes := make([]NodeLogs, 0, len(results))
	for _, h := range hosts {
		if m, found := results[h]; found {
			nodes = append(nodes, m)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Host < nodes[j].Host })
	s.logger.Info("logs_collected", "nodes", len(nodes))
	return &LogsSnapshot{CollectedAt: time.Now().UTC(), Nodes: nodes}, nil
}

func collectLogsAll(ctx context.Context, r Runner, hosts []string) map[string]NodeLogs {
	out := make(map[string]NodeLogs, len(hosts))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			m := collectHostLogs(ctx, r, host)
			mu.Lock()
			out[host] = m
			mu.Unlock()
		}(h)
	}
	wg.Wait()
	return out
}

func collectHostLogs(ctx context.Context, r Runner, host string) NodeLogs {
	m := NodeLogs{Host: host}
	// The AMD log command is the reachability gate.
	amd, err := r.Run(ctx, host, cmdAMDLogs)
	if err != nil {
		m.Error = err.Error()
		return m
	}
	m.AMDLogs = strings.TrimSpace(amd)
	if out, err := r.Run(ctx, host, cmdDmesgErrors); err == nil {
		m.DmesgErrors = strings.TrimSpace(out)
	}
	if out, err := r.Run(ctx, host, cmdUserspaceErrors); err == nil {
		m.UserspaceErrors = strings.TrimSpace(out)
	}
	return m
}

// Search runs a validated grep pipeline against `sudo dmesg -T` on each node,
// returning the first 5 matching lines per node.
func (s *LogsService) Search(ctx context.Context, grepCommand string) (*SearchResponse, error) {
	if err := validateGrepCommand(grepCommand); err != nil {
		return nil, &invalidGrepError{msg: err.Error()}
	}
	pool, hosts, err := s.poolForCollect()
	if err != nil {
		return nil, err
	}
	defer pool.Close()

	// Escape single quotes for the outer bash -c '...': ' -> '\''.
	escaped := strings.ReplaceAll(grepCommand, "'", `'\''`)
	cmd := fmt.Sprintf(`bash -c 'sudo dmesg -T 2>/dev/null | %s | head -5 || echo ""'`, escaped)

	out := make(map[string]string, len(hosts))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			res, err := pool.Run(ctx, host, cmd)
			if err != nil {
				return
			}
			mu.Lock()
			out[host] = strings.TrimSpace(res)
			mu.Unlock()
		}(h)
	}
	wg.Wait()

	results := make([]SearchResult, 0, len(out))
	withResults := 0
	for _, h := range hosts {
		if r, found := out[h]; found {
			results = append(results, SearchResult{Host: h, Output: r})
			if r != "" {
				withResults++
			}
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Host < results[j].Host })
	return &SearchResponse{
		GrepCommand:      grepCommand,
		Results:          results,
		TotalNodes:       len(results),
		NodesWithResults: withResults,
	}, nil
}

// invalidGrepError marks a rejected grep command so the handler can return 400.
type invalidGrepError struct{ msg string }

func (e *invalidGrepError) Error() string { return e.msg }

var (
	grepDangerousChars = []string{";", "&", "$", "`", "(", ")", "{", "}", "<", ">", "\n", "\r"}
	grepDangerousWords = []string{
		"bash", "sh", "exec", "eval", "sudo", "rm", "mv", "cp", "dd", "cat", "tee", "chmod", "chown",
	}
	grepAllowedFlags = map[string]bool{
		"-i": true, "-v": true, "-E": true, "-A": true, "-B": true, "-C": true,
		"-w": true, "-x": true, "-o": true, "-n": true, "-c": true, "-m": true,
	}
)

// validateGrepCommand mirrors the cluster-mon Python validate_grep_command:
// only grep/egrep segments joined by pipes, a safe-flag allowlist, and a
// dangerous-character/keyword denylist. The keyword check is substring-based
// (parity), so words containing e.g. "sh"/"cat" are rejected.
func validateGrepCommand(grepCmd string) error {
	if strings.TrimSpace(grepCmd) == "" {
		return fmt.Errorf("empty command")
	}
	for _, c := range grepDangerousChars {
		if strings.Contains(grepCmd, c) {
			return fmt.Errorf("invalid character %q in command", c)
		}
	}
	lower := strings.ToLower(grepCmd)
	for _, kw := range grepDangerousWords {
		if strings.Contains(lower, kw) {
			return fmt.Errorf("command contains forbidden keyword: %s", kw)
		}
	}

	for _, seg := range strings.Split(grepCmd, "|") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		if !strings.HasPrefix(seg, "grep ") && !strings.HasPrefix(seg, "egrep ") {
			return fmt.Errorf("invalid segment (must start with 'grep' or 'egrep'): %s", seg)
		}
		parts := strings.Fields(seg)
		for _, part := range parts[1:] {
			if !strings.HasPrefix(part, "-") {
				continue
			}
			if grepAllowedFlags[part] {
				continue
			}
			if combinedFlagsAllowed(part) {
				continue
			}
			base := part
			if i := strings.IndexByte(part, '='); i >= 0 {
				base = part[:i]
			}
			if grepAllowedFlags[base] || grepAllowedFlags[strings.TrimRight(base, "0123456789")] {
				continue
			}
			return fmt.Errorf("invalid flag: %s", part)
		}
	}
	return nil
}

// combinedFlagsAllowed checks bundled short flags like -iE (every alpha char
// must be an allowed single flag).
func combinedFlagsAllowed(part string) bool {
	for _, c := range part[1:] {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			if !grepAllowedFlags["-"+string(c)] {
				return false
			}
		}
	}
	return true
}
