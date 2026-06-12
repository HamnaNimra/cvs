package clustermon

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// GPU metric commands. cmdGPUMetric returns comprehensive per-GPU metrics
// (utilization, memory, temperature, power). S9d adds the dedicated PCIe/ECC/
// XGMI-error commands (parity with the cluster-mon Python collector, which
// issues these separately "for cleaner data") merged per-GPU.
const (
	cmdGPUMetric = "amd-smi metric --json"
	cmdGPUPCIe   = "amd-smi metric --pcie --json"
	cmdGPUECC    = "amd-smi metric --ecc --json"
	cmdGPUXGMI   = "amd-smi metric --xgmi-err --json"
)

// Runner executes a command on a host over SSH (satisfied by ssh.Pool).
type Runner interface {
	Run(ctx context.Context, host, cmd string) (string, error)
}

// GPU is one accelerator's live metrics on a node.
type GPU struct {
	Index          int       `json:"index"`
	UtilizationPct float64   `json:"utilization_pct"`
	MemTotalMB     float64   `json:"mem_total_mb"`
	MemUsedMB      float64   `json:"mem_used_mb"`
	MemUsedPct     float64   `json:"mem_used_pct"`
	TempEdgeC      float64   `json:"temp_edge_c"`
	TempHotspotC   float64   `json:"temp_hotspot_c"`
	TempMemC       float64   `json:"temp_mem_c"`
	PowerW         float64   `json:"power_w"`
	PCIe           *PCIeInfo `json:"pcie,omitempty"`
	ECC            *ECCInfo  `json:"ecc,omitempty"`
	XGMI           *XGMIInfo `json:"xgmi,omitempty"`
}

// PCIeInfo is the per-GPU PCIe link status + error counters (amd-smi --pcie).
type PCIeInfo struct {
	Width             string `json:"width"`
	Speed             string `json:"speed"`
	Bandwidth         string `json:"bandwidth"`
	ReplayCount       int    `json:"replay_count"`
	L0ToRecoveryCount int    `json:"l0_to_recovery_count"`
	NakSentCount      int    `json:"nak_sent_count"`
	NakReceivedCount  int    `json:"nak_received_count"`
}

// ECCInfo is the per-GPU RAS/ECC error totals (amd-smi --ecc).
type ECCInfo struct {
	Correctable   int `json:"correctable"`
	Uncorrectable int `json:"uncorrectable"`
	Deferred      int `json:"deferred"`
}

// XGMIInfo is the per-GPU XGMI interconnect error status (amd-smi --xgmi-err).
type XGMIInfo struct {
	Status     string `json:"status,omitempty"`
	ErrorCount int    `json:"error_count"`
}

// NodeGPUMetrics is the per-node GPU collection result.
type NodeGPUMetrics struct {
	Host  string `json:"host"`
	GPUs  []GPU  `json:"gpus"`
	Error string `json:"error,omitempty"`
}

// CollectGPU runs the GPU metric command on every host concurrently (bounded by
// the pool's MaxParallel) and parses the result per host.
func CollectGPU(ctx context.Context, r Runner, hosts []string) map[string]NodeGPUMetrics {
	out := make(map[string]NodeGPUMetrics, len(hosts))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			m := collectHostGPU(ctx, r, host)
			mu.Lock()
			out[host] = m
			mu.Unlock()
		}(h)
	}
	wg.Wait()
	return out
}

func collectHostGPU(ctx context.Context, r Runner, host string) NodeGPUMetrics {
	raw, err := r.Run(ctx, host, cmdGPUMetric)
	if err != nil {
		return NodeGPUMetrics{Host: host, Error: err.Error()}
	}
	gpus, perr := parseGPUMetric(raw)
	if perr != "" {
		return NodeGPUMetrics{Host: host, Error: perr}
	}

	// Best-effort enrichment from the dedicated commands; a missing/failed one
	// simply leaves that section empty (parity with the Python collector, which
	// also issues these separately).
	byIdx := make(map[int]*GPU, len(gpus))
	for i := range gpus {
		byIdx[gpus[i].Index] = &gpus[i]
	}
	if out, err := r.Run(ctx, host, cmdGPUPCIe); err == nil {
		for idx, p := range parseGPUBlocks(out, parsePCIeBlock) {
			if g := byIdx[idx]; g != nil {
				g.PCIe = p
			}
		}
	}
	if out, err := r.Run(ctx, host, cmdGPUECC); err == nil {
		for idx, e := range parseGPUBlocks(out, parseECCBlock) {
			if g := byIdx[idx]; g != nil {
				g.ECC = e
			}
		}
	}
	if out, err := r.Run(ctx, host, cmdGPUXGMI); err == nil {
		for idx, x := range parseGPUBlocks(out, parseXGMIBlock) {
			if g := byIdx[idx]; g != nil {
				g.XGMI = x
			}
		}
	}
	return NodeGPUMetrics{Host: host, GPUs: gpus}
}

// parseGPUBlocks runs a per-GPU block extractor over an amd-smi JSON payload
// (list or {"gpu_data":[...]}), returning a map keyed by GPU index. A nil block
// (extractor returned nil) is skipped.
func parseGPUBlocks[T any](raw string, extract func(map[string]any) *T) map[int]*T {
	out := map[int]*T{}
	js := extractJSON(raw)
	if js == "" {
		return out
	}
	objs, perr := gpuObjects(js)
	if perr != "" {
		return out
	}
	for _, obj := range objs {
		if b := extract(obj); b != nil {
			out[intField(obj, "gpu")] = b
		}
	}
	return out
}

// parsePCIeBlock flattens the per-GPU "pcie" object into PCIeInfo, mirroring the
// Python _parse_pcie_metrics_from_amd_smi reshaping (x<width>, "<v> <unit>").
func parsePCIeBlock(obj map[string]any) *PCIeInfo {
	pcie, ok := obj["pcie"].(map[string]any)
	if !ok {
		return nil
	}
	p := &PCIeInfo{
		Width:             "-",
		Speed:             valueUnit(pcie, "speed", "GT/s"),
		Bandwidth:         valueUnit(pcie, "bandwidth", "Mb/s"),
		ReplayCount:       intField(pcie, "replay_count"),
		L0ToRecoveryCount: intField(pcie, "l0_to_recovery_count"),
		NakSentCount:      intField(pcie, "nak_sent_count"),
		NakReceivedCount:  intField(pcie, "nak_received_count"),
	}
	if w := numField(pcie, "width"); w > 0 {
		p.Width = "x" + strconv.Itoa(int(w))
	}
	return p
}

// parseECCBlock extracts correctable/uncorrectable/deferred totals from the
// per-GPU "ecc" (or "ras") object, tolerant of the differing field names amd-smi
// uses across ROCm versions.
func parseECCBlock(obj map[string]any) *ECCInfo {
	ecc, ok := obj["ecc"].(map[string]any)
	if !ok {
		if ecc, ok = obj["ras"].(map[string]any); !ok {
			return nil
		}
	}
	return &ECCInfo{
		Correctable:   firstIntField(ecc, "total_correctable_count", "total_correctable", "correctable_count", "correctable"),
		Uncorrectable: firstIntField(ecc, "total_uncorrectable_count", "total_uncorrectable", "uncorrectable_count", "uncorrectable"),
		Deferred:      firstIntField(ecc, "total_deferred_count", "deferred_count", "deferred"),
	}
}

// parseXGMIBlock extracts an XGMI error status/count from the per-GPU
// "xgmi_err"/"xgmi" field, which may be a string status or an object of counts.
func parseXGMIBlock(obj map[string]any) *XGMIInfo {
	v, ok := obj["xgmi_err"]
	if !ok {
		v, ok = obj["xgmi"]
	}
	if !ok {
		return nil
	}
	switch t := v.(type) {
	case string:
		if t == "" || strings.EqualFold(t, "N/A") {
			return nil
		}
		return &XGMIInfo{Status: t}
	case map[string]any:
		x := &XGMIInfo{}
		for k, val := range t {
			if strings.Contains(strings.ToLower(k), "err") {
				x.ErrorCount += int(toFloat(val))
			}
		}
		return x
	}
	return nil
}

// firstIntField returns the first present numeric field among keys.
func firstIntField(m map[string]any, keys ...string) int {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return int(toFloat(v))
		}
	}
	return 0
}

// valueUnit renders a {"value":N,"unit":"U"} field as "N U", a bare number as
// "N <defUnit>", and missing/"N/A" as "-".
func valueUnit(m map[string]any, key, defUnit string) string {
	v, ok := m[key]
	if !ok {
		return "-"
	}
	switch t := v.(type) {
	case map[string]any:
		val, ok := t["value"]
		if !ok {
			return "-"
		}
		unit := defUnit
		if u, ok := t["unit"].(string); ok && u != "" {
			unit = u
		}
		return trimNum(toFloat(val)) + " " + unit
	case string:
		if t == "" || strings.EqualFold(t, "N/A") {
			return "-"
		}
		return t
	case float64, json.Number:
		return trimNum(toFloat(v)) + " " + defUnit
	}
	return "-"
}

func trimNum(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// parseGPUMetric parses `amd-smi metric --json`, which is either a list of GPU
// objects or an object wrapping {"gpu_data":[...]}. Each value field may be a
// bare number, a {"value":N,"unit":"..."} object, or "N/A".
func parseGPUMetric(raw string) ([]GPU, string) {
	js := extractJSON(raw)
	if js == "" {
		return nil, "no JSON in amd-smi output"
	}

	gpuList, perr := gpuObjects(js)
	if perr != "" {
		return nil, perr
	}

	gpus := make([]GPU, 0, len(gpuList))
	for _, obj := range gpuList {
		g := GPU{Index: intField(obj, "gpu")}

		if usage, ok := obj["usage"].(map[string]any); ok {
			g.UtilizationPct = numField(usage, "gfx_activity")
		}

		if mem, ok := obj["mem_usage"].(map[string]any); ok {
			g.MemTotalMB = numField(mem, "total_vram")
			g.MemUsedMB = numField(mem, "used_vram")
			if g.MemTotalMB > 0 {
				g.MemUsedPct = g.MemUsedMB / g.MemTotalMB * 100
			}
		}

		if temp, ok := obj["temperature"].(map[string]any); ok {
			g.TempEdgeC = numField(temp, "edge")
			g.TempHotspotC = numField(temp, "hotspot")
			g.TempMemC = numField(temp, "mem")
		}

		if power, ok := obj["power"].(map[string]any); ok {
			// amd-smi exposes socket_power (newer) or average_socket_power.
			g.PowerW = numField(power, "socket_power")
			if g.PowerW == 0 {
				g.PowerW = numField(power, "average_socket_power")
			}
		}

		gpus = append(gpus, g)
	}

	sort.Slice(gpus, func(i, j int) bool { return gpus[i].Index < gpus[j].Index })
	return gpus, ""
}

// gpuObjects extracts the list of per-GPU objects from either a top-level list
// or a {"gpu_data":[...]} wrapper.
func gpuObjects(js string) ([]map[string]any, string) {
	var list []map[string]any
	if err := json.Unmarshal([]byte(js), &list); err == nil {
		return list, ""
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(js), &obj); err != nil {
		return nil, "amd-smi output is not valid JSON"
	}
	if e, ok := obj["error"].(string); ok && e != "" {
		return nil, e
	}
	if gd, ok := obj["gpu_data"].([]any); ok {
		out := make([]map[string]any, 0, len(gd))
		for _, item := range gd {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out, ""
	}
	return nil, "no gpu_data in amd-smi output"
}

// numField extracts a numeric value from a field that may be a bare number, a
// {"value":N} object, a numeric string, or "N/A" (-> 0).
func numField(m map[string]any, key string) float64 {
	v, ok := m[key]
	if !ok {
		return 0
	}
	return toFloat(v)
}

func toFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case json.Number:
		f, _ := t.Float64()
		return f
	case string:
		s := strings.TrimSpace(t)
		if s == "" || strings.EqualFold(s, "N/A") {
			return 0
		}
		f, _ := strconv.ParseFloat(s, 64)
		return f
	case map[string]any:
		if val, ok := t["value"]; ok {
			return toFloat(val)
		}
	}
	return 0
}

func intField(m map[string]any, key string) int {
	return int(toFloat(m[key]))
}

// extractJSON trims any leading non-JSON noise (e.g. "WARNING:" banners that
// amd-smi prints when the user is not in the render/video groups) so the payload
// can be unmarshalled. Mirrors the probe package's preprocessing.
func extractJSON(s string) string {
	s = strings.TrimSpace(s)
	obj := strings.IndexByte(s, '{')
	arr := strings.IndexByte(s, '[')
	start := -1
	switch {
	case obj < 0:
		start = arr
	case arr < 0:
		start = obj
	case obj < arr:
		start = obj
	default:
		start = arr
	}
	if start < 0 {
		return ""
	}
	return s[start:]
}
