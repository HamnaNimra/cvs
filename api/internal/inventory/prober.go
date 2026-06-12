package inventory

import (
	"context"
	"time"

	"github.com/ROCm/cvs/api/internal/probe"
	"github.com/ROCm/cvs/api/internal/ssh"
)

// Prober runs connectivity + basic-info collection for an inventory and returns
// the per-node statuses. It is the F2 capability that populates reachability,
// GPU type/count, and ROCm version for the tiles.
type Prober interface {
	Probe(ctx context.Context, inv Inventory) ([]NodeStatus, error)
}

// SSHProber implements Prober: a parallel TCP sweep plus, when SSH credentials
// are available (key auth), an amd-smi/rocm-smi basic-info collection over a
// shared SSH pool. Password-auth inventories get TCP-only status because F1 does
// not persist passwords.
type SSHProber struct {
	keys        *KeyStore
	tcpTimeout  time.Duration
	maxParallel int
}

// NewSSHProber returns a prober that resolves key files via keys.
func NewSSHProber(keys *KeyStore) *SSHProber {
	return &SSHProber{keys: keys, tcpTimeout: probe.DefaultTCPTimeout, maxParallel: 16}
}

// Probe sweeps reachability for every node, collects basic info on the reachable
// ones (when SSH is possible), and returns one NodeStatus per node.
func (p *SSHProber) Probe(ctx context.Context, inv Inventory) ([]NodeStatus, error) {
	reachable, _ := probe.TCPAll(inv.Nodes, probe.SSHPort, p.tcpTimeout, 0)
	reachSet := make(map[string]struct{}, len(reachable))
	for _, h := range reachable {
		reachSet[h] = struct{}{}
	}

	var infos map[string]probe.Info
	if pool, err := NewSSHPool(inv, p.keys, p.maxParallel); err == nil && pool != nil {
		defer pool.Close()
		infos = probe.Collect(ctx, pool, reachable)
	}

	now := time.Now().UTC()
	statuses := make([]NodeStatus, 0, len(inv.Nodes))
	for _, host := range inv.Nodes {
		st := NodeStatus{Host: host, CheckedAt: now}
		if _, ok := reachSet[host]; ok {
			st.Reachable = true
			if info, found := infos[host]; found {
				st.GPUType = info.GPUType
				st.GPUCount = info.GPUCount
				st.ROCmVersion = info.ROCmVersion
				st.Error = info.Err
			}
		} else {
			st.Error = "tcp: port 22 unreachable"
		}
		statuses = append(statuses, st)
	}
	return statuses, nil
}

// NewSSHPool constructs a shared SSH pool for a key-auth inventory. It returns
// (nil, nil) when no usable SSH credentials exist (password auth, or no key),
// so callers can fall back to a TCP-only / no-SSH path. maxParallel<=0 uses the
// pool default. Reused by the F2 probe and the Cluster Monitor collectors.
func NewSSHPool(inv Inventory, keys *KeyStore, maxParallel int) (*ssh.Pool, error) {
	if keys == nil || inv.AuthMethod != AuthKey || inv.KeyName == "" {
		return nil, nil
	}
	keyPath, err := keys.Path(inv.KeyName)
	if err != nil {
		return nil, err
	}
	cfg := ssh.Config{
		User:        inv.Username,
		KeyFile:     keyPath,
		MaxParallel: maxParallel,
	}
	if jh := inv.JumpHost; jh != nil && jh.Host != "" {
		jc := &ssh.JumpConfig{Host: jh.Host, User: jh.Username}
		if jh.KeyName != "" {
			if jp, err := keys.Path(jh.KeyName); err == nil {
				jc.KeyFile = jp
			}
		}
		cfg.JumpHost = jc
	}
	return ssh.NewPool(cfg)
}
