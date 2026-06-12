import { useCallback, useEffect, useState } from "react";
import { Loader2, RefreshCw } from "lucide-react";
import { getNicDevlink, type DevlinkSnapshot, type NodeDevlink } from "@/shared/api";
import { fmt } from "./format";

function DeviceTable({ node }: { node: NodeDevlink }) {
  const devs = node.devices ?? [];
  if (devs.length === 0) return <p className="mt-2 text-xs text-muted-foreground">No devices.</p>;
  return (
    <div className="mt-2 overflow-x-auto">
      <table className="w-full text-xs">
        <thead className="text-left text-muted-foreground">
          <tr>
            <th className="py-1 pr-2 font-medium">PCI</th>
            <th className="py-1 pr-2 font-medium">Vendor</th>
            <th className="py-1 pr-2 font-medium">Driver</th>
            <th className="py-1 pr-2 font-medium">FW</th>
            <th className="py-1 font-medium">Serial</th>
          </tr>
        </thead>
        <tbody className="font-mono">
          {devs.map((d) => (
            <tr key={d.pci_address} className="border-t border-border/50">
              <td className="py-1 pr-2">{d.pci_address}</td>
              <td className="py-1 pr-2">{d.vendor}</td>
              <td className="py-1 pr-2">{d.driver}</td>
              <td className="py-1 pr-2">{d.fw_version}</td>
              <td className="py-1">{d.serial_number}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function NicSoftware() {
  const [snap, setSnap] = useState<DevlinkSnapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      const data = await getNicDevlink();
      setSnap(data);
      setError(null);
    } catch (e) {
      setError(String(e));
    } finally {
      if (!silent) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // Poll every 10s while the server is still collecting.
  useEffect(() => {
    if (!snap?.collecting) return;
    const id = setInterval(() => void load(true), 10_000);
    return () => clearInterval(id);
  }, [snap?.collecting, load]);

  return (
    <div>
      <div className="mb-4 flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <p className="text-sm text-muted-foreground">
            NIC firmware and device info from{" "}
            <code className="font-mono text-xs">devlink dev info --json</code> (per-host, capped at 20s). Results are
            cached on the server for <strong>180s</strong> (same as cluster-mon).
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            <strong>Why the first load can take 1–2 minutes:</strong> a cold cache runs{" "}
            <code className="font-mono">devlink dev info</code> (capped at 20s/node) over all inventory hosts in
            parallel. Opening NIC SW again within three minutes returns the cached snapshot instantly.
          </p>
          {snap?.collected_at && (
            <p className="mt-1 text-xs text-muted-foreground">Last snapshot: {fmt(snap.collected_at)}.</p>
          )}
        </div>
        <button
          type="button"
          onClick={() => void load()}
          disabled={loading}
          className="inline-flex shrink-0 items-center gap-1.5 self-start rounded-lg border border-border px-3 py-1.5 text-sm hover:border-primary hover:text-primary disabled:opacity-50"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {(loading || snap?.collecting) && (
        <p className="mb-4 flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          {snap?.collecting
            ? "Collecting devlink from nodes in the background… refreshing automatically."
            : "Loading…"}
        </p>
      )}

      {error && (
        <div className="mb-4 rounded-lg border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
          {error}
        </div>
      )}

      {!loading && snap && snap.nodes.length === 0 && !error && (
        <p className="text-sm text-muted-foreground">No devlink data (empty inventory or collection returned no nodes).</p>
      )}

      {snap && snap.nodes.length > 0 && (
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
          {snap.nodes.map((n) => (
            <div key={n.host} className="rounded-xl border border-border bg-card p-4">
              <span className="font-mono text-sm font-medium">{n.host}</span>
              {n.error ? <p className="mt-2 text-xs text-destructive">{n.error}</p> : <DeviceTable node={n} />}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
