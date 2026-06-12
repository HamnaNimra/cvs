import { useCallback, useEffect, useState } from "react";
import { Loader2, RefreshCw } from "lucide-react";
import { getGpuSoftware, type GPUSoftwareSnapshotOrPending, type NodeGPUSoftware } from "@/shared/api";
import { fmt } from "./format";

function VersionTable({ node }: { node: NodeGPUSoftware }) {
  const v = node.version;
  if (!v) return <p className="mt-2 text-xs text-muted-foreground">No version data.</p>;
  const rows: [string, string][] = [
    ["ROCm", v.rocm_version],
    ["amdgpu", v.amdgpu_version],
    ["AMD SMI tool", v.amdsmi_tool],
    ["AMD SMI lib", v.amdsmi_library],
    ["HSMP", v.amd_hsmp_version],
  ];
  return (
    <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
      {rows.map(([k, val]) => (
        <div key={k} className="flex items-center justify-between gap-2">
          <dt className="text-muted-foreground">{k}</dt>
          <dd className="font-mono">{val || "N/A"}</dd>
        </div>
      ))}
    </dl>
  );
}

function FirmwareTable({ node }: { node: NodeGPUSoftware }) {
  const fw = node.firmware ?? [];
  if (fw.length === 0) return null;
  // Union of firmware component IDs across GPUs for stable rows.
  const ids = Array.from(
    fw.reduce((set, g) => {
      g.fw_list.forEach((e) => set.add(e.fw_id));
      return set;
    }, new Set<string>()),
  ).sort();
  const versionOf = (gpuIdx: number, id: string) =>
    fw[gpuIdx]?.fw_list.find((e) => e.fw_id === id)?.fw_version ?? "—";

  return (
    <div className="mt-3 overflow-x-auto">
      <table className="w-full text-xs">
        <thead className="text-left text-muted-foreground">
          <tr>
            <th className="py-1 pr-2 font-medium">Component</th>
            {fw.map((g) => (
              <th key={g.gpu} className="py-1 pr-2 font-medium">
                GPU {g.gpu}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="font-mono">
          {ids.map((id) => (
            <tr key={id} className="border-t border-border/50">
              <td className="py-1 pr-2">{id}</td>
              {fw.map((_, i) => (
                <td key={i} className="py-1 pr-2">
                  {versionOf(i, id)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function GpuSoftware() {
  const [snap, setSnap] = useState<GPUSoftwareSnapshotOrPending | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      setSnap(await getGpuSoftware());
      setError(null);
    } catch (e) {
      setError(String(e));
    } finally {
      if (!silent) setLoading(false);
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  // Poll every 10s while the server is still collecting in the background.
  useEffect(() => {
    if (!snap?.collecting) return;
    const id = setInterval(() => void load(true), 10_000);
    return () => clearInterval(id);
  }, [snap?.collecting, load]);

  return (
    <div>
      <div className="mb-4 flex items-center gap-3">
        <p className="text-sm text-muted-foreground">
          ROCm/driver versions and per-GPU firmware from{" "}
          <code className="font-mono text-xs">amd-smi version/firmware --json</code>. Cached ~180s.
          {snap?.collected_at && <span className="ml-1">Collected {fmt(snap.collected_at)}.</span>}
        </p>
        <button
          type="button"
          onClick={() => void load()}
          disabled={loading}
          className="ml-auto inline-flex items-center gap-1.5 rounded-lg border border-border px-3 py-1.5 text-sm hover:border-primary hover:text-primary disabled:opacity-50"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {error && (
        <div className="mb-4 rounded-lg border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
          {error}
        </div>
      )}

      {(loading || snap?.collecting) && (
        <p className="mb-4 flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          {snap?.collecting
            ? "Collecting GPU software from nodes in the background… refreshing automatically."
            : "Loading…"}
        </p>
      )}

      {snap && snap.nodes.length > 0 && (
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
          {snap.nodes.map((n) => (
            <div key={n.host} className="rounded-xl border border-border bg-card p-4">
              <span className="font-mono text-sm font-medium">{n.host}</span>
              {n.error ? (
                <p className="mt-2 text-xs text-destructive">{n.error}</p>
              ) : (
                <>
                  <VersionTable node={n} />
                  <FirmwareTable node={n} />
                </>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
