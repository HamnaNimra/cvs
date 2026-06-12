import { useCallback, useEffect, useState } from "react";
import { Loader2, RefreshCw, Search } from "lucide-react";
import {
  getDmesgLogs,
  searchLogs,
  type LogsSnapshot,
  type NodeLogs,
  type SearchResponse,
} from "@/shared/api";
import { fmt } from "./format";

function LogBucket({ title, body }: { title: string; body: string }) {
  const clean = body.trim() === "";
  return (
    <details className="mt-2 rounded-lg border border-border/60">
      <summary className="flex cursor-pointer items-center justify-between px-3 py-1.5 text-xs font-medium">
        <span>{title}</span>
        <span className={clean ? "text-green-600" : "text-amber-600"}>
          {clean ? "clean" : `${body.split("\n").length} lines`}
        </span>
      </summary>
      {!clean && (
        <pre className="max-h-64 overflow-auto border-t border-border/60 bg-muted/40 px-3 py-2 text-[11px] leading-relaxed">
          {body}
        </pre>
      )}
    </details>
  );
}

function NodeLogsCard({ node }: { node: NodeLogs }) {
  return (
    <div className="rounded-xl border border-border bg-card p-4">
      <span className="font-mono text-sm font-medium">{node.host}</span>
      {node.error ? (
        <p className="mt-2 text-xs text-destructive">{node.error}</p>
      ) : (
        <>
          <LogBucket title="AMD hardware / driver" body={node.amd_logs} />
          <LogBucket title="System errors (dmesg)" body={node.dmesg_errors} />
          <LogBucket title="Userspace / ML errors" body={node.userspace_errors} />
        </>
      )}
    </div>
  );
}

function SearchPanel() {
  const [grep, setGrep] = useState("grep -i error");
  const [res, setRes] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const run = useCallback(async () => {
    setLoading(true);
    try {
      setRes(await searchLogs(grep));
      setError(null);
    } catch (e) {
      setError(String(e instanceof Error ? e.message : e));
      setRes(null);
    } finally {
      setLoading(false);
    }
  }, [grep]);

  return (
    <div className="mb-6 rounded-xl border border-border bg-card p-4">
      <h3 className="mb-1 text-sm font-semibold">Search dmesg</h3>
      <p className="mb-3 text-xs text-muted-foreground">
        Pipe of <code className="font-mono">grep</code>/<code className="font-mono">egrep</code>{" "}
        segments only (validated server-side); first 5 matches per node.
      </p>
      <div className="flex gap-2">
        <input
          value={grep}
          onChange={(e) => setGrep(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") void run();
          }}
          placeholder="grep -i 'xgmi' | grep -v warn"
          className="flex-1 rounded-lg border border-border bg-background px-3 py-1.5 font-mono text-sm focus:border-primary focus:outline-none"
        />
        <button
          type="button"
          onClick={() => void run()}
          disabled={loading}
          className="inline-flex items-center gap-1.5 rounded-lg bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
        >
          {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Search className="h-3.5 w-3.5" />}
          Search
        </button>
      </div>

      {error && <p className="mt-3 text-xs text-destructive">{error}</p>}

      {res && (
        <div className="mt-3">
          <p className="mb-2 text-xs text-muted-foreground">
            {res.nodes_with_results}/{res.total_nodes_searched} nodes matched
          </p>
          <div className="space-y-2">
            {res.results
              .filter((r) => r.output.trim() !== "")
              .map((r) => (
                <div key={r.host} className="rounded-lg border border-border/60">
                  <div className="px-3 py-1.5 font-mono text-xs font-medium">{r.host}</div>
                  <pre className="max-h-48 overflow-auto border-t border-border/60 bg-muted/40 px-3 py-2 text-[11px]">
                    {r.output}
                  </pre>
                </div>
              ))}
            {res.nodes_with_results === 0 && (
              <p className="text-xs text-muted-foreground">No matches on any node.</p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export default function LogsPage() {
  const [snap, setSnap] = useState<LogsSnapshot | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      setSnap(await getDmesgLogs());
      setError(null);
    } catch (e) {
      setError(String(e instanceof Error ? e.message : e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <div>
      <SearchPanel />

      <div className="mb-4 flex items-center gap-3">
        <p className="text-sm text-muted-foreground">
          AMD hardware, system, and userspace/ML error logs from{" "}
          <code className="font-mono text-xs">dmesg</code> across the fleet.
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

      {snap === null && !error && (
        <div className="flex items-center gap-2 text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Collecting…
        </div>
      )}

      {snap && snap.nodes.length > 0 && (
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
          {snap.nodes.map((n) => (
            <NodeLogsCard key={n.host} node={n} />
          ))}
        </div>
      )}
    </div>
  );
}
