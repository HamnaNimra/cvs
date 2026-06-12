import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import {
  getLatestMetrics,
  listClusterNodes,
  streamClusterMetrics,
  type ClusterNodes,
  type MetricsSnapshot,
} from "@/shared/api";

interface ClusterState {
  nodes: ClusterNodes | null;
  nodesError: string | null;
  snapshot: MetricsSnapshot | null;
  live: boolean;
  setLive: (v: boolean) => void;
  connected: boolean;
  refreshing: boolean;
  reload: () => void;
}

const Ctx = createContext<ClusterState | null>(null);

// useCluster exposes the shared live snapshot + node grid to every cluster page,
// so the GPU/NIC/Nodes tabs all read one WebSocket stream and one node fetch.
export function useCluster(): ClusterState {
  const v = useContext(Ctx);
  if (!v) throw new Error("useCluster must be used within ClusterProvider");
  return v;
}

export function ClusterProvider({ children }: { children: ReactNode }) {
  const [nodes, setNodes] = useState<ClusterNodes | null>(null);
  const [nodesError, setNodesError] = useState<string | null>(null);
  const [snapshot, setSnapshot] = useState<MetricsSnapshot | null>(null);
  const [live, setLive] = useState(true);
  const [connected, setConnected] = useState(false);
  const [refreshing, setRefreshing] = useState(false);

  const reload = useCallback(async () => {
    setRefreshing(true);
    try {
      setNodes(await listClusterNodes());
      setNodesError(null);
    } catch (e) {
      setNodesError(String(e));
    } finally {
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    void reload();
    // Paint any cached snapshot immediately; the WS also sends latest on connect.
    getLatestMetrics()
      .then(setSnapshot)
      .catch(() => undefined);
  }, [reload]);

  // One shared subscription to the server-side metrics broadcast. The server
  // owns the poll cadence (default 60s) and pushes each GPU+NIC snapshot; we
  // also refresh the node grid on each frame so reachability stays current.
  useEffect(() => {
    if (!live) {
      setConnected(false);
      return;
    }
    const close = streamClusterMetrics(
      (snap) => {
        setConnected(true);
        setSnapshot(snap);
        void reload();
      },
      () => setConnected(false),
    );
    return close;
  }, [live, reload]);

  return (
    <Ctx.Provider
      value={{ nodes, nodesError, snapshot, live, setLive, connected, refreshing, reload }}
    >
      {children}
    </Ctx.Provider>
  );
}
