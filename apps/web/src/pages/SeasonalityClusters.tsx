import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Layers, Plus, RefreshCw } from "lucide-react";
import { getSeasonalityClusters, createSeasonalityCluster } from "@/lib/api";
import type { SeasonalityCluster } from "@/lib/api";
import { cn } from "@/lib/utils";

const MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

const CLASS_COLORS: Record<string, string> = {
  EVERGREEN: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  MILD_SEASONAL: "bg-blue-500/15 text-blue-400 border-blue-500/30",
  STRONG_SEASONAL: "bg-orange-500/15 text-orange-400 border-orange-500/30",
  PEAK_SEASONAL: "bg-red-500/15 text-red-400 border-red-500/30",
};

export default function SeasonalityClustersPage() {
  const qc = useQueryClient();
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDesc, setNewDesc] = useState("");

  const { data: clusters, isLoading } = useQuery({
    queryKey: ["seasonality-clusters"],
    queryFn: getSeasonalityClusters,
    staleTime: 5 * 60_000,
  });

  const createMut = useMutation({
    mutationFn: () => createSeasonalityCluster({
      cluster_name: newName,
      description: newDesc || undefined,
    }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["seasonality-clusters"] });
      setShowCreate(false);
      setNewName("");
      setNewDesc("");
    },
  });

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Layers className="h-6 w-6 text-amazon" />
          <h1 className="text-xl font-bold tracking-tight">Product Clusters</h1>
        </div>
        <button onClick={() => setShowCreate(!showCreate)}
          className="flex items-center gap-1.5 rounded-lg border border-amazon bg-amazon/10 px-3 py-1.5 text-xs font-medium text-amazon hover:bg-amazon/20">
          <Plus className="h-3.5 w-3.5" /> New Cluster
        </button>
      </div>

      {/* Create Form */}
      {showCreate && (
        <div className="rounded-xl border border-border bg-card p-4 space-y-3">
          <input type="text" placeholder="Cluster name…" value={newName}
            onChange={e => setNewName(e.target.value)}
            className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm" />
          <textarea placeholder="Description (optional)…" value={newDesc}
            onChange={e => setNewDesc(e.target.value)} rows={2}
            className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm" />
          <div className="flex gap-2">
            <button onClick={() => createMut.mutate()} disabled={!newName || createMut.isPending}
              className="rounded-lg bg-amazon px-4 py-1.5 text-xs font-medium text-white hover:bg-amazon/90 disabled:opacity-50">
              {createMut.isPending ? "Creating…" : "Create"}
            </button>
            <button onClick={() => setShowCreate(false)}
              className="rounded-lg border border-border px-4 py-1.5 text-xs">Cancel</button>
          </div>
        </div>
      )}

      {/* Clusters List */}
      <div className="rounded-xl border border-border bg-card overflow-hidden">
        {isLoading ? (
          <div className="p-8 text-sm text-muted-foreground animate-pulse">Loading clusters…</div>
        ) : (!clusters || clusters.length === 0) ? (
          <div className="p-8 text-sm text-muted-foreground text-center">
            No clusters yet. Create one to group products for seasonal analysis.
          </div>
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-muted-foreground">
                <th className="text-left px-3 py-2">Name</th>
                <th className="px-3 py-2 text-right">Members</th>
                <th className="px-3 py-2">Class</th>
                <th className="px-3 py-2 text-center">Peak</th>
                <th className="px-3 py-2 text-right">Confidence</th>
                <th className="px-3 py-2 text-left">Description</th>
                <th className="px-3 py-2">Created</th>
              </tr>
            </thead>
            <tbody>
              {clusters.map((c: SeasonalityCluster) => (
                <tr key={c.id} className="border-b border-border/30 hover:bg-muted/20">
                  <td className="px-3 py-2 font-medium">{c.cluster_name}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{c.members_count}</td>
                  <td className="px-3 py-2">
                    {c.seasonality_class ? (
                      <span className={cn("rounded-full border px-1.5 py-0.5 text-[9px] font-bold uppercase",
                        CLASS_COLORS[c.seasonality_class] || "")}>
                        {c.seasonality_class?.replace("_"," ")}
                      </span>
                    ) : "—"}
                  </td>
                  <td className="px-3 py-2 text-center text-[10px]">
                    {c.peak_months?.map(m => MONTH_NAMES[m - 1]).join(", ") || "—"}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums">
                    {c.confidence != null ? c.confidence.toFixed(0) : "—"}
                  </td>
                  <td className="px-3 py-2 text-muted-foreground max-w-[250px] truncate">
                    {c.description || "—"}
                  </td>
                  <td className="px-3 py-2 text-muted-foreground">{c.created_at?.slice(0, 10) ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
