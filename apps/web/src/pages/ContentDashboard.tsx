import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { bulkUpdateContentTasks, createContentTask, getContentDataQuality, getContentImpact, getContentTasks, runContentOnboardPreflight } from "@/lib/api";
import { ClientExportButton } from "@/components/shared";

export default function ContentDashboardPage() {
  const qc = useQueryClient();
  const [status, setStatus] = useState("open");
  const [skuSearch, setSkuSearch] = useState("");
  const [newSku, setNewSku] = useState("");
  const [preflightSkus, setPreflightSkus] = useState("");
  const [selectedTaskIds, setSelectedTaskIds] = useState<string[]>([]);
  const [bulkStatus, setBulkStatus] = useState<"open" | "investigating" | "resolved">("investigating");
  const [impactSku, setImpactSku] = useState("");
  const [impactMarket, setImpactMarket] = useState("DE");
  const [impactRange, setImpactRange] = useState(14);

  const tasksQuery = useQuery({
    queryKey: ["content-dashboard-tasks", status, skuSearch],
    queryFn: () =>
      getContentTasks({
        page: 1,
        page_size: 50,
        ...(status ? { status } : {}),
        ...(skuSearch.trim() ? { sku_search: skuSearch.trim() } : {}),
      }),
  });

  const createTaskMutation = useMutation({
    mutationFn: () =>
      createContentTask({
        type: "refresh_content",
        sku: newSku.trim(),
        priority: "p1",
        source_page: "content_dashboard",
      }),
    onSuccess: () => {
      setNewSku("");
      qc.invalidateQueries({ queryKey: ["content-dashboard-tasks"] });
    },
  });

  const preflightMutation = useMutation({
    mutationFn: () => {
      const sku_list = preflightSkus
        .split(/\r?\n|,/)
        .map((x) => x.trim())
        .filter(Boolean);
      return runContentOnboardPreflight({
        sku_list,
        main_market: "DE",
        target_markets: ["FR", "IT", "ES", "NL", "PL", "SE", "BE"],
        auto_create_tasks: true,
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["content-dashboard-tasks"] }),
  });
  const impactMutation = useMutation({
    mutationFn: () =>
      getContentImpact({
        sku: impactSku.trim(),
        marketplace: impactMarket.trim().toUpperCase(),
        range: impactRange,
      }),
  });
  const qualityQuery = useQuery({
    queryKey: ["content-data-quality"],
    queryFn: getContentDataQuality,
  });
  const bulkMutation = useMutation({
    mutationFn: () => bulkUpdateContentTasks({ task_ids: selectedTaskIds, status: bulkStatus }),
    onSuccess: () => {
      setSelectedTaskIds([]);
      qc.invalidateQueries({ queryKey: ["content-dashboard-tasks"] });
    },
  });

  const summary = useMemo(() => {
    const rows = tasksQuery.data?.items ?? [];
    return {
      total: tasksQuery.data?.total ?? 0,
      p0: rows.filter((r) => r.priority === "p0").length,
      open: rows.filter((r) => r.status === "open").length,
      investigating: rows.filter((r) => r.status === "investigating").length,
    };
  }, [tasksQuery.data]);
  const releaseCalendar = useMemo(() => {
    const rows = tasksQuery.data?.items ?? [];
    const bucket: Record<string, { count: number; p0: number; skus: Set<string> }> = {};
    for (const row of rows) {
      if (!row.due_date) continue;
      const day = row.due_date.slice(0, 10);
      if (!bucket[day]) bucket[day] = { count: 0, p0: 0, skus: new Set<string>() };
      bucket[day].count += 1;
      if (row.priority === "p0") bucket[day].p0 += 1;
      bucket[day].skus.add(row.sku);
    }
    return Object.entries(bucket)
      .map(([day, v]) => ({ day, count: v.count, p0: v.p0, skuCount: v.skus.size }))
      .sort((a, b) => a.day.localeCompare(b.day))
      .slice(0, 14);
  }, [tasksQuery.data]);
  const ownerLoad = useMemo(() => {
    const rows = tasksQuery.data?.items ?? [];
    const bucket: Record<string, { total: number; open: number; investigating: number; overdue: number }> = {};
    for (const row of rows) {
      const owner = row.owner || "unassigned";
      if (!bucket[owner]) bucket[owner] = { total: 0, open: 0, investigating: 0, overdue: 0 };
      bucket[owner].total += 1;
      if (row.status === "open") bucket[owner].open += 1;
      if (row.status === "investigating") bucket[owner].investigating += 1;
      if (row.due_date && row.due_date.slice(0, 10) < new Date().toISOString().slice(0, 10) && row.status !== "resolved") {
        bucket[owner].overdue += 1;
      }
    }
    return Object.entries(bucket)
      .map(([owner, x]) => ({ owner, ...x }))
      .sort((a, b) => b.overdue - a.overdue || b.total - a.total)
      .slice(0, 12);
  }, [tasksQuery.data]);

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold">Content Dashboard</h1>
        <p className="text-sm text-muted-foreground">Backlog + onboarding gate shortcuts</p>
      </div>

      <div className="grid gap-2 md:grid-cols-4">
        <div className="rounded border border-border bg-card p-3 text-xs">Total: {summary.total}</div>
        <div className="rounded border border-border bg-card p-3 text-xs">P0: {summary.p0}</div>
        <div className="rounded border border-border bg-card p-3 text-xs">Open: {summary.open}</div>
        <div className="rounded border border-border bg-card p-3 text-xs">Investigating: {summary.investigating}</div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Data Quality</h2>
        <div className="grid gap-2 md:grid-cols-3">
          {(qualityQuery.data?.cards ?? []).map((c) => (
            <div key={c.key} className="rounded border border-border p-2 text-xs">
              <div className="font-medium">{c.key}</div>
              <div>{c.value.toFixed(2)}{c.unit === "pct" ? "%" : ""}</div>
            </div>
          ))}
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Quick Task</h2>
        <div className="flex gap-2">
          <input value={newSku} onChange={(e) => setNewSku(e.target.value)} placeholder="SKU" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
          <button
            onClick={() => createTaskMutation.mutate()}
            disabled={!newSku.trim() || createTaskMutation.isPending}
            className="rounded border border-border px-3 py-1 text-xs disabled:opacity-40"
          >
            Add
          </button>
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Release Calendar (next due dates)</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Date</th>
                <th className="px-2 py-2">Tasks</th>
                <th className="px-2 py-2">P0</th>
                <th className="px-2 py-2">SKUs</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {releaseCalendar.map((x) => (
                <tr key={x.day} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5">{x.day}</td>
                  <td className="px-2 py-1.5">{x.count}</td>
                  <td className="px-2 py-1.5">{x.p0}</td>
                  <td className="px-2 py-1.5">{x.skuCount}</td>
                </tr>
              ))}
              {releaseCalendar.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-2 py-4 text-center text-muted-foreground">No due dates on current filter</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Owner Load / Overload</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Owner</th>
                <th className="px-2 py-2">Total</th>
                <th className="px-2 py-2">Open</th>
                <th className="px-2 py-2">Investigating</th>
                <th className="px-2 py-2">Overdue</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {ownerLoad.map((x) => (
                <tr key={x.owner} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5">{x.owner}</td>
                  <td className="px-2 py-1.5">{x.total}</td>
                  <td className="px-2 py-1.5">{x.open}</td>
                  <td className="px-2 py-1.5">{x.investigating}</td>
                  <td className="px-2 py-1.5">{x.overdue}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Impact Margin 7/14/30</h2>
        <div className="grid gap-2 md:grid-cols-4">
          <input value={impactSku} onChange={(e) => setImpactSku(e.target.value)} placeholder="SKU" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={impactMarket} onChange={(e) => setImpactMarket(e.target.value)} placeholder="Marketplace" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <select value={impactRange} onChange={(e) => setImpactRange(Number(e.target.value))} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value={7}>7d</option>
            <option value={14}>14d</option>
            <option value={30}>30d</option>
          </select>
          <button onClick={() => impactMutation.mutate()} disabled={!impactSku.trim()} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Check</button>
        </div>
        {impactMutation.data && (
          <div className="rounded border border-border p-2 text-xs">
            signal: {impactMutation.data.impact_signal} | confidence: {impactMutation.data.confidence_score.toFixed(2)}%
            <br />
            before impact margin: {impactMutation.data.before.impact_margin_pln.toFixed(2)} | after impact margin: {impactMutation.data.after.impact_margin_pln.toFixed(2)} | delta: {impactMutation.data.delta.impact_margin_pln.toFixed(2)}
            <br />
            baseline impact margin: {impactMutation.data.baseline_expected.impact_margin_pln.toFixed(2)} | delta vs baseline: {impactMutation.data.delta_vs_baseline.impact_margin_pln.toFixed(2)}
            <br />
            metric note: snapshot margin = revenue - cogs - transport, not canonical CM1
          </div>
        )}
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Preflight (auto-create tasks)</h2>
        <textarea
          rows={3}
          value={preflightSkus}
          onChange={(e) => setPreflightSkus(e.target.value)}
          placeholder="SKU list"
          className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
        />
        <button
          onClick={() => preflightMutation.mutate()}
          disabled={!preflightSkus.trim() || preflightMutation.isPending}
          className="rounded border border-border px-3 py-1 text-xs disabled:opacity-40"
        >
          Run gate
        </button>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <div className="flex gap-2">
          <select value={status} onChange={(e) => setStatus(e.target.value)} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="">all</option>
            <option value="open">open</option>
            <option value="investigating">investigating</option>
            <option value="resolved">resolved</option>
          </select>
          <input value={skuSearch} onChange={(e) => setSkuSearch(e.target.value)} placeholder="SKU search" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
        </div>
        <div className="flex gap-2">
          <select value={bulkStatus} onChange={(e) => setBulkStatus(e.target.value as "open" | "investigating" | "resolved")} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="open">open</option>
            <option value="investigating">investigating</option>
            <option value="resolved">resolved</option>
          </select>
          <button
            onClick={() => bulkMutation.mutate()}
            disabled={selectedTaskIds.length === 0 || bulkMutation.isPending}
            className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40"
          >
            Bulk update ({selectedTaskIds.length})
          </button>
          <ClientExportButton data={tasksQuery.data?.items ?? []} filename="content_dashboard_tasks" />
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">
                  <input
                    type="checkbox"
                    checked={(tasksQuery.data?.items ?? []).length > 0 && selectedTaskIds.length === (tasksQuery.data?.items ?? []).length}
                    onChange={(e) => {
                      const rows = tasksQuery.data?.items ?? [];
                      if (e.target.checked) {
                        setSelectedTaskIds(rows.map((r) => r.id));
                      } else {
                        setSelectedTaskIds([]);
                      }
                    }}
                  />
                </th>
                <th className="px-2 py-2">Type</th>
                <th className="px-2 py-2">SKU</th>
                <th className="px-2 py-2">Status</th>
                <th className="px-2 py-2">Priority</th>
                <th className="px-2 py-2">Owner</th>
                <th className="px-2 py-2">Updated</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(tasksQuery.data?.items ?? []).map((row) => (
                <tr key={row.id} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5">
                    <input
                      type="checkbox"
                      checked={selectedTaskIds.includes(row.id)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedTaskIds((prev) => Array.from(new Set([...prev, row.id])));
                        } else {
                          setSelectedTaskIds((prev) => prev.filter((id) => id !== row.id));
                        }
                      }}
                    />
                  </td>
                  <td className="px-2 py-1.5">{row.type}</td>
                  <td className="px-2 py-1.5 font-mono">{row.sku}</td>
                  <td className="px-2 py-1.5">{row.status}</td>
                  <td className="px-2 py-1.5">{row.priority}</td>
                  <td className="px-2 py-1.5">{row.owner ?? "-"}</td>
                  <td className="px-2 py-1.5 text-muted-foreground">{row.updated_at.slice(0, 19).replace("T", " ")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
