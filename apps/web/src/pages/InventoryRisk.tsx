import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getInventoryRiskDashboard,
  getInventoryRiskScores,
  getStockoutWatchlist,
  getOverstockReport,
  triggerInventoryRiskCompute,
  getReplenishmentPlan,
  acknowledgeReplenishment,
  getRiskAlerts,
  resolveRiskAlert,
  getVelocityTrends,
  type InventoryRiskDashboard,
  type InventoryRiskScore,
  type InventoryRiskWatchlistItem,
  type InventoryRiskOverstockItem,
  type ReplenishmentPlanItem,
  type RiskAlert,
  type VelocityTrendDay,
} from "@/lib/api";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell, LineChart, Line, Legend,
} from "recharts";

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

function KpiCard({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="rounded-xl border bg-white p-4 shadow-sm">
      <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</p>
      <p className="mt-1 text-2xl font-bold text-gray-900">{value}</p>
      {sub && <p className="mt-0.5 text-xs text-gray-400">{sub}</p>}
    </div>
  );
}

const TIER_COLORS: Record<string, string> = {
  critical: "#ef4444",
  high: "#f97316",
  medium: "#eab308",
  low: "#22c55e",
};

function tierBadge(tier: string) {
  const bg = tier === "critical" ? "bg-red-100 text-red-700"
    : tier === "high" ? "bg-orange-100 text-orange-700"
    : tier === "medium" ? "bg-yellow-100 text-yellow-700"
    : "bg-green-100 text-green-700";
  return <span className={`rounded-full px-2 py-0.5 text-xs font-semibold ${bg}`}>{tier}</span>;
}

function urgencyBadge(urgency: string) {
  const bg = urgency === "critical" ? "bg-red-100 text-red-700"
    : urgency === "high" ? "bg-orange-100 text-orange-700"
    : urgency === "medium" ? "bg-yellow-100 text-yellow-700"
    : "bg-green-100 text-green-700";
  return <span className={`rounded-full px-2 py-0.5 text-xs font-semibold ${bg}`}>{urgency}</span>;
}

function trendBadge(trend: string, changePct: number | null) {
  const arrow = trend === "accelerating" ? "↑" : trend === "decelerating" ? "↓" : "→";
  const color = trend === "accelerating" ? "text-green-600"
    : trend === "decelerating" ? "text-red-600"
    : "text-gray-500";
  return <span className={`text-xs font-semibold ${color}`}>{arrow} {changePct != null ? `${changePct.toFixed(0)}%` : ""}</span>;
}

function severityBadge(severity: string) {
  const bg = severity === "critical" ? "bg-red-100 text-red-700"
    : severity === "warning" ? "bg-orange-100 text-orange-700"
    : "bg-blue-100 text-blue-700";
  return <span className={`rounded-full px-2 py-0.5 text-xs font-semibold ${bg}`}>{severity}</span>;
}

function pct(v: number) {
  return (v * 100).toFixed(1) + "%";
}

function pln(v: number) {
  return v.toLocaleString("pl-PL", { style: "currency", currency: "PLN", maximumFractionDigits: 0 });
}

/* ------------------------------------------------------------------ */
/*  Page                                                              */
/* ------------------------------------------------------------------ */

export default function InventoryRiskPage() {
  const [marketplace] = useState("ATVPDKIKX0DER");
  const [tab, setTab] = useState<"overview" | "replenishment" | "alerts">("overview");
  const [trendSku, setTrendSku] = useState<string | null>(null);
  const qc = useQueryClient();

  const { data: dashboard } = useQuery<InventoryRiskDashboard>({
    queryKey: ["inv-risk-dashboard", marketplace],
    queryFn: () => getInventoryRiskDashboard({ marketplace_id: marketplace }),
  });

  const { data: scoresResp } = useQuery({
    queryKey: ["inv-risk-scores", marketplace],
    queryFn: () => getInventoryRiskScores({ marketplace_id: marketplace, limit: 50, sort_by: "risk_score", sort_dir: "desc" }),
  });

  const { data: watchlistResp } = useQuery({
    queryKey: ["inv-risk-stockout", marketplace],
    queryFn: () => getStockoutWatchlist({ marketplace_id: marketplace, limit: 10 }),
  });

  const { data: overstockResp } = useQuery({
    queryKey: ["inv-risk-overstock", marketplace],
    queryFn: () => getOverstockReport({ marketplace_id: marketplace, limit: 10 }),
  });

  /* Sprint 14 queries */
  const { data: planResp } = useQuery({
    queryKey: ["inv-risk-plan", marketplace],
    queryFn: () => getReplenishmentPlan({ marketplace_id: marketplace, limit: 50 }),
    enabled: tab === "replenishment",
  });

  const { data: alertsResp } = useQuery({
    queryKey: ["inv-risk-alerts", marketplace],
    queryFn: () => getRiskAlerts({ marketplace_id: marketplace, limit: 50 }),
    enabled: tab === "alerts",
  });

  const { data: trendResp } = useQuery({
    queryKey: ["inv-risk-trends", trendSku, marketplace],
    queryFn: () => getVelocityTrends(trendSku!, marketplace, 30),
    enabled: !!trendSku,
  });

  const computeMut = useMutation({
    mutationFn: () => triggerInventoryRiskCompute(marketplace),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["inv-risk-dashboard"] }),
  });

  const ackMut = useMutation({
    mutationFn: (item: ReplenishmentPlanItem) =>
      acknowledgeReplenishment(item.seller_sku, item.marketplace_id, "user"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["inv-risk-plan"] }),
  });

  const resolveMut = useMutation({
    mutationFn: (id: number) => resolveRiskAlert(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["inv-risk-alerts"] }),
  });

  const scores = scoresResp?.items ?? [];
  const watchlist = watchlistResp?.items ?? [];
  const overstock = overstockResp?.items ?? [];
  const planItems = planResp?.items ?? [];
  const alerts = alertsResp?.items ?? [];
  const trends = trendResp?.trends ?? [];

  /* Bar chart data: tier distribution */
  const tierChart = dashboard
    ? [
        { tier: "Critical", count: dashboard.critical, fill: TIER_COLORS.critical },
        { tier: "High", count: dashboard.high, fill: TIER_COLORS.high },
        { tier: "Medium", count: dashboard.medium, fill: TIER_COLORS.medium },
        { tier: "Low", count: dashboard.low, fill: TIER_COLORS.low },
      ]
    : [];

  return (
    <div className="space-y-6 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-bold text-gray-900">Inventory Risk Engine</h1>
          <p className="text-sm text-gray-500">Stockout probability · Overstock cost · Aging write-off risk · Replenishment</p>
        </div>
        <button
          className="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
          disabled={computeMut.isPending}
          onClick={() => computeMut.mutate()}
        >
          {computeMut.isPending ? "Computing…" : "Compute Now"}
        </button>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 rounded-lg border bg-gray-50 p-1">
        {(["overview", "replenishment", "alerts"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`rounded-md px-4 py-1.5 text-sm font-medium capitalize transition ${
              tab === t ? "bg-white shadow text-gray-900" : "text-gray-500 hover:text-gray-700"
            }`}
          >
            {t}
          </button>
        ))}
      </div>

      {/* ============ OVERVIEW TAB ============ */}
      {tab === "overview" && (
        <>
          {/* KPI Cards */}
          <div className="grid grid-cols-2 gap-4 sm:grid-cols-4 lg:grid-cols-6">
            <KpiCard label="Total SKUs" value={dashboard?.total_skus ?? "—"} />
            <KpiCard label="Critical" value={dashboard?.critical ?? 0} sub="risk >= 70" />
            <KpiCard label="High" value={dashboard?.high ?? 0} sub="risk >= 50" />
            <KpiCard label="Avg Score" value={dashboard?.avg_risk_score?.toFixed(1) ?? "—"} sub="0–100" />
            <KpiCard label="Overstock Cost" value={dashboard ? pln(dashboard.total_holding_cost_pln) : "—"} sub="holding cost" />
            <KpiCard label="Aging Risk" value={dashboard ? pln(dashboard.total_aging_risk_pln) : "—"} sub="write-off exposure" />
          </div>

          {/* Tier distribution chart */}
          {tierChart.length > 0 && (
            <div className="rounded-xl border bg-white p-4 shadow-sm">
              <h2 className="mb-3 text-sm font-semibold text-gray-700">Risk Tier Distribution</h2>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={tierChart}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="tier" tick={{ fontSize: 12 }} />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Bar dataKey="count" name="SKUs">
                    {tierChart.map((e, i) => (
                      <Cell key={i} fill={e.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Two-column: Stockout watchlist + Overstock report */}
          <div className="grid gap-6 lg:grid-cols-2">
            <div className="rounded-xl border bg-white shadow-sm">
              <div className="border-b px-4 py-3">
                <h2 className="text-sm font-semibold text-gray-700">Stockout Watchlist</h2>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead className="border-b bg-gray-50 text-xs font-medium uppercase text-gray-500">
                    <tr>
                      <th className="px-4 py-2">SKU</th>
                      <th className="px-4 py-2">P(7d)</th>
                      <th className="px-4 py-2">P(14d)</th>
                      <th className="px-4 py-2">Units</th>
                      <th className="px-4 py-2">Days Cover</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    {watchlist.map((r) => (
                      <tr key={r.seller_sku} className="hover:bg-gray-50">
                        <td className="px-4 py-2 font-mono text-xs">{r.seller_sku}</td>
                        <td className="px-4 py-2 font-semibold text-red-600">{pct(r.stockout_prob_7d ?? 0)}</td>
                        <td className="px-4 py-2">{pct(r.stockout_prob_14d ?? 0)}</td>
                        <td className="px-4 py-2">{r.units_available}</td>
                        <td className="px-4 py-2">{r.days_cover?.toFixed(0) ?? "—"}</td>
                      </tr>
                    ))}
                    {watchlist.length === 0 && (
                      <tr><td colSpan={5} className="px-4 py-6 text-center text-gray-400">No data</td></tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="rounded-xl border bg-white shadow-sm">
              <div className="border-b px-4 py-3">
                <h2 className="text-sm font-semibold text-gray-700">Highest Holding Cost</h2>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead className="border-b bg-gray-50 text-xs font-medium uppercase text-gray-500">
                    <tr>
                      <th className="px-4 py-2">SKU</th>
                      <th className="px-4 py-2">Holding</th>
                      <th className="px-4 py-2">Excess</th>
                      <th className="px-4 py-2">Excess Val</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    {overstock.map((r) => (
                      <tr key={r.seller_sku} className="hover:bg-gray-50">
                        <td className="px-4 py-2 font-mono text-xs">{r.seller_sku}</td>
                        <td className="px-4 py-2 font-semibold">{pln(r.overstock_holding_cost_pln ?? 0)}</td>
                        <td className="px-4 py-2">{r.excess_units}</td>
                        <td className="px-4 py-2">{pln(r.excess_value_pln ?? 0)}</td>
                      </tr>
                    ))}
                    {overstock.length === 0 && (
                      <tr><td colSpan={4} className="px-4 py-6 text-center text-gray-400">No data</td></tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* Full risk scores table */}
          <div className="rounded-xl border bg-white shadow-sm">
            <div className="border-b px-4 py-3">
              <h2 className="text-sm font-semibold text-gray-700">All Risk Scores</h2>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="border-b bg-gray-50 text-xs font-medium uppercase text-gray-500">
                  <tr>
                    <th className="px-3 py-2">SKU</th>
                    <th className="px-3 py-2">Score</th>
                    <th className="px-3 py-2">Tier</th>
                    <th className="px-3 py-2">P(7d)</th>
                    <th className="px-3 py-2">P(30d)</th>
                    <th className="px-3 py-2">Overstock</th>
                    <th className="px-3 py-2">Aging</th>
                    <th className="px-3 py-2">Units</th>
                    <th className="px-3 py-2">Vel 30d</th>
                    <th className="px-3 py-2">Days Cover</th>
                  </tr>
                </thead>
                <tbody className="divide-y">
                  {scores.map((r) => (
                    <tr key={r.seller_sku} className="hover:bg-gray-50">
                      <td className="px-3 py-2 font-mono text-xs">{r.seller_sku}</td>
                      <td className="px-3 py-2 font-bold">{r.risk_score.toFixed(1)}</td>
                      <td className="px-3 py-2">{tierBadge(r.risk_tier)}</td>
                      <td className="px-3 py-2">{pct(r.stockout_prob_7d ?? 0)}</td>
                      <td className="px-3 py-2">{pct(r.stockout_prob_30d ?? 0)}</td>
                      <td className="px-3 py-2">{pln(r.overstock_holding_cost_pln ?? 0)}</td>
                      <td className="px-3 py-2">{pln(r.aging_risk_pln ?? 0)}</td>
                      <td className="px-3 py-2">{r.units_available}</td>
                      <td className="px-3 py-2">{r.velocity_30d?.toFixed(1)}</td>
                      <td className="px-3 py-2">{r.days_cover?.toFixed(0) ?? "—"}</td>
                    </tr>
                  ))}
                  {scores.length === 0 && (
                    <tr><td colSpan={10} className="px-4 py-6 text-center text-gray-400">No risk scores yet — click Compute Now</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}

      {/* ============ REPLENISHMENT TAB ============ */}
      {tab === "replenishment" && (
        <>
          <div className="rounded-xl border bg-white shadow-sm">
            <div className="border-b px-4 py-3">
              <h2 className="text-sm font-semibold text-gray-700">Replenishment Plan</h2>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="border-b bg-gray-50 text-xs font-medium uppercase text-gray-500">
                  <tr>
                    <th className="px-3 py-2">SKU</th>
                    <th className="px-3 py-2">Urgency</th>
                    <th className="px-3 py-2">Reorder Qty</th>
                    <th className="px-3 py-2">Vel Trend</th>
                    <th className="px-3 py-2">Days Cover</th>
                    <th className="px-3 py-2">Stockout Date</th>
                    <th className="px-3 py-2">Score</th>
                    <th className="px-3 py-2">Tier</th>
                    <th className="px-3 py-2">Units</th>
                    <th className="px-3 py-2">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y">
                  {planItems.map((r) => (
                    <tr key={`${r.seller_sku}-${r.marketplace_id}`} className="hover:bg-gray-50">
                      <td className="px-3 py-2 font-mono text-xs">
                        <button className="underline hover:text-indigo-600" onClick={() => setTrendSku(r.seller_sku)}>
                          {r.seller_sku}
                        </button>
                      </td>
                      <td className="px-3 py-2">{urgencyBadge(r.reorder_urgency)}</td>
                      <td className="px-3 py-2 font-bold">{r.suggested_reorder_qty}</td>
                      <td className="px-3 py-2">{trendBadge(r.velocity_trend, r.velocity_change_pct)}</td>
                      <td className="px-3 py-2">{r.days_cover?.toFixed(0) ?? "—"}</td>
                      <td className="px-3 py-2 text-xs">{r.estimated_stockout_date ?? "—"}</td>
                      <td className="px-3 py-2 font-bold">{r.risk_score.toFixed(1)}</td>
                      <td className="px-3 py-2">{tierBadge(r.risk_tier)}</td>
                      <td className="px-3 py-2">{r.units_available}</td>
                      <td className="px-3 py-2">
                        {r.acknowledged_at ? (
                          <span className="text-xs text-green-600">Ack</span>
                        ) : (
                          <button
                            className="rounded bg-indigo-50 px-2 py-0.5 text-xs font-medium text-indigo-700 hover:bg-indigo-100"
                            onClick={() => ackMut.mutate(r)}
                            disabled={ackMut.isPending}
                          >
                            Acknowledge
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                  {planItems.length === 0 && (
                    <tr><td colSpan={10} className="px-4 py-6 text-center text-gray-400">No replenishment plan — run Compute Now</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {/* Velocity Trend Chart (shown when a SKU is selected) */}
          {trendSku && trends.length > 0 && (
            <div className="rounded-xl border bg-white p-4 shadow-sm">
              <div className="mb-3 flex items-center justify-between">
                <h2 className="text-sm font-semibold text-gray-700">Velocity Trend — {trendSku}</h2>
                <button className="text-xs text-gray-400 hover:text-gray-600" onClick={() => setTrendSku(null)}>Close</button>
              </div>
              <ResponsiveContainer width="100%" height={240}>
                <LineChart data={trends}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="score_date" tick={{ fontSize: 10 }} />
                  <YAxis yAxisId="vel" tick={{ fontSize: 11 }} />
                  <YAxis yAxisId="score" orientation="right" tick={{ fontSize: 11 }} domain={[0, 100]} />
                  <Tooltip />
                  <Legend />
                  <Line yAxisId="vel" type="monotone" dataKey="velocity_7d" stroke="#6366f1" name="Vel 7d" dot={false} />
                  <Line yAxisId="vel" type="monotone" dataKey="velocity_30d" stroke="#a5b4fc" name="Vel 30d" dot={false} />
                  <Line yAxisId="score" type="monotone" dataKey="risk_score" stroke="#ef4444" name="Risk Score" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </>
      )}

      {/* ============ ALERTS TAB ============ */}
      {tab === "alerts" && (
        <div className="rounded-xl border bg-white shadow-sm">
          <div className="border-b px-4 py-3">
            <h2 className="text-sm font-semibold text-gray-700">Risk Alerts</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead className="border-b bg-gray-50 text-xs font-medium uppercase text-gray-500">
                <tr>
                  <th className="px-3 py-2">Severity</th>
                  <th className="px-3 py-2">Type</th>
                  <th className="px-3 py-2">SKU</th>
                  <th className="px-3 py-2">Title</th>
                  <th className="px-3 py-2">Current</th>
                  <th className="px-3 py-2">Previous</th>
                  <th className="px-3 py-2">Created</th>
                  <th className="px-3 py-2">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {alerts.map((a) => (
                  <tr key={a.id} className="hover:bg-gray-50">
                    <td className="px-3 py-2">{severityBadge(a.severity)}</td>
                    <td className="px-3 py-2 text-xs">{a.alert_type}</td>
                    <td className="px-3 py-2 font-mono text-xs">{a.seller_sku}</td>
                    <td className="px-3 py-2 text-xs">{a.title}</td>
                    <td className="px-3 py-2">{a.current_value?.toFixed(2) ?? "—"}</td>
                    <td className="px-3 py-2">{a.previous_value?.toFixed(2) ?? "—"}</td>
                    <td className="px-3 py-2 text-xs">{a.created_at?.slice(0, 10)}</td>
                    <td className="px-3 py-2">
                      {a.resolved_at ? (
                        <span className="text-xs text-green-600">Resolved</span>
                      ) : (
                        <button
                          className="rounded bg-orange-50 px-2 py-0.5 text-xs font-medium text-orange-700 hover:bg-orange-100"
                          onClick={() => resolveMut.mutate(a.id)}
                          disabled={resolveMut.isPending}
                        >
                          Resolve
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
                {alerts.length === 0 && (
                  <tr><td colSpan={8} className="px-4 py-6 text-center text-gray-400">No alerts</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
