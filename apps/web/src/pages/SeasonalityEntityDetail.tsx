import { useQuery } from "@tanstack/react-query";
import { useParams, useSearchParams } from "react-router-dom";
import { BarChart3, TrendingUp, AlertTriangle, Globe, CalendarDays } from "lucide-react";
import { getSeasonalityEntityDetail } from "@/lib/api";
import type { SeasonalityEntityDetail, SeasonalityMonthIndex, SeasonalityMonthlyMetric } from "@/lib/api";
import { cn } from "@/lib/utils";

const MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

const CLASS_COLORS: Record<string, string> = {
  EVERGREEN: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  MILD_SEASONAL: "bg-blue-500/15 text-blue-400 border-blue-500/30",
  STRONG_SEASONAL: "bg-orange-500/15 text-orange-400 border-orange-500/30",
  PEAK_SEASONAL: "bg-red-500/15 text-red-400 border-red-500/30",
  EVENT_DRIVEN: "bg-purple-500/15 text-purple-400 border-purple-500/30",
  IRREGULAR: "bg-zinc-500/15 text-zinc-400 border-zinc-500/30",
};

function ScoreBar({ label, value, max = 100, color = "bg-amazon" }: {
  label: string; value: number; max?: number; color?: string;
}) {
  const pct = Math.min((value / max) * 100, 100);
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-xs">
        <span className="text-muted-foreground">{label}</span>
        <span className="font-bold tabular-nums">{value.toFixed(1)}</span>
      </div>
      <div className="h-2 rounded-full bg-zinc-800">
        <div className={cn("h-2 rounded-full", color)} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function indexColor(v: number | null): string {
  if (v == null) return "bg-zinc-800/30";
  if (v >= 1.8) return "bg-red-600/70";
  if (v >= 1.4) return "bg-orange-500/60";
  if (v >= 1.1) return "bg-yellow-500/40";
  if (v >= 0.9) return "bg-emerald-500/30";
  if (v >= 0.6) return "bg-blue-500/30";
  return "bg-zinc-700/40";
}

export default function SeasonalityEntityDetailPage() {
  const { entityType, entityId } = useParams<{ entityType: string; entityId: string }>();
  const [search] = useSearchParams();
  const marketplace = search.get("marketplace") || undefined;

  const { data, isLoading } = useQuery({
    queryKey: ["seasonality-entity", entityType, entityId, marketplace],
    queryFn: () => getSeasonalityEntityDetail(entityType!, entityId!, marketplace),
    enabled: !!entityType && !!entityId,
    staleTime: 5 * 60_000,
  });

  if (isLoading) return <div className="p-6 text-sm text-muted-foreground animate-pulse">Loading entity…</div>;
  if (!data) return <div className="p-6 text-sm text-red-400">Entity not found</div>;

  const prof = data.profile;

  return (
    <div className="space-y-6 p-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <BarChart3 className="h-7 w-7 text-amazon" />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">{prof.entity_id}</h1>
          <p className="text-sm text-muted-foreground">
            {prof.entity_type} · {prof.marketplace} ·{" "}
            <span className={cn("rounded-full border px-2 py-0.5 text-[10px] font-bold uppercase",
              CLASS_COLORS[prof.seasonality_class] || "")}>
              {prof.seasonality_class?.replace("_", " ")}
            </span>
          </p>
        </div>
      </div>

      {/* Score Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="rounded-xl border border-border bg-card p-4 space-y-3">
          <ScoreBar label="Demand Strength" value={prof.demand_strength_score} color="bg-blue-500" />
          <ScoreBar label="Sales Strength" value={prof.sales_strength_score} color="bg-green-500" />
          <ScoreBar label="Profit Strength" value={prof.profit_strength_score} color="bg-amber-500" />
        </div>
        <div className="rounded-xl border border-border bg-card p-4 space-y-3">
          <ScoreBar label="Evergreen Score" value={prof.evergreen_score} color="bg-emerald-500" />
          <ScoreBar label="Volatility" value={prof.volatility_score} color="bg-red-500" />
          <ScoreBar label="Confidence" value={prof.seasonality_confidence_score} color="bg-purple-500" />
        </div>
        <div className="rounded-xl border border-border bg-card p-4">
          <div className="text-xs text-muted-foreground mb-2">Peak Months</div>
          <div className="flex flex-wrap gap-1">
            {prof.peak_months?.map(m => (
              <span key={m} className="rounded bg-red-500/20 px-2 py-0.5 text-xs font-bold text-red-300">
                {MONTH_NAMES[m - 1]}
              </span>
            ))}
          </div>
          <div className="text-xs text-muted-foreground mt-3 mb-1">Ramp</div>
          <div className="flex flex-wrap gap-1">
            {prof.ramp_months?.map(m => (
              <span key={m} className="rounded bg-yellow-500/20 px-2 py-0.5 text-xs text-yellow-300">
                {MONTH_NAMES[m - 1]}
              </span>
            ))}
          </div>
          <div className="text-xs text-muted-foreground mt-2">
            Season length: <strong>{prof.season_length_months ?? "—"}</strong> months
          </div>
        </div>
        <div className="rounded-xl border border-border bg-card p-4">
          <div className="text-xs text-muted-foreground mb-2">Execution Gaps</div>
          <div className="space-y-2">
            <div className="flex justify-between text-xs">
              <span>Demand ↔ Sales</span>
              <span className={cn("font-bold", (prof.demand_vs_sales_gap ?? 0) > 0.3 ? "text-red-400" : "text-green-400")}>
                {prof.demand_vs_sales_gap?.toFixed(3) ?? "—"}
              </span>
            </div>
            <div className="flex justify-between text-xs">
              <span>Sales ↔ Profit</span>
              <span className={cn("font-bold", (prof.sales_vs_profit_gap ?? 0) > 0.3 ? "text-red-400" : "text-green-400")}>
                {prof.sales_vs_profit_gap?.toFixed(3) ?? "—"}
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Month Index Table */}
      {data.indices?.length > 0 && (
        <div className="rounded-xl border border-border bg-card p-4 overflow-x-auto">
          <h2 className="text-sm font-semibold mb-3">Monthly Seasonality Indices (Jan-Dec)</h2>
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border">
                <th className="text-left px-2 py-1">Layer</th>
                {MONTH_NAMES.map(m => <th key={m} className="px-2 py-1 text-center">{m}</th>)}
              </tr>
            </thead>
            <tbody>
              {["demand_index","sales_index","profit_index"].map(layer => (
                <tr key={layer} className="border-b border-border/30">
                  <td className="px-2 py-1 font-medium capitalize">{layer.replace("_index","")}</td>
                  {Array.from({length: 12}, (_, i) => {
                    const idx = data.indices.find((ix: SeasonalityMonthIndex) => ix.month === i + 1);
                    const v = idx ? idx[layer as keyof SeasonalityMonthIndex] as number | null : null;
                    return (
                      <td key={i} className={cn("px-2 py-1 text-center tabular-nums", indexColor(v))}>
                        {v?.toFixed(2) ?? "—"}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Monthly Metrics History */}
      {data.monthly_metrics?.length > 0 && (
        <div className="rounded-xl border border-border bg-card p-4 overflow-x-auto">
          <h2 className="text-sm font-semibold mb-3">Monthly Metrics (Last 36 Months)</h2>
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-muted-foreground">
                <th className="text-left px-2 py-1">Period</th>
                <th className="px-2 py-1 text-right">Units</th>
                <th className="px-2 py-1 text-right">Orders</th>
                <th className="px-2 py-1 text-right">Revenue</th>
                <th className="px-2 py-1 text-right">CM2</th>
                <th className="px-2 py-1 text-right">NP</th>
                <th className="px-2 py-1 text-right">Ad Spend</th>
                <th className="px-2 py-1 text-right">Refunds</th>
                <th className="px-2 py-1 text-center">Stockout</th>
              </tr>
            </thead>
            <tbody>
              {data.monthly_metrics.slice(-36).map((m: SeasonalityMonthlyMetric) => (
                <tr key={`${m.year}-${m.month}`} className="border-b border-border/30">
                  <td className="px-2 py-1 font-medium">{m.year}-{String(m.month).padStart(2, "0")}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{m.units?.toFixed(0) ?? "—"}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{m.orders?.toFixed(0) ?? "—"}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{m.revenue?.toFixed(0) ?? "—"}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{m.profit_cm2?.toFixed(0) ?? "—"}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{m.profit_np?.toFixed(0) ?? "—"}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{m.ad_spend?.toFixed(0) ?? "—"}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{m.refunds?.toFixed(0) ?? "—"}</td>
                  <td className="px-2 py-1 text-center">
                    {m.stockout_days != null && m.stockout_days > 0
                      ? <span className="text-red-400">{m.stockout_days}d</span>
                      : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Demand vs Execution Gap */}
      {Array.isArray(data.demand_vs_execution_gap?.root_causes) && data.demand_vs_execution_gap.root_causes.length > 0 && (
        <div className="rounded-xl border border-border bg-card p-4">
          <h2 className="text-sm font-semibold mb-3 flex items-center gap-2">
            <AlertTriangle className="h-4 w-4 text-red-400" /> Execution Gap Root Causes
          </h2>
          <div className="space-y-2">
            {(data.demand_vs_execution_gap.root_causes as Array<{type: string; months: number}>).map((rc, i) => (
              <div key={i} className="flex items-center gap-2 text-xs">
                <span className="rounded bg-red-500/20 px-2 py-0.5 text-red-300 uppercase font-bold">
                  {rc.type}
                </span>
                <span className="text-muted-foreground">{rc.months} months affected</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Marketplace Comparison */}
      {data.marketplace_comparison?.length > 1 && (
        <div className="rounded-xl border border-border bg-card p-4">
          <h2 className="text-sm font-semibold mb-3 flex items-center gap-2">
            <Globe className="h-4 w-4 text-blue-400" /> Marketplace Comparison
          </h2>
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-muted-foreground">
                <th className="text-left px-2 py-1">Marketplace</th>
                <th className="px-2 py-1 text-right">Demand</th>
                <th className="px-2 py-1 text-right">Sales</th>
                <th className="px-2 py-1 text-right">Profit</th>
                <th className="px-2 py-1">Class</th>
                <th className="px-2 py-1">Peak</th>
              </tr>
            </thead>
            <tbody>
              {data.marketplace_comparison.map((mc: Record<string, unknown>, i: number) => (
                <tr key={i} className="border-b border-border/30">
                  <td className="px-2 py-1 font-medium">{mc.marketplace as string}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{(mc.demand_strength as number)?.toFixed(1)}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{(mc.sales_strength as number)?.toFixed(1)}</td>
                  <td className="px-2 py-1 text-right tabular-nums">{(mc.profit_strength as number)?.toFixed(1)}</td>
                  <td className="px-2 py-1">
                    <span className={cn("rounded-full border px-1.5 py-0.5 text-[9px] font-bold uppercase",
                      CLASS_COLORS[mc.class as string] || "")}>
                      {(mc.class as string)?.replace("_"," ")}
                    </span>
                  </td>
                  <td className="px-2 py-1 text-[10px]">
                    {(mc.peak_months as number[])?.map(m => MONTH_NAMES[m-1]).join(", ")}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
