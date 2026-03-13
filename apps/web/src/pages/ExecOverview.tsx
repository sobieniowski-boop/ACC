import { useState, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { format, subDays, startOfMonth, subMonths, endOfMonth, startOfYear } from "date-fns";
import {
  TrendingUp,
  TrendingDown,
  DollarSign,
  ShoppingCart,
  Percent,
  Megaphone,
  RotateCcw,
  Shield,
  Zap,
  AlertTriangle,
  ArrowUpRight,
  ArrowDownRight,
  ChevronDown,
  RefreshCw,
  Activity,
} from "lucide-react";
import {
  getExecOverview,
  triggerExecRecompute,
} from "@/lib/api";
import type {
  ExecOverviewResponse,
  ExecOpportunity,
} from "@/lib/api";
import { formatPLN, formatPct, cn } from "@/lib/utils";
import { ProfitTierBadge } from "@/components/shared";

/* ---------- Date presets ---------- */
type PresetKey = "mtd" | "30d" | "90d" | "ytd" | "pm";
const today = () => new Date();
const DATE_PRESETS: { key: PresetKey; label: string; range: () => [Date, Date] }[] = [
  { key: "mtd", label: "MTD",     range: () => [startOfMonth(today()), today()] },
  { key: "30d", label: "30d",     range: () => [subDays(today(), 29), today()] },
  { key: "90d", label: "Q",       range: () => [subDays(today(), 89), today()] },
  { key: "ytd", label: "YTD",     range: () => [startOfYear(today()), today()] },
  { key: "pm",  label: "Prev Mo", range: () => [startOfMonth(subMonths(today(), 1)), endOfMonth(subMonths(today(), 1))] },
];

const MARKETPLACE_OPTIONS = [
  { id: "", label: "All markets" },
  { id: "A1PA6795UKMFR9", label: "🇩🇪 DE" },
  { id: "A13V1IB3VIYZZH", label: "🇫🇷 FR" },
  { id: "APJ6JRA9NG5V4",  label: "🇮🇹 IT" },
  { id: "A1RKKUPIHCS9HS", label: "🇪🇸 ES" },
  { id: "A1C3SOZRARQ6R3", label: "🇵🇱 PL" },
  { id: "A1805IZSGTT6HS", label: "🇳🇱 NL" },
  { id: "A2NODRKZP88ZB9", label: "🇸🇪 SE" },
  { id: "AMEN7PMS3EDWL",  label: "🇧🇪 BE" },
  { id: "A28R8C7NBKEWEA", label: "🇮🇪 IE" },
];

/* ---------- Health Score Badge ---------- */
function HealthBadge({ score, label, color }: { score: number; label: string; color: string }) {
  const colorMap: Record<string, string> = {
    green: "bg-green-500/15 text-green-400 border-green-500/30",
    blue: "bg-blue-500/15 text-blue-400 border-blue-500/30",
    yellow: "bg-yellow-500/15 text-yellow-400 border-yellow-500/30",
    orange: "bg-orange-500/15 text-orange-400 border-orange-500/30",
    red: "bg-red-500/15 text-red-400 border-red-500/30",
  };
  return (
    <div className={cn("inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-semibold", colorMap[color] || colorMap.blue)}>
      <Activity className="h-4 w-4" />
      <span className="text-2xl font-bold tabular-nums">{score.toFixed(0)}</span>
      <span className="uppercase text-xs tracking-wider opacity-80">{label}</span>
    </div>
  );
}

/* ---------- KPI Card ---------- */
function KPICard({
  title, value, growth, icon: Icon, color = "text-foreground",
}: {
  title: string; value: string; growth?: number | null; icon: React.ElementType; color?: string;
}) {
  return (
    <div className="rounded-xl border border-border bg-card p-5 hover:border-border/80 transition-colors">
      <div className="mb-3 flex items-center justify-between">
        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">{title}</span>
        <div className="rounded-lg bg-muted p-1.5">
          <Icon className={cn("h-4 w-4", color)} />
        </div>
      </div>
      <div className={cn("text-2xl font-bold tabular-nums", color)}>{value}</div>
      {growth !== undefined && growth !== null && (
        <div className={cn("mt-1 flex items-center gap-1 text-xs font-medium",
          growth >= 0 ? "text-green-500" : "text-destructive")}>
          {growth >= 0 ? <ArrowUpRight className="h-3 w-3" /> : <ArrowDownRight className="h-3 w-3" />}
          {growth >= 0 ? "+" : ""}{growth.toFixed(1)}% vs prev period
        </div>
      )}
    </div>
  );
}

/* ---------- Opportunity/Risk card ---------- */
function OppCard({ item, type }: { item: ExecOpportunity; type: "risk" | "growth" }) {
  const isRisk = type === "risk";
  const priorityColor: Record<string, string> = {
    P1: "bg-red-500/20 text-red-400",
    P2: "bg-yellow-500/20 text-yellow-400",
    P3: "bg-blue-500/20 text-blue-400",
  };
  return (
    <div className={cn("rounded-lg border p-4 transition-colors hover:bg-muted/20",
      isRisk ? "border-destructive/20" : "border-green-500/20")}>
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className={cn("rounded px-1.5 py-0.5 text-[10px] font-bold uppercase", priorityColor[item.priority] || priorityColor.P3)}>
              {item.priority}
            </span>
            {item.marketplace_code && (
              <span className="text-xs text-muted-foreground">{item.marketplace_code}</span>
            )}
            {item.sku && (
              <span className="text-xs font-mono text-muted-foreground truncate max-w-[120px]" title={item.sku}>{item.sku}</span>
            )}
          </div>
          <div className="text-sm font-medium leading-snug">{item.title}</div>
          {item.description && (
            <div className="mt-1 text-xs text-muted-foreground/70 line-clamp-2">{item.description}</div>
          )}
        </div>
        {item.impact_estimate != null && item.impact_estimate > 0 && (
          <div className="text-right shrink-0">
            <div className="text-xs text-muted-foreground">Impact</div>
            <div className={cn("text-sm font-bold tabular-nums", isRisk ? "text-destructive" : "text-green-500")}>
              {formatPLN(item.impact_estimate)}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/* ---------- SKU mini-table ---------- */
function SkuMiniTable({
  title, items, positive,
}: {
  title: string;
  items: { sku: string; marketplace_code?: string; revenue_pln: number; profit_pln: number; margin_pct: number; units: number }[];
  positive: boolean;
}) {
  if (!items.length) return null;
  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-5 py-3 border-b border-border flex items-center gap-2">
        {positive ? <TrendingUp className="h-4 w-4 text-green-500" /> : <TrendingDown className="h-4 w-4 text-destructive" />}
        <span className="text-sm font-semibold">{title}</span>
        <span className="ml-auto text-xs text-muted-foreground">{items.length} SKUs</span>
      </div>
      <div className="overflow-x-auto max-h-[380px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-card">
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              <th className="px-4 py-2 text-left">SKU</th>
              <th className="px-4 py-2 text-left">MKT</th>
              <th className="px-4 py-2 text-right">Revenue</th>
              <th className="px-4 py-2 text-right">NP</th>
              <th className="px-4 py-2 text-right">NP %</th>
            </tr>
          </thead>
          <tbody>
            {items.map((s, i) => (
              <tr key={`${s.sku}-${i}`} className="border-b border-border/50 hover:bg-muted/20">
                <td className="px-4 py-2 font-mono text-xs truncate max-w-[180px]" title={s.sku}>{s.sku}</td>
                <td className="px-4 py-2">{s.marketplace_code || "—"}</td>
                <td className="px-4 py-2 text-right tabular-nums">{formatPLN(s.revenue_pln)}</td>
                <td className={cn("px-4 py-2 text-right tabular-nums font-medium",
                  s.profit_pln >= 0 ? "text-green-500" : "text-destructive")}>{formatPLN(s.profit_pln)}</td>
                <td className="px-4 py-2 text-right tabular-nums">{formatPct(s.margin_pct)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ================================================================== */
/*  Page Component                                                     */
/* ================================================================== */
export default function ExecOverviewPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [recomputing, setRecomputing] = useState(false);

  const preset = (searchParams.get("preset") as PresetKey) || "30d";
  const marketplace = searchParams.get("mp") || "";

  const setPreset = useCallback((v: PresetKey) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      if (v === "30d") next.delete("preset"); else next.set("preset", v);
      return next;
    }, { replace: true });
  }, [setSearchParams]);

  const setMarketplace = useCallback((v: string) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      if (!v) next.delete("mp"); else next.set("mp", v);
      return next;
    }, { replace: true });
  }, [setSearchParams]);

  const [from, to] = DATE_PRESETS.find((p) => p.key === preset)!.range();
  const params: Record<string, string> = {
    from: format(from, "yyyy-MM-dd"),
    to: format(to, "yyyy-MM-dd"),
  };
  if (marketplace) params.marketplace_id = marketplace;

  const { data, isLoading, isError, refetch } = useQuery<ExecOverviewResponse>({
    queryKey: ["exec-overview", params],
    queryFn: () => getExecOverview(params),
    staleTime: 60_000,
  });

  const kpi = data?.kpi;

  const handleRecompute = async () => {
    setRecomputing(true);
    try {
      await triggerExecRecompute(30);
      refetch();
    } finally {
      setRecomputing(false);
    }
  };

  return (
    <div className="space-y-6 p-6">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Executive Command Center</h1>
          <ProfitTierBadge tier="np" />
          <p className="text-sm text-muted-foreground mt-0.5">Strategiczny widok biznesu — decyzje w 30 sekund</p>
        </div>
        <div className="flex items-center gap-3">
          {/* Health Score Badge */}
          {data?.health_label && (
            <HealthBadge
              score={data.health_label.score}
              label={data.health_label.label}
              color={data.health_label.color}
            />
          )}
        </div>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex rounded-lg border border-border overflow-hidden">
          {DATE_PRESETS.map((p) => (
            <button
              key={p.key}
              onClick={() => setPreset(p.key)}
              className={cn(
                "px-3 py-1.5 text-xs font-medium transition-colors",
                preset === p.key ? "bg-amazon text-white" : "bg-card hover:bg-muted",
              )}
            >
              {p.label}
            </button>
          ))}
        </div>
        <div className="relative">
          <select
            value={marketplace}
            onChange={(e) => setMarketplace(e.target.value)}
            className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm font-medium focus:outline-none focus:ring-2 focus:ring-amazon/50"
          >
            {MARKETPLACE_OPTIONS.map((m) => (
              <option key={m.id} value={m.id}>{m.label}</option>
            ))}
          </select>
          <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
        </div>
        <button
          onClick={handleRecompute}
          disabled={recomputing}
          className="flex items-center gap-1.5 rounded-lg border border-border bg-card px-3 py-1.5 text-sm font-medium hover:bg-muted disabled:opacity-50"
        >
          <RefreshCw className={cn("h-3.5 w-3.5", recomputing && "animate-spin")} />
          {recomputing ? "Computing…" : "Recompute"}
        </button>
      </div>

      {isError && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">Failed to load executive overview. Please try again later.</p>
        </div>
      )}

      {/* KPI Cards */}
      {isLoading ? (
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="rounded-xl border border-border bg-card p-5 h-[120px] animate-pulse" />
          ))}
        </div>
      ) : kpi ? (
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-4">
          <KPICard title="Revenue" value={formatPLN(kpi.revenue_pln)} growth={kpi.revenue_growth_pct}
            icon={DollarSign} color="text-amazon" />
          <KPICard title="Net Profit" value={formatPLN(kpi.profit_pln)} growth={kpi.profit_growth_pct}
            icon={TrendingUp} color={kpi.profit_pln >= 0 ? "text-green-500" : "text-destructive"} />
          <KPICard title="NP Margin" value={formatPct(kpi.margin_pct)}
            icon={Percent} color={kpi.margin_pct >= 10 ? "text-green-500" : kpi.margin_pct >= 0 ? "text-yellow-500" : "text-destructive"} />
          <KPICard title="Orders" value={kpi.orders.toLocaleString()} icon={ShoppingCart} />
          <KPICard title="Ad Spend" value={formatPLN(kpi.ad_spend_pln)}
            icon={Megaphone} color={kpi.acos_pct && kpi.acos_pct > 25 ? "text-destructive" : "text-foreground"} />
          <KPICard title="Return Rate" value={kpi.return_rate_pct != null ? formatPct(kpi.return_rate_pct) : "—"}
            icon={RotateCcw} color={kpi.return_rate_pct && kpi.return_rate_pct > 5 ? "text-destructive" : "text-foreground"} />
        </div>
      ) : null}

      {/* Health Score Breakdown */}
      {data?.health && (
        <div className="rounded-xl border border-border bg-card p-5">
          <h2 className="text-sm font-semibold mb-3 flex items-center gap-2">
            <Shield className="h-4 w-4 text-amazon" /> Business Health Score
          </h2>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
            {[
              { label: "Profitability", score: data.health.profit_score, weight: "30%" },
              { label: "Demand", score: data.health.demand_score, weight: "20%" },
              { label: "Inventory", score: data.health.inventory_score, weight: "20%" },
              { label: "Operations", score: data.health.operations_score, weight: "15%" },
              { label: "Revenue", score: data.health.revenue_score, weight: "15%" },
            ].map((h) => (
              <div key={h.label} className="text-center">
                <div className="text-xs text-muted-foreground mb-1">{h.label} ({h.weight})</div>
                <div className={cn("text-xl font-bold tabular-nums",
                  h.score >= 75 ? "text-green-500" : h.score >= 50 ? "text-yellow-500" : "text-destructive")}>
                  {h.score.toFixed(0)}
                </div>
                <div className="mt-1 h-1.5 rounded-full bg-muted overflow-hidden">
                  <div
                    className={cn("h-full rounded-full transition-all",
                      h.score >= 75 ? "bg-green-500" : h.score >= 50 ? "bg-yellow-500" : "bg-destructive")}
                    style={{ width: `${Math.min(100, h.score)}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Risks + Growth — side by side */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Risks */}
        <div>
          <h2 className="text-sm font-semibold mb-3 flex items-center gap-2">
            <AlertTriangle className="h-4 w-4 text-destructive" /> Top Risks
            {data?.risks && <span className="text-xs text-muted-foreground font-normal">({data.risks.length})</span>}
          </h2>
          <div className="space-y-2">
            {(data?.risks ?? []).length === 0 && !isLoading && (
              <div className="rounded-lg border border-border p-4 text-sm text-muted-foreground text-center">No active risks</div>
            )}
            {(data?.risks ?? []).slice(0, 10).map((r) => (
              <OppCard key={r.id} item={r} type="risk" />
            ))}
          </div>
        </div>

        {/* Growth */}
        <div>
          <h2 className="text-sm font-semibold mb-3 flex items-center gap-2">
            <Zap className="h-4 w-4 text-green-500" /> Growth Opportunities
            {data?.growth && <span className="text-xs text-muted-foreground font-normal">({data.growth.length})</span>}
          </h2>
          <div className="space-y-2">
            {(data?.growth ?? []).length === 0 && !isLoading && (
              <div className="rounded-lg border border-border p-4 text-sm text-muted-foreground text-center">No growth opportunities detected</div>
            )}
            {(data?.growth ?? []).slice(0, 10).map((g) => (
              <OppCard key={g.id} item={g} type="growth" />
            ))}
          </div>
        </div>
      </div>

      {/* Best / Worst SKUs */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <SkuMiniTable title="Top Profitable SKUs" items={data?.best_skus ?? []} positive />
        <SkuMiniTable title="Worst Performing SKUs" items={data?.worst_skus ?? []} positive={false} />
      </div>
    </div>
  );
}
