import { useState, useCallback } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  TrendingUp,
  TrendingDown,
  DollarSign,
  ShoppingCart,
  Megaphone,
  RotateCcw,
  Percent,
  RefreshCw,
  AlertTriangle,
} from "lucide-react";
import {
  getProfitabilityOverview,
  triggerProfitabilityRecompute,
} from "@/lib/api";
import type {
  ProfitabilityOverview as OverviewData,
  SkuRankItem,
  ProfitabilityLossOrderItem,
} from "@/lib/api";
import { formatPLN, formatPct, cn } from "@/lib/utils";
import { ProfitTierBadge, DataWarningBanner, DataFreshness, PageFilterBar } from "@/components/shared";
import { usePageFilters } from "@/lib/usePageFilters";

/* ---------- KPI Card ---------- */
function KPICard({
  title, value, icon: Icon, color = "text-foreground", note,
}: {
  title: string; value: string; icon: React.ElementType; color?: string; note?: string;
}) {
  return (
    <div className="rounded-xl border border-border bg-card p-5">
      <div className="mb-3 flex items-center justify-between">
        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">{title}</span>
        <div className="rounded-lg bg-muted p-1.5">
          <Icon className={cn("h-4 w-4", color)} />
        </div>
      </div>
      <div className={cn("text-2xl font-bold", color)}>{value}</div>
      {note && <div className="mt-2 text-[11px] leading-5 text-white/45">{note}</div>}
    </div>
  );
}

/* ---------- SKU mini-table ---------- */
function SkuTable({ title, items, positive }: { title: string; items: SkuRankItem[]; positive: boolean }) {
  if (!items.length) return null;
  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-5 py-3 border-b border-border flex items-center gap-2">
        {positive
          ? <TrendingUp className="h-4 w-4 text-green-500" />
          : <TrendingDown className="h-4 w-4 text-destructive" />}
        <span className="text-sm font-semibold">{title}</span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              <th className="px-4 py-2 text-left">SKU</th>
              <th className="px-4 py-2 text-left">MKT</th>
              <th className="px-4 py-2 text-right">Revenue</th>
              <th className="px-4 py-2 text-right">NP</th>
              <th className="px-4 py-2 text-right">NP %</th>
              <th className="px-4 py-2 text-right">Units</th>
            </tr>
          </thead>
          <tbody>
            {items.map((s, i) => (
              <tr key={`${s.sku}-${s.marketplace_id}-${i}`} className="border-b border-border/50 hover:bg-muted/20">
                <td className="px-4 py-2 font-mono text-xs truncate max-w-[200px]" title={s.sku}>{s.sku}</td>
                <td className="px-4 py-2">{s.marketplace_code || s.marketplace_id.slice(-2)}</td>
                <td className="px-4 py-2 text-right tabular-nums">{formatPLN(s.revenue_pln)}</td>
                <td className={cn("px-4 py-2 text-right tabular-nums", s.profit_pln >= 0 ? "text-green-500" : "text-destructive")}>
                  {formatPLN(s.profit_pln)}
                </td>
                <td className="px-4 py-2 text-right tabular-nums">{s.margin_pct != null ? formatPct(s.margin_pct) : "—"}</td>
                <td className="px-4 py-2 text-right tabular-nums">{s.units}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ---------- Loss orders mini-table ---------- */
function LossOrdersTable({ items }: { items: ProfitabilityLossOrderItem[] }) {
  if (!items.length) return null;
  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="px-5 py-3 border-b border-border flex items-center gap-2">
        <AlertTriangle className="h-4 w-4 text-destructive" />
        <span className="text-sm font-semibold">Loss Orders (top 50)</span>
      </div>
      <div className="overflow-x-auto max-h-[400px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-card">
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              <th className="px-4 py-2 text-left">Order ID</th>
              <th className="px-4 py-2 text-left">Date</th>
              <th className="px-4 py-2 text-left">MKT</th>
              <th className="px-4 py-2 text-left">SKU</th>
              <th className="px-4 py-2 text-right">Revenue</th>
              <th className="px-4 py-2 text-right">NP</th>
              <th className="px-4 py-2 text-right">NP %</th>
            </tr>
          </thead>
          <tbody>
            {items.map((o, i) => (
              <tr key={`${o.amazon_order_id}-${i}`} className="border-b border-border/50 hover:bg-muted/20">
                <td className="px-4 py-2 font-mono text-xs">{o.amazon_order_id}</td>
                <td className="px-4 py-2 text-xs">{o.purchase_date?.slice(0, 10)}</td>
                <td className="px-4 py-2">{o.marketplace_code || "—"}</td>
                <td className="px-4 py-2 font-mono text-xs truncate max-w-[180px]" title={o.sku || ""}>{o.sku || "—"}</td>
                <td className="px-4 py-2 text-right tabular-nums">{formatPLN(o.revenue_pln)}</td>
                <td className="px-4 py-2 text-right tabular-nums text-destructive">{formatPLN(o.profit_pln)}</td>
                <td className="px-4 py-2 text-right tabular-nums">{o.margin_pct != null ? formatPct(o.margin_pct) : "—"}</td>
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
export default function ProfitOverviewPage() {
  const filters = usePageFilters();
  const [recomputing, setRecomputing] = useState(false);

  const params: Record<string, string> = {
    from: filters.dateFrom,
    to: filters.dateTo,
  };
  if (filters.marketplaceIds.length === 1) params.marketplace_id = filters.marketplaceIds[0];

  const { data, isLoading, isError, refetch } = useQuery<OverviewData>({
    queryKey: ["profitability-overview", params],
    queryFn: () => getProfitabilityOverview(params),
    staleTime: 60_000,
  });

  const kpi = data?.kpi;

  const handleRecompute = async () => {
    setRecomputing(true);
    try {
      await triggerProfitabilityRecompute(30);
      refetch();
    } finally {
      setRecomputing(false);
    }
  };

  return (
    <div className="space-y-6 p-6">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-2">
          <h1 className="text-2xl font-bold tracking-tight">Profit Dashboard</h1>
          <ProfitTierBadge tier="np" />
        </div>
        <div className="flex items-center gap-3">
          {/* Recompute */}
          <button
            onClick={handleRecompute}
            disabled={recomputing}
            className="flex items-center gap-1.5 rounded-lg border border-border bg-card px-3 py-1.5 text-sm font-medium hover:bg-muted disabled:opacity-50"
          >
            <RefreshCw className={cn("h-3.5 w-3.5", recomputing && "animate-spin")} />
            {recomputing ? "Recomputing…" : "Recompute"}
          </button>
        </div>
      </div>

      {/* Filter bar with custom date range support */}
      <PageFilterBar filters={filters} />

      <div className="flex items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <DataWarningBanner warnings={data?.warnings} />
          {data?.data_freshness?.data_source && (
            <span
              className={cn(
                "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium",
                data.data_freshness.data_source === "live"
                  ? "bg-green-500/15 text-green-400"
                  : data.data_freshness.data_source === "rollup"
                    ? "bg-amber-500/15 text-amber-400"
                    : "bg-blue-500/15 text-blue-400",
              )}
              title={
                data.data_freshness.data_source === "mixed"
                  ? `KPI z rollup, loss orders z live. Rollup: ${data.data_freshness.rollup_covers?.date_from ?? "?"} → ${data.data_freshness.rollup_covers?.date_to ?? "?"}`
                  : `Źródło: ${data.data_freshness.data_source}`
              }
            >
              {data.data_freshness.data_source === "live" ? "● Live" : data.data_freshness.data_source === "rollup" ? "◐ Rollup" : "◑ Mixed"}
            </span>
          )}
        </div>
        {data?.data_freshness?.rollup_recomputed_at && (
          <DataFreshness
            lastSync={data.data_freshness.rollup_recomputed_at}
            staleMinutes={1440}
            label="Rollup"
            onRefresh={() => { handleRecompute(); }}
            refreshing={recomputing}
          />
        )}
      </div>

      {/* KPI Cards */}
      {isError && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">Failed to load profitability data. Please try again later.</p>
        </div>
      )}

      {isLoading ? (
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="rounded-xl border border-border bg-card p-5 h-[120px] animate-pulse" />
          ))}
        </div>
      ) : kpi ? (
        <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-8 gap-4">
          <KPICard title="Revenue" value={formatPLN(kpi.total_revenue_pln)} icon={DollarSign} color="text-amazon" />
          <KPICard title="CM1" value={formatPLN(kpi.total_cm1_pln)} icon={TrendingUp}
            color={kpi.total_cm1_pln >= 0 ? "text-blue-500" : "text-destructive"}
            note={`CM1 %: ${formatPct(kpi.cm1_margin_pct)}`} />
          <KPICard title="CM2" value={formatPLN(kpi.total_cm2_pln)} icon={TrendingUp}
            color={kpi.total_cm2_pln >= 0 ? "text-violet-500" : "text-destructive"} />
          <KPICard title="Net Profit" value={formatPLN(kpi.total_profit_pln)} icon={TrendingUp}
            color={kpi.total_profit_pln >= 0 ? "text-green-500" : "text-destructive"} />
          <KPICard title="NP Margin" value={formatPct(kpi.total_margin_pct)} icon={Percent}
            color={kpi.total_margin_pct >= 10 ? "text-green-500" : kpi.total_margin_pct >= 0 ? "text-yellow-500" : "text-destructive"} />
          <KPICard title="Orders" value={kpi.total_orders.toLocaleString()} icon={ShoppingCart} />
          <KPICard title="Ad Spend" value={formatPLN(kpi.total_ad_spend_pln)} icon={Megaphone}
            note={`TACoS: ${formatPct(kpi.tacos_pct)}`} />
          <KPICard title="Returns" value={formatPct(kpi.return_rate_pct)} icon={RotateCcw}
            color={kpi.return_rate_pct > 5 ? "text-destructive" : "text-foreground"} />
        </div>
      ) : null}

      {/* Best / Worst SKUs */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <SkuTable title="Best Performing SKUs" items={data?.best_skus ?? []} positive />
        <SkuTable title="Worst Performing SKUs" items={data?.worst_skus ?? []} positive={false} />
      </div>

      {/* Loss Orders */}
      <LossOrdersTable items={data?.loss_orders ?? []} />
    </div>
  );
}
