import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Rocket, TrendingUp, AlertTriangle, Clock, CheckCircle, Zap, BarChart3, Users, RefreshCw } from "lucide-react";
import { getStrategyOverview, runStrategyDetection } from "@/lib/api";
import type { StrategyOverviewResponse, GrowthOpportunity, TypeBreakdown, MarketBreakdown } from "@/lib/api";
import { formatPLN, cn } from "@/lib/utils";
import { useNavigate } from "react-router-dom";

const PRIO_COLORS: Record<string, string> = {
  do_now: "bg-red-500/15 text-red-400 border-red-500/30",
  this_week: "bg-orange-500/15 text-orange-400 border-orange-500/30",
  this_month: "bg-blue-500/15 text-blue-400 border-blue-500/30",
  backlog: "bg-zinc-500/15 text-zinc-400 border-zinc-500/30",
  low: "bg-zinc-800/30 text-zinc-500 border-zinc-700",
};

const TYPE_LABELS: Record<string, string> = {
  PRICE_INCREASE: "💰 Price ↑",
  PRICE_DECREASE: "💸 Price ↓",
  ADS_SCALE_UP: "📈 Ads Scale",
  ADS_CUT_WASTE: "✂️ Ads Cut",
  CONTENT_FIX: "📝 Content Fix",
  CONTENT_EXPANSION: "📝 Content+",
  STOCK_REPLENISH: "📦 Replenish",
  STOCK_PROTECTION: "🛡️ Stock Protect",
  BUNDLE_CREATE: "🎁 Bundle",
  VARIANT_EXPANSION: "🔀 Variant",
  MARKETPLACE_EXPANSION: "🌍 Expansion",
  FAMILY_REPAIR: "🔧 Family Fix",
  RETURN_REDUCTION: "↩️ Returns",
  SUPPRESSION_FIX: "⚠️ Suppression",
  FBA_MIGRATION: "📤 FBA",
  FBM_FALLBACK: "📥 FBM",
  LIQUIDATE_OR_PROMO: "🏷️ Liquidate",
  COST_RENEGOTIATION: "💼 Cost Nego",
  CATEGORY_WINNER_SCALE: "🏆 Scale Winner",
  LOW_POTENTIAL_DEPRIORITIZE: "⬇️ Deprioritize",
};

function KPICard({ label, value, sub, icon: Icon, accent }: { label: string; value: string | number; sub?: string; icon: React.ElementType; accent?: string }) {
  return (
    <div className="rounded-xl border border-border bg-card p-4">
      <div className="flex items-center gap-2 text-xs text-muted-foreground mb-1">
        <Icon className={cn("h-3.5 w-3.5", accent)} />
        {label}
      </div>
      <div className="text-2xl font-bold tabular-nums">{value}</div>
      {sub && <div className="text-xs text-muted-foreground mt-0.5">{sub}</div>}
    </div>
  );
}

function PrioBadge({ label }: { label?: string }) {
  const l = label || "low";
  return (
    <span className={cn("rounded-full border px-2 py-0.5 text-[10px] font-bold uppercase", PRIO_COLORS[l] || PRIO_COLORS.low)}>
      {l.replace("_", " ")}
    </span>
  );
}

function OppRow({ opp, onClick }: { opp: GrowthOpportunity; onClick: () => void }) {
  return (
    <tr className="border-b border-border/50 hover:bg-muted/20 cursor-pointer" onClick={onClick}>
      <td className="px-3 py-2"><PrioBadge label={opp.priority_label} /></td>
      <td className="px-3 py-2 text-xs">{TYPE_LABELS[opp.opportunity_type] || opp.opportunity_type}</td>
      <td className="px-3 py-2 text-xs max-w-[300px] truncate" title={opp.title}>{opp.title}</td>
      <td className="px-3 py-2 text-xs">{opp.marketplace_code || "—"}</td>
      <td className="px-3 py-2 text-right text-xs tabular-nums text-green-500 font-medium">
        {opp.estimated_profit_uplift != null ? formatPLN(opp.estimated_profit_uplift) : "—"}
      </td>
      <td className="px-3 py-2 text-right text-xs tabular-nums">{opp.confidence_score.toFixed(0)}%</td>
    </tr>
  );
}

export default function StrategyOverviewPage() {
  const nav = useNavigate();
  const qc = useQueryClient();
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ["strategy-overview"],
    queryFn: getStrategyOverview,
    staleTime: 60_000,
  });

  const detect = useMutation({
    mutationFn: () => runStrategyDetection(30),
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: ["strategy-overview"] });
      refetch();
    },
  });

  const kpi = data?.kpi;
  const queryErrorDetail =
    (error as any)?.response?.data?.detail ||
    (error as Error | undefined)?.message ||
    "Failed to load strategy overview.";
  const detectErrorDetail =
    (detect.error as any)?.response?.data?.detail ||
    (detect.error as Error | undefined)?.message ||
    "Detection failed.";

  return (
    <div className="space-y-6 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Rocket className="h-7 w-7 text-amazon" />
          <div>
            <h1 className="text-2xl font-bold tracking-tight">Strategy / Growth Engine</h1>
            <p className="text-sm text-muted-foreground">Rekomendacje wzrostowe oparte o dane — priorytetyzacja działań</p>
          </div>
        </div>
        <button
          onClick={() => detect.mutate()}
          disabled={detect.isPending}
          className="flex items-center gap-2 rounded-lg bg-amazon px-4 py-2 text-sm font-medium text-black hover:bg-amazon/90 disabled:opacity-50"
        >
          <RefreshCw className={cn("h-4 w-4", detect.isPending && "animate-spin")} />
          {detect.isPending ? "Detecting…" : "Run Detection"}
        </button>
      </div>

      {detect.isError && (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {detectErrorDetail}
        </div>
      )}

      {isLoading ? (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {Array.from({ length: 8 }).map((_, i) => (
            <div key={i} className="rounded-xl border border-border bg-card p-4 h-24 animate-pulse" />
          ))}
        </div>
      ) : isError ? (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          <div>{queryErrorDetail}</div>
          <button
            onClick={() => refetch()}
            className="mt-2 rounded-md border border-destructive/50 px-2 py-1 text-xs hover:bg-destructive/10"
          >
            Retry
          </button>
        </div>
      ) : !kpi ? (
        <div className="text-center py-16 text-muted-foreground">No data. Run detection first.</div>
      ) : (
        <>
          {/* KPI Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <KPICard label="Revenue Upside" value={formatPLN(kpi.total_revenue_uplift)} icon={TrendingUp} accent="text-green-500" />
            <KPICard label="Profit Upside" value={formatPLN(kpi.total_profit_uplift)} icon={TrendingUp} accent="text-green-500" />
            <KPICard label="Total Opportunities" value={kpi.total_opportunities} icon={BarChart3} />
            <KPICard label="Completed (30d)" value={kpi.completed_30d} sub={`Impact: ${formatPLN(kpi.completed_impact_30d)}`} icon={CheckCircle} accent="text-green-500" />
          </div>

          {/* Priority buckets */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <button onClick={() => nav("/strategy/opportunities?quick_filter=do_now")} className="rounded-xl border-2 border-red-500/30 bg-red-500/5 p-4 text-left hover:bg-red-500/10 transition">
              <div className="flex items-center gap-2 text-xs text-red-400 mb-1"><Zap className="h-3.5 w-3.5" />DO NOW</div>
              <div className="text-3xl font-bold text-red-400">{kpi.do_now_count}</div>
            </button>
            <button onClick={() => nav("/strategy/opportunities?min_priority=75&max_priority=89")} className="rounded-xl border border-orange-500/30 bg-orange-500/5 p-4 text-left hover:bg-orange-500/10 transition">
              <div className="flex items-center gap-2 text-xs text-orange-400 mb-1"><Clock className="h-3.5 w-3.5" />THIS WEEK</div>
              <div className="text-3xl font-bold text-orange-400">{kpi.this_week_count}</div>
            </button>
            <button onClick={() => nav("/strategy/opportunities?min_priority=60&max_priority=74")} className="rounded-xl border border-blue-500/30 bg-blue-500/5 p-4 text-left hover:bg-blue-500/10 transition">
              <div className="flex items-center gap-2 text-xs text-blue-400 mb-1"><Clock className="h-3.5 w-3.5" />THIS MONTH</div>
              <div className="text-3xl font-bold text-blue-400">{kpi.this_month_count}</div>
            </button>
            <div className="rounded-xl border border-border bg-card p-4">
              <div className="flex items-center gap-2 text-xs text-muted-foreground mb-1"><AlertTriangle className="h-3.5 w-3.5" />BLOCKED</div>
              <div className="text-3xl font-bold">{kpi.blocked_count}</div>
            </div>
          </div>

          {/* By Type chart */}
          <div className="grid md:grid-cols-2 gap-6">
            <div className="rounded-xl border border-border bg-card p-4">
              <h3 className="text-sm font-semibold mb-3">By Type — Est. Profit Uplift</h3>
              <div className="space-y-2">
                {(data?.by_type ?? []).slice(0, 10).map((t: TypeBreakdown) => {
                  const maxP = Math.max(...(data?.by_type ?? []).map((x) => x.profit_uplift), 1);
                  const pct = (t.profit_uplift / maxP) * 100;
                  return (
                    <div key={t.opportunity_type} className="flex items-center gap-2 text-xs">
                      <span className="w-28 truncate">{TYPE_LABELS[t.opportunity_type] || t.opportunity_type}</span>
                      <div className="flex-1 h-4 rounded bg-muted/30 overflow-hidden">
                        <div className="h-full bg-amazon/60 rounded" style={{ width: `${pct}%` }} />
                      </div>
                      <span className="w-24 text-right tabular-nums text-green-500">{formatPLN(t.profit_uplift)}</span>
                      <span className="w-8 text-right tabular-nums text-muted-foreground">{t.count}</span>
                    </div>
                  );
                })}
              </div>
            </div>

            <div className="rounded-xl border border-border bg-card p-4">
              <h3 className="text-sm font-semibold mb-3">By Marketplace</h3>
              <div className="space-y-2">
                {(data?.by_market ?? []).slice(0, 9).map((m: MarketBreakdown) => {
                  const maxP = Math.max(...(data?.by_market ?? []).map((x) => x.profit_uplift), 1);
                  const pct = (m.profit_uplift / maxP) * 100;
                  return (
                    <div key={m.marketplace_id} className="flex items-center gap-2 text-xs">
                      <span className="w-10 font-medium">{m.marketplace_code || m.marketplace_id}</span>
                      <div className="flex-1 h-4 rounded bg-muted/30 overflow-hidden">
                        <div className="h-full bg-blue-500/60 rounded" style={{ width: `${pct}%` }} />
                      </div>
                      <span className="w-24 text-right tabular-nums text-green-500">{formatPLN(m.profit_uplift)}</span>
                      <span className="w-8 text-right tabular-nums text-muted-foreground">{m.count}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>

          {/* By Owner */}
          {(data?.by_owner ?? []).length > 0 && (
            <div className="rounded-xl border border-border bg-card p-4">
              <h3 className="text-sm font-semibold mb-3 flex items-center gap-2"><Users className="h-4 w-4" />By Owner</h3>
              <div className="flex flex-wrap gap-3">
                {data!.by_owner.map((o) => (
                  <div key={o.owner_role} className="rounded-lg border border-border px-3 py-2 text-xs">
                    <span className="font-medium">{o.owner_role}</span>
                    <span className="ml-2 text-muted-foreground">{o.count} opps</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Do Now table */}
          {(data?.do_now ?? []).length > 0 && (
            <div className="rounded-xl border-2 border-red-500/30 bg-card overflow-hidden">
              <div className="bg-red-500/10 px-4 py-2 flex items-center gap-2">
                <Zap className="h-4 w-4 text-red-400" />
                <span className="text-sm font-semibold text-red-400">Do Now — Priority 90+</span>
              </div>
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-xs text-muted-foreground uppercase">
                    <th className="px-3 py-2 text-left">Prio</th>
                    <th className="px-3 py-2 text-left">Type</th>
                    <th className="px-3 py-2 text-left">Title</th>
                    <th className="px-3 py-2 text-left">MKT</th>
                    <th className="px-3 py-2 text-right">Profit ↑</th>
                    <th className="px-3 py-2 text-right">Conf</th>
                  </tr>
                </thead>
                <tbody>
                  {data!.do_now.map((opp) => (
                    <OppRow key={opp.id} opp={opp} onClick={() => nav(`/strategy/opportunities?id=${opp.id}`)} />
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* This Week table */}
          {(data?.this_week ?? []).length > 0 && (
            <div className="rounded-xl border border-orange-500/30 bg-card overflow-hidden">
              <div className="bg-orange-500/10 px-4 py-2 flex items-center gap-2">
                <Clock className="h-4 w-4 text-orange-400" />
                <span className="text-sm font-semibold text-orange-400">This Week — Priority 75–89</span>
              </div>
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-xs text-muted-foreground uppercase">
                    <th className="px-3 py-2 text-left">Prio</th>
                    <th className="px-3 py-2 text-left">Type</th>
                    <th className="px-3 py-2 text-left">Title</th>
                    <th className="px-3 py-2 text-left">MKT</th>
                    <th className="px-3 py-2 text-right">Profit ↑</th>
                    <th className="px-3 py-2 text-right">Conf</th>
                  </tr>
                </thead>
                <tbody>
                  {data!.this_week.slice(0, 15).map((opp) => (
                    <OppRow key={opp.id} opp={opp} onClick={() => nav(`/strategy/opportunities?id=${opp.id}`)} />
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Top 10 */}
          <div className="rounded-xl border border-border bg-card overflow-hidden">
            <div className="px-4 py-2 border-b border-border flex items-center justify-between">
              <span className="text-sm font-semibold">Top 10 by Priority</span>
              <button onClick={() => nav("/strategy/opportunities")} className="text-xs text-amazon hover:underline">View all →</button>
            </div>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-xs text-muted-foreground uppercase">
                  <th className="px-3 py-2 text-left">Prio</th>
                  <th className="px-3 py-2 text-left">Type</th>
                  <th className="px-3 py-2 text-left">Title</th>
                  <th className="px-3 py-2 text-left">MKT</th>
                  <th className="px-3 py-2 text-right">Profit ↑</th>
                  <th className="px-3 py-2 text-right">Conf</th>
                </tr>
              </thead>
              <tbody>
                {(data?.top_priorities ?? []).map((opp) => (
                  <OppRow key={opp.id} opp={opp} onClick={() => nav(`/strategy/opportunities?id=${opp.id}`)} />
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
