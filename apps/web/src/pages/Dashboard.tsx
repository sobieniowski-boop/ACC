import { useState, useMemo, useDeferredValue, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import {
  ComposedChart,
  Area,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import {
  TrendingUp,
  TrendingDown,
  ShoppingCart,
  DollarSign,
  Percent,
  Bell,
  ChevronDown,
  Calendar,
  AlertTriangle,
  ArrowUpRight,
  ArrowDownRight,
  Package,
  Megaphone,
  BarChart3,
  Clock,
  Lightbulb,
  RotateCcw,
} from "lucide-react";
import { getDataQuality, getKPISummary, getRevenueChart, getMarketplaces, getTopDrivers, getRecentAlerts, getIntelligenceFunnel } from "@/lib/api";
import type { TopDriverItem, IntelligenceFunnelResponse } from "@/lib/api";
import { formatPLN, formatPct, formatDelta, cn } from "@/lib/utils";
import { DataFreshness, ClientExportButton } from "@/components/shared";
import {
  format,
  subDays,
  startOfMonth,
  endOfMonth,
  subMonths,
  startOfQuarter,
  endOfQuarter,
  subQuarters,
  startOfYear,
  subYears,
  endOfYear,
} from "date-fns";

/* ---------- Date presets ---------- */
interface DatePreset {
  label: string;
  key: string;
  range: () => [Date, Date];
}

const today = () => new Date();

const DATE_PRESETS: DatePreset[] = [
  { label: "7d",               key: "7d",       range: () => [subDays(today(), 6), today()] },
  { label: "30d",              key: "30d",      range: () => [subDays(today(), 29), today()] },
  { label: "90d",              key: "90d",      range: () => [subDays(today(), 89), today()] },
  { label: "Bieżący miesiąc",  key: "cm",       range: () => [startOfMonth(today()), today()] },
  { label: "Poprzedni miesiąc", key: "pm",      range: () => [startOfMonth(subMonths(today(), 1)), endOfMonth(subMonths(today(), 1))] },
  { label: "Bieżący kwartał",  key: "cq",       range: () => [startOfQuarter(today()), today()] },
  { label: "Poprzedni kwartał", key: "pq",      range: () => [startOfQuarter(subQuarters(today(), 1)), endOfQuarter(subQuarters(today(), 1))] },
  { label: "2 kwartały",       key: "2q",       range: () => [startOfQuarter(subQuarters(today(), 1)), today()] },
  { label: "Od początku roku", key: "ytd",      range: () => [startOfYear(today()), today()] },
  { label: "Poprzedni rok",    key: "py",       range: () => [startOfYear(subYears(today(), 1)), endOfYear(subYears(today(), 1))] },
];

/* ---------- Fulfillment options ---------- */
const FULFILLMENT_OPTIONS = [
  { label: "Razem", value: "" },
  { label: "FBA",   value: "AFN" },
  { label: "FBM",   value: "MFN" },
];

/* ---------- Chart series config ---------- */
interface ChartSeries {
  key: string;
  label: string;
  color: string;
  yAxisId: "pln" | "pct" | "count";
  type: "area" | "line" | "bar";
  defaultOn: boolean;
}

const CHART_SERIES: ChartSeries[] = [
  { key: "revenue_pln", label: "Revenue",  color: "#FF9900", yAxisId: "pln",   type: "area",  defaultOn: true },
  { key: "cm1_pln",     label: "CM1",      color: "#22c55e", yAxisId: "pln",   type: "area",  defaultOn: true },
  { key: "cm1_percent", label: "CM%",      color: "#38bdf8", yAxisId: "pct",   type: "line",  defaultOn: true },
  { key: "orders",      label: "Orders",   color: "#a78bfa", yAxisId: "count", type: "bar",   defaultOn: false },
];

/* ---------- Relative time helper ---------- */
function timeAgo(iso: string | null | undefined): string {
  if (!iso) return "";
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60_000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

/* ---------- KPI Card ---------- */
function KPICard({
  title,
  value,
  delta,
  icon: Icon,
  color = "text-foreground",
  note,
}: {
  title: string;
  value: string;
  delta?: number;
  icon: React.ElementType;
  color?: string;
  note?: string;
}) {
  const deltaPositive = (delta ?? 0) >= 0;
  return (
    <div className="rounded-xl border border-border bg-card p-5">
      <div className="mb-3 flex items-center justify-between">
        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">{title}</span>
        <div className="rounded-lg bg-muted p-1.5">
          <Icon className={cn("h-4 w-4", color)} />
        </div>
      </div>
      <div className={cn("text-2xl font-bold", color)}>{value}</div>
      {delta !== undefined && (
        <div className={cn("mt-1 flex items-center gap-1 text-xs", deltaPositive ? "text-green-500" : "text-destructive")}>
          {deltaPositive ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
          {formatDelta(delta)} vs prev period
        </div>
      )}
      {note ? <div className="mt-2 text-[11px] leading-5 text-white/45 whitespace-pre-line">{note}</div> : null}
    </div>
  );
}

/* ---------- Simple dropdown ---------- */
function Dropdown<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: { label: string; value: T }[];
  value: T;
  onChange: (v: T) => void;
}) {
  return (
    <div className="relative">
      <select
        value={value}
        onChange={(e) => onChange(e.target.value as T)}
        className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm font-medium focus:outline-none focus:ring-2 focus:ring-amazon/50"
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>{o.label}</option>
        ))}
      </select>
      <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
    </div>
  );
}

/* ---------- Drivers / Leaks mini-table ---------- */
function DriversTable({
  title,
  icon,
  items,
  positive,
}: {
  title: string;
  icon: React.ReactNode;
  items: TopDriverItem[];
  positive: boolean;
}) {
  return (
    <div className="rounded-xl border border-border bg-card p-5">
      <div className="mb-3 flex items-center gap-2">
        {icon}
        <h2 className="text-sm font-semibold">{title}</h2>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs text-muted-foreground">
              <th className="pb-2 pr-2">#</th>
              <th className="pb-2 pr-4">SKU / ASIN</th>
              <th className="pb-2 pr-4 text-right">Units</th>
              <th className="pb-2 pr-4 text-right">Revenue</th>
              <th className="pb-2 pr-4 text-right">CM1</th>
              <th className="pb-2 text-right">CM%</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item, idx) => (
              <tr key={item.sku ?? idx} className="border-b border-border/50 last:border-0">
                <td className="py-2 pr-2 text-xs text-muted-foreground">{idx + 1}</td>
                <td className="max-w-[260px] py-2 pr-4">
                  <div className="flex items-baseline gap-1.5">
                    <span className="font-mono text-xs select-all">{item.sku}</span>
                    {item.asin && <span className="font-mono text-[10px] text-muted-foreground select-all">{item.asin}</span>}
                  </div>
                  {item.title && <p className="truncate text-[10px] text-muted-foreground mt-0.5">{item.title}</p>}
                  {item.internal_sku && <p className="font-mono text-[9px] text-muted-foreground/60 mt-0.5 select-all">{item.internal_sku}</p>}
                </td>
                <td className="py-2 pr-4 text-right tabular-nums">{item.units}</td>
                <td className="py-2 pr-4 text-right tabular-nums">{formatPLN(item.revenue_pln)}</td>
                <td className={cn(
                  "py-2 pr-4 text-right tabular-nums font-semibold",
                  positive ? "text-green-500" : "text-destructive"
                )}>
                  {formatPLN(item.cm1_pln)}
                </td>
                <td className={cn(
                  "py-2 text-right tabular-nums",
                  item.cm1_percent >= 20 ? "text-green-500" : item.cm1_percent >= 10 ? "text-amber-500" : "text-destructive"
                )}>
                  {formatPct(item.cm1_percent)}
                </td>
              </tr>
            ))}
            {items.length === 0 && (
              <tr>
                <td colSpan={6} className="py-4 text-center text-xs text-muted-foreground">No data</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ---------- Dashboard ---------- */
export default function DashboardPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [brandFilter, setBrandFilter] = useState("");
  const [categoryFilter, setCategoryFilter] = useState("");
  const [chartVisible, setChartVisible] = useState<Record<string, boolean>>(
    () => Object.fromEntries(CHART_SERIES.map((s) => [s.key, s.defaultOn]))
  );
  const toggleSeries = useCallback((key: string) => {
    setChartVisible((prev) => ({ ...prev, [key]: !prev[key] }));
  }, []);

  const presetKey = searchParams.get("preset") || "30d";
  const customFrom = searchParams.get("from") || "";
  const customTo = searchParams.get("to") || "";
  const marketplaceId = searchParams.get("mp") || "";
  const fulfillment = searchParams.get("ff") || "";

  const updateParams = useCallback((patch: Record<string, string | null>) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      for (const [k, v] of Object.entries(patch)) {
        if (v === null || v === "") next.delete(k); else next.set(k, v);
      }
      return next;
    }, { replace: true });
  }, [setSearchParams]);

  const setPresetKey = useCallback((v: string) => updateParams({ preset: v === "30d" ? null : v }), [updateParams]);
  const setCustomFrom = useCallback((v: string) => updateParams({ from: v || null }), [updateParams]);
  const setCustomTo = useCallback((v: string) => updateParams({ to: v || null }), [updateParams]);
  const setMarketplaceId = useCallback((v: string) => updateParams({ mp: v || null }), [updateParams]);
  const setFulfillment = useCallback((v: string) => updateParams({ ff: v || null }), [updateParams]);
  const deferredBrandFilter = useDeferredValue(brandFilter);
  const deferredCategoryFilter = useDeferredValue(categoryFilter);

  // Resolve dates
  const { dateFrom, dateTo } = useMemo(() => {
    if (presetKey === "custom" && customFrom && customTo) {
      return { dateFrom: customFrom, dateTo: customTo };
    }
    const preset = DATE_PRESETS.find((p) => p.key === presetKey) ?? DATE_PRESETS[1];
    const [from, to] = preset.range();
    return { dateFrom: format(from, "yyyy-MM-dd"), dateTo: format(to, "yyyy-MM-dd") };
  }, [presetKey, customFrom, customTo]);

  // Build query params
  const queryParams = useMemo(() => {
    const p: Record<string, string> = { date_from: dateFrom, date_to: dateTo };
    if (marketplaceId) p.marketplace_id = marketplaceId;
    if (fulfillment) p.fulfillment_channel = fulfillment;
    return p;
  }, [dateFrom, dateTo, marketplaceId, fulfillment]);

  // Fetch marketplaces for dropdown
  const { data: marketplaces } = useQuery({
    queryKey: ["marketplaces"],
    queryFn: getMarketplaces,
    staleTime: 10 * 60_000,
  });

  const marketplaceOptions = useMemo(() => {
    const base = [{ label: "Wszystkie rynki", value: "" }];
    if (!marketplaces) return base;
    return [
      ...base,
      ...marketplaces.map((m) => ({
        label: `${m.code} (${m.country})`,
        value: m.marketplace_id,
      })),
    ];
  }, [marketplaces]);

  // KPI summary
  const { data: kpi, isLoading: kpiLoading, isError: kpiError } = useQuery({
    queryKey: ["kpi-summary", queryParams],
    queryFn: () => getKPISummary(queryParams),
    staleTime: 5 * 60_000,
    placeholderData: keepPreviousData,
  });

  // Chart
  const { data: chart } = useQuery({
    queryKey: ["revenue-chart", queryParams],
    queryFn: () => getRevenueChart(queryParams),
    staleTime: 5 * 60_000,
    placeholderData: keepPreviousData,
  });

  const chartData = useMemo(
    () =>
      (chart?.points ?? []).map((point) => ({
        ...point,
        cm1_percent:
          point.revenue_pln > 0
            ? Number(((point.cm1_pln / point.revenue_pln) * 100).toFixed(2))
            : 0,
      })),
    [chart]
  );

  // Determine if any "count" axis is active (for orders bar)
  const showCountAxis = chartVisible["orders"];

  // Top profit drivers & leaks
  const { data: drivers } = useQuery({
    queryKey: ["top-drivers", queryParams, deferredBrandFilter, deferredCategoryFilter],
    queryFn: () =>
      getTopDrivers({
        ...queryParams,
        ...(deferredBrandFilter.trim() ? { brand: deferredBrandFilter.trim() } : {}),
        ...(deferredCategoryFilter.trim() ? { category: deferredCategoryFilter.trim() } : {}),
        limit: "15",
      }),
    staleTime: 5 * 60_000,
    placeholderData: keepPreviousData,
  });

  // Recent critical alerts for dashboard panel
  const { data: recentAlerts } = useQuery({
    queryKey: ["recent-alerts"],
    queryFn: () => getRecentAlerts(5),
    staleTime: 60_000,
    refetchInterval: 30_000,
  });

  const { data: quality } = useQuery({
    queryKey: ["profit-data-quality-dashboard", queryParams],
    queryFn: () => getDataQuality(queryParams),
    staleTime: 30 * 60_000, // 30 min — expensive query, rarely changes
    gcTime: 60 * 60_000,    // keep in cache for 1h
    placeholderData: keepPreviousData,
  });

  const cogsCoverage = quality?.overview.cogs_coverage_pct ?? 0;

  // Intelligence opportunity funnel
  const { data: funnel } = useQuery({
    queryKey: ["intelligence-funnel", marketplaceId],
    queryFn: () => getIntelligenceFunnel(marketplaceId ? { marketplace_id: marketplaceId } : undefined),
    staleTime: 5 * 60_000,
  });

  const feesCoverage = Math.min(
    quality?.overview.fba_fee_coverage_pct ?? 0,
    quality?.overview.referral_fee_coverage_pct ?? 0,
  );
  const lowCogsCoverage = cogsCoverage < 80;

  return (
    <div className="space-y-6">
      {/* ===== Header + Filters ===== */}
      <div className="space-y-3">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">Executive Dashboard</h1>
            <p className="text-sm text-muted-foreground">
              {dateFrom} — {dateTo}
              {marketplaceId ? ` · ${marketplaceOptions.find((o) => o.value === marketplaceId)?.label ?? ""}` : ""}
              {fulfillment ? ` · ${FULFILLMENT_OPTIONS.find((o) => o.value === fulfillment)?.label ?? ""}` : ""}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <DataFreshness lastSync={(kpi as any)?.last_sync} staleMinutes={30} label="KPI" />
            {kpi?.by_marketplace && kpi.by_marketplace.length > 0 && (
              <ClientExportButton
                data={kpi.by_marketplace}
                filename={`dashboard_${dateFrom}_${dateTo}`}
                label="Export CSV"
              />
            )}
          </div>
        </div>

        {/* Date presets row */}
        <div className="flex flex-wrap items-center gap-2">
          <Calendar className="h-4 w-4 text-muted-foreground" />
          <div className="flex flex-wrap gap-1 rounded-lg border border-border bg-card p-1">
            {DATE_PRESETS.map((p) => (
              <button
                key={p.key}
                onClick={() => setPresetKey(p.key)}
                className={cn(
                  "rounded-md px-2.5 py-1 text-xs font-medium transition-colors whitespace-nowrap",
                  presetKey === p.key
                    ? "bg-amazon text-black"
                    : "text-muted-foreground hover:text-foreground"
                )}
              >
                {p.label}
              </button>
            ))}
            <button
              onClick={() => setPresetKey("custom")}
              className={cn(
                "rounded-md px-2.5 py-1 text-xs font-medium transition-colors whitespace-nowrap",
                presetKey === "custom"
                  ? "bg-amazon text-black"
                  : "text-muted-foreground hover:text-foreground"
              )}
            >
              Niestandardowy
            </button>
          </div>

          {presetKey === "custom" && (
            <div className="flex items-center gap-1.5">
              <input
                type="date"
                value={customFrom}
                onChange={(e) => setCustomFrom(e.target.value)}
                className="rounded-lg border border-border bg-card px-2 py-1 text-xs"
              />
              <span className="text-xs text-muted-foreground">—</span>
              <input
                type="date"
                value={customTo}
                onChange={(e) => setCustomTo(e.target.value)}
                className="rounded-lg border border-border bg-card px-2 py-1 text-xs"
              />
            </div>
          )}

          <div className="ml-auto flex items-center gap-2">
            <Dropdown
              label="Marketplace"
              options={marketplaceOptions}
              value={marketplaceId}
              onChange={setMarketplaceId}
            />
            <Dropdown
              label="Fulfillment"
              options={FULFILLMENT_OPTIONS}
              value={fulfillment}
              onChange={setFulfillment}
            />
          </div>
        </div>
      </div>

      {/* ===== KPI tiles ===== */}
      {kpiError && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">Failed to load dashboard data. Please try again later.</p>
        </div>
      )}

      {lowCogsCoverage ? (
        <div className="rounded-xl border border-amber-500/30 bg-amber-500/5 p-4">
          <div className="flex items-start gap-3">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-400" />
            <div>
              <div className="text-sm font-semibold text-amber-300">CM1 jest partial dla tego zakresu</div>
              <div className="mt-1 text-sm leading-6 text-white/70">
                Revenue zostało historycznie dosypane szerzej niż line-level COGS. W tym zakresie pokrycie COGS to{" "}
                <span className="font-semibold text-white">{formatPct(cogsCoverage)}</span>, a pokrycie fee{" "}
                <span className="font-semibold text-white">{formatPct(feesCoverage)}</span>.
                Przy takim stanie CM1 nie jest jeszcze pełną prawdą biznesową.
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {kpiLoading ? (
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          {[...Array(8)].map((_, i) => (
            <div key={i} className="h-28 animate-pulse rounded-xl bg-muted" />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <KPICard
            title="Revenue"
            value={formatPLN(kpi?.total_revenue_pln ?? 0)}
            delta={kpi?.revenue_delta_pct}
            icon={DollarSign}
            color="text-amazon"
          />
          <KPICard
            title="Orders"
            value={(kpi?.total_orders ?? 0).toLocaleString("pl-PL")}
            delta={kpi?.orders_delta_pct}
            icon={ShoppingCart}
          />
          <KPICard
            title="Net Profit"
            value={`${formatPLN(kpi?.total_net_profit_pln ?? 0)} (${formatPct(kpi?.total_net_profit_percent ?? 0)})`}
            delta={kpi?.cm1_delta_pct}
            icon={Percent}
            note={
              kpi
                ? [
                    `CM1: ${formatPLN(kpi.total_cm1_pln)} (${formatPct(kpi.total_cm1_percent)})`,
                    lowCogsCoverage ? `Pokrycie COGS: ${formatPct(cogsCoverage)} · fee: ${formatPct(feesCoverage)} · partial` : null,
                    `CM2: ${formatPLN(kpi.total_cm2_pln)} (${formatPct(kpi.total_cm2_percent)})`,
                    kpi.total_overhead_pln ? `Overhead: ${formatPLN(kpi.total_overhead_pln)}` : null,
                  ].filter(Boolean).join("\n")
                : undefined
            }
            color={
              (kpi?.total_net_profit_percent ?? 0) >= 15
                ? "text-green-500"
                : (kpi?.total_net_profit_percent ?? 0) >= 5
                ? "text-amber-500"
                : "text-destructive"
            }
          />
          <KPICard
            title="Courier Cost"
            value={formatPLN(kpi?.total_courier_cost_pln ?? 0)}
            icon={Package}
            color="text-orange-400"
            note={
              kpi
                ? [
                    kpi.fbm_coverage_pct != null
                      ? `Pokrycie: ${formatPct(kpi.fbm_coverage_pct)} zamówień FBM (${formatPct(kpi.fbm_billing_pct ?? 0)} z faktur)`
                      : null,
                    kpi.fbm_logistics_by_mkt?.length
                      ? `Śr. paczka FBM: ${kpi.fbm_logistics_by_mkt.map((m) => `${m.mkt} ${m.avg_cost.toFixed(1)} zł`).join(" · ")}`
                      : null,
                    kpi.fbm_logistics_by_mkt?.some((m) => m.billing_avg_cost != null)
                      ? `Śr. z faktur: ${kpi.fbm_logistics_by_mkt.filter((m) => m.billing_avg_cost != null).map((m) => `${m.mkt} ${m.billing_avg_cost!.toFixed(1)} zł`).join(" · ")}`
                      : null,
                  ]
                    .filter(Boolean)
                    .join("\n")
                : undefined
            }
          />
          <KPICard
            title="Returns"
            value={`${(kpi?.total_refund_units ?? 0).toLocaleString("pl-PL")} szt.`}
            icon={RotateCcw}
            color="text-rose-400"
            note={
              kpi
                ? [
                    `Refund: ${formatPLN(kpi.total_refund_pln)}`,
                    kpi.total_return_rate_pct != null ? `Return rate: ${formatPct(kpi.total_return_rate_pct)}` : null,
                  ].filter(Boolean).join("\n")
                : undefined
            }
          />
          <KPICard
            title="Units Sold"
            value={(kpi?.total_units ?? 0).toLocaleString("pl-PL")}
            icon={Package}
            note={
              kpi
                ? `FBA: ${kpi.fba_orders} zam. · ${kpi.fba_units_per_order?.toFixed(1) ?? "–"} szt/zam\nFBM: ${kpi.fbm_orders} zam. · ${kpi.fbm_units_per_order?.toFixed(1) ?? "–"} szt/zam`
                : undefined
            }
          />
          <KPICard
            title="Avg Order Value"
            value={formatPLN(kpi?.avg_order_value_pln ?? 0)}
            icon={BarChart3}
          />
          <KPICard
            title="Ad Spend"
            value={formatPLN(kpi?.total_ads_spend_pln ?? 0)}
            icon={Megaphone}
            color="text-violet-400"
          />
          <KPICard
            title="TACoS"
            value={kpi?.total_tacos != null ? formatPct(kpi.total_tacos) : "—"}
            icon={Percent}
            color={
              kpi?.total_tacos == null
                ? "text-muted-foreground"
                : kpi.total_tacos <= 8
                ? "text-green-500"
                : kpi.total_tacos <= 15
                ? "text-amber-500"
                : "text-destructive"
            }
            note={
              kpi?.total_acos != null
                ? `ACoS (PPC): ${formatPct(kpi.total_acos)} · Ad spend / total revenue`
                : "Ad spend / total revenue"
            }
          />
        </div>
      )}

      {/* ===== Revenue chart ===== */}
      <div className="rounded-xl border border-border bg-card p-5">
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-sm font-semibold">KPI w czasie</h2>
          <div className="flex flex-wrap items-center gap-1.5">
            {CHART_SERIES.map((s) => {
              const active = chartVisible[s.key] && !(lowCogsCoverage && (s.key === "cm1_pln" || s.key === "cm1_percent"));
              const disabled = lowCogsCoverage && (s.key === "cm1_pln" || s.key === "cm1_percent");
              return (
                <button
                  key={s.key}
                  onClick={() => !disabled && toggleSeries(s.key)}
                  disabled={disabled}
                  className={cn(
                    "flex items-center gap-1.5 rounded-lg border px-2.5 py-1 text-xs font-medium transition-colors",
                    disabled
                      ? "cursor-not-allowed border-border/50 text-muted-foreground/40"
                      : active
                      ? "border-border bg-muted text-foreground"
                      : "border-transparent text-muted-foreground hover:text-foreground"
                  )}
                >
                  <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: disabled ? "hsl(var(--muted-foreground))" : s.color }} />
                  {s.label}
                </button>
              );
            })}
            {lowCogsCoverage && (
              <span className="rounded-lg border border-amber-500/30 bg-amber-500/5 px-2.5 py-1 text-xs text-amber-300">
                CM partial · COGS {formatPct(cogsCoverage)}
              </span>
            )}
          </div>
        </div>
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart data={chartData}>
            <defs>
              <linearGradient id="revGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#FF9900" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#FF9900" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="cmGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#22c55e" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
            <XAxis dataKey="date" tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }} />
            <YAxis
              yAxisId="pln"
              tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }}
              tickFormatter={(value: number) => formatPLN(value)}
              width={88}
            />
            <YAxis
              yAxisId="pct"
              orientation="right"
              tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }}
              tickFormatter={(value: number) => `${value}%`}
              width={52}
              domain={[0, "auto"]}
            />
            {showCountAxis && (
              <YAxis
                yAxisId="count"
                orientation="right"
                tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }}
                tickFormatter={(value: number) => value.toLocaleString("pl-PL")}
                width={52}
              />
            )}
            <Tooltip
              contentStyle={{
                background: "hsl(var(--card))",
                border: "1px solid hsl(var(--border))",
                borderRadius: "8px",
                fontSize: "12px",
              }}
              formatter={(value: number, name: string) => {
                if (name === "CM%") return [formatPct(value), name];
                if (name === "Orders") return [value.toLocaleString("pl-PL"), name];
                return [formatPLN(value), name];
              }}
            />
            <Legend wrapperStyle={{ fontSize: "12px" }} />
            {chartVisible["revenue_pln"] && (
              <Area yAxisId="pln" type="monotone" dataKey="revenue_pln" name="Revenue" stroke="#FF9900" fill="url(#revGrad)" strokeWidth={2} dot={false} />
            )}
            {chartVisible["cm1_pln"] && !lowCogsCoverage && (
              <Area yAxisId="pln" type="monotone" dataKey="cm1_pln" name="CM1" stroke="#22c55e" fill="url(#cmGrad)" strokeWidth={2} dot={false} />
            )}
            {chartVisible["cm1_percent"] && !lowCogsCoverage && (
              <Line yAxisId="pct" type="monotone" dataKey="cm1_percent" name="CM%" stroke="#38bdf8" strokeWidth={2} dot={false} activeDot={{ r: 4 }} />
            )}
            {chartVisible["orders"] && (
              <Bar yAxisId="count" dataKey="orders" name="Orders" fill="#a78bfa" opacity={0.4} radius={[2, 2, 0, 0]} />
            )}
          </ComposedChart>
        </ResponsiveContainer>
        {lowCogsCoverage ? (
          <div className="mt-3 text-xs leading-5 text-white/50">
            Linie CM1 i CM% są ukryte, bo historyczne revenue jest już w bazie, ale line-level COGS nadal nie ma pełnego pokrycia dla tego zakresu.
          </div>
        ) : null}
      </div>

      {/* ===== Marketplace breakdown ===== */}
      {kpi?.by_marketplace && kpi.by_marketplace.length > 0 && (
        <div className="rounded-xl border border-border bg-card p-5">
          <h2 className="mb-4 text-sm font-semibold">By Marketplace</h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-xs text-muted-foreground">
                  <th className="pb-2 pr-4">Marketplace</th>
                  <th className="pb-2 pr-4 text-right">Revenue</th>
                  <th className="pb-2 pr-4 text-right">Orders</th>
                  <th className="pb-2 pr-4 text-right">Units</th>
                  <th className="pb-2 pr-4 text-right">AOV</th>
                  <th className="pb-2 pr-4 text-right">CM1</th>
                  <th className="pb-2 pr-4 text-right">CM%</th>
                  <th className="pb-2 pr-4 text-right">CM2</th>
                  <th className="pb-2 pr-4 text-right">NP</th>
                  <th className="pb-2 pr-4 text-right">Ad Spend</th>
                  <th className="pb-2 text-right">ACoS</th>
                </tr>
              </thead>
              <tbody>
                {kpi.by_marketplace.map((m) => (
                  <tr key={m.marketplace_id} className="border-b border-border/50 last:border-0">
                    <td className="py-2.5 pr-4 font-medium">{m.marketplace_code}</td>
                    <td className="py-2.5 pr-4 text-right tabular-nums">{formatPLN(m.revenue_pln)}</td>
                    <td className="py-2.5 pr-4 text-right tabular-nums">{m.orders.toLocaleString("pl-PL")}</td>
                    <td className="py-2.5 pr-4 text-right tabular-nums">{m.units.toLocaleString("pl-PL")}</td>
                    <td className="py-2.5 pr-4 text-right tabular-nums">{formatPLN(m.avg_order_value_pln)}</td>
                    <td className="py-2.5 pr-4 text-right tabular-nums">{formatPLN(m.cm1_pln)}</td>
                    <td className={cn(
                      "py-2.5 pr-4 text-right tabular-nums font-medium",
                      m.cm1_percent >= 20 ? "text-green-500" : m.cm1_percent >= 10 ? "text-amber-500" : "text-destructive"
                    )}>
                      {formatPct(m.cm1_percent)}
                    </td>
                    <td className={cn(
                      "py-2.5 pr-4 text-right tabular-nums",
                      m.cm2_percent >= 10 ? "text-green-500" : m.cm2_percent >= 0 ? "text-amber-500" : "text-destructive"
                    )}>
                      {formatPLN(m.cm2_pln)}
                    </td>
                    <td className={cn(
                      "py-2.5 pr-4 text-right tabular-nums font-medium",
                      m.net_profit_percent >= 5 ? "text-green-500" : m.net_profit_percent >= 0 ? "text-amber-500" : "text-destructive"
                    )}>
                      {formatPLN(m.net_profit_pln)}
                    </td>
                    <td className="py-2.5 pr-4 text-right tabular-nums text-violet-400">
                      {formatPLN(m.ads_spend_pln)}
                    </td>
                    <td className="py-2.5 text-right tabular-nums text-muted-foreground">
                      {m.acos != null ? formatPct(m.acos) : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ===== Top Profit Drivers & Leaks ===== */}
      {drivers && (
        <div className="space-y-4">
          <div className="rounded-xl border border-border bg-card p-4">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
              <div>
                <h2 className="text-sm font-semibold">Top Drivers / Leaks Filters</h2>
                <p className="text-xs text-muted-foreground">
                  Brand i kategoria dzialaja tylko dla tego bloku. Kategoria idzie z PIM / acc_product.category.
                </p>
              </div>
              {(brandFilter || categoryFilter) && (
                <button
                  onClick={() => {
                    setBrandFilter("");
                    setCategoryFilter("");
                  }}
                  className="rounded-lg border border-border px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:text-foreground"
                >
                  Resetuj
                </button>
              )}
            </div>
            <div className="grid gap-3 md:grid-cols-2">
              <label className="space-y-1">
                <span className="text-xs font-medium text-muted-foreground">Brand</span>
                <input
                  value={brandFilter}
                  onChange={(e) => setBrandFilter(e.target.value)}
                  placeholder="np. KADAX"
                  className="w-full rounded-lg border border-border bg-card px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amazon/50"
                />
              </label>
              <label className="space-y-1">
                <span className="text-xs font-medium text-muted-foreground">Kategoria (PIM, PL)</span>
                <input
                  value={categoryFilter}
                  onChange={(e) => setCategoryFilter(e.target.value)}
                  placeholder="np. doniczki, suszarki, szklo"
                  className="w-full rounded-lg border border-border bg-card px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amazon/50"
                />
              </label>
            </div>
          </div>
          <div className="grid gap-4 lg:grid-cols-2">
            <DriversTable title="Top 15 Profit Drivers" icon={<ArrowUpRight className="h-4 w-4 text-green-500" />} items={drivers.drivers} positive />
            <DriversTable title="Top 15 Profit Leaks" icon={<ArrowDownRight className="h-4 w-4 text-destructive" />} items={drivers.leaks} positive={false} />
          </div>
        </div>
      )}

      {/* ===== Recent Alerts Panel ===== */}
      {recentAlerts && recentAlerts.length > 0 && (
        <div className="rounded-xl border border-border bg-card p-5">
          <div className="mb-3 flex items-center gap-2">
            <AlertTriangle className="h-4 w-4 text-amber-500" />
            <h2 className="text-sm font-semibold">Critical Alerts</h2>
          </div>
          <div className="space-y-2">
            {recentAlerts.map((a) => (
              <div
                key={a.id}
                className={cn(
                  "flex items-start gap-3 rounded-lg border p-3 text-sm",
                  a.severity === "critical"
                    ? "border-destructive/30 bg-destructive/5"
                    : a.severity === "warning"
                    ? "border-amber-500/30 bg-amber-500/5"
                    : "border-blue-500/30 bg-blue-500/5"
                )}
              >
                <span className={cn(
                  "mt-0.5 inline-block rounded px-1.5 py-0.5 text-[10px] font-bold uppercase leading-none",
                  a.severity === "critical" ? "bg-destructive text-white" :
                  a.severity === "warning" ? "bg-amber-500 text-black" : "bg-blue-500 text-white"
                )}>
                  {a.severity}
                </span>
                <div className="min-w-0 flex-1">
                  <p className="font-medium leading-tight">{a.title}</p>
                  {a.detail && <p className="mt-0.5 text-xs text-muted-foreground">{a.detail}</p>}
                </div>
                <div className="flex shrink-0 flex-col items-end gap-1">
                  {a.sku && <span className="font-mono text-xs text-muted-foreground">{a.sku}</span>}
                  <div className="flex items-center gap-2">
                    {a.marketplace_id && (
                      <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium text-muted-foreground">
                        {marketplaceOptions.find((o) => o.value === a.marketplace_id)?.label?.split(" ")[0] ?? a.marketplace_id}
                      </span>
                    )}
                    {a.triggered_at && (
                      <span className="flex items-center gap-1 text-[10px] text-muted-foreground/70">
                        <Clock className="h-2.5 w-2.5" />
                        {timeAgo(a.triggered_at)}
                      </span>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
