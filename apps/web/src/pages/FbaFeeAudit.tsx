import { useState, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { format, subDays } from "date-fns";
import {
  ChevronDown,
  AlertTriangle,
  SearchX,
  DollarSign,
  TrendingUp,
  ShieldAlert,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";
import { getFbaFeeAnomalies, getFbaFeeOvercharges } from "@/lib/api";
import type { FbaFeeAnomaly, FbaSkuOvercharge } from "@/lib/api";
import { cn } from "@/lib/utils";

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const MARKETPLACE_OPTIONS = [
  { id: "", label: "All markets" },
  { id: "A1PA6795UKMFR9", label: "🇩🇪 DE" },
  { id: "A13V1IB3VIYZZH", label: "🇫🇷 FR" },
  { id: "APJ6JRA9NG5V4", label: "🇮🇹 IT" },
  { id: "A1RKKUPIHCS9HS", label: "🇪🇸 ES" },
  { id: "A1C3SOZRARQ6R3", label: "🇵🇱 PL" },
  { id: "A1805IZSGTT6HS", label: "🇳🇱 NL" },
  { id: "A2NODRKZP88ZB9", label: "🇸🇪 SE" },
  { id: "AMEN7PMS3EDWL", label: "🇧🇪 BE" },
  { id: "A28R8C7NBKEWEA", label: "🇮🇪 IE" },
];

const SEVERITY_OPTIONS = [
  { id: "", label: "All severities" },
  { id: "critical", label: "Critical" },
  { id: "high", label: "High" },
  { id: "medium", label: "Medium" },
];

type TabKey = "overcharges" | "anomalies";
const PAGE_SIZE = 25;

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function eur(v: number | null | undefined): string {
  if (v == null || isNaN(v)) return "€0.00";
  return new Intl.NumberFormat("de-DE", {
    style: "currency",
    currency: "EUR",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(v);
}

function severityBadge(severity: string | null) {
  if (!severity) return <span className="text-muted-foreground">—</span>;
  const map: Record<string, string> = {
    critical: "text-red-400 bg-red-400/10",
    high: "text-orange-400 bg-orange-400/10",
    medium: "text-amber-400 bg-amber-400/10",
  };
  return (
    <span className={cn("inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium capitalize", map[severity] ?? "text-muted-foreground bg-muted/20")}>
      {severity}
    </span>
  );
}

function KpiCard({ label, value, sub, icon: Icon, accent }: {
  label: string; value: string; sub?: string; icon: typeof DollarSign; accent?: string;
}) {
  return (
    <div className="rounded-xl border border-border bg-card p-4 space-y-1">
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        <Icon className={cn("h-4 w-4", accent)} />
        {label}
      </div>
      <div className="text-xl font-bold tracking-tight">{value}</div>
      {sub && <div className="text-xs text-muted-foreground">{sub}</div>}
    </div>
  );
}

function ClientPagination({ page, totalPages, total, pageSize, onPageChange }: {
  page: number; totalPages: number; total: number; pageSize: number; onPageChange: (p: number) => void;
}) {
  if (totalPages <= 1) return null;
  const from = (page - 1) * pageSize + 1;
  const to = Math.min(page * pageSize, total);
  return (
    <div className="flex items-center justify-between text-sm text-muted-foreground">
      <span>{from}–{to} of {total}</span>
      <div className="flex items-center gap-1">
        <button onClick={() => onPageChange(page - 1)} disabled={page <= 1}
          className="rounded-lg border border-border px-2 py-1 hover:bg-muted/30 disabled:opacity-30 disabled:cursor-not-allowed">
          <ChevronLeft className="h-4 w-4" />
        </button>
        <span className="px-2 tabular-nums">{page} / {totalPages}</span>
        <button onClick={() => onPageChange(page + 1)} disabled={page >= totalPages}
          className="rounded-lg border border-border px-2 py-1 hover:bg-muted/30 disabled:opacity-30 disabled:cursor-not-allowed">
          <ChevronRight className="h-4 w-4" />
        </button>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Page component                                                     */
/* ------------------------------------------------------------------ */

export default function FbaFeeAuditPage() {
  const [searchParams, setSearchParams] = useSearchParams();

  // Read URL-backed filters
  const tab = (searchParams.get("tab") as TabKey) || "overcharges";
  const marketplace = searchParams.get("mp") || "";
  const skuFilter = searchParams.get("sku") || "";
  const severityFilter = searchParams.get("sev") || "";
  const dateFrom = searchParams.get("from") || format(subDays(new Date(), 89), "yyyy-MM-dd");
  const dateTo = searchParams.get("to") || format(new Date(), "yyyy-MM-dd");
  const [page, setPage] = useState(1);

  // URL update helper — preserves other params
  const setParam = (key: string, value: string) => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (value) next.set(key, value);
      else next.delete(key);
      return next;
    });
    setPage(1);
  };

  /* ── Overcharges query ── */
  const overchargeParams: Record<string, string | number> = {};
  if (dateFrom) overchargeParams.date_from = dateFrom;
  if (dateTo) overchargeParams.date_to = dateTo;
  if (marketplace) overchargeParams.marketplace_id = marketplace;

  const {
    data: overchargeData,
    isLoading: overLoading,
    isError: overError,
  } = useQuery({
    queryKey: ["fba-fee-overcharges", overchargeParams],
    queryFn: () => getFbaFeeOvercharges(overchargeParams),
    staleTime: 60_000,
    enabled: tab === "overcharges",
  });

  /* ── Anomalies query ── */
  const anomalyParams: Record<string, string | number> = {};
  if (marketplace) anomalyParams.marketplace_id = marketplace;

  const {
    data: anomalyData,
    isLoading: anomLoading,
    isError: anomError,
  } = useQuery({
    queryKey: ["fba-fee-anomalies", anomalyParams],
    queryFn: () => getFbaFeeAnomalies(anomalyParams),
    staleTime: 60_000,
    enabled: tab === "anomalies",
  });

  /* ── Client-side filtering + pagination ── */
  const filteredOvercharges = useMemo(() => {
    let items = overchargeData?.items ?? [];
    if (skuFilter) {
      const q = skuFilter.toLowerCase();
      items = items.filter(
        (i) =>
          i.sku?.toLowerCase().includes(q) ||
          i.asin?.toLowerCase().includes(q)
      );
    }
    if (severityFilter) {
      items = items.filter((i) => i.severity === severityFilter);
    }
    return items;
  }, [overchargeData, skuFilter, severityFilter]);

  const overTotalPages = Math.max(1, Math.ceil(filteredOvercharges.length / PAGE_SIZE));
  const pagedOvercharges = filteredOvercharges.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  const filteredAnomalies = useMemo(() => {
    let items = anomalyData?.anomalies ?? [];
    if (skuFilter) {
      const q = skuFilter.toLowerCase();
      items = items.filter(
        (i) =>
          i.sku?.toLowerCase().includes(q) ||
          i.asin?.toLowerCase().includes(q)
      );
    }
    if (severityFilter) {
      items = items.filter((i) => i.severity === severityFilter);
    }
    return items;
  }, [anomalyData, skuFilter, severityFilter]);

  const anomTotalPages = Math.max(1, Math.ceil(filteredAnomalies.length / PAGE_SIZE));
  const pagedAnomalies = filteredAnomalies.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  const isLoading = tab === "overcharges" ? overLoading : anomLoading;
  const isError = tab === "overcharges" ? overError : anomError;

  return (
    <div className="space-y-4 p-6">
      {/* Header */}
      <div className="flex items-center gap-2">
        <SearchX className="h-6 w-6 text-amber-500" />
        <h1 className="text-2xl font-bold tracking-tight">FBA Fee Audit</h1>
      </div>

      {/* KPI summary — from overcharges */}
      {tab === "overcharges" && overchargeData && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <KpiCard
            label="Total overcharge"
            value={eur(overchargeData.total_estimated_overcharge_eur)}
            icon={DollarSign}
            accent="text-red-400"
          />
          <KpiCard
            label="SKUs affected"
            value={overchargeData.total_skus_affected.toLocaleString()}
            icon={ShieldAlert}
            accent="text-amber-400"
          />
          <KpiCard
            label="Affected orders"
            value={overchargeData.total_affected_orders.toLocaleString()}
            icon={TrendingUp}
            accent="text-orange-400"
          />
          <KpiCard
            label="Scan date"
            value={overchargeData.scan_date?.slice(0, 10) ?? "—"}
            icon={SearchX}
          />
        </div>
      )}

      {/* KPI summary — from anomalies */}
      {tab === "anomalies" && anomalyData && (
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
          <KpiCard
            label="Anomalies found"
            value={anomalyData.total_anomalies.toLocaleString()}
            icon={ShieldAlert}
            accent="text-amber-400"
          />
          <KpiCard
            label="Est. overcharge"
            value={eur(anomalyData.total_estimated_overcharge_eur)}
            icon={DollarSign}
            accent="text-red-400"
          />
          <KpiCard
            label="Period"
            value={anomalyData.scan_period?.date_from?.slice(0, 10) ?? "—"}
            sub={`→ ${anomalyData.scan_period?.date_to?.slice(0, 10) ?? "now"}`}
            icon={TrendingUp}
          />
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 border-b border-border">
        {([
          { key: "overcharges" as TabKey, label: "Overcharges" },
          { key: "anomalies" as TabKey, label: "Anomalies" },
        ]).map((t) => (
          <button
            key={t.key}
            onClick={() => { setParam("tab", t.key === "overcharges" ? "" : t.key); setPage(1); }}
            className={cn(
              "px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors",
              tab === t.key
                ? "border-amber-500 text-foreground"
                : "border-transparent text-muted-foreground hover:text-foreground"
            )}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-end gap-3">
        {tab === "overcharges" && (
          <>
            <label className="space-y-1">
              <span className="text-xs text-muted-foreground">From</span>
              <input type="date" value={dateFrom} onChange={(e) => setParam("from", e.target.value)}
                className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm" />
            </label>
            <label className="space-y-1">
              <span className="text-xs text-muted-foreground">To</span>
              <input type="date" value={dateTo} onChange={(e) => setParam("to", e.target.value)}
                className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm" />
            </label>
          </>
        )}
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Marketplace</span>
          <div className="relative">
            <select value={marketplace} onChange={(e) => setParam("mp", e.target.value)}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm focus:outline-none">
              {MARKETPLACE_OPTIONS.map((m) => (
                <option key={m.id} value={m.id}>{m.label}</option>
              ))}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          </div>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">SKU / ASIN</span>
          <input type="text" value={skuFilter} onChange={(e) => setParam("sku", e.target.value)}
            placeholder="Filter…" className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm w-44" />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Severity</span>
          <div className="relative">
            <select value={severityFilter} onChange={(e) => setParam("sev", e.target.value)}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm focus:outline-none">
              {SEVERITY_OPTIONS.map((s) => (
                <option key={s.id} value={s.id}>{s.label}</option>
              ))}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          </div>
        </label>
      </div>

      {/* Error state */}
      {isError && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">Failed to load fee audit data. Please try again later.</p>
        </div>
      )}

      {/* ════════════ Overcharges Tab ════════════ */}
      {tab === "overcharges" && (
        <>
          <div className="rounded-xl border border-border bg-card overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-xs text-muted-foreground uppercase">
                  <th className="px-4 py-2 text-left">SKU</th>
                  <th className="px-4 py-2 text-left">ASIN</th>
                  <th className="px-4 py-2 text-left">Title</th>
                  <th className="px-4 py-2 text-left">Currency</th>
                  <th className="px-4 py-2 text-right">Median fee</th>
                  <th className="px-4 py-2 text-right">Threshold</th>
                  <th className="px-4 py-2 text-right">Overcharged orders</th>
                  <th className="px-4 py-2 text-right">Est. overcharge</th>
                  <th className="px-4 py-2 text-right">Overcharge (EUR)</th>
                  <th className="px-4 py-2 text-left">Severity</th>
                </tr>
              </thead>
              <tbody>
                {isLoading ? (
                  Array.from({ length: 8 }).map((_, i) => (
                    <tr key={i} className="border-b border-border/50">
                      <td colSpan={10} className="px-4 py-3">
                        <div className="h-4 bg-muted/30 rounded animate-pulse" />
                      </td>
                    </tr>
                  ))
                ) : pagedOvercharges.length === 0 ? (
                  <tr>
                    <td colSpan={10} className="px-4 py-12 text-center text-muted-foreground">
                      <SearchX className="mx-auto h-8 w-8 mb-2 opacity-30" />
                      No overcharged SKUs found for the selected filters.
                    </td>
                  </tr>
                ) : (
                  pagedOvercharges.map((item, idx) => (
                    <OverchargeRow key={`${item.sku}-${idx}`} item={item} />
                  ))
                )}
              </tbody>
            </table>
          </div>
          <ClientPagination
            page={page}
            totalPages={overTotalPages}
            total={filteredOvercharges.length}
            pageSize={PAGE_SIZE}
            onPageChange={setPage}
          />
        </>
      )}

      {/* ════════════ Anomalies Tab ════════════ */}
      {tab === "anomalies" && (
        <>
          <div className="rounded-xl border border-border bg-card overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-xs text-muted-foreground uppercase">
                  <th className="px-4 py-2 text-left">SKU</th>
                  <th className="px-4 py-2 text-left">ASIN</th>
                  <th className="px-4 py-2 text-left">Title</th>
                  <th className="px-4 py-2 text-left">Currency</th>
                  <th className="px-4 py-2 text-left">Prev period</th>
                  <th className="px-4 py-2 text-right">Prev avg fee</th>
                  <th className="px-4 py-2 text-left">Current period</th>
                  <th className="px-4 py-2 text-right">Curr avg fee</th>
                  <th className="px-4 py-2 text-right">Ratio</th>
                  <th className="px-4 py-2 text-right">Est. overcharge</th>
                  <th className="px-4 py-2 text-left">Severity</th>
                  <th className="px-4 py-2 text-left">Recommendation</th>
                </tr>
              </thead>
              <tbody>
                {isLoading ? (
                  Array.from({ length: 8 }).map((_, i) => (
                    <tr key={i} className="border-b border-border/50">
                      <td colSpan={12} className="px-4 py-3">
                        <div className="h-4 bg-muted/30 rounded animate-pulse" />
                      </td>
                    </tr>
                  ))
                ) : pagedAnomalies.length === 0 ? (
                  <tr>
                    <td colSpan={12} className="px-4 py-12 text-center text-muted-foreground">
                      <SearchX className="mx-auto h-8 w-8 mb-2 opacity-30" />
                      No fee anomalies detected.
                    </td>
                  </tr>
                ) : (
                  pagedAnomalies.map((a, idx) => (
                    <AnomalyRow key={`${a.sku}-${idx}`} item={a} />
                  ))
                )}
              </tbody>
            </table>
          </div>
          <ClientPagination
            page={page}
            totalPages={anomTotalPages}
            total={filteredAnomalies.length}
            pageSize={PAGE_SIZE}
            onPageChange={setPage}
          />
        </>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Row components                                                     */
/* ------------------------------------------------------------------ */

function OverchargeRow({ item }: { item: FbaSkuOvercharge }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <>
      <tr
        className="border-b border-border/50 hover:bg-muted/20 cursor-pointer"
        onClick={() => setExpanded(!expanded)}
      >
        <td className="px-4 py-2 font-mono text-xs truncate max-w-[140px]" title={item.sku}>{item.sku}</td>
        <td className="px-4 py-2 font-mono text-xs">{item.asin || "—"}</td>
        <td className="px-4 py-2 text-xs truncate max-w-[180px]" title={item.title ?? ""}>{item.title || "—"}</td>
        <td className="px-4 py-2 text-xs">{item.currency}</td>
        <td className="px-4 py-2 text-right tabular-nums">{eur(item.median_fee)}</td>
        <td className="px-4 py-2 text-right tabular-nums">{eur(item.threshold)}</td>
        <td className="px-4 py-2 text-right tabular-nums text-amber-400">{item.overcharged_order_count}</td>
        <td className="px-4 py-2 text-right tabular-nums text-red-400 font-medium">{eur(item.estimated_overcharge)}</td>
        <td className="px-4 py-2 text-right tabular-nums text-red-400">{eur(item.estimated_overcharge_eur)}</td>
        <td className="px-4 py-2">{severityBadge(item.severity)}</td>
      </tr>
      {expanded && item.overcharged_orders && item.overcharged_orders.length > 0 && (
        <tr className="bg-muted/10">
          <td colSpan={10} className="px-6 py-3">
            <div className="text-xs text-muted-foreground mb-2">
              Overcharged orders ({item.overcharged_orders.length}{item.overcharged_orders.length >= 50 ? "+" : ""}):
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-muted-foreground">
                    <th className="px-3 py-1 text-left">Order ID</th>
                    <th className="px-3 py-1 text-left">Date</th>
                    <th className="px-3 py-1 text-right">Expected fee</th>
                    <th className="px-3 py-1 text-right">Actual fee</th>
                    <th className="px-3 py-1 text-right">Overcharge</th>
                  </tr>
                </thead>
                <tbody>
                  {item.overcharged_orders.map((o, oi) => (
                    <tr key={oi} className="border-t border-border/30">
                      <td className="px-3 py-1 font-mono">{o.order_id}</td>
                      <td className="px-3 py-1">{o.date?.slice(0, 10)}</td>
                      <td className="px-3 py-1 text-right tabular-nums">{eur(o.expected_fee)}</td>
                      <td className="px-3 py-1 text-right tabular-nums">{eur(o.actual_fee)}</td>
                      <td className="px-3 py-1 text-right tabular-nums text-red-400">{eur(o.overcharge)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

function AnomalyRow({ item }: { item: FbaFeeAnomaly }) {
  return (
    <tr className="border-b border-border/50 hover:bg-muted/20">
      <td className="px-4 py-2 font-mono text-xs truncate max-w-[140px]" title={item.sku}>{item.sku}</td>
      <td className="px-4 py-2 font-mono text-xs">{item.asin || "—"}</td>
      <td className="px-4 py-2 text-xs truncate max-w-[160px]" title={item.title ?? ""}>{item.title || "—"}</td>
      <td className="px-4 py-2 text-xs">{item.currency}</td>
      <td className="px-4 py-2 text-xs">{item.previous_period?.week_start?.slice(0, 10) ?? "—"}</td>
      <td className="px-4 py-2 text-right tabular-nums">{eur(item.previous_period?.avg_fee)}</td>
      <td className="px-4 py-2 text-xs">{item.current_period?.week_start?.slice(0, 10) ?? "—"}</td>
      <td className="px-4 py-2 text-right tabular-nums">{eur(item.current_period?.avg_fee)}</td>
      <td className="px-4 py-2 text-right tabular-nums font-medium text-amber-400">
        {item.fee_ratio?.toFixed(2)}x
      </td>
      <td className="px-4 py-2 text-right tabular-nums text-red-400 font-medium">{eur(item.estimated_overcharge)}</td>
      <td className="px-4 py-2">{severityBadge(item.severity)}</td>
      <td className="px-4 py-2 text-xs truncate max-w-[200px]" title={item.recommendation ?? ""}>
        {item.recommendation || "—"}
      </td>
    </tr>
  );
}
