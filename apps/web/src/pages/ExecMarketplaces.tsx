import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { format, subDays } from "date-fns";
import { ArrowUpDown, ArrowUp, ArrowDown, Globe, AlertTriangle } from "lucide-react";
import { getExecMarketplaces } from "@/lib/api";
import type { ExecMarketplaceItem } from "@/lib/api";
import { formatPLN, formatPct, cn } from "@/lib/utils";
import { ProfitTierBadge } from "@/components/shared";

const MKT_FLAGS: Record<string, string> = {
  A1PA6795UKMFR9: "🇩🇪", A13V1IB3VIYZZH: "🇫🇷", APJ6JRA9NG5V4: "🇮🇹",
  A1RKKUPIHCS9HS: "🇪🇸", A1C3SOZRARQ6R3: "🇵🇱", A28R8C7NBKEWEA: "🇮🇪",
  A1805IZSGTT6HS: "🇳🇱", A2NODRKZP88ZB9: "🇸🇪", AMEN7PMS3EDWL: "🇧🇪",
};

type SortKey = "revenue_pln" | "profit_pln" | "margin_pct" | "orders" | "units" | "acos_pct" | "return_rate_pct" | "health_score";

const COLUMNS: { key: string; label: string; sortable: boolean; align: "left" | "right" }[] = [
  { key: "marketplace", label: "Marketplace", sortable: false, align: "left" },
  { key: "revenue_pln", label: "Revenue", sortable: true, align: "right" },
  { key: "profit_pln", label: "NP", sortable: true, align: "right" },
  { key: "margin_pct", label: "NP %", sortable: true, align: "right" },
  { key: "orders", label: "Orders", sortable: true, align: "right" },
  { key: "units", label: "Units", sortable: true, align: "right" },
  { key: "acos_pct", label: "ACoS", sortable: true, align: "right" },
  { key: "return_rate_pct", label: "Return %", sortable: true, align: "right" },
  { key: "health_score", label: "Health", sortable: true, align: "right" },
];

export default function ExecMarketplacesPage() {
  const [searchParams, setSearchParams] = useSearchParams();

  const sortField = (searchParams.get("sort") as SortKey) || "revenue_pln";
  const sortDir = (searchParams.get("dir") as "asc" | "desc") || "desc";
  const from = searchParams.get("from") || format(subDays(new Date(), 29), "yyyy-MM-dd");
  const to = searchParams.get("to") || format(new Date(), "yyyy-MM-dd");

  const updateParams = useCallback((patch: Record<string, string | null>) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      for (const [k, v] of Object.entries(patch)) {
        if (v === null || v === "") next.delete(k); else next.set(k, v);
      }
      return next;
    }, { replace: true });
  }, [setSearchParams]);

  const { data, isLoading, isError } = useQuery({
    queryKey: ["exec-marketplaces", from, to],
    queryFn: () => getExecMarketplaces({ from, to }),
    staleTime: 60_000,
  });

  const sorted = [...(data?.items ?? [])].sort((a, b) => {
    const av = (a as any)[sortField] ?? 0;
    const bv = (b as any)[sortField] ?? 0;
    return sortDir === "desc" ? bv - av : av - bv;
  });

  const handleSort = useCallback((field: string) => {
    const sf = field as SortKey;
    const newDir = sf === sortField ? (sortDir === "desc" ? "asc" : "desc") : "desc";
    updateParams({
      sort: sf === "revenue_pln" ? null : sf,
      dir: newDir === "desc" ? null : newDir,
    });
  }, [sortField, sortDir, updateParams]);

  const SortIcon = ({ field }: { field: string }) => {
    if (field !== sortField) return <ArrowUpDown className="h-3 w-3 opacity-30" />;
    return sortDir === "desc"
      ? <ArrowDown className="h-3 w-3 text-amazon" />
      : <ArrowUp className="h-3 w-3 text-amazon" />;
  };

  /* Summary row */
  const totals = sorted.reduce(
    (acc, m) => ({
      revenue: acc.revenue + (m.revenue_pln ?? 0),
      profit: acc.profit + (m.profit_pln ?? 0),
      orders: acc.orders + (m.orders ?? 0),
      units: acc.units + (m.units ?? 0),
    }),
    { revenue: 0, profit: 0, orders: 0, units: 0 },
  );

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center gap-3">
        <Globe className="h-6 w-6 text-amazon" />
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-2xl font-bold tracking-tight">Executive — Marketplaces</h1>
            <ProfitTierBadge tier="np" />
          </div>
          <p className="text-sm text-muted-foreground mt-0.5">Porównanie rynków Amazon</p>
        </div>
      </div>

      {/* Date filters */}
      <div className="flex items-end gap-3">
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">From</span>
          <input type="date" value={from} onChange={(e) => updateParams({ from: e.target.value || null })}
            className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm" />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">To</span>
          <input type="date" value={to} onChange={(e) => updateParams({ to: e.target.value || null })}
            className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm" />
        </label>
      </div>

      {isError && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">Failed to load marketplace data. Please try again later.</p>
        </div>
      )}

      {/* Table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              {COLUMNS.map((col) => (
                <th
                  key={col.key}
                  className={cn(
                    "px-4 py-2",
                    col.align === "right" ? "text-right" : "text-left",
                    col.sortable && "cursor-pointer select-none hover:text-foreground",
                  )}
                  onClick={col.sortable ? () => handleSort(col.key) : undefined}
                >
                  <span className="inline-flex items-center gap-1">
                    {col.label}
                    {col.sortable && <SortIcon field={col.key} />}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <tr key={i} className="border-b border-border/50">
                  <td colSpan={COLUMNS.length} className="px-4 py-3"><div className="h-4 bg-muted/30 rounded animate-pulse" /></td>
                </tr>
              ))
            ) : sorted.length === 0 ? (
              <tr><td colSpan={COLUMNS.length} className="px-4 py-8 text-center text-muted-foreground">No data for this period</td></tr>
            ) : (
              <>
                {sorted.map((m) => (
                  <tr key={m.marketplace_id} className="border-b border-border/50 hover:bg-muted/20">
                    <td className="px-4 py-2 font-medium">
                      {MKT_FLAGS[m.marketplace_id] ?? "🌐"} {m.marketplace_code ?? m.marketplace_id}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums">{formatPLN(m.revenue_pln)}</td>
                    <td className={cn("px-4 py-2 text-right tabular-nums font-medium",
                      (m.profit_pln ?? 0) >= 0 ? "text-green-500" : "text-destructive")}>{formatPLN(m.profit_pln)}</td>
                    <td className={cn("px-4 py-2 text-right tabular-nums",
                      (m.margin_pct ?? 0) >= 0 ? "text-green-500" : "text-destructive")}>{formatPct(m.margin_pct)}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{(m.orders ?? 0).toLocaleString()}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{(m.units ?? 0).toLocaleString()}</td>
                    <td className={cn("px-4 py-2 text-right tabular-nums",
                      (m.acos_pct ?? 0) > 30 ? "text-destructive" : "text-foreground")}>{formatPct(m.acos_pct)}</td>
                    <td className={cn("px-4 py-2 text-right tabular-nums",
                      (m.return_rate_pct ?? 0) > 5 ? "text-destructive" : "text-foreground")}>{formatPct(m.return_rate_pct)}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{m.health_score != null ? m.health_score.toFixed(0) : "—"}</td>
                  </tr>
                ))}
                {/* Totals row */}
                <tr className="font-bold bg-muted/30">
                  <td className="px-4 py-2">TOTAL</td>
                  <td className="px-4 py-2 text-right tabular-nums">{formatPLN(totals.revenue)}</td>
                  <td className={cn("px-4 py-2 text-right tabular-nums",
                    totals.profit >= 0 ? "text-green-500" : "text-destructive")}>{formatPLN(totals.profit)}</td>
                  <td className="px-4 py-2 text-right tabular-nums">
                    {totals.revenue > 0 ? formatPct((totals.profit / totals.revenue) * 100) : "—"}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums">{totals.orders.toLocaleString()}</td>
                  <td className="px-4 py-2 text-right tabular-nums">{totals.units.toLocaleString()}</td>
                  <td className="px-4 py-2 text-right" colSpan={3} />
                </tr>
              </>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
