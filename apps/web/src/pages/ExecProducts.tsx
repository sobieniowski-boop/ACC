import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { format, subDays } from "date-fns";
import { ChevronDown, ArrowUpDown, ArrowUp, ArrowDown, AlertTriangle } from "lucide-react";
import { getExecProducts } from "@/lib/api";
import type { ExecProductItem } from "@/lib/api";
import { formatPLN, formatPct, cn } from "@/lib/utils";
import { ProfitTierBadge, ServerPagination } from "@/components/shared";

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

type SortField = "revenue_pln" | "profit_pln" | "margin_pct" | "units" | "return_rate_pct" | "acos_pct";

const COLUMNS: { key: string; label: string; sortable: boolean; align: "left" | "right" }[] = [
  { key: "sku",             label: "SKU",          sortable: false, align: "left" },
  { key: "asin",            label: "ASIN",         sortable: false, align: "left" },
  { key: "mkt",             label: "MKT",          sortable: false, align: "left" },
  { key: "revenue_pln",     label: "Revenue",      sortable: true,  align: "right" },
  { key: "profit_pln",      label: "NP",           sortable: true,  align: "right" },
  { key: "margin_pct",      label: "NP %",         sortable: true,  align: "right" },
  { key: "units",           label: "Units",        sortable: true,  align: "right" },
  { key: "acos_pct",        label: "ACoS",         sortable: true,  align: "right" },
  { key: "return_rate_pct", label: "Return %",     sortable: true,  align: "right" },
  { key: "inventory_risk",  label: "Inv. Risk",    sortable: false, align: "left" },
];

const INV_RISK_COLORS: Record<string, string> = {
  ok: "bg-green-500/15 text-green-400",
  low: "bg-yellow-500/15 text-yellow-400",
  critical: "bg-orange-500/15 text-orange-400",
  stockout: "bg-red-500/15 text-red-400",
};

export default function ExecProductsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const pageSize = 50;

  const from = searchParams.get("from") || format(subDays(new Date(), 29), "yyyy-MM-dd");
  const to = searchParams.get("to") || format(new Date(), "yyyy-MM-dd");
  const marketplace = searchParams.get("mp") || "";
  const sku = searchParams.get("sku") || "";
  const sortField = (searchParams.get("sort") as SortField) || "profit_pln";
  const sortDir = (searchParams.get("dir") as "asc" | "desc") || "desc";
  const page = Number(searchParams.get("page")) || 1;

  const updateParams = useCallback((patch: Record<string, string | null>) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      for (const [k, v] of Object.entries(patch)) {
        if (v === null || v === "") next.delete(k); else next.set(k, v);
      }
      return next;
    }, { replace: true });
  }, [setSearchParams]);

  const setPage = useCallback((n: number) => updateParams({ page: n > 1 ? String(n) : null }), [updateParams]);

  const params: Record<string, string | number> = {
    from, to, page, page_size: pageSize,
    sort: sortField, dir: sortDir,
  };
  if (marketplace) params.marketplace_id = marketplace;
  if (sku) params.sku = sku;

  const { data, isLoading, isError } = useQuery({
    queryKey: ["exec-products", params],
    queryFn: () => getExecProducts(params),
    staleTime: 30_000,
  });

  const items: ExecProductItem[] = data?.items ?? [];

  const handleSort = useCallback((field: string) => {
    const sf = field as SortField;
    const newDir = sf === sortField ? (sortDir === "desc" ? "asc" : "desc") : "desc";
    updateParams({
      sort: sf === "profit_pln" ? null : sf,
      dir: newDir === "desc" ? null : newDir,
      page: null,
    });
  }, [sortField, sortDir, updateParams]);

  const SortIcon = ({ field }: { field: string }) => {
    if (field !== sortField) return <ArrowUpDown className="h-3 w-3 opacity-30" />;
    return sortDir === "desc"
      ? <ArrowDown className="h-3 w-3 text-amazon" />
      : <ArrowUp className="h-3 w-3 text-amazon" />;
  };

  return (
    <div className="space-y-4 p-6">
      <div>
        <div className="flex items-center gap-2">
          <h1 className="text-2xl font-bold tracking-tight">Executive — Products</h1>
          <ProfitTierBadge tier="np" />
        </div>
        <p className="text-sm text-muted-foreground mt-0.5">Rentowność i ryzyko na poziomie SKU</p>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-end gap-3">
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">From</span>
          <input type="date" value={from} onChange={(e) => updateParams({ from: e.target.value || null, page: null })}
            className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm" />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">To</span>
          <input type="date" value={to} onChange={(e) => updateParams({ to: e.target.value || null, page: null })}
            className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm" />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Marketplace</span>
          <div className="relative">
            <select value={marketplace} onChange={(e) => updateParams({ mp: e.target.value || null, page: null })}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm focus:outline-none">
              {MARKETPLACE_OPTIONS.map((m) => (
                <option key={m.id} value={m.id}>{m.label}</option>
              ))}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          </div>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">SKU</span>
          <input type="text" value={sku} onChange={(e) => updateParams({ sku: e.target.value || null, page: null })}
            placeholder="Filter by SKU…" className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm w-44" />
        </label>
      </div>

      {isError && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">Failed to load product data. Please try again later.</p>
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
              Array.from({ length: 10 }).map((_, i) => (
                <tr key={i} className="border-b border-border/50">
                  <td colSpan={COLUMNS.length} className="px-4 py-3"><div className="h-4 bg-muted/30 rounded animate-pulse" /></td>
                </tr>
              ))
            ) : items.length === 0 ? (
              <tr><td colSpan={COLUMNS.length} className="px-4 py-8 text-center text-muted-foreground">No products found</td></tr>
            ) : (
              items.map((p, i) => (
                <tr key={`${p.sku}-${p.marketplace_id}-${i}`} className="border-b border-border/50 hover:bg-muted/20">
                  <td className="px-4 py-2 font-mono text-xs truncate max-w-[160px]" title={p.sku}>{p.sku}</td>
                  <td className="px-4 py-2 font-mono text-xs">{p.asin || "—"}</td>
                  <td className="px-4 py-2">{p.marketplace_code || "—"}</td>
                  <td className="px-4 py-2 text-right tabular-nums">{formatPLN(p.revenue_pln)}</td>
                  <td className={cn("px-4 py-2 text-right tabular-nums font-medium",
                    p.profit_pln >= 0 ? "text-green-500" : "text-destructive")}>{formatPLN(p.profit_pln)}</td>
                  <td className={cn("px-4 py-2 text-right tabular-nums",
                    (p.margin_pct ?? 0) >= 0 ? "text-green-500" : "text-destructive")}>
                    {p.margin_pct != null ? formatPct(p.margin_pct) : "—"}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums">{p.units.toLocaleString()}</td>
                  <td className={cn("px-4 py-2 text-right tabular-nums",
                    (p.acos_pct ?? 0) > 30 ? "text-destructive" : "text-foreground")}>
                    {p.acos_pct != null ? formatPct(p.acos_pct) : "—"}
                  </td>
                  <td className={cn("px-4 py-2 text-right tabular-nums",
                    (p.return_rate_pct ?? 0) > 5 ? "text-destructive" : "text-foreground")}>
                    {p.return_rate_pct != null ? formatPct(p.return_rate_pct) : "—"}
                  </td>
                  <td className="px-4 py-2">
                    {p.inventory_risk ? (
                      <span className={cn("rounded px-1.5 py-0.5 text-[10px] font-bold uppercase",
                        INV_RISK_COLORS[p.inventory_risk] || "bg-muted text-muted-foreground")}>
                        {p.inventory_risk}
                      </span>
                    ) : "—"}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {data && data.pages > 1 && (
        <ServerPagination page={page} pages={data.pages} total={data.total} pageSize={pageSize} onPageChange={setPage} />
      )}
    </div>
  );
}
