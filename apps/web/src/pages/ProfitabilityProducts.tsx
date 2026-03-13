import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, subDays } from "date-fns";
import { ChevronDown, ArrowUpDown, ArrowUp, ArrowDown, AlertTriangle, Info } from "lucide-react";
import { getProfitabilityProducts } from "@/lib/api";
import type { ProfitabilityProductItem } from "@/lib/api";
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

type SortField = "revenue_pln" | "cm1_pln" | "cm2_pln" | "profit_pln" | "margin_pct" | "units" | "acos_pct" | "return_rate_pct";
type SortDir = "asc" | "desc";

const COLUMNS: { key: SortField | "sku" | "asin" | "mkt" | "orders" | "cogs_pln" | "amazon_fees_pln" | "logistics_pln" | "ad_spend_pln" | "refund_pln"; label: string; sortable: boolean; align: "left" | "right" }[] = [
  { key: "sku",              label: "SKU",       sortable: false, align: "left" },
  { key: "asin",             label: "ASIN",      sortable: false, align: "left" },
  { key: "mkt",              label: "MKT",       sortable: false, align: "left" },
  { key: "units",            label: "Units",     sortable: true,  align: "right" },
  { key: "orders",           label: "Orders",    sortable: false, align: "right" },
  { key: "revenue_pln",      label: "Revenue",   sortable: true,  align: "right" },
  { key: "cogs_pln",         label: "COGS",      sortable: false, align: "right" },
  { key: "amazon_fees_pln",  label: "Fees",      sortable: false, align: "right" },
  { key: "logistics_pln",    label: "Logistics", sortable: false, align: "right" },
  { key: "ad_spend_pln",     label: "Ad Spend",  sortable: false, align: "right" },
  { key: "refund_pln",       label: "Refund",    sortable: false, align: "right" },
  { key: "cm1_pln",          label: "CM1",       sortable: true,  align: "right" },
  { key: "cm2_pln",          label: "CM2",       sortable: true,  align: "right" },
  { key: "profit_pln",       label: "NP",        sortable: true,  align: "right" },
  { key: "margin_pct",       label: "NP %",      sortable: true,  align: "right" },
  { key: "acos_pct",         label: "ACoS",      sortable: true,  align: "right" },
  { key: "return_rate_pct",  label: "Return %",  sortable: true,  align: "right" },
];

export default function ProfitabilityProductsPage() {
  const [page, setPage] = useState(1);
  const [marketplace, setMarketplace] = useState("");
  const [sku, setSku] = useState("");
  const [sortField, setSortField] = useState<SortField>("profit_pln");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [from, setFrom] = useState(() => format(subDays(new Date(), 29), "yyyy-MM-dd"));
  const [to, setTo] = useState(() => format(new Date(), "yyyy-MM-dd"));
  const pageSize = 50;

  const params: Record<string, string | number> = {
    from, to, page, page_size: pageSize,
    sort_by: sortField, sort_dir: sortDir,
  };
  if (marketplace) params.marketplace_id = marketplace;
  if (sku) params.sku = sku;

  const { data, isLoading, isError } = useQuery({
    queryKey: ["profitability-products", params],
    queryFn: () => getProfitabilityProducts(params),
    staleTime: 30_000,
  });

  const items: ProfitabilityProductItem[] = data?.items ?? [];

  const handleSort = (field: string) => {
    const sf = field as SortField;
    if (sf === sortField) {
      setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    } else {
      setSortField(sf);
      setSortDir("desc");
    }
    setPage(1);
  };

  const SortIcon = ({ field }: { field: string }) => {
    if (field !== sortField) return <ArrowUpDown className="h-3 w-3 opacity-30" />;
    return sortDir === "desc"
      ? <ArrowDown className="h-3 w-3 text-amazon" />
      : <ArrowUp className="h-3 w-3 text-amazon" />;
  };

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center gap-2">
        <h1 className="text-2xl font-bold tracking-tight">Profitability — Products</h1>
        <ProfitTierBadge tier="np" />
      </div>

      {/* Deprecation notice */}
      <div role="status" className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-300">
        <div className="flex items-start gap-2">
          <Info className="mt-0.5 h-4 w-4 shrink-0 text-amber-400" />
          <div>
            <p className="font-medium">Ten widok jest przestarzały.</p>
            <p className="mt-0.5 text-xs text-amber-300/80">
              Przejdź do <a href="/profit/products" className="underline hover:text-amber-200">Products</a> — tam znajdziesz pełną analizę CM1/CM2/NP z lepszą granulacją.
            </p>
          </div>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-end gap-3">
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">From</span>
          <input type="date" value={from} onChange={(e) => { setFrom(e.target.value); setPage(1); }}
            className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm" />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">To</span>
          <input type="date" value={to} onChange={(e) => { setTo(e.target.value); setPage(1); }}
            className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm" />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Marketplace</span>
          <div className="relative">
            <select value={marketplace} onChange={(e) => { setMarketplace(e.target.value); setPage(1); }}
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
          <input type="text" value={sku} onChange={(e) => { setSku(e.target.value); setPage(1); }}
            placeholder="Filter by SKU…" className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm w-44" />
        </label>
      </div>

      {isError && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">Failed to load products. Please try again later.</p>
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
                    "px-3 py-2",
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
                  <td colSpan={COLUMNS.length} className="px-3 py-3"><div className="h-4 bg-muted/30 rounded animate-pulse" /></td>
                </tr>
              ))
            ) : items.length === 0 ? (
              <tr><td colSpan={COLUMNS.length} className="px-3 py-8 text-center text-muted-foreground">No products found</td></tr>
            ) : (
              items.map((p, i) => (
                <tr key={`${p.sku}-${p.marketplace_id}-${i}`} className="border-b border-border/50 hover:bg-muted/20">
                  <td className="px-3 py-2 font-mono text-xs truncate max-w-[160px]" title={p.sku}>{p.sku}</td>
                  <td className="px-3 py-2 font-mono text-xs">{p.asin || "—"}</td>
                  <td className="px-3 py-2">{p.marketplace_code || "—"}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{p.units.toLocaleString()}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{p.orders.toLocaleString()}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{formatPLN(p.revenue_pln)}</td>
                  <td className="px-3 py-2 text-right tabular-nums text-red-400">{formatPLN(p.cogs_pln)}</td>
                  <td className="px-3 py-2 text-right tabular-nums text-red-400">{formatPLN(p.amazon_fees_pln)}</td>
                  <td className="px-3 py-2 text-right tabular-nums text-red-400">{formatPLN(p.logistics_pln)}</td>
                  <td className="px-3 py-2 text-right tabular-nums text-red-400">{formatPLN(p.ad_spend_pln)}</td>
                  <td className="px-3 py-2 text-right tabular-nums text-red-400">{formatPLN(p.refund_pln)}</td>
                  <td className={cn("px-3 py-2 text-right tabular-nums font-medium",
                    p.cm1_pln >= 0 ? "text-blue-500" : "text-destructive")}>{formatPLN(p.cm1_pln)}</td>
                  <td className={cn("px-3 py-2 text-right tabular-nums font-medium",
                    p.cm2_pln >= 0 ? "text-violet-500" : "text-destructive")}>{formatPLN(p.cm2_pln)}</td>
                  <td className={cn("px-3 py-2 text-right tabular-nums font-medium",
                    p.profit_pln >= 0 ? "text-green-500" : "text-destructive")}>{formatPLN(p.profit_pln)}</td>
                  <td className={cn("px-3 py-2 text-right tabular-nums",
                    (p.margin_pct ?? 0) >= 0 ? "text-green-500" : "text-destructive")}>
                    {p.margin_pct != null ? formatPct(p.margin_pct) : "—"}
                  </td>
                  <td className={cn("px-3 py-2 text-right tabular-nums",
                    (p.acos_pct ?? 0) > 30 ? "text-destructive" : "text-foreground")}>
                    {p.acos_pct != null ? formatPct(p.acos_pct) : "—"}
                  </td>
                  <td className={cn("px-3 py-2 text-right tabular-nums",
                    (p.return_rate_pct ?? 0) > 5 ? "text-destructive" : "text-foreground")}>
                    {p.return_rate_pct != null ? formatPct(p.return_rate_pct) : "—"}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {data && data.pages > 1 && (
        <ServerPagination page={page} pages={data.pages} total={data.total} pageSize={pageSize} onPageChange={setPage} />
      )}
    </div>
  );
}
