import { useState, Fragment } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, subDays } from "date-fns";
import { ChevronDown, ChevronRight, AlertTriangle, Info, Package } from "lucide-react";
import { getProfitabilityOrders, getOrderDetail, getMarketplaces } from "@/lib/api";
import type { ProfitabilityOrderItem, OrderLineDetail } from "@/lib/api";
import { formatPLN, formatPct, cn } from "@/lib/utils";
import { ProfitTierBadge, ServerPagination } from "@/components/shared";

const FLAG: Record<string, string> = {
  DE: "🇩🇪", FR: "🇫🇷", IT: "🇮🇹", ES: "🇪🇸", PL: "🇵🇱",
  NL: "🇳🇱", SE: "🇸🇪", BE: "🇧🇪", IE: "🇮🇪", UK: "🇬🇧",
};

function CostCell({ v }: { v: number }) {
  if (!v) return <span className="text-muted-foreground/40">—</span>;
  return <span className="text-red-400">{formatPLN(v)}</span>;
}

function OrderLines({ orderId }: { orderId: string }) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ["order-detail", orderId],
    queryFn: () => getOrderDetail(orderId),
    staleTime: 60_000,
  });
  if (isLoading) return (
    <tr><td colSpan={11} className="px-6 py-3"><div className="h-4 w-1/2 bg-muted/30 rounded animate-pulse" /></td></tr>
  );
  if (isError || !data?.lines?.length) return (
    <tr><td colSpan={11} className="px-6 py-3 text-xs text-muted-foreground">Brak pozycji zamówienia</td></tr>
  );
  return (
    <>
      <tr className="bg-muted/10">
        <td colSpan={11} className="px-6 py-1.5">
          <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            Pozycje zamówienia ({data.lines.length})
          </span>
        </td>
      </tr>
      {data.lines.map((l: OrderLineDetail, i: number) => (
        <tr key={i} className="bg-muted/5 border-b border-border/30 text-xs">
          <td className="pl-10 pr-2 py-1.5 font-mono truncate max-w-[130px]" title={l.sku}>{l.sku}</td>
          <td className="px-2 py-1.5 truncate max-w-[200px]" title={l.title || ""}>{l.title || l.asin || "—"}</td>
          <td className="px-2 py-1.5 text-center">{l.quantity}</td>
          <td className="px-2 py-1.5 text-right tabular-nums">{formatPLN(l.item_price)}</td>
          <td className="px-2 py-1.5 text-right tabular-nums text-red-400">{formatPLN(l.referral_fee_pln)}</td>
          <td className="px-2 py-1.5 text-right tabular-nums text-red-400">{l.fba_fee_pln ? formatPLN(l.fba_fee_pln) : "—"}</td>
          <td className="px-2 py-1.5 text-right tabular-nums text-red-400">{formatPLN(l.cogs_pln)}</td>
          <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">{l.purchase_price_pln ? formatPLN(l.purchase_price_pln) : "—"}</td>
          <td className={cn("px-2 py-1.5 text-right tabular-nums font-medium",
            l.line_profit_pln >= 0 ? "text-green-500" : "text-destructive")}>{formatPLN(l.line_profit_pln)}</td>
          <td className="px-2 py-1.5 text-right text-muted-foreground text-[10px]">{l.price_source || "—"}</td>
          <td />
        </tr>
      ))}
    </>
  );
}

export default function ProfitabilityOrdersPage() {
  const [page, setPage] = useState(1);
  const [marketplace, setMarketplace] = useState("");
  const [sku, setSku] = useState("");
  const [lossOnly, setLossOnly] = useState(false);
  const [from, setFrom] = useState(() => format(subDays(new Date(), 29), "yyyy-MM-dd"));
  const [to, setTo] = useState(() => format(new Date(), "yyyy-MM-dd"));
  const [expandedOrder, setExpandedOrder] = useState<string | null>(null);
  const pageSize = 50;

  const params: Record<string, string | number> = { from, to, page, page_size: pageSize };
  if (marketplace) params.marketplace_id = marketplace;
  if (sku) params.sku = sku;
  if (lossOnly) params.loss_only = 1;

  const { data, isLoading, isError } = useQuery({
    queryKey: ["profitability-orders", params],
    queryFn: () => getProfitabilityOrders(params),
    staleTime: 30_000,
  });

  const { data: marketplaces } = useQuery({
    queryKey: ["marketplaces"],
    queryFn: getMarketplaces,
    staleTime: 300_000,
  });

  const items: ProfitabilityOrderItem[] = data?.items ?? [];

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center gap-2">
        <h1 className="text-2xl font-bold tracking-tight">Rentowność — Zamówienia</h1>
        <ProfitTierBadge tier="cm1" />
      </div>

      {/* Info banner */}
      <div className="rounded-xl border border-blue-500/20 bg-blue-500/5 p-3 flex items-start gap-3">
        <Info className="h-4 w-4 text-blue-400 shrink-0 mt-0.5" />
        <p className="text-xs text-blue-300/80 leading-relaxed">
          Koszty Amazon (opłaty, logistyka, reklamy) są przypisywane do zamówień na podstawie danych rozliczeniowych.
          Dla najnowszych zamówień wartości mogą być szacowane z dziennych rollupów.
          Kliknij wiersz, aby zobaczyć szczegóły pozycji zamówienia.
        </p>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-end gap-3">
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Od</span>
          <input type="date" value={from} onChange={(e) => { setFrom(e.target.value); setPage(1); }}
            className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm" />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Do</span>
          <input type="date" value={to} onChange={(e) => { setTo(e.target.value); setPage(1); }}
            className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm" />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Marketplace</span>
          <div className="relative">
            <select value={marketplace} onChange={(e) => { setMarketplace(e.target.value); setPage(1); }}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm focus:outline-none">
              <option value="">Wszystkie rynki</option>
              {(marketplaces ?? []).map((m) => (
                <option key={m.marketplace_id} value={m.marketplace_id}>
                  {FLAG[m.code] || ""} {m.code}
                </option>
              ))}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          </div>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">SKU</span>
          <input type="text" value={sku} onChange={(e) => { setSku(e.target.value); setPage(1); }}
            placeholder="Szukaj SKU…" className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm w-44" />
        </label>
        <label className="flex items-center gap-2 self-end py-1.5">
          <input type="checkbox" checked={lossOnly} onChange={(e) => { setLossOnly(e.target.checked); setPage(1); }}
            className="rounded border-border" />
          <span className="text-sm">Tylko stratne</span>
        </label>
      </div>

      {isError && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">Nie udało się załadować zamówień. Spróbuj ponownie później.</p>
        </div>
      )}

      {/* Table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              <th className="px-3 py-2 text-left w-8" />
              <th className="px-3 py-2 text-left">Zamówienie</th>
              <th className="px-3 py-2 text-left">Data</th>
              <th className="px-3 py-2 text-left">MKT</th>
              <th className="px-3 py-2 text-left">SKU</th>
              <th className="px-3 py-2 text-right">Przychód</th>
              <th className="px-3 py-2 text-right">Opłaty</th>
              <th className="px-3 py-2 text-right">FBA</th>
              <th className="px-3 py-2 text-right">Logistyka</th>
              <th className="px-3 py-2 text-right">COGS</th>
              <th className="px-3 py-2 text-right">Reklamy</th>
              <th className="px-3 py-2 text-right">Zwroty</th>
              <th className="px-3 py-2 text-right">CM1</th>
              <th className="px-3 py-2 text-right">CM1 %</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 10 }).map((_, i) => (
                <tr key={i} className="border-b border-border/50">
                  <td colSpan={14} className="px-4 py-3"><div className="h-4 bg-muted/30 rounded animate-pulse" /></td>
                </tr>
              ))
            ) : items.length === 0 ? (
              <tr>
                <td colSpan={14} className="px-4 py-12 text-center">
                  <Package className="h-8 w-8 mx-auto text-muted-foreground/30 mb-2" />
                  <p className="text-muted-foreground">Brak zamówień w wybranym okresie</p>
                </td>
              </tr>
            ) : (
              items.map((o, i) => {
                const isExpanded = expandedOrder === o.amazon_order_id;
                return (
                  <Fragment key={`${o.amazon_order_id}-${i}`}>
                    <tr
                      onClick={() => setExpandedOrder(isExpanded ? null : o.amazon_order_id)}
                      className={cn(
                        "border-b border-border/50 cursor-pointer transition-colors",
                        isExpanded ? "bg-muted/30" : "hover:bg-muted/20"
                      )}
                    >
                      <td className="px-3 py-2 text-muted-foreground">
                        {isExpanded
                          ? <ChevronDown className="h-3.5 w-3.5" />
                          : <ChevronRight className="h-3.5 w-3.5" />}
                      </td>
                      <td className="px-3 py-2 font-mono text-xs">{o.amazon_order_id}</td>
                      <td className="px-3 py-2 text-xs whitespace-nowrap">{o.purchase_date?.slice(0, 10)}</td>
                      <td className="px-3 py-2 text-xs">{o.marketplace_code ? `${FLAG[o.marketplace_code] || ""} ${o.marketplace_code}` : "—"}</td>
                      <td className="px-3 py-2 font-mono text-xs truncate max-w-[160px]" title={o.all_skus || o.sku || ""}>
                        {o.sku || "—"}
                        {(o.sku_count ?? 1) > 1 && (
                          <span className="ml-1 text-[10px] font-semibold text-amber-400 bg-amber-400/10 rounded px-1">
                            +{(o.sku_count ?? 1) - 1}
                          </span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums">{formatPLN(o.revenue_pln)}</td>
                      <td className="px-3 py-2 text-right tabular-nums"><CostCell v={o.amazon_fees_pln} /></td>
                      <td className="px-3 py-2 text-right tabular-nums"><CostCell v={o.fba_fees_pln} /></td>
                      <td className="px-3 py-2 text-right tabular-nums"><CostCell v={o.logistics_pln} /></td>
                      <td className="px-3 py-2 text-right tabular-nums"><CostCell v={o.cogs_pln} /></td>
                      <td className="px-3 py-2 text-right tabular-nums"><CostCell v={o.ad_cost_pln} /></td>
                      <td className="px-3 py-2 text-right tabular-nums"><CostCell v={o.refund_pln} /></td>
                      <td className={cn("px-3 py-2 text-right tabular-nums font-medium",
                        o.profit_pln >= 0 ? "text-green-500" : "text-destructive")}>{formatPLN(o.profit_pln)}</td>
                      <td className={cn("px-3 py-2 text-right tabular-nums",
                        (o.margin_pct ?? 0) >= 0 ? "text-green-500" : "text-destructive")}>
                        {o.margin_pct != null ? formatPct(o.margin_pct) : "—"}
                      </td>
                    </tr>
                    {isExpanded && <OrderLines orderId={o.amazon_order_id} />}
                  </Fragment>
                );
              })
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
