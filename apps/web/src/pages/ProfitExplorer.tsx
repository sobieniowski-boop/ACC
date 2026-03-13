import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, subDays, startOfDay } from "date-fns";
import { getProfitOrders, exportProfitCSV } from "@/lib/api";
import type { ProfitOrder, OrderLine } from "@/lib/api";
import { formatPLN, formatPct, cn } from "@/lib/utils";
import { Download, ChevronRight, ChevronDown, Package, Truck, Warehouse, AlertTriangle, Info } from "lucide-react";
import {
  ColumnChooser,
  useColumnVisibility,
  ServerPagination,
  type ColumnDef,
} from "@/components/shared";

type DateMode = "today" | "yesterday" | "7d" | "30d" | "90d" | "custom";
type FulfillmentFilter = "" | "AFN" | "MFN";

function dateRange(mode: DateMode, customFrom?: string, customTo?: string) {
  const today = startOfDay(new Date());
  switch (mode) {
    case "today":
      return { from: format(today, "yyyy-MM-dd"), to: format(today, "yyyy-MM-dd") };
    case "yesterday": {
      const y = format(subDays(today, 1), "yyyy-MM-dd");
      return { from: y, to: y };
    }
    case "7d":
      return { from: format(subDays(today, 6), "yyyy-MM-dd"), to: format(today, "yyyy-MM-dd") };
    case "30d":
      return { from: format(subDays(today, 29), "yyyy-MM-dd"), to: format(today, "yyyy-MM-dd") };
    case "90d":
      return { from: format(subDays(today, 89), "yyyy-MM-dd"), to: format(today, "yyyy-MM-dd") };
    case "custom":
      return {
        from: customFrom || format(subDays(today, 29), "yyyy-MM-dd"),
        to: customTo || format(today, "yyyy-MM-dd"),
      };
  }
}

const DATE_PRESETS: { key: DateMode; label: string }[] = [
  { key: "today", label: "Dziś" },
  { key: "yesterday", label: "Wczoraj" },
  { key: "7d", label: "7d" },
  { key: "30d", label: "30d" },
  { key: "90d", label: "90d" },
  { key: "custom", label: "Custom" },
];

const MARKETPLACE_OPTIONS: { id: string; code: string; flag: string }[] = [
  { id: "A1PA6795UKMFR9", code: "DE", flag: "🇩🇪" },
  { id: "A13V1IB3VIYZZH", code: "FR", flag: "🇫🇷" },
  { id: "APJ6JRA9NG5V4",  code: "IT", flag: "🇮🇹" },
  { id: "A1RKKUPIHCS9HS", code: "ES", flag: "🇪🇸" },
  { id: "A1C3SOZRARQ6R3", code: "PL", flag: "🇵🇱" },
  { id: "A1805IZSGTT6HS", code: "NL", flag: "🇳🇱" },
  { id: "A2NODRKZP88ZB9", code: "SE", flag: "🇸🇪" },
  { id: "AMEN7PMS3EDWL",  code: "BE", flag: "🇧🇪" },
  { id: "A28R8C7NBKEWEA", code: "IE", flag: "🇮🇪" },
];

const FULFILLMENT_OPTIONS: { key: FulfillmentFilter; label: string; icon?: React.ElementType }[] = [
  { key: "", label: "Razem" },
  { key: "AFN", label: "FBA", icon: Warehouse },
  { key: "MFN", label: "FBM", icon: Truck },
];

function OrderLineCard({ line, currency }: { line: OrderLine; currency: string }) {
  const displayTitle = line.title_pl || line.title || "—";
  const hasCogs = line.cogs_pln != null && line.cogs_pln > 0;
  const hasUnitCost = line.purchase_price_pln != null && line.purchase_price_pln > 0;
  const lineFees = (line.fba_fee_pln ?? 0) + (line.referral_fee_pln ?? 0);
  const hasFees = lineFees > 0;
  return (
    <div className="flex items-center gap-4 px-3 py-2 rounded-lg hover:bg-muted/20 transition-colors group">
      <Package className="h-4 w-4 text-muted-foreground/30 shrink-0 group-hover:text-muted-foreground/60 transition-colors" />
      {/* Product info */}
      <div className="flex-1 min-w-0">
        <div className="text-sm text-foreground truncate leading-snug" title={displayTitle}>
          {displayTitle}
        </div>
        <div className="flex items-center gap-2 text-[11px] text-muted-foreground/60 mt-0.5">
          {line.sku && <span className="font-mono">{line.sku}</span>}
          {line.sku && line.asin && <span className="text-muted-foreground/25">·</span>}
          {line.asin && <span className="font-mono">{line.asin}</span>}
        </div>
      </div>
      {/* Numbers */}
      <div className="flex items-center gap-5 text-xs tabular-nums shrink-0">
        <div className="text-right w-10">
          <div className="text-[10px] text-muted-foreground/40 uppercase">Ilość</div>
          <div className="text-foreground">{line.quantity}×</div>
        </div>
        <div className="text-right w-20">
          <div className="text-[10px] text-muted-foreground/40 uppercase">Cena</div>
          <div className="text-foreground">
            {line.item_price != null ? `${line.item_price.toFixed(2)} ${currency}` : "—"}
          </div>
        </div>
        <div className="text-right w-20">
          <div className="text-[10px] text-muted-foreground/40 uppercase">Koszt/szt.</div>
          <div className={hasUnitCost ? "text-foreground" : "text-muted-foreground/30"}>
            {hasUnitCost ? formatPLN(line.purchase_price_pln!) : "—"}
          </div>
        </div>
        <div className="text-right w-20">
          <div className="text-[10px] text-muted-foreground/40 uppercase">COGS</div>
          <div className={hasCogs ? "text-foreground font-medium" : "text-muted-foreground/30"}>
            {hasCogs ? formatPLN(line.cogs_pln!) : "brak"}
          </div>
        </div>
        <div className="text-right w-20">
          <div className="text-[10px] text-muted-foreground/40 uppercase">Fees</div>
          <div className={hasFees ? "text-foreground" : "text-muted-foreground/30"}>
            {hasFees ? formatPLN(lineFees) : "—"}
          </div>
        </div>
      </div>
    </div>
  );
}

export default function ProfitExplorerPage() {
  const [dateMode, setDateMode] = useState<DateMode>("30d");
  const [customFrom, setCustomFrom] = useState("");
  const [customTo, setCustomTo] = useState("");
  const [page, setPage] = useState(1);
  const [marketplace, setMarketplace] = useState("");
  const [sku, setSku] = useState("");
  const [fulfillment, setFulfillment] = useState<FulfillmentFilter>("");
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const PE_COLUMNS: ColumnDef[] = [
    { key: "expand", label: "" },
    { key: "orderId", label: "Order ID" },
    { key: "date", label: "Date" },
    { key: "mkt", label: "Mkt" },
    { key: "fulf", label: "Fulf." },
    { key: "status", label: "Status" },
    { key: "revenue", label: "Revenue" },
    { key: "cogs", label: "COGS" },
    { key: "fees", label: "Fees" },
    { key: "cm", label: "CM" },
    { key: "cmPct", label: "CM %" },
  ];
  const colVis = useColumnVisibility(PE_COLUMNS);

  const { from: dateFrom, to: dateTo } = useMemo(
    () => dateRange(dateMode, customFrom, customTo),
    [dateMode, customFrom, customTo]
  );

  const params: Record<string, string | number> = {
    date_from: dateFrom,
    date_to: dateTo,
    page,
    page_size: 50,
  };
  if (marketplace) params.marketplace_id = marketplace;
  if (sku) params.sku = sku;
  if (fulfillment) params.fulfillment_channel = fulfillment;

  const { data, isLoading, isError } = useQuery({
    queryKey: ["profit-orders", dateFrom, dateTo, page, marketplace, sku, fulfillment],
    queryFn: () => getProfitOrders(params),
  });

  const toggleExpand = (id: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  return (
    <div className="space-y-4 p-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold">Profit Explorer</h1>
        <p className="text-sm text-muted-foreground">Order-level contribution margin analysis (legacy explorer metric)</p>
      </div>

      {/* Deprecation notice */}
      <div role="status" className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-300">
        <div className="flex items-start gap-2">
          <Info className="mt-0.5 h-4 w-4 shrink-0 text-amber-400" />
          <div>
            <p className="font-medium">Ten widok jest przestarzały i zostanie usunięty.</p>
            <p className="mt-0.5 text-xs text-amber-300/80">
              Przejdź do <a href="/profit/orders" className="underline hover:text-amber-200">Orders</a> lub{" "}
              <a href="/profit/products" className="underline hover:text-amber-200">Products</a> aby zobaczyć pełną analizę NP.
            </p>
          </div>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3 items-center">
        {/* Date presets */}
        <div className="flex gap-1 rounded-lg border border-border bg-card p-1">
          {DATE_PRESETS.map(({ key, label }) => (
            <button
              key={key}
              onClick={() => { setDateMode(key); setPage(1); }}
              className={cn(
                "rounded-md px-3 py-1 text-sm font-medium transition-colors",
                dateMode === key ? "bg-amazon text-black" : "text-muted-foreground hover:text-foreground"
              )}
            >
              {label}
            </button>
          ))}
        </div>

        {/* Custom date inputs */}
        {dateMode === "custom" && (
          <div className="flex items-center gap-1.5">
            <input
              type="date"
              value={customFrom}
              onChange={(e) => { setCustomFrom(e.target.value); setPage(1); }}
              className="rounded-md border border-input bg-background px-2 py-1 text-sm outline-none focus:ring-1 focus:ring-ring"
            />
            <span className="text-muted-foreground text-xs">→</span>
            <input
              type="date"
              value={customTo}
              onChange={(e) => { setCustomTo(e.target.value); setPage(1); }}
              className="rounded-md border border-input bg-background px-2 py-1 text-sm outline-none focus:ring-1 focus:ring-ring"
            />
          </div>
        )}

        {/* FBA / FBM / Razem toggle */}
        <div className="flex gap-0.5 rounded-lg border border-border bg-card p-1">
          {FULFILLMENT_OPTIONS.map(({ key, label, icon: Icon }) => (
            <button
              key={key || "all"}
              onClick={() => { setFulfillment(key); setPage(1); }}
              className={cn(
                "flex items-center gap-1 rounded-md px-2.5 py-1 text-sm font-medium transition-colors",
                fulfillment === key
                  ? "bg-amazon text-black"
                  : "text-muted-foreground hover:text-foreground"
              )}
            >
              {Icon && <Icon className="h-3.5 w-3.5" />}
              {label}
            </button>
          ))}
        </div>

        {/* Marketplace / Country dropdown */}
        <select
          value={marketplace}
          onChange={(e) => { setMarketplace(e.target.value); setPage(1); }}
          className="rounded-md border border-input bg-background px-3 py-1.5 text-sm outline-none focus:ring-1 focus:ring-ring min-w-[140px]"
        >
          <option value="">Wszystkie kraje</option>
          {MARKETPLACE_OPTIONS.map(({ id, code, flag }) => (
            <option key={id} value={id}>{flag} {code}</option>
          ))}
        </select>

        <input
          type="text"
          placeholder="Filter SKU…"
          value={sku}
          onChange={(e) => { setSku(e.target.value); setPage(1); }}
          className="rounded-md border border-input bg-background px-3 py-1.5 text-sm outline-none focus:ring-1 focus:ring-ring"
        />
        <button
          onClick={() => {
            const p: Record<string, string> = { date_from: dateFrom, date_to: dateTo };
            if (marketplace) p.marketplace_id = marketplace;
            if (sku) p.sku = sku;
            if (fulfillment) p.fulfillment_channel = fulfillment;
            exportProfitCSV(p);
          }}
          className="ml-auto flex items-center gap-1.5 rounded-lg bg-amazon px-3 py-1.5 text-sm font-medium text-black hover:bg-amazon/90 transition-colors"
        >
          <Download className="h-3.5 w-3.5" />
          Eksport CSV
        </button>
      </div>

      {isError && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">Failed to load profit data. Please try again later.</p>
        </div>
      )}

      {/* Date range label */}
      <div className="text-xs text-muted-foreground">
        {dateFrom} → {dateTo}
        {data && !isLoading && (
          <span className="ml-3 text-foreground/80">{data.total.toLocaleString("pl-PL")} zamówień</span>
        )}
        {fulfillment && (
          <span className="ml-2 rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium">
            {fulfillment === "AFN" ? "FBA" : "FBM"}
          </span>
        )}
        {marketplace && (
          <span className="ml-2 rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium">
            {MARKETPLACE_OPTIONS.find(m => m.id === marketplace)?.code ?? marketplace}
          </span>
        )}
      </div>

      {/* Table */}
      <div className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="flex items-center gap-2 px-3 py-2 border-b border-border">
          <ColumnChooser columns={PE_COLUMNS} visible={colVis.visible} onChange={colVis.setVisible} />
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                {colVis.isVisible("expand") && <th className="px-2 py-2 w-8" />}
                {colVis.isVisible("orderId") && <th className="px-2 py-2">Order ID</th>}
                {colVis.isVisible("date") && <th className="px-2 py-2">Date</th>}
                {colVis.isVisible("mkt") && <th className="px-2 py-2">Mkt</th>}
                {colVis.isVisible("fulf") && <th className="px-2 py-2">Fulf.</th>}
                {colVis.isVisible("status") && <th className="px-2 py-2">Status</th>}
                {colVis.isVisible("revenue") && <th className="px-2 py-2 text-right">Revenue</th>}
                {colVis.isVisible("cogs") && <th className="px-2 py-2 text-right">COGS</th>}
                {colVis.isVisible("fees") && <th className="px-2 py-2 text-right">Fees</th>}
                {colVis.isVisible("cm") && <th className="px-2 py-2 text-right">CM</th>}
                {colVis.isVisible("cmPct") && <th className="px-2 py-2 text-right">CM %</th>}
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {isLoading
                ? [...Array(10)].map((_, i) => (
                    <tr key={i}>
                      <td colSpan={11} className="px-2 py-1.5">
                        <div className="h-4 animate-pulse rounded bg-muted" />
                      </td>
                    </tr>
                  ))
                : data?.items.map((order: ProfitOrder) => {
                    const isExpanded = expanded.has(order.id);
                    const hasLines = order.lines && order.lines.length > 0;
                    return (
                      <>
                        <tr
                          key={order.id}
                          className={cn(
                            "hover:bg-muted/20 transition-colors",
                            hasLines && "cursor-pointer",
                            isExpanded && "bg-muted/10"
                          )}
                          onClick={() => hasLines && toggleExpand(order.id)}
                        >
                          <td className="px-2 py-1.5 text-center">
                            {hasLines ? (
                              isExpanded
                                ? <ChevronDown className="h-4 w-4 text-muted-foreground" />
                                : <ChevronRight className="h-4 w-4 text-muted-foreground/50" />
                            ) : (
                              <span className="inline-block w-4" />
                            )}
                          </td>
                          <td className="px-2 py-1.5 font-mono text-xs text-muted-foreground">
                            {order.amazon_order_id}
                          </td>
                          <td className="px-2 py-1.5 text-xs text-muted-foreground">
                            {format(new Date(order.purchase_date), "dd.MM.yy")}
                          </td>
                          <td className="px-2 py-1.5">
                            <span className="rounded bg-muted px-1.5 py-0.5 text-xs font-medium">
                              {order.marketplace_code ?? order.marketplace_id.slice(-2)}
                            </span>
                          </td>
                          <td className="px-2 py-1.5 text-xs">
                            <span className={cn(
                              "rounded px-1.5 py-0.5 text-[10px] font-medium",
                              order.fulfillment_channel === "AFN" && "bg-blue-500/10 text-blue-400",
                              order.fulfillment_channel === "MFN" && "bg-orange-500/10 text-orange-400",
                            )}>
                              {order.fulfillment_channel === "AFN" ? "FBA" : order.fulfillment_channel === "MFN" ? "FBM" : order.fulfillment_channel ?? "—"}
                            </span>
                          </td>
                          <td className="px-2 py-1.5 text-xs">
                            <span className={cn(
                              "rounded px-1.5 py-0.5",
                              order.status === "Shipped" && "bg-green-500/10 text-green-500",
                              order.status === "Unshipped" && "bg-amber-500/10 text-amber-500",
                            )}>
                              {order.status}
                            </span>
                          </td>
                          <td className="px-2 py-1.5 text-right tabular-nums">
                            {order.revenue_pln ? formatPLN(order.revenue_pln) : <span className="text-muted-foreground/50">—</span>}
                          </td>
                          <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
                            {order.cogs_pln ? formatPLN(order.cogs_pln) : <span className="text-muted-foreground/50">—</span>}
                          </td>
                          <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
                            {order.amazon_fees_pln ? formatPLN(order.amazon_fees_pln) : <span className="text-muted-foreground/50">—</span>}
                          </td>
                          <td className={cn(
                            "px-2 py-1.5 text-right font-semibold tabular-nums",
                            (order.contribution_margin_pln ?? 0) > 0 ? "text-green-500" : (order.contribution_margin_pln ?? 0) < 0 ? "text-destructive" : "text-muted-foreground/50"
                          )}>
                            {order.contribution_margin_pln ? formatPLN(order.contribution_margin_pln) : <span className="text-muted-foreground/50">—</span>}
                          </td>
                          <td className={cn(
                            "px-2 py-1.5 text-right tabular-nums",
                            (order.cm1_percent ?? 0) >= 20
                              ? "text-green-500"
                              : (order.cm1_percent ?? 0) >= 10
                              ? "text-amber-500"
                              : (order.cm1_percent ?? 0) > 0
                              ? "text-destructive"
                              : "text-muted-foreground/50"
                          )}>
                            {order.cm1_percent ? formatPct(order.cm1_percent) : <span className="text-muted-foreground/50">—</span>}
                          </td>
                        </tr>
                        {/* Expanded line items */}
                        {isExpanded && hasLines && (
                          <tr key={`${order.id}-lines`}>
                            <td colSpan={11} className="p-0">
                              <div className="bg-muted/5 border-t border-border/20 py-2 px-4">
                                {order.lines.map((line: OrderLine, idx: number) => (
                                  <OrderLineCard
                                    key={`${order.id}-${idx}`}
                                    line={line}
                                    currency={order.currency}
                                  />
                                ))}
                              </div>
                            </td>
                          </tr>
                        )}
                      </>
                    );
                  })}
              {!isLoading && data?.items.length === 0 && (
                <tr>
                  <td colSpan={11} className="px-2 py-8 text-center text-muted-foreground">
                    Brak zamówień w wybranym zakresie
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {data && data.total > 50 && (
          <ServerPagination page={page} pages={data.pages ?? Math.ceil(data.total / 50)} total={data.total} pageSize={50} onPageChange={setPage} />
        )}
      </div>
    </div>
  );
}
