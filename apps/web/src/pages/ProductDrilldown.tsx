import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams, useNavigate } from "react-router-dom";
import { format, subDays } from "date-fns";
import {
  getProductDrilldown,
  type DrilldownItem,
} from "@/lib/api";
import { formatPLN, formatPct, cn } from "@/lib/utils";
import {
  ArrowLeft,
  TrendingUp,
  TrendingDown,
  ShieldCheck,
  Shield,
  ShieldAlert,
} from "lucide-react";
import {
  ClientExportButton,
  ServerPagination,
} from "@/components/shared";

/* --------------- Waterfall Bar --------------- */
function WaterfallBar({ item }: { item: DrilldownItem }) {
  const total = Math.max(item.revenue_pln, 1);
  const segments = [
    { label: "COGS", value: item.cogs_pln, color: "bg-orange-500" },
    { label: "FBA Fee", value: item.fba_fee_pln, color: "bg-blue-500" },
    { label: "Referral", value: item.referral_fee_pln, color: "bg-purple-500" },
    { label: "Logistics", value: item.logistics_pln, color: "bg-cyan-500" },
    {
      label: "CM1",
      value: Math.max(item.cm1_profit, 0),
      color: item.cm1_profit >= 0 ? "bg-green-500" : "bg-red-500",
    },
  ];

  return (
    <div className="flex h-2 w-full rounded-full overflow-hidden bg-muted/40">
      {segments.map((s) => {
        const w = Math.max((Math.abs(s.value) / total) * 100, 0);
        if (w < 0.5) return null;
        return (
          <div
            key={s.label}
            className={cn(s.color, "h-full")}
            style={{ width: `${Math.min(w, 100)}%` }}
            title={`${s.label}: ${formatPLN(s.value)}`}
          />
        );
      })}
    </div>
  );
}

/* --------------- Cost Source Badge --------------- */
function CostBadge({ source }: { source: string }) {
  if (source === "Actual")
    return (
      <span className="inline-flex items-center gap-0.5 rounded-full bg-green-500/10 px-2 py-0.5 text-[10px] text-green-400">
        <ShieldCheck className="h-2.5 w-2.5" /> Actual
      </span>
    );
  if (source === "Partial")
    return (
      <span className="inline-flex items-center gap-0.5 rounded-full bg-yellow-500/10 px-2 py-0.5 text-[10px] text-yellow-400">
        <Shield className="h-2.5 w-2.5" /> Partial
      </span>
    );
  return (
    <span className="inline-flex items-center gap-0.5 rounded-full bg-red-500/10 px-2 py-0.5 text-[10px] text-red-400">
      <ShieldAlert className="h-2.5 w-2.5" /> Missing
    </span>
  );
}

/* ============================================================ */
/*  MAIN PAGE                                                    */
/* ============================================================ */

export default function ProductDrilldownPage() {
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const sku = params.get("sku") || "";
  const marketplaceId = params.get("marketplace_id") || "";
  const daysParam = parseInt(params.get("days") || "30", 10);

  const [page, setPage] = useState(1);
  const [days, setDays] = useState(daysParam);
  const pageSize = 50;

  const dateFrom = format(subDays(new Date(), days), "yyyy-MM-dd");
  const dateTo = format(new Date(), "yyyy-MM-dd");

  const { data, isLoading } = useQuery({
    queryKey: ["drilldown", sku, marketplaceId, days, page],
    queryFn: () =>
      getProductDrilldown({
        sku,
        date_from: dateFrom,
        date_to: dateTo,
        page,
        page_size: pageSize,
        ...(marketplaceId ? { marketplace_id: marketplaceId } : {}),
      }),
    enabled: !!sku,
  });

  const s = data?.summary;

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center gap-3">
        <button
          onClick={() => navigate(-1)}
          className="rounded-md border border-border p-1.5 text-muted-foreground hover:bg-muted transition-colors"
        >
          <ArrowLeft className="h-4 w-4" />
        </button>
        <div>
          <h1 className="text-2xl font-bold font-mono">{sku || "Product Drilldown"}</h1>
          <p className="text-sm text-muted-foreground">
            Order-level canonical CM1 waterfall - {data?.total ?? 0} order lines
          </p>
        </div>
        {data?.items && <ClientExportButton data={data.items} filename={`drilldown_${sku}_${days}d`} />}
      </div>

      {/* Summary Cards */}
      {s && (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-7">
          {[
            { label: "Revenue", value: formatPLN(s.revenue_pln) },
            { label: "COGS", value: formatPLN(s.cogs_pln) },
            { label: "Fees", value: formatPLN(s.fees_pln) },
            { label: "Logistics", value: formatPLN(s.logistics_pln) },
            {
              label: "CM1 Profit",
              value: formatPLN(s.cm1_pln),
              color: s.cm1_pln >= 0 ? "text-green-400" : "text-red-400",
            },
            { label: "CM1 %", value: formatPct(s.cm1_pct) },
            { label: "Units", value: s.units.toLocaleString() },
          ].map(({ label, value, color }) => (
            <div key={label} className="rounded-lg border border-border bg-card p-3">
              <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
                {label}
              </div>
              <div className={cn("text-lg font-bold", color)}>{value}</div>
            </div>
          ))}
        </div>
      )}

      {/* Period */}
      <div className="flex gap-1 rounded-lg border border-border bg-card p-1 w-fit">
        {[7, 30, 90, 365].map((d) => (
          <button
            key={d}
            onClick={() => { setDays(d); setPage(1); }}
            className={cn(
              "rounded-md px-3 py-1 text-xs font-medium transition-colors",
              days === d
                ? "bg-amazon text-black"
                : "text-muted-foreground hover:text-foreground"
            )}
          >
            {d === 365 ? "1Y" : `${d}d`}
          </button>
        ))}
      </div>

      {/* Waterfall Legend */}
      <div className="flex gap-4 text-[10px] text-muted-foreground">
        <span className="flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-orange-500" /> COGS</span>
        <span className="flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-blue-500" /> FBA Fee</span>
        <span className="flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-purple-500" /> Referral</span>
        <span className="flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-cyan-500" /> Logistics</span>
        <span className="flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-green-500" /> CM1</span>
      </div>

      <div className="rounded-lg border border-border bg-card px-3 py-2 text-xs text-muted-foreground">
        Summary cards cover the full filtered result set; the table below is paginated.
      </div>

      {/* Table */}
      <div className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="border-b border-border bg-muted/30 text-left text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-3 py-2.5">Order ID</th>
                <th className="px-3 py-2.5">Date</th>
                <th className="px-3 py-2.5">Mkt</th>
                <th className="px-3 py-2.5 text-right">Qty</th>
                <th className="px-3 py-2.5 text-right">Price</th>
                <th className="px-3 py-2.5 text-right">FX</th>
                <th className="px-3 py-2.5 text-right">Revenue PLN</th>
                <th className="px-3 py-2.5 text-right">COGS</th>
                <th className="px-3 py-2.5 text-right">FBA Fee</th>
                <th className="px-3 py-2.5 text-right">Referral</th>
                <th className="px-3 py-2.5 text-right">Logistics</th>
                <th className="px-3 py-2.5 text-right">CM1</th>
                <th className="px-3 py-2.5 text-right">CM1 %</th>
                <th className="px-3 py-2.5">Refund</th>
                <th className="px-3 py-2.5 w-32">Waterfall</th>
                <th className="px-3 py-2.5">Source</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {isLoading && (
                <tr>
                  <td colSpan={16} className="px-3 py-12 text-center text-muted-foreground">
                    Loading…
                  </td>
                </tr>
              )}
              {!isLoading && data?.items.length === 0 && (
                <tr>
                  <td colSpan={16} className="px-3 py-12 text-center text-muted-foreground">
                    No order lines found
                  </td>
                </tr>
              )}
              {data?.items.map((item: DrilldownItem, idx: number) => (
                <tr
                  key={`${item.amazon_order_id}-${idx}`}
                  className={cn(
                    "hover:bg-muted/20 transition-colors",
                    item.cm1_profit < 0 && "bg-red-500/5",
                    item.is_refund && "bg-amber-500/5"
                  )}
                >
                  <td className="px-3 py-2 font-mono text-[11px] whitespace-nowrap">
                    {item.amazon_order_id}
                  </td>
                  <td className="px-3 py-2 whitespace-nowrap text-muted-foreground">
                    {item.purchase_date?.slice(0, 10)}
                  </td>
                  <td className="px-3 py-2">
                    <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] font-semibold">
                      {item.marketplace_code}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums">{item.qty}</td>
                  <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                    {item.item_price.toFixed(2)} {item.currency}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                    {item.fx_rate.toFixed(4)}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums">{formatPLN(item.revenue_pln)}</td>
                  <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                    {formatPLN(item.cogs_pln)}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                    {formatPLN(item.fba_fee_pln)}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                    {formatPLN(item.referral_fee_pln)}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                    {formatPLN(item.logistics_pln)}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums font-medium">
                    <span
                      className={cn(
                        "inline-flex items-center gap-0.5",
                        item.cm1_profit >= 0 ? "text-green-400" : "text-red-400"
                      )}
                    >
                      {item.cm1_profit >= 0 ? (
                        <TrendingUp className="h-3 w-3" />
                      ) : (
                        <TrendingDown className="h-3 w-3" />
                      )}
                      {formatPLN(item.cm1_profit)}
                    </span>
                  </td>
                  <td
                    className={cn(
                      "px-3 py-2 text-right tabular-nums",
                      item.cm1_percent >= 20
                        ? "text-green-400"
                        : item.cm1_percent >= 0
                        ? "text-yellow-400"
                        : "text-red-400"
                    )}
                  >
                    {formatPct(item.cm1_percent)}
                  </td>
                  <td className="px-3 py-2 whitespace-nowrap">
                    {item.is_refund ? (
                      <span className="inline-flex items-center gap-1">
                        <span className="rounded-full bg-amber-500/10 px-2 py-0.5 text-[10px] font-semibold text-amber-400">
                          {item.refund_type || "Refund"}
                        </span>
                        {item.refund_amount_pln != null && (
                          <span className="text-[10px] tabular-nums text-amber-400">
                            {formatPLN(item.refund_amount_pln)}
                          </span>
                        )}
                      </span>
                    ) : (
                      <span className="text-muted-foreground/30 text-[10px]">—</span>
                    )}
                  </td>
                  <td className="px-3 py-2">
                    <WaterfallBar item={item} />
                  </td>
                  <td className="px-3 py-2">
                    <CostBadge source={item.cost_source} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {data && data.pages > 1 && (
          <ServerPagination page={data.page} pages={data.pages} total={data.total} pageSize={pageSize} onPageChange={setPage} />
        )}
      </div>
    </div>
  );
}
