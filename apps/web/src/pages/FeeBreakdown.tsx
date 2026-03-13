import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams, useNavigate } from "react-router-dom";
import { format, subDays } from "date-fns";
import {
  getFeeBreakdown,
  type FeeBreakdownLine,
} from "@/lib/api";
import { formatPLN, cn } from "@/lib/utils";
import { ArrowLeft, TrendingUp, TrendingDown, Info } from "lucide-react";
import { ClientExportButton } from "@/components/shared";

/* ── Section styling ── */
const SECTION_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  revenue:  { bg: "bg-green-500/5",  text: "text-green-400",  border: "border-green-500/20" },
  cm1_cost: { bg: "bg-blue-500/5",   text: "text-blue-400",   border: "border-blue-500/20" },
  cm2_cost: { bg: "bg-purple-500/5",  text: "text-purple-400", border: "border-purple-500/20" },
  np_cost:  { bg: "bg-amber-500/5",   text: "text-amber-400",  border: "border-amber-500/20" },
};

const CATEGORY_DOTS: Record<string, string> = {
  REVENUE: "bg-green-400",
  COGS: "bg-orange-400",
  FBA_FEE: "bg-blue-400",
  REFERRAL_FEE: "bg-purple-400",
  FBA_STORAGE: "bg-amber-400",
  FBA_INBOUND: "bg-teal-400",
  FBA_REMOVAL: "bg-red-400",
  FBA_LIQUIDATION: "bg-pink-400",
  WAREHOUSE_LOSS: "bg-rose-400",
  REFUND: "bg-yellow-400",
  SHIPPING_SURCHARGE: "bg-cyan-400",
  PROMO_FEE: "bg-indigo-400",
  ADS_FEE: "bg-violet-400",
  ADJUSTMENT: "bg-slate-400",
  SERVICE_FEE: "bg-gray-400",
  REGULATORY_FEE: "bg-lime-400",
  OTHER_FEE: "bg-zinc-400",
};

function WaterfallBar({ value, max }: { value: number; max: number }) {
  if (!max) return null;
  const pct = Math.min(Math.abs(value / max) * 100, 100);
  const isPositive = value >= 0;
  return (
    <div className="w-20 h-2 rounded-full bg-muted/30 overflow-hidden">
      <div
        className={cn("h-full rounded-full transition-all", isPositive ? "bg-green-500/60" : "bg-red-500/40")}
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}

export default function FeeBreakdownPage() {
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const skuParam = params.get("sku") || "";
  const marketplaceId = params.get("marketplace_id") || "";
  const daysParam = parseInt(params.get("days") || "30", 10);

  const [days, setDays] = useState(daysParam);

  const dateFrom = format(subDays(new Date(), days), "yyyy-MM-dd");
  const dateTo = format(new Date(), "yyyy-MM-dd");

  const { data, isLoading } = useQuery({
    queryKey: ["fee-breakdown", skuParam, marketplaceId, days],
    queryFn: () =>
      getFeeBreakdown({
        date_from: dateFrom,
        date_to: dateTo,
        ...(marketplaceId ? { marketplace_id: marketplaceId } : {}),
        ...(skuParam ? { sku: skuParam } : {}),
      }),
  });

  const s = data?.summary;
  const revenue = s?.revenue_pln || 0;

  const marginPct = (val: number) => (revenue ? `${((val / revenue) * 100).toFixed(1)}%` : "—");

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center gap-3 flex-wrap">
        <button
          onClick={() => navigate(-1)}
          className="rounded-md border border-border p-1.5 text-muted-foreground hover:bg-muted transition-colors"
        >
          <ArrowLeft className="h-4 w-4" />
        </button>
        <div className="flex-1 min-w-0">
          <h1 className="text-2xl font-bold">
            Rachunek zysków i strat
            {skuParam && <span className="font-mono text-lg text-muted-foreground ml-2">{skuParam}</span>}
          </h1>
          <p className="text-sm text-muted-foreground">
            {dateFrom} — {dateTo}
            {marketplaceId && ` · ${marketplaceId}`}
            {data && ` · ${data.lines.filter((l) => l.line_type !== "section_header" && l.line_type !== "subtotal").length} pozycji`}
          </p>
        </div>
        {data?.lines && <ClientExportButton data={data.lines.filter((l) => l.line_type !== "section_header")} filename={`pl_${days}d`} />}
      </div>

      {/* Period selector */}
      <div className="flex gap-1 rounded-lg border border-border bg-card p-1 w-fit">
        {[7, 30, 90, 365].map((d) => (
          <button
            key={d}
            onClick={() => setDays(d)}
            className={cn(
              "rounded-md px-3 py-1 text-xs font-medium transition-colors",
              days === d ? "bg-amazon text-black" : "text-muted-foreground hover:text-foreground"
            )}
          >
            {d === 365 ? "1Y" : `${d}d`}
          </button>
        ))}
      </div>

      {/* KPI Waterfall */}
      {s && (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
          {[
            { label: "Przychód", value: s.revenue_pln, sub: `${s.units.toLocaleString()} szt.`, icon: TrendingUp, color: "text-green-400" },
            { label: "CM1", value: s.cm1_pln, sub: `marża ${marginPct(s.cm1_pln)}`, icon: null, color: s.cm1_pln >= 0 ? "text-green-400" : "text-red-400" },
            { label: "CM2", value: s.cm2_pln, sub: `marża ${marginPct(s.cm2_pln)}`, icon: null, color: s.cm2_pln >= 0 ? "text-green-400" : "text-red-400" },
            { label: "Zysk netto", value: s.np_pln, sub: `marża ${marginPct(s.np_pln)}`, icon: s.np_pln >= 0 ? TrendingUp : TrendingDown, color: s.np_pln >= 0 ? "text-green-400" : "text-red-400" },
            { label: "Koszty łącznie", value: s.revenue_pln - s.np_pln, sub: `${marginPct(s.revenue_pln - s.np_pln)} przychodu`, icon: null, color: "text-red-400" },
          ].map(({ label, value, sub, icon: Icon, color }) => (
            <div key={label} className="rounded-lg border border-border bg-card p-3">
              <div className="flex items-center gap-1.5">
                {Icon && <Icon className={cn("h-3.5 w-3.5", color)} />}
                <span className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</span>
              </div>
              <div className={cn("text-lg font-bold tabular-nums", color)}>
                {formatPLN(value)}
              </div>
              <div className="text-[10px] text-muted-foreground">{sub}</div>
            </div>
          ))}
        </div>
      )}

      {/* Data source info */}
      <div className="flex items-start gap-2 rounded-lg border border-border bg-card/50 p-3 text-[11px] text-muted-foreground">
        <Info className="h-3.5 w-3.5 mt-0.5 shrink-0" />
        <span>
          Dane P&L pochodzą z dwóch źródeł:{" "}
          <strong className="text-foreground">zamówienia</strong> (przychód, COGS, FBA, prowizja — z acc_order_line) oraz{" "}
          <strong className="text-foreground">finanse Amazon</strong> (magazynowanie, zwroty, korekty, EPR — z acc_finance_transaction).
          Koszty pokazane są ze znakiem minus (−). Procenty odnoszą się do łącznego przychodu.
        </span>
      </div>

      {/* P&L Waterfall Table */}
      <div className="rounded-xl border border-border bg-card overflow-hidden">
        {isLoading && (
          <div className="px-4 py-16 text-center text-muted-foreground">Ładowanie rachunku P&L…</div>
        )}
        {!isLoading && (!data?.lines || data.lines.length === 0) && (
          <div className="px-4 py-16 text-center text-muted-foreground">Brak danych za wybrany okres</div>
        )}
        {data?.lines && data.lines.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b border-border bg-muted/30 text-left text-[10px] uppercase tracking-wider text-muted-foreground">
                <tr>
                  <th className="px-4 py-2.5 w-[50%]">Pozycja</th>
                  <th className="px-3 py-2.5 text-right w-[18%]">Kwota PLN</th>
                  <th className="px-3 py-2.5 text-right w-[10%]">% przychodu</th>
                  <th className="px-3 py-2.5 w-[12%]">Udział</th>
                  <th className="px-3 py-2.5 text-center w-[5%]">Źródło</th>
                  <th className="px-3 py-2.5 text-right w-[5%]">Txn</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border/50">
                {data.lines.map((line: FeeBreakdownLine, idx: number) => {
                  const isSection = line.line_type === "section_header";
                  const isSubtotal = line.line_type === "subtotal";
                  const isRevenue = line.line_type === "revenue";
                  const isCost = line.line_type === "cost";

                  // Section header row
                  if (isSection) {
                    const sectionName = line.description.includes("CM1") ? "cm1_cost"
                      : line.description.includes("CM2") ? "cm2_cost"
                      : line.description.includes("NP") ? "np_cost"
                      : "revenue";
                    const sc = SECTION_COLORS[sectionName];
                    return (
                      <tr key={`sec-${idx}`} className={cn(sc.bg, "border-t border-border")}>
                        <td colSpan={6} className={cn("px-4 py-2 text-[11px] font-bold uppercase tracking-wider", sc.text)}>
                          {line.description}
                        </td>
                      </tr>
                    );
                  }

                  // Subtotal row
                  if (isSubtotal) {
                    const isPositive = line.amount_pln >= 0;
                    return (
                      <tr key={`sub-${idx}`} className="bg-muted/30 border-t-2 border-border">
                        <td className="px-4 py-2.5">
                          <span className="font-bold text-sm">
                            = {line.description}
                            {line.charge_type && <span className="text-muted-foreground font-normal ml-1.5 text-xs">({line.charge_type})</span>}
                          </span>
                        </td>
                        <td className={cn("px-3 py-2.5 text-right font-bold text-base tabular-nums", isPositive ? "text-green-400" : "text-red-400")}>
                          {formatPLN(line.amount_pln)}
                        </td>
                        <td className={cn("px-3 py-2.5 text-right font-bold tabular-nums text-xs", isPositive ? "text-green-400" : "text-red-400")}>
                          {line.pct_of_revenue !== 0 ? `${line.pct_of_revenue}%` : ""}
                        </td>
                        <td className="px-3 py-2.5">
                          <WaterfallBar value={line.amount_pln} max={revenue} />
                        </td>
                        <td />
                        <td />
                      </tr>
                    );
                  }

                  // Regular line (revenue or cost)
                  const dotColor = CATEGORY_DOTS[line.category] || "bg-gray-400";
                  return (
                    <tr key={`${line.charge_type}-${idx}`} className="hover:bg-muted/10 transition-colors">
                      <td className="px-4 py-1.5">
                        <div className="flex items-center gap-2">
                          <span className={cn("h-2 w-2 rounded-full shrink-0", dotColor)} />
                          <div className="min-w-0">
                            <div className="text-xs font-medium truncate" title={line.description}>
                              {line.description}
                            </div>
                            <div className="text-[10px] text-muted-foreground font-mono">
                              {line.charge_type}
                            </div>
                          </div>
                        </div>
                      </td>
                      <td className={cn(
                        "px-3 py-1.5 text-right tabular-nums text-xs font-medium",
                        isRevenue ? "text-green-400" : isCost ? "text-red-400" : ""
                      )}>
                        {formatPLN(line.amount_pln)}
                      </td>
                      <td className="px-3 py-1.5 text-right tabular-nums text-[11px] text-muted-foreground">
                        {line.pct_of_revenue !== 0 ? `${line.pct_of_revenue}%` : "—"}
                      </td>
                      <td className="px-3 py-1.5">
                        <WaterfallBar value={line.amount_pln} max={revenue} />
                      </td>
                      <td className="px-3 py-1.5 text-center">
                        {line.source === "orders" && (
                          <span className="text-[9px] rounded bg-blue-500/10 text-blue-400 px-1.5 py-0.5" title="Dane z zamówień (acc_order_line)">ZAM</span>
                        )}
                        {line.source === "finance" && (
                          <span className="text-[9px] rounded bg-amber-500/10 text-amber-400 px-1.5 py-0.5" title="Dane z raportów finansowych Amazon (acc_finance_transaction)">FIN</span>
                        )}
                      </td>
                      <td className="px-3 py-1.5 text-right tabular-nums text-[11px] text-muted-foreground">
                        {line.txn_count > 0 ? line.txn_count.toLocaleString() : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
