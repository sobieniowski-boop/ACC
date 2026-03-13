import { useQuery } from "@tanstack/react-query";
import { Globe2, ArrowRight, CheckCircle, AlertCircle, XCircle, Package } from "lucide-react";
import { getMarketExpansion } from "@/lib/api";
import type { MarketExpansionItem } from "@/lib/api";
import { formatPLN, cn } from "@/lib/utils";

const READINESS_COLORS: Record<string, string> = {
  launch_ready: "bg-green-500/15 text-green-400",
  needs_content: "bg-yellow-500/15 text-yellow-400",
  needs_family_fix: "bg-orange-500/15 text-orange-400",
  needs_inventory: "bg-blue-500/15 text-blue-400",
  not_viable: "bg-red-500/15 text-red-400",
};

const READINESS_ICONS: Record<string, React.ReactNode> = {
  launch_ready: <CheckCircle className="h-3.5 w-3.5 text-green-400" />,
  needs_content: <AlertCircle className="h-3.5 w-3.5 text-yellow-400" />,
  needs_family_fix: <AlertCircle className="h-3.5 w-3.5 text-orange-400" />,
  needs_inventory: <Package className="h-3.5 w-3.5 text-blue-400" />,
  not_viable: <XCircle className="h-3.5 w-3.5 text-red-400" />,
};

const MKT_FLAGS: Record<string, string> = {
  A1PA6795UKMFR9: "🇩🇪 DE",
  A13V1IB3VIYZZH: "🇫🇷 FR",
  APJ6JRA9NG5V4:  "🇮🇹 IT",
  A1RKKUPIHCS9HS: "🇪🇸 ES",
  A1C3SOZRARQ6R3: "🇵🇱 PL",
  A28R8C7NBKEWEA: "🇮🇪 IE",
  A1805IZSGTT6HS: "🇳🇱 NL",
  A2NODRKZP88ZB9: "🇸🇪 SE",
  AMEN7PMS3EDWL:  "🇧🇪 BE",
};

export default function StrategyMarketExpansionPage() {
  const { data, isLoading } = useQuery({
    queryKey: ["strategy-market-expansion"],
    queryFn: () => getMarketExpansion(),
    staleTime: 60_000,
  });

  const items: MarketExpansionItem[] = data?.items ?? [];

  const readyCnt = items.filter((i) => i.readiness_label === "launch_ready").length;
  const totalUplift = items.reduce((s, i) => s + (i.estimated_revenue_uplift ?? 0), 0);
  const totalProfit = items.reduce((s, i) => s + (i.estimated_profit_uplift ?? 0), 0);

  return (
    <div className="space-y-5 p-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Marketplace Expansion</h1>
        <p className="text-sm text-muted-foreground mt-0.5">Silne SKU z DE, gotowe do launchu na nowych rynkach</p>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-4 gap-4">
        <KPI label="Expansion Opportunities" value={items.length} />
        <KPI label="Launch Ready" value={readyCnt} color="text-green-400" />
        <KPI label="Revenue Upside" value={formatPLN(totalUplift)} />
        <KPI label="Profit Upside" value={formatPLN(totalProfit)} color="text-green-400" />
      </div>

      {/* Table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              <th className="px-3 py-2 text-left">SKU</th>
              <th className="px-3 py-2 text-left">Parent ASIN</th>
              <th className="px-3 py-2 text-left">Family</th>
              <th className="px-3 py-2 text-center">Expansion</th>
              <th className="px-3 py-2 text-right">Source Rev (30d)</th>
              <th className="px-3 py-2 text-right">Est. Revenue ↑</th>
              <th className="px-3 py-2 text-right">Est. Profit ↑</th>
              <th className="px-3 py-2 text-right">Confidence</th>
              <th className="px-3 py-2 text-left">Readiness</th>
              <th className="px-3 py-2 text-left">Missing</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 8 }).map((_, i) => (
                <tr key={i} className="border-b border-border/50"><td colSpan={10} className="px-3 py-3"><div className="h-4 bg-muted/30 rounded animate-pulse" /></td></tr>
              ))
            ) : items.length === 0 ? (
              <tr><td colSpan={10} className="px-3 py-8 text-center text-muted-foreground">No expansion opportunities detected</td></tr>
            ) : items.map((item) => (
              <tr key={`${item.sku ?? item.parent_asin}-${item.source_marketplace}-${item.target_marketplace}`} className="border-b border-border/50 hover:bg-muted/20">
                <td className="px-3 py-2 font-mono text-xs">{item.sku || "—"}</td>
                <td className="px-3 py-2 text-xs">{item.parent_asin || "—"}</td>
                <td className="px-3 py-2 text-xs font-mono">{item.family_id || "—"}</td>
                <td className="px-3 py-2">
                  <div className="flex items-center justify-center gap-1 text-xs">
                    <span>{MKT_FLAGS[item.source_marketplace] || item.source_marketplace}</span>
                    <ArrowRight className="h-3 w-3 text-muted-foreground" />
                    <span>{MKT_FLAGS[item.target_marketplace] || item.target_marketplace}</span>
                  </div>
                </td>
                <td className="px-3 py-2 text-right text-xs tabular-nums">
                  {item.source_revenue != null ? formatPLN(item.source_revenue) : "—"}
                </td>
                <td className="px-3 py-2 text-right text-xs tabular-nums text-green-500 font-medium">
                  {item.estimated_revenue_uplift != null ? formatPLN(item.estimated_revenue_uplift) : "—"}
                </td>
                <td className="px-3 py-2 text-right text-xs tabular-nums text-green-500">
                  {item.estimated_profit_uplift != null ? formatPLN(item.estimated_profit_uplift) : "—"}
                </td>
                <td className="px-3 py-2 text-right text-xs tabular-nums">{item.confidence?.toFixed(0) ?? "—"}%</td>
                <td className="px-3 py-2">
                  <span className={cn("inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-bold",
                    READINESS_COLORS[item.readiness_label || ""] || "bg-muted")}>
                    {READINESS_ICONS[item.readiness_label || ""]}
                    {(item.readiness_label || "—").replace(/_/g, " ")}
                  </span>
                </td>
                <td className="px-3 py-2 text-xs text-muted-foreground max-w-[160px] truncate" title={item.missing_components?.join(", ") || ""}>
                  {item.missing_components?.join(", ") || "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function KPI({ label, value, color }: { label: string; value: string | number; color?: string }) {
  return (
    <div className="rounded-xl border border-border bg-card p-4">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className={cn("text-2xl font-bold mt-1 tabular-nums", color)}>{value}</p>
    </div>
  );
}
