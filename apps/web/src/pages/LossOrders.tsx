import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getLossOrders, type LossOrderItem } from "@/lib/api";
import { usePageFilters, pageFiltersToApiParams } from "@/lib/usePageFilters";
import { formatPLN, formatPct, cn } from "@/lib/utils";
import { useUserPreferences } from "@/store/userPreferences";
import {
  ClientExportButton,
  ColumnChooser,
  useColumnVisibility,
  ServerPagination,
  type ColumnDef,
} from "@/components/shared";
import {
  AlertTriangle,
  TrendingDown,
  Tag,
  Percent,
  Box,
  HelpCircle,
  Gift,
  Truck,
} from "lucide-react";

function DriverBadge({ driver }: { driver: string }) {
  const map: Record<string, { icon: React.ElementType; color: string }> = {
    "Vine": { icon: Gift, color: "text-green-400 bg-green-500/10" },
    "Sell price too low": { icon: Tag, color: "text-orange-400 bg-orange-500/10" },
    "Logistics too high": { icon: Truck, color: "text-blue-400 bg-blue-500/10" },
    "Fees anomaly": { icon: Percent, color: "text-purple-400 bg-purple-500/10" },
    "Missing cost data": { icon: HelpCircle, color: "text-gray-400 bg-gray-500/10" },
    "Combined costs": { icon: Box, color: "text-red-400 bg-red-500/10" },
  };
  const cfg = map[driver] || map["Combined costs"];
  const Icon = cfg.icon;
  return (
    <span className={cn("inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium", cfg.color)}>
      <Icon className="h-3 w-3" />
      {driver}
    </span>
  );
}

export default function LossOrdersPage() {
  const [page, setPage] = useState(1);
  const pageSize = 50;
  const filters = usePageFilters();
  const { profitMode, currencyView, rowDensity } = useUserPreferences();

  const LOSS_COLUMNS: ColumnDef[] = [
    { key: "orderId", label: "Order ID" },
    { key: "date", label: "Date" },
    { key: "mkt", label: "Mkt" },
    { key: "sku", label: "SKU" },
    { key: "product", label: "Produkt" },
    { key: "qty", label: "Qty" },
    { key: "revenue", label: "Revenue" },
    { key: "cogs", label: "COGS" },
    { key: "fees", label: "Fees" },
    { key: "logistics", label: "Logistics" },
    { key: "cm1", label: "CM1" },
    { key: "cm1pct", label: "CM1 %" },
    { key: "driver", label: "Driver" },
  ];
  const colVis = useColumnVisibility(LOSS_COLUMNS);

  const params = {
    ...pageFiltersToApiParams(filters, { profitMode, currencyView }),
    page,
    page_size: pageSize,
  };

  const { data, isLoading } = useQuery({
    queryKey: ["loss-orders", params],
    queryFn: () => getLossOrders(params),
  });

  const rowPadding = rowDensity === "compact" ? "py-1.5" : "py-2";

  return (
    <div className="space-y-4">
      <div>
        <div className="flex items-center gap-2">
          <AlertTriangle className="h-6 w-6 text-red-400" />
          <h1 className="text-2xl font-bold">Loss Analysis</h1>
          <span className="rounded-full border border-border bg-muted px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            CM1 only
          </span>
          {data?.items && <ClientExportButton data={data.items} filename={`loss_orders_${filters.dateFrom}_${filters.dateTo}`} />}
        </div>
        <p className="text-sm text-muted-foreground">
          Order lines where canonical CM1 is below zero
        </p>
      </div>

      {data && (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
          <div className="rounded-lg border border-red-500/20 bg-red-500/5 p-4">
            <div className="text-[10px] uppercase tracking-wider text-red-400">Total Loss</div>
            <div className="text-2xl font-bold text-red-400">{formatPLN(data.total_loss_pln)}</div>
          </div>
          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
              Loss Lines
            </div>
            <div className="text-2xl font-bold">{data.total.toLocaleString()}</div>
          </div>
          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
              Avg Loss / Line
            </div>
            <div className="text-2xl font-bold text-red-400">
              {data.total > 0 ? formatPLN(data.total_loss_pln / data.total) : "-"}
            </div>
          </div>
        </div>
      )}

      <div className="rounded-lg border border-border bg-card px-3 py-2 text-xs text-muted-foreground">
        Filter scope: {filters.dateFrom} to {filters.dateTo} | Summary covers the full filtered result set | Confidence &gt;= {filters.confidenceMin}%
      </div>

      <div className="rounded-xl border border-border bg-card overflow-hidden">
        <div className="flex items-center gap-2 px-3 py-2 border-b border-border">
          <ColumnChooser columns={LOSS_COLUMNS} visible={colVis.visible} onChange={colVis.setVisible} />
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                {colVis.isVisible("orderId") && <th className="px-2 py-2">Order ID</th>}
                {colVis.isVisible("date") && <th className="px-2 py-2">Date</th>}
                {colVis.isVisible("mkt") && <th className="px-2 py-2">Mkt</th>}
                {colVis.isVisible("sku") && <th className="px-2 py-2">SKU</th>}
                {colVis.isVisible("product") && <th className="px-2 py-2">Produkt</th>}
                {colVis.isVisible("qty") && <th className="px-2 py-2 text-right">Qty</th>}
                {colVis.isVisible("revenue") && <th className="px-2 py-2 text-right">Revenue</th>}
                {colVis.isVisible("cogs") && <th className="px-2 py-2 text-right">COGS</th>}
                {colVis.isVisible("fees") && <th className="px-2 py-2 text-right">Fees</th>}
                {colVis.isVisible("logistics") && <th className="px-2 py-2 text-right">Logistics</th>}
                {colVis.isVisible("cm1") && <th className="px-2 py-2 text-right">CM1</th>}
                {colVis.isVisible("cm1pct") && <th className="px-2 py-2 text-right">CM1 %</th>}
                {colVis.isVisible("driver") && <th className="px-2 py-2">Driver</th>}
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {isLoading && (
                <tr>
                    <td colSpan={colVis.visible.length} className="px-2 py-12 text-center text-muted-foreground">
                    Loading...
                  </td>
                </tr>
              )}
              {!isLoading && data?.items.length === 0 && (
                <tr>
                  <td colSpan={colVis.visible.length} className="px-2 py-12 text-center text-green-400">
                    No loss orders found
                  </td>
                </tr>
              )}
              {data?.items.map((item: LossOrderItem, idx: number) => (
                <tr key={`${item.amazon_order_id}-${idx}`} className="hover:bg-red-500/5 transition-colors">
                  {colVis.isVisible("orderId") && <td className={cn("px-2 font-mono text-[11px] whitespace-nowrap", rowPadding)}>
                    {item.amazon_order_id}
                  </td>}
                  {colVis.isVisible("date") && <td className={cn("px-2 whitespace-nowrap text-muted-foreground", rowPadding)}>
                    {item.purchase_date?.slice(0, 10)}
                  </td>}
                  {colVis.isVisible("mkt") && <td className={cn("px-2", rowPadding)}>
                    <span className="rounded bg-muted px-1 py-0.5 text-[10px] font-semibold">
                      {item.marketplace_code}
                    </span>
                  </td>}
                  {colVis.isVisible("sku") && <td className={cn("px-2 font-mono text-[10px] truncate", rowPadding)} title={item.sku || ""}>
                    {item.sku || "-"}
                  </td>}
                  {colVis.isVisible("product") && <td className={cn("px-2 truncate text-muted-foreground", rowPadding)} title={item.product_title || item.title || ""}>
                    {item.product_title || item.title || "-"}
                  </td>}
                  {colVis.isVisible("qty") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{item.qty}</td>}
                  {colVis.isVisible("revenue") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{formatPLN(item.revenue_pln)}</td>}
                  {colVis.isVisible("cogs") && <td className={cn("px-2 text-right tabular-nums text-muted-foreground", rowPadding)}>
                    {formatPLN(item.cogs_pln)}
                  </td>}
                  {colVis.isVisible("fees") && <td className={cn("px-2 text-right tabular-nums text-muted-foreground", rowPadding)}>
                    {formatPLN(item.amazon_fees_pln)}
                  </td>}
                  {colVis.isVisible("logistics") && <td className={cn("px-2 text-right tabular-nums text-muted-foreground", rowPadding)}>
                    {formatPLN(item.logistics_pln)}
                  </td>}
                  {colVis.isVisible("cm1") && <td className={cn("px-2 text-right tabular-nums font-medium", rowPadding)}>
                    <span className="inline-flex items-center gap-0.5 text-red-400">
                      <TrendingDown className="h-3 w-3" />
                      {formatPLN(item.cm1_profit)}
                    </span>
                  </td>}
                  {colVis.isVisible("cm1pct") && <td className={cn("px-2 text-right tabular-nums text-red-400", rowPadding)}>
                    {formatPct(item.cm1_percent)}
                  </td>}
                  {colVis.isVisible("driver") && <td className={cn("px-2", rowPadding)}>
                    <DriverBadge driver={item.primary_loss_driver} />
                  </td>}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {data && data.pages > 1 && (
          <ServerPagination page={data.page} pages={data.pages} total={data.total} pageSize={pageSize} onPageChange={setPage} />
        )}
      </div>
    </div>
  );
}
