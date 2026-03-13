import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, subDays } from "date-fns";
import {
  ChevronDown,
  AlertTriangle,
  Undo2,
  Package,
  DollarSign,
  ShieldAlert,
  CheckCircle2,
  Clock,
  XCircle,
} from "lucide-react";
import { getReturnsDashboard, getReturnsItems } from "@/lib/api";
import type { ReturnItem, ReturnsDashboard } from "@/lib/api";
import { formatPLN, cn } from "@/lib/utils";
import { ServerPagination } from "@/components/shared";

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

const STATUS_OPTIONS = [
  { id: "", label: "All statuses" },
  { id: "sellable_return", label: "Sellable" },
  { id: "damaged_return", label: "Damaged" },
  { id: "pending", label: "Pending" },
  { id: "lost_in_transit", label: "Lost" },
  { id: "reimbursed", label: "Reimbursed" },
];

const SORT_OPTIONS = [
  { id: "refund_date", label: "Refund date" },
  { id: "return_date", label: "Return date" },
  { id: "sku", label: "SKU" },
  { id: "cogs_pln", label: "COGS" },
  { id: "financial_status", label: "Status" },
  { id: "quantity", label: "Quantity" },
  { id: "marketplace_id", label: "Marketplace" },
];

function statusBadge(status: string | null) {
  if (!status) return <span className="text-muted-foreground">—</span>;
  const map: Record<string, { label: string; cls: string; icon: typeof CheckCircle2 }> = {
    sellable_return: { label: "Sellable", cls: "text-green-400 bg-green-400/10", icon: CheckCircle2 },
    damaged_return: { label: "Damaged", cls: "text-red-400 bg-red-400/10", icon: XCircle },
    pending: { label: "Pending", cls: "text-amber-400 bg-amber-400/10", icon: Clock },
    lost_in_transit: { label: "Lost", cls: "text-orange-400 bg-orange-400/10", icon: ShieldAlert },
    reimbursed: { label: "Reimbursed", cls: "text-blue-400 bg-blue-400/10", icon: DollarSign },
  };
  const m = map[status] ?? { label: status, cls: "text-muted-foreground bg-muted/20", icon: Clock };
  const Icon = m.icon;
  return (
    <span className={cn("inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium", m.cls)}>
      <Icon className="h-3 w-3" />
      {m.label}
    </span>
  );
}

function KpiCard({ label, value, sub, icon: Icon, accent }: {
  label: string; value: string; sub?: string; icon: typeof Package; accent?: string;
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

export default function ReturnsTrackerPage() {
  const [page, setPage] = useState(1);
  const [from, setFrom] = useState(() => format(subDays(new Date(), 29), "yyyy-MM-dd"));
  const [to, setTo] = useState(() => format(new Date(), "yyyy-MM-dd"));
  const [marketplace, setMarketplace] = useState("");
  const [skuSearch, setSkuSearch] = useState("");
  const [status, setStatus] = useState("");
  const [sortBy, setSortBy] = useState("refund_date");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const pageSize = 50;

  // Dashboard KPIs
  const dashParams: Record<string, string> = { date_from: from, date_to: to };
  if (marketplace) dashParams.marketplace_id = marketplace;

  const { data: dash } = useQuery({
    queryKey: ["returns-dashboard", dashParams],
    queryFn: () => getReturnsDashboard(dashParams),
    staleTime: 60_000,
  });

  // Items table
  const itemParams: Record<string, string | number> = {
    date_from: from,
    date_to: to,
    page,
    page_size: pageSize,
    sort_by: sortBy,
    sort_dir: sortDir,
  };
  if (marketplace) itemParams.marketplace_id = marketplace;
  if (skuSearch) itemParams.sku_search = skuSearch;
  if (status) itemParams.financial_status = status;

  const { data, isLoading, isError } = useQuery({
    queryKey: ["returns-items", itemParams],
    queryFn: () => getReturnsItems(itemParams),
    staleTime: 30_000,
  });

  const items: ReturnItem[] = data?.items ?? [];
  const summary = dash?.summary;

  const handleSort = (col: string) => {
    if (sortBy === col) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(col);
      setSortDir("desc");
    }
    setPage(1);
  };

  const sortIndicator = (col: string) =>
    sortBy === col ? (sortDir === "asc" ? " ↑" : " ↓") : "";

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center gap-2">
        <Undo2 className="h-6 w-6 text-amber-500" />
        <h1 className="text-2xl font-bold tracking-tight">Return Tracker</h1>
      </div>

      {/* KPI Cards */}
      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-6 gap-3">
          <KpiCard label="Total items" value={summary.total_items.toLocaleString()}
            sub={`${summary.total_units} units / ${summary.total_orders} orders`} icon={Package} />
          <KpiCard label="Refunds" value={formatPLN(summary.total_refund_pln)}
            icon={DollarSign} accent="text-red-400" />
          <KpiCard label="COGS at risk" value={formatPLN(summary.total_cogs_at_risk_pln)}
            sub={`Recovered: ${formatPLN(summary.cogs_recovered_pln)}`} icon={ShieldAlert} accent="text-amber-400" />
          <KpiCard label="Write-off" value={formatPLN(summary.cogs_write_off_pln)}
            sub={`Net loss: ${formatPLN(summary.net_loss_pln)}`} icon={XCircle} accent="text-red-400" />
          <KpiCard label="Sellable" value={`${summary.sellable_count}`}
            sub={`${summary.sellable_rate_pct.toFixed(1)}% rate`} icon={CheckCircle2} accent="text-green-400" />
          <KpiCard label="Pending" value={`${summary.pending_count}`}
            sub={`Damaged: ${summary.damaged_count} / Lost: ${summary.lost_count}`} icon={Clock} accent="text-amber-400" />
        </div>
      )}

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
          <span className="text-xs text-muted-foreground">SKU / ASIN</span>
          <input type="text" value={skuSearch} onChange={(e) => { setSkuSearch(e.target.value); setPage(1); }}
            placeholder="Search…" className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm w-44" />
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Status</span>
          <div className="relative">
            <select value={status} onChange={(e) => { setStatus(e.target.value); setPage(1); }}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm focus:outline-none">
              {STATUS_OPTIONS.map((s) => (
                <option key={s.id} value={s.id}>{s.label}</option>
              ))}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          </div>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Sort by</span>
          <div className="relative">
            <select value={sortBy} onChange={(e) => { setSortBy(e.target.value); setPage(1); }}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm focus:outline-none">
              {SORT_OPTIONS.map((s) => (
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
          <p className="text-sm text-destructive">Failed to load return items. Please try again later.</p>
        </div>
      )}

      {/* Table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              <th className="px-4 py-2 text-left cursor-pointer hover:text-foreground" onClick={() => handleSort("refund_date")}>
                Refund date{sortIndicator("refund_date")}
              </th>
              <th className="px-4 py-2 text-left">Order ID</th>
              <th className="px-4 py-2 text-left cursor-pointer hover:text-foreground" onClick={() => handleSort("marketplace_id")}>
                MKT{sortIndicator("marketplace_id")}
              </th>
              <th className="px-4 py-2 text-left cursor-pointer hover:text-foreground" onClick={() => handleSort("sku")}>
                SKU{sortIndicator("sku")}
              </th>
              <th className="px-4 py-2 text-left">ASIN</th>
              <th className="px-4 py-2 text-left">Refund type</th>
              <th className="px-4 py-2 text-right">Refund PLN</th>
              <th className="px-4 py-2 text-right cursor-pointer hover:text-foreground" onClick={() => handleSort("quantity")}>
                Qty{sortIndicator("quantity")}
              </th>
              <th className="px-4 py-2 text-left cursor-pointer hover:text-foreground" onClick={() => handleSort("financial_status")}>
                Status{sortIndicator("financial_status")}
              </th>
              <th className="px-4 py-2 text-left">Disposition</th>
              <th className="px-4 py-2 text-left">Reason</th>
              <th className="px-4 py-2 text-right cursor-pointer hover:text-foreground" onClick={() => handleSort("cogs_pln")}>
                COGS{sortIndicator("cogs_pln")}
              </th>
              <th className="px-4 py-2 text-right">Recovered</th>
              <th className="px-4 py-2 text-right">Write-off</th>
              <th className="px-4 py-2 text-left cursor-pointer hover:text-foreground" onClick={() => handleSort("return_date")}>
                Return date{sortIndicator("return_date")}
              </th>
              <th className="px-4 py-2 text-left">Source</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 10 }).map((_, i) => (
                <tr key={i} className="border-b border-border/50">
                  <td colSpan={16} className="px-4 py-3">
                    <div className="h-4 bg-muted/30 rounded animate-pulse" />
                  </td>
                </tr>
              ))
            ) : items.length === 0 ? (
              <tr>
                <td colSpan={16} className="px-4 py-12 text-center text-muted-foreground">
                  <Undo2 className="mx-auto h-8 w-8 mb-2 opacity-30" />
                  No return items found for the selected filters.
                </td>
              </tr>
            ) : (
              items.map((r) => (
                <tr key={r.id} className="border-b border-border/50 hover:bg-muted/20">
                  <td className="px-4 py-2 text-xs">{r.refund_date?.slice(0, 10) ?? "—"}</td>
                  <td className="px-4 py-2 font-mono text-xs">{r.amazon_order_id}</td>
                  <td className="px-4 py-2 text-xs">{r.marketplace_code || "—"}</td>
                  <td className="px-4 py-2 font-mono text-xs truncate max-w-[140px]" title={r.sku ?? ""}>
                    {r.sku || "—"}
                  </td>
                  <td className="px-4 py-2 font-mono text-xs truncate max-w-[100px]" title={r.asin ?? ""}>
                    {r.asin || "—"}
                  </td>
                  <td className="px-4 py-2 text-xs">{r.refund_type || "—"}</td>
                  <td className="px-4 py-2 text-right tabular-nums text-red-400">
                    {formatPLN(r.refund_amount_pln)}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums">{r.quantity}</td>
                  <td className="px-4 py-2">{statusBadge(r.financial_status)}</td>
                  <td className="px-4 py-2 text-xs">{r.disposition || "—"}</td>
                  <td className="px-4 py-2 text-xs truncate max-w-[160px]" title={r.return_reason ?? ""}>
                    {r.return_reason || "—"}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums">{formatPLN(r.cogs_pln)}</td>
                  <td className="px-4 py-2 text-right tabular-nums text-green-500">
                    {r.cogs_recovered_pln ? formatPLN(r.cogs_recovered_pln) : "—"}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-red-400">
                    {r.write_off_pln ? formatPLN(r.write_off_pln) : "—"}
                  </td>
                  <td className="px-4 py-2 text-xs">{r.return_date?.slice(0, 10) ?? "—"}</td>
                  <td className="px-4 py-2 text-xs text-muted-foreground">{r.source || "—"}</td>
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

      {/* Total count */}
      {data && (
        <div className="text-xs text-muted-foreground">
          Showing {items.length} of {data.total} items
        </div>
      )}
    </div>
  );
}
