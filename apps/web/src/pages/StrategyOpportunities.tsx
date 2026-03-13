import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { ArrowUpDown, ArrowUp, ArrowDown, ChevronDown, Filter, X } from "lucide-react";
import { getStrategyOpportunities } from "@/lib/api";
import type { GrowthOpportunity } from "@/lib/api";
import { formatPLN, cn } from "@/lib/utils";
import { ServerPagination } from "@/components/shared";
import OpportunityDetailDrawer from "@/components/strategy/OpportunityDetailDrawer";

const MKT_OPTIONS = [
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

const TYPE_OPTIONS = [
  { id: "", label: "All types" },
  { id: "PRICE_INCREASE", label: "Price Increase" },
  { id: "PRICE_DECREASE", label: "Price Decrease" },
  { id: "ADS_SCALE_UP", label: "Ads Scale Up" },
  { id: "ADS_CUT_WASTE", label: "Ads Cut Waste" },
  { id: "CONTENT_FIX", label: "Content Fix" },
  { id: "STOCK_REPLENISH", label: "Stock Replenish" },
  { id: "MARKETPLACE_EXPANSION", label: "Marketplace Expansion" },
  { id: "FAMILY_REPAIR", label: "Family Repair" },
  { id: "RETURN_REDUCTION", label: "Return Reduction" },
  { id: "COST_RENEGOTIATION", label: "Cost Renegotiation" },
  { id: "CATEGORY_WINNER_SCALE", label: "Scale Winner" },
];

const STATUS_OPTIONS = [
  { id: "", label: "Active (new+review+accepted)" },
  { id: "new", label: "New" },
  { id: "in_review", label: "In Review" },
  { id: "accepted", label: "Accepted" },
  { id: "rejected", label: "Rejected" },
  { id: "completed", label: "Completed" },
];

const QUICK_FILTERS = [
  { id: "", label: "All" },
  { id: "do_now", label: "🔴 Do Now" },
  { id: "high_impact_low_effort", label: "⚡ High Impact / Low Effort" },
  { id: "pricing", label: "💰 Pricing" },
  { id: "inventory", label: "📦 Inventory" },
  { id: "marketplace_expansion", label: "🌍 Expansion" },
  { id: "content", label: "📝 Content" },
  { id: "bundles", label: "🎁 Bundles" },
  { id: "family_repair", label: "🔧 Family" },
];

const PRIO_COLORS: Record<string, string> = {
  do_now: "bg-red-500/15 text-red-400",
  this_week: "bg-orange-500/15 text-orange-400",
  this_month: "bg-blue-500/15 text-blue-400",
  backlog: "bg-zinc-500/15 text-zinc-400",
  low: "bg-zinc-800/30 text-zinc-500",
};

const STATUS_COLORS: Record<string, string> = {
  new: "bg-blue-500/15 text-blue-400",
  in_review: "bg-yellow-500/15 text-yellow-400",
  accepted: "bg-green-500/15 text-green-400",
  rejected: "bg-red-500/15 text-red-400",
  completed: "bg-emerald-500/15 text-emerald-400",
};

type SortField = "priority_score" | "confidence_score" | "estimated_profit_uplift" | "estimated_revenue_uplift" | "effort_score" | "created_at";

export default function StrategyOpportunitiesPage() {
  const [search] = useSearchParams();
  const [page, setPage] = useState(1);
  const [marketplace, setMarketplace] = useState(search.get("marketplace_id") || "");
  const [oppType, setOppType] = useState(search.get("opportunity_type") || "");
  const [status, setStatus] = useState(search.get("status") || "");
  const [quickFilter, setQuickFilter] = useState(search.get("quick_filter") || "");
  const [sku, setSku] = useState("");
  const [sortField, setSortField] = useState<SortField>("priority_score");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [selectedId, setSelectedId] = useState<number | null>(() => {
    const id = search.get("id");
    return id ? Number(id) : null;
  });

  const params: Record<string, string | number> = { page, page_size: 50, sort: sortField, dir: sortDir };
  if (marketplace) params.marketplace_id = marketplace;
  if (oppType) params.opportunity_type = oppType;
  if (status) params.status = status;
  if (quickFilter) params.quick_filter = quickFilter;
  if (sku) params.sku = sku;

  const { data, isLoading } = useQuery({
    queryKey: ["strategy-opportunities", params],
    queryFn: () => getStrategyOpportunities(params),
    staleTime: 30_000,
  });

  const items: GrowthOpportunity[] = data?.items ?? [];

  const handleSort = (field: string) => {
    const sf = field as SortField;
    if (sf === sortField) setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    else { setSortField(sf); setSortDir("desc"); }
    setPage(1);
  };

  const SortIcon = ({ field }: { field: string }) => {
    if (field !== sortField) return <ArrowUpDown className="h-3 w-3 opacity-30" />;
    return sortDir === "desc" ? <ArrowDown className="h-3 w-3 text-amazon" /> : <ArrowUp className="h-3 w-3 text-amazon" />;
  };

  return (
    <div className="space-y-4 p-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Growth Opportunities</h1>
        <p className="text-sm text-muted-foreground mt-0.5">Wszystkie wykryte szanse wzrostu — filtruj, sortuj, działaj</p>
      </div>

      {/* Quick filters */}
      <div className="flex flex-wrap gap-2">
        {QUICK_FILTERS.map((qf) => (
          <button
            key={qf.id}
            onClick={() => { setQuickFilter(qf.id); setPage(1); }}
            className={cn(
              "rounded-full border px-3 py-1 text-xs font-medium transition",
              quickFilter === qf.id
                ? "border-amazon bg-amazon/15 text-amazon"
                : "border-border hover:border-amazon/50",
            )}
          >
            {qf.label}
          </button>
        ))}
      </div>

      {/* Filters row */}
      <div className="flex flex-wrap items-end gap-3">
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Marketplace</span>
          <div className="relative">
            <select value={marketplace} onChange={(e) => { setMarketplace(e.target.value); setPage(1); }}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm">
              {MKT_OPTIONS.map((m) => <option key={m.id} value={m.id}>{m.label}</option>)}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          </div>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Type</span>
          <div className="relative">
            <select value={oppType} onChange={(e) => { setOppType(e.target.value); setPage(1); }}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm">
              {TYPE_OPTIONS.map((t) => <option key={t.id} value={t.id}>{t.label}</option>)}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          </div>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Status</span>
          <div className="relative">
            <select value={status} onChange={(e) => { setStatus(e.target.value); setPage(1); }}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm">
              {STATUS_OPTIONS.map((s) => <option key={s.id} value={s.id}>{s.label}</option>)}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          </div>
        </label>
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">SKU</span>
          <input type="text" value={sku} onChange={(e) => { setSku(e.target.value); setPage(1); }}
            placeholder="Filter…" className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm w-36" />
        </label>
      </div>

      {/* Table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              <Th label="Prio" field="priority_score" sortable onSort={handleSort} SortIcon={SortIcon} />
              <th className="px-3 py-2 text-left">Type</th>
              <th className="px-3 py-2 text-left">MKT</th>
              <th className="px-3 py-2 text-left">SKU</th>
              <th className="px-3 py-2 text-left max-w-[200px]">Title</th>
              <th className="px-3 py-2 text-left">Root Cause</th>
              <Th label="Profit ↑" field="estimated_profit_uplift" sortable align="right" onSort={handleSort} SortIcon={SortIcon} />
              <Th label="Revenue ↑" field="estimated_revenue_uplift" sortable align="right" onSort={handleSort} SortIcon={SortIcon} />
              <Th label="Confidence" field="confidence_score" sortable align="right" onSort={handleSort} SortIcon={SortIcon} />
              <Th label="Effort" field="effort_score" sortable align="right" onSort={handleSort} SortIcon={SortIcon} />
              <th className="px-3 py-2 text-left">Owner</th>
              <th className="px-3 py-2 text-left">Status</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 10 }).map((_, i) => (
                <tr key={i} className="border-b border-border/50"><td colSpan={12} className="px-3 py-3"><div className="h-4 bg-muted/30 rounded animate-pulse" /></td></tr>
              ))
            ) : items.length === 0 ? (
              <tr><td colSpan={12} className="px-3 py-8 text-center text-muted-foreground">No opportunities found</td></tr>
            ) : items.map((opp) => (
              <tr key={opp.id} className="border-b border-border/50 hover:bg-muted/20 cursor-pointer" onClick={() => setSelectedId(opp.id)}>
                <td className="px-3 py-2">
                  <span className={cn("rounded-full px-2 py-0.5 text-[10px] font-bold", PRIO_COLORS[opp.priority_label || "low"])}>
                    {opp.priority_score.toFixed(0)}
                  </span>
                </td>
                <td className="px-3 py-2 text-xs whitespace-nowrap">{opp.opportunity_type.replace(/_/g, " ")}</td>
                <td className="px-3 py-2 text-xs">{opp.marketplace_code || "—"}</td>
                <td className="px-3 py-2 font-mono text-xs max-w-[120px] truncate" title={opp.sku || ""}>{opp.sku || "—"}</td>
                <td className="px-3 py-2 text-xs max-w-[200px] truncate" title={opp.title}>{opp.title}</td>
                <td className="px-3 py-2 text-xs text-muted-foreground">{opp.root_cause?.replace(/_/g, " ") || "—"}</td>
                <td className="px-3 py-2 text-right text-xs tabular-nums text-green-500 font-medium">
                  {opp.estimated_profit_uplift != null ? formatPLN(opp.estimated_profit_uplift) : "—"}
                </td>
                <td className="px-3 py-2 text-right text-xs tabular-nums">
                  {opp.estimated_revenue_uplift != null ? formatPLN(opp.estimated_revenue_uplift) : "—"}
                </td>
                <td className="px-3 py-2 text-right text-xs tabular-nums">{opp.confidence_score.toFixed(0)}%</td>
                <td className="px-3 py-2 text-right text-xs tabular-nums">{opp.effort_score != null ? opp.effort_score.toFixed(0) : "—"}</td>
                <td className="px-3 py-2 text-xs text-muted-foreground">{opp.owner_role?.replace(/_/g, " ") || "—"}</td>
                <td className="px-3 py-2">
                  <span className={cn("rounded px-1.5 py-0.5 text-[10px] font-bold uppercase", STATUS_COLORS[opp.status] || "bg-muted")}>
                    {opp.status}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {data && data.pages > 1 && (
        <ServerPagination page={page} pages={data.pages} total={data.total} pageSize={50} onPageChange={setPage} />
      )}

      {selectedId && (
        <OpportunityDetailDrawer oppId={selectedId} onClose={() => setSelectedId(null)} />
      )}
    </div>
  );
}

/* Sortable table header cell */
function Th({ label, field, sortable, align, onSort, SortIcon }: {
  label: string; field: string; sortable?: boolean; align?: "left" | "right";
  onSort: (f: string) => void; SortIcon: React.FC<{ field: string }>;
}) {
  return (
    <th
      className={cn("px-3 py-2", align === "right" ? "text-right" : "text-left", sortable && "cursor-pointer select-none hover:text-foreground")}
      onClick={sortable ? () => onSort(field) : undefined}
    >
      <span className="inline-flex items-center gap-1">
        {label}
        {sortable && <SortIcon field={field} />}
      </span>
    </th>
  );
}
