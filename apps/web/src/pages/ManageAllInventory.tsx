import { Fragment, useEffect, useMemo, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { AlertTriangle, ChevronDown, ChevronRight, FilterX, ShieldAlert, TrendingDown } from "lucide-react";
import { useSearchParams } from "react-router-dom";
import {
  getManageInventoryAll,
  getManageInventorySkuDetail,
  type ManageInventoryDecisionItem,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ClientExportButton } from "@/components/shared";
import { formatPLN } from "@/lib/utils";

type OptionalColumnKey =
  | "brand"
  | "category"
  | "family"
  | "traffic30"
  | "deltas"
  | "stranded"
  | "aged";

const COLUMN_LABELS: Record<OptionalColumnKey, string> = {
  brand: "Brand / category",
  family: "Family",
  traffic30: "Traffic 30d",
  deltas: "Deltas",
  stranded: "Stranded",
  aged: "Aged 90+",
  category: "Product type",
};

const DEFAULT_COLUMNS: Record<OptionalColumnKey, boolean> = {
  brand: true,
  category: false,
  family: true,
  traffic30: false,
  deltas: false,
  stranded: false,
  aged: false,
};

function badgeVariant(status: string): "success" | "secondary" | "warning" | "destructive" {
  if (status === "critical") return "destructive";
  if (status === "warning") return "warning";
  if (status === "ok") return "success";
  return "secondary";
}

function trafficBadge(item: ManageInventoryDecisionItem) {
  if (item.traffic_coverage_flag) return "partial";
  return "live";
}

function formatPct(value?: number | null) {
  if (value == null || Number.isNaN(value)) return "-";
  return `${value.toFixed(1)}%`;
}

function decisionHint(summary: ManageInventoryDecisionItem, allItems: ManageInventoryDecisionItem[]) {
  const hasSuppressed = allItems.some((item) => item.listing_status.toLowerCase() === "suppressed");
  const daysCover = summary.days_cover ?? 9999;
  const sessions = summary.sessions_7d ?? 0;
  const cvrDelta = summary.cvr_delta_pct ?? 0;

  if (daysCover < 7 || summary.stockout_risk_badge === "critical") return "P1: replenishment / inbound teraz";
  if (hasSuppressed) return "P1: listing suppression do naprawy";
  if (sessions >= 150 && cvrDelta <= -20) return "P2: ruch jest, konwersja spada";
  if (summary.stranded_value_pln > 0) return "P2: stranded wymagaja case/fix";
  if (summary.aged_90_plus_value_pln > 0) return "P3: aged 90+ do akcji";
  return "Monitoruj";
}

function actionScore(summary: ManageInventoryDecisionItem, allItems: ManageInventoryDecisionItem[]) {
  const hasSuppressed = allItems.some((item) => item.listing_status.toLowerCase() === "suppressed");
  const daysCover = summary.days_cover ?? 9999;
  const sessions = summary.sessions_7d ?? 0;
  const cvrDelta = summary.cvr_delta_pct ?? 0;
  let score = 0;

  if (daysCover < 7 || summary.stockout_risk_badge === "critical") score += 120;
  if (hasSuppressed) score += 80;
  if (sessions >= 150 && cvrDelta <= -20) score += 65;
  if (summary.stranded_value_pln > 0) score += 35;
  if (summary.aged_90_plus_value_pln > 0) score += 20;
  if (summary.traffic_coverage_flag) score -= 10;
  return score;
}

export default function ManageAllInventoryPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [search, setSearch] = useState(searchParams.get("search") ?? "");
  const [marketplace, setMarketplace] = useState(searchParams.get("marketplace") ?? "");
  const [riskType, setRiskType] = useState(searchParams.get("risk_type") ?? "");
  const [listingStatus, setListingStatus] = useState(searchParams.get("listing_status") ?? "");
  const [columns, setColumns] = useState<Record<OptionalColumnKey, boolean>>(DEFAULT_COLUMNS);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [selectedSku, setSelectedSku] = useState<string | null>(null);
  const [selectedMarketplaceId, setSelectedMarketplaceId] = useState<string | null>(null);

  useEffect(() => {
    const next = new URLSearchParams();
    if (search.trim()) next.set("search", search.trim());
    if (marketplace.trim()) next.set("marketplace", marketplace.trim());
    if (riskType.trim()) next.set("risk_type", riskType.trim());
    if (listingStatus.trim()) next.set("listing_status", listingStatus.trim());
    setSearchParams(next, { replace: true });
  }, [listingStatus, marketplace, riskType, search, setSearchParams]);

  const listQuery = useQuery({
    queryKey: ["manage-inventory-all", search, marketplace, riskType, listingStatus],
    queryFn: () =>
      getManageInventoryAll({
        ...(search.trim() ? { search: search.trim() } : {}),
        ...(marketplace.trim() ? { marketplace: marketplace.trim() } : {}),
        ...(riskType.trim() ? { risk_type: riskType.trim() } : {}),
        ...(listingStatus.trim() ? { listing_status: listingStatus.trim() } : {}),
      }),
    staleTime: 2 * 60_000,
    placeholderData: keepPreviousData,
  });

  const detailQuery = useQuery({
    queryKey: ["manage-inventory-sku-detail", selectedSku, selectedMarketplaceId],
    queryFn: () => getManageInventorySkuDetail(selectedSku!, selectedMarketplaceId ?? undefined),
    enabled: !!selectedSku,
    staleTime: 5 * 60_000,
    placeholderData: keepPreviousData,
  });

  const groupedItems = useMemo(() => {
    const grouped = new Map<
      string,
      {
        sku: string;
        title_preferred?: string | null;
        marketplaces: string[];
        marketplace_ids: string[];
        itemCount: number;
        summary: ManageInventoryDecisionItem;
        items: ManageInventoryDecisionItem[];
      }
    >();

    for (const item of listQuery.data?.items ?? []) {
      const existing = grouped.get(item.sku);
      if (existing) {
        if (!existing.marketplaces.includes(item.marketplace_code)) existing.marketplaces.push(item.marketplace_code);
        if (!existing.marketplace_ids.includes(item.marketplace_id)) existing.marketplace_ids.push(item.marketplace_id);
        existing.itemCount += 1;
        existing.items.push(item);
        existing.summary.fba_on_hand += item.fba_on_hand ?? 0;
        existing.summary.fba_available += item.fba_available ?? 0;
        existing.summary.inbound += item.inbound ?? 0;
        existing.summary.reserved += item.reserved ?? 0;
        existing.summary.fbm_on_hand += item.fbm_on_hand ?? 0;
        existing.summary.velocity_7d_units += item.velocity_7d_units ?? 0;
        existing.summary.velocity_30d_units += item.velocity_30d_units ?? 0;
        existing.summary.orders_7d += item.orders_7d ?? 0;
        existing.summary.units_ordered_7d += item.units_ordered_7d ?? 0;
        existing.summary.stranded_units += item.stranded_units ?? 0;
        existing.summary.stranded_value_pln += item.stranded_value_pln ?? 0;
        existing.summary.aged_90_plus_units += item.aged_90_plus_units ?? 0;
        existing.summary.aged_90_plus_value_pln += item.aged_90_plus_value_pln ?? 0;
        if ((item.days_cover ?? 9999) < (existing.summary.days_cover ?? 9999)) {
          existing.summary.days_cover = item.days_cover;
          existing.summary.stockout_risk_badge = item.stockout_risk_badge;
          existing.summary.overstock_risk_badge = item.overstock_risk_badge;
          existing.summary.demand_vs_supply_badge = item.demand_vs_supply_badge;
        }
        if (!existing.summary.title_preferred && item.title_preferred) existing.summary.title_preferred = item.title_preferred;
        continue;
      }

      grouped.set(item.sku, {
        sku: item.sku,
        title_preferred: item.title_preferred,
        marketplaces: item.marketplace_code ? [item.marketplace_code] : [],
        marketplace_ids: item.marketplace_id ? [item.marketplace_id] : [],
        itemCount: 1,
        summary: { ...item },
        items: [item],
      });
    }

    return Array.from(grouped.values()).sort((left, right) => {
      const leftDays = left.summary.days_cover ?? 9999;
      const rightDays = right.summary.days_cover ?? 9999;
      if (leftDays !== rightDays) return leftDays - rightDays;
      return right.summary.sessions_7d ?? 0 - (left.summary.sessions_7d ?? 0);
    });
  }, [listQuery.data?.items]);

  const coverageTraffic = useMemo(
    () => listQuery.data?.coverage.find((item) => item.key === "traffic") ?? null,
    [listQuery.data?.coverage],
  );

  const dashboardStats = useMemo(() => {
    let criticalStockout = 0;
    let suppressed = 0;
    let cvrCrash = 0;
    let dataPartial = 0;
    for (const group of groupedItems) {
      const summary = group.summary;
      const hasSuppressed = group.items.some((item) => item.listing_status.toLowerCase() === "suppressed");
      if ((summary.days_cover ?? 9999) < 7 || summary.stockout_risk_badge === "critical") criticalStockout += 1;
      if (hasSuppressed) suppressed += 1;
      if ((summary.sessions_7d ?? 0) >= 150 && (summary.cvr_delta_pct ?? 0) <= -20) cvrCrash += 1;
      if (group.items.some((item) => item.traffic_coverage_flag)) dataPartial += 1;
    }
    return {
      total: groupedItems.length,
      criticalStockout,
      suppressed,
      cvrCrash,
      dataPartial,
    };
  }, [groupedItems]);

  const actionQueue = useMemo(
    () =>
      groupedItems
        .map((group) => ({
          sku: group.sku,
          title: group.title_preferred ?? group.sku,
          marketplaces: group.marketplaces.join(", "),
          marketplaceId: group.marketplace_ids[0] ?? group.summary.marketplace_id,
          score: actionScore(group.summary, group.items),
          hint: decisionHint(group.summary, group.items),
          summary: group.summary,
        }))
        .filter((item) => item.score > 0)
        .sort((left, right) => right.score - left.score)
        .slice(0, 8),
    [groupedItems],
  );

  const resetAllFilters = () => {
    setSearch("");
    setMarketplace("");
    setRiskType("");
    setListingStatus("");
  };

  const toggleColumn = (key: OptionalColumnKey) => {
    setColumns((current) => ({ ...current, [key]: !current[key] }));
  };

  const tableColSpan =
    7 +
    (columns.brand ? 1 : 0) +
    (columns.family ? 1 : 0) +
    (columns.traffic30 ? 1 : 0) +
    (columns.deltas ? 1 : 0) +
    (columns.stranded ? 1 : 0) +
    (columns.aged ? 1 : 0);

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-2 xl:flex-row xl:items-end xl:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Inventory Control Desk</h1>
          <p className="text-sm text-white/50">Najpierw decyzje operacyjne, potem szczegóły SKU i marketplace.</p>
        </div>
        <div className="text-xs text-white/45">
          Snapshot: {listQuery.data?.snapshot_date ?? "-"} | Produkty: {dashboardStats.total}
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <Card className="border-red-500/30">
          <CardContent className="flex items-center justify-between p-4">
            <div>
              <div className="text-xs uppercase tracking-wide text-white/50">P1 Replenishment</div>
              <div className="text-2xl font-semibold text-white">{dashboardStats.criticalStockout}</div>
              <div className="text-xs text-white/45">cover &lt; 7 dni / stockout critical</div>
            </div>
            <ShieldAlert className="h-5 w-5 text-red-400" />
          </CardContent>
        </Card>
        <Card className="border-amber-500/30">
          <CardContent className="flex items-center justify-between p-4">
            <div>
              <div className="text-xs uppercase tracking-wide text-white/50">Suppressions</div>
              <div className="text-2xl font-semibold text-white">{dashboardStats.suppressed}</div>
              <div className="text-xs text-white/45">produkty z blokadą listingu</div>
            </div>
            <AlertTriangle className="h-5 w-5 text-amber-400" />
          </CardContent>
        </Card>
        <Card className="border-[#FF9900]/30">
          <CardContent className="flex items-center justify-between p-4">
            <div>
              <div className="text-xs uppercase tracking-wide text-white/50">CVR Crash</div>
              <div className="text-2xl font-semibold text-white">{dashboardStats.cvrCrash}</div>
              <div className="text-xs text-white/45">sesje &gt;= 150 i CVR delta &lt;= -20%</div>
            </div>
            <TrendingDown className="h-5 w-5 text-[#FF9900]" />
          </CardContent>
        </Card>
        <Card className={coverageTraffic?.status === "ok" ? "border-emerald-500/30" : "border-amber-500/30"}>
          <CardContent className="space-y-2 p-4">
            <div className="flex items-center justify-between">
              <div className="text-xs uppercase tracking-wide text-white/50">Traffic coverage</div>
              <Badge variant={badgeVariant(coverageTraffic?.status ?? "warning")}>{coverageTraffic?.status ?? "partial"}</Badge>
            </div>
            <Progress value={Math.max(0, Math.min(100, coverageTraffic?.pct ?? 0))} />
            <div className="text-xs text-white/45">
              {coverageTraffic?.label ?? "Traffic completeness"} | partial rows: {dashboardStats.dataPartial}
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader className="gap-4">
          <div className="flex flex-wrap items-center gap-2">
            <Input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Szukaj: SKU / ASIN / EAN (multi-line)" className="w-64" />
            <Input value={marketplace} onChange={(e) => setMarketplace(e.target.value)} placeholder="Marketplace (ID lub kod)" className="w-52" />
            <select value={riskType} onChange={(e) => setRiskType(e.target.value)} className="rounded-md border border-white/10 bg-[#111827] px-3 py-2 text-sm">
              <option value="">Wszystkie ryzyka</option>
              <option value="stockout">Stockout</option>
              <option value="overstock">Overstock</option>
              <option value="stranded">Stranded</option>
              <option value="aged">Aged 90+</option>
            </select>
            <select value={listingStatus} onChange={(e) => setListingStatus(e.target.value)} className="rounded-md border border-white/10 bg-[#111827] px-3 py-2 text-sm">
              <option value="">Wszystkie statusy listingu</option>
              <option value="active">Active</option>
              <option value="suppressed">Suppressed</option>
              <option value="inactive">Inactive</option>
            </select>
            <Button variant="outline" onClick={resetAllFilters}>
              <FilterX className="mr-2 h-4 w-4" />
              Reset filtrów
            </Button>
            <Button variant="secondary" onClick={() => setColumns(DEFAULT_COLUMNS)}>Reset kolumn</Button>
            <ClientExportButton data={listQuery.data?.items ?? []} filename="manage_inventory" />
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Button
              size="sm"
              variant={riskType === "stockout" ? "default" : "outline"}
              onClick={() => setRiskType(riskType === "stockout" ? "" : "stockout")}
            >
              Priorytet: Stockout
            </Button>
            <Button
              size="sm"
              variant={listingStatus === "suppressed" ? "default" : "outline"}
              onClick={() => setListingStatus(listingStatus === "suppressed" ? "" : "suppressed")}
            >
              Priorytet: Suppressed
            </Button>
            <Button
              size="sm"
              variant={riskType === "stranded" ? "default" : "outline"}
              onClick={() => setRiskType(riskType === "stranded" ? "" : "stranded")}
            >
              Priorytet: Stranded
            </Button>
          </div>
          <div className="flex flex-wrap gap-2">
            {(Object.keys(COLUMN_LABELS) as OptionalColumnKey[]).map((key) => (
              <button
                key={key}
                type="button"
                onClick={() => toggleColumn(key)}
                className={`rounded-full border px-2 py-1 text-xs ${columns[key] ? "border-[#FF9900]/40 bg-[#FF9900]/10 text-[#FF9900]" : "border-white/10 text-white/50"}`}
              >
                {COLUMN_LABELS[key]}
              </button>
            ))}
          </div>
        </CardHeader>
        <CardContent className="grid gap-4 xl:grid-cols-[2.2fr_1fr]">
          <div className="overflow-hidden rounded-lg border border-white/10">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>SKU / ASIN</TableHead>
                  {columns.brand ? <TableHead>Brand / category</TableHead> : null}
                  {columns.family ? <TableHead>Family</TableHead> : null}
                  <TableHead className="text-right">FBA Avail</TableHead>
                  <TableHead className="text-right">Inbound</TableHead>
                  <TableHead className="text-right">Days cover</TableHead>
                  <TableHead className="text-right">Sessions 7d</TableHead>
                  {columns.traffic30 ? <TableHead className="text-right">Sessions 30d</TableHead> : null}
                  <TableHead className="text-right">CVR 7d</TableHead>
                  {columns.deltas ? <TableHead className="text-right">Delta</TableHead> : null}
                  {columns.stranded ? <TableHead className="text-right">Stranded</TableHead> : null}
                  {columns.aged ? <TableHead className="text-right">Aged 90+</TableHead> : null}
                  <TableHead>Decision</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {groupedItems.map((group) => {
                  const expandedRow = !!expanded[group.sku];
                  return (
                    <Fragment key={group.sku}>
                      <TableRow className={selectedSku === group.sku ? "bg-white/5" : "hover:bg-white/[0.03]"}>
                        <TableCell>
                          <button
                            type="button"
                            onClick={() => {
                              setExpanded((current) => ({ ...current, [group.sku]: !current[group.sku] }));
                              setSelectedSku(group.sku);
                              setSelectedMarketplaceId(group.summary.marketplace_id);
                            }}
                            className="flex w-full items-start gap-2 text-left"
                          >
                            {expandedRow ? <ChevronDown className="mt-0.5 h-4 w-4 shrink-0 text-white/50" /> : <ChevronRight className="mt-0.5 h-4 w-4 shrink-0 text-white/50" />}
                            <div className="min-w-0">
                              <div className="max-w-[22rem] truncate text-sm font-medium text-white">{group.title_preferred ?? group.sku}</div>
                              <div className="font-mono text-[11px] text-white/45">{group.sku}</div>
                              <div className="text-[11px] text-white/40">
                                {group.summary.asin ?? "-"} | {group.marketplaces.join(", ") || "-"} | traffic {trafficBadge(group.summary)}
                              </div>
                            </div>
                          </button>
                        </TableCell>
                        {columns.brand ? (
                          <TableCell className="text-xs text-white/60">
                            {group.summary.brand ?? "-"}
                            <div>{group.summary.category ?? "-"}</div>
                            {columns.category ? <div>{group.summary.product_type ?? "-"}</div> : null}
                          </TableCell>
                        ) : null}
                        {columns.family ? (
                          <TableCell className="text-xs">
                            <div>{group.summary.local_parent_asin ?? "-"}</div>
                            <div className="text-white/45">{group.summary.family_health} / {group.summary.global_family_status}</div>
                          </TableCell>
                        ) : null}
                        <TableCell className="text-right">{group.summary.fba_available}</TableCell>
                        <TableCell className="text-right">{group.summary.inbound}</TableCell>
                        <TableCell className={`text-right ${(group.summary.days_cover ?? 9999) < 7 ? "font-semibold text-red-300" : ""}`}>
                          {group.summary.days_cover ?? "-"}
                        </TableCell>
                        <TableCell className="text-right">{group.summary.sessions_7d ?? "-"}</TableCell>
                        {columns.traffic30 ? <TableCell className="text-right">{group.summary.sessions_30d ?? "-"}</TableCell> : null}
                        <TableCell className="text-right">{formatPct(group.summary.unit_session_pct_7d)}</TableCell>
                        {columns.deltas ? (
                          <TableCell className="text-right text-xs">
                            <div>{formatPct(group.summary.sessions_delta_pct)}</div>
                            <div className="text-white/45">{formatPct(group.summary.cvr_delta_pct)} CVR</div>
                          </TableCell>
                        ) : null}
                        {columns.stranded ? <TableCell className="text-right">{formatPLN(group.summary.stranded_value_pln)}</TableCell> : null}
                        {columns.aged ? <TableCell className="text-right">{formatPLN(group.summary.aged_90_plus_value_pln)}</TableCell> : null}
                        <TableCell>
                          <div className="space-y-1">
                            <Badge variant={badgeVariant(group.summary.stockout_risk_badge === "ok" ? group.summary.overstock_risk_badge : group.summary.stockout_risk_badge)}>
                              {group.summary.demand_vs_supply_badge}
                            </Badge>
                            <div className="max-w-[16rem] text-[11px] leading-tight text-white/60">
                              {decisionHint(group.summary, group.items)}
                            </div>
                          </div>
                        </TableCell>
                      </TableRow>
                      {expandedRow
                        ? group.items.map((item) => (
                            <TableRow
                              key={`${item.marketplace_id}-${item.sku}`}
                              className={selectedMarketplaceId === item.marketplace_id ? "bg-[#FF9900]/5" : "bg-white/[0.02]"}
                              onClick={() => {
                                setSelectedSku(item.sku);
                                setSelectedMarketplaceId(item.marketplace_id);
                              }}
                            >
                              <TableCell className="pl-10 text-xs text-white/50">{item.marketplace_code}</TableCell>
                              {columns.brand ? <TableCell className="text-xs text-white/45">{item.fulfillment_badge}</TableCell> : null}
                              {columns.family ? <TableCell className="text-xs text-white/45">{item.local_theme ?? "-"}</TableCell> : null}
                              <TableCell className="text-right">{item.fba_available}</TableCell>
                              <TableCell className="text-right">{item.inbound}</TableCell>
                              <TableCell className={`text-right ${(item.days_cover ?? 9999) < 7 ? "font-semibold text-red-300" : ""}`}>{item.days_cover ?? "-"}</TableCell>
                              <TableCell className="text-right">{item.sessions_7d ?? "-"}</TableCell>
                              {columns.traffic30 ? <TableCell className="text-right">{item.sessions_30d ?? "-"}</TableCell> : null}
                              <TableCell className="text-right">{formatPct(item.unit_session_pct_7d)}</TableCell>
                              {columns.deltas ? <TableCell className="text-right">{formatPct(item.cvr_delta_pct)}</TableCell> : null}
                              {columns.stranded ? <TableCell className="text-right">{formatPLN(item.stranded_value_pln)}</TableCell> : null}
                              {columns.aged ? <TableCell className="text-right">{formatPLN(item.aged_90_plus_value_pln)}</TableCell> : null}
                              <TableCell>
                                <Badge variant={badgeVariant(item.traffic_coverage_flag ? "warning" : "ok")}>
                                  {item.traffic_coverage_flag ? "partial" : item.listing_status}
                                </Badge>
                              </TableCell>
                            </TableRow>
                          ))
                        : null}
                    </Fragment>
                  );
                })}
                {!listQuery.isLoading && groupedItems.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={tableColSpan} className="text-center text-white/50">
                      Brak rekordow dla biezacych filtrow.
                    </TableCell>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>
          </div>

          <Card className="h-fit">
            <CardHeader>
              <CardTitle className="text-sm">{selectedSku ? "SKU detail" : "Co robić teraz"}</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              {!selectedSku ? (
                <div className="space-y-3">
                  <div className="text-sm text-white/60">
                    Kolejka priorytetów zbudowana na cover, suppression, CVR i stranded.
                  </div>
                  {actionQueue.length === 0 ? (
                    <div className="text-sm text-white/45">Brak pozycji wymagających pilnej akcji.</div>
                  ) : (
                    actionQueue.map((item) => (
                      <button
                        key={item.sku}
                        type="button"
                        onClick={() => {
                          setSelectedSku(item.sku);
                          setSelectedMarketplaceId(item.marketplaceId);
                        }}
                        className="w-full rounded-lg border border-white/10 bg-white/[0.02] p-3 text-left transition hover:bg-white/[0.05]"
                      >
                        <div className="flex items-start justify-between gap-2">
                          <div className="min-w-0">
                            <div className="truncate text-sm font-medium text-white">{item.title}</div>
                            <div className="font-mono text-[11px] text-white/45">{item.sku}</div>
                            <div className="text-[11px] text-white/45">{item.marketplaces || "-"}</div>
                          </div>
                          <Badge variant={item.score >= 100 ? "destructive" : item.score >= 60 ? "warning" : "secondary"}>
                            {item.score}
                          </Badge>
                        </div>
                        <div className="mt-2 text-xs text-white/65">{item.hint}</div>
                      </button>
                    ))
                  )}
                </div>
              ) : detailQuery.isLoading ? (
                <div className="text-sm text-white/50">Ladowanie detalu...</div>
              ) : detailQuery.data ? (
                <>
                  <div>
                    <div className="text-lg font-semibold text-white">{detailQuery.data.item.title_preferred ?? detailQuery.data.item.sku}</div>
                    <div className="font-mono text-xs text-white/45">{detailQuery.data.item.sku}</div>
                    <div className="mt-1 flex flex-wrap gap-2">
                      <Badge variant={badgeVariant(detailQuery.data.item.stockout_risk_badge)}>{detailQuery.data.item.stockout_risk_badge}</Badge>
                      <Badge variant={badgeVariant(detailQuery.data.item.overstock_risk_badge)}>{detailQuery.data.item.overstock_risk_badge}</Badge>
                      <Badge variant={badgeVariant(detailQuery.data.item.traffic_coverage_flag ? "warning" : "ok")}>
                        {detailQuery.data.item.traffic_coverage_flag ? "traffic partial" : "traffic live"}
                      </Badge>
                    </div>
                  </div>

                  <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-1">
                    <div className="rounded-lg border border-white/10 p-3 text-sm">
                      <div className="font-medium text-white">Inventory</div>
                      <div className="mt-2 text-white/60">FBA on-hand {detailQuery.data.item.fba_on_hand}</div>
                      <div className="text-white/60">Available {detailQuery.data.item.fba_available}</div>
                      <div className="text-white/60">Inbound {detailQuery.data.item.inbound}</div>
                      <div className="text-white/60">Reserved {detailQuery.data.item.reserved}</div>
                    </div>
                    <div className="rounded-lg border border-white/10 p-3 text-sm">
                      <div className="font-medium text-white">Traffic & conversion</div>
                      <div className="mt-2 text-white/60">Sessions 7d {detailQuery.data.item.sessions_7d ?? "-"}</div>
                      <div className="text-white/60">Sessions 30d {detailQuery.data.item.sessions_30d ?? "-"}</div>
                      <div className="text-white/60">CVR 7d {detailQuery.data.item.unit_session_pct_7d ?? "-"}</div>
                      <div className="text-white/60">CVR 30d {detailQuery.data.item.unit_session_pct_30d ?? "-"}</div>
                    </div>
                  </div>

                  <div className="rounded-lg border border-white/10 p-3 text-sm">
                    <div className="font-medium text-white">Family / listing</div>
                    <div className="mt-2 text-white/60">Local parent {String(detailQuery.data.family_context.local_parent_asin ?? "-")}</div>
                    <div className="text-white/60">Local theme {String(detailQuery.data.family_context.local_theme ?? "-")}</div>
                    <div className="text-white/60">Global family {String(detailQuery.data.family_context.global_family_status ?? "-")}</div>
                  </div>

                  <div className="rounded-lg border border-white/10 p-3 text-sm">
                    <div className="font-medium text-white">Issues</div>
                    <ul className="mt-2 space-y-1 text-white/60">
                      {detailQuery.data.issues.length > 0 ? detailQuery.data.issues.map((issue) => <li key={issue}>- {issue}</li>) : <li>- Brak aktywnych issue flags.</li>}
                    </ul>
                  </div>

                  <div className="rounded-lg border border-white/10 p-3 text-sm">
                    <div className="font-medium text-white">Recent changes</div>
                    <div className="mt-2 space-y-2">
                      {detailQuery.data.change_history.slice(0, 6).map((event) => (
                        <div key={`${event.event_type}-${event.created_at}`} className="rounded border border-white/10 px-2 py-1">
                          <div className="text-xs text-white">{event.event_type}</div>
                          <div className="text-[11px] text-white/45">{event.actor ?? "system"} | {new Date(event.created_at).toLocaleString()}</div>
                        </div>
                      ))}
                      {detailQuery.data.change_history.length === 0 ? <div className="text-white/45">Brak historii zmian.</div> : null}
                    </div>
                  </div>
                </>
              ) : (
                <div className="text-sm text-white/50">Nie udalo sie zaladowac detalu SKU.</div>
              )}
            </CardContent>
          </Card>
        </CardContent>
      </Card>
    </div>
  );
}
