import { useState, useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { useDeferredValue, useMemo, Fragment } from "react";
import { useNavigate } from "react-router-dom";
import {
  createProductTask,
  exportProductProfitXlsx,
  getMarketplaces,
  getProductProfitTable,
  getProductWhatIfTable,
  type ProductProfitItem,
  type ProductWhatIfItem,
  type ProductProfitTableResponse,
  type ProductWhatIfResponse,
} from "@/lib/api";
import { usePageFilters, pageFiltersToApiParams } from "@/lib/usePageFilters";
import { cn, formatPct, formatPLN } from "@/lib/utils";
import { useUserPreferences } from "@/store/userPreferences";
import { DataWarningBanner } from "@/components/shared";
import { AlertTriangle, ChevronLeft, ChevronRight, Download, Filter, Settings2, Search, MoreHorizontal, X } from "lucide-react";

type GroupByMode =
  | "sku_marketplace"
  | "sku"
  | "asin_marketplace"
  | "asin"
  | "parent_marketplace"
  | "parent";
type SortDir = "asc" | "desc";
type TableMode = "realized" | "what_if";

type ColumnKey =
  | "sku"
  | "asin"
  | "title"
  | "marketplace"
  | "fulfillment"
  | "units"
  | "revenue"
  | "cogsUnit"
  | "feesUnit"
  | "cm1Profit"
  | "cm1Pct"
  | "adsCost"
  | "logisticsCost"
  | "returnsNet"
  | "fbaStorage"
  | "fbaAged"
  | "fbaRemoval"
  | "fbaLiquidation"
  | "overhead"
  | "cm2Profit"
  | "cm2Pct"
  | "npProfit"
  | "npPct"
  | "lossOrders"
  | "returnRate"
  | "tacos"
  | "daysCover"
  | "cogsCoverage"
  | "shippingMatch"
  | "financeMatch"
  | "confidence"
  | "flags"
  | "actions";

interface TableRow {
  rowId: string;
  entity_type?: "sku" | "asin" | "parent";
  group_key?: string;
  sku: string;
  sample_sku?: string;
  asin: string;
  parent_asin?: string;
  title: string;
  marketplace_id: string;
  marketplace_code: string;
  fulfillment_channel: string;
  units: number;
  sku_count?: number;
  child_count?: number;
  revenue_pln: number;
  cogs_per_unit: number;
  fees_per_unit: number;
  cm1_profit: number;
  cm1_percent: number;
  ads_cost_pln?: number;
  logistics_pln?: number;
  returns_net_pln?: number;
  fba_storage_fee_pln?: number;
  fba_aged_fee_pln?: number;
  fba_removal_fee_pln?: number;
  fba_liquidation_fee_pln?: number;
  overhead_allocated_pln?: number;
  overhead_allocation_method?: string;
  overhead_confidence_pct?: number;
  cm2_profit?: number;
  cm2_percent?: number;
  np_profit?: number;
  np_percent?: number;
  loss_orders_pct: number;
  return_rate?: number;
  tacos?: number;
  days_of_cover?: number;
  cogs_coverage_pct: number;
  shipping_match_pct?: number;
  finance_match_pct?: number;
  confidence_score: number;
  flags: string[];
}

const ALL_COLUMNS: Array<{ key: ColumnKey; label: string; align?: "left" | "right" }> = [
  { key: "sku", label: "SKU" },
  { key: "asin", label: "ASIN" },
  { key: "title", label: "Tytuł" },
  { key: "marketplace", label: "Marketplace" },
  { key: "fulfillment", label: "Fulfillment" },
  { key: "units", label: "Units", align: "right" },
  { key: "revenue", label: "Revenue", align: "right" },
  { key: "cogsUnit", label: "COGS/unit", align: "right" },
  { key: "feesUnit", label: "Fees/unit", align: "right" },
  { key: "cm1Profit", label: "CM1 Profit", align: "right" },
  { key: "cm1Pct", label: "CM1 %", align: "right" },
  { key: "adsCost", label: "Ads Cost", align: "right" },
  { key: "logisticsCost", label: "Logistics", align: "right" },
  { key: "returnsNet", label: "Returns Net", align: "right" },
  { key: "fbaStorage", label: "FBA Storage", align: "right" },
  { key: "fbaAged", label: "FBA Aged", align: "right" },
  { key: "fbaRemoval", label: "FBA Removal", align: "right" },
  { key: "fbaLiquidation", label: "FBA Liquidation", align: "right" },
  { key: "overhead", label: "Overhead", align: "right" },
  { key: "cm2Profit", label: "CM2 Profit", align: "right" },
  { key: "cm2Pct", label: "CM2 %", align: "right" },
  { key: "npProfit", label: "NP Profit", align: "right" },
  { key: "npPct", label: "NP %", align: "right" },
  { key: "lossOrders", label: "Loss Orders %", align: "right" },
  { key: "returnRate", label: "Return Rate", align: "right" },
  { key: "tacos", label: "TACoS", align: "right" },
  { key: "daysCover", label: "Days Cover", align: "right" },
  { key: "cogsCoverage", label: "COGS Coverage", align: "right" },
  { key: "shippingMatch", label: "Shipping Match", align: "right" },
  { key: "financeMatch", label: "Finance Match", align: "right" },
  { key: "confidence", label: "Confidence", align: "right" },
  { key: "flags", label: "Flags" },
  { key: "actions", label: "Actions" },
];

const DEFAULT_VISIBLE: ColumnKey[] = [
  "sku",
  "asin",
  "title",
  "marketplace",
  "units",
  "revenue",
  "cm1Profit",
  "cm1Pct",
  "adsCost",
  "logisticsCost",
  "returnsNet",
  "fbaStorage",
  "overhead",
  "cm2Profit",
  "cm2Pct",
  "npProfit",
  "npPct",
  "lossOrders",
  "returnRate",
  "tacos",
  "daysCover",
  "confidence",
  "actions",
];

function metricProfit(row: TableRow, mode: "cm1" | "cm2" | "np"): number {
  if (mode === "np") return row.np_profit ?? row.cm2_profit ?? row.cm1_profit;
  if (mode === "cm2") return row.cm2_profit ?? row.cm1_profit;
  return row.cm1_profit;
}

function metricPct(row: TableRow, mode: "cm1" | "cm2" | "np"): number {
  if (mode === "np") return row.np_percent ?? row.cm2_percent ?? row.cm1_percent;
  if (mode === "cm2") return row.cm2_percent ?? row.cm1_percent;
  return row.cm1_percent;
}

function whatIfMetricProfit(row: ProductWhatIfItem, mode: "cm1" | "cm2" | "np"): number {
  if (mode === "np") return row.np_profit ?? row.cm2_profit ?? row.cm1_profit;
  if (mode === "cm2") return row.cm2_profit ?? row.cm1_profit;
  return row.cm1_profit;
}

function whatIfMetricPct(row: ProductWhatIfItem, mode: "cm1" | "cm2" | "np"): number {
  if (mode === "np") return row.np_percent ?? row.cm2_percent ?? row.cm1_percent;
  if (mode === "cm2") return row.cm2_percent ?? row.cm1_percent;
  return row.cm1_percent;
}

function profitBadge(value: number) {
  if (value < 0) return "bg-red-500/10 text-red-400";
  if (value < 3) return "bg-yellow-500/10 text-yellow-400";
  return "bg-green-500/10 text-green-400";
}

function confidenceLabel(row: TableRow): string {
  if (row.flags.some((f) => f.toLowerCase().includes("missing"))) return "Estimated";
  if (row.flags.some((f) => f.toLowerCase().includes("alloc"))) return "Allocated";
  return "Actual";
}

export default function ProductProfitTablePage() {
  const navigate = useNavigate();
  const filters = usePageFilters();
  const { profitMode, currencyView, rowDensity } = useUserPreferences();

  const [page, setPage] = useState(1);
  const [pageSize] = useState(50);
  const [tableMode, setTableMode] = useState<TableMode>("realized");
  const [groupBy, setGroupBy] = useState<GroupByMode>("asin_marketplace");
  const [sortBy, setSortBy] = useState<ColumnKey>(() =>
    profitMode === "np" ? "npProfit" : profitMode === "cm2" ? "cm2Profit" : "cm1Profit"
  );
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [showColumns, setShowColumns] = useState(false);
  const [visibleColumns, setVisibleColumns] = useState<ColumnKey[]>(DEFAULT_VISIBLE);
  const [lossOnly, setLossOnly] = useState(false);
  const [lowConfidence, setLowConfidence] = useState(false);
  const [highReturns, setHighReturns] = useState(false);
  const [stockoutRisk, setStockoutRisk] = useState(false);
  const [adsHeavy, setAdsHeavy] = useState(false);
  const [scenarioQty, setScenarioQty] = useState(1);
  const [includeShippingCharge, setIncludeShippingCharge] = useState(true);
  const [expandedParents, setExpandedParents] = useState<Record<string, boolean>>({});
  const [parentChildrenRows, setParentChildrenRows] = useState<Record<string, ProductWhatIfItem[]>>({});
  const [parentChildrenLoading, setParentChildrenLoading] = useState<Record<string, boolean>>({});
  const [expandedRealizedParents, setExpandedRealizedParents] = useState<Record<string, boolean>>({});
  const [realizedParentChildrenRows, setRealizedParentChildrenRows] = useState<Record<string, TableRow[]>>({});
  const [realizedParentChildrenLoading, setRealizedParentChildrenLoading] = useState<Record<string, boolean>>({});
  const [inlineSearch, setInlineSearch] = useState("");
  const [inlineMarketplace, setInlineMarketplace] = useState("");
  const [jumpPageInput, setJumpPageInput] = useState("");
  const [actionMenuRow, setActionMenuRow] = useState<string | null>(null);
  const actionMenuRef = useRef<HTMLDivElement>(null);
  const deferredInlineSearch = useDeferredValue(inlineSearch.trim());
  const { data: marketplacesData } = useQuery({
    queryKey: ["marketplaces", "product-table-inline-filter"],
    queryFn: getMarketplaces,
    staleTime: 300_000,
  });
  const marketplaceOptions = useMemo(
    () =>
      [{ id: "", label: filters.marketplaceIds.length ? "Wszystkie (filtry strony)" : "Wszystkie rynki" }].concat(
        [...(marketplacesData ?? [])]
          .sort((a, b) => String(a.code || "").localeCompare(String(b.code || "")))
          .map((m) => ({ id: m.marketplace_id, label: m.code }))
      ),
    [marketplacesData, filters.marketplaceIds.length]
  );
  const inlineMarketplaceCode = useMemo(() => {
    if (!inlineMarketplace) return "";
    const hit = (marketplacesData ?? []).find((m) => m.marketplace_id === inlineMarketplace);
    return hit?.code || inlineMarketplace;
  }, [marketplacesData, inlineMarketplace]);

  // Close action menu on click outside
  useEffect(() => {
    if (!actionMenuRow) return;
    const handler = (e: MouseEvent) => {
      if (actionMenuRef.current && !actionMenuRef.current.contains(e.target as Node)) {
        setActionMenuRow(null);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [actionMenuRow]);

  const apiSortMap: Partial<Record<ColumnKey, string>> = {
    sku: "sku",
    units: "units",
    revenue: "revenue_pln",
    cm1Profit: "cm1_profit",
    adsCost: "ads_cost_pln",
    logisticsCost: "logistics_pln",
    returnsNet: "returns_net_pln",
    fbaStorage: "fba_storage_fee_pln",
    fbaAged: "fba_aged_fee_pln",
    fbaRemoval: "fba_removal_fee_pln",
    fbaLiquidation: "fba_liquidation_fee_pln",
    overhead: "overhead_allocated_pln",
    cm2Profit: "cm2_profit",
    npProfit: "np_profit",
    confidence: "confidence_score",
    lossOrders: "loss_orders_pct",
    returnRate: "return_rate",
    tacos: "tacos",
    daysCover: "days_of_cover",
  };

  const needExtendedCostComponents = useMemo(() => {
    if (profitMode !== "cm1") return true;
    const costColumns: ColumnKey[] = [
      "adsCost",
      "returnsNet",
      "fbaStorage",
      "fbaAged",
      "fbaRemoval",
      "fbaLiquidation",
      "overhead",
      "cm2Profit",
      "cm2Pct",
      "npProfit",
      "npPct",
    ];
    return visibleColumns.some((col) => costColumns.includes(col));
  }, [profitMode, visibleColumns]);

  const effectiveSortBy = sortBy;

  const effectiveFilters = useMemo(
    () => ({
      ...filters,
      skuAsinQuery: deferredInlineSearch || filters.skuAsinQuery,
      marketplaceIds: inlineMarketplace ? [inlineMarketplace] : filters.marketplaceIds,
    }),
    [filters, deferredInlineSearch, inlineMarketplace]
  );

  const baseApiParams = useMemo(
    () => pageFiltersToApiParams(effectiveFilters, { profitMode, currencyView }),
    [effectiveFilters, profitMode, currencyView]
  );

  const realizedParams = {
    ...baseApiParams,
    page,
    page_size: pageSize,
    group_by: groupBy,
    include_cost_components: needExtendedCostComponents,
    ...(apiSortMap[effectiveSortBy]
      ? { sort_by: apiSortMap[effectiveSortBy], sort_dir: sortDir }
      : {}),
  };

  const whatIfParams = {
    ...baseApiParams,
    page,
    page_size: pageSize,
    quantity: scenarioQty,
    include_shipping_charge: includeShippingCharge,
    include_cost_components: needExtendedCostComponents,
    group_by:
      groupBy === "asin_marketplace" || groupBy === "asin"
        ? groupBy
        : groupBy === "parent_marketplace" || groupBy === "parent"
        ? groupBy
        : "offer",
    sort_by:
      profitMode === "np"
        ? "np_profit"
        : profitMode === "cm2"
        ? "cm2_profit"
        : "cm1_profit",
    sort_dir: "desc",
  };

  const { data, isLoading, isError } = useQuery<ProductProfitTableResponse>({
    queryKey: ["profit-v2-products", realizedParams, groupBy, effectiveSortBy, sortDir],
    queryFn: () => getProductProfitTable(realizedParams),
    enabled: tableMode === "realized",
    placeholderData: (previous) => previous,
  });

  const {
    data: whatIfData,
    isLoading: isLoadingWhatIf,
  } = useQuery<ProductWhatIfResponse>({
    queryKey: ["profit-v2-what-if", whatIfParams, scenarioQty, includeShippingCharge],
    queryFn: () => getProductWhatIfTable(whatIfParams),
    enabled: tableMode === "what_if",
    placeholderData: (previous) => previous,
  });

  useEffect(() => {
    setPage(1);
  }, [tableMode, groupBy, sortBy, sortDir, scenarioQty, includeShippingCharge, baseApiParams]);

  useEffect(() => {
    setExpandedParents({});
    setParentChildrenRows({});
    setParentChildrenLoading({});
    setExpandedRealizedParents({});
    setRealizedParentChildrenRows({});
    setRealizedParentChildrenLoading({});
  }, [tableMode, groupBy, page, scenarioQty, includeShippingCharge, baseApiParams]);

  useEffect(() => {
    setJumpPageInput(String(page));
  }, [page]);

  function toTableRow(item: ProductProfitItem): TableRow {
    return {
      rowId: `${item.entity_type ?? "sku"}-${item.group_key ?? item.sku}-${item.marketplace_id}`,
      entity_type: item.entity_type,
      group_key: item.group_key ?? undefined,
      sku: item.sku,
      sample_sku: item.sample_sku ?? undefined,
      asin: item.asin ?? "",
      parent_asin: item.parent_asin ?? undefined,
      title: item.title ?? "",
      marketplace_id: item.marketplace_id,
      marketplace_code: item.marketplace_code,
      fulfillment_channel: item.fulfillment_channel,
      units: item.units,
      sku_count: item.sku_count,
      child_count: item.child_count,
      revenue_pln: item.revenue_pln,
      cogs_per_unit: item.cogs_per_unit,
      fees_per_unit: item.fees_per_unit,
      cm1_profit: item.cm1_profit,
      cm1_percent: item.cm1_percent,
      ads_cost_pln: item.ads_cost_pln,
      logistics_pln: item.logistics_pln,
      returns_net_pln: item.returns_net_pln,
      fba_storage_fee_pln: item.fba_storage_fee_pln,
      fba_aged_fee_pln: item.fba_aged_fee_pln,
      fba_removal_fee_pln: item.fba_removal_fee_pln,
      fba_liquidation_fee_pln: item.fba_liquidation_fee_pln,
      overhead_allocated_pln: item.overhead_allocated_pln,
      overhead_allocation_method: item.overhead_allocation_method,
      overhead_confidence_pct: item.overhead_confidence_pct,
      cm2_profit: item.cm2_profit,
      cm2_percent: item.cm2_percent,
      np_profit: item.np_profit,
      np_percent: item.np_percent,
      loss_orders_pct: item.loss_orders_pct,
      return_rate: item.return_rate,
      tacos: item.tacos,
      days_of_cover: item.days_of_cover,
      cogs_coverage_pct: item.cogs_coverage_pct,
      shipping_match_pct: item.shipping_match_pct,
      finance_match_pct: item.finance_match_pct,
      confidence_score: item.confidence_score,
      flags: item.flags ?? [],
    };
  }

  let rows: TableRow[] = (data?.items ?? []).map(toTableRow);

  rows = rows.filter((row) => {
    const selectedProfit = metricProfit(row, profitMode);
    const selectedPct = metricPct(row, profitMode);
    if (lossOnly && selectedProfit >= 0) return false;
    if (lowConfidence && row.confidence_score >= 70) return false;
    if (highReturns && (row.return_rate ?? 0) <= 8) return false;
    if (stockoutRisk && (row.days_of_cover ?? 999) >= 7) return false;
    if (adsHeavy && !((row.tacos ?? 0) > 15 && selectedPct < 10)) return false;
    return true;
  });

  let whatIfRows: ProductWhatIfItem[] = whatIfData?.items ?? [];
  whatIfRows = whatIfRows.filter((row) => {
    if (lowConfidence && row.confidence_score >= 70) return false;
    if (lossOnly && whatIfMetricProfit(row, profitMode) >= 0) return false;
    return true;
  });
  whatIfRows = [...whatIfRows].sort(
    (a, b) => whatIfMetricProfit(b, profitMode) - whatIfMetricProfit(a, profitMode)
  );

  const whatIfDiagnostics = useMemo(() => {
    let driftCount = 0;
    let gapSum = 0;
    let gapCount = 0;
    const mpStats = new Map<string, { sum: number; count: number; drift: number; total: number }>();

    for (const row of whatIfRows) {
      const mp = row.marketplace_code || row.marketplace_id || "-";
      if (!mpStats.has(mp)) {
        mpStats.set(mp, { sum: 0, count: 0, drift: 0, total: 0 });
      }
      const stats = mpStats.get(mp)!;
      stats.total += 1;

      if (row.execution_drift) {
        driftCount += 1;
        stats.drift += 1;
      }

      if (typeof row.logistics_gap_pct === "number" && Number.isFinite(row.logistics_gap_pct)) {
        gapSum += row.logistics_gap_pct;
        gapCount += 1;
        stats.sum += row.logistics_gap_pct;
        stats.count += 1;
      }
    }

    const byMarketplace = Array.from(mpStats.entries())
      .map(([marketplace, s]) => ({
        marketplace,
        avgGapPct: s.count > 0 ? s.sum / s.count : null,
        driftCount: s.drift,
        total: s.total,
      }))
      .sort((a, b) => Math.abs(b.avgGapPct ?? 0) - Math.abs(a.avgGapPct ?? 0));

    return {
      total: whatIfRows.length,
      driftCount,
      avgGapPct: gapCount > 0 ? gapSum / gapCount : null,
      byMarketplace,
    };
  }, [whatIfRows]);

  const rowPadding = rowDensity === "compact" ? "py-1.5" : "py-2";

  function toggleColumn(column: ColumnKey) {
    setVisibleColumns((prev) =>
      prev.includes(column) ? prev.filter((c) => c !== column) : [...prev, column]
    );
  }

  function handleSort(column: ColumnKey) {
    if (sortBy !== column) {
      setSortBy(column);
      setSortDir("desc");
      return;
    }
    setSortDir((d) => (d === "desc" ? "asc" : "desc"));
  }

  function goToPage(target: number, pages: number) {
    const safePages = Math.max(1, pages);
    const clamped = Math.max(1, Math.min(safePages, target));
    setPage(clamped);
  }

  function applyPageJump(pages: number) {
    const parsed = Number(jumpPageInput);
    if (!Number.isFinite(parsed) || parsed < 1) {
      setJumpPageInput(String(page));
      return;
    }
    goToPage(Math.trunc(parsed), pages);
  }

  function parentKey(row: ProductWhatIfItem): string {
    const parent = String(row.group_key || row.parent_asin || row.asin || row.sku || "").trim();
    const mp = String(row.marketplace_id || "__ALL__").trim();
    return `${parent}::${mp}`;
  }

  function realizedParentKey(row: TableRow): string {
    const parent = String(row.group_key || row.parent_asin || row.asin || row.sku || "").trim();
    const mp = String(row.marketplace_id || "__ALL__").trim();
    return `${parent}::${mp}`;
  }

  async function toggleParentChildren(row: ProductWhatIfItem) {
    if (row.entity_type !== "parent") return;
    const key = parentKey(row);
    const nextOpen = !expandedParents[key];

    if (!nextOpen) {
      setExpandedParents((prev) => ({ ...prev, [key]: false }));
      return;
    }

    setExpandedParents((prev) => ({ ...prev, [key]: true }));
    if (parentChildrenRows[key] || parentChildrenLoading[key]) return;

    const targetParentAsin = String(row.group_key || row.parent_asin || row.asin || "").trim();
    if (!targetParentAsin) return;

    setParentChildrenLoading((prev) => ({ ...prev, [key]: true }));
    try {
      const response = await getProductWhatIfTable({
        ...whatIfParams,
        parent_asin: targetParentAsin,
        group_by: "asin_marketplace",
        include_cost_components: true,
        page: 1,
        page_size: 200,
        sort_by:
          profitMode === "np"
            ? "np_profit"
            : profitMode === "cm2"
            ? "cm2_profit"
            : "cm1_profit",
        sort_dir: "desc",
      });
      const rows = (response.items ?? []).filter((item) => {
        const p = String(item.parent_asin || "").trim();
        return !p || p === targetParentAsin;
      });
      setParentChildrenRows((prev) => ({ ...prev, [key]: rows }));
    } catch (error) {
      console.error("parent children load failed", error);
      setParentChildrenRows((prev) => ({ ...prev, [key]: [] }));
    } finally {
      setParentChildrenLoading((prev) => ({ ...prev, [key]: false }));
    }
  }

  async function toggleRealizedParentChildren(row: TableRow) {
    if (row.entity_type !== "parent") return;
    const key = realizedParentKey(row);
    const nextOpen = !expandedRealizedParents[key];

    if (!nextOpen) {
      setExpandedRealizedParents((prev) => ({ ...prev, [key]: false }));
      return;
    }

    setExpandedRealizedParents((prev) => ({ ...prev, [key]: true }));
    if (realizedParentChildrenRows[key] || realizedParentChildrenLoading[key]) return;

    const targetParentAsin = String(row.group_key || row.parent_asin || row.asin || "").trim();
    if (!targetParentAsin) return;

    setRealizedParentChildrenLoading((prev) => ({ ...prev, [key]: true }));
    try {
      const response = await getProductProfitTable({
        ...realizedParams,
        parent_asin: targetParentAsin,
        group_by: "asin_marketplace",
        include_cost_components: true,
        page: 1,
        page_size: 200,
      });
      const childRows = (response.items ?? [])
        .filter((item) => {
          const p = String(item.parent_asin || "").trim();
          return !p || p === targetParentAsin;
        })
        .map(toTableRow);
      setRealizedParentChildrenRows((prev) => ({ ...prev, [key]: childRows }));
    } catch (error) {
      console.error("realized parent children load failed", error);
      setRealizedParentChildrenRows((prev) => ({ ...prev, [key]: [] }));
    } finally {
      setRealizedParentChildrenLoading((prev) => ({ ...prev, [key]: false }));
    }
  }

  function exportXlsx() {
    const columnMap: Record<ColumnKey, string> = {
      sku: "sku",
      asin: "asin",
      title: "title",
      marketplace: "marketplace_code",
      fulfillment: "fulfillment_channel",
      units: "units",
      revenue: "revenue_pln",
      cogsUnit: "cogs_per_unit",
      feesUnit: "fees_per_unit",
      cm1Profit: "cm1_profit",
      cm1Pct: "cm1_percent",
      adsCost: "ads_cost_pln",
      logisticsCost: "logistics_pln",
      returnsNet: "returns_net_pln",
      fbaStorage: "fba_storage_fee_pln",
      fbaAged: "fba_aged_fee_pln",
      fbaRemoval: "fba_removal_fee_pln",
      fbaLiquidation: "fba_liquidation_fee_pln",
      overhead: "overhead_allocated_pln",
      cm2Profit: "cm2_profit",
      cm2Pct: "cm2_percent",
      npProfit: "np_profit",
      npPct: "np_percent",
      lossOrders: "loss_orders_pct",
      returnRate: "return_rate",
      tacos: "tacos",
      daysCover: "days_of_cover",
      cogsCoverage: "cogs_coverage_pct",
      shippingMatch: "shipping_match_pct",
      financeMatch: "finance_match_pct",
      confidence: "confidence_score",
      flags: "flags",
      actions: "actions",
    };
    const exportCols = visibleColumns
      .map((c) => columnMap[c])
      .filter((c) => c && c !== "actions");

    exportProductProfitXlsx({
      ...realizedParams,
      columns: exportCols.join(","),
    });
  }

  async function createTask(taskType: "pricing" | "content" | "watchlist", row: TableRow) {
    const taskSku = row.entity_type === "sku" ? row.sku : (row.sample_sku || row.sku);
    try {
      await createProductTask({
        task_type: taskType,
        sku: taskSku,
        marketplace_id: row.marketplace_id === "__ALL__" ? undefined : row.marketplace_id,
        source_page: "product_profit",
      });
      window.alert(`Task created: ${taskType} for ${taskSku}`);
    } catch {
      window.alert(`Failed to create ${taskType} task for ${taskSku}`);
    }
  }

  const whatIfSummary = whatIfData?.summary;

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold">Product Profit Table</h1>
        <p className="text-sm text-muted-foreground">
          ASIN-first profitability with parent rollups, CM1/CM2/NP and confidence controls
        </p>
      </div>

      <DataWarningBanner warnings={data?.warnings} />

      {isError && tableMode === "realized" && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">Nie udało się załadować danych produktowych. Spróbuj ponownie później.</p>
        </div>
      )}

      <div className="flex flex-wrap items-center gap-2">
        <div className="flex items-center gap-1 rounded-lg border border-border bg-card p-1">
          <button
            onClick={() => {
              setTableMode("realized");
              setPage(1);
            }}
            className={cn(
              "rounded px-2 py-1 text-xs",
              tableMode === "realized" ? "bg-amazon text-black" : "text-muted-foreground"
            )}
          >
            Realized
          </button>
          <button
            onClick={() => {
              setTableMode("what_if");
              setPage(1);
            }}
            className={cn(
              "rounded px-2 py-1 text-xs",
              tableMode === "what_if" ? "bg-amazon text-black" : "text-muted-foreground"
            )}
          >
            What-if
          </button>
        </div>

        <div className="flex items-center gap-1 rounded-lg border border-border bg-card p-1">
          {[
            { key: "asin_marketplace", label: "ASIN + MP" },
            { key: "asin", label: "ASIN global" },
            { key: "parent_marketplace", label: "Parent + MP" },
            { key: "parent", label: "Parent global" },
            { key: "sku_marketplace", label: "SKU + MP" },
            { key: "sku", label: "SKU global" },
          ].map((mode) => (
            <button
              key={mode.key}
              onClick={() => {
                setGroupBy(mode.key as GroupByMode);
                setPage(1);
              }}
              className={cn(
                "rounded px-2 py-1 text-xs",
                groupBy === mode.key ? "bg-amazon text-black" : "text-muted-foreground"
              )}
            >
              {mode.label}
            </button>
          ))}
        </div>

        {tableMode === "realized" && (
          <>
            <button
              onClick={() => setShowColumns((v) => !v)}
              className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs"
            >
              <Settings2 className="h-3.5 w-3.5" />
              Columns
            </button>
            <button
              onClick={exportXlsx}
              className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs"
            >
              <Download className="h-3.5 w-3.5" />
              Export XLSX
            </button>
          </>
        )}

        {tableMode === "what_if" && (
          <div className="inline-flex items-center gap-2 rounded border border-border px-2 py-1 text-xs">
            <span className="text-muted-foreground">Scenariusz qty:</span>
            <input
              type="number"
              min={1}
              max={200}
              value={scenarioQty}
              onChange={(e) => setScenarioQty(Math.max(1, Math.min(200, Number(e.target.value) || 1)))}
              className="w-16 rounded border border-input bg-background px-1 py-0.5 text-right outline-none focus:ring-1 focus:ring-ring"
            />
            <label className="inline-flex items-center gap-1">
              <input
                type="checkbox"
                checked={includeShippingCharge}
                onChange={(e) => setIncludeShippingCharge(e.target.checked)}
              />
              dolicz ShippingCharge
            </label>
          </div>
        )}
        <span className="ml-auto text-xs text-muted-foreground">
          {tableMode === "realized"
            ? `Profit mode: ${profitMode.toUpperCase()} | Confidence >= ${filters.confidenceMin}% | MP scope: ${inlineMarketplaceCode ? `local ${inlineMarketplaceCode}` : filters.marketplaceIds.length ? `(${filters.marketplaceIds.length})` : "all"}`
            : `What-if (offer/asin/parent) | Confidence >= ${filters.confidenceMin}% | MP scope: ${inlineMarketplaceCode ? `local ${inlineMarketplaceCode}` : filters.marketplaceIds.length ? `(${filters.marketplaceIds.length})` : "all"}`}
        </span>
      </div>

      {tableMode === "realized" && showColumns && (
        <div className="rounded-lg border border-border bg-card p-3">
          <div className="mb-2 text-xs font-semibold text-muted-foreground">Column chooser</div>
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
            {ALL_COLUMNS.map((col) => (
              <label key={col.key} className="inline-flex items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  checked={visibleColumns.includes(col.key)}
                  onChange={() => toggleColumn(col.key)}
                />
                {col.label}
              </label>
            ))}
          </div>
        </div>
      )}

      {/* Inline search + marketplace + quick filters */}
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground/50" />
          <input
            type="text"
            placeholder="Szukaj SKU / ASIN / tytuł (server)..."
            value={inlineSearch}
            onChange={(e) => setInlineSearch(e.target.value)}
            className="rounded-md border border-input bg-background pl-8 pr-7 py-1.5 text-xs outline-none focus:ring-1 focus:ring-ring w-56"
          />
          {inlineSearch && (
            <button onClick={() => setInlineSearch("")} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground/50 hover:text-foreground">
              <X className="h-3 w-3" />
            </button>
          )}
        </div>
        <select
          value={inlineMarketplace}
          onChange={(e) => setInlineMarketplace(e.target.value)}
          className="rounded-md border border-input bg-background px-2 py-1.5 text-xs outline-none focus:ring-1 focus:ring-ring"
        >
          {marketplaceOptions.map((mp) => (
            <option key={mp.id} value={mp.id}>{mp.label}</option>
          ))}
        </select>
        {inlineMarketplaceCode && (
          <span className="text-[11px] text-amber-300">
            Lokalny MP aktywny: {inlineMarketplaceCode} (nadpisuje Global Filters)
          </span>
        )}

        <span className="mx-1 h-4 w-px bg-border" />

        <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
          <Filter className="h-3.5 w-3.5" /> Quick filters
        </span>
        <button onClick={() => setLossOnly((v) => !v)} className={cn("rounded px-2 py-1 text-xs border border-border", lossOnly && "bg-red-500/10 text-red-400")}>Loss-making only</button>
        <button onClick={() => setLowConfidence((v) => !v)} className={cn("rounded px-2 py-1 text-xs border border-border", lowConfidence && "bg-yellow-500/10 text-yellow-400")}>Low confidence</button>
        {tableMode === "realized" && (
          <>
            <button onClick={() => setHighReturns((v) => !v)} className={cn("rounded px-2 py-1 text-xs border border-border", highReturns && "bg-yellow-500/10 text-yellow-400")}>High returns</button>
            <button onClick={() => setStockoutRisk((v) => !v)} className={cn("rounded px-2 py-1 text-xs border border-border", stockoutRisk && "bg-orange-500/10 text-orange-400")}>Stockout risk</button>
            <button onClick={() => setAdsHeavy((v) => !v)} className={cn("rounded px-2 py-1 text-xs border border-border", adsHeavy && "bg-purple-500/10 text-purple-400")}>Ads heavy</button>
          </>
        )}
      </div>

      {tableMode === "realized" && (groupBy === "sku" || groupBy === "sku_marketplace") && (
        <div className="rounded border border-yellow-500/30 bg-yellow-500/5 px-3 py-2 text-xs text-yellow-300">
          Tryb SKU służy do debugu operacyjnego. Decyzje marżowe/Ads traktuj jako kanoniczne w trybie ASIN lub Parent.
        </div>
      )}

      {/* Refund exclusion banner */}
      {tableMode === "realized" && (data?.summary?.refund_orders_excluded ?? 0) > 0 && (
        <div className="rounded border border-orange-500/30 bg-orange-500/5 px-3 py-2 text-xs text-orange-300 flex items-center gap-2">
          <span className="font-semibold">⚠ Refundy wyłączone z kalkulacji:</span>
          <span>{data!.summary.refund_orders_excluded!.toLocaleString()} zamówień</span>
          <span className="text-orange-400/70">
            (pełne: {data!.summary.refund_full_count ?? 0}, częściowe: {data!.summary.refund_partial_count ?? 0})
          </span>
          {(data!.summary.refund_total_pln ?? 0) !== 0 && (
            <span className="text-orange-400/70">
              | Wartość zwrotów: {formatPLN(Math.abs(data!.summary.refund_total_pln ?? 0))}
            </span>
          )}
        </div>
      )}

      {tableMode === "what_if" && whatIfSummary && (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
          {(() => {
            const selectedMode = profitMode;
            const selectedProfit =
              selectedMode === "np"
                ? (whatIfSummary.total_np_pln ?? whatIfSummary.total_cm2_pln)
                : selectedMode === "cm2"
                  ? whatIfSummary.total_cm2_pln
                  : whatIfSummary.total_cm1_pln;
            const selectedPct =
              selectedMode === "np"
                ? (whatIfSummary.total_np_pct ?? whatIfSummary.total_cm2_pct)
                : selectedMode === "cm2"
                  ? whatIfSummary.total_cm2_pct
                  : whatIfSummary.total_cm1_pct;
            return (
          <div className="rounded border border-border bg-card p-3">
                <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
                  {selectedMode.toUpperCase()} (scenariusz)
                </div>
                <div className={cn("mt-1 text-xl font-semibold", selectedProfit < 0 ? "text-red-400" : "text-green-400")}>
                  {formatPLN(selectedProfit)}
                </div>
                <div className="text-xs text-muted-foreground">{formatPct(selectedPct)}</div>
              </div>
            );
          })()}
          <div className="rounded border border-border bg-card p-3">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">CM2 / NP</div>
            <div className={cn("mt-1 text-xl font-semibold", (whatIfSummary.total_np_pln ?? whatIfSummary.total_cm2_pln) < 0 ? "text-red-400" : "text-green-400")}>
              {formatPLN(whatIfSummary.total_cm2_pln)} / {formatPLN(whatIfSummary.total_np_pln ?? whatIfSummary.total_cm2_pln)}
            </div>
            <div className="text-xs text-muted-foreground">
              {formatPct(whatIfSummary.total_cm2_pct)} / {formatPct(whatIfSummary.total_np_pct ?? whatIfSummary.total_cm2_pct)}
            </div>
          </div>
          <div className="rounded border border-border bg-card p-3">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Przychód (scen.)</div>
            <div className="mt-1 text-xl font-semibold text-foreground">{formatPLN(whatIfSummary.total_revenue_pln)}</div>
            <div className="text-xs text-muted-foreground">
              ShippingCharge: {formatPLN(whatIfSummary.total_shipping_charge_pln)}
            </div>
          </div>
          <div className="rounded border border-border bg-card p-3">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Koszty</div>
            <div className="mt-1 text-xl font-semibold text-foreground">
              {formatPLN(
                whatIfSummary.total_cogs_pln
                + whatIfSummary.total_fees_pln
                + whatIfSummary.total_logistics_pln
                + whatIfSummary.total_ads_pln
                + (whatIfSummary.total_returns_net_pln ?? 0)
                + (whatIfSummary.total_fba_storage_fee_pln ?? 0)
                + (whatIfSummary.total_fba_aged_fee_pln ?? 0)
                + (whatIfSummary.total_fba_removal_fee_pln ?? 0)
                + (whatIfSummary.total_fba_liquidation_fee_pln ?? 0)
                + (whatIfSummary.total_overhead_allocated_pln ?? 0)
              )}
            </div>
            <div className="text-xs text-muted-foreground">
              COGS {formatPLN(whatIfSummary.total_cogs_pln)} | Fees {formatPLN(whatIfSummary.total_fees_pln)}
            </div>
            <div className="text-[10px] text-muted-foreground">
              Returns {formatPLN(whatIfSummary.total_returns_net_pln ?? 0)}
              {" · "}Storage {formatPLN(whatIfSummary.total_fba_storage_fee_pln ?? 0)}
              {" · "}OH {formatPLN(whatIfSummary.total_overhead_allocated_pln ?? 0)}
            </div>
          </div>
          <div className="rounded border border-border bg-card p-3">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Coverage</div>
            <div className="mt-1 text-xl font-semibold text-foreground">{(whatIfSummary.avg_confidence ?? 0).toFixed(1)}%</div>
            <div className="text-xs text-muted-foreground">
              Oferty: {whatIfSummary.total_offers} (scope: {whatIfSummary.summary_scope})
            </div>
          </div>
          <div className="rounded border border-border bg-card p-3">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Execution drift</div>
            <div className={cn("mt-1 text-xl font-semibold", whatIfDiagnostics.driftCount > 0 ? "text-red-400" : "text-green-400")}>
              {whatIfDiagnostics.driftCount}
            </div>
            <div className="text-xs text-muted-foreground">
              {whatIfDiagnostics.total > 0
                ? `${((whatIfDiagnostics.driftCount / whatIfDiagnostics.total) * 100).toFixed(1)}% ofert (scope: page)`
                : "0.0% ofert (scope: page)"}
            </div>
          </div>
          <div className="rounded border border-border bg-card p-3">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Średni gap% MP</div>
            <div className="mt-1 text-xl font-semibold text-foreground">
              {whatIfDiagnostics.avgGapPct != null ? formatPct(whatIfDiagnostics.avgGapPct) : "-"}
            </div>
            <div className="mt-1 space-y-0.5 text-[10px] text-muted-foreground">
              {whatIfDiagnostics.byMarketplace.slice(0, 4).map((item) => (
                <div key={item.marketplace} className="flex items-center justify-between">
                  <span>{item.marketplace}</span>
                  <span>{item.avgGapPct != null ? formatPct(item.avgGapPct) : "-"}</span>
                </div>
              ))}
              {whatIfDiagnostics.byMarketplace.length === 0 && <div>brak danych (scope: page)</div>}
            </div>
          </div>
        </div>
      )}

      {tableMode === "realized" ? (
      <div className="rounded-xl border border-border bg-card overflow-hidden flex flex-col" style={{ maxHeight: "calc(100vh - 280px)" }}>
        <div className="overflow-auto flex-1">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground sticky top-0 z-20">
              <tr>
                {ALL_COLUMNS.filter((c) => visibleColumns.includes(c.key)).map((col) => (
                  <th
                    key={col.key}
                    className={cn(
                      "px-2 py-2 whitespace-nowrap bg-card",
                      col.align === "right" && "text-right",
                      col.key === "sku" && "sticky left-0 z-30 bg-[hsl(var(--card))]",
                      col.key === "title" && "min-w-[200px]",
                      col.key === "actions" && "sticky right-0 z-30 bg-[hsl(var(--card))]"
                    )}
                  >
                    <button
                      onClick={() => handleSort(col.key)}
                      className={cn(
                        "inline-flex items-center gap-1",
                        col.align === "right" && "ml-auto",
                        !apiSortMap[col.key] && groupBy !== "sku" && "cursor-default"
                      )}
                      disabled={!apiSortMap[col.key] && groupBy !== "sku"}
                    >
                      {col.label}
                      {sortBy === col.key && <span>{sortDir === "desc" ? "▼" : "▲"}</span>}
                    </button>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {isLoading && (
                <tr>
                  <td colSpan={visibleColumns.length} className="px-2 py-12 text-center text-muted-foreground">
                    Ładowanie...
                  </td>
                </tr>
              )}
              {!isLoading && !isError && rows.length === 0 && (
                <tr>
                  <td colSpan={visibleColumns.length} className="px-2 py-12 text-center text-muted-foreground">
                    Brak produktów pasujących do filtrów
                  </td>
                </tr>
              )}
              {rows.map((row) => {
                const selectedProfit = metricProfit(row, profitMode);
                const selectedPct = metricPct(row, profitMode);
                const isMenuOpen = actionMenuRow === row.rowId;
                const isParent = row.entity_type === "parent";
                const parentRowId = realizedParentKey(row);
                const isExpanded = !!expandedRealizedParents[parentRowId];
                const isLoadingChildren = !!realizedParentChildrenLoading[parentRowId];
                const children = realizedParentChildrenRows[parentRowId] ?? [];
                const parentAsinLabel = String(row.group_key || row.parent_asin || row.asin || row.sku || "");
                return (
                  <Fragment key={row.rowId}>
                  <tr className="hover:bg-muted/20 transition-colors">
                    {visibleColumns.includes("sku") && (
                      <td className={cn("px-2 font-mono font-medium whitespace-nowrap sticky left-0 z-10 bg-card", rowPadding)}>
                        {isParent ? (
                          <button
                            type="button"
                            onClick={() => void toggleRealizedParentChildren(row)}
                            className="mb-1 inline-flex items-center gap-1 rounded border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground hover:bg-muted/30"
                          >
                            <span>{isExpanded ? "▼" : "▶"}</span>
                            <span>Parent</span>
                          </button>
                        ) : null}
                        <div>{row.sku}</div>
                        {row.sample_sku && row.sample_sku !== row.sku ? (
                          <div className="text-[10px] text-muted-foreground">sample SKU: {row.sample_sku}</div>
                        ) : null}
                        {isParent && row.child_count ? (
                          <div className="text-[10px] text-muted-foreground">{row.child_count} child ASIN</div>
                        ) : null}
                      </td>
                    )}
                    {visibleColumns.includes("asin") && <td className={cn("px-2 whitespace-nowrap", rowPadding)}>{row.asin || "-"}</td>}
                    {visibleColumns.includes("title") && (
                      <td className={cn("px-2 max-w-[280px] truncate", rowPadding)} title={row.title || ""}>
                        {row.title || <span className="text-muted-foreground/40">—</span>}
                      </td>
                    )}
                    {visibleColumns.includes("marketplace") && <td className={cn("px-2", rowPadding)}>{row.marketplace_code}</td>}
                    {visibleColumns.includes("fulfillment") && <td className={cn("px-2", rowPadding)}>{row.fulfillment_channel}</td>}
                    {visibleColumns.includes("units") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.units.toLocaleString()}</td>}
                    {visibleColumns.includes("revenue") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{formatPLN(row.revenue_pln)}</td>}
                    {visibleColumns.includes("cogsUnit") && <td className={cn("px-2 text-right tabular-nums text-muted-foreground", rowPadding)}>{formatPLN(row.cogs_per_unit)}</td>}
                    {visibleColumns.includes("feesUnit") && <td className={cn("px-2 text-right tabular-nums text-muted-foreground", rowPadding)}>{formatPLN(row.fees_per_unit)}</td>}
                    {visibleColumns.includes("cm1Profit") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{formatPLN(row.cm1_profit)}</td>}
                    {visibleColumns.includes("cm1Pct") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{formatPct(row.cm1_percent)}</td>}
                    {visibleColumns.includes("adsCost") && <td className={cn("px-2 text-right tabular-nums text-orange-400", rowPadding)}>{row.ads_cost_pln != null ? formatPLN(row.ads_cost_pln) : "-"}</td>}
                    {visibleColumns.includes("logisticsCost") && <td className={cn("px-2 text-right tabular-nums text-cyan-300", rowPadding)}>{row.logistics_pln != null ? formatPLN(row.logistics_pln) : "-"}</td>}
                    {visibleColumns.includes("returnsNet") && <td className={cn("px-2 text-right tabular-nums text-red-300", rowPadding)}>{row.returns_net_pln != null ? formatPLN(row.returns_net_pln) : "-"}</td>}
                    {visibleColumns.includes("fbaStorage") && <td className={cn("px-2 text-right tabular-nums text-amber-300", rowPadding)}>{row.fba_storage_fee_pln != null ? formatPLN(row.fba_storage_fee_pln) : "-"}</td>}
                    {visibleColumns.includes("fbaAged") && <td className={cn("px-2 text-right tabular-nums text-amber-300", rowPadding)}>{row.fba_aged_fee_pln != null ? formatPLN(row.fba_aged_fee_pln) : "-"}</td>}
                    {visibleColumns.includes("fbaRemoval") && <td className={cn("px-2 text-right tabular-nums text-amber-300", rowPadding)}>{row.fba_removal_fee_pln != null ? formatPLN(row.fba_removal_fee_pln) : "-"}</td>}
                    {visibleColumns.includes("fbaLiquidation") && <td className={cn("px-2 text-right tabular-nums text-amber-300", rowPadding)}>{row.fba_liquidation_fee_pln != null ? formatPLN(row.fba_liquidation_fee_pln) : "-"}</td>}
                    {visibleColumns.includes("overhead") && (
                      <td className={cn("px-2 text-right tabular-nums text-purple-300", rowPadding)}>
                        {row.overhead_allocated_pln != null ? formatPLN(row.overhead_allocated_pln) : "-"}
                        {row.overhead_allocation_method ? (
                          <div className="text-[10px] text-muted-foreground">
                            {row.overhead_allocation_method}
                            {typeof row.overhead_confidence_pct === "number" ? ` · ${row.overhead_confidence_pct.toFixed(0)}%` : ""}
                          </div>
                        ) : null}
                      </td>
                    )}
                    {visibleColumns.includes("cm2Profit") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.cm2_profit != null ? formatPLN(row.cm2_profit) : "-"}</td>}
                    {visibleColumns.includes("cm2Pct") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.cm2_percent != null ? formatPct(row.cm2_percent) : "-"}</td>}
                    {visibleColumns.includes("npProfit") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.np_profit != null ? formatPLN(row.np_profit) : "-"}</td>}
                    {visibleColumns.includes("npPct") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.np_percent != null ? formatPct(row.np_percent) : "-"}</td>}
                    {visibleColumns.includes("lossOrders") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{formatPct(row.loss_orders_pct)}</td>}
                    {visibleColumns.includes("returnRate") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.return_rate != null ? formatPct(row.return_rate) : "-"}</td>}
                    {visibleColumns.includes("tacos") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.tacos != null ? formatPct(row.tacos) : "-"}</td>}
                    {visibleColumns.includes("daysCover") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.days_of_cover != null ? row.days_of_cover.toFixed(1) : "-"}</td>}
                    {visibleColumns.includes("cogsCoverage") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{formatPct(row.cogs_coverage_pct)}</td>}
                    {visibleColumns.includes("shippingMatch") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.shipping_match_pct != null ? formatPct(row.shipping_match_pct) : "-"}</td>}
                    {visibleColumns.includes("financeMatch") && <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.finance_match_pct != null ? formatPct(row.finance_match_pct) : "-"}</td>}
                    {visibleColumns.includes("confidence") && (
                      <td className={cn("px-2 text-right whitespace-nowrap", rowPadding)}>
                        <span className="mr-1 tabular-nums">{(row.confidence_score ?? 0).toFixed(0)}%</span>
                        <span className={cn("rounded-full px-1.5 py-0.5 text-[10px]", confidenceLabel(row) === "Actual" ? "bg-green-500/10 text-green-400" : confidenceLabel(row) === "Allocated" ? "bg-blue-500/10 text-blue-400" : "bg-yellow-500/10 text-yellow-400")}>
                          {confidenceLabel(row)}
                        </span>
                      </td>
                    )}
                    {visibleColumns.includes("flags") && (
                      <td className={cn("px-2", rowPadding)}>
                        {row.flags.length ? (
                          <span className="rounded-full bg-muted px-2 py-0.5 text-[10px]">{row.flags.join(", ")}</span>
                        ) : (
                          <span className="text-muted-foreground">-</span>
                        )}
                      </td>
                    )}
                    {visibleColumns.includes("actions") && (
                      <td className={cn("px-2 sticky right-0 z-10 bg-card", rowPadding)}>
                        <div className="relative flex items-center justify-end gap-1">
                          <span className={cn("rounded-full px-2 py-0.5 text-[10px]", profitBadge(selectedPct))}>
                            {selectedProfit >= 0 ? "+" : "−"}
                          </span>
                          <button
                            onClick={(e) => { e.stopPropagation(); setActionMenuRow(isMenuOpen ? null : row.rowId); }}
                            className="rounded p-1 hover:bg-muted text-muted-foreground"
                            title="Akcje"
                          >
                            <MoreHorizontal className="h-3.5 w-3.5" />
                          </button>
                          {isMenuOpen && (
                            <div
                              ref={actionMenuRef}
                              className="absolute right-0 top-full mt-1 z-50 bg-card border border-border rounded-lg shadow-lg py-1 min-w-[170px]"
                              onClick={(e) => e.stopPropagation()}
                            >
                              {/* ---- Navigation ---- */}
                              <button
                                onClick={() => {
                                  const drillSku = row.entity_type === "sku" ? row.sku : (row.sample_sku || row.sku);
                                  navigate(`/profit/drilldown?sku=${encodeURIComponent(drillSku)}`);
                                  setActionMenuRow(null);
                                }}
                                className="w-full text-left px-3 py-1.5 text-xs hover:bg-muted/50 transition-colors"
                              >
                                📊 Drilldown
                              </button>
                              <button
                                onClick={() => {
                                  const feeSku = row.entity_type === "sku" ? row.sku : (row.sample_sku || row.sku);
                                  navigate(`/profit/fee-breakdown?sku=${encodeURIComponent(feeSku)}${row.marketplace_id ? `&marketplace_id=${encodeURIComponent(row.marketplace_id)}` : ""}`);
                                  setActionMenuRow(null);
                                }}
                                className="w-full text-left px-3 py-1.5 text-xs hover:bg-muted/50 transition-colors"
                              >
                                💰 Fee Breakdown
                              </button>
                              {row.asin && row.marketplace_code && (
                                <button
                                  onClick={() => {
                                    const domain: Record<string, string> = { DE: "de", FR: "fr", IT: "it", ES: "es", NL: "nl", PL: "pl", SE: "se", BE: "com.be", UK: "co.uk" };
                                    const tld = domain[row.marketplace_code.toUpperCase()] ?? "de";
                                    window.open(`https://www.amazon.${tld}/dp/${row.asin}`, "_blank");
                                    setActionMenuRow(null);
                                  }}
                                  className="w-full text-left px-3 py-1.5 text-xs hover:bg-muted/50 transition-colors"
                                >
                                  🔗 Amazon listing
                                </button>
                              )}
                              <button
                                onClick={() => {
                                  const contentSku = row.entity_type === "sku" ? row.sku : (row.sample_sku || row.sku);
                                  navigate(`/content/studio?tab=editor&sku=${encodeURIComponent(contentSku)}&marketplace=${row.marketplace_code || "DE"}`);
                                  setActionMenuRow(null);
                                }}
                                className="w-full text-left px-3 py-1.5 text-xs hover:bg-muted/50 transition-colors"
                              >
                                📝 Content Studio
                              </button>
                              <button
                                onClick={() => {
                                  const pricingSku = row.entity_type === "sku" ? row.sku : (row.sample_sku || row.sku);
                                  navigate(`/pricing?sku=${encodeURIComponent(pricingSku)}`);
                                  setActionMenuRow(null);
                                }}
                                className="w-full text-left px-3 py-1.5 text-xs hover:bg-muted/50 transition-colors"
                              >
                                💰 Pricing
                              </button>

                              {/* ---- Clipboard ---- */}
                              <div className="my-1 border-t border-border" />
                              <button
                                onClick={() => {
                                  const skuToCopy = row.entity_type === "sku" ? row.sku : (row.sample_sku || row.sku);
                                  navigator.clipboard.writeText(skuToCopy);
                                  setActionMenuRow(null);
                                }}
                                className="w-full text-left px-3 py-1.5 text-xs hover:bg-muted/50 transition-colors"
                              >
                                📋 Kopiuj SKU
                              </button>
                              {row.asin && (
                                <button
                                  onClick={() => { navigator.clipboard.writeText(row.asin); setActionMenuRow(null); }}
                                  className="w-full text-left px-3 py-1.5 text-xs hover:bg-muted/50 transition-colors"
                                >
                                  📋 Kopiuj ASIN
                                </button>
                              )}

                              {/* ---- Tasks ---- */}
                              <div className="my-1 border-t border-border" />
                              <div className="px-3 py-1 text-[9px] uppercase tracking-wider text-muted-foreground">Utwórz task</div>
                              <button
                                onClick={() => { createTask("pricing", row); setActionMenuRow(null); }}
                                className="w-full text-left px-3 py-1.5 text-xs hover:bg-muted/50 transition-colors"
                              >
                                🏷️ Pricing task
                              </button>
                              <button
                                onClick={() => { createTask("content", row); setActionMenuRow(null); }}
                                className="w-full text-left px-3 py-1.5 text-xs hover:bg-muted/50 transition-colors"
                              >
                                ✏️ Content task
                              </button>
                              <button
                                onClick={() => { createTask("watchlist", row); setActionMenuRow(null); }}
                                className="w-full text-left px-3 py-1.5 text-xs hover:bg-muted/50 transition-colors"
                              >
                                👁 Watchlist
                              </button>
                            </div>
                          )}
                        </div>
                      </td>
                    )}
                  </tr>
                  {isParent && isExpanded && (
                    <tr className="bg-muted/10">
                      <td colSpan={visibleColumns.length} className="px-3 py-2">
                        <div className="rounded border border-border/50 bg-background/40 p-2">
                          <div className="mb-2 text-[10px] uppercase tracking-wider text-muted-foreground">
                            Child rows for parent {parentAsinLabel}
                          </div>
                          {isLoadingChildren ? (
                            <div className="text-xs text-muted-foreground">Ładowanie childów...</div>
                          ) : children.length === 0 ? (
                            <div className="text-xs text-muted-foreground">Brak childów dla tego parenta.</div>
                          ) : (
                            <div className="space-y-1">
                              {children.map((child, idx) => (
                                <div key={`${parentRowId}-${child.rowId}-${idx}`} className="flex items-center justify-between gap-3 rounded border border-border/40 px-2 py-1 text-xs">
                                  <div className="min-w-0">
                                    <div className="font-mono text-foreground">
                                      {child.asin || child.sku} <span className="text-muted-foreground">({child.marketplace_code})</span>
                                    </div>
                                    <div className="truncate text-muted-foreground">{child.title || "-"}</div>
                                  </div>
                                  <div className="shrink-0 text-right tabular-nums">
                                    <div>Rev {formatPLN(child.revenue_pln)}</div>
                                    <div className="text-muted-foreground">CM1 {formatPLN(child.cm1_profit)} | CM2 {formatPLN(child.cm2_profit ?? child.cm1_profit)} | NP {formatPLN(child.np_profit ?? child.cm2_profit ?? child.cm1_profit)}</div>
                                  </div>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      </td>
                    </tr>
                  )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>

        {data && (
          <div className="flex items-center justify-between border-t border-border px-4 py-3 shrink-0">
            <span className="text-xs text-muted-foreground">
              Strona {data.page} z {Math.max(1, data.pages)} | {data.total.toLocaleString()} wierszy
              {" • "}filtrowanie i paginacja po stronie serwera
            </span>
            <div className="flex items-center gap-1">
              <button
                disabled={page <= 1}
                onClick={() => goToPage(1, data.pages)}
                className="rounded px-2 py-1 text-xs text-muted-foreground hover:bg-muted disabled:opacity-30"
                title="Pierwsza strona"
              >
                «
              </button>
              <button
                disabled={page <= 1}
                onClick={() => goToPage(page - 1, data.pages)}
                className="rounded p-1 text-muted-foreground hover:bg-muted disabled:opacity-30"
                title="Poprzednia strona"
              >
                <ChevronLeft className="h-4 w-4" />
              </button>
              <input
                value={jumpPageInput}
                onChange={(e) => setJumpPageInput(e.target.value.replace(/[^0-9]/g, ""))}
                onKeyDown={(e) => {
                  if (e.key === "Enter") applyPageJump(data.pages);
                }}
                className="h-7 w-12 rounded border border-input bg-background px-1 text-center text-xs outline-none focus:ring-1 focus:ring-ring"
                title="Przejdź do strony"
              />
              <button
                disabled={page >= data.pages}
                onClick={() => goToPage(page + 1, data.pages)}
                className="rounded p-1 text-muted-foreground hover:bg-muted disabled:opacity-30"
                title="Następna strona"
              >
                <ChevronRight className="h-4 w-4" />
              </button>
              <button
                disabled={page >= data.pages}
                onClick={() => goToPage(data.pages, data.pages)}
                className="rounded px-2 py-1 text-xs text-muted-foreground hover:bg-muted disabled:opacity-30"
                title="Ostatnia strona"
              >
                »
              </button>
            </div>
          </div>
        )}
      </div>
      ) : (
        <div className="rounded-xl border border-border bg-card overflow-hidden flex flex-col" style={{ maxHeight: "calc(100vh - 280px)" }}>
          <div className="overflow-auto flex-1">
            <table className="w-full text-[11px]">
              <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground sticky top-0 z-20 bg-card">
                <tr>
                  <th className="px-2 py-2">Group / ASIN</th>
                  <th className="px-2 py-2">Tytuł</th>
                  <th className="px-2 py-2">MP</th>
                  <th className="px-2 py-2">FC</th>
                  <th className="px-2 py-2 text-right">Cena oferty</th>
                  <th className="px-2 py-2 text-right">Qty</th>
                  <th className="px-2 py-2 text-right">Szt./paczka</th>
                  <th className="px-2 py-2 text-right">Paczki</th>
                  <th className="px-2 py-2 text-right">Revenue (scen.)</th>
                  <th className="px-2 py-2 text-right">COGS</th>
                  <th className="px-2 py-2 text-right">ShippingCharge</th>
                  <th className="px-2 py-2 text-right">Plan (TKL)</th>
                  <th className="px-2 py-2 text-right">Observed (hist.)</th>
                  <th className="px-2 py-2 text-right">Decision</th>
                  <th className="px-2 py-2 text-right">Gap %</th>
                  <th className="px-2 py-2 text-right">Fees</th>
                  <th className="px-2 py-2 text-right">Returns</th>
                  <th className="px-2 py-2 text-right">FBA Storage</th>
                  <th className="px-2 py-2 text-right">FBA Aged</th>
                  <th className="px-2 py-2 text-right">FBA Removal</th>
                  <th className="px-2 py-2 text-right">FBA Liquidation</th>
                  <th className="px-2 py-2 text-right">Overhead</th>
                  <th className="px-2 py-2 text-right">CM1</th>
                  <th className="px-2 py-2 text-right">CM2</th>
                  <th className="px-2 py-2 text-right">CM2 %</th>
                  <th className="px-2 py-2 text-right">NP</th>
                  <th className="px-2 py-2 text-right">NP %</th>
                  <th className="px-2 py-2 text-right">Confidence</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {isLoadingWhatIf && (
                  <tr>
                    <td colSpan={28} className="px-2 py-12 text-center text-muted-foreground">
                      Ładowanie scenariusza...
                    </td>
                  </tr>
                )}
                {!isLoadingWhatIf && whatIfRows.length === 0 && (
                  <tr>
                    <td colSpan={28} className="px-2 py-12 text-center text-muted-foreground">
                      Brak ofert pasujących do filtrów / scenariusza
                      {inlineMarketplaceCode ? (
                        <div className="mt-2 text-[11px] text-amber-300">
                          What-if działa na aktywnych ofertach (`acc_offer`). Dla MP {inlineMarketplaceCode} brak aktualnych listingów w `acc_offer` albo filtr jest zbyt wąski.
                        </div>
                      ) : null}
                    </td>
                  </tr>
                )}
                {!isLoadingWhatIf && whatIfRows.map((row) => {
                  const isParent = row.entity_type === "parent";
                  const parentRowId = parentKey(row);
                  const isExpanded = !!expandedParents[parentRowId];
                  const isLoadingChildren = !!parentChildrenLoading[parentRowId];
                  const children = parentChildrenRows[parentRowId] ?? [];
                  const parentAsinLabel = String(row.group_key || row.parent_asin || row.asin || row.sku || "");

                  return (
                    <Fragment key={`${row.sku}-${row.marketplace_id}`}>
                      <tr className="hover:bg-muted/20 transition-colors">
                        <td className={cn("px-2 font-mono", rowPadding)}>
                          {isParent ? (
                            <button
                              type="button"
                              onClick={() => void toggleParentChildren(row)}
                              className="mb-1 inline-flex items-center gap-1 rounded border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground hover:bg-muted/30"
                            >
                              <span>{isExpanded ? "▼" : "▶"}</span>
                              <span>Parent</span>
                            </button>
                          ) : null}
                          <div>{row.sku}</div>
                          <div className="text-muted-foreground">{row.asin || "-"}</div>
                          {row.parent_asin ? <div className="text-[10px] text-muted-foreground">parent: {row.parent_asin}</div> : null}
                          {row.sample_sku && row.sample_sku !== row.sku ? <div className="text-[10px] text-muted-foreground">sample SKU: {row.sample_sku}</div> : null}
                        </td>
                        <td className={cn("px-2 max-w-[280px] truncate", rowPadding)} title={row.title ?? undefined}>
                          {row.title || "-"}
                          {row.flags.length > 0 && (
                            <div className="mt-0.5 text-[10px] text-yellow-400">{row.flags.join(", ")}</div>
                          )}
                        </td>
                        <td className={cn("px-2", rowPadding)}>{row.marketplace_code}</td>
                        <td className={cn("px-2", rowPadding)}>{row.fulfillment_channel}</td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>
                          {(row.offer_price ?? 0).toFixed(2)} {row.offer_currency}
                          <div className="text-muted-foreground">{formatPLN(row.offer_price_pln)}</div>
                        </td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.scenario_qty}</td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.suggested_pack_qty}</td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{row.packages_count}</td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{formatPLN(row.revenue_pln)}</td>
                        <td className={cn("px-2 text-right tabular-nums text-amber-300", rowPadding)}>
                          {formatPLN(row.cogs_pln)}
                          <div className="text-[10px] text-muted-foreground">{row.cogs_source}</div>
                        </td>
                        <td className={cn("px-2 text-right tabular-nums text-green-400", rowPadding)}>
                          {formatPLN(row.estimated_shipping_charge_pln)}
                          <div className="text-[10px] text-muted-foreground">
                            {row.shipping_charge_source}
                            {row.shipping_charge_mode ? ` · ${row.shipping_charge_mode}` : ""}
                          </div>
                        </td>
                        <td className={cn("px-2 text-right tabular-nums text-muted-foreground", rowPadding)}>
                          {formatPLN(row.plan_logistics_pln ?? 0)}
                          <div className="text-[10px] text-muted-foreground">{row.logistics_plan_source}</div>
                        </td>
                        <td className={cn("px-2 text-right tabular-nums text-blue-300", rowPadding)}>
                          {formatPLN(row.observed_logistics_pln ?? 0)}
                          <div className="text-[10px] text-muted-foreground">
                            {row.logistics_observed_source} · n={row.logistics_observed_samples ?? 0}
                          </div>
                        </td>
                        <td className={cn("px-2 text-right tabular-nums text-cyan-300", rowPadding)}>
                          {formatPLN(row.decision_logistics_pln ?? row.estimated_logistics_pln)}
                          <div className="text-[10px] text-muted-foreground">
                            {row.logistics_decision_rule || row.logistics_source}
                          </div>
                          {row.execution_drift && (
                            <div className="text-[10px] text-red-400">execution drift</div>
                          )}
                        </td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>
                          {row.logistics_gap_pct != null ? (
                            <span className={cn(
                              "rounded px-1.5 py-0.5",
                              row.logistics_gap_pct > 10
                                ? "bg-red-500/10 text-red-400"
                                : row.logistics_gap_pct > 0
                                  ? "bg-yellow-500/10 text-yellow-400"
                                  : "bg-green-500/10 text-green-400"
                            )}>
                              {formatPct(row.logistics_gap_pct)}
                            </span>
                          ) : (
                            <span className="text-muted-foreground">-</span>
                          )}
                        </td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{formatPLN(row.amazon_fees_pln)}</td>
                        <td className={cn("px-2 text-right tabular-nums text-red-300", rowPadding)}>
                          {formatPLN(row.estimated_returns_net_pln ?? 0)}
                        </td>
                        <td className={cn("px-2 text-right tabular-nums text-amber-300", rowPadding)}>
                          {formatPLN(row.estimated_fba_storage_fee_pln ?? 0)}
                        </td>
                        <td className={cn("px-2 text-right tabular-nums text-amber-300", rowPadding)}>
                          {formatPLN(row.estimated_fba_aged_fee_pln ?? 0)}
                        </td>
                        <td className={cn("px-2 text-right tabular-nums text-amber-300", rowPadding)}>
                          {formatPLN(row.estimated_fba_removal_fee_pln ?? 0)}
                        </td>
                        <td className={cn("px-2 text-right tabular-nums text-amber-300", rowPadding)}>
                          {formatPLN(row.estimated_fba_liquidation_fee_pln ?? 0)}
                        </td>
                        <td className={cn("px-2 text-right tabular-nums text-purple-300", rowPadding)}>
                          {formatPLN(row.overhead_allocated_pln ?? 0)}
                          <div className="text-[10px] text-muted-foreground">
                            {row.overhead_allocation_method || "none"}
                            {typeof row.overhead_confidence_pct === "number" ? ` · ${row.overhead_confidence_pct.toFixed(0)}%` : ""}
                          </div>
                        </td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{formatPLN(row.cm1_profit)}</td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{formatPLN(row.cm2_profit)}</td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>
                          <span className={cn("rounded px-1.5 py-0.5", profitBadge(row.cm2_percent))}>{formatPct(row.cm2_percent)}</span>
                        </td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>{formatPLN(row.np_profit ?? row.cm2_profit)}</td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>
                          <span className={cn("rounded px-1.5 py-0.5", profitBadge(row.np_percent ?? row.cm2_percent))}>
                            {formatPct(row.np_percent ?? row.cm2_percent)}
                          </span>
                        </td>
                        <td className={cn("px-2 text-right tabular-nums", rowPadding)}>
                          {(row.confidence_score ?? 0).toFixed(0)}%
                          <div className="text-[10px] text-muted-foreground">
                            H: {row.history_orders} / S: {row.single_order_samples} / P: {row.pack_suggestion_source}
                          </div>
                        </td>
                      </tr>
                      {isParent && isExpanded && (
                        <tr className="bg-muted/10">
                          <td colSpan={28} className="px-3 py-2">
                            <div className="rounded border border-border/50 bg-background/40 p-2">
                              <div className="mb-2 text-[10px] uppercase tracking-wider text-muted-foreground">
                                Child offers for parent {parentAsinLabel}
                              </div>
                              {isLoadingChildren ? (
                                <div className="text-xs text-muted-foreground">Ładowanie childów...</div>
                              ) : children.length === 0 ? (
                                <div className="text-xs text-muted-foreground">Brak childów dla tego parenta.</div>
                              ) : (
                                <div className="space-y-1">
                                  {children.map((child) => (
                                    <div key={`${parentRowId}-${child.sku}-${child.marketplace_id}`} className="flex items-center justify-between gap-3 rounded border border-border/40 px-2 py-1 text-xs">
                                      <div className="min-w-0">
                                        <div className="font-mono text-foreground">
                                          {child.asin || child.sku} <span className="text-muted-foreground">({child.marketplace_code})</span>
                                        </div>
                                        <div className="truncate text-muted-foreground">{child.title || "-"}</div>
                                      </div>
                                      <div className="shrink-0 text-right tabular-nums">
                                        <div>Rev {formatPLN(child.revenue_pln)}</div>
                                        <div className="text-muted-foreground">CM1 {formatPLN(child.cm1_profit)} | CM2 {formatPLN(child.cm2_profit)} | NP {formatPLN(child.np_profit ?? child.cm2_profit)}</div>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
          {whatIfData && (
            <div className="flex items-center justify-between border-t border-border px-4 py-3 shrink-0">
              <span className="text-xs text-muted-foreground">
                Strona {whatIfData.page} z {Math.max(1, whatIfData.pages)} | {whatIfData.total.toLocaleString()} ofert
                {" • "}filtrowanie i paginacja po stronie serwera
              </span>
              <div className="flex items-center gap-1">
                <button
                  disabled={page <= 1}
                  onClick={() => goToPage(1, whatIfData.pages)}
                  className="rounded px-2 py-1 text-xs text-muted-foreground hover:bg-muted disabled:opacity-30"
                  title="Pierwsza strona"
                >
                  «
                </button>
                <button
                  disabled={page <= 1}
                  onClick={() => goToPage(page - 1, whatIfData.pages)}
                  className="rounded p-1 text-muted-foreground hover:bg-muted disabled:opacity-30"
                  title="Poprzednia strona"
                >
                  <ChevronLeft className="h-4 w-4" />
                </button>
                <input
                  value={jumpPageInput}
                  onChange={(e) => setJumpPageInput(e.target.value.replace(/[^0-9]/g, ""))}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") applyPageJump(whatIfData.pages);
                  }}
                  className="h-7 w-12 rounded border border-input bg-background px-1 text-center text-xs outline-none focus:ring-1 focus:ring-ring"
                  title="Przejdź do strony"
                />
                <button
                  disabled={page >= whatIfData.pages}
                  onClick={() => goToPage(page + 1, whatIfData.pages)}
                  className="rounded p-1 text-muted-foreground hover:bg-muted disabled:opacity-30"
                  title="Następna strona"
                >
                  <ChevronRight className="h-4 w-4" />
                </button>
                <button
                  disabled={page >= whatIfData.pages}
                  onClick={() => goToPage(whatIfData.pages, whatIfData.pages)}
                  className="rounded px-2 py-1 text-xs text-muted-foreground hover:bg-muted disabled:opacity-30"
                  title="Ostatnia strona"
                >
                  »
                </button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
