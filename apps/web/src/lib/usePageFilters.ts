import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { format, subDays, startOfMonth, startOfWeek } from "date-fns";

export type DatePreset = "today" | "wtd" | "mtd" | "7d" | "30d" | "90d" | "custom";

function getDateRangeForPreset(preset: DatePreset): { dateFrom: string; dateTo: string } {
  const now = new Date();
  const dateTo = format(now, "yyyy-MM-dd");
  switch (preset) {
    case "today":
      return { dateFrom: dateTo, dateTo };
    case "wtd":
      return { dateFrom: format(startOfWeek(now, { weekStartsOn: 1 }), "yyyy-MM-dd"), dateTo };
    case "mtd":
      return { dateFrom: format(startOfMonth(now), "yyyy-MM-dd"), dateTo };
    case "7d":
      return { dateFrom: format(subDays(now, 6), "yyyy-MM-dd"), dateTo };
    case "90d":
      return { dateFrom: format(subDays(now, 89), "yyyy-MM-dd"), dateTo };
    case "30d":
    default:
      return { dateFrom: format(subDays(now, 29), "yyyy-MM-dd"), dateTo };
  }
}

export interface PageFilters {
  datePreset: DatePreset;
  dateFrom: string;
  dateTo: string;
  marketplaceIds: string[];
  brands: string[];
  categories: string[];
  skuAsinQuery: string;
  fulfillments: string[];
  confidenceMin: number;
}

export interface PageFilterActions {
  setDatePreset: (preset: DatePreset) => void;
  setCustomDateRange: (dateFrom: string, dateTo: string) => void;
  setMarketplaceIds: (ids: string[]) => void;
  setBrands: (values: string[]) => void;
  setCategories: (values: string[]) => void;
  setSkuAsinQuery: (query: string) => void;
  setFulfillments: (values: string[]) => void;
  setConfidenceMin: (value: number) => void;
  resetFilters: () => void;
}

const DEFAULT_PRESET: DatePreset = "30d";

/**
 * Hook providing page-local filters backed by URL search params.
 * Each page gets its own independent filter state reflected in the URL,
 * enabling bookmarking, sharing, and browser history navigation.
 */
export function usePageFilters(): PageFilters & PageFilterActions {
  const [searchParams, setSearchParams] = useSearchParams();

  const filters = useMemo<PageFilters>(() => {
    const preset = (searchParams.get("preset") as DatePreset) || DEFAULT_PRESET;
    const defaultRange = getDateRangeForPreset(preset);

    return {
      datePreset: preset,
      dateFrom: searchParams.get("from") || defaultRange.dateFrom,
      dateTo: searchParams.get("to") || defaultRange.dateTo,
      marketplaceIds: searchParams.get("mp")?.split(",").filter(Boolean) ?? [],
      brands: searchParams.get("brands")?.split(",").filter(Boolean) ?? [],
      categories: searchParams.get("cats")?.split(",").filter(Boolean) ?? [],
      skuAsinQuery: searchParams.get("sku") ?? "",
      fulfillments: searchParams.get("ff")?.split(",").filter(Boolean) ?? [],
      confidenceMin: Number(searchParams.get("conf")) || 0,
    };
  }, [searchParams]);

  const update = useCallback(
    (patch: Record<string, string | null>) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          for (const [key, value] of Object.entries(patch)) {
            if (value === null || value === "" || value === "0") {
              next.delete(key);
            } else {
              next.set(key, value);
            }
          }
          return next;
        },
        { replace: true },
      );
    },
    [setSearchParams],
  );

  const setDatePreset = useCallback(
    (preset: DatePreset) => {
      const range = getDateRangeForPreset(preset);
      update({
        preset: preset === DEFAULT_PRESET ? null : preset,
        from: preset === "custom" ? filters.dateFrom : range.dateFrom,
        to: preset === "custom" ? filters.dateTo : range.dateTo,
      });
    },
    [update, filters.dateFrom, filters.dateTo],
  );

  const setCustomDateRange = useCallback(
    (dateFrom: string, dateTo: string) => {
      update({ preset: "custom", from: dateFrom, to: dateTo });
    },
    [update],
  );

  const setMarketplaceIds = useCallback(
    (ids: string[]) => update({ mp: ids.length ? ids.join(",") : null }),
    [update],
  );

  const setBrands = useCallback(
    (values: string[]) => update({ brands: values.length ? values.join(",") : null }),
    [update],
  );

  const setCategories = useCallback(
    (values: string[]) => update({ cats: values.length ? values.join(",") : null }),
    [update],
  );

  const setSkuAsinQuery = useCallback(
    (query: string) => update({ sku: query.trim() || null }),
    [update],
  );

  const setFulfillments = useCallback(
    (values: string[]) => update({ ff: values.length ? values.join(",") : null }),
    [update],
  );

  const setConfidenceMin = useCallback(
    (value: number) => update({ conf: value > 0 ? String(value) : null }),
    [update],
  );

  const resetFilters = useCallback(() => {
    setSearchParams({}, { replace: true });
  }, [setSearchParams]);

  return { ...filters, setDatePreset, setCustomDateRange, setMarketplaceIds, setBrands, setCategories, setSkuAsinQuery, setFulfillments, setConfidenceMin, resetFilters };
}

/**
 * Converts page filters + user preferences into API query params.
 * Drop-in replacement for the old globalFiltersToApiParams.
 */
export function pageFiltersToApiParams(
  filters: PageFilters,
  prefs: { profitMode: string; currencyView: string },
): Record<string, unknown> {
  const skuTokens = filters.skuAsinQuery
    .split(/\r?\n|,|;/g)
    .map((v) => v.trim())
    .filter(Boolean);
  const skuSearch = skuTokens.join(" ");

  return {
    date_from: filters.dateFrom,
    date_to: filters.dateTo,
    ...(filters.marketplaceIds.length === 1
      ? { marketplace_id: filters.marketplaceIds[0] }
      : filters.marketplaceIds.length > 1
        ? { marketplace_ids: filters.marketplaceIds.join(",") }
        : {}),
    ...(filters.brands.length === 1
      ? { brand: filters.brands[0] }
      : filters.brands.length > 1
        ? { brands: filters.brands.join(",") }
        : {}),
    ...(filters.categories.length === 1
      ? { category: filters.categories[0] }
      : filters.categories.length > 1
        ? { categories: filters.categories.join(",") }
        : {}),
    ...(filters.fulfillments.length > 0 ? { fulfillment_channels: filters.fulfillments.join(",") } : {}),
    ...(filters.confidenceMin > 0 ? { confidence_min: filters.confidenceMin } : {}),
    profit_mode: prefs.profitMode,
    currency_view: prefs.currencyView,
    ...(skuSearch ? { sku_search: skuSearch } : {}),
  };
}
