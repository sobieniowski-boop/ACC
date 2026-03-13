import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getMarketplaces } from "@/lib/api";
import type { PageFilters, PageFilterActions, DatePreset } from "@/lib/usePageFilters";
import type { ProfitMode, CurrencyView } from "@/store/userPreferences";

function parseCommaList(value: string): string[] {
  return value.split(",").map((v) => v.trim()).filter(Boolean);
}

interface PageFilterBarProps {
  filters: PageFilters & PageFilterActions;
  profitMode?: ProfitMode;
  onProfitModeChange?: (v: ProfitMode) => void;
  currencyView?: CurrencyView;
  onCurrencyViewChange?: (v: CurrencyView) => void;
  confidenceSlider?: boolean;
  showProfitMode?: boolean;
}

const PRESET_OPTIONS: { id: DatePreset; label: string }[] = [
  { id: "today", label: "Today" },
  { id: "wtd", label: "WTD" },
  { id: "mtd", label: "MTD" },
  { id: "7d", label: "7d" },
  { id: "30d", label: "30d" },
  { id: "90d", label: "90d" },
  { id: "custom", label: "Custom" },
];

export default function PageFilterBar({
  filters,
  profitMode,
  onProfitModeChange,
  currencyView,
  onCurrencyViewChange,
  confidenceSlider = false,
  showProfitMode = false,
}: PageFilterBarProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [brandInput, setBrandInput] = useState("");
  const [categoryInput, setCategoryInput] = useState("");

  const { data: marketplaces } = useQuery({
    queryKey: ["marketplaces"],
    queryFn: getMarketplaces,
    staleTime: 300_000,
  });

  const summaryParts = [
    `${filters.dateFrom} – ${filters.dateTo}`,
    filters.marketplaceIds.length ? `MP ${filters.marketplaceIds.length}` : "All MP",
    filters.brands.length ? `Brand ${filters.brands.length}` : null,
    filters.fulfillments.length ? filters.fulfillments.join("/") : null,
    showProfitMode && profitMode ? profitMode.toUpperCase() : null,
    filters.confidenceMin > 0 ? `Conf ≥ ${filters.confidenceMin}%` : null,
  ].filter(Boolean).join(" · ");

  return (
    <div className="rounded-xl border border-border bg-card px-4 py-3">
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Filters
        </span>
        <button
          onClick={() => setIsExpanded((v) => !v)}
          className="rounded border border-border px-2 py-1 text-xs text-muted-foreground hover:text-foreground"
        >
          {isExpanded ? "Collapse" : "Expand"}
        </button>
        <button
          onClick={filters.resetFilters}
          className="rounded border border-border px-2 py-1 text-xs text-muted-foreground hover:text-foreground"
        >
          Reset
        </button>
        <span className="ml-auto text-xs text-muted-foreground">{summaryParts}</span>
      </div>

      {isExpanded && (
        <div className="mt-3 space-y-3">
          <div className="flex flex-wrap items-end gap-3">
            <div>
              <label className="mb-1 block text-[11px] text-muted-foreground">Date preset</label>
              <select
                value={filters.datePreset}
                onChange={(e) => filters.setDatePreset(e.target.value as DatePreset)}
                className="rounded border border-input bg-background px-2 py-1 text-xs"
              >
                {PRESET_OPTIONS.map((o) => (
                  <option key={o.id} value={o.id}>{o.label}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-[11px] text-muted-foreground">From</label>
              <input
                type="date"
                value={filters.dateFrom}
                onChange={(e) => filters.setCustomDateRange(e.target.value, filters.dateTo)}
                className="rounded border border-input bg-background px-2 py-1 text-xs"
              />
            </div>
            <div>
              <label className="mb-1 block text-[11px] text-muted-foreground">To</label>
              <input
                type="date"
                value={filters.dateTo}
                onChange={(e) => filters.setCustomDateRange(filters.dateFrom, e.target.value)}
                className="rounded border border-input bg-background px-2 py-1 text-xs"
              />
            </div>
            {showProfitMode && profitMode && onProfitModeChange && (
              <div>
                <label className="mb-1 block text-[11px] text-muted-foreground">Profit mode</label>
                <select
                  value={profitMode}
                  onChange={(e) => onProfitModeChange(e.target.value as ProfitMode)}
                  className="rounded border border-input bg-background px-2 py-1 text-xs"
                >
                  <option value="cm1">CM1</option>
                  <option value="cm2">CM2</option>
                  <option value="np">NP</option>
                </select>
              </div>
            )}
            {currencyView !== undefined && onCurrencyViewChange && (
              <div>
                <label className="mb-1 block text-[11px] text-muted-foreground">Currency</label>
                <select
                  value={currencyView}
                  onChange={(e) => onCurrencyViewChange(e.target.value as CurrencyView)}
                  className="rounded border border-input bg-background px-2 py-1 text-xs"
                >
                  <option value="base">Base</option>
                  <option value="original">Original</option>
                </select>
              </div>
            )}
            {confidenceSlider && (
              <div className="min-w-44">
                <label className="mb-1 block text-[11px] text-muted-foreground">
                  Confidence: {filters.confidenceMin}%
                </label>
                <input
                  type="range"
                  min={0}
                  max={100}
                  step={5}
                  value={filters.confidenceMin}
                  onChange={(e) => filters.setConfidenceMin(Number(e.target.value))}
                  className="w-full"
                />
              </div>
            )}
          </div>

          <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-4">
            <div>
              <label className="mb-1 block text-[11px] text-muted-foreground">Marketplace (multi)</label>
              <select
                multiple
                value={filters.marketplaceIds}
                onChange={(e) => {
                  const vals = Array.from(e.target.selectedOptions).map((o) => o.value);
                  filters.setMarketplaceIds(vals);
                }}
                className="h-24 w-full rounded border border-input bg-background px-2 py-1 text-xs"
              >
                {(marketplaces ?? []).map((m) => (
                  <option key={m.marketplace_id} value={m.marketplace_id}>{m.code}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-[11px] text-muted-foreground">Brand (comma list)</label>
              <input
                value={brandInput || filters.brands.join(", ")}
                onChange={(e) => setBrandInput(e.target.value)}
                onBlur={() => {
                  const vals = parseCommaList(brandInput || filters.brands.join(", "));
                  filters.setBrands(vals);
                  setBrandInput(vals.join(", "));
                }}
                placeholder="kadax, garden..."
                className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
              />
            </div>
            <div>
              <label className="mb-1 block text-[11px] text-muted-foreground">Category / product_type</label>
              <input
                value={categoryInput || filters.categories.join(", ")}
                onChange={(e) => setCategoryInput(e.target.value)}
                onBlur={() => {
                  const vals = parseCommaList(categoryInput || filters.categories.join(", "));
                  filters.setCategories(vals);
                  setCategoryInput(vals.join(", "));
                }}
                placeholder="home, garden..."
                className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
              />
            </div>
            <div>
              <label className="mb-1 block text-[11px] text-muted-foreground">Fulfillment (multi)</label>
              <select
                multiple
                value={filters.fulfillments}
                onChange={(e) => {
                  const vals = Array.from(e.target.selectedOptions).map((o) => o.value);
                  filters.setFulfillments(vals);
                }}
                className="h-24 w-full rounded border border-input bg-background px-2 py-1 text-xs"
              >
                <option value="AFN">FBA</option>
                <option value="MFN">FBM</option>
                <option value="OTHER">SellerFlex/Other</option>
              </select>
            </div>
          </div>

          <div>
            <label className="mb-1 block text-[11px] text-muted-foreground">
              SKU / ASIN search (multi-line)
            </label>
            <textarea
              value={filters.skuAsinQuery}
              onChange={(e) => filters.setSkuAsinQuery(e.target.value)}
              placeholder={"B0XXXX\nSKU-123\nSKU-456"}
              rows={2}
              className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
            />
          </div>
        </div>
      )}
    </div>
  );
}
