import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getDataQuality,
  upsertPurchasePrice,
  mapAndPrice,
  runAIMatching,
  getAIMatchSuggestions,
  approveAIMatch,
  rejectAIMatch,
} from "@/lib/api";
import type { AIMatchSuggestionItem, DataQualityResponse } from "@/lib/api";
import { pageFiltersToApiParams, usePageFilters } from "@/lib/usePageFilters";
import { formatPct, cn } from "@/lib/utils";
import { useUserPreferences } from "@/store/userPreferences";
import {
  ShieldCheck,
  ShieldAlert,
  Shield,
  Database,
  BarChart3,
  AlertTriangle,
  CheckCircle,
  XCircle,
  Save,
  Loader2,
  Sparkles,
  ThumbsUp,
  ThumbsDown,
  ChevronDown,
  ChevronUp,
  Zap,
} from "lucide-react";
import { ClientExportButton } from "@/components/shared";

type MissingCogsItem = NonNullable<DataQualityResponse["missing_cogs_top"]>[number];

function extractApiErrorMessage(error: unknown): string {
  const maybe = error as {
    response?: { data?: { detail?: string } };
    message?: string;
  };
  return maybe?.response?.data?.detail || maybe?.message || "Nieznany błąd";
}

function CoverageGauge({
  label,
  value,
  icon: Icon,
}: {
  label: string;
  value: number;
  icon: React.ElementType;
}) {
  const color =
    value >= 90
      ? "text-green-400"
      : value >= 70
      ? "text-yellow-400"
      : "text-red-400";
  const bg =
    value >= 90
      ? "bg-green-500"
      : value >= 70
      ? "bg-yellow-500"
      : "bg-red-500";

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="mb-3 flex items-center gap-2">
        <Icon className={cn("h-4 w-4", color)} />
        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
          {label}
        </span>
      </div>
      <div className={cn("text-3xl font-bold tabular-nums", color)}>
        {formatPct(value)}
      </div>
      <div className="mt-2 h-2 w-full overflow-hidden rounded-full bg-muted/40">
        <div
          className={cn(bg, "h-full rounded-full transition-all duration-500")}
          style={{ width: `${Math.min(value, 100)}%` }}
        />
      </div>
    </div>
  );
}

function StatusDot({ ok }: { ok: boolean }) {
  return ok ? (
    <CheckCircle className="h-4 w-4 text-green-400" />
  ) : (
    <XCircle className="h-4 w-4 text-red-400" />
  );
}

function SuggestionBadge({
  label,
  tone,
}: {
  label: string;
  tone: "hard" | "ai-hard" | "ai";
}) {
  const toneClass =
    tone === "hard"
      ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-300"
      : tone === "ai-hard"
      ? "border-violet-500/30 bg-violet-500/10 text-violet-200"
      : "border-slate-500/30 bg-slate-500/10 text-slate-300";
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-medium",
        toneClass
      )}
    >
      {label}
    </span>
  );
}

function hasMissingCogsSuggestion(item: MissingCogsItem): boolean {
  return Boolean(
    item.hard_suggestion ||
      item.ai_candidate?.matched_internal_sku ||
      item.ai_candidate?.hard_price_pln != null
  );
}

// ---------------------------------------------------------------------------
// AI Match Suggestions Panel
// ---------------------------------------------------------------------------

function ConfidenceBadge({ value }: { value: number }) {
  const color =
    value >= 80
      ? "bg-green-500/20 text-green-400 border-green-500/30"
      : value >= 50
      ? "bg-yellow-500/20 text-yellow-400 border-yellow-500/30"
      : "bg-red-500/20 text-red-400 border-red-500/30";
  return (
    <span className={cn("inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-bold tabular-nums", color)}>
      {value}%
    </span>
  );
}

function SuggestionRow({
  item,
  onApprove,
  onReject,
  isApproving,
  isRejecting,
}: {
  item: AIMatchSuggestionItem;
  onApprove: (id: number) => void;
  onReject: (id: number) => void;
  isApproving: boolean;
  isRejecting: boolean;
}) {
  const [expanded, setExpanded] = useState(false);

  return (
    <>
      <tr className="hover:bg-muted/20 transition-colors group">
        <td className="px-2 py-1.5">
          <button
            onClick={() => setExpanded(!expanded)}
            className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground"
          >
            {expanded ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
          </button>
        </td>
        <td className="px-2 py-1.5">
          <div className="font-mono text-xs font-medium">{item.unmapped_sku}</div>
          <div className="text-[10px] text-muted-foreground truncate max-w-[250px]" title={item.unmapped_title ?? ""}>
            {item.unmapped_title || "-"}
          </div>
        </td>
        <td className="px-2 py-1.5 text-center">
          <span className="text-muted-foreground text-lg">→</span>
        </td>
        <td className="px-2 py-1.5">
          <div className="font-mono text-xs font-medium text-amazon">{item.matched_internal_sku}</div>
          <div className="text-[10px] text-muted-foreground truncate max-w-[250px]" title={item.matched_title ?? ""}>
            {item.matched_title || "-"}
          </div>
        </td>
        <td className="px-2 py-1.5 text-center">
          <ConfidenceBadge value={item.confidence} />
        </td>
        <td className="px-2 py-1.5 text-center tabular-nums text-xs">
          {item.quantity_in_bundle > 1 ? `${item.quantity_in_bundle}×` : "1"}
        </td>
        <td className="px-2 py-1.5 text-right tabular-nums text-xs">
          {item.total_price_pln != null
            ? item.total_price_pln.toFixed(2).replace(".", ",")
            : "-"}{" "}
          <span className="text-muted-foreground">PLN</span>
        </td>
        <td className="px-2 py-1.5">
          <div className="flex items-center gap-1">
            <button
              onClick={() => onApprove(item.id)}
              disabled={isApproving || isRejecting}
              className={cn(
                "rounded p-1.5 transition-colors",
                "text-green-400 hover:bg-green-500/10 cursor-pointer",
                (isApproving || isRejecting) && "opacity-40 cursor-not-allowed"
              )}
              title="Zatwierdź dopasowanie"
            >
              {isApproving ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <ThumbsUp className="h-4 w-4" />
              )}
            </button>
            <button
              onClick={() => onReject(item.id)}
              disabled={isApproving || isRejecting}
              className={cn(
                "rounded p-1.5 transition-colors",
                "text-red-400 hover:bg-red-500/10 cursor-pointer",
                (isApproving || isRejecting) && "opacity-40 cursor-not-allowed"
              )}
              title="Odrzuć dopasowanie"
            >
              {isRejecting ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <ThumbsDown className="h-4 w-4" />
              )}
            </button>
          </div>
        </td>
      </tr>
      {expanded && (
        <tr className="bg-muted/10">
          <td colSpan={8} className="px-6 py-3">
            <div className="space-y-2 text-xs">
              {item.reasoning && (
                <div>
                  <span className="font-semibold text-muted-foreground">Uzasadnienie AI:</span>{" "}
                  <span className="text-foreground">{item.reasoning}</span>
                </div>
              )}
              {item.bom && item.bom.length > 0 && (
                <div>
                  <span className="font-semibold text-muted-foreground">BOM (Bill of Materials):</span>
                  <div className="mt-1 ml-2 space-y-0.5">
                    {item.bom.map((b, i) => (
                      <div key={i} className="flex items-center gap-2 text-[11px]">
                        <span className="font-mono text-amazon">{b.internal_sku || "?"}</span>
                        <span className="text-muted-foreground">×{b.qty}</span>
                        <span>{b.name || ""}</span>
                        {b.unit_price_pln != null && (
                          <span className="text-muted-foreground">
                            @ {b.unit_price_pln.toFixed(2).replace(".", ",")} PLN
                          </span>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
              <div className="flex gap-4 text-[10px] text-muted-foreground">
                {item.unmapped_asin && <span>ASIN: {item.unmapped_asin}</span>}
                {item.unit_price_pln != null && (
                  <span>Cena jedn.: {item.unit_price_pln.toFixed(2).replace(".", ",")} PLN</span>
                )}
                <span>Utworzono: {item.created_at?.slice(0, 19).replace("T", " ")}</span>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

function AIMatchPanel() {
  const queryClient = useQueryClient();
  const [showPanel, setShowPanel] = useState(true);

  const suggestionsQuery = useQuery({
    queryKey: ["ai-match-suggestions", "pending"],
    queryFn: () => getAIMatchSuggestions({ status: "pending", page: 1, page_size: 100 }),
  });

  const runMutation = useMutation({
    mutationFn: runAIMatching,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["ai-match-suggestions"] });
    },
  });

  const approveMutation = useMutation({
    mutationFn: approveAIMatch,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["ai-match-suggestions"] });
      queryClient.invalidateQueries({ queryKey: ["data-quality"] });
    },
  });

  const rejectMutation = useMutation({
    mutationFn: rejectAIMatch,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["ai-match-suggestions"] });
    },
  });

  const items = suggestionsQuery.data?.items ?? [];
  const total = suggestionsQuery.data?.total ?? 0;

  return (
    <div>
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-purple-400" />
          <h2 className="text-sm font-semibold">AI Product Matcher</h2>
          {total > 0 && (
            <span className="rounded-full bg-purple-500/20 px-2 py-0.5 text-[10px] font-bold text-purple-300">
              {total} do weryfikacji
            </span>
          )}
          <button
            onClick={() => setShowPanel(!showPanel)}
            className="ml-2 text-muted-foreground hover:text-foreground"
          >
            {showPanel ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
          </button>
        </div>
        <button
          onClick={() => runMutation.mutate()}
          disabled={runMutation.isPending}
          className={cn(
            "flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
            "bg-purple-600 text-white hover:bg-purple-700",
            runMutation.isPending && "opacity-60 cursor-not-allowed"
          )}
        >
          {runMutation.isPending ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : (
            <Zap className="h-3.5 w-3.5" />
          )}
          {runMutation.isPending ? "Analizuję..." : "Uruchom AI Matching"}
        </button>
      </div>

      {runMutation.isSuccess && (
        <div
          className={cn(
            "mb-3 rounded px-3 py-2 text-xs",
            runMutation.data.status === "error"
              ? "border border-red-500/30 bg-red-500/10 text-red-400"
              : runMutation.data.status === "partial"
              ? "border border-yellow-500/30 bg-yellow-500/10 text-yellow-300"
              : "border border-purple-500/30 bg-purple-500/10 text-purple-300"
          )}
        >
          <div>
            {runMutation.data.message} (produkty: {runMutation.data.unmapped_count}, wyniki GPT: {runMutation.data.gpt_results}, zapisane: {runMutation.data.suggestions_saved})
          </div>
          {runMutation.data.error_summary && (
            <div className="mt-1 text-[11px] opacity-90">
              Szczegóły: {runMutation.data.error_summary}
            </div>
          )}
        </div>
      )}

      {runMutation.isError && (
        <div className="mb-3 rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
          Błąd AI: {(runMutation.error as Error)?.message ?? "Nieznany błąd"}
        </div>
      )}

      {showPanel && (
        <>
          {suggestionsQuery.isLoading && (
            <div className="py-6 text-center text-xs text-muted-foreground">
              <Loader2 className="mx-auto h-5 w-5 animate-spin mb-2" />
              Ładuję sugestie...
            </div>
          )}

          {items.length === 0 && !suggestionsQuery.isLoading && (
            <div className="rounded-xl border border-border bg-card p-6 text-center text-xs text-muted-foreground">
              <Sparkles className="mx-auto h-8 w-8 mb-2 text-muted-foreground/30" />
              Brak sugestii do weryfikacji. Kliknij "Uruchom AI Matching" aby wygenerować propozycje dopasowań.
            </div>
          )}

          {items.length > 0 && (
            <div className="overflow-hidden rounded-xl border border-border bg-card">
              <table className="w-full text-[11px]">
                <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
                  <tr>
                    <th className="px-2 py-2 w-8"></th>
                    <th className="px-2 py-2">Produkt Amazon</th>
                    <th className="px-2 py-2 w-8"></th>
                    <th className="px-2 py-2">Dopasowany ISK</th>
                    <th className="px-2 py-2 text-center">Pewność</th>
                    <th className="px-2 py-2 text-center">Ilość</th>
                    <th className="px-2 py-2 text-right">Cena COGS</th>
                    <th className="px-2 py-2 w-20">Akcja</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {items.map((item) => (
                    <SuggestionRow
                      key={item.id}
                      item={item}
                      onApprove={(id) => approveMutation.mutate(id)}
                      onReject={(id) => rejectMutation.mutate(id)}
                      isApproving={approveMutation.isPending && approveMutation.variables === item.id}
                      isRejecting={rejectMutation.isPending && rejectMutation.variables === item.id}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {(approveMutation.isError || rejectMutation.isError) && (
            <div className="mt-2 rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
              Błąd: {((approveMutation.error || rejectMutation.error) as Error)?.message ?? "Nieznany błąd"}
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default function DataQualityPage() {
  const queryClient = useQueryClient();
  const [editPrices, setEditPrices] = useState<Record<string, string>>({});
  const [editIsks, setEditIsks] = useState<Record<string, string>>({});
  const [savedSkus, setSavedSkus] = useState<Set<string>>(new Set());
  const [showOnlySuggestedRows, setShowOnlySuggestedRows] = useState(false);

  const filters = usePageFilters();
  const { profitMode, currencyView } = useUserPreferences();

  const params = pageFiltersToApiParams(filters, { profitMode, currencyView });

  const { data, isLoading } = useQuery({
    queryKey: ["data-quality", params],
    queryFn: () => getDataQuality(params),
  });

  const priceMutation = useMutation({
    mutationFn: upsertPurchasePrice,
    onSuccess: (_res, vars) => {
      setSavedSkus((prev) => new Set(prev).add(vars.internal_sku));
      setTimeout(() => queryClient.invalidateQueries({ queryKey: ["data-quality"] }), 500);
    },
  });

  const mapMutation = useMutation({
    mutationFn: mapAndPrice,
    onSuccess: (_res, vars) => {
      setSavedSkus((prev) => new Set(prev).add(vars.sku));
      setTimeout(() => queryClient.invalidateQueries({ queryKey: ["data-quality"] }), 500);
    },
  });

  const parsePrice = (raw: string | undefined): number => {
    if (!raw) return 0;
    return parseFloat(raw.replace(",", ".").replace(/\s/g, "")) || 0;
  };

  const formatPriceInput = (value: number): string =>
    value.toFixed(2).replace(".", ",");

  const handleSavePrice = (internalSku: string) => {
    const val = parsePrice(editPrices[internalSku]);
    if (val <= 0) return;
    priceMutation.mutate({ internal_sku: internalSku, netto_price_pln: val });
  };

  const handleMapAndPrice = (sku: string) => {
    const isk = editIsks[sku]?.trim();
    const val = parsePrice(editPrices[sku]);
    if (!isk || val <= 0) return;
    mapMutation.mutate({ sku, internal_sku: isk, netto_price_pln: val });
  };

  const applyHardSuggestion = (item: NonNullable<DataQualityResponse["missing_cogs_top"]>[number]) => {
    if (!item.hard_suggestion) return;
    if (item.hard_suggestion.suggested_internal_sku) {
      setEditIsks((prev) => ({
        ...prev,
        [item.sku]: item.hard_suggestion?.suggested_internal_sku ?? "",
      }));
    }
    if (item.hard_suggestion.suggested_price_pln != null) {
      const stateKey = item.internal_sku || item.sku;
      setEditPrices((prev) => ({
        ...prev,
        [stateKey]: formatPriceInput(item.hard_suggestion!.suggested_price_pln!),
      }));
    }
  };

  const applyAICandidate = (item: NonNullable<DataQualityResponse["missing_cogs_top"]>[number]) => {
    if (!item.ai_candidate) return;
    setEditIsks((prev) => ({
      ...prev,
      [item.sku]: item.ai_candidate?.matched_internal_sku ?? "",
    }));
    if (item.ai_candidate.hard_price_pln != null) {
      const stateKey = item.internal_sku || item.sku;
      setEditPrices((prev) => ({
        ...prev,
        [stateKey]: formatPriceInput(item.ai_candidate!.hard_price_pln!),
      }));
    }
  };

  const ovr = data?.overview;
  const missingCogsItems = data?.missing_cogs_top ?? [];
  const visibleMissingCogs = showOnlySuggestedRows
    ? missingCogsItems.filter((item) => hasMissingCogsSuggestion(item))
    : missingCogsItems;

  return (
    <div className="space-y-6">
      <div>
        <div className="flex items-center gap-2">
          <Database className="h-6 w-6 text-amazon" />
          <h1 className="text-2xl font-bold">Data Quality & Coverage</h1>
          {missingCogsItems.length > 0 && <ClientExportButton data={missingCogsItems} filename="data_quality_missing_cogs" />}
        </div>
        <p className="text-sm text-muted-foreground">
          Trust dashboard for profitability and finance mapping
        </p>
      </div>

      <div className="rounded-lg border border-border bg-card px-3 py-2 text-xs text-muted-foreground">
        Filters: {filters.dateFrom} to {filters.dateTo} | Marketplaces selected:{" "}
        {filters.marketplaceIds.length || "all"} | Confidence floor: {filters.confidenceMin}%
      </div>

      {isLoading && (
        <div className="py-20 text-center text-muted-foreground">Loading...</div>
      )}

      {ovr && (
        <>
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <CoverageGauge
              label="COGS Coverage"
              value={ovr.cogs_coverage_pct}
              icon={ShieldCheck}
            />
            <CoverageGauge
              label="FBA Fee Coverage"
              value={ovr.fba_fee_coverage_pct}
              icon={Shield}
            />
            <CoverageGauge
              label="Referral Fee Coverage"
              value={ovr.referral_fee_coverage_pct}
              icon={Shield}
            />
            <CoverageGauge
              label="Product Mapping"
              value={ovr.product_mapping_pct}
              icon={BarChart3}
            />
          </div>

          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <div className="rounded-lg border border-border bg-card p-4">
              <div className="mb-1 text-[10px] uppercase tracking-wider text-muted-foreground">
                Purchase Price Coverage
              </div>
              <div className="text-xl font-bold">{formatPct(ovr.purchase_price_coverage_pct)}</div>
            </div>
            <div className="rounded-lg border border-border bg-card p-4">
              <div className="mb-1 text-[10px] uppercase tracking-wider text-muted-foreground">
                Finance Match
              </div>
              <div className="text-xl font-bold">{formatPct(ovr.finance_match_pct)}</div>
            </div>
            <div className="rounded-lg border border-border bg-card p-4">
              <div className="mb-1 text-[10px] uppercase tracking-wider text-muted-foreground">
                FX Rate Coverage
              </div>
              <div className="flex items-center gap-2">
                <div className="text-xl font-bold">{ovr.fx_rate_coverage}</div>
                <StatusDot ok={ovr.fx_rate_coverage.split("/")[0] === ovr.fx_rate_coverage.split("/")[1]} />
              </div>
            </div>
            <div className="rounded-lg border border-border bg-card p-4">
              <div className="mb-1 text-[10px] uppercase tracking-wider text-muted-foreground">
                Overall Confidence
              </div>
              <div className={cn(
                "text-xl font-bold",
                ovr.cogs_coverage_pct >= 90 && ovr.fba_fee_coverage_pct >= 80
                  ? "text-green-400"
                  : ovr.cogs_coverage_pct >= 70
                  ? "text-yellow-400"
                  : "text-red-400"
              )}>
                {ovr.cogs_coverage_pct >= 90 && ovr.fba_fee_coverage_pct >= 80
                  ? "HIGH"
                  : ovr.cogs_coverage_pct >= 70
                  ? "MEDIUM"
                  : "LOW"}
              </div>
            </div>
          </div>

          {data?.by_marketplace && data.by_marketplace.length > 0 && (
            <div>
              <h2 className="mb-3 text-sm font-semibold">Coverage by Marketplace</h2>
              <div className="overflow-hidden rounded-xl border border-border bg-card">
                <table className="w-full text-[11px]">
                  <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
                    <tr>
                      <th className="px-2 py-2">Marketplace</th>
                      <th className="px-2 py-2 text-right">Lines</th>
                      <th className="px-2 py-2 text-right">COGS %</th>
                      <th className="px-2 py-2 text-right">Fees %</th>
                      <th className="px-2 py-2">Status</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {data.by_marketplace.map((m) => (
                      <tr key={m.marketplace_id} className="hover:bg-muted/20 transition-colors">
                        <td className="px-2 py-1.5">
                          <span className="rounded bg-muted px-2 py-0.5 text-[10px] font-semibold">
                            {m.marketplace_code}
                          </span>
                        </td>
                        <td className="px-2 py-1.5 text-right tabular-nums">
                          {m.total_lines.toLocaleString()}
                        </td>
                        <td className={cn(
                          "px-2 py-1.5 text-right tabular-nums font-medium",
                          m.cogs_coverage_pct >= 90
                            ? "text-green-400"
                            : m.cogs_coverage_pct >= 70
                            ? "text-yellow-400"
                            : "text-red-400"
                        )}>
                          {formatPct(m.cogs_coverage_pct)}
                        </td>
                        <td className={cn(
                          "px-2 py-1.5 text-right tabular-nums font-medium",
                          m.fees_coverage_pct >= 90
                            ? "text-green-400"
                            : m.fees_coverage_pct >= 70
                            ? "text-yellow-400"
                            : "text-red-400"
                        )}>
                          {formatPct(m.fees_coverage_pct)}
                        </td>
                        <td className="px-2 py-1.5">
                          {m.cogs_coverage_pct >= 80 && m.fees_coverage_pct >= 80 ? (
                            <ShieldCheck className="h-4 w-4 text-green-400" />
                          ) : m.cogs_coverage_pct >= 50 || m.fees_coverage_pct >= 50 ? (
                            <Shield className="h-4 w-4 text-yellow-400" />
                          ) : (
                            <ShieldAlert className="h-4 w-4 text-red-400" />
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {missingCogsItems.length > 0 && (
            <div>
              <div className="mb-3 flex items-center gap-2">
                <AlertTriangle className="h-4 w-4 text-red-400" />
                <h2 className="text-sm font-semibold">Missing COGS - Top SKUs by Revenue</h2>
                <span className="text-[10px] text-muted-foreground ml-2">
                  ({visibleMissingCogs.length}/{missingCogsItems.length} pozycji)
                </span>
                <button
                  type="button"
                  onClick={() => setShowOnlySuggestedRows((prev) => !prev)}
                  className={cn(
                    "ml-auto rounded border px-2 py-1 text-[10px] transition-colors",
                    showOnlySuggestedRows
                      ? "border-amazon/40 bg-amazon/10 text-amazon"
                      : "border-border bg-card text-muted-foreground hover:text-foreground"
                  )}
                >
                  {showOnlySuggestedRows ? "Pokaż wszystkie wiersze" : "Pokaż tylko wiersze z sugestią"}
                </button>
              </div>
              <div className="overflow-hidden rounded-xl border border-border bg-card">
                <table className="w-full text-[11px]">
                  <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
                    <tr>
                      <th className="px-2 py-2">SKU</th>
                      <th className="px-2 py-2">ASIN</th>
                      <th className="px-2 py-2">Internal SKU</th>
                      <th className="px-2 py-2">Sugestia</th>
                      <th className="px-2 py-2 text-right">Units</th>
                      <th className="px-2 py-2 text-right">Revenue (orig)</th>
                      <th className="px-2 py-2 text-right">Lines</th>
                      <th className="px-2 py-2 text-right">Cena zakupu (PLN netto)</th>
                      <th className="px-2 py-2 w-10"></th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {visibleMissingCogs.map((item) => {
                      const isk = item.internal_sku;
                      const hasIsk = !!isk;
                      const hardSuggestion = item.hard_suggestion;
                      const aiCandidate = item.ai_candidate;
                      // Key for state: use internal_sku if available, else sku
                      const stateKey = isk || item.sku;
                      const isSaved = savedSkus.has(stateKey);
                      const isSaving =
                        (priceMutation.isPending &&
                          priceMutation.variables?.internal_sku === stateKey) ||
                        (mapMutation.isPending &&
                          mapMutation.variables?.sku === item.sku);

                      return (
                        <tr
                          key={item.sku}
                          className={cn(
                            "transition-colors",
                            hasMissingCogsSuggestion(item)
                              ? "bg-emerald-500/[0.03] hover:bg-emerald-500/[0.06]"
                              : "hover:bg-muted/20"
                          )}
                        >
                          <td className="px-2 py-1.5 font-mono font-medium">{item.sku}</td>
                          <td className="px-2 py-1.5 text-muted-foreground">{item.asin || "-"}</td>
                          <td className="px-2 py-1.5 font-mono text-muted-foreground text-[10px]">
                            <div className="flex min-w-[140px] flex-col gap-1">
                              {hasIsk ? (
                                <span>{isk}</span>
                              ) : (
                                <input
                                  type="text"
                                  placeholder="wpisz internal_sku"
                                  value={editIsks[item.sku] ?? ""}
                                  onChange={(e) =>
                                    setEditIsks((prev) => ({
                                      ...prev,
                                      [item.sku]: e.target.value,
                                    }))
                                  }
                                  className={cn(
                                    "w-36 rounded border border-border bg-background px-2 py-1 text-xs font-mono",
                                    "focus:outline-none focus:ring-1 focus:ring-amazon",
                                    "placeholder:text-red-400/60",
                                    isSaved && "border-green-500"
                                  )}
                                />
                              )}
                            </div>
                          </td>
                          <td className="px-2 py-1.5 align-top">
                            <div className="flex min-w-[240px] flex-col gap-1">
                              {hardSuggestion && (
                                <div className="rounded border border-emerald-500/20 bg-emerald-500/[0.05] px-2 py-1.5">
                                  <div className="flex flex-wrap items-center gap-2">
                                    <SuggestionBadge label="Hard source" tone="hard" />
                                    <span className="text-[10px] text-muted-foreground">
                                      {hardSuggestion.source_label}
                                    </span>
                                  </div>
                                  <div className="mt-1 text-[10px] text-foreground">
                                    {hardSuggestion.suggested_internal_sku && (
                                      <span className="mr-2 font-mono">
                                        ISK: {hardSuggestion.suggested_internal_sku}
                                      </span>
                                    )}
                                    {hardSuggestion.suggested_price_pln != null && (
                                      <span>
                                        Cena: {formatPriceInput(hardSuggestion.suggested_price_pln)} PLN
                                      </span>
                                    )}
                                  </div>
                                  <button
                                    type="button"
                                    onClick={() => applyHardSuggestion(item)}
                                    className="mt-1 text-[10px] text-amazon hover:underline"
                                    title={hardSuggestion.note ?? hardSuggestion.source_label}
                                  >
                                    Wstaw do formularza
                                  </button>
                                </div>
                              )}

                              {aiCandidate && (
                                <div className="rounded border border-violet-500/20 bg-violet-500/[0.05] px-2 py-1.5">
                                  <div className="flex flex-wrap items-center gap-2">
                                    <SuggestionBadge
                                      label={aiCandidate.hard_price_pln != null ? "AI + hard source" : "AI candidate"}
                                      tone={aiCandidate.hard_price_pln != null ? "ai-hard" : "ai"}
                                    />
                                    <span className="text-[10px] text-muted-foreground">
                                      {Math.round(aiCandidate.confidence)}%
                                    </span>
                                  </div>
                                  <div className="mt-1 text-[10px] text-foreground">
                                    {aiCandidate.matched_internal_sku && (
                                      <span className="mr-2 font-mono">
                                        ISK: {aiCandidate.matched_internal_sku}
                                      </span>
                                    )}
                                    {aiCandidate.hard_price_pln != null && (
                                      <span>
                                        Cena: {formatPriceInput(aiCandidate.hard_price_pln)} PLN
                                      </span>
                                    )}
                                  </div>
                                  <div className="mt-1 text-[10px] text-muted-foreground">
                                    {aiCandidate.hard_price_pln != null
                                      ? `Źródło ceny: ${aiCandidate.hard_price_source ?? "twarde źródło"}`
                                      : "AI wskazuje kandydata produktu, ale bez twardej ceny"}
                                  </div>
                                  <button
                                    type="button"
                                    onClick={() => applyAICandidate(item)}
                                    className="mt-1 text-[10px] text-violet-300 hover:underline"
                                    title={aiCandidate.reasoning ?? "Kandydat AI"}
                                  >
                                    Wstaw do formularza
                                  </button>
                                </div>
                              )}

                              {!hardSuggestion && !aiCandidate && (
                                <span className="text-[10px] text-muted-foreground">Brak sugestii</span>
                              )}
                            </div>
                          </td>
                          <td className="px-2 py-1.5 text-right tabular-nums">
                            {item.units.toLocaleString()}
                          </td>
                          <td className="px-2 py-1.5 text-right tabular-nums">
                            {item.revenue_orig.toLocaleString("pl-PL", {
                              minimumFractionDigits: 2,
                              maximumFractionDigits: 2,
                            })}
                          </td>
                          <td className="px-2 py-1.5 text-right tabular-nums">
                            {item.line_count.toLocaleString()}
                          </td>
                          <td className="px-2 py-1.5 text-right">
                            <div className="flex flex-col items-end gap-1">
                              <div className="flex items-center justify-end gap-1">
                                {item.current_price_pln != null && (
                                  <span className="text-[10px] text-muted-foreground mr-1">
                                    ({formatPriceInput(item.current_price_pln)})
                                  </span>
                                )}
                                <input
                                  type="text"
                                  inputMode="decimal"
                                  placeholder={item.current_price_pln != null ? formatPriceInput(item.current_price_pln) : "0,00"}
                                  value={editPrices[stateKey] ?? ""}
                                  onChange={(e) =>
                                    setEditPrices((prev) => ({
                                      ...prev,
                                      [stateKey]: e.target.value,
                                    }))
                                  }
                                  onKeyDown={(e) => {
                                    if (e.key === "Enter") {
                                      hasIsk ? handleSavePrice(isk!) : handleMapAndPrice(item.sku);
                                    }
                                  }}
                                  className={cn(
                                    "w-24 rounded border border-border bg-background px-2 py-1 text-right text-xs tabular-nums",
                                    "focus:outline-none focus:ring-1 focus:ring-amazon",
                                    isSaved && "border-green-500"
                                  )}
                                />
                              </div>

                              {item.current_price_source && (
                                <div className="text-[10px] text-muted-foreground">
                                  Źródło: {item.current_price_source}
                                </div>
                              )}
                            </div>
                          </td>
                          <td className="px-2 py-1.5">
                            <button
                              onClick={() =>
                                hasIsk ? handleSavePrice(isk!) : handleMapAndPrice(item.sku)
                              }
                              disabled={
                                hasIsk
                                  ? !editPrices[stateKey] || isSaving
                                  : !editIsks[item.sku] || !editPrices[stateKey] || isSaving
                              }
                              className={cn(
                                "rounded p-1 transition-colors",
                                (hasIsk ? editPrices[stateKey] : editIsks[item.sku] && editPrices[stateKey])
                                  ? "text-amazon hover:bg-amazon/10 cursor-pointer"
                                  : "text-muted-foreground/30 cursor-not-allowed",
                                isSaved && "text-green-400"
                              )}
                              title={hasIsk ? "Zapisz cenę zakupu" : "Zapisz mapowanie + cenę"}
                            >
                              {isSaving ? (
                                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                              ) : isSaved ? (
                                <CheckCircle className="h-3.5 w-3.5" />
                              ) : (
                                <Save className="h-3.5 w-3.5" />
                              )}
                            </button>
                          </td>
                        </tr>
                      );
                    })}
                    {visibleMissingCogs.length === 0 && (
                      <tr>
                        <td colSpan={9} className="px-3 py-6 text-center text-xs text-muted-foreground">
                          Brak pozycji z gotową bezpieczną sugestią ceny lub mapowania.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
              {(priceMutation.isError || mapMutation.isError) && (
                <div className="mt-2 rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
                  Błąd zapisu: {((priceMutation.error || mapMutation.error) as Error)?.message ?? "Nieznany błąd"}
                </div>
              )}
            </div>
          )}

          {/* AI Match Suggestions Panel */}
          <AIMatchPanel />
        </>
      )}
    </div>
  );
}
