import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { BarChart3, RefreshCw, Eye, TrendingUp, TrendingDown, CheckCircle2, Clock } from "lucide-react";
import { getDecisionOutcomes, getExecutionDetail, triggerOutcomeEvaluation } from "@/lib/api";
import type { OutcomeExecution, ExecutionDetail } from "@/lib/api";
import { cn } from "@/lib/utils";

const SUCCESS_COLORS: Record<string, string> = {
  overperformed: "bg-emerald-500/15 text-emerald-400",
  on_target: "bg-green-500/15 text-green-400",
  partial_success: "bg-amber-500/15 text-amber-400",
  failure: "bg-red-500/15 text-red-400",
};

const STATUS_COLORS: Record<string, string> = {
  monitoring: "bg-blue-500/15 text-blue-400",
  evaluated: "bg-emerald-500/15 text-emerald-400",
  expired: "bg-zinc-500/15 text-zinc-400",
};

const STATUS_TABS = [
  { id: "", label: "All" },
  { id: "monitoring", label: "Monitoring" },
  { id: "evaluated", label: "Evaluated" },
  { id: "expired", label: "Expired" },
];

const TYPE_TABS = [
  { id: "", label: "All Types" },
  { id: "PRICE_INCREASE", label: "Price ↑" },
  { id: "PRICE_DECREASE", label: "Price ↓" },
  { id: "ADS_SCALE_UP", label: "Ads Scale" },
  { id: "ADS_CUT_WASTE", label: "Ads Cut" },
  { id: "CONTENT_FIX", label: "Content" },
  { id: "STOCK_REPLENISH", label: "Stock" },
  { id: "MARKETPLACE_EXPANSION", label: "Expansion" },
  { id: "BUNDLE_CREATE", label: "Bundle" },
];

export default function StrategyOutcomesPage() {
  const [statusFilter, setStatusFilter] = useState("");
  const [typeFilter, setTypeFilter] = useState("");
  const [page, setPage] = useState(1);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [evaluating, setEvaluating] = useState(false);

  const { data, isLoading, refetch } = useQuery({
    queryKey: ["decision-outcomes", statusFilter, typeFilter, page],
    queryFn: () =>
      getDecisionOutcomes({
        page,
        page_size: 50,
        status: statusFilter || undefined,
        opportunity_type: typeFilter || undefined,
      }),
    staleTime: 30_000,
  });

  const { data: detail } = useQuery({
    queryKey: ["execution-detail", selectedId],
    queryFn: () => getExecutionDetail(selectedId!),
    enabled: !!selectedId,
  });

  const items: OutcomeExecution[] = data?.items ?? [];
  const total = data?.total ?? 0;
  const pages = data?.pages ?? 0;

  const handleEvaluate = async () => {
    setEvaluating(true);
    try {
      await triggerOutcomeEvaluation();
      refetch();
    } finally {
      setEvaluating(false);
    }
  };

  return (
    <div className="space-y-5 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight flex items-center gap-2">
            <BarChart3 className="h-6 w-6 text-amazon" /> Decision Outcomes
          </h1>
          <p className="text-sm text-muted-foreground mt-0.5">
            Wyniki wdrożonych rekomendacji — porównanie prognoz z rzeczywistymi wynikami sprzedaży
          </p>
        </div>
        <button
          onClick={handleEvaluate}
          disabled={evaluating}
          className="flex items-center gap-1.5 rounded-lg bg-amazon px-4 py-2 text-sm font-medium text-black hover:bg-amazon/80 disabled:opacity-50"
        >
          <RefreshCw className={cn("h-4 w-4", evaluating && "animate-spin")} />
          {evaluating ? "Evaluating…" : "Run Evaluation"}
        </button>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-4 gap-4">
        {[
          {
            label: "Wdrożone decyzje",
            value: total,
            icon: <CheckCircle2 className="h-4 w-4 text-emerald-400" />,
            sub: "Łącznie rekomendacji przetestowanych",
          },
          {
            label: "W monitoringu",
            value: items.filter((i) => i.status === "monitoring").length,
            icon: <Clock className="h-4 w-4 text-blue-400" />,
            sub: `${items.filter((i) => i.status === "evaluated").length} już ocenionych`,
          },
          {
            label: "Średni sukces",
            value:
              items.filter((i) => i.success_score != null).length > 0
                ? (
                    items
                      .filter((i) => i.success_score != null)
                      .reduce((a, b) => a + (b.success_score ?? 0), 0) /
                    items.filter((i) => i.success_score != null).length
                  ).toFixed(0) + "%"
                : "—",
            icon: <TrendingUp className="h-4 w-4 text-amazon" />,
            sub: "Średnia trafność prognoz",
          },
          {
            label: "Win Rate",
            value:
              items.filter((i) => i.success_score != null).length > 0
                ? (
                    (items.filter((i) => (i.success_score ?? 0) >= 0.8).length /
                      items.filter((i) => i.success_score != null).length) *
                    100
                  ).toFixed(0) + "%"
                : "—",
            icon: <TrendingDown className="h-4 w-4 text-green-400" />,
            sub: "Decyzje z wynikiem ≥ 80%",
          },
        ].map((card) => (
          <div
            key={card.label}
            className="rounded-xl border border-border bg-card p-4"
          >
            <div className="flex items-center gap-2">
              {card.icon}
              <p className="text-xs text-muted-foreground">{card.label}</p>
            </div>
            <p className="mt-1 text-2xl font-bold tabular-nums">{card.value}</p>
            <p className="text-[10px] text-muted-foreground mt-0.5">{card.sub}</p>
          </div>
        ))}
      </div>

      {/* Filters */}
      <div className="flex gap-4 flex-wrap">
        <div className="flex gap-2">
          {STATUS_TABS.map((t) => (
            <button
              key={t.id}
              onClick={() => {
                setStatusFilter(t.id);
                setPage(1);
              }}
              className={cn(
                "rounded-full border px-3 py-1 text-xs font-medium transition",
                statusFilter === t.id
                  ? "border-amazon bg-amazon/15 text-amazon"
                  : "border-border hover:border-amazon/50"
              )}
            >
              {t.label}
            </button>
          ))}
        </div>
        <div className="flex gap-2">
          {TYPE_TABS.map((t) => (
            <button
              key={t.id}
              onClick={() => {
                setTypeFilter(t.id);
                setPage(1);
              }}
              className={cn(
                "rounded-full border px-3 py-1 text-xs font-medium transition",
                typeFilter === t.id
                  ? "border-blue-500 bg-blue-500/15 text-blue-400"
                  : "border-border hover:border-blue-500/50"
              )}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {/* Table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              <th className="px-3 py-2 text-left">Produkt</th>
              <th className="px-3 py-2 text-left">Rodzaj decyzji</th>
              <th className="px-3 py-2 text-left">Rynek</th>
              <th className="px-3 py-2 text-right">Oczekiwany zysk</th>
              <th className="px-3 py-2 text-right">Rzeczywista zmiana</th>
              <th className="px-3 py-2 text-center">Trafność</th>
              <th className="px-3 py-2 text-center">Status</th>
              <th className="px-3 py-2 text-left">Data</th>
              <th className="px-3 py-2 text-center">Szczegóły</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 8 }).map((_, i) => (
                <tr key={i} className="border-b border-border/50">
                  <td colSpan={9} className="px-3 py-3">
                    <div className="h-4 bg-muted/30 rounded animate-pulse" />
                  </td>
                </tr>
              ))
            ) : items.length === 0 ? (
              <tr>
                <td
                  colSpan={9}
                  className="px-3 py-8 text-center text-muted-foreground"
                >
                  Brak wdrożonych decyzji — zaakceptuj rekomendacje w Growth Opportunities aby śledzić wyniki
                </td>
              </tr>
            ) : (
              items.map((ex) => (
                <tr
                  key={ex.execution_id}
                  className="border-b border-border/50 hover:bg-muted/20"
                >
                  {/* Product — name + SKU */}
                  <td className="px-3 py-2 max-w-[260px]">
                    <div
                      className="text-xs font-medium truncate"
                      title={ex.product_title ?? ex.title ?? ""}
                    >
                      {ex.product_title ?? ex.title ?? "—"}
                    </div>
                    <div className="text-[10px] text-muted-foreground font-mono mt-0.5">
                      {ex.sku ?? "—"}
                    </div>
                  </td>
                  {/* Decision type */}
                  <td className="px-3 py-2">
                    <span className="inline-flex items-center gap-1 rounded-full bg-muted/30 px-2 py-0.5 text-[10px] font-medium uppercase">
                      {ex.opportunity_type?.replace(/_/g, " ") ?? "—"}
                    </span>
                  </td>
                  {/* Marketplace */}
                  <td className="px-3 py-2 text-xs">
                    {ex.marketplace_code ?? "—"}
                  </td>
                  {/* Expected profit */}
                  <td className="px-3 py-2 text-right text-xs tabular-nums">
                    {ex.expected_profit?.toFixed(0) ?? "—"} zł
                  </td>
                  {/* Actual delta */}
                  <td
                    className={cn(
                      "px-3 py-2 text-right text-xs tabular-nums font-medium",
                      ex.delta?.profit_delta != null && ex.delta.profit_delta > 0
                        ? "text-green-400"
                        : ex.delta?.profit_delta != null &&
                          ex.delta.profit_delta < 0
                        ? "text-red-400"
                        : ""
                    )}
                  >
                    {ex.delta?.profit_delta != null
                      ? (ex.delta.profit_delta > 0 ? "+" : "") +
                        ex.delta.profit_delta.toFixed(0) +
                        " zł"
                      : "—"}
                  </td>
                  {/* Success score */}
                  <td className="px-3 py-2 text-center">
                    {ex.success_label ? (
                      <span
                        className={cn(
                          "rounded px-1.5 py-0.5 text-[10px] font-bold uppercase",
                          SUCCESS_COLORS[ex.success_label] ?? "bg-muted"
                        )}
                      >
                        {ex.success_label === "overperformed"
                          ? "Powyżej"
                          : ex.success_label === "on_target"
                          ? "W normie"
                          : ex.success_label === "partial_success"
                          ? "Częściowy"
                          : "Porażka"}{" "}
                        · {Math.round((ex.success_score ?? 0) * 100)}%
                      </span>
                    ) : (
                      <span className="text-xs text-muted-foreground">—</span>
                    )}
                  </td>
                  {/* Status */}
                  <td className="px-3 py-2 text-center">
                    <span
                      className={cn(
                        "rounded px-1.5 py-0.5 text-[10px] font-bold uppercase",
                        STATUS_COLORS[ex.status] ?? "bg-muted"
                      )}
                    >
                      {ex.status === "monitoring"
                        ? "Monitoring"
                        : ex.status === "evaluated"
                        ? "Oceniony"
                        : "Wygasły"}
                    </span>
                  </td>
                  {/* Date */}
                  <td className="px-3 py-2 text-xs text-muted-foreground">
                    {ex.executed_at ? ex.executed_at.slice(0, 10) : "—"}
                    {ex.monitoring_days ? (
                      <span className="ml-1 text-[10px]">({ex.monitoring_days}d)</span>
                    ) : null}
                  </td>
                  {/* Detail */}
                  <td className="px-3 py-2 text-center">
                    <button
                      onClick={() => setSelectedId(ex.execution_id)}
                      className="text-muted-foreground hover:text-amazon"
                    >
                      <Eye className="h-4 w-4" />
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {pages > 1 && (
        <div className="flex items-center justify-between text-sm text-muted-foreground">
          <p>
            Page {page} of {pages} ({total} total)
          </p>
          <div className="flex gap-2">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page <= 1}
              className="rounded border border-border px-3 py-1 text-xs hover:bg-muted disabled:opacity-30"
            >
              Prev
            </button>
            <button
              onClick={() => setPage((p) => Math.min(pages, p + 1))}
              disabled={page >= pages}
              className="rounded border border-border px-3 py-1 text-xs hover:bg-muted disabled:opacity-30"
            >
              Next
            </button>
          </div>
        </div>
      )}

      {/* Detail Drawer */}
      {selectedId && detail && (
        <ExecutionDetailDrawer
          detail={detail}
          onClose={() => setSelectedId(null)}
        />
      )}
    </div>
  );
}

/* ────────────────────────────────────────────────────────────────── */
/*  Execution Detail Drawer                                          */
/* ────────────────────────────────────────────────────────────────── */

function ExecutionDetailDrawer({
  detail,
  onClose,
}: {
  detail: ExecutionDetail;
  onClose: () => void;
}) {
  const ex = detail.execution;
  const outcomes = detail.outcomes;

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />
      <div className="relative z-10 w-[520px] bg-card border-l border-border overflow-y-auto shadow-2xl">
        <div className="sticky top-0 bg-card border-b border-border p-4 flex items-center justify-between">
          <div>
            <h2 className="font-bold text-lg">
              {ex.product_title ?? ex.title ?? `Execution #${ex.id}`}
            </h2>
            <p className="text-xs text-muted-foreground mt-0.5">
              {ex.sku} · {ex.opportunity_type?.replace(/_/g, " ")}
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground"
          >
            ✕
          </button>
        </div>

        <div className="p-4 space-y-5">
          {/* Info */}
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div>
              <p className="text-xs text-muted-foreground">Rodzaj decyzji</p>
              <p className="font-medium">
                {ex.opportunity_type?.replace(/_/g, " ")}
              </p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">SKU</p>
              <p className="font-mono text-xs">{ex.sku ?? "—"}</p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">Akcja</p>
              <p>{ex.action_type}</p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">Status</p>
              <span
                className={cn(
                  "rounded px-1.5 py-0.5 text-[10px] font-bold uppercase",
                  STATUS_COLORS[ex.status] ?? "bg-muted"
                )}
              >
                {ex.status}
              </span>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">Okres monitoringu</p>
              <p className="text-xs">
                {ex.monitoring_start?.slice(0, 10)} →{" "}
                {ex.monitoring_end?.slice(0, 10)}
              </p>
            </div>
            <div>
              <p className="text-xs text-muted-foreground">Wdrożony przez</p>
              <p className="text-xs">{ex.executed_by ?? "—"}</p>
            </div>
          </div>

          <hr className="border-border" />

          {/* Baseline Metrics */}
          <div>
            <h3 className="text-xs font-bold uppercase text-muted-foreground mb-2">
              Metryki bazowe (30d przed wdrożeniem)
            </h3>
            <div className="grid grid-cols-3 gap-2">
              {Object.entries(ex.baseline_metrics || {}).map(([k, v]) => (
                <div key={k} className="rounded bg-muted/20 p-2">
                  <p className="text-[10px] text-muted-foreground">
                    {k.replace(/_/g, " ")}
                  </p>
                  <p className="text-sm font-medium tabular-nums">
                    {typeof v === "number" ? v.toFixed(1) : v}
                  </p>
                </div>
              ))}
            </div>
          </div>

          {/* Expected Impact */}
          <div>
            <h3 className="text-xs font-bold uppercase text-muted-foreground mb-2">
              Oczekiwany wpływ
            </h3>
            <div className="grid grid-cols-2 gap-2">
              {Object.entries(ex.expected_metrics || {}).map(([k, v]) => (
                <div key={k} className="rounded bg-blue-500/10 p-2">
                  <p className="text-[10px] text-muted-foreground">
                    {k.replace(/_/g, " ")}
                  </p>
                  <p className="text-sm font-medium tabular-nums text-blue-400">
                    {typeof v === "number" ? v.toFixed(1) : v}
                  </p>
                </div>
              ))}
            </div>
          </div>

          <hr className="border-border" />

          {/* Outcome Windows */}
          <div>
            <h3 className="text-xs font-bold uppercase text-muted-foreground mb-2">
              Okna wyników
            </h3>
            {outcomes.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                Brak jeszcze wyników — monitoring w toku
              </p>
            ) : (
              <div className="space-y-3">
                {outcomes.map((oc) => (
                  <div
                    key={oc.id}
                    className="rounded-lg border border-border p-3 space-y-2"
                  >
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium">
                        Okno {oc.monitoring_days}-dniowe
                      </span>
                      {oc.success_label && (
                        <span
                          className={cn(
                            "rounded px-1.5 py-0.5 text-[10px] font-bold uppercase",
                            SUCCESS_COLORS[oc.success_label] ?? "bg-muted"
                          )}
                        >
                          {oc.success_label.replace(/_/g, " ")} ·{" "}
                          {Math.round((oc.success_score ?? 0) * 100)}%
                        </span>
                      )}
                    </div>

                    {oc.delta && (
                      <div className="grid grid-cols-4 gap-2 text-xs">
                        {Object.entries(oc.delta).map(([k, v]) => (
                          <div key={k}>
                            <p className="text-[10px] text-muted-foreground">
                              {k.replace(/_/g, " ")}
                            </p>
                            <p
                              className={cn(
                                "font-medium tabular-nums",
                                typeof v === "number" && v > 0
                                  ? "text-green-400"
                                  : typeof v === "number" && v < 0
                                  ? "text-red-400"
                                  : ""
                              )}
                            >
                              {typeof v === "number"
                                ? (v > 0 ? "+" : "") + v.toFixed(1)
                                : v}
                            </p>
                          </div>
                        ))}
                      </div>
                    )}

                    <div className="flex gap-4 text-xs text-muted-foreground">
                      <span>
                        Wpływ: {oc.impact_score?.toFixed(0) ?? "—"}
                      </span>
                      <span>
                        Korekta pewności: {oc.confidence_adjustment?.toFixed(2) ?? "—"}
                      </span>
                      <span>
                        Oceniony:{" "}
                        {oc.evaluated_at?.slice(0, 10) ?? "w toku"}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
