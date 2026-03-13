import { useQuery } from "@tanstack/react-query";
import {
  Brain,
  TrendingUp,
  TrendingDown,
  Target,
  RefreshCw,
  CheckCircle2,
  AlertTriangle,
  Info,
} from "lucide-react";
import {
  getLearningDashboard,
  getWeeklyReport,
  triggerLearningAggregation,
  triggerModelRecalibration,
} from "@/lib/api";
import type { LearningEntry, ModelAdjustment, WeeklyReport } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Legend,
} from "recharts";

/* ── Opportunity type → human-readable Polish name ── */
const TYPE_LABELS: Record<string, string> = {
  PRICE_INCREASE: "Podwyżka ceny",
  PRICE_DECREASE: "Obniżka ceny",
  ADS_SCALE_UP: "Zwiększenie reklam",
  ADS_CUT_WASTE: "Redukcja marnotrawnych reklam",
  CONTENT_FIX: "Poprawa contentu",
  STOCK_REPLENISH: "Uzupełnienie zapasu",
  MARKETPLACE_EXPANSION: "Ekspansja na rynek",
  BUNDLE_CREATE: "Tworzenie bundla",
  CATEGORY_WINNER_SCALE: "Skalowanie lidera kategorii",
};
const typeLabel = (t: string) => TYPE_LABELS[t] ?? t.replace(/_/g, " ");

const ACCURACY_COLORS = (v: number) =>
  v >= 0.85 ? "text-emerald-400" : v >= 0.6 ? "text-amber-400" : "text-red-400";
const ACCURACY_BG = (v: number) =>
  v >= 0.85 ? "bg-emerald-500/10" : v >= 0.6 ? "bg-amber-500/10" : "bg-red-500/10";

export default function StrategyLearningPage() {
  const [running, setRunning] = useState<string | null>(null);

  const { data: dashboard, isLoading, refetch } = useQuery({
    queryKey: ["decision-learning"],
    queryFn: getLearningDashboard,
    staleTime: 60_000,
  });

  const { data: report } = useQuery({
    queryKey: ["decision-weekly-report"],
    queryFn: getWeeklyReport,
    staleTime: 60_000,
  });

  const learning: LearningEntry[] = dashboard?.learning ?? [];
  const adjustments: ModelAdjustment[] = dashboard?.adjustments ?? [];
  const summary = dashboard?.summary;

  const handleRun = async (type: "aggregate" | "recalibrate") => {
    setRunning(type);
    try {
      if (type === "aggregate") await triggerLearningAggregation();
      else await triggerModelRecalibration();
      refetch();
    } finally {
      setRunning(null);
    }
  };

  // Chart data: predicted vs actual per type
  const chartData = learning.map((l) => ({
    name: typeLabel(l.opportunity_type),
    type: l.opportunity_type,
    expected: l.avg_expected_profit,
    actual: l.avg_actual_profit,
    accuracy: l.prediction_accuracy,
    sample: l.sample_size,
  }));

  return (
    <div className="space-y-5 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight flex items-center gap-2">
            <Brain className="h-6 w-6 text-amazon" /> Uczenie się systemu
          </h1>
          <p className="text-sm text-muted-foreground mt-0.5">
            System analizuje, które strategie przynoszą najlepsze wyniki i automatycznie dostosowuje priorytety
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => handleRun("aggregate")}
            disabled={running !== null}
            className="flex items-center gap-1.5 rounded-lg border border-border px-3 py-2 text-sm hover:bg-muted disabled:opacity-50"
          >
            <RefreshCw className={cn("h-4 w-4", running === "aggregate" && "animate-spin")} />
            Przelicz statystyki
          </button>
          <button
            onClick={() => handleRun("recalibrate")}
            disabled={running !== null}
            className="flex items-center gap-1.5 rounded-lg bg-amazon px-4 py-2 text-sm font-medium text-black hover:bg-amazon/80 disabled:opacity-50"
          >
            <RefreshCw className={cn("h-4 w-4", running === "recalibrate" && "animate-spin")} />
            Rekalibruj model
          </button>
        </div>
      </div>

      {/* Explanation banner */}
      <div className="rounded-xl border border-blue-500/20 bg-blue-500/5 p-4 flex gap-3 items-start">
        <Info className="h-5 w-5 text-blue-400 mt-0.5 shrink-0" />
        <div className="text-sm text-muted-foreground space-y-1">
          <p>
            Ta strona pokazuje jak dobrze system przewiduje wyniki Twoich decyzji biznesowych. 
            Każda zaakceptowana rekomendacja (np. podwyżka ceny, zwiększenie reklam) jest monitorowana — 
            porównujemy prognozę z rzeczywistym wynikiem sprzedaży.
          </p>
          <p>
            <strong className="text-foreground">Trafność prognozy</strong> = jak blisko realnych wyników były nasze predykcje. 
            <strong className="text-foreground"> Win Rate</strong> = % decyzji, które przyniosły oczekiwany efekt. 
            <strong className="text-foreground"> ROI</strong> = stosunek rzeczywistego zysku do prognozowanego.
          </p>
        </div>
      </div>

      {/* Summary Cards */}
      {summary && (
        <div className="grid grid-cols-5 gap-4">
          {[
            {
              label: "Typy strategii",
              value: summary.types_tracked,
              icon: <Target className="h-4 w-4 text-blue-400" />,
              sub: "Śledzone rodzaje decyzji",
            },
            {
              label: "Trafność prognoz",
              value: (summary.avg_prediction_accuracy * 100).toFixed(0) + "%",
              icon: <Brain className="h-4 w-4 text-amazon" />,
              color: ACCURACY_COLORS(summary.avg_prediction_accuracy),
              sub: summary.avg_prediction_accuracy >= 0.75 ? "Dobra jakość predykcji" : "Wymaga poprawy",
            },
            {
              label: "Win Rate",
              value: (summary.avg_win_rate * 100).toFixed(0) + "%",
              icon: <CheckCircle2 className="h-4 w-4 text-green-400" />,
              sub: "Decyzji z pozytywnym wynikiem",
            },
            {
              label: "Ocenionych decyzji",
              value: summary.total_evaluations,
              icon: <TrendingUp className="h-4 w-4 text-emerald-400" />,
              sub: "Łącznie przeanalizowanych",
            },
            {
              label: "Średni ROI",
              value: summary.avg_roi ? summary.avg_roi.toFixed(2) + "x" : "—",
              icon: <TrendingUp className="h-4 w-4 text-amazon" />,
              sub: summary.avg_roi && summary.avg_roi >= 1 ? "Zysk > prognoza" : "Zysk < prognoza",
            },
          ].map((card) => (
            <div key={card.label} className="rounded-xl border border-border bg-card p-4">
              <div className="flex items-center gap-2">
                {card.icon}
                <p className="text-xs text-muted-foreground">{card.label}</p>
              </div>
              <p className={cn("mt-1 text-2xl font-bold tabular-nums", (card as { color?: string }).color)}>
                {card.value}
              </p>
              <p className="text-[10px] text-muted-foreground mt-0.5">{card.sub}</p>
            </div>
          ))}
        </div>
      )}

      {/* Chart - Predicted vs Actual */}
      <div className="rounded-xl border border-border bg-card p-4">
        <h3 className="text-sm font-bold mb-1">Prognoza vs Rzeczywistość (średni zysk wg typu strategii)</h3>
        <p className="text-xs text-muted-foreground mb-3">
          Niebieski = prognozowany zysk na decyzję, zielony = rzeczywisty wynik. Im bliżej siebie, tym lepsza predykcja.
        </p>
        <div className="h-[280px]">
          {chartData.length > 0 ? (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData} barGap={4}>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                <XAxis dataKey="name" tick={{ fontSize: 10, fill: "#888" }} angle={-15} textAnchor="end" height={60} />
                <YAxis tick={{ fontSize: 10, fill: "#888" }} tickFormatter={(v) => `${v} zł`} />
                <Tooltip
                  contentStyle={{ backgroundColor: "#1a1a2e", border: "1px solid #333", borderRadius: 8, fontSize: 12 }}
                  formatter={(value: number, name: string) => [
                    `${value.toFixed(0)} zł`,
                    name === "expected" ? "Prognoza" : "Rzeczywistość",
                  ]}
                  labelFormatter={(label) => `Strategia: ${label}`}
                />
                <Legend formatter={(value) => (value === "expected" ? "Prognoza" : "Rzeczywistość")} />
                <Bar dataKey="expected" name="expected" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                <Bar dataKey="actual" name="actual" fill="#10b981" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <p className="text-muted-foreground text-sm text-center pt-20">Brak danych — uruchom przeliczenie statystyk</p>
          )}
        </div>
      </div>

      {/* Learning Table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        <div className="px-4 py-3 border-b border-border">
          <h3 className="text-sm font-bold">Skuteczność wg typu strategii</h3>
          <p className="text-xs text-muted-foreground mt-0.5">
            Każdy typ decyzji jest oceniany osobno — widać, które strategie działają najlepiej
          </p>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              <th className="px-3 py-2 text-left">Typ strategii</th>
              <th className="px-3 py-2 text-right">Próbka</th>
              <th className="px-3 py-2 text-right">Śr. prognoza</th>
              <th className="px-3 py-2 text-right">Śr. wynik</th>
              <th className="px-3 py-2 text-center">Trafność</th>
              <th className="px-3 py-2 text-right">Win Rate</th>
              <th className="px-3 py-2 text-right">ROI</th>
              <th className="px-3 py-2 text-left">Aktualizacja</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <tr key={i} className="border-b border-border/50">
                  <td colSpan={8} className="px-3 py-3">
                    <div className="h-4 bg-muted/30 rounded animate-pulse" />
                  </td>
                </tr>
              ))
            ) : learning.length === 0 ? (
              <tr>
                <td colSpan={8} className="px-3 py-8 text-center text-muted-foreground">
                  Brak danych — uruchom ewaluację decyzji, a potem przelicz statystyki
                </td>
              </tr>
            ) : (
              learning.map((l) => {
                const accuracyPct = (l.prediction_accuracy * 100).toFixed(0);
                return (
                  <tr key={l.opportunity_type} className="border-b border-border/50 hover:bg-muted/20">
                    <td className="px-3 py-2 text-xs font-medium">
                      {typeLabel(l.opportunity_type)}
                    </td>
                    <td className="px-3 py-2 text-right text-xs tabular-nums">{l.sample_size}</td>
                    <td className="px-3 py-2 text-right text-xs tabular-nums">
                      {l.avg_expected_profit.toFixed(0)} zł
                    </td>
                    <td className="px-3 py-2 text-right text-xs tabular-nums font-medium">
                      {l.avg_actual_profit.toFixed(0)} zł
                    </td>
                    <td className="px-3 py-2 text-center">
                      <span
                        className={cn(
                          "inline-flex items-center rounded-full px-2 py-0.5 text-xs font-bold tabular-nums",
                          ACCURACY_BG(l.prediction_accuracy),
                          ACCURACY_COLORS(l.prediction_accuracy)
                        )}
                      >
                        {accuracyPct}%
                      </span>
                    </td>
                    <td className="px-3 py-2 text-right text-xs tabular-nums">
                      {(l.win_rate * 100).toFixed(0)}%
                    </td>
                    <td className="px-3 py-2 text-right text-xs tabular-nums">
                      {l.avg_roi != null ? l.avg_roi.toFixed(2) + "x" : "—"}
                    </td>
                    <td className="px-3 py-2 text-xs text-muted-foreground">
                      {l.last_updated?.slice(0, 10) ?? "—"}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {/* Model Adjustments — simplified */}
      {adjustments.length > 0 && (
        <div className="rounded-xl border border-border bg-card p-4">
          <h3 className="text-sm font-bold mb-1">Automatyczne korekty modelu</h3>
          <p className="text-xs text-muted-foreground mb-3">
            Na podstawie wyników system automatycznie dostosowuje priorytety — strategie z dobrymi wynikami dostają wyższy priorytet
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {adjustments.map((a) => {
              const isPositive = a.priority_weight_adjustment > 0;
              const isNegative = a.priority_weight_adjustment < 0;
              return (
                <div
                  key={a.opportunity_type}
                  className={cn(
                    "rounded-lg border p-3 flex items-start gap-3",
                    isPositive ? "border-green-500/20 bg-green-500/5" : isNegative ? "border-red-500/20 bg-red-500/5" : "border-border"
                  )}
                >
                  <div className="mt-0.5">
                    {isPositive ? (
                      <TrendingUp className="h-4 w-4 text-green-400" />
                    ) : isNegative ? (
                      <TrendingDown className="h-4 w-4 text-red-400" />
                    ) : (
                      <Target className="h-4 w-4 text-muted-foreground" />
                    )}
                  </div>
                  <div>
                    <p className="text-xs font-medium">{typeLabel(a.opportunity_type)}</p>
                    <p className="text-[11px] text-muted-foreground mt-0.5">{a.reason}</p>
                    <p className="text-[10px] text-muted-foreground mt-1">
                      Ostatnia kalibracja: {a.updated_at?.slice(0, 10) ?? "—"}
                    </p>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Weekly Report */}
      {report && (
        <div className="rounded-xl border border-border bg-card p-4 space-y-4">
          <div>
            <h3 className="text-sm font-bold">
              Raport tygodniowy ({report.period_start} — {report.period_end})
            </h3>
            <p className="text-xs text-muted-foreground mt-0.5">
              Podsumowanie wyników decyzji ocenionych w ostatnich 7 dniach
            </p>
          </div>

          <div className="grid grid-cols-3 gap-4 text-sm">
            <div className="rounded bg-muted/20 p-3">
              <p className="text-xs text-muted-foreground">Ocenionych w tym tygodniu</p>
              <p className="text-xl font-bold">{report.total_evaluated}</p>
            </div>
            <div className="rounded bg-muted/20 p-3">
              <p className="text-xs text-muted-foreground">Udanych decyzji</p>
              <p className="text-xl font-bold text-green-400">{report.total_success}</p>
            </div>
            <div className="rounded bg-muted/20 p-3">
              <p className="text-xs text-muted-foreground">Trafność prognoz</p>
              <p className={cn("text-xl font-bold", ACCURACY_COLORS(report.prediction_accuracy))}>
                {(report.prediction_accuracy * 100).toFixed(0)}%
              </p>
            </div>
          </div>

          {/* Top / Worst */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <h4 className="text-xs font-bold uppercase text-muted-foreground flex items-center gap-1 mb-2">
                <TrendingUp className="h-3 w-3 text-green-400" /> Najlepsze wyniki
              </h4>
              {report.top_performing.length === 0 ? (
                <p className="text-xs text-muted-foreground">Brak danych w tym tygodniu</p>
              ) : (
                <div className="space-y-1.5">
                  {report.top_performing.map((item, i) => (
                    <div key={i} className="rounded bg-green-500/5 px-3 py-2 text-xs">
                      <div className="flex items-center justify-between">
                        <span className="font-medium truncate max-w-[240px]" title={item.product_title ?? item.title ?? ""}>
                          {item.product_title ?? item.sku ?? "—"}
                        </span>
                        <span className="font-bold text-green-400 ml-2 shrink-0">
                          {Math.round(item.success_score * 100)}%
                        </span>
                      </div>
                      <div className="text-[10px] text-muted-foreground mt-0.5 flex gap-2">
                        <span>{typeLabel(item.opportunity_type)}</span>
                        <span>·</span>
                        <span>{item.marketplace_code}</span>
                        {item.profit_delta != null && (
                          <>
                            <span>·</span>
                            <span className="text-green-400">+{item.profit_delta.toFixed(0)} zł</span>
                          </>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div>
              <h4 className="text-xs font-bold uppercase text-muted-foreground flex items-center gap-1 mb-2">
                <TrendingDown className="h-3 w-3 text-red-400" /> Najsłabsze wyniki
              </h4>
              {report.worst_performing.length === 0 ? (
                <p className="text-xs text-muted-foreground">Brak danych w tym tygodniu</p>
              ) : (
                <div className="space-y-1.5">
                  {report.worst_performing.map((item, i) => (
                    <div key={i} className="rounded bg-red-500/5 px-3 py-2 text-xs">
                      <div className="flex items-center justify-between">
                        <span className="font-medium truncate max-w-[240px]" title={item.product_title ?? item.title ?? ""}>
                          {item.product_title ?? item.sku ?? "—"}
                        </span>
                        <span className="font-bold text-red-400 ml-2 shrink-0">
                          {Math.round(item.success_score * 100)}%
                        </span>
                      </div>
                      <div className="text-[10px] text-muted-foreground mt-0.5 flex gap-2">
                        <span>{typeLabel(item.opportunity_type)}</span>
                        <span>·</span>
                        <span>{item.marketplace_code}</span>
                        {item.profit_delta != null && item.profit_delta < 0 && (
                          <>
                            <span>·</span>
                            <span className="text-red-400">{item.profit_delta.toFixed(0)} zł</span>
                          </>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Insights */}
          {report.insights.length > 0 && (
            <div className="rounded bg-amazon/5 border border-amazon/20 p-3">
              <h4 className="text-xs font-bold uppercase text-amazon mb-2 flex items-center gap-1">
                <AlertTriangle className="h-3 w-3" /> Wnioski systemowe
              </h4>
              <ul className="text-xs space-y-1 text-muted-foreground">
                {report.insights.map((ins, i) => (
                  <li key={i}>• {ins}</li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
