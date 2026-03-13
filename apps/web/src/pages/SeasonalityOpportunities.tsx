import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { TrendingUp, Check, X, ChevronLeft, ChevronRight, Info } from "lucide-react";
import {
  getSeasonalityOpportunities,
  acceptSeasonalityOpportunity,
  rejectSeasonalityOpportunity,
} from "@/lib/api";
import type { SeasonalityOpportunity } from "@/lib/api";
import { cn } from "@/lib/utils";

const MONTH_NAMES = ["Sty","Lut","Mar","Kwi","Maj","Cze","Lip","Sie","Wrz","Paź","Lis","Gru"];

const STATUS_COLORS: Record<string, string> = {
  new: "bg-blue-500/15 text-blue-400 border-blue-500/30",
  accepted: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  rejected: "bg-red-500/15 text-red-400 border-red-500/30",
  in_progress: "bg-amber-500/15 text-amber-400 border-amber-500/30",
  done: "bg-gray-500/15 text-gray-400 border-gray-500/30",
};

const STATUS_LABELS: Record<string, string> = {
  new: "Nowa", accepted: "Zaakceptowana", rejected: "Odrzucona",
  in_progress: "W trakcie", done: "Zakończona",
};

const TYPE_LABELS: Record<string, string> = {
  PREPARE_STOCK: "Przygotuj stany",
  PREPARE_CONTENT: "Przygotuj content",
  PREPARE_ADS: "Przygotuj reklamy",
  PREPARE_PRICING: "Przygotuj ceny",
  EXECUTION_GAP: "Luka realizacji",
  PROFIT_PROTECTION: "Ochrona marży",
  LIQUIDATE_POST_SEASON: "Wyprzedaż po sezonie",
  MARKET_EXPANSION_PREP: "Ekspansja rynkowa",
};

const TYPE_COLORS: Record<string, string> = {
  PREPARE_STOCK: "bg-indigo-500/15 text-indigo-400 border-indigo-500/30",
  PREPARE_CONTENT: "bg-cyan-500/15 text-cyan-400 border-cyan-500/30",
  PREPARE_ADS: "bg-amber-500/15 text-amber-400 border-amber-500/30",
  PREPARE_PRICING: "bg-violet-500/15 text-violet-400 border-violet-500/30",
  EXECUTION_GAP: "bg-red-500/15 text-red-400 border-red-500/30",
  PROFIT_PROTECTION: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  LIQUIDATE_POST_SEASON: "bg-orange-500/15 text-orange-400 border-orange-500/30",
  MARKET_EXPANSION_PREP: "bg-blue-500/15 text-blue-400 border-blue-500/30",
};

const ENTITY_LABELS: Record<string, string> = {
  sku: "SKU (produkty)", category: "Kategorie",
};

function SignalBadges({ signals, type }: { signals: Record<string, unknown> | null; type: string }) {
  if (!signals) return null;
  const badges: { label: string; value: string }[] = [];

  const peakMonths = signals.peak_months as number[] | undefined;
  if (peakMonths?.length) {
    badges.push({ label: "Szczyty", value: peakMonths.map(m => MONTH_NAMES[m - 1] ?? "?").join(", ") });
  }
  const justPassed = signals.just_passed as number[] | undefined;
  if (justPassed?.length) {
    badges.push({ label: "Minęły", value: justPassed.map(m => MONTH_NAMES[m - 1] ?? "?").join(", ") });
  }
  if (signals.strength != null) badges.push({ label: "Siła", value: `${Number(signals.strength).toFixed(0)}` });
  if (signals.demand_vs_sales_gap != null) badges.push({ label: "Luka P↔S", value: `${Number(signals.demand_vs_sales_gap).toFixed(2)}` });
  if (signals.sales_strength != null) badges.push({ label: "Sprzedaż", value: `${Number(signals.sales_strength).toFixed(0)}` });
  if (signals.profit_strength != null) badges.push({ label: "Zysk", value: `${Number(signals.profit_strength).toFixed(0)}` });
  if (signals.gap != null) badges.push({ label: "Gap", value: `${Number(signals.gap).toFixed(2)}` });
  if (signals.source_marketplace) badges.push({ label: "Źródło", value: String(signals.source_marketplace) });

  if (!badges.length) return null;
  return (
    <div className="flex flex-wrap gap-1 mt-0.5">
      {badges.map((b, i) => (
        <span key={i} className="rounded bg-zinc-700/40 px-1 py-0.5 text-[9px] text-muted-foreground">
          {b.label}: <strong className="text-foreground">{b.value}</strong>
        </span>
      ))}
    </div>
  );
}

export default function SeasonalityOpportunitiesPage() {
  const qc = useQueryClient();
  const [page, setPage] = useState(1);
  const [mkt, setMkt] = useState("");
  const [type, setType] = useState("");
  const [status, setStatus] = useState("");
  const [entityType, setEntityType] = useState("");

  const { data, isLoading } = useQuery({
    queryKey: ["seasonality-opps", page, mkt, type, status, entityType],
    queryFn: () => getSeasonalityOpportunities({
      page, page_size: 30,
      marketplace: mkt || undefined,
      opportunity_type: type || undefined,
      status: status || undefined,
      entity_type: entityType || undefined,
    }),
    staleTime: 60_000,
  });

  const acceptMut = useMutation({
    mutationFn: (id: number) => acceptSeasonalityOpportunity(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["seasonality-opps"] }),
  });
  const rejectMut = useMutation({
    mutationFn: (id: number) => rejectSeasonalityOpportunity(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["seasonality-opps"] }),
  });

  const items = (data?.items ?? []) as (SeasonalityOpportunity & { product_title?: string | null })[];
  const totalPages = data ? Math.ceil(data.total / data.page_size) : 1;
  const avail = (data as Record<string, unknown> | undefined)?.available_filters as
    { marketplaces: string[]; opportunity_types: string[]; statuses: string[]; entity_types: string[] } | undefined;

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center gap-3">
        <TrendingUp className="h-6 w-6 text-amazon" />
        <div>
          <h1 className="text-xl font-bold tracking-tight">Okazje sezonowe</h1>
          <p className="text-sm text-muted-foreground">{data?.total ?? 0} wykrytych okazji do działania</p>
        </div>
      </div>

      {/* Info banner */}
      <div className="rounded-xl border border-blue-500/30 bg-blue-500/5 p-4 flex gap-3">
        <Info className="h-5 w-5 text-blue-400 mt-0.5 shrink-0" />
        <div className="text-sm text-blue-300/90 space-y-1">
          <p className="font-medium">Jak działają okazje sezonowe?</p>
          <p>System analizuje profile sezonowości i wykrywa konkretne sytuacje wymagające działania:</p>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-x-3 gap-y-0.5 text-xs mt-1">
            <span><strong>Wyprzedaż</strong> — szczyt minął, czas likwidacji stanów</span>
            <span><strong>Ekspansja</strong> — silna sezonowość w jednym mkt, brakuje w innych</span>
            <span><strong>Ochrona marży</strong> — sprzedaż rośnie ale zysk nie nadąża</span>
            <span><strong>Przygotowanie</strong> — stany/content/reklamy/ceny przed szczytem</span>
          </div>
          <p className="text-xs">Kliknij ✓ aby zaakceptować okazję lub ✗ aby ją odrzucić. Sygnały pokazują dane źródłowe wykrycia.</p>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-2">
        <select value={mkt} onChange={e => { setMkt(e.target.value); setPage(1); }}
          className="rounded-lg border border-border bg-background px-2 py-1.5 text-xs">
          <option value="">Wszystkie marketplace&apos;y</option>
          {(avail?.marketplaces ?? []).map(m => <option key={m} value={m}>{m}</option>)}
        </select>
        <select value={type} onChange={e => { setType(e.target.value); setPage(1); }}
          className="rounded-lg border border-border bg-background px-2 py-1.5 text-xs">
          <option value="">Wszystkie typy</option>
          {(avail?.opportunity_types ?? []).map(t => (
            <option key={t} value={t}>{TYPE_LABELS[t] ?? t}</option>
          ))}
        </select>
        <select value={status} onChange={e => { setStatus(e.target.value); setPage(1); }}
          className="rounded-lg border border-border bg-background px-2 py-1.5 text-xs">
          <option value="">Wszystkie statusy</option>
          {(avail?.statuses ?? []).map(s => (
            <option key={s} value={s}>{STATUS_LABELS[s] ?? s}</option>
          ))}
        </select>
        <select value={entityType} onChange={e => { setEntityType(e.target.value); setPage(1); }}
          className="rounded-lg border border-border bg-background px-2 py-1.5 text-xs">
          <option value="">Wszystkie typy encji</option>
          {(avail?.entity_types ?? []).map(t => (
            <option key={t} value={t}>{ENTITY_LABELS[t] ?? t}</option>
          ))}
        </select>
      </div>

      {/* Table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        {isLoading ? (
          <div className="p-8 text-sm text-muted-foreground animate-pulse">Ładowanie okazji…</div>
        ) : items.length === 0 ? (
          <div className="p-8 text-sm text-muted-foreground text-center">Brak okazji spełniających kryteria.</div>
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-muted-foreground">
                <th className="text-left px-3 py-2">Typ okazji</th>
                <th className="px-3 py-2">Mkt</th>
                <th className="px-3 py-2 text-left min-w-[200px]">Produkt / Kategoria</th>
                <th className="px-3 py-2 text-left min-w-[260px]">Opis i sygnały</th>
                <th className="px-3 py-2 text-right">Priorytet</th>
                <th className="px-3 py-2">Data</th>
                <th className="px-3 py-2">Status</th>
                <th className="px-3 py-2 text-center">Akcje</th>
              </tr>
            </thead>
            <tbody>
              {items.map((o) => {
                const displayName = o.product_title || o.entity_id;
                return (
                  <tr key={o.id} className="border-b border-border/30 hover:bg-muted/20">
                    <td className="px-3 py-2">
                      <span className={cn("rounded-full border px-1.5 py-0.5 text-[9px] font-bold uppercase whitespace-nowrap",
                        TYPE_COLORS[o.opportunity_type] || "bg-zinc-500/15 text-zinc-400 border-zinc-500/30")}>
                        {TYPE_LABELS[o.opportunity_type] || o.opportunity_type}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-center font-medium">{o.marketplace}</td>
                    <td className="px-3 py-2 max-w-[250px]">
                      <div className="font-medium truncate" title={`${o.entity_id}${o.product_title ? ` — ${o.product_title}` : ""}`}>
                        {displayName}
                      </div>
                      <div className="text-[9px] text-muted-foreground">
                        {o.entity_type === "category" ? "Kategoria" : "SKU"}
                      </div>
                    </td>
                    <td className="px-3 py-2 max-w-[300px]">
                      <div className="truncate text-muted-foreground" title={o.description}>
                        {o.description}
                      </div>
                      <SignalBadges signals={o.source_signals} type={o.opportunity_type} />
                    </td>
                    <td className="px-3 py-2 text-right">
                      <div className="flex items-center justify-end gap-1">
                        <div className="w-10 h-1.5 rounded-full bg-zinc-700/50 overflow-hidden">
                          <div className="h-full rounded-full bg-amber-500" style={{ width: `${Math.min(100, o.priority_score)}%` }} />
                        </div>
                        <span className="tabular-nums font-medium">{o.priority_score?.toFixed(0)}</span>
                      </div>
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap text-[10px]">{o.recommended_start_date?.slice(0, 10) ?? "—"}</td>
                    <td className="px-3 py-2">
                      <span className={cn("rounded-full border px-1.5 py-0.5 text-[9px] font-bold uppercase",
                        STATUS_COLORS[o.status] || "")}>
                        {STATUS_LABELS[o.status] ?? o.status}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-center">
                      {o.status === "new" && (
                        <div className="flex gap-1 justify-center">
                          <button onClick={() => acceptMut.mutate(o.id)} title="Zaakceptuj"
                            className="rounded p-1 hover:bg-emerald-500/20">
                            <Check className="h-3.5 w-3.5 text-emerald-400" />
                          </button>
                          <button onClick={() => rejectMut.mutate(o.id)} title="Odrzuce"
                            className="rounded p-1 hover:bg-red-500/20">
                            <X className="h-3.5 w-3.5 text-red-400" />
                          </button>
                        </div>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-2">
          <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page <= 1}
            className="rounded border border-border px-3 py-1 text-xs disabled:opacity-30">Poprzednia</button>
          <span className="text-xs text-muted-foreground">Strona {page} / {totalPages}</span>
          <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page >= totalPages}
            className="rounded border border-border px-3 py-1 text-xs disabled:opacity-30">Następna</button>
        </div>
      )}
    </div>
  );
}
