import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { List, Info } from "lucide-react";
import { getSeasonalityEntities } from "@/lib/api";
import type { SeasonalityProfile } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useNavigate } from "react-router-dom";

const MONTH_NAMES = ["Sty","Lut","Mar","Kwi","Maj","Cze","Lip","Sie","Wrz","Paź","Lis","Gru"];

const CLASS_LABELS: Record<string, string> = {
  EVERGREEN: "Stałe",
  MILD_SEASONAL: "Lekko sezonowe",
  STRONG_SEASONAL: "Silnie sezonowe",
  PEAK_SEASONAL: "Szczytowe",
  EVENT_DRIVEN: "Eventowe",
  IRREGULAR: "Nieregularne",
};

const ENTITY_LABELS: Record<string, string> = {
  sku: "SKU (produkty)",
  category: "Kategorie",
};

const SORTS = [
  { value: "demand_strength_score", label: "Siła popytu (najsilniejsza sezonowość)" },
  { value: "sales_strength_score", label: "Siła sprzedaży" },
  { value: "profit_strength_score", label: "Siła zysku" },
  { value: "evergreen_score", label: "Stabilność (evergreen)" },
  { value: "demand_vs_sales_gap", label: "Luka popyt vs sprzedaż" },
  { value: "seasonality_confidence_score", label: "Pewność klasyfikacji" },
];

const CLASS_COLORS: Record<string, string> = {
  EVERGREEN: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  MILD_SEASONAL: "bg-blue-500/15 text-blue-400 border-blue-500/30",
  STRONG_SEASONAL: "bg-orange-500/15 text-orange-400 border-orange-500/30",
  PEAK_SEASONAL: "bg-red-500/15 text-red-400 border-red-500/30",
  EVENT_DRIVEN: "bg-purple-500/15 text-purple-400 border-purple-500/30",
  IRREGULAR: "bg-zinc-500/15 text-zinc-400 border-zinc-500/30",
};

function ScoreBar({ value, max = 100, color }: { value: number; max?: number; color: string }) {
  const pct = Math.min(100, (value / max) * 100);
  return (
    <div className="flex items-center gap-1.5">
      <div className="w-12 h-1.5 rounded-full bg-zinc-700/50 overflow-hidden">
        <div className={cn("h-full rounded-full", color)} style={{ width: `${pct}%` }} />
      </div>
      <span className="tabular-nums">{value?.toFixed(0)}</span>
    </div>
  );
}

export default function SeasonalityEntitiesPage() {
  const nav = useNavigate();
  const [entityType, setEntityType] = useState<string>("");
  const [marketplace, setMarketplace] = useState<string>("");
  const [seasonClass, setSeasonClass] = useState<string>("");
  const [sort, setSort] = useState("demand_strength_score");
  const [page, setPage] = useState(1);
  const pageSize = 50;

  const { data, isLoading } = useQuery({
    queryKey: ["seasonality-entities", entityType, marketplace, seasonClass, sort, page],
    queryFn: () => getSeasonalityEntities({
      entity_type: entityType || undefined,
      marketplace: marketplace || undefined,
      seasonality_class: seasonClass || undefined,
      sort,
      page,
      page_size: pageSize,
    }),
    staleTime: 5 * 60_000,
  });

  const totalPages = data ? Math.ceil(data.total / pageSize) : 0;
  const avail = (data as Record<string, unknown> | undefined)?.available_filters as
    { entity_types: string[]; marketplaces: string[]; classes: string[] } | undefined;

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center gap-3">
        <List className="h-6 w-6 text-amazon" />
        <div>
          <h1 className="text-xl font-bold tracking-tight">Profile sezonowości</h1>
          <p className="text-sm text-muted-foreground">Klasyfikacja i scoring każdego produktu/kategorii — {data?.total ?? 0} pozycji</p>
        </div>
      </div>

      {/* Info banner */}
      <div className="rounded-xl border border-blue-500/30 bg-blue-500/5 p-4 flex gap-3">
        <Info className="h-5 w-5 text-blue-400 mt-0.5 shrink-0" />
        <div className="text-sm text-blue-300/90 space-y-1">
          <p className="font-medium">Co pokazuje ta tabela?</p>
          <p>Każdy wiersz to profil sezonowości produktu (SKU) lub kategorii na danym marketplace. System analizuje historyczne dane sprzedażowe
          i klasyfikuje każdą pozycję:</p>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-x-4 gap-y-0.5 text-xs mt-1">
            <span><strong>Siła popytu</strong> — jak silna jest sezonowość (0–100)</span>
            <span><strong>Siła sprzedaży</strong> — wahania sprzedaży w ciągu roku</span>
            <span><strong>Siła zysku</strong> — wahania rentowności w ciągu roku</span>
            <span><strong>Stabilność</strong> — 100 = stały popyt, 0 = silnie sezonowy</span>
            <span><strong>Pewność</strong> — pewność klasyfikacji (ilość danych)</span>
            <span><strong>Luka P↔S</strong> — różnica między popytem a sprzedażą (okazja!)</span>
          </div>
          <p className="text-xs">Kliknij wiersz aby zobaczyć szczegółowy profil z wykresami miesięcznymi.</p>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3">
        <select value={entityType} onChange={e => { setEntityType(e.target.value); setPage(1); }}
          className="rounded-lg border border-border bg-card px-3 py-1.5 text-xs">
          <option value="">Wszystkie typy</option>
          {(avail?.entity_types ?? []).map(t => (
            <option key={t} value={t}>{ENTITY_LABELS[t] ?? t}</option>
          ))}
        </select>
        <select value={marketplace} onChange={e => { setMarketplace(e.target.value); setPage(1); }}
          className="rounded-lg border border-border bg-card px-3 py-1.5 text-xs">
          <option value="">Wszystkie marketplace&apos;y</option>
          {(avail?.marketplaces ?? []).map(m => <option key={m} value={m}>{m}</option>)}
        </select>
        <select value={seasonClass} onChange={e => { setSeasonClass(e.target.value); setPage(1); }}
          className="rounded-lg border border-border bg-card px-3 py-1.5 text-xs">
          <option value="">Wszystkie klasy</option>
          {(avail?.classes ?? []).map(c => (
            <option key={c} value={c}>{CLASS_LABELS[c] ?? c.replace("_"," ")}</option>
          ))}
        </select>
        <select value={sort} onChange={e => { setSort(e.target.value); setPage(1); }}
          className="rounded-lg border border-border bg-card px-3 py-1.5 text-xs">
          {SORTS.map(s => <option key={s.value} value={s.value}>{s.label}</option>)}
        </select>
      </div>

      {/* Table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        {isLoading ? (
          <div className="p-8 text-sm text-muted-foreground animate-pulse">Ładowanie profili…</div>
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-muted-foreground">
                <th className="text-left px-3 py-2">Typ</th>
                <th className="text-left px-3 py-2 min-w-[200px]">Produkt / Kategoria</th>
                <th className="px-3 py-2">Mkt</th>
                <th className="px-3 py-2">Klasa sezonowości</th>
                <th className="px-3 py-2 text-right">Siła popytu</th>
                <th className="px-3 py-2 text-right">Siła sprzedaży</th>
                <th className="px-3 py-2 text-right">Siła zysku</th>
                <th className="px-3 py-2 text-right">Stabilność</th>
                <th className="px-3 py-2 text-right">Pewność</th>
                <th className="px-3 py-2 text-center">Szczyty</th>
                <th className="px-3 py-2 text-right">Sezon (mies.)</th>
                <th className="px-3 py-2 text-right">Luka P↔S</th>
              </tr>
            </thead>
            <tbody>
              {data?.items?.map((p: SeasonalityProfile & { product_title?: string | null }) => {
                const displayName = p.product_title || p.entity_id;
                return (
                  <tr key={p.id} className="border-b border-border/30 hover:bg-muted/20 cursor-pointer"
                      onClick={() => nav(`/seasonality/entity/${p.entity_type}/${p.entity_id}`)}>
                    <td className="px-3 py-2">
                      <span className={cn("rounded px-1 py-0.5 text-[9px] font-bold uppercase",
                        p.entity_type === "category" ? "bg-violet-500/20 text-violet-400" : "bg-blue-500/20 text-blue-400")}>
                        {p.entity_type === "category" ? "Kat." : "SKU"}
                      </span>
                    </td>
                    <td className="px-3 py-2 font-medium max-w-[250px] truncate" title={`${p.entity_id}${p.product_title ? ` — ${p.product_title}` : ""}`}>
                      {displayName}
                    </td>
                    <td className="px-3 py-2 text-center">{p.marketplace}</td>
                    <td className="px-3 py-2">
                      <span className={cn("rounded-full border px-1.5 py-0.5 text-[9px] font-bold uppercase whitespace-nowrap",
                        CLASS_COLORS[p.seasonality_class] || "")}>
                        {CLASS_LABELS[p.seasonality_class] ?? p.seasonality_class?.replace("_"," ")}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-right">
                      <ScoreBar value={p.demand_strength_score} color="bg-orange-500" />
                    </td>
                    <td className="px-3 py-2 text-right">
                      <ScoreBar value={p.sales_strength_score} color="bg-blue-500" />
                    </td>
                    <td className="px-3 py-2 text-right">
                      <ScoreBar value={p.profit_strength_score} color="bg-green-500" />
                    </td>
                    <td className="px-3 py-2 text-right">
                      <ScoreBar value={p.evergreen_score} color="bg-emerald-500" />
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">{p.seasonality_confidence_score?.toFixed(0)}</td>
                    <td className="px-3 py-2 text-center text-[10px]">
                      {p.peak_months?.map(m => MONTH_NAMES[m - 1]).join(", ")}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">{p.season_length_months ?? "—"}</td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {p.demand_vs_sales_gap != null ? p.demand_vs_sales_gap.toFixed(2) : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-2">
          <button disabled={page <= 1} onClick={() => setPage(p => p - 1)}
            className="rounded border border-border px-3 py-1 text-xs disabled:opacity-30">Poprzednia</button>
          <span className="text-xs text-muted-foreground">Strona {page} / {totalPages}</span>
          <button disabled={page >= totalPages} onClick={() => setPage(p => p + 1)}
            className="rounded border border-border px-3 py-1 text-xs disabled:opacity-30">Następna</button>
        </div>
      )}
    </div>
  );
}
