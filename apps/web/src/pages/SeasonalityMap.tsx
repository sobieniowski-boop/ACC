import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Map, Info, Search } from "lucide-react";
import { getSeasonalityMap } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useNavigate } from "react-router-dom";

const MONTH_NAMES = ["Sty","Lut","Mar","Kwi","Maj","Cze","Lip","Sie","Wrz","Paź","Lis","Gru"];

const CLASS_LABELS: Record<string, string> = {
  EVERGREEN: "Stałe (Evergreen)",
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

const CLASS_COLORS: Record<string, string> = {
  EVERGREEN: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  MILD_SEASONAL: "bg-blue-500/15 text-blue-400 border-blue-500/30",
  STRONG_SEASONAL: "bg-orange-500/15 text-orange-400 border-orange-500/30",
  PEAK_SEASONAL: "bg-red-500/15 text-red-400 border-red-500/30",
  EVENT_DRIVEN: "bg-purple-500/15 text-purple-400 border-purple-500/30",
  IRREGULAR: "bg-zinc-500/15 text-zinc-400 border-zinc-500/30",
};

function indexColor(v: number | null): string {
  if (v == null) return "bg-zinc-800/30";
  if (v >= 1.8) return "bg-red-600/70";
  if (v >= 1.4) return "bg-orange-500/60";
  if (v >= 1.1) return "bg-yellow-500/40";
  if (v >= 0.9) return "bg-emerald-500/30";
  if (v >= 0.6) return "bg-blue-500/30";
  return "bg-zinc-700/40";
}

export default function SeasonalityMapPage() {
  const nav = useNavigate();
  const [entityType, setEntityType] = useState("sku");
  const [marketplace, setMarketplace] = useState<string>("");
  const [seasonClass, setSeasonClass] = useState<string>("");
  const [page, setPage] = useState(1);
  const pageSize = 50;

  const { data, isLoading } = useQuery({
    queryKey: ["seasonality-map", entityType, marketplace, seasonClass, page],
    queryFn: () => getSeasonalityMap({
      entity_type: entityType,
      marketplace: marketplace || undefined,
      seasonality_class: seasonClass || undefined,
      page,
      page_size: pageSize,
    }),
    staleTime: 5 * 60_000,
  });

  const totalPages = data ? Math.ceil(data.total / pageSize) : 0;
  const avail = data?.available_filters;

  // Build search demand curve by marketplace
  const searchCurves: Record<string, Record<number, number>> = {};
  for (const c of data?.search_demand_curves ?? []) {
    if (!searchCurves[c.marketplace]) searchCurves[c.marketplace] = {};
    searchCurves[c.marketplace][c.month] = c.demand_index;
  }

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center gap-3">
        <Map className="h-6 w-6 text-amazon" />
        <div>
          <h1 className="text-xl font-bold tracking-tight">Mapa sezonowości</h1>
          <p className="text-sm text-muted-foreground">Indeks popytu i sezonowości dla produktów i kategorii</p>
        </div>
      </div>

      {/* Info banner */}
      <div className="rounded-xl border border-blue-500/30 bg-blue-500/5 p-4 flex gap-3">
        <Info className="h-5 w-5 text-blue-400 mt-0.5 shrink-0" />
        <div className="text-sm text-blue-300/90 space-y-1">
          <p>Indeks popytu oparty na danych sprzedażowych i <strong>search termach z Brand Analytics</strong>. Wartość &gt;1.0 = popyt powyżej średniej rocznej.
          Krzywa "Popyt wg Search Terms" poniżej pokazuje sezonowość opartą wyłącznie na częstotliwości wyszukiwań na Amazon.</p>
        </div>
      </div>

      {/* Search Demand Curves section */}
      {Object.keys(searchCurves).length > 0 && (
        <div className="rounded-xl border border-border bg-card p-4 overflow-x-auto">
          <div className="flex items-center gap-2 mb-1">
            <Search className="h-4 w-4 text-cyan-400" />
            <h2 className="text-sm font-semibold">Popyt wg Search Terms (Brand Analytics)</h2>
          </div>
          <p className="text-xs text-muted-foreground mb-3">
            Indeks oparty na częstotliwości wyszukiwań — pokazuje kiedy klienci szukają produktów na Amazon
          </p>
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-muted-foreground">
                <th className="text-left px-2 py-1">Marketplace</th>
                {MONTH_NAMES.map(m => <th key={m} className="px-1 py-1 text-center min-w-[36px]">{m}</th>)}
              </tr>
            </thead>
            <tbody>
              {Object.entries(searchCurves).sort((a,b) => a[0].localeCompare(b[0])).map(([mkt, months]) => (
                <tr key={mkt} className="border-b border-border/30">
                  <td className="px-2 py-1.5 font-medium">{mkt}</td>
                  {Array.from({ length: 12 }, (_, i) => {
                    const v = months[i + 1] ?? 1.0;
                    return (
                      <td key={i} className={cn("px-1 py-1.5 text-center tabular-nums", indexColor(v))}>
                        {v.toFixed(2)}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap gap-3 items-center">
        <select value={entityType} onChange={e => { setEntityType(e.target.value); setPage(1); }}
          className="rounded-lg border border-border bg-card px-3 py-1.5 text-xs">
          {(avail?.entity_types ?? [entityType]).map(t => (
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
        <span className="text-xs text-muted-foreground ml-2">
          {data?.total ?? 0} {entityType === "category" ? "kategorii" : "produktów"}
        </span>
      </div>

      {/* Heatmap Table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        {isLoading ? (
          <div className="p-8 text-sm text-muted-foreground animate-pulse">Ładowanie mapy sezonowości…</div>
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-muted-foreground">
                <th className="text-left px-2 py-2 sticky left-0 bg-card z-10 min-w-[200px]">
                  {entityType === "category" ? "Kategoria" : "Produkt"}
                </th>
                <th className="px-2 py-2">Mkt</th>
                {MONTH_NAMES.map(m => <th key={m} className="px-1 py-2 text-center min-w-[36px]">{m}</th>)}
                <th className="px-2 py-2">Klasa</th>
                <th className="px-2 py-2 text-center">Szczyty</th>
                <th className="px-2 py-2 text-right">Siła</th>
                <th className="px-2 py-2 text-right">Pewn.</th>
                <th className="px-2 py-2 text-right">EG</th>
              </tr>
            </thead>
            <tbody>
              {data?.items?.map((row, idx) => {
                const indexMap: Record<number, number | null> = {};
                for (const i of row.indices) indexMap[i.month] = i.demand_index;
                const displayName = row.product_title || row.entity_id;
                return (
                  <tr key={idx} className="border-b border-border/30 hover:bg-muted/20 cursor-pointer"
                      onClick={() => nav(`/seasonality/entity/${row.entity_type}/${row.entity_id}`)}>
                    <td className="px-2 py-1.5 font-medium sticky left-0 bg-card max-w-[250px] truncate"
                        title={`${row.entity_id}${row.product_title ? ` — ${row.product_title}` : ""}`}>
                      {displayName}
                    </td>
                    <td className="px-2 py-1.5 text-center">{row.marketplace}</td>
                    {Array.from({ length: 12 }, (_, i) => (
                      <td key={i} className={cn("px-1 py-1.5 text-center tabular-nums", indexColor(indexMap[i + 1] ?? null))}>
                        {indexMap[i + 1]?.toFixed(1) ?? "—"}
                      </td>
                    ))}
                    <td className="px-2 py-1.5">
                      <span className={cn("rounded-full border px-1.5 py-0.5 text-[9px] font-bold uppercase whitespace-nowrap",
                        CLASS_COLORS[row.seasonality_class] || "")}>
                        {CLASS_LABELS[row.seasonality_class] ?? row.seasonality_class?.replace("_"," ")}
                      </span>
                    </td>
                    <td className="px-2 py-1.5 text-center text-[10px]">
                      {row.peak_months?.map(m => MONTH_NAMES[m-1]).join(",")}
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums">{row.strength_score?.toFixed(0)}</td>
                    <td className="px-2 py-1.5 text-right tabular-nums">{row.confidence_score?.toFixed(0)}</td>
                    <td className="px-2 py-1.5 text-right tabular-nums">{row.evergreen_score?.toFixed(0)}</td>
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
