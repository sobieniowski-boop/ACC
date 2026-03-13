import { useQuery } from "@tanstack/react-query";
import {
  CalendarDays, TrendingUp, Leaf, Flame, BarChart3, AlertTriangle, RefreshCw, Search, Info,
  Package, FolderOpen,
} from "lucide-react";
import { getSeasonalityOverview } from "@/lib/api";
import type { SeasonalityOverviewResponse } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useNavigate } from "react-router-dom";

const MONTH_NAMES = ["Sty","Lut","Mar","Kwi","Maj","Cze","Lip","Sie","Wrz","Paź","Lis","Gru"];

const CLASS_COLORS: Record<string, string> = {
  EVERGREEN: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  MILD_SEASONAL: "bg-blue-500/15 text-blue-400 border-blue-500/30",
  STRONG_SEASONAL: "bg-orange-500/15 text-orange-400 border-orange-500/30",
  PEAK_SEASONAL: "bg-red-500/15 text-red-400 border-red-500/30",
  EVENT_DRIVEN: "bg-purple-500/15 text-purple-400 border-purple-500/30",
  IRREGULAR: "bg-zinc-500/15 text-zinc-400 border-zinc-500/30",
};

const CLASS_LABELS: Record<string, string> = {
  EVERGREEN: "Stałe (Evergreen)",
  MILD_SEASONAL: "Lekko sezonowe",
  STRONG_SEASONAL: "Silnie sezonowe",
  PEAK_SEASONAL: "Szczytowe",
  EVENT_DRIVEN: "Eventowe",
  IRREGULAR: "Nieregularne",
};

const OPP_LABELS: Record<string, string> = {
  LIQUIDATE_POST_SEASON: "Wyprzedaż po sezonie",
  MARKET_EXPANSION_PREP: "Przygotowanie ekspansji",
  PREPARE_STOCK: "Przygotowanie stanów",
  PREPARE_CONTENT: "Przygotowanie contentu",
  PREPARE_ADS: "Przygotowanie reklam",
  PREPARE_PRICING: "Przygotowanie cen",
  EXECUTION_GAP: "Luka realizacji",
  PROFIT_PROTECTION: "Ochrona marży",
};

function KPICard({ label, value, sub, icon: Icon, accent }: {
  label: string; value: string | number; sub?: string; icon: React.ElementType; accent?: string;
}) {
  return (
    <div className="rounded-xl border border-border bg-card p-4">
      <div className="flex items-center gap-2 text-xs text-muted-foreground mb-1">
        <Icon className={cn("h-3.5 w-3.5", accent)} />
        {label}
      </div>
      <div className="text-2xl font-bold tabular-nums">{value}</div>
      {sub && <div className="text-xs text-muted-foreground mt-0.5">{sub}</div>}
    </div>
  );
}

function ClassBadge({ cls }: { cls: string }) {
  return (
    <span className={cn("rounded-full border px-2 py-0.5 text-[10px] font-bold uppercase",
      CLASS_COLORS[cls] || CLASS_COLORS.IRREGULAR)}>
      {CLASS_LABELS[cls] || cls.replace("_", " ")}
    </span>
  );
}

function HeatCell({ value }: { value: number }) {
  const bg = value > 1.5 ? "bg-red-500/60" : value > 1.2 ? "bg-orange-500/50"
    : value > 0.8 ? "bg-emerald-500/30" : value > 0.5 ? "bg-blue-500/30" : "bg-zinc-700/30";
  return (
    <td className={cn("px-1 py-1 text-center text-[10px] tabular-nums", bg)}>
      {value?.toFixed(2) ?? "—"}
    </td>
  );
}

function formatNum(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export default function SeasonalityOverviewPage() {
  const nav = useNavigate();
  const { data, isLoading } = useQuery({
    queryKey: ["seasonality-overview"],
    queryFn: () => getSeasonalityOverview(),
    staleTime: 5 * 60_000,
  });

  const kpi = data?.kpi;

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center gap-3">
        <CalendarDays className="h-7 w-7 text-amazon" />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Sezonowość i Analiza Popytu</h1>
          <p className="text-sm text-muted-foreground">Analiza sezonowości popytu, sprzedaży i rentowności na podstawie danych historycznych i search termów Amazon</p>
        </div>
      </div>

      {/* Info banner */}
      <div className="rounded-xl border border-blue-500/30 bg-blue-500/5 p-4 flex gap-3">
        <Info className="h-5 w-5 text-blue-400 mt-0.5 shrink-0" />
        <div className="text-sm text-blue-300/90 space-y-1">
          <p className="font-medium">Jak działa ta analiza?</p>
          <p>System analizuje historyczne dane sprzedażowe i <strong>{formatNum(kpi?.search_terms_count ?? 0)} search termów</strong> z Brand Analytics aby wykryć
          wzorce sezonowe w Twoich produktach. Na tej podstawie klasyfikuje każdy produkt i kategorię (stały / lekko sezonowy / silnie sezonowy / szczytowy)
          i identyfikuje nadchodzące szczyty popytu oraz okazje do działania.</p>
          <p>Mapa cieplna poniżej pokazuje <strong>indeks popytu na poziomie kategorii</strong> — wartości &gt;1.0 oznaczają ponadprzeciętny popyt w danym miesiącu, &lt;1.0 poniżej średniej.</p>
        </div>
      </div>

      {isLoading ? (
        <div className="text-sm text-muted-foreground animate-pulse">Ładowanie danych sezonowości…</div>
      ) : (
        <>
          {/* KPI cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
            <KPICard label="Produktów (SKU)" value={kpi?.sku_count ?? 0} icon={Package} accent="text-blue-400"
              sub="analizowanych" />
            <KPICard label="Kategorii" value={kpi?.category_count ?? 0} icon={FolderOpen} accent="text-violet-400"
              sub="analizowanych" />
            <KPICard label="Sezonowe produkty" value={kpi?.seasonal_categories ?? 0} icon={Flame} accent="text-orange-400"
              sub="z wahaniami popytu" />
            <KPICard label="Stałe (Evergreen)" value={kpi?.evergreen_categories ?? 0} icon={Leaf} accent="text-emerald-400"
              sub="stabilny popyt cały rok" />
            <KPICard label="Search termów" value={formatNum(kpi?.search_terms_count ?? 0)} icon={Search} accent="text-cyan-400"
              sub="z Brand Analytics" />
            <KPICard label="Zbliżający się szczyt"
              value={(kpi?.strongest_upcoming_season as Record<string,unknown>)?.display_name as string
                ?? (kpi?.strongest_upcoming_season as Record<string,unknown>)?.entity_id as string ?? "—"}
              sub={kpi?.strongest_upcoming_season ? `${(kpi.strongest_upcoming_season as Record<string,unknown>).marketplace}` : "brak w najbliższych 2 mies."}
              icon={TrendingUp} accent="text-red-400" />
          </div>

          {/* Class distribution */}
          <div className="rounded-xl border border-border bg-card p-4">
            <h2 className="text-sm font-semibold mb-3">Rozkład sezonowości</h2>
            <div className="flex flex-wrap gap-3">
              {Object.entries(data?.class_distribution ?? {}).map(([cls, count]) => (
                <div key={cls} className="flex items-center gap-2">
                  <ClassBadge cls={cls} />
                  <span className="text-sm font-bold tabular-nums">{count}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Marketplace Heatmap */}
          {data?.marketplace_heatmap && data.marketplace_heatmap.length > 0 && (
            <div className="rounded-xl border border-border bg-card p-4 overflow-x-auto">
              <h2 className="text-sm font-semibold mb-1">Mapa cieplna popytu wg kategorii</h2>
              <p className="text-xs text-muted-foreground mb-3">Średni indeks popytu na poziomie kategorii produktów — wartość 1.0 = średnia roczna, &gt;1.2 = ponadprzeciętny popyt</p>
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left px-2 py-1">Marketplace</th>
                    {MONTH_NAMES.map(m => <th key={m} className="px-1 py-1 text-center">{m}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {(() => {
                    const byMkt: Record<string, Record<number, number>> = {};
                    for (const h of data.marketplace_heatmap) {
                      const mkt = h.marketplace as string;
                      if (!byMkt[mkt]) byMkt[mkt] = {};
                      byMkt[mkt][h.month as number] = h.demand_index as number;
                    }
                    return Object.entries(byMkt).map(([mkt, months]) => (
                      <tr key={mkt} className="border-b border-border/30">
                        <td className="px-2 py-1 font-medium">{mkt}</td>
                        {Array.from({ length: 12 }, (_, i) => (
                          <HeatCell key={i} value={months[i + 1] ?? 1.0} />
                        ))}
                      </tr>
                    ));
                  })()}
                </tbody>
              </table>
            </div>
          )}

          {/* Peak Calendar */}
          {data?.peak_calendar && data.peak_calendar.length > 0 && (
            <div className="rounded-xl border border-border bg-card p-4">
              <h2 className="text-sm font-semibold mb-1">Kalendarz szczytów — Top sezonowe produkty</h2>
              <p className="text-xs text-muted-foreground mb-3">Produkty i kategorie z najsilniejszym wzorcem sezonowym — kliknij aby zobaczyć szczegóły</p>
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border text-muted-foreground">
                    <th className="text-left px-2 py-1">Produkt / Kategoria</th>
                    <th className="text-left px-2 py-1">Typ</th>
                    <th className="text-left px-2 py-1">Mkt</th>
                    <th className="text-left px-2 py-1">Miesiące szczytowe</th>
                    <th className="text-right px-2 py-1">Siła</th>
                  </tr>
                </thead>
                <tbody>
                  {data.peak_calendar.slice(0, 15).map((c, i) => (
                    <tr key={i} className="border-b border-border/30 hover:bg-muted/20 cursor-pointer"
                        onClick={() => nav(`/seasonality/entity/${c.entity_type}/${c.entity_id}`)}>
                      <td className="px-2 py-1 font-medium max-w-[300px] truncate" title={c.entity_id as string}>
                        {(c.product_title as string) || (c.entity_id as string)}
                      </td>
                      <td className="px-2 py-1">
                        <span className={cn("rounded px-1 py-0.5 text-[9px] font-bold uppercase",
                          c.entity_type === "category" ? "bg-violet-500/20 text-violet-400" : "bg-blue-500/20 text-blue-400")}>
                          {c.entity_type === "category" ? "Kategoria" : "SKU"}
                        </span>
                      </td>
                      <td className="px-2 py-1">{c.marketplace as string}</td>
                      <td className="px-2 py-1">
                        {(c.peak_months as number[])?.map(m => MONTH_NAMES[m - 1] ?? "?").join(", ")}
                      </td>
                      <td className="px-2 py-1 text-right tabular-nums">{(c.strength as number)?.toFixed(0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Upcoming Opportunities */}
          {data?.upcoming_opportunities && data.upcoming_opportunities.length > 0 && (
            <div className="rounded-xl border border-border bg-card p-4">
              <div className="flex items-center justify-between mb-3">
                <div>
                  <h2 className="text-sm font-semibold">Okazje sezonowe do działania</h2>
                  <p className="text-xs text-muted-foreground">Rekomendowane akcje przygotowujące do nadchodzących sezonów</p>
                </div>
                <button onClick={() => nav("/seasonality/opportunities")}
                  className="text-xs text-amazon hover:underline">Wszystkie →</button>
              </div>
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border text-muted-foreground">
                    <th className="text-left px-2 py-1">Typ</th>
                    <th className="text-left px-2 py-1">Produkt</th>
                    <th className="text-left px-2 py-1">Opis</th>
                    <th className="text-left px-2 py-1">Mkt</th>
                    <th className="text-right px-2 py-1">Priorytet</th>
                  </tr>
                </thead>
                <tbody>
                  {data.upcoming_opportunities.slice(0, 10).map((o, i) => (
                    <tr key={i} className="border-b border-border/30 hover:bg-muted/20">
                      <td className="px-2 py-1">
                        <span className="rounded px-1.5 py-0.5 text-[9px] font-bold bg-amber-500/15 text-amber-400 border border-amber-500/30">
                          {OPP_LABELS[o.type as string] ?? (o.type as string)}
                        </span>
                      </td>
                      <td className="px-2 py-1 font-medium max-w-[200px] truncate" title={o.entity_id as string}>
                        {(o.product_title as string) || (o.entity_id as string)}
                      </td>
                      <td className="px-2 py-1 max-w-[300px] truncate">{o.title as string}</td>
                      <td className="px-2 py-1">{o.marketplace as string}</td>
                      <td className="px-2 py-1 text-right tabular-nums">{(o.priority as number)?.toFixed(0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}
