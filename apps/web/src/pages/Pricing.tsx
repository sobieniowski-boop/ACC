import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import { Package, Globe, Box, Clock } from "lucide-react";
import { getPricingOffers, getBuyBoxStats, type BuyBoxStats } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import {
  DataFreshness,
  ClientExportButton,
  ColumnChooser,
  useColumnVisibility,
  ServerPagination,
  StickyFilterBar,
  BatchBar,
  BatchActionButton,
  type ColumnDef,
} from "@/components/shared";

const STATUS_OPTIONS = ["Active", "Inactive", "Incomplete"] as const;
const FULFILLMENT_OPTIONS = ["FBA", "FBM"] as const;
const BAR_COLORS = [
  "#FF9900", "#38bdf8", "#4ade80", "#f472b6", "#a78bfa",
  "#fb923c", "#34d399", "#818cf8", "#fbbf24", "#f87171",
  "#22d3ee", "#e879f9", "#84cc16",
];

function fmtDate(d?: string | null): string {
  if (!d) return "—";
  return new Date(d).toLocaleDateString("pl-PL", { day: "2-digit", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" });
}

export default function Pricing() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [skuFilter, setSkuFilter] = useState(searchParams.get("sku") ?? "");
  const [mpFilter, setMpFilter] = useState(searchParams.get("mp") ?? "");
  const [statusFilter, setStatusFilter] = useState(searchParams.get("status") ?? "");
  const [ffFilter, setFfFilter] = useState(searchParams.get("ff") ?? "");
  const [page, setPage] = useState(1);
  const PAGE_SIZE = 50;
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());

  const OFFER_COLUMNS: ColumnDef[] = [
    { key: "sku", label: "SKU" },
    { key: "asin", label: "ASIN" },
    { key: "marketplace", label: "Marketplace" },
    { key: "price", label: "Cena" },
    { key: "buybox", label: "Buy Box" },
    { key: "status", label: "Status" },
    { key: "fulfillment", label: "Realizacja" },
    { key: "gap", label: "Różnica" },
  ];
  const colVis = useColumnVisibility(OFFER_COLUMNS);

  // Sync URL params
  useEffect(() => {
    const next = new URLSearchParams(searchParams);
    const syncs = [
      ["sku", skuFilter.trim()],
      ["mp", mpFilter],
      ["status", statusFilter],
      ["ff", ffFilter],
    ] as const;
    for (const [key, val] of syncs) {
      if (val) next.set(key, val);
      else next.delete(key);
    }
    if (next.toString() !== searchParams.toString()) {
      setSearchParams(next, { replace: true });
    }
  }, [skuFilter, mpFilter, statusFilter, ffFilter, searchParams, setSearchParams]);

  const { data: offersData, isLoading } = useQuery({
    queryKey: ["pricing-offers", skuFilter, mpFilter, statusFilter, ffFilter, page],
    queryFn: () =>
      getPricingOffers({
        ...(skuFilter.trim() ? { sku: skuFilter.trim() } : {}),
        ...(mpFilter ? { marketplace_id: mpFilter } : {}),
        ...(statusFilter ? { status: statusFilter } : {}),
        ...(ffFilter ? { fulfillment_channel: ffFilter } : {}),
        page,
        page_size: PAGE_SIZE,
      }),
    staleTime: 60_000,
  });

  const { data: bbStats } = useQuery({
    queryKey: ["buybox-stats"],
    queryFn: getBuyBoxStats,
    staleTime: 120_000,
  });

  const totalOffers = offersData?.total ?? 0;
  const totalPages = Math.ceil(totalOffers / PAGE_SIZE);

  // Aggregate stats across all marketplaces
  const agg = (bbStats ?? []).reduce(
    (acc, s) => ({
      total: acc.total + s.total_active_offers,
      active: acc.active + s.active_offers,
      inactive: acc.inactive + s.inactive_offers,
      fba: acc.fba + s.fba_offers,
      fbm: acc.fbm + s.fbm_offers,
      lastSync: !acc.lastSync || (s.last_sync && s.last_sync > acc.lastSync) ? s.last_sync : acc.lastSync,
    }),
    { total: 0, active: 0, inactive: 0, fba: 0, fbm: 0, lastSync: undefined as string | undefined },
  );

  // Chart data: sort by total offers desc
  const chartData = [...(bbStats ?? [])].sort((a, b) => b.total_active_offers - a.total_active_offers);

  return (
    <div className="space-y-6">
      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Cennik & Oferty</h1>
          <div className="flex items-center gap-3 mt-1">
            <p className="text-sm text-white/50">
              Monitorowanie cen, statusów ofert i realizacji na marketplace'ach.
            </p>
            <DataFreshness lastSync={agg.lastSync} staleMinutes={1440} label="Sync" />
          </div>
        </div>
        {offersData && offersData.items.length > 0 && (
          <ClientExportButton
            data={offersData.items}
            filename="pricing_offers"
            label="Export CSV"
          />
        )}
      </div>

      {/* ── KPI Cards ── */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-xs text-white/50">
              <Package className="h-4 w-4 text-[#FF9900]" /> Łącznie ofert
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">{agg.total.toLocaleString("pl-PL")}</div>
            <div className="mt-1 text-xs text-white/40">
              <span className="text-emerald-400">{agg.active.toLocaleString("pl-PL")} aktywnych</span>
              {" · "}
              <span className="text-red-400">{agg.inactive.toLocaleString("pl-PL")} nieaktywnych</span>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-xs text-white/50">
              <Box className="h-4 w-4 text-blue-400" /> Realizacja
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              {agg.total > 0 ? `${((agg.fba / agg.total) * 100).toFixed(0)}% FBA` : "—"}
            </div>
            <div className="mt-1 text-xs text-white/40">
              FBA {agg.fba.toLocaleString("pl-PL")} · FBM {agg.fbm.toLocaleString("pl-PL")}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-xs text-white/50">
              <Globe className="h-4 w-4 text-emerald-400" /> Marketplace'y
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">{bbStats?.length ?? "—"}</div>
            <div className="mt-1 text-xs text-white/40">aktywne rynki sprzedaży</div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-xs text-white/50">
              <Clock className="h-4 w-4 text-amber-400" /> Ostatnia synchronizacja
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-lg font-bold text-white">{fmtDate(agg.lastSync)}</div>
            <div className="mt-1 text-xs text-white/40">dane z Amazon SP-API</div>
          </CardContent>
        </Card>
      </div>

      {/* ── Chart: offers per marketplace ── */}
      {chartData.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Oferty per marketplace</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={chartData} margin={{ top: 0, right: 8, left: -20, bottom: 0 }}>
                <XAxis dataKey="marketplace_code" tick={{ fontSize: 11, fill: "#9ca3af" }} />
                <YAxis tick={{ fontSize: 11, fill: "#9ca3af" }} />
                <Tooltip
                  formatter={(v: number, _: string, entry: { payload: BuyBoxStats }) => {
                    const s = entry.payload;
                    return [`${v.toLocaleString("pl-PL")} (aktywne: ${s.active_offers}, FBA: ${s.fba_offers})`, "Oferty"];
                  }}
                  contentStyle={{
                    background: "#1e293b",
                    border: "1px solid rgba(255,255,255,0.1)",
                    borderRadius: 8,
                  }}
                  labelStyle={{ color: "#fff" }}
                />
                <Bar dataKey="total_active_offers" radius={[4, 4, 0, 0]}>
                  {chartData.map((_, i) => (
                    <Cell key={i} fill={BAR_COLORS[i % BAR_COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      )}

      {/* ── Offers table ── */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between gap-3 flex-wrap">
          <CardTitle className="text-sm">Oferty</CardTitle>
          <div className="flex items-center gap-2 flex-wrap">
            <ColumnChooser columns={OFFER_COLUMNS} visible={colVis.visible} onChange={colVis.setVisible} />
            <select
              value={mpFilter}
              onChange={(e) => { setMpFilter(e.target.value); setPage(1); }}
              className="h-9 rounded-md border border-white/10 bg-[#1e293b] px-3 text-xs text-white"
            >
              <option value="">Wszystkie marketplace</option>
              {(bbStats ?? []).map((s) => (
                <option key={s.marketplace_id} value={s.marketplace_id}>
                  {s.marketplace_code} ({s.total_active_offers.toLocaleString("pl-PL")})
                </option>
              ))}
            </select>
            <select
              value={statusFilter}
              onChange={(e) => { setStatusFilter(e.target.value); setPage(1); }}
              className="h-9 rounded-md border border-white/10 bg-[#1e293b] px-3 text-xs text-white"
            >
              <option value="">Wszystkie statusy</option>
              {STATUS_OPTIONS.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
            <select
              value={ffFilter}
              onChange={(e) => { setFfFilter(e.target.value); setPage(1); }}
              className="h-9 rounded-md border border-white/10 bg-[#1e293b] px-3 text-xs text-white"
            >
              <option value="">FBA + FBM</option>
              {FULFILLMENT_OPTIONS.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
            <Input
              placeholder="Filtruj po SKU..."
              value={skuFilter}
              onChange={(e) => { setSkuFilter(e.target.value); setPage(1); }}
              className="w-48"
            />
          </div>
        </CardHeader>
        <CardContent className="p-0">
          {isLoading ? (
            <div className="space-y-2 p-6">
              {Array.from({ length: 8 }).map((_, i) => (
                <Skeleton key={i} className="h-8 w-full" />
              ))}
            </div>
          ) : offersData && offersData.total === 0 ? (
            <div className="p-8 text-center">
              <div className="text-base font-semibold text-white">Brak ofert spełniających kryteria</div>
              <div className="mx-auto mt-2 max-w-md text-sm text-white/60">
                Zmień filtry lub uruchom job <span className="font-mono">sync_pricing</span> w&nbsp;
                <Link to="/jobs" className="text-[#FF9900] underline">panelu zadań</Link>.
              </div>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-8">
                    <input
                      type="checkbox"
                      checked={selectedIds.size === (offersData?.items.length ?? 0) && selectedIds.size > 0}
                      onChange={(e) => {
                        if (e.target.checked) setSelectedIds(new Set(offersData?.items.map((o) => o.id)));
                        else setSelectedIds(new Set());
                      }}
                      className="h-3 w-3 accent-[#FF9900]"
                    />
                  </TableHead>
                  {colVis.isVisible("sku") && <TableHead>SKU</TableHead>}
                  {colVis.isVisible("asin") && <TableHead>ASIN</TableHead>}
                  {colVis.isVisible("marketplace") && <TableHead>Marketplace</TableHead>}
                  {colVis.isVisible("price") && <TableHead className="text-right">Cena</TableHead>}
                  {colVis.isVisible("buybox") && <TableHead className="text-right">Buy Box</TableHead>}
                  {colVis.isVisible("status") && <TableHead>Status</TableHead>}
                  {colVis.isVisible("fulfillment") && <TableHead>Realizacja</TableHead>}
                  {colVis.isVisible("gap") && <TableHead className="text-right">Różnica</TableHead>}
                </TableRow>
              </TableHeader>
              <TableBody>
                {offersData?.items.map((offer) => {
                  const gap = offer.buybox_price ? offer.current_price - offer.buybox_price : null;
                  return (
                    <TableRow key={offer.id} data-state={selectedIds.has(offer.id) ? "selected" : undefined}>
                      <TableCell>
                        <input
                          type="checkbox"
                          checked={selectedIds.has(offer.id)}
                          onChange={(e) => {
                            const next = new Set(selectedIds);
                            if (e.target.checked) next.add(offer.id);
                            else next.delete(offer.id);
                            setSelectedIds(next);
                          }}
                          className="h-3 w-3 accent-[#FF9900]"
                        />
                      </TableCell>
                      {colVis.isVisible("sku") && <TableCell className="font-mono text-xs">{offer.sku}</TableCell>}
                      {colVis.isVisible("asin") && <TableCell className="font-mono text-xs text-white/60">{offer.asin}</TableCell>}
                      {colVis.isVisible("marketplace") && (
                        <TableCell>
                          <Badge variant="outline">{offer.marketplace_code}</Badge>
                        </TableCell>
                      )}
                      {colVis.isVisible("price") && (
                        <TableCell className="text-right font-medium tabular-nums">
                          {offer.current_price.toFixed(2)} <span className="text-white/40 text-[10px]">{offer.currency}</span>
                        </TableCell>
                      )}
                      {colVis.isVisible("buybox") && (
                        <TableCell className="text-right text-white/60 tabular-nums">
                          {offer.buybox_price?.toFixed(2) ?? "—"}
                        </TableCell>
                      )}
                      {colVis.isVisible("status") && (
                        <TableCell>
                          <Badge variant={offer.status === "Active" ? "success" : offer.status === "Inactive" ? "destructive" : "outline"}>
                            {offer.status === "Active" ? "Aktywna" : offer.status === "Inactive" ? "Nieaktywna" : offer.status}
                          </Badge>
                        </TableCell>
                      )}
                      {colVis.isVisible("fulfillment") && (
                        <TableCell>
                          <Badge variant={offer.fulfillment_channel === "FBA" ? "default" : "outline"} className={offer.fulfillment_channel === "FBA" ? "bg-blue-500/20 text-blue-300 border-blue-500/30" : ""}>
                            {offer.fulfillment_channel}
                          </Badge>
                        </TableCell>
                      )}
                      {colVis.isVisible("gap") && (
                        <TableCell className="text-right tabular-nums">
                          {gap != null ? (
                            <span className={gap > 0 ? "text-red-400" : "text-emerald-400"}>
                              {gap > 0 ? "+" : ""}{gap.toFixed(2)}
                            </span>
                          ) : (
                            "—"
                          )}
                        </TableCell>
                      )}
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
          {totalPages > 1 && (
            <ServerPagination
              page={page}
              pages={totalPages}
              total={totalOffers}
              pageSize={PAGE_SIZE}
              onPageChange={setPage}
            />
          )}
          <BatchBar selectedCount={selectedIds.size} onClear={() => setSelectedIds(new Set())}>
            <BatchActionButton label="Utwórz task cenowy" onClick={() => { /* placeholder */ }} />
          </BatchBar>
        </CardContent>
      </Card>
    </div>
  );
}
