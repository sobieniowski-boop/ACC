import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getBuyBoxDashboard,
  getBuyBoxLandscape,
  getBuyBoxAlerts,
  getCompetitorPriceHistory,
  triggerCompetitorCapture,
  type LandscapeEntry,
  type CompetitorPriceDay,
  type BuyBoxDashboard,
} from "@/lib/api";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line, Legend,
} from "recharts";

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------
function KpiCard({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="rounded-xl border bg-white p-4 shadow-sm">
      <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</p>
      <p className="mt-1 text-2xl font-bold text-gray-900">{value}</p>
      {sub && <p className="mt-0.5 text-xs text-gray-400">{sub}</p>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------
export default function CompetitorRadarPage() {
  const [marketplace, setMarketplace] = useState<string>("");
  const [selectedAsin, setSelectedAsin] = useState<string | null>(null);
  const queryClient = useQueryClient();

  // Dashboard KPIs
  const { data: dashboard } = useQuery<BuyBoxDashboard>({
    queryKey: ["buybox-dashboard", marketplace],
    queryFn: () => getBuyBoxDashboard(marketplace ? { marketplace_id: marketplace, days: 7 } : { days: 7 }),
  });

  // Landscape table
  const { data: landscape, isLoading: landscapeLoading } = useQuery({
    queryKey: ["buybox-landscape", marketplace],
    queryFn: () => getBuyBoxLandscape(marketplace ? { marketplace_id: marketplace, limit: 100 } : { limit: 100 }),
  });

  // Price history for selected ASIN
  const { data: priceHistory } = useQuery({
    queryKey: ["competitor-price-history", selectedAsin, marketplace],
    queryFn: () =>
      selectedAsin && marketplace
        ? getCompetitorPriceHistory(selectedAsin, marketplace, 30)
        : Promise.resolve(null),
    enabled: !!selectedAsin && !!marketplace,
  });

  // Alerts
  const { data: alerts } = useQuery({
    queryKey: ["buybox-alerts", marketplace],
    queryFn: () => getBuyBoxAlerts(marketplace ? { marketplace_id: marketplace, limit: 20 } : { limit: 20 }),
  });

  // Capture trigger
  const captureMutation = useMutation({
    mutationFn: (mkt: string) => triggerCompetitorCapture(mkt),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["buybox-landscape"] });
      queryClient.invalidateQueries({ queryKey: ["buybox-dashboard"] });
    },
  });

  const rows: LandscapeEntry[] = landscape?.landscape ?? [];
  const history: CompetitorPriceDay[] = priceHistory?.history ?? [];

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold tracking-tight">Competitor Radar</h1>
        <div className="flex items-center gap-3">
          <input
            className="rounded-md border px-3 py-1.5 text-sm"
            placeholder="Marketplace ID"
            value={marketplace}
            onChange={(e) => setMarketplace(e.target.value)}
          />
          <button
            className="rounded-md bg-blue-600 px-4 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
            disabled={!marketplace || captureMutation.isPending}
            onClick={() => marketplace && captureMutation.mutate(marketplace)}
          >
            {captureMutation.isPending ? "Capturing…" : "Capture Now"}
          </button>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <KpiCard
          label="ASINs Tracked"
          value={dashboard?.asins_tracked ?? "—"}
        />
        <KpiCard
          label="Buy Box Win Rate"
          value={dashboard?.overall_win_rate != null ? `${(dashboard.overall_win_rate * 100).toFixed(1)}%` : "—"}
        />
        <KpiCard
          label="Total Snapshots"
          value={dashboard?.total_snapshots?.toLocaleString() ?? "—"}
          sub="Last 7 days"
        />
        <KpiCard
          label="Trend"
          value={dashboard?.trend_direction ?? "—"}
        />
      </div>

      {/* Alerts banner */}
      {(alerts?.count ?? 0) > 0 && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
          <strong>{alerts!.count} active alert{alerts!.count > 1 ? "s" : ""}</strong>
          {" — SKUs with sustained Buy Box loss detected."}
        </div>
      )}

      {/* Price history chart */}
      {selectedAsin && history.length > 0 && (
        <div className="rounded-xl border bg-white p-4 shadow-sm">
          <h2 className="mb-3 text-sm font-semibold text-gray-700">
            Price History — {selectedAsin}
          </h2>
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={history}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="date" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="min_price" stroke="#22c55e" name="Min" dot={false} />
              <Line type="monotone" dataKey="avg_price" stroke="#3b82f6" name="Avg" dot={false} />
              <Line type="monotone" dataKey="max_price" stroke="#ef4444" name="Max" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Seller distribution chart */}
      {rows.length > 0 && (
        <div className="rounded-xl border bg-white p-4 shadow-sm">
          <h2 className="mb-3 text-sm font-semibold text-gray-700">Sellers per ASIN (Top 20)</h2>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={rows.slice(0, 20)}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="asin" tick={{ fontSize: 10 }} angle={-35} textAnchor="end" height={60} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Bar dataKey="fba_sellers" stackId="a" fill="#3b82f6" name="FBA" />
              <Bar dataKey="fbm_sellers" stackId="a" fill="#f59e0b" name="FBM" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Landscape table */}
      <div className="rounded-xl border bg-white shadow-sm">
        <div className="border-b px-4 py-3">
          <h2 className="text-sm font-semibold text-gray-700">Competitive Landscape</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="border-b bg-gray-50 text-xs font-medium uppercase text-gray-500">
              <tr>
                <th className="px-4 py-2">ASIN</th>
                <th className="px-4 py-2">Marketplace</th>
                <th className="px-4 py-2 text-right">Sellers</th>
                <th className="px-4 py-2 text-right">FBA</th>
                <th className="px-4 py-2 text-right">FBM</th>
                <th className="px-4 py-2 text-right">Min Price</th>
                <th className="px-4 py-2 text-right">Avg Price</th>
                <th className="px-4 py-2 text-right">Max Price</th>
                <th className="px-4 py-2">BB Winner</th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {landscapeLoading && (
                <tr><td colSpan={9} className="px-4 py-8 text-center text-gray-400">Loading…</td></tr>
              )}
              {!landscapeLoading && rows.length === 0 && (
                <tr><td colSpan={9} className="px-4 py-8 text-center text-gray-400">No competitor data captured yet.</td></tr>
              )}
              {rows.map((r) => (
                <tr
                  key={`${r.asin}-${r.marketplace_id}`}
                  className={`cursor-pointer hover:bg-gray-50 ${selectedAsin === r.asin ? "bg-blue-50" : ""}`}
                  onClick={() => {
                    setSelectedAsin(r.asin);
                    if (!marketplace) setMarketplace(r.marketplace_id);
                  }}
                >
                  <td className="px-4 py-2 font-mono text-xs">{r.asin}</td>
                  <td className="px-4 py-2">{r.marketplace_id}</td>
                  <td className="px-4 py-2 text-right font-semibold">{r.total_sellers}</td>
                  <td className="px-4 py-2 text-right">{r.fba_sellers}</td>
                  <td className="px-4 py-2 text-right">{r.fbm_sellers}</td>
                  <td className="px-4 py-2 text-right">{r.min_price?.toFixed(2) ?? "—"}</td>
                  <td className="px-4 py-2 text-right">{r.avg_price?.toFixed(2) ?? "—"}</td>
                  <td className="px-4 py-2 text-right">{r.max_price?.toFixed(2) ?? "—"}</td>
                  <td className="px-4 py-2 font-mono text-xs text-gray-500">{r.buybox_winner_seller_id ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
