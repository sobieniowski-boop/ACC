import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from "recharts";
import { DollarSign, TrendingUp, MousePointer, Eye, AlertTriangle } from "lucide-react";
import { getAdsSummary, getAdsChart, getTopCampaigns } from "@/lib/api";
import { formatPLN } from "@/lib/utils";
import { DataFreshness, ClientExportButton, StickyFilterBar } from "@/components/shared";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";

const DAY_OPTIONS = [7, 14, 30, 60, 90] as const;
type Days = (typeof DAY_OPTIONS)[number];

export default function Ads() {
  const [searchParams, setSearchParams] = useSearchParams();
  const days = (Number(searchParams.get("days")) || 30) as Days;

  const setDays = useCallback((d: Days) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      if (d === 30) next.delete("days"); else next.set("days", String(d));
      return next;
    }, { replace: true });
  }, [setSearchParams]);

  const { data: summary, isLoading: loadingSummary, isError: summaryError } = useQuery({
    queryKey: ["ads-summary", days],
    queryFn: () => getAdsSummary(days),
    staleTime: 120_000,
  });

  const { data: chartData, isLoading: loadingChart } = useQuery({
    queryKey: ["ads-chart", days],
    queryFn: () => getAdsChart(days),
    staleTime: 120_000,
  });

  const { data: topCampaigns, isLoading: loadingCampaigns } = useQuery({
    queryKey: ["top-campaigns", days],
    queryFn: () => getTopCampaigns(days),
    staleTime: 120_000,
  });

  function acosClass(acos: number) {
    if (acos < 10) return "text-emerald-400";
    if (acos < 20) return "text-[#FF9900]";
    return "text-red-400";
  }

  function efficiencyClass(score: number) {
    if (score >= 70) return "text-emerald-400";
    if (score >= 40) return "text-[#FF9900]";
    return "text-red-400";
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex flex-col gap-1">
          <h1 className="text-2xl font-bold text-white">Reklamy & PPC</h1>
          <div className="flex items-center gap-3">
            <p className="text-white/50 text-sm">
              Wydatki reklamowe, ACoS, ROAS i wydajność kampanii
            </p>
            <DataFreshness lastSync={(summary as any)?.last_sync} staleMinutes={60} label="Ads" />
          </div>
        </div>
        <div className="flex items-center gap-2">
          {topCampaigns && topCampaigns.length > 0 && (
            <ClientExportButton
              data={topCampaigns}
              filename={`ads_top_campaigns_${days}d`}
              label="Export CSV"
            />
          )}
        </div>

      <StickyFilterBar>
        <div className="flex items-center gap-2">
          {DAY_OPTIONS.map((d) => (
            <button
              key={d}
              onClick={() => setDays(d)}
              className={`px-3 py-1.5 rounded text-sm font-medium transition-colors ${
                days === d
                  ? "bg-[#FF9900] text-black"
                  : "bg-white/10 text-white hover:bg-white/20"
              }`}
            >
              {d}d
            </button>
          ))}
        </div>
      </StickyFilterBar>
      </div>

      {summaryError && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">Failed to load ads data. Please try again later.</p>
        </div>
      )}

      {/* KPI tiles */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-xs text-white/50 flex items-center gap-2">
              <DollarSign className="w-4 h-4 text-red-400" /> Wydatki Ads
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loadingSummary ? <Skeleton className="h-8 w-28" /> : (
              <>
                <div className="text-2xl font-bold text-white">
                  {formatPLN(summary?.total_spend_pln ?? 0)}
                </div>
                <div className="text-xs text-white/40 mt-1">ostatnie {days} dni</div>
              </>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-xs text-white/50 flex items-center gap-2">
              <TrendingUp className="w-4 h-4 text-emerald-400" /> Sprzedaż z Ads
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loadingSummary ? <Skeleton className="h-8 w-28" /> : (
              <>
                <div className="text-2xl font-bold text-white">
                  {formatPLN(summary?.total_sales_pln ?? 0)}
                </div>
                <div className="text-xs text-white/40 mt-1">
                  ROAS: <span className="text-[#FF9900]">{summary?.avg_roas.toFixed(2) ?? "—"}x</span>
                </div>
              </>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-xs text-white/50 flex items-center gap-2">
              <MousePointer className="w-4 h-4 text-blue-400" /> Avg ACoS
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loadingSummary ? <Skeleton className="h-8 w-20" /> : (
              <>
                <div className={`text-2xl font-bold ${acosClass(summary?.avg_acos ?? 0)}`}>
                  {summary?.avg_acos.toFixed(1)}%
                </div>
                <div className="text-xs text-white/40 mt-1">
                  CPC: {summary?.avg_cpc.toFixed(2) ?? "—"}
                </div>
              </>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-xs text-white/50 flex items-center gap-2">
              <Eye className="w-4 h-4 text-purple-400" /> Kliknięcia
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loadingSummary ? <Skeleton className="h-8 w-20" /> : (
              <>
                <div className="text-2xl font-bold text-white">
                  {(summary?.total_clicks ?? 0).toLocaleString()}
                </div>
                <div className="text-xs text-white/40 mt-1">
                  CTR: {summary?.avg_ctr.toFixed(3) ?? "—"}%
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Spend vs Sales chart */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Wydatki vs Sprzedaż z Ads — ostatnie {days} dni</CardTitle>
        </CardHeader>
        <CardContent>
          {loadingChart ? (
            <Skeleton className="h-56 w-full" />
          ) : (
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart
                data={chartData?.points ?? []}
                margin={{ top: 0, right: 8, left: 0, bottom: 0 }}
              >
                <defs>
                  <linearGradient id="salesGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#FF9900" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#FF9900" stopOpacity={0} />
                  </linearGradient>
                  <linearGradient id="spendGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                <XAxis dataKey="report_date" tick={{ fontSize: 10, fill: "#9ca3af" }} tickFormatter={(v) => v.slice(5)} />
                <YAxis tick={{ fontSize: 10, fill: "#9ca3af" }} tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
                <Tooltip
                  formatter={(v: number, name: string) => [formatPLN(v), name]}
                  contentStyle={{ background: "#1e293b", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 8 }}
                  labelStyle={{ color: "#fff" }}
                />
                <Area type="monotone" dataKey="sales_pln" name="Sprzedaż" stroke="#FF9900" fill="url(#salesGrad)" strokeWidth={2} />
                <Area type="monotone" dataKey="spend_pln" name="Wydatki" stroke="#ef4444" fill="url(#spendGrad)" strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
          )}
        </CardContent>
      </Card>

      {/* Top Campaigns table */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Top Kampanie</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {loadingCampaigns ? (
            <div className="p-6 space-y-2">
              {Array.from({ length: 8 }).map((_, i) => <Skeleton key={i} className="h-8 w-full" />)}
            </div>
          ) : topCampaigns && topCampaigns.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Kampania</TableHead>
                  <TableHead>Marketplace</TableHead>
                  <TableHead className="text-right">Wydatki</TableHead>
                  <TableHead className="text-right">Sprzedaż</TableHead>
                  <TableHead className="text-right">ACoS</TableHead>
                  <TableHead className="text-right">ROAS</TableHead>
                  <TableHead className="text-right">Zamówienia</TableHead>
                  <TableHead className="text-right">Ocena</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {topCampaigns.map((c) => (
                  <TableRow key={c.campaign_id}>
                    <TableCell className="text-xs max-w-48 truncate" title={c.campaign_name}>
                      {c.campaign_name}
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">{c.marketplace_code}</Badge>
                    </TableCell>
                    <TableCell className="text-right">{formatPLN(c.total_spend_pln)}</TableCell>
                    <TableCell className="text-right">{formatPLN(c.total_sales_pln)}</TableCell>
                    <TableCell className={`text-right ${acosClass(c.avg_acos)}`}>
                      {c.avg_acos.toFixed(1)}%
                    </TableCell>
                    <TableCell className="text-right">{c.avg_roas.toFixed(2)}x</TableCell>
                    <TableCell className="text-right">{c.orders.toLocaleString()}</TableCell>
                    <TableCell className={`text-right font-bold ${efficiencyClass(c.efficiency_score)}`}>
                      {c.efficiency_score.toFixed(0)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <div className="p-12 text-center text-white/40">Brak danych kampanii</div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
