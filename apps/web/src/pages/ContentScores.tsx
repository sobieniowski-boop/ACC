import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getContentScores,
  getContentScoreDistribution,
  getContentOpportunities,
  triggerContentScoring,
  type ContentScore,
  type ContentScoreDistribution,
  type ContentOpportunity,
} from "@/lib/api";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";

/* ── helpers ─────────────────────────────────────────────────────── */

function KpiCard({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="rounded-xl border bg-white p-4 shadow-sm">
      <p className="text-xs text-gray-500">{label}</p>
      <p className="mt-1 text-2xl font-semibold">{value}</p>
      {sub && <p className="mt-0.5 text-xs text-gray-400">{sub}</p>}
    </div>
  );
}

function scoreBadge(score: number) {
  if (score >= 81) return <span className="rounded bg-green-100 px-2 py-0.5 text-xs font-medium text-green-700">Excellent</span>;
  if (score >= 61) return <span className="rounded bg-blue-100 px-2 py-0.5 text-xs font-medium text-blue-700">Good</span>;
  if (score >= 41) return <span className="rounded bg-yellow-100 px-2 py-0.5 text-xs font-medium text-yellow-700">Average</span>;
  if (score >= 21) return <span className="rounded bg-orange-100 px-2 py-0.5 text-xs font-medium text-orange-700">Below Avg</span>;
  return <span className="rounded bg-red-100 px-2 py-0.5 text-xs font-medium text-red-700">Poor</span>;
}

const DIST_COLORS = ["#ef4444", "#f97316", "#eab308", "#3b82f6", "#22c55e"];

/* ── page ────────────────────────────────────────────────────────── */

export default function ContentScoresPage() {
  const [tab, setTab] = useState<"overview" | "listings">("overview");
  const qc = useQueryClient();

  const { data: dist, isLoading: distLoading } = useQuery({
    queryKey: ["content-opt-dist"],
    queryFn: () => getContentScoreDistribution(),
    refetchInterval: 60_000,
  });

  const { data: opps } = useQuery({
    queryKey: ["content-opt-opps"],
    queryFn: () => getContentOpportunities({ limit: 15 }),
    enabled: tab === "overview",
  });

  const { data: scores, isLoading: scoresLoading } = useQuery({
    queryKey: ["content-opt-scores"],
    queryFn: () => getContentScores({ limit: 50 }),
    enabled: tab === "listings",
  });

  const computeMut = useMutation({
    mutationFn: () => triggerContentScoring("A1PA6795UKMFR9"),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["content-opt-dist"] });
      qc.invalidateQueries({ queryKey: ["content-opt-opps"] });
      qc.invalidateQueries({ queryKey: ["content-opt-scores"] });
    },
  });

  const tabs = ["overview", "listings"] as const;

  /* ── distribution chart data ──────────────────────────────────── */
  const chartData = dist
    ? [
        { name: "Poor\n0-20", value: dist.distribution.poor },
        { name: "Below Avg\n21-40", value: dist.distribution.below_avg },
        { name: "Average\n41-60", value: dist.distribution.average },
        { name: "Good\n61-80", value: dist.distribution.good },
        { name: "Excellent\n81-100", value: dist.distribution.excellent },
      ]
    : [];

  return (
    <div className="space-y-6 p-6">
      {/* header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Content Quality Scores</h1>
          <p className="text-sm text-gray-500">
            Listing content quality analysis &amp; optimization opportunities
          </p>
        </div>
        <button
          onClick={() => computeMut.mutate()}
          disabled={computeMut.isPending}
          className="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
        >
          {computeMut.isPending ? "Scoring…" : "Run Scoring"}
        </button>
      </div>

      {/* tabs */}
      <div className="flex gap-1 rounded-lg bg-gray-100 p-1 w-fit">
        {tabs.map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`rounded-md px-4 py-1.5 text-sm font-medium capitalize transition ${
              tab === t ? "bg-white shadow text-gray-900" : "text-gray-500 hover:text-gray-700"
            }`}
          >
            {t}
          </button>
        ))}
      </div>

      {/* ═══ overview tab ═══════════════════════════════════════════ */}
      {tab === "overview" && (
        <>
          {/* KPIs */}
          <div className="grid grid-cols-2 gap-4 sm:grid-cols-4 lg:grid-cols-6">
            <KpiCard
              label="Avg Content Score"
              value={dist?.avg_score != null ? Math.round(dist.avg_score) : "—"}
              sub="out of 100"
            />
            <KpiCard label="Listings Scored" value={dist?.total ?? 0} />
            <KpiCard label="Excellent (81+)" value={dist?.distribution.excellent ?? 0} />
            <KpiCard label="Poor (0-20)" value={dist?.distribution.poor ?? 0} />
            <KpiCard
              label="Avg Title Score"
              value={dist?.avg_by_component?.title != null ? Math.round(dist.avg_by_component.title) : "—"}
            />
            <KpiCard
              label="Avg Bullet Score"
              value={dist?.avg_by_component?.bullets != null ? Math.round(dist.avg_by_component.bullets) : "—"}
            />
          </div>

          {/* distribution chart */}
          {!distLoading && chartData.length > 0 && (
            <div className="rounded-xl border bg-white p-4 shadow-sm">
              <h2 className="mb-3 text-sm font-semibold text-gray-700">Score Distribution</h2>
              <ResponsiveContainer width="100%" height={240}>
                <BarChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} />
                  <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                  <YAxis allowDecimals={false} />
                  <Tooltip />
                  <Bar dataKey="value" radius={[6, 6, 0, 0]}>
                    {chartData.map((_, i) => (
                      <Cell key={i} fill={DIST_COLORS[i]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* component averages */}
          {dist?.avg_by_component && (
            <div className="rounded-xl border bg-white p-4 shadow-sm">
              <h2 className="mb-3 text-sm font-semibold text-gray-700">Average by Component</h2>
              <div className="space-y-2">
                {(["title", "bullets", "description", "keywords", "images", "aplus"] as const).map((key) => {
                  const val = dist.avg_by_component[key];
                  return (
                    <div key={key} className="flex items-center gap-3">
                      <span className="w-24 text-xs font-medium text-gray-500 capitalize">{key}</span>
                      <div className="flex-1 rounded-full bg-gray-100 h-3">
                        <div
                          className="h-3 rounded-full bg-indigo-500"
                          style={{ width: `${Math.max(0, Math.min(100, val ?? 0))}%` }}
                        />
                      </div>
                      <span className="w-10 text-right text-xs font-medium text-gray-600">
                        {val != null ? Math.round(val) : "—"}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* top opportunities */}
          {opps && opps.length > 0 && (
            <div className="rounded-xl border bg-white shadow-sm overflow-hidden">
              <div className="px-4 py-3 border-b">
                <h2 className="text-sm font-semibold text-gray-700">Top Optimization Opportunities</h2>
              </div>
              <div className="overflow-x-auto">
                <table className="min-w-full text-sm">
                  <thead className="bg-gray-50 text-left text-xs uppercase text-gray-500">
                    <tr>
                      <th className="px-4 py-2">SKU</th>
                      <th className="px-4 py-2">ASIN</th>
                      <th className="px-4 py-2">Score</th>
                      <th className="px-4 py-2">Title</th>
                      <th className="px-4 py-2">Bullets</th>
                      <th className="px-4 py-2">Desc</th>
                      <th className="px-4 py-2">KW</th>
                      <th className="px-4 py-2">Imgs</th>
                      <th className="px-4 py-2">Issues</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    {opps.map((o: ContentOpportunity) => (
                      <tr key={`${o.seller_sku}-${o.marketplace_id}`} className="hover:bg-gray-50">
                        <td className="px-4 py-2 font-mono text-xs">{o.seller_sku}</td>
                        <td className="px-4 py-2 text-xs">{o.asin || "—"}</td>
                        <td className="px-4 py-2">{scoreBadge(o.total_score)}</td>
                        <td className="px-4 py-2 text-center">{o.title_score}</td>
                        <td className="px-4 py-2 text-center">{o.bullet_score}</td>
                        <td className="px-4 py-2 text-center">{o.description_score}</td>
                        <td className="px-4 py-2 text-center">{o.keyword_score}</td>
                        <td className="px-4 py-2 text-center">{o.image_score}</td>
                        <td className="px-4 py-2 text-xs text-gray-500">{o.issues?.length ?? 0}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}

      {/* ═══ listings tab ═══════════════════════════════════════════ */}
      {tab === "listings" && (
        <div className="rounded-xl border bg-white shadow-sm overflow-hidden">
          <div className="overflow-x-auto">
            {scoresLoading ? (
              <p className="p-6 text-sm text-gray-400">Loading scores…</p>
            ) : !scores?.items?.length ? (
              <p className="p-6 text-sm text-gray-400">
                No scores yet. Click &quot;Run Scoring&quot; to compute content quality scores.
              </p>
            ) : (
              <table className="min-w-full text-sm">
                <thead className="bg-gray-50 text-left text-xs uppercase text-gray-500">
                  <tr>
                    <th className="px-4 py-2">SKU</th>
                    <th className="px-4 py-2">ASIN</th>
                    <th className="px-4 py-2">Score</th>
                    <th className="px-4 py-2">Title</th>
                    <th className="px-4 py-2">Bullets</th>
                    <th className="px-4 py-2">Desc</th>
                    <th className="px-4 py-2">KW</th>
                    <th className="px-4 py-2">Imgs</th>
                    <th className="px-4 py-2">A+</th>
                    <th className="px-4 py-2">Scored</th>
                  </tr>
                </thead>
                <tbody className="divide-y">
                  {scores.items.map((s: ContentScore) => (
                    <tr key={s.id} className="hover:bg-gray-50">
                      <td className="px-4 py-2 font-mono text-xs">{s.seller_sku}</td>
                      <td className="px-4 py-2 text-xs">{s.asin || "—"}</td>
                      <td className="px-4 py-2">{scoreBadge(s.total_score)}</td>
                      <td className="px-4 py-2 text-center">{s.title_score}</td>
                      <td className="px-4 py-2 text-center">{s.bullet_score}</td>
                      <td className="px-4 py-2 text-center">{s.description_score}</td>
                      <td className="px-4 py-2 text-center">{s.keyword_score}</td>
                      <td className="px-4 py-2 text-center">{s.image_score}</td>
                      <td className="px-4 py-2 text-center">{s.aplus_score}</td>
                      <td className="px-4 py-2 text-xs text-gray-400">{s.scored_at?.slice(0, 10)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
