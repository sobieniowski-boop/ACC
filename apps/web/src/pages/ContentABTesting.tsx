import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getMultilangJobs,
  getExperiments,
  getExperimentSummary,
  createExperiment,
  startExperiment,
  concludeExperiment,
  generateMultilang,
  type MultilangJob,
  type ContentExperiment,
  type ExperimentSummary,
} from "@/lib/api";

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

function statusBadge(status: string) {
  const m: Record<string, string> = {
    draft: "bg-gray-100 text-gray-700",
    running: "bg-blue-100 text-blue-700",
    concluded: "bg-green-100 text-green-700",
    cancelled: "bg-red-100 text-red-700",
    paused: "bg-yellow-100 text-yellow-700",
    pending: "bg-gray-100 text-gray-600",
    generating: "bg-indigo-100 text-indigo-700",
    completed: "bg-green-100 text-green-700",
    failed: "bg-red-100 text-red-700",
    review: "bg-orange-100 text-orange-700",
  };
  return (
    <span className={`rounded px-2 py-0.5 text-xs font-medium ${m[status] ?? "bg-gray-100 text-gray-700"}`}>
      {status}
    </span>
  );
}

/* ── page ────────────────────────────────────────────────────────── */

export default function ContentABTestingPage() {
  const [tab, setTab] = useState<"experiments" | "multilang">("experiments");
  const qc = useQueryClient();

  /* ── queries ──────────────────────────────────────────────────── */
  const { data: summary } = useQuery<ExperimentSummary>({
    queryKey: ["experiment-summary"],
    queryFn: () => getExperimentSummary(),
  });

  const { data: experiments, isLoading: expLoading } = useQuery<ContentExperiment[]>({
    queryKey: ["experiments"],
    queryFn: () => getExperiments({ limit: 50 }),
    enabled: tab === "experiments",
  });

  const { data: jobs, isLoading: jobsLoading } = useQuery<MultilangJob[]>({
    queryKey: ["multilang-jobs"],
    queryFn: () => getMultilangJobs({ limit: 50 }),
    enabled: tab === "multilang",
    refetchInterval: 30_000,
  });

  /* ── mutations ────────────────────────────────────────────────── */
  const createMut = useMutation({
    mutationFn: (body: { name: string; seller_sku: string; marketplace_id: string; hypothesis?: string; metric_primary?: string }) =>
      createExperiment(body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["experiments"] });
      qc.invalidateQueries({ queryKey: ["experiment-summary"] });
    },
  });

  const startMut = useMutation({
    mutationFn: (id: number) => startExperiment(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["experiments"] });
      qc.invalidateQueries({ queryKey: ["experiment-summary"] });
    },
  });

  const concludeMut = useMutation({
    mutationFn: (id: number) => concludeExperiment(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["experiments"] });
      qc.invalidateQueries({ queryKey: ["experiment-summary"] });
    },
  });

  const genMut = useMutation({
    mutationFn: (body: { seller_sku: string; source_marketplace_id: string }) =>
      generateMultilang(body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["multilang-jobs"] }),
  });

  const tabs = ["experiments", "multilang"] as const;

  return (
    <div className="space-y-6 p-6">
      {/* header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Content A/B Testing</h1>
          <p className="text-sm text-gray-500">
            Multi-language generation &amp; A/B content experiments
          </p>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
        <KpiCard label="Experiments" value={summary?.total ?? "—"} />
        <KpiCard label="Draft" value={summary?.draft ?? 0} />
        <KpiCard label="Running" value={summary?.running ?? 0} />
        <KpiCard label="Concluded" value={summary?.concluded ?? 0} />
        <KpiCard label="Cancelled" value={summary?.cancelled ?? 0} />
      </div>

      {/* tabs */}
      <div className="flex gap-2 border-b">
        {tabs.map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm font-medium capitalize ${
              tab === t ? "border-b-2 border-blue-600 text-blue-600" : "text-gray-500 hover:text-gray-700"
            }`}
          >
            {t === "multilang" ? "Multi-Language" : "Experiments"}
          </button>
        ))}
      </div>

      {/* Experiments tab */}
      {tab === "experiments" && (
        <div className="space-y-4">
          <div className="flex justify-end">
            <button
              className="rounded bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
              disabled={createMut.isPending}
              onClick={() =>
                createMut.mutate({
                  name: `Experiment ${Date.now()}`,
                  seller_sku: "",
                  marketplace_id: "A1PA6795UKMFR9",
                  metric_primary: "conversion_rate",
                })
              }
            >
              + New Experiment
            </button>
          </div>

          {expLoading ? (
            <p className="py-12 text-center text-sm text-gray-400">Loading experiments…</p>
          ) : !experiments?.length ? (
            <p className="py-12 text-center text-sm text-gray-400">No experiments yet</p>
          ) : (
            <div className="overflow-x-auto rounded-xl border bg-white shadow-sm">
              <table className="min-w-full text-sm">
                <thead className="bg-gray-50 text-left text-xs uppercase text-gray-500">
                  <tr>
                    <th className="px-4 py-3">ID</th>
                    <th className="px-4 py-3">Name</th>
                    <th className="px-4 py-3">SKU</th>
                    <th className="px-4 py-3">Status</th>
                    <th className="px-4 py-3">Metric</th>
                    <th className="px-4 py-3">Winner</th>
                    <th className="px-4 py-3">Created</th>
                    <th className="px-4 py-3">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y">
                  {experiments.map((exp) => (
                    <tr key={exp.id} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-mono">{exp.id}</td>
                      <td className="px-4 py-3 font-medium">{exp.name}</td>
                      <td className="px-4 py-3 font-mono text-xs">{exp.seller_sku || "—"}</td>
                      <td className="px-4 py-3">{statusBadge(exp.status)}</td>
                      <td className="px-4 py-3 text-xs">{exp.metric_primary}</td>
                      <td className="px-4 py-3">{exp.winner_variant_id ?? "—"}</td>
                      <td className="px-4 py-3 text-xs text-gray-500">
                        {exp.created_at?.slice(0, 10)}
                      </td>
                      <td className="px-4 py-3 space-x-1">
                        {exp.status === "draft" && (
                          <button
                            className="rounded bg-green-600 px-2 py-1 text-xs text-white hover:bg-green-700"
                            onClick={() => startMut.mutate(exp.id)}
                          >
                            Start
                          </button>
                        )}
                        {exp.status === "running" && (
                          <button
                            className="rounded bg-amber-600 px-2 py-1 text-xs text-white hover:bg-amber-700"
                            onClick={() => concludeMut.mutate(exp.id)}
                          >
                            Conclude
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* Multi-language tab */}
      {tab === "multilang" && (
        <div className="space-y-4">
          <div className="flex justify-end">
            <button
              className="rounded bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700 disabled:opacity-50"
              disabled={genMut.isPending}
              onClick={() =>
                genMut.mutate({
                  seller_sku: "",
                  source_marketplace_id: "A1PA6795UKMFR9",
                })
              }
            >
              {genMut.isPending ? "Generating…" : "Generate All Languages"}
            </button>
          </div>

          {jobsLoading ? (
            <p className="py-12 text-center text-sm text-gray-400">Loading jobs…</p>
          ) : !jobs?.length ? (
            <p className="py-12 text-center text-sm text-gray-400">No multilang jobs yet</p>
          ) : (
            <div className="overflow-x-auto rounded-xl border bg-white shadow-sm">
              <table className="min-w-full text-sm">
                <thead className="bg-gray-50 text-left text-xs uppercase text-gray-500">
                  <tr>
                    <th className="px-4 py-3">SKU</th>
                    <th className="px-4 py-3">Target</th>
                    <th className="px-4 py-3">Language</th>
                    <th className="px-4 py-3">Status</th>
                    <th className="px-4 py-3">Quality</th>
                    <th className="px-4 py-3">Model</th>
                    <th className="px-4 py-3">Created</th>
                  </tr>
                </thead>
                <tbody className="divide-y">
                  {jobs.map((j) => (
                    <tr key={j.id} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-mono text-xs">{j.seller_sku}</td>
                      <td className="px-4 py-3 text-xs">{j.target_marketplace_id}</td>
                      <td className="px-4 py-3 text-xs">{j.target_language}</td>
                      <td className="px-4 py-3">{statusBadge(j.status)}</td>
                      <td className="px-4 py-3">
                        {j.quality_score != null ? (
                          <span className={`font-medium ${j.quality_score >= 50 ? "text-green-600" : "text-orange-600"}`}>
                            {j.quality_score}
                          </span>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="px-4 py-3 text-xs text-gray-500">{j.model ?? "—"}</td>
                      <td className="px-4 py-3 text-xs text-gray-500">
                        {j.created_at?.slice(0, 10)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
