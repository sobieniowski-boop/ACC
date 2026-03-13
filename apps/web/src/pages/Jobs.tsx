import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { format } from "date-fns";
import { Play, RefreshCw } from "lucide-react";
import { getJobs, getNetfoxSessionHealth, runJob } from "@/lib/api";
import { cn } from "@/lib/utils";
import { ClientExportButton } from "@/components/shared";

const JOB_TYPES = [
  { value: "sync_orders", label: "Sync Orders" },
  { value: "sync_finances", label: "Sync Finances" },
  { value: "sync_inventory", label: "Sync Inventory" },
  { value: "sync_pricing", label: "Sync Pricing / Buy Box" },
  { value: "sync_offer_fee_estimates", label: "Sync Expected Fees (SP-API)" },
  { value: "sync_tkl_cache", label: "Sync TKL SQL Cache" },
  { value: "sync_fba_inventory", label: "Sync FBA Inventory" },
  { value: "sync_fba_inbound", label: "Sync FBA Inbound" },
  { value: "run_fba_alerts", label: "Run FBA Alerts" },
  { value: "recompute_fba_replenishment", label: "Recompute FBA Replenishment" },
  { value: "calc_profit", label: "Recalculate Profit" },
  { value: "recompute_profitability", label: "Recompute Profitability Rollups" },
  { value: "generate_ai_report", label: "AI Report" },
];

const STATUS_CLASSES: Record<string, string> = {
  pending: "bg-muted text-muted-foreground",
  running: "bg-blue-500/10 text-blue-500",
  success: "bg-green-500/10 text-green-500",
  failure: "bg-destructive/10 text-destructive",
  revoked: "bg-muted text-muted-foreground",
};

export default function JobsPage() {
  const [jobType, setJobType] = useState("sync_orders");
  const qc = useQueryClient();

  const { data, isLoading } = useQuery({
    queryKey: ["jobs"],
    queryFn: () => getJobs({ page_size: 30 }),
    staleTime: 15_000,
    refetchInterval: 15_000,
  });
  const netfoxHealthQuery = useQuery({
    queryKey: ["netfox-session-health-jobs"],
    queryFn: getNetfoxSessionHealth,
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  const trigger = useMutation({
    mutationFn: () => runJob(jobType),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["jobs"] }),
  });

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold">Jobs</h1>
        <p className="text-sm text-muted-foreground">Manage background sync tasks</p>
      </div>

      {/* Trigger */}
      <div className="flex gap-3 rounded-xl border border-border bg-card p-4">
        <select
          value={jobType}
          onChange={(e) => setJobType(e.target.value)}
          className="rounded-md border border-input bg-background px-3 py-1.5 text-sm outline-none focus:ring-1 focus:ring-ring"
        >
          {JOB_TYPES.map((j) => (
            <option key={j.value} value={j.value}>{j.label}</option>
          ))}
        </select>
        <button
          onClick={() => trigger.mutate()}
          disabled={trigger.isPending}
          className="flex items-center gap-2 rounded-md bg-amazon px-4 py-1.5 text-sm font-semibold text-black hover:bg-amazon-dark disabled:opacity-60"
        >
          <Play className="h-3.5 w-3.5" />
          {trigger.isPending ? "Starting…" : "Run Job"}
        </button>
        <button
          onClick={() => qc.invalidateQueries({ queryKey: ["jobs"] })}
          className="ml-auto rounded-md p-1.5 text-muted-foreground hover:bg-muted"
          title="Refresh"
        >
          <RefreshCw className="h-4 w-4" />
        </button>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <div className="rounded-xl border border-border bg-card p-4">
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Netfox sessions</div>
          <div className="mt-2 text-2xl font-semibold">
            {netfoxHealthQuery.data?.session_count ?? "-"}
          </div>
          <div className="mt-1 text-xs text-muted-foreground">
            {netfoxHealthQuery.data?.ok ? "ACC-Netfox-RO active sessions" : (netfoxHealthQuery.data?.error ?? "Health error")}
          </div>
        </div>
      </div>

      {/* Jobs table */}
      <div className="flex items-center justify-between">
        <span className="text-xs text-muted-foreground">{data?.items.length ?? 0} jobs</span>
        <ClientExportButton data={data?.items ?? []} filename="jobs" />
      </div>
      <div className="rounded-xl border border-border bg-card overflow-hidden">
        <table className="w-full text-[11px]">
          <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
            <tr>
              <th className="px-2 py-2">Type</th>
              <th className="px-2 py-2">Status</th>
              <th className="px-2 py-2">Progress</th>
              <th className="px-2 py-2">Records</th>
              <th className="px-2 py-2">Duration</th>
              <th className="px-2 py-2">Started</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {isLoading
              ? [...Array(5)].map((_, i) => (
                  <tr key={i}>
                    <td colSpan={6} className="px-2 py-1.5">
                      <div className="h-4 animate-pulse rounded bg-muted" />
                    </td>
                  </tr>
                ))
              : data?.items.map((job) => (
                  <tr key={job.id} className="hover:bg-muted/20 transition-colors">
                    <td className="px-2 py-1.5 font-medium">{job.job_type}</td>
                    <td className="px-2 py-1.5">
                      <span className={cn(
                        "rounded px-2 py-0.5 text-xs font-medium",
                        STATUS_CLASSES[job.status] ?? "bg-muted text-muted-foreground"
                      )}>
                        {job.status}
                      </span>
                    </td>
                    <td className="px-2 py-1.5">
                      {job.status === "running" ? (
                        <div className="flex items-center gap-2">
                          <div className="h-1.5 flex-1 rounded-full bg-muted">
                            <div
                              className="h-1.5 rounded-full bg-blue-500 transition-all"
                              style={{ width: `${job.progress_pct}%` }}
                            />
                          </div>
                          <span className="text-xs">{job.progress_pct}%</span>
                        </div>
                      ) : (
                        <span className="text-xs text-muted-foreground">
                          {job.progress_message ?? (job.status === "success" ? "Done" : "—")}
                        </span>
                      )}
                    </td>
                    <td className="px-2 py-1.5 tabular-nums text-xs text-muted-foreground">
                      {job.records_processed?.toLocaleString("pl-PL") ?? "—"}
                    </td>
                    <td className="px-2 py-1.5 tabular-nums text-xs text-muted-foreground">
                      {job.duration_seconds != null ? `${job.duration_seconds.toFixed(1)}s` : "—"}
                    </td>
                    <td className="px-2 py-1.5 text-xs text-muted-foreground">
                      {format(new Date(job.created_at), "dd.MM HH:mm")}
                    </td>
                  </tr>
                ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
