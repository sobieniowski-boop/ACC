import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { getManageInventoryJobs, runManageInventoryJob, type ManageInventoryJobItem } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { ClientExportButton } from "@/components/shared";

const JOB_TYPES: Array<{
  key: "inventory_sync_listings" | "inventory_sync_snapshots" | "inventory_sync_sales_traffic" | "inventory_compute_rollups" | "inventory_run_alerts";
  label: string;
  note: string;
}> = [
  { key: "inventory_sync_listings", label: "Sync listings", note: "Refresh marketplace listing state and family context." },
  { key: "inventory_sync_snapshots", label: "Sync inventory", note: "Refresh FBA inventory snapshot feeding manage inventory." },
  { key: "inventory_sync_sales_traffic", label: "Sync sales & traffic", note: "Connector placeholder until Sales & Traffic reports are fully validated." },
  { key: "inventory_compute_rollups", label: "Compute rollups", note: "Rebuild 7d/30d traffic rollups for inventory table." },
  { key: "inventory_run_alerts", label: "Run alerts", note: "Evaluate decision-focused inventory alert candidates." },
];

function statusVariant(status: string): "success" | "secondary" | "warning" | "destructive" {
  if (status === "completed" || status === "success") return "success";
  if (status === "running" || status === "pending") return "warning";
  if (status === "failed" || status === "error") return "destructive";
  return "secondary";
}

export default function InventoryJobsPage() {
  const qc = useQueryClient();
  const jobsQuery = useQuery({
    queryKey: ["manage-inventory-jobs"],
    queryFn: getManageInventoryJobs,
    staleTime: 20_000,
    refetchInterval: 20_000,
    placeholderData: keepPreviousData,
  });

  const runMutation = useMutation({
    mutationFn: (jobType: typeof JOB_TYPES[number]["key"]) => runManageInventoryJob(jobType),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["manage-inventory-jobs"] }),
  });

  const latestByType = new Map((jobsQuery.data?.latest_by_type ?? []).map((job) => [job.job_type, job]));

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Inventory jobs</h1>
        <p className="text-sm text-white/50">Manual controls and observability for heavy inventory sync and rollup work.</p>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {JOB_TYPES.map((job) => (
          <Card key={job.key}>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">{job.label}</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="text-sm text-white/55">{job.note}</div>
              {latestByType.get(job.key) ? (
                <div className="rounded-md border border-white/10 bg-white/[0.03] p-3">
                  <div className="flex items-center justify-between gap-3">
                    <div className="text-xs text-white/55">Current status</div>
                    <Badge variant={statusVariant(latestByType.get(job.key)?.status ?? "unknown")}>
                      {latestByType.get(job.key)?.status ?? "unknown"}
                    </Badge>
                  </div>
                  <div className="mt-2 text-xs text-white/70">
                    {latestByType.get(job.key)?.progress_message ?? "-"}
                  </div>
                  <div className="mt-2 grid gap-1 text-[11px] text-white/45">
                    <div>Rows: {latestByType.get(job.key)?.records_processed ?? "-"}</div>
                    <div>
                      Started:{" "}
                      {latestByType.get(job.key)?.started_at
                        ? new Date(latestByType.get(job.key)!.started_at!).toLocaleString()
                        : "-"}
                    </div>
                    <div>
                      Finished:{" "}
                      {latestByType.get(job.key)?.finished_at
                        ? new Date(latestByType.get(job.key)!.finished_at!).toLocaleString()
                        : "-"}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="rounded-md border border-dashed border-white/10 p-3 text-xs text-white/45">
                  No runs yet for this job type.
                </div>
              )}
              <Button onClick={() => runMutation.mutate(job.key)} disabled={runMutation.isPending}>
                {runMutation.isPending && runMutation.variables === job.key ? "Running..." : "Run job"}
              </Button>
            </CardContent>
          </Card>
        ))}
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-sm">Recent job history</CardTitle>
          <ClientExportButton data={jobsQuery.data?.items ?? []} filename="inventory_jobs" />
        </CardHeader>
        <CardContent className="space-y-3">
          {(jobsQuery.data?.items ?? []).map((job: ManageInventoryJobItem) => (
            <div key={job.id} className="rounded-lg border border-white/10 p-3">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <div className="font-medium text-white">{job.job_type}</div>
                  <div className="text-xs text-white/45">{job.progress_message ?? "-"}</div>
                </div>
                <Badge variant={statusVariant(job.status)}>{job.status}</Badge>
              </div>
              <div className="mt-3">
                <Progress value={job.progress_pct ?? 0} />
              </div>
              <div className="mt-2 grid gap-2 text-xs text-white/50 md:grid-cols-4">
                <div>Progress: {job.progress_pct ?? 0}%</div>
                <div>Rows: {job.records_processed ?? "-"}</div>
                <div>Started: {job.started_at ? new Date(job.started_at).toLocaleString() : "-"}</div>
                <div>Finished: {job.finished_at ? new Date(job.finished_at).toLocaleString() : "-"}</div>
              </div>
              {job.error_message ? <div className="mt-2 text-xs text-red-300">{job.error_message}</div> : null}
            </div>
          ))}
          {!jobsQuery.isLoading && (jobsQuery.data?.items.length ?? 0) === 0 ? (
            <div className="text-sm text-white/50">Brak jobow inventory.</div>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}
