import { useQuery } from "@tanstack/react-query";
import { getJobs } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

const FBA_JOB_LABELS: Record<string, string> = {
  sync_fba_inventory: "FBA Inventory Sync",
  sync_fba_inbound: "FBA Inbound Sync",
  run_fba_alerts: "FBA Alert Engine",
};

const FBA_JOB_TYPES = Object.keys(FBA_JOB_LABELS);

function statusVariant(status?: string): "secondary" | "warning" | "destructive" | "success" {
  if (status === "running") return "warning";
  if (status === "failure" || status === "failed") return "destructive";
  if (status === "success" || status === "completed") return "success";
  return "secondary";
}

function summarizeJobMessage(jobType: string, message?: string) {
  if (!message) return "Brak uruchomienia";
  if (jobType === "sync_fba_inventory") {
    const rows = message.match(/synced=(\d+)/i)?.[1];
    const fatalCount = (message.match(/FATAL/g) || []).length;
    const fallbackCount = (message.match(/fallback_inventory_api/g) || []).length;
    const cooldownCount = (message.match(/COOLDOWN/g) || []).length;
    const parts = [
      rows ? `Snapshot: ${rows} SKU` : "",
      fallbackCount ? `API fallback: ${fallbackCount}` : "",
      cooldownCount ? `Cooldown: ${cooldownCount}` : "",
      fatalCount ? `Problemy: ${fatalCount}` : "",
    ].filter(Boolean);
    return parts.join(" | ") || "Sync zakończony";
  }
  if (jobType === "run_fba_alerts") {
    const created = message.match(/(?:alerts_created|fba_alerts_created)=([0-9]+)/i)?.[1];
    return created ? `Utworzone alerty: ${created}` : message;
  }
  if (jobType === "sync_fba_inbound") {
    const rows = message.match(/(?:shipments|synced|rows)=([0-9]+)/i)?.[1];
    return rows ? `Shipmenty: ${rows}` : message;
  }
  return message;
}

export function FbaJobStatusStrip() {
  const { data: jobs } = useQuery({
    queryKey: ["fba-sync-jobs-strip"],
    queryFn: () => getJobs({ page_size: 100 }),
    refetchInterval: 30_000,
  });

  return (
    <div className="grid gap-4 md:grid-cols-3">
      {FBA_JOB_TYPES.map((jobType) => {
        const job = (jobs?.items ?? []).find((item) => item.job_type === jobType);
        return (
          <Card key={jobType}>
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-white/60">{FBA_JOB_LABELS[jobType]}</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              <Badge variant={statusVariant(job?.status)}>{job?.status ?? "no-run"}</Badge>
              <div className="text-sm font-medium text-white/85">{summarizeJobMessage(jobType, job?.progress_message)}</div>
              <div className="mt-2 text-[11px] text-white/35">
                {job?.created_at ? new Date(job.created_at).toLocaleString("pl-PL") : "-"}
              </div>
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}
