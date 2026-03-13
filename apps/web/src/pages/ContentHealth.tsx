import { useQuery } from "@tanstack/react-query";
import { getContentOpsHealth } from "@/lib/api";

export default function ContentHealthPage() {
  const healthQuery = useQuery({
    queryKey: ["content-ops-health"],
    queryFn: getContentOpsHealth,
    refetchInterval: 15_000,
  });

  const data = healthQuery.data;

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold">Content Ops Health</h1>
        <p className="text-sm text-muted-foreground">Operational health dashboard for queue, compliance, tasks and data quality</p>
      </div>

      <div className="grid gap-2 md:grid-cols-6">
        <div className="rounded border border-border bg-card p-3 text-xs">queued: {data?.queue_health.queued_total ?? 0}</div>
        <div className="rounded border border-border bg-card p-3 text-xs">stale queued: {data?.queue_health.queued_stale_30m ?? 0}</div>
        <div className="rounded border border-border bg-card p-3 text-xs">retry in progress: {data?.queue_health.retry_in_progress ?? 0}</div>
        <div className="rounded border border-border bg-card p-3 text-xs">failed 24h: {data?.queue_health.failed_last_24h ?? 0}</div>
        <div className="rounded border border-border bg-card p-3 text-xs">compliance critical: {data?.compliance_backlog?.critical ?? 0}</div>
        <div className="rounded border border-border bg-card p-3 text-xs">tasks overdue: {data?.tasks_health?.overdue ?? 0}</div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4">
        <h2 className="mb-2 text-sm font-semibold">Data Quality Cards</h2>
        <div className="grid gap-2 md:grid-cols-3">
          {(data?.data_quality_cards ?? []).map((c) => (
            <div key={c.key} className="rounded border border-border p-2 text-xs">
              <div className="font-medium">{c.key}</div>
              <div>{Number(c.value).toFixed(2)}{c.unit === "pct" ? "%" : ""}</div>
              {c.note && <div className="text-muted-foreground">{c.note}</div>}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
