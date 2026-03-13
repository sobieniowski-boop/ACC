import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getSqsQueues,
  getTopologyHealth,
  getDlqSummary,
  getDlqEntries,
  getRoutingTable,
  updateSqsQueueStatus,
  pollDomainQueue,
  pollAllQueues,
  seedTopology,
  resolveDlqEntry,
  type SqsQueueTopology,
  type DlqEntry,
} from "@/lib/api";

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

const statusColors: Record<string, string> = {
  active: "bg-green-100 text-green-800",
  paused: "bg-yellow-100 text-yellow-800",
  error: "bg-red-100 text-red-800",
  disabled: "bg-gray-100 text-gray-600",
};

function Badge({ label, className }: { label: string; className?: string }) {
  return (
    <span className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${className ?? "bg-gray-100 text-gray-600"}`}>
      {label}
    </span>
  );
}

/* ------------------------------------------------------------------ */
/*  Page                                                               */
/* ------------------------------------------------------------------ */

export default function SqsTopologyPage() {
  const qc = useQueryClient();
  const [tab, setTab] = useState<"queues" | "dlq" | "routing">("queues");
  const [dlqDomain, setDlqDomain] = useState<string>("");
  const [dlqStatus, setDlqStatus] = useState<string>("");

  // ── Queries ──────────────────────────────────────────────────────
  const healthQ = useQuery({ queryKey: ["topology-health"], queryFn: getTopologyHealth });
  const queuesQ = useQuery({ queryKey: ["topology-queues"], queryFn: getSqsQueues });
  const dlqSumQ = useQuery({ queryKey: ["dlq-summary"], queryFn: getDlqSummary });
  const dlqQ = useQuery({
    queryKey: ["dlq-entries", dlqDomain, dlqStatus],
    queryFn: () => getDlqEntries({ domain: dlqDomain || undefined, status: dlqStatus || undefined, limit: 50 }),
    enabled: tab === "dlq",
  });
  const routingQ = useQuery({ queryKey: ["routing-table"], queryFn: getRoutingTable, enabled: tab === "routing" });

  // ── Mutations ────────────────────────────────────────────────────
  const toggleM = useMutation({
    mutationFn: ({ domain, enabled }: { domain: string; enabled: boolean }) =>
      updateSqsQueueStatus(domain, { enabled }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["topology-queues"] }); qc.invalidateQueries({ queryKey: ["topology-health"] }); },
  });

  const pollOneM = useMutation({
    mutationFn: (domain: string) => pollDomainQueue(domain),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["topology-queues"] }); qc.invalidateQueries({ queryKey: ["topology-health"] }); },
  });

  const pollAllM = useMutation({
    mutationFn: pollAllQueues,
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["topology-queues"] }); qc.invalidateQueries({ queryKey: ["topology-health"] }); },
  });

  const seedM = useMutation({
    mutationFn: () => seedTopology(),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["topology-queues"] }); qc.invalidateQueries({ queryKey: ["topology-health"] }); },
  });

  const resolveM = useMutation({
    mutationFn: ({ id, resolution }: { id: number; resolution: string }) =>
      resolveDlqEntry(id, { resolution }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["dlq-entries"] }); qc.invalidateQueries({ queryKey: ["dlq-summary"] }); },
  });

  const health = healthQ.data;
  const dlqSum = dlqSumQ.data;

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">SQS Event Topology</h1>
        <div className="flex gap-2">
          <button onClick={() => seedM.mutate()} className="rounded bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700" disabled={seedM.isPending}>
            {seedM.isPending ? "Seeding…" : "Seed Defaults"}
          </button>
          <button onClick={() => pollAllM.mutate()} className="rounded bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700" disabled={pollAllM.isPending}>
            {pollAllM.isPending ? "Polling…" : "Poll All Queues"}
          </button>
        </div>
      </div>

      {/* ── Health cards ──────────────────────────────────────────── */}
      {health && (
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-4 lg:grid-cols-5">
          <Card label="Total Queues" value={health.total_queues} />
          <Card label="Active" value={health.active_queues} color="text-green-600" />
          <Card label="Errors" value={health.error_queues} color="text-red-600" />
          <Card label="Messages Received" value={health.total_received} />
          <Card label="Unresolved DLQ" value={health.unresolved_dlq} color={health.unresolved_dlq > 0 ? "text-red-600" : undefined} />
        </div>
      )}

      {/* ── Tabs ──────────────────────────────────────────────────── */}
      <div className="flex gap-1 border-b">
        {(["queues", "dlq", "routing"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm font-medium capitalize ${tab === t ? "border-b-2 border-blue-600 text-blue-600" : "text-gray-500 hover:text-gray-700"}`}
          >
            {t === "dlq" ? `DLQ${dlqSum ? ` (${dlqSum.unresolved})` : ""}` : t}
          </button>
        ))}
      </div>

      {/* ── Queues tab ────────────────────────────────────────────── */}
      {tab === "queues" && (
        <div className="overflow-x-auto rounded-lg border">
          <table className="min-w-full divide-y text-sm">
            <thead className="bg-gray-50">
              <tr>
                {["Domain", "Status", "Enabled", "Received", "Processed", "Failed", "DLQ", "Last Poll", "Actions"].map((h) => (
                  <th key={h} className="px-4 py-2 text-left font-medium text-gray-600">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y">
              {queuesQ.data?.map((q: SqsQueueTopology) => (
                <tr key={q.id} className="hover:bg-gray-50">
                  <td className="px-4 py-2 font-medium">{q.domain}</td>
                  <td className="px-4 py-2"><Badge label={q.status} className={statusColors[q.status]} /></td>
                  <td className="px-4 py-2">{q.enabled ? "✓" : "✕"}</td>
                  <td className="px-4 py-2 text-right">{q.messages_received.toLocaleString()}</td>
                  <td className="px-4 py-2 text-right">{q.messages_processed.toLocaleString()}</td>
                  <td className="px-4 py-2 text-right">{q.messages_failed.toLocaleString()}</td>
                  <td className="px-4 py-2 text-right">{q.messages_dlq.toLocaleString()}</td>
                  <td className="px-4 py-2 text-xs text-gray-500">{q.last_poll_at ?? "—"}</td>
                  <td className="flex gap-1 px-4 py-2">
                    <button
                      onClick={() => toggleM.mutate({ domain: q.domain, enabled: !q.enabled })}
                      className="rounded bg-gray-200 px-2 py-1 text-xs hover:bg-gray-300"
                    >
                      {q.enabled ? "Disable" : "Enable"}
                    </button>
                    <button
                      onClick={() => pollOneM.mutate(q.domain)}
                      className="rounded bg-blue-100 px-2 py-1 text-xs text-blue-700 hover:bg-blue-200"
                    >
                      Poll
                    </button>
                  </td>
                </tr>
              ))}
              {!queuesQ.data?.length && (
                <tr><td colSpan={9} className="px-4 py-8 text-center text-gray-400">No queues registered. Click "Seed Defaults" to create the standard topology.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* ── DLQ tab ───────────────────────────────────────────────── */}
      {tab === "dlq" && (
        <div className="space-y-4">
          {/* DLQ summary */}
          {dlqSum && (
            <div className="flex gap-4">
              <Badge label={`Total: ${dlqSum.total}`} className="bg-gray-100 text-gray-700" />
              <Badge label={`Unresolved: ${dlqSum.unresolved}`} className={dlqSum.unresolved > 0 ? "bg-red-100 text-red-700" : "bg-gray-100 text-gray-600"} />
              <Badge label={`Replayed: ${dlqSum.replayed}`} className="bg-green-100 text-green-700" />
              <Badge label={`Discarded: ${dlqSum.discarded}`} className="bg-yellow-100 text-yellow-700" />
            </div>
          )}

          {/* Filters */}
          <div className="flex gap-3">
            <select className="rounded border px-2 py-1 text-sm" value={dlqDomain} onChange={(e) => setDlqDomain(e.target.value)}>
              <option value="">All domains</option>
              {["pricing", "listing", "order", "inventory", "report", "feed"].map((d) => (
                <option key={d} value={d}>{d}</option>
              ))}
            </select>
            <select className="rounded border px-2 py-1 text-sm" value={dlqStatus} onChange={(e) => setDlqStatus(e.target.value)}>
              <option value="">All statuses</option>
              {["unresolved", "replayed", "discarded", "investigating"].map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
          </div>

          {/* DLQ entries table */}
          <div className="overflow-x-auto rounded-lg border">
            <table className="min-w-full divide-y text-sm">
              <thead className="bg-gray-50">
                <tr>
                  {["ID", "Domain", "Message ID", "Error", "Status", "Receives", "Created", "Actions"].map((h) => (
                    <th key={h} className="px-4 py-2 text-left font-medium text-gray-600">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y">
                {dlqQ.data?.items?.map((e: DlqEntry) => (
                  <tr key={e.id} className="hover:bg-gray-50">
                    <td className="px-4 py-2">{e.id}</td>
                    <td className="px-4 py-2">{e.domain}</td>
                    <td className="px-4 py-2 font-mono text-xs">{e.message_id.slice(0, 16)}…</td>
                    <td className="max-w-xs truncate px-4 py-2 text-xs text-gray-500">{e.error_message ?? "—"}</td>
                    <td className="px-4 py-2"><Badge label={e.status} className={e.status === "unresolved" ? "bg-red-100 text-red-700" : "bg-gray-100 text-gray-600"} /></td>
                    <td className="px-4 py-2 text-right">{e.approximate_receive_count}</td>
                    <td className="px-4 py-2 text-xs text-gray-500">{e.created_at}</td>
                    <td className="flex gap-1 px-4 py-2">
                      {e.status === "unresolved" && (
                        <>
                          <button
                            onClick={() => resolveM.mutate({ id: e.id, resolution: "replayed" })}
                            className="rounded bg-green-100 px-2 py-1 text-xs text-green-700 hover:bg-green-200"
                          >
                            Replay
                          </button>
                          <button
                            onClick={() => resolveM.mutate({ id: e.id, resolution: "discarded" })}
                            className="rounded bg-yellow-100 px-2 py-1 text-xs text-yellow-700 hover:bg-yellow-200"
                          >
                            Discard
                          </button>
                        </>
                      )}
                    </td>
                  </tr>
                ))}
                {!dlqQ.data?.items?.length && (
                  <tr><td colSpan={8} className="px-4 py-8 text-center text-gray-400">No DLQ entries.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Routing tab ───────────────────────────────────────────── */}
      {tab === "routing" && routingQ.data && (
        <div className="space-y-4">
          <p className="text-sm text-gray-500">
            {routingQ.data.total_types} notification types routed to {routingQ.data.total_domains} domain queues.
          </p>
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {Object.entries(routingQ.data.domains).map(([domain, types]) => (
              <div key={domain} className="rounded-lg border p-4">
                <h3 className="mb-2 font-semibold capitalize">{domain}</h3>
                <ul className="space-y-1">
                  {types.map((t) => (
                    <li key={t} className="text-xs text-gray-600 font-mono">{t}</li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Small presentational helpers                                       */
/* ------------------------------------------------------------------ */

function Card({ label, value, color }: { label: string; value: number; color?: string }) {
  return (
    <div className="rounded-lg border bg-white p-4 shadow-sm">
      <p className="text-xs font-medium text-gray-500">{label}</p>
      <p className={`mt-1 text-2xl font-bold ${color ?? "text-gray-900"}`}>{value.toLocaleString()}</p>
    </div>
  );
}
