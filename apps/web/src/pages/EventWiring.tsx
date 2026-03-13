import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getEventWires,
  getWiringHealth,
  getReplayJobs,
  getReplaySummary,
  toggleEventWire,
  seedEventWiring,
  registerDomainHandlers,
  replayEvents,
  replayDlqEntries,
  pollTopologyQueues,
  type EventWireConfig,
  type ReplayJob,
} from "@/lib/api";

/* ------------------------------------------------------------------ */

function Badge({ label, className }: { label: string; className?: string }) {
  return (
    <span className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${className ?? "bg-gray-100 text-gray-600"}`}>
      {label}
    </span>
  );
}

/* ------------------------------------------------------------------ */

export default function EventWiringPage() {
  const qc = useQueryClient();
  const [tab, setTab] = useState<"wiring" | "replay" | "health">("wiring");
  const [replayDomain, setReplayDomain] = useState("");
  const [replayLimit, setReplayLimit] = useState(100);

  // ── Queries ──────────────────────────────────────────────────────
  const wiresQ = useQuery({ queryKey: ["event-wires"], queryFn: () => getEventWires() });
  const healthQ = useQuery({ queryKey: ["wiring-health"], queryFn: getWiringHealth });
  const replayJobsQ = useQuery({ queryKey: ["replay-jobs"], queryFn: () => getReplayJobs({ limit: 50 }), enabled: tab === "replay" });
  const replaySumQ = useQuery({ queryKey: ["replay-summary"], queryFn: getReplaySummary });

  // ── Mutations ────────────────────────────────────────────────────
  const toggleM = useMutation({
    mutationFn: ({ handler, enabled }: { handler: string; enabled: boolean }) => toggleEventWire(handler, enabled),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["event-wires"] }); qc.invalidateQueries({ queryKey: ["wiring-health"] }); },
  });

  const seedM = useMutation({
    mutationFn: seedEventWiring,
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["event-wires"] }); qc.invalidateQueries({ queryKey: ["wiring-health"] }); },
  });

  const registerM = useMutation({
    mutationFn: registerDomainHandlers,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["wiring-health"] }),
  });

  const replayM = useMutation({
    mutationFn: () => replayEvents({ event_domain: replayDomain || undefined, limit: replayLimit }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["replay-jobs"] }); qc.invalidateQueries({ queryKey: ["replay-summary"] }); },
  });

  const dlqReplayM = useMutation({
    mutationFn: () => replayDlqEntries({ domain: replayDomain || undefined }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["replay-jobs"] }); qc.invalidateQueries({ queryKey: ["replay-summary"] }); },
  });

  const pollM = useMutation({
    mutationFn: pollTopologyQueues,
  });

  const health = healthQ.data;
  const replaySum = replaySumQ.data;

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Event Wiring & Replay</h1>
        <div className="flex gap-2">
          <button onClick={() => seedM.mutate()} className="rounded bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700" disabled={seedM.isPending}>
            {seedM.isPending ? "Seeding…" : "Seed Wiring"}
          </button>
          <button onClick={() => registerM.mutate()} className="rounded bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700" disabled={registerM.isPending}>
            {registerM.isPending ? "Registering…" : "Register Handlers"}
          </button>
          <button onClick={() => pollM.mutate()} className="rounded bg-green-600 px-3 py-1.5 text-sm text-white hover:bg-green-700" disabled={pollM.isPending}>
            {pollM.isPending ? "Polling…" : "Poll Topology"}
          </button>
        </div>
      </div>

      {/* ── Health cards ──────────────────────────────────────────── */}
      {health && (
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-5">
          <Card label="Total Wires" value={health.total_wires} />
          <Card label="Enabled" value={health.enabled_wires} color="text-green-600" />
          <Card label="Disabled" value={health.disabled_wires} color={health.disabled_wires > 0 ? "text-yellow-600" : undefined} />
          <Card label="Domains Covered" value={health.domains_covered} />
          <Card label="Modules Wired" value={health.modules_wired} />
        </div>
      )}

      {health && health.unwired_domains.length > 0 && (
        <div className="rounded-lg border border-yellow-200 bg-yellow-50 p-3 text-sm text-yellow-800">
          Unwired domains: {health.unwired_domains.join(", ")}
        </div>
      )}

      {/* ── Tabs ──────────────────────────────────────────────────── */}
      <div className="flex gap-1 border-b">
        {(["wiring", "replay", "health"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm font-medium capitalize ${tab === t ? "border-b-2 border-blue-600 text-blue-600" : "text-gray-500 hover:text-gray-700"}`}
          >
            {t}
          </button>
        ))}
      </div>

      {/* ── Wiring tab ────────────────────────────────────────────── */}
      {tab === "wiring" && (
        <div className="overflow-x-auto rounded-lg border">
          <table className="min-w-full divide-y text-sm">
            <thead className="bg-gray-50">
              <tr>
                {["Module", "Domain", "Action", "Handler", "Enabled", "Priority", "Timeout", "Actions"].map((h) => (
                  <th key={h} className="px-4 py-2 text-left font-medium text-gray-600">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y">
              {wiresQ.data?.map((w: EventWireConfig) => (
                <tr key={w.id} className="hover:bg-gray-50">
                  <td className="px-4 py-2 font-medium">{w.module_name}</td>
                  <td className="px-4 py-2">{w.event_domain}</td>
                  <td className="px-4 py-2 font-mono text-xs">{w.event_action}</td>
                  <td className="px-4 py-2 font-mono text-xs">{w.handler_name}</td>
                  <td className="px-4 py-2">{w.enabled ? <Badge label="ON" className="bg-green-100 text-green-700" /> : <Badge label="OFF" className="bg-red-100 text-red-700" />}</td>
                  <td className="px-4 py-2 text-right">{w.priority}</td>
                  <td className="px-4 py-2 text-right">{w.timeout_seconds}s</td>
                  <td className="px-4 py-2">
                    <button
                      onClick={() => toggleM.mutate({ handler: w.handler_name, enabled: !w.enabled })}
                      className="rounded bg-gray-200 px-2 py-1 text-xs hover:bg-gray-300"
                    >
                      {w.enabled ? "Disable" : "Enable"}
                    </button>
                  </td>
                </tr>
              ))}
              {!wiresQ.data?.length && (
                <tr><td colSpan={8} className="px-4 py-8 text-center text-gray-400">No wires configured. Click "Seed Wiring" to set up defaults.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* ── Replay tab ────────────────────────────────────────────── */}
      {tab === "replay" && (
        <div className="space-y-4">
          {/* Replay summary */}
          {replaySum && (
            <div className="flex gap-4">
              <Badge label={`Total: ${replaySum.total_jobs}`} className="bg-gray-100 text-gray-700" />
              <Badge label={`Completed: ${replaySum.completed}`} className="bg-green-100 text-green-700" />
              <Badge label={`Failed: ${replaySum.failed}`} className={replaySum.failed > 0 ? "bg-red-100 text-red-700" : "bg-gray-100 text-gray-600"} />
              <Badge label={`Events Replayed: ${replaySum.total_events_replayed}`} className="bg-blue-100 text-blue-700" />
            </div>
          )}

          {/* Replay controls */}
          <div className="flex items-end gap-3 rounded-lg border bg-gray-50 p-4">
            <div>
              <label className="mb-1 block text-xs font-medium text-gray-600">Domain</label>
              <select className="rounded border px-2 py-1 text-sm" value={replayDomain} onChange={(e) => setReplayDomain(e.target.value)}>
                <option value="">All domains</option>
                {["pricing", "listing", "order", "inventory", "report", "feed"].map((d) => (
                  <option key={d} value={d}>{d}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-gray-600">Limit</label>
              <input type="number" min={1} max={500} value={replayLimit} onChange={(e) => setReplayLimit(+e.target.value)}
                className="w-20 rounded border px-2 py-1 text-sm" />
            </div>
            <button
              onClick={() => replayM.mutate()}
              className="rounded bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700"
              disabled={replayM.isPending}
            >
              {replayM.isPending ? "Replaying…" : "Replay Events"}
            </button>
            <button
              onClick={() => dlqReplayM.mutate()}
              className="rounded bg-amber-600 px-3 py-1.5 text-sm text-white hover:bg-amber-700"
              disabled={dlqReplayM.isPending}
            >
              {dlqReplayM.isPending ? "Re-ingesting…" : "Replay DLQ"}
            </button>
          </div>

          {/* Replay jobs table */}
          <div className="overflow-x-auto rounded-lg border">
            <table className="min-w-full divide-y text-sm">
              <thead className="bg-gray-50">
                <tr>
                  {["ID", "Type", "Domain", "Status", "Matched", "Replayed", "Processed", "Failed", "Started"].map((h) => (
                    <th key={h} className="px-4 py-2 text-left font-medium text-gray-600">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y">
                {replayJobsQ.data?.items?.map((j: ReplayJob) => (
                  <tr key={j.id} className="hover:bg-gray-50">
                    <td className="px-4 py-2">{j.id}</td>
                    <td className="px-4 py-2">{j.replay_type}</td>
                    <td className="px-4 py-2">{j.scope_domain ?? "all"}</td>
                    <td className="px-4 py-2">
                      <Badge
                        label={j.status}
                        className={j.status === "completed" ? "bg-green-100 text-green-700" :
                          j.status === "failed" ? "bg-red-100 text-red-700" :
                          j.status === "running" ? "bg-blue-100 text-blue-700" : "bg-gray-100 text-gray-600"}
                      />
                    </td>
                    <td className="px-4 py-2 text-right">{j.events_matched}</td>
                    <td className="px-4 py-2 text-right">{j.events_replayed}</td>
                    <td className="px-4 py-2 text-right">{j.events_processed}</td>
                    <td className="px-4 py-2 text-right">{j.events_failed}</td>
                    <td className="px-4 py-2 text-xs text-gray-500">{j.started_at}</td>
                  </tr>
                ))}
                {!replayJobsQ.data?.items?.length && (
                  <tr><td colSpan={9} className="px-4 py-8 text-center text-gray-400">No replay jobs yet.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Health tab ────────────────────────────────────────────── */}
      {tab === "health" && health && (
        <div className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {health.domain_coverage.map((dc) => (
              <div key={dc.domain} className="rounded-lg border p-4">
                <h3 className="mb-2 font-semibold capitalize">{dc.domain}</h3>
                <div className="flex gap-3 text-sm">
                  <span className="text-gray-600">Wires: <strong>{dc.wire_count}</strong></span>
                  <span className="text-green-600">Enabled: <strong>{dc.enabled_count}</strong></span>
                </div>
              </div>
            ))}
          </div>

          {health.unwired_domains.length > 0 && (
            <div className="rounded-lg border border-red-200 bg-red-50 p-4">
              <h3 className="mb-2 font-semibold text-red-800">Unwired Domains</h3>
              <p className="text-sm text-red-700">
                These domains have no event handlers: {health.unwired_domains.join(", ")}.
                Click "Register Handlers" to wire them.
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function Card({ label, value, color }: { label: string; value: number; color?: string }) {
  return (
    <div className="rounded-lg border bg-white p-4 shadow-sm">
      <p className="text-xs font-medium text-gray-500">{label}</p>
      <p className={`mt-1 text-2xl font-bold ${color ?? "text-gray-900"}`}>{value.toLocaleString()}</p>
    </div>
  );
}
