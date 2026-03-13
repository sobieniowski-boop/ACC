import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { FlaskConical, Plus, X } from "lucide-react";
import { getStrategyExperiments, createStrategyExperiment } from "@/lib/api";
import type { StrategyExperiment } from "@/lib/api";
import { cn } from "@/lib/utils";

const STATUS_COLORS: Record<string, string> = {
  planned: "bg-blue-500/15 text-blue-400",
  running: "bg-green-500/15 text-green-400",
  completed: "bg-emerald-500/15 text-emerald-400",
  cancelled: "bg-zinc-500/15 text-zinc-400",
};

const STATUS_TABS = [
  { id: "", label: "All" },
  { id: "planned", label: "Planned" },
  { id: "running", label: "Running" },
  { id: "completed", label: "Completed" },
  { id: "cancelled", label: "Cancelled" },
];

export default function StrategyExperimentsPage() {
  const qc = useQueryClient();
  const [statusFilter, setStatusFilter] = useState("");
  const [showForm, setShowForm] = useState(false);

  const { data, isLoading } = useQuery({
    queryKey: ["strategy-experiments", statusFilter],
    queryFn: () => getStrategyExperiments(statusFilter || undefined),
    staleTime: 30_000,
  });

  const items: StrategyExperiment[] = data?.items ?? [];

  return (
    <div className="space-y-5 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Experiments</h1>
          <p className="text-sm text-muted-foreground mt-0.5">Testuj hipotezy przed pełnym wdrożeniem</p>
        </div>
        <button onClick={() => setShowForm(true)}
          className="flex items-center gap-1.5 rounded-lg bg-amazon px-4 py-2 text-sm font-medium text-black hover:bg-amazon/80">
          <Plus className="h-4 w-4" /> New Experiment
        </button>
      </div>

      {/* Status tabs */}
      <div className="flex gap-2">
        {STATUS_TABS.map((t) => (
          <button key={t.id} onClick={() => setStatusFilter(t.id)}
            className={cn("rounded-full border px-3 py-1 text-xs font-medium transition",
              statusFilter === t.id ? "border-amazon bg-amazon/15 text-amazon" : "border-border hover:border-amazon/50")}>
            {t.label}
          </button>
        ))}
      </div>

      {/* Table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              <th className="px-3 py-2 text-left">ID</th>
              <th className="px-3 py-2 text-left">Type</th>
              <th className="px-3 py-2 text-left">MKT</th>
              <th className="px-3 py-2 text-left">SKU</th>
              <th className="px-3 py-2 text-left max-w-[220px]">Hypothesis</th>
              <th className="px-3 py-2 text-left">Owner</th>
              <th className="px-3 py-2 text-left">Status</th>
              <th className="px-3 py-2 text-left">Start</th>
              <th className="px-3 py-2 text-left">End</th>
              <th className="px-3 py-2 text-left">Metric</th>
              <th className="px-3 py-2 text-right">Baseline</th>
              <th className="px-3 py-2 text-right">Result</th>
              <th className="px-3 py-2 text-left max-w-[180px]">Summary</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <tr key={i} className="border-b border-border/50"><td colSpan={13} className="px-3 py-3"><div className="h-4 bg-muted/30 rounded animate-pulse" /></td></tr>
              ))
            ) : items.length === 0 ? (
              <tr><td colSpan={13} className="px-3 py-8 text-center text-muted-foreground">No experiments yet — create your first one</td></tr>
            ) : items.map((exp) => (
              <tr key={exp.id} className="border-b border-border/50 hover:bg-muted/20">
                <td className="px-3 py-2 text-xs font-mono">#{exp.id}</td>
                <td className="px-3 py-2 text-xs">{exp.experiment_type?.replace(/_/g, " ") || "—"}</td>
                <td className="px-3 py-2 text-xs">{exp.marketplace_id || "—"}</td>
                <td className="px-3 py-2 text-xs font-mono">{exp.sku || "—"}</td>
                <td className="px-3 py-2 text-xs max-w-[220px] truncate" title={exp.hypothesis}>{exp.hypothesis}</td>
                <td className="px-3 py-2 text-xs">{exp.owner || "—"}</td>
                <td className="px-3 py-2">
                  <span className={cn("rounded px-1.5 py-0.5 text-[10px] font-bold uppercase", STATUS_COLORS[exp.status] || "bg-muted")}>
                    {exp.status}
                  </span>
                </td>
                <td className="px-3 py-2 text-xs">{exp.start_date || "—"}</td>
                <td className="px-3 py-2 text-xs">{exp.end_date || "—"}</td>
                <td className="px-3 py-2 text-xs">{exp.success_metric || "—"}</td>
                <td className="px-3 py-2 text-right text-xs tabular-nums">{exp.baseline_value ?? "—"}</td>
                <td className="px-3 py-2 text-right text-xs tabular-nums font-medium">{exp.result_value ?? "—"}</td>
                <td className="px-3 py-2 text-xs max-w-[180px] truncate text-muted-foreground" title={exp.result_summary || ""}>
                  {exp.result_summary || "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {showForm && (
        <CreateExperimentModal onClose={() => setShowForm(false)} onCreated={() => {
          qc.invalidateQueries({ queryKey: ["strategy-experiments"] });
          setShowForm(false);
        }} />
      )}
    </div>
  );
}

/* --- Create Experiment Modal --- */
function CreateExperimentModal({ onClose, onCreated }: { onClose: () => void; onCreated: () => void }) {
  const [form, setForm] = useState({
    opportunity_id: "",
    experiment_type: "price_test",
    marketplace_id: "",
    sku: "",
    asin: "",
    hypothesis: "",
    owner: "",
    success_metric: "",
    baseline_value: "",
    start_date: "",
    end_date: "",
  });

  const create = useMutation({
    mutationFn: () => createStrategyExperiment({
      opportunity_id: form.opportunity_id ? Number(form.opportunity_id) : undefined,
      experiment_type: form.experiment_type,
      marketplace_id: form.marketplace_id || undefined,
      sku: form.sku || undefined,
      asin: form.asin || undefined,
      hypothesis: form.hypothesis,
      owner: form.owner || undefined,
      success_metric: form.success_metric || undefined,
      start_date: form.start_date || undefined,
      end_date: form.end_date || undefined,
    }),
    onSuccess: onCreated,
  });

  const set = (key: string, val: string) => setForm((p) => ({ ...p, [key]: val }));

  return (
    <>
      <div className="fixed inset-0 bg-black/50 z-40" onClick={onClose} />
      <div className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-full max-w-lg bg-card border border-border rounded-2xl shadow-2xl z-50 p-6 space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-base font-semibold flex items-center gap-2"><FlaskConical className="h-4 w-4" /> New Experiment</h3>
          <button onClick={onClose} className="rounded p-1 hover:bg-muted"><X className="h-4 w-4" /></button>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <Field label="Opportunity ID (opt)" value={form.opportunity_id} onChange={(v) => set("opportunity_id", v)} />
          <Field label="Type" value={form.experiment_type} onChange={(v) => set("experiment_type", v)} />
          <Field label="Marketplace ID" value={form.marketplace_id} onChange={(v) => set("marketplace_id", v)} />
          <Field label="SKU" value={form.sku} onChange={(v) => set("sku", v)} />
          <Field label="ASIN" value={form.asin} onChange={(v) => set("asin", v)} />
          <Field label="Owner" value={form.owner} onChange={(v) => set("owner", v)} />
          <Field label="Success Metric" value={form.success_metric} onChange={(v) => set("success_metric", v)} />
          <Field label="Baseline Value" value={form.baseline_value} onChange={(v) => set("baseline_value", v)} />
          <Field label="Start Date" value={form.start_date} onChange={(v) => set("start_date", v)} placeholder="YYYY-MM-DD" />
          <Field label="End Date" value={form.end_date} onChange={(v) => set("end_date", v)} placeholder="YYYY-MM-DD" />
        </div>
        <div className="col-span-2">
          <label className="text-xs text-muted-foreground">Hypothesis *</label>
          <textarea value={form.hypothesis} onChange={(e) => set("hypothesis", e.target.value)}
            className="block w-full rounded-lg border border-border bg-transparent px-3 py-2 text-sm mt-1 resize-none" rows={2} />
        </div>
        <div className="flex justify-end gap-2 pt-2">
          <button onClick={onClose} className="rounded-lg border border-border px-4 py-2 text-sm hover:bg-muted">Cancel</button>
          <button disabled={!form.hypothesis || create.isPending} onClick={() => create.mutate()}
            className="rounded-lg bg-amazon px-4 py-2 text-sm font-medium text-black hover:bg-amazon/80 disabled:opacity-50">
            Create
          </button>
        </div>
        {create.isError && <p className="text-xs text-red-400">{String((create.error as Error)?.message ?? "Error")}</p>}
      </div>
    </>
  );
}

function Field({ label, value, onChange, placeholder }: { label: string; value: string; onChange: (v: string) => void; placeholder?: string }) {
  return (
    <label className="space-y-1">
      <span className="text-xs text-muted-foreground">{label}</span>
      <input type="text" value={value} onChange={(e) => onChange(e.target.value)} placeholder={placeholder}
        className="block w-full rounded-lg border border-border bg-transparent px-3 py-1.5 text-sm" />
    </label>
  );
}
