import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { checkContentPolicy, createContentTask, getContentComplianceQueue, getContentPolicyRules, upsertContentPolicyRules } from "@/lib/api";
import { Link } from "react-router-dom";
import { ClientExportButton } from "@/components/shared";

export default function ContentCompliancePage() {
  const qc = useQueryClient();
  const [versionId, setVersionId] = useState("");
  const [checkOutput, setCheckOutput] = useState("");
  const [newRuleName, setNewRuleName] = useState("");
  const [newRulePattern, setNewRulePattern] = useState("");
  const [newRuleSeverity, setNewRuleSeverity] = useState<"critical" | "major" | "minor">("major");
  const [queueSeverity, setQueueSeverity] = useState<"critical" | "major" | "minor">("critical");
  const [selectedVersionIds, setSelectedVersionIds] = useState<string[]>([]);

  const rulesQuery = useQuery({
    queryKey: ["content-policy-rules"],
    queryFn: getContentPolicyRules,
  });
  const queueQuery = useQuery({
    queryKey: ["content-compliance-queue", queueSeverity],
    queryFn: () => getContentComplianceQueue({ severity: queueSeverity, page: 1, page_size: 50 }),
  });

  const checkMutation = useMutation({
    mutationFn: () => checkContentPolicy(versionId.trim()),
    onSuccess: (resp) => {
      setCheckOutput(
        `passed=${resp.passed} | critical=${resp.critical_count} major=${resp.major_count} minor=${resp.minor_count}`
      );
    },
  });

  const addRuleMutation = useMutation({
    mutationFn: () =>
      upsertContentPolicyRules([
        ...(rulesQuery.data ?? []),
        {
          name: newRuleName.trim(),
          pattern: newRulePattern.trim(),
          severity: newRuleSeverity,
          applies_to_json: { fields: ["title", "bullets", "description", "keywords"] },
          is_active: true,
        },
      ]),
    onSuccess: () => {
      setNewRuleName("");
      setNewRulePattern("");
      qc.invalidateQueries({ queryKey: ["content-policy-rules"] });
    },
  });
  const createTasksMutation = useMutation({
    mutationFn: async () => {
      const rows = (queueQuery.data?.items ?? []).filter((x) => selectedVersionIds.includes(x.version_id));
      await Promise.all(
        rows.map((row) =>
          createContentTask({
            type: "fix_policy",
            sku: row.sku,
            marketplace_id: row.marketplace_id,
            priority: row.critical_count > 0 ? "p0" : "p1",
            source_page: "compliance_center",
            title: `Fix policy flags v${row.version_no}`,
            note: `Findings: critical=${row.critical_count}, major=${row.major_count}, minor=${row.minor_count}`,
          })
        )
      );
    },
    onSuccess: () => {
      setSelectedVersionIds([]);
    },
  });

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold">Compliance Center</h1>
        <p className="text-sm text-muted-foreground">Rules management + on-demand policy checks</p>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Check version</h2>
        <div className="flex gap-2">
          <input value={versionId} onChange={(e) => setVersionId(e.target.value)} placeholder="version_id" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
          <button onClick={() => checkMutation.mutate()} disabled={!versionId.trim()} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Run</button>
        </div>
        {checkOutput && <div className="rounded border border-border p-2 text-xs text-muted-foreground">{checkOutput}</div>}
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Policy Rules</h2>
        <div className="grid gap-2 md:grid-cols-4">
          <input value={newRuleName} onChange={(e) => setNewRuleName(e.target.value)} placeholder="Rule name" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={newRulePattern} onChange={(e) => setNewRulePattern(e.target.value)} placeholder="Regex pattern" className="rounded border border-input bg-background px-2 py-1 text-xs md:col-span-2" />
          <select value={newRuleSeverity} onChange={(e) => setNewRuleSeverity(e.target.value as "critical" | "major" | "minor")} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="critical">critical</option>
            <option value="major">major</option>
            <option value="minor">minor</option>
          </select>
        </div>
        <div className="flex gap-2">
          <button onClick={() => addRuleMutation.mutate()} disabled={!newRuleName.trim() || !newRulePattern.trim()} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Add rule</button>
          <ClientExportButton data={rulesQuery.data ?? []} filename="policy_rules" />
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Name</th>
                <th className="px-2 py-2">Pattern</th>
                <th className="px-2 py-2">Severity</th>
                <th className="px-2 py-2">Active</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(rulesQuery.data ?? []).map((r) => (
                <tr key={r.id ?? `${r.name}-${r.pattern}`} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5">{r.name}</td>
                  <td className="px-2 py-1.5 font-mono text-[11px]">{r.pattern}</td>
                  <td className="px-2 py-1.5">{r.severity}</td>
                  <td className="px-2 py-1.5">{String(r.is_active)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <div className="flex items-center gap-2">
          <h2 className="text-sm font-semibold">Failures Queue</h2>
          <select value={queueSeverity} onChange={(e) => setQueueSeverity(e.target.value as "critical" | "major" | "minor")} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="critical">critical</option>
            <option value="major">major+</option>
            <option value="minor">minor+</option>
          </select>
          <button
            onClick={() => createTasksMutation.mutate()}
            disabled={selectedVersionIds.length === 0 || createTasksMutation.isPending}
            className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40"
          >
            Create Fix Tasks ({selectedVersionIds.length})
          </button>
          <ClientExportButton data={queueQuery.data?.items ?? []} filename="compliance_queue" />
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Sel</th>
                <th className="px-2 py-2">Version</th>
                <th className="px-2 py-2">SKU</th>
                <th className="px-2 py-2">MP</th>
                <th className="px-2 py-2">Severity</th>
                <th className="px-2 py-2">Checked</th>
                <th className="px-2 py-2">Flow</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(queueQuery.data?.items ?? []).map((q) => (
                <tr key={q.version_id} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5">
                    <input
                      type="checkbox"
                      checked={selectedVersionIds.includes(q.version_id)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedVersionIds((prev) => Array.from(new Set([...prev, q.version_id])));
                        } else {
                          setSelectedVersionIds((prev) => prev.filter((id) => id !== q.version_id));
                        }
                      }}
                    />
                  </td>
                  <td className="px-2 py-1.5 font-mono">v{q.version_no}</td>
                  <td className="px-2 py-1.5">{q.sku}</td>
                  <td className="px-2 py-1.5">{q.marketplace_id}</td>
                  <td className="px-2 py-1.5">
                    c:{q.critical_count} m:{q.major_count} n:{q.minor_count}
                  </td>
                  <td className="px-2 py-1.5 text-muted-foreground">{q.checked_at.slice(0, 19).replace("T", " ")}</td>
                  <td className="px-2 py-1.5">
                    <div className="flex gap-1">
                      <Link
                        to={`/content/editor?sku=${encodeURIComponent(q.sku)}&marketplace=${encodeURIComponent(q.marketplace_id)}`}
                        className="rounded border border-border px-2 py-1 text-[10px]"
                      >
                        Editor
                      </Link>
                      <Link
                        to={`/content/publish?sku_filter=${encodeURIComponent(q.sku)}`}
                        className="rounded border border-border px-2 py-1 text-[10px]"
                      >
                        Publish
                      </Link>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
