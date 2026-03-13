import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import {
  approveContentVersion,
  checkContentPolicy,
  createContentVersion,
  getContentDiff,
  getContentVersions,
  submitContentVersionReview,
  syncContent,
  updateContentVersion,
  type ContentFieldsPayload,
} from "@/lib/api";

export default function ContentEditorPage() {
  const qc = useQueryClient();
  const [searchParams] = useSearchParams();
  const [sku, setSku] = useState("");
  const [marketplaceId, setMarketplaceId] = useState("DE");
  const [selectedVersionId, setSelectedVersionId] = useState<string | null>(null);
  const [diffMain, setDiffMain] = useState("DE");
  const [diffTarget, setDiffTarget] = useState("FR");
  const [syncTargets, setSyncTargets] = useState("FR,IT,ES");
  const [syncFields, setSyncFields] = useState<string>("title,bullets,description,keywords");
  const [fields, setFields] = useState<ContentFieldsPayload>({
    title: "",
    bullets: [],
    description: "",
    keywords: "",
    special_features: [],
    attributes_json: {},
    aplus_json: {},
    compliance_notes: "",
  });
  const [policyOutput, setPolicyOutput] = useState<string>("");

  useEffect(() => {
    const qsSku = (searchParams.get("sku") || "").trim();
    const qsMarketplace = (searchParams.get("marketplace") || "").trim();
    if (qsSku) setSku(qsSku);
    if (qsMarketplace) setMarketplaceId(qsMarketplace.toUpperCase());
  }, [searchParams]);

  const versionsQuery = useQuery({
    queryKey: ["content-editor-versions", sku, marketplaceId],
    queryFn: () => getContentVersions(sku.trim(), marketplaceId),
    enabled: !!sku.trim(),
  });

  const diffQuery = useQuery({
    queryKey: ["content-editor-diff", sku, diffMain, diffTarget],
    queryFn: () =>
      getContentDiff(sku.trim(), {
        main: diffMain.trim().toUpperCase(),
        target: diffTarget.trim().toUpperCase(),
      }),
    enabled: !!sku.trim() && !!diffMain.trim() && !!diffTarget.trim() && diffMain !== diffTarget,
  });

  useEffect(() => {
    const first = versionsQuery.data?.items?.[0];
    if (!first) return;
    setSelectedVersionId(first.id);
    setFields(first.fields ?? {});
  }, [versionsQuery.data?.items?.[0]?.id]);

  const createDraftMutation = useMutation({
    mutationFn: () =>
      createContentVersion(sku.trim(), marketplaceId, {
        fields,
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["content-editor-versions", sku, marketplaceId] }),
  });

  const saveMutation = useMutation({
    mutationFn: () => {
      if (!selectedVersionId) throw new Error("No version selected");
      return updateContentVersion(selectedVersionId, { fields });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["content-editor-versions", sku, marketplaceId] }),
  });

  const submitMutation = useMutation({
    mutationFn: () => {
      if (!selectedVersionId) throw new Error("No version selected");
      return submitContentVersionReview(selectedVersionId);
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["content-editor-versions", sku, marketplaceId] }),
  });

  const approveMutation = useMutation({
    mutationFn: () => {
      if (!selectedVersionId) throw new Error("No version selected");
      return approveContentVersion(selectedVersionId);
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["content-editor-versions", sku, marketplaceId] }),
  });

  const checkMutation = useMutation({
    mutationFn: () => {
      if (!selectedVersionId) throw new Error("No version selected");
      return checkContentPolicy(selectedVersionId);
    },
    onSuccess: (resp) => {
      setPolicyOutput(
        `passed=${resp.passed} critical=${resp.critical_count} major=${resp.major_count} minor=${resp.minor_count}`
      );
    },
  });
  const syncMutation = useMutation({
    mutationFn: () =>
      syncContent(sku.trim(), {
        fields: syncFields
          .split(",")
          .map((x) => x.trim())
          .filter(Boolean),
        from_market: diffMain.trim().toUpperCase(),
        to_markets: syncTargets
          .split(",")
          .map((x) => x.trim().toUpperCase())
          .filter(Boolean),
        overwrite_mode: "missing_only",
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["content-editor-versions"] });
      qc.invalidateQueries({ queryKey: ["content-editor-diff"] });
    },
  });

  const bulletsText = (fields.bullets ?? []).join("\n");

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold">Content Editor</h1>
        <p className="text-sm text-muted-foreground">Versioned editing with review/approve workflow</p>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <div className="grid gap-2 md:grid-cols-3">
          <input value={sku} onChange={(e) => setSku(e.target.value)} placeholder="SKU" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={marketplaceId} onChange={(e) => setMarketplaceId(e.target.value.toUpperCase())} placeholder="Marketplace (DE/FR...)" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <button
            onClick={() => versionsQuery.refetch()}
            disabled={!sku.trim()}
            className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40"
          >
            Load versions
          </button>
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-[1fr_2fr]">
        <div className="rounded-xl border border-border bg-card p-4 space-y-2">
          <div className="text-sm font-semibold">Versions</div>
          <button
            onClick={() => createDraftMutation.mutate()}
            disabled={!sku.trim() || createDraftMutation.isPending}
            className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40"
          >
            Create draft
          </button>
          <div className="max-h-80 overflow-auto space-y-1">
            {(versionsQuery.data?.items ?? []).map((v) => (
              <button
                key={v.id}
                onClick={() => {
                  setSelectedVersionId(v.id);
                  setFields(v.fields ?? {});
                }}
                className={`w-full rounded border px-2 py-1 text-left text-xs ${selectedVersionId === v.id ? "border-blue-500" : "border-border"}`}
              >
                v{v.version_no} | {v.status}
              </button>
            ))}
          </div>
        </div>

        <div className="rounded-xl border border-border bg-card p-4 space-y-2">
          <input
            value={fields.title ?? ""}
            onChange={(e) => setFields((f) => ({ ...f, title: e.target.value }))}
            placeholder="Title"
            className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
          />
          <textarea
            rows={5}
            value={bulletsText}
            onChange={(e) =>
              setFields((f) => ({
                ...f,
                bullets: e.target.value
                  .split(/\r?\n/)
                  .map((x) => x.trim())
                  .filter(Boolean),
              }))
            }
            placeholder="Bullets, one per line"
            className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
          />
          <textarea
            rows={4}
            value={fields.description ?? ""}
            onChange={(e) => setFields((f) => ({ ...f, description: e.target.value }))}
            placeholder="Description"
            className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
          />
          <input
            value={fields.keywords ?? ""}
            onChange={(e) => setFields((f) => ({ ...f, keywords: e.target.value }))}
            placeholder="Keywords"
            className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
          />
          <div className="flex flex-wrap gap-2">
            <button onClick={() => saveMutation.mutate()} disabled={!selectedVersionId} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Save</button>
            <button onClick={() => checkMutation.mutate()} disabled={!selectedVersionId} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Policy check</button>
            <button onClick={() => submitMutation.mutate()} disabled={!selectedVersionId} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Submit review</button>
            <button onClick={() => approveMutation.mutate()} disabled={!selectedVersionId} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Approve</button>
          </div>
          {policyOutput && <div className="rounded border border-border p-2 text-xs text-muted-foreground">{policyOutput}</div>}
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-3">
        <h2 className="text-sm font-semibold">MAIN↔TARGET Diff + Sync</h2>
        <div className="grid gap-2 md:grid-cols-5">
          <input value={diffMain} onChange={(e) => setDiffMain(e.target.value.toUpperCase())} placeholder="MAIN (DE)" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={diffTarget} onChange={(e) => setDiffTarget(e.target.value.toUpperCase())} placeholder="TARGET (FR)" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={syncTargets} onChange={(e) => setSyncTargets(e.target.value)} placeholder="sync targets FR,IT,ES" className="rounded border border-input bg-background px-2 py-1 text-xs md:col-span-2" />
          <button onClick={() => diffQuery.refetch()} disabled={!sku.trim()} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Refresh diff</button>
        </div>
        <div className="grid gap-2 md:grid-cols-4">
          <input value={syncFields} onChange={(e) => setSyncFields(e.target.value)} placeholder="fields title,bullets,description,keywords" className="rounded border border-input bg-background px-2 py-1 text-xs md:col-span-3" />
          <button onClick={() => syncMutation.mutate()} disabled={!sku.trim() || syncMutation.isPending} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Sync MAIN→TARGET</button>
        </div>
        {syncMutation.data && (
          <div className="rounded border border-border p-2 text-xs text-muted-foreground">
            drafts_created={syncMutation.data.drafts_created} skipped={syncMutation.data.skipped}
          </div>
        )}
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="border-b border-border text-left text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Field</th>
                <th className="px-2 py-2">Change</th>
                <th className="px-2 py-2">MAIN</th>
                <th className="px-2 py-2">TARGET</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(diffQuery.data?.fields ?? []).map((row) => (
                <tr key={row.field}>
                  <td className="px-2 py-2">{row.field}</td>
                  <td className="px-2 py-2">{row.change_type}</td>
                  <td className="px-2 py-2 max-w-[320px] truncate">{JSON.stringify(row.main_value)}</td>
                  <td className="px-2 py-2 max-w-[320px] truncate">{JSON.stringify(row.target_value)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
