import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  checkContentRestrictions,
  getContentPublishJobs,
  pushContentPublish,
  runContentOnboardPreflight,
  searchContentCatalogByEan,
  type ContentOnboardPreflightItem,
} from "@/lib/api";
import { ClientExportButton } from "@/components/shared";

const MARKET_OPTIONS = ["DE", "FR", "IT", "ES", "NL", "PL", "SE", "BE"] as const;

function badgeClass(value: string): string {
  if (value === "completed" || value === "submitted" || value === "preview_ready") return "bg-green-500/10 text-green-400";
  if (value === "partial" || value === "investigating") return "bg-yellow-500/10 text-yellow-400";
  if (value === "failed") return "bg-red-500/10 text-red-400";
  return "bg-blue-500/10 text-blue-400";
}

export default function ContentOpsPage() {
  const qc = useQueryClient();

  const [skuInput, setSkuInput] = useState("");
  const [mainMarket, setMainMarket] = useState("DE");
  const [targetMarketsInput, setTargetMarketsInput] = useState("FR,IT,ES,NL,PL,SE,BE");
  const [autoCreateTasks, setAutoCreateTasks] = useState(false);
  const [preflightRows, setPreflightRows] = useState<ContentOnboardPreflightItem[]>([]);

  const [catalogSku, setCatalogSku] = useState("");
  const [catalogEan, setCatalogEan] = useState("");
  const [catalogMarket, setCatalogMarket] = useState("DE");
  const [restrictionAsin, setRestrictionAsin] = useState("");
  const [restrictionMarket, setRestrictionMarket] = useState("DE");
  const [restrictionResult, setRestrictionResult] = useState<{ can_list: boolean; requires_approval: boolean; reasons: string[] } | null>(null);

  const [pushSelection, setPushSelection] = useState<"approved" | "draft">("approved");
  const [pushMode, setPushMode] = useState<"preview" | "confirm">("preview");
  const [pushMarketsInput, setPushMarketsInput] = useState("DE,FR,IT");
  const [pushSkuInput, setPushSkuInput] = useState("");

  const jobsQuery = useQuery({
    queryKey: ["content-publish-jobs"],
    queryFn: () => getContentPublishJobs({ page: 1, page_size: 20 }),
  });

  const preflightMutation = useMutation({
    mutationFn: () => {
      const skus = skuInput
        .split(/\r?\n/)
        .map((x) => x.trim())
        .filter(Boolean);
      const target_markets = targetMarketsInput
        .split(",")
        .map((x) => x.trim().toUpperCase())
        .filter(Boolean);
      return runContentOnboardPreflight({
        sku_list: skus,
        main_market: mainMarket,
        target_markets,
        auto_create_tasks: autoCreateTasks,
      });
    },
    onSuccess: (resp) => {
      setPreflightRows(resp.items ?? []);
    },
  });

  const catalogMutation = useMutation({
    mutationFn: () => searchContentCatalogByEan(catalogEan.trim(), catalogMarket),
  });

  const restrictionMutation = useMutation({
    mutationFn: () => checkContentRestrictions(restrictionAsin.trim(), restrictionMarket),
    onSuccess: (resp) => {
      setRestrictionResult({
        can_list: resp.can_list,
        requires_approval: resp.requires_approval,
        reasons: resp.reasons,
      });
    },
  });

  const pushMutation = useMutation({
    mutationFn: () => {
      const marketplaces = pushMarketsInput
        .split(",")
        .map((x) => x.trim().toUpperCase())
        .filter(Boolean);
      const sku_filter = pushSkuInput
        .split(/\r?\n|,/)
        .map((x) => x.trim())
        .filter(Boolean);
      return pushContentPublish({
        marketplaces,
        selection: pushSelection,
        mode: pushMode,
        sku_filter,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["content-publish-jobs"] });
    },
  });

  const pushJobId = (() => {
    const data = pushMutation.data as any;
    if (!data) return "";
    if (typeof data?.id === "string") return data.id;
    if (typeof data?.job?.id === "string") return data.job.id;
    return "";
  })();
  const pushJobStatus = (() => {
    const data = pushMutation.data as any;
    if (!data) return "";
    if (typeof data?.status === "string") return data.status;
    if (typeof data?.job?.status === "string") return data.job.status;
    return "";
  })();

  const preflightSummary = useMemo(() => {
    const total = preflightRows.length;
    const blocked = preflightRows.filter((r) => r.blockers.length > 0).length;
    const ready = total - blocked;
    return { total, blocked, ready };
  }, [preflightRows]);

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold">Content Ops Studio</h1>
        <p className="text-sm text-muted-foreground">Onboard gate (catalog/restrictions) + publish push preview/confirm</p>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-3">
        <h2 className="text-sm font-semibold">Onboard Preflight</h2>
        <div className="grid gap-2 md:grid-cols-4">
          <select value={mainMarket} onChange={(e) => setMainMarket(e.target.value)} className="rounded border border-input bg-background px-2 py-1 text-xs">
            {MARKET_OPTIONS.map((m) => (
              <option key={m} value={m}>{m}</option>
            ))}
          </select>
          <input value={targetMarketsInput} onChange={(e) => setTargetMarketsInput(e.target.value)} placeholder="FR,IT,ES,NL..." className="rounded border border-input bg-background px-2 py-1 text-xs md:col-span-2" />
          <label className="inline-flex items-center gap-2 text-xs text-muted-foreground">
            <input type="checkbox" checked={autoCreateTasks} onChange={(e) => setAutoCreateTasks(e.target.checked)} />
            auto create tasks
          </label>
        </div>
        <textarea
          rows={4}
          value={skuInput}
          onChange={(e) => setSkuInput(e.target.value)}
          placeholder="SKU list (one per line)"
          className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
        />
        <div className="flex gap-2">
          <button
            onClick={() => preflightMutation.mutate()}
            disabled={preflightMutation.isPending || !skuInput.trim()}
            className="rounded border border-border px-3 py-1.5 text-xs disabled:opacity-40"
          >
            {preflightMutation.isPending ? "Running..." : "Run preflight"}
          </button>
          {preflightRows.length > 0 && <ClientExportButton data={preflightRows} filename="ops_preflight" />}
        </div>
        {preflightMutation.isError && <div className="text-xs text-red-400">Preflight failed</div>}
        {preflightRows.length > 0 && (
          <div className="text-xs text-muted-foreground">
            Total: {preflightSummary.total} | Ready: {preflightSummary.ready} | Blocked: {preflightSummary.blocked}
          </div>
        )}
        {preflightRows.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-[11px]">
              <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
                <tr>
                  <th className="px-2 py-2">SKU</th>
                  <th className="px-2 py-2">PIM</th>
                  <th className="px-2 py-2">Family</th>
                  <th className="px-2 py-2">Blockers</th>
                  <th className="px-2 py-2">Warnings</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {preflightRows.map((row) => (
                  <tr key={row.sku} className="hover:bg-muted/20 transition-colors">
                    <td className="px-2 py-1.5 font-mono">{row.sku}</td>
                    <td className="px-2 py-1.5">{row.pim_score}%</td>
                    <td className="px-2 py-1.5">{row.family_coverage_pct}%</td>
                    <td className="px-2 py-1.5">{row.blockers.length ? row.blockers.join(" | ") : "-"}</td>
                    <td className="px-2 py-1.5">{row.warnings.length ? row.warnings.join(" | ") : "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <div className="rounded-xl border border-border bg-card p-4 space-y-2">
          <h2 className="text-sm font-semibold">Catalog by EAN</h2>
          <div className="flex gap-2">
            <input value={catalogSku} onChange={(e) => setCatalogSku(e.target.value)} placeholder="SKU (optional helper)" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
            <input value={catalogEan} onChange={(e) => setCatalogEan(e.target.value)} placeholder="EAN" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
            <select value={catalogMarket} onChange={(e) => setCatalogMarket(e.target.value)} className="rounded border border-input bg-background px-2 py-1 text-xs">
              {MARKET_OPTIONS.map((m) => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
            <button
              onClick={() => catalogMutation.mutate()}
              disabled={catalogMutation.isPending || !catalogEan.trim()}
              className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40"
            >
              Check
            </button>
          </div>
          {catalogMutation.data && (
            <div className="text-xs text-muted-foreground">
              {catalogSku.trim() ? `SKU: ${catalogSku.trim()} | ` : ""}Matches: {catalogMutation.data.total}
            </div>
          )}
          {catalogMutation.data?.matches?.length ? (
            <div className="max-h-40 overflow-auto rounded border border-border p-2 text-xs">
              {catalogMutation.data.matches.map((m) => (
                <div key={m.asin} className="py-1">
                  <span className="font-mono">{m.asin}</span> | {m.product_type ?? "-"} | {m.title ?? "-"}
                </div>
              ))}
            </div>
          ) : null}
        </div>

        <div className="rounded-xl border border-border bg-card p-4 space-y-2">
          <h2 className="text-sm font-semibold">Restrictions Check</h2>
          <div className="flex gap-2">
            <input value={restrictionAsin} onChange={(e) => setRestrictionAsin(e.target.value)} placeholder="ASIN" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
            <select value={restrictionMarket} onChange={(e) => setRestrictionMarket(e.target.value)} className="rounded border border-input bg-background px-2 py-1 text-xs">
              {MARKET_OPTIONS.map((m) => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
            <button
              onClick={() => restrictionMutation.mutate()}
              disabled={restrictionMutation.isPending || !restrictionAsin.trim()}
              className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40"
            >
              Check
            </button>
          </div>
          {restrictionResult && (
            <div className="space-y-1 text-xs">
              <div className={restrictionResult.can_list ? "text-green-400" : "text-red-400"}>
                can_list: {String(restrictionResult.can_list)} | requires_approval: {String(restrictionResult.requires_approval)}
              </div>
              {restrictionResult.reasons.length > 0 && (
                <div className="max-h-32 overflow-auto rounded border border-border p-2 text-muted-foreground">
                  {restrictionResult.reasons.join(" | ")}
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-3">
        <h2 className="text-sm font-semibold">Publish Push</h2>
        <div className="grid gap-2 md:grid-cols-4">
          <select value={pushSelection} onChange={(e) => setPushSelection(e.target.value as "approved" | "draft")} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="approved">approved</option>
            <option value="draft">draft</option>
          </select>
          <select value={pushMode} onChange={(e) => setPushMode(e.target.value as "preview" | "confirm")} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="preview">preview</option>
            <option value="confirm">confirm</option>
          </select>
          <input value={pushMarketsInput} onChange={(e) => setPushMarketsInput(e.target.value)} placeholder="DE,FR,IT..." className="rounded border border-input bg-background px-2 py-1 text-xs md:col-span-2" />
        </div>
        <textarea
          rows={2}
          value={pushSkuInput}
          onChange={(e) => setPushSkuInput(e.target.value)}
          placeholder="SKU filter (optional, comma/newline separated)"
          className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
        />
        <button
          onClick={() => pushMutation.mutate()}
          disabled={pushMutation.isPending}
          className="rounded border border-border px-3 py-1.5 text-xs disabled:opacity-40"
        >
          {pushMutation.isPending ? "Submitting..." : `Run ${pushMode}`}
        </button>
        {pushMutation.data && (
          <div className="text-xs text-muted-foreground">
            job: {pushJobId} | status: {pushJobStatus}
          </div>
        )}
      </div>

      <div className="rounded-xl border border-border bg-card p-4">
        <h2 className="mb-2 text-sm font-semibold">Publish Jobs</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Job</th>
                <th className="px-2 py-2">Type</th>
                <th className="px-2 py-2">Status</th>
                <th className="px-2 py-2">Markets</th>
                <th className="px-2 py-2">Created</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(jobsQuery.data?.items ?? []).map((job) => (
                <tr key={job.id} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5 font-mono">{job.id.slice(0, 8)}</td>
                  <td className="px-2 py-1.5">{job.job_type}</td>
                  <td className="px-2 py-1.5">
                    <span className={`rounded-full px-2 py-0.5 text-[10px] ${badgeClass(job.status)}`}>{job.status}</span>
                  </td>
                  <td className="px-2 py-1.5">{(job.marketplaces ?? []).join(", ")}</td>
                  <td className="px-2 py-1.5 text-muted-foreground">{job.created_at.slice(0, 19).replace("T", " ")}</td>
                </tr>
              ))}
              {!jobsQuery.isLoading && (jobsQuery.data?.items ?? []).length === 0 && (
                <tr>
                  <td colSpan={5} className="px-2 py-6 text-center text-muted-foreground">No publish jobs</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
