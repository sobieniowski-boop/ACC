import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import {
  applyContentPublishMappingSuggestions,
  createContentPublishPackage,
  getContentAttributeMappings,
  getContentPublishMappingSuggestions,
  getContentProductTypeDefinitions,
  getContentPublishCoverage,
  getContentProductTypeMappings,
  getContentPublishJobs,
  pushContentPublish,
  refreshContentProductTypeDefinition,
  retryContentPublishJob,
  upsertContentAttributeMappings,
  upsertContentProductTypeMappings,
} from "@/lib/api";
import { ClientExportButton } from "@/components/shared";

export default function ContentPublishPage() {
  const qc = useQueryClient();
  const [searchParams] = useSearchParams();
  const [markets, setMarkets] = useState("DE,FR,IT");
  const [selection, setSelection] = useState<"approved" | "draft">("approved");
  const [mode, setMode] = useState<"preview" | "confirm">("preview");
  const [skuFilter, setSkuFilter] = useState("");
  const [idempotencyKey, setIdempotencyKey] = useState("");
  const [minSuggestionConfidence, setMinSuggestionConfidence] = useState(75);
  const jobStatusFilter = (searchParams.get("job_status") || "").trim().toLowerCase();
  const skuFilterFromQuery = (searchParams.get("sku_filter") || "").trim();
  useEffect(() => {
    if (skuFilterFromQuery && !skuFilter) {
      setSkuFilter(skuFilterFromQuery);
    }
  }, [skuFilterFromQuery, skuFilter]);

  const [mapMarket, setMapMarket] = useState("");
  const [mapCategory, setMapCategory] = useState("");
  const [mapSubcategory, setMapSubcategory] = useState("");
  const [mapBrand, setMapBrand] = useState("");
  const [mapProductType, setMapProductType] = useState("");
  const [mapRequiredAttrs, setMapRequiredAttrs] = useState("");
  const [defMarket, setDefMarket] = useState("DE");
  const [defProductType, setDefProductType] = useState("");
  const [attrMarket, setAttrMarket] = useState("");
  const [attrProductType, setAttrProductType] = useState("");
  const [attrSourceField, setAttrSourceField] = useState("");
  const [attrTargetField, setAttrTargetField] = useState("");
  const [attrTransform, setAttrTransform] = useState<"identity" | "stringify" | "upper" | "lower" | "trim">("identity");

  const jobsQuery = useQuery({
    queryKey: ["content-publish-jobs-view"],
    queryFn: () => getContentPublishJobs({ page: 1, page_size: 30 }),
  });

  const mappingsQuery = useQuery({
    queryKey: ["content-product-type-mappings"],
    queryFn: getContentProductTypeMappings,
  });
  const definitionsQuery = useQuery({
    queryKey: ["content-product-type-definitions"],
    queryFn: () => getContentProductTypeDefinitions(),
  });
  const attributeMappingsQuery = useQuery({
    queryKey: ["content-attribute-mappings"],
    queryFn: getContentAttributeMappings,
  });
  const coverageQuery = useQuery({
    queryKey: ["content-publish-coverage", markets, selection],
    queryFn: () => getContentPublishCoverage(markets, selection),
  });
  const mappingSuggestionsQuery = useQuery({
    queryKey: ["content-publish-mapping-suggestions", markets, selection],
    queryFn: () => getContentPublishMappingSuggestions(markets, selection, 120),
  });

  const packageMutation = useMutation({
    mutationFn: () => {
      const marketplaces = markets.split(",").map((x) => x.trim().toUpperCase()).filter(Boolean);
      const sku_filter = skuFilter.split(/\r?\n|,/).map((x) => x.trim()).filter(Boolean);
      return createContentPublishPackage({ marketplaces, selection, format: "xlsx", sku_filter });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["content-publish-jobs-view"] }),
  });

  const pushMutation = useMutation({
    mutationFn: () => {
      const marketplaces = markets.split(",").map((x) => x.trim().toUpperCase()).filter(Boolean);
      const sku_filter = skuFilter.split(/\r?\n|,/).map((x) => x.trim()).filter(Boolean);
      return pushContentPublish({
        marketplaces,
        selection,
        mode,
        sku_filter,
        ...(idempotencyKey.trim() ? { idempotency_key: idempotencyKey.trim() } : {}),
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["content-publish-jobs-view"] }),
  });
  const refreshDefinitionMutation = useMutation({
    mutationFn: () =>
      refreshContentProductTypeDefinition({
        marketplace: defMarket.trim().toUpperCase(),
        product_type: defProductType.trim().toUpperCase(),
      }),
    onSuccess: () => {
      setDefProductType("");
      qc.invalidateQueries({ queryKey: ["content-product-type-definitions"] });
    },
  });

  const addMappingMutation = useMutation({
    mutationFn: () => {
      const rules = mappingsQuery.data ?? [];
      const required_attrs = mapRequiredAttrs.split(",").map((x) => x.trim()).filter(Boolean);
      return upsertContentProductTypeMappings([
        ...rules,
        {
          marketplace_id: mapMarket.trim() || undefined,
          brand: mapBrand.trim() || undefined,
          category: mapCategory.trim() || undefined,
          subcategory: mapSubcategory.trim() || undefined,
          product_type: mapProductType.trim().toUpperCase(),
          required_attrs,
          priority: 100,
          is_active: true,
        },
      ]);
    },
    onSuccess: () => {
      setMapMarket("");
      setMapCategory("");
      setMapSubcategory("");
      setMapBrand("");
      setMapProductType("");
      setMapRequiredAttrs("");
      qc.invalidateQueries({ queryKey: ["content-product-type-mappings"] });
    },
  });
  const addAttributeMapMutation = useMutation({
    mutationFn: () => {
      const rules = attributeMappingsQuery.data ?? [];
      return upsertContentAttributeMappings([
        ...rules,
        {
          marketplace_id: attrMarket.trim() || undefined,
          product_type: attrProductType.trim().toUpperCase() || undefined,
          source_field: attrSourceField.trim(),
          target_attribute: attrTargetField.trim(),
          transform: attrTransform,
          priority: 100,
          is_active: true,
        },
      ]);
    },
    onSuccess: () => {
      setAttrMarket("");
      setAttrProductType("");
      setAttrSourceField("");
      setAttrTargetField("");
      setAttrTransform("identity");
      qc.invalidateQueries({ queryKey: ["content-attribute-mappings"] });
    },
  });
  const applySuggestionsMutation = useMutation({
    mutationFn: (dry_run: boolean) =>
      applyContentPublishMappingSuggestions({
        marketplaces: markets.split(",").map((x) => x.trim().toUpperCase()).filter(Boolean),
        selection,
        min_confidence: minSuggestionConfidence,
        limit: 150,
        dry_run,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["content-attribute-mappings"] });
      qc.invalidateQueries({ queryKey: ["content-publish-mapping-suggestions"] });
    },
  });
  const retryJobMutation = useMutation({
    mutationFn: ({ jobId, skuFilter }: { jobId: string; skuFilter: string[] }) =>
      retryContentPublishJob(jobId, {
        sku_filter: skuFilter,
        failed_only: skuFilter.length === 0,
        idempotency_key: `manual-retry-${jobId}-${Date.now()}`,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["content-publish-jobs-view"] });
    },
  });
  const visibleJobs = (jobsQuery.data?.items ?? []).filter((job) => {
    if (!jobStatusFilter) return true;
    return String(job.status || "").toLowerCase() === jobStatusFilter;
  });

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold">Publish Center</h1>
        <p className="text-sm text-muted-foreground">Package + push + product type mappings</p>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <div className="grid gap-2 md:grid-cols-4">
          <input value={markets} onChange={(e) => setMarkets(e.target.value)} placeholder="DE,FR,IT..." className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <select value={selection} onChange={(e) => setSelection(e.target.value as "approved" | "draft")} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="approved">approved</option>
            <option value="draft">draft</option>
          </select>
          <select value={mode} onChange={(e) => setMode(e.target.value as "preview" | "confirm")} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="preview">preview</option>
            <option value="confirm">confirm</option>
          </select>
          <input value={skuFilter} onChange={(e) => setSkuFilter(e.target.value)} placeholder="SKU filter optional" className="rounded border border-input bg-background px-2 py-1 text-xs" />
        </div>
        <div className="grid gap-2 md:grid-cols-5">
          <input
            value={idempotencyKey}
            onChange={(e) => setIdempotencyKey(e.target.value)}
            placeholder="idempotency key (confirm mode)"
            className="rounded border border-input bg-background px-2 py-1 text-xs md:col-span-4"
          />
          <button
            onClick={() => setIdempotencyKey(`co-push-${Date.now()}`)}
            className="rounded border border-border px-2 py-1 text-xs"
          >
            Generate key
          </button>
        </div>
        <div className="flex gap-2">
          <button onClick={() => packageMutation.mutate()} className="rounded border border-border px-2 py-1 text-xs">Create package</button>
          <button onClick={() => pushMutation.mutate()} className="rounded border border-border px-2 py-1 text-xs">Run push</button>
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Product Type Mapping Rules</h2>
        <div className="grid gap-2 md:grid-cols-4">
          <input value={defMarket} onChange={(e) => setDefMarket(e.target.value)} placeholder="market (DE...)" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={defProductType} onChange={(e) => setDefProductType(e.target.value)} placeholder="product_type to refresh" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <button onClick={() => refreshDefinitionMutation.mutate()} disabled={!defProductType.trim()} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Refresh PTD</button>
          <div className="text-xs text-muted-foreground self-center">Definitions: {definitionsQuery.data?.length ?? 0}</div>
        </div>
        <div className="grid gap-2 md:grid-cols-7">
          <input value={mapMarket} onChange={(e) => setMapMarket(e.target.value)} placeholder="marketplace_id/code" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={mapBrand} onChange={(e) => setMapBrand(e.target.value)} placeholder="brand" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={mapCategory} onChange={(e) => setMapCategory(e.target.value)} placeholder="category" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={mapSubcategory} onChange={(e) => setMapSubcategory(e.target.value)} placeholder="subcategory" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={mapProductType} onChange={(e) => setMapProductType(e.target.value)} placeholder="product_type" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={mapRequiredAttrs} onChange={(e) => setMapRequiredAttrs(e.target.value)} placeholder="required attrs (comma)" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <button onClick={() => addMappingMutation.mutate()} disabled={!mapProductType.trim()} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Add</button>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Market</th>
                <th className="px-2 py-2">Brand</th>
                <th className="px-2 py-2">Category</th>
                <th className="px-2 py-2">Subcategory</th>
                <th className="px-2 py-2">ProductType</th>
                <th className="px-2 py-2">Required Attrs</th>
                <th className="px-2 py-2">Priority</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(mappingsQuery.data ?? []).map((r) => (
                <tr key={r.id ?? `${r.product_type}-${r.category ?? ""}-${r.subcategory ?? ""}`} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5">{r.marketplace_id ?? "*"}</td>
                  <td className="px-2 py-1.5">{r.brand ?? "*"}</td>
                  <td className="px-2 py-1.5">{r.category ?? "*"}</td>
                  <td className="px-2 py-1.5">{r.subcategory ?? "*"}</td>
                  <td className="px-2 py-1.5">{r.product_type}</td>
                  <td className="px-2 py-1.5">{(r.required_attrs ?? []).join(", ")}</td>
                  <td className="px-2 py-1.5">{r.priority}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Attribute Mapping Registry</h2>
        <div className="grid gap-2 md:grid-cols-6">
          <input value={attrMarket} onChange={(e) => setAttrMarket(e.target.value)} placeholder="market (optional)" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={attrProductType} onChange={(e) => setAttrProductType(e.target.value)} placeholder="product_type (optional)" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={attrSourceField} onChange={(e) => setAttrSourceField(e.target.value)} placeholder="source_field e.g. fields.title" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={attrTargetField} onChange={(e) => setAttrTargetField(e.target.value)} placeholder="target_attribute" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <select value={attrTransform} onChange={(e) => setAttrTransform(e.target.value as typeof attrTransform)} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="identity">identity</option>
            <option value="stringify">stringify</option>
            <option value="upper">upper</option>
            <option value="lower">lower</option>
            <option value="trim">trim</option>
          </select>
          <button onClick={() => addAttributeMapMutation.mutate()} disabled={!attrSourceField.trim() || !attrTargetField.trim()} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Add</button>
        </div>
        <div className="text-xs text-muted-foreground">Rules: {attributeMappingsQuery.data?.length ?? 0}</div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Coverage by Category / Product Type</h2>
        <ClientExportButton data={coverageQuery.data?.items ?? []} filename="publish_coverage" />
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Market</th>
                <th className="px-2 py-2">Category</th>
                <th className="px-2 py-2">ProductType</th>
                <th className="px-2 py-2">Coverage</th>
                <th className="px-2 py-2">Missing required top</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(coverageQuery.data?.items ?? []).map((x) => (
                <tr key={`${x.marketplace_id}-${x.category ?? "-"}-${x.product_type}`} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5">{x.marketplace_id}</td>
                  <td className="px-2 py-1.5">{x.category ?? "-"}</td>
                  <td className="px-2 py-1.5">{x.product_type}</td>
                  <td className="px-2 py-1.5">{x.coverage_pct.toFixed(2)}% ({x.fully_covered}/{x.total_candidates})</td>
                  <td className="px-2 py-1.5">{x.missing_required_top.join(", ") || "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Mapping Suggestions (missing required attrs)</h2>
        <div className="text-xs text-muted-foreground">
          Suggestions are generated from live candidates and can be copied into attribute mapping registry.
        </div>
        <div className="flex items-center gap-2">
          <input
            value={minSuggestionConfidence}
            type="number"
            min={0}
            max={100}
            onChange={(e) => setMinSuggestionConfidence(Number(e.target.value || 0))}
            className="w-24 rounded border border-input bg-background px-2 py-1 text-xs"
          />
          <button
            onClick={() => applySuggestionsMutation.mutate(true)}
            className="rounded border border-border px-2 py-1 text-xs"
          >
            Dry run apply
          </button>
          <button
            onClick={() => applySuggestionsMutation.mutate(false)}
            className="rounded border border-border px-2 py-1 text-xs"
          >
            Apply suggestions
          </button>
          {applySuggestionsMutation.data && (
            <span className="text-xs text-muted-foreground">
              created={applySuggestionsMutation.data.created} skipped={applySuggestionsMutation.data.skipped}
            </span>
          )}
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Market</th>
                <th className="px-2 py-2">ProductType</th>
                <th className="px-2 py-2">Missing attr</th>
                <th className="px-2 py-2">Suggested source</th>
                <th className="px-2 py-2">Confidence</th>
                <th className="px-2 py-2">Affected SKUs</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(mappingSuggestionsQuery.data?.items ?? []).map((x) => (
                <tr key={`${x.marketplace_id}-${x.product_type}-${x.missing_attribute}`} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5">{x.marketplace_id}</td>
                  <td className="px-2 py-1.5">{x.product_type}</td>
                  <td className="px-2 py-1.5">{x.missing_attribute}</td>
                  <td className="px-2 py-1.5 font-mono">{x.suggested_source_field ?? "-"}</td>
                  <td className="px-2 py-1.5">{x.confidence.toFixed(2)}%</td>
                  <td className="px-2 py-1.5">{x.affected_skus}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4">
        <h2 className="mb-2 text-sm font-semibold">Jobs</h2>
        <ClientExportButton data={visibleJobs} filename="publish_jobs" />
        <div className="mb-2 text-xs text-muted-foreground">`confirm` runs async: status changes queued/running/completed via polling list.</div>
        {jobStatusFilter && (
          <div className="mb-2 text-xs text-muted-foreground">
            Active filter: status = <span className="font-semibold">{jobStatusFilter}</span>
          </div>
        )}
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">ID</th>
                <th className="px-2 py-2">Type</th>
                <th className="px-2 py-2">Status</th>
                <th className="px-2 py-2">Markets</th>
                <th className="px-2 py-2">Errors</th>
                <th className="px-2 py-2">Retry</th>
                <th className="px-2 py-2">Created</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {visibleJobs.map((job) => (
                <tr key={job.id} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5 font-mono">{job.id.slice(0, 8)}</td>
                  <td className="px-2 py-1.5">{job.job_type}</td>
                  <td className="px-2 py-1.5">{job.status}</td>
                  <td className="px-2 py-1.5">{job.marketplaces.join(", ")}</td>
                  <td className="px-2 py-1.5">
                    {(() => {
                      const perMarket = (job.log_json?.per_marketplace as Record<string, any> | undefined) ?? {};
                      let count = 0;
                      for (const k of Object.keys(perMarket)) {
                        const errs = perMarket[k]?.native_errors;
                        if (Array.isArray(errs)) count += errs.length;
                      }
                      return count;
                    })()}
                  </td>
                  <td className="px-2 py-1.5">
                    <button
                      onClick={() => {
                        const perMarket = (job.log_json?.per_marketplace as Record<string, any> | undefined) ?? {};
                        const failedSkuSet = new Set<string>();
                        for (const key of Object.keys(perMarket)) {
                          const errs = perMarket[key]?.native_errors;
                          if (!Array.isArray(errs)) continue;
                          for (const err of errs) {
                            const sku = typeof err?.sku === "string" ? err.sku : "";
                            if (sku) failedSkuSet.add(sku);
                          }
                        }
                        retryJobMutation.mutate({ jobId: job.id, skuFilter: Array.from(failedSkuSet) });
                      }}
                      disabled={retryJobMutation.isPending || !(job.status === "failed" || job.status === "partial")}
                      className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40"
                    >
                      Retry
                    </button>
                  </td>
                  <td className="px-2 py-1.5 text-muted-foreground">{job.created_at.slice(0, 19).replace("T", " ")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
