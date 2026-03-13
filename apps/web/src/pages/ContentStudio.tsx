import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import {
  approveContentVersion,
  bulkUpdateContentTasks,
  checkContentPolicy,
  checkContentRestrictions,
  createContentTask,
  createContentVersion,
  getContentDataQuality,
  getContentDiff,
  getContentImpact,
  getContentOpsHealth,
  getContentPublishJobs,
  getContentTasks,
  getContentVersions,
  pushContentPublish,
  runContentOnboardPreflight,
  searchContentCatalogByEan,
  submitContentVersionReview,
  syncContent,
  updateContentVersion,
  type ContentFieldsPayload,
  type ContentOnboardPreflightItem,
} from "@/lib/api";
import { ClientExportButton } from "@/components/shared";

/* ────────────────────────────────────────────────────────────────── */
/* Helpers                                                            */
/* ────────────────────────────────────────────────────────────────── */

const MARKET_OPTIONS = ["DE", "FR", "IT", "ES", "NL", "PL", "SE", "BE"] as const;

const TABS = [
  { id: "overview", label: "Przegląd", hint: "Metryki, zdrowie systemu, jakość danych" },
  { id: "tasks", label: "Zadania", hint: "Backlog zadań content, bulk operacje" },
  { id: "editor", label: "Edytor", hint: "Edycja treści per SKU/marketplace, wersje, diff" },
  { id: "onboard", label: "Onboarding", hint: "Preflight, katalog, restrykcje, quick publish" },
] as const;

type TabId = (typeof TABS)[number]["id"];

function badgeClass(value: string): string {
  if (value === "completed" || value === "submitted" || value === "preview_ready") return "bg-green-500/10 text-green-400";
  if (value === "partial" || value === "investigating") return "bg-yellow-500/10 text-yellow-400";
  if (value === "failed") return "bg-red-500/10 text-red-400";
  return "bg-blue-500/10 text-blue-400";
}

function Tip({ text }: { text: string }) {
  return (
    <span title={text} className="ml-1 inline-flex h-4 w-4 cursor-help items-center justify-center rounded-full border border-border text-[9px] text-muted-foreground">
      ?
    </span>
  );
}

/* ────────────────────────────────────────────────────────────────── */
/* MAIN                                                               */
/* ────────────────────────────────────────────────────────────────── */

export default function ContentStudioPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const initialTab = (searchParams.get("tab") as TabId) || "overview";
  const [tab, setTab] = useState<TabId>(initialTab);

  const changeTab = (t: TabId) => {
    setTab(t);
    setSearchParams({ tab: t }, { replace: true });
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold">Content Studio</h1>
        <p className="text-sm text-muted-foreground">
          Zarządzaj treściami produktów — od onboardingu, przez edycję, po publikację na Amazon
        </p>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 rounded-lg border border-border bg-card p-1">
        {TABS.map((t) => (
          <button
            key={t.id}
            onClick={() => changeTab(t.id)}
            title={t.hint}
            className={`rounded-md px-4 py-2 text-sm font-medium transition-colors ${
              tab === t.id ? "bg-primary text-primary-foreground shadow-sm" : "text-muted-foreground hover:bg-accent hover:text-foreground"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === "overview" && <OverviewTab />}
      {tab === "tasks" && <TasksTab />}
      {tab === "editor" && <EditorTab />}
      {tab === "onboard" && <OnboardTab />}
    </div>
  );
}

/* ────────────────────────────────────────────────────────────────── */
/* TAB 1: Overview (z Dashboard + Health)                             */
/* ────────────────────────────────────────────────────────────────── */

function OverviewTab() {
  const tasksQuery = useQuery({
    queryKey: ["cs-overview-tasks"],
    queryFn: () => getContentTasks({ page: 1, page_size: 200 }),
  });
  const qualityQuery = useQuery({
    queryKey: ["cs-data-quality"],
    queryFn: getContentDataQuality,
  });
  const healthQuery = useQuery({
    queryKey: ["cs-health"],
    queryFn: getContentOpsHealth,
    refetchInterval: 30_000,
  });

  const summary = useMemo(() => {
    const rows = tasksQuery.data?.items ?? [];
    return {
      total: tasksQuery.data?.total ?? 0,
      p0: rows.filter((r) => r.priority === "p0").length,
      open: rows.filter((r) => r.status === "open").length,
      investigating: rows.filter((r) => r.status === "investigating").length,
      overdue: rows.filter((r) => r.due_date && r.due_date.slice(0, 10) < new Date().toISOString().slice(0, 10) && r.status !== "resolved").length,
    };
  }, [tasksQuery.data]);

  const health = healthQuery.data;

  return (
    <div className="space-y-4">
      {/* Quick Start Guide */}
      <div className="rounded-xl border border-blue-500/20 bg-blue-500/5 p-4">
        <h3 className="text-sm font-semibold text-blue-400">Szybki start</h3>
        <ol className="mt-2 list-inside list-decimal space-y-1 text-xs text-muted-foreground">
          <li><strong>Onboarding</strong> — sprawdź czy SKU spełnia wymagania (tab Onboarding → Preflight)</li>
          <li><strong>Edycja</strong> — uzupełnij tytuł, bullets, opis, słowa kluczowe (tab Edytor)</li>
          <li><strong>Compliance</strong> — sprawdź policy check (Edytor → Policy check lub Compliance w menu)</li>
          <li><strong>Publikacja</strong> — wypchnij na Amazon (tab Onboarding → Publish push lub Publish w menu)</li>
        </ol>
      </div>

      {/* KPI Cards */}
      <div className="grid gap-2 md:grid-cols-5">
        <div className="rounded-lg border border-border bg-card p-3">
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Zadania łącznie</div>
          <div className="text-xl font-bold">{summary.total}</div>
        </div>
        <div className="rounded-lg border border-border bg-card p-3">
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Priorytet P0</div>
          <div className={`text-xl font-bold ${summary.p0 > 0 ? "text-red-400" : ""}`}>{summary.p0}</div>
        </div>
        <div className="rounded-lg border border-border bg-card p-3">
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Otwarte</div>
          <div className="text-xl font-bold">{summary.open}</div>
        </div>
        <div className="rounded-lg border border-border bg-card p-3">
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">W trakcie</div>
          <div className="text-xl font-bold">{summary.investigating}</div>
        </div>
        <div className="rounded-lg border border-border bg-card p-3">
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Zaległe</div>
          <div className={`text-xl font-bold ${summary.overdue > 0 ? "text-orange-400" : ""}`}>{summary.overdue}</div>
        </div>
      </div>

      {/* System health + data quality side by side */}
      <div className="grid gap-4 lg:grid-cols-2">
        <div className="rounded-xl border border-border bg-card p-4 space-y-2">
          <h2 className="text-sm font-semibold">Zdrowie systemu <Tip text="Automatyczne metryki z kolejki publish, compliance i zadań" /></h2>
          {health ? (
            <div className="grid gap-2 grid-cols-3">
              <div className="rounded border border-border p-2 text-xs">
                <div className="text-muted-foreground">W kolejce</div>
                <div className="font-bold">{health.queue_health.queued_total}</div>
              </div>
              <div className="rounded border border-border p-2 text-xs">
                <div className="text-muted-foreground">Stale &gt;30m</div>
                <div className={`font-bold ${health.queue_health.queued_stale_30m > 0 ? "text-orange-400" : ""}`}>{health.queue_health.queued_stale_30m}</div>
              </div>
              <div className="rounded border border-border p-2 text-xs">
                <div className="text-muted-foreground">Błędy 24h</div>
                <div className={`font-bold ${health.queue_health.failed_last_24h > 0 ? "text-red-400" : ""}`}>{health.queue_health.failed_last_24h}</div>
              </div>
              <div className="rounded border border-border p-2 text-xs">
                <div className="text-muted-foreground">Compliance critical</div>
                <div className={`font-bold ${(health.compliance_backlog?.critical ?? 0) > 0 ? "text-red-400" : ""}`}>{health.compliance_backlog?.critical ?? 0}</div>
              </div>
              <div className="rounded border border-border p-2 text-xs">
                <div className="text-muted-foreground">Retry w toku</div>
                <div className="font-bold">{health.queue_health.retry_in_progress}</div>
              </div>
              <div className="rounded border border-border p-2 text-xs">
                <div className="text-muted-foreground">Zadania zaległe</div>
                <div className={`font-bold ${(health.tasks_health?.overdue ?? 0) > 0 ? "text-orange-400" : ""}`}>{health.tasks_health?.overdue ?? 0}</div>
              </div>
            </div>
          ) : (
            <div className="text-xs text-muted-foreground">Ładowanie...</div>
          )}
        </div>

        <div className="rounded-xl border border-border bg-card p-4 space-y-2">
          <h2 className="text-sm font-semibold">Jakość danych <Tip text="Automatycznie obliczane metryki pokrycia pól, obrazków itp." /></h2>
          <div className="grid gap-2 grid-cols-3">
            {(qualityQuery.data?.cards ?? []).map((c) => (
              <div key={c.key} className="rounded border border-border p-2 text-xs">
                <div className="text-muted-foreground">{c.key}</div>
                <div className="font-bold">{c.value.toFixed(1)}{c.unit === "pct" ? "%" : ""}</div>
              </div>
            ))}
            {(qualityQuery.data?.cards ?? []).length === 0 && !qualityQuery.isLoading && (
              <div className="col-span-3 text-xs text-muted-foreground">Brak danych quality</div>
            )}
          </div>
        </div>
      </div>

      {/* Release calendar */}
      <ReleaseCalendar tasks={tasksQuery.data?.items ?? []} />
    </div>
  );
}

function ReleaseCalendar({ tasks }: { tasks: { due_date?: string | null; priority?: string; sku: string }[] }) {
  const calendar = useMemo(() => {
    const bucket: Record<string, { count: number; p0: number; skus: Set<string> }> = {};
    for (const row of tasks) {
      if (!row.due_date) continue;
      const day = row.due_date.slice(0, 10);
      if (!bucket[day]) bucket[day] = { count: 0, p0: 0, skus: new Set() };
      bucket[day].count += 1;
      if (row.priority === "p0") bucket[day].p0 += 1;
      bucket[day].skus.add(row.sku);
    }
    return Object.entries(bucket)
      .map(([day, v]) => ({ day, count: v.count, p0: v.p0, skuCount: v.skus.size }))
      .sort((a, b) => a.day.localeCompare(b.day))
      .slice(0, 14);
  }, [tasks]);

  if (calendar.length === 0) return null;

  return (
    <div className="rounded-xl border border-border bg-card p-4 space-y-2">
      <h2 className="text-sm font-semibold">Kalendarz wydań <Tip text="Najbliższe due dates z backlogu zadań" /></h2>
      <div className="overflow-x-auto">
        <table className="w-full text-[11px]">
          <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
            <tr><th className="px-2 py-2">Data</th><th className="px-2 py-2">Zadania</th><th className="px-2 py-2">P0</th><th className="px-2 py-2">SKU</th></tr>
          </thead>
          <tbody className="divide-y divide-border">
            {calendar.map((x) => (
              <tr key={x.day} className="hover:bg-muted/20"><td className="px-2 py-1.5">{x.day}</td><td className="px-2 py-1.5">{x.count}</td><td className="px-2 py-1.5">{x.p0}</td><td className="px-2 py-1.5">{x.skuCount}</td></tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ────────────────────────────────────────────────────────────────── */
/* TAB 2: Tasks (z Dashboard)                                         */
/* ────────────────────────────────────────────────────────────────── */

function TasksTab() {
  const qc = useQueryClient();
  const [status, setStatus] = useState("open");
  const [skuSearch, setSkuSearch] = useState("");
  const [newSku, setNewSku] = useState("");
  const [selectedTaskIds, setSelectedTaskIds] = useState<string[]>([]);
  const [bulkStatus, setBulkStatus] = useState<"open" | "investigating" | "resolved">("investigating");

  const tasksQuery = useQuery({
    queryKey: ["cs-tasks", status, skuSearch],
    queryFn: () =>
      getContentTasks({
        page: 1,
        page_size: 50,
        ...(status ? { status } : {}),
        ...(skuSearch.trim() ? { sku_search: skuSearch.trim() } : {}),
      }),
  });

  const createMut = useMutation({
    mutationFn: () =>
      createContentTask({ type: "refresh_content", sku: newSku.trim(), priority: "p1", source_page: "content_studio" }),
    onSuccess: () => { setNewSku(""); qc.invalidateQueries({ queryKey: ["cs-tasks"] }); },
  });

  const bulkMut = useMutation({
    mutationFn: () => bulkUpdateContentTasks({ task_ids: selectedTaskIds, status: bulkStatus }),
    onSuccess: () => { setSelectedTaskIds([]); qc.invalidateQueries({ queryKey: ["cs-tasks"] }); },
  });

  return (
    <div className="space-y-4">
      {/* Quick task */}
      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Szybkie zadanie <Tip text="Utwórz task typu refresh_content dla danego SKU" /></h2>
        <div className="flex gap-2">
          <input value={newSku} onChange={(e) => setNewSku(e.target.value)} placeholder="SKU" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
          <button onClick={() => createMut.mutate()} disabled={!newSku.trim() || createMut.isPending} className="rounded border border-border px-3 py-1 text-xs disabled:opacity-40">Dodaj</button>
        </div>
      </div>

      {/* Filters + Bulk */}
      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <div className="flex gap-2">
          <select value={status} onChange={(e) => setStatus(e.target.value)} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="">Wszystkie</option>
            <option value="open">Otwarte</option>
            <option value="investigating">W trakcie</option>
            <option value="resolved">Zamknięte</option>
          </select>
          <input value={skuSearch} onChange={(e) => setSkuSearch(e.target.value)} placeholder="Szukaj SKU..." className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
        </div>
        <div className="flex gap-2">
          <select value={bulkStatus} onChange={(e) => setBulkStatus(e.target.value as typeof bulkStatus)} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="open">→ otwarte</option>
            <option value="investigating">→ w trakcie</option>
            <option value="resolved">→ zamknięte</option>
          </select>
          <button onClick={() => bulkMut.mutate()} disabled={selectedTaskIds.length === 0 || bulkMut.isPending} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">
            Zmień status ({selectedTaskIds.length})
          </button>
          <ClientExportButton data={tasksQuery.data?.items ?? []} filename="content_tasks" />
        </div>

        {/* Table */}
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">
                  <input type="checkbox" checked={(tasksQuery.data?.items ?? []).length > 0 && selectedTaskIds.length === (tasksQuery.data?.items ?? []).length} onChange={(e) => setSelectedTaskIds(e.target.checked ? (tasksQuery.data?.items ?? []).map((r) => r.id) : [])} />
                </th>
                <th className="px-2 py-2">Typ</th>
                <th className="px-2 py-2">SKU</th>
                <th className="px-2 py-2">Status</th>
                <th className="px-2 py-2">Priorytet</th>
                <th className="px-2 py-2">Owner</th>
                <th className="px-2 py-2">Zmieniono</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(tasksQuery.data?.items ?? []).map((row) => (
                <tr key={row.id} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5">
                    <input type="checkbox" checked={selectedTaskIds.includes(row.id)} onChange={(e) => setSelectedTaskIds((p) => e.target.checked ? [...new Set([...p, row.id])] : p.filter((id) => id !== row.id))} />
                  </td>
                  <td className="px-2 py-1.5">{row.type}</td>
                  <td className="px-2 py-1.5 font-mono">{row.sku}</td>
                  <td className="px-2 py-1.5"><span className={`rounded-full px-2 py-0.5 text-[10px] ${badgeClass(row.status)}`}>{row.status}</span></td>
                  <td className="px-2 py-1.5">{row.priority}</td>
                  <td className="px-2 py-1.5">{row.owner ?? "-"}</td>
                  <td className="px-2 py-1.5 text-muted-foreground">{row.updated_at.slice(0, 16).replace("T", " ")}</td>
                </tr>
              ))}
              {!tasksQuery.isLoading && (tasksQuery.data?.items ?? []).length === 0 && (
                <tr><td colSpan={7} className="px-2 py-6 text-center text-muted-foreground">Brak zadań dla wybranego filtra</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

/* ────────────────────────────────────────────────────────────────── */
/* TAB 3: Editor (z ContentEditor)                                    */
/* ────────────────────────────────────────────────────────────────── */

function EditorTab() {
  const qc = useQueryClient();
  const [searchParams] = useSearchParams();
  const [sku, setSku] = useState(searchParams.get("sku") || "");
  const [marketplaceId, setMarketplaceId] = useState(searchParams.get("marketplace")?.toUpperCase() || "DE");
  const [selectedVersionId, setSelectedVersionId] = useState<string | null>(null);
  const [diffMain, setDiffMain] = useState("DE");
  const [diffTarget, setDiffTarget] = useState("FR");
  const [syncTargets, setSyncTargets] = useState("FR,IT,ES");
  const [syncFields, setSyncFields] = useState("title,bullets,description,keywords");
  const [policyOutput, setPolicyOutput] = useState("");
  const [fields, setFields] = useState<ContentFieldsPayload>({
    title: "", bullets: [], description: "", keywords: "",
    special_features: [], attributes_json: {}, aplus_json: {}, compliance_notes: "",
  });

  const versionsQuery = useQuery({
    queryKey: ["cs-editor-versions", sku, marketplaceId],
    queryFn: () => getContentVersions(sku.trim(), marketplaceId),
    enabled: !!sku.trim(),
  });

  const diffQuery = useQuery({
    queryKey: ["cs-editor-diff", sku, diffMain, diffTarget],
    queryFn: () => getContentDiff(sku.trim(), { main: diffMain.toUpperCase(), target: diffTarget.toUpperCase() }),
    enabled: !!sku.trim() && diffMain !== diffTarget,
  });

  useEffect(() => {
    const first = versionsQuery.data?.items?.[0];
    if (!first) return;
    setSelectedVersionId(first.id);
    setFields(first.fields ?? {});
  }, [versionsQuery.data?.items?.[0]?.id]);

  const createDraft = useMutation({
    mutationFn: () => createContentVersion(sku.trim(), marketplaceId, { fields }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["cs-editor-versions"] }),
  });
  const save = useMutation({
    mutationFn: () => { if (!selectedVersionId) throw new Error("Brak wersji"); return updateContentVersion(selectedVersionId, { fields }); },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["cs-editor-versions"] }),
  });
  const submit = useMutation({
    mutationFn: () => { if (!selectedVersionId) throw new Error("Brak wersji"); return submitContentVersionReview(selectedVersionId); },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["cs-editor-versions"] }),
  });
  const approve = useMutation({
    mutationFn: () => { if (!selectedVersionId) throw new Error("Brak wersji"); return approveContentVersion(selectedVersionId); },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["cs-editor-versions"] }),
  });
  const check = useMutation({
    mutationFn: () => { if (!selectedVersionId) throw new Error("Brak wersji"); return checkContentPolicy(selectedVersionId); },
    onSuccess: (r) => setPolicyOutput(`passed=${r.passed} critical=${r.critical_count} major=${r.major_count} minor=${r.minor_count}`),
  });
  const sync = useMutation({
    mutationFn: () =>
      syncContent(sku.trim(), {
        fields: syncFields.split(",").map((x) => x.trim()).filter(Boolean),
        from_market: diffMain.toUpperCase(),
        to_markets: syncTargets.split(",").map((x) => x.trim().toUpperCase()).filter(Boolean),
        overwrite_mode: "missing_only",
      }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["cs-editor-versions"] }); qc.invalidateQueries({ queryKey: ["cs-editor-diff"] }); },
  });

  const bulletsText = (fields.bullets ?? []).join("\n");

  return (
    <div className="space-y-4">
      {/* SKU selector */}
      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Wybierz produkt <Tip text="Wpisz SKU i marketplace, aby załadować wersje treści" /></h2>
        <div className="grid gap-2 md:grid-cols-3">
          <input value={sku} onChange={(e) => setSku(e.target.value)} placeholder="SKU" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <select value={marketplaceId} onChange={(e) => setMarketplaceId(e.target.value)} className="rounded border border-input bg-background px-2 py-1 text-xs">
            {MARKET_OPTIONS.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
          <button onClick={() => versionsQuery.refetch()} disabled={!sku.trim()} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Załaduj wersje</button>
        </div>
      </div>

      {/* Version list + Form */}
      <div className="grid gap-4 lg:grid-cols-[240px_1fr]">
        <div className="rounded-xl border border-border bg-card p-4 space-y-2">
          <div className="text-sm font-semibold">Wersje <Tip text="Lista wersji treści — kliknij aby edytować" /></div>
          <button onClick={() => createDraft.mutate()} disabled={!sku.trim() || createDraft.isPending} className="w-full rounded border border-border px-2 py-1 text-xs disabled:opacity-40">+ Nowy draft</button>
          <div className="max-h-60 overflow-auto space-y-1">
            {(versionsQuery.data?.items ?? []).map((v) => (
              <button
                key={v.id}
                onClick={() => { setSelectedVersionId(v.id); setFields(v.fields ?? {}); }}
                className={`w-full rounded border px-2 py-1 text-left text-xs ${selectedVersionId === v.id ? "border-blue-500 bg-blue-500/5" : "border-border"}`}
              >
                v{v.version_no} — <span className={`${badgeClass(v.status)} rounded-full px-1.5 py-0.5 text-[10px]`}>{v.status}</span>
              </button>
            ))}
          </div>
        </div>

        <div className="rounded-xl border border-border bg-card p-4 space-y-3">
          <h3 className="text-sm font-semibold">Treść <Tip text="Wypełnij pola — Title, Bullets, Description, Keywords" /></h3>
          <div>
            <label className="text-[10px] uppercase tracking-wider text-muted-foreground">Tytuł</label>
            <input value={fields.title ?? ""} onChange={(e) => setFields((f) => ({ ...f, title: e.target.value }))} className="w-full rounded border border-input bg-background px-2 py-1.5 text-xs" />
          </div>
          <div>
            <label className="text-[10px] uppercase tracking-wider text-muted-foreground">Bullets (po jednym w linii)</label>
            <textarea rows={5} value={bulletsText} onChange={(e) => setFields((f) => ({ ...f, bullets: e.target.value.split(/\r?\n/).map((x) => x.trim()).filter(Boolean) }))} className="w-full rounded border border-input bg-background px-2 py-1.5 text-xs" />
          </div>
          <div>
            <label className="text-[10px] uppercase tracking-wider text-muted-foreground">Opis</label>
            <textarea rows={3} value={fields.description ?? ""} onChange={(e) => setFields((f) => ({ ...f, description: e.target.value }))} className="w-full rounded border border-input bg-background px-2 py-1.5 text-xs" />
          </div>
          <div>
            <label className="text-[10px] uppercase tracking-wider text-muted-foreground">Słowa kluczowe</label>
            <input value={fields.keywords ?? ""} onChange={(e) => setFields((f) => ({ ...f, keywords: e.target.value }))} className="w-full rounded border border-input bg-background px-2 py-1.5 text-xs" />
          </div>
          <div className="flex flex-wrap gap-2 pt-1">
            <button onClick={() => save.mutate()} disabled={!selectedVersionId || save.isPending} className="rounded bg-primary px-3 py-1.5 text-xs text-primary-foreground disabled:opacity-40">Zapisz</button>
            <button onClick={() => check.mutate()} disabled={!selectedVersionId || check.isPending} className="rounded border border-border px-3 py-1.5 text-xs disabled:opacity-40">Policy check</button>
            <button onClick={() => submit.mutate()} disabled={!selectedVersionId || submit.isPending} className="rounded border border-border px-3 py-1.5 text-xs disabled:opacity-40">Do recenzji</button>
            <button onClick={() => approve.mutate()} disabled={!selectedVersionId || approve.isPending} className="rounded border border-green-500 px-3 py-1.5 text-xs text-green-400 disabled:opacity-40">Zatwierdź</button>
          </div>
          {policyOutput && <div className="rounded border border-border p-2 text-xs text-muted-foreground">{policyOutput}</div>}
        </div>
      </div>

      {/* Diff + Sync */}
      <div className="rounded-xl border border-border bg-card p-4 space-y-3">
        <h2 className="text-sm font-semibold">Porównanie & synchronizacja <Tip text="Porównaj treść między rynkami i synchronizuj brakujące pola" /></h2>
        <div className="grid gap-2 md:grid-cols-5">
          <select value={diffMain} onChange={(e) => setDiffMain(e.target.value)} className="rounded border border-input bg-background px-2 py-1 text-xs">
            {MARKET_OPTIONS.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
          <select value={diffTarget} onChange={(e) => setDiffTarget(e.target.value)} className="rounded border border-input bg-background px-2 py-1 text-xs">
            {MARKET_OPTIONS.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
          <input value={syncTargets} onChange={(e) => setSyncTargets(e.target.value)} placeholder="Sync do: FR,IT,ES" className="rounded border border-input bg-background px-2 py-1 text-xs md:col-span-2" />
          <button onClick={() => sync.mutate()} disabled={!sku.trim() || sync.isPending} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Synchronizuj</button>
        </div>
        {sync.data && <div className="text-xs text-muted-foreground">Drafts: {sync.data.drafts_created} | Pominięte: {sync.data.skipped}</div>}
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="border-b border-border text-left text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr><th className="px-2 py-2">Pole</th><th className="px-2 py-2">Zmiana</th><th className="px-2 py-2">Źródło</th><th className="px-2 py-2">Cel</th></tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(diffQuery.data?.fields ?? []).map((row) => (
                <tr key={row.field}><td className="px-2 py-2">{row.field}</td><td className="px-2 py-2">{row.change_type}</td><td className="px-2 py-2 max-w-[300px] truncate">{JSON.stringify(row.main_value)}</td><td className="px-2 py-2 max-w-[300px] truncate">{JSON.stringify(row.target_value)}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

/* ────────────────────────────────────────────────────────────────── */
/* TAB 4: Onboard (z ContentOps)                                      */
/* ────────────────────────────────────────────────────────────────── */

function OnboardTab() {
  const qc = useQueryClient();

  /* Preflight */
  const [skuInput, setSkuInput] = useState("");
  const [mainMarket, setMainMarket] = useState("DE");
  const [targetMarketsInput, setTargetMarketsInput] = useState("FR,IT,ES,NL,PL,SE,BE");
  const [autoCreateTasks, setAutoCreateTasks] = useState(false);
  const [preflightRows, setPreflightRows] = useState<ContentOnboardPreflightItem[]>([]);

  /* Catalog / Restriction */
  const [catalogEan, setCatalogEan] = useState("");
  const [catalogMarket, setCatalogMarket] = useState("DE");
  const [restrictionAsin, setRestrictionAsin] = useState("");
  const [restrictionMarket, setRestrictionMarket] = useState("DE");
  const [restrictionResult, setRestrictionResult] = useState<{ can_list: boolean; requires_approval: boolean; reasons: string[] } | null>(null);

  /* Quick publish push */
  const [pushSelection, setPushSelection] = useState<"approved" | "draft">("approved");
  const [pushMode, setPushMode] = useState<"preview" | "confirm">("preview");
  const [pushMarkets, setPushMarkets] = useState("DE,FR,IT");
  const [pushSkuInput, setPushSkuInput] = useState("");

  const jobsQuery = useQuery({
    queryKey: ["cs-publish-jobs"],
    queryFn: () => getContentPublishJobs({ page: 1, page_size: 20 }),
  });

  const preflight = useMutation({
    mutationFn: () => {
      const skus = skuInput.split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
      const target_markets = targetMarketsInput.split(",").map((x) => x.trim().toUpperCase()).filter(Boolean);
      return runContentOnboardPreflight({ sku_list: skus, main_market: mainMarket, target_markets, auto_create_tasks: autoCreateTasks });
    },
    onSuccess: (r) => setPreflightRows(r.items ?? []),
  });

  const catalog = useMutation({ mutationFn: () => searchContentCatalogByEan(catalogEan.trim(), catalogMarket) });

  const restriction = useMutation({
    mutationFn: () => checkContentRestrictions(restrictionAsin.trim(), restrictionMarket),
    onSuccess: (r) => setRestrictionResult({ can_list: r.can_list, requires_approval: r.requires_approval, reasons: r.reasons }),
  });

  const push = useMutation({
    mutationFn: () => {
      const marketplaces = pushMarkets.split(",").map((x) => x.trim().toUpperCase()).filter(Boolean);
      const sku_filter = pushSkuInput.split(/\r?\n|,/).map((x) => x.trim()).filter(Boolean);
      return pushContentPublish({ marketplaces, selection: pushSelection, mode: pushMode, sku_filter });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["cs-publish-jobs"] }),
  });

  const preflightSummary = useMemo(() => {
    const total = preflightRows.length;
    const blocked = preflightRows.filter((r) => r.blockers.length > 0).length;
    return { total, blocked, ready: total - blocked };
  }, [preflightRows]);

  return (
    <div className="space-y-4">
      {/* Preflight */}
      <div className="rounded-xl border border-border bg-card p-4 space-y-3">
        <h2 className="text-sm font-semibold">Preflight onboardingowy <Tip text="Sprawdza czy SKU mają kompletne dane PIM, rodziny, restrykcje — zanim wystawisz na Amazon" /></h2>
        <div className="grid gap-2 md:grid-cols-4">
          <select value={mainMarket} onChange={(e) => setMainMarket(e.target.value)} className="rounded border border-input bg-background px-2 py-1 text-xs">
            {MARKET_OPTIONS.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
          <input value={targetMarketsInput} onChange={(e) => setTargetMarketsInput(e.target.value)} placeholder="Docelowe rynki: FR,IT,ES..." className="rounded border border-input bg-background px-2 py-1 text-xs md:col-span-2" />
          <label className="inline-flex items-center gap-2 text-xs text-muted-foreground">
            <input type="checkbox" checked={autoCreateTasks} onChange={(e) => setAutoCreateTasks(e.target.checked)} />
            Auto-utwórz zadania
          </label>
        </div>
        <textarea rows={3} value={skuInput} onChange={(e) => setSkuInput(e.target.value)} placeholder="Lista SKU (jeden na linię)" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
        <div className="flex gap-2">
          <button onClick={() => preflight.mutate()} disabled={preflight.isPending || !skuInput.trim()} className="rounded border border-border px-3 py-1.5 text-xs disabled:opacity-40">
            {preflight.isPending ? "Sprawdzam..." : "Uruchom preflight"}
          </button>
          {preflightRows.length > 0 && <ClientExportButton data={preflightRows} filename="preflight_results" />}
        </div>
        {preflightRows.length > 0 && (
          <>
            <div className="text-xs text-muted-foreground">Łącznie: {preflightSummary.total} | Gotowe: {preflightSummary.ready} | Zablokowane: {preflightSummary.blocked}</div>
            <div className="overflow-x-auto">
              <table className="w-full text-[11px]">
                <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
                  <tr><th className="px-2 py-2">SKU</th><th className="px-2 py-2">PIM</th><th className="px-2 py-2">Rodzina</th><th className="px-2 py-2">Blokery</th><th className="px-2 py-2">Ostrzeżenia</th></tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {preflightRows.map((r) => (
                    <tr key={r.sku} className="hover:bg-muted/20"><td className="px-2 py-1.5 font-mono">{r.sku}</td><td className="px-2 py-1.5">{r.pim_score}%</td><td className="px-2 py-1.5">{r.family_coverage_pct}%</td><td className="px-2 py-1.5">{r.blockers.length ? r.blockers.join(" | ") : "-"}</td><td className="px-2 py-1.5">{r.warnings.length ? r.warnings.join(" | ") : "-"}</td></tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>

      {/* Catalog + Restrictions side by side */}
      <div className="grid gap-4 lg:grid-cols-2">
        <div className="rounded-xl border border-border bg-card p-4 space-y-2">
          <h2 className="text-sm font-semibold">Katalog wg EAN <Tip text="Szuka produktu w katalogu Amazon po kodzie EAN" /></h2>
          <div className="flex gap-2">
            <input value={catalogEan} onChange={(e) => setCatalogEan(e.target.value)} placeholder="EAN" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
            <select value={catalogMarket} onChange={(e) => setCatalogMarket(e.target.value)} className="rounded border border-input bg-background px-2 py-1 text-xs">
              {MARKET_OPTIONS.map((m) => <option key={m} value={m}>{m}</option>)}
            </select>
            <button onClick={() => catalog.mutate()} disabled={catalog.isPending || !catalogEan.trim()} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Szukaj</button>
          </div>
          {catalog.data?.matches?.length ? (
            <div className="max-h-40 overflow-auto rounded border border-border p-2 text-xs">
              {catalog.data.matches.map((m) => (
                <div key={m.asin} className="py-1"><span className="font-mono">{m.asin}</span> | {m.product_type ?? "-"} | {m.title ?? "-"}</div>
              ))}
            </div>
          ) : catalog.data ? <div className="text-xs text-muted-foreground">Brak wyników</div> : null}
        </div>

        <div className="rounded-xl border border-border bg-card p-4 space-y-2">
          <h2 className="text-sm font-semibold">Restrykcje sprzedaży <Tip text="Sprawdza czy dany ASIN wymaga zgód lub jest zablokowany na danym rynku" /></h2>
          <div className="flex gap-2">
            <input value={restrictionAsin} onChange={(e) => setRestrictionAsin(e.target.value)} placeholder="ASIN" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
            <select value={restrictionMarket} onChange={(e) => setRestrictionMarket(e.target.value)} className="rounded border border-input bg-background px-2 py-1 text-xs">
              {MARKET_OPTIONS.map((m) => <option key={m} value={m}>{m}</option>)}
            </select>
            <button onClick={() => restriction.mutate()} disabled={restriction.isPending || !restrictionAsin.trim()} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Sprawdź</button>
          </div>
          {restrictionResult && (
            <div className="space-y-1 text-xs">
              <div className={restrictionResult.can_list ? "text-green-400" : "text-red-400"}>Można listować: {String(restrictionResult.can_list)} | Wymaga akceptacji: {String(restrictionResult.requires_approval)}</div>
              {restrictionResult.reasons.length > 0 && <div className="max-h-32 overflow-auto rounded border border-border p-2 text-muted-foreground">{restrictionResult.reasons.join(" | ")}</div>}
            </div>
          )}
        </div>
      </div>

      {/* Quick Publish Push */}
      <div className="rounded-xl border border-border bg-card p-4 space-y-3">
        <h2 className="text-sm font-semibold">Szybki publish <Tip text="Wypchnij zatwierdzone treści na Amazon — preview sprawdzi co zostanie wysłane, confirm wyśle" /></h2>
        <div className="grid gap-2 md:grid-cols-4">
          <select value={pushSelection} onChange={(e) => setPushSelection(e.target.value as "approved" | "draft")} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="approved">Zatwierdzone</option>
            <option value="draft">Drafty</option>
          </select>
          <select value={pushMode} onChange={(e) => setPushMode(e.target.value as "preview" | "confirm")} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="preview">Podgląd</option>
            <option value="confirm">Wyślij</option>
          </select>
          <input value={pushMarkets} onChange={(e) => setPushMarkets(e.target.value)} placeholder="Rynki: DE,FR,IT..." className="rounded border border-input bg-background px-2 py-1 text-xs md:col-span-2" />
        </div>
        <textarea rows={2} value={pushSkuInput} onChange={(e) => setPushSkuInput(e.target.value)} placeholder="Filtr SKU (opcjonalnie, po przecinku lub w nowej linii)" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
        <button onClick={() => push.mutate()} disabled={push.isPending} className="rounded border border-border px-3 py-1.5 text-xs disabled:opacity-40">
          {push.isPending ? "Wysyłam..." : `Uruchom ${pushMode === "preview" ? "podgląd" : "publish"}`}
        </button>
      </div>

      {/* Recent publish jobs */}
      <div className="rounded-xl border border-border bg-card p-4">
        <h2 className="mb-2 text-sm font-semibold">Ostatnie joby publish</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr><th className="px-2 py-2">Job</th><th className="px-2 py-2">Typ</th><th className="px-2 py-2">Status</th><th className="px-2 py-2">Rynki</th><th className="px-2 py-2">Kiedy</th></tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(jobsQuery.data?.items ?? []).map((job) => (
                <tr key={job.id} className="hover:bg-muted/20">
                  <td className="px-2 py-1.5 font-mono">{job.id.slice(0, 8)}</td>
                  <td className="px-2 py-1.5">{job.job_type}</td>
                  <td className="px-2 py-1.5"><span className={`rounded-full px-2 py-0.5 text-[10px] ${badgeClass(job.status)}`}>{job.status}</span></td>
                  <td className="px-2 py-1.5">{(job.marketplaces ?? []).join(", ")}</td>
                  <td className="px-2 py-1.5 text-muted-foreground">{job.created_at.slice(0, 16).replace("T", " ")}</td>
                </tr>
              ))}
              {!jobsQuery.isLoading && (jobsQuery.data?.items ?? []).length === 0 && (
                <tr><td colSpan={5} className="px-2 py-6 text-center text-muted-foreground">Brak jobów</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
