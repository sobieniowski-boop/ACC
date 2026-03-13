import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Bell,
  Briefcase,
  ListChecks,
  AlertTriangle,
  ShieldAlert,
  Clock,
  CheckCircle2,
  XCircle,
  Plus,
  ChevronRight,
  Eye,
  ThumbsUp,
  ThumbsDown,
} from "lucide-react";
import {
  getOperatorDashboard,
  getUnifiedFeed,
  getOperatorCases,
  createOperatorCase,
  updateOperatorCase,
  getActionQueue,
  approveAction,
  rejectAction,
} from "@/lib/api";
import type {
  FeedItem,
  OperatorCase,
  ActionQueueItem,
  OperatorDashboard,
} from "@/lib/api";
import { cn } from "@/lib/utils";

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const SEVERITY_STYLES: Record<string, string> = {
  critical: "bg-red-500/10 text-red-400 border-red-500/30",
  high: "bg-orange-500/10 text-orange-400 border-orange-500/30",
  warning: "bg-amber-500/10 text-amber-400 border-amber-500/30",
  medium: "bg-yellow-500/10 text-yellow-400 border-yellow-500/30",
  low: "bg-blue-500/10 text-blue-400 border-blue-500/30",
  info: "bg-blue-500/10 text-blue-400 border-blue-500/30",
};

const STATUS_STYLES: Record<string, string> = {
  open: "bg-blue-500/10 text-blue-400",
  in_progress: "bg-amber-500/10 text-amber-400",
  waiting: "bg-purple-500/10 text-purple-400",
  resolved: "bg-green-500/10 text-green-400",
  closed: "bg-zinc-500/10 text-zinc-400",
  pending_approval: "bg-amber-500/10 text-amber-400",
  approved: "bg-green-500/10 text-green-400",
  rejected: "bg-red-500/10 text-red-400",
  executed: "bg-emerald-500/10 text-emerald-400",
  failed: "bg-red-500/10 text-red-400",
  expired: "bg-zinc-500/10 text-zinc-400",
};

const PRIORITY_LABELS: Record<string, string> = {
  critical: "Krytyczny",
  high: "Wysoki",
  medium: "Sredni",
  low: "Niski",
};

const CATEGORY_LABELS: Record<string, string> = {
  refund_anomaly: "Anomalie zwrotow",
  fee_dispute: "Spor o oplaty",
  inventory_discrepancy: "Rozbieznosc inwentarza",
  listing_issue: "Problem z listingiem",
  buybox_loss: "Utrata Buy Box",
  content_quality: "Jakosc tresci",
  compliance: "Zgodnosc",
  other: "Inne",
};

const SOURCE_LABELS: Record<string, string> = {
  alert: "Alerty",
  system: "System",
  anomaly: "Anomalie",
};

/* ------------------------------------------------------------------ */
/*  KPI Cards                                                          */
/* ------------------------------------------------------------------ */

function KPICards({ data }: { data: OperatorDashboard | undefined }) {
  const cards = [
    {
      label: "Alerty",
      icon: Bell,
      value: data?.alerts.unresolved ?? 0,
      sub: `${data?.alerts.critical ?? 0} krytycznych`,
      color: "text-red-400",
    },
    {
      label: "Anomalie",
      icon: ShieldAlert,
      value: data?.anomalies.open ?? 0,
      sub: `${data?.anomalies.critical ?? 0} krytycznych`,
      color: "text-amber-400",
    },
    {
      label: "Sprawy",
      icon: Briefcase,
      value: data?.cases.open ?? 0,
      sub: `${data?.cases.critical ?? 0} krytycznych`,
      color: "text-blue-400",
    },
    {
      label: "Akcje do zatwierdzenia",
      icon: ListChecks,
      value: data?.action_queue.pending_approval ?? 0,
      sub: `${data?.action_queue.total ?? 0} lacznie`,
      color: "text-purple-400",
    },
  ];

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      {cards.map((c) => (
        <div key={c.label} className="rounded-xl border border-border bg-card p-4">
          <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
            <c.icon className={cn("h-4 w-4", c.color)} />
            {c.label}
          </div>
          <p className="text-2xl font-bold">{c.value}</p>
          <p className="text-xs text-muted-foreground">{c.sub}</p>
        </div>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Unified Feed Tab                                                   */
/* ------------------------------------------------------------------ */

function FeedPanel() {
  const [source, setSource] = useState<string>("");
  const [severity, setSeverity] = useState<string>("");
  const [page, setPage] = useState(1);

  const { data, isLoading } = useQuery({
    queryKey: ["operator-feed", source, severity, page],
    queryFn: () =>
      getUnifiedFeed({
        days: 7,
        source: source || undefined,
        severity: severity || undefined,
        page,
        page_size: 30,
      }),
  });

  return (
    <div className="space-y-4">
      <div className="flex gap-2 flex-wrap">
        <select
          value={source}
          onChange={(e) => { setSource(e.target.value); setPage(1); }}
          className="rounded-md border border-border bg-background px-3 py-1.5 text-sm"
        >
          <option value="">Wszystkie zrodla</option>
          {Object.entries(SOURCE_LABELS).map(([k, v]) => (
            <option key={k} value={k}>{v}</option>
          ))}
        </select>
        <select
          value={severity}
          onChange={(e) => { setSeverity(e.target.value); setPage(1); }}
          className="rounded-md border border-border bg-background px-3 py-1.5 text-sm"
        >
          <option value="">Wszystkie waznosci</option>
          <option value="critical">Krytyczny</option>
          <option value="warning">Ostrzezenie</option>
          <option value="high">Wysoki</option>
          <option value="medium">Sredni</option>
          <option value="low">Niski</option>
        </select>
      </div>

      {isLoading && <p className="text-sm text-muted-foreground">Ladowanie...</p>}

      <div className="space-y-2">
        {data?.items.map((item: FeedItem, idx: number) => (
          <div
            key={`${item.source}-${item.source_id}-${idx}`}
            className="flex items-start gap-3 rounded-lg border border-border p-3"
          >
            <span className={cn("mt-0.5 rounded px-2 py-0.5 text-xs font-medium border", SEVERITY_STYLES[item.severity] || SEVERITY_STYLES.low)}>
              {item.severity}
            </span>
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium truncate">{item.title}</p>
              {item.description && (
                <p className="text-xs text-muted-foreground mt-0.5 truncate">{item.description}</p>
              )}
              <div className="flex gap-3 mt-1 text-xs text-muted-foreground">
                <span className="rounded bg-muted px-1.5 py-0.5">{SOURCE_LABELS[item.source] || item.source}</span>
                {item.sku && <span>SKU: {item.sku}</span>}
                <span>{new Date(item.created_at).toLocaleDateString("pl-PL")}</span>
              </div>
            </div>
            <span className={cn("rounded px-2 py-0.5 text-xs", STATUS_STYLES[item.status] || "bg-muted text-muted-foreground")}>
              {item.status}
            </span>
          </div>
        ))}
        {data && data.items.length === 0 && (
          <p className="text-sm text-muted-foreground text-center py-8">Brak elementow w feedzie</p>
        )}
      </div>

      {data && data.total > 30 && (
        <div className="flex justify-center gap-2">
          <button onClick={() => setPage(Math.max(1, page - 1))} disabled={page === 1}
            className="rounded border px-3 py-1 text-sm disabled:opacity-50">Wstecz</button>
          <span className="text-sm py-1">Strona {page} / {Math.ceil(data.total / 30)}</span>
          <button onClick={() => setPage(page + 1)} disabled={page * 30 >= data.total}
            className="rounded border px-3 py-1 text-sm disabled:opacity-50">Dalej</button>
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Cases Tab                                                          */
/* ------------------------------------------------------------------ */

function CasesPanel() {
  const [statusFilter, setStatusFilter] = useState<string>("");
  const [priorityFilter, setPriorityFilter] = useState<string>("");
  const [showCreate, setShowCreate] = useState(false);
  const [page, setPage] = useState(1);
  const queryClient = useQueryClient();

  const { data, isLoading } = useQuery({
    queryKey: ["operator-cases", statusFilter, priorityFilter, page],
    queryFn: () =>
      getOperatorCases({
        status: statusFilter || undefined,
        priority: priorityFilter || undefined,
        page,
        page_size: 30,
      }),
  });

  const createMut = useMutation({
    mutationFn: (d: { title: string; category: string; priority: string; description?: string }) =>
      createOperatorCase(d),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["operator-cases"] });
      queryClient.invalidateQueries({ queryKey: ["operator-dashboard"] });
      setShowCreate(false);
    },
  });

  const updateMut = useMutation({
    mutationFn: ({ id, ...rest }: { id: number; status?: string; priority?: string }) =>
      updateOperatorCase(id, rest),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["operator-cases"] });
      queryClient.invalidateQueries({ queryKey: ["operator-dashboard"] });
    },
  });

  const [newTitle, setNewTitle] = useState("");
  const [newCategory, setNewCategory] = useState("other");
  const [newPriority, setNewPriority] = useState("medium");
  const [newDesc, setNewDesc] = useState("");

  return (
    <div className="space-y-4">
      <div className="flex gap-2 flex-wrap items-center">
        <select value={statusFilter} onChange={(e) => { setStatusFilter(e.target.value); setPage(1); }}
          className="rounded-md border border-border bg-background px-3 py-1.5 text-sm">
          <option value="">Wszystkie statusy</option>
          <option value="open">Otwarte</option>
          <option value="in_progress">W toku</option>
          <option value="waiting">Oczekujace</option>
          <option value="resolved">Rozwiazane</option>
          <option value="closed">Zamkniete</option>
        </select>
        <select value={priorityFilter} onChange={(e) => { setPriorityFilter(e.target.value); setPage(1); }}
          className="rounded-md border border-border bg-background px-3 py-1.5 text-sm">
          <option value="">Wszystkie priorytety</option>
          {Object.entries(PRIORITY_LABELS).map(([k, v]) => (
            <option key={k} value={k}>{v}</option>
          ))}
        </select>
        <button onClick={() => setShowCreate(!showCreate)}
          className="ml-auto flex items-center gap-1 rounded-md bg-primary px-3 py-1.5 text-sm text-primary-foreground">
          <Plus className="h-4 w-4" /> Nowa sprawa
        </button>
      </div>

      {showCreate && (
        <div className="rounded-lg border border-border p-4 space-y-3 bg-card">
          <input value={newTitle} onChange={(e) => setNewTitle(e.target.value)}
            placeholder="Tytul sprawy" className="w-full rounded border border-border bg-background px-3 py-1.5 text-sm" />
          <div className="flex gap-2">
            <select value={newCategory} onChange={(e) => setNewCategory(e.target.value)}
              className="rounded border border-border bg-background px-3 py-1.5 text-sm">
              {Object.entries(CATEGORY_LABELS).map(([k, v]) => (
                <option key={k} value={k}>{v}</option>
              ))}
            </select>
            <select value={newPriority} onChange={(e) => setNewPriority(e.target.value)}
              className="rounded border border-border bg-background px-3 py-1.5 text-sm">
              {Object.entries(PRIORITY_LABELS).map(([k, v]) => (
                <option key={k} value={k}>{v}</option>
              ))}
            </select>
          </div>
          <textarea value={newDesc} onChange={(e) => setNewDesc(e.target.value)}
            placeholder="Opis (opcjonalny)" rows={2}
            className="w-full rounded border border-border bg-background px-3 py-1.5 text-sm" />
          <div className="flex gap-2">
            <button onClick={() => { if (newTitle.trim()) createMut.mutate({ title: newTitle, category: newCategory, priority: newPriority, description: newDesc || undefined }); }}
              disabled={!newTitle.trim()} className="rounded bg-primary px-4 py-1.5 text-sm text-primary-foreground disabled:opacity-50">Utworz</button>
            <button onClick={() => setShowCreate(false)} className="rounded border px-4 py-1.5 text-sm">Anuluj</button>
          </div>
        </div>
      )}

      {isLoading && <p className="text-sm text-muted-foreground">Ladowanie...</p>}

      <div className="space-y-2">
        {data?.items.map((c: OperatorCase) => (
          <div key={c.id} className="flex items-center gap-3 rounded-lg border border-border p-3">
            <span className={cn("rounded px-2 py-0.5 text-xs font-medium border", SEVERITY_STYLES[c.priority] || SEVERITY_STYLES.medium)}>
              {PRIORITY_LABELS[c.priority] || c.priority}
            </span>
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium truncate">{c.title}</p>
              <div className="flex gap-2 mt-0.5 text-xs text-muted-foreground">
                <span>{CATEGORY_LABELS[c.category] || c.category}</span>
                {c.assigned_to && <span>→ {c.assigned_to}</span>}
                {c.sku && <span>SKU: {c.sku}</span>}
              </div>
            </div>
            <span className={cn("rounded px-2 py-0.5 text-xs", STATUS_STYLES[c.status] || "bg-muted text-muted-foreground")}>
              {c.status}
            </span>
            {c.status === "open" && (
              <button onClick={() => updateMut.mutate({ id: c.id, status: "in_progress" })}
                className="rounded border px-2 py-1 text-xs hover:bg-muted" title="Rozpocznij">
                <ChevronRight className="h-3 w-3" />
              </button>
            )}
            {c.status === "in_progress" && (
              <button onClick={() => updateMut.mutate({ id: c.id, status: "resolved" })}
                className="rounded border border-green-500/30 px-2 py-1 text-xs text-green-400 hover:bg-green-500/10" title="Rozwiaz">
                <CheckCircle2 className="h-3 w-3" />
              </button>
            )}
          </div>
        ))}
        {data && data.items.length === 0 && (
          <p className="text-sm text-muted-foreground text-center py-8">Brak spraw</p>
        )}
      </div>

      {data && data.total > 30 && (
        <div className="flex justify-center gap-2">
          <button onClick={() => setPage(Math.max(1, page - 1))} disabled={page === 1}
            className="rounded border px-3 py-1 text-sm disabled:opacity-50">Wstecz</button>
          <span className="text-sm py-1">Strona {page} / {Math.ceil(data.total / 30)}</span>
          <button onClick={() => setPage(page + 1)} disabled={page * 30 >= data.total}
            className="rounded border px-3 py-1 text-sm disabled:opacity-50">Dalej</button>
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Action Queue Tab                                                   */
/* ------------------------------------------------------------------ */

function ActionsPanel() {
  const [statusFilter, setStatusFilter] = useState<string>("pending_approval");
  const [page, setPage] = useState(1);
  const queryClient = useQueryClient();

  const { data, isLoading } = useQuery({
    queryKey: ["action-queue", statusFilter, page],
    queryFn: () =>
      getActionQueue({
        status: statusFilter || undefined,
        page,
        page_size: 30,
      }),
  });

  const approveMut = useMutation({
    mutationFn: (id: number) => approveAction(id, { approved_by: "operator" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["action-queue"] });
      queryClient.invalidateQueries({ queryKey: ["operator-dashboard"] });
    },
  });

  const rejectMut = useMutation({
    mutationFn: (id: number) => rejectAction(id, { rejected_by: "operator" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["action-queue"] });
      queryClient.invalidateQueries({ queryKey: ["operator-dashboard"] });
    },
  });

  return (
    <div className="space-y-4">
      <div className="flex gap-2 flex-wrap">
        <select value={statusFilter} onChange={(e) => { setStatusFilter(e.target.value); setPage(1); }}
          className="rounded-md border border-border bg-background px-3 py-1.5 text-sm">
          <option value="">Wszystkie statusy</option>
          <option value="pending_approval">Do zatwierdzenia</option>
          <option value="approved">Zatwierdzone</option>
          <option value="rejected">Odrzucone</option>
          <option value="executed">Wykonane</option>
          <option value="failed">Nieudane</option>
          <option value="expired">Wygasle</option>
        </select>
      </div>

      {isLoading && <p className="text-sm text-muted-foreground">Ladowanie...</p>}

      <div className="space-y-2">
        {data?.items.map((a: ActionQueueItem) => (
          <div key={a.id} className="flex items-center gap-3 rounded-lg border border-border p-3">
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium truncate">{a.title}</p>
              <div className="flex gap-2 mt-0.5 text-xs text-muted-foreground">
                <span className="rounded bg-muted px-1.5 py-0.5">{a.action_type}</span>
                <span className={cn("rounded px-1.5 py-0.5", a.risk_level === "high" ? "bg-red-500/10 text-red-400" : a.risk_level === "medium" ? "bg-amber-500/10 text-amber-400" : "bg-green-500/10 text-green-400")}>
                  {a.risk_level}
                </span>
                {a.sku && <span>SKU: {a.sku}</span>}
                <span>przez {a.requested_by}</span>
              </div>
            </div>
            <span className={cn("rounded px-2 py-0.5 text-xs", STATUS_STYLES[a.status] || "bg-muted text-muted-foreground")}>
              {a.status}
            </span>
            {a.status === "pending_approval" && (
              <div className="flex gap-1">
                <button onClick={() => approveMut.mutate(a.id)}
                  className="rounded border border-green-500/30 px-2 py-1 text-xs text-green-400 hover:bg-green-500/10" title="Zatwierdz">
                  <ThumbsUp className="h-3 w-3" />
                </button>
                <button onClick={() => rejectMut.mutate(a.id)}
                  className="rounded border border-red-500/30 px-2 py-1 text-xs text-red-400 hover:bg-red-500/10" title="Odrzuc">
                  <ThumbsDown className="h-3 w-3" />
                </button>
              </div>
            )}
          </div>
        ))}
        {data && data.items.length === 0 && (
          <p className="text-sm text-muted-foreground text-center py-8">Brak akcji w kolejce</p>
        )}
      </div>

      {data && data.total > 30 && (
        <div className="flex justify-center gap-2">
          <button onClick={() => setPage(Math.max(1, page - 1))} disabled={page === 1}
            className="rounded border px-3 py-1 text-sm disabled:opacity-50">Wstecz</button>
          <span className="text-sm py-1">Strona {page} / {Math.ceil(data.total / 30)}</span>
          <button onClick={() => setPage(page + 1)} disabled={page * 30 >= data.total}
            className="rounded border px-3 py-1 text-sm disabled:opacity-50">Dalej</button>
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Page                                                               */
/* ------------------------------------------------------------------ */

const TABS = [
  { key: "feed", label: "Feed", icon: Bell },
  { key: "cases", label: "Sprawy", icon: Briefcase },
  { key: "actions", label: "Kolejka akcji", icon: ListChecks },
] as const;

type TabKey = (typeof TABS)[number]["key"];

export default function OperatorConsole() {
  const [tab, setTab] = useState<TabKey>("feed");

  const { data: dashboard } = useQuery({
    queryKey: ["operator-dashboard"],
    queryFn: getOperatorDashboard,
    refetchInterval: 30_000,
  });

  return (
    <div className="mx-auto max-w-7xl space-y-6 p-6">
      <h1 className="text-2xl font-bold">Konsola Operatora</h1>

      <KPICards data={dashboard} />

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-border">
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={cn(
              "flex items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors",
              tab === t.key
                ? "border-primary text-primary"
                : "border-transparent text-muted-foreground hover:text-foreground"
            )}
          >
            <t.icon className="h-4 w-4" />
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === "feed" && <FeedPanel />}
      {tab === "cases" && <CasesPanel />}
      {tab === "actions" && <ActionsPanel />}
    </div>
  );
}
