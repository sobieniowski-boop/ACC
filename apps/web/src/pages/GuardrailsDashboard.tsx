import { useState, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
  Shield,
  AlertTriangle,
  SearchX,
  Activity,
  CheckCircle2,
  XCircle,
  Clock,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  RefreshCw,
  HelpCircle,
} from "lucide-react";
import {
  getGuardrailsSummary,
  getGuardrailsRunAll,
  getGuardrailsHistory,
} from "@/lib/api";
import type {
  GuardrailsSummaryResponse,
  GuardrailCheckResult,
  GuardrailsRunAllResponse,
  GuardrailHistoryPoint,
} from "@/lib/api";
import { cn } from "@/lib/utils";

/* ================================================================== */
/*  Constants                                                          */
/* ================================================================== */

const SEVERITY_OPTIONS = [
  { id: "", label: "All severities" },
  { id: "ok", label: "OK" },
  { id: "warning", label: "Warning" },
  { id: "critical", label: "Critical" },
  { id: "unknown", label: "Unknown" },
];

const DOMAIN_OPTIONS = [
  { id: "", label: "All domains" },
  { id: "freshness", label: "Pipeline Freshness" },
  { id: "financial", label: "Financial Corruption" },
  { id: "infra", label: "Infrastructure" },
  { id: "integrity", label: "Daily Integrity" },
  { id: "throttle", label: "Throttle & Jobs" },
];

const HOURS_OPTIONS = [
  { id: "1", label: "Last 1h" },
  { id: "6", label: "Last 6h" },
  { id: "24", label: "Last 24h" },
  { id: "48", label: "Last 48h" },
  { id: "168", label: "Last 7d" },
];

/** Map check_name → domain for client-side filtering */
const CHECK_DOMAIN: Record<string, string> = {
  order_sync_freshness: "freshness",
  finance_freshness: "freshness",
  inventory_freshness: "freshness",
  profitability_freshness: "freshness",
  fx_rate_freshness: "freshness",
  ads_freshness: "freshness",
  content_queue_depth: "freshness",
  unknown_fee_types: "financial",
  fee_coverage: "financial",
  profit_margin_anomalies: "financial",
  missing_fx_rates: "financial",
  duplicate_finance_txn: "financial",
  order_finance_drift: "financial",
  order_finance_totals: "financial",
  scheduler_health: "infra",
  circuit_breaker: "infra",
  rate_limit_blocks: "infra",
  inventory_integrity: "integrity",
  ads_spend: "integrity",
  shipping_costs: "integrity",
  profit_completeness: "integrity",
  spapi_throttle: "throttle",
  job_duplication: "throttle",
};

const PAGE_SIZE = 25;

/* ================================================================== */
/*  Helpers                                                            */
/* ================================================================== */

function severityBadge(severity: string | null | undefined) {
  if (!severity) return <span className="text-muted-foreground">—</span>;
  const map: Record<string, { class: string; Icon: typeof CheckCircle2 }> = {
    ok: { class: "text-emerald-400 bg-emerald-400/10", Icon: CheckCircle2 },
    warning: { class: "text-amber-400 bg-amber-400/10", Icon: AlertTriangle },
    critical: { class: "text-red-400 bg-red-400/10", Icon: XCircle },
    unknown: { class: "text-zinc-400 bg-zinc-400/10", Icon: HelpCircle },
  };
  const cfg = map[severity] ?? map.unknown!;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium capitalize",
        cfg.class,
      )}
    >
      <cfg.Icon className="h-3 w-3" />
      {severity}
    </span>
  );
}

function overallStatusBadge(status: string) {
  const map: Record<string, string> = {
    healthy: "text-emerald-400 border-emerald-400/30 bg-emerald-400/5",
    degraded: "text-amber-400 border-amber-400/30 bg-amber-400/5",
    critical: "text-red-400 border-red-400/30 bg-red-400/5",
    partial: "text-zinc-400 border-zinc-400/30 bg-zinc-400/5",
  };
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-lg border px-3 py-1 text-sm font-semibold uppercase tracking-wide",
        map[status] ?? map.partial,
      )}
    >
      <Activity className="h-4 w-4" />
      {status}
    </span>
  );
}

function formatAge(isoStr: string | null | undefined): string {
  if (!isoStr) return "—";
  const diff = Date.now() - new Date(isoStr).getTime();
  if (diff < 0) return "just now";
  const mins = Math.floor(diff / 60_000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ${mins % 60}m ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ${hrs % 24}h ago`;
}

function KpiCard({
  label,
  value,
  sub,
  icon: Icon,
  accent,
}: {
  label: string;
  value: string | number;
  sub?: string;
  icon: typeof Activity;
  accent?: string;
}) {
  return (
    <div className="rounded-xl border border-border bg-card p-4 space-y-1">
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        <Icon className={cn("h-4 w-4", accent)} />
        {label}
      </div>
      <div className="text-xl font-bold tracking-tight">{value}</div>
      {sub && <div className="text-xs text-muted-foreground">{sub}</div>}
    </div>
  );
}

function ClientPagination({
  page,
  totalPages,
  total,
  pageSize,
  onPageChange,
}: {
  page: number;
  totalPages: number;
  total: number;
  pageSize: number;
  onPageChange: (p: number) => void;
}) {
  if (totalPages <= 1) return null;
  const from = (page - 1) * pageSize + 1;
  const to = Math.min(page * pageSize, total);
  return (
    <div className="flex items-center justify-between text-sm text-muted-foreground">
      <span>
        {from}–{to} of {total}
      </span>
      <div className="flex items-center gap-1">
        <button
          onClick={() => onPageChange(page - 1)}
          disabled={page <= 1}
          className="rounded-lg border border-border px-2 py-1 hover:bg-muted/30 disabled:opacity-30 disabled:cursor-not-allowed"
        >
          <ChevronLeft className="h-4 w-4" />
        </button>
        <span className="px-2 tabular-nums">
          {page} / {totalPages}
        </span>
        <button
          onClick={() => onPageChange(page + 1)}
          disabled={page >= totalPages}
          className="rounded-lg border border-border px-2 py-1 hover:bg-muted/30 disabled:opacity-30 disabled:cursor-not-allowed"
        >
          <ChevronRight className="h-4 w-4" />
        </button>
      </div>
    </div>
  );
}

/* ================================================================== */
/*  Sparkline — tiny inline trend for the last N data points          */
/* ================================================================== */

function Sparkline({ points }: { points: GuardrailHistoryPoint[] }) {
  if (points.length < 2) return <span className="text-muted-foreground text-xs">—</span>;

  const values = points.map((p) => p.value ?? 0);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const w = 80;
  const h = 20;
  const step = w / (values.length - 1);
  const pts = values.map((v, i) => `${i * step},${h - ((v - min) / range) * h}`).join(" ");

  const lastSev = points[points.length - 1]?.severity ?? "ok";
  const color =
    lastSev === "critical" ? "#f87171" : lastSev === "warning" ? "#fbbf24" : "#34d399";

  return (
    <svg width={w} height={h} className="inline-block align-middle">
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.5" />
    </svg>
  );
}

/* ================================================================== */
/*  Page component                                                     */
/* ================================================================== */

export default function GuardrailsDashboardPage() {
  const [searchParams, setSearchParams] = useSearchParams();

  // URL-backed filters
  const severityFilter = searchParams.get("sev") || "";
  const domainFilter = searchParams.get("dom") || "";
  const nameFilter = searchParams.get("q") || "";
  const hours = parseInt(searchParams.get("hours") || "24", 10);
  const [page, setPage] = useState(1);

  const setParam = (key: string, value: string) => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (value) next.set(key, value);
      else next.delete(key);
      return next;
    });
    setPage(1);
  };

  /* ── Summary query (latest per check, last N hours) ── */
  const {
    data: summaryData,
    isLoading: summaryLoading,
    isError: summaryError,
    refetch: refetchSummary,
  } = useQuery({
    queryKey: ["guardrails-summary", hours],
    queryFn: () => getGuardrailsSummary({ hours }),
    staleTime: 30_000,
    refetchInterval: 60_000,
  });

  /* ── Full run query (live checks) — only on manual trigger ── */
  const {
    data: liveData,
    isFetching: liveRunning,
    refetch: runAll,
  } = useQuery({
    queryKey: ["guardrails-run-all"],
    queryFn: () => getGuardrailsRunAll(),
    enabled: false, // only on manual trigger
    staleTime: 0,
  });

  /* ── History queries for sparklines — fetch all check histories ── */
  const checkNames = useMemo(() => {
    if (!summaryData?.latest_per_check) return [];
    return Object.keys(summaryData.latest_per_check);
  }, [summaryData]);

  const {
    data: historyMap,
  } = useQuery({
    queryKey: ["guardrails-history-all", checkNames],
    queryFn: async () => {
      const map: Record<string, GuardrailHistoryPoint[]> = {};
      const results = await Promise.all(
        checkNames.map((name) =>
          getGuardrailsHistory({ check_name: name, days: 2 }).then((r) => ({
            name,
            history: r.history,
          })),
        ),
      );
      for (const r of results) {
        map[r.name] = r.history;
      }
      return map;
    },
    enabled: checkNames.length > 0,
    staleTime: 60_000,
  });

  /* ── Merge data: prefer summary (persisted), overlay live results ── */
  const checks: GuardrailCheckResult[] = useMemo(() => {
    const map = new Map<string, GuardrailCheckResult>();

    // Start with summary's latest_per_check
    if (summaryData?.latest_per_check) {
      for (const [name, check] of Object.entries(summaryData.latest_per_check)) {
        map.set(name, { ...check, check_name: name });
      }
    }

    // Overlay live run data if fresher
    if (liveData?.checks) {
      for (const c of liveData.checks) {
        const existing = map.get(c.check_name);
        if (
          !existing ||
          !existing.checked_at ||
          (c.checked_at && new Date(c.checked_at) > new Date(existing.checked_at))
        ) {
          map.set(c.check_name, c);
        }
      }
    }

    return Array.from(map.values());
  }, [summaryData, liveData]);

  /* ── Client-side filtering ── */
  const filteredChecks = useMemo(() => {
    let items = checks;
    if (severityFilter) {
      items = items.filter((c) => c.severity === severityFilter);
    }
    if (domainFilter) {
      items = items.filter((c) => CHECK_DOMAIN[c.check_name] === domainFilter);
    }
    if (nameFilter) {
      const q = nameFilter.toLowerCase();
      items = items.filter(
        (c) =>
          c.check_name.toLowerCase().includes(q) ||
          c.message?.toLowerCase().includes(q),
      );
    }
    return items;
  }, [checks, severityFilter, domainFilter, nameFilter]);

  const totalPages = Math.max(1, Math.ceil(filteredChecks.length / PAGE_SIZE));
  const pagedChecks = filteredChecks.slice(
    (page - 1) * PAGE_SIZE,
    page * PAGE_SIZE,
  );

  /* ── Summary counts ── */
  const summary = summaryData?.summary ?? liveData?.summary ?? {
    ok: 0,
    warning: 0,
    critical: 0,
    unknown: 0,
  };
  const overallStatus = summaryData?.status ?? liveData?.status ?? "unknown";
  const isLoading = summaryLoading;
  const isError = summaryError;

  return (
    <div className="space-y-4 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Shield className="h-6 w-6 text-amber-500" />
          <h1 className="text-2xl font-bold tracking-tight">Guardrails</h1>
          {!isLoading && overallStatusBadge(overallStatus)}
        </div>

        <button
          onClick={() => {
            runAll();
            refetchSummary();
          }}
          disabled={liveRunning}
          className={cn(
            "inline-flex items-center gap-2 rounded-lg border border-border px-3 py-1.5 text-sm font-medium transition-colors hover:bg-muted/30",
            liveRunning && "opacity-50 cursor-not-allowed",
          )}
        >
          <RefreshCw
            className={cn("h-4 w-4", liveRunning && "animate-spin")}
          />
          {liveRunning ? "Running…" : "Run all checks"}
        </button>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <KpiCard
          label="Total checks"
          value={checks.length}
          icon={Shield}
          accent="text-zinc-400"
        />
        <KpiCard
          label="OK"
          value={summary.ok}
          icon={CheckCircle2}
          accent="text-emerald-400"
        />
        <KpiCard
          label="Warning"
          value={summary.warning}
          icon={AlertTriangle}
          accent="text-amber-400"
        />
        <KpiCard
          label="Critical"
          value={summary.critical}
          icon={XCircle}
          accent="text-red-400"
        />
        <KpiCard
          label="Unknown"
          value={summary.unknown}
          icon={HelpCircle}
          accent="text-zinc-400"
        />
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-end gap-3">
        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Time window</span>
          <div className="relative">
            <select
              value={String(hours)}
              onChange={(e) => setParam("hours", e.target.value === "24" ? "" : e.target.value)}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm focus:outline-none"
            >
              {HOURS_OPTIONS.map((h) => (
                <option key={h.id} value={h.id}>
                  {h.label}
                </option>
              ))}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          </div>
        </label>

        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Severity</span>
          <div className="relative">
            <select
              value={severityFilter}
              onChange={(e) => setParam("sev", e.target.value)}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm focus:outline-none"
            >
              {SEVERITY_OPTIONS.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.label}
                </option>
              ))}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          </div>
        </label>

        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Domain</span>
          <div className="relative">
            <select
              value={domainFilter}
              onChange={(e) => setParam("dom", e.target.value)}
              className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-1.5 text-sm focus:outline-none"
            >
              {DOMAIN_OPTIONS.map((d) => (
                <option key={d.id} value={d.id}>
                  {d.label}
                </option>
              ))}
            </select>
            <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          </div>
        </label>

        <label className="space-y-1">
          <span className="text-xs text-muted-foreground">Search</span>
          <input
            type="text"
            value={nameFilter}
            onChange={(e) => setParam("q", e.target.value)}
            placeholder="Check name or message…"
            className="block rounded-lg border border-border bg-card px-3 py-1.5 text-sm w-56"
          />
        </label>
      </div>

      {/* Error state */}
      {isError && (
        <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-4 flex items-center gap-3">
          <AlertTriangle className="h-5 w-5 text-destructive shrink-0" />
          <p className="text-sm text-destructive">
            Failed to load guardrails data. Please try again later.
          </p>
        </div>
      )}

      {/* Checks table */}
      <div className="rounded-xl border border-border bg-card overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-xs text-muted-foreground uppercase">
              <th className="px-4 py-2 text-left">Check</th>
              <th className="px-4 py-2 text-left">Domain</th>
              <th className="px-4 py-2 text-left">Status</th>
              <th className="px-4 py-2 text-right">Value</th>
              <th className="px-4 py-2 text-right">Threshold</th>
              <th className="px-4 py-2 text-right">Time (ms)</th>
              <th className="px-4 py-2 text-left">Last run</th>
              <th className="px-4 py-2 text-center">Trend (48h)</th>
              <th className="px-4 py-2 text-left">Message</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 10 }).map((_, i) => (
                <tr key={i} className="border-b border-border/50">
                  <td colSpan={9} className="px-4 py-3">
                    <div className="h-4 bg-muted/30 rounded animate-pulse" />
                  </td>
                </tr>
              ))
            ) : pagedChecks.length === 0 ? (
              <tr>
                <td
                  colSpan={9}
                  className="px-4 py-12 text-center text-muted-foreground"
                >
                  <SearchX className="mx-auto h-8 w-8 mb-2 opacity-30" />
                  No checks match the selected filters.
                </td>
              </tr>
            ) : (
              pagedChecks.map((check) => (
                <CheckRow
                  key={check.check_name}
                  check={check}
                  history={historyMap?.[check.check_name]}
                />
              ))
            )}
          </tbody>
        </table>
      </div>

      <ClientPagination
        page={page}
        totalPages={totalPages}
        total={filteredChecks.length}
        pageSize={PAGE_SIZE}
        onPageChange={setPage}
      />

      {/* Live run timing */}
      {liveData && (
        <div className="text-xs text-muted-foreground">
          <Clock className="inline h-3 w-3 mr-1" />
          Full run completed in {liveData.elapsed_ms.toFixed(0)}ms —{" "}
          {liveData.total_checks} checks executed
        </div>
      )}
    </div>
  );
}

/* ================================================================== */
/*  Row component                                                      */
/* ================================================================== */

function CheckRow({
  check,
  history,
}: {
  check: GuardrailCheckResult;
  history?: GuardrailHistoryPoint[];
}) {
  const domain = CHECK_DOMAIN[check.check_name] ?? "—";
  const domainLabel: Record<string, string> = {
    freshness: "Freshness",
    financial: "Financial",
    infra: "Infrastructure",
    integrity: "Integrity",
    throttle: "Throttle",
  };

  return (
    <tr className="border-b border-border/50 hover:bg-muted/20">
      <td className="px-4 py-2 font-mono text-xs">{check.check_name}</td>
      <td className="px-4 py-2 text-xs text-muted-foreground">
        {domainLabel[domain] ?? domain}
      </td>
      <td className="px-4 py-2">{severityBadge(check.severity)}</td>
      <td className="px-4 py-2 text-right tabular-nums">
        {check.value != null ? check.value.toLocaleString() : "—"}
      </td>
      <td className="px-4 py-2 text-right tabular-nums text-muted-foreground">
        {check.threshold != null ? check.threshold.toLocaleString() : "—"}
      </td>
      <td className="px-4 py-2 text-right tabular-nums text-muted-foreground">
        {check.elapsed_ms != null ? check.elapsed_ms.toFixed(0) : "—"}
      </td>
      <td className="px-4 py-2 text-xs text-muted-foreground">
        {formatAge(check.checked_at)}
      </td>
      <td className="px-4 py-2 text-center">
        {history ? <Sparkline points={history} /> : "—"}
      </td>
      <td className="px-4 py-2 text-xs truncate max-w-[280px]" title={check.message ?? ""}>
        {check.message || "—"}
      </td>
    </tr>
  );
}
