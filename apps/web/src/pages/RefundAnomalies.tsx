import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle,
  Users,
  FileCheck,
  DollarSign,
  TrendingUp,
  ShieldAlert,
  Play,
  CheckCircle2,
  Clock,
  XCircle,
  Eye,
  Ban,
  Download,
  BarChart3,
} from "lucide-react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import {
  getAnomalyDashboard,
  getRefundAnomalies,
  getSerialReturners,
  getReimbursementCases,
  updateAnomalyStatus,
  updateReturnerStatus,
  updateCaseStatus,
  triggerAnomalyScan,
  getAnomalyTrends,
  exportAnomaliesCsv,
  exportReturnersCsv,
  exportCasesCsv,
} from "@/lib/api";
import type {
  RefundAnomaly,
  SerialReturner,
  ReimbursementCase,
  AnomalyDashboard,
  AnomalyTrendPoint,
} from "@/lib/api";
import { cn } from "@/lib/utils";

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const MARKETPLACE_OPTIONS = [
  { id: "", label: "All markets" },
  { id: "A1PA6795UKMFR9", label: "🇩🇪 DE" },
  { id: "A13V1IB3VIYZZH", label: "🇫🇷 FR" },
  { id: "APJ6JRA9NG5V4", label: "🇮🇹 IT" },
  { id: "A1RKKUPIHCS9HS", label: "🇪🇸 ES" },
  { id: "A1C3SOZRARQ6R3", label: "🇵🇱 PL" },
  { id: "A1805IZSGTT6HS", label: "🇳🇱 NL" },
  { id: "A2NODRKZP88ZB9", label: "🇸🇪 SE" },
  { id: "AMEN7PMS3EDWL", label: "🇧🇪 BE" },
  { id: "A28R8C7NBKEWEA", label: "🇮🇪 IE" },
];

const TABS = [
  { id: "anomalies", label: "Refund Anomalies", icon: AlertTriangle },
  { id: "returners", label: "Serial Returners", icon: Users },
  { id: "reimbursements", label: "Reimbursements", icon: FileCheck },
] as const;

type TabId = (typeof TABS)[number]["id"];

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function formatPLN(v: number | null | undefined) {
  if (v == null) return "—";
  return `${v.toLocaleString("pl-PL", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} PLN`;
}

function severityBadge(severity: string) {
  const map: Record<string, string> = {
    critical: "text-red-400 bg-red-400/10",
    high: "text-orange-400 bg-orange-400/10",
    medium: "text-amber-400 bg-amber-400/10",
    low: "text-slate-400 bg-slate-400/10",
  };
  return (
    <span className={cn("inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium", map[severity] ?? "text-muted-foreground bg-muted/20")}>
      <ShieldAlert className="h-3 w-3" />
      {severity}
    </span>
  );
}

function statusBadge(status: string) {
  const map: Record<string, { cls: string; icon: typeof CheckCircle2 }> = {
    open: { cls: "text-amber-400 bg-amber-400/10", icon: Clock },
    investigating: { cls: "text-blue-400 bg-blue-400/10", icon: Eye },
    resolved: { cls: "text-green-400 bg-green-400/10", icon: CheckCircle2 },
    dismissed: { cls: "text-slate-400 bg-slate-400/10", icon: XCircle },
    flagged: { cls: "text-red-400 bg-red-400/10", icon: AlertTriangle },
    monitoring: { cls: "text-blue-400 bg-blue-400/10", icon: Eye },
    cleared: { cls: "text-green-400 bg-green-400/10", icon: CheckCircle2 },
    blocked: { cls: "text-slate-500 bg-slate-500/10", icon: Ban },
    identified: { cls: "text-amber-400 bg-amber-400/10", icon: Clock },
    filed: { cls: "text-blue-400 bg-blue-400/10", icon: FileCheck },
    accepted: { cls: "text-green-400 bg-green-400/10", icon: CheckCircle2 },
    rejected: { cls: "text-red-400 bg-red-400/10", icon: XCircle },
    paid: { cls: "text-emerald-400 bg-emerald-400/10", icon: DollarSign },
  };
  const m = map[status] ?? { cls: "text-muted-foreground bg-muted/20", icon: Clock };
  const Icon = m.icon;
  return (
    <span className={cn("inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium", m.cls)}>
      <Icon className="h-3 w-3" />
      {status}
    </span>
  );
}

function KpiCard({ label, value, sub, icon: Icon, accent }: {
  label: string; value: string; sub?: string; icon: typeof DollarSign; accent?: string;
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

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

const TREND_COLORS: Record<string, string> = {
  refund_spike: "#ef4444",
  fee_spike: "#f59e0b",
  return_rate_spike: "#3b82f6",
};

/* ------------------------------------------------------------------ */
/*  Trends Chart                                                       */
/* ------------------------------------------------------------------ */

function TrendsChart({ marketplace }: { marketplace: string }) {
  const { data } = useQuery<AnomalyTrendPoint[]>({
    queryKey: ["anomaly-trends", marketplace],
    queryFn: () => getAnomalyTrends({ days: 90, marketplace_id: marketplace || undefined }),
  });

  if (!data?.length) {
    return (
      <div className="rounded-xl border border-border bg-card p-6 text-center text-sm text-muted-foreground">
        <BarChart3 className="mx-auto h-8 w-8 mb-2 opacity-40" />
        No trend data available yet.
      </div>
    );
  }

  // Pivot: group by week_start, each anomaly_type becomes a series
  const weekMap = new Map<string, Record<string, number>>();
  for (const pt of data) {
    const wk = pt.week_start ?? "unknown";
    if (!weekMap.has(wk)) weekMap.set(wk, { week: 0 });
    const entry = weekMap.get(wk)!;
    entry[pt.anomaly_type] = pt.count;
  }
  const chartData = Array.from(weekMap.entries()).map(([wk, v]) => ({
    week: wk.slice(0, 10),
    ...v,
  }));

  const types = [...new Set(data.map((d) => d.anomaly_type))];

  return (
    <div className="rounded-xl border border-border bg-card p-4 space-y-2">
      <h3 className="text-sm font-semibold">Anomaly Trends (last 90 days)</h3>
      <ResponsiveContainer width="100%" height={240}>
        <LineChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
          <XAxis dataKey="week" tick={{ fontSize: 11 }} stroke="hsl(var(--muted-foreground))" />
          <YAxis allowDecimals={false} tick={{ fontSize: 11 }} stroke="hsl(var(--muted-foreground))" />
          <Tooltip contentStyle={{ background: "hsl(var(--card))", border: "1px solid hsl(var(--border))", borderRadius: 8, fontSize: 12 }} />
          <Legend wrapperStyle={{ fontSize: 12 }} />
          {types.map((t) => (
            <Line
              key={t}
              type="monotone"
              dataKey={t}
              name={t.replace(/_/g, " ")}
              stroke={TREND_COLORS[t] ?? "#94a3b8"}
              strokeWidth={2}
              dot={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Page component                                                     */
/* ------------------------------------------------------------------ */

export default function RefundAnomaliesPage() {
  const [tab, setTab] = useState<TabId>("anomalies");
  const [marketplace, setMarketplace] = useState("");
  const qc = useQueryClient();

  // Dashboard KPIs
  const { data: dashboard } = useQuery<AnomalyDashboard>({
    queryKey: ["anomaly-dashboard"],
    queryFn: getAnomalyDashboard,
    refetchInterval: 60_000,
  });

  // Scan mutation
  const scanMut = useMutation({
    mutationFn: () => triggerAnomalyScan(marketplace ? { marketplace_id: marketplace } : undefined),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["anomaly-dashboard"] });
      qc.invalidateQueries({ queryKey: ["refund-anomalies"] });
      qc.invalidateQueries({ queryKey: ["serial-returners"] });
      qc.invalidateQueries({ queryKey: ["reimbursement-cases"] });
    },
  });

  const a = dashboard?.anomalies;
  const r = dashboard?.serial_returners;
  const c = dashboard?.reimbursements;

  return (
    <div className="space-y-6 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Refund Anomaly Engine</h1>
          <p className="text-sm text-muted-foreground">
            Spike detection · Serial returners · Reimbursement claims
          </p>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={marketplace}
            onChange={(e) => setMarketplace(e.target.value)}
            className="rounded-lg border border-border bg-background px-3 py-1.5 text-sm"
          >
            {MARKETPLACE_OPTIONS.map((m) => (
              <option key={m.id} value={m.id}>{m.label}</option>
            ))}
          </select>
          <button
            onClick={() => scanMut.mutate()}
            disabled={scanMut.isPending}
            className="inline-flex items-center gap-2 rounded-lg bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
          >
            <Play className="h-4 w-4" />
            {scanMut.isPending ? "Scanning…" : "Run Scan"}
          </button>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-6">
        <KpiCard icon={AlertTriangle} accent="text-red-400" label="Open Anomalies" value={String(a?.open ?? 0)} sub={`${a?.critical_open ?? 0} critical`} />
        <KpiCard icon={DollarSign} accent="text-amber-400" label="Est. Loss (Open)" value={formatPLN(a?.open_estimated_loss_pln)} />
        <KpiCard icon={Users} accent="text-orange-400" label="Active Returners" value={String(r?.total_active ?? 0)} sub={`${r?.critical ?? 0} critical`} />
        <KpiCard icon={TrendingUp} accent="text-blue-400" label="Refund Exposure" value={formatPLN(r?.total_refund_exposure_pln)} />
        <KpiCard icon={FileCheck} accent="text-green-400" label="Pending Claims" value={String(c?.pending ?? 0)} sub={`${c?.filed ?? 0} filed`} />
        <KpiCard icon={DollarSign} accent="text-emerald-400" label="Reimbursed" value={formatPLN(c?.total_reimbursed_pln)} sub={`of ${formatPLN(c?.total_estimated_value_pln)}`} />
      </div>

      {/* Trend chart */}
      <TrendsChart marketplace={marketplace} />

      {/* Tabs */}
      <div className="flex gap-1 border-b border-border">
        {TABS.map((t) => {
          const Icon = t.icon;
          return (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={cn(
                "inline-flex items-center gap-2 border-b-2 px-4 py-2 text-sm font-medium transition-colors",
                tab === t.id
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:text-foreground"
              )}
            >
              <Icon className="h-4 w-4" />
              {t.label}
            </button>
          );
        })}
      </div>

      {/* Tab panels */}
      {tab === "anomalies" && <AnomaliesPanel marketplace={marketplace} />}
      {tab === "returners" && <ReturnersPanel marketplace={marketplace} />}
      {tab === "reimbursements" && <ReimbursementsPanel marketplace={marketplace} />}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Anomalies Panel                                                    */
/* ------------------------------------------------------------------ */

function AnomaliesPanel({ marketplace }: { marketplace: string }) {
  const [severity, setSeverity] = useState("");
  const [status, setStatus] = useState("");
  const qc = useQueryClient();

  const { data, isLoading } = useQuery({
    queryKey: ["refund-anomalies", marketplace, severity, status],
    queryFn: () =>
      getRefundAnomalies({
        marketplace_id: marketplace || undefined,
        severity: severity || undefined,
        status: status || undefined,
        limit: 50,
      }),
  });

  const updateMut = useMutation({
    mutationFn: ({ id, status: s }: { id: number; status: string }) =>
      updateAnomalyStatus(id, { status: s }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["refund-anomalies"] });
      qc.invalidateQueries({ queryKey: ["anomaly-dashboard"] });
    },
  });

  const handleExport = async () => {
    const blob = await exportAnomaliesCsv({
      anomaly_type: undefined,
      severity: severity || undefined,
      status: status || undefined,
      marketplace_id: marketplace || undefined,
    });
    downloadBlob(blob, "refund_anomalies.csv");
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <select value={severity} onChange={(e) => setSeverity(e.target.value)} className="rounded-lg border border-border bg-background px-3 py-1.5 text-sm">
          <option value="">All severities</option>
          <option value="critical">Critical</option>
          <option value="high">High</option>
          <option value="medium">Medium</option>
        </select>
        <select value={status} onChange={(e) => setStatus(e.target.value)} className="rounded-lg border border-border bg-background px-3 py-1.5 text-sm">
          <option value="">All statuses</option>
          <option value="open">Open</option>
          <option value="investigating">Investigating</option>
          <option value="resolved">Resolved</option>
          <option value="dismissed">Dismissed</option>
        </select>
        <button onClick={handleExport} className="ml-auto inline-flex items-center gap-1.5 rounded-lg border border-border px-3 py-1.5 text-xs hover:bg-muted/50">
          <Download className="h-3.5 w-3.5" /> Export CSV
        </button>
      </div>
      {isLoading ? (
        <div className="text-center py-8 text-muted-foreground">Loading anomalies...</div>
      ) : !data?.items?.length ? (
        <div className="text-center py-8 text-muted-foreground">No anomalies found. Run a scan to detect refund spikes.</div>
      ) : (
        <div className="overflow-x-auto rounded-xl border border-border">
          <table className="w-full text-sm">
            <thead className="bg-muted/50 text-xs uppercase text-muted-foreground">
              <tr>
                <th className="px-4 py-3 text-left">SKU</th>
                <th className="px-4 py-3 text-left">Type</th>
                <th className="px-4 py-3 text-center">Spike Ratio</th>
                <th className="px-4 py-3 text-center">Refunds / Orders</th>
                <th className="px-4 py-3 text-right">Refund Amount</th>
                <th className="px-4 py-3 text-right">Est. Loss</th>
                <th className="px-4 py-3 text-center">Severity</th>
                <th className="px-4 py-3 text-center">Status</th>
                <th className="px-4 py-3 text-center">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {data.items.map((a: RefundAnomaly) => (
                <tr key={a.id} className="hover:bg-muted/30 transition-colors">
                  <td className="px-4 py-3 font-mono text-xs">{a.sku}</td>
                  <td className="px-4 py-3 text-xs">{a.anomaly_type.replace(/_/g, " ")}</td>
                  <td className="px-4 py-3 text-center font-bold">{a.spike_ratio}×</td>
                  <td className="px-4 py-3 text-center">{a.refund_count} / {a.order_count}</td>
                  <td className="px-4 py-3 text-right">{formatPLN(a.refund_amount_pln)}</td>
                  <td className="px-4 py-3 text-right">{formatPLN(a.estimated_loss_pln)}</td>
                  <td className="px-4 py-3 text-center">{severityBadge(a.severity)}</td>
                  <td className="px-4 py-3 text-center">{statusBadge(a.status)}</td>
                  <td className="px-4 py-3 text-center">
                    {a.status === "open" && (
                      <div className="flex gap-1 justify-center">
                        <button onClick={() => updateMut.mutate({ id: a.id, status: "investigating" })} className="rounded px-2 py-1 text-xs bg-blue-500/10 text-blue-400 hover:bg-blue-500/20">Investigate</button>
                        <button onClick={() => updateMut.mutate({ id: a.id, status: "dismissed" })} className="rounded px-2 py-1 text-xs bg-slate-500/10 text-slate-400 hover:bg-slate-500/20">Dismiss</button>
                      </div>
                    )}
                    {a.status === "investigating" && (
                      <button onClick={() => updateMut.mutate({ id: a.id, status: "resolved" })} className="rounded px-2 py-1 text-xs bg-green-500/10 text-green-400 hover:bg-green-500/20">Resolve</button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="text-xs text-muted-foreground text-right">{data?.total ?? 0} anomalies total</div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Serial Returners Panel                                             */
/* ------------------------------------------------------------------ */

function ReturnersPanel({ marketplace }: { marketplace: string }) {
  const [riskTier, setRiskTier] = useState("");
  const [status, setStatus] = useState("");
  const qc = useQueryClient();

  const { data, isLoading } = useQuery({
    queryKey: ["serial-returners", marketplace, riskTier, status],
    queryFn: () =>
      getSerialReturners({
        marketplace_id: marketplace || undefined,
        risk_tier: riskTier || undefined,
        status: status || undefined,
        limit: 50,
      }),
  });

  const updateMut = useMutation({
    mutationFn: ({ id, status: s }: { id: number; status: string }) =>
      updateReturnerStatus(id, { status: s }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["serial-returners"] });
      qc.invalidateQueries({ queryKey: ["anomaly-dashboard"] });
    },
  });

  const handleExport = async () => {
    const blob = await exportReturnersCsv({
      risk_tier: riskTier || undefined,
      status: status || undefined,
      marketplace_id: marketplace || undefined,
    });
    downloadBlob(blob, "serial_returners.csv");
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <select value={riskTier} onChange={(e) => setRiskTier(e.target.value)} className="rounded-lg border border-border bg-background px-3 py-1.5 text-sm">
          <option value="">All risk tiers</option>
          <option value="critical">Critical</option>
          <option value="high">High</option>
          <option value="medium">Medium</option>
          <option value="low">Low</option>
        </select>
        <select value={status} onChange={(e) => setStatus(e.target.value)} className="rounded-lg border border-border bg-background px-3 py-1.5 text-sm">
          <option value="">All statuses</option>
          <option value="flagged">Flagged</option>
          <option value="monitoring">Monitoring</option>
          <option value="cleared">Cleared</option>
          <option value="blocked">Blocked</option>
        </select>
        <button onClick={handleExport} className="ml-auto inline-flex items-center gap-1.5 rounded-lg border border-border px-3 py-1.5 text-xs hover:bg-muted/50">
          <Download className="h-3.5 w-3.5" /> Export CSV
        </button>
      </div>
      {isLoading ? (
        <div className="text-center py-8 text-muted-foreground">Loading serial returners...</div>
      ) : !data?.items?.length ? (
        <div className="text-center py-8 text-muted-foreground">No serial returners detected. Run a scan to analyze return patterns.</div>
      ) : (
        <div className="overflow-x-auto rounded-xl border border-border">
          <table className="w-full text-sm">
            <thead className="bg-muted/50 text-xs uppercase text-muted-foreground">
              <tr>
                <th className="px-4 py-3 text-left">Buyer Pattern</th>
                <th className="px-4 py-3 text-center">Returns / Orders</th>
                <th className="px-4 py-3 text-center">Return Rate</th>
                <th className="px-4 py-3 text-right">Total Refund</th>
                <th className="px-4 py-3 text-right">Avg Refund</th>
                <th className="px-4 py-3 text-center">Risk Score</th>
                <th className="px-4 py-3 text-center">Risk Tier</th>
                <th className="px-4 py-3 text-center">Status</th>
                <th className="px-4 py-3 text-center">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {data.items.map((r: SerialReturner) => (
                <tr key={r.id} className="hover:bg-muted/30 transition-colors">
                  <td className="px-4 py-3 font-mono text-xs max-w-[200px] truncate" title={r.buyer_identifier}>{r.buyer_identifier}</td>
                  <td className="px-4 py-3 text-center">{r.return_count} / {r.order_count}</td>
                  <td className="px-4 py-3 text-center font-bold">{(r.return_rate * 100).toFixed(1)}%</td>
                  <td className="px-4 py-3 text-right">{formatPLN(r.total_refund_pln)}</td>
                  <td className="px-4 py-3 text-right">{formatPLN(r.avg_refund_pln)}</td>
                  <td className="px-4 py-3 text-center">
                    <span className="font-bold">{r.risk_score}</span>
                    <span className="text-muted-foreground">/100</span>
                  </td>
                  <td className="px-4 py-3 text-center">{severityBadge(r.risk_tier)}</td>
                  <td className="px-4 py-3 text-center">{statusBadge(r.status)}</td>
                  <td className="px-4 py-3 text-center">
                    {r.status === "flagged" && (
                      <div className="flex gap-1 justify-center">
                        <button onClick={() => updateMut.mutate({ id: r.id, status: "monitoring" })} className="rounded px-2 py-1 text-xs bg-blue-500/10 text-blue-400 hover:bg-blue-500/20">Monitor</button>
                        <button onClick={() => updateMut.mutate({ id: r.id, status: "blocked" })} className="rounded px-2 py-1 text-xs bg-red-500/10 text-red-400 hover:bg-red-500/20">Block</button>
                      </div>
                    )}
                    {r.status === "monitoring" && (
                      <button onClick={() => updateMut.mutate({ id: r.id, status: "cleared" })} className="rounded px-2 py-1 text-xs bg-green-500/10 text-green-400 hover:bg-green-500/20">Clear</button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="text-xs text-muted-foreground text-right">{data?.total ?? 0} serial returners total</div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Reimbursements Panel                                               */
/* ------------------------------------------------------------------ */

function ReimbursementsPanel({ marketplace }: { marketplace: string }) {
  const [caseType, setCaseType] = useState("");
  const [status, setStatus] = useState("");
  const [filingCase, setFilingCase] = useState<ReimbursementCase | null>(null);
  const [amazonCaseId, setAmazonCaseId] = useState("");
  const qc = useQueryClient();

  const { data, isLoading } = useQuery({
    queryKey: ["reimbursement-cases", marketplace, caseType, status],
    queryFn: () =>
      getReimbursementCases({
        marketplace_id: marketplace || undefined,
        case_type: caseType || undefined,
        status: status || undefined,
        limit: 50,
      }),
  });

  const updateMut = useMutation({
    mutationFn: ({ id, status: s, amazon_case_id }: { id: number; status: string; amazon_case_id?: string }) =>
      updateCaseStatus(id, { status: s, amazon_case_id }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["reimbursement-cases"] });
      qc.invalidateQueries({ queryKey: ["anomaly-dashboard"] });
      setFilingCase(null);
      setAmazonCaseId("");
    },
  });

  const handleExport = async () => {
    const blob = await exportCasesCsv({
      case_type: caseType || undefined,
      status: status || undefined,
      marketplace_id: marketplace || undefined,
    });
    downloadBlob(blob, "reimbursement_cases.csv");
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <select value={caseType} onChange={(e) => setCaseType(e.target.value)} className="rounded-lg border border-border bg-background px-3 py-1.5 text-sm">
          <option value="">All case types</option>
          <option value="lost_inventory">Lost Inventory</option>
          <option value="damaged_inbound">Damaged Inbound</option>
          <option value="fee_overcharge">Fee Overcharge</option>
          <option value="customer_return_not_received">Return Not Received</option>
        </select>
        <select value={status} onChange={(e) => setStatus(e.target.value)} className="rounded-lg border border-border bg-background px-3 py-1.5 text-sm">
          <option value="">All statuses</option>
          <option value="identified">Identified</option>
          <option value="filed">Filed</option>
          <option value="accepted">Accepted</option>
          <option value="rejected">Rejected</option>
          <option value="paid">Paid</option>
        </select>
        <button onClick={handleExport} className="ml-auto inline-flex items-center gap-1.5 rounded-lg border border-border px-3 py-1.5 text-xs hover:bg-muted/50">
          <Download className="h-3.5 w-3.5" /> Export CSV
        </button>
      </div>

      {/* Filing modal */}
      {filingCase && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="w-full max-w-md rounded-xl border border-border bg-card p-6 shadow-lg space-y-4">
            <h3 className="text-lg font-semibold">File Reimbursement Claim</h3>
            <p className="text-sm text-muted-foreground">
              SKU: <span className="font-mono">{filingCase.sku}</span> · {filingCase.case_type.replace(/_/g, " ")}
            </p>
            <div className="space-y-2">
              <label className="block text-sm font-medium">Amazon Case ID</label>
              <input
                type="text"
                value={amazonCaseId}
                onChange={(e) => setAmazonCaseId(e.target.value)}
                placeholder="e.g. 1234567890"
                className="w-full rounded-lg border border-border bg-background px-3 py-2 text-sm"
              />
            </div>
            <div className="flex justify-end gap-2">
              <button
                onClick={() => { setFilingCase(null); setAmazonCaseId(""); }}
                className="rounded-lg border border-border px-4 py-2 text-sm hover:bg-muted/50"
              >
                Cancel
              </button>
              <button
                onClick={() => updateMut.mutate({ id: filingCase.id, status: "filed", amazon_case_id: amazonCaseId || undefined })}
                disabled={updateMut.isPending}
                className="rounded-lg bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              >
                {updateMut.isPending ? "Filing…" : "File Claim"}
              </button>
            </div>
          </div>
        </div>
      )}
      {isLoading ? (
        <div className="text-center py-8 text-muted-foreground">Loading reimbursement cases...</div>
      ) : !data?.items?.length ? (
        <div className="text-center py-8 text-muted-foreground">No reimbursement cases found. Run a scan to detect eligible items.</div>
      ) : (
        <div className="overflow-x-auto rounded-xl border border-border">
          <table className="w-full text-sm">
            <thead className="bg-muted/50 text-xs uppercase text-muted-foreground">
              <tr>
                <th className="px-4 py-3 text-left">SKU</th>
                <th className="px-4 py-3 text-left">Case Type</th>
                <th className="px-4 py-3 text-left">Order ID</th>
                <th className="px-4 py-3 text-center">Qty</th>
                <th className="px-4 py-3 text-right">Est. Value</th>
                <th className="px-4 py-3 text-right">Reimbursed</th>
                <th className="px-4 py-3 text-center">Status</th>
                <th className="px-4 py-3 text-center">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {data.items.map((c: ReimbursementCase) => (
                <tr key={c.id} className="hover:bg-muted/30 transition-colors">
                  <td className="px-4 py-3 font-mono text-xs">{c.sku}</td>
                  <td className="px-4 py-3 text-xs">{c.case_type.replace(/_/g, " ")}</td>
                  <td className="px-4 py-3 font-mono text-xs">{c.amazon_order_id ?? "—"}</td>
                  <td className="px-4 py-3 text-center">{c.quantity}</td>
                  <td className="px-4 py-3 text-right">{formatPLN(c.estimated_value_pln)}</td>
                  <td className="px-4 py-3 text-right">{c.reimbursed_amount_pln ? formatPLN(c.reimbursed_amount_pln) : "—"}</td>
                  <td className="px-4 py-3 text-center">{statusBadge(c.status)}</td>
                  <td className="px-4 py-3 text-center">
                    {c.status === "identified" && (
                      <button onClick={() => setFilingCase(c)} className="rounded px-2 py-1 text-xs bg-blue-500/10 text-blue-400 hover:bg-blue-500/20">File Claim</button>
                    )}
                    {c.status === "filed" && (
                      <div className="flex gap-1 justify-center">
                        <button onClick={() => updateMut.mutate({ id: c.id, status: "accepted" })} className="rounded px-2 py-1 text-xs bg-green-500/10 text-green-400 hover:bg-green-500/20">Accepted</button>
                        <button onClick={() => updateMut.mutate({ id: c.id, status: "rejected" })} className="rounded px-2 py-1 text-xs bg-red-500/10 text-red-400 hover:bg-red-500/20">Rejected</button>
                      </div>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="text-xs text-muted-foreground text-right">{data?.total ?? 0} reimbursement cases total</div>
    </div>
  );
}
