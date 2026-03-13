import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { format } from "date-fns";
import { Bell, CheckCircle, ExternalLink, Eye, Plus, Settings, Trash2 } from "lucide-react";
import { Link } from "react-router-dom";
import {
  createAlertRule,
  deleteAlertRule,
  getAlertRules,
  getAlerts,
  getContentPublishQueueHealth,
  markAlertRead,
  resolveAlert,
} from "@/lib/api";
import type { AlertRuleCreate } from "@/lib/api";
import { cn } from "@/lib/utils";
import { ClientExportButton } from "@/components/shared";

const SEVERITY_CLASSES = {
  critical: "bg-destructive/10 text-destructive border-destructive/30",
  warning: "bg-amber-500/10 text-amber-500 border-amber-500/30",
  info: "bg-blue-500/10 text-blue-500 border-blue-500/30",
};

const SEVERITY_LABELS: Record<string, string> = {
  critical: "Krytyczny",
  warning: "Ostrzezenie",
  info: "Info",
};

const HEALTH_BADGE = {
  green: "bg-green-500/10 text-green-400 border-green-500/30",
  yellow: "bg-amber-500/10 text-amber-400 border-amber-500/30",
  red: "bg-destructive/10 text-destructive border-destructive/30",
};

const RULE_TYPES = [
  { value: "margin_below", label: "Marza ponizej progu" },
  { value: "cogs_missing", label: "Brak COGS" },
  { value: "stock_low", label: "Niski stan" },
  { value: "price_change", label: "Zmiana ceny" },
  { value: "acos_above", label: "ACoS powyzej progu" },
  { value: "buybox_lost", label: "Utrata Buy Box" },
];

const OPERATORS = [
  { value: "lt", label: "<" },
  { value: "lte", label: "<=" },
  { value: "gt", label: ">" },
  { value: "gte", label: ">=" },
  { value: "eq", label: "=" },
];

function metricSeverity(value: number, warning: number, critical: number): "green" | "yellow" | "red" {
  if (value >= critical) return "red";
  if (value >= warning) return "yellow";
  return "green";
}

function buildAlertHref(alert: { context_json?: Record<string, unknown> }) {
  const route = typeof alert.context_json?.route === "string" ? alert.context_json.route : "";
  if (!route) return null;
  const query = alert.context_json?.query;
  if (!query || typeof query !== "object" || Array.isArray(query)) return route;
  const params = new URLSearchParams();
  Object.entries(query).forEach(([key, value]) => {
    if (value === null || value === undefined || value === "") return;
    params.set(key, String(value));
  });
  const suffix = params.toString();
  return suffix ? `${route}?${suffix}` : route;
}

function prettifyAlertTitle(title: string) {
  return title
    .replace("Stockout Top SKU", "Brak zapasu Top SKU")
    .replace("Receiving Variance", "Rozbieznosc przyjecia")
    .replace("Inbound Stuck", "Inbound zablokowany")
    .replace("Stranded Value Spike", "Wzrost stranded")
    .replace("Aged Spike", "Wzrost aged");
}

function prettifyAlertDetail(detail?: string) {
  if (!detail) return null;
  return detail
    .replace(/Why:/g, "Powod:")
    .replace(/Next step:/g, "Nastepny krok:")
    .replace(/Top missing lines:/g, "Najwieksze braki:")
    .replace(/Top SKU:/g, "Top SKU:");
}

function renderAlertContext(detailJson?: Record<string, unknown>) {
  if (!detailJson || Object.keys(detailJson).length === 0) return null;

  const rows: string[] = [];

  const product = detailJson.product;
  if (product && typeof product === "object" && !Array.isArray(product)) {
    const productData = product as Record<string, unknown>;
    const label = typeof productData.title_preferred === "string" ? productData.title_preferred : "";
    const brand = typeof productData.brand === "string" ? productData.brand : "";
    const category = typeof productData.category === "string" ? productData.category : "";
    if (label) rows.push(`Produkt: ${label}`);
    if (brand || category) rows.push(`PIM: ${brand || "-"} | ${category || "-"}`);
  }

  const metrics = detailJson.metrics;
  if (metrics && typeof metrics === "object" && !Array.isArray(metrics)) {
    const metricsData = metrics as Record<string, unknown>;
    const parts = [
      typeof metricsData.days_cover !== "undefined" ? `DOI=${metricsData.days_cover}` : "",
      typeof metricsData.on_hand !== "undefined" ? `on_hand=${metricsData.on_hand}` : "",
      typeof metricsData.inbound !== "undefined" ? `inbound=${metricsData.inbound}` : "",
      typeof metricsData.velocity_30d !== "undefined" ? `vel30=${metricsData.velocity_30d}` : "",
    ].filter(Boolean);
    if (parts.length) rows.push(`Metryki: ${parts.join(", ")}`);
  }

  const shipmentId = typeof detailJson.shipment_id === "string" ? detailJson.shipment_id : "";
  const marketplaceCode = typeof detailJson.marketplace_code === "string" ? detailJson.marketplace_code : "";
  const receivedPct = typeof detailJson.received_pct !== "undefined" ? `received=${detailJson.received_pct}%` : "";
  const daysInStatus = typeof detailJson.days_in_status !== "undefined" ? `days_in_status=${detailJson.days_in_status}` : "";
  if (marketplaceCode) {
    rows.push(`Marketplace: ${marketplaceCode}`);
  }
  if (shipmentId) {
    rows.push(`Shipment: ${shipmentId}${receivedPct ? ` | ${receivedPct}` : ""}${daysInStatus ? ` | ${daysInStatus}` : ""}`);
  }

  const topLines = Array.isArray(detailJson.top_lines) ? detailJson.top_lines : [];
  if (topLines.length > 0) {
    const formatted = topLines
      .slice(0, 3)
      .map((line) => {
        if (!line || typeof line !== "object") return null;
        const data = line as Record<string, unknown>;
        const sku = typeof data.sku === "string" ? data.sku : "-";
        const label =
          typeof data.title_preferred === "string" && data.title_preferred ? data.title_preferred : sku;
        const variance = typeof data.variance_units !== "undefined" ? `-${data.variance_units}` : "";
        return `${label}${variance ? ` (${variance})` : ""}`;
      })
      .filter(Boolean);
    if (formatted.length) rows.push(`Najwieksze braki: ${formatted.join("; ")}`);
  }

  const topSkus = Array.isArray(detailJson.top_skus) ? detailJson.top_skus : [];
  if (topSkus.length > 0) {
    rows.push(`Top SKU: ${topSkus.slice(0, 3).join("; ")}`);
  }

  if (!rows.length) {
    rows.push(JSON.stringify(detailJson, null, 2));
  }

  return (
    <details className="mt-2 rounded-md border border-white/10 bg-black/10 px-3 py-2 text-xs">
      <summary className="cursor-pointer select-none text-[11px] font-medium uppercase tracking-[0.18em] opacity-70">
        Szczegoly alertu
      </summary>
      <div className="mt-2 space-y-1 whitespace-pre-wrap opacity-80">
        {rows.map((row) => (
          <div key={row}>{row}</div>
        ))}
      </div>
    </details>
  );
}

export default function AlertsPage() {
  const [tab, setTab] = useState<"alerts" | "rules">("alerts");

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Alerty i reguly</h1>
          <p className="text-sm text-muted-foreground">Monitorowanie alertow i automatycznych regul.</p>
        </div>
      </div>

      <div className="flex w-fit gap-1 rounded-lg border border-border bg-card p-1">
        <button
          onClick={() => setTab("alerts")}
          className={cn(
            "flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
            tab === "alerts" ? "bg-amazon text-black" : "text-muted-foreground hover:text-foreground"
          )}
        >
          <Bell className="h-3.5 w-3.5" /> Alerty
        </button>
        <button
          onClick={() => setTab("rules")}
          className={cn(
            "flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
            tab === "rules" ? "bg-amazon text-black" : "text-muted-foreground hover:text-foreground"
          )}
        >
          <Settings className="h-3.5 w-3.5" /> Reguly
        </button>
      </div>

      {tab === "alerts" ? <AlertsList /> : <RulesManager />}
    </div>
  );
}

function AlertsList() {
  const qc = useQueryClient();

  const { data, isLoading } = useQuery({
    queryKey: ["alerts"],
    queryFn: () => getAlerts({ is_resolved: false }),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });
  const queueHealthQuery = useQuery({
    queryKey: ["content-publish-queue-health", 30],
    queryFn: () => getContentPublishQueueHealth(30),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  const qh = queueHealthQuery.data;
  const sevQueued = metricSeverity(qh?.queued_total ?? 0, 1, 8);
  const sevStale = metricSeverity(qh?.queued_stale_30m ?? 0, 1, 5);
  const sevRunning = metricSeverity(qh?.running_total ?? 0, 10, 20);
  const sevRetry = metricSeverity(qh?.retry_in_progress ?? 0, 1, 5);
  const sevFailed24 = metricSeverity(qh?.failed_last_24h ?? 0, 1, 5);
  const sevMaxRetry24 = metricSeverity(qh?.max_retry_reached_last_24h ?? 0, 1, 3);

  const readMut = useMutation({
    mutationFn: markAlertRead,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["alerts"] }),
  });

  const resolveMut = useMutation({
    mutationFn: resolveAlert,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["alerts"] }),
  });

  if (isLoading) {
    return (
      <div className="space-y-2">
        {[...Array(5)].map((_, i) => (
          <div key={i} className="h-16 animate-pulse rounded-xl bg-muted" />
        ))}
      </div>
    );
  }

  if (!data?.items.length) {
    return (
      <div className="flex flex-col items-center justify-center rounded-xl border border-border bg-card py-16 text-muted-foreground">
        <Bell className="mb-2 h-8 w-8 opacity-30" />
        <p>Brak aktywnych alertow</p>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {queueHealthQuery.data && (
        <div className="rounded-xl border border-border bg-card p-3">
          <div className="flex items-center justify-between gap-2">
            <div className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              Kondycja kolejki publikacji Content
            </div>
            <Link
              to="/content/publish?job_status=failed"
              className="rounded border border-border px-2 py-1 text-[11px] text-muted-foreground hover:bg-muted"
            >
              Otworz nieudane publikacje
            </Link>
          </div>
          <div className="mt-2 grid gap-2 text-xs md:grid-cols-6">
            <div className={cn("rounded border p-2", HEALTH_BADGE[sevQueued])}>
              w kolejce: {queueHealthQuery.data.queued_total}
            </div>
            <div className={cn("rounded border p-2", HEALTH_BADGE[sevStale])}>
              zalegle {queueHealthQuery.data.thresholds?.stale_minutes ?? 30}m: {queueHealthQuery.data.queued_stale_30m}
            </div>
            <div className={cn("rounded border p-2", HEALTH_BADGE[sevRunning])}>
              w toku: {queueHealthQuery.data.running_total}
            </div>
            <div className={cn("rounded border p-2", HEALTH_BADGE[sevRetry])}>
              retry: {queueHealthQuery.data.retry_in_progress}
            </div>
            <div className={cn("rounded border p-2", HEALTH_BADGE[sevFailed24])}>
              bledy 24h: {queueHealthQuery.data.failed_last_24h}
            </div>
            <div className={cn("rounded border p-2", HEALTH_BADGE[sevMaxRetry24])}>
              max retry 24h: {queueHealthQuery.data.max_retry_reached_last_24h}
            </div>
          </div>
        </div>
      )}

      <div className="rounded-xl border border-border bg-card px-3 py-2 text-xs text-muted-foreground flex items-center justify-between">
        <span>
          Nieprzeczytane: <span className="font-medium text-foreground">{data.unread}</span>
          {" · "}
          Krytyczne: <span className="font-medium text-foreground">{data.critical_count}</span>
        </span>
        <ClientExportButton data={data.items} filename="alerts" />
      </div>

      {data.items.map((alert) => {
        const href = buildAlertHref(alert);
        return (
          <div
            key={alert.id}
            className={cn(
              "flex items-start justify-between gap-4 rounded-xl border p-4",
              SEVERITY_CLASSES[alert.severity as keyof typeof SEVERITY_CLASSES] ?? SEVERITY_CLASSES.info,
              !alert.is_read && "ring-1 ring-current/20"
            )}
          >
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <span
                  className={cn(
                    "inline-block rounded px-1.5 py-0.5 text-xs font-bold uppercase",
                    alert.severity === "critical"
                      ? "bg-destructive text-white"
                      : alert.severity === "warning"
                        ? "bg-amber-500 text-black"
                        : "bg-blue-500 text-white"
                  )}
                >
                  {SEVERITY_LABELS[alert.severity] ?? alert.severity}
                </span>
                {alert.marketplace_id ? (
                  <span className="text-xs text-muted-foreground">{alert.marketplace_id}</span>
                ) : null}
                {alert.sku ? <span className="font-mono text-xs text-muted-foreground">{alert.sku}</span> : null}
                <span className="ml-auto text-xs text-muted-foreground">
                  {format(new Date(alert.triggered_at), "dd.MM HH:mm")}
                </span>
              </div>

              <p className="mt-1 font-medium">{prettifyAlertTitle(alert.title)}</p>

              {alert.detail ? (
                <p className="mt-0.5 whitespace-pre-wrap text-xs opacity-70">{prettifyAlertDetail(alert.detail)}</p>
              ) : null}

              {renderAlertContext(alert.detail_json)}
            </div>

            <div className="flex shrink-0 gap-1">
              {href ? (
                <Link
                  to={href}
                  className="rounded-md p-1.5 opacity-60 hover:bg-black/10"
                  title="Otworz powiazany widok"
                >
                  <ExternalLink className="h-4 w-4" />
                </Link>
              ) : null}
              {!alert.is_read ? (
                <button
                  onClick={() => readMut.mutate(alert.id)}
                  className="rounded-md p-1.5 opacity-60 hover:bg-black/10"
                  title="Oznacz jako przeczytane"
                >
                  <Eye className="h-4 w-4" />
                </button>
              ) : null}
              <button
                onClick={() => resolveMut.mutate(alert.id)}
                className="rounded-md p-1.5 opacity-60 hover:bg-black/10"
                title="Rozwiaz"
              >
                <CheckCircle className="h-4 w-4" />
              </button>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function RulesManager() {
  const qc = useQueryClient();
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState<AlertRuleCreate>({
    name: "",
    rule_type: "margin_below",
    severity: "warning",
    threshold_operator: "lt",
  });

  const { data: rules, isLoading } = useQuery({
    queryKey: ["alert-rules"],
    queryFn: getAlertRules,
  });

  const createMut = useMutation({
    mutationFn: createAlertRule,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["alert-rules"] });
      setShowForm(false);
      setForm({ name: "", rule_type: "margin_below", severity: "warning", threshold_operator: "lt" });
    },
  });

  const deleteMut = useMutation({
    mutationFn: deleteAlertRule,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["alert-rules"] }),
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!form.name.trim()) return;
    createMut.mutate(form);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">Skonfigurowane reguly: {rules?.length ?? 0}</p>
        <button
          onClick={() => setShowForm(!showForm)}
          className="flex items-center gap-1.5 rounded-lg bg-amazon px-3 py-1.5 text-sm font-medium text-black transition-colors hover:bg-amazon/90"
        >
          <Plus className="h-3.5 w-3.5" />
          Nowa regula
        </button>
      </div>

      {showForm ? (
        <form onSubmit={handleSubmit} className="space-y-4 rounded-xl border border-border bg-card p-5">
          <h3 className="text-sm font-semibold">Utworz regule alertu</h3>
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">Nazwa *</label>
              <input
                value={form.name}
                onChange={(e) => setForm({ ...form, name: e.target.value })}
                placeholder="np. Niski stan zapasu"
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm outline-none focus:ring-1 focus:ring-ring"
                required
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">Typ reguly</label>
              <select
                value={form.rule_type}
                onChange={(e) => setForm({ ...form, rule_type: e.target.value })}
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm outline-none"
              >
                {RULE_TYPES.map((ruleType) => (
                  <option key={ruleType.value} value={ruleType.value}>
                    {ruleType.label}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">Waznosc</label>
              <select
                value={form.severity}
                onChange={(e) => setForm({ ...form, severity: e.target.value })}
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm outline-none"
              >
                <option value="info">Info</option>
                <option value="warning">Ostrzezenie</option>
                <option value="critical">Krytyczny</option>
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">Operator</label>
              <select
                value={form.threshold_operator ?? "lt"}
                onChange={(e) => setForm({ ...form, threshold_operator: e.target.value })}
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm outline-none"
              >
                {OPERATORS.map((operator) => (
                  <option key={operator.value} value={operator.value}>
                    {operator.label}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">Prog</label>
              <input
                type="number"
                step="0.01"
                value={form.threshold_value ?? ""}
                onChange={(e) =>
                  setForm({ ...form, threshold_value: e.target.value ? Number(e.target.value) : undefined })
                }
                placeholder="np. 10"
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm outline-none focus:ring-1 focus:ring-ring"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">SKU opcjonalnie</label>
              <input
                value={form.sku ?? ""}
                onChange={(e) => setForm({ ...form, sku: e.target.value || undefined })}
                placeholder="puste = wszystkie SKU"
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm outline-none focus:ring-1 focus:ring-ring"
              />
            </div>
            <div className="sm:col-span-2">
              <label className="mb-1 block text-xs font-medium text-muted-foreground">Opis opcjonalny</label>
              <input
                value={form.description ?? ""}
                onChange={(e) => setForm({ ...form, description: e.target.value || undefined })}
                placeholder="Co monitoruje ta regula"
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm outline-none focus:ring-1 focus:ring-ring"
              />
            </div>
          </div>
          <div className="flex gap-2">
            <button
              type="submit"
              disabled={createMut.isPending}
              className="rounded-lg bg-amazon px-4 py-1.5 text-sm font-medium text-black transition-colors hover:bg-amazon/90 disabled:opacity-50"
            >
              {createMut.isPending ? "Tworzenie..." : "Utworz regule"}
            </button>
            <button
              type="button"
              onClick={() => setShowForm(false)}
              className="rounded-lg border border-border px-4 py-1.5 text-sm font-medium transition-colors hover:bg-muted"
            >
              Anuluj
            </button>
          </div>
        </form>
      ) : null}

      {isLoading ? (
        <div className="space-y-2">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="h-12 animate-pulse rounded-xl bg-muted" />
          ))}
        </div>
      ) : !rules?.length ? (
        <div className="flex flex-col items-center justify-center rounded-xl border border-border bg-card py-12 text-muted-foreground">
          <Settings className="mb-2 h-8 w-8 opacity-30" />
          <p>Brak skonfigurowanych regul</p>
          <p className="mt-1 text-xs">Dodaj pierwsza regule alertu.</p>
        </div>
      ) : (
        <div className="overflow-hidden rounded-xl border border-border bg-card">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Nazwa</th>
                <th className="px-2 py-2">Typ</th>
                <th className="px-2 py-2">Waznosc</th>
                <th className="px-2 py-2">Prog</th>
                <th className="px-2 py-2">SKU</th>
                <th className="px-2 py-2">Status</th>
                <th className="px-2 py-2 text-right">Akcje</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {rules.map((rule) => (
                <tr key={rule.id} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5">
                    <p className="font-medium">{rule.name}</p>
                    {rule.description ? <p className="text-xs text-muted-foreground">{rule.description}</p> : null}
                  </td>
                  <td className="px-2 py-1.5">
                    <span className="rounded bg-muted px-1.5 py-0.5 text-xs font-medium">
                      {RULE_TYPES.find((ruleType) => ruleType.value === rule.rule_type)?.label ?? rule.rule_type}
                    </span>
                  </td>
                  <td className="px-2 py-1.5">
                    <span
                      className={cn(
                        "inline-block rounded px-1.5 py-0.5 text-xs font-bold uppercase",
                        rule.severity === "critical"
                          ? "bg-destructive text-white"
                          : rule.severity === "warning"
                            ? "bg-amber-500 text-black"
                            : "bg-blue-500 text-white"
                      )}
                    >
                      {SEVERITY_LABELS[rule.severity] ?? rule.severity}
                    </span>
                  </td>
                  <td className="px-2 py-1.5 tabular-nums text-muted-foreground">
                    {rule.threshold_value != null
                      ? `${OPERATORS.find((operator) => operator.value === rule.threshold_operator)?.label ?? rule.threshold_operator ?? ""} ${rule.threshold_value}`
                      : "-"}
                  </td>
                  <td className="px-2 py-1.5 font-mono text-xs text-muted-foreground">{rule.sku ?? "Wszystkie"}</td>
                  <td className="px-2 py-1.5">
                    <span
                      className={cn(
                        "inline-block rounded-full px-2 py-0.5 text-[10px] font-semibold",
                        rule.is_active ? "bg-green-500/15 text-green-500" : "bg-muted text-muted-foreground"
                      )}
                    >
                      {rule.is_active ? "Aktywna" : "Wylaczona"}
                    </span>
                  </td>
                  <td className="px-2 py-1.5 text-right">
                    <button
                      onClick={() => deleteMut.mutate(rule.id)}
                      className="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-destructive/10 hover:text-destructive"
                      title="Usun regule"
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
