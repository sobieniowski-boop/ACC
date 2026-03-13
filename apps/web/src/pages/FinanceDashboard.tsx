import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { format, subMonths } from "date-fns";
import {
  autoMatchFinancePayouts,
  getFinanceDashboard,
  runFinanceGenerateLedger,
  runFinanceImportTransactions,
  runFinancePrepareSettlements,
  runFinanceReconciliation,
} from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { formatPLN } from "@/lib/utils";
import { DataFreshness } from "@/components/shared";

function formatSectionStatus(status?: string | null) {
  if (!status) {
    return { label: "unknown", className: "border-white/10 bg-white/5 text-white/60" };
  }
  if (status === "real_data") {
    return { label: "real data", className: "border-emerald-500/30 bg-emerald-500/10 text-emerald-200" };
  }
  if (status === "partial") {
    return { label: "partial", className: "border-amber-500/30 bg-amber-500/10 text-amber-200" };
  }
  if (status === "blocked_by_missing_bank_import") {
    return { label: "blocked by missing bank import", className: "border-red-500/30 bg-red-500/10 text-red-200" };
  }
  return { label: "no data", className: "border-white/10 bg-white/5 text-white/60" };
}

function MetricCard({ label, value, note, status }: { label: string; value: string; note?: string; status?: string | null }) {
  const badge = formatSectionStatus(status);
  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between gap-3">
          <CardTitle className="text-xs text-white/60">{label}</CardTitle>
          <span className={`rounded-full border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide ${badge.className}`}>{badge.label}</span>
        </div>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold text-white">{value}</div>
        {note ? <div className="mt-1 text-xs text-white/45">{note}</div> : null}
      </CardContent>
    </Card>
  );
}

function GapBadge({ reason }: { reason?: string | null }) {
  if (!reason || reason === "ok") {
    return null;
  }
  const label =
    reason === "rows_not_attributed_to_marketplace"
      ? "rows not attributed"
      : reason === "coverage_gap_after_import"
        ? "coverage gap"
        : reason === "groups_without_rows"
          ? "groups without rows"
          : reason === "imported_rows_missing_orders"
            ? "missing orders"
            : reason === "unmapped_finance_rows"
              ? "unmapped rows"
              : reason;
  const tone =
    reason === "rows_not_attributed_to_marketplace"
      ? "border-red-500/40 bg-red-500/10 text-red-200"
      : "border-amber-500/40 bg-amber-500/10 text-amber-200";
  return <span className={`rounded-full border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide ${tone}`}>{label}</span>;
}

function formatGapDriver(driver?: string | null) {
  if (!driver) {
    return null;
  }
  const labels: Record<string, string> = {
    older_orders_not_backfilled: "stare zamowienia bez backfillu",
    mid_window_gap: "luka 7-13 dni",
    mfn_undercovered: "MFN undercovered",
    afn_undercovered: "AFN undercovered",
    unmapped_finance_rows: "niezmapowane finance rows",
    missing_orders_in_acc: "brakujace ordery w ACC",
    general_coverage_gap: "ogolna luka coverage",
  };
  return labels[driver] ?? driver;
}

function formatCoverageBreakdown(
  items: Array<{ key: string; coverage_pct: number }> | undefined,
) {
  if (!items || items.length === 0) {
    return "-";
  }
  return items.map((item) => `${item.key}:${item.coverage_pct.toFixed(1)}%`).join(" | ");
}

function formatCountMap(map: Record<string, number> | undefined, limit = 3) {
  if (!map) {
    return "-";
  }
  const entries = Object.entries(map)
    .filter(([, value]) => value > 0)
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit);
  if (entries.length === 0) {
    return "-";
  }
  return entries.map(([key, value]) => `${key}:${value}`).join(" | ");
}

function formatMissingCause(cause?: string | null) {
  if (!cause) {
    return null;
  }
  const labels: Record<string, string> = {
    recent_order_sync_lag: "swiezy lag order syncu",
    historical_order_backfill_gap: "stara luka backfillu orderow",
    older_order_coverage_gap: "luka starszych orderow",
    shipment_events_without_orders: "shipment events bez orderow",
    mixed_missing_orders: "mieszany profil brakujacych orderow",
  };
  return labels[cause] ?? cause;
}

function formatStatusCountMap(map: Record<string, number> | undefined) {
  if (!map) {
    return "-";
  }
  const entries = Object.entries(map)
    .filter(([, value]) => value > 0)
    .sort((a, b) => b[1] - a[1]);
  if (entries.length === 0) {
    return "-";
  }
  return entries.map(([key, value]) => `${key}:${value}`).join(" | ");
}

export default function FinanceDashboardPage() {
  const qc = useQueryClient();
  const [mutError, setMutError] = useState<string | null>(null);
  const dashboardQuery = useQuery({
    queryKey: ["finance-dashboard"],
    queryFn: () => getFinanceDashboard({ from: format(subMonths(new Date(), 6), "yyyy-MM-dd") }),
    refetchInterval: 15000,
  });

  const onMutError = (err: unknown, label: string) => {
    const msg = err instanceof Error ? err.message : "Unknown error";
    setMutError(`${label} failed: ${msg}`);
  };

  const importMut = useMutation({
    mutationFn: () => runFinanceImportTransactions(180),
    onSuccess: () => { setMutError(null); qc.invalidateQueries({ queryKey: ["finance-dashboard"] }); },
    onError: (err) => onMutError(err, "Sync finances"),
  });
  const settlementsMut = useMutation({
    mutationFn: runFinancePrepareSettlements,
    onSuccess: () => { setMutError(null); qc.invalidateQueries({ queryKey: ["finance-dashboard"] }); },
    onError: (err) => onMutError(err, "Prepare settlements"),
  });
  const ledgerMut = useMutation({
    mutationFn: () => runFinanceGenerateLedger(180),
    onSuccess: () => { setMutError(null); qc.invalidateQueries({ queryKey: ["finance-dashboard"] }); },
    onError: (err) => onMutError(err, "Generate ledger"),
  });
  const reconcileJobMut = useMutation({
    mutationFn: runFinanceReconciliation,
    onSuccess: () => { setMutError(null); qc.invalidateQueries({ queryKey: ["finance-dashboard"] }); },
    onError: (err) => onMutError(err, "Reconcile job"),
  });
  const autoMatchMut = useMutation({
    mutationFn: autoMatchFinancePayouts,
    onSuccess: () => { setMutError(null); qc.invalidateQueries({ queryKey: ["finance-dashboard"] }); },
    onError: (err) => onMutError(err, "Auto-match payouts"),
  });

  const sectionMap = useMemo(
    () => new Map((dashboardQuery.data?.sections ?? []).map((item) => [item.key, item])),
    [dashboardQuery.data?.sections],
  );

  const openDiagnosticGroups = useMemo(
    () => (dashboardQuery.data?.sync_diagnostics?.items ?? []).filter((item) => item.sync_state !== "closed"),
    [dashboardQuery.data?.sync_diagnostics?.items],
  );
  const largestOpenGroups = useMemo(
    () => [...openDiagnosticGroups].sort((a, b) => b.last_row_count - a.last_row_count).slice(0, 5),
    [openDiagnosticGroups],
  );
  const costliestOpenGroups = useMemo(
    () => [...openDiagnosticGroups].sort((a, b) => b.cost_score - a.cost_score).slice(0, 5),
    [openDiagnosticGroups],
  );
  const gapByMarketplace = useMemo(() => {
    const items = dashboardQuery.data?.gap_diagnostics?.marketplaces ?? [];
    return new Map(items.map((item) => [item.marketplace_id, item]));
  }, [dashboardQuery.data?.gap_diagnostics?.marketplaces]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Finance Dashboard</h1>
          <p className="text-sm text-white/50">Operacyjny podglad feedu finansowego, ledgera i uzgodnien payout po Financial Event Group.</p>
          <DataFreshness lastSync={dashboardQuery.data?.sync_diagnostics?.latest_watermark_from} staleMinutes={120} label="Finance" />
        </div>
        <div className="flex flex-wrap gap-2">
          <Button onClick={() => importMut.mutate()} disabled={importMut.isPending}>
            {importMut.isPending ? "Sync..." : "Sync finances"}
          </Button>
          <Button onClick={() => settlementsMut.mutate()} disabled={settlementsMut.isPending} variant="secondary">
            {settlementsMut.isPending ? "Building..." : "Prepare settlements"}
          </Button>
          <Button onClick={() => ledgerMut.mutate()} disabled={ledgerMut.isPending} variant="secondary">
            {ledgerMut.isPending ? "Building..." : "Generate ledger"}
          </Button>
          <Button onClick={() => reconcileJobMut.mutate()} disabled={reconcileJobMut.isPending} variant="secondary">
            {reconcileJobMut.isPending ? "Running..." : "Reconcile job"}
          </Button>
        </div>
      </div>

      {mutError && (
        <div className="rounded-lg border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {mutError}
          <button onClick={() => setMutError(null)} className="ml-3 underline text-xs">Dismiss</button>
        </div>
      )}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricCard
          label="Revenue base"
          value={formatPLN(dashboardQuery.data?.revenue_base ?? 0)}
          note={sectionMap.get("ledger")?.note ?? "konto 700"}
          status={sectionMap.get("ledger")?.status}
        />
        <MetricCard
          label="Fees base"
          value={formatPLN(dashboardQuery.data?.fees_base ?? 0)}
          note="520 / 530 / 540 / 550 / 580"
          status={sectionMap.get("finance_feed")?.status}
        />
        <MetricCard
          label="VAT base"
          value={formatPLN(dashboardQuery.data?.vat_base ?? 0)}
          note="konto 220"
          status={sectionMap.get("ledger")?.status}
        />
        <MetricCard
          label="Profit proxy"
          value={formatPLN(dashboardQuery.data?.profit_proxy ?? 0)}
          note={
            dashboardQuery.data?.partial
              ? "partial: revenue - fees on incomplete feed"
              : "revenue - fees"
          }
          status={sectionMap.get("finance_feed")?.status}
        />
        <MetricCard
          label="Unmatched payouts"
          value={String(dashboardQuery.data?.unmatched_payouts ?? 0)}
          note={sectionMap.get("reconciliation")?.note ?? `ledger rows: ${dashboardQuery.data?.ledger_rows ?? 0}`}
          status={sectionMap.get("reconciliation")?.status}
        />
      </div>

      {dashboardQuery.data?.partial ? (
        <Card className="border-amber-500/30">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-amber-300">Data completeness: partial</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-white/70">
            {dashboardQuery.data.note}
          </CardContent>
        </Card>
      ) : null}

      {!dashboardQuery.data?.order_sync?.ok || (dashboardQuery.data?.order_sync?.items ?? []).some((item) => item.status !== "ok") ? (
        <Card className="border-amber-500/30">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-amber-300">Order sync gap risk</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-white/70">
            <div>
              Order sync had marketplace gaps beyond the safe threshold. Fresh finance rows may reference orders that are not yet present in `acc_order`.
            </div>
            <div className="text-xs text-white/50">
              {(dashboardQuery.data?.order_sync?.items ?? [])
                .filter((item) => item.status !== "ok")
                .map((item) => `${item.marketplace_code}:${item.gap_minutes?.toFixed?.(1) ?? item.gap_minutes ?? "-"}m`)
                .join(" | ") || (dashboardQuery.data?.order_sync?.error ?? "-")}
            </div>
          </CardContent>
        </Card>
      ) : null}

      {dashboardQuery.data?.order_revenue_integrity ? (
        <Card className="border-white/10">
          <CardHeader>
            <CardTitle className="text-sm">2025 order revenue integrity</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              <MetricCard
                label="Missing revenue (active only)"
                value={String(dashboardQuery.data.order_revenue_integrity.missing_revenue_active)}
                note={`excludes canceled/cancelled | active orders ${dashboardQuery.data.order_revenue_integrity.active_orders}`}
              />
              <MetricCard
                label="Missing order total (active only)"
                value={String(dashboardQuery.data.order_revenue_integrity.missing_order_total_active)}
                note={`total missing ${dashboardQuery.data.order_revenue_integrity.missing_order_total_total}`}
              />
              <MetricCard
                label="Shipped anomalies"
                value={`${dashboardQuery.data.order_revenue_integrity.missing_revenue_shipped}/${dashboardQuery.data.order_revenue_integrity.missing_order_total_shipped}`}
                note="missing revenue / missing order_total"
              />
              <MetricCard
                label="Unshipped anomalies"
                value={`${dashboardQuery.data.order_revenue_integrity.missing_revenue_unshipped}/${dashboardQuery.data.order_revenue_integrity.missing_order_total_unshipped}`}
                note="header-only cases reviewed separately"
              />
            </div>
            <div className="grid gap-4 xl:grid-cols-2">
              <div className="rounded border border-white/10 px-3 py-3 text-sm">
                <div className="font-medium text-white">Status breakdown</div>
                <div className="mt-2 text-xs text-white/60">
                  missing revenue: {formatStatusCountMap(dashboardQuery.data.order_revenue_integrity.missing_revenue_by_status)}
                </div>
                <div className="mt-1 text-xs text-white/60">
                  missing order_total: {formatStatusCountMap(dashboardQuery.data.order_revenue_integrity.missing_order_total_by_status)}
                </div>
              </div>
              <div className="rounded border border-white/10 px-3 py-3 text-sm">
                <div className="font-medium text-white">Why this is safer</div>
                <div className="mt-2 text-xs text-white/60">{dashboardQuery.data.order_revenue_integrity.note}</div>
                <div className="mt-2 text-xs text-white/60">
                  shipped zero-line headers: {dashboardQuery.data.order_revenue_integrity.shipped_missing_revenue_zero_line_headers} | unshipped zero-line headers:{" "}
                  {dashboardQuery.data.order_revenue_integrity.unshipped_missing_revenue_zero_line_headers}
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      ) : null}

      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Recent finance jobs</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {(dashboardQuery.data?.recent_jobs ?? []).map((job) => (
              <div key={job.id} className="rounded border border-white/10 px-3 py-2 text-sm">
                <div className="flex items-center justify-between gap-3">
                  <div className="font-medium text-white">{job.job_type}</div>
                  <div className="text-xs text-white/45">{job.status}</div>
                </div>
                <div className="mt-1 text-xs text-white/50">{job.progress_message ?? "-"}</div>
              </div>
            ))}
            {(dashboardQuery.data?.recent_jobs.length ?? 0) === 0 ? <div className="text-sm text-white/50">Brak jobow finansowych.</div> : null}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Payout reconciliation status</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex items-center justify-between">
              <div className="text-sm text-white/60">
                Reconciliation opiera sie na `FinancialEventGroupId`. Jesli dany feed nie zwroci grup platnosci, lista pozostanie pusta.
              </div>
              <Button onClick={() => autoMatchMut.mutate()} disabled={autoMatchMut.isPending} variant="secondary">
                {autoMatchMut.isPending ? "Matching..." : "Auto-match"}
              </Button>
            </div>
            {(dashboardQuery.data?.payout_reconciliation?.items ?? []).slice(0, 8).map((item) => (
              <div key={`${item.financial_event_group_id ?? item.settlement_id}-${item.id ?? "x"}`} className="rounded border border-white/10 px-3 py-2 text-sm">
                <div className="flex items-center justify-between gap-3">
                  <div className="font-medium text-white">{item.financial_event_group_id ?? item.settlement_id}</div>
                  <div className="text-xs text-white/45">{item.status}</div>
                </div>
                <div className="mt-1 text-xs text-white/50">
                  {item.marketplace_code ?? "-"} | {item.currency} | total {item.total_amount}
                </div>
              </div>
            ))}
            {(dashboardQuery.data?.payout_reconciliation?.items.length ?? 0) === 0 ? <div className="text-sm text-white/50">Brak payout settlementow do uzgodnienia.</div> : null}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Finance sync diagnostics</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="flex flex-wrap gap-4 text-sm text-white/60">
            <div>watermark: {dashboardQuery.data?.sync_diagnostics?.latest_watermark_from ? new Date(dashboardQuery.data.sync_diagnostics.latest_watermark_from).toLocaleString() : "-"}</div>
            <div>open groups: {dashboardQuery.data?.sync_diagnostics?.tracked_open_groups ?? 0}</div>
          </div>
          <div className="rounded border border-white/10">
            <div className="border-b border-white/10 px-3 py-2 text-sm font-medium text-white">Marketplace completeness (30d)</div>
            <div className="space-y-2 p-3">
              {(dashboardQuery.data?.completeness?.marketplaces ?? []).map((item) => (
                <div key={item.marketplace_id} className="rounded border border-white/10 px-3 py-2 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <div className="flex items-center gap-2">
                      <div className="font-medium text-white">{item.marketplace_code}</div>
                      <GapBadge reason={gapByMarketplace.get(item.marketplace_id)?.gap_reason} />
                    </div>
                    <div className="text-xs text-white/45">{item.status}</div>
                  </div>
                  <div className="mt-1 text-xs text-white/60">
                    day coverage {item.day_coverage_pct.toFixed(1)}% ({item.finance_days}/{item.order_days}) | order coverage {item.order_coverage_pct.toFixed(1)}% ({item.orders_with_finance}/{item.orders_total})
                  </div>
                  <div className="mt-1 text-xs text-white/50">
                    imported rows {gapByMarketplace.get(item.marketplace_id)?.imported_rows ?? 0} | tracked groups {gapByMarketplace.get(item.marketplace_id)?.tracked_groups ?? 0} | imported orders {gapByMarketplace.get(item.marketplace_id)?.imported_orders ?? 0}
                  </div>
                  <div className="mt-1 text-xs text-white/50">
                    missing order rows {gapByMarketplace.get(item.marketplace_id)?.missing_order_rows ?? 0} ({gapByMarketplace.get(item.marketplace_id)?.missing_order_distinct_orders ?? 0} orders) | unmapped rows {gapByMarketplace.get(item.marketplace_id)?.unmapped_rows ?? 0}
                  </div>
                  {gapByMarketplace.get(item.marketplace_id)?.likely_gap_driver ? (
                    <div className="mt-1 text-xs text-white/50">
                      likely driver: {formatGapDriver(gapByMarketplace.get(item.marketplace_id)?.likely_gap_driver)}
                    </div>
                  ) : null}
                  {gapByMarketplace.get(item.marketplace_id)?.missing_order_likely_cause ? (
                    <div className="mt-1 text-xs text-white/50">
                      missing cause: {formatMissingCause(gapByMarketplace.get(item.marketplace_id)?.missing_order_likely_cause)}
                    </div>
                  ) : null}
                  <div className="mt-1 text-xs text-white/40">
                    age {formatCoverageBreakdown(gapByMarketplace.get(item.marketplace_id)?.by_age_bucket)} | channel {formatCoverageBreakdown(gapByMarketplace.get(item.marketplace_id)?.by_fulfillment_channel)}
                  </div>
                  <div className="mt-1 text-xs text-white/40">
                    missing age {formatCountMap(gapByMarketplace.get(item.marketplace_id)?.missing_order_age_bucket_counts)} | missing type {formatCountMap(gapByMarketplace.get(item.marketplace_id)?.missing_order_transaction_type_counts)}
                  </div>
                  {gapByMarketplace.get(item.marketplace_id)?.note ? (
                    <div className="mt-1 text-xs text-white/45">{gapByMarketplace.get(item.marketplace_id)?.note}</div>
                  ) : null}
                </div>
              ))}
              {(dashboardQuery.data?.completeness?.marketplaces.length ?? 0) === 0 ? <div className="text-sm text-white/50">Brak danych completeness dla finance feedu.</div> : null}
            </div>
          </div>
          <div className="grid gap-4 xl:grid-cols-2">
            <div className="space-y-2">
              <div className="text-sm font-medium text-white">Largest open groups</div>
              {largestOpenGroups.map((item) => (
                <div key={`largest-${item.financial_event_group_id}`} className="rounded border border-white/10 px-3 py-2 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <div className="font-medium text-white">{item.financial_event_group_id}</div>
                    <div className="text-xs text-white/45">{item.last_row_count} rows</div>
                  </div>
                  <div className="mt-1 text-xs text-white/50">
                    {item.marketplace_code ?? "-"} | age {item.open_age_hours.toFixed(1)}h | score {item.cost_score.toFixed(1)}
                  </div>
                </div>
              ))}
              {largestOpenGroups.length === 0 ? <div className="text-sm text-white/50">Brak otwartych grup.</div> : null}
            </div>
            <div className="space-y-2">
              <div className="text-sm font-medium text-white">Most expensive open groups</div>
              {costliestOpenGroups.map((item) => (
                <div key={`costliest-${item.financial_event_group_id}`} className="rounded border border-white/10 px-3 py-2 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <div className="font-medium text-white">{item.financial_event_group_id}</div>
                    <div className="text-xs text-white/45">score {item.cost_score.toFixed(1)}</div>
                  </div>
                  <div className="mt-1 text-xs text-white/50">
                    {item.marketplace_code ?? "-"} | rows {item.last_row_count} | age {item.open_age_hours.toFixed(1)}h
                  </div>
                </div>
              ))}
              {costliestOpenGroups.length === 0 ? <div className="text-sm text-white/50">Brak otwartych grup.</div> : null}
            </div>
          </div>
          <div className="space-y-2">
            {(dashboardQuery.data?.sync_diagnostics?.items ?? []).map((item) => (
              <div key={item.financial_event_group_id} className="rounded border border-white/10 px-3 py-2 text-sm">
                <div className="flex items-center justify-between gap-3">
                  <div className="font-medium text-white">{item.financial_event_group_id}</div>
                  <div className="text-xs text-white/45">{item.sync_state}</div>
                </div>
                <div className="mt-1 text-xs text-white/50">
                  {item.marketplace_code ?? "-"} | {item.processing_status ?? "-"} / {item.fund_transfer_status ?? "-"} | rows {item.last_row_count} | age {item.open_age_hours.toFixed(1)}h | score {item.cost_score.toFixed(1)}
                </div>
                <div className="mt-1 text-xs text-white/45">
                  last synced {item.last_synced_at ? new Date(item.last_synced_at).toLocaleString() : "-"} | next refresh {item.open_refresh_after ? new Date(item.open_refresh_after).toLocaleString() : "-"}
                </div>
                <div className="mt-1 text-xs text-white/40">
                  events: {Object.entries(item.event_type_counts_json ?? {}).map(([k, v]) => `${k}:${v}`).join(", ") || "-"}
                </div>
              </div>
            ))}
            {(dashboardQuery.data?.sync_diagnostics?.items.length ?? 0) === 0 ? <div className="text-sm text-white/50">Brak danych diagnostycznych syncu finansowego.</div> : null}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
