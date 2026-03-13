import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { getFbaScorecard, runFbaJob, syncFbaReconciliation } from "@/lib/api";
import { FbaJobStatusStrip } from "@/components/fba/FbaJobStatusStrip";
import { ImportCSVDialog } from "@/components/fba/ImportCSVDialog";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ClientExportButton } from "@/components/shared";

function currentQuarter() {
  const now = new Date();
  const quarter = Math.floor(now.getMonth() / 3) + 1;
  return `${now.getFullYear()}-Q${quarter}`;
}

export default function FbaScorecardPage() {
  const qc = useQueryClient();
  const quarter = currentQuarter();
  const { data } = useQuery({
    queryKey: ["fba-scorecard", quarter],
    queryFn: () => getFbaScorecard(quarter),
  });

  const reconMut = useMutation({
    mutationFn: () => syncFbaReconciliation(),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["fba-scorecard"] }),
  });

  const inventoryMut = useMutation({
    mutationFn: () => runFbaJob("sync_fba_inventory"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["fba-scorecard"] }),
  });

  const formatValue = (value?: number | null, unit?: string) => {
    if (value === null || value === undefined) return "n/a";
    if (unit === "%") return `${value.toFixed(2)}%`;
    if (unit === "days") return `${value.toFixed(1)} d`;
    return value.toFixed(2);
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">KPI & Bonus Scorecard</h1>
        <p className="text-sm text-white/50">Quarterly FBA Ops score based on 9 KPI components with weighted factor scale 0.00-1.20.</p>
      </div>
      <FbaJobStatusStrip />

      {/* Quick-action bar: sync + import buttons */}
      <div className="flex flex-wrap items-center gap-2">
        <Button variant="outline" size="sm" onClick={() => inventoryMut.mutate()} disabled={inventoryMut.isPending}>
          {inventoryMut.isPending ? "Syncing..." : "Sync Inventory Snapshot"}
        </Button>
        <Button variant="outline" size="sm" onClick={() => reconMut.mutate()} disabled={reconMut.isPending}>
          {reconMut.isPending ? "Running..." : "Sync Reconciliation"}
        </Button>
        <div className="mx-2 h-5 w-px bg-white/10" />
        <ImportCSVDialog registerType="shipment_plan" quarter={quarter} invalidateKeys={[["fba-shipment-plans"], ["fba-scorecard"]]} buttonLabel="Import Plans" />
        <ImportCSVDialog registerType="case" invalidateKeys={[["fba-cases"], ["fba-scorecard"]]} buttonLabel="Import Cases" />
        <ImportCSVDialog registerType="launch" quarter={quarter} invalidateKeys={[["fba-launches"], ["fba-scorecard"]]} buttonLabel="Import Launches" />
        <ImportCSVDialog registerType="initiative" quarter={quarter} invalidateKeys={[["fba-initiatives"], ["fba-scorecard"]]} buttonLabel="Import Initiatives" />
        <div className="mx-2 h-5 w-px bg-white/10" />
        <ClientExportButton data={data?.components ?? []} filename="fba_scorecard" />
      </div>
      <Card>
        <CardHeader><CardTitle className="text-sm">{quarter}</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          <div className="flex flex-wrap items-end gap-6">
            <div>
              <div className="text-xs uppercase tracking-[0.2em] text-white/40">Score_Q</div>
              <div className="text-4xl font-bold">{(data?.score ?? 0).toFixed(3)}</div>
            </div>
            <div>
              <div className="text-xs uppercase tracking-[0.2em] text-white/40">Vs Target</div>
              <div className="text-xl font-semibold">{(data?.score_pct_of_target ?? 0).toFixed(1)}%</div>
            </div>
            <div className={`rounded-full px-3 py-1 text-xs font-medium ${data?.data_ready ? "bg-emerald-500/20 text-emerald-300" : "bg-amber-500/20 text-amber-300"}`}>
              {data?.data_ready ? "Data ready" : "Partial data"}
            </div>
          </div>
          <p className="text-sm text-white/70">{data?.explanation}</p>
          {!data?.data_ready ? (
            <div className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-4 text-sm text-white/70">
              This scorecard is partial. Do not use it for compensation or quarter-close decisions until missing inputs are resolved.
            </div>
          ) : null}
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {(data?.components ?? []).map((component) => (
              <div key={component.key} className="rounded-lg border border-white/10 p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <div className="text-xs uppercase tracking-[0.2em] text-white/40">{component.label}</div>
                    <div className="mt-2 text-lg font-semibold">{formatValue(component.actual, component.unit)}</div>
                  </div>
                  <div className={`rounded-full px-2 py-1 text-[11px] font-medium ${component.data_ready ? "bg-emerald-500/15 text-emerald-300" : "bg-amber-500/15 text-amber-300"}`}>
                    {component.data_ready ? "ready" : "missing"}
                  </div>
                </div>
                <div className="mt-3 grid grid-cols-2 gap-2 text-sm text-white/70">
                  <div>Factor: {component.factor.toFixed(2)}</div>
                  <div>Weight: {(component.weight * 100).toFixed(0)}%</div>
                  <div>Target: {formatValue(component.target, component.unit)}</div>
                  <div>Good: {formatValue(component.good, component.unit)}</div>
                </div>
                <div className="mt-2 text-xs text-white/45">
                  Contribution: {component.score_contribution.toFixed(3)}
                </div>
                {component.note ? (
                  <p className="mt-3 text-xs text-white/50">{component.note}</p>
                ) : null}
              </div>
            ))}
          </div>
          {(data?.missing_inputs?.length ?? 0) > 0 ? (
            <div className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-4">
              <div className="text-xs uppercase tracking-[0.2em] text-amber-300/80">Missing inputs</div>
              <div className="mt-2 space-y-1 text-sm text-white/70">
                {data?.missing_inputs.map((item) => (
                  <div key={item}>{item}</div>
                ))}
              </div>
            </div>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}
