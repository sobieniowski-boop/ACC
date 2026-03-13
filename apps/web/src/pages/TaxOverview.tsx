import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { getTaxOverview, runTaxPipeline, detectTaxIssues } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { AlertTriangle, CheckCircle, Clock, RefreshCw, Shield } from "lucide-react";
import { useState } from "react";

export default function TaxOverviewPage() {
  const qc = useQueryClient();
  const { data, isLoading } = useQuery({ queryKey: ["tax-overview"], queryFn: getTaxOverview });
  const [running, setRunning] = useState(false);

  const pipelineMut = useMutation({
    mutationFn: () => runTaxPipeline(30),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["tax-overview"] }); setRunning(false); },
    onError: () => setRunning(false),
  });

  const detectMut = useMutation({
    mutationFn: () => detectTaxIssues(90),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tax-overview"] }),
  });

  if (isLoading) return <div className="p-6 text-muted-foreground">Loading tax compliance overview…</div>;

  const d = data ?? {};
  const cls = d.classification_summary ?? {};
  const filing = d.filing_readiness?.snapshot ?? {};

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">VAT & OSS Compliance Center</h1>
          <p className="text-sm text-muted-foreground">Unified tax compliance dashboard</p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={() => detectMut.mutate()} disabled={detectMut.isPending}>
            <AlertTriangle className="mr-2 h-4 w-4" />
            Detect Issues
          </Button>
          <Button size="sm" onClick={() => { setRunning(true); pipelineMut.mutate(); }} disabled={running}>
            <RefreshCw className={`mr-2 h-4 w-4 ${running ? "animate-spin" : ""}`} />
            Run Full Pipeline
          </Button>
        </div>
      </div>

      {/* Issue summary */}
      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <StatCard label="Open Issues" value={d.open_issues ?? 0} icon={<AlertTriangle className="h-5 w-5" />} variant={d.p1_issues > 0 ? "destructive" : "default"} />
        <StatCard label="P1 Critical" value={d.p1_issues ?? 0} icon={<Shield className="h-5 w-5" />} variant={d.p1_issues > 0 ? "destructive" : "default"} />
        <StatCard label="Events Classified" value={cls.total ?? 0} icon={<CheckCircle className="h-5 w-5" />} />
        <StatCard label="Filing Score" value={filing.overall_score != null ? `${Math.round(filing.overall_score)}%` : "—"} icon={<Clock className="h-5 w-5" />} />
      </div>

      {/* Classification breakdown */}
      <Card>
        <CardHeader><CardTitle>VAT Classification Breakdown</CardTitle></CardHeader>
        <CardContent>
          {cls.by_classification?.length > 0 ? (
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              {cls.by_classification.map((c: Record<string, unknown>) => (
                <div key={String(c.vat_classification)} className="rounded-lg border p-3">
                  <div className="text-xs text-muted-foreground">{String(c.vat_classification)}</div>
                  <div className="text-lg font-semibold">{Number(c.cnt).toLocaleString()}</div>
                  <div className="text-xs text-muted-foreground">{Number(c.total_gross ?? 0).toLocaleString("pl-PL", { style: "currency", currency: "PLN" })}</div>
                </div>
              ))}
            </div>
          ) : <p className="text-sm text-muted-foreground">No classifications yet. Run the pipeline to start.</p>}
        </CardContent>
      </Card>

      {/* Module status cards */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <ModuleCard title="OSS (VIU-DO)" data={d.oss_summary} />
        <ModuleCard title="Local VAT" data={d.local_vat_summary} />
        <ModuleCard title="Evidence Control" data={d.evidence_summary} />
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <ModuleCard title="FBA Movements" data={d.movements_summary} />
        <ModuleCard title="Amazon Clearing" data={d.reconciliation_summary} />
        <ModuleCard title="Filing Readiness" data={d.filing_readiness} />
      </div>
    </div>
  );
}

function StatCard({ label, value, icon, variant = "default" }: { label: string; value: string | number; icon: React.ReactNode; variant?: string }) {
  return (
    <Card>
      <CardContent className="flex items-center gap-3 p-4">
        <div className={variant === "destructive" ? "text-destructive" : "text-muted-foreground"}>{icon}</div>
        <div>
          <div className="text-2xl font-bold">{value}</div>
          <div className="text-xs text-muted-foreground">{label}</div>
        </div>
      </CardContent>
    </Card>
  );
}

function ModuleCard({ title, data }: { title: string; data?: Record<string, unknown> | null }) {
  return (
    <Card>
      <CardHeader className="pb-2"><CardTitle className="text-sm">{title}</CardTitle></CardHeader>
      <CardContent>
        {data ? (
          <div className="space-y-1 text-sm">
            {Object.entries(data).slice(0, 5).map(([k, v]) => (
              <div key={k} className="flex justify-between">
                <span className="text-muted-foreground">{k.replace(/_/g, " ")}</span>
                <span className="font-medium">{typeof v === "number" ? v.toLocaleString() : String(v ?? "—")}</span>
              </div>
            ))}
          </div>
        ) : (
          <Badge variant="outline">No data</Badge>
        )}
      </CardContent>
    </Card>
  );
}
