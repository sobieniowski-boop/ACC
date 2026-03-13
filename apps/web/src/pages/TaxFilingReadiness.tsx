import { useQuery } from "@tanstack/react-query";
import { getTaxFilingReadiness, getTaxFilingBlockers } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { CheckCircle, AlertTriangle, XCircle } from "lucide-react";
import { useState } from "react";

const FILING_TYPES = ["oss", "jpk", "local_vat"];

export default function TaxFilingReadinessPage() {
  const [filingType, setFilingType] = useState("");
  const [blockerPage, setBlockerPage] = useState(1);

  const { data, isLoading } = useQuery({ queryKey: ["tax-filing-readiness"], queryFn: () => getTaxFilingReadiness() });
  const { data: blockers } = useQuery({
    queryKey: ["tax-filing-blockers", filingType, blockerPage],
    queryFn: () => getTaxFilingBlockers({ filing_type: filingType || undefined, page: blockerPage, page_size: 30 }),
  });

  const snapshot = data?.snapshot ?? {};
  const topBlockers = data?.blockers ?? [];

  const metrics = [
    { key: "viu_do", label: "VIU-DO (OSS)", score: snapshot.viu_do_score },
    { key: "jpk", label: "JPK (PL)", score: snapshot.jpk_score },
    { key: "local_vat", label: "Local VAT", score: snapshot.local_vat_score },
    { key: "evidence", label: "Evidence", score: snapshot.evidence_score },
    { key: "movements", label: "Movements", score: snapshot.movement_score },
  ];

  return (
    <div className="space-y-6 p-6">
      <h1 className="text-2xl font-bold">Filing Readiness</h1>
      <p className="text-sm text-muted-foreground">Readiness metrics for VIU-DO, JPK, and local VAT filings</p>

      {isLoading ? <p className="text-sm text-muted-foreground">Loading…</p> : (
        <>
          {/* Overall score */}
          <Card>
            <CardContent className="p-6">
              <div className="flex items-center gap-4">
                <div className="text-4xl font-bold">{snapshot.overall_score != null ? `${Math.round(Number(snapshot.overall_score))}%` : "—"}</div>
                <div className="flex-1">
                  <Progress value={Number(snapshot.overall_score ?? 0)} className="h-3" />
                  <div className="mt-1 text-sm text-muted-foreground">Overall Filing Readiness — {snapshot.period_ref ?? ""}</div>
                </div>
                {ScoreIcon(Number(snapshot.overall_score ?? 0))}
              </div>
            </CardContent>
          </Card>

          {/* Metric cards */}
          <div className="grid grid-cols-1 gap-4 md:grid-cols-5">
            {metrics.map(m => (
              <Card key={m.key}>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    {ScoreIcon(Number(m.score ?? 0))}
                    <div className="text-2xl font-bold">{m.score != null ? `${Math.round(Number(m.score))}%` : "—"}</div>
                  </div>
                  <Progress value={Number(m.score ?? 0)} className="mt-2 h-2" />
                  <div className="mt-1 text-xs text-muted-foreground">{m.label}</div>
                </CardContent>
              </Card>
            ))}
          </div>

          {/* Top blockers from snapshot */}
          {topBlockers.length > 0 && (
            <Card>
              <CardHeader><CardTitle>Top Blockers</CardTitle></CardHeader>
              <CardContent>
                <div className="space-y-2">
                  {topBlockers.map((b: Record<string, unknown>, i: number) => (
                    <div key={i} className="flex items-center gap-2 rounded border p-2">
                      <Badge variant={b.severity === "P1" ? "destructive" : "secondary"}>{String(b.severity)}</Badge>
                      <span className="text-sm">{String(b.description ?? "")}</span>
                      {b.country ? <Badge variant="outline">{String(b.country)}</Badge> : null}
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}

      {/* Detailed blockers */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>All Blockers</CardTitle>
            <Select value={filingType} onValueChange={v => { setFilingType(v); setBlockerPage(1); }}>
              <SelectTrigger className="w-40"><SelectValue placeholder="All types" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="">All</SelectItem>
                {FILING_TYPES.map(t => <SelectItem key={t} value={t}>{t.toUpperCase()}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Type</TableHead>
                <TableHead>Severity</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Country</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(blockers?.items ?? []).map((b: Record<string, unknown>, i: number) => (
                <TableRow key={i}>
                  <TableCell>{String(b.issue_type ?? "")}</TableCell>
                  <TableCell><Badge variant={b.severity === "P1" ? "destructive" : "secondary"}>{String(b.severity)}</Badge></TableCell>
                  <TableCell className="text-sm">{String(b.description ?? "")}</TableCell>
                  <TableCell>{String(b.country ?? "—")}</TableCell>
                  <TableCell><Badge variant="outline">{String(b.status ?? "")}</Badge></TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          <div className="mt-4 flex items-center justify-between">
            <span className="text-sm text-muted-foreground">Page {blockerPage}</span>
            <div className="flex gap-2">
              <Button variant="outline" size="sm" disabled={blockerPage <= 1} onClick={() => setBlockerPage(p => p - 1)}>Prev</Button>
              <Button variant="outline" size="sm" onClick={() => setBlockerPage(p => p + 1)}>Next</Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function ScoreIcon(score: number) {
  if (score >= 80) return <CheckCircle className="h-5 w-5 text-green-500" />;
  if (score >= 50) return <AlertTriangle className="h-5 w-5 text-yellow-500" />;
  return <XCircle className="h-5 w-5 text-red-500" />;
}
