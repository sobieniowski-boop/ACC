import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { getTaxAuditArchive, generateTaxAuditPack } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { FileArchive, RefreshCw } from "lucide-react";

export default function TaxAuditArchivePage() {
  const qc = useQueryClient();
  const { data, isLoading } = useQuery({ queryKey: ["tax-audit-archive"], queryFn: getTaxAuditArchive });

  const genMut = useMutation({
    mutationFn: () => generateTaxAuditPack("quarter"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tax-audit-archive"] }),
  });

  const packs = data?.months_with_data ?? [];
  const snapshots = data?.readiness_snapshots ?? [];

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Audit Archive</h1>
          <p className="text-sm text-muted-foreground">Complete audit packs for tax authority review</p>
        </div>
        <Button size="sm" onClick={() => genMut.mutate()} disabled={genMut.isPending}>
          <RefreshCw className={`mr-2 h-4 w-4 ${genMut.isPending ? "animate-spin" : ""}`} />
          Generate Current Pack
        </Button>
      </div>

      {isLoading ? <p className="text-sm text-muted-foreground">Loading…</p> : (
        <>
          {/* Available periods */}
          <Card>
            <CardHeader><CardTitle>Available Periods</CardTitle></CardHeader>
            <CardContent>
              {(data?.available_periods ?? []).length > 0 ? (
                <div className="flex flex-wrap gap-2">
                  {(data?.available_periods ?? []).map((p: string) => (
                    <Badge key={p} variant="outline" className="px-3 py-1 text-sm">
                      <FileArchive className="mr-1 h-3 w-3" />
                      {p}
                    </Badge>
                  ))}
                </div>
              ) : <p className="text-sm text-muted-foreground">No audit packs generated yet.</p>}
            </CardContent>
          </Card>

          {/* Months with data */}
          {packs.length > 0 && (
            <Card>
              <CardHeader><CardTitle>Months with VAT Data</CardTitle></CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2">
                  {packs.map((m: Record<string, unknown>) => (
                    <div key={String(m.month)} className="rounded border px-3 py-2 text-center">
                      <div className="text-sm font-medium">{String(m.month)}</div>
                      <div className="text-xs text-muted-foreground">{Number(m.event_count ?? 0)} events</div>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Filing readiness snapshots */}
          {snapshots.length > 0 && (
            <Card>
              <CardHeader><CardTitle>Filing Readiness Snapshots</CardTitle></CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Period</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Overall Score</TableHead>
                      <TableHead>VIU-DO</TableHead>
                      <TableHead>JPK</TableHead>
                      <TableHead>Local VAT</TableHead>
                      <TableHead>Evidence</TableHead>
                      <TableHead>Movements</TableHead>
                      <TableHead>Created</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {snapshots.map((s: Record<string, unknown>, i: number) => (
                      <TableRow key={i}>
                        <TableCell className="font-medium">{String(s.period_ref ?? "")}</TableCell>
                        <TableCell>{String(s.period_type ?? "")}</TableCell>
                        <TableCell>
                          <Badge variant={Number(s.overall_score ?? 0) >= 80 ? "default" : "destructive"}>
                            {Number(s.overall_score ?? 0).toFixed(0)}%
                          </Badge>
                        </TableCell>
                        <TableCell>{score(s.viu_do_score)}</TableCell>
                        <TableCell>{score(s.jpk_score)}</TableCell>
                        <TableCell>{score(s.local_vat_score)}</TableCell>
                        <TableCell>{score(s.evidence_score)}</TableCell>
                        <TableCell>{score(s.movement_score)}</TableCell>
                        <TableCell className="text-xs">{String(s.created_at ?? "").slice(0, 16)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}

function score(v: unknown): string {
  if (v == null) return "—";
  return `${Number(v).toFixed(0)}%`;
}
