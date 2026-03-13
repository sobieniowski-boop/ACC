import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { getTaxOssOverview, getTaxOssPeriod, buildOssPeriod, getTaxOssCorrections } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { RefreshCw } from "lucide-react";
import { useState } from "react";

export default function TaxOssPage() {
  const qc = useQueryClient();
  const [selectedPeriod, setSelectedPeriod] = useState<{ year: number; quarter: number } | null>(null);

  const { data: overview, isLoading } = useQuery({ queryKey: ["tax-oss-overview"], queryFn: getTaxOssOverview });

  const { data: detail } = useQuery({
    queryKey: ["tax-oss-period", selectedPeriod],
    queryFn: () => selectedPeriod ? getTaxOssPeriod(selectedPeriod.year, selectedPeriod.quarter) : null,
    enabled: !!selectedPeriod,
  });

  const { data: corrections } = useQuery({
    queryKey: ["tax-oss-corrections"],
    queryFn: () => getTaxOssCorrections(),
  });

  const buildMut = useMutation({
    mutationFn: () => buildOssPeriod(),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tax-oss"] }),
  });

  const periods = overview?.periods ?? [];
  const stats = overview?.current_quarter ?? {};

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">OSS / VIU-DO Center</h1>
          <p className="text-sm text-muted-foreground">One-Stop Shop return management</p>
        </div>
        <Button size="sm" onClick={() => buildMut.mutate()} disabled={buildMut.isPending}>
          <RefreshCw className={`mr-2 h-4 w-4 ${buildMut.isPending ? "animate-spin" : ""}`} />
          Build Current Period
        </Button>
      </div>

      {/* Current quarter stats */}
      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <StatCard label="Total Taxable EUR" value={fmt(stats.total_taxable_eur)} />
        <StatCard label="Total VAT EUR" value={fmt(stats.total_vat_eur)} />
        <StatCard label="Countries" value={stats.country_count ?? 0} />
        <StatCard label="Transactions" value={stats.transaction_count ?? 0} />
      </div>

      {/* Periods table */}
      <Card>
        <CardHeader><CardTitle>OSS Return Periods</CardTitle></CardHeader>
        <CardContent>
          {isLoading ? <p className="text-muted-foreground text-sm">Loading…</p> : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Period</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Taxable EUR</TableHead>
                  <TableHead className="text-right">VAT EUR</TableHead>
                  <TableHead>Countries</TableHead>
                  <TableHead>Transactions</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {periods.map((p: Record<string, unknown>) => (
                  <TableRow key={String(p.period_ref)} className="cursor-pointer" onClick={() => setSelectedPeriod({ year: Number(p.year), quarter: Number(p.quarter) })}>
                    <TableCell className="font-medium">{String(p.period_ref)}</TableCell>
                    <TableCell><Badge variant={p.status === "filed" ? "default" : "outline"}>{String(p.status)}</Badge></TableCell>
                    <TableCell className="text-right">{fmt(p.total_taxable_eur)}</TableCell>
                    <TableCell className="text-right">{fmt(p.total_vat_eur)}</TableCell>
                    <TableCell>{String(p.country_count ?? "")}</TableCell>
                    <TableCell>{String(p.transaction_count ?? "")}</TableCell>
                    <TableCell><Button variant="ghost" size="sm">Detail</Button></TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* Period detail */}
      {detail?.period && (
        <Card>
          <CardHeader><CardTitle>Period Detail: {detail.period.period_ref}</CardTitle></CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Country</TableHead>
                  <TableHead>VAT Rate</TableHead>
                  <TableHead className="text-right">Taxable EUR</TableHead>
                  <TableHead className="text-right">VAT EUR</TableHead>
                  <TableHead>Orders</TableHead>
                  <TableHead>Correction</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(detail.lines ?? []).map((l: Record<string, unknown>, i: number) => (
                  <TableRow key={i}>
                    <TableCell>{String(l.consumption_country)}</TableCell>
                    <TableCell>{Number(l.vat_rate ?? 0).toFixed(1)}%</TableCell>
                    <TableCell className="text-right">{fmt(l.taxable_amount_eur)}</TableCell>
                    <TableCell className="text-right">{fmt(l.vat_amount_eur)}</TableCell>
                    <TableCell>{String(l.order_count ?? "")}</TableCell>
                    <TableCell>{Number(l.correction_flag) ? <Badge variant="secondary">Correction</Badge> : "—"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {/* Corrections */}
      {(corrections?.items?.length ?? 0) > 0 && (
        <Card>
          <CardHeader><CardTitle>Corrections</CardTitle></CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Period</TableHead>
                  <TableHead>Country</TableHead>
                  <TableHead className="text-right">Taxable EUR</TableHead>
                  <TableHead className="text-right">VAT EUR</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {corrections.items.map((c: Record<string, unknown>, i: number) => (
                  <TableRow key={i}>
                    <TableCell>{String(c.period_ref ?? "")}</TableCell>
                    <TableCell>{String(c.consumption_country)}</TableCell>
                    <TableCell className="text-right">{fmt(c.taxable_amount_eur)}</TableCell>
                    <TableCell className="text-right">{fmt(c.vat_amount_eur)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <Card>
      <CardContent className="p-4">
        <div className="text-2xl font-bold">{value}</div>
        <div className="text-xs text-muted-foreground">{label}</div>
      </CardContent>
    </Card>
  );
}

function fmt(v: unknown): string {
  if (v == null) return "—";
  return Number(v).toLocaleString("de-DE", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
