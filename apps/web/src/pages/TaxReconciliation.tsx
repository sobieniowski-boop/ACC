import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { getTaxReconciliation, runTaxReconciliation, getTaxReconciliationSummary } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { RefreshCw } from "lucide-react";
import { useState } from "react";

export default function TaxReconciliationPage() {
  const qc = useQueryClient();
  const [status, setStatus] = useState("");
  const [page, setPage] = useState(1);

  const params: Record<string, unknown> = { page, page_size: 50 };
  if (status) params.status = status;

  const { data: summary } = useQuery({ queryKey: ["tax-reconciliation-summary"], queryFn: getTaxReconciliationSummary });
  const { data, isLoading } = useQuery({ queryKey: ["tax-reconciliation", params], queryFn: () => getTaxReconciliation(params) });

  const runMut = useMutation({
    mutationFn: () => runTaxReconciliation(60),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["tax-reconciliation"] });
      qc.invalidateQueries({ queryKey: ["tax-reconciliation-summary"] });
    },
  });

  const items = data?.items ?? [];
  const total = data?.total ?? 0;
  const byStatus = summary?.by_status ?? [];

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Amazon Clearing & Reconciliation</h1>
          <p className="text-sm text-muted-foreground">Settlement decomposition and VAT reconciliation</p>
        </div>
        <Button size="sm" onClick={() => runMut.mutate()} disabled={runMut.isPending}>
          <RefreshCw className={`mr-2 h-4 w-4 ${runMut.isPending ? "animate-spin" : ""}`} />
          Run Reconciliation
        </Button>
      </div>

      {/* Status summary */}
      <div className="grid grid-cols-3 gap-4">
        {byStatus.map((s: Record<string, unknown>) => (
          <Card key={String(s.status)}>
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <Badge variant={s.status === "matched" ? "default" : s.status === "mismatch" ? "destructive" : "secondary"}>
                  {String(s.status)}
                </Badge>
                <span className="text-2xl font-bold">{Number(s.count ?? 0)}</span>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Filters */}
      <div className="flex gap-3">
        <Select value={status} onValueChange={v => { setStatus(v); setPage(1); }}>
          <SelectTrigger className="w-40"><SelectValue placeholder="All statuses" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="">All</SelectItem>
            <SelectItem value="matched">Matched</SelectItem>
            <SelectItem value="partial">Partial</SelectItem>
            <SelectItem value="mismatch">Mismatch</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Table */}
      <Card>
        <CardHeader><CardTitle>Settlements ({total.toLocaleString()})</CardTitle></CardHeader>
        <CardContent>
          {isLoading ? <p className="text-sm text-muted-foreground">Loading…</p> : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Settlement</TableHead>
                  <TableHead>Period</TableHead>
                  <TableHead className="text-right">Gross Sales</TableHead>
                  <TableHead className="text-right">VAT OSS</TableHead>
                  <TableHead className="text-right">VAT Local</TableHead>
                  <TableHead className="text-right">Fees</TableHead>
                  <TableHead className="text-right">Refunds</TableHead>
                  <TableHead className="text-right">Payout</TableHead>
                  <TableHead className="text-right">Diff</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((r: Record<string, unknown>, i: number) => (
                  <TableRow key={i}>
                    <TableCell className="font-mono text-xs">{String(r.settlement_id ?? "")}</TableCell>
                    <TableCell className="text-xs">{String(r.period_start ?? "").slice(0, 10)} — {String(r.period_end ?? "").slice(0, 10)}</TableCell>
                    <TableCell className="text-right">{fmt(r.gross_sales)}</TableCell>
                    <TableCell className="text-right">{fmt(r.vat_oss)}</TableCell>
                    <TableCell className="text-right">{fmt(r.vat_local)}</TableCell>
                    <TableCell className="text-right">{fmt(r.amazon_fees)}</TableCell>
                    <TableCell className="text-right">{fmt(r.refunds)}</TableCell>
                    <TableCell className="text-right">{fmt(r.payout_net)}</TableCell>
                    <TableCell className="text-right font-medium">
                      <span className={Number(r.difference_amount ?? 0) > 1 ? "text-destructive" : ""}>
                        {fmt(r.difference_amount)}
                      </span>
                    </TableCell>
                    <TableCell>
                      <Badge variant={r.status === "matched" ? "default" : r.status === "mismatch" ? "destructive" : "secondary"}>
                        {String(r.status ?? "")}
                      </Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
          <div className="mt-4 flex items-center justify-between">
            <span className="text-sm text-muted-foreground">Page {page} of {Math.ceil(total / 50) || 1}</span>
            <div className="flex gap-2">
              <Button variant="outline" size="sm" disabled={page <= 1} onClick={() => setPage(p => p - 1)}>Prev</Button>
              <Button variant="outline" size="sm" disabled={page * 50 >= total} onClick={() => setPage(p => p + 1)}>Next</Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function fmt(v: unknown): string {
  if (v == null) return "—";
  return Number(v).toLocaleString("pl-PL", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
