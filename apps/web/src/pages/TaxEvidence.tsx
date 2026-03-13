import { useQuery } from "@tanstack/react-query";
import { getTaxEvidenceList, getTaxEvidenceSummary } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Progress } from "@/components/ui/progress";
import { CheckCircle, AlertTriangle, XCircle } from "lucide-react";
import { useState } from "react";

export default function TaxEvidencePage() {
  const [status, setStatus] = useState("");
  const [page, setPage] = useState(1);

  const params: Record<string, unknown> = { page, page_size: 50 };
  if (status) params.status = status;

  const { data: summary } = useQuery({ queryKey: ["tax-evidence-summary"], queryFn: () => getTaxEvidenceSummary({}) });
  const { data, isLoading } = useQuery({ queryKey: ["tax-evidence", params], queryFn: () => getTaxEvidenceList(params) });

  const items = data?.items ?? [];
  const total = data?.total ?? 0;
  const s = summary ?? {};

  return (
    <div className="space-y-6 p-6">
      <h1 className="text-2xl font-bold">Evidence Control (Art. 22a)</h1>
      <p className="text-sm text-muted-foreground">Transport & delivery evidence for cross-border WSTO transactions</p>

      {/* Summary */}
      <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
        <Card>
          <CardContent className="p-4">
            <div className="text-2xl font-bold">{Number(s.total ?? 0).toLocaleString()}</div>
            <div className="text-xs text-muted-foreground">Total Records</div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-2 p-4">
            <CheckCircle className="h-5 w-5 text-green-500" />
            <div>
              <div className="text-2xl font-bold">{Number(s.complete ?? 0)}</div>
              <div className="text-xs text-muted-foreground">Complete (4/4)</div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-2 p-4">
            <AlertTriangle className="h-5 w-5 text-yellow-500" />
            <div>
              <div className="text-2xl font-bold">{Number(s.partial ?? 0)}</div>
              <div className="text-xs text-muted-foreground">Partial</div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-2 p-4">
            <XCircle className="h-5 w-5 text-red-500" />
            <div>
              <div className="text-2xl font-bold">{Number(s.missing ?? 0)}</div>
              <div className="text-xs text-muted-foreground">Missing</div>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="text-2xl font-bold">{Number(s.completeness_pct ?? 0).toFixed(1)}%</div>
            <Progress value={Number(s.completeness_pct ?? 0)} className="mt-1" />
            <div className="text-xs text-muted-foreground mt-1">Completeness</div>
          </CardContent>
        </Card>
      </div>

      {/* Filter */}
      <div className="flex gap-3">
        <Select value={status} onValueChange={v => { setStatus(v); setPage(1); }}>
          <SelectTrigger className="w-40"><SelectValue placeholder="All statuses" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="">All</SelectItem>
            <SelectItem value="complete">Complete</SelectItem>
            <SelectItem value="partial">Partial</SelectItem>
            <SelectItem value="missing">Missing</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Evidence records */}
      <Card>
        <CardHeader><CardTitle>Evidence Records ({total.toLocaleString()})</CardTitle></CardHeader>
        <CardContent>
          {isLoading ? <p className="text-sm text-muted-foreground">Loading…</p> : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Order</TableHead>
                  <TableHead>Marketplace</TableHead>
                  <TableHead>Transport</TableHead>
                  <TableHead>Delivery</TableHead>
                  <TableHead>Order Proof</TableHead>
                  <TableHead>Payment</TableHead>
                  <TableHead>Proofs</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Carrier</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((r: Record<string, unknown>, i: number) => (
                  <TableRow key={i}>
                    <TableCell className="font-mono text-xs">{String(r.order_id ?? "").slice(-8)}</TableCell>
                    <TableCell>{String(r.marketplace ?? "")}</TableCell>
                    <TableCell>{ProofIcon(r.proof_transport)}</TableCell>
                    <TableCell>{ProofIcon(r.proof_delivery)}</TableCell>
                    <TableCell>{ProofIcon(r.proof_order)}</TableCell>
                    <TableCell>{ProofIcon(r.proof_payment)}</TableCell>
                    <TableCell>{String(r.proofs_collected ?? 0)}/{String(r.proofs_required ?? 4)}</TableCell>
                    <TableCell>
                      <Badge variant={r.evidence_status === "complete" ? "default" : r.evidence_status === "partial" ? "secondary" : "destructive"}>
                        {String(r.evidence_status ?? "")}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-xs">{String(r.carrier ?? "—")}</TableCell>
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

function ProofIcon(val: unknown) {
  return Number(val) === 1
    ? <CheckCircle className="h-4 w-4 text-green-500" />
    : <XCircle className="h-4 w-4 text-red-400" />;
}
