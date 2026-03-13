import { useQuery } from "@tanstack/react-query";
import { getTaxFbaMovements, getTaxFbaMovementsSummary } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ArrowRight } from "lucide-react";
import { useState } from "react";

export default function TaxFbaMovementsPage() {
  const [status, setStatus] = useState("");
  const [page, setPage] = useState(1);

  const params: Record<string, unknown> = { page, page_size: 50 };
  if (status) params.status = status;

  const { data: summary } = useQuery({ queryKey: ["tax-fba-movements-summary"], queryFn: getTaxFbaMovementsSummary });
  const { data, isLoading } = useQuery({ queryKey: ["tax-fba-movements", params], queryFn: () => getTaxFbaMovements(params) });

  const items = data?.items ?? [];
  const total = data?.total ?? 0;
  const byTreatment = summary?.by_treatment ?? [];
  const routes = summary?.routes ?? [];

  return (
    <div className="space-y-6 p-6">
      <h1 className="text-2xl font-bold">FBA Stock Movements</h1>
      <p className="text-sm text-muted-foreground">Cross-border WDT/WNT tracking for Amazon FBA transfers</p>

      {/* Summary cards */}
      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        {byTreatment.map((t: Record<string, unknown>) => (
          <Card key={String(t.vat_treatment)}>
            <CardContent className="p-4">
              <div className="text-2xl font-bold">{Number(t.count ?? 0).toLocaleString()}</div>
              <div className="text-xs text-muted-foreground">{String(t.vat_treatment ?? "")}</div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Top routes */}
      {routes.length > 0 && (
        <Card>
          <CardHeader><CardTitle className="text-sm">Top Movement Routes</CardTitle></CardHeader>
          <CardContent>
            <div className="flex flex-wrap gap-3">
              {routes.slice(0, 10).map((r: Record<string, unknown>, i: number) => (
                <div key={i} className="flex items-center gap-1 rounded border px-3 py-1 text-sm">
                  <Badge>{String(r.from_country)}</Badge>
                  <ArrowRight className="h-3 w-3 text-muted-foreground" />
                  <Badge>{String(r.to_country)}</Badge>
                  <span className="ml-1 text-muted-foreground">({Number(r.count ?? 0)})</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Filters */}
      <div className="flex gap-3">
        <Select value={status} onValueChange={v => { setStatus(v); setPage(1); }}>
          <SelectTrigger className="w-48"><SelectValue placeholder="All statuses" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="">All</SelectItem>
            <SelectItem value="matched">Matched</SelectItem>
            <SelectItem value="unmatched">Unmatched</SelectItem>
            <SelectItem value="partial">Partial</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Table */}
      <Card>
        <CardHeader><CardTitle>Movements ({total.toLocaleString()})</CardTitle></CardHeader>
        <CardContent>
          {isLoading ? <p className="text-sm text-muted-foreground">Loading…</p> : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Ref</TableHead>
                  <TableHead>Date</TableHead>
                  <TableHead>SKU</TableHead>
                  <TableHead>From</TableHead>
                  <TableHead>To</TableHead>
                  <TableHead>Qty</TableHead>
                  <TableHead>Treatment</TableHead>
                  <TableHead>Match</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((m: Record<string, unknown>, i: number) => (
                  <TableRow key={i}>
                    <TableCell className="font-mono text-xs">{String(m.movement_ref ?? "").slice(-10)}</TableCell>
                    <TableCell className="text-xs">{String(m.movement_date ?? "").slice(0, 10)}</TableCell>
                    <TableCell className="text-xs">{String(m.sku ?? "")}</TableCell>
                    <TableCell><Badge>{String(m.from_country ?? "")}</Badge></TableCell>
                    <TableCell><Badge>{String(m.to_country ?? "")}</Badge></TableCell>
                    <TableCell>{String(m.quantity ?? "")}</TableCell>
                    <TableCell><Badge variant="outline">{String(m.vat_treatment ?? "")}</Badge></TableCell>
                    <TableCell>
                      <Badge variant={m.matching_pair_status === "matched" ? "default" : "destructive"}>
                        {String(m.matching_pair_status ?? "")}
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
