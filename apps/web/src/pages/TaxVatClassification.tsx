import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { getTaxVatEvents, recomputeClassification, overrideVatClassification } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { RefreshCw } from "lucide-react";
import { useState } from "react";

const CLASSIFICATIONS = ["WSTO", "LOCAL_VAT", "WDT_OWN_GOODS", "WNT_OWN_GOODS", "B2B_WDT", "OUT_OF_SCOPE", "UNCLASSIFIED"];

export default function TaxVatClassificationPage() {
  const qc = useQueryClient();
  const [page, setPage] = useState(1);
  const [classification, setClassification] = useState<string>("");
  const [marketplace, setMarketplace] = useState("");

  const params: Record<string, unknown> = { page, page_size: 50 };
  if (classification) params.classification = classification;
  if (marketplace) params.marketplace = marketplace;

  const { data, isLoading } = useQuery({
    queryKey: ["tax-vat-events", params],
    queryFn: () => getTaxVatEvents(params),
  });

  const recomputeMut = useMutation({
    mutationFn: () => recomputeClassification({ reprocess: true }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tax-vat-events"] }),
  });

  const overrideMut = useMutation({
    mutationFn: ({ id, cls }: { id: number; cls: string }) => overrideVatClassification(id, cls, "manual"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tax-vat-events"] }),
  });

  const items = data?.items ?? [];
  const total = data?.total ?? 0;

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">VAT Classification</h1>
        <Button size="sm" onClick={() => recomputeMut.mutate()} disabled={recomputeMut.isPending}>
          <RefreshCw className={`mr-2 h-4 w-4 ${recomputeMut.isPending ? "animate-spin" : ""}`} />
          Recompute All
        </Button>
      </div>

      {/* Filters */}
      <div className="flex gap-3">
        <Select value={classification} onValueChange={setClassification}>
          <SelectTrigger className="w-48"><SelectValue placeholder="All classifications" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="">All</SelectItem>
            {CLASSIFICATIONS.map(c => <SelectItem key={c} value={c}>{c}</SelectItem>)}
          </SelectContent>
        </Select>
        <Input placeholder="Marketplace" value={marketplace} onChange={e => setMarketplace(e.target.value)} className="w-40" />
      </div>

      <Card>
        <CardHeader><CardTitle>VAT Events ({total.toLocaleString()})</CardTitle></CardHeader>
        <CardContent>
          {isLoading ? <p className="text-sm text-muted-foreground">Loading…</p> : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Order</TableHead>
                  <TableHead>Date</TableHead>
                  <TableHead>MKT</TableHead>
                  <TableHead>Classification</TableHead>
                  <TableHead>Ship From</TableHead>
                  <TableHead>Ship To</TableHead>
                  <TableHead>Net</TableHead>
                  <TableHead>VAT</TableHead>
                  <TableHead>Confidence</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((ev: Record<string, unknown>) => (
                  <TableRow key={String(ev.id)}>
                    <TableCell className="font-mono text-xs">{String(ev.order_id ?? "").slice(-8)}</TableCell>
                    <TableCell className="text-xs">{String(ev.event_date ?? "").slice(0, 10)}</TableCell>
                    <TableCell>{String(ev.marketplace ?? "")}</TableCell>
                    <TableCell>
                      <Badge variant={ev.vat_classification === "UNCLASSIFIED" ? "destructive" : "outline"}>
                        {String(ev.vat_classification ?? "")}
                      </Badge>
                    </TableCell>
                    <TableCell>{String(ev.warehouse_country ?? "")}</TableCell>
                    <TableCell>{String(ev.consumption_country ?? "")}</TableCell>
                    <TableCell className="text-right">{Number(ev.amount_net ?? 0).toFixed(2)}</TableCell>
                    <TableCell className="text-right">{Number(ev.amount_vat ?? 0).toFixed(2)}</TableCell>
                    <TableCell>
                      <Badge variant={Number(ev.confidence_score ?? 0) >= 0.8 ? "default" : "secondary"}>
                        {(Number(ev.confidence_score ?? 0) * 100).toFixed(0)}%
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Select onValueChange={cls => overrideMut.mutate({ id: Number(ev.id), cls })}>
                        <SelectTrigger className="h-7 w-32 text-xs"><SelectValue placeholder="Override…" /></SelectTrigger>
                        <SelectContent>
                          {CLASSIFICATIONS.filter(c => c !== ev.vat_classification).map(c => (
                            <SelectItem key={c} value={c}>{c}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}

          {/* Pagination */}
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
