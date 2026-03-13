import { useQuery } from "@tanstack/react-query";
import { getTaxLocalVat, getTaxLocalVatSummary } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useState } from "react";

const EU_COUNTRIES = ["DE", "FR", "IT", "ES", "NL", "SE", "BE", "AT", "CZ", "PL"];

export default function TaxLocalVatPage() {
  const [country, setCountry] = useState("");
  const [page, setPage] = useState(1);

  const params: Record<string, unknown> = { page, page_size: 50 };
  if (country) params.country = country;

  const { data: summary } = useQuery({ queryKey: ["tax-local-vat-summary"], queryFn: getTaxLocalVatSummary });
  const { data, isLoading } = useQuery({ queryKey: ["tax-local-vat", params], queryFn: () => getTaxLocalVat(params) });

  const items = data?.items ?? [];
  const total = data?.total ?? 0;
  const countries = summary?.by_country ?? [];

  return (
    <div className="space-y-6 p-6">
      <h1 className="text-2xl font-bold">Local VAT Ledger</h1>
      <p className="text-sm text-muted-foreground">Sales via FBA warehouses requiring local VAT registration</p>

      {/* Summary by country */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        {countries.map((c: Record<string, unknown>) => (
          <Card key={String(c.country)} className="cursor-pointer hover:border-amazon" onClick={() => { setCountry(String(c.country)); setPage(1); }}>
            <CardContent className="p-3">
              <div className="flex items-center justify-between">
                <span className="text-lg font-bold">{String(c.country)}</span>
                <Badge variant="outline">{Number(c.transaction_count ?? 0)} txns</Badge>
              </div>
              <div className="text-sm">Net: {Number(c.total_net ?? 0).toLocaleString("pl-PL", { style: "currency", currency: String(c.currency ?? "EUR") })}</div>
              <div className="text-xs text-muted-foreground">VAT: {Number(c.total_vat ?? 0).toLocaleString("pl-PL", { style: "currency", currency: String(c.currency ?? "EUR") })}</div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Filters */}
      <div className="flex gap-3">
        <Select value={country} onValueChange={v => { setCountry(v); setPage(1); }}>
          <SelectTrigger className="w-40"><SelectValue placeholder="All countries" /></SelectTrigger>
          <SelectContent>
            <SelectItem value="">All</SelectItem>
            {EU_COUNTRIES.map(c => <SelectItem key={c} value={c}>{c}</SelectItem>)}
          </SelectContent>
        </Select>
      </div>

      {/* Table */}
      <Card>
        <CardHeader><CardTitle>Local VAT Entries ({total.toLocaleString()})</CardTitle></CardHeader>
        <CardContent>
          {isLoading ? <p className="text-sm text-muted-foreground">Loading…</p> : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Order</TableHead>
                  <TableHead>Date</TableHead>
                  <TableHead>Country</TableHead>
                  <TableHead>SKU</TableHead>
                  <TableHead className="text-right">Net</TableHead>
                  <TableHead className="text-right">VAT</TableHead>
                  <TableHead>Rate</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((row: Record<string, unknown>, i: number) => (
                  <TableRow key={i}>
                    <TableCell className="font-mono text-xs">{String(row.order_id ?? "").slice(-8)}</TableCell>
                    <TableCell className="text-xs">{String(row.event_date ?? "").slice(0, 10)}</TableCell>
                    <TableCell><Badge>{String(row.warehouse_country ?? "")}</Badge></TableCell>
                    <TableCell className="text-xs">{String(row.sku ?? "")}</TableCell>
                    <TableCell className="text-right">{Number(row.amount_net ?? 0).toFixed(2)}</TableCell>
                    <TableCell className="text-right">{Number(row.amount_vat ?? 0).toFixed(2)}</TableCell>
                    <TableCell>{Number(row.tax_rate ?? 0).toFixed(1)}%</TableCell>
                    <TableCell><Badge variant="outline">{String(row.filing_status ?? "pending")}</Badge></TableCell>
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
