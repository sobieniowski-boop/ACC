import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFinanceManualLedgerEntry, getFinanceLedger, reverseFinanceLedgerEntry } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { formatPLN } from "@/lib/utils";
import { ClientExportButton } from "@/components/shared";

export default function FinanceLedgerPage() {
  const qc = useQueryClient();
  const [sku, setSku] = useState("");
  const { data } = useQuery({
    queryKey: ["finance-ledger", sku],
    queryFn: () => getFinanceLedger(sku ? { sku } : {}),
  });

  const createMut = useMutation({
    mutationFn: () =>
      createFinanceManualLedgerEntry({
        entry_date: new Date().toISOString().slice(0, 10),
        currency: "PLN",
        amount: 0,
        account_code: "580",
        description: "Manual placeholder entry",
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["finance-ledger"] }),
  });

  const reverseMut = useMutation({
    mutationFn: (entryId: string) => reverseFinanceLedgerEntry(entryId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["finance-ledger"] }),
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Ledger Explorer</h1>
          <p className="text-sm text-white/50">Kanoniczny ledger z feedu Amazon fees, charge i tax.</p>
        </div>
        <div className="flex gap-2">
          {data?.items && <ClientExportButton data={data.items.slice(0, 200)} filename="finance_ledger" />}
          <Input value={sku} onChange={(e) => setSku(e.target.value)} placeholder="Filtruj SKU" className="w-44" />
          <Button onClick={() => createMut.mutate()} disabled={createMut.isPending} variant="secondary">
            {createMut.isPending ? "Creating..." : "Manual entry"}
          </Button>
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Ledger rows</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Data</TableHead>
                <TableHead>Opis</TableHead>
                <TableHead>MP</TableHead>
                <TableHead>Konto</TableHead>
                <TableHead>Grupa platnosci</TableHead>
                <TableHead>SKU</TableHead>
                <TableHead className="text-right">Amount base</TableHead>
                <TableHead>Akcje</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(data?.items ?? []).slice(0, 200).map((item) => (
                <TableRow key={item.id}>
                  <TableCell>{item.entry_date}</TableCell>
                  <TableCell>
                    <div className="max-w-[24rem] truncate text-sm font-medium text-white">{item.description ?? item.charge_type ?? item.source_ref}</div>
                    <div className="text-[11px] text-white/45">{item.transaction_type ?? item.source}</div>
                  </TableCell>
                  <TableCell>{item.marketplace_code ?? "-"}</TableCell>
                  <TableCell>{item.account_code}</TableCell>
                  <TableCell className="font-mono text-[11px]">{item.financial_event_group_id ?? item.settlement_id ?? "-"}</TableCell>
                  <TableCell className="font-mono text-xs">{item.sku ?? "-"}</TableCell>
                  <TableCell className="text-right">{formatPLN(item.amount_base)}</TableCell>
                  <TableCell>
                    <Button size="sm" variant="secondary" onClick={() => reverseMut.mutate(item.id)} disabled={reverseMut.isPending}>
                      Reverse
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
              {(data?.items.length ?? 0) === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="text-center text-white/50">
                    Brak rekordow ledgera dla biezacych filtrow.
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
