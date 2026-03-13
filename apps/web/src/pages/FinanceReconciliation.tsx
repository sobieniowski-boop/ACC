import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { autoMatchFinancePayouts, getFinancePayoutReconciliation } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { formatPLN } from "@/lib/utils";
import { ClientExportButton } from "@/components/shared";

export default function FinanceReconciliationPage() {
  const qc = useQueryClient();
  const { data } = useQuery({
    queryKey: ["finance-payout-reconciliation"],
    queryFn: () => getFinancePayoutReconciliation(),
  });

  const autoMatchMut = useMutation({
    mutationFn: autoMatchFinancePayouts,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["finance-payout-reconciliation"] }),
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Reconciliation</h1>
          <p className="text-sm text-white/50">Payout vs bank. Uzgodnienie jest budowane po `FinancialEventGroupId` zgodnie z modelem Amazon.</p>
        </div>
        <Button onClick={() => autoMatchMut.mutate()} disabled={autoMatchMut.isPending}>
          {autoMatchMut.isPending ? "Matching..." : "Auto-match"}
        </Button>
        {data?.items && <ClientExportButton data={data.items} filename="finance_reconciliation" />}
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Payout vs Bank</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Grupa platnosci</TableHead>
                <TableHead>MP</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Expected</TableHead>
                <TableHead className="text-right">Matched</TableHead>
                <TableHead className="text-right">Diff</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(data?.items ?? []).map((item) => (
                <TableRow key={`${item.financial_event_group_id ?? item.settlement_id}-${item.id ?? "x"}`}>
                  <TableCell>
                    <div className="font-mono text-xs">{item.financial_event_group_id ?? item.settlement_id}</div>
                    <div className="text-[11px] text-white/45">alias settlement: {item.settlement_id ?? "-"}</div>
                  </TableCell>
                  <TableCell>{item.marketplace_code ?? "-"}</TableCell>
                  <TableCell>{item.status}</TableCell>
                  <TableCell className="text-right">{formatPLN(item.total_amount_base)}</TableCell>
                  <TableCell className="text-right">{formatPLN(item.matched_amount ?? 0)}</TableCell>
                  <TableCell className="text-right">{formatPLN(item.diff_amount ?? 0)}</TableCell>
                </TableRow>
              ))}
              {(data?.items.length ?? 0) === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center text-white/50">
                    Brak grup platnosci do uzgodnienia. To znaczy, ze feed finansowy nie zapisal jeszcze `FinancialEventGroupId` dla tego zakresu.
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
