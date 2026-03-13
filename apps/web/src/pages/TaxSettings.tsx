import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { upsertVatRate, syncEcbRates, runTaxPipeline, getTaxVatRates } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { RefreshCw, Settings } from "lucide-react";
import { useState } from "react";

export default function TaxSettingsPage() {
  const qc = useQueryClient();
  const [country, setCountry] = useState("");
  const [rate, setRate] = useState("");

  const vatRatesQ = useQuery({ queryKey: ["tax-vat-rates"], queryFn: getTaxVatRates });
  const vatRates = (vatRatesQ.data?.items ?? []) as Array<{ country: string; rate: number; valid_from?: string }>;

  const upsertMut = useMutation({
    mutationFn: () => upsertVatRate(country, "standard", parseFloat(rate)),
    onSuccess: () => { setCountry(""); setRate(""); qc.invalidateQueries({ queryKey: ["tax-vat-rates"] }); },
  });

  const ecbMut = useMutation({
    mutationFn: () => syncEcbRates(30),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["tax-ecb-rates"] }),
  });

  const pipelineMut = useMutation({
    mutationFn: () => runTaxPipeline(30),
  });

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center gap-2">
        <Settings className="h-6 w-6" />
        <h1 className="text-2xl font-bold">Tax Compliance Settings</h1>
      </div>

      <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
        {/* VAT rates */}
        <Card>
          <CardHeader><CardTitle>EU VAT Standard Rates</CardTitle></CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Country</TableHead>
                  <TableHead className="text-right">Standard Rate</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {vatRates.sort((a, b) => a.country.localeCompare(b.country)).map((vr) => (
                  <TableRow key={vr.country}>
                    <TableCell className="font-medium">{vr.country}</TableCell>
                    <TableCell className="text-right">{vr.rate}%</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>

            <div className="mt-4 space-y-3 border-t pt-4">
              <h4 className="text-sm font-medium">Override / Add Rate</h4>
              <div className="flex gap-3">
                <div>
                  <Label className="text-xs">Country</Label>
                  <Input placeholder="DE" value={country} onChange={e => setCountry(e.target.value.toUpperCase())} className="w-20" maxLength={2} />
                </div>
                <div>
                  <Label className="text-xs">Rate (%)</Label>
                  <Input placeholder="19" value={rate} onChange={e => setRate(e.target.value)} className="w-20" type="number" />
                </div>
                <div className="flex items-end">
                  <Button size="sm" onClick={() => upsertMut.mutate()} disabled={!country || !rate || upsertMut.isPending}>
                    Save
                  </Button>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Actions */}
        <div className="space-y-4">
          <Card>
            <CardHeader><CardTitle>ECB Exchange Rates</CardTitle></CardHeader>
            <CardContent>
              <p className="mb-3 text-sm text-muted-foreground">Sync latest EUR/PLN and other rates from ECB for OSS reporting.</p>
              <Button size="sm" onClick={() => ecbMut.mutate()} disabled={ecbMut.isPending}>
                <RefreshCw className={`mr-2 h-4 w-4 ${ecbMut.isPending ? "animate-spin" : ""}`} />
                Sync ECB Rates (30d)
              </Button>
              {ecbMut.isSuccess && <p className="mt-2 text-sm text-green-600">ECB rates synced successfully.</p>}
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle>Full Pipeline</CardTitle></CardHeader>
            <CardContent>
              <p className="mb-3 text-sm text-muted-foreground">Run the complete compliance pipeline: classify → evidence → OSS → local VAT → reconciliation → filing readiness → issue detection.</p>
              <Button size="sm" onClick={() => pipelineMut.mutate()} disabled={pipelineMut.isPending}>
                <RefreshCw className={`mr-2 h-4 w-4 ${pipelineMut.isPending ? "animate-spin" : ""}`} />
                Run Full Pipeline
              </Button>
              {pipelineMut.isSuccess && <p className="mt-2 text-sm text-green-600">Pipeline completed successfully.</p>}
              {pipelineMut.isError && <p className="mt-2 text-sm text-destructive">Pipeline failed. Check logs.</p>}
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle>Module Info</CardTitle></CardHeader>
            <CardContent>
              <div className="space-y-1 text-sm">
                <div className="flex justify-between"><span className="text-muted-foreground">Seller Country</span><span>PL</span></div>
                <div className="flex justify-between"><span className="text-muted-foreground">OSS Registration</span><span>VIU-DO (Poland)</span></div>
                <div className="flex justify-between"><span className="text-muted-foreground">Evidence Threshold</span><span>30 days</span></div>
                <div className="flex justify-between"><span className="text-muted-foreground">Unclassified Alert</span><span>&gt;5%</span></div>
                <div className="flex justify-between"><span className="text-muted-foreground">Mismatch Threshold</span><span>&gt;1,000 PLN</span></div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
