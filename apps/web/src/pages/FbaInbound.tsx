import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import {
  createFbaShipmentPlan,
  deleteFbaShipmentPlan,
  getFbaInboundShipmentDetail,
  getFbaInboundShipments,
  getFbaShipmentPlans,
  updateFbaShipmentPlan,
} from "@/lib/api";
import { FbaJobStatusStrip } from "@/components/fba/FbaJobStatusStrip";
import { ImportCSVDialog } from "@/components/fba/ImportCSVDialog";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ClientExportButton } from "@/components/shared";

function currentQuarter() {
  const now = new Date();
  const quarter = Math.floor(now.getMonth() / 3) + 1;
  return `${now.getFullYear()}-Q${quarter}`;
}

export default function FbaInboundPage() {
  const qc = useQueryClient();
  const [searchParams] = useSearchParams();
  const quarter = currentQuarter();
  const [selectedPlanId, setSelectedPlanId] = useState<string | null>(null);
  const [selectedShipmentId, setSelectedShipmentId] = useState<string | null>(searchParams.get("shipment_id"));
  const [createForm, setCreateForm] = useState({
    quarter,
    marketplace_id: "A1PA6795UKMFR9",
    shipment_id: "",
    plan_week_start: new Date().toISOString().slice(0, 10),
    planned_ship_date: new Date().toISOString().slice(0, 10),
    planned_units: "0",
    owner: "",
  });

  const { data: inbound } = useQuery({
    queryKey: ["fba-inbound"],
    queryFn: () => getFbaInboundShipments(),
  });

  const { data: shipmentDetail } = useQuery({
    queryKey: ["fba-inbound-detail", selectedShipmentId],
    queryFn: () => getFbaInboundShipmentDetail(selectedShipmentId!),
    enabled: !!selectedShipmentId,
  });

  const { data: plans } = useQuery({
    queryKey: ["fba-shipment-plans", quarter],
    queryFn: () => getFbaShipmentPlans({ quarter }),
  });

  const selectedPlan = useMemo(
    () => (plans?.items ?? []).find((item) => item.id === selectedPlanId) ?? null,
    [plans?.items, selectedPlanId]
  );

  const createMut = useMutation({
    mutationFn: () =>
      createFbaShipmentPlan({
        quarter: createForm.quarter,
        marketplace_id: createForm.marketplace_id,
        shipment_id: createForm.shipment_id || undefined,
        plan_week_start: createForm.plan_week_start,
        planned_ship_date: createForm.planned_ship_date || undefined,
        planned_units: Number(createForm.planned_units || 0),
        status: "planned",
        owner: createForm.owner || undefined,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["fba-shipment-plans"] });
      setCreateForm((prev) => ({ ...prev, shipment_id: "", planned_units: "0", owner: "" }));
    },
  });

  const updateMut = useMutation({
    mutationFn: ({ id, payload }: { id: string; payload: Record<string, unknown> }) => updateFbaShipmentPlan(id, payload),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["fba-shipment-plans"] }),
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => deleteFbaShipmentPlan(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["fba-shipment-plans"] });
      setSelectedPlanId(null);
    },
  });

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Inbound Control Tower</h1>
          <p className="text-sm text-white/50">Live shipments from Amazon plus weekly shipment plan register for adherence KPI.</p>
        </div>
        <div className="flex items-center gap-2">
          {inbound?.items && <ClientExportButton data={inbound.items} filename="fba_inbound_shipments" />}
          <ImportCSVDialog registerType="shipment_plan" quarter={quarter} invalidateKeys={[["fba-shipment-plans"]]} buttonLabel="Import Plans CSV" />
        </div>
      </div>

      <FbaJobStatusStrip />

      <div className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Live Inbound Shipments</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Shipment</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Warehouse</TableHead>
                  <TableHead className="text-right">Planned</TableHead>
                  <TableHead className="text-right">Received</TableHead>
                  <TableHead className="text-right">Days in status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(inbound?.items ?? []).map((item) => (
                  <TableRow key={item.shipment_id} onClick={() => setSelectedShipmentId(item.shipment_id)} className={selectedShipmentId === item.shipment_id ? "bg-white/5" : ""}>
                    <TableCell>
                      <div className="font-mono text-xs">{item.shipment_id}</div>
                      <div className="text-[11px] text-white/45">{item.shipment_name ?? "-"}</div>
                    </TableCell>
                    <TableCell>{item.status}</TableCell>
                    <TableCell>{item.from_warehouse ?? "-"}</TableCell>
                    <TableCell className="text-right">{item.units_planned}</TableCell>
                    <TableCell className="text-right">{item.units_received}</TableCell>
                    <TableCell className="text-right">{item.days_in_status}</TableCell>
                  </TableRow>
                ))}
                {(inbound?.items?.length ?? 0) === 0 && (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center text-white/50">
                      No inbound shipments in synced feed. Verify inbound sync completeness before treating this as a real zero.
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Shipment Detail</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {!shipmentDetail ? (
              <div className="text-sm text-white/50">Select a shipment to inspect line-level planned vs received quantities.</div>
            ) : (
              <>
                <div className="rounded border border-white/10 p-3 text-sm">
                  <div className="font-mono text-xs">{shipmentDetail.shipment.shipment_id}</div>
                  <div className="mt-1 text-white/70">{shipmentDetail.shipment.shipment_name ?? "-"}</div>
                  <div className="mt-2 grid gap-2 text-xs text-white/55 md:grid-cols-2">
                    <div>Status: {shipmentDetail.shipment.status}</div>
                    <div>Warehouse: {shipmentDetail.shipment.from_warehouse ?? "-"}</div>
                    <div>Planned: {shipmentDetail.shipment.units_planned}</div>
                    <div>Received: {shipmentDetail.shipment.units_received}</div>
                  </div>
                </div>
                <div className="max-h-[26rem] overflow-auto rounded border border-white/10">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Produkt</TableHead>
                        <TableHead>SKU</TableHead>
                        <TableHead>ASIN</TableHead>
                        <TableHead className="text-right">Planned</TableHead>
                        <TableHead className="text-right">Received</TableHead>
                        <TableHead className="text-right">Variance</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {shipmentDetail.lines.map((line) => (
                        <TableRow key={`${line.sku}-${line.asin ?? ""}`}>
                          <TableCell>
                            <div className="max-w-[22rem] truncate text-sm font-medium text-white">
                              {line.title_preferred ?? line.sku}
                            </div>
                          </TableCell>
                          <TableCell className="font-mono text-xs">{line.sku}</TableCell>
                          <TableCell className="font-mono text-xs">{line.asin ?? "-"}</TableCell>
                          <TableCell className="text-right">{line.qty_planned}</TableCell>
                          <TableCell className="text-right">{line.qty_received}</TableCell>
                          <TableCell className="text-right">{line.variance_units}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 xl:grid-cols-[1.3fr_0.7fr]">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Shipment Plan Register</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Shipment</TableHead>
                  <TableHead>Week</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Planned</TableHead>
                  <TableHead className="text-right">Actual</TableHead>
                  <TableHead>Owner</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(plans?.items ?? []).map((item) => (
                  <TableRow key={item.id} onClick={() => setSelectedPlanId(item.id)} className={selectedPlanId === item.id ? "bg-white/5" : ""}>
                    <TableCell>
                      <div className="font-mono text-xs">{item.shipment_id ?? "-"}</div>
                      <div className="text-[11px] text-white/45">{item.marketplace_code ?? item.marketplace_id ?? "-"}</div>
                    </TableCell>
                    <TableCell>{item.plan_week_start}</TableCell>
                    <TableCell>{item.status}</TableCell>
                    <TableCell className="text-right">{item.planned_units}</TableCell>
                    <TableCell className="text-right">{item.actual_units ?? "-"}</TableCell>
                    <TableCell>{item.owner ?? "-"}</TableCell>
                  </TableRow>
                ))}
                {(plans?.items?.length ?? 0) === 0 && (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center text-white/50">
                      No shipment plans yet.
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">{selectedPlan ? "Plan Detail" : "Add Shipment Plan"}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {!selectedPlan ? (
              <>
                <input value={createForm.quarter} onChange={(e) => setCreateForm((prev) => ({ ...prev, quarter: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="2026-Q1" />
                <input value={createForm.marketplace_id} onChange={(e) => setCreateForm((prev) => ({ ...prev, marketplace_id: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Marketplace ID" />
                <input value={createForm.shipment_id} onChange={(e) => setCreateForm((prev) => ({ ...prev, shipment_id: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Shipment ID" />
                <div className="grid gap-3 md:grid-cols-2">
                  <input type="date" value={createForm.plan_week_start} onChange={(e) => setCreateForm((prev) => ({ ...prev, plan_week_start: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" />
                  <input type="date" value={createForm.planned_ship_date} onChange={(e) => setCreateForm((prev) => ({ ...prev, planned_ship_date: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" />
                </div>
                <div className="grid gap-3 md:grid-cols-2">
                  <input value={createForm.planned_units} onChange={(e) => setCreateForm((prev) => ({ ...prev, planned_units: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Planned units" />
                  <input value={createForm.owner} onChange={(e) => setCreateForm((prev) => ({ ...prev, owner: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Owner" />
                </div>
                <button onClick={() => createMut.mutate()} className="rounded border border-white/10 px-3 py-2 text-sm">
                  Save shipment plan
                </button>
              </>
            ) : (
              <>
                <div className="text-xs text-white/45">{selectedPlan.shipment_id ?? "manual plan"}</div>
                <div className="grid gap-3">
                  <input defaultValue={selectedPlan.owner ?? ""} onBlur={(e) => updateMut.mutate({ id: selectedPlan.id, payload: { owner: e.target.value || null } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Owner" />
                  <input defaultValue={selectedPlan.actual_units ?? ""} onBlur={(e) => updateMut.mutate({ id: selectedPlan.id, payload: { actual_units: Number(e.target.value || 0) } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Actual units" />
                  <input type="date" defaultValue={selectedPlan.actual_ship_date ?? ""} onBlur={(e) => updateMut.mutate({ id: selectedPlan.id, payload: { actual_ship_date: e.target.value || null } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" />
                  <select defaultValue={selectedPlan.status} onChange={(e) => updateMut.mutate({ id: selectedPlan.id, payload: { status: e.target.value } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm">
                    {["planned", "shipped", "partial", "closed", "cancelled"].map((status) => (
                      <option key={status} value={status}>{status}</option>
                    ))}
                  </select>
                </div>
                <button onClick={() => deleteMut.mutate(selectedPlan.id)} className="rounded border border-red-500/20 px-3 py-2 text-sm text-red-300">
                  Delete plan
                </button>
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
