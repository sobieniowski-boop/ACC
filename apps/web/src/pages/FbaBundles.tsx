import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  createFbaInitiative,
  createFbaLaunch,
  deleteFbaInitiative,
  deleteFbaLaunch,
  getFbaInitiatives,
  getFbaLaunches,
  updateFbaInitiative,
  updateFbaLaunch,
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

export default function FbaBundlesPage() {
  const qc = useQueryClient();
  const quarter = currentQuarter();
  const [selectedLaunchId, setSelectedLaunchId] = useState<string | null>(null);
  const [selectedInitiativeId, setSelectedInitiativeId] = useState<string | null>(null);
  const [launchForm, setLaunchForm] = useState({
    quarter,
    launch_type: "bundle",
    sku: "",
    marketplace_id: "A1PA6795UKMFR9",
    planned_go_live_date: new Date().toISOString().slice(0, 10),
    owner: "",
  });
  const [initiativeForm, setInitiativeForm] = useState({
    quarter,
    initiative_type: "bundle",
    title: "",
    sku: "",
    owner: "",
  });

  const { data: launches } = useQuery({
    queryKey: ["fba-launches", quarter],
    queryFn: () => getFbaLaunches({ quarter }),
  });
  const { data: initiatives } = useQuery({
    queryKey: ["fba-initiatives", quarter],
    queryFn: () => getFbaInitiatives({ quarter }),
  });

  const selectedLaunch = useMemo(
    () => (launches?.items ?? []).find((item) => item.id === selectedLaunchId) ?? null,
    [launches?.items, selectedLaunchId]
  );
  const selectedInitiative = useMemo(
    () => (initiatives?.items ?? []).find((item) => item.id === selectedInitiativeId) ?? null,
    [initiatives?.items, selectedInitiativeId]
  );

  const createLaunchMut = useMutation({
    mutationFn: () => createFbaLaunch(launchForm),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["fba-launches"] });
      setLaunchForm((prev) => ({ ...prev, sku: "", owner: "" }));
    },
  });
  const updateLaunchMut = useMutation({
    mutationFn: ({ id, payload }: { id: string; payload: Record<string, unknown> }) => updateFbaLaunch(id, payload),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["fba-launches"] }),
  });
  const deleteLaunchMut = useMutation({
    mutationFn: (id: string) => deleteFbaLaunch(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["fba-launches"] });
      setSelectedLaunchId(null);
    },
  });

  const createInitiativeMut = useMutation({
    mutationFn: () => createFbaInitiative(initiativeForm),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["fba-initiatives"] });
      setInitiativeForm((prev) => ({ ...prev, title: "", sku: "", owner: "" }));
    },
  });
  const updateInitiativeMut = useMutation({
    mutationFn: ({ id, payload }: { id: string; payload: Record<string, unknown> }) => updateFbaInitiative(id, payload),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["fba-initiatives"] }),
  });
  const deleteInitiativeMut = useMutation({
    mutationFn: (id: string) => deleteFbaInitiative(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["fba-initiatives"] });
      setSelectedInitiativeId(null);
    },
  });

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold text-white">Bundles / New Products</h1>
          <p className="text-sm text-white/50">Launch register and quarterly initiative plan for on-time launch, Vine coverage and completion KPIs.</p>
        </div>
        <div className="flex items-center gap-2">
          {launches?.items && <ClientExportButton data={launches.items} filename="fba_launches" />}
          <ImportCSVDialog registerType="launch" quarter={quarter} invalidateKeys={[["fba-launches"]]} buttonLabel="Import Launches" />
          <ImportCSVDialog registerType="initiative" quarter={quarter} invalidateKeys={[["fba-initiatives"]]} buttonLabel="Import Initiatives" />
        </div>
      </div>
      <FbaJobStatusStrip />

      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader><CardTitle className="text-sm">Launch Register</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            <div className="grid gap-3 md:grid-cols-2">
              <input value={launchForm.quarter} onChange={(e) => setLaunchForm((prev) => ({ ...prev, quarter: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Quarter" />
              <input value={launchForm.marketplace_id} onChange={(e) => setLaunchForm((prev) => ({ ...prev, marketplace_id: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Marketplace ID" />
            </div>
            <div className="grid gap-3 md:grid-cols-2">
              <select value={launchForm.launch_type} onChange={(e) => setLaunchForm((prev) => ({ ...prev, launch_type: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm">
                {["bundle", "new_sku", "new_product"].map((value) => <option key={value} value={value}>{value}</option>)}
              </select>
              <input value={launchForm.sku} onChange={(e) => setLaunchForm((prev) => ({ ...prev, sku: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="SKU" />
            </div>
            <div className="grid gap-3 md:grid-cols-2">
              <input type="date" value={launchForm.planned_go_live_date} onChange={(e) => setLaunchForm((prev) => ({ ...prev, planned_go_live_date: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" />
              <input value={launchForm.owner} onChange={(e) => setLaunchForm((prev) => ({ ...prev, owner: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Owner" />
            </div>
            <button onClick={() => createLaunchMut.mutate()} className="rounded border border-white/10 px-3 py-2 text-sm">Save launch</button>

            <Table>
              <TableHeader><TableRow><TableHead>SKU</TableHead><TableHead>Status</TableHead><TableHead>Plan</TableHead><TableHead>Owner</TableHead></TableRow></TableHeader>
              <TableBody>
                {(launches?.items ?? []).map((item) => (
                  <TableRow key={item.id} onClick={() => setSelectedLaunchId(item.id)} className={selectedLaunchId === item.id ? "bg-white/5" : ""}>
                    <TableCell className="font-mono text-xs">{item.sku ?? "-"}</TableCell>
                    <TableCell>{item.status}</TableCell>
                    <TableCell>{item.planned_go_live_date ?? "-"}</TableCell>
                    <TableCell>{item.owner ?? "-"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>

            {selectedLaunch ? (
              <div className="space-y-3 rounded border border-white/10 p-3">
                <select defaultValue={selectedLaunch.status} onChange={(e) => updateLaunchMut.mutate({ id: selectedLaunch.id, payload: { status: e.target.value } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm">
                  {["planned", "go_live", "live_stable", "blocked"].map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
                <input type="date" defaultValue={selectedLaunch.actual_go_live_date ?? ""} onBlur={(e) => updateLaunchMut.mutate({ id: selectedLaunch.id, payload: { actual_go_live_date: e.target.value || null } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" />
                <input type="date" defaultValue={selectedLaunch.live_stable_at ?? ""} onBlur={(e) => updateLaunchMut.mutate({ id: selectedLaunch.id, payload: { live_stable_at: e.target.value || null } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" />
                <input type="date" defaultValue={selectedLaunch.vine_submitted_at ?? ""} onBlur={(e) => updateLaunchMut.mutate({ id: selectedLaunch.id, payload: { vine_submitted_at: e.target.value || null } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" />
                <label className="flex items-center gap-2 text-sm text-white/70">
                  <input type="checkbox" defaultChecked={selectedLaunch.vine_eligible} onChange={(e) => updateLaunchMut.mutate({ id: selectedLaunch.id, payload: { vine_eligible: e.target.checked } })} />
                  Vine eligible
                </label>
                <label className="flex items-center gap-2 text-sm text-white/70">
                  <input type="checkbox" defaultChecked={selectedLaunch.incident_free} onChange={(e) => updateLaunchMut.mutate({ id: selectedLaunch.id, payload: { incident_free: e.target.checked } })} />
                  Incident free
                </label>
                <button onClick={() => deleteLaunchMut.mutate(selectedLaunch.id)} className="rounded border border-red-500/20 px-3 py-2 text-sm text-red-300">Delete launch</button>
              </div>
            ) : null}
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle className="text-sm">Quarterly Initiatives</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            <input value={initiativeForm.quarter} onChange={(e) => setInitiativeForm((prev) => ({ ...prev, quarter: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Quarter" />
            <div className="grid gap-3 md:grid-cols-2">
              <select value={initiativeForm.initiative_type} onChange={(e) => setInitiativeForm((prev) => ({ ...prev, initiative_type: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm">
                {["bundle", "new_product", "research", "ops_improvement"].map((value) => <option key={value} value={value}>{value}</option>)}
              </select>
              <input value={initiativeForm.sku} onChange={(e) => setInitiativeForm((prev) => ({ ...prev, sku: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="SKU" />
            </div>
            <input value={initiativeForm.title} onChange={(e) => setInitiativeForm((prev) => ({ ...prev, title: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Initiative title" />
            <input value={initiativeForm.owner} onChange={(e) => setInitiativeForm((prev) => ({ ...prev, owner: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Owner" />
            <button onClick={() => createInitiativeMut.mutate()} className="rounded border border-white/10 px-3 py-2 text-sm">Save initiative</button>

            <Table>
              <TableHeader><TableRow><TableHead>Title</TableHead><TableHead>Status</TableHead><TableHead>Owner</TableHead></TableRow></TableHeader>
              <TableBody>
                {(initiatives?.items ?? []).map((item) => (
                  <TableRow key={item.id} onClick={() => setSelectedInitiativeId(item.id)} className={selectedInitiativeId === item.id ? "bg-white/5" : ""}>
                    <TableCell>{item.title}</TableCell>
                    <TableCell>{item.status}</TableCell>
                    <TableCell>{item.owner ?? "-"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>

            {selectedInitiative ? (
              <div className="space-y-3 rounded border border-white/10 p-3">
                <select defaultValue={selectedInitiative.status} onChange={(e) => updateInitiativeMut.mutate({ id: selectedInitiative.id, payload: { status: e.target.value } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm">
                  {["planned", "in_progress", "live_stable", "done", "blocked"].map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
                <input type="date" defaultValue={selectedInitiative.live_stable_at ?? ""} onBlur={(e) => updateInitiativeMut.mutate({ id: selectedInitiative.id, payload: { live_stable_at: e.target.value || null } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" />
                <label className="flex items-center gap-2 text-sm text-white/70">
                  <input type="checkbox" defaultChecked={selectedInitiative.planned} onChange={(e) => updateInitiativeMut.mutate({ id: selectedInitiative.id, payload: { planned: e.target.checked } })} />
                  Planned
                </label>
                <label className="flex items-center gap-2 text-sm text-white/70">
                  <input type="checkbox" defaultChecked={selectedInitiative.approved} onChange={(e) => updateInitiativeMut.mutate({ id: selectedInitiative.id, payload: { approved: e.target.checked } })} />
                  Approved
                </label>
                <button onClick={() => deleteInitiativeMut.mutate(selectedInitiative.id)} className="rounded border border-red-500/20 px-3 py-2 text-sm text-red-300">Delete initiative</button>
              </div>
            ) : null}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
