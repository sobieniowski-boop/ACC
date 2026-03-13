import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  applyManageInventoryDraft,
  approveManageInventoryDraft,
  createManageInventoryDraft,
  getManageInventoryDrafts,
  rollbackManageInventoryDraft,
  validateManageInventoryDraft,
  type ManageInventoryDraftItem,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ClientExportButton } from "@/components/shared";

function variant(status: string): "success" | "secondary" | "warning" | "destructive" {
  if (status === "success" || status === "approved" || status === "passed") return "success";
  if (status === "failed" || status === "rejected") return "destructive";
  if (status === "draft" || status === "pending") return "warning";
  return "secondary";
}

export default function InventoryDraftsPage() {
  const qc = useQueryClient();
  const [draftType, setDraftType] = useState("reparent");
  const [marketplaceId, setMarketplaceId] = useState("");
  const [affectedParentAsin, setAffectedParentAsin] = useState("");
  const [affectedSku, setAffectedSku] = useState("");
  const [payloadText, setPayloadText] = useState('{\n  "target_parent_asin": ""\n}');
  const [selectedDraftId, setSelectedDraftId] = useState<string | null>(null);

  const draftsQuery = useQuery({
    queryKey: ["manage-inventory-drafts"],
    queryFn: getManageInventoryDrafts,
  });

  const selectedDraft = useMemo(
    () => draftsQuery.data?.items.find((item) => item.id === selectedDraftId) ?? null,
    [draftsQuery.data?.items, selectedDraftId],
  );

  const refresh = () => qc.invalidateQueries({ queryKey: ["manage-inventory-drafts"] });

  const createMutation = useMutation({
    mutationFn: async () => {
      const parsed = JSON.parse(payloadText);
      return createManageInventoryDraft({
        draft_type: draftType,
        marketplace_id: marketplaceId || undefined,
        affected_parent_asin: affectedParentAsin || undefined,
        affected_sku: affectedSku || undefined,
        payload_json: parsed,
        snapshot_before_json: {},
        created_by: "ui",
      });
    },
    onSuccess: (draft) => {
      refresh();
      setSelectedDraftId(draft.id);
    },
  });

  const validateMutation = useMutation({
    mutationFn: (draftId: string) => validateManageInventoryDraft(draftId),
    onSuccess: refresh,
  });

  const approveMutation = useMutation({
    mutationFn: (draftId: string) => approveManageInventoryDraft(draftId),
    onSuccess: refresh,
  });

  const applyMutation = useMutation({
    mutationFn: (draftId: string) => applyManageInventoryDraft(draftId),
    onSuccess: refresh,
  });

  const rollbackMutation = useMutation({
    mutationFn: (draftId: string) => rollbackManageInventoryDraft(draftId),
    onSuccess: refresh,
  });

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Drafts & Approvals</h1>
        <p className="text-sm text-white/50">Safe workflow shell: draft to validate to approve to apply to rollback.</p>
      </div>

      <div className="grid gap-4 xl:grid-cols-[1.3fr_1fr]">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Create draft</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid gap-3 md:grid-cols-2">
              <select value={draftType} onChange={(e) => setDraftType(e.target.value)} className="rounded-md border border-white/10 bg-[#111827] px-3 py-2 text-sm">
                <option value="reparent">reparent</option>
                <option value="create_parent">create_parent</option>
                <option value="update_theme">update_theme</option>
                <option value="detach">detach</option>
              </select>
              <Input value={marketplaceId} onChange={(e) => setMarketplaceId(e.target.value)} placeholder="marketplace_id" />
              <Input value={affectedParentAsin} onChange={(e) => setAffectedParentAsin(e.target.value)} placeholder="affected parent ASIN" />
              <Input value={affectedSku} onChange={(e) => setAffectedSku(e.target.value)} placeholder="affected SKU" />
            </div>
            <textarea
              rows={10}
              value={payloadText}
              onChange={(e) => setPayloadText(e.target.value)}
              className="w-full rounded-md border border-white/10 bg-[#111827] px-3 py-2 font-mono text-xs text-white"
            />
            <div className="flex gap-2">
              <Button onClick={() => createMutation.mutate()} disabled={createMutation.isPending}>
                {createMutation.isPending ? "Creating..." : "Create draft"}
              </Button>
              {createMutation.isError ? <div className="text-sm text-red-300">Invalid payload JSON or backend error.</div> : null}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Selected draft</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {!selectedDraft ? (
              <div className="text-sm text-white/50">Wybierz draft z listy, aby zobaczyc walidacje, diff snapshotu i akcje.</div>
            ) : (
              <>
                <div className="space-y-1">
                  <div className="font-mono text-sm text-white">{selectedDraft.id}</div>
                  <div className="text-xs text-white/50">{selectedDraft.draft_type} | {selectedDraft.marketplace_code ?? selectedDraft.marketplace_id ?? "-"}</div>
                </div>
                <div className="flex flex-wrap gap-2">
                  <Badge variant={variant(selectedDraft.validation_status)}>{selectedDraft.validation_status}</Badge>
                  <Badge variant={variant(selectedDraft.approval_status)}>{selectedDraft.approval_status}</Badge>
                  <Badge variant={variant(selectedDraft.apply_status)}>{selectedDraft.apply_status}</Badge>
                </div>
                <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-1">
                  <Button variant="secondary" onClick={() => validateMutation.mutate(selectedDraft.id)} disabled={validateMutation.isPending}>
                    Validate
                  </Button>
                  <Button variant="secondary" onClick={() => approveMutation.mutate(selectedDraft.id)} disabled={approveMutation.isPending}>
                    Approve
                  </Button>
                  <Button variant="secondary" onClick={() => applyMutation.mutate(selectedDraft.id)} disabled={applyMutation.isPending}>
                    Apply
                  </Button>
                  <Button variant="secondary" onClick={() => rollbackMutation.mutate(selectedDraft.id)} disabled={rollbackMutation.isPending}>
                    Rollback
                  </Button>
                </div>
                <div className="rounded-lg border border-white/10 p-3">
                  <div className="text-sm font-medium text-white">Validation errors</div>
                  <ul className="mt-2 space-y-1 text-xs text-white/60">
                    {selectedDraft.validation_errors.length > 0 ? selectedDraft.validation_errors.map((error) => <li key={error}>- {error}</li>) : <li>- Brak bledow walidacji.</li>}
                  </ul>
                </div>
                <div className="rounded-lg border border-white/10 p-3">
                  <div className="text-sm font-medium text-white">Payload</div>
                  <pre className="mt-2 overflow-auto text-xs text-white/55">{JSON.stringify(selectedDraft.payload_json, null, 2)}</pre>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-sm">Draft list</CardTitle>
          <ClientExportButton data={draftsQuery.data?.items ?? []} filename="inventory_drafts" />
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Draft ID</TableHead>
                <TableHead>Type</TableHead>
                <TableHead>Marketplace</TableHead>
                <TableHead>Parent / SKU</TableHead>
                <TableHead>Validation</TableHead>
                <TableHead>Approval</TableHead>
                <TableHead>Apply</TableHead>
                <TableHead>Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(draftsQuery.data?.items ?? []).map((item: ManageInventoryDraftItem) => (
                <TableRow key={item.id} className={`cursor-pointer ${selectedDraftId === item.id ? "bg-white/5" : ""}`} onClick={() => setSelectedDraftId(item.id)}>
                  <TableCell className="font-mono text-xs">{item.id.slice(0, 8)}...</TableCell>
                  <TableCell>{item.draft_type}</TableCell>
                  <TableCell>{item.marketplace_code ?? item.marketplace_id ?? "-"}</TableCell>
                  <TableCell className="text-xs text-white/60">
                    {item.affected_parent_asin ?? "-"}
                    <div>{item.affected_sku ?? "-"}</div>
                  </TableCell>
                  <TableCell><Badge variant={variant(item.validation_status)}>{item.validation_status}</Badge></TableCell>
                  <TableCell><Badge variant={variant(item.approval_status)}>{item.approval_status}</Badge></TableCell>
                  <TableCell><Badge variant={variant(item.apply_status)}>{item.apply_status}</Badge></TableCell>
                  <TableCell>{new Date(item.created_at).toLocaleString()}</TableCell>
                </TableRow>
              ))}
              {!draftsQuery.isLoading && (draftsQuery.data?.items.length ?? 0) === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="text-center text-white/50">
                    Brak draftow inventory.
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
