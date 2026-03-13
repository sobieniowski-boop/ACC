import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  addFbaCaseComment,
  createFbaCase,
  deleteFbaCaseComment,
  deleteFbaCase,
  getFbaAgedItems,
  getFbaCaseTimeline,
  getFbaCases,
  getFbaReportDiagnostics,
  getFbaStrandedItems,
  updateFbaCaseComment,
  updateFbaCase,
} from "@/lib/api";
import { FbaJobStatusStrip } from "@/components/fba/FbaJobStatusStrip";
import { ImportCSVDialog } from "@/components/fba/ImportCSVDialog";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { formatPLN } from "@/lib/utils";
import { ClientExportButton } from "@/components/shared";

export default function FbaAgedStrandedPage() {
  const qc = useQueryClient();
  const [selectedCaseId, setSelectedCaseId] = useState<string | null>(null);
  const [commentInput, setCommentInput] = useState("");
  const [editingCommentId, setEditingCommentId] = useState<string | null>(null);
  const [editingCommentText, setEditingCommentText] = useState("");
  const [createForm, setCreateForm] = useState({
    case_type: "stranded",
    marketplace_id: "A1PA6795UKMFR9",
    sku: "",
    detected_date: new Date().toISOString().slice(0, 10),
    owner: "",
    root_cause: "",
  });

  const { data: aged } = useQuery({ queryKey: ["fba-aged"], queryFn: getFbaAgedItems });
  const { data: stranded } = useQuery({ queryKey: ["fba-stranded"], queryFn: getFbaStrandedItems });
  const diagnosticsQuery = useQuery({ queryKey: ["fba-report-diagnostics-aged"], queryFn: () => getFbaReportDiagnostics(72), refetchInterval: 120000 });
  const { data: cases } = useQuery({ queryKey: ["fba-cases"], queryFn: () => getFbaCases() });
  const { data: caseTimeline } = useQuery({
    queryKey: ["fba-case-timeline", selectedCaseId],
    queryFn: () => getFbaCaseTimeline(selectedCaseId!),
    enabled: !!selectedCaseId,
  });

  const selectedCase = useMemo(
    () => (cases?.items ?? []).find((item) => item.id === selectedCaseId) ?? null,
    [cases?.items, selectedCaseId]
  );

  const agedGrouped = useMemo(() => {
    const grouped = new Map<
      string,
      {
        sku: string;
        title_preferred?: string | null;
        marketplace_codes: string[];
        aged_90_plus_units: number;
        aged_90_plus_value_pln: number;
      }
    >();

    for (const item of aged ?? []) {
      const existing = grouped.get(item.sku);
      if (existing) {
        if (item.marketplace_code && !existing.marketplace_codes.includes(item.marketplace_code)) {
          existing.marketplace_codes.push(item.marketplace_code);
        }
        existing.aged_90_plus_units += item.aged_90_plus_units;
        existing.aged_90_plus_value_pln += item.aged_90_plus_value_pln;
        if (!existing.title_preferred && item.title_preferred) {
          existing.title_preferred = item.title_preferred;
        }
        continue;
      }

      grouped.set(item.sku, {
        sku: item.sku,
        title_preferred: item.title_preferred,
        marketplace_codes: item.marketplace_code ? [item.marketplace_code] : [],
        aged_90_plus_units: item.aged_90_plus_units,
        aged_90_plus_value_pln: item.aged_90_plus_value_pln,
      });
    }

    return Array.from(grouped.values()).sort((left, right) => right.aged_90_plus_value_pln - left.aged_90_plus_value_pln);
  }, [aged]);

  const strandedGrouped = useMemo(() => {
    const grouped = new Map<
      string,
      {
        sku: string;
        title_preferred?: string | null;
        marketplace_codes: string[];
        stranded_units: number;
        stranded_value_pln: number;
      }
    >();

    for (const item of stranded ?? []) {
      const existing = grouped.get(item.sku);
      if (existing) {
        if (item.marketplace_code && !existing.marketplace_codes.includes(item.marketplace_code)) {
          existing.marketplace_codes.push(item.marketplace_code);
        }
        existing.stranded_units += item.stranded_units;
        existing.stranded_value_pln += item.stranded_value_pln;
        if (!existing.title_preferred && item.title_preferred) {
          existing.title_preferred = item.title_preferred;
        }
        continue;
      }

      grouped.set(item.sku, {
        sku: item.sku,
        title_preferred: item.title_preferred,
        marketplace_codes: item.marketplace_code ? [item.marketplace_code] : [],
        stranded_units: item.stranded_units,
        stranded_value_pln: item.stranded_value_pln,
      });
    }

    return Array.from(grouped.values()).sort((left, right) => right.stranded_value_pln - left.stranded_value_pln);
  }, [stranded]);

  const createMut = useMutation({
    mutationFn: () =>
      createFbaCase({
        case_type: createForm.case_type,
        marketplace_id: createForm.marketplace_id,
        sku: createForm.sku || undefined,
        detected_date: createForm.detected_date,
        owner: createForm.owner || undefined,
        root_cause: createForm.root_cause || undefined,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["fba-cases"] });
      setCreateForm((prev) => ({ ...prev, sku: "", owner: "", root_cause: "" }));
    },
  });

  const updateMut = useMutation({
    mutationFn: ({ id, payload }: { id: string; payload: Record<string, unknown> }) => updateFbaCase(id, payload),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["fba-cases"] }),
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => deleteFbaCase(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["fba-cases"] });
      setSelectedCaseId(null);
    },
  });
  const commentMut = useMutation({
    mutationFn: ({ id, comment }: { id: string; comment: string }) => addFbaCaseComment(id, { comment }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["fba-case-timeline", selectedCaseId] });
      setCommentInput("");
    },
  });
  const updateCommentMut = useMutation({
    mutationFn: ({ id, eventId, comment }: { id: string; eventId: string; comment: string }) =>
      updateFbaCaseComment(id, eventId, { comment }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["fba-case-timeline", selectedCaseId] });
      setEditingCommentId(null);
      setEditingCommentText("");
    },
  });
  const deleteCommentMut = useMutation({
    mutationFn: ({ id, eventId }: { id: string; eventId: string }) => deleteFbaCaseComment(id, eventId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["fba-case-timeline", selectedCaseId] });
      setEditingCommentId(null);
      setEditingCommentText("");
    },
  });
  const hasStrandedProxy = useMemo(
    () =>
      (diagnosticsQuery.data?.items ?? []).some(
        (item) =>
          item.stranded?.request_status === "CANCELLED" ||
          item.stranded?.fetch_mode === "fallback_planning_unfulfillable" ||
          item.stranded?.fetch_mode === "fallback_inventory_api_unfulfillable",
      ),
    [diagnosticsQuery.data?.items],
  );

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Aged / Stranded Actions</h1>
          <p className="text-sm text-white/50">Live aged and stranded snapshot plus case register used by resolution KPI.</p>
        </div>
        <div className="flex items-center gap-2">
          {aged && aged.length > 0 && <ClientExportButton data={aged} filename="fba_aged_items" />}
          <ImportCSVDialog registerType="case" invalidateKeys={[["fba-cases"]]} buttonLabel="Import Cases CSV" />
        </div>
      </div>
      <FbaJobStatusStrip />
      {hasStrandedProxy ? (
        <Card className="border-amber-500/30">
          <CardHeader className="pb-2"><CardTitle className="text-sm text-amber-300">Aged / Stranded data: partial</CardTitle></CardHeader>
          <CardContent className="text-sm text-white/70">
            Canonical stranded feed is not fully available for at least one marketplace. This screen may currently use proxy data and should not be treated as complete for critical decisions.
          </CardContent>
        </Card>
      ) : null}
      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader><CardTitle className="text-sm">Aged 90+</CardTitle></CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader><TableRow><TableHead>Produkt</TableHead><TableHead>MP</TableHead><TableHead className="text-right">Units</TableHead><TableHead className="text-right">Value</TableHead></TableRow></TableHeader>
              <TableBody>
                {agedGrouped.map((item) => (
                  <TableRow key={item.sku}>
                    <TableCell>
                      <div className="max-w-[28rem] truncate text-sm font-medium text-white">
                        {item.title_preferred ?? item.sku}
                      </div>
                      <div className="font-mono text-[11px] text-white/45">{item.sku}</div>
                    </TableCell>
                    <TableCell>{item.marketplace_codes.join(", ") || "-"}</TableCell>
                    <TableCell className="text-right">{item.aged_90_plus_units}</TableCell>
                    <TableCell className="text-right">{formatPLN(item.aged_90_plus_value_pln)}</TableCell>
                  </TableRow>
                ))}
                {agedGrouped.length === 0 && (
                  <TableRow><TableCell colSpan={4} className="text-center text-white/50">Brak aged 90+ w aktualnym snapshotcie.</TableCell></TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle className="text-sm">Stranded</CardTitle></CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader><TableRow><TableHead>Produkt</TableHead><TableHead>MP</TableHead><TableHead className="text-right">Units</TableHead><TableHead className="text-right">Value</TableHead></TableRow></TableHeader>
              <TableBody>
                {strandedGrouped.map((item) => (
                  <TableRow key={item.sku}>
                    <TableCell>
                      <div className="max-w-[28rem] truncate text-sm font-medium text-white">
                        {item.title_preferred ?? item.sku}
                      </div>
                      <div className="font-mono text-[11px] text-white/45">{item.sku}</div>
                    </TableCell>
                    <TableCell>{item.marketplace_codes.join(", ") || "-"}</TableCell>
                    <TableCell className="text-right">{item.stranded_units}</TableCell>
                    <TableCell className="text-right">{formatPLN(item.stranded_value_pln)}</TableCell>
                  </TableRow>
                ))}
                {strandedGrouped.length === 0 && (
                  <TableRow><TableCell colSpan={4} className="text-center text-white/50">Brak stranded w aktualnym snapshotcie.</TableCell></TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
        <Card>
          <CardHeader><CardTitle className="text-sm">Case Register</CardTitle></CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Type</TableHead>
                  <TableHead>SKU</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Detected</TableHead>
                  <TableHead>Owner</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(cases?.items ?? []).map((item) => (
                  <TableRow key={item.id} onClick={() => setSelectedCaseId(item.id)} className={selectedCaseId === item.id ? "bg-white/5" : ""}>
                    <TableCell>{item.case_type}</TableCell>
                    <TableCell className="font-mono text-xs">{item.sku ?? "-"}</TableCell>
                    <TableCell>{item.status}</TableCell>
                    <TableCell>{item.detected_date}</TableCell>
                    <TableCell>{item.owner ?? "-"}</TableCell>
                  </TableRow>
                ))}
                {(cases?.items?.length ?? 0) === 0 && (
                  <TableRow><TableCell colSpan={5} className="text-center text-white/50">No cases yet.</TableCell></TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle className="text-sm">{selectedCase ? "Case Detail" : "Add Case"}</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            {!selectedCase ? (
              <>
                <select value={createForm.case_type} onChange={(e) => setCreateForm((prev) => ({ ...prev, case_type: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm">
                  {["stranded", "fc_issue", "operations"].map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
                <input value={createForm.marketplace_id} onChange={(e) => setCreateForm((prev) => ({ ...prev, marketplace_id: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Marketplace ID" />
                <input value={createForm.sku} onChange={(e) => setCreateForm((prev) => ({ ...prev, sku: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="SKU" />
                <input type="date" value={createForm.detected_date} onChange={(e) => setCreateForm((prev) => ({ ...prev, detected_date: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" />
                <input value={createForm.owner} onChange={(e) => setCreateForm((prev) => ({ ...prev, owner: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Owner" />
                <textarea value={createForm.root_cause} onChange={(e) => setCreateForm((prev) => ({ ...prev, root_cause: e.target.value }))} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Root cause" rows={3} />
                <button onClick={() => createMut.mutate()} className="rounded border border-white/10 px-3 py-2 text-sm">Save case</button>
              </>
            ) : (
              <>
                <div className="text-xs text-white/45">{selectedCase.case_type} | {selectedCase.sku ?? "-"}</div>
                <select defaultValue={selectedCase.status} onChange={(e) => updateMut.mutate({ id: selectedCase.id, payload: { status: e.target.value } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm">
                  {["open", "investigating", "resolved", "closed"].map((value) => <option key={value} value={value}>{value}</option>)}
                </select>
                <input defaultValue={selectedCase.owner ?? ""} onBlur={(e) => updateMut.mutate({ id: selectedCase.id, payload: { owner: e.target.value || null } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" placeholder="Owner" />
                <input type="date" defaultValue={selectedCase.close_date ?? ""} onBlur={(e) => updateMut.mutate({ id: selectedCase.id, payload: { close_date: e.target.value || null } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" />
                <textarea defaultValue={selectedCase.root_cause ?? ""} onBlur={(e) => updateMut.mutate({ id: selectedCase.id, payload: { root_cause: e.target.value || null } })} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" rows={3} />
                <div className="rounded border border-white/10 p-3">
                  <div className="mb-2 text-xs uppercase tracking-[0.2em] text-white/40">Timeline</div>
                  <div className="space-y-2">
                    {(caseTimeline?.events ?? []).map((event) => (
                      <div key={event.id} className="rounded border border-white/10 px-3 py-2 text-xs">
                        <div className="flex items-center justify-between gap-3">
                          <div className="font-medium text-white/80">{event.event_type}</div>
                          <div className="text-white/40">{event.event_at.slice(0, 19).replace("T", " ")}</div>
                        </div>
                        <div className="mt-1 text-white/50">{event.actor ?? "system"}</div>
                        {event.event_type === "comment" ? (
                          <div className="mt-2 space-y-2">
                            {editingCommentId === event.id ? (
                              <>
                                <textarea
                                  value={editingCommentText}
                                  onChange={(e) => setEditingCommentText(e.target.value)}
                                  className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm"
                                  rows={3}
                                />
                                <div className="flex gap-2">
                                  <button
                                    onClick={() => {
                                      if (!selectedCase || !editingCommentText.trim()) return;
                                      updateCommentMut.mutate({ id: selectedCase.id, eventId: event.id, comment: editingCommentText.trim() });
                                    }}
                                    className="rounded border border-white/10 px-2 py-1 text-xs"
                                  >
                                    Save
                                  </button>
                                  <button
                                    onClick={() => {
                                      setEditingCommentId(null);
                                      setEditingCommentText("");
                                    }}
                                    className="rounded border border-white/10 px-2 py-1 text-xs text-white/60"
                                  >
                                    Cancel
                                  </button>
                                </div>
                              </>
                            ) : (
                              <>
                                <div className="whitespace-pre-wrap text-sm text-white/75">
                                  {event.payload_json?.deleted ? "[deleted comment]" : (event.payload_json?.comment as string) || "-"}
                                </div>
                                {event.payload_json?.edited_at ? (
                                  <div className="text-[11px] text-white/40">edited {String(event.payload_json.edited_at).slice(0, 19).replace("T", " ")}</div>
                                ) : null}
                                {!event.payload_json?.deleted && selectedCase ? (
                                  <div className="flex gap-2">
                                    <button
                                      onClick={() => {
                                        setEditingCommentId(event.id);
                                        setEditingCommentText(String(event.payload_json?.comment ?? ""));
                                      }}
                                      className="rounded border border-white/10 px-2 py-1 text-xs"
                                    >
                                      Edit
                                    </button>
                                    <button
                                      onClick={() => deleteCommentMut.mutate({ id: selectedCase.id, eventId: event.id })}
                                      className="rounded border border-red-500/20 px-2 py-1 text-xs text-red-300"
                                    >
                                      Delete
                                    </button>
                                  </div>
                                ) : null}
                              </>
                            )}
                          </div>
                        ) : Object.keys(event.payload_json ?? {}).length > 0 ? (
                          <pre className="mt-2 overflow-auto whitespace-pre-wrap text-[11px] text-white/45">{JSON.stringify(event.payload_json, null, 2)}</pre>
                        ) : null}
                      </div>
                    ))}
                    {(caseTimeline?.events?.length ?? 0) === 0 ? (
                      <div className="text-xs text-white/45">No timeline events yet.</div>
                    ) : null}
                  </div>
                </div>
                <div className="space-y-2 rounded border border-white/10 p-3">
                  <div className="text-xs uppercase tracking-[0.2em] text-white/40">Add Comment</div>
                  <textarea value={commentInput} onChange={(e) => setCommentInput(e.target.value)} className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm" rows={3} placeholder="Manual update, root cause note, owner handoff..." />
                  <button
                    onClick={() => {
                      if (!selectedCase || !commentInput.trim()) return;
                      commentMut.mutate({ id: selectedCase.id, comment: commentInput.trim() });
                    }}
                    className="rounded border border-white/10 px-3 py-2 text-sm"
                  >
                    Save comment
                  </button>
                </div>
                <button onClick={() => deleteMut.mutate(selectedCase.id)} className="rounded border border-red-500/20 px-3 py-2 text-sm text-red-300">Delete case</button>
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
