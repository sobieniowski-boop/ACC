import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  addProductTaskComment,
  createTaskOwnerRule,
  deleteTaskOwnerRule,
  getProductTaskComments,
  getTaskOwnerRules,
  getProductTasks,
  updateProductTask,
  type ProductTaskItem,
} from "@/lib/api";
import { cn } from "@/lib/utils";
import { MessageSquare, UserCog } from "lucide-react";
import {
  ClientExportButton,
  ColumnChooser,
  useColumnVisibility,
  ServerPagination,
  type ColumnDef,
} from "@/components/shared";

const STATUS_OPTIONS = ["all", "open", "investigating", "resolved"] as const;
const TYPE_OPTIONS = ["all", "pricing", "content", "watchlist"] as const;

function statusBadge(status: string): string {
  if (status === "resolved") return "bg-green-500/10 text-green-400";
  if (status === "investigating") return "bg-yellow-500/10 text-yellow-400";
  return "bg-red-500/10 text-red-400";
}

export default function ProductTasksPage() {
  const qc = useQueryClient();
  const [status, setStatus] = useState<string>("all");
  const [taskType, setTaskType] = useState<string>("all");
  const [owner, setOwner] = useState("");
  const [skuSearch, setSkuSearch] = useState("");
  const [page, setPage] = useState(1);
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [commentInput, setCommentInput] = useState("");
  const [ownerInput, setOwnerInput] = useState("");
  const [newRuleOwner, setNewRuleOwner] = useState("");
  const [newRulePriority, setNewRulePriority] = useState(100);
  const [newRuleTaskType, setNewRuleTaskType] = useState("");
  const [newRuleMarketplace, setNewRuleMarketplace] = useState("");
  const [newRuleBrand, setNewRuleBrand] = useState("");

  const params = useMemo(
    () => ({
      page,
      page_size: 30,
      ...(status !== "all" ? { status } : {}),
      ...(taskType !== "all" ? { task_type: taskType } : {}),
      ...(owner.trim() ? { owner: owner.trim() } : {}),
      ...(skuSearch.trim() ? { sku_search: skuSearch.trim() } : {}),
    }),
    [page, status, taskType, owner, skuSearch]
  );

  const { data, isLoading } = useQuery({
    queryKey: ["product-tasks", params],
    queryFn: () => getProductTasks(params),
  });

  const selectedTask = (data?.items ?? []).find((t) => t.id === selectedTaskId) ?? null;

  useEffect(() => {
    setOwnerInput(selectedTask?.owner ?? "");
  }, [selectedTask?.id, selectedTask?.owner]);

  const { data: comments } = useQuery({
    queryKey: ["product-task-comments", selectedTaskId],
    queryFn: () => getProductTaskComments(selectedTaskId!),
    enabled: !!selectedTaskId,
  });

  const { data: ownerRules } = useQuery({
    queryKey: ["task-owner-rules"],
    queryFn: getTaskOwnerRules,
  });

  const updateMut = useMutation({
    mutationFn: ({ taskId, payload }: { taskId: string; payload: { status?: "open" | "investigating" | "resolved"; owner?: string } }) =>
      updateProductTask(taskId, payload),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["product-tasks"] });
      if (selectedTaskId) qc.invalidateQueries({ queryKey: ["product-task-comments", selectedTaskId] });
    },
  });

  const commentMut = useMutation({
    mutationFn: ({ taskId, comment }: { taskId: string; comment: string }) =>
      addProductTaskComment(taskId, { comment }),
    onSuccess: () => {
      if (selectedTaskId) qc.invalidateQueries({ queryKey: ["product-task-comments", selectedTaskId] });
      setCommentInput("");
    },
  });

  const createRuleMut = useMutation({
    mutationFn: () =>
      createTaskOwnerRule({
        owner: newRuleOwner.trim(),
        priority: newRulePriority,
        task_type: newRuleTaskType.trim() || undefined,
        marketplace_id: newRuleMarketplace.trim() || undefined,
        brand: newRuleBrand.trim() || undefined,
        is_active: true,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["task-owner-rules"] });
      setNewRuleOwner("");
      setNewRulePriority(100);
      setNewRuleTaskType("");
      setNewRuleMarketplace("");
      setNewRuleBrand("");
    },
  });

  const deleteRuleMut = useMutation({
    mutationFn: (ruleId: number) => deleteTaskOwnerRule(ruleId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["task-owner-rules"] }),
  });

  const items = data?.items ?? [];

  const TASK_COLUMNS: ColumnDef[] = [
    { key: "type", label: "Type" },
    { key: "sku", label: "SKU" },
    { key: "marketplace", label: "Marketplace" },
    { key: "status", label: "Status" },
    { key: "owner", label: "Owner" },
    { key: "created", label: "Created" },
  ];
  const colVis = useColumnVisibility(TASK_COLUMNS);

  return (
    <div className="space-y-4">
      <div>
        <div className="flex items-center gap-2">
          <h1 className="text-2xl font-bold">Product Tasks</h1>
          {items.length > 0 && <ClientExportButton data={items} filename="product_tasks" />}
        </div>
        <p className="text-sm text-muted-foreground">
          Operational queue for pricing/content/watchlist with owners and comments
        </p>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <select
          value={status}
          onChange={(e) => {
            setStatus(e.target.value);
            setPage(1);
          }}
          className="rounded border border-input bg-background px-2 py-1 text-xs"
        >
          {STATUS_OPTIONS.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <select
          value={taskType}
          onChange={(e) => {
            setTaskType(e.target.value);
            setPage(1);
          }}
          className="rounded border border-input bg-background px-2 py-1 text-xs"
        >
          {TYPE_OPTIONS.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
        <input
          value={owner}
          onChange={(e) => {
            setOwner(e.target.value);
            setPage(1);
          }}
          placeholder="Owner"
          className="rounded border border-input bg-background px-2 py-1 text-xs"
        />
        <input
          value={skuSearch}
          onChange={(e) => {
            setSkuSearch(e.target.value);
            setPage(1);
          }}
          placeholder="SKU search"
          className="rounded border border-input bg-background px-2 py-1 text-xs"
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-[2fr_1fr]">
        <div className="overflow-hidden rounded-xl border border-border bg-card">
          <div className="flex items-center gap-2 px-3 py-2 border-b border-border">
            <ColumnChooser columns={TASK_COLUMNS} visible={colVis.visible} onChange={colVis.setVisible} />
          </div>
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                {colVis.isVisible("type") && <th className="px-2 py-2">Type</th>}
                {colVis.isVisible("sku") && <th className="px-2 py-2">SKU</th>}
                {colVis.isVisible("marketplace") && <th className="px-2 py-2">Marketplace</th>}
                {colVis.isVisible("status") && <th className="px-2 py-2">Status</th>}
                {colVis.isVisible("owner") && <th className="px-2 py-2">Owner</th>}
                {colVis.isVisible("created") && <th className="px-2 py-2">Created</th>}
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {isLoading && (
                <tr>
                  <td colSpan={colVis.visible.length} className="px-2 py-10 text-center text-muted-foreground">
                    Loading...
                  </td>
                </tr>
              )}
              {!isLoading && items.length === 0 && (
                <tr>
                  <td colSpan={colVis.visible.length} className="px-2 py-10 text-center text-muted-foreground">
                    No tasks found
                  </td>
                </tr>
              )}
              {items.map((task: ProductTaskItem) => (
                <tr
                  key={task.id}
                  onClick={() => setSelectedTaskId(task.id)}
                  className={cn(
                    "cursor-pointer hover:bg-muted/20 transition-colors",
                    task.id === selectedTaskId && "bg-muted/30"
                  )}
                >
                  {colVis.isVisible("type") && <td className="px-2 py-1.5">{task.task_type}</td>}
                  {colVis.isVisible("sku") && <td className="px-2 py-1.5 font-mono">{task.sku}</td>}
                  {colVis.isVisible("marketplace") && <td className="px-2 py-1.5">{task.marketplace_id ?? "ALL"}</td>}
                  {colVis.isVisible("status") && <td className="px-2 py-1.5">
                    <span className={cn("rounded-full px-2 py-0.5 text-[10px]", statusBadge(task.status))}>
                      {task.status}
                    </span>
                  </td>}
                  {colVis.isVisible("owner") && <td className="px-2 py-1.5">{task.owner ?? "-"}</td>}
                  {colVis.isVisible("created") && <td className="px-2 py-1.5 text-muted-foreground">{task.created_at.slice(0, 10)}</td>}
                </tr>
              ))}
            </tbody>
          </table>

          {data && data.pages > 1 && (
            <ServerPagination page={data.page} pages={data.pages} total={data.total} pageSize={30} onPageChange={setPage} />
          )}
        </div>

        <div className="rounded-xl border border-border bg-card p-4">
          {!selectedTask ? (
            <div className="text-xs text-muted-foreground">Select task to manage status, owner and comments</div>
          ) : (
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div>
                  <div className="text-sm font-semibold">{selectedTask.task_type}</div>
                  <div className="font-mono text-xs text-muted-foreground">{selectedTask.sku}</div>
                </div>
                <span className={cn("rounded-full px-2 py-0.5 text-[10px]", statusBadge(selectedTask.status))}>
                  {selectedTask.status}
                </span>
              </div>

              <div className="space-y-2">
                <label className="text-[11px] text-muted-foreground">Status</label>
                <select
                  value={selectedTask.status}
                  onChange={(e) =>
                    updateMut.mutate({
                      taskId: selectedTask.id,
                      payload: { status: e.target.value as "open" | "investigating" | "resolved" },
                    })
                  }
                  className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
                >
                  {STATUS_OPTIONS.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              </div>

              <div className="space-y-2">
                <label className="inline-flex items-center gap-1 text-[11px] text-muted-foreground">
                  <UserCog className="h-3.5 w-3.5" />
                  Owner
                </label>
                <div className="flex gap-1">
                  <input
                    value={ownerInput}
                    onChange={(e) => setOwnerInput(e.target.value)}
                    placeholder="owner"
                    className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
                  />
                  <button
                    onClick={() => {
                      updateMut.mutate({
                        taskId: selectedTask.id,
                        payload: { owner: ownerInput.trim() || "" },
                      });
                    }}
                    className="rounded border border-border px-2 py-1 text-xs"
                  >
                    Save
                  </button>
                </div>
              </div>

              <div className="space-y-2">
                <div className="inline-flex items-center gap-1 text-[11px] text-muted-foreground">
                  <MessageSquare className="h-3.5 w-3.5" />
                  Comments
                </div>
                <div className="max-h-44 space-y-1 overflow-auto rounded border border-border p-2">
                  {(comments ?? []).length === 0 && (
                    <div className="text-xs text-muted-foreground">No comments</div>
                  )}
                  {(comments ?? []).map((c) => (
                    <div key={c.id} className="rounded border border-border/60 px-2 py-1">
                      <div className="text-[10px] text-muted-foreground">
                        {c.author ?? "system"} | {c.created_at.slice(0, 19).replace("T", " ")}
                      </div>
                      <div className="text-xs">{c.comment}</div>
                    </div>
                  ))}
                </div>
                <textarea
                  rows={2}
                  value={commentInput}
                  onChange={(e) => setCommentInput(e.target.value)}
                  placeholder="Add comment..."
                  className="w-full rounded border border-input bg-background px-2 py-1 text-xs"
                />
                <button
                  onClick={() => {
                    if (!selectedTaskId || !commentInput.trim()) return;
                    commentMut.mutate({ taskId: selectedTaskId, comment: commentInput.trim() });
                  }}
                  className="rounded border border-border px-2 py-1 text-xs"
                >
                  Add comment
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4">
        <h2 className="mb-3 text-sm font-semibold">Auto Owner Rules (marketplace/brand/task type)</h2>
        <div className="mb-3 grid gap-2 md:grid-cols-5">
          <input
            value={newRuleOwner}
            onChange={(e) => setNewRuleOwner(e.target.value)}
            placeholder="Owner (required)"
            className="rounded border border-input bg-background px-2 py-1 text-xs"
          />
          <input
            type="number"
            value={newRulePriority}
            onChange={(e) => setNewRulePriority(Number(e.target.value) || 100)}
            placeholder="Priority"
            className="rounded border border-input bg-background px-2 py-1 text-xs"
          />
          <input
            value={newRuleTaskType}
            onChange={(e) => setNewRuleTaskType(e.target.value)}
            placeholder="task_type (optional)"
            className="rounded border border-input bg-background px-2 py-1 text-xs"
          />
          <input
            value={newRuleMarketplace}
            onChange={(e) => setNewRuleMarketplace(e.target.value)}
            placeholder="marketplace_id (optional)"
            className="rounded border border-input bg-background px-2 py-1 text-xs"
          />
          <input
            value={newRuleBrand}
            onChange={(e) => setNewRuleBrand(e.target.value)}
            placeholder="brand (optional)"
            className="rounded border border-input bg-background px-2 py-1 text-xs"
          />
        </div>
        <button
          disabled={!newRuleOwner.trim()}
          onClick={() => createRuleMut.mutate()}
          className="mb-3 rounded border border-border px-2 py-1 text-xs disabled:opacity-40"
        >
          Add rule
        </button>

        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Priority</th>
                <th className="px-2 py-2">Owner</th>
                <th className="px-2 py-2">Task Type</th>
                <th className="px-2 py-2">Marketplace</th>
                <th className="px-2 py-2">Brand</th>
                <th className="px-2 py-2">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(ownerRules ?? []).map((r) => (
                <tr key={r.id} className="hover:bg-muted/20 transition-colors">
                  <td className="px-2 py-1.5">{r.priority}</td>
                  <td className="px-2 py-1.5">{r.owner}</td>
                  <td className="px-2 py-1.5">{r.task_type ?? "*"}</td>
                  <td className="px-2 py-1.5">{r.marketplace_id ?? "*"}</td>
                  <td className="px-2 py-1.5">{r.brand ?? "*"}</td>
                  <td className="px-2 py-1.5">
                    <button
                      onClick={() => deleteRuleMut.mutate(r.id)}
                      className="rounded border border-border px-2 py-0.5 text-[10px]"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
              {(ownerRules ?? []).length === 0 && (
                <tr>
                  <td colSpan={6} className="px-2 py-6 text-center text-muted-foreground">
                    No owner rules defined
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
