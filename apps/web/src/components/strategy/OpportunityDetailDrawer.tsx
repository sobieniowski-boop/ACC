import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { X, CheckCircle2, XCircle, Flag, Clock, User, Target, TrendingUp, AlertTriangle, FileText, BarChart3 } from "lucide-react";
import { getStrategyOpportunityDetail, acceptOpportunity, rejectOpportunity, completeOpportunity, getOpportunityOutcomes } from "@/lib/api";
import type { GrowthOpportunity, OpportunityLogEntry, OpportunityExecution } from "@/lib/api";
import { formatPLN, cn } from "@/lib/utils";

const PRIO_COLORS: Record<string, string> = {
  do_now: "bg-red-500/15 text-red-400 border-red-500/30",
  this_week: "bg-orange-500/15 text-orange-400 border-orange-500/30",
  this_month: "bg-blue-500/15 text-blue-400 border-blue-500/30",
  backlog: "bg-zinc-500/15 text-zinc-400 border-zinc-500/30",
  low: "bg-zinc-800/30 text-zinc-500 border-zinc-700/30",
};

interface Props {
  oppId: number;
  onClose: () => void;
}

export default function OpportunityDetailDrawer({ oppId, onClose }: Props) {
  const qc = useQueryClient();

  const { data, isLoading } = useQuery({
    queryKey: ["strategy-opp-detail", oppId],
    queryFn: () => getStrategyOpportunityDetail(oppId),
    staleTime: 20_000,
  });

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ["strategy-opp-detail", oppId] });
    qc.invalidateQueries({ queryKey: ["strategy-opportunities"] });
    qc.invalidateQueries({ queryKey: ["strategy-overview"] });
  };

  const accept = useMutation({ mutationFn: () => acceptOpportunity(oppId), onSuccess: invalidate });
  const reject = useMutation({ mutationFn: () => rejectOpportunity(oppId), onSuccess: invalidate });
  const complete = useMutation({ mutationFn: () => completeOpportunity(oppId), onSuccess: invalidate });

  const opp: GrowthOpportunity | undefined = data?.opportunity;
  const timeline: OpportunityLogEntry[] = data?.timeline ?? [];

  const { data: executions } = useQuery({
    queryKey: ["opp-executions", oppId],
    queryFn: () => getOpportunityOutcomes(oppId),
    staleTime: 30_000,
  });

  return (
    <>
      {/* Overlay */}
      <div className="fixed inset-0 bg-black/50 z-40" onClick={onClose} />

      {/* Drawer panel */}
      <div className="fixed top-0 right-0 h-full w-full max-w-xl bg-card border-l border-border z-50 overflow-y-auto shadow-2xl">
        {/* Header */}
        <div className="sticky top-0 flex items-center justify-between border-b border-border bg-card/95 backdrop-blur px-5 py-3 z-10">
          <h2 className="text-base font-semibold truncate">Opportunity #{oppId}</h2>
          <button onClick={onClose} className="rounded p-1 hover:bg-muted"><X className="h-4 w-4" /></button>
        </div>

        {isLoading || !opp ? (
          <div className="p-6 space-y-3">{Array.from({ length: 5 }).map((_, i) => <div key={i} className="h-5 bg-muted/30 rounded animate-pulse" />)}</div>
        ) : (
          <div className="p-5 space-y-6">
            {/* Priority + Status badges */}
            <div className="flex items-center gap-3 flex-wrap">
              <span className={cn("rounded-full border px-3 py-1 text-xs font-bold", PRIO_COLORS[opp.priority_label || "low"])}>
                Prio {opp.priority_score.toFixed(0)} — {(opp.priority_label || "low").replace(/_/g, " ").toUpperCase()}
              </span>
              <span className="rounded border px-2 py-0.5 text-[10px] font-bold uppercase bg-muted/30">{opp.status}</span>
              <span className="rounded border px-2 py-0.5 text-[10px] uppercase bg-muted/30">{opp.opportunity_type.replace(/_/g, " ")}</span>
            </div>

            {/* Title + description */}
            <div>
              <h3 className="text-lg font-semibold">{opp.title}</h3>
              <p className="text-sm text-muted-foreground mt-1">{opp.description}</p>
            </div>

            {/* Identifiers */}
            <div className="grid grid-cols-3 gap-3 text-xs">
              {opp.sku && <InfoCell label="SKU" value={opp.sku} />}
              {opp.asin && <InfoCell label="ASIN" value={opp.asin} />}
              {opp.parent_asin && <InfoCell label="Parent ASIN" value={opp.parent_asin} />}
              {opp.family_id && <InfoCell label="Family" value={String(opp.family_id)} />}
              {opp.marketplace_code && <InfoCell label="Marketplace" value={opp.marketplace_code} />}
            </div>

            {/* Root cause */}
            {opp.root_cause && (
              <Section icon={<AlertTriangle className="h-4 w-4 text-yellow-500" />} title="Root Cause">
                <p className="text-sm">{opp.root_cause.replace(/_/g, " ")}</p>
              </Section>
            )}

            {/* Recommendation */}
            {opp.recommendation && (
              <Section icon={<Target className="h-4 w-4 text-amazon" />} title="Recommendation">
                <p className="text-sm">{opp.recommendation}</p>
              </Section>
            )}

            {/* Estimated Impact */}
            <Section icon={<TrendingUp className="h-4 w-4 text-green-500" />} title="Estimated Impact">
              <div className="grid grid-cols-2 gap-3 text-sm">
                <Metric label="Revenue uplift" value={opp.estimated_revenue_uplift != null ? formatPLN(opp.estimated_revenue_uplift) : "—"} />
                <Metric label="Profit uplift" value={opp.estimated_profit_uplift != null ? formatPLN(opp.estimated_profit_uplift) : "—"} />
                <Metric label="Margin uplift" value={opp.estimated_margin_uplift != null ? `${opp.estimated_margin_uplift.toFixed(1)}pp` : "—"} />
                <Metric label="Units uplift" value={opp.estimated_units_uplift != null ? `+${opp.estimated_units_uplift.toFixed(0)}` : "—"} />
              </div>
            </Section>

            {/* Scores */}
            <Section icon={<Flag className="h-4 w-4 text-purple-400" />} title="Scores">
              <div className="grid grid-cols-3 gap-3 text-sm">
                <Metric label="Confidence" value={`${opp.confidence_score.toFixed(0)}%`} />
                <Metric label="Effort" value={opp.effort_score != null ? `${opp.effort_score.toFixed(0)}/100` : "—"} />
                <Metric label="Owner" value={opp.owner_role?.replace(/_/g, " ") || "—"} />
              </div>
            </Section>

            {/* Source Signals */}
            {opp.source_signals_json != null && typeof opp.source_signals_json === "object" && Object.keys(opp.source_signals_json as Record<string, unknown>).length > 0 ? (
              <Section icon={<FileText className="h-4 w-4 text-blue-400" />} title="Source Signals">
                <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
                  {Object.entries(opp.source_signals_json as Record<string, unknown>).map(([k, v]) => (
                    <div key={k} className="flex justify-between py-0.5 border-b border-border/30">
                      <span className="text-muted-foreground">{k.replace(/_/g, " ")}</span>
                      <span className="font-medium tabular-nums">{typeof v === "number" ? v.toLocaleString("pl-PL", { maximumFractionDigits: 2 }) : String(v)}</span>
                    </div>
                  ))}
                </div>
              </Section>
            ) : null}

            {/* Blockers */}
            {Array.isArray(opp.blocker_json) && (opp.blocker_json as unknown[]).length > 0 ? (
              <Section icon={<XCircle className="h-4 w-4 text-red-400" />} title="Blockers">
                <ul className="text-sm list-disc pl-5 space-y-0.5">
                  {(opp.blocker_json as unknown[]).map((b: unknown, i: number) => <li key={i}>{typeof b === "string" ? b : JSON.stringify(b)}</li>)}
                </ul>
              </Section>
            ) : null}

            {/* Action buttons */}
            {(opp.status === "new" || opp.status === "in_review" || opp.status === "accepted") && (
              <div className="flex items-center gap-2 pt-2 border-t border-border">
                {(opp.status === "new" || opp.status === "in_review") && (
                  <>
                    <button disabled={accept.isPending} onClick={() => accept.mutate()}
                      className="flex items-center gap-1.5 rounded-lg bg-green-600 px-4 py-2 text-sm font-medium text-white hover:bg-green-500 disabled:opacity-50">
                      <CheckCircle2 className="h-4 w-4" /> Accept
                    </button>
                    <button disabled={reject.isPending} onClick={() => reject.mutate()}
                      className="flex items-center gap-1.5 rounded-lg bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-500 disabled:opacity-50">
                      <XCircle className="h-4 w-4" /> Reject
                    </button>
                  </>
                )}
                {opp.status === "accepted" && (
                  <button disabled={complete.isPending} onClick={() => complete.mutate()}
                    className="flex items-center gap-1.5 rounded-lg bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-500 disabled:opacity-50">
                    <CheckCircle2 className="h-4 w-4" /> Mark Complete
                  </button>
                )}
              </div>
            )}

            {/* Outcome Analysis */}
            {executions && executions.length > 0 && (
              <Section icon={<BarChart3 className="h-4 w-4 text-amazon" />} title="Outcome Analysis">
                {executions.map((ex: OpportunityExecution) => (
                  <div key={ex.execution_id} className="rounded-lg border border-border p-3 space-y-3">
                    <div className="flex items-center justify-between text-xs">
                      <span className="font-medium">Execution #{ex.execution_id} · {ex.action_type}</span>
                      <span className={cn("rounded px-1.5 py-0.5 text-[10px] font-bold uppercase",
                        ex.status === "evaluated" ? "bg-emerald-500/15 text-emerald-400" :
                        ex.status === "monitoring" ? "bg-blue-500/15 text-blue-400" : "bg-zinc-500/15 text-zinc-400"
                      )}>{ex.status}</span>
                    </div>
                    {ex.baseline_metrics && Object.keys(ex.baseline_metrics).length > 0 && (
                      <div>
                        <p className="text-[10px] text-muted-foreground mb-1 uppercase font-bold">Baseline</p>
                        <div className="grid grid-cols-3 gap-1">
                          {Object.entries(ex.baseline_metrics).map(([k, v]) => (
                            <div key={k} className="rounded bg-muted/20 px-1.5 py-1">
                              <p className="text-[9px] text-muted-foreground">{k.replace(/_/g, " ")}</p>
                              <p className="text-xs font-medium tabular-nums">{typeof v === "number" ? v.toFixed(1) : v}</p>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                    {ex.outcomes.length > 0 && ex.outcomes.map((oc) => (
                      <div key={oc.monitoring_days} className="rounded bg-card border border-border/50 p-2 space-y-1">
                        <div className="flex items-center justify-between text-xs">
                          <span>{oc.monitoring_days}-day window</span>
                          {oc.success_label && (
                            <span className={cn("rounded px-1.5 py-0.5 text-[10px] font-bold uppercase",
                              oc.success_label === "overperformed" ? "bg-emerald-500/15 text-emerald-400" :
                              oc.success_label === "on_target" ? "bg-green-500/15 text-green-400" :
                              oc.success_label === "partial_success" ? "bg-amber-500/15 text-amber-400" :
                              "bg-red-500/15 text-red-400"
                            )}>
                              {oc.success_label.replace(/_/g, " ")} · {Math.round((oc.success_score ?? 0) * 100)}%
                            </span>
                          )}
                        </div>
                        {oc.delta && (
                          <div className="grid grid-cols-4 gap-1 text-xs">
                            {Object.entries(oc.delta).map(([k, v]) => (
                              <div key={k}>
                                <p className="text-[9px] text-muted-foreground">{k.replace(/_/g, " ")}</p>
                                <p className={cn("tabular-nums font-medium",
                                  typeof v === "number" && v > 0 ? "text-green-400" :
                                  typeof v === "number" && v < 0 ? "text-red-400" : ""
                                )}>
                                  {typeof v === "number" ? (v > 0 ? "+" : "") + v.toFixed(1) : v}
                                </p>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    ))}
                    {ex.outcomes.length === 0 && (
                      <p className="text-xs text-muted-foreground">Monitoring in progress…</p>
                    )}
                  </div>
                ))}
              </Section>
            )}

            {/* Timeline */}
            {timeline.length > 0 && (
              <Section icon={<Clock className="h-4 w-4 text-muted-foreground" />} title="Timeline">
                <ol className="relative border-l border-border ml-1">
                  {timeline.map((log, i) => (
                    <li key={i} className="mb-3 ml-4">
                      <div className="absolute -left-1.5 mt-1 h-3 w-3 rounded-full border bg-card border-border" />
                      <p className="text-xs text-muted-foreground">{log.created_at ? new Date(log.created_at).toLocaleString("pl-PL") : "—"}</p>
                      <p className="text-sm font-medium">{log.action}</p>
                      {log.actor && <p className="text-xs text-muted-foreground flex items-center gap-1"><User className="h-3 w-3" />{log.actor}</p>}
                      {log.note && <p className="text-xs mt-0.5">{log.note}</p>}
                    </li>
                  ))}
                </ol>
              </Section>
            )}
          </div>
        )}
      </div>
    </>
  );
}

function InfoCell({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-border px-2 py-1">
      <p className="text-[10px] text-muted-foreground">{label}</p>
      <p className="font-mono font-medium truncate" title={value}>{value}</p>
    </div>
  );
}

function Section({ icon, title, children }: { icon: React.ReactNode; title: string; children: React.ReactNode }) {
  return (
    <div className="space-y-2">
      <h4 className="flex items-center gap-2 text-sm font-semibold">{icon}{title}</h4>
      {children}
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="font-semibold">{value}</p>
    </div>
  );
}
