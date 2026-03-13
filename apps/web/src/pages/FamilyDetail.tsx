import { useEffect, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getFamily,
  getFamilyLinks,
  getFamilyCoverage,
  getFamilyIssues,
  updateLinkStatus,
  analyzeRestructure,
  analyzeRestructureAll,
  executeRestructureStart,
  getExecuteRestructureStatus,
  type FamilyChild,
  type ChildMarketLink,
  type FamilyCoverage,
  type FamilyIssue,
  type RestructureAnalysis,
  type RestructureAllResult,
  type ExecuteRestructureResult,
  type ExecuteRestructureRunStatus,
  type ExecuteRestructureStep,
} from "@/lib/api";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Progress } from "@/components/ui/progress";
import { ClientExportButton } from "@/components/shared";
import {
  ArrowLeft,
  Check,
  X,
  AlertTriangle,
  Search,
  Loader2,
  Play,
  ChevronDown,
  ChevronUp,
  Zap,
  Shield,
} from "lucide-react";

const MARKETPLACES = [
  { id: "A1C3SOZRARQ6R3", code: "PL" },
  { id: "A13V1IB3VIYZZH", code: "FR" },
  { id: "APJ6JRA9NG5V4",  code: "IT" },
  { id: "A1RKKUPIHCS9HS", code: "ES" },
  { id: "A1805IZSGTT6HS", code: "NL" },
  { id: "AMEN7PMS3EDWL",  code: "BE" },
  { id: "A2NODRKZP88ZB9", code: "SE" },
  { id: "A28R8C7NBKEWEA", code: "IE" },
] as const;

const STATUS_COLORS: Record<string, string> = {
  safe_auto: "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300",
  proposed: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300",
  needs_review: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300",
  unmatched: "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300",
  approved: "bg-green-100 text-green-800",
  rejected: "bg-gray-100 text-gray-800",
};

export default function FamilyDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const familyId = Number(id);

  const [selectedMp, setSelectedMp] = useState<string | undefined>();
  const [restructureMp, setRestructureMp] = useState<string>("A13V1IB3VIYZZH"); // FR by default
  const [analysisResult, setAnalysisResult] = useState<RestructureAnalysis | null>(null);
  const [allAnalysisResult, setAllAnalysisResult] = useState<RestructureAllResult | null>(null);
  const [execResult, setExecResult] = useState<ExecuteRestructureResult | null>(null);
  const [executeRunId, setExecuteRunId] = useState<string | null>(null);
  const [confirmExec, setConfirmExec] = useState(false);
  const [showDetails, setShowDetails] = useState(false);
  const runStorageKey = `family-restructure-run:${familyId}:${restructureMp}`;

  // Sync restructure marketplace with selected marketplace
  useEffect(() => {
    if (selectedMp) {
      setRestructureMp(selectedMp);
      setAnalysisResult(null);
      setExecResult(null);
      setExecuteRunId(null);
      setConfirmExec(false);
    }
  }, [selectedMp]);

  const { data: family, isLoading } = useQuery({
    queryKey: ["family", familyId],
    queryFn: () => getFamily(familyId),
    enabled: !!familyId,
  });

  const { data: links } = useQuery({
    queryKey: ["family-links", familyId, selectedMp],
    queryFn: () => getFamilyLinks(familyId, selectedMp),
    enabled: !!familyId,
  });

  const { data: coverage } = useQuery({
    queryKey: ["family-coverage", familyId],
    queryFn: () => getFamilyCoverage(familyId),
    enabled: !!familyId,
  });

  const { data: issues } = useQuery({
    queryKey: ["family-issues", familyId],
    queryFn: () => getFamilyIssues(familyId),
    enabled: !!familyId,
  });

  const statusMutation = useMutation({
    mutationFn: (vars: { masterKey: string; marketplace: string; status: string }) =>
      updateLinkStatus(familyId, {
        status: vars.status,
        master_key: vars.masterKey,
        marketplace: vars.marketplace,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["family-links", familyId] });
      queryClient.invalidateQueries({ queryKey: ["family-coverage", familyId] });
    },
  });

  const analyzeMutation = useMutation({
    mutationFn: (marketplaceId: string) => analyzeRestructure(familyId, marketplaceId),
    onSuccess: (data) => {
      setAnalysisResult(data);
      setAllAnalysisResult(null);
      setExecResult(null);
      setConfirmExec(false);
    },
  });

  const analyzeAllMutation = useMutation({
    mutationFn: () => analyzeRestructureAll(familyId),
    onSuccess: (data) => {
      setAllAnalysisResult(data);
      setAnalysisResult(null);
      setExecResult(null);
      setConfirmExec(false);
    },
  });

  const executeStartMutation = useMutation({
    mutationFn: ({ marketplaceId, dryRun }: { marketplaceId: string; dryRun: boolean }) =>
      executeRestructureStart(familyId, marketplaceId, dryRun),
    onSuccess: (data) => {
      setExecuteRunId(data.run_id);
      localStorage.setItem(runStorageKey, data.run_id);
      setConfirmExec(false);
    },
  });

  const { data: executeRunStatus } = useQuery<ExecuteRestructureRunStatus>({
    queryKey: ["family-restructure-run", familyId, restructureMp, executeRunId],
    queryFn: () => getExecuteRestructureStatus(familyId, restructureMp, executeRunId ?? undefined),
    enabled: !!familyId && !!executeRunId,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status === "running" ? 3000 : false;
    },
    refetchOnWindowFocus: true,
  });

  const { data: latestExecuteRunStatus } = useQuery<ExecuteRestructureRunStatus>({
    queryKey: ["family-restructure-run-latest", familyId, restructureMp],
    queryFn: () => getExecuteRestructureStatus(familyId, restructureMp),
    enabled: !!familyId && !executeRunId,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status === "running" ? 3000 : false;
    },
    retry: false,
  });

  useEffect(() => {
    if (!familyId || !restructureMp) return;
    const saved = localStorage.getItem(runStorageKey);
    if (saved) {
      setExecuteRunId(saved);
    }
  }, [familyId, restructureMp, runStorageKey]);

  useEffect(() => {
    if (executeRunId || !latestExecuteRunStatus) return;
    if (latestExecuteRunStatus.status === "running") {
      setExecuteRunId(latestExecuteRunStatus.run_id);
      localStorage.setItem(runStorageKey, latestExecuteRunStatus.run_id);
    }
  }, [executeRunId, latestExecuteRunStatus, runStorageKey]);

  useEffect(() => {
    if (!executeRunStatus) return;
    if (executeRunStatus.status !== "running") {
      if (executeRunStatus.result) {
        setExecResult(executeRunStatus.result);
      } else if (executeRunStatus.status === "failed") {
        setExecResult({
          status: "error",
          dry_run: false,
          errors: 1,
          total_steps: 0,
          children_planned: executeRunStatus.children_total,
          children_actionable: executeRunStatus.children_total,
          children_skipped: 0,
          steps: [],
          error: executeRunStatus.error_message ?? "Execution failed",
        });
      }
      setExecuteRunId(null);
      localStorage.removeItem(runStorageKey);
      queryClient.invalidateQueries({ queryKey: ["family", familyId] });
      queryClient.invalidateQueries({ queryKey: ["family-links", familyId] });
      queryClient.invalidateQueries({ queryKey: ["family-coverage", familyId] });
      queryClient.invalidateQueries({ queryKey: ["family-issues", familyId] });
    }
  }, [executeRunStatus, queryClient, familyId, runStorageKey]);

  if (isLoading || !family) {
    return (
      <div className="flex items-center justify-center h-64 text-muted-foreground">
        Loading…
      </div>
    );
  }

  const children = family.children ?? [];
  const allCoverage = coverage ?? [];
  const allIssues = issues ?? [];
  const isExecuteRunning =
    executeStartMutation.isPending ||
    (!!executeRunStatus && executeRunStatus.status === "running");
  const anyBusy = analyzeMutation.isPending || analyzeAllMutation.isPending || isExecuteRunning;

  return (
    <div className="space-y-6 p-6">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Button variant="ghost" size="sm" onClick={() => navigate("/families")}>
          <ArrowLeft className="h-4 w-4 mr-1" /> Back
        </Button>
        <div className="flex-1">
          <h1 className="text-xl font-bold">
            Family: {family.de_parent_asin}
          </h1>
          <p className="text-sm text-muted-foreground">
            {family.brand ?? "—"} · {family.category ?? "—"}
            {family.variation_theme_de && ` · Theme: ${family.variation_theme_de}`}
            {" · "}{children.length} DE children
          </p>
        </div>
      </div>

      {/* ================================================================= */}
      {/*  RESTRUCTURE PIPELINE — primary section                           */}
      {/* ================================================================= */}
      <Card className="border-2 border-primary/20">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between flex-wrap gap-3">
            <CardTitle className="flex items-center gap-2">
              <Zap className="h-5 w-5 text-primary" />
              Restructure Pipeline
            </CardTitle>
            <div className="flex items-center gap-2 flex-wrap">
              <select
                className="h-9 rounded-md border border-input bg-background px-3 text-sm"
                value={restructureMp}
                onChange={(e) => {
                  setRestructureMp(e.target.value);
                  setAnalysisResult(null);
                  setExecResult(null);
                  setExecuteRunId(null);
                  setConfirmExec(false);
                }}
              >
                {MARKETPLACES.map((mp) => (
                  <option key={mp.id} value={mp.id}>{mp.code}</option>
                ))}
              </select>
              <Button
                size="sm"
                onClick={() => analyzeMutation.mutate(restructureMp)}
                disabled={anyBusy}
              >
                {analyzeMutation.isPending ? (
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                ) : (
                  <Search className="mr-2 h-4 w-4" />
                )}
                Analyze
              </Button>
              <Button
                size="sm"
                variant="outline"
                onClick={() => analyzeAllMutation.mutate()}
                disabled={anyBusy}
              >
                {analyzeAllMutation.isPending ? (
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                ) : (
                  <Search className="mr-2 h-4 w-4" />
                )}
                Analyze All
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Single MP analysis */}
          {analysisResult && (
            <>
              <RestructureReport data={analysisResult} />

              {/* Execute controls */}
              {analysisResult.verdict === "needs_restructure" && !execResult && (
                <div className="border-t pt-4 space-y-3">
                  <h4 className="text-sm font-semibold flex items-center gap-2">
                    <Play className="h-4 w-4" /> Execute
                  </h4>

                  {!confirmExec ? (
                    <div className="flex items-center gap-2">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() =>
                          executeStartMutation.mutate({ marketplaceId: restructureMp, dryRun: true })
                        }
                        disabled={anyBusy}
                      >
                        {executeStartMutation.isPending ? (
                          <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        ) : (
                          <Shield className="mr-2 h-4 w-4" />
                        )}
                        Dry Run
                      </Button>
                      <Button
                        size="sm"
                        variant="destructive"
                        onClick={() => setConfirmExec(true)}
                        disabled={anyBusy}
                      >
                        <Zap className="mr-2 h-4 w-4" />
                        Execute
                      </Button>
                    </div>
                  ) : (
                    <div className="rounded-lg border-2 border-destructive/50 bg-destructive/5 p-4 space-y-3">
                      <p className="text-sm font-medium text-destructive">
                        This will send PATCH/DELETE requests to Amazon SP-API for marketplace{" "}
                        <strong>
                          {MARKETPLACES.find((m) => m.id === restructureMp)?.code ?? restructureMp}
                        </strong>
                        . {analysisResult.actions.length} actions will be executed.
                      </p>
                      <div className="flex items-center gap-2">
                        <Button
                          size="sm"
                          variant="destructive"
                          onClick={() =>
                            executeStartMutation.mutate({ marketplaceId: restructureMp, dryRun: false })
                          }
                          disabled={isExecuteRunning}
                        >
                          {isExecuteRunning ? (
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                          ) : (
                            <Zap className="mr-2 h-4 w-4" />
                          )}
                          {isExecuteRunning
                            ? `Executing ${executeRunStatus?.children_done ?? 0}/${executeRunStatus?.children_total ?? 0}`
                            : "Confirm Execute"}
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => setConfirmExec(false)}
                          disabled={isExecuteRunning}
                        >
                          Cancel
                        </Button>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </>
          )}

          {/* All MPs analysis */}
          {allAnalysisResult && (
            <div className="space-y-4">
              <div className="flex gap-4 text-sm">
                <span className="text-green-600 font-medium">
                  Aligned: {allAnalysisResult.aligned}
                </span>
                <span className="text-red-600 font-medium">
                  Needs restructure: {allAnalysisResult.needs_restructure}
                </span>
                <span className="text-muted-foreground">
                  No data: {allAnalysisResult.no_data}
                </span>
              </div>
              {Object.entries(allAnalysisResult.results).map(([mpCode, result]) => (
                <div key={mpCode} className="border rounded-lg p-3">
                  <div className="flex items-center gap-2 mb-2">
                    <span className="font-bold text-sm">{mpCode}</span>
                    <VerdictBadge verdict={result.verdict} />
                  </div>
                  {result.verdict === "needs_restructure" && (
                    <RestructureReport data={result} compact />
                  )}
                  {result.verdict === "no_data" && (
                    <p className="text-xs text-muted-foreground">No listings found.</p>
                  )}
                  {result.verdict === "aligned" && (
                    <p className="text-xs text-green-600">
                      All {result.target_state.children_found} children aligned under DE parent.
                    </p>
                  )}
                </div>
              ))}
            </div>
          )}

          {/* Execution result */}
          {isExecuteRunning && (
            <div className="rounded-lg border border-primary/40 bg-primary/5 p-3 text-sm space-y-2">
              <div className="flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin" />
                <span className="font-medium">
                  Execute in progress: {executeRunStatus?.children_done ?? 0}/{executeRunStatus?.children_total ?? 0} children
                </span>
              </div>
              <Progress value={executeRunStatus?.progress_pct ?? 0} className="h-2" />
              <p className="text-xs text-muted-foreground">
                {executeRunStatus?.progress_message ?? "Executing restructure steps..."}
              </p>
            </div>
          )}

          {execResult && (
            <ExecutionLog
              result={execResult}
              onExecute={() =>
                executeStartMutation.mutate({ marketplaceId: restructureMp, dryRun: false })
              }
              isExecuting={isExecuteRunning}
            />
          )}

          {/* Empty state */}
          {!analysisResult && !allAnalysisResult && !execResult && (
            <p className="text-sm text-muted-foreground">
              Select a marketplace and click "Analyze" to compare structure vs DE canonical.
            </p>
          )}
        </CardContent>
      </Card>

      {/* ================================================================= */}
      {/*  COLLAPSIBLE DETAILS                                              */}
      {/* ================================================================= */}
      <div>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setShowDetails(!showDetails)}
          className="text-muted-foreground"
        >
          {showDetails ? <ChevronUp className="mr-2 h-4 w-4" /> : <ChevronDown className="mr-2 h-4 w-4" />}
          {showDetails ? "Hide" : "Show"} Details (Children, Coverage, Links, Issues)
        </Button>

        {showDetails && (
          <div className="space-y-6 mt-4">
            {/* Coverage overview */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm text-muted-foreground">DE Children</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-2xl font-bold">{children.length}</p>
                </CardContent>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm text-muted-foreground">Marketplaces</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-2xl font-bold">
                    {(family.market_links ?? []).filter((m) => m.status !== "unmapped").length} / {(family.market_links ?? []).length || 12}
                  </p>
                </CardContent>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm text-muted-foreground">Issues</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-2xl font-bold text-yellow-600">{allIssues.length}</p>
                </CardContent>
              </Card>
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm text-muted-foreground">Avg Confidence</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-2xl font-bold">
                    {(family.market_links ?? []).length > 0
                      ? Math.round(
                          (family.market_links ?? []).reduce((s, m) => s + m.confidence_avg, 0) /
                            (family.market_links ?? []).length
                        )
                      : 0}
                    %
                  </p>
                </CardContent>
              </Card>
            </div>

            {/* Split view: children + marketplace coverage */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle>DE Canonical Children</CardTitle>
                </CardHeader>
                <CardContent>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>ASIN</TableHead>
                        <TableHead>SKU</TableHead>
                        <TableHead>EAN</TableHead>
                        <TableHead>Key Type</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {children.map((c: FamilyChild) => (
                        <TableRow key={c.id}>
                          <TableCell className="font-mono text-xs">{c.de_child_asin}</TableCell>
                          <TableCell className="text-xs">{c.sku_de ?? "—"}</TableCell>
                          <TableCell className="text-xs">{c.ean_de ?? "—"}</TableCell>
                          <TableCell>
                            <Badge variant="outline">{c.key_type}</Badge>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Marketplace Coverage</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  {allCoverage.length === 0 ? (
                    <p className="text-sm text-muted-foreground">No coverage data.</p>
                  ) : (
                    allCoverage.map((cov: FamilyCoverage) => (
                      <div
                        key={cov.marketplace}
                        className="flex items-center gap-4 cursor-pointer hover:bg-accent/50 rounded-md p-2 -mx-2"
                        onClick={() =>
                          setSelectedMp(cov.marketplace === selectedMp ? undefined : cov.marketplace)
                        }
                      >
                        <span className="w-8 font-bold text-sm">{cov.marketplace}</span>
                        <div className="flex-1">
                          <Progress value={cov.coverage_pct} className="h-2" />
                        </div>
                        <span className="text-sm font-medium w-12 text-right">
                          {cov.coverage_pct}%
                        </span>
                        <span className="text-xs text-muted-foreground w-20">
                          {cov.matched_children_count}/{cov.de_children_count}
                        </span>
                        {cov.theme_mismatch && (
                          <AlertTriangle className="h-4 w-4 text-yellow-500" />
                        )}
                      </div>
                    ))
                  )}
                </CardContent>
              </Card>
            </div>

            {/* Child market links */}
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle>
                    Child Market Links
                    {selectedMp && (
                      <Badge variant="secondary" className="ml-2">
                        {selectedMp}
                        <button
                          className="ml-1"
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedMp(undefined);
                          }}
                        >
                          <X className="h-3 w-3" />
                        </button>
                      </Badge>
                    )}
                  </CardTitle>
                  <ClientExportButton data={links ?? []} filename={`family_${familyId}_links`} />
                </div>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Marketplace</TableHead>
                      <TableHead>Master Key</TableHead>
                      <TableHead>Target ASIN</TableHead>
                      <TableHead>Match Type</TableHead>
                      <TableHead className="text-center">Confidence</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {(links ?? []).length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={7} className="text-center py-4 text-muted-foreground">
                          No links.
                        </TableCell>
                      </TableRow>
                    ) : (
                      (links ?? []).map((l: ChildMarketLink, idx: number) => (
                        <TableRow key={`${l.marketplace}-${l.master_key}-${idx}`}>
                          <TableCell className="font-bold">{l.marketplace}</TableCell>
                          <TableCell className="font-mono text-xs max-w-[120px] truncate">
                            {l.master_key}
                          </TableCell>
                          <TableCell className="font-mono text-xs">
                            {l.target_child_asin ?? "—"}
                          </TableCell>
                          <TableCell>
                            <Badge variant="outline">{l.match_type}</Badge>
                          </TableCell>
                          <TableCell className="text-center">
                            <span
                              className={`font-bold ${
                                l.confidence >= 90
                                  ? "text-green-600"
                                  : l.confidence >= 75
                                    ? "text-blue-600"
                                    : l.confidence >= 60
                                      ? "text-yellow-600"
                                      : "text-red-600"
                              }`}
                            >
                              {l.confidence}
                            </span>
                          </TableCell>
                          <TableCell>
                            <span
                              className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${
                                STATUS_COLORS[l.status] ?? "bg-gray-100 text-gray-800"
                              }`}
                            >
                              {l.status}
                            </span>
                          </TableCell>
                          <TableCell>
                            {(l.status === "proposed" || l.status === "needs_review") && (
                              <div className="flex gap-1">
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-7 w-7 p-0 text-green-600"
                                  title="Approve"
                                  onClick={() =>
                                    statusMutation.mutate({
                                      masterKey: l.master_key,
                                      marketplace: l.marketplace,
                                      status: "approved",
                                    })
                                  }
                                >
                                  <Check className="h-3.5 w-3.5" />
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  className="h-7 w-7 p-0 text-red-600"
                                  title="Reject"
                                  onClick={() =>
                                    statusMutation.mutate({
                                      masterKey: l.master_key,
                                      marketplace: l.marketplace,
                                      status: "rejected",
                                    })
                                  }
                                >
                                  <X className="h-3.5 w-3.5" />
                                </Button>
                              </div>
                            )}
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>

            {/* Issues */}
            {allIssues.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <AlertTriangle className="h-4 w-4 text-yellow-500" />
                    Issues ({allIssues.length})
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {allIssues.map((issue: FamilyIssue) => (
                      <div
                        key={issue.id}
                        className="flex items-center gap-3 rounded-md border p-3"
                      >
                        <Badge
                          variant={issue.severity === "warning" ? "destructive" : "secondary"}
                        >
                          {issue.severity}
                        </Badge>
                        <span className="text-sm font-medium">{issue.issue_type}</span>
                        {issue.marketplace && (
                          <Badge variant="outline">{issue.marketplace}</Badge>
                        )}
                        {issue.payload && (
                          <span className="text-xs text-muted-foreground">
                            {JSON.stringify(issue.payload)}
                          </span>
                        )}
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

/* ======================================================================= */
/*  Sub-components                                                         */
/* ======================================================================= */

function VerdictBadge({ verdict }: { verdict: string }) {
  if (verdict === "aligned")
    return <Badge className="bg-green-100 text-green-800">Aligned</Badge>;
  if (verdict === "needs_restructure")
    return <Badge variant="destructive">Needs Restructure</Badge>;
  return <Badge variant="secondary">No Data</Badge>;
}

function RestructureReport({
  data,
  compact,
}: {
  data: RestructureAnalysis;
  compact?: boolean;
}) {
  return (
    <div className="space-y-3">
      {!compact && (
        <div className="flex items-center gap-3">
          <span className="font-bold">{data.marketplace}</span>
          <VerdictBadge verdict={data.verdict} />
        </div>
      )}

      {/* Stats row */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs">
        <div className="rounded bg-muted p-2">
          <span className="text-muted-foreground">DE children</span>
          <p className="font-bold">{data.de_canonical.children_count}</p>
        </div>
        <div className="rounded bg-muted p-2">
          <span className="text-muted-foreground">Found on {data.marketplace}</span>
          <p className="font-bold">{data.target_state.children_found}</p>
        </div>
        <div className="rounded bg-green-50 dark:bg-green-900/20 p-2">
          <span className="text-muted-foreground">Aligned</span>
          <p className="font-bold text-green-600">
            {data.target_state.children_aligned ?? 0}
          </p>
        </div>
        <div className="rounded bg-red-50 dark:bg-red-900/20 p-2">
          <span className="text-muted-foreground">Misaligned</span>
          <p className="font-bold text-red-600">
            {data.target_state.children_misaligned ?? 0}
          </p>
        </div>
        <div className="rounded bg-yellow-50 dark:bg-yellow-900/20 p-2">
          <span className="text-muted-foreground">Missing</span>
          <p className="font-bold text-yellow-600">
            {data.missing_children?.length ?? 0}
          </p>
        </div>
      </div>

      {/* Parent ASINs on target MP */}
      {data.target_state.parent_asins &&
        Object.keys(data.target_state.parent_asins).length > 0 && (
          <div>
            <h4 className="text-xs font-semibold mb-1">
              Parent ASINs on {data.marketplace}:
            </h4>
            <div className="flex flex-wrap gap-2">
              {Object.entries(data.target_state.parent_asins).map(([pa, count]) => (
                <span
                  key={pa ?? "null"}
                  className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-mono ${
                    pa === data.de_canonical.de_parent_asin
                      ? "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300"
                      : "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300"
                  }`}
                >
                  {pa ?? "none"} ({count})
                  {pa === data.de_canonical.de_parent_asin && " ✓ DE"}
                </span>
              ))}
            </div>
          </div>
        )}

      {/* Foreign parents */}
      {data.foreign_parents && data.foreign_parents.length > 0 && (
        <div>
          <h4 className="text-xs font-semibold text-red-600 mb-1">
            Foreign Parents (to delete):
          </h4>
          {data.foreign_parents.map((fp) => (
            <div
              key={fp.parent_asin}
              className="flex items-center gap-2 text-xs p-1 rounded bg-red-50 dark:bg-red-900/10 mb-1"
            >
              <span className="font-mono font-bold">{fp.parent_asin}</span>
              <span className="text-muted-foreground">
                {fp.children_count} children
              </span>
            </div>
          ))}
        </div>
      )}

      {/* Actions plan */}
      {data.actions.length > 0 && !compact && (
        <div>
          <h4 className="text-xs font-semibold mb-1">Proposed Actions:</h4>
          <div className="space-y-1 max-h-48 overflow-y-auto">
            {data.actions.map((action, idx) => (
              <div
                key={idx}
                className={`text-xs p-2 rounded border ${
                  action.action === "DELETE_FOREIGN_PARENT"
                    ? "border-red-200 bg-red-50 dark:bg-red-900/10"
                    : action.action === "CREATE_PARENT"
                      ? "border-blue-200 bg-blue-50 dark:bg-blue-900/10"
                      : "border-yellow-200 bg-yellow-50 dark:bg-yellow-900/10"
                }`}
              >
                <span className="font-bold mr-2">
                  {action.action === "DELETE_FOREIGN_PARENT" && "🗑️ DELETE"}
                  {action.action === "CREATE_PARENT" && "➕ CREATE"}
                  {action.action === "REASSIGN_CHILD" && "🔄 REASSIGN"}
                </span>
                {action.note}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Summary */}
      {!compact && data.summary && (
        <details className="text-xs">
          <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
            Full summary
          </summary>
          <pre className="bg-muted p-3 rounded-md whitespace-pre-wrap font-mono mt-1">
            {data.summary}
          </pre>
        </details>
      )}
    </div>
  );
}

function ExecutionLog({
  result,
  onExecute,
  isExecuting,
}: {
  result: ExecuteRestructureResult;
  onExecute?: () => void;
  isExecuting?: boolean;
}) {
  const isDryRun = result.dry_run || result.steps.some((s) => s.status === "dry_run");
  return (
    <div className="border-t pt-4 space-y-3">
      <div className="flex items-center gap-3">
        <h4 className="text-sm font-semibold">
          {isDryRun ? "Dry Run Results" : "Execution Results"}
        </h4>
        <Badge
          className={
            result.status === "completed"
              ? "bg-green-100 text-green-800"
              : result.status === "already_aligned" || result.status === "nothing_to_do"
                ? "bg-blue-100 text-blue-800"
                : result.status === "completed_with_errors"
                  ? "bg-yellow-100 text-yellow-800"
                  : "bg-red-100 text-red-800"
          }
        >
          {result.status}
        </Badge>
        <span className="text-xs text-muted-foreground">
          {result.total_steps ?? 0} steps · {result.errors} errors
          {result.children_planned != null && (
            <> · {result.children_actionable ?? 0}/{result.children_planned} children actionable</>
          )}
        </span>
      </div>

      <div className="text-xs space-y-1">
        {result.de_parent_sku && (
          <p className="text-muted-foreground">
            DE Parent SKU: <span className="font-mono bg-muted px-1 rounded">{result.de_parent_sku}</span>
            {result.de_parent_asin && (
              <> (ASIN: <span className="font-mono">{result.de_parent_asin}</span>)</>
            )}
          </p>
        )}
        {result.parent_on_target != null && (
          <p className={result.parent_on_target ? "text-green-600" : "text-orange-600"}>
            Parent on target MP: {result.parent_on_target ? "EXISTS" : "MISSING — will be created"}
          </p>
        )}
        {result.product_type_detected && result.product_type_detected !== "PRODUCT" && (
          <p className="text-muted-foreground">
            Product type: <span className="font-mono">{result.product_type_detected}</span>
          </p>
        )}
        {result.variation_theme && (
          <p className="text-muted-foreground">
            Variation theme: <span className="font-mono">{result.variation_theme}</span>
          </p>
        )}
        {result.child_attr_audit && (
          <p className={
            result.child_attr_audit.missing_color === 0 && result.child_attr_audit.missing_size === 0
              ? "text-green-600"
              : "text-orange-600"
          }>
            Child attrs ({result.child_attr_audit.sample_checked} checked):
            {" color "}
            {result.child_attr_audit.missing_color === 0
              ? <span className="font-semibold">OK</span>
              : <span className="font-semibold">{result.child_attr_audit.missing_color} missing</span>}
            {" · size "}
            {result.child_attr_audit.missing_size === 0
              ? <span className="font-semibold">OK</span>
              : <span className="font-semibold">{result.child_attr_audit.missing_size} missing</span>}
          </p>
        )}
        {result.pim_enrichment && result.pim_enrichment.total_missing > 0 && (
          <p className={result.pim_enrichment.pim_found > 0 ? "text-blue-600" : "text-muted-foreground"}>
            PIM enrichment: {result.pim_enrichment.pim_found}/{result.pim_enrichment.total_missing} found
            {result.pim_enrichment.patched > 0 && (
              <span className="font-semibold"> · {result.pim_enrichment.patched} patched</span>
            )}
          </p>
        )}
      </div>

      <div className="space-y-1 max-h-64 overflow-y-auto">
        {result.steps.map((step, idx) => (
          <StepRow key={idx} step={step} />
        ))}
      </div>

      {isDryRun && onExecute && (result.children_actionable ?? 0) > 0 && (
        <div className="rounded-lg border-2 border-orange-500/50 bg-orange-500/5 p-4 space-y-2">
          <p className="text-sm font-medium">
            Dry run OK — <strong>{result.children_actionable}</strong> children ready to reassign.
            {result.children_skipped != null && result.children_skipped > 0 && (
              <span className="text-muted-foreground"> ({result.children_skipped} skipped — no SKU)</span>
            )}
          </p>
          <Button
            size="sm"
            variant="destructive"
            onClick={onExecute}
            disabled={isExecuting}
            className="w-full sm:w-auto"
          >
            {isExecuting ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Zap className="mr-2 h-4 w-4" />
            )}
            Execute Now — Reassign {result.children_actionable} Children
          </Button>
        </div>
      )}
    </div>
  );
}

function StepRow({ step }: { step: ExecuteRestructureStep }) {
  const statusColor =
    step.status === "ACCEPTED" || step.status === "dry_run" || step.status === "ok"
      ? "text-green-600"
      : step.status === "skipped" || step.status === "info"
        ? "text-yellow-600"
        : step.status === "error"
          ? "text-red-600"
          : "text-muted-foreground";

  const actionLabel: Record<string, string> = {
    ORPHAN_CHILD: "🧷 ORPHAN",
    REASSIGN_CHILD: "🔄 REASSIGN",
    CREATE_PARENT: "📦 CREATE PARENT",
    PREFLIGHT_DE_CHILD: "🔍 PREFLIGHT",
    CHECK_PARENT_ON_TARGET: "🔎 CHECK PARENT",
    FOREIGN_PARENT_INFO: "ℹ️ FOREIGN",
    DELETE_FOREIGN_PARENT: "🗑️ DELETE",
    PREFLIGHT_CHECK: "🔍 PREFLIGHT",
    VALIDATE_THEME: "🎨 VARIATION THEME",
    AUDIT_CHILD_ATTRS: "📋 ATTR AUDIT",
    ENRICH_FROM_PIM: "🏭 PIM ENRICH",
    TRANSLATE_PARENT: "🌐 TRANSLATE",
  };

  return (
    <div className="flex items-center gap-2 text-xs p-2 rounded border">
      <span className="font-bold w-40 shrink-0">
        {actionLabel[step.action] ?? step.action}
      </span>
      {step.asin && (
        <span className="font-mono w-28 shrink-0">{step.asin}</span>
      )}
      {step.sku && (
        <span className="text-muted-foreground font-mono w-36 shrink-0 truncate">
          SKU: {step.sku}
        </span>
      )}
      {step.to_parent_sku && (
        <span className="text-muted-foreground truncate">
          → <span className="font-mono">{step.to_parent_sku}</span>
        </span>
      )}
      {step.action === "VALIDATE_THEME" && step.effective_theme && (
        <span className="text-muted-foreground truncate">
          <span className="font-mono">{step.effective_theme}</span>
          {step.allowed_themes && step.allowed_themes.length > 0 && (
            <span className="ml-1" title={step.allowed_themes.join(", ")}>
              ({step.allowed_themes.length} allowed)
            </span>
          )}
        </span>
      )}
      {step.action === "TRANSLATE_PARENT" && step.target_language && (
        <span className="text-muted-foreground truncate">
          → {step.target_language}
          {step.translated_fields && step.translated_fields.length > 0 && (
            <span className="ml-1">({step.translated_fields.join(", ")})</span>
          )}
        </span>
      )}
      {step.action === "AUDIT_CHILD_ATTRS" && (
        <span className="text-muted-foreground truncate">
          {step.sample_checked} checked · color: {step.missing_color === 0 ? "OK" : `${step.missing_color} missing`} · size: {step.missing_size === 0 ? "OK" : `${step.missing_size} missing`}
        </span>
      )}
      {step.action === "ENRICH_FROM_PIM" && (
        <span className="text-muted-foreground truncate">
          {step.total_missing} missing · {step.pim_found} in PIM · {step.dry_run ? "would patch" : "patched"} {step.patched ?? step.pim_found}
          {step.target_language && <span className="ml-1">→ {step.target_language}</span>}
        </span>
      )}
      <span className={`ml-auto font-semibold shrink-0 ${statusColor}`}>
        {step.status}
      </span>
      {step.reason && (
        <span className="text-muted-foreground truncate max-w-[200px]" title={step.reason}>
          {step.reason}
        </span>
      )}
      {step.error && (
        <span className="text-red-500 truncate max-w-[200px]" title={step.error}>
          {step.error}
        </span>
      )}
    </div>
  );
}
