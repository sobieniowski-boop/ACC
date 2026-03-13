import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import {
  getFixPackages,
  generateFixPackages,
  approveFixPackage,
  type FixPackage,
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Package,
  RefreshCw,
  Check,
  Eye,
} from "lucide-react";
import { ClientExportButton, ServerPagination } from "@/components/shared";
import { useAuthStore } from "@/store/authStore";

const STATUS_BADGE: Record<string, "default" | "secondary" | "destructive" | "outline"> = {
  draft: "secondary",
  pending_approve: "outline",
  approved: "default",
  applied: "default",
};

export default function FixPackagesPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [page, setPage] = useState(1);
  const [marketplaceFilter, setMarketplaceFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [viewPlan, setViewPlan] = useState<FixPackage | null>(null);
  const pageSize = 30;

  // Get user info for approval
  const userId = useAuthStore((s) => s.accessToken); // We'll use a simple identifier

  const { data, isLoading } = useQuery({
    queryKey: ["fix-packages", page, marketplaceFilter, statusFilter],
    queryFn: () =>
      getFixPackages({
        page,
        page_size: pageSize,
        ...(marketplaceFilter ? { marketplace: marketplaceFilter } : {}),
        ...(statusFilter ? { status: statusFilter } : {}),
      }),
  });

  const generateMut = useMutation({
    mutationFn: () => generateFixPackages(),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["fix-packages"] }),
  });

  const approveMut = useMutation({
    mutationFn: (pkgId: number) => approveFixPackage(pkgId, "current_user"),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["fix-packages"] }),
  });

  const items = data?.items ?? [];
  const total = data?.total ?? 0;
  const totalPages = Math.ceil(total / pageSize);

  return (
    <div className="space-y-6 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Fix Packages</h1>
          <p className="text-sm text-muted-foreground">
            Actionable plans to fix variation family structures per marketplace
          </p>
        </div>
        <Button
          onClick={() => generateMut.mutate()}
          disabled={generateMut.isPending}
        >
          <RefreshCw className={`mr-2 h-4 w-4 ${generateMut.isPending ? "animate-spin" : ""}`} />
          Generate Packages
        </Button>
      </div>

      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="flex items-center gap-2">
              <Package className="h-4 w-4" />
              {total} packages
            </CardTitle>
            <div className="flex gap-2">
              <ClientExportButton data={items} filename="fix_packages" />
              <Select value={statusFilter} onValueChange={(v) => { setStatusFilter(v === "all" ? "" : v); setPage(1); }}>
                <SelectTrigger className="w-[160px]">
                  <SelectValue placeholder="All statuses" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All statuses</SelectItem>
                  <SelectItem value="draft">Draft</SelectItem>
                  <SelectItem value="pending_approve">Pending Approve</SelectItem>
                  <SelectItem value="approved">Approved</SelectItem>
                  <SelectItem value="applied">Applied</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>ID</TableHead>
                <TableHead>Marketplace</TableHead>
                <TableHead>Family ID</TableHead>
                <TableHead>Steps</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Generated</TableHead>
                <TableHead>Approved By</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={8} className="text-center py-8 text-muted-foreground">
                    Loading…
                  </TableCell>
                </TableRow>
              ) : items.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="text-center py-8 text-muted-foreground">
                    No fix packages. Click "Generate Packages" to create.
                  </TableCell>
                </TableRow>
              ) : (
                items.map((pkg: FixPackage) => {
                  const plan = pkg.action_plan as { steps?: unknown[]; summary?: Record<string, number> };
                  const stepCount = Array.isArray(plan?.steps) ? plan.steps.length : 0;
                  const summary = plan?.summary;

                  return (
                    <TableRow key={pkg.id}>
                      <TableCell className="font-mono text-xs">#{pkg.id}</TableCell>
                      <TableCell>
                        <Badge variant="outline">{pkg.marketplace}</Badge>
                      </TableCell>
                      <TableCell>
                        <button
                          className="text-blue-600 hover:underline text-sm"
                          onClick={() => navigate(`/families/${pkg.global_family_id}`)}
                        >
                          {pkg.global_family_id}
                        </button>
                      </TableCell>
                      <TableCell>
                        <div className="flex gap-1">
                          {summary && (
                            <>
                              {(summary.delete_count ?? 0) > 0 && (
                                <Badge variant="destructive" className="text-xs">
                                  {summary.delete_count} DEL
                                </Badge>
                              )}
                              {(summary.create_count ?? 0) > 0 && (
                                <Badge variant="default" className="text-xs">
                                  {summary.create_count} CREATE
                                </Badge>
                              )}
                              {(summary.review_count ?? 0) > 0 && (
                                <Badge variant="secondary" className="text-xs">
                                  {summary.review_count} REVIEW
                                </Badge>
                              )}
                            </>
                          )}
                          {!summary && <span className="text-xs">{stepCount} steps</span>}
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge variant={STATUS_BADGE[pkg.status] ?? "secondary"}>
                          {pkg.status}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {pkg.generated_at ? new Date(pkg.generated_at).toLocaleDateString() : "—"}
                      </TableCell>
                      <TableCell className="text-xs">
                        {pkg.approved_by ?? "—"}
                      </TableCell>
                      <TableCell>
                        <div className="flex gap-1">
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 w-7 p-0"
                            title="View plan"
                            onClick={() => setViewPlan(pkg)}
                          >
                            <Eye className="h-3.5 w-3.5" />
                          </Button>
                          {(pkg.status === "draft" || pkg.status === "pending_approve") && (
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-7 w-7 p-0 text-green-600"
                              title="Approve"
                              disabled={approveMut.isPending}
                              onClick={() => approveMut.mutate(pkg.id)}
                            >
                              <Check className="h-3.5 w-3.5" />
                            </Button>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>

          {totalPages > 1 && (
            <div className="mt-4">
              <ServerPagination page={page} pages={totalPages} total={total} pageSize={pageSize} onPageChange={setPage} />
            </div>
          )}
        </CardContent>
      </Card>

      {/* Plan detail dialog */}
      <Dialog open={!!viewPlan} onOpenChange={() => setViewPlan(null)}>
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              Fix Package #{viewPlan?.id} — {viewPlan?.marketplace}
            </DialogTitle>
          </DialogHeader>
          {viewPlan && (
            <div className="space-y-4">
              <div className="text-sm text-muted-foreground">
                Family ID: {viewPlan.global_family_id} · Status: {viewPlan.status}
              </div>
              <div className="space-y-2">
                {Array.isArray((viewPlan.action_plan as { steps?: unknown[] })?.steps) &&
                  ((viewPlan.action_plan as { steps: Record<string, unknown>[] }).steps).map(
                    (step: Record<string, unknown>, idx: number) => (
                      <div
                        key={idx}
                        className="rounded-md border p-3 space-y-1"
                      >
                        <div className="flex items-center gap-2">
                          <Badge
                            variant={
                              step.action === "DELETE"
                                ? "destructive"
                                : step.action === "CREATE"
                                  ? "default"
                                  : "secondary"
                            }
                          >
                            {String(step.action)}
                          </Badge>
                          <span className="text-sm font-medium">
                            {String(step.type)}
                          </span>
                        </div>
                        <p className="text-xs text-muted-foreground">
                          {String(step.reason ?? "")}
                        </p>
                        {step.asin ? (
                          <p className="text-xs font-mono">ASIN: {String(step.asin)}</p>
                        ) : null}
                        {step.master_key ? (
                          <p className="text-xs font-mono">
                            Key: {String(step.master_key)}
                          </p>
                        ) : null}
                      </div>
                    )
                  )}
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
