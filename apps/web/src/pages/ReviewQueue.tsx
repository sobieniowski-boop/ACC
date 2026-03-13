import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import {
  getReviewQueue,
  updateLinkStatus,
  type ReviewQueueItem,
} from "@/lib/api";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
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
  Check,
  X,
  Eye,
  Filter,
} from "lucide-react";
import { ClientExportButton, ServerPagination } from "@/components/shared";

export default function ReviewQueuePage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [page, setPage] = useState(1);
  const [marketplace, setMarketplace] = useState<string>("");
  const [statusFilter, setStatusFilter] = useState<string>("");
  const pageSize = 50;

  const { data, isLoading } = useQuery({
    queryKey: ["review-queue", page, marketplace, statusFilter],
    queryFn: () =>
      getReviewQueue({
        page,
        page_size: pageSize,
        ...(marketplace ? { marketplace } : {}),
        ...(statusFilter ? { status: statusFilter } : {}),
      }),
  });

  const statusMutation = useMutation({
    mutationFn: (vars: { familyId: number; masterKey: string; marketplace: string; status: string }) =>
      updateLinkStatus(vars.familyId, {
        status: vars.status,
        master_key: vars.masterKey,
        marketplace: vars.marketplace,
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["review-queue"] }),
  });

  const items = data?.items ?? [];
  const total = data?.total ?? 0;
  const totalPages = Math.ceil(total / pageSize);

  return (
    <div className="space-y-6 p-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Review Queue</h1>
        <p className="text-sm text-muted-foreground">
          Proposed and needs-review child market links awaiting human decision
        </p>
      </div>

      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="flex items-center gap-2">
              <Filter className="h-4 w-4" />
              {total} items pending review
            </CardTitle>
            <div className="flex gap-2">
              <ClientExportButton data={items} filename="review_queue" />
              <Select value={statusFilter} onValueChange={(v) => { setStatusFilter(v === "all" ? "" : v); setPage(1); }}>
                <SelectTrigger className="w-[160px]">
                  <SelectValue placeholder="All statuses" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All statuses</SelectItem>
                  <SelectItem value="proposed">Proposed</SelectItem>
                  <SelectItem value="needs_review">Needs Review</SelectItem>
                </SelectContent>
              </Select>
              <Input
                className="w-32"
                placeholder="Marketplace…"
                value={marketplace}
                onChange={(e) => { setMarketplace(e.target.value); setPage(1); }}
              />
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>DE Parent</TableHead>
                <TableHead>Brand</TableHead>
                <TableHead>Marketplace</TableHead>
                <TableHead>DE Child</TableHead>
                <TableHead>Target Child</TableHead>
                <TableHead>Match</TableHead>
                <TableHead className="text-center">Confidence</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={9} className="text-center py-8 text-muted-foreground">
                    Loading…
                  </TableCell>
                </TableRow>
              ) : items.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={9} className="text-center py-8 text-muted-foreground">
                    No items pending review.
                  </TableCell>
                </TableRow>
              ) : (
                items.map((item: ReviewQueueItem, idx: number) => (
                  <TableRow key={`${item.global_family_id}-${item.master_key}-${item.marketplace}-${idx}`}>
                    <TableCell>
                      <button
                        className="font-mono text-xs text-blue-600 hover:underline"
                        onClick={() => navigate(`/families/${item.global_family_id}`)}
                      >
                        {item.de_parent_asin}
                      </button>
                    </TableCell>
                    <TableCell className="text-sm">{item.brand ?? "—"}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{item.marketplace}</Badge>
                    </TableCell>
                    <TableCell className="font-mono text-xs">{item.de_child_asin ?? "—"}</TableCell>
                    <TableCell className="font-mono text-xs">{item.target_child_asin ?? "—"}</TableCell>
                    <TableCell>
                      <Badge variant="secondary">{item.match_type}</Badge>
                    </TableCell>
                    <TableCell className="text-center">
                      <span
                        className={`font-bold ${
                          item.confidence >= 90
                            ? "text-green-600"
                            : item.confidence >= 75
                              ? "text-blue-600"
                              : item.confidence >= 60
                                ? "text-yellow-600"
                                : "text-red-600"
                        }`}
                      >
                        {item.confidence}
                      </span>
                    </TableCell>
                    <TableCell>
                      <Badge
                        variant={item.status === "needs_review" ? "destructive" : "secondary"}
                      >
                        {item.status}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <div className="flex gap-1">
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-7 w-7 p-0 text-green-600"
                          title="Approve"
                          disabled={statusMutation.isPending}
                          onClick={() =>
                            statusMutation.mutate({
                              familyId: item.global_family_id,
                              masterKey: item.master_key,
                              marketplace: item.marketplace,
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
                          disabled={statusMutation.isPending}
                          onClick={() =>
                            statusMutation.mutate({
                              familyId: item.global_family_id,
                              masterKey: item.master_key,
                              marketplace: item.marketplace,
                              status: "rejected",
                            })
                          }
                        >
                          <X className="h-3.5 w-3.5" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-7 w-7 p-0"
                          title="View family"
                          onClick={() => navigate(`/families/${item.global_family_id}`)}
                        >
                          <Eye className="h-3.5 w-3.5" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))
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
    </div>
  );
}
