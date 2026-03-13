import { useState, useEffect, useRef, useCallback } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import {
  getFamilies,
  triggerRebuildDE,
  getRebuildStatus,
  triggerSyncMP,
  triggerMatching,
  type FamilySummary,
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
  GitBranch,
  RefreshCw,
  Search,
  ArrowRight,
  Globe,
} from "lucide-react";
import { ClientExportButton, ServerPagination } from "@/components/shared";

const ALL_MARKETPLACES = [
  { id: "A1C3SOZRARQ6R3", code: "PL" },
  { id: "A13V1IB3VIYZZH", code: "FR" },
  { id: "APJ6JRA9NG5V4",  code: "IT" },
  { id: "A1RKKUPIHCS9HS", code: "ES" },
  { id: "A1805IZSGTT6HS", code: "NL" },
  { id: "AMEN7PMS3EDWL",  code: "BE" },
  { id: "A2NODRKZP88ZB9", code: "SE" },
  { id: "A28R8C7NBKEWEA", code: "IE" },
] as const;

export default function FamilyMapperPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState("");
  const [brandFilter, setBrandFilter] = useState("KADAX");
  const [maxParents, setMaxParents] = useState(200);
  const [onlyMissing, setOnlyMissing] = useState(true);
  const [selectedMPs, setSelectedMPs] = useState<Set<string>>(
    () => new Set(ALL_MARKETPLACES.map((m) => m.id)),
  );
  const [selectedFamilies, setSelectedFamilies] = useState<Set<number>>(new Set());
  const [sortBy, setSortBy] = useState("sales_de");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const pageSize = 30;

  const toggleMP = (id: string) =>
    setSelectedMPs((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });

  const toggleFamily = (id: number) =>
    setSelectedFamilies((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });

  const toggleAllFamilies = () =>
    setSelectedFamilies((prev) =>
      prev.size === families.length
        ? new Set()
        : new Set(families.map((f) => f.id)),
    );

  const { data, isLoading } = useQuery({
    queryKey: ["families", page, search, brandFilter, sortBy, sortDir],
    queryFn: () => getFamilies({
      page,
      page_size: pageSize,
      search: search || undefined,
      brand: brandFilter || undefined,
      sort_by: sortBy,
      sort_dir: sortDir,
    }),
  });

  const [rebuildRunning, setRebuildRunning] = useState(false);
  const [rebuildPhase, setRebuildPhase] = useState("");
  const [rebuildDetail, setRebuildDetail] = useState("");
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = useCallback(() => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
  }, []);

  // Poll rebuild status while running
  useEffect(() => {
    // Check on mount if a rebuild is already running
  getRebuildStatus().then((s) => {
      if (s.running) { setRebuildRunning(true); setRebuildPhase(s.phase); setRebuildDetail(s.detail || ""); }
    }).catch(() => {});
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!rebuildRunning) return;
    const poll = async () => {
      try {
        const s = await getRebuildStatus();
        setRebuildPhase(s.phase);
        setRebuildDetail(s.detail || "");
        if (!s.running) {
          setRebuildRunning(false);
          stopPolling();
          queryClient.invalidateQueries({ queryKey: ["families"] });
        }
      } catch { /* ignore */ }
    };
    pollRef.current = setInterval(poll, 2000);
    poll();
    return stopPolling;
  }, [rebuildRunning, stopPolling, queryClient]);

  const rebuildDE = useMutation({
    mutationFn: () => triggerRebuildDE(maxParents, brandFilter || undefined, onlyMissing),
    onSuccess: (res) => {
      if (res.status === "started" || res.status === "already_running") {
        setRebuildRunning(true);
      }
    },
  });

  const syncMP = useMutation({
    mutationFn: () => {
      const mpIds = [...selectedMPs].join(",");
      const famIds = selectedFamilies.size > 0 ? [...selectedFamilies].join(",") : undefined;
      return triggerSyncMP(mpIds || undefined, famIds);
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["families"] }),
  });

  const matchAll = useMutation({
    mutationFn: () => {
      const mpIds = [...selectedMPs].join(",");
      const famIds = selectedFamilies.size > 0 ? [...selectedFamilies].join(",") : undefined;
      return triggerMatching(mpIds || undefined, famIds);
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["families"] }),
  });

  const families = data?.items ?? [];
  const total = data?.total ?? 0;
  const totalPages = Math.ceil(total / pageSize);

  return (
    <div className="space-y-6 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Family Mapper</h1>
          <p className="text-sm text-muted-foreground">
            DE Canonical → EU variation family mapping
          </p>
        </div>
        <div className="flex gap-2 items-center">
          <Input
            className="w-32 h-9 text-sm"
            placeholder="Brand…"
            value={brandFilter}
            onChange={(e) => setBrandFilter(e.target.value)}
          />
          <Input
            className="w-20 h-9 text-sm text-center"
            type="number"
            min={1}
            max={1000}
            title="Max parents"
            value={maxParents}
            onChange={(e) => setMaxParents(Number(e.target.value) || 200)}
          />
          <label className="flex items-center gap-1.5 text-xs text-muted-foreground cursor-pointer select-none" title="Only discover new families (skip existing)">
            <input
              type="checkbox"
              className="rounded border-muted-foreground"
              checked={onlyMissing}
              onChange={(e) => setOnlyMissing(e.target.checked)}
            />
            Only new
          </label>
          <Button
            variant="outline"
            size="sm"
            onClick={() => rebuildDE.mutate()}
            disabled={rebuildDE.isPending || rebuildRunning}
            title={rebuildDetail || undefined}
          >
            <RefreshCw className={`mr-2 h-4 w-4 ${(rebuildDE.isPending || rebuildRunning) ? "animate-spin" : ""}`} />
            {rebuildRunning ? (rebuildPhase === "discovery" ? "Discovering…" : rebuildPhase === "processing" ? rebuildDetail.replace(/^Family /, "").replace(/: .*/, "") : "Running…") : "Rebuild DE"}
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => syncMP.mutate()}
            disabled={syncMP.isPending}
          >
            <Globe className={`mr-2 h-4 w-4 ${syncMP.isPending ? "animate-spin" : ""}`} />
            Sync{selectedFamilies.size > 0 ? ` (${selectedFamilies.size})` : " All"}
          </Button>
          <Button
            size="sm"
            onClick={() => matchAll.mutate()}
            disabled={matchAll.isPending}
          >
            <GitBranch className={`mr-2 h-4 w-4 ${matchAll.isPending ? "animate-spin" : ""}`} />
            Match{selectedFamilies.size > 0 ? ` (${selectedFamilies.size})` : " All"}
          </Button>
        </div>
      </div>

      {/* Marketplace selection */}
      <div className="flex flex-wrap items-center gap-3 px-1">
        <span className="text-xs font-medium text-muted-foreground mr-1">Marketplaces:</span>
        {ALL_MARKETPLACES.map((mp) => (
          <label
            key={mp.id}
            className="flex items-center gap-1 text-xs cursor-pointer select-none"
          >
            <input
              type="checkbox"
              className="rounded border-muted-foreground"
              checked={selectedMPs.has(mp.id)}
              onChange={() => toggleMP(mp.id)}
            />
            {mp.code}
          </label>
        ))}
        <button
          type="button"
          className="text-[10px] text-muted-foreground underline ml-1"
          onClick={() =>
            setSelectedMPs((prev) =>
              prev.size === ALL_MARKETPLACES.length
                ? new Set()
                : new Set(ALL_MARKETPLACES.map((m) => m.id)),
            )
          }
        >
          {selectedMPs.size === ALL_MARKETPLACES.length ? "Uncheck all" : "Check all"}
        </button>
      </div>

      {/* Quick stats */}
      <div className="grid grid-cols-4 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Total Families
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{total}</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Fully Mapped
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold text-green-600">
              {families.filter((f) => f.marketplaces_mapped >= 10).length}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Partial
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold text-yellow-600">
              {families.filter((f) => f.marketplaces_mapped > 0 && f.marketplaces_mapped < 10).length}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Unmapped
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold text-red-600">
              {families.filter((f) => f.marketplaces_mapped === 0).length}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Search + Table */}
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle>Variation Families</CardTitle>
            <div className="flex items-center gap-2">
              <ClientExportButton data={families} filename="families" />
              <div className="relative w-72">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                className="pl-9"
                placeholder="Search ASIN or brand…"
                value={search}
                onChange={(e) => {
                  setSearch(e.target.value);
                  setPage(1);
                }}
              />
              </div>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-10">
                  <input
                    type="checkbox"
                    className="rounded border-muted-foreground"
                    checked={families.length > 0 && selectedFamilies.size === families.length}
                    ref={(el) => {
                      if (el) el.indeterminate = selectedFamilies.size > 0 && selectedFamilies.size < families.length;
                    }}
                    onChange={toggleAllFamilies}
                  />
                </TableHead>
                <TableHead>DE Parent ASIN</TableHead>
                <TableHead>Brand</TableHead>
                <TableHead>Category</TableHead>
                <TableHead>Product Type</TableHead>
                <TableHead>Theme</TableHead>
                <TableHead
                  className="text-center cursor-pointer select-none"
                  onClick={() => {
                    if (sortBy === "children") setSortDir((d) => d === "asc" ? "desc" : "asc");
                    else { setSortBy("children"); setSortDir("desc"); }
                  }}
                >
                  Children {sortBy === "children" ? (sortDir === "desc" ? "↓" : "↑") : ""}
                </TableHead>
                <TableHead
                  className="text-center cursor-pointer select-none"
                  onClick={() => {
                    if (sortBy === "sales_de") setSortDir((d) => d === "asc" ? "desc" : "asc");
                    else { setSortBy("sales_de"); setSortDir("desc"); }
                  }}
                >
                  DE Sales {sortBy === "sales_de" ? (sortDir === "desc" ? "↓" : "↑") : ""}
                </TableHead>
                <TableHead
                  className="text-center cursor-pointer select-none"
                  onClick={() => {
                    if (sortBy === "marketplaces") setSortDir((d) => d === "asc" ? "desc" : "asc");
                    else { setSortBy("marketplaces"); setSortDir("desc"); }
                  }}
                >
                  Marketplaces {sortBy === "marketplaces" ? (sortDir === "desc" ? "↓" : "↑") : ""}
                </TableHead>
                <TableHead />
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={10} className="text-center py-8 text-muted-foreground">
                    Loading…
                  </TableCell>
                </TableRow>
              ) : families.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={10} className="text-center py-8 text-muted-foreground">
                    No families found. Run "Rebuild DE" to populate.
                  </TableCell>
                </TableRow>
              ) : (
                families.map((f) => (
                  <FamilyRow
                    key={f.id}
                    family={f}
                    selected={selectedFamilies.has(f.id)}
                    onToggle={() => toggleFamily(f.id)}
                    onClick={() => navigate(`/families/${f.id}`)}
                  />
                ))
              )}
            </TableBody>
          </Table>

          {/* Pagination */}
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

function FamilyRow({ family, selected, onToggle, onClick }: {
  family: FamilySummary;
  selected: boolean;
  onToggle: () => void;
  onClick: () => void;
}) {
  const mpVariant =
    family.marketplaces_mapped >= 10
      ? "default"
      : family.marketplaces_mapped > 0
        ? "secondary"
        : "destructive";

  return (
    <TableRow className="cursor-pointer hover:bg-accent/50">
      <TableCell onClick={(e) => e.stopPropagation()}>
        <input
          type="checkbox"
          className="rounded border-muted-foreground"
          checked={selected}
          onChange={onToggle}
        />
      </TableCell>
      <TableCell className="font-mono text-xs" onClick={onClick}>{family.de_parent_asin}</TableCell>
      <TableCell onClick={onClick}>{family.brand ?? "—"}</TableCell>
      <TableCell className="max-w-[200px] truncate" onClick={onClick}>{family.category ?? "—"}</TableCell>
      <TableCell className="text-xs" onClick={onClick}>{family.product_type ?? "—"}</TableCell>
      <TableCell className="text-xs" onClick={onClick}>{family.variation_theme_de ?? "—"}</TableCell>
      <TableCell className="text-center" onClick={onClick}>{family.children_count}</TableCell>
      <TableCell className="text-center font-medium" onClick={onClick}>
        {family.de_sales_qty > 0 ? family.de_sales_qty.toLocaleString() : "—"}
      </TableCell>
      <TableCell className="text-center" onClick={onClick}>
        <Badge variant={mpVariant}>{family.marketplaces_mapped} / 12</Badge>
      </TableCell>
      <TableCell onClick={onClick}>
        <ArrowRight className="h-4 w-4 text-muted-foreground" />
      </TableCell>
    </TableRow>
  );
}
