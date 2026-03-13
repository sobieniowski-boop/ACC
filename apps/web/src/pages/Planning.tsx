import { useState, useCallback } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
} from "recharts";
import {
  Target, TrendingUp, Calendar, CheckCircle, Plus, Trash2, Lock,
  ClipboardCheck, FileEdit, X,
} from "lucide-react";
import {
  getPlanMonths, getPlanVsActual, createPlanMonth, updatePlanStatus,
  deletePlanMonth, getMarketplaces,
} from "@/lib/api";
import type { PlanLineCreate } from "@/lib/api";
import { formatPLN } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { Progress } from "@/components/ui/progress";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter,
  DialogDescription,
} from "@/components/ui/dialog";
import { ClientExportButton } from "@/components/shared";

const CURRENT_YEAR = new Date().getFullYear();

const MONTH_NAMES = [
  "", "Styczeń", "Luty", "Marzec", "Kwiecień", "Maj", "Czerwiec",
  "Lipiec", "Sierpień", "Wrzesień", "Październik", "Listopad", "Grudzień",
];

function statusBadge(status: string) {
  if (status === "locked")
    return <Badge className="bg-red-500/20 text-red-400 border-red-500/30">Zablokowany</Badge>;
  if (status === "approved")
    return <Badge className="bg-emerald-500/20 text-emerald-400 border-emerald-500/30">Zatwierdzony</Badge>;
  return <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30">Szkic</Badge>;
}

function attainmentColor(pct?: number | null) {
  if (!pct) return "text-white/40";
  if (pct >= 100) return "text-emerald-400";
  if (pct >= 80) return "text-[#FF9900]";
  return "text-red-400";
}

const DEFAULT_LINE: Omit<PlanLineCreate, "marketplace_id"> = {
  target_revenue_pln: 10000,
  target_orders: 100,
  target_acos_pct: 10,
  target_cm_pct: 20,
  budget_ads_pln: 1000,
};

export default function Planning() {
  const [year, setYear] = useState(CURRENT_YEAR);
  const [selectedMonthId, setSelectedMonthId] = useState<number | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [createMonth, setCreateMonth] = useState(new Date().getMonth() + 1);
  const [createLines, setCreateLines] = useState<PlanLineCreate[]>([]);
  const [deleteConfirm, setDeleteConfirm] = useState<number | null>(null);

  const qc = useQueryClient();

  const { data: marketplaces } = useQuery({
    queryKey: ["marketplaces"],
    queryFn: getMarketplaces,
    staleTime: 600_000,
  });

  const { data: months, isLoading } = useQuery({
    queryKey: ["plan-months", year],
    queryFn: () => getPlanMonths(year),
    staleTime: 120_000,
  });

  const { data: vsActual } = useQuery({
    queryKey: ["plan-vs-actual", year],
    queryFn: () => getPlanVsActual(year),
    staleTime: 120_000,
  });

  const invalidate = useCallback(() => {
    qc.invalidateQueries({ queryKey: ["plan-months", year] });
    qc.invalidateQueries({ queryKey: ["plan-vs-actual", year] });
  }, [qc, year]);

  const createMut = useMutation({
    mutationFn: createPlanMonth,
    onSuccess: () => { invalidate(); setShowCreate(false); },
  });

  const statusMut = useMutation({
    mutationFn: ({ planId, status }: { planId: number; status: string }) =>
      updatePlanStatus(planId, status),
    onSuccess: invalidate,
  });

  const deleteMut = useMutation({
    mutationFn: deletePlanMonth,
    onSuccess: () => { invalidate(); setDeleteConfirm(null); setSelectedMonthId(null); },
  });

  const selectedMonth = months?.find((m) => m.id === selectedMonthId);

  const usedMonths = new Set(months?.map((m) => m.month) ?? []);

  function openCreateDialog() {
    const firstFree = Array.from({ length: 12 }, (_, i) => i + 1).find((m) => !usedMonths.has(m));
    setCreateMonth(firstFree ?? 1);
    const mks = marketplaces ?? [];
    setCreateLines(mks.map((mk) => ({ marketplace_id: mk.marketplace_id, ...DEFAULT_LINE })));
    setShowCreate(true);
  }

  function updateLine(idx: number, field: keyof PlanLineCreate, value: string) {
    setCreateLines((prev) => {
      const next = [...prev];
      const num = parseFloat(value) || 0;
      next[idx] = { ...next[idx], [field]: field === "marketplace_id" ? value : num };
      return next;
    });
  }

  function handleCreate() {
    if (createLines.length === 0) return;
    createMut.mutate({ year, month: createMonth, lines: createLines });
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Planowanie & Budżet</h1>
          <p className="text-white/50 text-sm mt-1">
            Plany miesięczne, realizacja celów i budżety reklamowe
          </p>
        </div>
        <div className="flex items-center gap-2">
          {[CURRENT_YEAR - 1, CURRENT_YEAR, CURRENT_YEAR + 1].map((y) => (
            <button
              key={y}
              onClick={() => { setYear(y); setSelectedMonthId(null); }}
              className={`px-3 py-1.5 rounded text-sm font-medium transition-colors ${
                year === y
                  ? "bg-[#FF9900] text-black"
                  : "bg-white/10 text-white hover:bg-white/20"
              }`}
            >
              {y}
            </button>
          ))}
        </div>
      </div>

      {/* YTD summary */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-xs text-white/50 flex items-center gap-2">
              <Target className="w-4 h-4 text-[#FF9900]" /> YTD Cel
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              {formatPLN(vsActual?.ytd_target_revenue_pln ?? 0)}
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-xs text-white/50 flex items-center gap-2">
              <TrendingUp className="w-4 h-4 text-emerald-400" /> YTD Wykonanie
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">
              {formatPLN(vsActual?.ytd_actual_revenue_pln ?? 0)}
            </div>
            <div className={`text-sm font-medium mt-1 ${attainmentColor(vsActual?.ytd_attainment_pct)}`}>
              {(vsActual?.ytd_attainment_pct ?? 0).toFixed(1)}% realizacji
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-xs text-white/50 flex items-center gap-2">
              <CheckCircle className="w-4 h-4 text-blue-400" /> Miesiące z planem
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-white">{months?.length ?? 0} / 12</div>
          </CardContent>
        </Card>
      </div>

      {/* Plan vs Actual chart */}
      {vsActual && vsActual.rows.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Plan vs Wykonanie {year}</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={vsActual.rows} margin={{ top: 0, right: 8, left: 0, bottom: 0 }}>
                <XAxis dataKey="month_label" tick={{ fontSize: 10, fill: "#9ca3af" }} />
                <YAxis tick={{ fontSize: 10, fill: "#9ca3af" }} tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
                <Tooltip
                  formatter={(v: number, name: string) => [formatPLN(v), name]}
                  contentStyle={{ background: "#1e293b", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 8 }}
                  labelStyle={{ color: "#fff" }}
                />
                <Legend wrapperStyle={{ fontSize: 11 }} />
                <Bar dataKey="target_revenue_pln" name="Cel" fill="#6b7280" radius={[4, 4, 0, 0]} />
                <Bar dataKey="actual_revenue_pln" name="Wykonanie" fill="#FF9900" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      )}

      {/* Months table */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-sm">Plany Miesięczne {year}</CardTitle>
          <div className="flex items-center gap-2">
            <ClientExportButton data={months ?? []} filename={`plan_${year}`} />
            <Button size="sm" onClick={openCreateDialog} className="bg-[#FF9900] text-black hover:bg-[#e88a00]">
              <Plus className="w-4 h-4 mr-1" /> Dodaj miesiąc
            </Button>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          {isLoading ? (
            <div className="p-6 space-y-2">
              {Array.from({ length: 6 }).map((_, i) => <Skeleton key={i} className="h-10 w-full" />)}
            </div>
          ) : months && months.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Miesiąc</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Cel Przychód</TableHead>
                  <TableHead className="text-right">Wykonanie</TableHead>
                  <TableHead className="text-right">Realizacja</TableHead>
                  <TableHead className="text-right">Budżet Ads</TableHead>
                  <TableHead className="text-right">Akcje</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {months.map((month) => (
                  <TableRow
                    key={month.id}
                    className="cursor-pointer"
                    onClick={() => setSelectedMonthId(month.id === selectedMonthId ? null : month.id)}
                  >
                    <TableCell className="font-medium">{month.month_label}</TableCell>
                    <TableCell>{statusBadge(month.status)}</TableCell>
                    <TableCell className="text-right">{formatPLN(month.total_target_revenue_pln)}</TableCell>
                    <TableCell className="text-right">
                      {month.total_actual_revenue_pln != null
                        ? formatPLN(month.total_actual_revenue_pln)
                        : <span className="text-white/30">—</span>}
                    </TableCell>
                    <TableCell className="text-right">
                      {month.revenue_attainment_pct != null ? (
                        <div className="flex items-center gap-2 justify-end">
                          <span className={attainmentColor(month.revenue_attainment_pct)}>
                            {month.revenue_attainment_pct.toFixed(1)}%
                          </span>
                          <Progress value={Math.min(100, month.revenue_attainment_pct)} className="w-16 h-1.5" />
                        </div>
                      ) : <span className="text-white/30">—</span>}
                    </TableCell>
                    <TableCell className="text-right">{formatPLN(month.total_target_budget_ads_pln)}</TableCell>
                    <TableCell className="text-right" onClick={(e) => e.stopPropagation()}>
                      <div className="flex items-center gap-1 justify-end">
                        {month.status === "draft" && (
                          <Button
                            size="icon"
                            variant="ghost"
                            className="h-7 w-7 text-emerald-400 hover:text-emerald-300 hover:bg-emerald-500/10"
                            title="Zatwierdź"
                            onClick={() => statusMut.mutate({ planId: month.id, status: "approved" })}
                          >
                            <ClipboardCheck className="h-4 w-4" />
                          </Button>
                        )}
                        {month.status === "approved" && (
                          <>
                            <Button
                              size="icon"
                              variant="ghost"
                              className="h-7 w-7 text-blue-400 hover:text-blue-300 hover:bg-blue-500/10"
                              title="Zablokuj"
                              onClick={() => statusMut.mutate({ planId: month.id, status: "locked" })}
                            >
                              <Lock className="h-4 w-4" />
                            </Button>
                            <Button
                              size="icon"
                              variant="ghost"
                              className="h-7 w-7 text-white/40 hover:text-white hover:bg-white/10"
                              title="Cofnij do szkicu"
                              onClick={() => statusMut.mutate({ planId: month.id, status: "draft" })}
                            >
                              <FileEdit className="h-4 w-4" />
                            </Button>
                          </>
                        )}
                        {month.status !== "locked" && (
                          <Button
                            size="icon"
                            variant="ghost"
                            className="h-7 w-7 text-red-400 hover:text-red-300 hover:bg-red-500/10"
                            title="Usuń"
                            onClick={() => setDeleteConfirm(month.id)}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <div className="p-12 text-center text-white/40">
              <Calendar className="w-10 h-10 mx-auto mb-3 opacity-30" />
              <p>Brak planów na rok {year}</p>
              <Button
                size="sm"
                className="mt-4 bg-[#FF9900] text-black hover:bg-[#e88a00]"
                onClick={openCreateDialog}
              >
                <Plus className="w-4 h-4 mr-1" /> Utwórz pierwszy plan
              </Button>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Selected month breakdown */}
      {selectedMonth && (
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-sm">
              Szczegóły: {selectedMonth.month_label}
            </CardTitle>
            <div className="flex items-center gap-2">
              {statusBadge(selectedMonth.status)}
            </div>
          </CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Marketplace</TableHead>
                  <TableHead className="text-right">Cel Przychód</TableHead>
                  <TableHead className="text-right">Wykonanie</TableHead>
                  <TableHead className="text-right">Realizacja</TableHead>
                  <TableHead className="text-right">Cel Zamówień</TableHead>
                  <TableHead className="text-right">Target ACoS</TableHead>
                  <TableHead className="text-right">Target CM%</TableHead>
                  <TableHead className="text-right">Budżet Ads</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {selectedMonth.lines.map((line) => (
                  <TableRow key={line.id}>
                    <TableCell>
                      <Badge variant="outline">{line.marketplace_code}</Badge>
                    </TableCell>
                    <TableCell className="text-right">{formatPLN(line.target_revenue_pln)}</TableCell>
                    <TableCell className="text-right">
                      {line.actual_revenue_pln != null
                        ? formatPLN(line.actual_revenue_pln)
                        : <span className="text-white/30">—</span>}
                    </TableCell>
                    <TableCell className="text-right">
                      {line.revenue_attainment_pct != null ? (
                        <span className={attainmentColor(line.revenue_attainment_pct)}>
                          {line.revenue_attainment_pct.toFixed(1)}%
                        </span>
                      ) : <span className="text-white/30">—</span>}
                    </TableCell>
                    <TableCell className="text-right">{line.target_orders.toLocaleString()}</TableCell>
                    <TableCell className="text-right">{line.target_acos_pct.toFixed(1)}%</TableCell>
                    <TableCell className="text-right">{line.target_cm_pct.toFixed(1)}%</TableCell>
                    <TableCell className="text-right">{formatPLN(line.budget_ads_pln)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {/* Create Plan Dialog */}
      <Dialog open={showCreate} onOpenChange={setShowCreate}>
        <DialogContent className="max-w-3xl bg-[#0f1729] border-white/10">
          <DialogHeader>
            <DialogTitle className="text-white">Nowy plan — {MONTH_NAMES[createMonth]} {year}</DialogTitle>
            <DialogDescription className="text-white/50">
              Ustaw cele sprzedażowe i budżety reklamowe dla każdego marketplace.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4">
            {/* Month selector */}
            <div className="flex items-center gap-3">
              <label className="text-sm text-white/60 w-20">Miesiąc:</label>
              <div className="flex flex-wrap gap-1">
                {Array.from({ length: 12 }, (_, i) => i + 1).map((m) => (
                  <button
                    key={m}
                    disabled={usedMonths.has(m)}
                    onClick={() => setCreateMonth(m)}
                    className={`px-2 py-1 rounded text-xs font-medium transition-colors ${
                      createMonth === m
                        ? "bg-[#FF9900] text-black"
                        : usedMonths.has(m)
                          ? "bg-white/5 text-white/20 cursor-not-allowed"
                          : "bg-white/10 text-white hover:bg-white/20"
                    }`}
                  >
                    {MONTH_NAMES[m].substring(0, 3)}
                  </button>
                ))}
              </div>
            </div>

            {/* Lines */}
            <div className="border border-white/10 rounded-lg overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-24">Marketplace</TableHead>
                    <TableHead className="text-right">Przychód (PLN)</TableHead>
                    <TableHead className="text-right">Zamówienia</TableHead>
                    <TableHead className="text-right">ACoS %</TableHead>
                    <TableHead className="text-right">CM %</TableHead>
                    <TableHead className="text-right">Budżet Ads</TableHead>
                    <TableHead className="w-10"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {createLines.map((line, idx) => {
                    const mk = marketplaces?.find((m) => m.marketplace_id === line.marketplace_id);
                    return (
                      <TableRow key={idx}>
                        <TableCell>
                          <Badge variant="outline">{mk?.code ?? line.marketplace_id.substring(0, 4)}</Badge>
                        </TableCell>
                        <TableCell className="text-right">
                          <Input
                            type="number"
                            className="w-28 ml-auto text-right bg-white/5 border-white/10 h-8 text-sm"
                            value={line.target_revenue_pln}
                            onChange={(e) => updateLine(idx, "target_revenue_pln", e.target.value)}
                          />
                        </TableCell>
                        <TableCell className="text-right">
                          <Input
                            type="number"
                            className="w-20 ml-auto text-right bg-white/5 border-white/10 h-8 text-sm"
                            value={line.target_orders}
                            onChange={(e) => updateLine(idx, "target_orders", e.target.value)}
                          />
                        </TableCell>
                        <TableCell className="text-right">
                          <Input
                            type="number"
                            className="w-16 ml-auto text-right bg-white/5 border-white/10 h-8 text-sm"
                            value={line.target_acos_pct}
                            onChange={(e) => updateLine(idx, "target_acos_pct", e.target.value)}
                          />
                        </TableCell>
                        <TableCell className="text-right">
                          <Input
                            type="number"
                            className="w-16 ml-auto text-right bg-white/5 border-white/10 h-8 text-sm"
                            value={line.target_cm_pct}
                            onChange={(e) => updateLine(idx, "target_cm_pct", e.target.value)}
                          />
                        </TableCell>
                        <TableCell className="text-right">
                          <Input
                            type="number"
                            className="w-24 ml-auto text-right bg-white/5 border-white/10 h-8 text-sm"
                            value={line.budget_ads_pln}
                            onChange={(e) => updateLine(idx, "budget_ads_pln", e.target.value)}
                          />
                        </TableCell>
                        <TableCell>
                          <Button
                            size="icon"
                            variant="ghost"
                            className="h-7 w-7 text-red-400 hover:text-red-300"
                            onClick={() => setCreateLines((prev) => prev.filter((_, i) => i !== idx))}
                          >
                            <X className="h-3.5 w-3.5" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>

            {/* Summary */}
            <div className="flex items-center justify-between text-sm text-white/50 px-1">
              <span>{createLines.length} marketplace(s)</span>
              <span>
                Razem: {formatPLN(createLines.reduce((s, l) => s + l.target_revenue_pln, 0))} przychód
                {" · "}
                {formatPLN(createLines.reduce((s, l) => s + l.budget_ads_pln, 0))} budżet
              </span>
            </div>
          </div>

          <DialogFooter className="gap-2">
            <Button variant="ghost" onClick={() => setShowCreate(false)} className="text-white/60">
              Anuluj
            </Button>
            <Button
              onClick={handleCreate}
              disabled={createLines.length === 0 || createMut.isPending}
              className="bg-[#FF9900] text-black hover:bg-[#e88a00]"
            >
              {createMut.isPending ? "Tworzenie..." : "Utwórz plan"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirm Dialog */}
      <Dialog open={deleteConfirm !== null} onOpenChange={() => setDeleteConfirm(null)}>
        <DialogContent className="max-w-sm bg-[#0f1729] border-white/10">
          <DialogHeader>
            <DialogTitle className="text-white">Usunąć plan?</DialogTitle>
            <DialogDescription className="text-white/50">
              Plan i wszystkie jego cele zostaną trwale usunięte.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter className="gap-2">
            <Button variant="ghost" onClick={() => setDeleteConfirm(null)} className="text-white/60">
              Anuluj
            </Button>
            <Button
              variant="destructive"
              onClick={() => deleteConfirm && deleteMut.mutate(deleteConfirm)}
              disabled={deleteMut.isPending}
            >
              {deleteMut.isPending ? "Usuwanie..." : "Usuń"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
                    <TableCell className="text-right">{line.target_cm_pct.toFixed(1)}%</TableCell>
                    <TableCell className="text-right">{formatPLN(line.budget_ads_pln)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
