import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, Cell,
} from "recharts";
import { Zap, Target, TrendingUp, CheckCircle, AlertTriangle, Trash2, Plus, X } from "lucide-react";
import {
  getRepricingDashboard,
  getRepricingStrategies,
  getRepricingExecutions,
  createRepricingStrategy,
  deleteRepricingStrategy,
  approveRepricingExecution,
  rejectRepricingExecution,
  triggerRepricingCompute,
  bulkApproveRepricingExecutions,
  bulkRejectRepricingExecutions,
  autoApproveRepricingExecutions,
  executeRepricingPrices,
  getRepricingAnalyticsTrend,
  getRepricingAnalyticsByStrategy,
  type RepricingStrategy,
  type RepricingExecution,
  type RepricingDashboard,
  type RepricingAnalyticsTrend,
  type RepricingStrategyAnalytics,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { BatchBar, BatchActionButton } from "@/components/shared";

/* ------------------------------------------------------------------ */
/*  Strategy / Status badges                                           */
/* ------------------------------------------------------------------ */
const STRAT_STYLES: Record<string, string> = {
  buybox_match: "bg-blue-500/20 text-blue-300 border-blue-500/30",
  competitive_undercut: "bg-amber-500/20 text-amber-300 border-amber-500/30",
  margin_target: "bg-emerald-500/20 text-emerald-300 border-emerald-500/30",
  velocity_based: "bg-purple-500/20 text-purple-300 border-purple-500/30",
};
const STRAT_LABELS: Record<string, string> = {
  buybox_match: "Buy Box Match",
  competitive_undercut: "Undercut",
  margin_target: "Marża docelowa",
  velocity_based: "Velocity",
};

function StrategyBadge({ type }: { type: string }) {
  return (
    <Badge variant="outline" className={STRAT_STYLES[type] ?? ""}>
      {STRAT_LABELS[type] ?? type.replace(/_/g, " ")}
    </Badge>
  );
}

const STATUS_STYLES: Record<string, string> = {
  proposed: "bg-amber-500/20 text-amber-300 border-amber-500/30",
  approved: "bg-blue-500/20 text-blue-300 border-blue-500/30",
  executed: "bg-emerald-500/20 text-emerald-300 border-emerald-500/30",
  rejected: "bg-red-500/20 text-red-300 border-red-500/30",
  expired: "bg-white/10 text-white/40 border-white/10",
  failed: "bg-red-500/30 text-red-200 border-red-500/40",
};
const STATUS_LABELS: Record<string, string> = {
  proposed: "Oczekuje",
  approved: "Zatwierdzony",
  executed: "Wykonany",
  rejected: "Odrzucony",
  expired: "Wygasły",
  failed: "Błąd",
};

function StatusBadge({ status }: { status: string }) {
  return (
    <Badge variant="outline" className={STATUS_STYLES[status] ?? ""}>
      {STATUS_LABELS[status] ?? status}
    </Badge>
  );
}

/* ------------------------------------------------------------------ */
/*  New Strategy Form                                                  */
/* ------------------------------------------------------------------ */
const STRATEGY_TYPES = ["buybox_match", "competitive_undercut", "margin_target", "velocity_based"];

function NewStrategyForm({ onClose, marketplace }: { onClose: () => void; marketplace: string }) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState({
    strategy_type: "buybox_match",
    seller_sku: "",
    min_price: "",
    max_price: "",
    min_margin_pct: "",
    max_daily_change_pct: "10",
    requires_approval: true,
    undercut_pct: "1",
    target_margin_pct: "15",
  });

  const mutation = useMutation({
    mutationFn: () => {
      const params: Record<string, unknown> = {};
      if (form.strategy_type === "competitive_undercut" && form.undercut_pct) {
        params.undercut_pct = parseFloat(form.undercut_pct);
      }
      if (form.strategy_type === "margin_target" && form.target_margin_pct) {
        params.target_margin_pct = parseFloat(form.target_margin_pct);
      }
      return createRepricingStrategy({
        strategy_type: form.strategy_type,
        seller_sku: form.seller_sku || undefined,
        marketplace_id: marketplace || undefined,
        parameters: Object.keys(params).length > 0 ? params : undefined,
        min_price: form.min_price ? parseFloat(form.min_price) : undefined,
        max_price: form.max_price ? parseFloat(form.max_price) : undefined,
        min_margin_pct: form.min_margin_pct ? parseFloat(form.min_margin_pct) : undefined,
        max_daily_change_pct: form.max_daily_change_pct ? parseFloat(form.max_daily_change_pct) : undefined,
        requires_approval: form.requires_approval,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["repricing"] });
      onClose();
    },
  });

  const inputCls = "h-9 rounded-md border border-white/10 bg-[#1e293b] px-3 text-sm text-white placeholder:text-white/30";

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between pb-3">
        <CardTitle className="text-sm">Nowa strategia</CardTitle>
        <button onClick={onClose} className="text-white/40 hover:text-white"><X className="h-4 w-4" /></button>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
          <label className="block">
            <span className="text-xs text-white/50">Typ</span>
            <select
              className={inputCls + " mt-1 block w-full"}
              value={form.strategy_type}
              onChange={(e) => setForm((f) => ({ ...f, strategy_type: e.target.value }))}
            >
              {STRATEGY_TYPES.map((t) => (
                <option key={t} value={t}>{STRAT_LABELS[t] ?? t}</option>
              ))}
            </select>
          </label>
          <label className="block">
            <span className="text-xs text-white/50">SKU (puste = wszystkie)</span>
            <Input className={inputCls + " mt-1"} value={form.seller_sku} onChange={(e) => setForm((f) => ({ ...f, seller_sku: e.target.value }))} />
          </label>
          <label className="block">
            <span className="text-xs text-white/50">Cena min</span>
            <Input type="number" step="0.01" className={inputCls + " mt-1"} value={form.min_price} onChange={(e) => setForm((f) => ({ ...f, min_price: e.target.value }))} />
          </label>
          <label className="block">
            <span className="text-xs text-white/50">Cena max</span>
            <Input type="number" step="0.01" className={inputCls + " mt-1"} value={form.max_price} onChange={(e) => setForm((f) => ({ ...f, max_price: e.target.value }))} />
          </label>
          <label className="block">
            <span className="text-xs text-white/50">Min marża %</span>
            <Input type="number" step="0.1" className={inputCls + " mt-1"} value={form.min_margin_pct} onChange={(e) => setForm((f) => ({ ...f, min_margin_pct: e.target.value }))} />
          </label>
          <label className="block">
            <span className="text-xs text-white/50">Max zmiana/dzień %</span>
            <Input type="number" step="0.1" className={inputCls + " mt-1"} value={form.max_daily_change_pct} onChange={(e) => setForm((f) => ({ ...f, max_daily_change_pct: e.target.value }))} />
          </label>
          {form.strategy_type === "competitive_undercut" && (
            <label className="block">
              <span className="text-xs text-white/50">Undercut %</span>
              <Input type="number" step="0.1" className={inputCls + " mt-1"} value={form.undercut_pct} onChange={(e) => setForm((f) => ({ ...f, undercut_pct: e.target.value }))} />
            </label>
          )}
          {form.strategy_type === "margin_target" && (
            <label className="block">
              <span className="text-xs text-white/50">Docelowa marża %</span>
              <Input type="number" step="0.1" className={inputCls + " mt-1"} value={form.target_margin_pct} onChange={(e) => setForm((f) => ({ ...f, target_margin_pct: e.target.value }))} />
            </label>
          )}
          <label className="flex items-center gap-2 col-span-2">
            <input
              type="checkbox"
              checked={form.requires_approval}
              onChange={(e) => setForm((f) => ({ ...f, requires_approval: e.target.checked }))}
              className="h-3.5 w-3.5 accent-[#FF9900]"
            />
            <span className="text-xs text-white/50">Wymaga zatwierdzenia</span>
          </label>
        </div>
        <div className="flex gap-2">
          <Button
            size="sm"
            className="bg-[#FF9900] text-black hover:bg-[#e68a00]"
            disabled={mutation.isPending}
            onClick={() => mutation.mutate()}
          >
            {mutation.isPending ? "Zapisywanie…" : "Utwórz strategię"}
          </Button>
          <Button variant="outline" size="sm" onClick={onClose}>Anuluj</Button>
        </div>
        {mutation.isError && (
          <p className="text-xs text-red-400">Błąd: {(mutation.error as Error).message}</p>
        )}
      </CardContent>
    </Card>
  );
}

/* ------------------------------------------------------------------ */
/*  Main page                                                          */
/* ------------------------------------------------------------------ */
export default function RepricingEnginePage() {
  const [marketplace, setMarketplace] = useState<string>("");
  const [showForm, setShowForm] = useState(false);
  const [execFilter, setExecFilter] = useState<string>("");
  const [tab, setTab] = useState<"proposals" | "analytics">("proposals");
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const queryClient = useQueryClient();

  const { data: dashboard } = useQuery<RepricingDashboard>({
    queryKey: ["repricing", "dashboard", marketplace],
    queryFn: () => getRepricingDashboard(marketplace ? { marketplace_id: marketplace } : undefined),
  });

  const { data: strategies, isLoading: stratLoading } = useQuery({
    queryKey: ["repricing", "strategies", marketplace],
    queryFn: () => getRepricingStrategies(marketplace ? { marketplace_id: marketplace } : undefined),
  });

  const { data: executions, isLoading: execLoading } = useQuery({
    queryKey: ["repricing", "executions", marketplace, execFilter],
    queryFn: () =>
      getRepricingExecutions({
        ...(marketplace ? { marketplace_id: marketplace } : {}),
        ...(execFilter ? { status: execFilter } : {}),
        limit: 100,
      }),
  });

  const computeMutation = useMutation({
    mutationFn: () => triggerRepricingCompute(marketplace || undefined),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["repricing"] }),
  });
  const approveMut = useMutation({
    mutationFn: (id: number) => approveRepricingExecution(id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["repricing"] }),
  });
  const rejectMut = useMutation({
    mutationFn: (id: number) => rejectRepricingExecution(id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["repricing"] }),
  });
  const deleteMut = useMutation({
    mutationFn: (id: number) => deleteRepricingStrategy(id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["repricing"] }),
  });
  const bulkApproveMut = useMutation({
    mutationFn: (ids: number[]) => bulkApproveRepricingExecutions(ids),
    onSuccess: () => { setSelected(new Set()); queryClient.invalidateQueries({ queryKey: ["repricing"] }); },
  });
  const bulkRejectMut = useMutation({
    mutationFn: (ids: number[]) => bulkRejectRepricingExecutions(ids),
    onSuccess: () => { setSelected(new Set()); queryClient.invalidateQueries({ queryKey: ["repricing"] }); },
  });
  const autoApproveMut = useMutation({
    mutationFn: () => autoApproveRepricingExecutions(marketplace || undefined),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["repricing"] }),
  });
  const executeMut = useMutation({
    mutationFn: () => executeRepricingPrices(marketplace),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["repricing"] }),
  });

  const { data: analyticsTrend } = useQuery<RepricingAnalyticsTrend[]>({
    queryKey: ["repricing", "analytics-trend", marketplace],
    queryFn: () => getRepricingAnalyticsTrend(marketplace ? { marketplace_id: marketplace } : undefined),
    enabled: tab === "analytics",
  });
  const { data: strategyAnalytics } = useQuery<RepricingStrategyAnalytics[]>({
    queryKey: ["repricing", "analytics-strategy", marketplace],
    queryFn: () => getRepricingAnalyticsByStrategy(marketplace ? { marketplace_id: marketplace } : undefined),
    enabled: tab === "analytics",
  });

  const stratItems: RepricingStrategy[] = strategies?.items ?? [];
  const execItems: RepricingExecution[] = executions?.items ?? [];

  const selectCls = "h-9 rounded-md border border-white/10 bg-[#1e293b] px-3 text-xs text-white";

  return (
    <div className="space-y-6">
      {/* ── Header ── */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold text-white">Repricing Engine</h1>
          <p className="mt-1 text-sm text-white/50">
            Strategie cenowe, propozycje zmian i automatyczne zatwierdzanie.
          </p>
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          <Input
            className="w-48 h-9 border-white/10 bg-[#1e293b] text-white text-xs placeholder:text-white/30"
            placeholder="Marketplace ID"
            value={marketplace}
            onChange={(e) => setMarketplace(e.target.value)}
          />
          <Button
            size="sm"
            className="bg-[#FF9900] text-black hover:bg-[#e68a00]"
            disabled={computeMutation.isPending}
            onClick={() => computeMutation.mutate()}
          >
            <Zap className="mr-1 h-3.5 w-3.5" />
            {computeMutation.isPending ? "Obliczanie…" : "Oblicz propozycje"}
          </Button>
          <Button
            size="sm"
            variant="outline"
            className="border-amber-500/40 text-amber-400 hover:bg-amber-500/10"
            disabled={autoApproveMut.isPending}
            onClick={() => autoApproveMut.mutate()}
          >
            {autoApproveMut.isPending ? "Auto…" : "Auto-Approve"}
          </Button>
          {marketplace && (
            <Button
              size="sm"
              variant="outline"
              className="border-emerald-500/40 text-emerald-400 hover:bg-emerald-500/10"
              disabled={executeMut.isPending}
              onClick={() => executeMut.mutate()}
            >
              {executeMut.isPending ? "Wysyłanie…" : "Wyślij ceny"}
            </Button>
          )}
        </div>
      </div>

      {/* ── KPI Cards ── */}
      {dashboard && (
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-xs text-white/50">
                <Target className="h-4 w-4 text-[#FF9900]" /> Strategie
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white">{dashboard.strategies_active}</div>
              <div className="mt-1 text-xs text-white/40">{dashboard.strategies_total} łącznie</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-xs text-white/50">
                <AlertTriangle className="h-4 w-4 text-amber-400" /> Oczekujące
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white">{dashboard.proposed}</div>
              <div className="mt-1 text-xs text-white/40">do przeglądu</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-xs text-white/50">
                <CheckCircle className="h-4 w-4 text-blue-400" /> Zatwierdzone
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white">{dashboard.approved}</div>
              <div className="mt-1 text-xs text-white/40">gotowe do wysyłki</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-xs text-white/50">
                <TrendingUp className="h-4 w-4 text-emerald-400" /> Wykonane (30d)
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white">{dashboard.executed}</div>
              <div className="mt-1 text-xs text-white/40">zmiany cen wysłane</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-xs text-white/50">
                <Zap className="h-4 w-4 text-[#FF9900]" /> Śr. zmiana
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white">
                {dashboard.avg_proposed_change_pct != null ? `${dashboard.avg_proposed_change_pct.toFixed(1)}%` : "—"}
              </div>
              <div className="mt-1 text-xs text-white/40">propozycje</div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* ── Success banners ── */}
      {computeMutation.isSuccess && (
        <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-4 py-2 text-sm text-emerald-300">
          Utworzono {computeMutation.data.proposals_created} propozycji
        </div>
      )}
      {autoApproveMut.isSuccess && (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-4 py-2 text-sm text-amber-300">
          Auto-zatwierdzono {autoApproveMut.data.auto_approved} propozycji
        </div>
      )}
      {executeMut.isSuccess && (
        <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-4 py-2 text-sm text-emerald-300">
          Wysłano {executeMut.data.submitted} zmian cen (Feed: {executeMut.data.feed_id ?? "—"})
        </div>
      )}
      {bulkApproveMut.isSuccess && (
        <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-4 py-2 text-sm text-emerald-300">
          Zatwierdzono hurtowo {bulkApproveMut.data.approved} ({bulkApproveMut.data.skipped} pominięto)
        </div>
      )}

      {/* ── Tab switcher ── */}
      <div className="flex gap-1 border-b border-white/10">
        <button
          className={`px-4 py-2 text-sm font-medium transition-colors ${tab === "proposals" ? "border-b-2 border-[#FF9900] text-[#FF9900]" : "text-white/50 hover:text-white/80"}`}
          onClick={() => setTab("proposals")}
        >
          Strategie i propozycje
        </button>
        <button
          className={`px-4 py-2 text-sm font-medium transition-colors ${tab === "analytics" ? "border-b-2 border-[#FF9900] text-[#FF9900]" : "text-white/50 hover:text-white/80"}`}
          onClick={() => setTab("analytics")}
        >
          Analityka
        </button>
      </div>

      {/* ═══════════════ PROPOSALS TAB ═══════════════ */}
      {tab === "proposals" && (<>

      {/* New strategy form toggle */}
      {showForm ? (
        <NewStrategyForm onClose={() => setShowForm(false)} marketplace={marketplace} />
      ) : (
        <Button
          variant="outline"
          size="sm"
          className="border-dashed border-white/20 text-white/50 hover:border-[#FF9900] hover:text-[#FF9900]"
          onClick={() => setShowForm(true)}
        >
          <Plus className="mr-1 h-3.5 w-3.5" /> Nowa strategia
        </Button>
      )}

      {/* ── Active Strategies ── */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Aktywne strategie</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {stratLoading ? (
            <div className="space-y-2 p-6">
              {Array.from({ length: 3 }).map((_, i) => <Skeleton key={i} className="h-8 w-full" />)}
            </div>
          ) : stratItems.length === 0 ? (
            <div className="p-8 text-center text-sm text-white/40">
              Brak skonfigurowanych strategii. Kliknij „Nowa strategia" aby dodać.
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Typ</TableHead>
                  <TableHead>SKU</TableHead>
                  <TableHead>Marketplace</TableHead>
                  <TableHead className="text-right">Cena min</TableHead>
                  <TableHead className="text-right">Cena max</TableHead>
                  <TableHead className="text-right">Min marża</TableHead>
                  <TableHead className="text-right">Max Δ/dzień</TableHead>
                  <TableHead>Zatwierdzanie</TableHead>
                  <TableHead className="w-10"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {stratItems.map((s) => (
                  <TableRow key={s.id}>
                    <TableCell><StrategyBadge type={s.strategy_type} /></TableCell>
                    <TableCell className="font-mono text-xs">{s.seller_sku ?? <span className="text-white/30">ALL</span>}</TableCell>
                    <TableCell className="text-xs">{s.marketplace_id ?? <span className="text-white/30">ALL</span>}</TableCell>
                    <TableCell className="text-right tabular-nums">{s.min_price?.toFixed(2) ?? "—"}</TableCell>
                    <TableCell className="text-right tabular-nums">{s.max_price?.toFixed(2) ?? "—"}</TableCell>
                    <TableCell className="text-right tabular-nums">{s.min_margin_pct != null ? `${s.min_margin_pct}%` : "—"}</TableCell>
                    <TableCell className="text-right tabular-nums">{s.max_daily_change_pct != null ? `${s.max_daily_change_pct}%` : "—"}</TableCell>
                    <TableCell>
                      <Badge variant={s.requires_approval ? "outline" : "default"} className={s.requires_approval ? "" : "bg-emerald-500/20 text-emerald-300 border-emerald-500/30"}>
                        {s.requires_approval ? "Ręczne" : "Auto"}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <button
                        className="text-white/30 hover:text-red-400 transition-colors"
                        onClick={() => deleteMut.mutate(s.id)}
                        title="Usuń strategię"
                      >
                        <Trash2 className="h-3.5 w-3.5" />
                      </button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* ── Execution Proposals ── */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between gap-3 flex-wrap">
          <CardTitle className="text-sm">Propozycje zmian cen</CardTitle>
          <div className="flex items-center gap-2">
            <select
              className={selectCls}
              value={execFilter}
              onChange={(e) => setExecFilter(e.target.value)}
            >
              <option value="">Oczekujące + Zatwierdzone</option>
              <option value="proposed">Oczekujące</option>
              <option value="approved">Zatwierdzone</option>
              <option value="executed">Wykonane</option>
              <option value="rejected">Odrzucone</option>
              <option value="expired">Wygasłe</option>
            </select>
            <span className="text-xs text-white/40">{executions?.total ?? 0} łącznie</span>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          {execLoading ? (
            <div className="space-y-2 p-6">
              {Array.from({ length: 5 }).map((_, i) => <Skeleton key={i} className="h-8 w-full" />)}
            </div>
          ) : execItems.length === 0 ? (
            <div className="p-8 text-center text-sm text-white/40">
              Brak propozycji. Kliknij „Oblicz propozycje" aby wygenerować nowe.
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-8">
                    <input
                      type="checkbox"
                      checked={execItems.filter((e) => e.status === "proposed").length > 0 && execItems.filter((e) => e.status === "proposed").every((e) => selected.has(e.id))}
                      onChange={(ev) => {
                        const proposed = execItems.filter((e) => e.status === "proposed").map((e) => e.id);
                        setSelected(ev.target.checked ? new Set(proposed) : new Set());
                      }}
                      className="h-3 w-3 accent-[#FF9900]"
                    />
                  </TableHead>
                  <TableHead>SKU</TableHead>
                  <TableHead>Marketplace</TableHead>
                  <TableHead>Strategia</TableHead>
                  <TableHead className="text-right">Obecna</TableHead>
                  <TableHead className="text-right">Docelowa</TableHead>
                  <TableHead className="text-right">Δ%</TableHead>
                  <TableHead className="text-right">Marża est.</TableHead>
                  <TableHead>Guardrail</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="max-w-[180px]">Powód</TableHead>
                  <TableHead className="w-24"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {execItems.map((e) => (
                  <TableRow key={e.id} data-state={selected.has(e.id) ? "selected" : undefined}>
                    <TableCell>
                      {e.status === "proposed" && (
                        <input
                          type="checkbox"
                          checked={selected.has(e.id)}
                          onChange={(ev) => {
                            const next = new Set(selected);
                            ev.target.checked ? next.add(e.id) : next.delete(e.id);
                            setSelected(next);
                          }}
                          className="h-3 w-3 accent-[#FF9900]"
                        />
                      )}
                    </TableCell>
                    <TableCell className="font-mono text-xs">{e.seller_sku}</TableCell>
                    <TableCell className="text-xs">{e.marketplace_id}</TableCell>
                    <TableCell><StrategyBadge type={e.strategy_type} /></TableCell>
                    <TableCell className="text-right tabular-nums">{e.current_price?.toFixed(2) ?? "—"}</TableCell>
                    <TableCell className="text-right tabular-nums font-medium">{e.target_price.toFixed(2)}</TableCell>
                    <TableCell className={`text-right tabular-nums font-medium ${(e.price_change_pct ?? 0) < 0 ? "text-red-400" : "text-emerald-400"}`}>
                      {e.price_change_pct != null ? `${e.price_change_pct > 0 ? "+" : ""}${e.price_change_pct.toFixed(1)}%` : "—"}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{e.estimated_margin_pct != null ? `${e.estimated_margin_pct.toFixed(1)}%` : "—"}</TableCell>
                    <TableCell className="text-xs text-white/40">{e.guardrail_applied ?? "—"}</TableCell>
                    <TableCell><StatusBadge status={e.status} /></TableCell>
                    <TableCell className="text-xs text-white/40 max-w-[180px] truncate" title={e.reason_text ?? ""}>{e.reason_text ?? "—"}</TableCell>
                    <TableCell>
                      {e.status === "proposed" && (
                        <div className="flex gap-1">
                          <Button
                            size="sm"
                            variant="outline"
                            className="h-6 px-2 text-[10px] border-emerald-500/40 text-emerald-400 hover:bg-emerald-500/10"
                            disabled={approveMut.isPending}
                            onClick={() => approveMut.mutate(e.id)}
                          >
                            Zatwierdź
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            className="h-6 px-2 text-[10px] border-red-500/40 text-red-400 hover:bg-red-500/10"
                            disabled={rejectMut.isPending}
                            onClick={() => rejectMut.mutate(e.id)}
                          >
                            Odrzuć
                          </Button>
                        </div>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
          <BatchBar selectedCount={selected.size} onClear={() => setSelected(new Set())}>
            <BatchActionButton
              label="Zatwierdź zaznaczone"
              onClick={() => bulkApproveMut.mutate([...selected])}
            />
            <BatchActionButton
              label="Odrzuć zaznaczone"
              onClick={() => bulkRejectMut.mutate([...selected])}
            />
          </BatchBar>
        </CardContent>
      </Card>
      </>)}

      {/* ═══════════════ ANALYTICS TAB ═══════════════ */}
      {tab === "analytics" && (
        <div className="space-y-6">
          {/* Trend chart */}
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Trend repricingu (30 dni)</CardTitle>
            </CardHeader>
            <CardContent>
              {analyticsTrend && analyticsTrend.length > 0 ? (
                <ResponsiveContainer width="100%" height={280}>
                  <BarChart data={analyticsTrend}>
                    <XAxis dataKey="date" tick={{ fontSize: 10, fill: "#9ca3af" }} />
                    <YAxis tick={{ fontSize: 10, fill: "#9ca3af" }} />
                    <Tooltip
                      contentStyle={{
                        background: "#1e293b",
                        border: "1px solid rgba(255,255,255,0.1)",
                        borderRadius: 8,
                      }}
                      labelStyle={{ color: "#fff" }}
                    />
                    <Legend wrapperStyle={{ fontSize: 11 }} />
                    <Bar dataKey="proposals_created" name="Utworzono" fill="#94a3b8" radius={[2, 2, 0, 0]} />
                    <Bar dataKey="proposals_approved" name="Zatwierdzono" fill="#3b82f6" radius={[2, 2, 0, 0]} />
                    <Bar dataKey="executions_succeeded" name="Wykonano" fill="#22c55e" radius={[2, 2, 0, 0]} />
                    <Bar dataKey="executions_failed" name="Błąd" fill="#ef4444" radius={[2, 2, 0, 0]} />
                    <Bar dataKey="auto_approved_count" name="Auto" fill="#f59e0b" radius={[2, 2, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              ) : (
                <div className="py-8 text-center text-sm text-white/40">
                  Brak danych analitycznych. Użyj „Oblicz propozycje" — dane pojawią się po uruchomieniu codziennego joba.
                </div>
              )}
            </CardContent>
          </Card>

          {/* Per-strategy breakdown */}
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Podsumowanie per strategia (30 dni)</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              {strategyAnalytics && strategyAnalytics.length > 0 ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Strategia</TableHead>
                      <TableHead className="text-right">Łącznie</TableHead>
                      <TableHead className="text-right">Wykonane</TableHead>
                      <TableHead className="text-right">Odrzucone</TableHead>
                      <TableHead className="text-right">Oczekujące</TableHead>
                      <TableHead className="text-right">Śr. Δ%</TableHead>
                      <TableHead className="text-right">Śr. marża</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {strategyAnalytics.map((s) => (
                      <TableRow key={s.strategy_type}>
                        <TableCell><StrategyBadge type={s.strategy_type} /></TableCell>
                        <TableCell className="text-right tabular-nums">{s.total}</TableCell>
                        <TableCell className="text-right tabular-nums text-emerald-400 font-medium">{s.executed}</TableCell>
                        <TableCell className="text-right tabular-nums text-red-400">{s.rejected}</TableCell>
                        <TableCell className="text-right tabular-nums text-amber-400">{s.pending}</TableCell>
                        <TableCell className="text-right tabular-nums">{s.avg_change_pct != null ? `${s.avg_change_pct.toFixed(1)}%` : "—"}</TableCell>
                        <TableCell className="text-right tabular-nums">{s.avg_margin != null ? `${s.avg_margin.toFixed(1)}%` : "—"}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <div className="p-8 text-center text-sm text-white/40">
                  Brak danych per strategia.
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
