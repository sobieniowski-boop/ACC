import { useMemo } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import {
  Activity,
  ArrowRight,
  Boxes,
  CircleAlert,
  GitBranch,
  Loader2,
  RefreshCw,
  ShieldAlert,
  TimerReset,
} from "lucide-react";
import {
  getManageInventoryOverview,
  type ManageInventoryCoverageItem,
  type ManageInventoryDecisionItem,
  type ManageInventoryFamilySummary,
  type ManageInventoryMetric,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn, formatPLN } from "@/lib/utils";

function metricValue(metric: ManageInventoryMetric) {
  if (metric.unit === "PLN") return formatPLN(metric.value);
  if (metric.unit === "%") return `${metric.value}%`;
  return `${metric.value}`;
}

function metricVariant(status: string): "success" | "secondary" | "warning" | "destructive" {
  if (status === "critical") return "destructive";
  if (status === "warning") return "warning";
  if (status === "ok") return "success";
  return "secondary";
}

function formatSyncTimestamp(value?: string | null) {
  if (!value) return "Brak danych";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Brak danych";
  return new Intl.DateTimeFormat("pl-PL", {
    dateStyle: "short",
    timeStyle: "short",
  }).format(date);
}

function isTrafficMetric(metric: ManageInventoryMetric) {
  return /sessions|page views|unit session|cvr/i.test(metric.label);
}

function CoverageChip({ item }: { item: ManageInventoryCoverageItem }) {
  return (
    <div className="rounded-lg border border-white/10 bg-white/[0.03] px-3 py-3">
      <div className="mb-2 flex items-center justify-between gap-2">
        <div className="text-[11px] uppercase tracking-[0.12em] text-white/45">{item.label}</div>
        <Badge variant={metricVariant(item.status)}>{item.status}</Badge>
      </div>
      <div className="text-xl font-semibold text-white">{item.pct.toFixed(1)}%</div>
      <div className="mt-1 text-xs leading-5 text-white/45">{item.note ?? "-"}</div>
    </div>
  );
}

function ActionPanel({
  title,
  count,
  detail,
  icon: Icon,
  tone,
  onClick,
}: {
  title: string;
  count: number;
  detail: string;
  icon: typeof Activity;
  tone: "critical" | "warning" | "secondary";
  onClick: () => void;
}) {
  const toneClass =
    tone === "critical"
      ? "border-red-500/30 bg-red-500/[0.08]"
      : tone === "warning"
        ? "border-amber-500/30 bg-amber-500/[0.08]"
        : "border-white/10 bg-white/[0.03]";

  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "flex h-full w-full flex-col rounded-xl border p-4 text-left transition hover:border-white/25 hover:bg-white/[0.05]",
        toneClass,
      )}
    >
      <div className="mb-3 flex items-start justify-between gap-3">
        <div className="rounded-lg border border-white/10 bg-black/20 p-2">
          <Icon className="h-4 w-4 text-white/80" />
        </div>
        <ArrowRight className="h-4 w-4 text-white/35" />
      </div>
      <div className="text-3xl font-bold text-white">{count}</div>
      <div className="mt-2 text-sm font-medium text-white">{title}</div>
      <div className="mt-2 text-xs leading-5 text-white/55">{detail}</div>
    </button>
  );
}

function QuickDecisionTable({
  title,
  items,
  onOpenSku,
}: {
  title: string;
  items: ManageInventoryDecisionItem[];
  onOpenSku: (item: ManageInventoryDecisionItem) => void;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">{title}</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Produkt</TableHead>
              <TableHead>MP</TableHead>
              <TableHead className="text-right">Cover</TableHead>
              <TableHead className="text-right">Sessions 7d</TableHead>
              <TableHead>Decyzja</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.map((item) => (
              <TableRow
                key={`${item.marketplace_id}-${item.sku}`}
                className="cursor-pointer"
                onClick={() => onOpenSku(item)}
              >
                <TableCell>
                  <div className="max-w-[24rem] truncate text-sm font-medium text-white">
                    {item.title_preferred ?? item.sku}
                  </div>
                  <div className="font-mono text-[11px] text-white/45">{item.sku}</div>
                </TableCell>
                <TableCell>{item.marketplace_code}</TableCell>
                <TableCell className="text-right">{item.days_cover ?? "-"}</TableCell>
                <TableCell className="text-right">{item.sessions_7d ?? "-"}</TableCell>
                <TableCell>
                  <Badge variant={metricVariant(item.traffic_coverage_flag ? "warning" : "ok")}>
                    {item.demand_vs_supply_badge}
                  </Badge>
                </TableCell>
              </TableRow>
            ))}
            {items.length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} className="text-center text-white/50">
                  Brak rekordow dla tej listy.
                </TableCell>
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

function FamilyChangesTable({
  items,
  onOpenFamily,
}: {
  items: ManageInventoryFamilySummary[];
  onOpenFamily: (item: ManageInventoryFamilySummary) => void;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Ostatnie zmiany rodzin</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Marketplace</TableHead>
              <TableHead>Parent</TableHead>
              <TableHead className="text-right">Children</TableHead>
              <TableHead className="text-right">Coverage vs DE</TableHead>
              <TableHead>Status</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.map((item) => (
              <TableRow
                key={`${item.marketplace_code}-${item.parent_asin}`}
                className="cursor-pointer"
                onClick={() => onOpenFamily(item)}
              >
                <TableCell>{item.marketplace_code}</TableCell>
                <TableCell className="font-mono text-xs">{item.parent_asin}</TableCell>
                <TableCell className="text-right">{item.children_count}</TableCell>
                <TableCell className="text-right">{item.coverage_vs_de_pct ?? "-"}</TableCell>
                <TableCell>
                  <Badge
                    variant={metricVariant(
                      item.status === "ok"
                        ? "ok"
                        : item.status === "needs_review"
                          ? "warning"
                          : "critical",
                    )}
                  >
                    {item.status}
                  </Badge>
                </TableCell>
              </TableRow>
            ))}
            {items.length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} className="text-center text-white/50">
                  Brak swiezych zmian rodzin.
                </TableCell>
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

export default function InventoryOverviewPage() {
  const navigate = useNavigate();
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ["manage-inventory-overview"],
    queryFn: () => getManageInventoryOverview(),
    staleTime: 2 * 60_000,
    refetchInterval: 120_000,
    placeholderData: keepPreviousData,
    retry: 2,
  });

  const coverage = data?.coverage ?? [];
  const partialTraffic = coverage.some((item) => item.key === "traffic" && item.status !== "ok");
  const trafficCoverage = coverage.find((item) => item.key === "traffic") ?? null;

  const primaryMetrics = useMemo(
    () => (data?.metrics ?? []).filter((metric) => !isTrafficMetric(metric)),
    [data?.metrics],
  );

  const trafficMetrics = useMemo(
    () => (data?.metrics ?? []).filter((metric) => isTrafficMetric(metric)),
    [data?.metrics],
  );

  const actionCards = useMemo(
    () => [
      {
        title: "Uzupelnij zapas teraz",
        count: data?.top_high_demand_low_supply.length ?? 0,
        detail: "Wysoki popyt i niski cover. To jest lista do natychmiastowego dzialania.",
        icon: Boxes,
        tone: "critical" as const,
        onClick: () => navigate("/inventory/all?risk=stockout"),
      },
      {
        title: "Napraw suppressions",
        count: data?.top_suppressed_high_sessions.length ?? 0,
        detail: "Oferty z ruchem, ktore nie sa buyable albo maja problem listingowy.",
        icon: ShieldAlert,
        tone: "warning" as const,
        onClick: () => navigate("/inventory/all?listing_status=suppressed"),
      },
      {
        title: "Sprawdz spadki CVR",
        count: data?.top_cvr_crash.length ?? 0,
        detail: "Sesje trzymaja poziom, ale konwersja siada. Najpierw listing, cena i buyability.",
        icon: CircleAlert,
        tone: "warning" as const,
        onClick: () => navigate("/inventory/all?risk=cvr_crash"),
      },
      {
        title: "Zweryfikuj rodziny",
        count: data?.recently_changed_families.length ?? 0,
        detail: "Swieze zmiany parent/child albo rodziny wymagajace przegladu przed apply.",
        icon: GitBranch,
        tone: "secondary" as const,
        onClick: () => navigate("/inventory/families"),
      },
    ],
    [
      data?.recently_changed_families.length,
      data?.top_cvr_crash.length,
      data?.top_high_demand_low_supply.length,
      data?.top_suppressed_high_sessions.length,
      navigate,
    ],
  );

  const openSku = (item: ManageInventoryDecisionItem) => {
    navigate(`/inventory/all?search=${encodeURIComponent(item.sku)}`);
  };

  const openFamily = (item: ManageInventoryFamilySummary) => {
    navigate(
      `/inventory/families?marketplace=${encodeURIComponent(item.marketplace_code)}&parent=${encodeURIComponent(item.parent_asin)}`,
    );
  };

  if (isLoading && !data) {
    return (
      <div className="flex min-h-[400px] flex-col items-center justify-center gap-4">
        <Loader2 className="h-8 w-8 animate-spin text-white/50" />
        <p className="text-sm text-white/50">Ladowanie danych inventory...</p>
      </div>
    );
  }

  if (isError && !data) {
    return (
      <div className="flex min-h-[400px] flex-col items-center justify-center gap-4">
        <p className="text-sm text-white/60">Nie udalo sie zaladowac danych. Sprawdz polaczenie i sprobuj ponownie.</p>
        <Button variant="outline" size="sm" onClick={() => refetch()}>
          <RefreshCw className="mr-2 h-4 w-4" />
          Ponow probe
        </Button>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {isError && (
        <Card className="border-red-500/30 bg-red-500/[0.05]">
          <CardContent className="flex items-center justify-between py-3">
            <p className="text-sm text-red-300">Odswiezanie danych nie powiodlo sie. Wyswietlane sa ostatnie dostepne dane.</p>
            <Button variant="outline" size="sm" onClick={() => refetch()}>
              <RefreshCw className="mr-2 h-4 w-4" />
              Ponow
            </Button>
          </CardContent>
        </Card>
      )}
      <div className="grid items-start gap-4 xl:grid-cols-[1.2fr_0.8fr]">
        <Card className="self-start border-white/10 bg-[linear-gradient(180deg,rgba(255,153,0,0.08),rgba(17,24,39,0.96))]">
          <CardHeader className="pb-4">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div>
                <CardTitle className="text-3xl text-white">Inventory Control Tower</CardTitle>
                <CardDescription className="mt-2 max-w-3xl text-sm leading-6 text-white/60">
                  Decyzje o zapasie, buyability i rodzinach w jednym miejscu, z kontekstem popytu
                  i jakosci danych.
                </CardDescription>
              </div>
              <div className="rounded-xl border border-white/10 bg-black/20 px-4 py-3">
                <div className="mb-1 flex items-center gap-2 text-[11px] uppercase tracking-[0.14em] text-white/45">
                  <TimerReset className="h-3.5 w-3.5" />
                  Last refresh
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-sm font-semibold text-white">{formatSyncTimestamp(data?.generated_at)}</span>
                  <button
                    type="button"
                    onClick={() => refetch()}
                    className="rounded p-1 text-white/40 transition hover:bg-white/10 hover:text-white/80"
                    title="Odswiez dane"
                  >
                    <RefreshCw className={cn("h-3.5 w-3.5", isLoading && "animate-spin")} />
                  </button>
                </div>
              </div>
            </div>
          </CardHeader>
          <CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            {coverage.map((item) => (
              <CoverageChip key={item.key} item={item} />
            ))}
          </CardContent>
        </Card>

        <Card className="self-start border-white/10">
          <CardHeader className="pb-3">
            <CardTitle className="text-base text-white">Co robic teraz</CardTitle>
            <CardDescription className="text-sm text-white/50">
              Priorytety ustawione pod decyzje sprzedazowe i operacyjne.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
            {actionCards.map((item) => (
              <ActionPanel key={item.title} {...item} />
            ))}
          </CardContent>
        </Card>
      </div>

      {partialTraffic ? (
        <Card className="border-amber-500/30 bg-amber-500/[0.05]">
          <CardHeader className="pb-2">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <CardTitle className="text-sm text-amber-300">Traffic coverage: partial</CardTitle>
                <CardDescription className="mt-2 text-sm leading-6 text-white/65">
                  Sessions i CVR sa ukryte, dopoki rollupy Sales & Traffic nie osiagnely
                  bezpiecznego pokrycia. Badges decyzji pozostaja konserwatywne.
                </CardDescription>
              </div>
              <div className="rounded-lg border border-amber-400/20 bg-black/20 px-3 py-2 text-right">
                <div className="text-[11px] uppercase tracking-[0.14em] text-white/45">
                  Traffic coverage
                </div>
                <div className="text-lg font-semibold text-amber-300">
                  {trafficCoverage?.pct.toFixed(1) ?? "0.0"}%
                </div>
              </div>
            </div>
          </CardHeader>
        </Card>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {primaryMetrics.map((metric) => (
          <Card key={metric.label}>
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-white/60">{metric.label}</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white">{metricValue(metric)}</div>
              <div className="mt-2 flex items-center gap-2">
                <Badge variant={metricVariant(metric.status)}>{metric.status}</Badge>
                {metric.delta_pct !== null && metric.delta_pct !== undefined ? (
                  <span className="text-xs text-white/45">
                    {metric.delta_pct > 0 ? "+" : ""}
                    {metric.delta_pct.toFixed(1)}% WoW
                  </span>
                ) : null}
              </div>
            </CardContent>
          </Card>
        ))}
        {!isLoading && primaryMetrics.length === 0 ? (
          <Card>
            <CardContent className="py-10 text-center text-sm text-white/50">
              Brak metryk overview.
            </CardContent>
          </Card>
        ) : null}
      </div>

      {partialTraffic ? (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {trafficMetrics.map((metric) => (
            <Card key={metric.label} className="border-white/10 bg-white/[0.02]">
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between gap-3">
                  <CardTitle className="text-xs text-white/45">{metric.label}</CardTitle>
                  <Badge variant="warning">partial</Badge>
                </div>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-white/35">--</div>
                <div className="mt-2 text-xs leading-5 text-white/45">
                  Metryka ukryta, bo coverage ruchu jest ponizej bezpiecznego progu.
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : trafficMetrics.length > 0 ? (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {trafficMetrics.map((metric) => (
            <Card key={metric.label}>
              <CardHeader className="pb-2">
                <CardTitle className="text-xs text-white/60">{metric.label}</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-white">{metricValue(metric)}</div>
                <div className="mt-2 flex items-center gap-2">
                  <Badge variant={metricVariant(metric.status)}>{metric.status}</Badge>
                  {metric.delta_pct !== null && metric.delta_pct !== undefined ? (
                    <span className="text-xs text-white/45">
                      {metric.delta_pct > 0 ? "+" : ""}
                      {metric.delta_pct.toFixed(1)}% WoW
                    </span>
                  ) : null}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : null}

      <Card>
        <CardHeader className="pb-3">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <CardTitle className="text-sm">Data quality and coverage</CardTitle>
              <CardDescription className="mt-2 text-sm text-white/50">
                Coverage pokazujemy jawnie, zeby nie podejmowac decyzji na slepych metrykach.
              </CardDescription>
            </div>
            <Button variant="outline" size="sm" onClick={() => navigate("/inventory/all")}>
              Otworz glowna tabele
            </Button>
          </div>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          {coverage.map((item) => (
            <div key={item.key} className="space-y-2 rounded-lg border border-white/10 p-3">
              <div className="flex items-center justify-between gap-2">
                <div className="text-sm font-medium text-white">{item.label}</div>
                <Badge variant={metricVariant(item.status)}>{item.status}</Badge>
              </div>
              <div className="text-xl font-bold text-white">{item.pct.toFixed(1)}%</div>
              <Progress value={Math.max(0, Math.min(100, item.pct))} />
              <div className="text-xs text-white/45">{item.note ?? "-"}</div>
            </div>
          ))}
        </CardContent>
      </Card>

      <div className="grid gap-4 xl:grid-cols-2">
        <QuickDecisionTable
          title="Top 20: wysoki popyt + niski cover"
          items={data?.top_high_demand_low_supply ?? []}
          onOpenSku={openSku}
        />
        <QuickDecisionTable
          title="Top 20: spadek CVR"
          items={data?.top_cvr_crash ?? []}
          onOpenSku={openSku}
        />
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <QuickDecisionTable
          title="Top 20: suppressed z ruchem"
          items={data?.top_suppressed_high_sessions ?? []}
          onOpenSku={openSku}
        />
        <FamilyChangesTable items={data?.recently_changed_families ?? []} onOpenFamily={openFamily} />
      </div>
    </div>
  );
}
