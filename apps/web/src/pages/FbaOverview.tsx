import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Boxes, PackageCheck, Truck } from "lucide-react";
import { getFbaOverview, getFbaReportDiagnostics, type FbaMarketplaceDiagnosticItem, type FbaOverviewMetric } from "@/lib/api";
import { FbaJobStatusStrip } from "@/components/fba/FbaJobStatusStrip";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { formatPLN } from "@/lib/utils";
import { DataFreshness, ClientExportButton } from "@/components/shared";

function metricIcon(label: string) {
  if (label.includes("OOS")) return AlertTriangle;
  if (label.includes("Inbound")) return Truck;
  if (label.includes("Aged")) return Boxes;
  return PackageCheck;
}

function formatMetric(metric: FbaOverviewMetric) {
  if (metric.unit === "PLN") return formatPLN(metric.value);
  if (metric.unit === "%") return `${metric.value}%`;
  return `${metric.value}`;
}

function inventorySourceLabel(item: FbaMarketplaceDiagnosticItem) {
  if (item.inventory_api?.fetch_mode === "fallback_inventory_api") return "API fallback";
  if (item.planning?.request_status === "DONE" || item.planning?.selected_status === "DONE") return "Planning report";
  if (item.planning?.request_status === "COOLDOWN") return "Cooldown -> API";
  return "Problem";
}

function inventorySourceVariant(item: FbaMarketplaceDiagnosticItem): "secondary" | "warning" | "destructive" {
  if (item.inventory_api?.fetch_mode === "fallback_inventory_api") return "warning";
  if (item.planning?.request_status === "DONE" || item.planning?.selected_status === "DONE") return "secondary";
  if (item.planning?.request_status === "COOLDOWN") return "warning";
  return "destructive";
}

function strandedSourceLabel(item: FbaMarketplaceDiagnosticItem) {
  const mode = item.stranded?.fetch_mode ?? "";
  if (mode === "fallback_planning_unfulfillable") return "Proxy z planning";
  if (mode === "fallback_inventory_api_unfulfillable") return "Proxy z API";
  if (item.stranded?.request_status === "DONE" || item.stranded?.selected_status === "DONE") return "Stranded report";
  if (!item.stranded) return "Brak danych";
  return "Problem";
}

function strandedSourceVariant(item: FbaMarketplaceDiagnosticItem): "secondary" | "warning" | "destructive" {
  const mode = item.stranded?.fetch_mode ?? "";
  if (mode === "fallback_planning_unfulfillable" || mode === "fallback_inventory_api_unfulfillable") return "warning";
  if (item.stranded?.request_status === "DONE" || item.stranded?.selected_status === "DONE") return "secondary";
  if (!item.stranded) return "secondary";
  return "destructive";
}

function diagnosticNote(item: FbaMarketplaceDiagnosticItem) {
  if (item.planning?.request_status === "COOLDOWN") return "Planning report pominięty po ostatnich FATAL.";
  if (item.planning?.request_status === "FATAL") return "Planning report nie działa stabilnie dla tego MP.";
  if (item.stranded?.request_status === "CANCELLED") return "Canonical stranded report niedostępny, użyto proxy.";
  if (item.inventory_api?.fetch_mode === "fallback_inventory_api") return "Inventory działa z API fallback.";
  return "OK";
}

export default function FbaOverviewPage() {
  const { data, isLoading } = useQuery({
    queryKey: ["fba-overview"],
    queryFn: getFbaOverview,
    refetchInterval: 60_000,
  });
  const diagnosticsQuery = useQuery({
    queryKey: ["fba-report-diagnostics"],
    queryFn: () => getFbaReportDiagnostics(72),
    refetchInterval: 120_000,
  });
  const groupedStockoutRisks = useMemo(() => {
    const grouped = new Map<
      string,
      {
        sku: string;
        title_preferred?: string | null;
        marketplace_codes: string[];
        on_hand: number;
        inbound: number;
        worst_days_cover: number | null;
      }
    >();

    for (const item of data?.top_stockout_risks ?? []) {
      const existing = grouped.get(item.sku);
      if (existing) {
        if (item.marketplace_code && !existing.marketplace_codes.includes(item.marketplace_code)) {
          existing.marketplace_codes.push(item.marketplace_code);
        }
        existing.on_hand += item.on_hand;
        existing.inbound += item.inbound;
        if (item.days_cover !== null && item.days_cover !== undefined) {
          existing.worst_days_cover =
            existing.worst_days_cover === null ? item.days_cover : Math.min(existing.worst_days_cover, item.days_cover);
        }
        if (!existing.title_preferred && item.title_preferred) {
          existing.title_preferred = item.title_preferred;
        }
        continue;
      }

      grouped.set(item.sku, {
        sku: item.sku,
        title_preferred: item.title_preferred,
        marketplace_codes: item.marketplace_code ? [item.marketplace_code] : [],
        on_hand: item.on_hand,
        inbound: item.inbound,
        worst_days_cover: item.days_cover ?? null,
      });
    }

    return Array.from(grouped.values())
      .sort((left, right) => {
        const leftDays = left.worst_days_cover ?? 9999;
        const rightDays = right.worst_days_cover ?? 9999;
        if (leftDays !== rightDays) return leftDays - rightDays;
        return right.inbound - left.inbound;
      })
      .slice(0, 20);
  }, [data?.top_stockout_risks]);
  const hasFallbackData = useMemo(
    () =>
      (diagnosticsQuery.data?.items ?? []).some(
        (item) =>
          item.planning?.request_status === "FATAL" ||
          item.planning?.request_status === "COOLDOWN" ||
          item.stranded?.request_status === "CANCELLED" ||
          item.inventory_api?.fetch_mode === "fallback_inventory_api" ||
          item.stranded?.fetch_mode === "fallback_planning_unfulfillable" ||
          item.stranded?.fetch_mode === "fallback_inventory_api_unfulfillable",
      ),
    [diagnosticsQuery.data?.items],
  );

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">FBA Ops Overview</h1>
          <p className="text-sm text-white/50">Codzienny radar ryzyk FBA: inventory, inbound, aged i jakość feedów.</p>
        </div>
        <div className="flex items-center gap-2">
          <DataFreshness lastSync={(data as any)?.last_sync} staleMinutes={60} label="FBA" />
          {data?.top_stockout_risks && <ClientExportButton data={data.top_stockout_risks} filename="fba_stockout_risks" />}
        </div>
      </div>

      <FbaJobStatusStrip />

      {hasFallbackData ? (
        <Card className="border-amber-500/30">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-amber-300">FBA data completeness: partial</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-white/70">
            At least one marketplace uses report fallback or proxy data. Aged/stranded and some inventory-driven decisions should be treated as partial until canonical feeds recover.
          </CardContent>
        </Card>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {(data?.metrics ?? []).map((metric) => {
          const Icon = metricIcon(metric.label);
          return (
            <Card key={metric.label}>
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-xs text-white/60">
                  <Icon className="h-4 w-4 text-[#FF9900]" />
                  {metric.label}
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{formatMetric(metric)}</div>
                <div className="mt-2">
                  <Badge variant={metric.status === "critical" ? "destructive" : metric.status === "warning" ? "warning" : "secondary"}>
                    {metric.status}
                  </Badge>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Top Stockout Risks</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Produkt</TableHead>
                  <TableHead>MP</TableHead>
                  <TableHead className="text-right">On hand</TableHead>
                  <TableHead className="text-right">Inbound</TableHead>
                  <TableHead className="text-right">Worst days cover</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {groupedStockoutRisks.map((item) => (
                  <TableRow key={item.sku}>
                    <TableCell>
                      <div className="max-w-[28rem] truncate text-sm font-medium text-white">
                        {item.title_preferred ?? item.sku}
                      </div>
                      <div className="font-mono text-[11px] text-white/45">{item.sku}</div>
                    </TableCell>
                    <TableCell>{item.marketplace_codes.join(", ") || "-"}</TableCell>
                    <TableCell className="text-right">{item.on_hand}</TableCell>
                    <TableCell className="text-right">{item.inbound}</TableCell>
                    <TableCell className="text-right">{item.worst_days_cover ?? "-"}</TableCell>
                  </TableRow>
                ))}
                {!isLoading && groupedStockoutRisks.length === 0 && (
                  <TableRow><TableCell colSpan={5} className="text-center text-white/50">Brak aktywnych ryzyk stockoutu.</TableCell></TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Inbound Delays</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Shipment</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Planned</TableHead>
                  <TableHead className="text-right">Received</TableHead>
                  <TableHead className="text-right">Days</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(data?.inbound_delays ?? []).map((item) => (
                  <TableRow key={item.shipment_id}>
                    <TableCell>
                      <div className="font-mono text-xs">{item.shipment_id}</div>
                      <div className="text-[11px] text-white/45">{item.shipment_name ?? "-"}</div>
                    </TableCell>
                    <TableCell>{item.status}</TableCell>
                    <TableCell className="text-right">{item.units_planned}</TableCell>
                    <TableCell className="text-right">{item.units_received}</TableCell>
                    <TableCell className="text-right">{item.days_in_status}</TableCell>
                  </TableRow>
                ))}
                {!isLoading && (data?.inbound_delays?.length ?? 0) === 0 && (
                  <TableRow><TableCell colSpan={5} className="text-center text-white/50">Brak opóźnionych shipmentów w aktualnym snapshotcie.</TableCell></TableRow>
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Marketplace Feed Diagnostics</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>MP</TableHead>
                <TableHead>Źródło inventory</TableHead>
                <TableHead>Źródło stranded</TableHead>
                <TableHead>Uwagi</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {(diagnosticsQuery.data?.items ?? []).map((item) => (
                <TableRow key={item.marketplace_id}>
                  <TableCell className="font-medium">{item.marketplace_code}</TableCell>
                  <TableCell>
                    <Badge variant={inventorySourceVariant(item)}>{inventorySourceLabel(item)}</Badge>
                  </TableCell>
                  <TableCell>
                    <Badge variant={strandedSourceVariant(item)}>{strandedSourceLabel(item)}</Badge>
                  </TableCell>
                  <TableCell className="text-xs text-white/60">{diagnosticNote(item)}</TableCell>
                </TableRow>
              ))}
              {!diagnosticsQuery.isLoading && (diagnosticsQuery.data?.items.length ?? 0) === 0 && (
                <TableRow><TableCell colSpan={4} className="text-center text-white/50">Brak świeżej diagnostyki feedów.</TableCell></TableRow>
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
