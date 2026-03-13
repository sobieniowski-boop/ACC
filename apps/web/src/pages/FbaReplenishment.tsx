import { Fragment, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ChevronDown, ChevronRight } from "lucide-react";
import { getFbaReplenishmentSuggestions, runFbaJob } from "@/lib/api";
import { FbaJobStatusStrip } from "@/components/fba/FbaJobStatusStrip";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ClientExportButton } from "@/components/shared";

function urgencyBadgeVariant(urgency: string): "destructive" | "warning" | "secondary" {
  if (urgency === "critical") return "destructive";
  if (urgency === "high") return "warning";
  return "secondary";
}

export default function FbaReplenishmentPage() {
  const qc = useQueryClient();
  const [expandedSkus, setExpandedSkus] = useState<Record<string, boolean>>({});
  const { data } = useQuery({
    queryKey: ["fba-replenishment"],
    queryFn: () => getFbaReplenishmentSuggestions(),
  });
  const recompute = useMutation({
    mutationFn: () => runFbaJob("recompute_fba_replenishment"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["fba-replenishment"] }),
  });

  const groupedItems = useMemo(() => {
    const grouped = new Map<
      string,
      {
        sku: string;
        title_preferred?: string | null;
        marketplace_codes: string[];
        current_days_cover: number | null;
        suggested_qty: number;
        suggested_ship_week: string;
        urgency: string;
        items: NonNullable<typeof data>["items"];
      }
    >();

    for (const item of data?.items ?? []) {
      const existing = grouped.get(item.sku);
      if (existing) {
        if (item.marketplace_code && !existing.marketplace_codes.includes(item.marketplace_code)) {
          existing.marketplace_codes.push(item.marketplace_code);
        }
        existing.suggested_qty += item.suggested_qty;
        if (item.current_days_cover !== null && item.current_days_cover !== undefined) {
          existing.current_days_cover =
            existing.current_days_cover === null ? item.current_days_cover : Math.min(existing.current_days_cover, item.current_days_cover);
        }
        if (item.suggested_ship_week < existing.suggested_ship_week) {
          existing.suggested_ship_week = item.suggested_ship_week;
        }
        if (item.urgency === "critical" || (item.urgency === "high" && existing.urgency !== "critical")) {
          existing.urgency = item.urgency;
        }
        if (!existing.title_preferred && item.title_preferred) {
          existing.title_preferred = item.title_preferred;
        }
        existing.items.push(item);
        continue;
      }

      grouped.set(item.sku, {
        sku: item.sku,
        title_preferred: item.title_preferred,
        marketplace_codes: item.marketplace_code ? [item.marketplace_code] : [],
        current_days_cover: item.current_days_cover ?? null,
        suggested_qty: item.suggested_qty,
        suggested_ship_week: item.suggested_ship_week,
        urgency: item.urgency,
        items: [item],
      });
    }

    return Array.from(grouped.values())
      .map((group) => ({
        ...group,
        items: [...group.items].sort((left, right) => {
          const leftDays = left.current_days_cover ?? 9999;
          const rightDays = right.current_days_cover ?? 9999;
          return leftDays - rightDays;
        }),
      }))
      .sort((left, right) => {
        const leftDays = left.current_days_cover ?? 9999;
        const rightDays = right.current_days_cover ?? 9999;
        if (leftDays !== rightDays) return leftDays - rightDays;
        return right.suggested_qty - left.suggested_qty;
      });
  }, [data?.items]);

  const toggleSku = (sku: string) => {
    setExpandedSkus((current) => ({ ...current, [sku]: !current[sku] }));
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Replenishment Planner</h1>
          <p className="text-sm text-white/50">
            Widok produktowy planera. SKU typu <span className="font-mono">amzn.gr.</span> sa celowo pomijane, bo oznaczaja
            zwroty przyjete przez Amazon na stan do odsprzedazy.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {data?.items && <ClientExportButton data={data.items} filename="fba_replenishment" />}
          <Button onClick={() => recompute.mutate()} disabled={recompute.isPending}>
            {recompute.isPending ? "Computing..." : "Recompute"}
          </Button>
        </div>
      </div>
      <FbaJobStatusStrip />
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Suggestions</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Produkt</TableHead>
                <TableHead>MP</TableHead>
                <TableHead className="text-right">Worst days cover</TableHead>
                <TableHead className="text-right">Suggested qty</TableHead>
                <TableHead className="text-right">Earliest ship week</TableHead>
                <TableHead>Urgency</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {groupedItems.map((group) => {
                const expanded = !!expandedSkus[group.sku];
                return (
                  <Fragment key={group.sku}>
                    <TableRow>
                      <TableCell>
                        <button onClick={() => toggleSku(group.sku)} className="flex w-full items-start gap-2 text-left">
                          {expanded ? (
                            <ChevronDown className="mt-0.5 h-4 w-4 shrink-0 text-white/50" />
                          ) : (
                            <ChevronRight className="mt-0.5 h-4 w-4 shrink-0 text-white/50" />
                          )}
                          <div className="min-w-0">
                            <div className="max-w-[28rem] truncate text-sm font-medium text-white">
                              {group.title_preferred ?? group.sku}
                            </div>
                            <div className="font-mono text-[11px] text-white/45">{group.sku}</div>
                          </div>
                        </button>
                      </TableCell>
                      <TableCell>{group.marketplace_codes.join(", ") || "-"}</TableCell>
                      <TableCell className="text-right">{group.current_days_cover ?? "-"}</TableCell>
                      <TableCell className="text-right">{group.suggested_qty}</TableCell>
                      <TableCell className="text-right">{group.suggested_ship_week}</TableCell>
                      <TableCell>
                        <Badge variant={urgencyBadgeVariant(group.urgency)}>{group.urgency}</Badge>
                      </TableCell>
                    </TableRow>
                    {expanded
                      ? group.items.map((item) => (
                          <TableRow key={`${item.marketplace_id}-${item.sku}`} className="bg-white/[0.02]">
                            <TableCell className="pl-10 text-xs text-white/50">Marketplace detail</TableCell>
                            <TableCell>{item.marketplace_code}</TableCell>
                            <TableCell className="text-right">{item.current_days_cover ?? "-"}</TableCell>
                            <TableCell className="text-right">{item.suggested_qty}</TableCell>
                            <TableCell className="text-right">{item.suggested_ship_week}</TableCell>
                            <TableCell>
                              <Badge variant={urgencyBadgeVariant(item.urgency)}>{item.urgency}</Badge>
                            </TableCell>
                          </TableRow>
                        ))
                      : null}
                  </Fragment>
                );
              })}
              {(groupedItems.length ?? 0) === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center text-white/50">
                    Brak sugestii replenishment dla aktualnych danych.
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
