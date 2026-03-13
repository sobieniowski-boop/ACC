import { Fragment, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useSearchParams } from "react-router-dom";
import { getFbaInventory } from "@/lib/api";
import { FbaJobStatusStrip } from "@/components/fba/FbaJobStatusStrip";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ClientExportButton } from "@/components/shared";

function summarizeRisk(stockoutRisk: string, overstockRisk: string) {
  if (stockoutRisk !== "ok") return stockoutRisk;
  if (overstockRisk !== "ok") return overstockRisk;
  return "ok";
}

function riskBadgeVariant(risk: string): "destructive" | "warning" | "secondary" | "success" {
  if (risk === "critical") return "destructive";
  if (risk === "warning") return "warning";
  if (risk !== "ok") return "secondary";
  return "success";
}

export default function FbaInventoryPage() {
  const [searchParams] = useSearchParams();
  const initialSkuSearch = searchParams.get("sku_search") ?? "";
  const initialMarketplaceId = searchParams.get("marketplace_id") ?? "";
  const [skuSearch, setSkuSearch] = useState(initialSkuSearch);
  const [riskType, setRiskType] = useState("all");
  const [expandedSkus, setExpandedSkus] = useState<Record<string, boolean>>(
    initialSkuSearch ? { [initialSkuSearch]: true } : {}
  );

  const { data } = useQuery({
    queryKey: ["fba-inventory", skuSearch, riskType, initialMarketplaceId],
    queryFn: () =>
      getFbaInventory({
        ...(skuSearch ? { sku_search: skuSearch } : {}),
        ...(initialMarketplaceId ? { marketplace_id: initialMarketplaceId } : {}),
        ...(riskType !== "all" ? { risk_type: riskType } : {}),
      }),
  });

  const groupedItems = useMemo(() => {
    const grouped = new Map<
      string,
      {
        sku: string;
        title_preferred?: string | null;
        brand?: string | null;
        category?: string | null;
        internal_sku?: string | null;
        ean?: string | null;
        parent_asin?: string | null;
        marketplace_codes: string[];
        on_hand: number;
        inbound: number;
        reserved: number;
        velocity_30d: number;
        worst_days_cover: number | null;
        risk: string;
        items: NonNullable<typeof data>["items"];
      }
    >();

    for (const item of data?.items ?? []) {
      const itemRisk = summarizeRisk(item.stockout_risk, item.overstock_risk);
      const existing = grouped.get(item.sku);
      if (existing) {
        if (item.marketplace_code && !existing.marketplace_codes.includes(item.marketplace_code)) {
          existing.marketplace_codes.push(item.marketplace_code);
        }
        existing.on_hand += item.on_hand;
        existing.inbound += item.inbound;
        existing.reserved += item.reserved;
        existing.velocity_30d += item.velocity_30d;
        if (item.days_cover !== null && item.days_cover !== undefined) {
          existing.worst_days_cover =
            existing.worst_days_cover === null ? item.days_cover : Math.min(existing.worst_days_cover, item.days_cover);
        }
        if (!existing.title_preferred && item.title_preferred) {
          existing.title_preferred = item.title_preferred;
        }
        if (!existing.brand && item.brand) {
          existing.brand = item.brand;
        }
        if (!existing.category && item.category) {
          existing.category = item.category;
        }
        if (!existing.internal_sku && item.internal_sku) {
          existing.internal_sku = item.internal_sku;
        }
        if (!existing.ean && item.ean) {
          existing.ean = item.ean;
        }
        if (!existing.parent_asin && item.parent_asin) {
          existing.parent_asin = item.parent_asin;
        }
        if (itemRisk === "critical" || (itemRisk === "warning" && existing.risk !== "critical") || (existing.risk === "ok" && itemRisk !== "ok")) {
          existing.risk = itemRisk;
        }
        existing.items.push(item);
        continue;
      }

      grouped.set(item.sku, {
        sku: item.sku,
        title_preferred: item.title_preferred,
        brand: item.brand,
        category: item.category,
        internal_sku: item.internal_sku,
        ean: item.ean,
        parent_asin: item.parent_asin,
        marketplace_codes: item.marketplace_code ? [item.marketplace_code] : [],
        on_hand: item.on_hand,
        inbound: item.inbound,
        reserved: item.reserved,
        velocity_30d: item.velocity_30d,
        worst_days_cover: item.days_cover ?? null,
        risk: itemRisk,
        items: [item],
      });
    }

    return Array.from(grouped.values())
      .map((group) => ({
        ...group,
        items: [...group.items].sort((left, right) => {
          const leftDays = left.days_cover ?? 9999;
          const rightDays = right.days_cover ?? 9999;
          return leftDays - rightDays;
        }),
      }))
      .sort((left, right) => {
        const leftDays = left.worst_days_cover ?? 9999;
        const rightDays = right.worst_days_cover ?? 9999;
        if (leftDays !== rightDays) return leftDays - rightDays;
        return right.velocity_30d - left.velocity_30d;
      });
  }, [data?.items]);

  const toggleSku = (sku: string) => {
    setExpandedSkus((current) => ({ ...current, [sku]: !current[sku] }));
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">FBA Inventory</h1>
          <p className="text-sm text-white/50">
            Widok produktowy FBA. Domyslnie jeden wiersz per SKU, z mozliwoscia rozwiniecia szczegolu per marketplace.
          </p>
        </div>
        {data?.items && <ClientExportButton data={data.items} filename="fba_inventory" />}
      </div>
      <FbaJobStatusStrip />
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-sm">Inventory Health</CardTitle>
          <div className="flex gap-2">
            <Input value={skuSearch} onChange={(e) => setSkuSearch(e.target.value)} placeholder="SKU / ASIN" className="w-40" />
            <select
              value={riskType}
              onChange={(e) => setRiskType(e.target.value)}
              className="rounded-md border border-white/10 bg-[#111827] px-3 text-sm"
            >
              <option value="all">All risks</option>
              <option value="stockout">Stockout</option>
              <option value="overstock">Overstock</option>
            </select>
          </div>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Produkt</TableHead>
                <TableHead>MP</TableHead>
                <TableHead className="text-right">On hand</TableHead>
                <TableHead className="text-right">Inbound</TableHead>
                <TableHead className="text-right">Reserved</TableHead>
                <TableHead className="text-right">Vel 30d</TableHead>
                <TableHead className="text-right">Worst days cover</TableHead>
                <TableHead>Risk</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {groupedItems.map((group) => {
                const expanded = !!expandedSkus[group.sku];
                const isHighlighted = initialSkuSearch && group.sku === initialSkuSearch;
                return (
                  <Fragment key={group.sku}>
                    <TableRow className={isHighlighted ? "bg-white/5 ring-1 ring-[#FF9900]/30" : ""}>
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
                            <div className="text-[11px] text-white/45">
                              {group.brand ?? "-"} {group.category ? `| ${group.category}` : ""}
                            </div>
                            <div className="text-[11px] text-white/40">
                              {group.internal_sku ? `ISK ${group.internal_sku}` : "ISK -"}
                              {group.ean ? ` | EAN ${group.ean}` : ""}
                              {group.parent_asin ? ` | Parent ${group.parent_asin}` : ""}
                            </div>
                          </div>
                        </button>
                      </TableCell>
                      <TableCell>{group.marketplace_codes.join(", ") || "-"}</TableCell>
                      <TableCell className="text-right">{group.on_hand}</TableCell>
                      <TableCell className="text-right">{group.inbound}</TableCell>
                      <TableCell className="text-right">{group.reserved}</TableCell>
                      <TableCell className="text-right">{group.velocity_30d.toFixed(2)}</TableCell>
                      <TableCell className="text-right">{group.worst_days_cover ?? "-"}</TableCell>
                      <TableCell>
                        <Badge variant={riskBadgeVariant(group.risk)}>{group.risk}</Badge>
                      </TableCell>
                    </TableRow>
                    {expanded
                      ? group.items.map((item) => {
                          const rowRisk = summarizeRisk(item.stockout_risk, item.overstock_risk);
                          return (
                            <TableRow
                              key={`${item.marketplace_id}-${item.sku}`}
                              className={item.marketplace_id === initialMarketplaceId ? "bg-[#FF9900]/5" : "bg-white/[0.02]"}
                            >
                              <TableCell className="pl-10 text-xs text-white/50">
                                {item.internal_sku ? `ISK ${item.internal_sku}` : "Marketplace detail"}
                              </TableCell>
                              <TableCell>{item.marketplace_code}</TableCell>
                              <TableCell className="text-right">{item.on_hand}</TableCell>
                              <TableCell className="text-right">{item.inbound}</TableCell>
                              <TableCell className="text-right">{item.reserved}</TableCell>
                              <TableCell className="text-right">{item.velocity_30d.toFixed(2)}</TableCell>
                              <TableCell className="text-right">{item.days_cover ?? "-"}</TableCell>
                              <TableCell>
                                <Badge variant={riskBadgeVariant(rowRisk)}>{rowRisk}</Badge>
                              </TableCell>
                            </TableRow>
                          );
                        })
                      : null}
                  </Fragment>
                );
              })}
              {groupedItems.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="text-center text-white/50">
                    Brak rekordow inventory dla biezacych filtrow.
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
