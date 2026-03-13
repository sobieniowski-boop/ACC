import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import {
  getManageInventoryFamilies,
  getManageInventoryFamilyDetail,
  type ManageInventoryFamilySummary,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ClientExportButton } from "@/components/shared";

function statusVariant(status: string): "success" | "secondary" | "warning" | "destructive" {
  if (status === "ok" || status === "mapped") return "success";
  if (status === "needs_review") return "warning";
  if (status === "broken") return "destructive";
  return "secondary";
}

export default function InventoryFamiliesPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [marketplace, setMarketplace] = useState(searchParams.get("marketplace") ?? "");
  const [selectedParent, setSelectedParent] = useState<string | null>(searchParams.get("parent"));

  useEffect(() => {
    const next = new URLSearchParams();
    if (marketplace.trim()) next.set("marketplace", marketplace.trim());
    if (selectedParent) next.set("parent", selectedParent);
    setSearchParams(next, { replace: true });
  }, [marketplace, selectedParent, setSearchParams]);

  const familiesQuery = useQuery({
    queryKey: ["manage-inventory-families", marketplace],
    queryFn: () => getManageInventoryFamilies({ ...(marketplace ? { marketplace } : {}) }),
  });

  const detailQuery = useQuery({
    queryKey: ["manage-inventory-family-detail", selectedParent, marketplace],
    queryFn: () => getManageInventoryFamilyDetail(selectedParent!, marketplace || "DE"),
    enabled: !!selectedParent,
  });

  const openFamily = (item: ManageInventoryFamilySummary) => {
    setSelectedParent(item.parent_asin);
    if (!marketplace) {
      setMarketplace(item.marketplace_code);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Family Manager</h1>
        <p className="text-sm text-white/50">Read-first enterprise view of local Amazon families vs DE canonical coverage.</p>
      </div>

      <div className="grid gap-4 xl:grid-cols-[1.4fr_1fr]">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between gap-3">
            <CardTitle className="text-sm">Families</CardTitle>
            <div className="flex items-center gap-2">
              <ClientExportButton data={familiesQuery.data?.items ?? []} filename="inventory_families" />
              <Input value={marketplace} onChange={(e) => setMarketplace(e.target.value)} placeholder="Marketplace code, e.g. DE" className="w-48" />
            </div>
          </CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Marketplace</TableHead>
                  <TableHead>Parent ASIN</TableHead>
                  <TableHead className="text-right">Children</TableHead>
                  <TableHead className="text-right">Coverage vs DE</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(familiesQuery.data?.items ?? []).map((item) => (
                  <TableRow
                    key={`${item.marketplace_code}-${item.parent_asin}`}
                    className={`cursor-pointer ${selectedParent === item.parent_asin ? "bg-white/5" : ""}`}
                    onClick={() => openFamily(item)}
                  >
                    <TableCell>{item.marketplace_code}</TableCell>
                    <TableCell className="font-mono text-xs">{item.parent_asin}</TableCell>
                    <TableCell className="text-right">{item.children_count}</TableCell>
                    <TableCell className="text-right">{item.coverage_vs_de_pct ?? "-"}</TableCell>
                    <TableCell>
                      <Badge variant={statusVariant(item.status)}>{item.status}</Badge>
                    </TableCell>
                  </TableRow>
                ))}
                {!familiesQuery.isLoading && (familiesQuery.data?.items.length ?? 0) === 0 ? (
                  <TableRow>
                    <TableCell colSpan={5} className="text-center text-white/50">
                      Brak rodzin dla biezacego filtra.
                    </TableCell>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Family editor preview</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {!selectedParent ? (
              <div className="text-sm text-white/50">Wybierz parent ASIN z listy, aby zobaczyc children, theme i diff-ready preview.</div>
            ) : detailQuery.isLoading ? (
              <div className="text-sm text-white/50">Ladowanie rodziny...</div>
            ) : detailQuery.data ? (
              <>
                <div className="space-y-1">
                  <div className="font-mono text-sm text-white">{detailQuery.data.parent_asin}</div>
                  <div className="text-xs text-white/50">{detailQuery.data.marketplace_code} | {detailQuery.data.theme ?? "-"} | {detailQuery.data.coverage_vs_de_pct ?? "-"}% vs DE</div>
                  <Badge variant={statusVariant(detailQuery.data.status)}>{detailQuery.data.status}</Badge>
                </div>

                <div className="rounded-lg border border-white/10 p-3">
                  <div className="text-sm font-medium text-white">Current children</div>
                  <div className="mt-2 space-y-2">
                    {detailQuery.data.current_children.slice(0, 12).map((child) => (
                      <div key={`${child.child_asin}-${child.master_key}`} className="rounded border border-white/10 px-2 py-2 text-xs">
                        <div className="font-mono text-white">{child.child_asin ?? "-"}</div>
                        <div className="text-white/50">{child.child_sku ?? "-"} | {child.key_type ?? "-"}</div>
                        <div className="text-white/40">{Object.entries(child.variant_attributes ?? {}).map(([key, value]) => `${key}:${String(value)}`).join(" | ") || "-"}</div>
                      </div>
                    ))}
                    {detailQuery.data.current_children.length === 0 ? <div className="text-xs text-white/45">Brak children w tym view.</div> : null}
                  </div>
                </div>

                <div className="rounded-lg border border-white/10 p-3">
                  <div className="text-sm font-medium text-white">Issues</div>
                  <ul className="mt-2 space-y-1 text-xs text-white/55">
                    {detailQuery.data.issues.length > 0 ? detailQuery.data.issues.map((issue) => <li key={issue}>- {issue}</li>) : <li>- Brak wykrytych issue flags.</li>}
                  </ul>
                </div>
              </>
            ) : (
              <div className="text-sm text-white/50">Nie udalo sie zaladowac detalu rodziny.</div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
