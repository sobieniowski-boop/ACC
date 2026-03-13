import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Package, TruckIcon, RefreshCw } from "lucide-react";
import { getInventory, getReorderSuggestions, getOpenPOs } from "@/lib/api";
import { formatPLN } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";

type Tab = "stock" | "reorder" | "pos";

function doiColor(doi?: number | null) {
  if (!doi) return "text-white/40";
  if (doi < 7) return "text-red-400 font-bold";
  if (doi < 14) return "text-amber-400 font-semibold";
  if (doi > 90) return "text-blue-400";
  return "text-emerald-400";
}

function statusBadge(status: string) {
  switch (status) {
    case "critical": return <Badge variant="destructive">Krytyczny</Badge>;
    case "low":      return <Badge variant="warning">Niski</Badge>;
    case "overstock":return <Badge variant="secondary">Nadmiar</Badge>;
    default:         return <Badge variant="success">OK</Badge>;
  }
}

function urgencyBadge(urgency: string) {
  switch (urgency) {
    case "critical": return <Badge variant="destructive">Pilne</Badge>;
    case "high":     return <Badge variant="warning">Wysoki</Badge>;
    case "medium":   return <Badge variant="secondary">Średni</Badge>;
    default:         return <Badge variant="outline">Niski</Badge>;
  }
}

export default function Inventory() {
  const [tab, setTab] = useState<Tab>("stock");
  const [skuFilter, setSkuFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [page, setPage] = useState(1);
  const PAGE_SIZE = 50;

  const { data: invData, isLoading } = useQuery({
    queryKey: ["inventory", skuFilter, statusFilter, page],
    queryFn: () =>
      getInventory({
        ...(skuFilter ? { sku: skuFilter } : {}),
        ...(statusFilter !== "all" ? { status: statusFilter } : {}),
        page,
        page_size: PAGE_SIZE,
      }),
    staleTime: 60_000,
    enabled: tab === "stock",
  });

  const { data: suggestions, isLoading: loadingSuggestions } = useQuery({
    queryKey: ["reorder-suggestions"],
    queryFn: () => getReorderSuggestions(),
    staleTime: 120_000,
    enabled: tab === "reorder",
  });

  const { data: openPOs, isLoading: loadingPOs } = useQuery({
    queryKey: ["open-pos"],
    queryFn: () => getOpenPOs(),
    staleTime: 120_000,
    enabled: tab === "pos",
  });

  const summary = invData?.summary;
  const totalPages = Math.ceil((invData?.total ?? 0) / PAGE_SIZE);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Magazyn & Stany</h1>
        <p className="text-white/50 text-sm mt-1">
          FBA inventory, poziomy zapasów, otwarte zamówienia zakupu
        </p>
      </div>

      {/* Summary tiles */}
      {summary && (
        <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-white/50">Wszystkie SKU</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{summary.total_skus.toLocaleString()}</div>
            </CardContent>
          </Card>
          <Card className="border-red-500/30">
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-red-400 flex items-center gap-1">
                <AlertTriangle className="w-3 h-3" /> Krytyczny
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-red-400">{summary.critical_count}</div>
              <div className="text-xs text-white/30">DOI &lt; 7 dni</div>
            </CardContent>
          </Card>
          <Card className="border-amber-500/30">
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-amber-400">Niski</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-amber-400">{summary.low_count}</div>
              <div className="text-xs text-white/30">DOI 7–14 dni</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-white/50">Avg DOI</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{summary.avg_doi.toFixed(0)} dni</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-white/50">Wartość FBA</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-xl font-bold">{formatPLN(summary.total_value_pln)}</div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Tab bar */}
      <div className="flex gap-2">
        {[
          { id: "stock", label: "Stan Magazynowy", icon: Package },
          { id: "reorder", label: `Zamów (${suggestions?.length ?? "…"})`, icon: RefreshCw },
          { id: "pos", label: "Otwarte PO", icon: TruckIcon },
        ].map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => setTab(id as Tab)}
            className={`flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors ${
              tab === id
                ? "bg-[#FF9900] text-black"
                : "bg-white/10 text-white hover:bg-white/20"
            }`}
          >
            <Icon className="w-4 h-4" />
            {label}
          </button>
        ))}
      </div>

      {/* Stock tab */}
      {tab === "stock" && (
        <Card>
          <CardHeader className="flex flex-row items-center justify-between gap-4 flex-wrap">
            <CardTitle className="text-sm">Stan FBA</CardTitle>
            <div className="flex items-center gap-2">
              <Input
                placeholder="Filtruj SKU…"
                value={skuFilter}
                onChange={(e) => { setSkuFilter(e.target.value); setPage(1); }}
                className="w-40"
              />
              <Select value={statusFilter} onValueChange={(v) => { setStatusFilter(v); setPage(1); }}>
                <SelectTrigger className="w-36">
                  <SelectValue placeholder="Status" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Wszystkie</SelectItem>
                  <SelectItem value="critical">Krytyczny</SelectItem>
                  <SelectItem value="low">Niski</SelectItem>
                  <SelectItem value="overstock">Nadmiar</SelectItem>
                  <SelectItem value="ok">OK</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </CardHeader>
          <CardContent className="p-0">
            {isLoading ? (
              <div className="p-6 space-y-2">
                {Array.from({ length: 8 }).map((_, i) => <Skeleton key={i} className="h-8 w-full" />)}
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>SKU</TableHead>
                    <TableHead>Marketplace</TableHead>
                    <TableHead className="text-right">Dostępne</TableHead>
                    <TableHead className="text-right">Inbound</TableHead>
                    <TableHead className="text-right">Razem</TableHead>
                    <TableHead className="text-right">DOI</TableHead>
                    <TableHead className="text-right">Vel./dzień</TableHead>
                    <TableHead className="text-right">Wartość</TableHead>
                    <TableHead>Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {invData?.items.map((item) => (
                    <TableRow key={item.id}>
                      <TableCell className="font-mono text-xs">{item.sku}</TableCell>
                      <TableCell>
                        <Badge variant="outline">{item.marketplace_code}</Badge>
                      </TableCell>
                      <TableCell className="text-right">{item.qty_fulfillable.toLocaleString()}</TableCell>
                      <TableCell className="text-right text-white/60">{item.qty_inbound.toLocaleString()}</TableCell>
                      <TableCell className="text-right font-medium">{item.qty_total.toLocaleString()}</TableCell>
                      <TableCell className={`text-right ${doiColor(item.days_of_inventory)}`}>
                        {item.days_of_inventory ?? "—"}
                      </TableCell>
                      <TableCell className="text-right text-white/60">
                        {item.velocity_30d?.toFixed(1) ?? "—"}
                      </TableCell>
                      <TableCell className="text-right">
                        {item.inventory_value_pln ? formatPLN(item.inventory_value_pln) : "—"}
                      </TableCell>
                      <TableCell>{statusBadge(item.status)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
            {totalPages > 1 && (
              <div className="flex items-center justify-between p-4 border-t border-white/10">
                <span className="text-xs text-white/40">
                  Strona {page} z {totalPages}
                </span>
                <div className="flex gap-2">
                  <Button size="sm" variant="outline" disabled={page <= 1} onClick={() => setPage(p => p - 1)}>
                    Poprzednia
                  </Button>
                  <Button size="sm" variant="outline" disabled={page >= totalPages} onClick={() => setPage(p => p + 1)}>
                    Następna
                  </Button>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Reorder suggestions tab */}
      {tab === "reorder" && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Sugestie Zamówień</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            {loadingSuggestions ? (
              <div className="p-6 space-y-2">
                {Array.from({ length: 5 }).map((_, i) => <Skeleton key={i} className="h-10 w-full" />)}
              </div>
            ) : suggestions && suggestions.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>SKU</TableHead>
                    <TableHead>Pilność</TableHead>
                    <TableHead className="text-right">Obecny DOI</TableHead>
                    <TableHead className="text-right">Vel./dzień</TableHead>
                    <TableHead className="text-right">Sugerowana ilość</TableHead>
                    <TableHead>Zamów do</TableHead>
                    <TableHead>Powód</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {suggestions.map((s) => (
                    <TableRow key={s.sku}>
                      <TableCell className="font-mono text-xs">{s.sku}</TableCell>
                      <TableCell>{urgencyBadge(s.urgency)}</TableCell>
                      <TableCell className={`text-right ${doiColor(s.current_doi)}`}>
                        {s.current_doi}
                      </TableCell>
                      <TableCell className="text-right">{s.velocity_30d.toFixed(1)}</TableCell>
                      <TableCell className="text-right font-bold text-[#FF9900]">
                        {s.suggested_qty.toLocaleString()}
                      </TableCell>
                      <TableCell className="text-sm">{s.suggested_order_date}</TableCell>
                      <TableCell className="text-xs text-white/50 max-w-64 truncate">{s.reason}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <div className="p-12 text-center text-white/40">
                <RefreshCw className="w-10 h-10 mx-auto mb-3 opacity-30" />
                <p>Brak sugestii zamówień</p>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Open POs tab */}
      {tab === "pos" && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Otwarte Zamówienia Zakupu (NetfoxAnalityka)</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            {loadingPOs ? (
              <div className="p-6 space-y-2">
                {Array.from({ length: 5 }).map((_, i) => <Skeleton key={i} className="h-10 w-full" />)}
              </div>
            ) : openPOs && openPOs.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>SKU</TableHead>
                    <TableHead>Nazwa</TableHead>
                    <TableHead>Data zamówienia</TableHead>
                    <TableHead>Dostawa</TableHead>
                    <TableHead className="text-right">Zamówiono</TableHead>
                    <TableHead className="text-right">Odebrano</TableHead>
                    <TableHead className="text-right">Otwarte</TableHead>
                    <TableHead className="text-right">Dni do dostawy</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {openPOs.map((po, idx) => (
                    <TableRow key={`${po.sku}-${idx}`}>
                      <TableCell className="font-mono text-xs">{po.sku}</TableCell>
                      <TableCell className="text-xs text-white/60">{po.product_name ?? "—"}</TableCell>
                      <TableCell className="text-xs">{po.order_date ?? "—"}</TableCell>
                      <TableCell className="text-xs">{po.expected_delivery ?? "—"}</TableCell>
                      <TableCell className="text-right">{po.qty_ordered.toLocaleString()}</TableCell>
                      <TableCell className="text-right text-white/60">{po.qty_received.toLocaleString()}</TableCell>
                      <TableCell className="text-right font-medium text-[#FF9900]">
                        {po.qty_open.toLocaleString()}
                      </TableCell>
                      <TableCell className="text-right">
                        {po.days_until_delivery != null ? (
                          <span className={po.days_until_delivery < 7 ? "text-amber-400" : "text-white"}>
                            {po.days_until_delivery}
                          </span>
                        ) : "—"}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <div className="p-12 text-center text-white/40">
                <TruckIcon className="w-10 h-10 mx-auto mb-3 opacity-30" />
                <p>Brak otwartych PO lub MSSQL niedostępny</p>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
