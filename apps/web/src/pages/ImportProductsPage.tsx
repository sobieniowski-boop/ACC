import { useState, useRef, useCallback } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getImportProducts,
  getImportProductsSummary,
  getImportFilterOptions,
  uploadImportProducts,
  type ImportProductItem,
  type ImportUploadResult,
} from "@/lib/api";
import { formatPLN, cn } from "@/lib/utils";
import {
  Upload,
  FileSpreadsheet,
  Package,
  TrendingUp,
  Warehouse,
  Search,
  CheckCircle,
  XCircle,
  Loader2,
  ShoppingCart,
  DollarSign,
  BarChart3,
  Filter,
  ArrowUpDown,
  ChevronDown,
} from "lucide-react";
import { ClientExportButton, ServerPagination } from "@/components/shared";

/* --------------- Upload Drop Zone --------------- */
function UploadZone({
  onUpload,
  isUploading,
}: {
  onUpload: (file: File) => void;
  isUploading: boolean;
}) {
  const [dragActive, setDragActive] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleDrag = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") setDragActive(true);
    else if (e.type === "dragleave") setDragActive(false);
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setDragActive(false);
      if (e.dataTransfer.files?.[0]) onUpload(e.dataTransfer.files[0]);
    },
    [onUpload]
  );

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      if (e.target.files?.[0]) onUpload(e.target.files[0]);
    },
    [onUpload]
  );

  return (
    <div
      onDragEnter={handleDrag}
      onDragLeave={handleDrag}
      onDragOver={handleDrag}
      onDrop={handleDrop}
      onClick={() => inputRef.current?.click()}
      className={cn(
        "relative flex cursor-pointer flex-col items-center justify-center rounded-xl border-2 border-dashed p-6 transition-colors",
        dragActive
          ? "border-blue-500 bg-blue-500/10"
          : "border-border hover:border-muted-foreground/50 hover:bg-muted/20",
        isUploading && "pointer-events-none opacity-60"
      )}
    >
      <input
        ref={inputRef}
        type="file"
        accept=".xlsx,.xls"
        onChange={handleChange}
        className="hidden"
      />
      {isUploading ? (
        <>
          <Loader2 className="h-8 w-8 animate-spin text-blue-400" />
          <p className="mt-2 text-sm font-medium text-blue-400">
            Przetwarzanie pliku...
          </p>
        </>
      ) : (
        <>
          <Upload className="h-8 w-8 text-muted-foreground" />
          <p className="mt-2 text-sm font-medium text-foreground">
            Przeciągnij plik Excel lub kliknij
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            Akceptowane: .xlsx (plik importowy CEO z N:\Analityka)
          </p>
        </>
      )}
    </div>
  );
}

/* --------------- Stats Card --------------- */
function StatCard({
  icon: Icon,
  label,
  value,
  sub,
  accent,
}: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  value: string | number;
  sub?: string;
  accent?: string;
}) {
  return (
    <div className="rounded-xl border border-border bg-card p-3">
      <div className="flex items-center gap-1.5 text-muted-foreground">
        <Icon className={cn("h-3.5 w-3.5", accent)} />
        <span className="text-[10px] font-medium uppercase tracking-wider">
          {label}
        </span>
      </div>
      <div className="mt-1.5 text-xl font-bold text-foreground">{value}</div>
      {sub && (
        <div className="mt-0.5 text-[10px] text-muted-foreground">{sub}</div>
      )}
    </div>
  );
}

/* --------------- Filter Select --------------- */
function FilterSelect({
  label,
  value,
  onChange,
  options,
  className,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  options: { value: string; label: string }[];
  className?: string;
}) {
  return (
    <div className={cn("relative", className)}>
      <label className="mb-1 block text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      <div className="relative">
        <select
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="w-full appearance-none rounded-lg border border-border bg-background py-1.5 pl-3 pr-8 text-xs text-foreground focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        >
          {options.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
      </div>
    </div>
  );
}

/* --------------- Sort Options --------------- */
const SORT_OPTIONS = [
  { value: "sku", label: "SKU" },
  { value: "nazwa_pelna", label: "Nazwa" },
  { value: "zasieg_dni", label: "Zasięg" },
  { value: "stan_magazynowy", label: "Stan mag." },
  { value: "cena_zakupu", label: "Cena zakupu" },
  { value: "marza", label: "Marża (holding)" },
  { value: "amz_revenue_pln_30d", label: "Amz przychód" },
  { value: "amz_units_30d", label: "Amz szt." },
  { value: "amz_cm1_pln_30d", label: "Amz margin PLN" },
  { value: "amz_cm1_pct_30d", label: "Amz margin %" },
  { value: "amz_cogs_coverage_pct", label: "Amz COGS cov." },
];

/* --------------- Main Page --------------- */
export default function ImportProductsPage() {
  const [page, setPage] = useState(1);
  const [skuSearch, setSkuSearch] = useState("");
  const [aktywny, setAktywny] = useState("");
  const [kodImportu, setKodImportu] = useState("");
  const [hasAmzSales, setHasAmzSales] = useState("");
  const [sortBy, setSortBy] = useState("amz_revenue_pln_30d");
  const [sortDir, setSortDir] = useState("desc");
  const [showUpload, setShowUpload] = useState(false);
  const [uploadResult, setUploadResult] = useState<ImportUploadResult | null>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const pageSize = 50;
  const queryClient = useQueryClient();

  const { data: summary } = useQuery({
    queryKey: ["import-products-summary"],
    queryFn: getImportProductsSummary,
    staleTime: 60_000,
  });

  const { data: filterOpts } = useQuery({
    queryKey: ["import-filter-options"],
    queryFn: getImportFilterOptions,
    staleTime: 300_000,
  });

  const { data, isLoading } = useQuery({
    queryKey: [
      "import-products",
      page,
      skuSearch,
      aktywny,
      kodImportu,
      hasAmzSales,
      sortBy,
      sortDir,
    ],
    queryFn: () =>
      getImportProducts({
        page,
        page_size: pageSize,
        ...(skuSearch ? { sku_search: skuSearch } : {}),
        ...(aktywny ? { aktywny: aktywny === "true" } : {}),
        ...(kodImportu ? { kod_importu: kodImportu } : {}),
        ...(hasAmzSales
          ? { has_amazon_sales: hasAmzSales === "true" }
          : {}),
        sort_by: sortBy,
        sort_dir: sortDir,
      }),
  });

  const uploadMut = useMutation({
    mutationFn: uploadImportProducts,
    onSuccess: (result) => {
      setUploadResult(result);
      setUploadError(null);
      queryClient.invalidateQueries({ queryKey: ["import-products"] });
      queryClient.invalidateQueries({ queryKey: ["import-products-summary"] });
      queryClient.invalidateQueries({ queryKey: ["import-skus"] });
      queryClient.invalidateQueries({ queryKey: ["import-filter-options"] });
    },
    onError: (err: unknown) => {
      const msg =
        err && typeof err === "object" && "response" in err
          ? (err as { response?: { data?: { detail?: string } } }).response
              ?.data?.detail ?? "Upload failed"
          : "Upload failed";
      setUploadError(msg);
      setUploadResult(null);
    },
  });

  const toggleSort = (col: string) => {
    if (sortBy === col) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(col);
      setSortDir("desc");
    }
    setPage(1);
  };

  const SortHeader = ({
    col,
    children,
    className,
  }: {
    col: string;
    children: React.ReactNode;
    className?: string;
  }) => (
    <th
      className={cn(
        "px-2 py-2 cursor-pointer hover:text-foreground transition-colors select-none",
        className
      )}
      onClick={() => toggleSort(col)}
    >
      <span className="inline-flex items-center gap-0.5">
        {children}
        {sortBy === col && (
          <ArrowUpDown
            className={cn(
              "h-3 w-3",
              sortDir === "asc" ? "rotate-180" : ""
            )}
          />
        )}
      </span>
    </th>
  );

  return (
    <div className="mx-auto max-w-[1600px] space-y-4 p-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-foreground">
            <FileSpreadsheet className="mr-2 inline-block h-5 w-5 text-blue-400" />
            Produkty Importowe
          </h1>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Dane holdingowe (Excel CEO) + metryki Amazon (30 dni)
          </p>
        </div>
        <div className="flex items-center gap-2">
          <ClientExportButton data={data?.items ?? []} filename="import_products" />
          <button
            onClick={() => setShowUpload((v) => !v)}
            className="flex items-center gap-2 rounded-lg border border-border bg-card px-3 py-1.5 text-xs font-medium text-foreground hover:bg-muted/40 transition-colors"
          >
            <Upload className="h-3.5 w-3.5" />
            Upload Excel
          </button>
        </div>
      </div>

      {/* Upload Section (collapsible) */}
      {showUpload && (
        <div className="rounded-xl border border-border bg-card p-4">
          <UploadZone
            onUpload={(file) => uploadMut.mutate(file)}
            isUploading={uploadMut.isPending}
          />
          {uploadResult && (
            <div className="mt-3 flex items-start gap-2 rounded-lg border border-green-500/30 bg-green-500/10 p-3">
              <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-green-400" />
              <div>
                <p className="text-xs font-medium text-green-400">
                  {uploadResult.message}
                </p>
                <p className="mt-0.5 text-[10px] text-green-400/70">
                  Plik: {uploadResult.filename} | Nowych:{" "}
                  {uploadResult.inserted} | Zaktualizowanych:{" "}
                  {uploadResult.updated}
                </p>
              </div>
            </div>
          )}
          {uploadError && (
            <div className="mt-3 flex items-start gap-2 rounded-lg border border-red-500/30 bg-red-500/10 p-3">
              <XCircle className="mt-0.5 h-4 w-4 shrink-0 text-red-400" />
              <p className="text-xs font-medium text-red-400">{uploadError}</p>
            </div>
          )}
        </div>
      )}

      {/* Summary Cards — two rows: Holding + Amazon */}
      {summary && (
        <div className="space-y-2">
          {/* Holding row */}
          <div>
            <p className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              <Warehouse className="mr-1 inline h-3 w-3" />
              Holding (Excel CEO)
            </p>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4 lg:grid-cols-6">
              <StatCard
                icon={Package}
                label="Produkty"
                value={summary.total_products}
                sub={`${summary.active_count} aktywnych`}
              />
              <StatCard
                icon={Warehouse}
                label="Stan mag."
                value={(summary.holding_total_stock ?? 0).toLocaleString()}
              />
              <StatCard
                icon={DollarSign}
                label="Wartość mag."
                value={formatPLN(summary.holding_stock_value ?? 0)}
              />
              <StatCard
                icon={TrendingUp}
                label="Śr. marża"
                value={`${(summary.holding_avg_margin ?? 0).toFixed(1)}%`}
              />
              <StatCard
                icon={ShoppingCart}
                label="Sprz. 30d (holding)"
                value={(summary.holding_sales_30d ?? 0).toLocaleString()}
                sub="szt. holding"
              />
              <StatCard
                icon={Package}
                label="Aktywne"
                value={summary.active_count}
                sub={`z ${summary.total_products}`}
              />
            </div>
          </div>
          {/* Amazon row */}
          <div>
            <p className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-orange-400/80">
              <ShoppingCart className="mr-1 inline h-3 w-3" />
              Amazon (nasze dane, 30 dni)
            </p>
            <p className="mb-2 text-[11px] text-muted-foreground">
              Margin tutaj oznacza revenue - COGS - fees i nie obejmuje logistyki.
            </p>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4 lg:grid-cols-6">
              <StatCard
                icon={ShoppingCart}
                label="Szt. sprzedanych"
                value={(summary.amz_units_30d ?? 0).toLocaleString()}
                sub={`${(summary.amz_orders_30d ?? 0).toLocaleString()} zamówień`}
                accent="text-orange-400"
              />
              <StatCard
                icon={DollarSign}
                label="Przychód"
                value={formatPLN(summary.amz_revenue_30d ?? 0)}
                accent="text-orange-400"
              />
              <StatCard
                icon={DollarSign}
                label="COGS"
                value={formatPLN(summary.amz_cogs_30d ?? 0)}
                accent="text-orange-400"
              />
              <StatCard
                icon={DollarSign}
                label="Opłaty AMZ"
                value={formatPLN(summary.amz_fees_30d ?? 0)}
                accent="text-orange-400"
              />
              <StatCard
                icon={BarChart3}
                label="Margin"
                value={formatPLN(summary.amz_cm1_30d ?? 0)}
                sub={`${(summary.amz_cm1_pct_30d ?? 0).toFixed(1)}%`}
                accent="text-orange-400"
              />
              <StatCard
                icon={BarChart3}
                label="Produkty ze sprz."
                value={summary.amz_products_with_sales ?? 0}
                sub={`z ${summary.total_products}`}
                accent="text-orange-400"
              />
            </div>
          </div>
        </div>
      )}

      {/* Filters + Table */}
      <div className="rounded-xl border border-border bg-card">
        {/* Filter bar */}
        <div className="flex flex-wrap items-end gap-3 border-b border-border p-3">
          {/* Search */}
          <div className="min-w-[200px] flex-1">
            <label className="mb-1 block text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
              Szukaj
            </label>
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
              <input
                type="text"
                placeholder="SKU lub nazwa..."
                value={skuSearch}
                onChange={(e) => {
                  setSkuSearch(e.target.value);
                  setPage(1);
                }}
                className="w-full rounded-lg border border-border bg-background py-1.5 pl-8 pr-3 text-xs text-foreground placeholder:text-muted-foreground focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
            </div>
          </div>
          {/* Kod importu */}
          <FilterSelect
            label="Kod importu"
            value={kodImportu}
            onChange={(v) => {
              setKodImportu(v);
              setPage(1);
            }}
            options={[
              { value: "", label: "Wszystkie" },
              ...(filterOpts?.kod_importu?.map((k) => ({
                value: k,
                label: k,
              })) ?? []),
            ]}
            className="w-[140px]"
          />
          {/* Aktywny */}
          <FilterSelect
            label="Aktywny"
            value={aktywny}
            onChange={(v) => {
              setAktywny(v);
              setPage(1);
            }}
            options={[
              { value: "", label: "Wszystkie" },
              { value: "true", label: "Tak" },
              { value: "false", label: "Nie" },
            ]}
            className="w-[100px]"
          />
          {/* Has Amazon sales */}
          <FilterSelect
            label="Sprz. Amazon"
            value={hasAmzSales}
            onChange={(v) => {
              setHasAmzSales(v);
              setPage(1);
            }}
            options={[
              { value: "", label: "Wszystkie" },
              { value: "true", label: "Ze sprzedażą" },
              { value: "false", label: "Bez sprzedaży" },
            ]}
            className="w-[140px]"
          />
          {/* Sort */}
          <FilterSelect
            label="Sortuj po"
            value={sortBy}
            onChange={(v) => {
              setSortBy(v);
              setPage(1);
            }}
            options={SORT_OPTIONS}
            className="w-[140px]"
          />
          <div className="w-[80px]">
            <label className="mb-1 block text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
              Kierunek
            </label>
            <button
              onClick={() => {
                setSortDir((d) => (d === "asc" ? "desc" : "asc"));
                setPage(1);
              }}
              className="flex w-full items-center justify-center gap-1 rounded-lg border border-border bg-background py-1.5 text-xs text-foreground hover:bg-muted/40"
            >
              <ArrowUpDown className="h-3 w-3" />
              {sortDir === "asc" ? "Rosnąco" : "Malejąco"}
            </button>
          </div>
          {/* Filter indicator */}
          {(skuSearch || aktywny || kodImportu || hasAmzSales) && (
            <button
              onClick={() => {
                setSkuSearch("");
                setAktywny("");
                setKodImportu("");
                setHasAmzSales("");
                setPage(1);
              }}
              className="flex items-center gap-1 rounded-lg border border-yellow-500/30 bg-yellow-500/10 px-2.5 py-1.5 text-[10px] font-medium text-yellow-400 hover:bg-yellow-500/20 transition-colors"
            >
              <Filter className="h-3 w-3" />
              Wyczyść filtry
            </button>
          )}
        </div>

        {/* Table */}
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              {/* Column group headers */}
              <tr className="border-b border-border">
                <th colSpan={5} className="px-2 py-1 bg-muted/20 font-semibold">
                  Produkt
                </th>
                <th
                  colSpan={4}
                  className="px-2 py-1 bg-muted/10 font-semibold text-muted-foreground"
                >
                  Holding (Excel)
                </th>
                <th
                  colSpan={7}
                  className="px-2 py-1 bg-orange-500/5 font-semibold text-orange-400/80"
                >
                  Amazon (30d)
                </th>
              </tr>
              {/* Actual column headers */}
              <tr className="bg-muted/30">
                <SortHeader
                  col="sku"
                  className="sticky left-0 bg-muted/30 z-10"
                >
                  SKU
                </SortHeader>
                <th className="px-2 py-2">Nazwa</th>
                <th className="px-2 py-2">Kod imp.</th>
                <th className="px-2 py-2 text-center">Akt.</th>
                <SortHeader col="zasieg_dni" className="text-right">
                  Zasięg
                </SortHeader>
                {/* Holding */}
                <SortHeader col="stan_magazynowy" className="text-right">
                  Stan
                </SortHeader>
                <SortHeader col="cena_zakupu" className="text-right">
                  Cena zak.
                </SortHeader>
                <SortHeader col="marza" className="text-right">
                  Marża
                </SortHeader>
                <SortHeader col="sprzedaz_30d" className="text-right">
                  Sprz.30d
                </SortHeader>
                {/* Amazon */}
                <SortHeader col="amz_units_30d" className="text-right">
                  Szt.
                </SortHeader>
                <SortHeader col="amz_revenue_pln_30d" className="text-right">
                  Przychód
                </SortHeader>
                <SortHeader col="amz_cogs_pln_30d" className="text-right">
                  COGS
                </SortHeader>
                <SortHeader col="amz_cm1_pln_30d" className="text-right">
                  Margin
                </SortHeader>
                <SortHeader col="amz_cm1_pct_30d" className="text-right">
                  Margin %
                </SortHeader>
                <SortHeader col="amz_avg_price_pln" className="text-right">
                  Śr. cena
                </SortHeader>
                <SortHeader
                  col="amz_cogs_coverage_pct"
                  className="text-right"
                >
                  COGS cov.
                </SortHeader>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {isLoading && (
                <tr>
                  <td
                    colSpan={16}
                    className="px-3 py-12 text-center text-muted-foreground"
                  >
                    <Loader2 className="mx-auto h-6 w-6 animate-spin" />
                  </td>
                </tr>
              )}
              {!isLoading && data?.items.length === 0 && (
                <tr>
                  <td
                    colSpan={16}
                    className="px-3 py-12 text-center text-muted-foreground"
                  >
                    {data?.total === 0
                      ? "Brak produktów — uploaduj plik Excel"
                      : "Brak wyników dla tych filtrów"}
                  </td>
                </tr>
              )}
              {data?.items.map((p: ImportProductItem) => {
                const hasAmz = (p.amz_units_30d ?? 0) > 0;
                return (
                  <tr
                    key={p.id}
                    className="hover:bg-muted/20 transition-colors"
                  >
                    {/* Product */}
                    <td className="px-2 py-1.5 sticky left-0 bg-card font-mono text-[11px] font-medium text-foreground whitespace-nowrap z-10">
                      {p.sku}
                    </td>
                    <td
                      className="px-2 py-1.5 text-muted-foreground max-w-[200px] truncate"
                      title={p.nazwa_pelna ?? ""}
                    >
                      {p.nazwa_pelna || "—"}
                    </td>
                    <td className="px-2 py-1.5 font-mono text-blue-400 text-[10px]">
                      {p.kod_importu || "—"}
                    </td>
                    <td className="px-2 py-1.5 text-center">
                      {p.aktywny ? (
                        <CheckCircle className="mx-auto h-3.5 w-3.5 text-green-400" />
                      ) : p.aktywny === false ? (
                        <XCircle className="mx-auto h-3.5 w-3.5 text-red-400" />
                      ) : (
                        <span className="text-muted-foreground">—</span>
                      )}
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums">
                      {p.zasieg_dni != null ? (
                        <span
                          className={cn(
                            "font-medium",
                            p.zasieg_dni < 14
                              ? "text-red-400"
                              : p.zasieg_dni < 30
                                ? "text-yellow-400"
                                : "text-green-400"
                          )}
                        >
                          {p.zasieg_dni}d
                        </span>
                      ) : (
                        "—"
                      )}
                    </td>
                    {/* Holding */}
                    <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
                      {p.stan_magazynowy?.toLocaleString() ?? "—"}
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
                      {p.cena_zakupu != null
                        ? formatPLN(p.cena_zakupu)
                        : "—"}
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums">
                      {p.marza != null ? (
                        <span
                          className={cn(
                            p.marza >= 20
                              ? "text-green-400"
                              : p.marza >= 0
                                ? "text-yellow-400"
                                : "text-red-400"
                          )}
                        >
                          {p.marza.toFixed(1)}%
                        </span>
                      ) : (
                        "—"
                      )}
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
                      {p.sprzedaz_30d?.toLocaleString() ?? "—"}
                    </td>
                    {/* Amazon */}
                    <td
                      className={cn(
                        "px-2 py-1.5 text-right tabular-nums",
                        hasAmz ? "text-orange-300 font-medium" : "text-muted-foreground/50"
                      )}
                    >
                      {hasAmz ? (p.amz_units_30d ?? 0).toLocaleString() : "—"}
                    </td>
                    <td
                      className={cn(
                        "px-2 py-1.5 text-right tabular-nums",
                        hasAmz ? "text-foreground" : "text-muted-foreground/50"
                      )}
                    >
                      {hasAmz
                        ? formatPLN(p.amz_revenue_pln_30d ?? 0)
                        : "—"}
                    </td>
                    <td
                      className={cn(
                        "px-2 py-1.5 text-right tabular-nums",
                        hasAmz ? "text-muted-foreground" : "text-muted-foreground/50"
                      )}
                    >
                      {hasAmz
                        ? formatPLN(p.amz_cogs_pln_30d ?? 0)
                        : "—"}
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums">
                      {hasAmz ? (
                        <span
                          className={cn(
                            "font-medium",
                            (p.amz_cm1_pln_30d ?? 0) > 0
                              ? "text-green-400"
                              : (p.amz_cm1_pln_30d ?? 0) < 0
                                ? "text-red-400"
                                : "text-muted-foreground"
                          )}
                        >
                          {formatPLN(p.amz_cm1_pln_30d ?? 0)}
                        </span>
                      ) : (
                        <span className="text-muted-foreground/50">—</span>
                      )}
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums">
                      {hasAmz ? (
                        <span
                          className={cn(
                            (p.amz_cm1_pct_30d ?? 0) >= 15
                              ? "text-green-400"
                              : (p.amz_cm1_pct_30d ?? 0) >= 0
                                ? "text-yellow-400"
                                : "text-red-400"
                          )}
                        >
                          {(p.amz_cm1_pct_30d ?? 0).toFixed(1)}%
                        </span>
                      ) : (
                        <span className="text-muted-foreground/50">—</span>
                      )}
                    </td>
                    <td
                      className={cn(
                        "px-2 py-1.5 text-right tabular-nums",
                        hasAmz ? "text-foreground" : "text-muted-foreground/50"
                      )}
                    >
                      {hasAmz
                        ? formatPLN(p.amz_avg_price_pln ?? 0)
                        : "—"}
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums">
                      {hasAmz ? (
                        <span
                          className={cn(
                            "text-[10px]",
                            (p.amz_cogs_coverage_pct ?? 0) >= 80
                              ? "text-green-400"
                              : (p.amz_cogs_coverage_pct ?? 0) >= 50
                                ? "text-yellow-400"
                                : "text-red-400"
                          )}
                        >
                          {(p.amz_cogs_coverage_pct ?? 0).toFixed(0)}%
                        </span>
                      ) : (
                        <span className="text-muted-foreground/50">—</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {data && data.pages > 1 && (
          <div className="px-4 py-2.5 border-t border-border">
            <ServerPagination page={page} pages={data.pages} total={data.total} pageSize={pageSize} onPageChange={setPage} />
          </div>
        )}
      </div>
    </div>
  );
}
