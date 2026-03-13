import { useState } from "react";
import { Download, FileSpreadsheet, Loader2 } from "lucide-react";
import { api } from "@/lib/api";

type ExportFormat = "csv" | "xlsx";

interface ExportButtonProps {
  /** API GET endpoint, e.g. "/profit/v2/products/export.xlsx" */
  endpoint: string;
  /** Query params passed to the API */
  params?: Record<string, unknown>;
  /** Filename without extension */
  filename?: string;
  /** Which formats to offer. Default: ["csv"] */
  formats?: ExportFormat[];
  /** Optional extra label */
  label?: string;
}

async function downloadBlob(endpoint: string, params: Record<string, unknown>, filename: string) {
  const resp = await api.get(endpoint, { params, responseType: "blob" });
  const url = window.URL.createObjectURL(new Blob([resp.data]));
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  window.URL.revokeObjectURL(url);
}

function buildClientCsv(rows: object[], columns?: string[]): Blob {
  if (!rows.length) return new Blob([""], { type: "text/csv" });
  const records = rows as Record<string, unknown>[];
  const cols = columns ?? Object.keys(records[0]);
  const header = cols.join(",");
  const body = records.map((r) =>
    cols.map((c) => {
      const v = r[c];
      if (v == null) return "";
      const s = String(v);
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    }).join(","),
  ).join("\n");
  return new Blob([`${header}\n${body}`], { type: "text/csv;charset=utf-8;" });
}

export function clientCsvDownload(rows: object[], filename: string, columns?: string[]) {
  const blob = buildClientCsv(rows, columns);
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename.endsWith(".csv") ? filename : `${filename}.csv`;
  a.click();
  window.URL.revokeObjectURL(url);
}

/** Server-side export button — calls API endpoint that returns blob */
export function ExportButton({ endpoint, params = {}, filename = "export", formats = ["csv"], label }: ExportButtonProps) {
  const [loading, setLoading] = useState(false);

  async function handleExport(fmt: ExportFormat) {
    setLoading(true);
    try {
      const ext = fmt === "xlsx" ? ".xlsx" : ".csv";
      const ep = endpoint.includes(".") ? endpoint : `${endpoint}${ext}`;
      await downloadBlob(ep, params, `${filename}${ext}`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="inline-flex items-center gap-1">
      {formats.map((fmt) => (
        <button
          key={fmt}
          disabled={loading}
          onClick={() => handleExport(fmt)}
          className="inline-flex items-center gap-1.5 rounded-md border border-white/20 bg-white/5 px-2.5 py-1.5 text-xs text-white/70 hover:bg-white/10 disabled:opacity-50 transition-colors"
        >
          {loading ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : fmt === "xlsx" ? (
            <FileSpreadsheet className="h-3.5 w-3.5" />
          ) : (
            <Download className="h-3.5 w-3.5" />
          )}
          {label ?? `Export ${fmt.toUpperCase()}`}
        </button>
      ))}
    </div>
  );
}

/** Client-side export button — uses data already in memory */
export function ClientExportButton({
  data,
  filename = "export",
  columns,
  label,
}: {
  data: object[];
  filename?: string;
  columns?: string[];
  label?: string;
}) {
  return (
    <button
      disabled={!data.length}
      onClick={() => clientCsvDownload(data, filename, columns)}
      className="inline-flex items-center gap-1.5 rounded-md border border-white/20 bg-white/5 px-2.5 py-1.5 text-xs text-white/70 hover:bg-white/10 disabled:opacity-50 transition-colors"
    >
      <Download className="h-3.5 w-3.5" />
      {label ?? "Export CSV"}
    </button>
  );
}
