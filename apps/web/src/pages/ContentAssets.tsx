import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { getContentAssets, linkContentAsset, uploadContentAsset } from "@/lib/api";
import { ClientExportButton } from "@/components/shared";

async function fileToBase64(file: File): Promise<string> {
  return await new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || "");
      const idx = result.indexOf(",");
      resolve(idx >= 0 ? result.slice(idx + 1) : result);
    };
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(file);
  });
}

export default function ContentAssetsPage() {
  const qc = useQueryClient();
  const [skuFilter, setSkuFilter] = useState("");
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [linkAssetId, setLinkAssetId] = useState("");
  const [linkSku, setLinkSku] = useState("");
  const [linkRole, setLinkRole] = useState<"main_image" | "manual" | "cert" | "aplus" | "lifestyle" | "infographic" | "other">("manual");

  const assetsQuery = useQuery({
    queryKey: ["content-assets", skuFilter],
    queryFn: () =>
      getContentAssets({
        page: 1,
        page_size: 100,
        ...(skuFilter.trim() ? { sku: skuFilter.trim() } : {}),
      }),
  });

  const uploadMutation = useMutation({
    mutationFn: async () => {
      if (!selectedFile) throw new Error("No file selected");
      const content_base64 = await fileToBase64(selectedFile);
      return uploadContentAsset({
        filename: selectedFile.name,
        mime: selectedFile.type || "application/octet-stream",
        content_base64,
        metadata_json: {},
      });
    },
    onSuccess: () => {
      setSelectedFile(null);
      qc.invalidateQueries({ queryKey: ["content-assets"] });
    },
  });

  const linkMutation = useMutation({
    mutationFn: () =>
      linkContentAsset(linkAssetId.trim(), {
        sku: linkSku.trim(),
        role: linkRole,
        status: "approved",
      }),
  });

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold">Asset Library</h1>
        <p className="text-sm text-muted-foreground">Upload, list and link assets to SKU</p>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Upload asset</h2>
        <input type="file" onChange={(e) => setSelectedFile(e.target.files?.[0] ?? null)} className="text-xs" />
        <button onClick={() => uploadMutation.mutate()} disabled={!selectedFile || uploadMutation.isPending} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">
          Upload
        </button>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <h2 className="text-sm font-semibold">Link asset to SKU</h2>
        <div className="grid gap-2 md:grid-cols-4">
          <input value={linkAssetId} onChange={(e) => setLinkAssetId(e.target.value)} placeholder="asset_id" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <input value={linkSku} onChange={(e) => setLinkSku(e.target.value)} placeholder="SKU" className="rounded border border-input bg-background px-2 py-1 text-xs" />
          <select value={linkRole} onChange={(e) => setLinkRole(e.target.value as typeof linkRole)} className="rounded border border-input bg-background px-2 py-1 text-xs">
            <option value="main_image">main_image</option>
            <option value="manual">manual</option>
            <option value="cert">cert</option>
            <option value="aplus">aplus</option>
            <option value="lifestyle">lifestyle</option>
            <option value="infographic">infographic</option>
            <option value="other">other</option>
          </select>
          <button onClick={() => linkMutation.mutate()} disabled={!linkAssetId.trim() || !linkSku.trim()} className="rounded border border-border px-2 py-1 text-xs disabled:opacity-40">Link</button>
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-4 space-y-2">
        <div className="flex gap-2">
          <input value={skuFilter} onChange={(e) => setSkuFilter(e.target.value)} placeholder="Filter by SKU" className="w-full rounded border border-input bg-background px-2 py-1 text-xs" />
          <button onClick={() => assetsQuery.refetch()} className="rounded border border-border px-2 py-1 text-xs">Refresh</button>
          <ClientExportButton data={assetsQuery.data?.items ?? []} filename="content_assets" />
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="border-b border-border text-left text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">ID</th>
                <th className="px-2 py-2">Filename</th>
                <th className="px-2 py-2">Mime</th>
                <th className="px-2 py-2">Status</th>
                <th className="px-2 py-2">Uploaded</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {(assetsQuery.data?.items ?? []).map((a) => (
                <tr key={a.id}>
                  <td className="px-2 py-2 font-mono">{a.id.slice(0, 8)}</td>
                  <td className="px-2 py-2">{a.filename}</td>
                  <td className="px-2 py-2">{a.mime}</td>
                  <td className="px-2 py-2">{a.status}</td>
                  <td className="px-2 py-2 text-muted-foreground">{a.uploaded_at.slice(0, 19).replace("T", " ")}</td>
                </tr>
              ))}
              {!assetsQuery.isLoading && (assetsQuery.data?.items ?? []).length === 0 && (
                <tr>
                  <td colSpan={5} className="px-2 py-6 text-center text-muted-foreground">No assets</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
