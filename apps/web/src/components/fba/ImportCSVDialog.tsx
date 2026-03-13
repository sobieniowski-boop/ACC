import { useRef, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { importFbaRegister, type FbaRegisterType } from "@/lib/api";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";

interface ImportCSVDialogProps {
  registerType: FbaRegisterType;
  quarter?: string;
  /** Query keys to invalidate after successful import */
  invalidateKeys: string[][];
  /** Label shown on the trigger button */
  buttonLabel?: string;
}

const REGISTER_META: Record<FbaRegisterType, { title: string; description: string; columns: string }> = {
  shipment_plan: {
    title: "Import Shipment Plans",
    description: "Upload a CSV/TSV with shipment plan rows. One row per plan entry.",
    columns: "quarter, marketplace_id, shipment_id, plan_week_start, planned_ship_date, planned_units, status, owner",
  },
  case: {
    title: "Import Cases",
    description: "Upload a CSV/TSV with case register rows.",
    columns: "case_type, marketplace_id, sku, detected_date, status, owner, root_cause, close_date",
  },
  launch: {
    title: "Import Launches",
    description: "Upload a CSV/TSV with product launch register rows.",
    columns: "quarter, launch_type, sku, marketplace_id, planned_go_live_date, actual_go_live_date, status, owner, vine_eligible, vine_submitted_at, live_stable_at, incident_free",
  },
  initiative: {
    title: "Import Initiatives",
    description: "Upload a CSV/TSV with quarterly initiative rows.",
    columns: "quarter, initiative_type, title, sku, status, owner, planned, approved, live_stable_at",
  },
};

export function ImportCSVDialog({ registerType, quarter, invalidateKeys, buttonLabel }: ImportCSVDialogProps) {
  const qc = useQueryClient();
  const [open, setOpen] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [result, setResult] = useState<{ imported: number; skipped: number; errors: string[] } | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  const meta = REGISTER_META[registerType];

  const importMut = useMutation({
    mutationFn: () => importFbaRegister(file!, registerType, quarter),
    onSuccess: (data) => {
      setResult(data);
      for (const key of invalidateKeys) {
        qc.invalidateQueries({ queryKey: key });
      }
    },
  });

  const reset = () => {
    setFile(null);
    setResult(null);
    importMut.reset();
    if (fileRef.current) fileRef.current.value = "";
  };

  const handleClose = (isOpen: boolean) => {
    if (!isOpen) reset();
    setOpen(isOpen);
  };

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm">
          {buttonLabel ?? "Import CSV"}
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{meta.title}</DialogTitle>
          <DialogDescription>{meta.description}</DialogDescription>
        </DialogHeader>

        {!result ? (
          <div className="space-y-4">
            <div className="rounded-lg border border-white/10 bg-white/5 p-3">
              <div className="text-xs uppercase tracking-[0.15em] text-white/40">Expected columns</div>
              <div className="mt-1 font-mono text-xs text-white/60">{meta.columns}</div>
            </div>

            <input
              ref={fileRef}
              type="file"
              accept=".csv,.tsv,.txt"
              onChange={(e) => setFile(e.target.files?.[0] ?? null)}
              className="w-full rounded border border-white/10 bg-white/5 px-3 py-2 text-sm file:mr-3 file:rounded file:border-0 file:bg-white/10 file:px-3 file:py-1 file:text-sm file:text-white"
            />

            {importMut.isError && (
              <div className="rounded border border-red-500/20 bg-red-500/5 p-3 text-sm text-red-300">
                {(importMut.error as Error)?.message ?? "Import failed"}
              </div>
            )}
          </div>
        ) : (
          <div className="space-y-3">
            <div className="grid grid-cols-2 gap-3">
              <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/5 p-3 text-center">
                <div className="text-2xl font-bold text-emerald-300">{result.imported}</div>
                <div className="text-xs text-white/50">Imported</div>
              </div>
              <div className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-3 text-center">
                <div className="text-2xl font-bold text-amber-300">{result.skipped}</div>
                <div className="text-xs text-white/50">Skipped</div>
              </div>
            </div>
            {result.errors.length > 0 && (
              <div className="max-h-40 overflow-auto rounded border border-red-500/20 bg-red-500/5 p-3 text-xs text-red-300">
                {result.errors.map((err, i) => (
                  <div key={i}>{err}</div>
                ))}
              </div>
            )}
          </div>
        )}

        <DialogFooter>
          {!result ? (
            <Button
              onClick={() => importMut.mutate()}
              disabled={!file || importMut.isPending}
            >
              {importMut.isPending ? "Importing..." : "Upload & Import"}
            </Button>
          ) : (
            <Button variant="outline" onClick={() => handleClose(false)}>
              Close
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
