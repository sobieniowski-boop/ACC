import { useState } from "react";
import { Settings2, X } from "lucide-react";
import { cn } from "@/lib/utils";

export interface ColumnDef<K extends string = string> {
  key: K;
  label: string;
  defaultVisible?: boolean;
}

interface ColumnChooserProps<K extends string> {
  columns: ColumnDef<K>[];
  visible: K[];
  onChange: (visible: K[]) => void;
}

export function useColumnVisibility<K extends string>(columns: ColumnDef<K>[]) {
  const [visible, setVisible] = useState<K[]>(
    columns.filter((c) => c.defaultVisible !== false).map((c) => c.key),
  );
  const toggle = (key: K) =>
    setVisible((prev) => (prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]));
  const resetCols = () =>
    setVisible(columns.filter((c) => c.defaultVisible !== false).map((c) => c.key));
  const isVisible = (key: K) => visible.includes(key);
  return { visible, setVisible, toggle, resetCols, isVisible };
}

export function ColumnChooser<K extends string>({ columns, visible, onChange }: ColumnChooserProps<K>) {
  const [open, setOpen] = useState(false);

  const toggle = (key: K) => {
    onChange(visible.includes(key) ? visible.filter((k) => k !== key) : [...visible, key]);
  };

  const selectAll = () => onChange(columns.map((c) => c.key));
  const reset = () => onChange(columns.filter((c) => c.defaultVisible !== false).map((c) => c.key));

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="inline-flex items-center gap-1.5 rounded-md border border-white/20 bg-white/5 px-2.5 py-1.5 text-xs text-white/70 hover:bg-white/10 transition-colors"
      >
        <Settings2 className="h-3.5 w-3.5" />
        Kolumny ({visible.length}/{columns.length})
      </button>
    );
  }

  return (
    <div className="rounded-lg border border-border bg-card p-3 space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
          Widoczne kolumny
        </span>
        <div className="flex items-center gap-2">
          <button onClick={selectAll} className="text-[10px] text-[#FF9900] hover:underline">
            Wszystkie
          </button>
          <button onClick={reset} className="text-[10px] text-white/50 hover:underline">
            Reset
          </button>
          <button onClick={() => setOpen(false)} className="text-white/40 hover:text-white">
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>
      <div className="grid gap-1.5 sm:grid-cols-2 lg:grid-cols-4">
        {columns.map((col) => (
          <label
            key={col.key}
            className={cn(
              "inline-flex items-center gap-2 rounded px-2 py-1 text-xs cursor-pointer transition-colors",
              visible.includes(col.key) ? "text-white bg-white/5" : "text-white/40",
            )}
          >
            <input
              type="checkbox"
              checked={visible.includes(col.key)}
              onChange={() => toggle(col.key)}
              className="h-3 w-3 rounded border-white/30 accent-[#FF9900]"
            />
            {col.label}
          </label>
        ))}
      </div>
    </div>
  );
}
