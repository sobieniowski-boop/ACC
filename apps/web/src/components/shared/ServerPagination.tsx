import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight } from "lucide-react";
import { useState } from "react";

interface ServerPaginationProps {
  page: number;
  pages: number;
  total: number;
  pageSize: number;
  onPageChange: (page: number) => void;
}

export function ServerPagination({ page, pages, total, pageSize, onPageChange }: ServerPaginationProps) {
  const [jumpInput, setJumpInput] = useState("");

  const safeGo = (p: number) => {
    const target = Math.max(1, Math.min(p, pages));
    if (target !== page) onPageChange(target);
  };

  const applyJump = () => {
    const p = parseInt(jumpInput, 10);
    if (!isNaN(p)) safeGo(p);
    setJumpInput("");
  };

  if (total === 0) return null;

  const from = (page - 1) * pageSize + 1;
  const to = Math.min(page * pageSize, total);

  return (
    <div className="flex items-center justify-between border-t border-border px-4 py-2.5">
      <span className="text-[10px] text-muted-foreground tabular-nums">
        {from.toLocaleString()}–{to.toLocaleString()} z {total.toLocaleString()}
      </span>
      <div className="flex items-center gap-1">
        <button
          onClick={() => safeGo(1)}
          disabled={page <= 1}
          className="rounded p-1.5 text-white/50 hover:bg-white/10 disabled:opacity-30 transition-colors"
        >
          <ChevronsLeft className="h-3.5 w-3.5" />
        </button>
        <button
          onClick={() => safeGo(page - 1)}
          disabled={page <= 1}
          className="rounded p-1.5 text-white/50 hover:bg-white/10 disabled:opacity-30 transition-colors"
        >
          <ChevronLeft className="h-3.5 w-3.5" />
        </button>
        <input
          value={jumpInput}
          onChange={(e) => setJumpInput(e.target.value.replace(/\D/g, ""))}
          onKeyDown={(e) => e.key === "Enter" && applyJump()}
          placeholder={`${page}`}
          className="h-7 w-12 rounded border border-white/20 bg-white/5 px-1 text-center text-xs text-white"
        />
        <span className="text-[10px] text-muted-foreground">/ {pages}</span>
        <button
          onClick={() => safeGo(page + 1)}
          disabled={page >= pages}
          className="rounded p-1.5 text-white/50 hover:bg-white/10 disabled:opacity-30 transition-colors"
        >
          <ChevronRight className="h-3.5 w-3.5" />
        </button>
        <button
          onClick={() => safeGo(pages)}
          disabled={page >= pages}
          className="rounded p-1.5 text-white/50 hover:bg-white/10 disabled:opacity-30 transition-colors"
        >
          <ChevronsRight className="h-3.5 w-3.5" />
        </button>
      </div>
    </div>
  );
}
