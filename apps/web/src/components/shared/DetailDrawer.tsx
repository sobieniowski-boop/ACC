import { X } from "lucide-react";
import { cn } from "@/lib/utils";

interface DetailDrawerProps {
  open: boolean;
  onClose: () => void;
  title?: string;
  width?: string;
  children: React.ReactNode;
}

export function DetailDrawer({
  open,
  onClose,
  title,
  width = "w-[420px]",
  children,
}: DetailDrawerProps) {
  return (
    <>
      {/* backdrop */}
      {open && (
        <div
          className="fixed inset-0 z-40 bg-black/40 backdrop-blur-[2px] transition-opacity"
          onClick={onClose}
        />
      )}
      {/* panel */}
      <div
        className={cn(
          "fixed right-0 top-0 z-50 h-full border-l border-white/10 bg-[#0f172a] shadow-2xl transition-transform duration-200",
          width,
          open ? "translate-x-0" : "translate-x-full",
        )}
      >
        {/* header */}
        <div className="flex items-center justify-between border-b border-white/10 px-4 py-3">
          {title && <h3 className="text-sm font-semibold text-white truncate">{title}</h3>}
          <button
            onClick={onClose}
            className="rounded p-1 text-white/40 hover:bg-white/10 hover:text-white transition-colors"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        {/* content */}
        <div className="h-[calc(100%-49px)] overflow-y-auto p-4">{children}</div>
      </div>
    </>
  );
}
