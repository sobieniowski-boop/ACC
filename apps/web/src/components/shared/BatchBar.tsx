import { cn } from "@/lib/utils";

interface BatchBarProps {
  selectedCount: number;
  onClear: () => void;
  children: React.ReactNode;
}

export function BatchBar({ selectedCount, onClear, children }: BatchBarProps) {
  if (selectedCount === 0) return null;

  return (
    <div className="sticky bottom-0 z-30 flex items-center gap-3 rounded-lg border border-[#FF9900]/30 bg-[#FF9900]/10 px-4 py-2 backdrop-blur-sm">
      <span className="text-xs font-semibold text-[#FF9900]">
        {selectedCount} zaznaczonych
      </span>
      <div className="flex items-center gap-2">{children}</div>
      <button
        onClick={onClear}
        className="ml-auto text-[10px] text-white/50 hover:text-white transition-colors"
      >
        Odznacz
      </button>
    </div>
  );
}

interface BatchActionButtonProps {
  onClick: () => void;
  label: string;
  variant?: "default" | "destructive";
  disabled?: boolean;
}

export function BatchActionButton({ onClick, label, variant = "default", disabled }: BatchActionButtonProps) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={cn(
        "rounded-md px-3 py-1.5 text-xs font-medium transition-colors disabled:opacity-50",
        variant === "destructive"
          ? "bg-red-600 text-white hover:bg-red-700"
          : "bg-[#FF9900] text-black hover:bg-[#e68a00]",
      )}
    >
      {label}
    </button>
  );
}
