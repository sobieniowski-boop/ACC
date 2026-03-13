import { cn } from "@/lib/utils";

interface StickyFilterBarProps {
  children: React.ReactNode;
  className?: string;
}

export function StickyFilterBar({ children, className }: StickyFilterBarProps) {
  return (
    <div
      className={cn(
        "sticky top-0 z-20 -mx-4 flex flex-wrap items-center gap-2 border-b border-border bg-background/95 px-4 py-2.5 backdrop-blur-sm",
        className,
      )}
    >
      {children}
    </div>
  );
}
