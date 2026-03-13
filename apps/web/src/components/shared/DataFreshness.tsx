import { formatDistanceToNow } from "date-fns";
import { pl } from "date-fns/locale";
import { Clock, RefreshCw, AlertTriangle, CheckCircle } from "lucide-react";
import { cn } from "@/lib/utils";

interface DataFreshnessProps {
  /** ISO timestamp of last sync, or null if never */
  lastSync: string | null | undefined;
  /** Threshold in minutes — stale = warning */
  staleMinutes?: number;
  /** Optional label prefix */
  label?: string;
  /** Optional onRefresh callback */
  onRefresh?: () => void;
  /** Is refresh in progress */
  refreshing?: boolean;
}

export function DataFreshness({
  lastSync,
  staleMinutes = 60,
  label = "Sync",
  onRefresh,
  refreshing,
}: DataFreshnessProps) {
  if (!lastSync) {
    return (
      <span className="inline-flex items-center gap-1.5 text-[10px] text-amber-400">
        <AlertTriangle className="h-3 w-3" />
        Brak synchronizacji
      </span>
    );
  }

  const ts = new Date(lastSync);
  const ageMs = Date.now() - ts.getTime();
  const isStale = ageMs > staleMinutes * 60_000;
  const ago = formatDistanceToNow(ts, { addSuffix: true, locale: pl });

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 text-[10px]",
        isStale ? "text-amber-400" : "text-white/40",
      )}
    >
      {isStale ? (
        <AlertTriangle className="h-3 w-3" />
      ) : (
        <CheckCircle className="h-3 w-3" />
      )}
      <span>
        {label} {ago}
      </span>
      {onRefresh && (
        <button
          onClick={onRefresh}
          disabled={refreshing}
          className="hover:text-[#FF9900] transition-colors disabled:opacity-40"
          title="Odśwież dane"
        >
          <RefreshCw className={cn("h-3 w-3", refreshing && "animate-spin")} />
        </button>
      )}
    </span>
  );
}
