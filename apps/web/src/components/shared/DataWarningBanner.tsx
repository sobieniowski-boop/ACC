import { AlertTriangle } from "lucide-react";

interface DataWarningBannerProps {
  warnings: string[] | undefined | null;
}

export function DataWarningBanner({ warnings }: DataWarningBannerProps) {
  if (!warnings || warnings.length === 0) return null;

  return (
    <div role="alert" aria-live="polite" className="rounded-md border border-yellow-500/30 bg-yellow-500/10 px-4 py-3 text-sm text-yellow-300">
      <div className="flex items-start gap-2">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-yellow-400" />
        <div>
          <p className="font-medium">⚠ Dane niekompletne</p>
          <ul className="mt-1 list-disc pl-4 text-xs text-yellow-300/80">
            {warnings.map((w, i) => (
              <li key={i}>{w}</li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
}
