import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Settings, Play, Loader2 } from "lucide-react";
import { getSeasonalitySettings, updateSeasonalitySettings, runSeasonalityJob } from "@/lib/api";

const SETTING_META: Record<string, { label: string; description: string; type: "number" }> = {
  reference_window_months: { label: "Reference Window (months)", description: "How many months of history to use for index & profile calculation", type: "number" },
  min_confidence_threshold: { label: "Min Confidence Threshold", description: "Minimum confidence score (0-100) to include in opportunity detection", type: "number" },
  upcoming_peak_horizon_days: { label: "Peak Horizon (days)", description: "How many days ahead to look for upcoming peaks", type: "number" },
  evergreen_threshold: { label: "Evergreen Threshold", description: "Evergreen score above this = EVERGREEN classification", type: "number" },
  strong_seasonal_threshold: { label: "Strong Seasonal Threshold", description: "Strength score above this = STRONG_SEASONAL", type: "number" },
  peak_seasonal_threshold: { label: "Peak Seasonal Threshold", description: "Strength score above this = PEAK_SEASONAL", type: "number" },
  mild_seasonal_threshold: { label: "Mild Seasonal Threshold", description: "Strength score above this = MILD_SEASONAL", type: "number" },
};

const JOB_TYPES = [
  { key: "build_monthly", label: "Build Monthly Metrics", desc: "Aggregate raw data into monthly metrics per entity" },
  { key: "recompute_indices", label: "Recompute Indices", desc: "Recalculate demand/sales/profit month indices" },
  { key: "recompute_profiles", label: "Recompute Profiles", desc: "Reclassify all entity seasonality profiles" },
  { key: "detect_opportunities", label: "Detect Opportunities", desc: "Run the 8-engine opportunity detection pipeline" },
];

export default function SeasonalitySettingsPage() {
  const qc = useQueryClient();
  const [draft, setDraft] = useState<Record<string, string>>({});
  const [runningJob, setRunningJob] = useState<string | null>(null);
  const [jobResult, setJobResult] = useState<string | null>(null);

  const { data: settings, isLoading } = useQuery({
    queryKey: ["seasonality-settings"],
    queryFn: getSeasonalitySettings,
    staleTime: 5 * 60_000,
  });

  const saveMut = useMutation({
    mutationFn: (kv: Record<string, string>) => updateSeasonalitySettings(kv),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["seasonality-settings"] });
      setDraft({});
    },
  });

  const jobMut = useMutation({
    mutationFn: (jobType: string) => {
      setRunningJob(jobType);
      setJobResult(null);
      return runSeasonalityJob(jobType);
    },
    onSuccess: (_data, jobType) => {
      setRunningJob(null);
      setJobResult(`✅ ${jobType} completed`);
      qc.invalidateQueries({ queryKey: ["seasonality"] });
    },
    onError: (err: Error, jobType) => {
      setRunningJob(null);
      setJobResult(`❌ ${jobType} failed: ${err.message}`);
    },
  });

  const currentValues: Record<string, string> = settings?.settings
    ? { ...settings.settings }
    : {};

  const hasDirty = Object.keys(draft).length > 0;

  const handleSave = () => {
    if (hasDirty) saveMut.mutate(draft);
  };

  return (
    <div className="space-y-6 p-6 max-w-3xl">
      <div className="flex items-center gap-3">
        <Settings className="h-6 w-6 text-amazon" />
        <h1 className="text-xl font-bold tracking-tight">Seasonality Settings</h1>
      </div>

      {/* Settings */}
      <div className="rounded-xl border border-border bg-card p-5 space-y-4">
        <h2 className="text-sm font-semibold">Parameters</h2>
        {isLoading ? (
          <div className="text-sm text-muted-foreground animate-pulse">Loading…</div>
        ) : (
          <div className="space-y-3">
            {Object.entries(SETTING_META).map(([key, meta]) => {
              const val = draft[key] ?? currentValues[key] ?? "";
              return (
                <div key={key} className="flex items-center gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="text-xs font-medium">{meta.label}</div>
                    <div className="text-[10px] text-muted-foreground">{meta.description}</div>
                  </div>
                  <input type="number" value={val}
                    onChange={e => setDraft(d => ({ ...d, [key]: e.target.value }))}
                    className="w-24 rounded-lg border border-border bg-background px-2 py-1.5 text-xs text-right tabular-nums" />
                </div>
              );
            })}
          </div>
        )}

        <div className="flex gap-2 pt-2">
          <button onClick={handleSave} disabled={!hasDirty || saveMut.isPending}
            className="rounded-lg bg-amazon px-4 py-1.5 text-xs font-medium text-white hover:bg-amazon/90 disabled:opacity-50">
            {saveMut.isPending ? "Saving…" : "Save Settings"}
          </button>
          {hasDirty && (
            <button onClick={() => setDraft({})}
              className="rounded-lg border border-border px-4 py-1.5 text-xs">Reset</button>
          )}
        </div>
      </div>

      {/* Manual Job Triggers */}
      <div className="rounded-xl border border-border bg-card p-5 space-y-4">
        <h2 className="text-sm font-semibold">Manual Jobs</h2>
        <p className="text-[10px] text-muted-foreground">
          These jobs run automatically on schedule. Use the buttons below to trigger them manually.
        </p>
        <div className="space-y-2">
          {JOB_TYPES.map(j => (
            <div key={j.key} className="flex items-center gap-3 rounded-lg border border-border/50 p-3">
              <div className="flex-1 min-w-0">
                <div className="text-xs font-medium">{j.label}</div>
                <div className="text-[10px] text-muted-foreground">{j.desc}</div>
              </div>
              <button
                onClick={() => jobMut.mutate(j.key)}
                disabled={runningJob !== null}
                className="flex items-center gap-1.5 rounded-lg border border-amazon bg-amazon/10 px-3 py-1.5 text-xs font-medium text-amazon hover:bg-amazon/20 disabled:opacity-50">
                {runningJob === j.key ? (
                  <><Loader2 className="h-3.5 w-3.5 animate-spin" /> Running…</>
                ) : (
                  <><Play className="h-3.5 w-3.5" /> Run</>
                )}
              </button>
            </div>
          ))}
        </div>
        {jobResult && (
          <div className="rounded-lg border border-border bg-muted/30 p-3 text-xs">{jobResult}</div>
        )}
      </div>
    </div>
  );
}
