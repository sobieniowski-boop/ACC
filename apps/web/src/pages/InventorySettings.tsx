import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { getManageInventorySettings, updateManageInventorySettings } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";

export default function InventorySettingsPage() {
  const qc = useQueryClient();
  const settingsQuery = useQuery({
    queryKey: ["manage-inventory-settings"],
    queryFn: getManageInventorySettings,
  });

  const [highSessions, setHighSessions] = useState("100");
  const [highUnits, setHighUnits] = useState("5");
  const [criticalCover, setCriticalCover] = useState("7");
  const [warningCover, setWarningCover] = useState("14");
  const [overstockDays, setOverstockDays] = useState("90");
  const [autoPropose, setAutoPropose] = useState("75");
  const [safeAuto, setSafeAuto] = useState("90");
  const [nightlyHour, setNightlyHour] = useState("2");
  const [savedViewsEnabled, setSavedViewsEnabled] = useState(true);

  useEffect(() => {
    if (!settingsQuery.data) return;
    setHighSessions(String(settingsQuery.data.thresholds?.high_sessions_threshold ?? 100));
    setHighUnits(String(settingsQuery.data.thresholds?.high_units_threshold ?? 5));
    setCriticalCover(String(settingsQuery.data.thresholds?.stockout_days_critical ?? 7));
    setWarningCover(String(settingsQuery.data.thresholds?.stockout_days_warning ?? 14));
    setOverstockDays(String(settingsQuery.data.thresholds?.overstock_days ?? 90));
    setAutoPropose(String(settingsQuery.data.apply_safety?.auto_propose_confidence ?? 75));
    setSafeAuto(String(settingsQuery.data.apply_safety?.safe_auto_confidence ?? 90));
    setNightlyHour(String(settingsQuery.data.traffic_schedule?.nightly_hour_utc ?? 2));
    setSavedViewsEnabled(Boolean(settingsQuery.data.saved_views_enabled));
  }, [settingsQuery.data]);

  const saveMutation = useMutation({
    mutationFn: () =>
      updateManageInventorySettings({
        thresholds: {
          high_sessions_threshold: Number(highSessions) || 0,
          high_units_threshold: Number(highUnits) || 0,
          stockout_days_critical: Number(criticalCover) || 0,
          stockout_days_warning: Number(warningCover) || 0,
          overstock_days: Number(overstockDays) || 0,
        },
        apply_safety: {
          ...(settingsQuery.data?.apply_safety ?? {}),
          auto_propose_confidence: Number(autoPropose) || 0,
          safe_auto_confidence: Number(safeAuto) || 0,
        },
        traffic_schedule: {
          ...(settingsQuery.data?.traffic_schedule ?? {}),
          nightly_hour_utc: Number(nightlyHour) || 0,
        },
        saved_views_enabled: savedViewsEnabled,
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["manage-inventory-settings"] }),
  });

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Inventory settings</h1>
        <p className="text-sm text-white/50">Guardrails, thresholds and schedule defaults for Manage All Inventory.</p>
      </div>

      <div className="grid gap-4 xl:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Thresholds</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <Input value={highSessions} onChange={(e) => setHighSessions(e.target.value)} placeholder="High sessions threshold" />
            <Input value={highUnits} onChange={(e) => setHighUnits(e.target.value)} placeholder="High units threshold" />
            <Input value={criticalCover} onChange={(e) => setCriticalCover(e.target.value)} placeholder="Critical cover days" />
            <Input value={warningCover} onChange={(e) => setWarningCover(e.target.value)} placeholder="Warning cover days" />
            <Input value={overstockDays} onChange={(e) => setOverstockDays(e.target.value)} placeholder="Overstock days" />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Apply safety</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <Input value={autoPropose} onChange={(e) => setAutoPropose(e.target.value)} placeholder="Auto-propose confidence" />
            <Input value={safeAuto} onChange={(e) => setSafeAuto(e.target.value)} placeholder="Safe-auto confidence" />
            <label className="flex items-center gap-2 text-sm text-white/70">
              <input
                type="checkbox"
                checked={savedViewsEnabled}
                onChange={(e) => setSavedViewsEnabled(e.target.checked)}
              />
              Saved views enabled
            </label>
            <div className="text-xs text-white/45">Apply still remains guarded by validation and approval status on the backend.</div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Traffic schedule</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <Input value={nightlyHour} onChange={(e) => setNightlyHour(e.target.value)} placeholder="Nightly refresh hour UTC" />
            <div className="text-xs text-white/45">
              Current backend keeps traffic coverage honest and partial until Sales & Traffic feed is fully validated.
            </div>
            <Button onClick={() => saveMutation.mutate()} disabled={saveMutation.isPending}>
              {saveMutation.isPending ? "Saving..." : "Save settings"}
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
