import { useQuery } from "@tanstack/react-query";
import { getNetfoxSessionHealth } from "@/lib/api";
import { ClientExportButton } from "@/components/shared";

export default function NetfoxHealthPage() {
  const healthQuery = useQuery({
    queryKey: ["netfox-session-health"],
    queryFn: getNetfoxSessionHealth,
    refetchInterval: 15000,
  });

  const data = healthQuery.data;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Netfox Sessions</h1>
        <p className="text-sm text-muted-foreground">
          Active ACC read-only sessions visible on Netfox (`ACC-Netfox-RO`).
        </p>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <div className="rounded border border-border bg-card p-4">
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Status</div>
          <div className="mt-2 text-2xl font-semibold">
            {data?.ok ? "ok" : "error"}
          </div>
        </div>
        <div className="rounded border border-border bg-card p-4">
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Session count</div>
          <div className="mt-2 text-2xl font-semibold">
            {data?.session_count ?? "-"}
          </div>
        </div>
        <div className="rounded border border-border bg-card p-4">
          <div className="text-xs uppercase tracking-wide text-muted-foreground">Last refresh</div>
          <div className="mt-2 text-sm font-medium">
            {healthQuery.dataUpdatedAt
              ? new Date(healthQuery.dataUpdatedAt).toLocaleString("pl-PL")
              : "-"}
          </div>
        </div>
      </div>

      {data?.error ? (
        <div className="rounded border border-amber-500/40 bg-amber-500/10 p-4 text-sm text-amber-200">
          Health check error: {data.error}
        </div>
      ) : null}

      <div className="rounded border border-border bg-card">
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <h2 className="text-sm font-semibold">Active sessions</h2>
          <ClientExportButton data={data?.items ?? []} filename="netfox_sessions" />
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead className="border-b border-border text-left text-[9px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-2 py-2">Session</th>
                <th className="px-2 py-2">Login</th>
                <th className="px-2 py-2">Host</th>
                <th className="px-2 py-2">Status</th>
                <th className="px-2 py-2">Database</th>
                <th className="px-2 py-2">Last request</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {data?.items?.length ? (
                data.items.map((item) => (
                  <tr key={item.session_id} className="hover:bg-muted/20 transition-colors">
                    <td className="px-2 py-1.5 font-mono">{item.session_id}</td>
                    <td className="px-2 py-1.5">{item.login_name ?? "-"}</td>
                    <td className="px-2 py-1.5">{item.host_name ?? "-"}</td>
                    <td className="px-2 py-1.5">{item.status ?? "-"}</td>
                    <td className="px-2 py-1.5">{item.database_name ?? "-"}</td>
                    <td className="px-2 py-1.5">
                      {item.last_request_start_time
                        ? new Date(item.last_request_start_time).toLocaleString("pl-PL")
                        : "-"}
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={6} className="px-2 py-6 text-center text-muted-foreground">
                    No active ACC Netfox sessions.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
