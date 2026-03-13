import { Bell, LogOut } from "lucide-react";
import { useAuthStore } from "@/store/authStore";
import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { getAlerts } from "@/lib/api";
import { ThemeToggle } from "@/components/ui/theme-toggle";

export default function TopBar() {
  const logout = useAuthStore((s) => s.logout);
  const user = useAuthStore((s) => s.user);
  const navigate = useNavigate();

  const { data: alertsData } = useQuery({
    queryKey: ["alerts", "topbar"],
    queryFn: () => getAlerts({ is_resolved: false }),
    refetchInterval: 30_000,
  });

  const unread = alertsData?.unread ?? 0;
  const critical = alertsData?.critical_count ?? 0;

  return (
    <header className="flex h-14 items-center justify-between border-b border-border bg-card px-6">
      <div className="text-sm text-muted-foreground">
        Amazon Command Center
      </div>
      <div className="flex items-center gap-4">
        {/* Alerts badge */}
        <button
          onClick={() => navigate("/alerts")}
          className="relative rounded-md p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
        >
          <Bell className="h-5 w-5" />
          {unread > 0 && (
            <span
              className={`absolute -right-1 -top-1 flex h-4 w-4 items-center justify-center rounded-full text-[10px] font-bold text-white ${
                critical > 0 ? "bg-destructive" : "bg-amazon"
              }`}
            >
              {unread > 9 ? "9+" : unread}
            </span>
          )}
        </button>

        {/* Theme toggle */}
        <ThemeToggle />

        {/* User */}
        <span className="text-sm text-muted-foreground">
          {user?.full_name || user?.email || "User"}
        </span>

        {/* Logout */}
        <button
          onClick={() => { logout(); navigate("/login"); }}
          className="rounded-md p-1.5 text-muted-foreground hover:bg-accent hover:text-foreground"
          title="Logout"
        >
          <LogOut className="h-4 w-4" />
        </button>
      </div>
    </header>
  );
}
