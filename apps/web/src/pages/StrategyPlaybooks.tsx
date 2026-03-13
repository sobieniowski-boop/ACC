import { useQuery } from "@tanstack/react-query";
import { BookOpen, ChevronRight, Clock, BarChart3, Target } from "lucide-react";
import { getStrategyPlaybooks } from "@/lib/api";
import type { StrategyPlaybook, PlaybookStep } from "@/lib/api";
import { cn } from "@/lib/utils";

const PLAYBOOK_COLORS: Record<string, string> = {
  high_sessions_low_cvr: "border-blue-500/30 bg-blue-500/5",
  rising_demand_low_cover: "border-orange-500/30 bg-orange-500/5",
  strong_de_weak_expansion: "border-green-500/30 bg-green-500/5",
  high_return_strong_traffic: "border-red-500/30 bg-red-500/5",
  ads_high_spend_low_profit: "border-yellow-500/30 bg-yellow-500/5",
  family_broken_abroad: "border-purple-500/30 bg-purple-500/5",
  bundle_complementary: "border-pink-500/30 bg-pink-500/5",
};

export default function StrategyPlaybooksPage() {
  const { data, isLoading } = useQuery({
    queryKey: ["strategy-playbooks"],
    queryFn: () => getStrategyPlaybooks(),
    staleTime: 300_000,
  });

  const playbooks: StrategyPlaybook[] = data?.playbooks ?? [];

  return (
    <div className="space-y-6 p-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Strategy Playbooks</h1>
        <p className="text-sm text-muted-foreground mt-0.5">Gotowe scenariusze działania — od diagnozy do egzekucji</p>
      </div>

      {isLoading ? (
        <div className="grid gap-5 lg:grid-cols-2">
          {Array.from({ length: 6 }).map((_, i) => <div key={i} className="h-40 bg-muted/30 rounded-xl animate-pulse" />)}
        </div>
      ) : playbooks.length === 0 ? (
        <p className="text-sm text-muted-foreground">No playbooks configured</p>
      ) : (
        <div className="grid gap-5 lg:grid-cols-2">
          {playbooks.map((pb) => (
            <PlaybookCard key={pb.id} playbook={pb} />
          ))}
        </div>
      )}
    </div>
  );
}

function PlaybookCard({ playbook }: { playbook: StrategyPlaybook }) {
  const color = PLAYBOOK_COLORS[playbook.id] || "border-border bg-card";

  return (
    <div className={cn("rounded-xl border p-5 space-y-4", color)}>
      {/* Header */}
      <div>
        <div className="flex items-center gap-2 mb-1">
          <BookOpen className="h-4 w-4 text-muted-foreground" />
          <h3 className="font-semibold text-sm">{playbook.name}</h3>
        </div>
        <p className="text-xs text-muted-foreground">{playbook.description}</p>
      </div>

      {/* Trigger */}
      <div className="flex items-center gap-2 text-xs">
        <Target className="h-3.5 w-3.5 text-amazon" />
        <span className="font-medium">Trigger:</span>
        <span className="text-muted-foreground">{playbook.trigger_condition}</span>
      </div>

      {/* Steps */}
      <div className="space-y-1.5">
        <p className="text-xs font-medium flex items-center gap-1"><ChevronRight className="h-3 w-3" /> Steps</p>
        <ol className="space-y-1 pl-4">
          {playbook.steps.map((step: PlaybookStep, i: number) => (
            <li key={i} className="flex items-start gap-2 text-xs">
              <span className="mt-0.5 flex-shrink-0 h-4 w-4 rounded-full bg-muted flex items-center justify-center text-[10px] font-bold">{i + 1}</span>
              <div>
                <span className="font-medium">{step.action}</span>
                {step.owner_role && <span className="text-muted-foreground ml-1">({step.owner_role})</span>}
              </div>
            </li>
          ))}
        </ol>
      </div>

      {/* Metrics & Time */}
      <div className="flex items-center gap-4 text-xs pt-1 border-t border-border/30">
        {playbook.metrics_to_monitor && playbook.metrics_to_monitor.length > 0 && (
          <div className="flex items-center gap-1.5">
            <BarChart3 className="h-3 w-3 text-muted-foreground" />
            <span className="text-muted-foreground">{playbook.metrics_to_monitor.join(", ")}</span>
          </div>
        )}
        {playbook.expected_time_to_impact && (
          <div className="flex items-center gap-1.5">
            <Clock className="h-3 w-3 text-muted-foreground" />
            <span className="text-muted-foreground">{playbook.expected_time_to_impact}</span>
          </div>
        )}
      </div>
    </div>
  );
}
