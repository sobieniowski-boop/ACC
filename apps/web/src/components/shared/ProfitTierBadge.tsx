import { cn } from "@/lib/utils";

export type ProfitTier = "cm1" | "cm2" | "np";

const TIER_CONFIG: Record<ProfitTier, { label: string; description: string; color: string }> = {
  cm1: {
    label: "CM1",
    description: "Revenue − COGS − Fees − Logistics",
    color: "border-blue-500/30 bg-blue-500/10 text-blue-400",
  },
  cm2: {
    label: "CM2",
    description: "CM1 − Ads − Returns − Storage − FBA fees",
    color: "border-violet-500/30 bg-violet-500/10 text-violet-400",
  },
  np: {
    label: "Net Profit",
    description: "CM2 − Overhead (all 9 cost categories)",
    color: "border-emerald-500/30 bg-emerald-500/10 text-emerald-400",
  },
};

interface ProfitTierBadgeProps {
  tier: ProfitTier;
  className?: string;
}

export function ProfitTierBadge({ tier, className }: ProfitTierBadgeProps) {
  const cfg = TIER_CONFIG[tier];
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider",
        cfg.color,
        className,
      )}
      title={cfg.description}
    >
      {cfg.label}
    </span>
  );
}
