import { useQuery } from "@tanstack/react-query";
import { Gift, Puzzle, TrendingUp } from "lucide-react";
import { getStrategyBundles } from "@/lib/api";
import type { BundleCandidate } from "@/lib/api";
import { formatPLN, cn } from "@/lib/utils";

export default function StrategyBundlesPage() {
  const { data, isLoading } = useQuery({
    queryKey: ["strategy-bundles"],
    queryFn: () => getStrategyBundles(),
    staleTime: 60_000,
  });

  const bundles: BundleCandidate[] = data?.bundles ?? [];
  const variants: BundleCandidate[] = (data?.variant_gaps ?? []) as unknown as BundleCandidate[];

  return (
    <div className="space-y-6 p-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Bundles & Variant Gaps</h1>
        <p className="text-sm text-muted-foreground mt-0.5">Wykryte okazje bundlowe i luki wariantowe w portfolio</p>
      </div>

      {/* Bundle Candidates */}
      <Section icon={<Gift className="h-5 w-5 text-amazon" />} title="Bundle Candidates" count={bundles.length}>
        {isLoading ? <Skeleton /> : bundles.length === 0 ? <Empty /> : (
          <div className="rounded-xl border border-border bg-card overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-xs text-muted-foreground uppercase">
                  <th className="px-3 py-2 text-left">SKU A</th>
                  <th className="px-3 py-2 text-left">SKU B</th>
                  <th className="px-3 py-2 text-left">MKT</th>
                  <th className="px-3 py-2 text-left max-w-[240px]">Bundle / Action</th>
                  <th className="px-3 py-2 text-left">Blocker</th>
                  <th className="px-3 py-2 text-right">Est. Margin</th>
                  <th className="px-3 py-2 text-right">Profit ↑</th>
                  <th className="px-3 py-2 text-right">Confidence</th>
                  <th className="px-3 py-2 text-right">Action</th>
                </tr>
              </thead>
              <tbody>
                {bundles.map((b) => <BundleRow key={b.id} item={b} />)}
              </tbody>
            </table>
          </div>
        )}
      </Section>

      {/* Variant Gaps */}
      <Section icon={<Puzzle className="h-5 w-5 text-purple-400" />} title="Variant Expansion Gaps" count={variants.length}>
        {isLoading ? <Skeleton /> : variants.length === 0 ? <Empty /> : (
          <div className="rounded-xl border border-border bg-card overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-xs text-muted-foreground uppercase">
                  <th className="px-3 py-2 text-left">SKU A</th>
                  <th className="px-3 py-2 text-left">SKU B</th>
                  <th className="px-3 py-2 text-left">MKT</th>
                  <th className="px-3 py-2 text-left max-w-[240px]">Bundle / Action</th>
                  <th className="px-3 py-2 text-left">Blocker</th>
                  <th className="px-3 py-2 text-right">Est. Margin</th>
                  <th className="px-3 py-2 text-right">Profit ↑</th>
                  <th className="px-3 py-2 text-right">Confidence</th>
                  <th className="px-3 py-2 text-right">Action</th>
                </tr>
              </thead>
              <tbody>
                {variants.map((v) => <BundleRow key={v.id} item={v} />)}
              </tbody>
            </table>
          </div>
        )}
      </Section>
    </div>
  );
}

function BundleRow({ item }: { item: BundleCandidate }) {
  return (
    <tr className="border-b border-border/50 hover:bg-muted/20">
      <td className="px-3 py-2 font-mono text-xs">{item.sku_a || "—"}</td>
      <td className="px-3 py-2 text-xs">{item.sku_b || "—"}</td>
      <td className="px-3 py-2 text-xs">{item.marketplace_id || "—"}</td>
      <td className="px-3 py-2 text-xs max-w-[240px] truncate" title={item.proposed_bundle_sku ?? item.action ?? ""}>{item.proposed_bundle_sku ?? item.action ?? "—"}</td>
      <td className="px-3 py-2 text-xs text-muted-foreground">{item.blocker?.replace(/_/g, " ") || "—"}</td>
      <td className="px-3 py-2 text-right text-xs tabular-nums">{item.est_margin != null ? `${item.est_margin.toFixed(1)}%` : "—"}</td>
      <td className="px-3 py-2 text-right text-xs tabular-nums text-green-500 font-medium">{item.est_profit_uplift != null ? formatPLN(item.est_profit_uplift) : "—"}</td>
      <td className="px-3 py-2 text-right text-xs tabular-nums">{item.confidence?.toFixed(0) ?? "—"}%</td>
      <td className="px-3 py-2 text-right text-xs">{item.action || "—"}</td>
    </tr>
  );
}

function Section({ icon, title, count, children }: { icon: React.ReactNode; title: string; count: number; children: React.ReactNode }) {
  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        {icon}
        <h2 className="text-lg font-semibold">{title}</h2>
        <span className="rounded-full bg-muted px-2 py-0.5 text-xs font-medium">{count}</span>
      </div>
      {children}
    </div>
  );
}

function Skeleton() {
  return <div className="space-y-2">{Array.from({ length: 5 }).map((_, i) => <div key={i} className="h-8 bg-muted/30 rounded animate-pulse" />)}</div>;
}

function Empty() {
  return <p className="text-sm text-muted-foreground py-4 text-center">No candidates detected yet</p>;
}
