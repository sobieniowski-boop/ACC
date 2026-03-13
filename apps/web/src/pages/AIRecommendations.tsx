import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Brain, Zap, CheckCircle, XCircle, Sparkles, TrendingUp } from "lucide-react";
import { getAIRecommendations, getAISummary, updateAIRecStatus, generateAIRec } from "@/lib/api";
import type { AIRecommendation } from "@/lib/api";
import { formatPLN } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { ClientExportButton } from "@/components/shared";

const REC_TYPES = [
  { value: "pricing", label: "Cennik" },
  { value: "reorder", label: "Uzupełnienie Zapasów" },
  { value: "listing_optimization", label: "Optymalizacja Listingów" },
  { value: "ad_budget", label: "Budżet Reklamowy" },
  { value: "risk_flag", label: "Alert Ryzyka" },
];

function typeLabel(type: string) {
  return REC_TYPES.find((r) => r.value === type)?.label ?? type;
}

function typeBadge(type: string) {
  switch (type) {
    case "pricing":               return <Badge variant="default">Cennik</Badge>;
    case "reorder":               return <Badge variant="secondary">Zapasy</Badge>;
    case "listing_optimization":  return <Badge variant="outline">Listing</Badge>;
    case "ad_budget":             return <Badge variant="warning">Ads</Badge>;
    case "risk_flag":             return <Badge variant="destructive">Ryzyko</Badge>;
    default:                      return <Badge variant="outline">{type}</Badge>;
  }
}

function statusBadge(status: string) {
  switch (status) {
    case "accepted":   return <Badge variant="success">Zaakceptowana</Badge>;
    case "dismissed":  return <Badge variant="secondary">Odrzucona</Badge>;
    default:           return <Badge variant="warning">Nowa</Badge>;
  }
}

function confidenceColor(score: number) {
  if (score >= 0.8) return "text-emerald-400";
  if (score >= 0.6) return "text-[#FF9900]";
  return "text-red-400";
}

function RecCard({ rec }: { rec: AIRecommendation }) {
  const qc = useQueryClient();
  const { mutate: updateStatus, isPending } = useMutation({
    mutationFn: ({ id, status }: { id: number; status: "accepted" | "dismissed" }) =>
      updateAIRecStatus(id, status),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["ai-recommendations"] });
      qc.invalidateQueries({ queryKey: ["ai-summary"] });
    },
  });

  return (
    <Card className={`transition-opacity ${rec.status !== "new" ? "opacity-60" : ""}`}>
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-4">
          <div className="flex items-center gap-2 flex-wrap">
            {typeBadge(rec.rec_type)}
            {statusBadge(rec.status)}
            {rec.sku && (
              <span className="font-mono text-xs text-white/40 bg-white/5 px-2 py-0.5 rounded">
                {rec.sku}
              </span>
            )}
          </div>
          <div className="flex items-center gap-1.5 shrink-0">
            {rec.status === "new" && (
              <>
                <Button
                  size="sm"
                  variant="outline"
                  className="h-7 text-xs gap-1 text-emerald-400 border-emerald-500/30 hover:bg-emerald-900/20"
                  disabled={isPending}
                  onClick={() => updateStatus({ id: rec.id, status: "accepted" })}
                >
                  <CheckCircle className="w-3 h-3" /> Akceptuj
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-7 text-xs gap-1 text-white/40"
                  disabled={isPending}
                  onClick={() => updateStatus({ id: rec.id, status: "dismissed" })}
                >
                  <XCircle className="w-3 h-3" /> Odrzuć
                </Button>
              </>
            )}
          </div>
        </div>
        <CardTitle className="text-sm leading-snug mt-2">{rec.title}</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <p className="text-sm text-white/60">{rec.summary}</p>
        {rec.action_items.length > 0 && (
          <ul className="space-y-1">
            {rec.action_items.map((item, i) => (
              <li key={i} className="flex items-start gap-2 text-xs text-white/70">
                <span className="text-[#FF9900] mt-0.5">▶</span>
                {item}
              </li>
            ))}
          </ul>
        )}
        <div className="flex items-center justify-between pt-1 text-xs text-white/30">
          <div className="flex items-center gap-3">
            <span>
              Pewność:{" "}
              <span className={confidenceColor(rec.confidence_score)}>
                {(rec.confidence_score * 100).toFixed(0)}%
              </span>
            </span>
            {rec.expected_impact_pln != null && (
              <span>
                Potencjał:{" "}
                <span className="text-[#FF9900]">{formatPLN(rec.expected_impact_pln)}</span>
              </span>
            )}
            <span>{rec.model_used}</span>
          </div>
          <span>{new Date(rec.created_at).toLocaleString("pl-PL")}</span>
        </div>
      </CardContent>
    </Card>
  );
}

export default function AIRecommendations() {
  const qc = useQueryClient();
  const [filterType, setFilterType] = useState("all");
  const [filterStatus, setFilterStatus] = useState("all");
  const [generateType, setGenerateType] = useState("pricing");
  const [generating, setGenerating] = useState(false);

  const { data: listData, isLoading } = useQuery({
    queryKey: ["ai-recommendations", filterType, filterStatus],
    queryFn: () =>
      getAIRecommendations({
        ...(filterType !== "all" ? { rec_type: filterType } : {}),
        ...(filterStatus !== "all" ? { status: filterStatus } : {}),
      }),
    staleTime: 60_000,
  });

  const { data: summary } = useQuery({
    queryKey: ["ai-summary"],
    queryFn: getAISummary,
    staleTime: 60_000,
  });

  async function handleGenerate() {
    setGenerating(true);
    try {
      await generateAIRec(generateType);
      qc.invalidateQueries({ queryKey: ["ai-recommendations"] });
      qc.invalidateQueries({ queryKey: ["ai-summary"] });
    } finally {
      setGenerating(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white flex items-center gap-2">
            <Brain className="w-6 h-6 text-[#FF9900]" />
            AI Rekomendacje
          </h1>
          <p className="text-white/50 text-sm mt-1">
            Rekomendacje GPT-5.2 — cennik, zapasy, listing, budżet reklamowy
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Select value={generateType} onValueChange={setGenerateType}>
            <SelectTrigger className="w-48">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {REC_TYPES.map((t) => (
                <SelectItem key={t.value} value={t.value}>{t.label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button onClick={handleGenerate} disabled={generating} className="gap-2">
            <Sparkles className="w-4 h-4" />
            {generating ? "Generowanie…" : "Generuj"}
          </Button>          <ClientExportButton data={listData?.items ?? []} filename="ai_recommendations" />        </div>
      </div>

      {/* Summary tiles */}
      {summary && (
        <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-white/50">Wszystkie</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{summary.total_recommendations}</div>
            </CardContent>
          </Card>
          <Card className="border-amber-500/30">
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-amber-400 flex items-center gap-1">
                <Zap className="w-3 h-3" /> Nowe
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-amber-400">{summary.new_count}</div>
            </CardContent>
          </Card>
          <Card className="border-emerald-500/30">
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-emerald-400 flex items-center gap-1">
                <CheckCircle className="w-3 h-3" /> Zaakceptowane
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-emerald-400">{summary.accepted_count}</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-white/50">Odrzucone</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-white/40">{summary.dismissed_count}</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-xs text-white/50 flex items-center gap-1">
                <TrendingUp className="w-3 h-3 text-[#FF9900]" /> Potencjał
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-xl font-bold text-[#FF9900]">
                {formatPLN(summary.total_expected_impact_pln)}
              </div>
              <div className="text-xs text-white/30">zaakceptowane</div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Filters */}
      <div className="flex items-center gap-3">
        <Select value={filterType} onValueChange={setFilterType}>
          <SelectTrigger className="w-48">
            <SelectValue placeholder="Typ" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">Wszystkie typy</SelectItem>
            {REC_TYPES.map((t) => (
              <SelectItem key={t.value} value={t.value}>{t.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select value={filterStatus} onValueChange={setFilterStatus}>
          <SelectTrigger className="w-40">
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">Wszystkie</SelectItem>
            <SelectItem value="new">Nowe</SelectItem>
            <SelectItem value="accepted">Zaakceptowane</SelectItem>
            <SelectItem value="dismissed">Odrzucone</SelectItem>
          </SelectContent>
        </Select>
        <span className="text-xs text-white/40">
          {listData?.total ?? 0} wyników
        </span>
      </div>

      {/* Cards */}
      {isLoading ? (
        <div className="space-y-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-40 w-full" />
          ))}
        </div>
      ) : listData && listData.items.length > 0 ? (
        <div className="space-y-4">
          {listData.items.map((rec) => (
            <RecCard key={rec.id} rec={rec} />
          ))}
        </div>
      ) : (
        <div className="flex flex-col items-center justify-center py-20 text-white/30">
          <Brain className="w-16 h-16 mb-4 opacity-20" />
          <p className="text-lg">Brak rekomendacji</p>
          <p className="text-sm mt-1">Użyj przycisku "Generuj" aby stworzyć pierwszą rekomendację AI</p>
        </div>
      )}
    </div>
  );
}
