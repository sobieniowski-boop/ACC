# ML System Design — ACC (Ascend Commerce Cloud)

| Field             | Value                                       |
|-------------------|---------------------------------------------|
| **Date**          | 2026-03-13                                  |
| **Version**       | 1.0                                         |
| **Agent**         | AI Engineer                                 |
| **Classification**| Internal — Confidential                     |
| **Status**        | Phase 1 — Baseline Intelligence Architecture|
| **Depends On**    | SYSTEM_ARCHITECTURE_SPEC_2026-03-13.md      |

---

## Table of Contents

1. [ML System Design](#1-ml-system-design)
2. [AI Ethics and Safety Framework](#2-ai-ethics-and-safety-framework)
3. [Model Monitoring and Retraining Plan](#3-model-monitoring-and-retraining-plan)
4. [Integration Points with Main Application](#4-integration-points-with-main-application)
5. [Cost Projections for ML Infrastructure](#5-cost-projections-for-ml-infrastructure)
6. [Appendices](#appendices)

---

## Executive Summary

ACC's intelligence layer comprises **12 modules** spanning LLM-based recommendations, algorithmic pricing, statistical anomaly detection, heuristic scoring, and feedback-loop learning. The system runs entirely within the FastAPI monolith — there are no self-hosted ML models or GPU instances.

**Architecture Philosophy:** Practical over exotic. Every intelligence module uses the simplest viable technique: rule-based scoring where rules suffice, statistical methods where distributions matter, and OpenAI GPT-5.2 only where natural language understanding is essential (product matching, natural-language recommendations). This keeps infrastructure costs at ~$308/month while delivering actionable intelligence across 4,300+ products in 10 EU marketplaces.

**Key Metrics:**
- 12 intelligence modules (2 LLM, 4 algorithmic, 4 statistical/heuristic, 1 feedback-loop, 1 computational)
- 14 scheduler jobs driving ML pipelines (daily, weekly, monthly cadences)
- ~87 measurable data signals across 8 domains
- Composite Data Quality Score: 74/100 (sufficient for current models, targeted improvements identified)
- Current AI spend: ~$15-30/month (OpenAI API) on top of $308 infrastructure

**Strategic Verdicts (from Architecture Spec):**
- Seasonality Engine: **FREEZE** (operational, 9,948 profiles computed)
- Strategy/Intelligence: **FREEZE** (Decision Intelligence feedback loop in early stage)
- Repricing Engine: **DEFER** (functional but SP-API Feeds execution pending)
- AI Recommendations / Product Matcher: **ACTIVE** (continuously used)
- Profit Engine: **ACTIVE** (primary revenue-driving module)

---

## 1. ML System Design

### 1.1 Intelligence Module Architecture Overview

The 12 intelligence modules form three tiers by model complexity:

```
┌──────────────────────────────────────────────────────────────────┐
│                    Intelligence Architecture                      │
│                                                                  │
│  TIER 1 — LLM-Powered (OpenAI GPT-5.2)                         │
│  ┌─────────────────────┐  ┌─────────────────────┐               │
│  │  AI Recommendations │  │  AI Product Matcher  │               │
│  │  (5 rec types)      │  │  (SKU matching)      │               │
│  └─────────────────────┘  └─────────────────────┘               │
│                                                                  │
│  TIER 2 — Algorithmic / Rule-Based                              │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │
│  │  Repricing   │ │  Catalog     │ │  Content     │            │
│  │  Engine (4)  │ │  Health      │ │  Optimization│            │
│  └──────────────┘ └──────────────┘ └──────────────┘            │
│                                                                  │
│  TIER 3 — Statistical / Probabilistic                           │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │
│  │  Inventory   │ │  Refund      │ │  Seasonality │            │
│  │  Risk (prob.)│ │  Anomaly     │ │  Service     │            │
│  └──────────────┘ └──────────────┘ └──────────────┘            │
│  ┌──────────────┐ ┌──────────────┐                              │
│  │  BuyBox      │ │  Taxonomy    │                              │
│  │  Radar       │ │  Predictions │                              │
│  └──────────────┘ └──────────────┘                              │
│                                                                  │
│  TIER 4 — Meta-Intelligence                                     │
│  ┌──────────────┐ ┌──────────────┐                              │
│  │  Decision    │ │  Profit      │                              │
│  │  Intelligence│ │  Engine (SQL)│                              │
│  └──────────────┘ └──────────────┘                              │
└──────────────────────────────────────────────────────────────────┘
```

### 1.2 Module-by-Module Technical Specification

#### Module 1: AI Recommendations Service

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/services/ai_service.py` |
| **Model Type** | LLM — OpenAI GPT-5.2 via `AsyncOpenAI` |
| **Function** | `generate_recommendation()` (async) |
| **Input Features** | Product performance data: revenue, margin, velocity, competition, inventory, PPC metrics |
| **Output** | `AIRecommendation` model: title, summary, action_items (JSON list), confidence_score (0-1) |
| **Recommendation Types** | `pricing`, `reorder`, `listing_optimization`, `ad_budget`, `risk_flag` |
| **Inference** | Real-time (on-demand), async HTTP to OpenAI |
| **Temperature** | 0.3 (deterministic-leaning) |
| **Response Format** | `json_object` (structured output) |
| **Token Tracking** | `prompt_tokens`, `completion_tokens` persisted per recommendation |
| **Storage** | `AIRecommendation` SQLAlchemy model (acc_ai_recommendation) |
| **System Prompt** | "Expert Amazon e-commerce analyst for a Polish seller" persona |
| **Max Tokens** | 4,096 (from `settings.OPENAI_MAX_TOKENS`) |
| **Verdict** | ACTIVE |

**Data Flow:**
```
Product metrics (DB) → _build_prompt() → OpenAI API → JSON parse → AIRecommendation → DB persist
```

#### Module 2: AI Product Matcher

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/services/ai_product_matcher.py` |
| **Model Type** | LLM — OpenAI GPT-5.2 + fuzzy string matching (SequenceMatcher) |
| **Function** | Match unmapped Amazon products to internal SKUs |
| **Input** | Unmapped products (sku, asin, title, ean) + mapped product pool (internal_sku, title, price) + listing registry |
| **Output** | Match suggestions stored in `acc_product_match_suggestion` (status: pending/approved) |
| **Pre-Filter** | Exact lookup via `acc_amazon_listing_registry` (SKU/ASIN/EAN → internal_sku) |
| **LLM Role** | Bundle decomposition, BOM generation, confidence scoring |
| **Domain Knowledge** | KADAX brand — kitchenware, jars, containers; bundle detection (20er Set, 10x, etc.) |
| **Error Classification** | 6 error types: auth_error, quota_exceeded, rate_limited, context_too_large, connection_error, api_status_error |
| **Human-in-the-Loop** | **REQUIRED** — mapping applied ONLY after user confirmation |
| **Verdict** | ACTIVE |

**Key Design Decision:** The matcher uses a multi-stage pipeline:
1. **Stage 1 — Exact match:** Registry lookup (SKU/ASIN/EAN), confidence 0.90-0.98
2. **Stage 2 — Fuzzy match:** SequenceMatcher on normalized titles
3. **Stage 3 — LLM match:** GPT-5.2 for complex bundle decomposition and BOM pricing

#### Module 3: Repricing Engine

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/intelligence/repricing_engine.py` |
| **Model Type** | Algorithmic — 4 pricing strategies with guardrail enforcement |
| **Strategies** | `buybox_match`, `competitive_undercut`, `margin_target`, `velocity_based` |
| **Input Features** | Current price, BuyBox price, competitor lowest price, purchase cost, Amazon fee %, FBA fee, shipping cost, ad cost, velocity (7d/30d) |
| **Output** | Execution proposals in `acc_repricing_execution` (status: proposed→approved→executed) |
| **Guardrails** | `min_price`, `max_price`, `min_margin_pct`, `max_daily_change_pct` (default 10%) |
| **Auto-Approval** | Changes ≤5% when `requires_approval=False` |
| **Tables** | `acc_repricing_strategy`, `acc_repricing_execution`, `acc_repricing_analytics` |
| **Verdict** | DEFER (functional, SP-API Feeds execution pending) |

**Strategy Algorithms:**
- **buybox_match:** `target = buybox_price`, clamped to [min_price, max_price]
- **competitive_undercut:** `target = competitor_lowest × (1 - undercut_pct/100)`, default 1% undercut
- **margin_target:** `target = fixed_costs / (1 - amazon_fee% - target_margin%)`, default 15% margin
- **velocity_based:** If 7d velocity > 30d velocity by >25% → raise price 3%; if <-25% → lower 5%

**Guardrail Chain:**
```
compute_*_price() → enforce_margin_guardrail() → enforce_daily_change_guardrail() → _apply_price_bounds()
```

#### Module 4: Refund Anomaly Detection

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/intelligence/refund_anomaly.py` |
| **Model Type** | Statistical — ratio-based spike detection |
| **Detection Types** | `refund_spike`, `fee_spike`, `return_rate_spike` |
| **Spike Thresholds** | Critical: 3.0× baseline, High: 2.0×, Medium: 1.5× |
| **Minimum Sample** | 5 orders (refund spike), 10 units (return rate spike) |
| **Serial Returner** | Min 3 returns, High risk: 50% return rate, Critical: 70% |
| **Input** | `acc_order` + `acc_order_line` — weekly refund rate vs baseline |
| **Output Tables** | `acc_refund_anomaly`, `acc_serial_returner`, `acc_reimbursement_case` |
| **Reimbursement Window** | 90 days (Amazon eligibility) |
| **Case Types** | `lost_inventory`, `damaged_inbound`, `fee_overcharge`, `customer_return_not_received` |
| **Verdict** | ACTIVE |

**Detection Algorithm:**
```
For each SKU in recent period (default 28 days):
  recent_rate = refund_count / order_count
  baseline_rate = (prior 28 days refund_count) / (prior 28 days order_count)
  spike_ratio = recent_rate / baseline_rate
  severity = classify(spike_ratio, thresholds)
```

#### Module 5: Inventory Risk Scoring

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/intelligence/inventory_risk.py` |
| **Model Type** | Probabilistic — normal approximation for stockout, heuristic for overstock/aging |
| **Composite Score** | 0-100 (stockout 40pts + overstock 30pts + aging 30pts) |
| **Risk Tiers** | critical ≥70, high ≥50, medium ≥30, low <30 |
| **Stockout Model** | `P(stockout) = Φ(-z)` where `z = (stock - μ·T) / (μ·CV·√T)`, approximated via logistic function |
| **Overstock Model** | `excess = max(0, stock - velocity_30d × target_days)`, storage fee (€0.50/unit/mo × 4.30 PLN) + capital cost (12% annual) |
| **Aging Model** | `aged_value = aged_90+ × unit_cost`, projects 30d aging based on velocity |
| **Constants** | Lead time: 21d, Target days: 45d, Safety stock: 14d |
| **Output Table** | `acc_inventory_risk_score` (27 columns) |
| **Verdict** | ACTIVE |

**Stockout Probability Function (pure computation):**
```python
def compute_stockout_probability(units, velocity_mean, velocity_cv, horizon):
    expected = velocity_mean * horizon
    demand_std = velocity_mean * velocity_cv * sqrt(horizon)
    z = (units - expected) / demand_std
    return 1 / (1 + exp(1.7 * z))  # logistic approx of Φ(-z)
```

#### Module 6: Seasonality Service

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/services/seasonality_service.py` |
| **Model Type** | Statistical aggregation + profile classification |
| **Functions** | `build_monthly_metrics()`, `recompute_indices()`, profile classification |
| **Input** | `acc_sku_profitability_rollup` (internal sales) + `acc_search_term_monthly` (Brand Analytics) |
| **Output** | `seasonality_monthly_metrics`, `seasonality_index_cache`, profiles (9,948), opportunities (1,402) |
| **Index Formula** | `demand_index = avg_metric_for_month / avg_metric_across_all_months` (1.0 = average) |
| **Blending** | When Brand Analytics data available: `search_demand_index` blended with sales-based `demand_index` |
| **SQL Pattern** | SQL MERGE for bulk upserts (SKU + category level) |
| **Aggregation** | 36-month lookback, SKU-level → rolled up to category-level |
| **Verdict** | FREEZE (operational, 91,760 rows) |

#### Module 7: Decision Intelligence

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/services/decision_intelligence_service.py` |
| **Model Type** | Feedback loop pipeline — 4 phases |
| **Phase 1** | `record_execution()` — snapshot baseline metrics from `acc_sku_profitability_rollup` |
| **Phase 2** | `run_outcome_monitoring()` — daily evaluation at matured windows (7/14/30/60 days) |
| **Phase 3** | `run_learning_aggregation()` — weekly per-type accuracy & ROI stats |
| **Phase 4** | `run_model_recalibration()` — monthly confidence/priority weight adjustments |
| **Opportunity Types** | 16+ types: PRICE_INCREASE, ADS_SCALE_UP, CONTENT_FIX, STOCK_REPLENISH, etc. |
| **Success Scoring** | `success_score = actual_profit_delta / expected_profit_delta` |
| **Success Labels** | overperformed (≥1.2), on_target (≥0.8), partial_success (≥0.4), failure (<0.4) |
| **Confidence Adjustment** | +0.05 (overperform), +0.02 (on-target), -0.05 (partial), -0.12 (failure) |
| **Tables** | `acc_di_execution`, `acc_di_outcome`, `acc_di_learning` |
| **Current State** | 50K growth opportunities, 80 outcomes tracked, 2 learning records (early stage) |
| **Verdict** | FREEZE (operational, accumulating data) |

#### Module 8: Catalog Health

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/intelligence/catalog_health.py` |
| **Model Type** | Rule-based scoring (0-100 per listing) |
| **Scoring** | Status (20pts) + No issues (15pts) + Not suppressed (15pts) + Title (10pts) + Image (10pts) + Price (10pts) + Content completeness (20pts) |
| **Content Completeness** | Title (4pts) + Bullets (2pts each, max 10) + Description (3pts) + Keywords (3pts) = max 20 |
| **Output Tables** | `acc_listing_health_snapshot`, `acc_listing_field_diff` |
| **Status Scores** | ACTIVE: 20, INACTIVE: 10, INCOMPLETE: 5, SUPPRESSED/DELETED: 0 |
| **Scheduler** | `catalog-health-snapshot-daily` at 03:00 |
| **Verdict** | ACTIVE |

#### Module 9: BuyBox Radar

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/intelligence/buybox_radar.py` |
| **Model Type** | Trend computation — daily win-rate aggregation |
| **Win Rate** | `win_rate = snapshots_won / snapshots_total × 100` (computed column in SQL) |
| **Input** | `acc_competitor_offer` — per-seller price snapshots via SP-API GetItemOffers |
| **Output** | `acc_buybox_trend` — daily per-SKU/marketplace aggregation |
| **Metrics** | avg_our_price, avg_buybox_price, avg_price_gap_pct, num_competitors, lowest_competitor_price |
| **Scheduler** | `buybox-trend-computation` at 03:30 |
| **Verdict** | ACTIVE |

#### Module 10: Content Optimization

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/intelligence/content_optimization.py` |
| **Model Type** | Rule-based scoring (0-100) + SEO gap analysis |
| **Sub-Scores** | Title (25w) + Bullets (25w) + Description (15w) + Keywords (15w) + Images (10w) + A+ (10w) |
| **Title Scoring** | Optimal: 100-150 chars, Good: 80-200, Short penalty: <80, Long penalty: >200 |
| **Bullet Scoring** | Ideal: 5 bullets, 50-256 chars each, optimal 120 |
| **SEO Analysis** | keyword_coverage_pct, missing_keywords, keyword_density, title_has_brand, title_has_primary_kw |
| **Data Sources** | `acc_listing_state`, `acc_co_versions`, Brand Analytics search terms |
| **Output Tables** | `acc_content_score`, `acc_seo_analysis`, `acc_content_score_history` |
| **A/B Testing** | Extended in `content_ab_testing.py` — experiment lifecycle (draft→running→concluded), multi-language generation |
| **Scheduler** | `content-scoring-daily` at 05:30 |
| **Verdict** | ACTIVE |

#### Module 11: Taxonomy Predictions

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/services/taxonomy.py` |
| **Model Type** | Classification — multi-source prediction pipeline |
| **Prediction Pipeline** | 1) Exact PIM match (internal_sku/ean/asin → confidence 0.93-0.98), 2) Registry lookup (SKU/ASIN/EAN → confidence 0.90-0.95), 3) Title similarity matching via difflib |
| **Output Fields** | suggested_brand, suggested_category, suggested_product_type |
| **Confidence Tiers** | PIM exact: 0.98 (internal_sku), 0.95 (EAN), 0.93 (ASIN); Registry: 0.95 (SKU), 0.93 (ASIN), 0.90 (EAN) |
| **Current Data** | 8,897 taxonomy predictions stored |
| **Targets** | Products with missing brand, category, or subcategory in `acc_product` |
| **Reference Pool** | Mapped products with known taxonomy + `acc_amazon_listing_registry` |
| **Verdict** | ACTIVE |

#### Module 12: Profit Engine ML Components

| Attribute | Detail |
|-----------|--------|
| **File** | `apps/api/app/services/profit_engine.py` (facade) → `app/intelligence/profit/` (4 sub-modules) |
| **Sub-Modules** | `helpers.py` (utilities), `cost_model.py` (COGS resolution), `calculator.py` (CM2/NP allocation), `query.py` (API queries) |
| **Model Type** | Computational (SQL-based) — not ML in the traditional sense, but complex multi-step calculation |
| **COGS Resolution** | 8-level priority chain via `acc_purchase_price` + TKL file parsing + Google Sheets integration |
| **CM1 Calculation** | Revenue - Amazon fees - COGS - shipping (per order line) |
| **CM2 Allocation** | CM1 - allocated FBA component costs (storage, aged, removal, etc.) per marketplace weight |
| **NP Allocation** | CM2 - allocated overhead costs |
| **Feature Engineering** | FX rate conversion (NBP/ECB), finance charge classification (45 charge types), FBA component classification |
| **Confidence Scoring** | Based on COGS source quality and fee data completeness |
| **Output** | `acc_order_line` computed fields, `acc_profit_daily_snapshot` |
| **Verdict** | ACTIVE (primary revenue-driving module) |

### 1.3 Data Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Data Pipeline — Source to Intelligence               │
│                                                                         │
│  INGESTION LAYER (Scheduler Jobs)                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │ SP-API Orders │  │ SP-API Ads   │  │ Netfox ERP   │  │ Brand      │ │
│  │ (15min cycle) │  │ (4h cycle)   │  │ (02:00 COGS) │  │ Analytics  │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬─────┘ │
│         │                  │                  │                  │       │
│  STORAGE LAYER (Azure SQL — 187 tables)                                 │
│  ┌──────▼───────┐  ┌──────▼───────┐  ┌──────▼───────┐  ┌──────▼─────┐ │
│  │ acc_order     │  │acc_ads_      │  │acc_purchase_ │  │acc_search_ │ │
│  │ acc_order_line│  │campaign_day  │  │price         │  │term_monthly│ │
│  │ acc_finance_  │  │acc_ads_      │  │              │  │            │ │
│  │ transaction   │  │product_day   │  │              │  │            │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬─────┘ │
│         │                  │                  │                  │       │
│  FEATURE ENGINEERING (Daily 02:00-05:00 batch window)                   │
│  ┌──────▼──────────────────▼──────────────────▼──────────────────▼─────┐│
│  │  Profit Engine: FX conversion, fee classification, COGS resolution  ││
│  │  Profitability Rollup: SKU-level daily aggregation                  ││
│  │  Seasonality: Monthly metric MERGE + index computation              ││
│  └──────────────────────────┬──────────────────────────────────────────┘│
│                             │                                           │
│  INTELLIGENCE LAYER (Daily 03:00-07:00)                                 │
│  ┌──────────────────────────▼──────────────────────────────────────────┐│
│  │  03:00  Catalog Health snapshot                                     ││
│  │  03:30  BuyBox trend computation                                    ││
│  │  04:00  Repricing proposal generation                               ││
│  │  04:30  Seasonality monthly build                                   ││
│  │  05:00  Profit calc + Inventory risk + Repricing analytics          ││
│  │  05:30  Content scoring                                             ││
│  │  07:00  Decision Intelligence outcome evaluation                    ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                         │
│  ON-DEMAND INTELLIGENCE (Real-time, user-triggered)                     │
│  ┌─────────────────────┐  ┌─────────────────────┐                      │
│  │  AI Recommendations │  │  AI Product Matcher  │                      │
│  │  (OpenAI API call)  │  │  (OpenAI + fuzzy)    │                      │
│  └─────────────────────┘  └─────────────────────┘                      │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.4 Feature Engineering Framework

Feature engineering in ACC is **SQL-native** — all transformations happen via T-SQL inside Azure SQL Server, not in Python/pandas. This is a deliberate architectural choice for a solo developer managing 26.6M+ rows.

**Key Feature Pipelines:**

| Pipeline | Source Tables | Transform | Output |
|----------|--------------|-----------|--------|
| **COGS Resolution** | `acc_purchase_price` (8-level priority), TKL files, Google Sheets | Priority chain: manual > AI-matched > TKL > historical | `netto_purchase_price_pln` per SKU |
| **FX Conversion** | NBP/ECB daily rates | `_fx_case()` SQL CASE expressions | `amount_pln` on all financial records |
| **Fee Classification** | `acc_finance_transaction.charge_type` (45 types) | `_classify_finance_charge()` → bucket mapping | CM1/CM2 cost pool allocation |
| **FBA Component Split** | `acc_finance_transaction` grouped by charge_type | `_classify_fba_component()` → 10 buckets | CM2 allocation (storage, aged, removal, etc.) |
| **Velocity Features** | `acc_order_line` (daily aggregation) | 7d/30d rolling window, coefficient of variation | Inventory risk, repricing inputs |
| **Seasonality Index** | `acc_sku_profitability_rollup` + Brand Analytics | Monthly average / global average | demand_index, search_demand_index |
| **Refund Rate** | `acc_order` (is_refund flag) | Period ratio vs baseline ratio | Spike detection |
| **BuyBox Win Rate** | `acc_competitor_offer` (snapshots) | Daily snapshots_won / snapshots_total | `acc_buybox_trend.win_rate` (persisted computed column) |

**Data Quality Gates (pre-inference):**
- COGS coverage: 96.0% (above 90% threshold for Profit Engine)
- FBA fee population: 30.1% (below 60% threshold — flagged as critical gap)
- Order freshness: 15-minute cadence (adequate for daily intelligence)
- Ads data freshness: 69-93h stale (pipeline issue — impacts CM2 accuracy)

### 1.5 Inference Strategy (Real-time vs Batch)

| Module | Inference Pattern | Latency Requirement | Trigger |
|--------|------------------|---------------------|---------|
| AI Recommendations | **Real-time** (on-demand) | <10s (OpenAI latency) | User request via `/ai/generate` |
| AI Product Matcher | **Real-time** (batch within request) | <30s (multi-product batch) | Manual trigger or scheduler |
| Repricing Engine | **Batch** (daily 04:00) | N/A (pre-computed) | APScheduler cron |
| Refund Anomaly | **Batch** (scheduled) | N/A | Periodic detection job |
| Inventory Risk | **Batch** (daily 05:00) | N/A | APScheduler cron |
| Seasonality | **Batch** (daily/weekly/monthly) | N/A | APScheduler cron |
| Decision Intelligence | **Batch** (daily/weekly/monthly) | N/A | APScheduler cron |
| Catalog Health | **Batch** (daily 03:00) | N/A | APScheduler cron |
| BuyBox Radar | **Batch** (daily 03:30) | N/A | APScheduler cron |
| Content Optimization | **Batch** (daily 05:30) | N/A | APScheduler cron |
| Taxonomy Predictions | **Batch** | N/A | Manual/scheduled |
| Profit Engine | **Hybrid** — batch daily + on-demand query | <5s for PPT query | Daily calc + API request |

**Caching Strategy:**
- Profit Engine uses `_RESULT_CACHE` (in-memory dict with TTL) for expensive SQL aggregations
- Redis caching for API responses (TanStack Query on frontend, 5-minute stale time)
- No ML model caching needed (no self-hosted models)

### 1.6 LLM Abstraction Layer Design

**Current State:** Direct coupling to OpenAI via `AsyncOpenAI` client in two services.

**Recommended Abstraction (Phase 2):**

```python
# Proposed: apps/api/app/intelligence/llm/base.py
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class LLMResponse:
    content: str           # Raw response text
    parsed: dict          # Parsed JSON (if json_object format)
    prompt_tokens: int
    completion_tokens: int
    model: str
    provider: str         # "openai", "anthropic", "azure_openai", "local"
    latency_ms: float

class LLMProvider(ABC):
    @abstractmethod
    async def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        json_mode: bool = False,
    ) -> LLMResponse: ...

    @abstractmethod
    async def health_check(self) -> bool: ...

class OpenAIProvider(LLMProvider):
    """Current implementation — extracted from ai_service.py"""

class AzureOpenAIProvider(LLMProvider):
    """Azure OpenAI — same models, different endpoint (data residency)"""

class AnthropicProvider(LLMProvider):
    """Anthropic Claude — fallback provider"""

# Factory
def get_llm_provider(provider: str = "openai") -> LLMProvider:
    providers = {
        "openai": OpenAIProvider,
        "azure_openai": AzureOpenAIProvider,
        "anthropic": AnthropicProvider,
    }
    return providers[provider]()
```

**Migration Path:**
1. **Phase 1 (now):** Keep `AsyncOpenAI` directly — no abstraction overhead for solo developer
2. **Phase 2 (Month 6):** Extract `LLMProvider` interface when adding second provider or Azure OpenAI
3. **Phase 3 (Month 12):** Full provider registry with fallback chains and cost routing

**Rationale:** Architecture Spec verdict is "KEEP OpenAI, ADD LLM abstraction layer, do NOT self-host LLMs." The abstraction should be introduced when there's a concrete second provider, not preemptively.

### 1.7 Model Selection Rationale & Recommendations

| Module | Current Approach | Why This Works | Potential Improvement | Priority |
|--------|-----------------|----------------|----------------------|----------|
| AI Recommendations | GPT-5.2 | NLU needed for actionable text generation | Fine-tune system prompts per rec type; add few-shot examples | LOW |
| Product Matcher | GPT-5.2 + fuzzy | Bundle decomposition requires NLU | Add embedding-based pre-filter (reduce LLM calls by ~60%) | MEDIUM |
| Repricing | Rule-based (4 strategies) | Transparent, auditable, predictable | Add ML-based demand elasticity estimation (Phase 3) | LOW |
| Refund Anomaly | Statistical thresholds | Simple, interpretable, low false-positive rate | Isolation Forest for multi-dimensional anomaly detection | LOW |
| Inventory Risk | Normal approx. + heuristics | Mathematically sound for Poisson-like demand | Bayesian demand forecasting with seasonal priors | MEDIUM |
| Seasonality | Monthly aggregation + index | Robust for slow-moving consumer goods | Prophet/ARIMA for time-series decomposition (Phase 3) | LOW |
| Decision Intelligence | Feedback loop | Self-improving, accumulates institutional knowledge | Increase sample threshold from 5→20 for recalibration | HIGH |
| Catalog Health | Rule-based scoring | Deterministic, easy to explain to sellers | Add ML-based image quality scoring | LOW |
| BuyBox Radar | Trend aggregation | Simple win-rate metric is industry standard | Add competitor behavior prediction (logistic regression) | LOW |
| Content Optimization | Rule-based + SEO | Transparent scoring sellers can act on | GPT-based title/bullet generation (already in A/B testing module) | MEDIUM |
| Taxonomy | Exact match + fuzzy | High accuracy for structured data (0.90-0.98) | Add embedding similarity as third tier | LOW |
| Profit Engine | SQL computation | 20K+ LoC SQL investment, performant at scale | No ML needed — computational, not predictive | NONE |

---

## 2. AI Ethics and Safety Framework

### 2.1 Responsible AI Principles

ACC operates in a **commercial e-commerce context** where AI decisions affect:
- Product pricing (direct revenue impact)
- Inventory purchasing (capital allocation)
- Product matching (catalog accuracy)
- Content recommendations (listing visibility)

**Core Principles:**

1. **Transparency:** Every AI decision must be explainable. Repricing shows reason_code + reason_text. Recommendations include confidence_score.
2. **Human Oversight:** Automated pricing changes require approval workflow (unless auto-approve threshold ≤5%). Product matching requires explicit user confirmation.
3. **Harm Prevention:** Guardrails prevent catastrophic pricing errors (min/max price, margin floor, daily change cap).
4. **Data Minimization:** Only sales/operational data processed. No consumer behavioral profiling beyond Amazon-provided aggregates.
5. **Auditability:** All AI actions logged with timestamps, model versions, and input data for forensic review.

### 2.2 Automated Pricing Guardrails

The repricing engine implements a **defense-in-depth** approach to prevent harmful pricing decisions:

```
LAYER 1 — Strategy Validation
  ├── strategy_type must be in VALID_STRATEGY_TYPES
  ├── Parameters validated at strategy creation time
  └── Only active strategies participate in computation

LAYER 2 — Computation Guardrails (per-strategy)
  ├── min_price / max_price — absolute bounds
  ├── min_margin_pct — margin floor (prevent selling at loss)
  └── max_daily_change_pct — caps price swing (default 10%)

LAYER 3 — Post-Computation Guardrails
  ├── enforce_margin_guardrail() — recalculates floor price if margin violated
  ├── enforce_daily_change_guardrail() — clamps to max daily Δ%
  └── guardrail_applied field records which guardrail fired

LAYER 4 — Approval Workflow
  ├── Default: requires_approval = True (human review)
  ├── Auto-approve only if: requires_approval = False AND change ≤ 5%
  └── All proposals expire if not approved (expires_at field)

LAYER 5 — Execution Audit
  ├── status lifecycle: proposed → approved → executed → (failed?)
  ├── approved_by, approved_at, executed_at tracked
  └── error_message captured on failure
```

**Safety Invariants:**
- A repricing proposal can NEVER set a price below `min_price` or above `max_price`
- A repricing proposal can NEVER reduce margin below `min_margin_pct`
- A repricing proposal can NEVER change price by more than `max_daily_change_pct` in one cycle
- ALL repricing proposals are persisted in `acc_repricing_execution` with full audit trail

### 2.3 Human-in-the-Loop Requirements

| Module | HITL Type | Requirement |
|--------|-----------|-------------|
| AI Product Matcher | **Mandatory approval** | ALL match suggestions start as `status='pending'`. Mapping applied ONLY after user sets `status='approved'`. |
| Repricing Engine | **Default approval** | `requires_approval = True` by default. Auto-approval only for changes ≤5% when explicitly opted in. |
| AI Recommendations | **Advisory only** | Recommendations are informational. No automated execution. User decides whether to act. |
| Refund Anomaly | **Review required** | Anomalies marked as `status='open'`. Reimbursement cases require manual filing with Amazon. |
| Decision Intelligence | **Implicit feedback** | User implicitly provides feedback by accepting/executing growth opportunities. System learns from outcomes. |
| All Other Modules | **None required** | Scoring and analytics modules produce informational output only. |

### 2.4 Bias Detection & Mitigation

**Marketplace Bias:** Intelligence modules operate across 10 EU marketplaces. Risk: models trained primarily on DE/PL data may underperform on smaller markets (SE, NL, BE).

**Mitigation:**
- Seasonality profiles computed per marketplace (not global averages)
- Inventory risk uses per-SKU-per-marketplace velocity (no cross-marketplace pooling)
- Content optimization scoring thresholds are language-neutral (character counts, not word counts)
- Repricing strategies are per-marketplace (currency-aware)

**Category Bias:** KADAX product catalog is primarily kitchenware/household goods. Risk: models may not generalize to new product categories.

**Mitigation:**
- Taxonomy predictions use multi-source matching (not category-specific rules)
- Content scoring uses Amazon-generic thresholds (not category-specific)
- Repricing algorithms are product-agnostic (price/cost/margin based)

**Monitoring (recommended Phase 2):**
- Track per-marketplace accuracy for repricing proposals
- Track per-category false-positive rate for anomaly detection
- Monitor Decision Intelligence `win_rate` and `prediction_accuracy` per `opportunity_type`

### 2.5 Data Privacy & PII Handling

**PII Inventory:**

| Data Element | Location | Classification | Handling |
|------------|----------|----------------|----------|
| Buyer identifiers | `acc_serial_returner.buyer_identifier` | PII (pseudonymized) | Amazon-provided hash, not real identity |
| Seller credentials | `acc_seller_credential.encrypted_value` | Secret | Fernet-encrypted at rest (SHA-256 derived key from SECRET_KEY) |
| API keys | `.env` file (OPENAI_API_KEY, SP_API_*) | Secret | Environment variables, `.gitignore`-protected |
| Sales data | All `acc_*` tables | Business Confidential | TLS 1.2 in-transit (Azure SQL), encrypted at rest (Azure) |
| Employee names | `approved_by`, `executed_by` fields | Internal PII | Minimal — only internal staff names |

**GDPR Compliance:**
- No EU consumer personal data stored beyond Amazon-provided aggregates
- `buyer_identifier` in serial returner detection is Amazon's pseudonymized ID
- Right-to-erasure: Not applicable (no direct consumer data collection)
- Data residency: Azure SQL in EU region (Western Europe)

**Recommendation:** Add data retention policies — archive `acc_finance_transaction` records older than 7 years, `acc_competitor_offer` older than 1 year.

### 2.6 Model Explainability

| Module | Explainability Mechanism |
|--------|------------------------|
| AI Recommendations | `confidence_score` (0-1) + `action_items` (human-readable list) + full JSON response stored |
| Product Matcher | `confidence` (0-100) + `reasoning` (Polish text explanation) + `bom` (component breakdown) |
| Repricing | `reason_code` + `reason_text` + `guardrail_applied` + full price computation chain |
| Refund Anomaly | `spike_ratio` + `baseline_rate` vs `current_rate` + `severity` classification |
| Inventory Risk | 3-component breakdown (stockout_prob_7d, overstock_holding_cost, aging_risk) + `risk_tier` |
| Seasonality | `demand_index` (1.0 = average, >1 = above average) + monthly breakdown |
| Decision Intelligence | `success_score` + `impact_score` + `confidence_adjustment` + `delta_json` (baseline vs actual) |
| Catalog Health | 6-component score breakdown (status, issues, suppression, content) |
| Content Optimization | 6 sub-scores (title, bullets, description, keywords, images, A+) + `issues_json` + `recommendations_json` |
| Taxonomy | `confidence` + `source` (pim_exact, registry, title_similarity) + `reason` + `evidence` |
| Profit Engine | `confidence_level` based on COGS source priority + data quality metrics per SKU |

---

## 3. Model Monitoring and Retraining Plan

### 3.1 Monitoring Metrics per Model Type

**LLM Models (AI Recommendations, Product Matcher):**

| Metric | Target | Alert Threshold |
|--------|--------|----------------|
| API success rate | >99% | <95% |
| Mean response latency | <5s | >10s |
| Token usage per request (avg) | <2,000 | >3,500 |
| JSON parse success rate | >99% | <97% |
| Cost per request (avg) | <$0.02 | >$0.05 |
| User acceptance rate (recommendations) | >60% | <30% |
| Match accuracy (product matcher) | >80% approved | <60% approved |

**Algorithmic Models (Repricing, Catalog Health, Content Optimization):**

| Metric | Target | Alert Threshold |
|--------|--------|----------------|
| Proposals generated per day | >10 | 0 for 2+ days |
| Auto-approval rate | 20-60% | >80% (too permissive) or 0% (too strict) |
| Guardrail activation rate | <30% | >60% (strategies misconfigured) |
| Margin improvement (post-repricing) | >0.5pp | Negative for 7+ days |
| Health score distribution | Normal around 65-80 | Bimodal (data quality issue) |

**Statistical Models (Inventory Risk, Refund Anomaly, Seasonality):**

| Metric | Target | Alert Threshold |
|--------|--------|----------------|
| Stockout prediction accuracy (7d) | >70% | <50% |
| False positive rate (anomalies) | <20% | >40% |
| Anomaly detection latency | <24h | >72h |
| Seasonality index std dev | <0.3 | >0.5 (unstable indices) |
| Risk tier distribution | 60% low, 25% medium, 10% high, 5% critical | >20% critical |

### 3.2 Drift Detection Strategy

**Concept Drift** (business environment changes):
- Monitor monthly revenue distribution across marketplaces (detect expansion/contraction)
- Track new product category introductions (taxonomy model may need retraining)
- Monitor competitor count per ASIN (market structure changes)

**Data Drift** (input distribution changes):
- Track `acc_order_line` daily volume — sudden drops indicate pipeline issues, not model drift
- Monitor COGS price distribution — large shifts indicate supplier changes
- Track `acc_finance_transaction.charge_type` distribution — Amazon fee structure changes

**Implementation (recommended):**
```sql
-- Example: Weekly data drift check for order volume
SELECT
    DATEPART(wk, purchase_date) AS week_num,
    COUNT(*) AS order_count,
    AVG(CAST(item_price_amount AS FLOAT)) AS avg_item_price,
    STDEV(CAST(item_price_amount AS FLOAT)) AS std_item_price
FROM acc_order WITH (NOLOCK)
WHERE purchase_date >= DATEADD(month, -3, GETUTCDATE())
GROUP BY DATEPART(wk, purchase_date)
ORDER BY week_num DESC
```

### 3.3 Retraining Triggers & Schedules

| Module | Retraining Type | Schedule | Trigger |
|--------|----------------|----------|---------|
| Seasonality | Full rebuild | Monthly (1st, 04:30) | `seasonality-build-monthly-daily` |
| Seasonality Profiles | Profile reclassification | Weekly (Sun, 05:00) | `seasonality-recompute-profiles-weekly` |
| Seasonality Opportunities | Opportunity detection | Weekly (Mon, 05:30) | `seasonality-detect-opps-weekly` |
| Decision Intelligence | Outcome evaluation | Daily (07:00) | `decision-outcome-evaluation-daily` |
| Decision Intelligence | Learning aggregation | Weekly (Sun, 08:00) | `decision-learning-weekly` |
| Decision Intelligence | Model recalibration | Monthly (1st, 09:00) | `decision-model-recalibration-monthly` |
| Inventory Risk | Full recomputation | Daily (05:00) | `inventory-risk-computation` |
| Repricing | Proposal regeneration | Daily (04:00) | `repricing-proposal-computation` |
| Catalog Health | Snapshot refresh | Daily (03:00) | `catalog-health-snapshot-daily` |
| Content Scoring | Score refresh | Daily (05:30) | `content-scoring-daily` |
| BuyBox | Trend recomputation | Daily (03:30) | `buybox-trend-computation` |

**Event-Based Triggers (recommended Phase 2):**
- New product added → trigger taxonomy prediction + catalog health scoring
- Large price change detected → trigger repricing analytics update
- COGS update → trigger profit recalculation for affected SKUs
- Anomaly detected → trigger Decision Intelligence re-evaluation

### 3.4 A/B Testing Framework

ACC includes a built-in A/B testing framework in `content_ab_testing.py`:

**Current Capabilities:**
- Experiment lifecycle: draft → running → paused → concluded → cancelled
- Variant management: control + challenger(s) per experiment
- Metrics: conversion_rate, CTR, revenue, orders
- Winner declaration based on primary metric
- Multi-language content generation with quality validation

**Extension for ML A/B Testing (recommended Phase 3):**

```
Experiment: repricing_strategy_v2
  Control:   buybox_match (current)
  Variant A: competitive_undercut (1% below)
  Variant B: velocity_based (dynamic)
  
  Allocation: 33/33/33 by SKU hash
  Duration: 30 days
  Primary metric: profit_improvement_pct
  Guard metric: win_rate does not drop below 60%
  
  Statistical test: Two-sample t-test
  Minimum sample: 100 repricing events per variant
  Significance level: α = 0.05
```

### 3.5 Decision Intelligence Enhancement

The Decision Intelligence feedback loop (Module 7) is ACC's most architecturally sophisticated intelligence component. Current state and enhancement plan:

**Current State (2 learning records, 80 outcomes):**
- Phase 1 (Record): Operational — baseline snapshots captured
- Phase 2 (Monitor): Operational — outcomes evaluated at 7/14/30/60 day windows
- Phase 3 (Aggregate): Operational — per-type statistics computed weekly
- Phase 4 (Adjust): Operational — monthly recalibration runs

**Enhancement Plan:**

| Enhancement | Description | Priority | Phase |
|------------|-------------|----------|-------|
| Increase sample threshold | Require 20+ outcomes (not 5) before recalibrating confidence weights | HIGH | Now |
| Add per-marketplace learning | Separate learning records per opportunity_type × marketplace | MEDIUM | Phase 2 |
| Bayesian updating | Replace linear confidence adjustment with Bayesian posterior updates | LOW | Phase 3 |
| Cross-type correlation | Detect when PRICE_INCREASE success predicts ADS_SCALE_UP success | LOW | Phase 3 |
| Automated opportunity sizing | Use learning data to auto-adjust `estimated_profit_uplift` for new opportunities | MEDIUM | Phase 2 |

**Confidence Adjustment Tuning:**

Current adjustments are conservative (hardcoded):
```
overperformed (≥1.2):      +0.05
on_target (≥0.8):          +0.02
partial_success (≥0.4):    -0.05
failure (<0.4):            -0.12
```

Recommended: Make adjustment factors a function of sample size (more data = larger adjustments):
```
adjustment = base_adjustment × min(1.0, sample_size / 50)
```

### 3.6 Alert Thresholds

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| OpenAI API down | 3+ consecutive failures | CRITICAL | Disable AI recommendations, notify developer |
| OpenAI quota exceeded | `quota_exceeded` error class | HIGH | Pause AI matcher, switch to fuzzy-only matching |
| Repricing guardrail storm | >60% proposals hit guardrails | WARNING | Review strategy configurations |
| Anomaly false positive spike | >5 dismissed anomalies in 7 days | WARNING | Adjust spike thresholds |
| Inventory risk all-critical | >20% SKUs in critical tier | HIGH | Data quality check (velocity data may be stale) |
| Decision Intel accuracy drop | prediction_accuracy <50% for any type | WARNING | Increase monitoring windows, review opportunity sizing |
| Profit Engine COGS gap | COGS coverage drops below 90% | HIGH | Check purchase price sync, alert for manual pricing |
| Seasonality index unstable | Standard deviation of demand_index >0.5 | WARNING | Check data completeness, extend lookback window |

### 3.7 Data Quality Gates

All intelligence modules should validate input data quality before inference. Current and recommended gates:

**Implemented Gates:**
- Profit Engine: COGS existence check, FBA fee gap detection, confidence scoring based on data completeness
- Refund Anomaly: `MIN_ORDERS_FOR_SPIKE = 5` (minimum sample size)
- Inventory Risk: Velocity CV fallback (if <3 data points, CV=0, deterministic check)
- Seasonality: `sku NOT LIKE 'amzn.gr.%'` filter (gift cards and promotional items excluded)

**Recommended Additional Gates:**

```python
# Proposed: apps/api/app/intelligence/data_quality_gate.py
def check_data_freshness(table: str, max_hours: int = 48) -> bool:
    """Gate: reject inference if source data is stale."""
    # SELECT MAX(updated_at) FROM {table} WITH (NOLOCK)
    pass

def check_sample_size(sku: str, min_orders: int = 10) -> bool:
    """Gate: reject inference if insufficient history."""
    pass

def check_completeness(sku: str, required_fields: list[str]) -> float:
    """Gate: return completeness ratio (0-1) for required fields."""
    pass
```

---

## 4. Integration Points with Main Application

### 4.1 API Layer Integration Map

| Intelligence Module | API Router | Endpoints | HTTP Methods |
|-------------------|------------|-----------|-------------|
| AI Recommendations | `api/v1/ai.py` | `/ai/recommendations`, `/ai/summary`, `/ai/generate`, `/ai/recommendations/{id}` | GET, POST, PATCH |
| AI Product Matcher | `api/v1/product_match.py` | `/product-match/run`, `/product-match/suggestions`, `/product-match/approve` | POST, GET, PATCH |
| Repricing Engine | `api/v1/repricing.py` | `/repricing/*` — 11 endpoints (strategies CRUD, proposals, approve, analytics) | GET, POST, PUT, PATCH |
| Refund Anomaly | `api/v1/returns.py` | `/returns/anomalies`, `/returns/serial-returners`, `/returns/reimbursement-cases` | GET, POST, PATCH |
| Inventory Risk | `api/v1/inventory.py` | `/inventory/risk-scores`, `/inventory/risk-summary` | GET |
| Seasonality | `api/v1/seasonality.py` | `/seasonality/*` — 14 endpoints (profiles, opportunities, indices, calendar) | GET, POST |
| Decision Intelligence | `api/v1/strategy.py`, `api/v1/outcomes.py` | `/strategy/decisions/*` — 9 endpoints | GET, POST, PUT |
| Catalog Health | `api/v1/catalog_health.py` | `/catalog-health/scorecard`, `/catalog-health/snapshots` | GET |
| BuyBox Radar | `api/v1/buybox_radar.py` | `/buybox/trends`, `/buybox/competitors`, `/buybox/analysis` | GET |
| Content Optimization | `api/v1/content_optimization.py` | `/content/scores`, `/content/seo-analysis`, `/content/experiments` | GET, POST |
| Taxonomy | `api/v1/taxonomy.py` | `/taxonomy/predictions`, `/taxonomy/review-queue` | GET, POST |
| Profit Engine | `api/v1/profit_v2.py` | `/profit/*` — product table, drilldown, KPIs, what-if, tasks, fee diagnostics | GET, POST, PUT |
| Intelligence Dashboard | `api/v1/intelligence.py` | `/intelligence/dashboard`, `/intelligence/funnel`, `/intelligence/forecast-accuracy` | GET |

### 4.2 Database Schema (ML I/O Tables)

**Input Tables (read by intelligence modules):**

| Table | Rows | Growth | Used By |
|-------|------|--------|---------|
| `acc_order` | ~850K | ~15K/mo | Refund Anomaly, Profit, Seasonality |
| `acc_order_line` | ~1.1M | ~20K/mo | Profit, Inventory Risk, Repricing |
| `acc_finance_transaction` | ~8M | ~300K/mo | Profit (CM2 pools), Refund Anomaly (fee spikes) |
| `acc_ads_campaign_day` | ~800K | ~50K/mo | Profit (CM2 ad spend), Decision Intelligence |
| `acc_ads_product_day` | ~2M | ~200K/mo | Content Optimization (ad performance) |
| `acc_product` | ~4.3K | Slow | Taxonomy, Product Matcher, all modules |
| `acc_purchase_price` | ~12K | ~200/mo | Profit (COGS), Repricing (margin calc) |
| `acc_listing_state` | ~4K | Slow | Catalog Health, Content Optimization |
| `acc_co_versions` | ~4K | Slow | Content Optimization (full content) |
| `acc_search_term_monthly` | Variable | Monthly | Seasonality (Brand Analytics blend), Content (SEO) |
| `acc_sku_profitability_rollup` | Variable | Daily | Decision Intelligence (baseline/actual), Seasonality |
| `acc_competitor_offer` | Growing | Per snapshot | BuyBox Radar, Repricing |

**Output Tables (written by intelligence modules):**

| Table | Module | Purpose |
|-------|--------|---------|
| `acc_inventory_risk_score` | Inventory Risk | Daily risk scores per SKU/marketplace |
| `acc_refund_anomaly` | Refund Anomaly | Detected refund spike anomalies |
| `acc_serial_returner` | Refund Anomaly | Serial returner patterns |
| `acc_reimbursement_case` | Refund Anomaly | Reimbursement claim tracking |
| `acc_repricing_strategy` | Repricing | Strategy definitions |
| `acc_repricing_execution` | Repricing | Price change proposals + audit trail |
| `acc_repricing_analytics` | Repricing | Daily execution metrics |
| `seasonality_monthly_metrics` | Seasonality | Monthly SKU/category aggregations |
| `seasonality_index_cache` | Seasonality | Demand index per entity/month |
| `acc_seasonality_profile` | Seasonality | SKU seasonality classification |
| `acc_seasonality_opportunity` | Seasonality | Detected seasonal opportunities |
| `opportunity_execution` | Decision Intelligence | Execution records with baseline snapshots |
| `opportunity_outcome` | Decision Intelligence | Outcome evaluations at monitoring windows |
| `decision_learning` | Decision Intelligence | Per-type accuracy & ROI statistics |
| `acc_listing_health_snapshot` | Catalog Health | Daily health score snapshots |
| `acc_listing_field_diff` | Catalog Health | Listing field change tracking |
| `acc_buybox_trend` | BuyBox Radar | Daily win-rate aggregation |
| `acc_content_score` | Content Optimization | Content quality scores |
| `acc_seo_analysis` | Content Optimization | SEO keyword analysis |
| `acc_content_score_history` | Content Optimization | Daily score snapshots |
| `acc_content_experiment` | Content A/B Testing | Experiment definitions |
| `acc_content_variant` | Content A/B Testing | Variant metrics |
| `acc_product_match_suggestion` | Product Matcher | AI match suggestions (pending/approved) |
| `acc_taxonomy_prediction` | Taxonomy | Brand/category/type predictions |

### 4.3 Scheduler Pipeline Integration

**Daily Batch Window (02:00-07:00 UTC):**

```
02:00  ─── Purchase price sync (COGS from Netfox ERP)
02:30  ─── FX rate refresh (NBP/ECB)
03:00  ─── Catalog Health snapshot ──────────────────── acc_listing_health_snapshot
03:00  ─── Finance transaction sync
03:30  ─── BuyBox trend computation ─────────────────── acc_buybox_trend
04:00  ─── Repricing proposal computation ───────────── acc_repricing_execution
04:00  ─── Inventory sync (FBA)
04:15  ─── Repricing auto-approve & execute ─────────── (proposals with auto_approved=1)
04:30  ─── Seasonality monthly build (daily) ────────── seasonality_monthly_metrics
05:00  ─── Profit calculation ───────────────────────── acc_order_line (computed), acc_profit_daily_snapshot
05:00  ─── Inventory risk computation ───────────────── acc_inventory_risk_score
05:00  ─── Repricing daily analytics ────────────────── acc_repricing_analytics
05:30  ─── Content scoring ──────────────────────────── acc_content_score, acc_content_score_history
07:00  ─── Decision outcome evaluation ──────────────── opportunity_outcome
```

**Weekly Jobs:**
```
Sunday 05:00  ─── Seasonality profile recompute ─────── acc_seasonality_profile
Sunday 08:00  ─── Decision learning aggregation ─────── decision_learning
Monday 05:30  ─── Seasonality opportunity detection ─── acc_seasonality_opportunity
```

**Monthly Jobs:**
```
1st 09:00  ─── Decision model recalibration ──────────── decision_learning (weight updates)
```

### 4.4 Frontend Surface Map

| Frontend Page | Intelligence Module(s) | Data Displayed |
|--------------|----------------------|----------------|
| **Product Profit Table** (`ProductProfitTable.tsx`) | Profit Engine | CM1/CM2/NP per ASIN, margin %, revenue, COGS, fees |
| **Intelligence Dashboard** | All modules | Unified funnel: opportunities → decisions → outcomes |
| **Repricing Console** | Repricing Engine | Strategy config, proposal list, approval workflow, analytics |
| **Inventory Risk** | Inventory Risk | Risk tier distribution, stockout alerts, replenishment plan |
| **Seasonality Calendar** | Seasonality Service | Demand index heatmap, seasonal profiles, opportunities |
| **Returns/Anomalies** | Refund Anomaly | Spike timeline, serial returner list, reimbursement tracker |
| **BuyBox Monitor** | BuyBox Radar | Win-rate trends, competitor price comparison, gap analysis |
| **Content Scoreboard** | Content Optimization | Score distribution, SEO gaps, A/B test results |
| **Catalog Health** | Catalog Health | Scorecard, health score distribution, suppression alerts |
| **AI Recommendations** | AI Recommendations | Recommendation cards with actions, confidence, token usage |
| **Product Matching** | AI Product Matcher | Suggestion queue, approve/reject workflow, match details |

### 4.5 Event Backbone Integration

ACC uses an event backbone (`event_backbone.py`) for domain event propagation. Intelligence modules integrate via:

**Event Emitters (intelligence → backbone):**
- Repricing proposal created → `repricing.proposal.created`
- Anomaly detected → `anomaly.detected`
- Risk tier changed → `inventory.risk.tier_changed`
- Opportunity identified → `strategy.opportunity.created`

**Event Consumers (backbone → intelligence):**
- Order created → triggers profit recalculation (if on-demand mode)
- COGS updated → invalidates profit cache (`_result_cache_invalidate`)
- Listing state changed → triggers catalog health re-scoring

**Recommended Enhancement (Phase 2):**
```
Domain Event → SQS Queue → Celery Worker → Intelligence Module
  order.created       → profit.recalculate(order_id)
  product.mapped      → taxonomy.predict(sku)
  cogs.updated        → profit.invalidate_cache(internal_sku)
  anomaly.critical    → notification.send(channel="slack", severity="critical")
```

### 4.6 External API Integration (OpenAI, Brand Analytics)

**OpenAI GPT-5.2:**

| Parameter | Value |
|-----------|-------|
| Client | `AsyncOpenAI` (from `openai` 1.58.1) |
| Model | `gpt-5.2` (configurable via `settings.OPENAI_MODEL`) |
| Max tokens | 4,096 (configurable via `settings.OPENAI_MAX_TOKENS`) |
| Temperature | 0.3 (hardcoded in ai_service.py) |
| Response format | `json_object` (structured output) |
| Error handling | 6 error classes with Polish error messages in product matcher |
| Retry strategy | None (recommended: exponential backoff with 3 retries) |
| Rate limiting | None (recommended: token bucket per minute based on OpenAI tier) |

**Recommended Error Handling Enhancement:**
```python
# Proposed retry wrapper
async def _call_openai_with_retry(
    client: AsyncOpenAI,
    *,
    max_retries: int = 3,
    base_delay: float = 1.0,
    **kwargs,
) -> ChatCompletion:
    for attempt in range(max_retries):
        try:
            return await client.chat.completions.create(**kwargs)
        except RateLimitError:
            delay = base_delay * (2 ** attempt)
            await asyncio.sleep(delay)
        except (APIConnectionError, APIStatusError) as e:
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(base_delay)
    raise RuntimeError("OpenAI retries exhausted")
```

**Brand Analytics (Amazon SP-API Reports):**
- Used by Seasonality Service for search term volume data
- Stored in `acc_search_term_monthly`
- Blended with internal sales data to produce `search_demand_index`
- Pull frequency: Monthly (report availability depends on Amazon)

---

## 5. Cost Projections for ML Infrastructure

### 5.1 Current Cost Baseline

**Monthly AI/ML Costs (March 2026):**

| Component | Cost/Month | Notes |
|-----------|-----------|-------|
| OpenAI API | ~$15-30 | GPT-5.2: AI recommendations (~10-20 calls/day) + product matcher (batch runs) |
| Azure SQL (ML tables storage) | Included in S3 ($150) | ~2 GB of 19.3 GB used by ML output tables |
| Compute (intelligence jobs) | Included in VM ($50) | Runs in-process with FastAPI |
| Redis (cache) | $0 (Docker) | Minimal ML use (profit cache) |
| **Total ML-specific** | **~$15-30** | On top of $308 base infrastructure |

**OpenAI Token Usage Estimate:**
- AI Recommendations: ~20 calls/day × ~1,500 tokens/call = ~30K tokens/day
- Product Matcher: ~50 products/batch × ~3,000 tokens/call = ~150K tokens/batch (weekly)
- Monthly total: ~1M tokens → ~$15-30 at GPT-5.2 pricing

### 5.2 Scaling Cost Model (10x, 100x)

| Scale Factor | Users | Products | Orders/Month | OpenAI Cost | Compute | SQL Tier | Total ML |
|-------------|-------|----------|-------------|-------------|---------|----------|----------|
| **1x (now)** | 1 | 4.3K | 15K | $15-30 | $0 (in-process) | S3 ($150) | $15-30 |
| **10x** | 10 | 43K | 150K | $100-200 | $50 (Celery worker) | S4 ($300) | $150-250 |
| **100x** | 100 | 430K | 1.5M | $500-1,000 | $300 (3 workers) | S6 ($600) | $800-1,300 |
| **1000x** | 1K | 4.3M | 15M | $2,000-5,000 | $1,500 (K8s) | Elastic ($1,200) | $3,500-6,700 |

**Key Scaling Assumptions:**
- OpenAI costs scale linearly with product count × recommendation frequency
- SQL costs driven by table growth (primarily `acc_finance_transaction` at 300K rows/mo/tenant)
- Compute costs jump at worker extraction boundaries (Phase 2, Phase 3)
- Intelligence batch jobs scale with product count, not user count

### 5.3 Cost Optimization Strategies

**Immediate (Phase 1):**

| Strategy | Expected Savings | Implementation |
|----------|-----------------|----------------|
| **Prompt caching** | 30-50% token reduction | Cache similar product analyses; skip re-recommendation for unchanged metrics |
| **Batch product matching** | 40-60% reduction | Batch 10-20 products per OpenAI call instead of 1:1 |
| **Response token limits** | 20-30% reduction | Reduce `OPENAI_MAX_TOKENS` to 2,048 for recommendations (most use <500) |
| **Conditional generation** | 50%+ reduction | Only generate recommendations when metrics change >10% from last run |

**Phase 2 (Month 6):**

| Strategy | Expected Savings | Implementation |
|----------|-----------------|----------------|
| **Model tier routing** | 40-60% reduction | Use GPT-4o-mini for low-complexity recommendations, GPT-5.2 only for complex analyses |
| **Embedding pre-filter** | 60% LLM cost reduction for matcher | Use text-embedding-3-small for candidate shortlisting before LLM matching |
| **SQL materialized views** | Reduced compute time | Pre-aggregate daily/weekly rollups for intelligence modules |

**Phase 3 (Month 12):**

| Strategy | Expected Savings | Implementation |
|----------|-----------------|----------------|
| **Azure OpenAI** | 10-20% reduction | Same models, lower latency, potential volume discount |
| **Local embeddings** | Eliminate embedding costs | sentence-transformers for product matching pre-filter |
| **Scheduled cost budgeting** | Cost ceiling guarantee | Set daily/weekly OpenAI budget limits with graceful degradation |

### 5.4 Build vs Buy Analysis

| Capability | Build | Buy | Recommendation |
|-----------|-------|-----|----------------|
| **NL Recommendations** | Fine-tuned local LLM ($2K+ GPU/mo) | OpenAI API ($15-30/mo) | **BUY** — 100x cheaper at current scale |
| **Product Matching** | Custom ML model (months of dev) | GPT-5.2 + fuzzy ($10/mo) | **BUY** — accuracy is comparable, zero training data needed |
| **Anomaly Detection** | Custom (built) | ✓ Already built | **BUILT** — simple threshold logic, no external dependency |
| **Inventory Risk** | Custom (built) | Amazon Inventory Planning? | **BUILT** — probabilistic model tailored to our data |
| **Seasonality** | Custom (built) | Prophet/ARIMA library | **BUILT** — SQL aggregation is sufficient for monthly patterns |
| **Repricing** | Custom (built) | RepricerExpress ($200+/mo) | **BUILT** — full control over strategies and guardrails |
| **Content Scoring** | Custom (built) | Helium 10 ($300+/mo) | **BUILT** — scoring logic is straightforward rules |
| **BuyBox Tracking** | Custom (built) | Keepa ($150+/mo) | **BUILT** — integrated with SP-API pricing data |

**Summary:** The build vs buy calculus strongly favors the current hybrid approach. Only the NL capabilities (recommendations, product matching) justify external API costs. All rule-based and statistical modules are more cost-effective as custom implementations.

### 5.5 Infrastructure Cost Breakdown

**Phase 1 (Now) — ML Component of $308 Total:**

```
┌──────────────────────────────────────────────────────────┐
│  Monthly Infrastructure Allocation (ML-relevant)          │
│                                                          │
│  Azure SQL S3 ($150 total)                               │
│  ├── ML output tables: ~$15 (10% of storage)             │
│  ├── ML query compute: ~$20 (13% of DTU usage)           │
│  └── Core tables (shared): $115                          │
│                                                          │
│  Azure VM ($50 total)                                    │
│  ├── Intelligence scheduler jobs: ~$5 (10% CPU time)     │
│  ├── API inference endpoints: ~$3 (6% CPU time)          │
│  └── Core API + scheduler: $42                           │
│                                                          │
│  OpenAI API: $15-30                                      │
│                                                          │
│  Total ML infrastructure: ~$53-68/month                  │
│  (17-22% of total $308)                                  │
└──────────────────────────────────────────────────────────┘
```

**Phase 2 (Month 6) — Projected ML Component of $499 Total:**

```
  Azure SQL S4: $35 (ML tables)
  Celery worker: $50 (ML batch jobs)
  OpenAI API: $50-100 (10x products)
  Total: ~$135-185/month (27-37% of $499)
```

### 5.6 ROI Analysis

**Revenue Impact per Module:**

| Module | Revenue/Profit Impact | Confidence | ROI Estimate |
|--------|----------------------|------------|-------------|
| **Profit Engine** | Core analytics — enables all pricing/margin decisions | HIGH | ∞ (foundational) |
| **Repricing Engine** | 1-3% margin improvement on repriced SKUs | MEDIUM | 10-30× (when active) |
| **Refund Anomaly** | Reimbursement recovery: est. $200-500/mo | MEDIUM | 10-20× |
| **Inventory Risk** | Reduce stockout losses: est. $500-1,000/mo | MEDIUM | 15-30× |
| **AI Recommendations** | Decision quality improvement (hard to quantify) | LOW | 3-5× |
| **Product Matcher** | Catalog completeness: enables accurate COGS | HIGH | 20-50× |
| **Seasonality** | Demand planning accuracy: reduces overstock | LOW | 5-10× |
| **Content Optimization** | Listing visibility: incremental sales lift | LOW | 3-5× |
| **BuyBox Radar** | Competitive intelligence: informs repricing | MEDIUM | 5-10× |
| **Decision Intelligence** | Self-improving accuracy: compounds over time | LOW (early) | TBD |

**Break-Even Analysis:**
- Total ML cost: ~$53-68/month
- Estimated ML-attributable revenue impact: $1,500-3,000/month (conservative)
- ROI: **22-56×** return on ML investment
- Even at 100x scale ($800-1,300/mo ML cost), projected impact ($15K-30K/mo) maintains >10× ROI

---

## Appendix A: ML Decision Records

| ID | Decision | Rationale | Status |
|----|----------|-----------|--------|
| MLR-001 | Use OpenAI GPT-5.2, not self-hosted LLMs | $15-30/mo vs $2K+/mo GPU; solo developer cannot maintain LLM infra | ACCEPTED |
| MLR-002 | Rule-based repricing, not ML-based | Transparent, auditable, predictable; ML demand elasticity deferred to Phase 3 | ACCEPTED |
| MLR-003 | Normal approximation for stockout probability | Analytically tractable, computationally cheap, adequate for Poisson-like demand | ACCEPTED |
| MLR-004 | SQL-native feature engineering (no pandas/spark) | 20K+ LoC SQL investment; Azure SQL handles aggregations efficiently | ACCEPTED |
| MLR-005 | Threshold-based anomaly detection, not Isolation Forest | Interpretable, low false-positive rate, sufficient for refund patterns | ACCEPTED |
| MLR-006 | Monthly aggregation for seasonality (not Prophet/ARIMA) | SQL MERGE is simple, robust; household goods have predictable monthly patterns | ACCEPTED |
| MLR-007 | Decision Intelligence feedback loop as meta-model | Self-improving architecture that learns from outcomes; compounds value over time | ACCEPTED |
| MLR-008 | LLM abstraction layer deferred to Phase 2 | No concrete second provider yet; premature abstraction adds complexity for solo dev | ACCEPTED |
| MLR-009 | All intelligence runs in-process (Phase 1) | Aligns with Architecture Spec ADR-001 (keep monolith); extract to Celery workers in Phase 2 | ACCEPTED |
| MLR-010 | Human-in-the-loop mandatory for product matching and repricing | Prevents automated errors in critical business operations (pricing, catalog) | ACCEPTED |

---

## Appendix B: Feature Store Schema

ACC does not use a formal feature store (e.g., Feast, Tecton). Features are computed inline via SQL and cached in result tables. This is appropriate for Phase 1 (single-developer, <5 users).

**Recommended Phase 2 Feature Store (lightweight):**

```sql
-- Proposed: dbo.acc_feature_store
CREATE TABLE dbo.acc_feature_store (
    id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    entity_type     VARCHAR(20)   NOT NULL,   -- 'sku', 'asin', 'marketplace'
    entity_id       NVARCHAR(100) NOT NULL,
    feature_group   VARCHAR(40)   NOT NULL,   -- 'velocity', 'pricing', 'content', 'risk'
    feature_name    VARCHAR(60)   NOT NULL,
    feature_value   FLOAT         NULL,
    feature_text    NVARCHAR(500) NULL,
    computed_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    valid_until     DATETIME2     NULL,
    CONSTRAINT uq_feature_store UNIQUE (entity_type, entity_id, feature_group, feature_name)
);

CREATE INDEX ix_feature_ts ON dbo.acc_feature_store (entity_type, entity_id, computed_at DESC);
```

**Feature Groups (from current codebase):**

| Group | Features | Source |
|-------|----------|--------|
| velocity | velocity_7d, velocity_30d, velocity_cv | `acc_order_line` aggregation |
| pricing | current_price, buybox_price, competitor_lowest, price_gap_pct | `acc_competitor_offer`, `acc_listing_state` |
| profitability | cm1_margin, cm2_margin, cogs_pln, revenue_pln | `acc_order_line` (computed fields) |
| inventory | units_available, days_cover, excess_units, aged_90_plus | `acc_inventory_risk_score` |
| risk | stockout_prob_7d, overstock_cost, aging_risk, composite_score | `acc_inventory_risk_score` |
| content | health_score, content_score, seo_score, bullet_count | `acc_listing_health_snapshot`, `acc_content_score` |
| seasonality | demand_index (per month 1-12), search_demand_index | `seasonality_index_cache` |
| competition | num_competitors, win_rate_7d, win_rate_30d | `acc_buybox_trend` |

---

## Appendix C: Model Registry Specification

ACC does not require a traditional model registry (MLflow, W&B) because:
1. No self-hosted trained models (all ML is LLM API + statistical/rule-based)
2. Algorithm versions are tracked via Git (code = model definition)
3. LLM model version is tracked in `settings.OPENAI_MODEL` + each recommendation stores `model_used`

**Phase 2 Recommendation — Lightweight Model Config Registry:**

```sql
-- Proposed: dbo.acc_model_config
CREATE TABLE dbo.acc_model_config (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    module_name     VARCHAR(60)   NOT NULL UNIQUE,  -- 'inventory_risk', 'repricing', etc.
    model_type      VARCHAR(30)   NOT NULL,          -- 'statistical', 'rule_based', 'llm'
    config_json     NVARCHAR(MAX) NOT NULL,          -- parameters, thresholds, weights
    version         INT           NOT NULL DEFAULT 1,
    is_active       BIT           NOT NULL DEFAULT 1,
    updated_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_by      NVARCHAR(60)  NULL
);
```

**Example Configs:**
```json
// inventory_risk
{
  "stockout_weight": 40,
  "overstock_weight": 30,
  "aging_weight": 30,
  "tier_thresholds": {"critical": 70, "high": 50, "medium": 30},
  "lead_time_days": 21,
  "target_days": 45
}

// refund_anomaly
{
  "spike_critical": 3.0,
  "spike_high": 2.0,
  "spike_medium": 1.5,
  "min_orders_for_spike": 5,
  "serial_return_min_count": 3,
  "serial_return_high_rate": 0.5,
  "serial_return_critical_rate": 0.7
}
```

This externalized configuration enables parameter tuning without code deployments — critical for the Decision Intelligence recalibration pipeline (Phase 4) to adjust module parameters based on learning data.

---

*End of ML System Design Document. Generated by AI Engineer agent.*
