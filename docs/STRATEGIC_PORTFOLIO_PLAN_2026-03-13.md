# 📋 ACC Strategic Portfolio Plan
## Amazon Command Center — FY2026/27 Portfolio Strategy

**Agent**: Studio Producer | **Date**: 2026-03-13 | **Classification**: Strategic — Decision Document  
**Prepared for**: Miłosz Sobieniowski, Founder & "Builder-Operator"  
**Input Sources**: Phase 0 Executive Summary, Market Intelligence Report, Feedback Synthesis Report, UX Research Findings, Data Audit Report, Technology Stack Assessment (all dated 2026-03-12)  
**Phase 0 Verdict**: ✅ **Conditional GO** — proceed to Phase 1 (Harden → Beta)

---

## TABLE OF CONTENTS

1. [Strategic Portfolio Plan with Project Positioning](#1-strategic-portfolio-plan-with-project-positioning)
2. [Vision, Objectives, and ROI Targets](#2-vision-objectives-and-roi-targets)
3. [Resource Allocation Strategy](#3-resource-allocation-strategy)
4. [Risk/Reward Assessment](#4-riskreward-assessment)
5. [Success Criteria and Milestone Definitions](#5-success-criteria-and-milestone-definitions)

---

# 1. STRATEGIC PORTFOLIO PLAN WITH PROJECT POSITIONING

## 1.1 Product Portfolio Matrix — Modified BCG/GE-McKinsey

ACC is not a portfolio of separate products — it is a **single product platform** with distinct **capability modules** that behave like a portfolio. Each module has different market attractiveness and competitive strength, requiring different investment strategies.

### Portfolio Positioning Grid

```
                              COMPETITIVE STRENGTH
                    HIGH (8-10)      MED (5-7)      LOW (1-4)
                 ┌───────────────┬──────────────┬──────────────┐
    HIGH (8-10)  │   ⭐ STAR      │  ❓ QUESTION  │              │
                 │               │    MARK       │              │
  MARKET         │  Profit       │  Ads→Profit   │              │
  ATTRACT-       │  Engine       │  Attribution  │              │
  IVENESS        │  (CM1/CM2/NP) │              │              │
                 │  Score: 8.1   │  Score: 6.5   │              │
                 ├───────────────┼──────────────┼──────────────┤
    MED (5-7)    │   🐄 CASH COW │  ❓ QUESTION  │  🐕 DOG      │
                 │               │    MARK       │              │
                 │  COGS/ERP     │  Logistics    │  Tax/OSS     │
                 │  Pipeline     │  Cost Model   │  Module      │
                 │  Score: 7.5   │  Score: 7.3   │  Score: 5.0  │
                 │               │              │              │
                 │  Dashboard/   │  FBA Ops      │  Content     │
                 │  Guardrails   │  Module       │  Module      │
                 │  Score: 8.0   │  Score: 7.5   │  Score: 4.5  │
                 ├───────────────┼──────────────┼──────────────┤
    LOW (1-4)    │               │              │  🐕 DOG      │
                 │               │              │              │
                 │               │  Seasonality │  Strategy    │
                 │               │  Engine      │  Intelligence│
                 │               │  Score: 5.6  │  Score: 4.0  │
                 │               │              │              │
                 │               │              │  Repricing   │
                 │               │              │  Score: 5.8  │
                 └───────────────┴──────────────┴──────────────┘
```

### Module Classification & Investment Strategy

| Module | Category | Action | Investment Level | Rationale |
|--------|----------|--------|-----------------|-----------|
| **Profit Engine** (CM1/CM2/NP) | ⭐ Star | **INVEST HEAVILY** | 40% of eng. time | Core moat. No competitor replicates real-time CM1 with ERP COGS + Ads + logistics. Market attractiveness validated (SAM $180-280M). |
| **COGS / ERP Pipeline** | 🐄 Cash Cow | **MAINTAIN & PROTECT** | 10% (maintenance) | 99.5% purchase price coverage. Unique competitive advantage via Netfox ERP. Already built, keep running. |
| **Dashboard & Guardrails** | 🐄 Cash Cow | **MAINTAIN & ENHANCE** | 10% (polish) | Trust infrastructure. Data Quality score on Dashboard, 8+ runtime guardrails. Enhance for beta users. |
| **Ads→Profit Attribution** | ❓ Question Mark | **INVEST SELECTIVELY** | 15% of eng. time | 5,083 campaigns synced, but 93h stale. Fix reliability first (C-2), then differentiate with ACoS→CM1 alerting. |
| **Logistics Cost Model** | ❓ Question Mark | **INVEST SELECTIVELY** | 10% of eng. time | 75% estimation gap on MFN orders. Weight-based model v3 = high-ROI accuracy improvement. |
| **FBA Operations** | ❓ Question Mark | **MAINTAIN** | 5% (maintenance) | 6 well-built pages. 30.1% FBA fee coverage gap needs fixing but via data pipeline, not new features. |
| **Seasonality Engine** | 🐕 Dog | **FREEZE** | 0% (freeze) | 7 tables, 91K rows, but low-usage. Over-built for current stage. |
| **Strategy/Intelligence** | 🐕 Dog | **FREEZE** | 0% (freeze) | 8 pages, no external users. Aspirational. Hide from sidebar. |
| **Tax/OSS Module** | 🐕 Dog | **FREEZE** | 0% (monitor DAC7) | 9 pages, schema-ready but partially populated. Revisit when DAC7 enforcement creates demand (90% probability Q3 2026). |
| **Content Module** | 🐕 Dog | **FREEZE** | 0% (freeze) | 4 pages, near-zero usage. 18 rows across 18 tables. Not core to profit analytics positioning. |
| **Repricing Engine** | 🐕 Dog | **DEFER** | 0% for 12 months | Evaluate/Buy decision. Not a differentiator for profit analytics positioning. |

### Portfolio Balance Assessment

| Metric | Current State | Target (6 months) | Assessment |
|--------|-------------|-------------------|-------------|
| % investment in Stars | ~25% (estimating build time) | 40% | 🔴 Under-invested in core |
| % investment in Dogs | ~30% (Strategy, Tax, Content, Seasonality) | 0% | 🔴 Over-invested in non-core |
| % modules actively used | ~60% (est. from sentiment map) | 85%+ | 🟡 Feature bloat |
| Pages in sidebar | 40+ | 15-20 | 🔴 Needs aggressive pruning |
| Build vs. maintain ratio | 80:20 | 40:60 (Phase 1), 60:40 (Phase 2+) | 🔴 Build fatigue confirmed |

**Strategic Directive**: ACC's portfolio is **over-diversified for a solo-founder stage**. The immediate action is to **contract surface area** (freeze 33 underused pages, hide 4+ modules) and **concentrate investment** on the Star (profit engine) and highest-value Question Marks (ads attribution, logistics model). This is not a breadth play — it is a **depth play on profit truth**.

---

## 1.2 Competitive Positioning Map

```
                    HIGHEST VALUE (Profit Truth)
                              ▲
                              │
                  ACC ◉       │
              (CM1+Ads+ERP+   │        ○ Sellerboard
               Logistics)     │      (profit dashboard)
                              │
    ← OPERATIONAL ────────────┼──────────── RESEARCH →
       ANALYTICS              │              ANALYTICS
                              │
                 ○ Helium 10  │     ○ SmartScout
               (keyword+list) │   (brand analytics)
                              │
              ○ Jungle Scout  │
            (product research)│
                              │
                    SURFACE-LEVEL (Feature Count)
```

**ACC's unique quadrant**: Only platform occupying **Operational Analytics × Highest Value Profit Truth**.

| Competitor | What They Lack vs. ACC | What They Do Better |
|-----------|----------------------|---------------------|
| **Sellerboard** | No ERP integration, no logistics model, limited ads attribution | UX polish (7.3 vs 5.9), sub-2s load times, weekly email reports |
| **Helium 10** | No profit engine, no CM1/CM2/NP, generalist tool | Brand awareness, distribution, feature documentation, onboarding |
| **Jungle Scout** | No financial analytics depth | Product research database, supplier database, $110M war chest |
| **Amazon Native** | Fragmented across 6+ dashboards, no unified P&L | Free, always-on, growing API access |

**Competitive Moat Durability**:

| Moat Component | Durability | Time to Replicate | Defensibility |
|---------------|-----------|-------------------|---------------|
| Real-time CM1 with ERP COGS | **18+ months** | Requires ERP partnership/integration | 🟢 HIGH |
| Multi-carrier logistics cost model | **12+ months** | Country-specific carrier APIs vary | 🟢 HIGH (PL market) |
| Amazon Ads → CM1 profit loop | **6-9 months** | API is public but join logic is complex | 🟡 MEDIUM |
| 9 EU marketplace coverage | **6 months** | Technical work, not strategic moat | 🟡 MEDIUM |
| Data freshness (15-min orders, 4h ads) | **3-6 months** | Infrastructure investment | 🟡 MEDIUM |

**Window of Opportunity**: 12–18 months before incumbents build comparable profit analytics. The clock started ~Q1 2026. By Q3 2027, either ACC has paying customers and a growing moat, or the window closes.

---

# 2. VISION, OBJECTIVES, AND ROI TARGETS

## 2.1 Product Vision

> **"ACC makes Amazon sellers profitable by telling them the truth about their margins — in real time, to the penny, across every marketplace."**

Supporting tagline: *"Profit Truth, Not Guesswork"*

## 2.2 Strategic Objectives (SMART Format, 12-Month Horizon)

### Objective 1: Achieve Product-Market Fit (PMF)

| Element | Target |
|---------|--------|
| **Specific** | 50+ paying users actively using the profit dashboard weekly, with <5% monthly churn |
| **Measurable** | NPS ≥ 40; weekly active usage ≥ 80% of paid users; <5% voluntary churn/month |
| **Achievable** | Phase 0 validated market need; private beta pipeline via Polish Amazon communities |
| **Relevant** | Without PMF, no sustainable business — all other objectives depend on this |
| **Time-bound** | Achieve by Dec 2026 (9 months from Phase 1 start) |

**Key Results**:
- KR1: 200+ private beta signups by Jun 2026
- KR2: 50+ converting to paid by Sep 2026
- KR3: 30-day retention ≥ 70% by Dec 2026
- KR4: NPS survey score ≥ 40 by Dec 2026

### Objective 2: Reach $10K MRR ($120K ARR)

| Element | Target |
|---------|--------|
| **Specific** | $10,000/month recurring revenue from subscription plans |
| **Measurable** | MRR tracked in Stripe/payment dashboard; ARPU tracked per cohort |
| **Achievable** | 200 users × $50/mo avg ARPU = $10K MRR; 8,000+ PL professional sellers = addressable |
| **Relevant** | Covers infrastructure costs + validates pricing model + proves unit economics |
| **Time-bound** | $10K MRR by Mar 2027 (12 months from now) |

**Key Results**:
- KR1: Pricing page live with 3 tiers by May 2026
- KR2: First paying customer by Jul 2026
- KR3: $2K MRR by Sep 2026
- KR4: $5K MRR by Dec 2026
- KR5: $10K MRR by Mar 2027

### Objective 3: Achieve Data Trust Score ≥ 90/100

| Element | Target |
|---------|--------|
| **Specific** | Composite Data Quality Score covering completeness, freshness, accuracy, consistency, coverage |
| **Measurable** | DQ Composite Score (currently 74/100) tracked via `/profit/data-quality` endpoint |
| **Achievable** | Phase 1 conditions (C-1 to C-5) + ads reliability + FBA fee coverage address the 26-point gap |
| **Relevant** | Data trust = #1 user need. Without trust, users revert to spreadsheets. |
| **Time-bound** | ≥85 by Jun 2026; ≥90 by Sep 2026 |

**Key Results**:
- KR1: Ads freshness < 6h (currently 93h) by Apr 2026
- KR2: FBA fee coverage ≥ 80% (currently 30.1%) by Jun 2026
- KR3: FX rate staleness alert live (replacing silent `return 1.0`) by Apr 2026
- KR4: Zero empty tables in production schema by Jun 2026 (drop or populate 72 tables)
- KR5: Composite DQ Score ≥ 90/100 by Sep 2026

### Objective 4: Reduce Decision Latency to < 30 Seconds

| Element | Target |
|---------|--------|
| **Specific** | Time from login to first actionable insight (KPI check, anomaly detection, margin alert) |
| **Measurable** | PPT load time (currently 14.5s), dashboard render time, morning routine elapsed time |
| **Achievable** | SQL pagination (C-1) targets < 2s PPT; dashboard already loads fast; Morning Brief automates check |
| **Relevant** | UX North Star metric; bridges gap vs. Sellerboard (sub-2s loads) |
| **Time-bound** | < 2s PPT by May 2026; < 30s morning check by Jul 2026 |

**Key Results**:
- KR1: PPT load time < 2s (from 14.5s) by May 2026
- KR2: Dashboard render < 1.5s by May 2026
- KR3: Morning Brief auto-digest live (email/push) by Aug 2026
- KR4: UX heuristic score ≥ 7.0/10 (from 5.9) by Oct 2026

### Objective 5: Survive as a Solo Founder Without Burnout

| Element | Target |
|---------|--------|
| **Specific** | Maintain sustainable work pace, reduce build-to-use ratio, prevent single-point-of-failure risk |
| **Measurable** | Build:Use ratio (currently 80:20, target 50:50); documented runbooks; bus factor improvement |
| **Achievable** | Phase 1 freezes 33 pages; build vs. buy decisions redirect effort; process documentation |
| **Relevant** | Build fatigue = #1 strategic risk per Feedback Synthesizer; bus factor = 1 |
| **Time-bound** | Build:Use ratio ≤ 50:50 by Jun 2026; first contractor/hire trigger by Dec 2026 |

**Key Results**:
- KR1: Weekly self-reported build:use ratio ≤ 60:40 by Jun 2026
- KR2: Core runbook documentation (deployment, incident response, data pipeline) by May 2026
- KR3: Automated morning digest replacing manual check routine by Aug 2026
- KR4: First paid contractor engagement (if $5K MRR trigger met) by Jan 2027

---

## 2.3 Financial ROI Targets

### Revenue Model

| Tier | Name | Price (monthly) | Target Users | Features |
|------|------|----------------|-------------|----------|
| **Free** | Explorer | $0 | Unlimited | Basic profit view (30-day window), 1 marketplace, limited export |
| **Pro** | Seller Pro | €39/mo (~$42) | 150+ by Mar'27 | Full CM1/CM2/NP, 9 marketplaces, 12-month history, data quality dashboard, CSV export |
| **Business** | Business Pro | €79/mo (~$85) | 50+ by Mar'27 | Everything in Pro + Ads attribution, logistics model, Morning Brief, API access, invoice-grade PDF reports |
| **Enterprise** | Enterprise | Custom (€199+) | 5-10 by Mar'27 | ERP integration, custom COGS mapping, dedicated support, white-label potential |

### MRR Ramp Projections

| Month | Conservative | Base | Optimistic |
|-------|-------------|------|-----------|
| **Jul 2026** (first paying) | $200 | $500 | $1,000 |
| **Sep 2026** | $800 | $2,000 | $4,000 |
| **Dec 2026** | $2,500 | $5,000 | $10,000 |
| **Mar 2027** | $5,000 | $10,000 | $22,000 |
| **Jun 2027** | $8,000 | $18,000 | $40,000 |
| **Sep 2027** | $12,000 | $30,000 | $65,000 |

### Unit Economics Targets

| Metric | Target (12-month) | Benchmark |
|--------|-------------------|-----------|
| **ARPU** | €55/mo blended ($59) | Sellerboard: €22/mo; Helium 10: €79/mo |
| **CAC** | < €100 (organic + community) | SaaS benchmark: €200-500 |
| **LTV** | €660 (12 months × €55) | Min. acceptable for bootstrapped |
| **LTV:CAC** | > 6:1 | SaaS standard: ≥ 3:1 |
| **Gross Margin** | > 85% | SaaS standard: 70-90% |
| **Monthly Churn** | < 5% | SMB SaaS average: 5-7% |
| **Net Revenue Retention** | > 105% | Indicates upsell working |
| **Payback Period** | < 2 months | Organic CAC = fast payback |

### Break-Even Analysis

| Cost Category | Monthly Estimate | Notes |
|--------------|-----------------|-------|
| **Azure SQL S3** | $150 | Current tier, sufficient to ~500 users |
| **Azure App Service** | $50 | B2 tier for production |
| **Redis** | $15 | Azure Cache for Redis Basic |
| **Sentry** | $26 | Team plan (5K errors/mo) |
| **UptimeRobot** | $7 | Pro tier (60 monitors) |
| **Domain + SSL** | $5 | Annual spread |
| **Email service (Resend/Postmark)** | $20 | Transactional emails |
| **Misc SaaS tools** | $30 | Analytics, support, etc. |
| **Total Infrastructure** | **~$303/mo** | |
| **Founder living cost** (min.) | ~$3,000/mo | Estimated minimum sustainable salary |
| **Total Break-Even** | **~$3,300/mo MRR** | |
| **Users needed at €55 ARPU** | **~60 paying users** | |

**Break-even timeline**:
- Conservative: **Oct 2026** (infra-only break-even at ~$303 MRR)
- Base: **Sep 2026** (infra break-even); **Feb 2027** (full break-even with founder salary)
- Optimistic: **Aug 2026** (infra); **Nov 2026** (full break-even)

### Exit / Valuation Milestones

| Milestone | ARR | Valuation (5-8x ARR) | Timeline |
|-----------|-----|----------------------|----------|
| Seed-ready | $120K | $600K–$960K | Mar 2027 |
| Series A-ready | $500K | $2.5M–$4M | Mar 2028 |
| Acquisition-attractive | $1M+ | $5M–$8M | 2028-2029 |

Precedents: Sellics acquired at ~5x ARR; Perpetua at ~8x ARR. Amazon seller tools space: 5-8x revenue multiple for profitable, growing tools.

---

## 2.4 Non-Financial ROI Targets

| Metric | Current | 6-Month Target | 12-Month Target |
|--------|---------|----------------|-----------------|
| **Data Trust Score** | 74/100 | 88/100 | 92/100 |
| **UX Heuristic Score** | 5.9/10 | 7.0/10 | 7.5/10 |
| **Decision Latency** | ~3-5 min | < 30s (daily check) | < 15s |
| **PPT Load Time** | 14.5s | < 2s | < 1s |
| **NPS** | N/A (no users) | ≥ 30 (beta) | ≥ 40 (paid) |
| **Time-to-Value** (signup → "aha") | N/A | < 10 min | < 5 min |
| **Ads Data Freshness** | 93h | < 6h | < 4h |
| **COGS Coverage** | 96% | 98% | 99% |
| **FBA Fee Coverage** | 30.1% | 80% | 95% |
| **Build:Use Ratio** | 80:20 | 50:50 | 40:60 |
| **Pipeline Reliability** (no silent failures) | Unknown | 99% uptime | 99.5% uptime |
| **Test Pass Rate** | 73% (422/577) | 90% | 95% |

---

# 3. RESOURCE ALLOCATION STRATEGY

## 3.1 Solo Founder Capacity Model

### Realistic Weekly Time Budget

| Activity | Hours/Week | % of Total | Notes |
|----------|-----------|-----------|-------|
| **Deep Engineering** (coding, debugging, architecture) | 20h | 40% | Peak productivity: morning blocks, 4h/day max |
| **Business Operations** (orders, customer support, accounting) | 8h | 16% | Amazon business still needs running |
| **GTM / Marketing** (content, community, outreach) | 5h | 10% | Critical from Phase 2 onward |
| **Strategic Thinking** (planning, market research, analysis) | 3h | 6% | Weekly review ritual |
| **Infrastructure Maintenance** (deployments, monitoring, fixes) | 5h | 10% | Reduce via automation |
| **Recovery / Buffer** | 9h | 18% | Weekends, breaks, unplanned issues |
| **Total Sustainable** | **50h/week** | 100% | **Not 80h. Not 60h. 50h max sustainable.** |

### Burnout Prevention Framework

| Signal | Threshold | Action |
|--------|-----------|--------|
| Build:Use ratio exceeds 70:30 | Weekly check | Stop new features, switch to using ACC for business decisions |
| > 3 consecutive 60h+ weeks | Bi-weekly check | Mandatory 30h cap for next week |
| No business revenue growth in 60 days | Monthly check | Revisit strategy, consider pivot or contractor help |
| Physical symptoms (sleep, focus) | Self-monitoring | Hard stop on evening/weekend coding |
| "I should add this feature" urge | Continuous | Write it down. Wait 7 days. If still important, RICE score it. |

### Capacity Allocation by Phase

```
PHASE 1 — HARDEN (Apr–May 2026)
┌──────────────────────────────────────────────────────┐
│  Engineering: ████████████████████░░░ 80%             │
│  ├─ C-1 SQL Pagination:    ████████  ~35%            │
│  ├─ C-2 Ads Heartbeat:     ███       ~12%            │
│  ├─ C-3 FX Warning:        ██        ~8%             │
│  ├─ C-4 Hide 33 Pages:     ██        ~8%             │
│  ├─ C-5 Uptime Monitor:    █         ~4%             │
│  └─ Bug fixes/polish:      ███       ~13%            │
│  Business Ops:  ████ 15%                              │
│  GTM Prep:      █ 5%                                  │
└──────────────────────────────────────────────────────┘

PHASE 2 — BETA (Jun–Sep 2026)
┌──────────────────────────────────────────────────────┐
│  Engineering: ████████████████░░░░░░ 55%             │
│  ├─ Onboarding flow:       ████      ~15%            │
│  ├─ Multi-tenant (basic):  █████     ~18%            │
│  ├─ Billing (Stripe):      ███       ~10%            │
│  ├─ Morning Brief:         ██        ~7%             │
│  └─ Polish/fixes:          █         ~5%             │
│  Business Ops:  ████ 15%                              │
│  GTM:           ████████ 25%                          │
│  Strategic:     █ 5%                                  │
└──────────────────────────────────────────────────────┘

PHASE 3 — LAUNCH (Oct 2026–Jan 2027)
┌──────────────────────────────────────────────────────┐
│  Engineering: ██████████░░░░░░░░░░░ 40%              │
│  ├─ Scale & reliability:   ████      ~15%            │
│  ├─ User-requested features:█████    ~20%            │
│  └─ Testing/docs:          █         ~5%             │
│  Business Ops:  ████ 15%                              │
│  GTM:           █████████████ 35%                     │
│  Strategic:     ██ 10%                                │
└──────────────────────────────────────────────────────┘

PHASE 4 — SCALE (Feb 2027+)
┌──────────────────────────────────────────────────────┐
│  Engineering: ████████░░░░░░░░░░░░░ 30%              │
│  Management:  ██████ 20% (if hired)                   │
│  Business Ops:  ████ 15%                              │
│  GTM:           ██████████████ 30%                    │
│  Strategic:     █ 5%                                  │
└──────────────────────────────────────────────────────┘
```

---

## 3.2 Time Allocation Framework: Hardening vs. Features vs. GTM

### The 40/30/30 Rule (Post-Phase 1)

| Category | Allocation | Activities |
|----------|-----------|------------|
| **Hardening/Reliability** | 40% → 30% → 20% | Performance, data quality, pipeline reliability, testing, documentation |
| **Feature Development** | 30% → 35% → 30% | New user-facing functionality driven by beta feedback and RICE scores |
| **GTM/Business** | 30% → 35% → 50% | Marketing, community, sales, customer success, partnerships |

Phase 1 is **80% hardening** — an intentional deviation. The platform must earn trust before earning revenue.

### Feature Prioritization Queue (Post-Phase 1 Conditions)

| Priority | Feature | Effort | ROI | Phase |
|----------|---------|--------|-----|-------|
| 1 | **Weight-based logistics model v3** | 8-13d | 170% | Phase 2 |
| 2 | **User onboarding + multi-tenant (basic)** | 13-21d | ∞ (required for revenue) | Phase 2 |
| 3 | **Stripe billing integration** | 5-8d | ∞ (required for revenue) | Phase 2 |
| 4 | **Morning Brief auto-digest** | 3-5d | 130% | Phase 2 |
| 5 | **Unified alert triage view** | 5-8d | 65% | Phase 2-3 |
| 6 | **Weekly P&L PDF report** | 5-8d | 90% | Phase 3 |
| 7 | **Bank feed automation** | 13-21d | 60% | Phase 3 |
| 8 | **Profit→refund drill path** | 5-8d | 50% | Phase 3 |

---

## 3.3 Build vs. Buy Prioritized Execution Plan

### Immediate BUY Decisions (Phase 1, Week 1)

| Item | Recommended Tool | Cost | Impact |
|------|-----------------|------|--------|
| **Uptime monitoring** | UptimeRobot Pro | $7/mo | Ends silent downtime — C-5 condition |
| **Email delivery** | Resend or Postmark | $0-20/mo | Required for Morning Brief, password reset, onboarding |
| **PDF report generation** | WeasyPrint (OSS lib) or Puppeteer | $0 | Weekly P&L PDF for beta users (Phase 3) |
| **Error tracking** | Sentry (already in place) | $26/mo | ✅ Already bought |

### Phase 2 BUY Decisions

| Item | Recommended Tool | Cost | Impact |
|------|-----------------|------|--------|
| **Billing/payments** | Stripe | 2.9% + €0.25/txn | Required for paid tiers |
| **Customer support** | Crisp.chat (free tier) or email-only | $0-25/mo | Needed at 50+ users |
| **Simple analytics** | PostHog (free/OSS) or Plausible | $0-10/mo | Page visit tracking, feature usage |
| **RBAC (basic)** | Custom JWT + role claim | $0 (build-light) | Team accounts for Enterprise tier |

### Budget Allocation (Monthly)

| Category | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|----------|---------|---------|---------|---------|
| **Infrastructure** (Azure, Redis, domain) | $220 | $250 | $350 | $500 |
| **SaaS tools** (Sentry, Uptime, email) | $60 | $80 | $120 | $150 |
| **Marketing** (community, content, ads) | $0 | $200 | $500 | $1,000 |
| **Contractor/freelance** | $0 | $0 | $0-500 | $1,000-3,000 |
| **Buffer (10%)** | $28 | $53 | $97-147 | $265-465 |
| **TOTAL** | **~$308** | **~$583** | **~$1,067-1,617** | **~$2,915-5,115** |

---

## 3.4 Hiring Triggers & First-Hire Recommendation

### Quantitative Hiring Triggers

| Trigger | Threshold | Hire Type |
|---------|-----------|-----------|
| **MRR exceeds $5,000** | Sustained for 2 months | Part-time contractor (frontend or DevOps) |
| **MRR exceeds $10,000** | Sustained for 3 months | First full-time hire |
| **Support tickets > 20/week** | Sustained for 4 weeks | Customer success part-time |
| **Engineering backlog > 6 months** | When total RICE-scored backlog exceeds 6 months solo capacity | Frontend developer contractor |
| **User base > 200 paying** | Any time | Dedicated support + community manager |

### First Hire Recommendation

**Role**: Senior Frontend Developer (React/TypeScript) — Part-time contractor (20h/week)

| Attribute | Rationale |
|-----------|-----------|
| **Why frontend first** | Miłosz's deepest expertise is backend/data engineering. Frontend UX is the biggest gap vs. Sellerboard (5.9 vs 7.3). A frontend specialist can accelerate onboarding, progressive disclosure, and responsive design without touching the profit engine core. |
| **Why contractor** | Reduces commitment risk. Tests working relationship before full-time. |
| **Estimated cost** | $2,000-4,000/mo (Polish market, senior React) |
| **When** | When $5K MRR is sustained for 2+ months (est. Nov-Dec 2026) |
| **Where to find** | useme.com (PL), JustJoinIT (PL), r/reactjs, LinkedIn Poland |

**Second Hire** (when $10K+ MRR sustained):
- **Role**: DevOps/Backend engineer (part-time → full-time)
- **Why**: Reduce bus factor from 1 to 2 on critical pipelines
- **Estimated cost**: $3,000-5,000/mo

---

# 4. RISK/REWARD ASSESSMENT

## 4.1 Risk Register — Probability × Impact Matrix

### Risk Heat Map

```
              IMPACT
         LOW    MED    HIGH    CRITICAL
       ┌──────┬──────┬───────┬──────────┐
  HIGH │      │ R-07 │ R-01  │          │
  PROB │      │ R-09 │ R-02  │          │
       ├──────┼──────┼───────┼──────────┤
  MED  │ R-10 │ R-06 │ R-03  │ R-05     │
  PROB │      │ R-08 │ R-04  │          │
       ├──────┼──────┼───────┼──────────┤
  LOW  │      │      │ R-11  │          │
  PROB │      │      │       │          │
       └──────┴──────┴───────┴──────────┘
```

### Full Risk Register

| ID | Risk | Category | Prob. | Impact | Risk Score (P×I) | Owner |
|----|------|----------|-------|--------|------------------|-------|
| **R-01** | **Founder burnout / build fatigue** — 80:20 build:use ratio leads to exhaustion, loss of strategic clarity, abandoned project | Personal | 70% | HIGH (8/10) | **5.6** | Miłosz |
| **R-02** | **Silent backend crash / data staleness** — APScheduler dies silently, profit data goes stale, decisions based on old numbers | Technical | 65% | HIGH (8/10) | **5.2** | Miłosz |
| **R-03** | **Competitive window closes** — Helium 10 acquires Sellerboard or builds CM1; Amazon launches native profit dashboard | Market | 40% | HIGH (8/10) | **3.2** | Market |
| **R-04** | **Zero paying users after beta** — product doesn't solve a willingness-to-pay problem; sellers stay on spreadsheets | Financial | 35% | HIGH (9/10) | **3.15** | Miłosz |
| **R-05** | **Single point of knowledge (bus factor = 1)** — illness, injury, or personal crisis halts all development and operations | Personal | 25% | CRITICAL (10/10) | **2.5** | Miłosz |
| **R-06** | **Amazon API changes / rate limiting** — SP-API or Ads API deprecation, throttling, or policy change disrupts data flow | Technical | 45% | MED (6/10) | **2.7** | Amazon |
| **R-07** | **FX rate / data accuracy silent failure** — `return 1.0` fallback corrupts margin calculations across non-EUR marketplaces | Technical | 60% | MED (7/10) | **4.2** | Miłosz |
| **R-08** | **Azure cost escalation** — database growth beyond S3 tier, concurrent user load increases compute costs | Financial | 50% | MED (5/10) | **2.5** | Miłosz |
| **R-09** | **Security breach / credential leak** — SP-API tokens, Ads API tokens, or ERP credentials exposed | Technical | 30% | MED (7/10) | **2.1** | Miłosz |
| **R-10** | **Regulatory / compliance surprise** — GDPR enforcement, DAC7 implementation changes, VAT OSS rules shift | Legal | 40% | LOW (4/10) | **1.6** | EU |
| **R-11** | **Netfox ERP access revoked** — company changes ERP, restricts direct SQL access, or infrastructure changes | Technical | 15% | HIGH (9/10) | **1.35** | Company |

---

## 4.2 Top 5 Risk Mitigation Strategies

### R-01: Founder Burnout (Risk Score: 5.6)

| Strategy | Action | Timeline |
|----------|--------|----------|
| **Prevention** | Weekly build:use ratio tracking; 50h/week hard cap; mandatory 1 day/week no-code | Immediate |
| **Reduction** | Freeze 33 underused pages (C-4); aggressively BUY not BUILD for non-core | Phase 1 |
| **Detection** | Monthly self-assessment: energy, sleep, strategic clarity; spouse/friend check-in | Monthly |
| **Contingency** | If symptoms detected: 1-week hard stop, reassess all commitments, drop lowest-priority work | On trigger |
| **Structural** | Hire contractor when $5K MRR reached; document all runbooks by Phase 2 end | Phase 2-3 |

### R-02: Silent Backend Crash (Risk Score: 5.2)

| Strategy | Action | Timeline |
|----------|--------|----------|
| **Prevention** | C-5: UptimeRobot external monitoring (30 min setup) | Week 1 |
| **Prevention** | C-2: Ads sync heartbeat + single-flight guard | Phase 1 |
| **Detection** | APScheduler job heartbeat updates every 5 min during execution | Phase 1 |
| **Detection** | Guardrail-triggered Sentry alerts for data staleness > threshold | Phase 1 |
| **Recovery** | Auto-restart via Azure App Service health check probe + keep-alive endpoint | Phase 1 |

### R-03: Competitive Window Closes (Risk Score: 3.2)

| Strategy | Action | Timeline |
|----------|--------|----------|
| **Monitoring** | Monthly competitor changelog review (Sellerboard, Helium 10, Jungle Scout) | Monthly |
| **Acceleration** | Focus on differentiation — ERP COGS + Ads→CM1 loop, not feature parity | Continuous |
| **Moat deepening** | Add exclusive data sources (InPost API, Poczta Polska, local PL billing) | Phase 2-3 |
| **Community** | Build Polish Amazon seller community (first-mover in PL market) | Phase 2 |
| **Pivot trigger** | If H10 acquires Sellerboard → pivot to enterprise/ERP-integrated niche only | On trigger |

### R-04: Zero Paying Users (Risk Score: 3.15)

| Strategy | Action | Timeline |
|----------|--------|----------|
| **Validation** | Private beta with 20-50 PL sellers BEFORE building billing | Phase 2 start |
| **Learning** | Weekly beta user interviews (5 users/week); track "aha moment" metrics | Phase 2 |
| **Pricing test** | Early bird pricing; test €29 vs €39 vs €49; measure conversion at each | Phase 2 |
| **Safety net** | If < 10 paid users after 3 months of beta → pivot to consulting/agency model | Kill gate |
| **Alternative** | Offer ACC as white-label to Amazon aggregators (Thrasio-model alumni) | Plan B |

### R-05: Single Point of Knowledge (Risk Score: 2.5)

| Strategy | Action | Timeline |
|----------|--------|----------|
| **Documentation** | Core system runbooks: deployment, incident response, database maintenance, scheduler | Phase 1-2 |
| **Automation** | CI/CD pipeline so anyone can deploy; health check auto-restart | Phase 1 |
| **Knowledge sharing** | Architecture docs (COPILOT_CONTEXT, Phase 0 reports) already exist — keep updated | Continuous |
| **Insurance** | If MRR allows → contractor with enough context to maintain core pipeline for 30 days | Phase 3 |
| **Legal** | Simple will / digital estate plan for business-critical credentials | Phase 1 (1 hour) |

---

## 4.3 Reward Scenarios — Financial Projections

### Conservative Scenario (25% probability)

Slow adoption, competitors react, PL market growth underwhelms.

| Metric | 6 months (Sep'26) | 12 months (Mar'27) | 18 months (Sep'27) | 24 months (Mar'28) |
|--------|-------------------|--------------------|--------------------|---------------------|
| **Paying Users** | 20 | 60 | 120 | 200 |
| **MRR** | $800 | $3,000 | $6,500 | $11,000 |
| **ARR** | $9,600 | $36,000 | $78,000 | $132,000 |
| **Monthly Costs** | $400 | $600 | $1,000 | $2,000 |
| **Monthly Profit** | $400 | $2,400 | $5,500 | $9,000 |
| **Cumulative Revenue** | $3,400 | $18,000 | $52,000 | $107,000 |

**Outcome**: Sustainable lifestyle business. Covers infrastructure + partial founder salary by Month 12. No external funding needed. Potential acquisition at $180K-$500K (5-8x trailing ARR floor).

### Base Scenario (50% probability)

Steady PMF, PL market grows 30%, word-of-mouth in seller communities.

| Metric | 6 months (Sep'26) | 12 months (Mar'27) | 18 months (Sep'27) | 24 months (Mar'28) |
|--------|-------------------|--------------------|--------------------|---------------------|
| **Paying Users** | 60 | 200 | 500 | 1,000 |
| **MRR** | $2,500 | $10,000 | $27,500 | $55,000 |
| **ARR** | $30,000 | $120,000 | $330,000 | $660,000 |
| **Monthly Costs** | $600 | $1,500 | $4,000 | $10,000 |
| **Monthly Profit** | $1,900 | $8,500 | $23,500 | $45,000 |
| **Cumulative Revenue** | $9,500 | $55,000 | $185,000 | $430,000 |

**Outcome**: Real SaaS business. First hire by Month 10. Seed-fundable at $120K ARR. Potential acquisition at $600K-$5.3M. DACH market expansion viable in Month 15+.

### Optimistic Scenario (25% probability)

Strong PMF, viral Polish community, DAC7 enforcement drives demand, Amazon native tools disappoint.

| Metric | 6 months (Sep'26) | 12 months (Mar'27) | 18 months (Sep'27) | 24 months (Mar'28) |
|--------|-------------------|--------------------|--------------------|---------------------|
| **Paying Users** | 150 | 600 | 1,500 | 3,000 |
| **MRR** | $6,000 | $25,000 | $75,000 | $165,000 |
| **ARR** | $72,000 | $300,000 | $900,000 | $1,980,000 |
| **Monthly Costs** | $800 | $3,000 | $12,000 | $30,000 |
| **Monthly Profit** | $5,200 | $22,000 | $63,000 | $135,000 |
| **Cumulative Revenue** | $24,000 | $120,000 | $450,000 | $1,200,000 |

**Outcome**: High-growth SaaS. Team of 5-8 by Month 18. Series A-ready at $900K ARR. Potential acquisition at $1.5M-$16M. Multi-country expansion (DACH, FR, IT, ES).

### Weighted Expected Value

| Metric | Weight | Value | Weighted |
|--------|--------|-------|----------|
| 12-month ARR (Conservative) | 25% | $36,000 | $9,000 |
| 12-month ARR (Base) | 50% | $120,000 | $60,000 |
| 12-month ARR (Optimistic) | 25% | $300,000 | $75,000 |
| **Expected 12-month ARR** | | | **$144,000** |

**Expected 12-month total investment** (excluding founder salary): ~$8,000-12,000  
**Expected 12-month ROI**: 1,100-1,700%  
**Risk-adjusted NPV** (3-year, 15% discount rate): ~$350,000-$600,000

---

## 4.4 Go/Kill Criteria by Phase

| Phase | GO Criteria | KILL Criteria | PIVOT Criteria |
|-------|------------|---------------|----------------|
| **Phase 1 (Harden)** | All 5 conditions (C-1 to C-5) met; DQ Score ≥ 82; PPT < 2s | > 8 weeks without completing C-1; loss of ERP access; major API shutdown | If ERP access lost → pivot to Ads-only profit analytics |
| **Phase 2 (Beta)** | 50+ beta signups; 20+ weekly active users; 5+ paid | < 20 signups after 8 weeks of outreach; NPS < 0; 0 willingness-to-pay signals | If no WTP → offer free tool + consulting revenue model |
| **Phase 3 (Launch)** | $2K+ MRR; <10% monthly churn; 100+ users; positive unit economics | MRR declining for 3 consecutive months; churn > 15%; CAC > LTV | If high churn → focus on Enterprise segment only (fewer, bigger customers) |
| **Phase 4 (Scale)** | $10K+ MRR; team of ≥2; LTV:CAC > 4:1; NRR > 100% | MRR plateaus for 6+ months; market consolidation makes niche unviable | If plateau → seek acquisition at current ARR multiple |

---

# 5. SUCCESS CRITERIA AND MILESTONE DEFINITIONS

## 5.1 Phase 1 — HARDEN (Apr 1 – May 15, 2026)

**Duration**: 6-7 weeks  
**Theme**: *"Earn trust in the numbers"*  
**Engineering allocation**: 80% hardening, 15% business ops, 5% GTM prep

### Exit Criteria

| # | Criterion | Metric | Target | How to Verify |
|---|-----------|--------|--------|---------------|
| H-1 | PPT load time | Server response + render | < 2.0 seconds (p95) | Browser DevTools network tab; 10 consecutive loads |
| H-2 | Ads data freshness | `ads_product_day` latest date vs. now() | < 6 hours | SQL query on production; guardrail check passes |
| H-3 | FX rate safety | No `return 1.0` fallback in production code | Alert triggers when FX > 24h stale | Unit test + guardrail integration test |
| H-4 | UI surface reduction | Active sidebar items | ≤ 20 pages visible (from 40+) | Manual count; hidden pages still accessible via URL |
| H-5 | Uptime monitoring | UptimeRobot health checks | 99%+ uptime confirmed for 7 consecutive days | UptimeRobot dashboard |
| H-6 | Data Quality Score | Composite DQ metric | ≥ 82/100 (from 74) | `/profit/data-quality` endpoint |
| H-7 | Test pass rate | pytest + vitest results | ≥ 85% (from 73%) | CI run |
| H-8 | Zero silent failures | Guardrail coverage | All 8+ guardrails passing with alert on fail | Guardrails dashboard |

### Milestone Schedule

| Week | Focus | Deliverable |
|------|-------|-------------|
| **W1** (Apr 1-7) | C-5: UptimeRobot + C-3: FX warning | External monitoring live; `return 1.0` replaced with alert |
| **W2** (Apr 8-14) | C-2: Ads heartbeat + single-flight guard | Job heartbeats updating; no duplicate sync runs |
| **W3-4** (Apr 15-28) | C-1: SQL pagination for PPT | Backend: SQL-level OFFSET/FETCH + sort; Frontend: server-side pagination |
| **W5** (Apr 29-May 5) | C-1 completion + C-4: Hide underused pages | PPT < 2s confirmed; sidebar pruned to ≤ 20 items |
| **W6** (May 6-12) | Polish, test, stabilize | Test pass rate ≥ 85%; DQ Score ≥ 82; runbook for top 5 incidents |
| **W7** (May 13-15) | Phase gate review | Self-assessment against H-1 to H-8; GO/NO-GO for Phase 2 |

---

## 5.2 Phase 2 — BETA (May 16 – Sep 30, 2026)

**Duration**: ~20 weeks  
**Theme**: *"First users, first revenue"*  
**Engineering allocation**: 55% features, 15% business ops, 25% GTM, 5% strategic

### Exit Criteria

| # | Criterion | Metric | Target | How to Verify |
|---|-----------|--------|--------|---------------|
| B-1 | Beta signups | Total registered users | ≥ 200 | User table count |
| B-2 | Weekly active users | Unique users with ≥ 1 session/week over last 4 weeks | ≥ 50 | Analytics/page visit tracking |
| B-3 | First paid customers | Users on Stripe-connected paid plan | ≥ 20 | Stripe dashboard |
| B-4 | MRR | Monthly recurring revenue | ≥ $2,000 | Stripe MRR metric |
| B-5 | NPS | Net Promoter Score (beta cohort survey) | ≥ 30 | NPS micro-survey (in-app) |
| B-6 | Time-to-value | Signup → first "aha moment" (viewing own profit data) | < 10 minutes | Onboarding funnel tracking |
| B-7 | Data Trust Score | Composite DQ metric | ≥ 88/100 | `/profit/data-quality` endpoint |
| B-8 | Support response time | Average first response to user issue | < 4 hours (business hours) | Email/support log |
| B-9 | Churn rate | Monthly voluntary churn (paid users) | < 8% | Stripe churn metric |
| B-10 | Zero critical bugs in production | P0 bugs open | 0 for 14 consecutive days before phase gate | Bug tracker |

### Milestone Schedule

| Month | Focus | Key Deliverables |
|-------|-------|-----------------|
| **May (late)** | Multi-tenant basics + onboarding | User registration flow; tenant isolation; basic onboarding wizard |
| **Jun** | Private beta launch (20-50 users) | Invite-only signups; recruitment via PL Amazon communities; feedback loop |
| **Jul** | Billing + pricing | Stripe integration; 3-tier pricing; first paid conversions |
| **Aug** | Feature iteration from feedback | Morning Brief; logistics model v3; top 3 user-requested features |
| **Sep** | Scale to 200+ users; stabilize | Public waitlist; performance at scale; churn analysis; Phase 2 gate |

### Beta User Acquisition Strategy

| Channel | Target | Expected Yield | Cost |
|---------|--------|---------------|------|
| Amazon FBA Polska (Facebook group, ~8K members) | 3 posts + 5 DMs | 30-50 signups | $0 |
| JustJoinIT / X / LinkedIn personal network | 5 posts | 20-30 signups | $0 |
| ASM PL community (Amazon Seller Mastermind) | 1 webinar/demo | 15-25 signups | $0 |
| Amazon.pl seller forums | Helpful posts + tool mention | 10-20 signups | $0 |
| Direct outreach (known PL Amazon sellers) | 20 personal emails | 10-15 signups | $0 |
| Referral from beta users (2x multiplier) | Existing beta | 30-50 signups | $0 |
| **Total estimated** | | **115-190 signups** | **$0** |

---

## 5.3 Phase 3 — LAUNCH (Oct 2026 – Jan 2027)

**Duration**: ~16 weeks  
**Theme**: *"From beta to business"*  
**Engineering allocation**: 40% features, 15% business ops, 35% GTM, 10% strategic

### Exit Criteria

| # | Criterion | Metric | Target | How to Verify |
|---|-----------|--------|--------|---------------|
| L-1 | MRR | Monthly recurring revenue | ≥ $5,000 | Stripe |
| L-2 | Paying users | Active paid subscriptions | ≥ 100 | Stripe |
| L-3 | Monthly churn | Voluntary churn rate | < 5% | Stripe |
| L-4 | NPS | Net Promoter Score | ≥ 40 | Survey |
| L-5 | Data Trust Score | Composite DQ | ≥ 90/100 | Endpoint |
| L-6 | PPT performance | p95 load time under production load | < 1.5s | APM |
| L-7 | Support SLA | First response time | < 2 hours (business hours) | Logs |
| L-8 | Documentation | Public docs site / help center | Live with ≥ 20 articles | URL check |
| L-9 | DACH preparation | Amazon.de marketplace fully tested with 5+ DE users | Yes | User data |
| L-10 | Unit economics | LTV:CAC ratio | > 4:1 | Computed |

### Milestone Schedule

| Month | Focus | Key Deliverables |
|-------|-------|-----------------|
| **Oct** | Public launch (PL market) | Marketing site; public signup; content marketing (3 blog posts); pricing live |
| **Nov** | Growth + retention | Referral program; weekly P&L PDF report; onboarding optimization from funnel data |
| **Dec** | DACH preparation | DE marketplace deep testing; German UI strings (critical paths); DE seller outreach |
| **Jan** | Phase gate + DACH soft launch | 100+ users; $5K+ MRR; DACH beta with 5-10 DE sellers; Phase 3 gate review |

---

## 5.4 Phase 4 — SCALE (Feb 2027+)

**Duration**: Ongoing  
**Theme**: *"Growth engine"*  
**Trigger**: Enter Phase 4 when L-1 ($5K MRR) + L-2 (100 users) + L-3 (<5% churn) all met.

### Entry KPIs and Scaling Triggers

| Trigger | Threshold | Unlocks |
|---------|-----------|---------|
| $10K MRR sustained 3 months | Revenue | First full-time hire |
| 500+ paying users | Scale | Azure SQL tier upgrade (S4/P1); Celery activation |
| DACH > 50 paying users | Market | Dedicated DE content + support |
| Enterprise inquiry pipeline ≥ 5 | Demand | Enterprise tier pricing; custom COGS onboarding |
| NRR > 110% | Product | Business tier upsell is working; add advanced features |
| $30K+ MRR | Revenue | Series A exploration; team of 3-5 |

### Phase 4 Strategic Targets (12-24 month horizon)

| Target | Metric | Timeline |
|--------|--------|----------|
| $30K MRR | Revenue | Aug 2027 (base scenario) |
| 1,000 paying users | Users | Mar 2028 (base) |
| 3 EU markets active | Geography | DE + PL + FR or IT |
| Team of 4-5 | Org | When $30K+ MRR sustained |
| Series A readiness | Funding | When $500K+ ARR |
| AI-powered margin alerts | Feature | Q3 2027 (competitive moat deepening) |
| Mobile companion app | Feature | Q4 2027 (if demand validated) |

---

## 5.5 Monthly Health Check Dashboard

Run this check **every first Monday of the month**. Answers should take < 30 minutes with proper tracking in place.

```
╔══════════════════════════════════════════════════════════════════════════╗
║                  ACC MONTHLY HEALTH DASHBOARD                          ║
║                  Month: ________  Date: ________                       ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                        ║
║  📊 BUSINESS METRICS                                                   ║
║  ┌─────────────────────────┬──────────┬──────────┬─────────────┐      ║
║  │ Metric                  │ Actual   │ Target   │ Status      │      ║
║  ├─────────────────────────┼──────────┼──────────┼─────────────┤      ║
║  │ MRR                     │ $______  │ $______  │ 🟢🟡🔴     │      ║
║  │ Paying Users            │ ______   │ ______   │ 🟢🟡🔴     │      ║
║  │ New Users (this month)  │ ______   │ ______   │ 🟢🟡🔴     │      ║
║  │ Churn Rate              │ ______%  │ < 5%     │ 🟢🟡🔴     │      ║
║  │ ARPU                    │ $______  │ $______  │ 🟢🟡🔴     │      ║
║  │ NPS (if surveyed)       │ ______   │ ≥ 40     │ 🟢🟡🔴     │      ║
║  └─────────────────────────┴──────────┴──────────┴─────────────┘      ║
║                                                                        ║
║  🔧 PRODUCT METRICS                                                    ║
║  ┌─────────────────────────┬──────────┬──────────┬─────────────┐      ║
║  │ Metric                  │ Actual   │ Target   │ Status      │      ║
║  ├─────────────────────────┼──────────┼──────────┼─────────────┤      ║
║  │ Data Quality Score      │ ___/100  │ ≥ 90     │ 🟢🟡🔴     │      ║
║  │ PPT Load Time (p95)     │ _____s   │ < 2s     │ 🟢🟡🔴     │      ║
║  │ Uptime (30-day)         │ _____%   │ ≥ 99%    │ 🟢🟡🔴     │      ║
║  │ Ads Freshness           │ ___h     │ < 6h     │ 🟢🟡🔴     │      ║
║  │ Test Pass Rate          │ _____%   │ ≥ 90%    │ 🟢🟡🔴     │      ║
║  │ Open P0 Bugs            │ ______   │ 0        │ 🟢🟡🔴     │      ║
║  └─────────────────────────┴──────────┴──────────┴─────────────┘      ║
║                                                                        ║
║  👤 FOUNDER HEALTH                                                     ║
║  ┌─────────────────────────┬──────────┬──────────┬─────────────┐      ║
║  │ Metric                  │ Actual   │ Target   │ Status      │      ║
║  ├─────────────────────────┼──────────┼──────────┼─────────────┤      ║
║  │ Avg Hours/Week          │ ______h  │ ≤ 50h    │ 🟢🟡🔴     │      ║
║  │ Build:Use Ratio         │ ___:___  │ ≤ 50:50  │ 🟢🟡🔴     │      ║
║  │ >60h Weeks (this month) │ ______   │ 0        │ 🟢🟡🔴     │      ║
║  │ Energy Level (1-10)     │ ______   │ ≥ 7      │ 🟢🟡🔴     │      ║
║  │ Strategic Clarity (1-10)│ ______   │ ≥ 8      │ 🟢🟡🔴     │      ║
║  └─────────────────────────┴──────────┴──────────┴─────────────┘      ║
║                                                                        ║
║  🏗️ MILESTONE PROGRESS                                                ║
║  Current Phase: ____________  Phase Gate Date: ____________            ║
║  ┌─────────────────────────────────────────┬─────────────┐            ║
║  │ Phase Exit Criterion                    │ Met? (Y/N)  │            ║
║  ├─────────────────────────────────────────┼─────────────┤            ║
║  │ 1. ____________________________________│ ___         │            ║
║  │ 2. ____________________________________│ ___         │            ║
║  │ 3. ____________________________________│ ___         │            ║
║  │ 4. ____________________________________│ ___         │            ║
║  │ 5. ____________________________________│ ___         │            ║
║  └─────────────────────────────────────────┴─────────────┘            ║
║                                                                        ║
║  🚨 TOP 3 RISKS THIS MONTH                                            ║
║  1. _________________________________________________________________ ║
║  2. _________________________________________________________________ ║
║  3. _________________________________________________________________ ║
║                                                                        ║
║  🎯 TOP 3 PRIORITIES NEXT MONTH                                       ║
║  1. _________________________________________________________________ ║
║  2. _________________________________________________________________ ║
║  3. _________________________________________________________________ ║
║                                                                        ║
║  📝 NOTES / DECISIONS MADE:                                           ║
║  ________________________________________________________________     ║
║  ________________________________________________________________     ║
║                                                                        ║
╚══════════════════════════════════════════════════════════════════════════╝
```

---

## 5.6 Quarterly Strategic Review Framework

Every **13 weeks** (end of each quarter), conduct a 2-hour structured review:

### Review Agenda (2 hours)

| Block | Duration | Content |
|-------|----------|---------|
| **1. Numbers** | 30 min | MRR, users, churn, unit economics, burn rate — pure data, no stories |
| **2. Market** | 15 min | Competitor moves (changelog review), Amazon API/policy changes, market signals |
| **3. Product** | 30 min | Feature usage analytics; top user complaints; data quality trends; technical debt assessment |
| **4. Risks** | 15 min | Update risk register; re-score probabilities; check if any new risks emerged |
| **5. Strategy** | 20 min | Are we on the right track? Portfolio rebalancing? Phase gate check? |
| **6. Decisions** | 10 min | Top 3 decisions for next quarter; write them down; assign deadlines |

### Quarterly Scorecard Template

| Dimension | Weight | Q Score (1-10) | Weighted | Notes |
|-----------|--------|----------------|----------|-------|
| Revenue Growth | 25% | — | — | MRR trend, new vs. expansion revenue |
| User Acquisition | 20% | — | — | Signups, conversion, activation |
| Product Quality | 20% | — | — | DQ Score, load time, uptime, bugs |
| Competitive Position | 15% | — | — | Feature parity, unique value, moat durability |
| Team & Sustainability | 10% | — | — | Founder health, bus factor, documentation |
| Strategic Progress | 10% | — | — | Phase milestones, vision alignment |
| **Composite Score** | 100% | — | **—/10** | ≥7.0 = on track; 5.0-6.9 = course correct; <5.0 = escalate |

### Quarterly Strategic Calendar

| Quarter | Phase(s) | Key Gate | Critical Decisions |
|---------|----------|----------|-------------------|
| **Q2 2026** (Apr-Jun) | Phase 1 → Phase 2 start | Phase 1 gate (May 15) | GO for beta? Pricing strategy? |
| **Q3 2026** (Jul-Sep) | Phase 2 (full) | Phase 2 gate (Sep 30) | First revenue real? Hire? DACH timing? |
| **Q4 2026** (Oct-Dec) | Phase 3 | Phase 3 mid-check | Public launch? Marketing spend increase? |
| **Q1 2027** (Jan-Mar) | Phase 3 → Phase 4 | Phase 3 gate (Jan 31) | Scale or consolidate? Funding exploration? |

---

# STRATEGIC SUMMARY — ONE-PAGE VIEW

```
╔══════════════════════════════════════════════════════════════════════════╗
║                                                                        ║
║    A C C   S T R A T E G I C   P O R T F O L I O   P L A N           ║
║    March 2026 — March 2027                                             ║
║                                                                        ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                        ║
║  VISION: "ACC makes Amazon sellers profitable by telling them the      ║
║  truth about their margins — in real time, to the penny,               ║
║  across every marketplace."                                            ║
║                                                                        ║
║  MARKET: SAM $180-280M │ SOM Y1-2: $1.2-3.6M │ Window: 12-18mo      ║
║                                                                        ║
║  ═══════════════════════════════════════════════════════════            ║
║                                                                        ║
║  PHASE 1 — HARDEN     │ Apr–May 2026         │ 6-7 weeks             ║
║    Gate: PPT < 2s, DQ ≥ 82, Ads < 6h, 20 pages, uptime live         ║
║                                                                        ║
║  PHASE 2 — BETA       │ May–Sep 2026         │ 20 weeks              ║
║    Gate: 200 signups, 50 WAU, 20 paid, $2K MRR, NPS ≥ 30            ║
║                                                                        ║
║  PHASE 3 — LAUNCH     │ Oct 2026–Jan 2027    │ 16 weeks              ║
║    Gate: 100 paid, $5K MRR, <5% churn, NPS ≥ 40, DACH ready         ║
║                                                                        ║
║  PHASE 4 — SCALE      │ Feb 2027+            │ Ongoing               ║
║    Trigger: $10K MRR → first hire; $30K MRR → Series A explore       ║
║                                                                        ║
║  ═══════════════════════════════════════════════════════════            ║
║                                                                        ║
║  FINANCIALS (BASE):  $10K MRR by Mar'27 │ 200 users │ $120K ARR     ║
║  BREAK-EVEN:         60 users @ €55 ARPU → ~$3.3K/mo                ║
║  EXPECTED ROI:       1,100-1,700% on investment                       ║
║                                                                        ║
║  TOP RISKS:          #1 Burnout │ #2 Silent crash │ #3 Competition   ║
║  TOP MOATS:          #1 ERP COGS │ #2 CM1 real-time │ #3 Logistics  ║
║                                                                        ║
║  RESOURCE RULE:      50h/week max │ Build:Use ≤ 50:50 │ Dogs = 0%   ║
║  FIRST HIRE:         Frontend contractor @ $5K MRR trigger            ║
║                                                                        ║
╚══════════════════════════════════════════════════════════════════════════╝
```

---

**Document prepared by**: Studio Producer Agent  
**Date**: 2026-03-13  
**Classification**: Strategic — Internal  
**Review cycle**: Quarterly (next review: Q2 2026 end-of-quarter, Jun 30, 2026)  
**Next action**: Phase 1 kickoff — Week 1 starts with C-5 (UptimeRobot) + C-3 (FX warning system)

---

*This document synthesizes findings from 6 specialist agent reports (Market Intelligence, Feedback Synthesis, UX Research, Data Audit, Technology Stack Assessment, Phase 0 Executive Summary) into a unified strategic portfolio plan. All financial projections are estimates based on Phase 0 validated data and should be revisited quarterly.*
