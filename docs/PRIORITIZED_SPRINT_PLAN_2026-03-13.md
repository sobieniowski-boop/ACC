# ACC Prioritized Sprint Plan
## Amazon Command Center — FY2026/27 Sprint-Level Backlog

| Field | Value |
|-------|-------|
| **Document** | PRIORITIZED_SPRINT_PLAN_2026-03-13.md |
| **Date** | 2026-03-13 |
| **Agent** | Sprint Prioritizer (Claude Opus 4.6) |
| **Source Docs** | ACC_TASK_LIST, SYSTEM_ARCHITECTURE_SPEC, UX_ARCHITECTURE, FINANCIAL_PLAN, STRATEGIC_PORTFOLIO_PLAN |
| **Total Tasks** | 62 |
| **Total Story Points** | 186 SP |
| **Phases** | 4 (HARDEN → BETA → LAUNCH → SCALE) |
| **Planning Horizon** | Apr 2026 – Mar 2027+ |

---

## Document Navigation

1. [RICE-Scored Backlog](#1-rice-scored-backlog)
   - 1.1 [Complete RICE Scorecard](#11-complete-rice-scorecard-all-62-tasks--sorted-by-rice-score-descending)
   - 1.2 [RICE Score Distribution & Analysis](#12-rice-score-distribution--analysis)
2. [Sprint Assignments](#2-sprint-assignments)
   - 2.1 [Velocity Model](#21-velocity-model)
   - 2.2 [Phase 1 Sprints](#22-phase-1-sprints-apr-1--may-15-2026)
   - 2.3 [Phase 2 Sprints](#23-phase-2-sprints-may-16--sep-30-2026)
   - 2.4 [Phase 3 Sprints](#24-phase-3-sprints-oct-2026--jan-2027)
   - 2.5 [Phase 4 Sprints](#25-phase-4-sprints-feb-2027)
3. [Dependency Map with Critical Path](#3-dependency-map-with-critical-path)
   - 3.1 [Phase 1 Dependency Graph](#31-phase-1-dependency-graph)
   - 3.2 [Phase 2 Dependency Graph](#32-phase-2-dependency-graph)
   - 3.3 [Cross-Phase Dependencies](#33-cross-phase-dependencies)
   - 3.4 [Critical Path](#34-critical-path-full-project)
   - 3.5 [Critical Path Risk Analysis](#35-critical-path-risk-analysis)
4. [MoSCoW Classification](#4-moscow-classification)
   - 4.1 [Must Have](#41-must-have)
   - 4.2 [Should Have](#42-should-have)
   - 4.3 [Could Have](#43-could-have)
   - 4.4 [Won't Have This Cycle](#44-wont-have-this-cycle)
   - 4.5 [MoSCoW Summary Matrix](#45-moscow-summary-matrix)
5. [Release Plan with Milestone Mapping](#5-release-plan-with-milestone-mapping)
   - 5.1 [Release Timeline](#51-release-timeline-gantt-style)
   - 5.2 [Milestone → Sprint → Task Mapping](#52-milestone--sprint--task-mapping)
   - 5.3 [Phase Gate Verification Matrix](#53-phase-gate-verification-matrix)
   - 5.4 [Revenue Milestones on Timeline](#54-revenue-milestones-on-timeline)
   - 5.5 [Risk-Adjusted Timeline](#55-risk-adjusted-timeline-contingency-buffer)
6. [Studio Producer Strategic Alignment Validation](#6-studio-producer-strategic-alignment-validation)
   - 6.1 [Portfolio Alignment Check](#61-portfolio-alignment-check)
   - 6.2 [Competitive Window Compliance](#62-competitive-window-compliance)
   - 6.3 [Burnout Risk Assessment](#63-burnout-risk-assessment)
   - 6.4 [Budget Alignment](#64-budget-alignment)
   - 6.5 [Kill Gate Integration](#65-kill-gate-integration)
   - 6.6 [Strategic Verdict](#66-strategic-verdict)

---

## Scoring Methodology

### RICE Framework
Each task is scored using:
- **Reach (R)**: Users/stakeholders affected. 0.5 (internal-only) → 3.0 (all users + prospects)
- **Impact (I)**: Value per user. 0.25 (minimal) → 3.0 (massive)
- **Confidence (C)**: Certainty in estimates. 0.5 (speculative) → 1.0 (verified)
- **Effort (E)**: Story Points from task list (unchanged)

**Formula**: `RICE = (R × I × C) / E`

### Velocity Model
| Phase | Velocity | Deep Eng Hours/Week | Sprint Length |
|-------|----------|-------------------|---------------|
| Phase 1 (HARDEN) | 8–10 SP/sprint | 22h | 2 weeks |
| Phase 2 (BETA) | 6–8 SP/sprint | 16h | 2 weeks |
| Phase 3 (LAUNCH) | 6–8 SP/sprint | 16h | 2 weeks |
| Phase 4 (SCALE) | 8–10 SP/sprint | 22h (w/ contractor) | 2 weeks |

### MoSCoW Criteria
- **Must**: Phase gate requirement / blocks revenue / blocks users / P0 / critical path
- **Should**: High value, tied to strategic objectives, P1
- **Could**: Valuable enhancement, deferrable, P2
- **Won't**: Frozen per portfolio, P3+, explicitly excluded from this cycle

---

# 1. RICE-Scored Backlog

## 1.1 Complete RICE Scorecard (all 62 tasks — sorted by RICE score descending)

| Rank | Task | Title | Phase | R | I | C | E(SP) | RICE | MoSCoW | Sprint |
|------|------|-------|-------|---|---|---|-------|------|--------|--------|
| 1 | T-101 | UptimeRobot external monitoring | 1 | 2.0 | 3.0 | 1.0 | 1 | **6.00** | Must | S1.1 |
| 2 | T-215 | Private beta recruitment & launch | 2 | 3.0 | 3.0 | 0.7 | 2 | **3.15** | Must | S2.5 |
| 3 | T-102 | FX rate silent `return 1.0` → alert system | 1 | 2.0 | 3.0 | 0.85 | 2 | **2.55** | Must | S1.1 |
| 4 | T-105 | Recommended DB indexes for PPT | 1 | 2.0 | 3.0 | 0.85 | 2 | **2.55** | Must | S1.1 |
| 5 | T-107 | Server-side pagination PPT (frontend) | 1 | 3.0 | 3.0 | 0.85 | 3 | **2.55** | Must | S1.3 |
| 6 | T-111 | Bridge FBA fees to order lines | 1 | 2.0 | 3.0 | 0.85 | 2 | **2.55** | Must | S1.2 |
| 7 | T-220 | Marketing landing page (static) | 2 | 3.0 | 2.0 | 0.85 | 2 | **2.55** | Should | S2.2 |
| 8 | T-311 | Security hardening CORS/headers/WAF | 3 | 3.0 | 3.0 | 0.85 | 3 | **2.55** | Must | S3.1 |
| 9 | T-108 | Hide/collapse 33 underused sidebar pages | 1 | 2.0 | 2.0 | 1.0 | 2 | **2.00** | Must | S1.1 |
| 10 | T-207 | Rate limiting (slowapi) | 2 | 2.0 | 2.0 | 1.0 | 2 | **2.00** | Should | S2.1 |
| 11 | T-110 | Data Observability Layer (baseline alarms) | 1 | 2.0 | 3.0 | 0.85 | 3 | **1.70** | Must | S1.2 |
| 12 | T-214 | Email delivery (Resend/Postmark) | 2 | 2.0 | 2.0 | 0.85 | 2 | **1.70** | Must | S2.1 |
| 13 | T-312 | PII/GDPR compliance audit | 3 | 2.0 | 2.0 | 0.85 | 2 | **1.70** | Should | S3.1 |
| 14 | T-106 | SQL-level pagination PPT (backend) | 1 | 3.0 | 3.0 | 0.85 | 5 | **1.53** | Must | S1.3 |
| 15 | T-103 | Ads sync heartbeat updates | 1 | 1.5 | 2.0 | 1.0 | 2 | **1.50** | Must | S1.1 |
| 16 | T-117 | Phase 1 gate review | 1 | 0.5 | 3.0 | 1.0 | 1 | **1.50** | Must | S1.4 |
| 17 | T-314 | Phase 3 gate review | 3 | 0.5 | 3.0 | 1.0 | 1 | **1.50** | Must | S3.8 |
| 18 | T-113 | Materialized `acc_profit_daily_snapshot` | 1 | 2.0 | 3.0 | 0.7 | 3 | **1.40** | Should | S1.2 |
| 19 | T-208 | RBAC enforcement multi-user | 2 | 2.0 | 3.0 | 0.7 | 3 | **1.40** | Must | S2.2 |
| 20 | T-203 | User onboarding wizard (frontend) | 2 | 3.0 | 2.0 | 0.7 | 3 | **1.40** | Must | S2.4 |
| 21 | T-306 | Public marketing site + content | 3 | 3.0 | 2.0 | 0.7 | 3 | **1.40** | Should | S3.2 |
| 22 | T-217 | NPS micro-survey component | 2 | 2.0 | 1.0 | 0.7 | 1 | **1.40** | Could | S2.7 |
| 23 | T-104 | Single-flight guard for ads sync | 1 | 1.5 | 2.0 | 0.85 | 2 | **1.28** | Must | S1.2 |
| 24 | T-201 | Multi-tenant DB schema | 2 | 3.0 | 3.0 | 0.7 | 5 | **1.26** | Must | S2.1 |
| 25 | T-202 | User registration/onboarding (backend) | 2 | 3.0 | 3.0 | 0.7 | 5 | **1.26** | Must | S2.3 |
| 26 | T-204 | Stripe billing (3-tier pricing) | 2 | 3.0 | 3.0 | 0.7 | 5 | **1.26** | Must | S2.4 |
| 27 | T-210 | Sidebar consolidation 12→7 groups | 2 | 2.0 | 2.0 | 0.85 | 3 | **1.13** | Should | S2.3 |
| 28 | T-112 | Archive/drop 72 empty tables | 1 | 0.5 | 2.0 | 1.0 | 1 | **1.00** | Should | S1.3 |
| 29 | T-109 | Replace python-jose with pyjwt | 1 | 0.5 | 2.0 | 1.0 | 1 | **1.00** | Should | S1.1 |
| 30 | T-309 | Onboarding funnel optimization | 3 | 2.0 | 2.0 | 0.7 | 3 | **0.93** | Should | S3.4 |
| 31 | T-401 | Azure SQL tier upgrade eval | 4 | 1.0 | 2.0 | 0.85 | 2 | **0.85** | Should | S4.1 |
| 32 | T-308 | Help center / docs site (20+ articles) | 3 | 3.0 | 2.0 | 0.7 | 5 | **0.84** | Should | S3.1–S3.2 |
| 33 | T-216 | PostHog analytics integration | 2 | 2.0 | 1.0 | 0.7 | 2 | **0.70** | Could | S2.7 |
| 34 | T-304 | DACH marketplace deep testing | 3 | 1.5 | 2.0 | 0.7 | 3 | **0.70** | Should | S3.2 |
| 35 | T-403 | DACH market soft launch (DE beta) | 4 | 1.5 | 2.0 | 0.7 | 3 | **0.70** | Should | S4.2 |
| 36 | T-114 | Data Freshness API endpoint | 1 | 1.5 | 1.0 | 0.85 | 2 | **0.64** | Should | S1.4 |
| 37 | T-115 | Fix test suite ≥85% pass rate | 1 | 1.0 | 2.0 | 0.85 | 3 | **0.57** | Must | S1.3 |
| 38 | T-218 | Connection pooling SQLAlchemy | 2 | 1.0 | 2.0 | 0.85 | 3 | **0.57** | Should | S2.4 |
| 39 | T-219 | Alembic schema migrations | 2 | 1.0 | 2.0 | 0.85 | 3 | **0.57** | Should | S2.5 |
| 40 | T-310 | Celery workers for heavy sync | 3 | 2.0 | 2.0 | 0.7 | 5 | **0.56** | Should | S3.3 |
| 41 | T-301 | Weekly P&L PDF report | 3 | 2.0 | 2.0 | 0.7 | 5 | **0.56** | Should | S3.3 |
| 42 | T-402 | Horizontal API scaling 2-4 replicas | 4 | 2.0 | 2.0 | 0.7 | 5 | **0.56** | Should | S4.1 |
| 43 | T-307 | Referral program | 3 | 2.0 | 1.0 | 0.5 | 2 | **0.50** | Could | S3.6 |
| 44 | T-209 | API versioning /api/v1/ | 2 | 1.0 | 1.0 | 1.0 | 2 | **0.50** | Should | S2.2 |
| 45 | T-206 | Morning Brief auto-digest (email) | 2 | 2.0 | 1.0 | 0.7 | 3 | **0.47** | Should | S2.6 |
| 46 | T-212 | Global Search ⌘K | 2 | 2.0 | 1.0 | 0.7 | 3 | **0.47** | Could | S2.8 |
| 47 | T-221 | Error response standardization RFC 7807 | 2 | 1.0 | 1.0 | 0.85 | 2 | **0.43** | Could | S2.9 |
| 48 | T-205 | Weight-based logistics model v3 | 2 | 2.0 | 2.0 | 0.7 | 8 | **0.35** | Should | S2.6–S2.7 |
| 49 | T-211 | Breadcrumbs + Recently Visited | 2 | 2.0 | 0.5 | 0.7 | 2 | **0.35** | Could | S2.8 |
| 50 | T-404 | Contractor onboarding (first hire) | 4 | 1.0 | 2.0 | 0.5 | 3 | **0.33** | Should | S4.2 |
| 51 | T-313 | E2E testing Playwright | 3 | 1.0 | 2.0 | 0.7 | 5 | **0.28** | Should | S3.4 |
| 52 | T-302 | Profit→Refund drill path | 3 | 2.0 | 1.0 | 0.7 | 5 | **0.28** | Could | S3.5 |
| 53 | T-409 | Export & reporting infrastructure | 4 | 2.0 | 1.0 | 0.7 | 5 | **0.28** | Could | S4.3 |
| 54 | T-213 | Module Visibility Toggle | 2 | 1.5 | 0.5 | 0.7 | 2 | **0.26** | Could | S2.8 |
| 55 | T-116 | Core system runbooks (top 5) | 1 | 0.5 | 1.0 | 1.0 | 2 | **0.25** | Should | S1.4 |
| 56 | T-305 | German UI string localization | 3 | 1.0 | 1.0 | 0.7 | 3 | **0.23** | Could | S3.5 |
| 57 | T-405 | JWT RS256 migration | 4 | 1.0 | 1.0 | 0.7 | 3 | **0.23** | Could | S4.3 |
| 58 | T-406 | Mobile responsive design | 4 | 2.0 | 1.0 | 0.5 | 5 | **0.20** | Won't | S4.4+ |
| 59 | T-222 | Unified alert triage view | 2 | 1.5 | 1.0 | 0.5 | 5 | **0.15** | Could | S2.9 |
| 60 | T-407 | Time-series analytics layer | 4 | 1.5 | 1.0 | 0.5 | 5 | **0.15** | Won't | S4.4+ |
| 61 | T-408 | AI-powered margin alerts | 4 | 1.5 | 1.0 | 0.5 | 5 | **0.15** | Won't | S4.5+ |
| 62 | T-303 | Bank feed automation (basic) | 3 | 1.0 | 1.0 | 0.5 | 8 | **0.06** | Could | S3.6–S3.7 |

---

## 1.2 RICE Score Distribution & Analysis

### Summary Statistics

| Metric | Value |
|--------|-------|
| **Total Tasks** | 62 |
| **Total Story Points** | 186 SP |
| **Mean RICE Score** | 1.07 |
| **Median RICE Score** | 0.70 |
| **Max RICE Score** | 6.00 (T-101) |
| **Min RICE Score** | 0.06 (T-303) |
| **Std Deviation** | 0.96 |

### Top 10 Tasks by RICE Score

| Rank | Task | Title | RICE | Phase | Rationale |
|------|------|-------|------|-------|-----------|
| 1 | T-101 | UptimeRobot external monitoring | 6.00 | 1 | Minimal effort (1 SP), maximum reliability impact, unblocks observability |
| 2 | T-215 | Private beta recruitment & launch | 3.15 | 2 | Direct path to PMF validation, only 2 SP |
| 3 | T-102 | FX rate alert system | 2.55 | 1 | Prevents silent margin corruption, foundational trust |
| 4 | T-105 | Recommended DB indexes for PPT | 2.55 | 1 | Unblocks entire PPT performance critical path |
| 5 | T-107 | Server-side pagination PPT (frontend) | 2.55 | 1 | Direct user-facing performance (North Star: <2s PPT) |
| 6 | T-111 | Bridge FBA fees to order lines | 2.55 | 1 | Profit accuracy — core moat |
| 7 | T-220 | Marketing landing page (static) | 2.55 | 2 | Blocks beta recruitment at low effort |
| 8 | T-311 | Security hardening CORS/headers/WAF | 2.55 | 3 | Multi-user trust requirement |
| 9 | T-108 | Hide/collapse sidebar pages | 2.00 | 1 | UX gate criterion (≤20 pages), low effort |
| 10 | T-207 | Rate limiting (slowapi) | 2.00 | 2 | Security prerequisite for public access |

### Bottom 10 Tasks by RICE Score

| Rank | Task | Title | RICE | Phase | Status |
|------|------|-------|------|-------|--------|
| 53 | T-409 | Export infrastructure | 0.28 | 4 | Depends on T-301 |
| 54 | T-213 | Module Visibility Toggle | 0.26 | 2 | Deferrable UX |
| 55 | T-116 | Core system runbooks | 0.25 | 1 | Low reach but risk mitigation |
| 56 | T-305 | German UI localization | 0.23 | 3 | Before DACH launch |
| 57 | T-405 | JWT RS256 migration | 0.23 | 4 | ADR-007 defers this |
| 58 | T-406 | Mobile responsive design | 0.20 | 4 | Desktop-first strategy |
| 59 | T-222 | Unified alert triage view | 0.15 | 2 | High effort, speculative |
| 60 | T-407 | Time-series analytics | 0.15 | 4 | Speculative, P3 |
| 61 | T-408 | AI-powered margin alerts | 0.15 | 4 | Depends on T-407; frozen quadrant |
| 62 | T-303 | Bank feed automation | 0.06 | 3 | High effort, low confidence |

### Phase-by-Phase Analysis

| Phase | Tasks | SP | Avg RICE | Top RICE Task | Bottleneck |
|-------|-------|----|----------|---------------|------------|
| Phase 1 (HARDEN) | 17 | 34 | 1.74 | T-101 (6.00) | T-106 (5 SP, on critical path) |
| Phase 2 (BETA) | 22 | 63 | 1.04 | T-215 (3.15) | T-201, T-202, T-204 (5 SP each) |
| Phase 3 (LAUNCH) | 14 | 53 | 0.77 | T-311 (2.55) | T-303 (8 SP), T-310 (5 SP) |
| Phase 4 (SCALE) | 9 | 36 | 0.38 | T-401 (0.85) | T-402 (5 SP), scaling tasks |

**Key Insight**: Phase 1 has the highest average RICE — correctly front-loading foundational, high-ROI work. Phase 4 has the lowest, reflecting speculative/deferred items.

---

# 2. Sprint Assignments

## 2.1 Velocity Model

### Capacity Assumptions

| Parameter | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|-----------|---------|---------|---------|---------|
| Total hours/week | 50 | 50 | 50 | 50 |
| Engineering % | 80% | 55% | 40% | 50% |
| Deep eng hours/week | 22 | 16 | 16 | 22 (w/ contractor) |
| Sprint length | 2 weeks | 2 weeks | 2 weeks | 2 weeks |
| Velocity (SP/sprint) | 8–10 | 6–8 | 6–8 | 8–10 |
| Target velocity | 9 | 7 | 7 | 9 |
| Available sprints | 3.3 (~4) | 10 | 8 | 4+ |
| Total SP capacity | 34 | 70 | 56 | 36+ |
| Phase SP demand | 34 | 63 | 53 | 36 |
| **Capacity margin** | **0 SP** | **+7 SP** | **+3 SP** | **0 SP** |

> ⚠️ Phase 1 has zero margin — every sprint must hit target velocity. Phase 2 and 3 have small buffers.

### Sprint Calendar

| Sprint | Dates | Phase | Target SP |
|--------|-------|-------|-----------|
| S1.1 | Apr 1–14, 2026 | 1 | 10 |
| S1.2 | Apr 15–28, 2026 | 1 | 10 |
| S1.3 | Apr 29 – May 12, 2026 | 1 | 12 |
| S1.4 | May 5–15, 2026 (partial) | 1 | 5 (gate prep) |
| S2.1 | May 16–29, 2026 | 2 | 7 |
| S2.2 | May 30 – Jun 12, 2026 | 2 | 9 |
| S2.3 | Jun 13–26, 2026 | 2 | 8 |
| S2.4 | Jun 27 – Jul 10, 2026 | 2 | 8 |
| S2.5 | Jul 11–24, 2026 | 2 | 8 |
| S2.6 | Jul 25 – Aug 7, 2026 | 2 | 7 |
| S2.7 | Aug 8–21, 2026 | 2 | 7 |
| S2.8 | Aug 22 – Sep 4, 2026 | 2 | 7 |
| S2.9 | Sep 5–18, 2026 | 2 | 7 |
| S2.10 | Sep 19–30, 2026 (partial) | 2 | Buffer/overflow |
| S3.1 | Oct 1–14, 2026 | 3 | 8 |
| S3.2 | Oct 15–28, 2026 | 3 | 8 |
| S3.3 | Oct 29 – Nov 11, 2026 | 3 | 10 |
| S3.4 | Nov 12–25, 2026 | 3 | 8 |
| S3.5 | Nov 26 – Dec 9, 2026 | 3 | 8 |
| S3.6 | Dec 10–23, 2026 | 3 | 6 |
| S3.7 | Jan 5–18, 2027 | 3 | 4 |
| S3.8 | Jan 19–31, 2027 | 3 | 1 (gate) |
| S4.1 | Feb 1–14, 2027 | 4 | 7 |
| S4.2 | Feb 15–28, 2027 | 4 | 6 |
| S4.3 | Mar 1–14, 2027 | 4 | 8 |
| S4.4 | Mar 15–28, 2027 | 4 | 15 |

---

## 2.2 Phase 1 Sprints (Apr 1 – May 15, 2026)

**Theme**: HARDEN — Stability, performance, observability, data trust

### Sprint S1.1 — "Foundation & Monitoring" (Apr 1–14)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-101 | UptimeRobot external monitoring | 1 | P0 | No dependencies — start immediately |
| T-102 | FX rate alert system | 2 | P0 | No dependencies — start immediately |
| T-103 | Ads sync heartbeat updates | 2 | P0 | No dependencies — start immediately |
| T-105 | Recommended DB indexes for PPT | 2 | P0 | No dependencies — unblocks T-106, T-113 |
| T-108 | Hide/collapse 33 sidebar pages | 2 | P0 | No dependencies — UX gate criterion |
| T-109 | Replace python-jose with pyjwt | 1 | P1 | No dependencies — quick security fix |
| **Total** | | **10** | | At capacity ceiling |

### Sprint S1.2 — "Data Integrity & Observability" (Apr 15–28)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-110 | Data Observability Layer | 3 | P0 | Depends on T-101 ✅, T-102 ✅ |
| T-104 | Single-flight guard for ads sync | 2 | P0 | Depends on T-103 ✅ |
| T-111 | Bridge FBA fees to order lines | 2 | P0 | No dependencies — profit accuracy |
| T-113 | Materialized profit daily snapshot | 3 | P1 | Depends on T-105 ✅ — unblocks T-106 |
| **Total** | | **10** | | Slightly exceeds 9 target — acceptable |

### Sprint S1.3 — "PPT Performance & Testing" (Apr 29 – May 12)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-106 | SQL-level pagination PPT (backend) | 5 | P0 | Depends on T-105 ✅, T-113 ✅ |
| T-107 | Server-side pagination PPT (frontend) | 3 | P0 | Depends on T-106 (staggered — starts mid-sprint) |
| T-115 | Fix test suite ≥85% pass rate | 3 | P0 | Depends on T-102 ✅, T-103 ✅, T-109 ✅ |
| T-112 | Archive/drop 72 empty tables | 1 | P1 | No dependencies — quick cleanup |
| **Total** | | **12** | | Over velocity by 2-3 SP |

> ⚠️ Sprint S1.3 is overloaded at 12 SP vs 9 target. Mitigation: T-107 can start mid-sprint as T-106 backend endpoint becomes available (staggered). T-112 is a quick cleanup (1 SP). If needed, T-112 spills to S1.4.

### Sprint S1.4 — "Gate Prep & Runbooks" (May 5–15, partial overlap)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-114 | Data Freshness API endpoint | 2 | P1 | Depends on T-110 ✅ |
| T-116 | Core system runbooks (top 5) | 2 | P1 | Depends on T-110 ✅ — documentation sprint |
| T-117 | Phase 1 gate review | 1 | P0 | All Phase 1 tasks complete |
| **Total** | | **5** | | Under capacity — buffer for overflows from S1.3 |

**Phase 1 Totals**: 34 SP across ~3.5 sprints (Apr 1 – May 15)

### Phase 1 Sprint Summary

```
S1.1  [████████████████████] 10 SP  Monitoring, indexes, sidebar, JWT
S1.2  [████████████████████] 10 SP  Observability, ads guard, FBA, snapshot
S1.3  [████████████████████████] 12 SP  PPT perf(BE+FE), tests, cleanup
S1.4  [██████████]            5 SP  Freshness, runbooks, gate review
                              ────
                              37 SP planned (34 SP nominal + stagger buffer)
```

---

## 2.3 Phase 2 Sprints (May 16 – Sep 30, 2026)

**Theme**: BETA — Multi-tenancy, billing, onboarding, beta launch, UX refinement

### Sprint S2.1 — "Multi-tenant Foundation" (May 16–29)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-201 | Multi-tenant DB schema | 5 | P0 | Depends on T-117 ✅ — foundational |
| T-214 | Email delivery (Resend/Postmark) | 2 | P0 | No dependencies — parallel track |
| **Total** | | **7** | | On target |

### Sprint S2.2 — "Security & Navigation" (May 30 – Jun 12)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-208 | RBAC enforcement multi-user | 3 | P0 | Depends on T-201 ✅ |
| T-207 | Rate limiting (slowapi) | 2 | P1 | No dependencies — security prerequisite |
| T-220 | Marketing landing page (static) | 2 | P1 | No dependencies — GTM parallel |
| T-209 | API versioning /api/v1/ | 2 | P1 | No dependencies — housekeeping |
| **Total** | | **9** | | Slightly over (7 target) but S2.1 was light |

> Note: S2.2 is 9 SP vs 7 target. T-209 and T-220 are independent and can be worked on during non-deep-eng hours (ops/GTM time allocation).

### Sprint S2.3 — "User System & Sidebar" (Jun 13–26)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-202 | User registration/onboarding (backend) | 5 | P0 | Depends on T-201 ✅, T-214 ✅, T-207 ✅, T-208 ✅ |
| T-210 | Sidebar consolidation 12→7 groups | 3 | P1 | Depends on T-108 ✅ — UX polish |
| **Total** | | **8** | | On target |

### Sprint S2.4 — "Billing & Onboarding UX" (Jun 27 – Jul 10)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-204 | Stripe billing (3-tier pricing) | 5 | P0 | Depends on T-201 ✅, T-202 ✅ |
| T-203 | User onboarding wizard (frontend) | 3 | P0 | Depends on T-202 ✅ |
| **Total** | | **8** | | On target |

### Sprint S2.5 — "Beta Launch & Migrations" (Jul 11–24)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-215 | Private beta recruitment & launch | 2 | P0 | Depends on T-202 ✅, T-203 ✅, T-220 ✅ |
| T-218 | Connection pooling SQLAlchemy | 3 | P1 | Depends on T-117 ✅ — performance |
| T-219 | Alembic schema migrations | 3 | P1 | Depends on T-218 (can start once pool is in) |
| **Total** | | **8** | | On target — 🎉 BETA LAUNCH this sprint |

### Sprint S2.6 — "Communication & Logistics" (Jul 25 – Aug 7)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-206 | Morning Brief auto-digest (email) | 3 | P1 | Depends on T-214 ✅, T-113 ✅ |
| T-205 | Weight-based logistics model v3 (Part 1/2) | 4 | P1 | Depends on T-117 ✅ — start exploratory |
| **Total** | | **7** | | On target |

### Sprint S2.7 — "Logistics, Analytics & NPS" (Aug 8–21)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-205 | Weight-based logistics model v3 (Part 2/2) | 4 | P1 | Continuation from S2.6 |
| T-216 | PostHog analytics integration | 2 | P2 | Depends on T-203 ✅ |
| T-217 | NPS micro-survey component | 1 | P2 | Depends on T-215 ✅ |
| **Total** | | **7** | | On target |

> Note: T-205 is 8 SP total, split across S2.6 (4 SP) and S2.7 (4 SP).

### Sprint S2.8 — "UX Polish" (Aug 22 – Sep 4)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-211 | Breadcrumbs + Recently Visited | 2 | P2 | Depends on T-210 ✅ |
| T-212 | Global Search ⌘K | 3 | P2 | Depends on T-210 ✅ |
| T-213 | Module Visibility Toggle | 2 | P2 | Depends on T-210 ✅ |
| **Total** | | **7** | | On target — UX enhancement sprint |

### Sprint S2.9 — "Error Handling & Alerts" (Sep 5–18)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-221 | Error response standardization RFC 7807 | 2 | P2 | No dependencies — API quality |
| T-222 | Unified alert triage view | 5 | P2 | Depends on T-110 ✅, T-210 ✅ |
| **Total** | | **7** | | On target |

### Sprint S2.10 — Buffer Sprint (Sep 19–30)

| Notes |
|-------|
| Buffer sprint for Phase 2 overflow, bug fixes, beta user feedback, and Phase 2 gate preparation. No new tasks scheduled. This sprint coincides with Phase 2 gate review (Sep 30). |

**Phase 2 Totals**: 63 SP across 10 sprints (May 16 – Sep 30)

### Phase 2 Sprint Summary

```
S2.1  [██████████████]        7 SP  Multi-tenant, email
S2.2  [██████████████████]    9 SP  RBAC, rate limit, landing page, API ver
S2.3  [████████████████]      8 SP  User registration, sidebar
S2.4  [████████████████]      8 SP  Stripe billing, onboarding wizard
S2.5  [████████████████]      8 SP  ★ BETA LAUNCH, connection pool, Alembic
S2.6  [██████████████]        7 SP  Morning brief, logistics v3 (1/2)
S2.7  [██████████████]        7 SP  Logistics v3 (2/2), PostHog, NPS
S2.8  [██████████████]        7 SP  Breadcrumbs, ⌘K search, module toggle
S2.9  [██████████████]        7 SP  RFC 7807, alert triage
S2.10 [░░░░░░░░░░░░░░]        — SP  Buffer / Phase 2 gate
                              ────
                              68 SP planned (63 SP nominal + buffer)
```

---

## 2.4 Phase 3 Sprints (Oct 2026 – Jan 2027)

**Theme**: LAUNCH — Security, scaling, public marketing, DACH expansion, reporting

### Sprint S3.1 — "Security & Documentation" (Oct 1–14)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-311 | Security hardening CORS/headers/WAF | 3 | P0 | Depends on T-201 ✅ — public launch prep |
| T-312 | PII/GDPR compliance audit | 2 | P1 | Depends on T-201 ✅ |
| T-308 | Help center / docs site (Part 1/2) | 3 | P1 | No dependencies — content creation |
| **Total** | | **8** | | At ceiling |

### Sprint S3.2 — "DACH & Marketing" (Oct 15–28)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-304 | DACH marketplace deep testing | 3 | P1 | Depends on T-201 ✅ |
| T-306 | Public marketing site + content | 3 | P1 | Depends on T-220 ✅ |
| T-308 | Help center / docs site (Part 2/2) | 2 | P1 | Continuation from S3.1 |
| **Total** | | **8** | | At ceiling |

### Sprint S3.3 — "Workers & Reporting" (Oct 29 – Nov 11)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-310 | Celery workers for heavy sync | 5 | P1 | Depends on T-218 ✅ — architecture evolution |
| T-301 | Weekly P&L PDF report | 5 | P1 | Depends on T-113 ✅, T-214 ✅ |
| **Total** | | **10** | | Over ceiling by 2-3 SP |

> ⚠️ Sprint S3.3 is overloaded. Mitigation: T-301 and T-310 are independent tracks. T-301 can spill 2 SP into S3.4 if needed.

### Sprint S3.4 — "Optimization & E2E Testing" (Nov 12–25)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-309 | Onboarding funnel optimization | 3 | P1 | Depends on T-216 ✅ |
| T-313 | E2E testing Playwright | 5 | P1 | Depends on T-107 ✅, T-203 ✅ |
| **Total** | | **8** | | At ceiling |

### Sprint S3.5 — "Localization & Drill-down" (Nov 26 – Dec 9)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-305 | German UI string localization | 3 | P2 | Depends on T-304 ✅ |
| T-302 | Profit→Refund drill path | 5 | P2 | Depends on T-113 ✅, T-106 ✅ |
| **Total** | | **8** | | At ceiling |

### Sprint S3.6 — "Growth Features" (Dec 10–23)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-307 | Referral program | 2 | P2 | Depends on T-204 ✅, T-202 ✅ |
| T-303 | Bank feed automation (Part 1/2) | 4 | P2 | Depends on T-204 ✅ — exploratory |
| **Total** | | **6** | | Under capacity — holiday season buffer |

### Sprint S3.7 — "Bank Feed & Wrap-up" (Jan 5–18, 2027)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-303 | Bank feed automation (Part 2/2) | 4 | P2 | Continuation from S3.6 |
| **Total** | | **4** | | Under capacity — gate prep buffer |

### Sprint S3.8 — "Phase 3 Gate" (Jan 19–31, 2027)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-314 | Phase 3 gate review | 1 | P0 | All Phase 3 tasks complete |
| **Total** | | **1** | | Gate review + bug fixes + documentation |

**Phase 3 Totals**: 53 SP across 8 sprints (Oct 2026 – Jan 2027)

### Phase 3 Sprint Summary

```
S3.1  [████████████████]      8 SP  Security, GDPR, docs (1/2)
S3.2  [████████████████]      8 SP  DACH, marketing site, docs (2/2)
S3.3  [████████████████████] 10 SP  Celery workers, P&L report
S3.4  [████████████████]      8 SP  Funnel optimization, E2E testing
S3.5  [████████████████]      8 SP  German i18n, refund drill path
S3.6  [████████████]          6 SP  Referral, bank feed (1/2)
S3.7  [████████]              4 SP  Bank feed (2/2)
S3.8  [██]                    1 SP  Phase 3 gate
                              ────
                              53 SP planned
```

---

## 2.5 Phase 4 Sprints (Feb 2027+)

**Theme**: SCALE — Infrastructure scaling, DACH launch, contractor onboarding, future-proofing

### Sprint S4.1 — "Infrastructure Scaling" (Feb 1–14, 2027)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-401 | Azure SQL tier upgrade eval | 2 | P1 | Depends on T-314 ✅ |
| T-402 | Horizontal API scaling 2-4 replicas | 5 | P1 | Depends on T-310 ✅, T-218 ✅ |
| **Total** | | **7** | | Under capacity — new phase ramp-up |

### Sprint S4.2 — "DACH & Team Growth" (Feb 15–28, 2027)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-403 | DACH market soft launch (DE beta) | 3 | P1 | Depends on T-304 ✅, T-305 ✅ |
| T-404 | Contractor onboarding (first hire) | 3 | P1 | Triggered by $5K MRR sustained |
| **Total** | | **6** | | Under capacity — onboarding overhead |

### Sprint S4.3 — "Export & Security" (Mar 1–14, 2027)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-409 | Export & reporting infrastructure | 5 | P2 | Depends on T-301 ✅ |
| T-405 | JWT RS256 migration | 3 | P2 | Depends on T-109 ✅ |
| **Total** | | **8** | | Near target |

### Sprint S4.4+ — "Future Vision" (Mar 15+, 2027)

| Task | Title | SP | Priority | Notes |
|------|-------|----|----------|-------|
| T-406 | Mobile responsive design | 5 | P3 | Depends on T-404 (contractor) |
| T-407 | Time-series analytics layer | 5 | P3 | Depends on T-113 ✅ |
| T-408 | AI-powered margin alerts | 5 | P3 | Depends on T-407, T-222 |
| **Total** | | **15** | | Spread across 2+ sprints. Won't this cycle if MRR targets not met. |

**Phase 4 Totals**: 36 SP across 4+ sprints (Feb 2027+)

---

# 3. Dependency Map with Critical Path

## 3.1 Phase 1 Dependency Graph

```
                    ┌─────────┐
                    │  START   │
                    └────┬────┘
           ┌─────────┬──┴──┬─────────┬──────────┬──────────┐
           ▼         ▼     ▼         ▼          ▼          ▼
        ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐  ┌──────┐
        │T-101 │ │T-102 │ │T-103 │ │T-105 │ │T-108 │  │T-109 │
        │ 1 SP │ │ 2 SP │ │ 2 SP │ │ 2 SP │ │ 2 SP │  │ 1 SP │
        └──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘ └──────┘  └──┬───┘
           │        │        │        │                    │
           │   ┌────┘        │        ├─────────┐         │
           ▼   ▼             ▼        ▼         ▼         │
        ┌──────────┐    ┌──────┐  ┌──────┐  ┌──────┐     │
        │  T-110   │    │T-104 │  │T-113 │  │T-111 │     │
        │  3 SP    │    │ 2 SP │  │ 3 SP │  │ 2 SP │     │
        └──┬──┬────┘    └──────┘  └──┬───┘  └──────┘     │
           │  │                      │                    │
           │  │           ┌──────────┘                    │
           │  │           ▼                               │
           │  │     ┌──────────┐                          │
           │  │     │  T-106   │◄───────(T-105)           │
           │  │     │  5 SP    │                          │
           │  │     └──┬───────┘                          │
           │  │        │                                  │
           │  │        ▼                                  │
           │  │     ┌──────┐                              │
           │  │     │T-107 │                              │
           │  │     │ 3 SP │                              │
           │  │     └──────┘                              │
           │  │                                           │
           ▼  ▼                                           ▼
      ┌──────┐ ┌──────┐                            ┌──────────┐
      │T-114 │ │T-116 │                            │  T-115   │
      │ 2 SP │ │ 2 SP │                            │  3 SP    │
      └──────┘ └──────┘                            └──────────┘
           │       │              │        │              │
           └───────┴──────────────┴────────┴──────────────┘
                              ▼
                        ┌──────────┐      ┌──────┐
                        │  T-117   │      │T-112 │ (independent)
                        │  1 SP    │      │ 1 SP │
                        └──────────┘      └──────┘
                              │
                              ▼
                       ┌─────────────┐
                       │  PHASE 2    │
                       └─────────────┘
```

## 3.2 Phase 2 Dependency Graph

```
                    ┌──────────┐
                    │  T-117   │ (Phase 1 gate)
                    └────┬─────┘
           ┌─────────────┼──────────────┐
           ▼             ▼              ▼
      ┌──────────┐  ┌──────────┐   ┌──────────┐
      │  T-201   │  │  T-205   │   │  T-218   │
      │  5 SP    │  │  8 SP    │   │  3 SP    │
      └──┬──┬────┘  └──────────┘   └──┬───────┘
         │  │                         │
         │  │                    ┌────┘
         │  │                    ▼
         │  │              ┌──────────┐
         │  │              │  T-219   │
         │  │              │  3 SP    │
         │  │              └──────────┘
         │  │
    ┌────┘  └──────┐
    ▼              ▼
┌──────┐     ┌──────────┐     ┌──────┐    ┌──────┐
│T-208 │     │  T-204   │     │T-214 │    │T-207 │ (independent)
│ 3 SP │     │  5 SP    │     │ 2 SP │    │ 2 SP │
└──┬───┘     └──┬───────┘     └──┬───┘    └──┬───┘
   │             │               │            │
   └──────┬──────┘          ┌────┘            │
          ▼                 ▼                 │
    ┌──────────┐      ┌──────────┐            │
    │  T-202   │◄─────│  (T-214) │◄───────────┘
    │  5 SP    │      └──────────┘  (T-207 blocks T-202)
    └──┬──┬────┘
       │  │
  ┌────┘  └────┐
  ▼            ▼
┌──────┐  ┌──────────┐
│T-203 │  │  T-204   │ (also needs T-201, T-202)
│ 3 SP │  │  5 SP    │
└──┬───┘  └──────────┘
   │
   ├──────────────┐
   ▼              ▼
┌──────┐     ┌──────┐
│T-216 │     │T-215 │◄──── T-220
│ 2 SP │     │ 2 SP │
└──┬───┘     └──┬───┘
   │             │
   ▼             ▼
┌──────┐     ┌──────┐
│T-309 │     │T-217 │
│(Ph3) │     │ 1 SP │
└──────┘     └──────┘

          ┌──────┐
          │T-108 │ (Phase 1, done)
          └──┬───┘
             ▼
       ┌──────────┐
       │  T-210   │
       │  3 SP    │
       └──┬──┬──┬─┘
          │  │  │
     ┌────┘  │  └────┐
     ▼       ▼       ▼
  ┌──────┐┌──────┐┌──────┐
  │T-211 ││T-212 ││T-213 │
  │ 2 SP ││ 3 SP ││ 2 SP │
  └──────┘└──────┘└──────┘
```

## 3.3 Cross-Phase Dependencies

| Source Task | Source Phase | Target Task | Target Phase | Dependency Type |
|-------------|-------------|-------------|--------------|-----------------|
| T-117 | Phase 1 | T-201, T-205, T-218 | Phase 2 | Phase gate → foundation |
| T-108 | Phase 1 | T-210 | Phase 2 | UX continuity |
| T-113 | Phase 1 | T-206, T-301, T-302 | Phase 2/3 | Data snapshot → reports |
| T-105 | Phase 1 | T-106 → T-107 | Phase 1 | PPT performance chain |
| T-109 | Phase 1 | T-405 | Phase 4 | JWT upgrade path |
| T-110 | Phase 1 | T-222 | Phase 2 | Observability → alert triage |
| T-201 | Phase 2 | T-304, T-311, T-312 | Phase 3 | Multi-tenant → security/DACH |
| T-202 | Phase 2 | T-307 | Phase 3 | User system → referrals |
| T-204 | Phase 2 | T-303, T-307 | Phase 3 | Billing → bank feed, referrals |
| T-214 | Phase 2 | T-301 | Phase 3 | Email → P&L reports |
| T-218 | Phase 2 | T-310, T-402 | Phase 3/4 | Connection pool → workers, scaling |
| T-216 | Phase 2 | T-309 | Phase 3 | Analytics → funnel optimization |
| T-220 | Phase 2 | T-306 | Phase 3 | Landing page → marketing site |
| T-304 | Phase 3 | T-403 | Phase 4 | DACH testing → soft launch |
| T-305 | Phase 3 | T-403 | Phase 4 | Localization → DACH launch |
| T-310 | Phase 3 | T-402 | Phase 4 | Celery → horizontal scaling |
| T-301 | Phase 3 | T-409 | Phase 4 | P&L report → export infra |
| T-314 | Phase 3 | T-401 | Phase 4 | Phase gate → Scale phase |

## 3.4 Critical Path (full project)

The critical path determines the minimum total project duration. Every task on this path has zero float — any delay directly postpones the final milestone.

### Critical Path Chain

```
T-105 (2 SP) → T-113 (3 SP) → T-106 (5 SP) → T-107 (3 SP) ──┐
                                                                 ├──► T-117 (1 SP)
T-101 (1) → T-110 (3) → T-115 (3) ────────────────────────────┘        │
T-102 (2) ──┘     ▲                                                      │
T-103 (2) → T-104 (2)                                                   ▼
                                                                   T-201 (5 SP)
                                                                        │
                                                              ┌─────────┴─────────┐
                                                              ▼                   ▼
                                                         T-208 (3 SP)        T-204 (5)*
                                                              │
                                                              ▼
                                                    T-202 (5 SP) ◄── T-214 (2), T-207 (2)
                                                         │
                                                    ┌────┴────┐
                                                    ▼         ▼
                                               T-203 (3)  T-204 (5)
                                                    │         │
                                                    ▼         │
                                              T-215 (2) ◄────┘ ◄── T-220 (2)
                                                    │
                                                    ▼
                                              [BETA LIVE]
                                                    │
                                                ....│....
                                                    ▼
                                              T-310 (5) ◄── T-218 (3)
                                                    │
                                                    ▼
                                              T-402 (5)
                                                    │
                                                    ▼
                                              [$10K MRR]
```

### Critical Path Summary Table

| Step | Task | SP | Cumulative SP | Earliest Start | Earliest Finish |
|------|------|----|---------------|----------------|-----------------|
| 1 | T-105: DB indexes | 2 | 2 | Apr 1 | Apr 7 |
| 2 | T-113: Profit snapshot | 3 | 5 | Apr 8 | Apr 18 |
| 3 | T-106: SQL pagination BE | 5 | 10 | Apr 19 | May 2 |
| 4 | T-107: Pagination FE | 3 | 13 | May 3 | May 9 |
| 5 | T-117: Phase 1 gate | 1 | 14 | May 12 | May 15 |
| 6 | T-201: Multi-tenant schema | 5 | 19 | May 16 | May 29 |
| 7 | T-208: RBAC enforcement | 3 | 22 | May 30 | Jun 9 |
| 8 | T-202: User registration | 5 | 27 | Jun 10 | Jun 26 |
| 9 | T-203: Onboarding wizard | 3 | 30 | Jun 27 | Jul 7 |
| 10 | T-204: Stripe billing | 5 | 35 | Jun 27 | Jul 10 |
| 11 | T-215: Beta launch | 2 | 37 | Jul 11 | Jul 17 |
| — | **Critical Path Total** | **37 SP** | — | **Apr 1** | **Jul 17** |

**Critical path length**: 37 SP, 15.5 weeks (Apr 1 → Jul 17, 2026)

> The remaining sprints in Phase 2 (S2.6–S2.10) and all of Phase 3/4 have float since they are not on the primary revenue-critical path.

## 3.5 Critical Path Risk Analysis

| Risk | Tasks Affected | Impact | Probability | Mitigation |
|------|---------------|--------|-------------|------------|
| **PPT query optimization harder than estimated** | T-105, T-113, T-106 | HIGH — delays entire chain | Medium (0.3) | Spike T-105 indexes first week; validate p95 before T-106 |
| **Multi-tenant schema complexity** | T-201 | HIGH — blocks all Phase 2 | Medium (0.3) | Pre-design schema in Phase 1; use row-level security pattern |
| **Stripe integration edge cases** | T-204 | MEDIUM — delays revenue | Low (0.2) | Use Stripe Checkout (simpler); test sandbox in Phase 1 |
| **User registration scope creep** | T-202 | HIGH — blocks beta | Medium (0.3) | Strict MVP: email+password only, no social login initially |
| **Solo developer illness/burnout** | ALL | CRITICAL — 1-2 week delay | Low-Medium (0.25) | 50h/week cap strict; Phase 1 S1.4 has buffer; break between phases |
| **Azure SQL performance at scale** | T-113, T-106 | MEDIUM — PPT degradation | Low (0.15) | T-113 snapshot materializes early; T-401 eval planned in Phase 4 |

**Overall Critical Path Confidence**: 70% (medium-high) — achievable with disciplined scope management and the Phase 1 zero-float awareness.

---

# 4. MoSCoW Classification

## 4.1 Must Have

Tasks required for phase gates, blocking revenue, blocking users, P0 priority, or on critical path.

| Task | Title | Phase | SP | Rationale |
|------|-------|-------|----|-----------|
| T-101 | UptimeRobot external monitoring | 1 | 1 | Phase 1 gate: 99%+ uptime (H-5) |
| T-102 | FX rate alert system | 1 | 2 | Phase 1 gate: zero silent failures (H-8) |
| T-103 | Ads sync heartbeat | 1 | 2 | Phase 1 gate: Ads freshness <6h (H-2) |
| T-104 | Single-flight guard ads sync | 1 | 2 | Data integrity — prevents duplicate sync |
| T-105 | DB indexes for PPT | 1 | 2 | Critical path — unblocks PPT <2s (H-1) |
| T-106 | SQL pagination PPT (backend) | 1 | 5 | Critical path — PPT <2s (H-1), core moat |
| T-107 | Pagination PPT (frontend) | 1 | 3 | Critical path — user-visible performance |
| T-108 | Hide/collapse sidebar pages | 1 | 2 | Phase 1 gate: ≤20 sidebar pages (H-4) |
| T-110 | Data Observability Layer | 1 | 3 | Phase 1 gate: DQ ≥82 (H-6) |
| T-111 | Bridge FBA fees | 1 | 2 | Profit accuracy — core moat |
| T-115 | Fix test suite ≥85% | 1 | 3 | Phase 1 gate: tests ≥85% (H-7) |
| T-117 | Phase 1 gate review | 1 | 1 | Phase gate — blocks Phase 2 |
| T-201 | Multi-tenant DB schema | 2 | 5 | Critical path — blocks all multi-user |
| T-202 | User registration (backend) | 2 | 5 | Critical path — blocks onboarding/billing |
| T-203 | Onboarding wizard (frontend) | 2 | 3 | Critical path — blocks beta UX |
| T-204 | Stripe billing | 2 | 5 | Revenue enabler — blocks first payment |
| T-208 | RBAC enforcement | 2 | 3 | Critical path — security for multi-user |
| T-214 | Email delivery | 2 | 2 | Blocks T-202, T-206 — core communication |
| T-215 | Private beta launch | 2 | 2 | PMF milestone — 200 signups target |
| T-311 | Security hardening | 3 | 3 | Public launch prerequisite — P0 security |
| T-314 | Phase 3 gate review | 3 | 1 | Phase gate — blocks Scale phase |
| **Total** | | | **56 SP** | **30% of total SP, 21 tasks** |

## 4.2 Should Have

High value, directly tied to strategic objectives, P1 priority.

| Task | Title | Phase | SP | Rationale |
|------|-------|-------|----|-----------|
| T-109 | Replace python-jose with pyjwt | 1 | 1 | Security debt — quick win |
| T-112 | Archive 72 empty tables | 1 | 1 | DB hygiene — enables easier management |
| T-113 | Materialized profit snapshot | 1 | 3 | Performance foundation — used by T-106, T-301 |
| T-114 | Data Freshness endpoint | 1 | 2 | DQ visibility for users |
| T-116 | Core system runbooks | 1 | 2 | R-05 risk mitigation (single point of knowledge) |
| T-205 | Logistics model v3 | 2 | 8 | Profit accuracy improvement — Question Mark quadrant |
| T-206 | Morning Brief email digest | 2 | 3 | Engagement & retention driver |
| T-207 | Rate limiting | 2 | 2 | Security prerequisite for public APIs |
| T-209 | API versioning | 2 | 2 | API stability for third-party consumers |
| T-210 | Sidebar consolidation | 2 | 3 | UX quality — Phase 2 exit: TTV <10min |
| T-218 | Connection pooling | 2 | 3 | Performance — ADR-005 QueuePool |
| T-219 | Alembic migrations | 2 | 3 | ADR-004 — schema management maturity |
| T-220 | Marketing landing page | 2 | 2 | Blocks beta recruitment funnel |
| T-301 | Weekly P&L PDF report | 3 | 5 | Revenue value-add for paying users |
| T-304 | DACH marketplace testing | 3 | 3 | DE expansion prerequisite |
| T-306 | Public marketing site | 3 | 3 | Growth engine for public launch |
| T-308 | Help center / docs | 3 | 5 | Phase 3 exit: 20+ docs articles |
| T-309 | Onboarding funnel optimization | 3 | 3 | Conversion rate improvement |
| T-310 | Celery workers | 3 | 5 | ADR-008 — architecture evolution |
| T-312 | PII/GDPR audit | 3 | 2 | Compliance requirement for EU market |
| T-313 | E2E testing Playwright | 3 | 5 | Quality gate — regression prevention |
| T-401 | Azure SQL tier upgrade eval | 4 | 2 | Scale readiness assessment |
| T-402 | Horizontal API scaling | 4 | 5 | Scale architecture — multi-replica |
| T-403 | DACH soft launch | 4 | 3 | Market expansion milestone |
| T-404 | Contractor onboarding | 4 | 3 | Team scaling (triggered by $5K MRR) |
| **Total** | | | **79 SP** | **42% of total SP, 25 tasks** |

## 4.3 Could Have

Valuable enhancements that can be deferred without blocking phase gates, P2 priority.

| Task | Title | Phase | SP | Rationale |
|------|-------|-------|----|-----------|
| T-211 | Breadcrumbs + Recently Visited | 2 | 2 | UX polish — nice to have |
| T-212 | Global Search ⌘K | 2 | 3 | Power user feature |
| T-213 | Module Visibility Toggle | 2 | 2 | Customization — deferrable |
| T-216 | PostHog analytics | 2 | 2 | Instrumentation — good but not gate-blocking |
| T-217 | NPS micro-survey | 2 | 1 | Measurement — useful for Phase 2 exit NPS ≥30 |
| T-221 | Error standardization RFC 7807 | 2 | 2 | API quality — can iterate later |
| T-222 | Unified alert triage view | 2 | 5 | Complex build, speculative value |
| T-302 | Profit→Refund drill path | 3 | 5 | Advanced feature, not gate-critical |
| T-303 | Bank feed automation | 3 | 8 | High effort, low confidence, deferrable |
| T-305 | German UI localization | 3 | 3 | Before DACH launch but can be Phase 4 |
| T-307 | Referral program | 3 | 2 | Growth — not needed for initial launch |
| T-405 | JWT RS256 migration | 4 | 3 | ADR-007 explicitly defers this |
| T-409 | Export infrastructure | 4 | 5 | Enhancement layer on T-301 |
| **Total** | | | **43 SP** | **23% of total SP, 13 tasks** |

## 4.4 Won't Have This Cycle

Frozen per portfolio strategy, P3 or explicitly excluded from FY2026/27 scope.

| Task | Title | Phase | SP | Rationale |
|------|-------|-------|----|-----------|
| T-406 | Mobile responsive design | 4 | 5 | Desktop-first strategy; contingent on contractor |
| T-407 | Time-series analytics layer | 4 | 5 | Speculative, no validated demand |
| T-408 | AI-powered margin alerts | 4 | 5 | Depends on T-407; frozen "Intelligence" dog quadrant |
| **Total** | | | **15 SP** | **8% of total SP, 3 tasks** |

> T-406, T-407, T-408 are scheduled in Phase 4 as aspirational. They proceed ONLY if $10K MRR is hit and contractor capacity is available. Otherwise, they are formally deferred to FY2027/28.

## 4.5 MoSCoW Summary Matrix

```
┌─────────────────────────────────────────────────────────────┐
│                    MoSCoW Distribution                       │
├──────────┬───────┬──────┬──────────────────────────────────┤
│ Category │ Tasks │  SP  │ % of Total SP                     │
├──────────┼───────┼──────┼──────────────────────────────────┤
│ Must     │  21   │  56  │ ██████████████████████████ 30%   │
│ Should   │  25   │  79  │ ██████████████████████████████████████ 42% │
│ Could    │  13   │  43  │ █████████████████████ 23%         │
│ Won't    │   3   │  15  │ ████████ 8%                       │
├──────────┼───────┼──────┼──────────────────────────────────┤
│ TOTAL    │  62   │ 186  │                            100%   │
└──────────┴───────┴──────┴──────────────────────────────────┘
```

**Healthy distribution check**: Must (30%) is within the recommended 25-35% range. Combined Must+Should (72%) ensures focus without overcommitment. Could (23%) provides flex capacity. Won't (8%) shows disciplined scope control.

---

# 5. Release Plan with Milestone Mapping

## 5.1 Release Timeline (Gantt-style)

```
2026                                                  2027
Apr      May      Jun      Jul      Aug      Sep      Oct      Nov      Dec      Jan      Feb      Mar
│────────│────────│────────│────────│────────│────────│────────│────────│────────│────────│────────│────────│
│                 │                                                                                       │
│◄── PHASE 1 ──►│◄────────────── PHASE 2 ──────────────────►│◄────────── PHASE 3 ────────────►│◄─ P4 ──►│
│   HARDEN       │              BETA                          │           LAUNCH                │  SCALE  │
│                 │                                            │                                 │         │
│ S1.1 S1.2 S1.3│S1.4                                        │                                 │         │
│  ██   ██   ██ │ █                                           │                                 │         │
│                 │                                            │                                 │         │
│                 │ S2.1 S2.2 S2.3 S2.4 S2.5 S2.6 S2.7 S2.8 S2.9                              │         │
│                 │  ██   ██   ██   ██   ██   ██   ██   ██   ██                                │         │
│                 │                                            │                                 │         │
│                 │                                            │ S3.1 S3.2 S3.3 S3.4 S3.5 S3.6 │S3.7 S3.8│
│                 │                                            │  ██   ██   ██   ██   ██   ██  │ ██   █  │
│                 │                                            │                                 │         │
│                 │                                            │                                 │S4.1 S4.2│S4.3 S4.4
│                 │                                            │                                 │ ██   ██ │ ██   ██
│                 │                                            │                                 │         │
│    MILESTONES:  │                                            │                                 │         │
│                 │                                            │                                 │         │
│            ◆ Phase 1 Gate (May 15)                           │                                 │         │
│                 │                 ◆ Beta Launch (Jul ~17)     │                                 │         │
│                 │                      ◆ First Revenue (Jul)  │                                 │         │
│                 │                                ◆ Phase 2 Gate (Sep 30)                       │         │
│                 │                                       ◆ Public Launch (Oct)                   │         │
│                 │                                            │              ◆ $5K MRR (Dec)    │         │
│                 │                                            │                      ◆ Phase 3 Gate (Jan 31)
│                 │                                            │                                 ◆ $10K MRR (Mar)
│────────│────────│────────│────────│────────│────────│────────│────────│────────│────────│────────│────────│
```

## 5.2 Milestone → Sprint → Task Mapping

### Milestone 1: Phase 1 Gate — May 15, 2026

| Exit Criteria | Metric | Task(s) | Sprint | Status at Gate |
|---------------|--------|---------|--------|----------------|
| H-1: PPT <2s | p95 latency | T-105, T-113, T-106, T-107 | S1.1–S1.3 | Verified |
| H-2: Ads freshness <6h | Staleness monitor | T-103, T-104 | S1.1–S1.2 | Verified |
| H-3: FX alert live | Alert fires on fallback | T-102 | S1.1 | Verified |
| H-4: ≤20 sidebar pages | Page count | T-108 | S1.1 | Verified |
| H-5: 99%+ uptime 7d | UptimeRobot | T-101 | S1.1 | 7-day window |
| H-6: DQ ≥82 | Data quality score | T-110, T-111 | S1.2 | Verified |
| H-7: Tests ≥85% | CI pass rate | T-115 | S1.3 | Verified |
| H-8: Zero silent failures | Heartbeat + alerts | T-101, T-102, T-103, T-110 | S1.1–S1.2 | Verified |

### Milestone 2: Beta Launch — Jul 2026

| Prerequisite | Task(s) | Sprint | Notes |
|-------------|---------|--------|-------|
| User registration working | T-202, T-203 | S2.3–S2.4 | Backend + frontend |
| Billing active | T-204 | S2.4 | Stripe sandbox → live |
| Landing page live | T-220 | S2.2 | Marketing funnel entry |
| Beta recruitment | T-215 | S2.5 | Email outreach + community |
| Email delivery working | T-214 | S2.1 | Transactional emails |
| RBAC enforced | T-208 | S2.2 | Multi-user security |

### Milestone 3: First Revenue — Jul 2026

| Prerequisite | Task(s) | Sprint | Notes |
|-------------|---------|--------|-------|
| Stripe billing live | T-204 | S2.4 | Payment processing |
| At least 1 paying user | T-215 | S2.5 | Beta → paid conversion |
| Value prop validated | T-106, T-107, T-113 | Phase 1 | PPT is the core value |

### Milestone 4: Phase 2 Gate — Sep 30, 2026

| Exit Criteria | Metric | Task(s) | Sprint |
|---------------|--------|---------|--------|
| B-1: 200 signups | Registration count | T-215, T-220 | S2.5+ |
| B-2: 50 WAU | Weekly active users | T-203, T-210 | S2.4+ |
| B-3: 20 paid users | Stripe subscriptions | T-204 | S2.5+ |
| B-4: $2K MRR | Revenue metric | T-204 | S2.7+ |
| B-5: NPS ≥30 | NPS survey | T-217 | S2.7 |
| B-6: TTV <10min | Onboarding timing | T-203, T-210 | S2.4+ |
| B-7: DQ ≥88 | Data quality score | T-110 (ongoing) | Continuous |
| B-8: Support <4h | Response time | T-214, T-116 | Continuous |
| B-9: Churn <8% | Monthly churn | T-206 (retention) | S2.6+ |
| B-10: Zero P0 bugs 14d | Bug tracker | All | S2.10 buffer |

### Milestone 5: Public Launch — Oct 2026

| Prerequisite | Task(s) | Sprint | Notes |
|-------------|---------|--------|-------|
| Security hardened | T-311 | S3.1 | CORS, headers, WAF |
| GDPR compliant | T-312 | S3.1 | PII audit |
| Public marketing site | T-306 | S3.2 | Content + SEO |
| Help center live | T-308 | S3.1–S3.2 | 20+ articles |

### Milestone 6: $5K MRR — Dec 2026

| Driver | Task(s) | Sprint | Notes |
|--------|---------|--------|-------|
| ~85 paying users @ $59 ARPU | T-204, T-309 | Phase 2–3 | Funnel optimization |
| P&L reports (value-add) | T-301 | S3.3 | Premium feature |
| Referral growth | T-307 | S3.6 | Organic acquisition |
| DACH testing | T-304 | S3.2 | Expansion market |

### Milestone 7: Phase 3 Gate — Jan 31, 2027

| Exit Criteria | Metric | Task(s) | Sprint |
|---------------|--------|---------|--------|
| L-1: $5K MRR | Revenue | T-204, T-307 | Ongoing |
| L-2: 100 paid users | Subscriptions | T-309 | S3.4 |
| L-3: Churn <5% | Monthly churn | T-206, T-301 | Ongoing |
| L-4: NPS ≥40 | NPS survey | T-217 | Ongoing |
| L-5: DQ ≥90 | Data quality | T-110 | Ongoing |
| L-6: PPT <1.5s | Performance | T-106, T-310 | S3.3 |
| L-7: Support <2h | Response time | T-308 | S3.1–S3.2 |
| L-8: 20+ docs | Help articles | T-308 | S3.1–S3.2 |
| L-9: 5+ DE users | DACH presence | T-304, T-305 | S3.2, S3.5 |
| L-10: LTV:CAC >4:1 | Unit economics | T-309 | S3.4 |

### Milestone 8: $10K MRR — Mar 2027

| Driver | Task(s) | Sprint | Notes |
|--------|---------|--------|-------|
| ~170 paying users @ $59 ARPU | All Phase 3 | Ongoing | Organic + referral growth |
| Horizontal scaling ready | T-402 | S4.1 | Handle increased load |
| DACH soft launch | T-403 | S4.2 | New market revenue |
| Contractor hire | T-404 | S4.2 | Capacity multiplication |

## 5.3 Phase Gate Verification Matrix

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    PHASE GATE VERIFICATION MATRIX                       │
├──────────┬──────────────────────┬─────────────┬────────────────────────┤
│ Gate     │ Date                 │ Gate Task   │ Required Prior Tasks    │
├──────────┼──────────────────────┼─────────────┼────────────────────────┤
│ Phase 1  │ May 15, 2026         │ T-117       │ T-101 thru T-116       │
│          │                      │             │ (all 16 other P1 tasks)│
├──────────┼──────────────────────┼─────────────┼────────────────────────┤
│ Phase 2  │ Sep 30, 2026         │ (implicit)  │ T-201 thru T-222       │
│          │                      │             │ Metrics: B-1 to B-10   │
├──────────┼──────────────────────┼─────────────┼────────────────────────┤
│ Phase 3  │ Jan 31, 2027         │ T-314       │ T-301 thru T-313       │
│          │                      │             │ Metrics: L-1 to L-10   │
├──────────┼──────────────────────┼─────────────┼────────────────────────┤
│ Phase 4  │ Ongoing (Mar 2027+)  │ —           │ $10K MRR target        │
└──────────┴──────────────────────┴─────────────┴────────────────────────┘
```

### Kill Gate Checkpoints

| Kill Gate | Trigger | Check Date | Action |
|-----------|---------|------------|--------|
| KG-1 | >8 weeks without completing Phase 1 criteria (C-1) | May 26, 2026 | Reassess scope; cut T-113, T-114, T-116 if needed |
| KG-2 | <20 signups 8 weeks after beta launch | Sep 12, 2026 | Pivot strategy; increase GTM allocation to 40% |
| KG-3 | MRR declining 3 consecutive months | Rolling | Halt new feature development; focus on retention |

## 5.4 Revenue Milestones on Timeline

```
Revenue ($)
│
│                                                                    $10K ─── ◆
│                                                               ╱
│                                                          $7K ╱
│                                                         ╱
│                                               $5K ─── ◆
│                                              ╱
│                                        $3.5K╱
│                                 $2K ─── ◆
│                                ╱
│                          $1.2K╱
│                    $500 ◆
│                   ╱
│              $0 ─┤
│  ─────────────────────────────────────────────────────────────────────
│  Apr   May   Jun   Jul   Aug   Sep   Oct   Nov   Dec   Jan   Feb   Mar
│  2026                                                         2027
│
│  Costs:  $281/mo ──────► $359/mo ──────────────► $1,021/mo ──► $5,623/mo
│
│  Break-even points:
│  ◇ Infrastructure break-even: ~5 users (~$303 MRR) — Aug 2026
│  ◇ Full break-even: ~60 users (~$3,300 MRR) — Nov 2026
│  ◇ Cash flow positive: Sep 2026
│  ◇ Pre-revenue trough: -$960 (Jun 2026)
```

## 5.5 Risk-Adjusted Timeline (contingency buffer)

### Nominal vs. Risk-Adjusted Dates

| Milestone | Nominal Date | Risk Buffer | Risk-Adjusted Date | Confidence |
|-----------|-------------|-------------|-------------------|------------|
| Phase 1 Gate | May 15, 2026 | +1 week | May 22, 2026 | 85% |
| Beta Launch | Jul 17, 2026 | +2 weeks | Jul 31, 2026 | 75% |
| First Revenue | Jul 2026 | +2 weeks | Aug 2026 | 70% |
| Phase 2 Gate | Sep 30, 2026 | +2 weeks | Oct 14, 2026 | 70% |
| Public Launch | Oct 2026 | +3 weeks | Nov 2026 | 65% |
| $5K MRR | Dec 2026 | +4 weeks | Jan 2027 | 60% |
| Phase 3 Gate | Jan 31, 2027 | +3 weeks | Feb 21, 2027 | 65% |
| $10K MRR | Mar 2027 | +6 weeks | Apr 2027 | 55% |

### Risk Buffer Justification

| Phase | Buffer | Rationale |
|-------|--------|-----------|
| Phase 1 | 1 week | Well-defined technical tasks; high confidence; zero capacity margin offset by clear scope |
| Phase 2 | 2 weeks | Multi-tenant complexity; Stripe integration unknowns; first external users |
| Phase 3 | 3 weeks | Market-facing uncertainty; DACH expansion; holiday season December velocity dip |
| Phase 4 | 4-6 weeks | Revenue-dependent milestones; contractor onboarding ramp-up; market uncertainty |

### Contingency Actions

| Scenario | Trigger | Response |
|----------|---------|----------|
| Phase 1 delayed 2+ weeks | S1.3 overflow >4 SP | Descope T-114, T-116 to Phase 2; fast-track T-106 |
| Beta launch delayed | T-201 or T-202 takes 7+ SP | Simplify multi-tenant to single-tenant+metadata; use magic links instead of full registration |
| Revenue ramp slower than plan | <$500 MRR by Aug | Double GTM time; pause Could items; intensify beta outreach |
| Burnout risk materializes | >55h/week for 3+ weeks | Mandatory 1-week break; defer all Could items; hire virtual assistant for ops |

---

# 6. Studio Producer Strategic Alignment Validation

## 6.1 Portfolio Alignment Check

| Portfolio Quadrant | Target Allocation | Sprint Plan Allocation | Status |
|-------------------|-------------------|----------------------|--------|
| ⭐ STAR: Profit Engine | 40% eng time | T-105, T-106, T-107, T-111, T-113 = 15 SP Phase 1 (44%) | ✅ ALIGNED |
| 🐄 CASH COW: COGS/ERP + Dashboard | 20% eng time | T-108, T-110, T-114 = 7 SP Phase 1 (21%) | ✅ ALIGNED |
| ❓ QUESTION: Ads, Logistics, FBA | 30% eng time | T-103, T-104, T-205 = 12 SP (~15% overall) | ⚠️ UNDER — by design, validation-first |
| 🐕 DOG: Frozen modules | 0% eng time | T-407, T-408 = Won't | ✅ FROZEN |

**Verdict**: Portfolio allocation is well-aligned. Question marks are intentionally under-allocated pending market validation — this is correct for the current stage. The Profit Engine STAR receives the heaviest investment in Phase 1, exactly as specified.

## 6.2 Competitive Window Compliance

| Metric | Target | Sprint Plan | Status |
|--------|--------|-------------|--------|
| Time to beta with real-time CM1 | <4 months from now | Jul 2026 (3.5 months from Phase 1 start) | ✅ ON TRACK |
| Time to paid product | <5 months from now | Jul 2026 (4 months from Phase 1 start) | ✅ ON TRACK |
| Time to 50+ paying users | <9 months | Dec 2026 (9 months from Phase 1) | ⚠️ TIGHT |
| 12-18 month competitive window | Mar 2027 worst case | $10K MRR target Mar 2027 | ✅ WITHIN WINDOW |

**Verdict**: The sprint plan delivers a paid product within the competitive window. The 50-user milestone is tight but achievable with focused GTM from Phase 2 onward. The critical differentiator (real-time CM1 with ERP COGS + Ads + logistics) is fully built by Phase 1 Gate.

## 6.3 Burnout Risk Assessment

| Phase | Eng Hours/Week | Non-Eng Hours/Week | Total | Risk Level |
|-------|---------------|-------------------|-------|------------|
| Phase 1 | 22 | 28 (ops 7.5, GTM 2.5, other 18) | 50 | 🟡 MODERATE — zero SP margin |
| Phase 2 | 16 | 34 (ops 7.5, GTM 12.5, strategic 2.5, other 11.5) | 50 | 🟢 LOW — 7 SP buffer, lower velocity |
| Phase 3 | 16 | 34 (ops 7.5, GTM 17.5, strategic 5, other 4) | 50 | 🟢 LOW — 3 SP buffer |
| Phase 4 | 22 | 28 (with contractor absorbing frontend) | 50 | 🟢 LOW — contractor leverage |

**Burnout Mitigation Measures in Plan**:

1. **Phase 1 S1.4** is deliberately underloaded (5 SP vs 9 capacity) — acts as recovery sprint
2. **Phase 2 S2.10** is a pure buffer sprint — no new tasks
3. **Phase 3 S3.6** is 6 SP (below target) — holiday season accommodation
4. **No sprint exceeds 12 SP** (and only S1.3 reaches that with staggered execution)
5. **Won't items** (15 SP) properly frozen — no scope creep temptation
6. **50h/week cap** is respected — no phase asks for more than 22h deep engineering

**Verdict**: ✅ Burnout risk is managed. Phase 1 is the highest-risk period (zero margin) but is only 6.5 weeks. Buffer sprints provide recovery windows.

## 6.4 Budget Alignment

| Phase | Monthly Cost | Sprint Plan Duration | Total Phase Cost | Revenue Offset |
|-------|-------------|---------------------|-----------------|----------------|
| Phase 1 (Apr–May) | $281/mo | 1.5 months | $422 | $0 |
| Phase 2 (May–Sep) | $359/mo | 4.5 months | $1,616 | ~$3,700 (Jul–Sep) |
| Phase 3 (Oct–Jan) | $1,021/mo | 4 months | $4,084 | ~$14,000 (Oct–Jan) |
| Phase 4 (Feb–Mar) | $5,623/mo | 2+ months | $11,246 | ~$17,000 (Feb–Mar) |
| **Total** | | **12 months** | **$17,368** | **~$34,700** |

**Pre-revenue trough**: -$960 projected for Jun 2026 — manageable with personal runway.

**Verdict**: ✅ Budget is aligned. Revenue exceeds costs from Sep 2026 onward. Phase 4 cost increase ($5,623) is triggered by $5K MRR, ensuring sustainability. The sprint plan's Phase 4 contractor hire (T-404) is correctly gated by revenue.

## 6.5 Kill Gate Integration

| Kill Gate | Sprint Plan Integration | Early Warning Sprint |
|-----------|------------------------|---------------------|
| KG-1: >8 weeks without Phase 1 criteria | Phase 1 has 6.5-week schedule with 1-week buffer. If S1.3 overflows significantly, T-114 and T-116 are first cuts. | S1.3 (Week 5) |
| KG-2: <20 signups after 8 weeks | Beta launch in S2.5 (Week 12). 8-week check = S2.9 (Week 20). T-217 NPS survey provides early signal in S2.7. | S2.7 (Week 16) |
| KG-3: MRR declining 3 months | Revenue tracking begins Jul 2026. First possible trigger: Oct 2026. T-309 funnel optimization in S3.4 is the response lever. | S3.2 (first revenue check) |

**Kill gate actions in sprint plan**:
- **KG-1 response**: S1.4 has 4 SP of slack. T-114 (2 SP) and T-116 (2 SP) can be deferred to Phase 2 without affecting Phase 1 gate criteria.
- **KG-2 response**: Phase 2 Could items (T-211, T-212, T-213, T-221, T-222 = 14 SP) can be fully cut, redirecting equivalent time to GTM activities.
- **KG-3 response**: Phase 3 Could items (T-302, T-303, T-305, T-307 = 18 SP) can be frozen, converting 4+ sprints of engineering to retention/growth work.

**Verdict**: ✅ Kill gates are integrated with specific sprint-level escape plans and early warning checkpoints.

## 6.6 Strategic Verdict

### Overall Assessment: ✅ STRATEGICALLY ALIGNED

| Dimension | Score | Notes |
|-----------|-------|-------|
| Portfolio fit | 9/10 | STAR investment matches 40% target; Dogs properly frozen |
| Competitive timing | 8/10 | Beta in 3.5 months; within 12-18 month window |
| Burnout protection | 8/10 | Buffer sprints included; Phase 1 zero-margin is the risk |
| Revenue path | 7/10 | Critical path to first revenue is 15.5 weeks; $10K MRR is achievable but uncertain |
| Technical soundness | 9/10 | Dependency chains respected; critical path identified; ADRs followed |
| Kill gate readiness | 9/10 | Clear cut-lines at each gate; early warning sprints identified |
| Scope discipline | 9/10 | Only 3 tasks in Won't; Could items are genuinely deferrable |
| **Overall** | **8.4/10** | |

### Top 3 Strategic Risks in Sprint Plan

1. **Phase 1 zero-margin (Risk: Medium)**: 34 SP in ~34 SP capacity. Any underestimation causes gate delay. **Mitigation**: T-114/T-116 are cut candidates; S1.3 can stagger T-106/T-107.

2. **Beta-to-Revenue conversion uncertainty (Risk: Medium-High)**: Sprint plan assumes first revenue in Jul 2026. If PMF signals are weak, the entire Phase 2-3 timeline shifts. **Mitigation**: T-217 NPS + T-216 PostHog provide data by S2.7; KG-2 kill gate is the backstop.

3. **Solo developer single point of failure (Risk: High)**: Illness, burnout, or personal emergency halts all progress. **Mitigation**: T-116 runbooks reduce knowledge concentration; T-404 contractor gives redundancy by Phase 4; 50h/week cap is non-negotiable.

### Strategic Recommendations

1. **Pre-wire Phase 2 in Phase 1**: During Phase 1, spend non-engineering hours designing T-201 multi-tenant schema on paper. This reduces Phase 2 critical path risk.
2. **Start T-220 landing page early**: T-220 has no dependencies. Consider starting it in Phase 1 non-engineering time (GTM allocation) to accelerate beta recruitment.
3. **Instrument from Day 1**: Ensure UptimeRobot (T-101) and Data Observability (T-110) are the absolute first tasks — they provide visibility for all subsequent decisions.
4. **Revenue validation sprint**: S2.5 (Beta Launch) should include explicit conversion experiments. Don't just launch — measure willingness to pay before building more.

---

*Document generated 2026-03-13 by Sprint Prioritizer Agent. Next review: Phase 1 Gate (May 15, 2026).*
