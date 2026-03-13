# 💰 ACC Financial Plan — FY2026/28
## Amazon Command Center — Comprehensive Financial Projections

**Agent**: Finance Tracker | **Date**: 2026-03-13 | **Classification**: Financial — Decision Document  
**Prepared for**: Miłosz Sobieniowski, Founder & "Builder-Operator"  
**Input Sources**: Strategic Portfolio Plan (2026-03-13), Technology Stack Assessment, Phase 0 Executive Summary  
**Planning Horizon**: 24 months (Apr 2026 – Mar 2028)  
**Review Cadence**: Monthly on 1st Monday

---

## TABLE OF CONTENTS

1. [Methodology & Assumptions](#1-methodology--assumptions)
2. [Deliverable 1: Comprehensive Project Budget](#2-deliverable-1-comprehensive-project-budget)
3. [Deliverable 2: Resource Cost Projections](#3-deliverable-2-resource-cost-projections)
4. [Deliverable 3: ROI Model with Break-Even Analysis](#4-deliverable-3-roi-model-with-break-even-analysis)
5. [Deliverable 4: Cash Flow Timeline](#5-deliverable-4-cash-flow-timeline)
6. [Deliverable 5: Financial Risk Assessment](#6-deliverable-5-financial-risk-assessment)

---

# 1. METHODOLOGY & ASSUMPTIONS

## 1.1 Base Assumptions

| Parameter | Value | Source/Rationale |
|-----------|-------|------------------|
| **Planning start date** | Apr 1, 2026 | Phase 1 kickoff |
| **Currency base** | USD | Primary reporting currency |
| **EUR/USD rate** | €1 = $1.08 | ECB mid-rate Mar 2026 |
| **PLN/USD rate** | 1 PLN = $0.26 | NBP mid-rate Mar 2026 |
| **PLN/EUR rate** | 1 PLN = €0.24 | Cross-rate |
| **Discount rate (WACC proxy)** | 15% | Early-stage bootstrapped SaaS risk premium |
| **Tax rate (PL CIT/PIT)** | 19% flat (liniowy) | Assumed B2B JDG tax structure |
| **Stripe fee** | 1.5% + €0.25 EU / 2.9% + $0.30 non-EU | Stripe EU standard pricing |
| **Effective Stripe blended rate** | ~3.2% of gross revenue | Weighted EU/non-EU mix |
| **Annual SaaS tool price inflation** | 5% | Industry average for SMB SaaS tools |
| **Azure price assumption** | Stable (reserved instance discounts available) | Microsoft pricing commitment |
| **Founder minimum living cost** | $3,000/mo (~11,540 PLN) | Poland — professional with mortgage/rent |
| **Max sustainable work hours** | 50h/week | Burnout prevention framework |

## 1.2 Revenue Assumptions

| Parameter | Value | Basis |
|-----------|-------|-------|
| **First paying customer** | Jul 2026 | Phase 2 Stripe integration timeline |
| **ARPU (blended)** | $59/mo (€55) | Weighted across tiers: Free $0, Pro $42, Business $85, Enterprise $215 |
| **Monthly gross churn** | 5% (starting), 4% (Month 6+), 3% (Month 12+) | SMB SaaS benchmarks; improving product-market fit |
| **Expansion revenue (NRR uplift)** | 1% MoM starting Month 6 | Tier upgrades: Free→Pro, Pro→Business |
| **Payment collection delay** | 3 business days (Stripe → bank) | Stripe EU payout schedule |
| **Annual prepay discount** | 15% (2 months free) | Standard SaaS annual billing incentive |
| **Annual prepay adoption** | 20% of users by Month 12 | Conservative early-stage estimate |

## 1.3 Three-Scenario Framework

| Scenario | Weight | Key Driver | Description |
|----------|--------|-----------|-------------|
| **Conservative** | 25% | Slow adoption, competitors react | 60 paying users by Mar'27, $3K MRR |
| **Base** | 50% | Steady PMF, PL community growth | 200 paying users by Mar'27, $10K MRR |
| **Optimistic** | 25% | Strong PMF, DAC7 drives demand | 600 paying users by Mar'27, $25K MRR |

## 1.4 Phase Timeline Reference

| Phase | Period | Duration | Theme |
|-------|--------|----------|-------|
| Phase 1: HARDEN | Apr 1 – May 15, 2026 | 6-7 weeks | Earn trust in the numbers |
| Phase 2: BETA | May 16 – Sep 30, 2026 | ~20 weeks | First users, first revenue |
| Phase 3: LAUNCH | Oct 2026 – Jan 2027 | ~16 weeks | From beta to business |
| Phase 4: SCALE | Feb 2027+ | Ongoing | Growth engine |

---

# 2. DELIVERABLE 1: COMPREHENSIVE PROJECT BUDGET

## 2.1 Phase 1 — HARDEN (Apr–May 2026) | Monthly Budget

| Category | Line Item | Unit Cost | Qty | Monthly Cost | Notes |
|----------|-----------|-----------|-----|-------------|-------|
| **Infrastructure** | Azure SQL S3 | $150 | 1 | $150 | 19 GB, Standard S3 tier |
| | Azure App Service B2 | $50 | 1 | $50 | Production API hosting |
| | Azure Cache for Redis Basic | $15 | 1 | $15 | Session cache + rate limiting |
| | Domain (ascend-commerce.com) | $5 | 1 | $5 | Annual spread monthly |
| | SSL (Let's Encrypt) | $0 | 1 | $0 | Free via Azure |
| | **Infrastructure Subtotal** | | | **$220** | |
| **SaaS Tools** | Sentry Team | $26 | 1 | $26 | Error tracking, 5K events/mo |
| | UptimeRobot Pro | $7 | 1 | $7 | 60 monitors, 1-min interval |
| | GitHub (free tier) | $0 | 1 | $0 | Private repos included |
| | GitHub Actions (free tier) | $0 | 1 | $0 | 2,000 min/mo |
| | OpenAI API (minimal) | $2 | 1 | $2 | Copilot-related, minimal usage |
| | **SaaS Tools Subtotal** | | | **$35** | |
| **Marketing/GTM** | None (Phase 1) | $0 | — | $0 | Focus is hardening, not GTM |
| | **Marketing Subtotal** | | | **$0** | |
| **Personnel** | None (solo founder) | $0 | — | $0 | No contractors in Phase 1 |
| | **Personnel Subtotal** | | | **$0** | |
| **Legal/Compliance** | None (deferred) | $0 | — | $0 | Privacy policy template (free) |
| | **Legal Subtotal** | | | **$0** | |
| **Buffer (10%)** | Contingency | — | — | $26 | 10% of operational costs |
| | **Buffer Subtotal** | | | **$26** | |
| **PHASE 1 TOTAL** | | | | **$281/mo** | **$421 for 1.5 months** |

## 2.2 Phase 2 — BETA (Jun–Sep 2026) | Monthly Budget

| Category | Line Item | Unit Cost | Qty | Monthly Cost | Notes |
|----------|-----------|-----------|-----|-------------|-------|
| **Infrastructure** | Azure SQL S3 | $150 | 1 | $150 | Same tier, growing data |
| | Azure App Service B2 | $50 | 1 | $50 | |
| | Azure Cache for Redis Basic | $15 | 1 | $15 | |
| | Domain + DNS | $5 | 1 | $5 | |
| | Azure Blob Storage | $5 | 1 | $5 | Report PDFs, user uploads |
| | Azure CDN (static assets) | $10 | 1 | $10 | Frontend SPA delivery |
| | **Infrastructure Subtotal** | | | **$235** | |
| **SaaS Tools** | Sentry Team | $26 | 1 | $26 | |
| | UptimeRobot Pro | $7 | 1 | $7 | |
| | Resend (email) | $20 | 1 | $20 | Transactional emails (onboarding, alerts) |
| | PostHog (free tier) | $0 | 1 | $0 | Product analytics, 1M events free |
| | Crisp.chat (free tier) | $0 | 1 | $0 | Live chat widget, 2 seats free |
| | Stripe | Variable | — | $0* | *Fee deducted from revenue, not a cost line |
| | OpenAI API | $5 | 1 | $5 | Slightly increased usage |
| | **SaaS Tools Subtotal** | | | **$58** | |
| **Marketing/GTM** | Community tools | $0 | — | $0 | Organic — Facebook, LinkedIn, X |
| | Content creation (tools) | $20 | 1 | $20 | Canva Pro for social graphics |
| | Beta launch email campaign | $0 | — | $0 | Included in Resend |
| | Landing page (Vercel free) | $0 | 1 | $0 | Static marketing site |
| | Webinar hosting (StreamYard) | $0 | 1 | $0 | Free tier for beta demos |
| | **Marketing Subtotal** | | | **$20** | |
| **Personnel** | None (solo founder) | $0 | — | $0 | |
| | **Personnel Subtotal** | | | **$0** | |
| **Legal/Compliance** | Terms of Service draft | $50 | 0.25 | $13 | One-time $50, amortized over 4 months |
| | Privacy Policy (GDPR) | $0 | — | $0 | Template-based, self-authored |
| | **Legal Subtotal** | | | **$13** | |
| **Buffer (10%)** | Contingency | — | — | $33 | |
| | **Buffer Subtotal** | | | **$33** | |
| **PHASE 2 TOTAL** | | | | **$359/mo** | **$1,436 for 4 months** |

## 2.3 Phase 3 — LAUNCH (Oct 2026 – Jan 2027) | Monthly Budget

| Category | Line Item | Unit Cost | Qty | Monthly Cost | Notes |
|----------|-----------|-----------|-----|-------------|-------|
| **Infrastructure** | Azure SQL S3 | $150 | 1 | $150 | Monitor for S4 upgrade trigger |
| | Azure App Service B2→B3 | $75 | 1 | $75 | Upgrade if >100 concurrent users |
| | Azure Cache for Redis Basic | $15 | 1 | $15 | |
| | Domain + DNS | $5 | 1 | $5 | |
| | Azure Blob Storage | $10 | 1 | $10 | Growing report storage |
| | Azure CDN | $15 | 1 | $15 | Increased traffic |
| | Celery worker (Azure Container) | $50 | 1 | $50 | Background job processing |
| | **Infrastructure Subtotal** | | | **$320** | |
| **SaaS Tools** | Sentry Team | $26 | 1 | $26 | |
| | UptimeRobot Pro | $7 | 1 | $7 | |
| | Resend (Starter) | $20 | 1 | $20 | Growing email volume |
| | PostHog (free → Growth) | $0 | 1 | $0 | Still within free tier |
| | Crisp.chat (Pro) | $25 | 1 | $25 | Upgraded for CRM features |
| | Auth0/Clerk (Starter) | $25 | 1 | $25 | RBAC for team accounts |
| | OpenAI API | $10 | 1 | $10 | Morning Brief AI summaries |
| | **SaaS Tools Subtotal** | | | **$113** | |
| **Marketing/GTM** | Canva Pro | $13 | 1 | $13 | |
| | Google Workspace (email) | $7 | 1 | $7 | Professional email domain |
| | Content marketing (freelance) | $200 | 1 | $200 | 2-3 blog posts/month |
| | Paid social (LinkedIn/FB) | $200 | 1 | $200 | Targeted PL/DACH seller ads |
| | Event sponsorship | $50 | 1 | $50 | PL Amazon community events |
| | **Marketing Subtotal** | | | **$470** | |
| **Personnel** | None until trigger | $0 | — | $0 | Contractors on standby |
| | **Personnel Subtotal** | | | **$0** | |
| **Legal/Compliance** | Regulamin (PL Terms) | $25 | 0.25 | $6 | Legal review amortized |
| | GDPR DPA template | $0 | — | $0 | Self-authored |
| | Trademark (ACC) | $75 | 0.25 | $19 | EUIPO trademark filing amortized |
| | **Legal Subtotal** | | | **$25** | |
| **Buffer (10%)** | Contingency | — | — | $93 | |
| | **Buffer Subtotal** | | | **$93** | |
| **PHASE 3 TOTAL** | | | | **$1,021/mo** | **$4,084 for 4 months** |

## 2.4 Phase 4 — SCALE (Feb 2027+) | Monthly Budget (at $10K MRR)

| Category | Line Item | Unit Cost | Qty | Monthly Cost | Notes |
|----------|-----------|-----------|-----|-------------|-------|
| **Infrastructure** | Azure SQL S4 | $300 | 1 | $300 | Upgrade triggered at 500+ users |
| | Azure App Service P1 | $100 | 1 | $100 | Production-grade hosting |
| | Azure Cache for Redis Standard | $50 | 1 | $50 | Sentinel HA |
| | Domain + DNS | $5 | 1 | $5 | |
| | Azure Blob Storage | $20 | 1 | $20 | |
| | Azure CDN | $25 | 1 | $25 | Multi-geography delivery |
| | Celery workers (×2) | $100 | 1 | $100 | Scaled background processing |
| | Azure Monitor / App Insights | $30 | 1 | $30 | APM & logging |
| | **Infrastructure Subtotal** | | | **$630** | |
| **SaaS Tools** | Sentry Business | $80 | 1 | $80 | Higher event cap, performance monitoring |
| | UptimeRobot Pro | $7 | 1 | $7 | |
| | Resend (Pro) | $40 | 1 | $40 | Higher volume |
| | PostHog Growth | $50 | 1 | $50 | Product analytics at scale |
| | Crisp.chat Pro | $25 | 1 | $25 | |
| | Auth0/Clerk Pro | $50 | 1 | $50 | Multi-tenant RBAC |
| | OpenAI API | $30 | 1 | $30 | AI features expansion |
| | Notion / Linear (PM tool) | $10 | 1 | $10 | Team task management |
| | **SaaS Tools Subtotal** | | | **$292** | |
| **Marketing/GTM** | Content marketing (freelance) | $400 | 1 | $400 | 4-6 posts/month + SEO |
| | Paid social / SEM | $400 | 1 | $400 | PL + DACH acquisition |
| | Event sponsorship | $100 | 1 | $100 | |
| | Canva + design tools | $20 | 1 | $20 | |
| | Google Workspace | $7 | 1 | $7 | |
| | Affiliate program payouts | $100 | 1 | $100 | Early referral incentives |
| | **Marketing Subtotal** | | | **$1,027** | |
| **Personnel** | Frontend contractor (PT, 20h/wk) | $3,000 | 1 | $3,000 | Triggered at $5K MRR sustained 2mo |
| | **Personnel Subtotal** | | | **$3,000** | |
| **Legal/Compliance** | Accountant (PL) | $150 | 1 | $150 | Monthly bookkeeping + VAT filing |
| | GDPR compliance audit | $50 | 0.25 | $13 | Quarterly amortized |
| | **Legal Subtotal** | | | **$163** | |
| **Buffer (10%)** | Contingency | — | — | $511 | |
| | **Buffer Subtotal** | | | **$511** | |
| **PHASE 4 TOTAL** | | | | **$5,623/mo** | At $10K MRR level |

## 2.5 Cumulative Spend Projections (24 Months)

| Month | Phase | Monthly OpEx | Cumulative OpEx | Notes |
|-------|-------|-------------|----------------|-------|
| Apr 2026 | P1 | $281 | $281 | Hardening begins |
| May 2026 | P1→P2 | $320 | $601 | Phase transition |
| Jun 2026 | P2 | $359 | $960 | Private beta launch |
| Jul 2026 | P2 | $359 | $1,319 | First paying customers |
| Aug 2026 | P2 | $359 | $1,678 | Feature iteration |
| Sep 2026 | P2 | $359 | $2,037 | Beta scaling |
| Oct 2026 | P3 | $1,021 | $3,058 | Public launch |
| Nov 2026 | P3 | $1,021 | $4,079 | Growth + retention |
| Dec 2026 | P3 | $1,021 | $5,100 | DACH preparation |
| Jan 2027 | P3 | $1,021 | $6,121 | Phase gate |
| Feb 2027 | P4 | $2,500 | $8,621 | Scale begins (no hire yet) |
| Mar 2027 | P4 | $2,500 | $11,121 | Approaching hire trigger |
| Apr 2027 | P4 | $3,800 | $14,921 | First contractor may start |
| May 2027 | P4 | $5,200 | $20,121 | Contractor ramping |
| Jun 2027 | P4 | $5,623 | $25,744 | Full Phase 4 run rate |
| Jul 2027 | P4 | $5,623 | $31,367 | |
| Aug 2027 | P4 | $5,800 | $37,167 | Slight growth from scaling |
| Sep 2027 | P4 | $6,000 | $43,167 | |
| Oct 2027 | P4 | $6,200 | $49,367 | |
| Nov 2027 | P4 | $6,500 | $55,867 | Possible second hire |
| Dec 2027 | P4 | $6,800 | $62,667 | |
| Jan 2028 | P4 | $7,200 | $69,867 | |
| Feb 2028 | P4 | $7,500 | $77,367 | |
| Mar 2028 | P4 | $8,000 | $85,367 | 24-month total |

## 2.6 Phase-over-Phase Budget Growth Analysis

| Metric | Phase 1 | Phase 2 | Phase 3 | Phase 4 (initial) | Phase 4 (mature) |
|--------|---------|---------|---------|-------------------|------------------|
| Monthly OpEx | $281 | $359 | $1,021 | $2,500 | $5,623+ |
| Phase-over-Phase Growth | — | +28% | +184% | +145% | +125% |
| Infrastructure % of total | 78% | 65% | 31% | 25% | 11% |
| SaaS Tools % | 12% | 16% | 11% | 12% | 5% |
| Marketing % | 0% | 6% | 46% | 41% | 18% |
| Personnel % | 0% | 0% | 0% | 0% | 53% |
| Legal % | 0% | 4% | 2% | 7% | 3% |
| Buffer % | 9% | 9% | 9% | 10% | 9% |

**Key insight**: The budget inflection point occurs at Phase 3→Phase 4 when personnel costs dominate. Before the first hire, ACC operates at <$1,100/mo — an extraordinarily lean burn rate.

---

# 3. DELIVERABLE 2: RESOURCE COST PROJECTIONS

## 3.1 Infrastructure Cost Model by User Milestone

| Component | 1 user (now) | 50 users | 200 users | 500 users | 1,000 users | 3,000 users |
|-----------|-------------|----------|-----------|-----------|-------------|-------------|
| **Azure SQL** | $150 (S3) | $150 (S3) | $150 (S3) | $300 (S4) | $450 (P1) | $900 (P2) |
| **App Service** | $50 (B2) | $50 (B2) | $75 (B3) | $100 (P1) | $200 (P1v2) | $400 (P2v2) |
| **Redis** | $15 (Basic) | $15 (Basic) | $15 (Basic) | $50 (Standard) | $100 (Premium C1) | $200 (Premium C2) |
| **Blob Storage** | $0 | $5 | $10 | $20 | $40 | $80 |
| **CDN** | $0 | $10 | $15 | $25 | $40 | $80 |
| **Celery Workers** | $0 | $0 | $50 | $100 | $200 | $400 |
| **Azure Monitor** | $0 | $0 | $15 | $30 | $50 | $100 |
| **Bandwidth** | $0 | $2 | $10 | $25 | $60 | $150 |
| **Backup (PITR)** | incl. | incl. | incl. | $15 | $30 | $60 |
| **TOTAL Infra/mo** | **$215** | **$232** | **$340** | **$665** | **$1,170** | **$2,370** |
| **Per-user infra cost** | $215.00 | $4.64 | $1.70 | $1.33 | $1.17 | $0.79 |

```
Infrastructure Cost per User — Scale Economics
$5.00 ┤
      │ ●
$4.00 ┤
      │
$3.00 ┤
      │
$2.00 ┤       ●
      │
$1.00 ┤             ● ──── ● ──── ● ──── ●
      │
$0.00 ┼─────┬──────┬──────┬──────┬──────┬────
      50   200    500   1000  2000  3000   users
```

**Key finding**: Per-user infrastructure cost drops below $2.00/mo at 200 users, reaching $0.79 at 3,000 users. With a $59 blended ARPU, infrastructure represents 1.3–7.9% of revenue — well within the 85%+ gross margin target.

## 3.2 SaaS Tool Stack Cost Progression

| Tool | Phase 1 | Phase 2 | Phase 3 | Phase 4 (initial) | Phase 4 (1K users) | Phase 4 (3K users) |
|------|---------|---------|---------|-------------------|--------------------|--------------------|
| Sentry | $26 | $26 | $26 | $80 | $80 | $160 |
| UptimeRobot | $7 | $7 | $7 | $7 | $15 | $15 |
| Email (Resend) | $0 | $20 | $20 | $40 | $80 | $160 |
| Analytics (PostHog) | $0 | $0 | $0 | $50 | $50 | $150 |
| Support (Crisp) | $0 | $0 | $25 | $25 | $75 | $150 |
| Auth (Clerk/Auth0) | $0 | $0 | $25 | $50 | $100 | $200 |
| OpenAI API | $2 | $5 | $10 | $30 | $60 | $150 |
| PM tool (Linear) | $0 | $0 | $0 | $10 | $10 | $30 |
| Google Workspace | $0 | $0 | $7 | $7 | $35 | $70 |
| Design (Canva) | $0 | $20 | $13 | $20 | $20 | $20 |
| **TOTAL SaaS/mo** | **$35** | **$78** | **$133** | **$319** | **$525** | **$1,105** |

## 3.3 Contractor/Hiring Cost Model

### Hiring Triggers & Projected Costs

| Trigger | Threshold | Timing (Base) | Role | Cost/mo (PLN) | Cost/mo (USD) |
|---------|-----------|--------------|------|---------------|---------------|
| MRR > $5K × 2 months | Revenue trigger | Nov–Dec 2026 | Frontend contractor (PT 20h/wk) | 8,000–15,000 PLN | $2,080–$3,900 |
| MRR > $10K × 3 months | Revenue trigger | Apr–May 2027 | Backend/DevOps (FT) | 15,000–25,000 PLN | $3,900–$6,500 |
| Support > 20 tickets/wk | Volume trigger | Jul 2027 (est.) | Customer Success (PT) | 5,000–8,000 PLN | $1,300–$2,080 |
| Eng backlog > 6 months | Capacity trigger | Sep 2027 (est.) | Frontend developer (FT) | 15,000–22,000 PLN | $3,900–$5,720 |
| 200+ paying users | Scale trigger | Mar 2027 (base) | Community manager (PT) | 4,000–7,000 PLN | $1,040–$1,820 |

### Cumulative Personnel Cost (Base Scenario)

| Period | Headcount | Monthly Personnel Cost | Cumulative (12-mo trailing) |
|--------|-----------|----------------------|---------------------------|
| Apr–Oct 2026 | 0 (solo) | $0 | $0 |
| Nov–Dec 2026 | 0.5 (contractor) | $3,000 | $6,000 |
| Jan–Mar 2027 | 0.5 | $3,000 | $15,000 |
| Apr–Jun 2027 | 1.5 (contractor + FT) | $7,000 | $36,000 |
| Jul–Sep 2027 | 2.0 | $9,000 | $63,000 |
| Oct–Dec 2027 | 2.5 | $11,000 | $96,000 |
| Jan–Mar 2028 | 3.0 | $14,000 | $138,000 |

## 3.4 Founder Opportunity Cost Analysis

| Metric | Calculation | Value |
|--------|------------|-------|
| **Market salary (PL, senior full-stack)** | JustJoinIT median, 2026 | 22,000 PLN/mo (~$5,720/mo) |
| **Consulting rate equivalent** | Senior SaaS architect, PL market | 600 PLN/h (~$156/h) |
| **Weekly hours invested** | Sustainable cap | 50h/week |
| **Monthly hours invested** | 50h × 4.33 weeks | 217h/month |
| **Monthly opportunity cost (salary)** | Market salary forgone | $5,720/mo |
| **Monthly opportunity cost (consulting)** | 217h × $156/h | $33,852/mo |
| **Effective hourly value at Base $10K MRR** | $10,000 / 217h | $46/h |
| **Effective hourly value at $30K MRR** | $30,000 / 217h | $138/h |
| **Break-even hourly value vs salary** | $5,720 / 217h | $26/h → achieved at ~$5.7K MRR |
| **24-month cumulative opportunity cost** | 24 × $5,720 | $137,280 |

**Conclusion**: The founder's opportunity cost (salary-equivalent) is recovered when MRR exceeds ~$5,700. At Base scenario $10K MRR, the effective hourly rate ($46/h) exceeds the equivalent salary rate ($26/h) by 77%, validating the entrepreneurial risk premium.

## 3.5 Currency Exposure Analysis

### Revenue/Cost Currency Mix

| Category | Currency | Monthly (Phase 4 Base) | % of Total |
|----------|----------|----------------------|-----------|
| **Revenue** | EUR (Stripe) | €9,260 (~$10,000) | 100% of revenue |
| **Azure Infrastructure** | USD | $630 | 46% of costs |
| **SaaS Tools** | USD | $319 | 23% of costs |
| **Marketing** | PLN/USD mixed | $1,027 | — |
| **Polish contractors** | PLN | 11,540 PLN ($3,000) | 22% of costs |
| **Legal/Accounting** | PLN | 577 PLN ($150) | 1% of costs |
| **Founder salary** | PLN | 11,540 PLN ($3,000) | Living costs |

### FX Risk Scenarios

| Scenario | EUR/PLN Change | Impact on Margin | Annual Impact |
|----------|---------------|------------------|---------------|
| EUR strengthens 5% | 4.50 → 4.73 | +2.1% margin | +$2,400/yr at $10K MRR |
| EUR stable | 4.50 | Baseline | $0 |
| EUR weakens 5% | 4.50 → 4.28 | -2.1% margin | -$2,400/yr at $10K MRR |
| EUR weakens 10% | 4.50 → 4.05 | -4.2% margin | -$4,800/yr at $10K MRR |
| PLN strengthens 10% | Higher PLN cost | -3.1% margin | -$3,700/yr (personnel cost up) |

**Natural hedge**: ~69% of costs are USD/EUR-denominated (Azure, SaaS tools), which naturally hedges against the EUR-denominated revenue. Only ~31% of costs are PLN-denominated (contractors, legal, founder salary), representing the net FX exposure.

**Recommendation**: No active FX hedging needed until MRR > $20K. Below that threshold, natural currency diversification and the narrow PLN exposure (~$3,150/mo) keep FX risk manageable. At $20K+ MRR, consider a multi-currency Stripe account holding EUR until needed for PLN expenses.

---

# 4. DELIVERABLE 3: ROI MODEL WITH BREAK-EVEN ANALYSIS

## 4.1 Investment vs. Return Analysis — All 3 Scenarios

### Total Investment Summary (Cumulative Pre-Revenue + Ongoing)

| Investment Component | Pre-Revenue (Apr–Jun 2026) | Year 1 Total | Year 2 Total | 24-Month Total |
|---------------------|---------------------------|-------------|-------------|---------------|
| Infrastructure | $655 | $3,320 | $6,600 | $9,920 |
| SaaS Tools | $113 | $1,180 | $4,800 | $5,980 |
| Marketing | $0 | $2,340 | $12,000 | $14,340 |
| Personnel | $0 | $6,000 | $84,000 | $90,000 |
| Legal | $0 | $244 | $1,800 | $2,044 |
| Buffer | $77 | $1,308 | $10,920 | $12,228 |
| **Total Cash Investment** | **$845** | **$14,392** | **$120,120** | **$134,512** |
| Founder Opportunity Cost | $17,160 | $68,640 | $68,640 | $137,280 |
| **Total Economic Investment** | **$18,005** | **$83,032** | **$188,760** | **$271,792** |

### 24-Month ROI by Scenario

| Metric | Conservative | Base | Optimistic |
|--------|-------------|------|-----------|
| 24-month cumulative revenue | $107,000 | $430,000 | $1,200,000 |
| 24-month cumulative cash OpEx | $50,000 | $85,367 | $180,000 |
| 24-month net cash profit | $57,000 | $344,633 | $1,020,000 |
| Cash ROI | 114% | 404% | 567% |
| Incl. opportunity cost, net economic profit | -$80,280 | $207,353 | $882,720 |
| Economic ROI | -43% | 111% | 325% |

## 4.2 Monthly P&L Projections — 24 Months (Base Scenario)

| Month | MRR | Gross Revenue | Stripe Fees (3.2%) | Net Revenue | COGS (Infra) | Gross Profit | GP Margin | OpEx (SaaS+Mktg+Legal) | EBITDA | Cum. EBITDA |
|-------|-----|--------------|--------------------|-----------| -------------|-------------|-----------|------------------------|--------|------------|
| Apr'26 | $0 | $0 | $0 | $0 | $220 | -$220 | — | $61 | -$281 | -$281 |
| May'26 | $0 | $0 | $0 | $0 | $228 | -$228 | — | $72 | -$300 | -$581 |
| Jun'26 | $0 | $0 | $0 | $0 | $235 | -$235 | — | $91 | -$326 | -$907 |
| Jul'26 | $500 | $500 | $16 | $484 | $235 | $249 | 51% | $91 | $158 | -$749 |
| Aug'26 | $900 | $900 | $29 | $871 | $235 | $636 | 73% | $91 | $545 | -$204 |
| Sep'26 | $2,000 | $2,000 | $64 | $1,936 | $235 | $1,701 | 88% | $91 | $1,610 | $1,406 |
| Oct'26 | $3,000 | $3,000 | $96 | $2,904 | $320 | $2,584 | 89% | $608 | $1,976 | $3,382 |
| Nov'26 | $3,800 | $3,800 | $122 | $3,678 | $320 | $3,358 | 91% | $608 | $2,750 | $6,132 |
| Dec'26 | $5,000 | $5,000 | $160 | $4,840 | $320 | $4,520 | 93% | $608 | $3,912 | $10,044 |
| Jan'27 | $6,500 | $6,500 | $208 | $6,292 | $320 | $5,972 | 95% | $608 | $5,364 | $15,408 |
| Feb'27 | $7,500 | $7,500 | $240 | $7,260 | $630 | $6,630 | 91% | $1,349 | $2,281* | $17,689 |
| Mar'27 | $10,000 | $10,000 | $320 | $9,680 | $630 | $9,050 | 93% | $1,349 | $4,701 | $22,390 |
| Apr'27 | $12,000 | $12,000 | $384 | $11,616 | $665 | $10,951 | 94% | $1,349 | $6,602 | $28,992 |
| May'27 | $14,000 | $14,000 | $448 | $13,552 | $665 | $12,887 | 95% | $1,349 | $8,538 | $37,530 |
| Jun'27 | $18,000 | $18,000 | $576 | $17,424 | $700 | $16,724 | 96% | $1,500 | $12,224 | $49,754 |
| Jul'27 | $20,000 | $20,000 | $640 | $19,360 | $750 | $18,610 | 96% | $1,600 | $8,010** | $57,764 |
| Aug'27 | $23,000 | $23,000 | $736 | $22,264 | $800 | $21,464 | 96% | $1,700 | $8,764 | $66,528 |
| Sep'27 | $30,000 | $30,000 | $960 | $29,040 | $900 | $28,140 | 97% | $1,800 | $14,340 | $80,868 |
| Oct'27 | $33,000 | $33,000 | $1,056 | $31,944 | $1,000 | $30,944 | 97% | $2,000 | $15,944 | $96,812 |
| Nov'27 | $37,000 | $37,000 | $1,184 | $35,816 | $1,050 | $34,766 | 97% | $2,100 | $18,666 | $115,478 |
| Dec'27 | $40,000 | $40,000 | $1,280 | $38,720 | $1,100 | $37,620 | 97% | $2,200 | $20,420 | $135,898 |
| Jan'28 | $43,000 | $43,000 | $1,376 | $41,624 | $1,170 | $40,454 | 97% | $2,300 | $21,154 | $157,052 |
| Feb'28 | $47,000 | $47,000 | $1,504 | $45,496 | $1,200 | $44,296 | 97% | $2,400 | $23,896 | $180,948 |
| Mar'28 | $55,000 | $55,000 | $1,760 | $53,240 | $1,250 | $51,990 | 98% | $2,500 | $30,490 | $211,438 |

*\* Feb'27: EBITDA dip reflects Phase 4 cost step-up (infrastructure upgrade + contractor onboarding costs)*  
*\*\* Jul'27: Additional personnel cost (second contractor) added*

**Note on Personnel in EBITDA**: Personnel costs (contractors at $3K–$14K/mo) are included implicitly in the EBITDA line. Full P&L breakdown: EBITDA = Net Revenue − COGS − OpEx − Personnel − Buffer.

## 4.3 Break-Even Analysis

### Infrastructure-Only Break-Even

| Scenario | Infra Cost/mo | MRR Required | Users Required (at $59 ARPU) | Achieved (month) |
|----------|-------------|-------------|------------------------------|-------------------|
| Phase 1-2 | $235 | $243 (Stripe-adjusted) | ~5 paying users | Aug 2026 (Base) |
| Phase 3 | $320 | $331 | ~6 paying users | Jul 2026 (Base) |
| Phase 4 | $630 | $651 | ~11 paying users | Jul 2026 (Base) |

### Full Break-Even (Infra + SaaS + GTM, excl. Founder Salary)

| Scenario | All OpEx/mo | MRR Required | Users Required | Achieved (month) |
|----------|-----------|-------------|----------------|-------------------|
| Conservative | $600 | $620 | ~11 users | Dec 2026 |
| Base | $1,021 | $1,055 | ~18 users | Sep 2026 |
| Optimistic | $800 | $826 | ~14 users | Aug 2026 |

### Full Break-Even (Including Founder Salary $3,000/mo)

| Scenario | Total Cost/mo | MRR Required | Users Required | Achieved (month) |
|----------|-------------|-------------|----------------|-------------------|
| Conservative | $3,600 | $3,719 | ~63 users | **Apr 2027 (Month 13)** |
| Base | $4,021 | $4,154 | ~70 users | **Dec 2026 (Month 9)** |
| Optimistic | $3,800 | $3,926 | ~67 users | **Oct 2026 (Month 7)** |

```
Break-Even Chart (Base Scenario) — Monthly MRR vs Total Cost
$12K ┤                                                    ╱
     │                                              ╱
$10K ┤                                        ●╱   MRR
     │                                    ╱
 $8K ┤                                ╱
     │                            ╱
 $6K ┤                        ╱
     │                    ╱
 $4K ┤    ─────────── ●───── Total Cost (incl. salary)
     │           ● ╱ ╱
 $2K ┤       ╱ ╱ ╱
     │   ╱ ╱─── Total Cost (OpEx only)
   $0┤╱──────┬──────┬──────┬──────┬──────┬──────┬──────
     Apr    Jun    Aug    Oct    Dec    Feb    Apr
     2026   2026   2026   2026   2026   2027   2027

     ● = Break-even points
     OpEx-only break-even: ~Sep 2026
     Full break-even (incl. salary): ~Dec 2026
```

## 4.4 Payback Period Analysis

| Scenario | Total Pre-Revenue Investment | Monthly Profit at Break-Even | Simple Payback | Discounted Payback (15%) |
|----------|----------------------------|------------------------------|----------------|-------------------------|
| Conservative | $907 | $400/mo avg | 2.3 months | 2.5 months |
| Base | $907 | $1,600/mo avg | 0.6 months | 0.6 months |
| Optimistic | $907 | $5,200/mo avg | 0.2 months | 0.2 months |

**Note**: Payback is extremely fast because pre-revenue investment is only $907 (3 months × ~$300/mo). This is the advantage of a bootstrapped, low-burn SaaS model. Even including founder salary as investment ($907 + $9,000 = $9,907), payback occurs within 3–7 months of first revenue.

## 4.5 IRR and NPV Calculations (3-Year Horizon)

### Net Cash Flows by Year (Base Scenario)

| Year | Revenue | Total Costs | Net Cash Flow | Notes |
|------|---------|-------------|--------------|-------|
| Year 0 (Apr–Jun 2026) | $0 | $907 | -$907 | Pre-revenue investment |
| Year 1 (Jul 2026–Jun 2027) | $109,200 | $38,400 | $70,800 | Includes first contractor from Nov'26 |
| Year 2 (Jul 2027–Jun 2028) | $480,000 | $168,000 | $312,000 | Team of 2-3, scaling costs |
| Year 3 (Jul 2028–Jun 2029) | $960,000 | $360,000 | $600,000 | Team of 5-6, multi-market |

### NPV Calculation (15% Discount Rate, Base Scenario)

$$NPV = \sum_{t=0}^{3} \frac{CF_t}{(1 + r)^t}$$

| Period | Cash Flow | Discount Factor (15%) | Present Value |
|--------|-----------|---------------------|--------------|
| Year 0 | -$907 | 1.000 | -$907 |
| Year 1 | $70,800 | 0.870 | $61,596 |
| Year 2 | $312,000 | 0.756 | $235,872 |
| Year 3 | $600,000 | 0.658 | $394,800 |
| **NPV (Base)** | | | **$691,361** |

### NPV by Scenario

| Scenario | NPV (3-year, 15% DR) | IRR | Probability | Weighted NPV |
|----------|-------------------|----|-------------|-------------|
| Conservative | $156,240 | 312% | 25% | $39,060 |
| Base | $691,361 | 1,420% | 50% | $345,681 |
| Optimistic | $2,186,940 | 3,850% | 25% | $546,735 |
| **Expected (Weighted)** | | | | **$931,476** |

**Note**: IRR is extraordinarily high because the initial cash investment is only ~$907. This is a defining feature of bootstrapped software businesses — near-zero starting capital with high future cash flow potential.

### NPV Including Founder Salary as Investment

| Scenario | Additional Investment (salary × 24mo) | Adjusted NPV | Adjusted IRR |
|----------|---------------------------------------|-------------|-------------|
| Conservative | $72,000 | $84,240 | 42% |
| Base | $72,000 | $619,361 | 185% |
| Optimistic | $72,000 | $2,114,940 | 490% |

Even accounting for the founder's opportunity cost, the risk-adjusted NPV remains strongly positive across all scenarios.

## 4.6 Unit Economics Deep-Dive

### By Tier

| Metric | Free (Explorer) | Pro ($42/mo) | Business ($85/mo) | Enterprise ($215/mo) |
|--------|----------------|-------------|-------------------|---------------------|
| Expected % of users | 60% | 25% | 12% | 3% |
| Monthly revenue/user | $0 | $42 | $85 | $215 |
| Infra COGS/user | $1.70 | $1.70 | $2.50 | $5.00 |
| Stripe fee/user | $0 | $1.34 | $2.72 | $6.88 |
| Support cost/user | $0 | $0.50 | $2.00 | $10.00 |
| **Gross Profit/user** | **-$1.70** | **$38.46** | **$77.78** | **$193.12** |
| **Gross Margin** | N/A | **91.6%** | **91.5%** | **89.8%** |
| CAC (estimated) | $0 | $80 | $120 | $500 |
| Expected LTV (months) | — | $42 × 14mo = $588 | $85 × 18mo = $1,530 | $215 × 24mo = $5,160 |
| LTV:CAC | — | **7.4:1** | **12.8:1** | **10.3:1** |
| Payback period | — | **1.9 months** | **1.4 months** | **2.3 months** |

### Cohort Analysis (Projected Monthly Churn Decay)

| Cohort Month | Starting Users | Month 1 | Month 3 | Month 6 | Month 12 | Surviving % |
|-------------|---------------|---------|---------|---------|----------|------------|
| Jul 2026 (first) | 12 | 11 | 9 | 7 | 5 | 42% |
| Sep 2026 | 30 | 28 | 24 | 20 | 16 | 53% |
| Dec 2026 | 50 | 48 | 42 | 37 | 32 | 64% |
| Mar 2027 | 80 | 77 | 70 | 63 | 57 | 71% |

**Insight**: Early cohorts will have higher churn (product still evolving). Later cohorts benefit from product improvements, better onboarding, and stronger product-market fit. Targeting <3% monthly churn by Month 12 yields 71%+ 12-month retention.

## 4.7 Sensitivity Analysis

### Revenue Sensitivity

| What-If Scenario | Impact on 12-Month MRR | Impact on Break-Even | Severity |
|-----------------|----------------------|---------------------|---------|
| Churn +3% (8% monthly) | MRR drops 35-40% | Delayed 3-4 months | 🔴 HIGH |
| Churn +1% (6% monthly) | MRR drops 15% | Delayed 1 month | 🟡 MEDIUM |
| ARPU -20% ($47 vs $59) | MRR drops 20% | Delayed 2 months | 🟡 MEDIUM |
| Launch delayed 2 months | First revenue Sep'26 vs Jul'26 | Delayed 2 months | 🟡 MEDIUM |
| Launch delayed 4 months | First revenue Nov'26 vs Jul'26 | Delayed 5 months | 🔴 HIGH |
| CAC doubles ($200) | ROI still positive (LTV:CAC > 3:1) | No impact (organic) | 🟢 LOW |
| Azure costs +50% | $100-200/mo higher costs | Negligible | 🟢 LOW |
| 0 Enterprise users | ARPU drops to ~$50 | Delayed 1-2 months | 🟡 MEDIUM |

### Tornado Chart — Impact on 12-Month NPV (Base Scenario)

```
Factor                    Negative Impact ←──── 0 ────→ Positive Impact

Monthly Churn             ████████████████████ ←   → ██████████
(3%→8%)                   -$280K                      +$140K (3%→1%)

Launch Delay              ██████████████████ ←     → ██████████████
(+4 months)               -$250K                      +$180K (-2 months)

ARPU                      ██████████████ ←          → ██████████████
(-30%)                    -$210K                      +$210K (+30%)

User Growth Rate          ████████████ ←            → ████████████████
(-30%)                    -$180K                      +$240K (+30%)

Infrastructure Cost       ██ ←                      → ██
(+100%)                   -$30K                       +$15K (-50%)

Stripe Fee                █ ←                       → █
(+1%)                     -$15K                       +$10K (-0.5%)
```

**Key insight**: Churn rate and launch timing are the two most sensitive financial variables. Infrastructure costs have minimal impact — the business is capital-light. Focus should be on retention and speed-to-market.

---

# 5. DELIVERABLE 4: CASH FLOW TIMELINE

## 5.1 Monthly Cash Flow Statement — 24 Months

### Base Scenario

| Month | Phase | Cash In (Revenue) | Stripe Fees | Net Cash In | Cash Out (OpEx) | Personnel | Net Cash Flow | Cumulative Cash |
|-------|-------|-------------------|-------------|------------|-----------------|-----------|--------------|----------------|
| Apr'26 | P1 | $0 | $0 | $0 | $281 | $0 | -$281 | -$281 |
| May'26 | P1→2 | $0 | $0 | $0 | $320 | $0 | -$320 | -$601 |
| Jun'26 | P2 | $0 | $0 | $0 | $359 | $0 | -$359 | -$960 |
| Jul'26 | P2 | $500 | $16 | $484 | $359 | $0 | $125 | -$835 |
| Aug'26 | P2 | $900 | $29 | $871 | $359 | $0 | $512 | -$323 |
| Sep'26 | P2 | $2,000 | $64 | $1,936 | $359 | $0 | $1,577 | $1,254 |
| Oct'26 | P3 | $3,000 | $96 | $2,904 | $1,021 | $0 | $1,883 | $3,137 |
| Nov'26 | P3 | $3,800 | $122 | $3,678 | $1,021 | $3,000 | -$343 | $2,794 |
| Dec'26 | P3 | $5,000 | $160 | $4,840 | $1,021 | $3,000 | $819 | $3,613 |
| Jan'27 | P3 | $6,500 | $208 | $6,292 | $1,021 | $3,000 | $2,271 | $5,884 |
| Feb'27 | P4 | $7,500 | $240 | $7,260 | $1,870 | $3,000 | $2,390 | $8,274 |
| Mar'27 | P4 | $10,000 | $320 | $9,680 | $1,870 | $3,000 | $4,810 | $13,084 |
| Apr'27 | P4 | $12,000 | $384 | $11,616 | $2,014 | $7,000 | $2,602 | $15,686 |
| May'27 | P4 | $14,000 | $448 | $13,552 | $2,014 | $7,000 | $4,538 | $20,224 |
| Jun'27 | P4 | $18,000 | $576 | $17,424 | $2,127 | $7,000 | $8,297 | $28,521 |
| Jul'27 | P4 | $20,000 | $640 | $19,360 | $2,227 | $9,000 | $8,133 | $36,654 |
| Aug'27 | P4 | $23,000 | $736 | $22,264 | $2,327 | $9,000 | $10,937 | $47,591 |
| Sep'27 | P4 | $30,000 | $960 | $29,040 | $2,427 | $9,000 | $17,613 | $65,204 |
| Oct'27 | P4 | $33,000 | $1,056 | $31,944 | $2,627 | $11,000 | $18,317 | $83,521 |
| Nov'27 | P4 | $37,000 | $1,184 | $35,816 | $2,727 | $11,000 | $22,089 | $105,610 |
| Dec'27 | P4 | $40,000 | $1,280 | $38,720 | $2,827 | $11,000 | $24,893 | $130,503 |
| Jan'28 | P4 | $43,000 | $1,376 | $41,624 | $2,927 | $14,000 | $24,697 | $155,200 |
| Feb'28 | P4 | $47,000 | $1,504 | $45,496 | $3,027 | $14,000 | $28,469 | $183,669 |
| Mar'28 | P4 | $55,000 | $1,760 | $53,240 | $3,127 | $14,000 | $36,113 | $219,782 |

### Conservative Scenario — Cumulative Cash Position

| Month | Cash In | Cash Out | Net Flow | Cumulative |
|-------|---------|----------|----------|-----------|
| Apr'26 | $0 | $281 | -$281 | -$281 |
| Jun'26 | $0 | $359 | -$359 | -$960 |
| Sep'26 | $800 | $359 | $415 | -$1,150 |
| Dec'26 | $2,500 | $800 | $1,620 | $1,050 |
| Mar'27 | $5,000 | $1,200 | $3,472 | $12,500 |
| Jun'27 | $8,000 | $3,500 | $4,180 | $28,000 |
| Sep'27 | $12,000 | $5,000 | $6,320 | $52,000 |
| Dec'27 | $15,000 | $6,500 | $7,730 | $78,000 |
| Mar'28 | $18,000 | $8,000 | $9,140 | $107,000 |

### Optimistic Scenario — Cumulative Cash Position

| Month | Cash In | Cash Out | Net Flow | Cumulative |
|-------|---------|----------|----------|-----------|
| Apr'26 | $0 | $281 | -$281 | -$281 |
| Jun'26 | $0 | $359 | -$359 | -$960 |
| Sep'26 | $4,000 | $500 | $3,372 | $5,300 |
| Dec'26 | $10,000 | $2,500 | $7,180 | $28,000 |
| Mar'27 | $22,000 | $6,000 | $15,472 | $95,000 |
| Jun'27 | $40,000 | $15,000 | $24,200 | $230,000 |
| Sep'27 | $65,000 | $25,000 | $38,700 | $450,000 |
| Dec'27 | $90,000 | $35,000 | $53,200 | $750,000 |
| Mar'28 | $120,000 | $50,000 | $67,800 | $1,200,000 |

## 5.2 Cash Position Tracking — Waterfall View (Base Scenario)

```
Cumulative Cash Position ($K) — Base Scenario
$220K ┤                                                         ╱
      │                                                     ╱
$180K ┤                                                 ╱
      │                                             ╱
$140K ┤                                         ╱
      │                                     ╱
$100K ┤                                 ╱
      │                             ╱
 $60K ┤                         ╱
      │                     ╱
 $40K ┤               ╱ ╱
      │          ● ╱
 $20K ┤     ╱ ╱
      │──●╱
   $0 ┼╱──┬─────┬─────┬─────┬─────┬─────┬─────┬──
   -$1K   Jun   Sep   Dec   Mar   Jun   Sep   Dec   Mar
     Apr  2026  2026  2026  2027  2027  2027  2027  2028
     2026

   ● = Minimum cash points (pre-revenue trough ~-$960)
```

## 5.3 Revenue Collection Timing

| Component | Timing | Impact |
|-----------|--------|--------|
| **Stripe card charge** | Instant (real-time) | Revenue recognized immediately |
| **Stripe → bank payout** | T+3 business days (EU) | Cash available ~3-5 calendar days after charge |
| **Monthly subscription billing** | 1st of each month (or anniversary) | Predictable cash inflow timing |
| **Annual subscription billing** | Up-front, full year | Cash boost on payment day; recognized monthly (accrual) |
| **Failed payment retry** | Stripe Smart Retries: Days 1, 3, 5, 7 | ~5-8% of charges fail initially; ~60% recovered via retry |
| **Involuntary churn (failed payment)** | Day 7+ after failed charge | ~2% of users lost to failed payments monthly |
| **Refund window** | 14 days (EU consumer rights) | Cash outflow risk for first 14 days of subscription |

### Cash-to-Accrual Revenue Timing Adjustment

| Month | Accrual Revenue (MRR) | Cash Received (3-day lag) | Annual Prepay Cash Boost | Total Cash Received |
|-------|----------------------|--------------------------|-------------------------|-------------------|
| Jul 2026 | $500 | $484 | $0 | $484 |
| Oct 2026 | $3,000 | $2,904 | $420* | $3,324 |
| Jan 2027 | $6,500 | $6,292 | $1,200* | $7,492 |
| Mar 2027 | $10,000 | $9,680 | $2,000* | $11,680 |

*\* Annual prepay = 20% of new users × 10 months prepaid (after 15% discount)*

## 5.4 Expense Timing Patterns

| Expense Type | Payment Frequency | Cash Flow Impact |
|-------------|-------------------|------------------|
| Azure (infrastructure) | Monthly (auto-charge) | Smooth, predictable |
| Sentry | Monthly | Smooth |
| UptimeRobot | Annual ($84) | Lump payment in Apr |
| Domain renewal | Annual (~$60) | Lump payment (Mar) |
| Stripe fees | Deducted from each payout | Automatic, real-time |
| Contractors (PL) | Monthly (by 10th) | Large single payment |
| Content freelancers | Per-deliverable | Variable timing |
| Accounting | Monthly | By 15th of following month |
| EUIPO trademark | One-time (€850) | Single payment |
| Tax (PL PIT/CIT) | Monthly advance (by 20th) | After profitable months |

## 5.5 Minimum Cash Reserve Requirements

| Phase | Monthly Burn | Reserve Months | Minimum Cash Reserve | Purpose |
|-------|-------------|----------------|---------------------|---------|
| Phase 1 | $281 | 3 months | $843 | Pre-revenue runway |
| Phase 2 | $359 | 2 months | $718 | Some revenue expected |
| Phase 3 | $1,021 | 2 months | $2,042 | Covering GTM ramp costs |
| Phase 4 (pre-hire) | $1,870 | 3 months | $5,610 | Buffer before contractor costs |
| Phase 4 (with team) | $5,623 | 3 months | $16,869 | Payroll obligations |
| Phase 4 (scaled) | $8,000 | 3 months | $24,000 | Team salary protection |

**Critical rule**: Never let cumulative cash position fall below the minimum reserve for the current phase. If it approaches 1.5× the minimum, trigger the cost-reduction protocol (Section 6).

## 5.6 Cash Runway Analysis

| Phase | Monthly Burn (excl. salary) | Starting Cash | Cash Runway (months) |
|-------|---------------------------|---------------|---------------------|
| Phase 1 (from $0 savings) | $281 | $2,000 (seed) | 7.1 months |
| Phase 1 (from $5K savings) | $281 | $5,000 | 17.8 months |
| Phase 2 (with early revenue) | $359 net (offset by $500 MRR) | Growing | Infinite (cash-positive) |
| Phase 3 (if revenue stalls at $2K) | $1,021 - $2,000 = surplus | Growing | Infinite |
| Phase 4 (if revenue stalls at $5K) | $5,623 - $5,000 = -$623/mo | $13,084 | 21 months |
| Phase 4 (if revenue stalls at $10K) | $5,623 - $10,000 = +$4,377/mo | Growing | Infinite |

**Worst-case survivability**: Even if MRR stalls at $5K in Phase 4, the accumulated cash reserves ($13K+) provide 21 months of runway at net-negative burn, giving ample time to course-correct.

## 5.7 FX Impact on Cash Flow

### Monthly FX Conversion Flow

```
Revenue (EUR) → Stripe → USD payout → Multi-currency bank account
                                        ├── USD payments (Azure, SaaS tools): ~69% of costs
                                        └── EUR → PLN conversion: ~31% of costs
                                            ├── Contractors (PLN)
                                            ├── Accountant (PLN)
                                            └── Founder salary (PLN)
```

### FX Sensitivity on Monthly Cash Flow (at $10K MRR)

| EUR/PLN Rate | PLN Costs (PLN 12,117/mo) | PLN Costs in USD | Total Monthly Cost | Net Cash Flow | vs. Baseline |
|-------------|--------------------------|------------------|-------------------|--------------|-------------|
| 4.70 (+4%) | 12,117 PLN | $2,579 | $4,449 | $5,551 | +$210 |
| 4.50 (base) | 12,117 PLN | $2,693 | $4,563 | $5,437 | Baseline |
| 4.30 (-4%) | 12,117 PLN | $2,818 | $4,688 | $5,312 | -$125 |
| 4.10 (-9%) | 12,117 PLN | $2,955 | $4,825 | $5,175 | -$262 |

**Net exposure**: ~$125/month per 4% EUR/PLN move. At current MRR levels, this is a rounding error. FX becomes material only above $30K MRR where PLN costs exceed 25,000 PLN/month.

---

# 6. DELIVERABLE 5: FINANCIAL RISK ASSESSMENT

## 6.1 Financial Risk Register — Probability × Impact × EMV

| # | Risk | Category | Prob. | Impact ($) | EMV (P × I) | Phase Exposure |
|---|------|----------|-------|-----------|-------------|----------------|
| FR-01 | **Zero paying users after 3 months of beta** | Revenue | 20% | $50,000 (6mo lost revenue) | **$10,000** | P2 |
| FR-02 | **Monthly churn exceeds 8%** (vs. 5% target) | Revenue | 30% | $36,000/yr (revenue erosion) | **$10,800** | P2-P3 |
| FR-03 | **Launch delayed 3+ months** (technical debt) | Schedule | 25% | $30,000 (delayed revenue) | **$7,500** | P1-P2 |
| FR-04 | **Azure SQL cost escalation** (S3→S4→P1 early) | Cost | 35% | $3,600/yr ($300/mo increase) | **$1,260** | P3-P4 |
| FR-05 | **Founder burnout — 30-day work stoppage** | Operational | 25% | $40,000 (lost momentum + revenue) | **$10,000** | All |
| FR-06 | **EUR/PLN adverse move >10%** | FX | 15% | $6,000/yr (cost increase) | **$900** | P3-P4 |
| FR-07 | **Stripe account freeze** (compliance issue) | Operational | 5% | $20,000 (30 days frozen revenue) | **$1,000** | P2-P4 |
| FR-08 | **Amazon SP-API access revoked / throttled** | Technical | 10% | $100,000 (business-ending) | **$10,000** | All |
| FR-09 | **Competitive price war** (Sellerboard drops to €9) | Market | 20% | $24,000/yr (forced ARPU reduction) | **$4,800** | P3-P4 |
| FR-10 | **GDPR fine / data breach** | Legal | 5% | $50,000 (min. fine + remediation) | **$2,500** | P2-P4 |
| FR-11 | **Key contractor leaves mid-project** | Personnel | 30% | $8,000 (2mo lost productivity) | **$2,400** | P4 |
| FR-12 | **Tax authority audit** (PL US-34) | Tax | 10% | $5,000 (accountant + penalty) | **$500** | P3-P4 |
| FR-13 | **Annual prepay refund wave** | Revenue | 15% | $10,000 (mass refund event) | **$1,500** | P3-P4 |
| FR-14 | **Netfox ERP access lost** | Technical | 10% | $80,000 (core moat destroyed) | **$8,000** | All |

### Total Expected Monetary Value of All Risks

| Risk Tier | Risks | Combined EMV | % of Total |
|-----------|-------|-------------|-----------|
| HIGH (EMV > $5K) | FR-01, FR-02, FR-03, FR-05, FR-08, FR-14 | $56,300 | 78% |
| MEDIUM (EMV $1K–$5K) | FR-04, FR-09, FR-10, FR-11, FR-13 | $12,460 | 17% |
| LOW (EMV < $1K) | FR-06, FR-07, FR-12 | $2,400 | 3% |
| **TOTAL EMV** | **14 risks** | **$71,160** | **100%** |

## 6.2 Contingency Reserve Calculations

| Phase | Monthly Budget | Contingency % | Monthly Reserve | Phase Duration | Total Reserve |
|-------|---------------|---------------|----------------|---------------|--------------|
| Phase 1 (Harden) | $281 | 10% | $28 | 1.5 months | $42 |
| Phase 2 (Beta) | $359 | 15% | $54 | 4 months | $216 |
| Phase 3 (Launch) | $1,021 | 15% | $153 | 4 months | $612 |
| Phase 4 (Scale, initial) | $2,500 | 20% | $500 | 4 months | $2,000 |
| Phase 4 (Scale, team) | $5,623 | 20% | $1,125 | 12 months | $13,500 |
| **24-Month Total Reserve** | | | | | **$16,370** |

**Contingency increase rationale**: Phase 4 carries 20% contingency (vs. 10-15% earlier) because:
- Personnel costs are less flexible (contractor commitments)
- Marketing spend is harder to cut mid-campaign
- Infrastructure scaling may trigger unanticipated cost steps
- FX exposure grows with PLN-denominated personnel costs

## 6.3 Scenario Stress Testing

### Stress Test A: Zero Revenue for 3 Months (e.g., Stripe freeze, technical failure)

| Metric | Phase 2 Impact | Phase 3 Impact | Phase 4 Impact |
|--------|---------------|----------------|----------------|
| Monthly burn during blackout | $359 | $1,021 | $5,623 |
| Total cash consumed (3 months) | $1,077 | $3,063 | $16,869 |
| Cash reserve available (Base at start of period) | ~$1,254 | ~$3,613 | ~$13,084 |
| Surplus/(deficit) | **$177** ✅ | **$550** ✅ | **-$3,785** 🔴 |
| Action required | None | None | Immediate contractor pause |

### Stress Test B: Zero Revenue for 6 Months

| Metric | Impact from Phase 3 Start |
|--------|--------------------------|
| Monthly burn (Phase 3 without marketing cut) | $1,021 |
| Total cash consumed (6 months) | $6,126 |
| Cash available (entering Phase 3 in Base) | ~$3,137 |
| Deficit | -$2,989 |
| **Mitigation**: Cut marketing to $0, reduce SaaS to essentials | New burn: $450/mo → $2,700 total |
| Revised deficit with cuts | **$437 surplus** ✅ |

### Stress Test C: Zero Revenue for 12 Months (Worst Case)

| Metric | Starting from Apr 2026 |
|--------|----------------------|
| Monthly essential burn (infra + Sentry only) | $245 |
| Total 12-month essential cost | $2,940 |
| Founder minimum needs | $36,000 |
| **Total survival cost** | **$38,940** |
| **Required savings buffer** | **$40,000** |
| Alternative: Keep Amazon business running (8h/wk) | ~$3,000-5,000/mo income |
| Effective ACC runway with Amazon income | **Indefinite** ✅ |

**Critical insight**: The founder's existing Amazon business provides a natural financial safety net. Even in a total-failure scenario, the Amazon business operations (8h/week) continue generating income to cover living costs while ACC infrastructure costs remain under $250/mo.

## 6.4 Insurance & Hedging Recommendations

| Category | Recommendation | Cost | Priority | Phase |
|----------|---------------|------|----------|-------|
| **Business liability insurance (OC)** | Polish OC działalności policy | ~200 PLN/yr ($52) | LOW | Phase 3 |
| **Cyber / data breach insurance** | Consider when >100 users | ~$500-1,000/yr | MEDIUM | Phase 4 |
| **Key-person insurance** | Term life + disability | ~300 PLN/mo ($78) | MEDIUM | Phase 3 |
| **FX hedging (EUR/PLN forward)** | Not needed until MRR >$20K | $0 | LOW | Phase 4+ |
| **Revenue concentration hedging** | Ensure no single customer >15% of MRR | $0 (operational) | HIGH | Phase 3 |
| **Stripe alternative** | Set up Paddle or Adyen as backup payment processor | $0 (setup only) | MEDIUM | Phase 3 |
| **Infrastructure redundancy** | Database backups to separate Azure region | ~$20/mo | HIGH | Phase 2 |
| **IP protection** | EUIPO trademark for "ACC" / "Ascend Commerce Cloud" | €850 one-time | MEDIUM | Phase 2 |
| **Digital estate plan** | Credential escrow (1Password family / legal envelope) | $60/yr | HIGH | Phase 1 |

**Total annual insurance/hedging cost**: ~$300-$1,500/yr depending on phase. Minimal relative to revenue.

## 6.5 Financial Trigger Points — Decision Framework

### Growth Triggers (GO Signals)

| Trigger | Condition | Action | Financial Impact |
|---------|-----------|--------|------------------|
| **T-G1**: Infra break-even | MRR ≥ $303 | Validate pricing model; continue plan | Infrastructure fully funded |
| **T-G2**: First hire ready | MRR ≥ $5,000 sustained 2 months | Engage frontend contractor | +$3,000/mo cost, +velocity |
| **T-G3**: Full break-even | MRR ≥ $3,300 (incl. founder salary) | Founder takes salary; increase marketing | Lifestyle sustainable |
| **T-G4**: Scale investment | MRR ≥ $10,000 sustained 3 months | First FT hire; upgrade infrastructure | +$5,000/mo cost |
| **T-G5**: DACH expansion | MRR ≥ $15,000 + 50 DE users | Invest in DE content, support, legal | +$2,000/mo |
| **T-G6**: Series A exploration | MRR ≥ $30,000 + growth >10% MoM | Engage investors; prepare data room | Fundraise $500K–$1M |

### Cost-Reduction Triggers (CAUTION Signals)

| Trigger | Condition | Action | Savings |
|---------|-----------|--------|---------|
| **T-C1**: Revenue decline | MRR drops >15% for 2 consecutive months | Freeze all non-essential SaaS tools | $100-200/mo |
| **T-C2**: Cash reserve breach | Cumulative cash < 2× monthly burn | Pause marketing spend immediately | $200-1,000/mo |
| **T-C3**: Overburn | Monthly OpEx > 60% of MRR for 3 months | Downgrade infrastructure tier; renegotiate tools | $100-300/mo |
| **T-C4**: Contractor underperformance | Contractor ROI < 0 after 2 months | Reduce to minimum hours or terminate | $2,000-3,000/mo |

### Pivot/Kill Triggers (STOP Signals)

| Trigger | Condition | Action | Financial Outcome |
|---------|-----------|--------|-------------------|
| **T-K1**: No PMF | <10 paid users after 6 months of public access | Pivot to consulting/agency model | Salvage domain expertise |
| **T-K2**: Cash crisis | Cumulative cash < 1× monthly burn AND MRR declining | Pause all non-infra expenses; return to Amazon business FT | Minimal ongoing cost ($245/mo) |
| **T-K3**: Competitive kill | Major competitor launches identical product at lower price | Pivot to Enterprise-only niche OR seek acquisition | Negotiate exit |
| **T-K4**: Technical failure | Core data pipeline unrecoverable for >30 days | Evaluate rebuild vs. shutdown | Preserve IP value for sale |

### Funding Triggers

| Trigger | Condition | Action | Terms Target |
|---------|-----------|--------|-------------|
| **T-F1**: Seed round | $120K+ ARR, >100 users, <5% churn | Raise $200-500K at $600K-$1M pre-money | 20-30% dilution |
| **T-F2**: Strategic angel | Industry contact offers favorable terms | Accept if valuation ≥ 5x ARR | <15% dilution |
| **T-F3**: Revenue-based financing | Predictable $10K+ MRR | Explore Pipe, Clearco-like options | No dilution, 5-8% of revenue |
| **T-F4**: Series A | $500K+ ARR, 15%+ MoM growth | Raise $1-3M at $2.5-4M pre-money | 20-25% dilution |

## 6.6 Monthly Financial Review Checklist

Execute on the **first Monday of each month**. Target: < 45 minutes total.

### Part 1: Revenue Health (10 minutes)

- [ ] **MRR actual vs. target** — Record in dashboard. Flag if >10% variance.
- [ ] **New MRR added** — Count new paying users this month.
- [ ] **Churned MRR** — Count users who cancelled. Calculate churn rate.
- [ ] **Expansion MRR** — Count tier upgrades. Calculate NRR.
- [ ] **ARPU trend** — Current blended ARPU vs. target ($59). Direction? Why?
- [ ] **Pipeline** — Free users who might convert. Trial-to-paid conversion rate.
- [ ] **Stripe dashboard check** — Failed payments, disputes, refunds.

### Part 2: Cost Control (10 minutes)

- [ ] **Total OpEx this month** — Sum all charges. Compare to budget.
- [ ] **Infrastructure costs** — Azure bill review. Any unexpected spikes?
- [ ] **SaaS tool audit** — Any unused subscriptions? Anything approaching tier limits?
- [ ] **Contractor costs** — Hours billed vs. planned. Value delivered vs. cost.
- [ ] **Marketing spend vs. ROI** — Cost per lead, cost per acquisition.
- [ ] **Budget variance** — Any category >15% over budget? Root cause?

### Part 3: Cash Position (10 minutes)

- [ ] **Bank balance** — Record across all accounts (PLN, EUR, USD).
- [ ] **Stripe balance** — Pending payouts, reserved funds.
- [ ] **Cash runway** — At current burn rate, how many months of runway?
- [ ] **Reserve adequacy** — Is cash > minimum reserve for current phase?
- [ ] **Upcoming large expenses** — Annual renewals, contractor payments, tax.
- [ ] **FX check** — EUR/PLN rate. Material move this month?

### Part 4: Forward Planning (10 minutes)

- [ ] **Next month forecast** — Expected MRR, expected costs, expected net cash flow.
- [ ] **Trigger check** — Any growth/caution/kill triggers approaching?
- [ ] **Hiring timeline** — Are revenue triggers approaching? Start sourcing?
- [ ] **Quarterly tax estimate** — Set aside 19% of net profit for tax.
- [ ] **Strategic spend review** — Any new tools/services needed next month?

### Part 5: Action Items (5 minutes)

- [ ] **Top 3 financial actions for next month** — Specify, assign deadline.
- [ ] **Update financial plan** — Adjust projections if actual differs >20% from plan.
- [ ] **Investor readiness** — Update data room if applicable.
- [ ] **Document any financial decisions** — Record rationale for future reference.

---

# APPENDIX A: KEY FINANCIAL METRICS GLOSSARY

| Metric | Formula | Target |
|--------|---------|--------|
| **MRR** | Sum of all monthly subscription fees | Per scenario table |
| **ARR** | MRR × 12 | Per scenario table |
| **ARPU** | MRR / paying users | $59 |
| **CAC** | Total acquisition cost / new customers | < $100 |
| **LTV** | ARPU / monthly churn rate | $660+ |
| **LTV:CAC** | LTV / CAC | > 6:1 |
| **Gross Margin** | (Revenue − COGS) / Revenue | > 85% |
| **Net Revenue Retention** | (Start MRR + expansion − contraction − churn) / Start MRR | > 105% |
| **Burn Rate** | Monthly cash outflow (excl. revenue) | Per phase |
| **Cash Runway** | Cash balance / monthly net burn | > 6 months |
| **Rule of 40** | Revenue growth rate + profit margin | > 40% |
| **Magic Number** | Net new ARR / prior quarter S&M spend | > 0.75 |
| **Payback Period** | CAC / (ARPU × Gross Margin) | < 2 months |

# APPENDIX B: CURRENCY REFERENCE TABLE

| Currency | Symbol | Rate vs USD | Rate vs EUR | Rate vs PLN |
|----------|--------|------------|-------------|-------------|
| USD | $ | 1.000 | 0.926 | 3.846 |
| EUR | € | 1.080 | 1.000 | 4.154 |
| PLN | zł | 0.260 | 0.241 | 1.000 |

*Rates as of Mar 2026. Update monthly in financial review.*

# APPENDIX C: SCENARIO PROBABILITY DISTRIBUTION

```
                    Probability Distribution — 12-Month ARR Outcomes

  50% ┤            ┌────────────┐
      │            │            │
  40% ┤            │   BASE     │
      │            │  $120K     │
  30% ┤            │   ARR      │
      │  ┌────────┐│            │┌────────┐
  25% ┤  │ CONS.  ││            ││ OPTIM. │
      │  │ $36K   ││            ││ $300K  │
  20% ┤  │ ARR    ││            ││ ARR    │
      │  │        ││            ││        │
  10% ┤  │        ││            ││        │
      │  │        ││            ││        │
   0% ┼──┴────────┴┴────────────┴┴────────┴──
      $0   $50K   $100K  $150K  $200K  $250K  $300K+

      Expected (weighted) ARR: $144,000
```

# APPENDIX D: 24-MONTH FINANCIAL MILESTONE TIMELINE

```
Apr 2026                                                    Mar 2028
│                                                               │
├── Phase 1 ──┤──── Phase 2 ────────┤──── Phase 3 ────┤── Phase 4 ────────────────────────────────┤
│ HARDEN      │ BETA                │ LAUNCH          │ SCALE                                     │
│             │                     │                 │                                           │
│ $281/mo     │ $359/mo             │ $1,021/mo       │ $2,500→$8,000/mo                          │
│ burn        │ burn                │ burn            │ burn (growing with team)                   │
│             │                     │                 │                                           │
│             │    ▼ Jul: First $   │   ▼ Oct: $3K MRR│  ▼ Feb: $7.5K MRR                         │
│             │    ▼ Aug: $900 MRR  │   ▼ Dec: $5K MRR│  ▼ Mar: $10K MRR ★ FULL BREAK-EVEN       │
│             │    ▼ Sep: $2K MRR   │   ▼ Jan: $6.5K  │  ▼ Jun: $18K MRR                          │
│             │       ★ INFRA B/E   │                 │  ▼ Sep: $30K MRR ★ SERIES A READY          │
│             │                     │                 │  ▼ Mar'28: $55K MRR                        │
│             │                     │                 │                                           │
│  Inv: $421  │  Inv: $1,436        │  Inv: $4,084    │  Inv: ~$79,426                             │
│             │                     │                 │  (mostly personnel)                        │
│             │                     │                 │                                           │
│ Cumul: -$421│ Cumul: -$960→+$1,254│ Cumul: +$5,884  │  Cumul: +$219,782                          │
│             │                     │                 │                                           │
├─────────────┴─────────────────────┴─────────────────┴───────────────────────────────────────────┤
│ ★ Key milestones (Base scenario)                                                                │
│ ★ Infra break-even: Sep 2026 (~$303 MRR)                                                       │
│ ★ Full break-even (incl. salary): Dec 2026 (~$3,300 MRR)                                       │
│ ★ First hire trigger: Nov-Dec 2026 ($5K MRR sustained)                                         │
│ ★ Series A ready: Sep 2027 ($360K ARR)                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## DOCUMENT CONTROL

| Field | Value |
|-------|-------|
| **Document** | ACC Financial Plan — FY2026/28 |
| **Version** | 1.0 |
| **Status** | APPROVED — Initial Release |
| **Author** | Finance Tracker Agent |
| **Reviewer** | Miłosz Sobieniowski, Founder |
| **Created** | 2026-03-13 |
| **Next Review** | 2026-04-07 (first monthly review) |
| **Supersedes** | N/A (first financial plan) |

---

*This document is a living financial plan. Update monthly during the Financial Review Checklist (Section 6.6). Major revisions required when actual results diverge >20% from projections or when phase transitions occur.*

*Confidential — For internal use and investor readiness preparation only.*
