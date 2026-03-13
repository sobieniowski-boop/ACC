# PHASE 0 — EXECUTIVE SUMMARY
## Amazon Command Center: Discovery Findings & GO/NO-GO Verdict

**Date**: 2026-03-12 | **Format**: McKinsey SCQA | **Classification**: Strategic — Decision Document  
**Prepared for**: Miłosz Sobieniowski, Founder  
**Input Reports**: Market Intelligence, Feedback Synthesis, UX Research, Data Audit, Tech Stack Assessment

---

## SITUATION

ACC is a functioning Amazon seller profit analytics platform covering 9 EU marketplaces, tracking 847K orders across 187 database tables (19.3 GB). It connects to 12 external data sources — including a direct ERP link (Netfox) that no competitor replicates — to calculate CM1/CM2/NP profit with 99.5% purchase price coverage and 96% COGS linkage. The Amazon seller tools market ($2.4–3.2B TAM) is in consolidation: Helium 10 revenue down 24%, Jungle Scout down 19%. Profit analytics — ACC's niche — sits early on the adoption curve with no dominant player and a fragmented field.

## COMPLICATION

Despite a strong data foundation and unique positioning, five cross-cutting risks threaten viability:

1. **Build fatigue** — 80% of time is spent building features vs. 20% using them for business decisions. 33 pages (37% of UI) are overinvested in low-usage modules (Strategy, Tax, Seasonality, Content).
2. **Performance erodes trust** — The core Product Profit Table loads in 14.5s (no SQL pagination). Users cannot trust a tool that makes them wait.
3. **Data gaps undermine the "profit truth" promise** — Ads data is 93h stale, FBA fees cover only 30.1% of applicable orders, and 72 tables (38.5%) are empty schema.
4. **Single point of knowledge** — One founder, no documentation, no E2E tests. Bus factor = 1.
5. **12–18 month competitive window** — Incumbents or VC-backed AI-native entrants will close the gap if ACC doesn't ship a usable paid product.

## QUESTION

Should ACC proceed to Phase 1 (product hardening + beta) or pivot strategy?

## ANSWER: **GO** — with conditions

The evidence supports a **conditional GO** for Phase 1. The rationale:

| Factor | Finding | Verdict |
|--------|---------|---------|
| **Market opportunity** | SAM €180–280M (EU profit analytics); SOM €1.2–3.6M Y1–2; incumbents contracting | ✅ Validated |
| **User needs** | #1 need = trust in profit numbers; sellers overestimate margins by 15–30% | ✅ Validated |
| **Technology feasibility** | Stack Health 78/100; core engine (8.1/10 build score) is unique; no re-architecture needed | ✅ Feasible |
| **Data foundation** | DQ Score 74/100; 99.5% purchase prices; 71 measurable signals; gaps are fixable in 2–4 sprints | ✅ Sufficient |
| **Regulatory path** | VAT/OSS schema exists; DAC7 enforcement (90% probability) creates demand; incremental compliance viable | ✅ Addressable |
| **UX readiness** | Score 5.9/10 vs Sellerboard 7.3; North Star (insight < 10s) requires PPT fix + IA simplification | ⚠️ Needs work |

### Phase 1 Conditions (must complete before paid beta)

| # | Condition | Effort | Impact |
|---|-----------|--------|--------|
| C-1 | SQL pagination for PPT (target < 2s) | 5–8 days | Unblocks trust |
| C-2 | Ads sync heartbeat + single-flight guard | 2–3 days | Fixes CM2 accuracy |
| C-3 | FX rate warning (replace silent `return 1.0`) | 1–2 days | Prevents wrong margins |
| C-4 | Hide/collapse 33 underused pages | 1–2 days | Reduces cognitive load |
| C-5 | External uptime monitor (UptimeRobot) | 30 min | Ends silent downtime |

**Total estimated effort**: 10–16 days of focused hardening before any new feature work.

### Strategic Guardrail

Redirect 30% of engineering capacity from "new modules" to hardening existing infrastructure. The competitive moat is not feature count — it is **trustworthy profit numbers delivered fast**.

---

**Decision**: ✅ **GO** — proceed to Phase 1 (Harden → Beta) with the 5 conditions above.

*Word count: 498*
