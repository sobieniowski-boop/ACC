# ACC — Full UI Screen Audit

> Wygenerowano: 2026-03-07  
> Źródło: apps/web/src/pages/* (40 ekranów)  
> Stack: React 18 + TypeScript + TanStack Query + Zustand + Tailwind + Recharts + shadcn/ui

---

## Spis treści

| # | Moduł | Route | Ekran |
|---|-------|-------|-------|
| 0 | Global | — | [Global Filter Bar](#0-global-filter-bar) |
| 1 | Dashboard | `/dashboard` | [Executive Dashboard](#1-dashboard) |
| 2 | Profit | `/profit` | [Profit Explorer (Orders)](#2-profit-explorer) |
| 3 | Profit | `/profit/products` | [Product Profit Table](#3-product-profit-table) |
| 4 | Profit | `/profit/drilldown` | [Product Drilldown](#4-product-drilldown) |
| 5 | Profit | `/profit/loss-orders` | [Loss Orders](#5-loss-orders) |
| 6 | Profit | `/profit/data-quality` | [Data Quality](#6-data-quality) |
| 7 | Profit | `/profit/tasks` | [Product Tasks](#7-product-tasks) |
| 8 | Pricing | `/pricing` | [Pricing & Buy Box](#8-pricing--buy-box) |
| 9 | Planning | `/planning` | [Planning & Budget](#9-planning--budget) |
| 10 | Ads | `/ads` | [Reklamy & PPC](#10-ads) |
| 11 | AI | `/ai` | [AI Recommendations](#11-ai-recommendations) |
| 12 | Alerts | `/alerts` | [Alerts & Rules](#12-alerts--rules) |
| 13 | Jobs | `/jobs` | [Job Queue](#13-jobs) |
| 14 | Import | `/import-products` | [Import Products (Excel)](#14-import-products) |
| 15 | FBA | `/fba/overview` | [FBA Overview](#15-fba-overview) |
| 16 | FBA | `/fba/inventory` | [FBA Inventory](#16-fba-inventory) |
| 17 | FBA | `/fba/replenishment` | [FBA Replenishment](#17-fba-replenishment) |
| 18 | FBA | `/fba/inbound` | [FBA Inbound](#18-fba-inbound) |
| 19 | FBA | `/fba/aged-stranded` | [FBA Aged & Stranded](#19-fba-aged--stranded) |
| 20 | FBA | `/fba/bundles` | [FBA Bundles & Launches](#20-fba-bundles--launches) |
| 21 | FBA | `/fba/kpi-scorecard` | [FBA Scorecard](#21-fba-scorecard) |
| 22 | Finance | `/finance/dashboard` | [Finance Dashboard](#22-finance-dashboard) |
| 23 | Finance | `/finance/ledger` | [Finance Ledger](#23-finance-ledger) |
| 24 | Finance | `/finance/reconciliation` | [Finance Reconciliation](#24-finance-reconciliation) |
| 25 | Content | `/content/studio` | [Content Studio](#25-content-studio) |
| 26 | Content | `/content/compliance` | [Content Compliance](#26-content-compliance) |
| 27 | Content | `/content/assets` | [Content Assets](#27-content-assets) |
| 28 | Content | `/content/publish` | [Content Publish](#28-content-publish) |
| 29 | Inventory | `/inventory/overview` | [Inventory Overview](#29-inventory-overview) |
| 30 | Inventory | `/inventory/all` | [Manage All Inventory](#30-manage-all-inventory) |
| 31 | Inventory | `/inventory/families` | [Inventory Families](#31-inventory-families) |
| 32 | Inventory | `/inventory/drafts` | [Inventory Drafts](#32-inventory-drafts) |
| 33 | Inventory | `/inventory/jobs` | [Inventory Jobs](#33-inventory-jobs) |
| 34 | Inventory | `/inventory/settings` | [Inventory Settings](#34-inventory-settings) |
| 35 | Families | `/families` | [Family Mapper](#35-family-mapper) |
| 36 | Families | `/families/:id` | [Family Detail](#36-family-detail) |
| 37 | Families | `/families/review` | [Review Queue](#37-review-queue) |
| 38 | Families | `/families/fix-packages` | [Fix Packages](#38-fix-packages) |
| 39 | System | `/system/netfox-health` | [Netfox Health](#39-netfox-health) |
| 40 | Auth | `/login` | [Login](#40-login) |

---

## 0. Global Filter Bar

**Komponent:** `GlobalFilterBar.tsx` (sticky, widoczny na każdym ekranie wewnątrz Layout)  
**Stan:** Zustand store `globalFilters.ts` z `persist` (localStorage)

### Filtry globalne

| Filtr | Typ | Wartości |
|-------|-----|----------|
| Date Preset | select | Today, WTD, MTD, 7d, 30d, 90d, Custom |
| Date From / To | date input | YYYY-MM-DD |
| Marketplace (multi) | multi-select | Wszystkie MP z API `/marketplaces` |
| Brand | comma-separated text | Dowolny tekst, parsowany onBlur |
| Category / product_type | comma-separated text | Dowolny tekst, parsowany onBlur |
| SKU / ASIN search | textarea (multi-line) | Wiele SKU/ASIN w osobnych liniach |
| Fulfillment | multi-select | AFN (FBA), MFN (FBM), OTHER |
| Currency view | select | Base (PLN), Original |
| Profit mode | select (widoczny na `/profit/products`) | CM1, CM2, NP |
| Confidence min | range slider 0–100% (step 5) | Wartość % |

### Saved Views
- **Save as** — prompt na nazwę → tworzy snapshot wszystkich filtrów
- **Save** — nadpisuje aktywny view
- **Delete** — usuwa wybrany view
- **Reset** — resetuje do defaults (30d, All MP, etc.)
- Persist: localStorage via Zustand middleware

### UI Pattern
- Domyślnie: zwinięty (1-liniowy summary)
- Expand/Collapse button → pełna forma filtrów
- Podsumowanie inline: `"2026-02-05 - 2026-03-07 · All MP · All brands · FBA+FBM · Conf >= 0%"`

---

## 1. Dashboard

**Route:** `/dashboard`  
**Cel:** Executive overview — revenue, orders, margin, alerts, trend chart

### KPI Cards (4)

| # | Label | Pole | Ikona | Kolor |
|---|-------|------|-------|-------|
| 1 | Revenue | `kpi.total_revenue_pln` | DollarSign | amazon orange |
| 2 | Orders | `kpi.total_orders` | ShoppingCart | foreground |
| 3 | CM1 Margin | `kpi.total_cm_pln` + `kpi.total_cm_percent` | Percent | Dynamiczny (green/amber/red) |
| 4 | Active Alerts | `kpi.active_alerts_count` | Bell | red jeśli critical |

### Chart
- **Typ:** Recharts ComposedChart (Area + Line)
- **Osie:** Y-left: Revenue PLN + CM1 PLN (area), Y-right: CM% (line)
- **Warunek:** CM1 i CM% ukryte gdy COGS coverage < 80%

### Tabele

**Marketplace Breakdown (6 kolumn):**

| Kolumna | Pole | Align | Renderer |
|---------|------|-------|----------|
| Marketplace | marketplace_code | left | plain |
| Revenue | revenue_pln | right | formatPLN |
| Orders | orders | right | localeString |
| CM1 | cm_pln | right | formatPLN |
| CM% | cm_percent | right | formatPct (color) |
| ACoS | acos | right | formatPct / "—" |

**Top Drivers + Top Leaks (mini-tabele, 6 kolumn):**

| Kolumna | Pole | Renderer |
|---------|------|----------|
| # | index | auto |
| SKU / ASIN | sku, asin, title, internal_sku | truncate + mono |
| Units | units | number |
| Revenue | revenue_pln | formatPLN |
| CM1 | cm_pln | formatPLN |
| CM% | cm_percent | color-coded (green ≥20%, amber 10-20%, red <10%) |

### Filtry lokalne
- Date presets: 10 przycisków (7d, 30d, 90d, curr month, prev month, curr/prev quarter, 2q, YTD, prev year, custom)
- Marketplace dropdown
- Fulfillment toggle (Razem / FBA / FBM)
- Brand / Category text inputs (server-side deferred)

### Alerts
- COGS Coverage banner (amber, gdy coverage < 80%)
- Recent Alerts panel (lista alertów z severity badge)

### Empty / Error / Loading
- Loading: 4 skeleton cards
- No drivers/leaks: "No data"

### Export: brak  
### Pagination: brak (dashboard aggregates)  
### Decision patterns: CM% color-coding, loss driver icons, coverage indicator

---

## 2. Profit Explorer

**Route:** `/profit`  
**Cel:** Order-level CM1 explorer z expandowalnym detail

### Tabela zamówień (11 kolumn)

| Kolumna | Pole | Sortable | Renderer |
|---------|------|----------|----------|
| ▶ Expand | lines presence | — | chevron |
| Order ID | amazon_order_id | — | mono, select-all |
| Date | purchase_date | — | dd.MM.yy |
| Mkt | marketplace_code | — | badge |
| Fulf. | fulfillment_channel | — | badge (AFN=blue, MFN=orange) |
| Status | status | — | badge (Shipped=green, Unshipped=amber) |
| Revenue | revenue_pln | — | formatPLN |
| COGS | cogs_pln | — | formatPLN |
| Fees | amazon_fees_pln | — | formatPLN |
| CM | contribution_margin_pln | — | formatPLN (green/red) |
| CM % | cm_percent | — | formatPct (3 kolory) |

### Expanded Row: OrderLineCard
- Per-line detail: title, SKU, ASIN, internal_sku, quantity, price, cost/unit, COGS, fees

### Filtry lokalne
- Date presets: Dziś, Wczoraj, 7d, 30d, 90d, Custom
- Fulfillment toggle (Razem / FBA / FBM)
- Marketplace dropdown (flagi)
- SKU filter input

### Row Actions: Expand/Collapse  
### Export: **CSV** (przycisk)  
### Pagination: **Server-side**, 50/page, Prev/Next + "Strona X z Y"  
### Empty: "Brak zamówień w wybranym zakresie"

---

## 3. Product Profit Table

**Route:** `/profit/products`  
**Cel:** ASIN-first profitability z parent rollups, CM1/CM2/NP, what-if, confidence filtering

### Tabela główna (33 konfigurowalne kolumny)

| Kolumna | Pole | Sortable | Renderer |
|---------|------|----------|----------|
| SKU | sku | ✓ | mono |
| ASIN | asin | — | plain |
| Tytuł | title | — | truncate |
| Marketplace | marketplace_code | — | plain |
| Fulfillment | fulfillment_channel | — | plain |
| Units | units | ✓ | number |
| Revenue | revenue_pln | ✓ | formatPLN |
| COGS/unit | cogs_per_unit | — | formatPLN |
| Fees/unit | fees_per_unit | — | formatPLN |
| CM1 Profit | cm1_profit | ✓ | formatPLN (color badge) |
| CM1 % | cm1_percent | — | formatPct |
| Ads Cost | ads_cost_pln | ✓ | formatPLN |
| Logistics | logistics_pln | ✓ | formatPLN |
| Returns Net | returns_net_pln | ✓ | formatPLN |
| FBA Storage | fba_storage_fee_pln | ✓ | formatPLN |
| FBA Aged | fba_aged_fee_pln | ✓ | formatPLN |
| FBA Removal | fba_removal_fee_pln | ✓ | formatPLN |
| FBA Liquidation | fba_liquidation_fee_pln | ✓ | formatPLN |
| Overhead | overhead_allocated_pln | ✓ | formatPLN |
| CM2 Profit | cm2_profit | ✓ | formatPLN (color badge) |
| CM2 % | cm2_percent | — | formatPct |
| NP Profit | np_profit | ✓ | formatPLN (color badge) |
| NP % | np_percent | — | formatPct |
| Loss Orders % | loss_orders_pct | ✓ | formatPct |
| Return Rate | return_rate | ✓ | formatPct |
| TACoS | tacos | ✓ | formatPct |
| Days Cover | days_of_cover | ✓ | number |
| COGS Coverage | cogs_coverage_pct | — | formatPct |
| Shipping Match | shipping_match_pct | — | formatPct |
| Finance Match | finance_match_pct | — | formatPct |
| Confidence | confidence_score | ✓ | badge (color: <50 red, 50-80 yellow, ≥80 green) |
| Flags | flags | — | string array |
| Actions | — | — | MoreHorizontal menu |

### Column Chooser
- Grid of checkboxes dla wszystkich 33 kolumn (Settings2 button)

### Filtry lokalne
- Tab: **Realized / What-if** (button group)
- Group By: 6 opcji (ASIN+MP, ASIN global, Parent+MP, Parent global, SKU+MP, SKU global)
- Search: "Szukaj SKU / ASIN / tytuł (server)..." (deferred)
- Marketplace override dropdown

### Quick Filters (chips)
| Chip | Kolor aktywny | Tryb |
|------|---------------|------|
| Loss-making only | red | oba |
| Low confidence | yellow | oba |
| High returns | yellow | Realized only |
| Stockout risk | orange | Realized only |
| Ads heavy | purple | Realized only |

### What-If Mode
- Scenario qty input (1–200)
- Checkbox: Include ShippingCharge
- 6 KPI cards (profit, CM2, NP, Revenue, Costs, Coverage, Drift)

### Row Actions
- MoreHorizontal menu: Create task (pricing, content, watchlist)
- Parent expand → async child loading

### Export: **XLSX** (z widocznymi kolumnami, Realized only)  
### Pagination: **Server-side**, 50/page, jump-to-page input  
### Empty: "Brak danych"  
### Decision patterns: profit badges, confidence gate ≥70%, loss-only filter, parent rollups

---

## 4. Product Drilldown

**Route:** `/profit/drilldown?sku=...&marketplace_id=...&days=...`  
**Cel:** Order-line CM1 waterfall per single SKU

### KPI Cards (7)
Revenue, COGS, Fees, Logistics, CM1 Profit (color), CM1 %, Units

### Tabela order lines (15 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Order ID | amazon_order_id | mono |
| Date | purchase_date | yyyy-MM-dd |
| Mkt | marketplace_code | badge |
| Qty | qty | number |
| Price | item_price | currency |
| FX | fx_rate | toFixed(4) |
| Revenue PLN | revenue_pln | formatPLN |
| COGS | cogs_pln | formatPLN |
| FBA Fee | fba_fee_pln | formatPLN |
| Referral | referral_fee_pln | formatPLN |
| Logistics | logistics_pln | formatPLN |
| CM1 | cm1_profit | formatPLN (green/red + icon) |
| CM1 % | cm1_percent | formatPct (3 kolory) |
| Waterfall | visual bar | WaterfallBar (stacked: COGS, FBA, Referral, Logistics, CM1) |
| Source | cost_source | CostBadge (Actual/Partial/Missing) |

### Filtry: Period (7d / 30d / 90d / 1Y)  
### Pagination: **Server-side**, 50/page  
### Empty: "No order lines found"

---

## 5. Loss Orders

**Route:** `/profit/loss-orders`  
**Cel:** Fokus na zamówieniach z CM1 < 0 + root cause (loss driver)

### KPI Cards (3)
Total Loss (red), Loss Lines, Avg Loss / Line (red)

### Tabela (13 kolumn)

| Kolumna | Pole | Width | Renderer |
|---------|------|-------|----------|
| Order ID | amazon_order_id | 155px | mono |
| Date | purchase_date | 82px | date |
| Mkt | marketplace_code | 36px | badge |
| SKU | sku | 130px | mono, truncate |
| Product | product_title | flex | truncate |
| Qty | qty | 32px | number |
| Revenue | revenue_pln | 62px | formatPLN |
| COGS | cogs_pln | 58px | formatPLN |
| Fees | amazon_fees_pln | 64px | formatPLN |
| Logistics | logistics_pln | 50px | formatPLN |
| CM1 | cm1_profit | 68px | formatPLN (red + icon) |
| CM1 % | cm1_percent | 56px | formatPct (red) |
| Driver | primary_loss_driver | 120px | DriverBadge |

### Loss Driver Types
- Vine (green), Sell price too low (orange), Logistics too high (blue), Fees anomaly (purple), Missing cost data (gray), Combined costs (red)

### Pagination: **Server-side**, 50/page  
### Empty: "No loss orders found" (green — brak strat!)

---

## 6. Data Quality

**Route:** `/profit/data-quality`  
**Cel:** Coverage dashboard + missing COGS resolution + AI product matching

### Coverage Gauges (4 + dodatkowe)

| Gauge | Pole | Progi |
|-------|------|-------|
| COGS Coverage | cogs_coverage_pct | ≥90% green, ≥70% yellow, <70% red |
| FBA Fee Coverage | fba_fee_coverage_pct | jw. |
| Referral Fee Coverage | referral_fee_coverage_pct | jw. |
| Product Mapping | product_mapping_pct | jw. |
| Purchase Price | purchase_price_coverage_pct | % |
| Finance Match | finance_match_pct | % |
| FX Rate Coverage | fx_rate_coverage | ratio "12/13" |
| Overall Confidence | computed | HIGH/MEDIUM/LOW badge |

### Coverage by Marketplace (5 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Marketplace | marketplace_code | badge |
| Lines | total_lines | localeString |
| COGS % | cogs_coverage_pct | formatPct (color) |
| Fees % | fees_coverage_pct | formatPct (color) |
| Status | computed | shield icon (3 kolory) |

### Missing COGS — Top SKUs (9 kolumn, **z edycją inline**)

| Kolumna | Pole | Editable | Renderer |
|---------|------|----------|----------|
| SKU | sku | — | mono |
| ASIN | asin | — | plain |
| Internal SKU | internal_sku | ✓ (input, jeśli unmapped) | mono / input |
| Suggestion | hard_suggestion / ai_candidate | — | badge panel |
| Units | units | — | localeString |
| Revenue (orig) | revenue_orig | — | number |
| Lines | line_count | — | localeString |
| Cena zakupu (PLN netto) | current_price_pln | ✓ (input) | input |
| Save | — | ✓ (button) | Save/Loader/Check icon |

### AI Product Matcher (expandable rows)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| ▶ Expand | — | chevron |
| Amazon Product | unmapped_sku + title | bold SKU + subtext |
| → Arrow | — | symbol |
| Matched ISK | matched_internal_sku + title | ISK bold + subtext |
| Confidence | confidence | badge (red/yellow/green) |
| Qty in Bundle | quantity_in_bundle | "Nx" |
| Total Price | total_price_pln | tabular-nums + PLN |
| Actions | approve / reject | ThumbsUp / ThumbsDown |

**Expanded:** AI Reasoning, BOM (Bill of Materials), unit_price, created_at

### Filtry
- Globalne filtry (date, MP, brand, category)
- Toggle: "Show only rows with suggestion"

### Batch Actions: **Run AI Matching** button  
### Empty States: "Brak sugestii do weryfikacji..." / "Brak pozycji z gotową sugestią..."

---

## 7. Product Tasks

**Route:** `/profit/tasks`  
**Cel:** Task queue dla pricing/content/watchlist z owner assignment i komentarzami

### Filtry (top)
- Status: all / open / investigating / resolved
- Task type: all / pricing / content / watchlist
- Owner: text input
- SKU search: text input

### Layout: 2-kolumnowy (tabela | detail panel)

### Tabela (6 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Type | task_type | text |
| SKU | sku | mono |
| Marketplace | marketplace_id | "ALL" if null |
| Status | status | pill (red open, yellow investigating, green resolved) |
| Owner | owner | text / "-" |
| Created | created_at | YYYY-MM-DD |

### Detail Panel (right)
- Status dropdown (zmiana → mutation)
- Owner input + Save
- Comments section (scrollable, add new comment textarea)

### Auto Owner Rules Table
- Inline form: owner, priority, task_type, marketplace_id, brand
- Rules table z Delete per row

### Pagination: **Server-side**, 30/page  
### Empty: "Select task to manage status, owner and comments"

---

## 8. Pricing & Buy Box

**Route:** `/pricing`  
**Cel:** Monitoring cen, Buy Box win rate, gap detection

### KPI Cards (4)
Buy Box Win Rate (%), Buy Box Wins (X/Y), Active offers, Marketplaces count

### Chart: Buy Box Win Rate per MP (BarChart)
- X: marketplace_code, Y: %, Reference line: 50%

### Tabela ofert (7 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| SKU | sku | mono |
| ASIN | asin | mono |
| Marketplace | marketplace_code | badge |
| Cena | current_price | 2 decimals |
| Buy Box | buybox_price | 2 decimals / "—" |
| Status | has_buybox | badge (success/destructive) |
| Różnica | gap | color (red positive, green negative) |

### Filtr: SKU search (URL param)  
### Pagination: **Client-side**, 50/page  
### Empty: "Moduł pusty — uruchom sync_pricing w Jobs"

---

## 9. Planning & Budget

**Route:** `/planning`  
**Cel:** Budget planning, sales targets, monthly plan vs actual

### KPI Cards (3)
YTD Cel (PLN), YTD Wykonanie (PLN + attainment%), Miesiące z planem (X/12)

### Chart: Plan vs Actual per month (BarChart)
- 2 bary: Cel (grey), Wykonanie (orange)

### Tabela miesięczna (6 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Miesiąc | month_label | text |
| Status | status | badge |
| Cel Przychód | total_target_revenue_pln | formatPLN |
| Wykonanie | total_actual_revenue_pln | formatPLN / "—" |
| Realizacja | revenue_attainment_pct | % + Progress bar |
| Budżet Ads | total_target_budget_ads_pln | formatPLN |

### Marketplace Breakdown (6 kolumn, warunkowy panel po kliknięciu miesiąca)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Marketplace | marketplace_code | badge |
| Cel Przychód | target_revenue_pln | formatPLN |
| Cel Zamówień | target_orders | localeString |
| Target ACoS | target_acos_pct | % |
| Target CM% | target_cm_pct | % |
| Budżet Ads | budget_ads_pln | formatPLN |

### Filtr: Year selector (year-1, year, year+1)  
### Empty: "Brak planów na rok {year}"

---

## 10. Ads

**Route:** `/ads`  
**Cel:** Advertising spend, ACoS, ROAS, campaign performance

### KPI Cards (4)
Wydatki Ads (PLN), Sprzedaż z Ads (PLN + ROASx), Avg ACoS (% + CPC), Kliknięcia (count + CTR)

### Chart: Spend vs Sales Area Chart
- 2 areas: Sales (orange), Spend (red)
- X: report_date, Y: "Xk" (thousands)

### Tabela kampanii (8 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Kampania | campaign_name | truncate max-w-48 |
| Marketplace | marketplace_code | badge |
| Wydatki | total_spend_pln | formatPLN |
| Sprzedaż | total_sales_pln | formatPLN |
| ACoS | avg_acos | color-coded (<10 green, 10-20 orange, >20 red) |
| ROAS | avg_roas | "Xx" |
| Zamówienia | orders | localeString |
| Ocena | efficiency_score | color-coded bold |

### Filtry: Period chips (7d, 14d, 30d, 60d, 90d)  
### Empty: "Brak danych kampanii"  
### Loading: 8 skeleton rows

---

## 11. AI Recommendations

**Route:** `/ai`  
**Cel:** AI-generated recommendations (pricing, reorder, listing, budget, risk)

### KPI Cards (5)
Wszystkie (total), Nowe (amber), Zaakceptowane (green), Odrzucone, Potencjał (PLN, orange)

### Card Grid (nie tabela — każda rekomendacja to Card)

**Per Card:**
- Type badge + Status badge + SKU tag
- Title + Summary
- Action items (bullet list, jeśli są)
- Footer: Confidence % (color-coded), Expected impact PLN, Model used, Created at
- Buttons (jeśli status=new): Accept (green) / Dismiss (grey)

### Filtry
- Type dropdown (pricing, reorder, listing_optimization, ad_budget, risk_flag)
- Status dropdown (all, new, accepted, dismissed)

### Actions: **Generate** button (per type, Sparkles icon)  
### Empty: Brain icon + "Brak rekomendacji" + "Użyj 'Generuj'"

---

## 12. Alerts & Rules

**Route:** `/alerts`  
**Cel:** Monitoring alerts + automatic rule management

### 2 Taby: Alerty | Reguły

### Tab: Alerty

**Queue Health Badges (6 metryki):**
queued_total, queued_stale_30m, running_total, retry_in_progress, failed_last_24h, max_retry_reached_last_24h

**Alert List (Card per alert):**
- Severity badge (critical/warning/info)
- Title (prettified PL)
- Detail text (prettified PL)
- Collapsible context: product metrics, shipment details, top lines/SKUs
- Actions: Navigate to context, Mark read, Resolve

### Tab: Reguły

**Form:** name, rule_type, severity, operator, threshold, sku, opis  
**Rule Types:** margin_below, cogs_missing, stock_low, price_change, acos_above, buybox_lost

**Rules Table (7 kolumn):**
Nazwa, Typ (badge), Ważność (color badge), Próg (operator+value), SKU (mono/"Wszystkie"), Status (Aktywna/Wyłączona pill), Akcje (Delete)

### Empty: "Brak aktywnych alertów" / "Brak skonfigurowanych reguł"

---

## 13. Jobs

**Route:** `/jobs`  
**Cel:** Background job queue + manual trigger

### Job Trigger
- Select: 12 job types (sync_orders, sync_finances, sync_inventory, sync_pricing, sync_offer_fee_estimates, sync_tkl_cache, sync_fba_inventory, sync_fba_inbound, run_fba_alerts, recompute_fba_replenishment, calc_profit, generate_ai_report)
- Run Job button + Refresh button

### Health Card: Netfox sessions count + status

### Tabela jobs (6 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Type | job_type | text |
| Status | status | pill (pending/running/success/failure/revoked) |
| Progress | progress_pct | progress bar + % (jeśli running) |
| Records | records_processed | localeString / "—" |
| Duration | duration_seconds | "Xs" / "—" |
| Started | created_at | dd.MM HH:mm |

### Auto-refresh: 15s  
### Pagination: brak (lista jobów)

---

## 14. Import Products

**Route:** `/import-products`  
**Cel:** Excel import (CEO holding data) + Amazon 30d metryki

### Upload
- Drag & drop XLSX/XLS
- Result: inserts count, updates count + success/error msg

### Summary Cards (2 × 6)
- **Holding (Excel):** Produkty, Stan mag., Wartość mag., Śr. marża%, Sprz. 30d, Aktywne
- **Amazon (30d, orange accent):** Szt. sprzedanych, Przychód, COGS, Opłaty AMZ, Margin, Produkty ze sprz.

### Filtry (sticky)
- Search: SKU / nazwa
- Kod importu (dropdown)
- Aktywny (Tak/Nie)
- Sprz. Amazon (Ze sprzedażą/Bez)
- Sortuj po (11 opcji)
- Kierunek (Rosnąco/Malejąco)
- Clear filters

### Tabela (16 kolumn, 2 group headers: Holding + Amazon)

| Kolumna | Pole | Group | Sortable | Renderer |
|---------|------|-------|----------|----------|
| SKU | sku | Produkt | ✓ | mono, sticky left |
| Nazwa | nazwa_pelna | Produkt | ✓ | truncate max-w-200 |
| Kod imp. | kod_importu | Produkt | — | mono blue |
| Akt. | aktywny | Produkt | — | CheckCircle/XCircle |
| Zasięg | zasieg_dni | Produkt | ✓ | color (<14 red, 14-30 yellow, >30 green) |
| Stan | stan_magazynowy | Holding | ✓ | localeString |
| Cena zak. | cena_zakupu | Holding | ✓ | formatPLN |
| Marża | marza | Holding | ✓ | color (≥20 green, 0-20 yellow, <0 red) |
| Sprz.30d | sprzedaz_30d | Holding | — | localeString |
| Szt. | amz_units_30d | Amazon | ✓ | orange accent |
| Przychód | amz_revenue_pln_30d | Amazon | ✓ | formatPLN |
| COGS | amz_cogs_pln_30d | Amazon | ✓ | formatPLN |
| Margin | amz_cm1_pln_30d | Amazon | ✓ | color (green/red) bold |
| Margin % | amz_cm1_pct_30d | Amazon | ✓ | color band |
| Śr. cena | amz_avg_price_pln | Amazon | ✓ | formatPLN |
| COGS cov. | amz_cogs_coverage_pct | Amazon | ✓ | color band |

### Pagination: **Server-side**, 50/page, full nav (1, ..., current, ..., last)  
### Empty: "Brak produktów — uploaduj plik Excel" / "Brak wyników dla tych filtrów"

---

## 15. FBA Overview

**Route:** `/fba/overview`  
**Cel:** Daily FBA operations radar — stockout risks, inbound delays, feed diagnostics

### KPI Cards (4)
OOS Items, Inbound Units, Aged Inventory, Scorecard metric

### Tabele

**Top Stockout Risks (5 kolumn, limit 20):**

| Kolumna | Pole | Sortable | Renderer |
|---------|------|----------|----------|
| Produkt | sku (title_preferred) | ✓ | title + mono SKU |
| MP | marketplace_codes[] | — | comma-joined badges |
| On hand | on_hand | ✓ | number |
| Inbound | inbound | ✓ | number |
| Worst days cover | worst_days_cover | ✓ | number |

**Inbound Delays (5 kolumn):**

| Kolumna | Pole | Sortable | Renderer |
|---------|------|----------|----------|
| Shipment | shipment_id + name | — | mono ID + subtext |
| Status | status | — | text |
| Planned | units_planned | ✓ | number |
| Received | units_received | ✓ | number |
| Days | days_in_status | ✓ | number |

**MP Feed Diagnostics (4 kolumn):**
MP, Źródło inventory (badge), Źródło stranded (badge), Uwagi (computed)

### Alert: Data completeness warning (amber, jeśli fallback/proxy data)  
### Pagination: Client-side  
### Empty: "Brak aktywnych ryzyk stockoutu" / "Brak opóźnionych shipmentów"

---

## 16. FBA Inventory

**Route:** `/fba/inventory`  
**Cel:** Product-level FBA grouped by SKU z marketplace expansion

### Filtry
- SKU search
- Risk type (All / Stockout / Overstock)

### Tabela (expandable, 8 kolumn group + 8 detail)

**Group row:**

| Kolumna | Pole | Sortable | Renderer |
|---------|------|----------|----------|
| Produkt | sku (expandable) | ✓ | title + mono + brand/cat + ISK/EAN/parent |
| MP | marketplace_codes[] | — | comma-joined |
| On hand | on_hand (sum) | ✓ | number |
| Inbound | inbound (sum) | ✓ | number |
| Reserved | reserved (sum) | ✓ | number |
| Vel 30d | velocity_30d (sum) | ✓ | toFixed(2) |
| Worst days cover | worst_days_cover | ✓ | number |
| Risk | risk | — | badge (critical/warning/ok) |

**Detail row (per MP):** Marketplace breakdown z tym samym layout  
### Sort default: worst_days_cover ASC → velocity_30d DESC  
### Pagination: Client-side  
### Empty: "Brak rekordov inventory dla biezacych filtrow."

---

## 17. FBA Replenishment

**Route:** `/fba/replenishment`  
**Cel:** Replenishment planner z urgency prioritization

### Action: **Recompute** button (top-right)

### Tabela (expandable, 6 kolumn)

| Kolumna | Pole | Sortable | Renderer |
|---------|------|----------|----------|
| Produkt | sku (expandable) | ✓ | title + mono |
| MP | marketplace_codes[] | — | comma-joined |
| Worst days cover | current_days_cover | ✓ | number |
| Suggested qty | suggested_qty (sum) | ✓ | number |
| Earliest ship week | suggested_ship_week | ✓ | date |
| Urgency | urgency | — | badge (critical/high/secondary) |

### Sort: worst_days_cover ASC, suggested_qty DESC  
### Pagination: Client-side  
### Empty: "Brak sugestii replenishment"

---

## 18. FBA Inbound

**Route:** `/fba/inbound`  
**Cel:** Live shipments + shipment plan register

### Header Action: Import Plans CSV

### 4-panel layout

**Live Inbound Shipments (tabela, 6 kolumn):**
Shipment (mono ID + name), Status, Warehouse, Planned, Received, Days in status

**Shipment Detail (panel prawy):**
- Per-item: Produkt, SKU, ASIN, Planned, Received, Variance

**Shipment Plan Register (tabela, 6 kolumn):**
Shipment, Week, Status (select dropdown), Planned, Actual, Owner

**Plan Detail Form (panel prawy):**
- Edycja: owner, actual_units, actual_ship_date, status (select) + Delete

### Pagination: Client-side

---

## 19. FBA Aged & Stranded

**Route:** `/fba/aged-stranded`  
**Cel:** Aged/stranded inventory snapshot + case register

### Header Action: Import Cases CSV  
### Alert: Stranded proxy warning (amber)

### 2 Tabele side-by-side

**Aged 90+ (4 kolumn):** Produkt, MP, Units, Value (formatPLN)  
**Stranded (4 kolumn):** Produkt, MP, Units, Value (formatPLN)

### Case Register (2-panel: tabela + detail)

**Case Table (5 kolumn):**
Type, SKU (mono), Status (select), Detected (date), Owner

**Case Detail Panel:**
- Status select, Owner input, Close date, Root cause textarea
- Timeline z events + comments (CRUD: add/edit/delete comment)
- Delete case button

### Pagination: Client-side  
### Empty: "Brak aged 90+" / "Brak stranded" / "No cases yet"

---

## 20. FBA Bundles & Launches

**Route:** `/fba/bundles`  
**Cel:** Launch register + quarterly initiatives

### Header Actions: Import Launches CSV, Import Initiatives CSV

### 2-panel layout

**Launch Register (tabela + form):**
Create: quarter, marketplace_id, launch_type, sku, planned_go_live_date, owner  
Table: SKU, Status, Plan date, Owner  
Detail: status select, actual_go_live_date, vine dates, checkboxes + Delete

**Quarterly Initiatives (tabela + form):**
Create: quarter, initiative_type, sku, title, owner  
Table: Title, Status, Owner  
Detail: status select, live_stable_at, planned/approved checkboxes + Delete

### Pagination: Client-side

---

## 21. FBA Scorecard

**Route:** `/fba/kpi-scorecard`  
**Cel:** Quarterly KPI scorecard (9 components, weighted 0.00–1.20)

### Quick Action Bar (6 buttons)
Sync Inventory Snapshot, Sync Reconciliation, Import Plans CSV, Import Cases CSV, Import Launches CSV, Import Initiatives CSV

### Score Display
- Score_Q (text-4xl bold): `score.toFixed(3)`
- Vs Target: `score_pct_of_target.toFixed(1)%`
- Data ready badge

### 9 Component Cards (3×3 grid)
Each: Label, Actual value, Data ready badge, Factor, Weight%, Target, Good threshold, Contribution, Note

### Missing Inputs Section (lista braków)

---

## 22. Finance Dashboard

**Route:** `/finance/dashboard`  
**Cel:** Finance feed overview, payout reconciliation, sync diagnostics

### Header Actions (4)
Sync finances, Prepare settlements, Generate ledger, Reconcile job

### KPI Cards (5)
Revenue base (PLN), Fees base (PLN), VAT base (PLN), Profit proxy (PLN), Unmatched payouts (count)

### Alerts
- Data completeness warning
- Order sync gap risk (per MP beyond threshold)
- Order revenue integrity (missing revenue/totals, shipped/unshipped anomalies)

### Recent Finance Jobs (lista)
job_type, status, progress_message

### Payout Reconciliation Status (lista, top 8)
financial_event_group_id (mono), status, marketplace, currency + total_amount  
Action: **Auto-match** button

### Finance Sync Diagnostics (pełna sekcja)

**Marketplace Completeness 30d (10 kolumn):**
MP + GapBadge, Status, Day coverage %, Order coverage %, Imported rows, Tracked groups, Missing order rows, Unmapped rows, Gap driver, Missing cause

**Largest/Most Expensive Open Groups (2×5):**
financial_event_group_id, rows/age/score

**Full Sync Diagnostics Items:**
financial_event_group_id + sync_state, marketplace, processing/fund status, rows, age, score, last synced, event types

---

## 23. Finance Ledger

**Route:** `/finance/ledger`  
**Cel:** Canonical ledger z Amazon fees, charges, tax

### Header: SKU filter + Manual entry button

### Tabela (8 kolumn)

| Kolumna | Pole | Sortable | Renderer |
|---------|------|----------|----------|
| Data | entry_date | — | text |
| Opis | description / charge_type | — | truncate + subtext |
| MP | marketplace_code | — | text / "-" |
| Konto | account_code | — | text |
| Grupa płatności | financial_event_group_id | — | mono / "-" |
| SKU | sku | — | mono / "-" |
| Amount base | amount_base | ✓ | formatPLN |
| Akcje | — | — | Reverse button |

### Pagination: **Server-side** (200 rows per load)  
### Empty: "Brak rekordów ledgera"

---

## 24. Finance Reconciliation

**Route:** `/finance/reconciliation`  
**Cel:** Payout vs bank reconciliation

### Header Action: **Auto-match** button

### Tabela (6 kolumn)

| Kolumna | Pole | Sortable | Renderer |
|---------|------|----------|----------|
| Grupa płatności | financial_event_group_id | — | mono + settlement_id subtext |
| MP | marketplace_code | — | text / "-" |
| Status | status | ✓ | text |
| Expected | total_amount_base | ✓ | formatPLN |
| Matched | matched_amount | ✓ | formatPLN |
| Diff | diff_amount | ✓ | formatPLN |

### Pagination: Client-side  
### Empty: "Brak grup płatności do uzgodnienia"

---

## 25. Content Studio

**Route:** `/content/studio`  
**Cel:** Complete CMS — onboarding, editing, publishing, tasks

### 4 Taby (URL param `?tab=`)

#### Tab 1: Przegląd (overview)

**KPI Cards (5):** Zadania łącznie, P0, Otwarte, W trakcie, Zaległe  
**Quick Start Guide:** 4 kroki info box  
**System Health (3×3):** queued_total, stale_30m, failed_24h, compliance critical, retry_in_progress, overdue  
**Data Quality (grid):** key/value/unit cards  
**Release Calendar (14d):** Date | Tasks | P0 | SKU

#### Tab 2: Zadania

**Quick add:** SKU input + Add  
**Filtry:** Status select, SKU search  
**Bulk:** Status select + "Change status (X)" button

**Tabela (7 kolumn):**
Sel (checkbox), Typ, SKU (mono), Status (badge), Priorytet, Owner, Zmieniono

#### Tab 3: Edytor

**SKU+MP selector → Load versions**  
**Version list (scrollable):** New draft + version buttons  
**Content Form:** Title, Bullets (textarea 5 rows), Description, Keywords  
**Actions:** Save, Policy check, Submit for review, Approve  
**Diff & Sync:** Main→Target market sync, diff table (Pole | Zmiana | Źródło | Cel)

#### Tab 4: Onboarding

**Preflight:** Markets, Auto-create tasks, SKU list → Run  
Results: Total | Ready | Blocked + detail table  
**Catalog Search:** EAN + Market → ASIN results  
**Restrictions Check:** ASIN + Market → Can list Y/N + reasons  
**Quick Publish Push:** Selection, Mode, Markets, SKU filter → Run  
**Recent Publish Jobs:** Job table (ID, Typ, Status, Rynki, Kiedy)

---

## 26. Content Compliance

**Route:** `/content/compliance`  
**Cel:** Policy rules management + compliance checks

### Check Version: version_id → Run → passed/critical/major/minor  
### Policy Rules: Form (name, pattern/regex, severity) + Rules table  
### Compliance Failures Queue: Severity filter, bulk "Create Fix Tasks"

**Queue Table (7 kolumn):**
Sel (checkbox), Version, SKU, MP, Severity (c:X m:Y n:Z), Checked date, Flow (Editor | Publish links)

---

## 27. Content Assets

**Route:** `/content/assets`  
**Cel:** Asset library — upload, link to SKU

### Upload: File input + Upload button  
### Link: asset_id, SKU, role (7 roles) + Link button  
### Assets Table: ID (mono), Filename, Mime, Status, Uploaded

---

## 28. Content Publish

**Route:** `/content/publish`  
**Cel:** Package push + product type mappings + attribute mappings

### Quick Push: Markets, Selection, Mode, SKU filter, Idempotency key → Create/Push  
### Product Type Mappings: PTD refresh + mapping form + table  
### Attribute Mapping Registry: Form + rules count  
### Coverage by Category: Market, Category, ProductType, Coverage %, Missing attrs  
### Mapping Suggestions: Min confidence slider, Dry run/Apply, suggestions table  
### Jobs: Status filter, table (ID, Type, Status, Markets, Errors, Retry button, Created)

---

## 29. Inventory Overview

**Route:** `/inventory/overview`  
**Cel:** Decision dashboard — coverage, quick decisions, family changes

### Coverage Chips (4): label, pct, status, note (color-coded)  
### Primary Metrics (4-grid): label, value PLN/%, delta_pct WoW  
### Traffic Metrics (3-grid, conditional when coverage sufficient)  
### Data Quality Section (4-grid): label, pct, Progress bar, note

### QuickDecisionTable (2 instancje: high demand + CVR crash)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Produkt | title_preferred / sku | truncate + mono subtext |
| MP | marketplace_code | text |
| Cover | days_cover | number |
| Sessions 7d | sessions_7d | number / "-" |
| Decyzja | demand_vs_supply_badge | badge (variant by coverage flag) |

Row click → navigate `/inventory/all?search={sku}`

### FamilyChangesTable
MP, Parent ASIN (mono), Children, Coverage vs DE %, Status badge

Row click → navigate `/inventory/families?marketplace={code}&parent={asin}`

### Pagination: N/A (all inline)  
### Decision patterns: Action panel cards, coverage chips, decision badges, priority hints

---

## 30. Manage All Inventory

**Route:** `/inventory/all`  
**Cel:** Main operational inventory control desk — SKU-level with decision scoring

### KPI Cards (4)
P1 Replenishment (red), Suppressions (amber), CVR Crash (orange), Traffic coverage (% + bar)

### Filtry
- Search w-64 (SKU/ASIN/EAN multi-line)
- Marketplace w-52
- Risk type: all / stockout / overstock / stranded / aged
- Listing status: all / active / suppressed / inactive
- Reset filters button

### Quick Filter Chips
Priorytet: Stockout | Suppressed | Stranded

### Column Visibility Toggles (pills)
Brand/category, Family, Traffic 30d, Deltas, Stranded, Aged 90+ → "Reset kolumn"

### Tabela (expandable groups, ~12 kolumn)

**Parent row:**

| Kolumna | Pole | Renderer |
|---------|------|----------|
| SKU / ASIN | title_preferred, sku, asin, MPs | expandable chevron + title + mono + meta |
| Brand/Cat (opt) | brand, category, product_type | stacked text |
| Family (opt) | local_parent_asin, family_health | parent + health |
| FBA Avail | fba_available | number |
| Inbound | inbound | number |
| Days Cover | days_cover (min) | **red+bold if < 7** |
| Sessions 7d | sessions_7d | number / "-" |
| Sessions 30d (opt) | sessions_30d | number / "-" |
| CVR 7d | unit_session_pct_7d | % |
| Deltas (opt) | sessions_delta_pct + cvr_delta_pct | 2-line |
| Stranded (opt) | stranded_value_pln | formatPLN |
| Aged 90+ (opt) | aged_90_plus_value_pln | formatPLN |
| Decision | demand_vs_supply_badge + hint | badge + xs text |

**Child row (per MP):** Same kolumny, indented, + fulfilment badge + listing status badge

### Right Panel: Action Queue / SKU Detail

**Action Queue (no selection):**
- "Co robić teraz" — top 8 items ranked by score
- `actionScore()`: P1 replenishment=120, suppression=80, CVR crash=65, etc.

**SKU Detail (on selection):**
- Title + SKU + risk badges
- 4 inventory boxes: FBA on-hand, Available, Inbound, Reserved
- 4 traffic boxes: Sessions 7d, 30d, CVR 7d, 30d
- Family/listing info
- Issues list
- Recent changes (max 6, scrollable)

### Pagination: brak (grouped view, all items)  
### Decision patterns: `actionScore()`, `decisionHint()`, days cover < 7 red, demand vs supply badge

---

## 31. Inventory Families

**Route:** `/inventory/families`  
**Cel:** Local Amazon families vs DE canonical coverage

### Tabela (5 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Marketplace | marketplace_code | text |
| Parent ASIN | parent_asin | mono |
| Children | children_count | number |
| Coverage vs DE | coverage_vs_de_pct | % / "-" |
| Status | status | badge (ok=success, needs_review=warning, broken=destructive) |

### Filtr: Marketplace input w-48  
### Row click → detail panel (right)

### Detail Panel: Family Editor Preview
- ASIN mono + metadata (MP, theme, coverage%)
- Children section (up to 12): child_asin, SKU, key_type, variant_attributes
- Issues section (bullet list)

### Empty: "Brak rodzin dla bieżącego filtra" / "Wybierz parent ASIN z listy..."

---

## 32. Inventory Drafts

**Route:** `/inventory/drafts`  
**Cel:** Safe workflow shell: draft → validate → approve → apply → rollback

### Create Draft (left panel)
- draft_type select: reparent / create_parent / update_theme / detach
- marketplace_id, parent ASIN, SKU, payload JSON (textarea 10 rows)
- Create button

### Drafts Table (8 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Draft ID | id (slice 0,8 + "...") | mono |
| Type | draft_type | text |
| Marketplace | marketplace_code | text |
| Parent / SKU | affected_parent_asin / affected_sku | stacked xs |
| Validation | validation_status | badge |
| Approval | approval_status | badge |
| Apply | apply_status | badge |
| Created | created_at | datetime |

### Detail Panel (right, on selection)
- Draft ID + metadata
- 3 badges: validation / approval / apply status
- 4 Action buttons: Validate, Approve, Apply, Rollback
- Validation errors (bullet list)
- Payload JSON display (pre)

### Empty: "Brak draftów inventory"

---

## 33. Inventory Jobs

**Route:** `/inventory/jobs`  
**Cel:** Manual controls + observability for heavy inventory sync/rollup

### Job Cards (5 typów, 3-column grid)
Each: label, note, status badge, progress message, rows processed, started/finished at + Run button

### Recent Job History (expandable items)
- Job type + status badge
- Progress bar (h-2) + 4-col grid (%, rows, started, finished)
- Error message (jeśli jest)

### Auto-refresh: 20s  
### Empty: "Brak jobów inventory"

---

## 34. Inventory Settings

**Route:** `/inventory/settings`  
**Cel:** Guardrails, thresholds, schedule defaults

### 3 Cards

**Thresholds:** highSessions, highUnits, criticalCover, warningCover, overstockDays  
**Apply Safety:** autoPropose, safeAuto, savedViewsEnabled checkbox  
**Traffic Schedule:** nightlyHour + Save button

---

## 35. Family Mapper

**Route:** `/families`  
**Cel:** DE Canonical → EU variation family mapping

### Header Controls
- Brand filter, Max parents input (1–1000), Only new checkbox
- Buttons: Rebuild DE (z phase status), Sync (count), Match (count)

### Marketplace Selection (12 checkboxes)
PL, FR, IT, ES, NL, BE, SE, GB, IE, AE, SA, TR + Check/Uncheck all

### Quick Stats (4 cards)
Total Families, Fully Mapped (≥10), Partial (1-9), Unmapped (0)

### Tabela (10 kolumn)

| Kolumna | Pole | Sortable | Renderer |
|---------|------|----------|----------|
| ☐ Select | selectedFamilies | — | checkbox |
| DE Parent ASIN | de_parent_asin | — | mono |
| Brand | brand | — | text / "—" |
| Category | category | — | truncate |
| Product Type | product_type | — | xs text |
| Theme | variation_theme_de | — | xs text |
| Children | children_count | ✓ | center |
| DE Sales | de_sales_qty | ✓ | number / "—" |
| Marketplaces | marketplaces_mapped | ✓ | badge (green ≥10, yellow partial, red 0) |
| → | — | — | arrow icon |

### Batch Actions: Rebuild DE, Sync (selected), Match (selected)  
### Row click → navigate `/families/{id}`  
### Pagination: **Server-side**, 30/page  
### Polling: Rebuild status every 2s while running

---

## 36. Family Detail

**Route:** `/families/:id`  
**Cel:** Deep-dive family view + restructure pipeline per MP

### Header: Back + breadcrumb (ASIN, brand, category, theme, children count)

### Restructure Pipeline (primary section)

**Controls:** MP selector + Analyze + Analyze All

**Analysis Result:**
- VerdictBadge (aligned / needs_restructure / no_data)
- Stats grid (5): DE children, Found, Aligned (green), Misaligned (red), Missing (yellow)
- Parent ASINs (green DE / red foreign)
- Proposed Actions list

**Execute:** Dry Run / Execute buttons → confirm flow → real-time progress (3s poll)

**Execution Log:** Status badge, total steps, errors, parent SKU/ASIN, progress bars

### Collapsible Details

**Coverage overview (4-grid):** DE Children, Marketplaces X/12, Issues, Avg Confidence%

**DE Canonical Children Table (4 kolumn):**
ASIN (mono), SKU/"-", EAN/"-", Key Type (badge)

**Marketplace Coverage:** Progress bars + % + matched/total + theme_mismatch alert

**Child Market Links Table (7 kolumn):**

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Marketplace | marketplace | bold |
| Master Key | master_key | mono, truncate |
| Target ASIN | target_child_asin | mono / "-" |
| Match Type | match_type | badge |
| Confidence | confidence | color (≥90 green, ≥75 blue, ≥60 yellow, <60 red) |
| Status | status | color span |
| Actions | — | Approve/Reject buttons (jeśli proposed/needs_review) |

**Issues section:** Severity badge + type + marketplace + payload

---

## 37. Review Queue

**Route:** `/families/review`  
**Cel:** Pending proposed/needs_review child links awaiting human decision

### Filtry
- Status: All / Proposed / Needs Review
- Marketplace input

### Tabela (8 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| DE Parent | de_parent_asin | blue link → family detail |
| Brand | brand | text / "—" |
| Marketplace | marketplace | badge |
| DE Child | de_child_asin | mono / "-" |
| Target Child | target_child_asin | mono / "-" |
| Match | match_type | badge |
| Confidence | confidence | color-coded (4 progi) |
| Status | status | badge (needs_review=destructive, proposed=secondary) |
| Actions | — | Approve ✓ / Reject ✗ / View 👁 |

### Pagination: **Server-side**, 50/page  
### Empty: "No items pending review."

---

## 38. Fix Packages

**Route:** `/families/fix-packages`  
**Cel:** Actionable fix plans per family/MP

### Header: **Generate Packages** button  
### Filtr: Status dropdown (Draft / Pending Approve / Approved / Applied)

### Tabela (8 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| ID | `#id` | mono |
| Marketplace | marketplace | badge |
| Family ID | global_family_id | blue link |
| Steps | summary / steps.length | badges: delete/create/review counts |
| Status | status | badge (variant map) |
| Generated | generated_at | date / "-" |
| Approved By | approved_by | text / "-" |
| Actions | — | View 👁 + Approve ✓ (jeśli draft/pending) |

### Detail Modal (Dialog)
- Steps list: action badge (DELETE/CREATE, color) + type + reason + ASIN + master key

### Pagination: **Server-side**, 30/page  
### Empty: "No fix packages. Click 'Generate Packages'"

---

## 39. Netfox Health

**Route:** `/system/netfox-health`  
**Cel:** ACC read-only Netfox session monitoring

### KPI Cards (3)
Status (ok/error), Session count, Last refresh (datetime)

### Error display (conditional): amber border box

### Sessions Table (6 kolumn)

| Kolumna | Pole | Renderer |
|---------|------|----------|
| Session | session_id | mono |
| Login | login_name | text / "-" |
| Host | host_name | text / "-" |
| Status | status | text / "-" |
| Database | database_name | text / "-" |
| Last request | last_request_start_time | datetime / "-" |

### Auto-refresh: 15s  
### Empty: "No active ACC Netfox sessions."

---

## 40. Login

**Route:** `/login`  
**Cel:** Authentication

### Form
- Email input (required)
- Password input (required)
- Submit: "Sign in" / "Signing in..."

### Flow: login → store tokens → getMe → setUser → redirect `/dashboard`  
### Error: destructive box with `err.response?.data?.detail` / "Login failed"

---

## Cross-Cutting Analysis

### ✅ Co jest dobrze

| Pattern | Gdzie | Ocena |
|---------|-------|-------|
| Server-side pagination | Profit, Products, Loss, Tasks, Families, Review, Fix, Import | ✅ OK |
| KPI cards / hero metrics | Wszystkie dashboardy | ✅ |
| Color-coded badges | Universalny pattern | ✅ Spójny |
| Decision-first layout | Inventory Overview, Products, Loss, DataQuality | ✅ |
| Saved views | Global Filter Bar | ✅ |
| Confidence scoring | Products, DataQuality, AI Recs, Review Queue | ✅ |
| Auto-refresh | Jobs (15s), NetfoxHealth (15s), InventoryJobs (20s), Inventory (120s) | ✅ |

### ⚠️ Braki vs wymagania enterprise

| Brak | Priorytet | Dotyczy ekranów |
|------|-----------|-----------------|
| **Column chooser** brakuje | HIGH | Większość tabel (jest TYLKO na Products) |
| **Export CSV/XLSX** | HIGH | Brakuje na: Dashboard, LossOrders, DataQuality, Ads, Alerts, FBA*, Finance*, Content*, Inventory*, Families* |
| **Saved views per screen** | MED | Jest globalnie, brak per-screen (np. Inventory, Families) |
| **Detail drawer zamiast navigate** | MED | Inventory Overview → navigate, Products → navigate, FamilyMapper → navigate |
| **Batch operations** | MED | Brakuje na: FBA Inventory, Pricing, most Inventory, Finance |
| **Sticky filters** | MED | GlobalFilterBar sticky, ale lokalne filtry NIE sticky (Products, Ads, Import) |
| **Last sync / freshness** | MED | Jest na: Inventory Overview, NetfoxHealth. Brakuje na: Dashboard, Ads, Pricing, FBA, Finance |
| **Empty states z CTA** | LOW | Większość ma tekst, ale brakuje jasnych CTA (przycisków "Uruchom sync" etc.) |
| **Server-side pagination** | LOW | FBA Inventory/Replenishment, Pricing — client-side, przy dużych danych nie skaluje |
| **Error states (API fail)** | LOW | TanStack Query handles, ale brak custom error UI na większości stron |
| **Row density toggle** | LOW | Jest w global store (`rowDensity`), ale nie widoczny w UI |
| **Search debounce** | LOW | Products ma deferred, ale inne (Pricing, Import) — instant/onChange |

### 🎯 Decision-First Maturity

| Screen | Decision Pattern | Maturity |
|--------|-----------------|----------|
| **Products (Profit Table)** | Confidence gate, profit mode priority, what-if, parent rollups, loss filter | ★★★★★ |
| **Manage All Inventory** | Action scoring, decision hints, days cover highlight, priority chips | ★★★★★ |
| **Loss Orders** | Loss driver badges, focused metric, red scheme | ★★★★ |
| **Data Quality** | Coverage gauges, AI matching, inline COGS edit | ★★★★ |
| **Dashboard** | CM% bands, driver/leaks split, coverage banner | ★★★ |
| **FBA Overview** | Stockout risks, feed diagnostics alert | ★★★ |
| **AI Recommendations** | Confidence + impact, accept/dismiss | ★★★ |
| **Pricing** | Buy Box status, gap calculation | ★★ |
| **Ads, Planning, Alerts** | Charts + metrics, no inline action | ★★ |
| **Finance, Content** | Data dump — pokazy dane, brak decision hints | ★ |
| **FBA detail screens** | Operational forms, nie decision-first | ★ |
