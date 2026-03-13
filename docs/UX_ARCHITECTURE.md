# ACC — UX Architecture Specification

| Field | Value |
|-------|-------|
| **Date** | 2026-03-13 |
| **Agent** | ArchitectUX |
| **Status** | Phase 1 — Foundation |
| **Cross-refs** | [Brand Identity System](BRAND_IDENTITY_SYSTEM.md) · [UX Research Report](UX_RESEARCH_REPORT_2026-03-12.md) · [Tech Stack Assessment](TECH_STACK_ASSESSMENT_2026-03-12.md) |

---

## Table of Contents

1. [Information Architecture](#1-information-architecture)
2. [Layout Framework](#2-layout-framework)
3. [Component Architecture](#3-component-architecture)
4. [Theme System](#4-theme-system)
5. [Accessibility Foundation](#5-accessibility-foundation)
6. [UX Patterns Library](#6-ux-patterns-library)
7. [CSS File Import Guide](#7-css-file-import-guide)
8. [Implementation Priorities](#8-implementation-priorities)

---

## 1. Information Architecture

### 1.1 Sidebar Consolidation (12 → 7 Groups)

UX Research finding BI-03: the current 12-group sidebar exceeds Miller's Law (7±2 items). Redesigned navigation:

| # | Group | Contains | Rationale |
|---|-------|----------|-----------|
| 1 | **Dashboard** | Overview, Quick Margin widget | Single entry point → first insight <10s |
| 2 | **Profitability** | Product Profit Table (PPT), CM1/CM2/NP views, Margin Analysis | Core value prop consolidated |
| 3 | **Orders & Inventory** | Orders, FBA Inventory, Manage All Inventory, Purchase Prices | Operational supply chain |
| 4 | **Advertising** | Campaigns, Ad Spend→Profit, Keyword Analytics | All PPC in one group |
| 5 | **Analytics** | Brand Analytics, Market Intelligence, Courier Analysis | Data-driven insights |
| 6 | **Finance** | Finance Center, Currency, Invoices, FX Rates | Money flow cluster |
| 7 | **Settings** | Account, Integrations, Jobs, Scheduler, Module Visibility | System config |

**Hidden by default** (via module visibility toggle in Settings):
- Taxonomy (power-user only)
- Listing Registry (low usage per BI-01: ~37% pages unused)
- Content Ops Studio (future)

### 1.2 Page Hierarchy

**Primary pages** (linked from sidebar, always visible):
- Dashboard Overview
- Product Profit Table
- Orders
- Campaign Manager
- Finance Center

**Secondary pages** (within groups, one click deep):
- Margin Analysis, CM2 breakdown, NP breakdown
- FBA Inventory, Purchase Prices
- Ad Spend→Profit, Keyword Analytics
- Brand Analytics, Market Intelligence
- Currency Management, FX Rates

**Tertiary pages** (drill-down from data):
- Single Product Detail (from PPT row click)
- Single Order Detail (from Orders row click)
- Campaign Detail (from Campaign Manager row click)
- SKU-level Ad Attribution

### 1.3 Navigation Enhancements

| Feature | Priority | Sprint |
|---------|----------|--------|
| **Breadcrumbs** | High | Sprint 2 — reduces disorientation |
| **Recently Visited** (last 5 pages) | Medium | Sprint 2 — quick backtracking |
| **Global Search** (⌘K) | High | Sprint 3 — power-user shortcut |
| **Module Visibility Toggle** | Medium | Sprint 3 — hide unused modules |
| **Keyboard navigation** (↑↓ in sidebar) | Low | Sprint 4 |

### 1.4 URL Structure

```
/                          → Dashboard Overview
/profitability             → Product Profit Table (PPT)
/profitability/:sku        → Product Detail drill-down
/profitability/margins     → Margin Analysis
/orders                    → Orders list
/orders/:id                → Order Detail
/inventory                 → FBA Inventory
/inventory/purchase-prices → Purchase Prices
/ads                       → Campaign Manager
/ads/:campaignId           → Campaign Detail
/ads/attribution           → Ad Spend→Profit
/analytics/brand           → Brand Analytics
/analytics/market          → Market Intelligence
/finance                   → Finance Center
/finance/currency          → Currency Management
/settings                  → Account Settings
/settings/integrations     → API Integrations
/settings/modules          → Module Visibility
```

---

## 2. Layout Framework

### 2.1 App Shell Specification

```
┌──────────────────────────────────────────────────────┐
│ Sidebar (224px)  │  TopBar (56px height)             │
│                  ├───────────────────────────────────┤
│  Logo            │                                   │
│  Nav Group 1     │  Content Area                     │
│  Nav Group 2     │  ┌─────────────────────────────┐  │
│  Nav Group 3     │  │ Page Header + Actions       │  │
│  Nav Group 4     │  ├─────────────────────────────┤  │
│  Nav Group 5     │  │ Dashboard Grid / Table /    │  │
│  Nav Group 6     │  │ Detail Content              │  │
│  Nav Group 7     │  │                             │  │
│  ─────────────   │  │                             │  │
│  User / Settings │  └─────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

**CSS implementation**: `apps/web/src/css/layout.css` — `.app-shell`, `.app-sidebar`, `.app-topbar`, `.app-content`

| Element | Dimension | Behavior |
|---------|-----------|----------|
| Sidebar | 224px (w-56) | Sticky, scrollable, collapses to 64px |
| TopBar | 56px (h-14) | Sticky, contains search + status + user |
| Content | fluid, max 1400px | Scrollable, 24px padding |
| Sidebar collapsed | 64px | Icon-only, tooltip on hover |

### 2.2 Dashboard Grid System

Based on CSS Grid with auto-responsive behavior:

```
┌──────┬──────┬──────┬──────┐   4-col KPI row
│ KPI  │ KPI  │ KPI  │ KPI  │
└──────┴──────┴──────┴──────┘
┌───────────────┬────────────┐   2/3 + 1/3 split
│ Revenue Chart │ Top ASINs  │
│               │            │
└───────────────┴────────────┘
┌────────────────────────────┐   Full-width table
│ Product Profit Table       │
└────────────────────────────┘
```

**CSS classes**: `.dashboard-grid`, `.dashboard-grid--2col`, `--3col`, `--4col`, `.kpi-card`, `.section-card`

**Responsive behavior**:
- **≥1024px**: 4-col KPI grid, 2-col chart/table split
- **768–1023px**: 2-col KPI grid, stacked charts
- **<768px**: Single column, sidebar becomes overlay

### 2.3 Page Template Catalog

#### Template A: Dashboard
- KPI card row (4-col)
- Chart section (2-col or full-width)
- Quick Actions widget
- Recent Activity feed

#### Template B: Data Table (PPT, Orders, Campaigns)
- Page header with filters toolbar
- Column visibility toggle
- Horizontally scrollable table with sticky header/first-col
- Pagination bar
- Bulk action bar (conditional)

#### Template C: Detail / Drill-Down
- Breadcrumb → back navigation
- Summary card row at top
- Tab interface for sub-sections
- Related data sidebar (optional split pane)

#### Template D: Form / Settings
- Sectioned form layout
- Save/Cancel action bar (sticky bottom)
- Validation inline

### 2.4 Responsive Strategy

Desktop-first approach (ACC is a desktop SaaS tool), with deliberate tablet/mobile breakpoints:

| Breakpoint | Target | Sidebar | Grid |
|------------|--------|---------|------|
| ≥1280px | Large desktop | Full (224px) | 4-col KPI |
| 1024–1279px | Desktop | Collapsible | 3-col KPI |
| 768–1023px | Tablet | Overlay | 2-col KPI |
| <768px | Mobile | Hidden + hamburger | 1-col |

**Critical**: PPT table on mobile degrades to a card-list view (1 product per card) rather than a horizontal scroll table. This is Sprint 4 scope.

---

## 3. Component Architecture

### 3.1 Component Inventory

| Layer | Source | Purpose |
|-------|--------|---------|
| **Primitives** | shadcn/ui (Radix) | Button, Input, Dialog, Select, Tabs, DropdownMenu, Tooltip, Sheet, Popover, Command, Skeleton |
| **Data Display** | Custom + Recharts | KPI cards, Data tables, Charts, Sparklines |
| **Financial** | Custom | Metric values, Margin badges, P&L indicators, Currency formatting |
| **Navigation** | Custom + shadcn | Sidebar, Breadcrumbs, Command palette |
| **Feedback** | shadcn + Custom | Toast, Loading overlay, Empty state, Error boundary |

### 3.2 Financial Data Components

#### KPI Card
```
┌──────────────────────┐
│ REVENUE        ↗ +5% │  ← .metric-label + .metric-trend
│ €124,589.32          │  ← .metric-value .font-mono-tabular
│ vs €118,204 prev     │  ← comparison line
└──────────────────────┘
```

CSS classes: `.kpi-card`, `.metric-value`, `.metric-label`, `.metric-trend`, `.metric-positive`, `.metric-negative`

#### Margin Health Badge
| Range | Class | Color |
|-------|-------|-------|
| >15% | `.margin-badge--healthy` | Green (success) |
| 5–15% | `.margin-badge--warning` | Amber (warning) |
| <5% | `.margin-badge--critical` | Red (destructive) |

#### Currency Formatting Rules
- All monetary values use `.font-mono-tabular`
- Currency symbol prefix: `€`, `£`, `$`, `zł`
- Negative values: red text, parentheses notation `(€1,234.56)`
- Thousand separator: locale-aware via `Intl.NumberFormat`

### 3.3 Data Table Specification

The Product Profit Table (PPT) is the most complex and most-used component.

#### Progressive Disclosure (BI-05 fix)

**Default visible columns** (8–10 max, per UX Research):
1. Image + ASIN/SKU
2. Product Title (truncated)
3. Revenue
4. Units Sold
5. CM1 (Contribution Margin 1)
6. CM1 %
7. Margin Health badge
8. Trend (sparkline or arrow)

**Expandable column groups** (on demand):
- **Costs**: COGS, FBA Fees, Referral Fee, Shipping
- **Advertising**: Ad Spend, ACoS, TACoS
- **Advanced**: CM2, NP, FX Impact, Refunds

#### Table Features
| Feature | Implementation | Priority |
|---------|---------------|----------|
| Virtual scrolling | `@tanstack/react-virtual` | Sprint 1 (perf) |
| Sticky header | `.table-container--sticky-header` | Sprint 1 |
| Sticky first column | `.data-table--sticky-col` | Sprint 1 |
| Column sort | Server-side via API `?sort=cm1&dir=desc` | Sprint 1 |
| Column reorder | Drag handle (DnD Kit) | Sprint 3 |
| Column visibility toggle | Dropdown checklist | Sprint 2 |
| Row selection | Checkbox + bulk actions | Sprint 3 |
| Export | CSV + PDF | Sprint 3 |
| Pagination | Server-side, 50 rows default | Sprint 1 |
| Search/filter toolbar | Debounced text + marketplace filter | Sprint 1 |

### 3.4 Chart Component Guidance

| Data Question | Chart Type | Recharts Component |
|---------------|------------|-------------------|
| Revenue over time | Line/Area | `<AreaChart>` |
| Profit breakdown (COGS, Fees, etc.) | Stacked Bar | `<BarChart>` |
| Margin distribution | Histogram | `<BarChart>` |
| Revenue by marketplace | Donut | `<PieChart>` |
| Ad spend vs. revenue | Dual axis line | `<ComposedChart>` |
| Single metric trend | Sparkline | `<LineChart>` mini |

**Chart palette** — use `--chart-1` through `--chart-8` tokens for consistent coloring across all charts.

**Dark mode**: All charts must use transparent backgrounds with `hsl(var(--foreground))` for axes and labels.

### 3.5 Filter & Form Patterns

**Standard filter bar** (above data tables):
```
┌─────────────────────────────────────────────────────────────┐
│ 🔍 Search...  │ Marketplace ▼ │ Date range 📅 │ ⚙ Columns │
└─────────────────────────────────────────────────────────────┘
```

- Search: debounced (300ms), applies to ASIN, SKU, title
- Marketplace selector: multi-select (DE, PL, CZ, etc.)
- Date range: preset ranges (7d, 30d, 90d, custom) via shadcn DatePicker
- Column toggle: dropdown with checkboxes

---

## 4. Theme System

### 4.1 Token Architecture

```
index.css (shadcn base tokens)
    ↓ @layer base
design-system.css (extended tokens)
    ↓ @layer base + @layer components
tailwind.config.js (maps tokens → utility classes)
    ↓
Component TSX (uses Tailwind classes + CSS classes)
```

Token flow:
1. **`index.css`** defines base shadcn tokens (`:root` / `.dark`)
2. **`design-system.css`** adds semantic tokens (`--success`, `--warning`, spacing, typography, motion, chart palette)
3. **`tailwind.config.js`** maps `hsl(var(--xxx))` tokens to Tailwind color utilities
4. **Components** consume tokens via Tailwind classes (`text-primary`, `bg-card`) or CSS classes (`.metric-value`, `.glow-primary`)

### 4.2 Dark Mode (Primary)

Dark mode is the default and primary theme. The "Dark Clarity" philosophy means:
- Backgrounds are deep navy (`#080D16`, `#0C1320`, `#111827`)
- Data and metrics provide visual "light" through accent colors
- Borders are subtle slate (`#1E293B`)
- Text hierarchy: Ice White → Slate → Slate Dim

Dark mode is activated by the `.dark` class on `<html>`, managed by a theme provider (already in the app via shadcn convention).

### 4.3 Light Mode (Secondary)

Light mode tokens are already defined in `index.css` `:root`. The design-system tokens for `--success`, `--warning`, `--info` are currently identical in both modes (they work on both backgrounds). If contrast issues arise, override in `:root` (light) specifically.

### 4.4 Chart Theme Coordination

Recharts components must read CSS custom properties for consistent theming:

```tsx
// Example: read CSS var in JS for Recharts
const style = getComputedStyle(document.documentElement);
const chartColors = [
  `hsl(${style.getPropertyValue('--chart-1').trim()})`,
  `hsl(${style.getPropertyValue('--chart-2').trim()})`,
  // ...
];
```

Or use a `useChartTheme()` hook that resolves `--chart-1` through `--chart-8` from the DOM.

### 4.5 Print Theme

For PDF export (Sprint 3):
```css
@media print {
  body { background: white; color: black; }
  .app-sidebar, .app-topbar { display: none; }
  .app-content { padding: 0; }
  .data-table th { background: #f0f0f0; }
}
```

This will be added to `components.css` when export is implemented.

---

## 5. Accessibility Foundation

### 5.1 WCAG 2.1 AA Target

All new components must meet WCAG 2.1 Level AA. Key requirements:

| Criterion | Requirement | Implementation |
|-----------|-------------|----------------|
| 1.4.3 Contrast (Minimum) | 4.5:1 for text, 3:1 for large text | Verified against dark backgrounds |
| 1.4.11 Non-text Contrast | 3:1 for UI components | Border and icon contrast |
| 2.1.1 Keyboard | All functionality via keyboard | Tab order, Enter/Space activation |
| 2.4.7 Focus Visible | Visible focus indicator | `ring` token used for focus-visible |
| 1.3.1 Info & Relationships | Semantic HTML | `<table>`, `<nav>`, `<main>`, headings |

### 5.2 Color Contrast Verification

**Dark mode critical pairs** (verified):

| Foreground | Background | Ratio | Pass? |
|------------|------------|-------|-------|
| Ice White `#F8FAFC` | Deep Navy `#080D16` | 17.5:1 | ✅ AAA |
| Slate `#94A3B8` | Deep Navy `#080D16` | 6.7:1 | ✅ AA |
| Slate Dim `#64748B` | Deep Navy `#080D16` | 4.1:1 | ⚠️ AA for large text only |
| ACC Amber `#FF9900` | Deep Navy `#080D16` | 8.7:1 | ✅ AA |
| Profit Green `#22C55E` | Deep Navy `#080D16` | 8.6:1 | ✅ AA |
| Loss Red `#EF4444` | Deep Navy `#080D16` | 5.2:1 | ✅ AA |

**Action**: `--muted-foreground` (Slate Dim) should only be used for non-essential labels, never for actionable text.

### 5.3 Focus Management

- Use shadcn's built-in focus ring (`ring` token)
- Dialogs trap focus (Radix handles this)
- Page navigation moves focus to `<main>` heading
- Skip-to-content link at top of page (hidden until focused)

### 5.4 Keyboard Navigation

| Context | Keys | Action |
|---------|------|--------|
| Sidebar | ↑↓ | Navigate items |
| Sidebar | Enter | Activate item |
| Sidebar | ← | Collapse group |
| Data table | ↑↓ | Navigate rows |
| Data table | Enter | Open detail |
| Command palette | ⌘K | Open search |
| Dialogs | Esc | Close |
| Tabs | ←→ | Switch tabs |

### 5.5 Reduced Motion

`design-system.css` includes a `prefers-reduced-motion: reduce` media query that disables all animations and transitions globally. Components should respect this:

```tsx
// In React, check preference:
const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
```

### 5.6 Screen Reader Support

- All images: `alt` text or `aria-hidden` if decorative
- Icon-only buttons: `aria-label`
- Status badges: include text, not just color
- Data table: proper `<thead>`, `<tbody>`, `scope="col"`
- KPI trend: `aria-label="Revenue increased by 5.3%"` not just a colored arrow
- Sidebar collapsed: `aria-expanded="false"` on toggle

---

## 6. UX Patterns Library

### 6.1 Loading States

| State | Pattern | CSS Class |
|-------|---------|-----------|
| Initial page load | Skeleton placeholders matching content shape | `.skeleton-pulse` |
| Table data loading | Skeleton rows (8 rows, matching column widths) | `.skeleton-pulse` |
| KPI loading | Skeleton rectangle matching metric dimensions | `.skeleton-pulse` |
| Slow operation (>2s) | Loading overlay with spinner | `.loading-overlay` + `.loading-spinner` |
| Background sync | Subtle topbar progress bar | Custom (thin bar) |

**Rule**: Never show an empty white/dark screen. Always show structure immediately.

### 6.2 Empty States

Empty states must communicate what will appear and how to populate it:

```
┌────────────────────────────────────────┐
│                                        │
│            📦  (icon, 40%)             │
│                                        │
│      No orders synced yet              │  ← .empty-state__title
│                                        │
│  Orders will appear here after your    │  ← .empty-state__description
│  first Amazon SP-API sync completes.   │
│                                        │
│       [ Trigger Sync ]                 │  ← CTA button
│                                        │
└────────────────────────────────────────┘
```

CSS: `.empty-state`, `.empty-state__icon`, `.empty-state__title`, `.empty-state__description`

### 6.3 Error States

| Error Type | UX Pattern |
|------------|------------|
| API timeout | Inline alert with retry button |
| 404 / not found | Empty state with "Go back" link |
| Permission denied | Toast notification |
| Validation error | Inline field-level message |
| System error (500) | Full-page error boundary with Sentry report |

### 6.4 Data Freshness Indicators

Every data page shows when data was last synced:

```
● Last synced: 3 min ago          ← .data-freshness (green dot)
● Last synced: 45 min ago         ← .data-freshness--stale (amber dot)
```

The `.sync-status` dot in the topbar shows overall system health.

**DataTrust Badge** (Sprint 1): A small component on PPT showing data completeness:
```
DataTrust: 94% ████████░░   ← green if >90%, amber if 70-90%, red if <70%
```

### 6.5 Tooltip Patterns — Metric Explanations

UX Research finding BI-08: CM1/CM2/NP terminology is opaque to 80%+ users.

Every financial metric header should have an info tooltip:

| Metric | Tooltip Text |
|--------|-------------|
| CM1 | "Contribution Margin 1 — Revenue minus direct product costs (COGS + FBA fees + referral fee)" |
| CM2 | "Contribution Margin 2 — CM1 minus advertising costs (PPC spend)" |
| NP | "Net Profit — CM2 minus allocated overhead costs (shipping, returns, FX losses)" |
| ACoS | "Advertising Cost of Sales — Ad spend as a percentage of ad-attributed revenue" |
| TACoS | "Total ACoS — Ad spend as a percentage of total revenue (organic + ad)" |
| ROAS | "Return on Ad Spend — Revenue generated per €1 of ad spend" |

Implementation: shadcn `<Tooltip>` on table header cells with `<InfoCircle>` icon.

### 6.6 Progressive Disclosure

**Summary → Detail → Raw** pattern:

1. **Summary** (default view): KPI cards with 8-10 columns in PPT
2. **Detail** (on-demand): Expand column groups, click row for product detail
3. **Raw** (power user): Full 60+ column export, API access

This addresses BI-05 and reduces cognitive load from 60+ columns.

### 6.7 Quick Wins from UX Research

Mapped to implementation sprints:

| Quick Win | Sprint | Effort | Impact |
|-----------|--------|--------|--------|
| SQL pagination for PPT (<2s load) | 1 | High | Critical (BI-02) |
| DataTrust badge | 1 | Low | Medium |
| FX rate alert banner | 1 | Low | Medium |
| Sidebar collapse to 7 groups | 2 | Medium | High (BI-03) |
| Metric tooltip explanations | 2 | Low | High (BI-08) |
| Column visibility toggle (prog. disclosure) | 2 | Medium | High (BI-05) |
| Consistent EN language across UI | 2 | Medium | Medium |
| Quick Margin widget on Dashboard | 2 | Medium | Medium |
| Breadcrumb navigation | 2 | Low | Medium |
| Ads→Profit cross-link | 3 | Medium | High |
| Module hide/show in settings | 3 | Low | Medium |
| Export (CSV/PDF) | 3 | Medium | Medium |
| Demo account + onboarding wizard | 4 | High | High |
| Mobile responsive | 4 | High | Medium |

---

## 7. CSS File Import Guide

### 7.1 Import Order in `main.tsx`

```tsx
// main.tsx — import order matters
import "./index.css";                // 1. Tailwind directives + shadcn base tokens
import "./css/design-system.css";    // 2. Extended tokens + utilities
import "./css/layout.css";           // 3. Layout framework
import "./css/components.css";       // 4. Component styles
```

### 7.2 File Responsibilities

| File | Layer | Contains | Size |
|------|-------|----------|------|
| `index.css` | `@layer base` | Tailwind directives, shadcn color tokens, scrollbar | ~70 lines (existing, don't modify) |
| `design-system.css` | `@layer base` + `@layer components` | Extended tokens (semantic colors, typography, spacing, motion, chart palette), utility classes, reduced-motion | ~190 lines |
| `layout.css` | `@layer components` | App shell grid, page layouts, dashboard grids, table containers, responsive breakpoints, mobile sidebar | ~240 lines |
| `components.css` | `@layer components` | Financial metrics, data tables, status badges, margin badges, chart wrappers, loading states, toasts, empty states | ~280 lines |

### 7.3 Naming Convention

- CSS custom properties: `--kebab-case` (e.g., `--chart-1`, `--space-4`)
- CSS classes: `.kebab-case` with BEM-lite modifiers (e.g., `.data-table--striped`, `.margin-badge--healthy`)
- No `!important` except in `prefers-reduced-motion` override
- All in `@layer components` to play correctly with Tailwind's cascade

### 7.4 Extending the System

To add a new component style:

1. Add CSS custom properties (if needed) to `design-system.css` inside `@layer base`
2. Add component class to `components.css` inside `@layer components`
3. Keep classes flat — avoid nesting beyond `.parent .child` or `.parent--modifier`
4. Prefer Tailwind utilities for one-off styles; use CSS classes for repeated multi-property patterns

---

## 8. Implementation Priorities

### Sprint 1 — Performance & Trust (Weeks 1–2)

**Goal**: PPT loads in <2s, data trust is visible

| Task | Files Affected |
|------|---------------|
| Server-side pagination API | `profit_v2.py` (backend) |
| Virtual scroll table component | New `DataTable.tsx` |
| Skeleton loading for PPT | Uses `.skeleton-pulse` from `components.css` |
| DataTrust badge component | New `DataTrustBadge.tsx` |
| FX rate alert banner | New `FxAlert.tsx` |
| Import new CSS files in `main.tsx` | `main.tsx` (3 import lines) |

### Sprint 2 — Clarity & Navigation (Weeks 3–4)

**Goal**: Reduced cognitive load, understandable metrics

| Task | Files Affected |
|------|---------------|
| Sidebar consolidation (12→7) | Sidebar component refactor |
| Breadcrumb component | New `Breadcrumbs.tsx` |
| Metric tooltip system | Table header enhancement |
| Column visibility toggle | PPT toolbar enhancement |
| Quick Margin widget | Dashboard component |
| Language consistency audit (PL→EN) | All page components |

### Sprint 3 — Cross-Features (Weeks 5–6)

**Goal**: Connected data, actionable exports

| Task | Files Affected |
|------|---------------|
| Ads→Profit cross-link | Campaign & PPT components |
| Unified alert triage | New `AlertCenter.tsx` |
| Module visibility settings | Settings page + sidebar |
| CSV/PDF export | Table toolbar + backend endpoint |
| Command palette (⌘K) | New `CommandPalette.tsx` |

### Sprint 4 — Onboarding & Mobile (Weeks 7–8)

**Goal**: New user success, tablet support

| Task | Files Affected |
|------|---------------|
| Demo account with seed data | Backend + new `DemoMode.tsx` |
| Onboarding wizard (3 steps) | New `OnboardingWizard.tsx` |
| Help panel with contextual docs | New `HelpPanel.tsx` |
| Responsive mobile layout | Layout CSS + component adjustments |
| Automated report scheduling UI | New `ReportScheduler.tsx` |

---

*Generated by ArchitectUX agent — 2026-03-13*
*Foundation for LuxuryDeveloper implementation handoff*
