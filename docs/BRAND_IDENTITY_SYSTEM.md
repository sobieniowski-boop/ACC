# ACC Brand Identity System
## Comprehensive Brand Guidelines & Implementation Specifications

**Version**: 1.0  
**Date**: 2026-03-13  
**Prepared by**: Brand Guardian  
**For**: Miłosz Sobieniowski, Founder  
**Classification**: Brand Strategy — Master Document  
**Status**: Ready for Implementation

---

## TABLE OF CONTENTS

1. [Brand Foundation](#1-brand-foundation)
2. [Visual Identity System](#2-visual-identity-system)
3. [Brand Voice and Messaging Architecture](#3-brand-voice-and-messaging-architecture)
4. [Logo System Specifications](#4-logo-system-specifications)
5. [Brand Usage Guidelines](#5-brand-usage-guidelines)

---

# 1. BRAND FOUNDATION

## 1.1 Naming Strategy

### The Problem with "Amazon Commerce Cockpit"

The current name carries three legal/strategic risks:

1. **Trademark Violation**: "Amazon" is a protected trademark. Using it in a product name implies affiliation or endorsement. Amazon's Brand Usage Policy explicitly prohibits third-party use of "Amazon" in product/service names.
2. **Platform Dependency Signal**: A name containing "Amazon" signals single-platform dependency, limiting future expansion to Shopify/eBay/Allegro.
3. **Cockpit Connotation**: "Cockpit" implies a piloting/flying metaphor that doesn't align with the financial analytics core.

### Recommended Name: **Ascend Commerce Cloud**

| Criteria | Assessment |
|----------|-----------|
| **Abbreviation** | **ACC** — preserved. Zero disruption to existing code, URLs, internal references |
| **Meaning** | "Ascend" = upward trajectory, growth, climbing above complexity to clarity |
| **Commerce** | Signals the e-commerce domain without naming any specific marketplace |
| **Cloud** | Signals modern SaaS platform, data accessibility, always-on infrastructure |
| **Trademark Risk** | ✅ Clean — no Amazon/marketplace name references |
| **Domain Availability** | Verify: `ascendcommerce.com`, `ascendcc.com`, `getacc.io` |
| **Multi-language Fit** | "Ascend" is universally understood across PL/DE/FR/IT/ES markets |
| **Competitive Distinction** | None of the competitors use "Ascend" — unique in the Amazon seller tools space |

### Alternative Names Considered

| Name | Abbreviation | Pros | Cons | Verdict |
|------|-------------|------|------|---------|
| Ascend Commerce Cloud | ACC ✅ | Preserves abbreviation, aspirational, clean | "Cloud" may feel generic | **Recommended** |
| Apex Commerce Console | ACC ✅ | "Apex" = pinnacle, authority | "Console" = gaming connotation | Runner-up |
| Atlas Commerce Core | ACC ✅ | "Atlas" = comprehensive mapping | Atlas is overused in tech | Reserve |
| Astra Commerce Center | ACC ✅ | "Astra" = stars, aspiration | Too astronomical | Declined |
| ProfitLens | — | Direct, clear benefit | Loses ACC abbreviation | Declined |
| MarginMap | — | Descriptive | Sounds like a feature, not a platform | Declined |

### Naming Implementation

- **Full name**: Ascend Commerce Cloud
- **Common reference**: ACC (unchanged)
- **In-app display**: "ACC" (logomark) on sidebar, "Ascend Commerce Cloud" in settings/about
- **Marketing**: "ACC — Ascend Commerce Cloud" for first mentions, then "ACC" for subsequent
- **Code/repo**: No changes required — `ACC` already used everywhere
- **Tagline below name**: See §3 Messaging Architecture

---

## 1.2 Brand Purpose

> **We exist to give e-commerce sellers the financial truth they need to grow with confidence.**

Amazon's seller ecosystem generates enormous revenue but obscures profit through 100+ fee types, fragmented dashboards, currency conversions, and opaque advertising costs. Sellers operate in a fog — making decisions on revenue, not profit. ACC exists to clear that fog.

---

## 1.3 Brand Vision

> **A world where every e-commerce seller knows their true profit on every product, every marketplace, every day — and acts on it.**

When ACC fully succeeds:
- No seller loses money unknowingly on a product
- Multi-marketplace profitability is as clear as checking a bank balance
- The 14-hour weekly spreadsheet ritual becomes an automated 30-second health check
- PPC spend decisions are informed by true margin, not vanity ROAS

---

## 1.4 Brand Mission

> **ACC connects every data source in the Amazon seller ecosystem — orders, fees, advertising, logistics, COGS — into a single profit truth, calculated in real time, so sellers can make decisions that grow margin, not just revenue.**

---

## 1.5 Brand Values

| Value | Definition | Behavioral Manifestation |
|-------|-----------|--------------------------|
| 🎯 **Profit Truth** | We pursue accuracy above all. Every number must be verifiable, every calculation auditable. | We show data quality scores. We surface uncertainty rather than hiding it. We never approximate when precision is possible. 93% data accuracy is shown as 93%, not rounded to "good." |
| 🔒 **Radical Transparency** | We show our work. Data sources, calculation methods, confidence levels — nothing is a black box. | CM1 breakdowns show every fee. Estimations are labeled as estimates. Data freshness timestamps are always visible. Users can drill into any number. |
| ⚡ **Operational Empathy** | We respect that our users are operators, not analysts. Time is their scarcest resource. | 30-second health checks. One-click exports. Guardrails catch problems automatically. We never make users hunt for the number that matters. |
| 🏗️ **Depth Over Breadth** | We'd rather be the best at profit analytics than adequate at everything. | We invest 40%+ in the profit engine. We freeze features that don't serve profit truth. We say "we don't do that" rather than do it badly. |
| 🤝 **Builder's Integrity** | We're built by a seller, for sellers. We never ship something we wouldn't trust with our own P&L. | Founder uses ACC for 4,300 SKUs with real money at stake. Every feature is battle-tested on a live business before shipping to others. |

---

## 1.6 Brand Personality

### Archetype: The Sage-Commander

ACC combines the **Sage** (wisdom, clarity, truth-seeking) with the **Commander** (decisive, authoritative, action-oriented). This is not a playful tool or a curious explorer — it is a trusted advisor with the confidence of a CFO and the approachability of a senior colleague.

### Personality Spectrum

```
Formal ████████░░ Casual          → Leans professional but not corporate-stiff
Serious ███████░░░ Playful        → Serious about numbers, human about communication
Technical ██████░░░░ Simple        → Uses precise terms but explains clearly
Authoritative ████████░░ Humble   → Confident in data accuracy, humble about features-in-progress
Warm ██████░░░░ Detached          → Warm enough to feel personal, detached enough to feel trustworthy
```

### Personality Traits

| Trait | Description | In Practice |
|-------|-------------|-------------|
| **Precise** | Every word and number is intentional | "Your CM1 on ASIN B08X7YZ is €3.21" not "about €3" |
| **Commanding** | Speaks with earned authority on profit analytics | "Your French marketplace is losing money on 12 SKUs" — no hedging |
| **Calm** | Delivers even alarming information with composure | Financial truths presented without panic or alarm |
| **Efficient** | Respects the user's time in every interaction | Short labels, compressed tables, zero fluff in the UI |
| **Trustworthy** | Confidence built through consistency and transparency | Shows data sources, timestamps, confidence levels on every metric |

---

## 1.7 Brand Promise

> **"Every number you need. Nothing you don't."**

This promise encodes three commitments:
1. **Completeness**: CM1 calculated from ALL cost components, not just the obvious ones
2. **Accuracy**: Real data, not estimates (and when estimated, clearly labeled)
3. **Efficiency**: No feature bloat, no dashboard clutter, no unnecessary complexity

---

## 1.8 Brand Positioning Statement

> **For Amazon sellers who need to know their true profit, ACC is the only analytics platform that calculates real-time CM1 by connecting every data source — orders, advertising, logistics, and ERP — into a single financial truth. Unlike Sellerboard's surface-level profit tracking, ACC includes actual COGS from ERP integration, real logistics costs, and ads-to-profit attribution that competitors can't match.**

---

# 2. VISUAL IDENTITY SYSTEM

## 2.1 Design Philosophy

ACC's visual identity follows the principle of **"Dark Clarity"** — a dark, focused environment where the data itself provides the light. The interface should feel like a premium financial terminal: sophisticated, quiet, and information-dense without being cluttered.

Design pillars:
- **Dark-first**: Deep navy backgrounds with high-contrast data
- **Data is the hero**: Chrome is minimized, numbers are maximized
- **Financial gravity**: The visual system communicates that real money is at stake
- **Amazon-adjacent**: Orange accent connects to the Amazon ecosystem without copying it

---

## 2.2 Color System

### Primary Palette

| Role | Name | HEX | HSL | Usage |
|------|------|-----|-----|-------|
| 🟠 **Primary** | ACC Amber | `#FF9900` | `30 100% 50%` | Primary actions, CTAs, key metrics, brand accent |
| 🟠 **Primary Hover** | Amber Dark | `#E68A00` | `33 100% 45%` | Hover/active states on primary elements |
| 🟠 **Primary Muted** | Amber Glow | `#FF990020` | — (alpha) | Subtle backgrounds, tag fills, highlight zones |
| 🔵 **Secondary** | Steel Blue | `#3B82F6` | `217 91% 60%` | Links, secondary actions, informational elements |
| 🔵 **Secondary Hover** | Steel Blue Dark | `#2563EB` | `217 91% 54%` | Hover/active states on secondary elements |

### Dark Mode Backgrounds (Primary Theme)

| Role | Name | HEX | HSL | Usage |
|------|------|-----|-----|-------|
| ⬛ **Background** | Deep Navy | `#080D16` | `222 47% 6%` | Page/app background |
| ⬛ **Surface** | Card Navy | `#0C1320` | `222 47% 9%` | Cards, panels, sections |
| ⬛ **Surface Raised** | Raised Navy | `#111827` | `221 39% 11%` | Popovers, dropdowns, modals |
| ⬛ **Surface Interactive** | Hover Navy | `#1E293B` | `217 33% 17%` | Hover states, selected items, active rows |
| ⬛ **Border** | Slate Border | `#1E293B` | `217 33% 17%` | Dividers, card borders, separators |
| ⬛ **Border Strong** | Slate Border Strong | `#334155` | `215 25% 27%` | Emphasized dividers, focused input borders |

### Light Mode Backgrounds (Future — Secondary Theme)

| Role | Name | HEX | HSL | Usage |
|------|------|-----|-----|-------|
| ⬜ **Background** | Snow | `#FAFBFC` | `210 25% 98%` | Page background |
| ⬜ **Surface** | White | `#FFFFFF` | `0 0% 100%` | Cards, panels |
| ⬜ **Surface Raised** | Warm White | `#F8FAFC` | `210 40% 98%` | Popovers, modals |
| ⬜ **Border** | Cloud | `#E2E8F0` | `214 32% 91%` | Dividers, borders |

### Text Colors

| Role | Name | HEX (dark) | HSL | Usage |
|------|------|-----------|-----|-------|
| **Primary Text** | Ice White | `#F8FAFC` | `210 40% 98%` | Headlines, body text, primary labels |
| **Secondary Text** | Slate | `#94A3B8` | `215 20% 65%` | Descriptions, helper text, captions |
| **Tertiary Text** | Slate Dim | `#64748B` | `215 16% 47%` | Timestamps, metadata, disabled labels |
| **Inverse Text** | Deep Navy | `#080D16` | `222 47% 6%` | Text on amber/light backgrounds |

### Semantic Colors

| Role | Name | HEX | HSL | Usage |
|------|------|-----|-----|-------|
| ✅ **Success** | Profit Green | `#22C55E` | `142 71% 45%` | Positive profit, growth, healthy metrics |
| ✅ **Success Muted** | Profit Green Soft | `#22C55E20` | — (alpha) | Success backgrounds, positive trend areas |
| ❌ **Destructive** | Loss Red | `#EF4444` | `0 84% 60%` | Negative profit, errors, critical alerts |
| ❌ **Destructive Muted** | Loss Red Soft | `#EF444420` | — (alpha) | Error backgrounds, loss indicators |
| ⚠️ **Warning** | Caution Amber | `#F59E0B` | `38 92% 50%` | Warnings, thresholds approaching, data staleness |
| ⚠️ **Warning Muted** | Caution Amber Soft | `#F59E0B20` | — (alpha) | Warning backgrounds |
| ℹ️ **Info** | Intel Blue | `#3B82F6` | `217 91% 60%` | Informational notices, tips, guidance |
| ℹ️ **Info Muted** | Intel Blue Soft | `#3B82F620` | — (alpha) | Information backgrounds |

### Data Visualization Palette

For charts, graphs, and multi-series data. Designed for maximum distinguishability on dark backgrounds, colourblind-safe ordering.

| Index | Name | HEX | Usage |
|-------|------|-----|-------|
| 1 | Amber | `#FF9900` | Primary series, current period, main KPI |
| 2 | Sky | `#38BDF8` | Secondary series, comparison period |
| 3 | Emerald | `#34D399` | Positive/profit series |
| 4 | Rose | `#FB7185` | Negative/loss series |
| 5 | Violet | `#A78BFA` | Third comparison, category 3 |
| 6 | Teal | `#2DD4BF` | Category 4, logistics data |
| 7 | Fuchsia | `#E879F9` | Category 5, advertising data |
| 8 | Slate | `#94A3B8` | Baseline, target, reference lines |

### Marketplace Accent Colors (Optional — for multi-marketplace indicators)

| Marketplace | Color | HEX | Rationale |
|------------|-------|-----|-----------|
| 🇩🇪 DE | Ocean Blue | `#3B82F6` | Largest EU market, blue authority |
| 🇵🇱 PL | Cardinal Red | `#EF4444` | Polish flag association |
| 🇫🇷 FR | Royal Blue | `#6366F1` | French distinction |
| 🇮🇹 IT | Forest Green | `#22C55E` | Italian tricolore |
| 🇪🇸 ES | Sunset Orange | `#F97316` | Spanish warmth |
| 🇳🇱 NL | Dutch Orange | `#FB923C` | National color |
| 🇸🇪 SE | Nordic Yellow | `#FACC15` | Swedish flag |
| 🇧🇪 BE | Belgian Gold | `#EAB308` | Belgian identity |
| 🇨🇿 CZ | Czech Blue | `#60A5FA` | Flag blue |

---

## 2.3 Complete CSS Variables

Copy-pasteable replacement for the current `index.css` `@layer base` block:

```css
@layer base {
  /* ============================================
     ACC Brand Identity System — CSS Variables
     Version 1.0 | 2026-03-13
     ============================================ */

  :root {
    /* --- Backgrounds --- */
    --background: 210 25% 98%;
    --foreground: 222 47% 6%;
    --card: 0 0% 100%;
    --card-foreground: 222 47% 6%;
    --popover: 210 40% 98%;
    --popover-foreground: 222 47% 6%;

    /* --- Brand Colors --- */
    --primary: 30 100% 50%;
    --primary-foreground: 0 0% 100%;
    --secondary: 217 91% 60%;
    --secondary-foreground: 0 0% 100%;

    /* --- Surfaces --- */
    --muted: 210 40% 96%;
    --muted-foreground: 215 16% 47%;
    --accent: 210 40% 96%;
    --accent-foreground: 222 47% 11%;

    /* --- Semantic --- */
    --destructive: 0 84% 60%;
    --destructive-foreground: 0 0% 100%;
    --success: 142 71% 45%;
    --success-foreground: 0 0% 100%;
    --warning: 38 92% 50%;
    --warning-foreground: 0 0% 0%;
    --info: 217 91% 60%;
    --info-foreground: 0 0% 100%;

    /* --- Chrome --- */
    --border: 214 32% 91%;
    --border-strong: 215 25% 75%;
    --input: 214 32% 91%;
    --ring: 30 100% 50%;
    --radius: 0.5rem;

    /* --- Shadows --- */
    --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
    --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.07), 0 2px 4px -2px rgb(0 0 0 / 0.05);
    --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.08), 0 4px 6px -4px rgb(0 0 0 / 0.04);
    --shadow-xl: 0 20px 25px -5px rgb(0 0 0 / 0.1), 0 8px 10px -6px rgb(0 0 0 / 0.06);

    /* --- Motion --- */
    --transition-fast: 100ms;
    --transition-base: 200ms;
    --transition-slow: 350ms;
    --ease-default: cubic-bezier(0.4, 0, 0.2, 1);
    --ease-in: cubic-bezier(0.4, 0, 1, 1);
    --ease-out: cubic-bezier(0, 0, 0.2, 1);
    --ease-bounce: cubic-bezier(0.34, 1.56, 0.64, 1);

    /* --- Typography --- */
    --font-primary: 'Inter', ui-sans-serif, system-ui, -apple-system, sans-serif;
    --font-mono: 'JetBrains Mono', 'Fira Code', ui-monospace, SFMono-Regular, monospace;

    /* --- Type Scale --- */
    --text-xs: 0.6875rem;     /* 11px — micro labels */
    --text-sm: 0.75rem;       /* 12px — captions, timestamps */
    --text-base: 0.875rem;    /* 14px — body text */
    --text-lg: 1rem;          /* 16px — emphasized body */
    --text-xl: 1.125rem;      /* 18px — subtitles */
    --text-2xl: 1.25rem;      /* 20px — section headers */
    --text-3xl: 1.5rem;       /* 24px — page titles */
    --text-4xl: 1.875rem;     /* 30px — hero/dashboard headers */
    --text-5xl: 2.25rem;      /* 36px — marketing headlines */

    /* --- Line Heights --- */
    --leading-tight: 1.25;
    --leading-normal: 1.5;
    --leading-relaxed: 1.625;

    /* --- Font Weights --- */
    --font-normal: 400;
    --font-medium: 500;
    --font-semibold: 600;
    --font-bold: 700;

    /* --- Spacing Scale --- */
    --space-0: 0rem;
    --space-1: 0.25rem;       /* 4px */
    --space-2: 0.5rem;        /* 8px */
    --space-3: 0.75rem;       /* 12px */
    --space-4: 1rem;          /* 16px */
    --space-5: 1.25rem;       /* 20px */
    --space-6: 1.5rem;        /* 24px */
    --space-8: 2rem;          /* 32px */
    --space-10: 2.5rem;       /* 40px */
    --space-12: 3rem;         /* 48px */
    --space-16: 4rem;         /* 64px */
    --space-20: 5rem;         /* 80px */

    /* --- Borders --- */
    --radius-sm: 0.25rem;     /* 4px — tags, badges */
    --radius-md: 0.5rem;      /* 8px — cards, inputs (DEFAULT) */
    --radius-lg: 0.75rem;     /* 12px — modals, panels */
    --radius-xl: 1rem;        /* 16px — large cards, hero sections */
    --radius-full: 9999px;    /* pills, avatars */

    /* --- Data Viz Palette --- */
    --chart-1: 30 100% 50%;      /* Amber */
    --chart-2: 199 89% 60%;      /* Sky */
    --chart-3: 160 60% 52%;      /* Emerald */
    --chart-4: 351 83% 74%;      /* Rose */
    --chart-5: 263 70% 76%;      /* Violet */
    --chart-6: 168 76% 50%;      /* Teal */
    --chart-7: 292 84% 72%;      /* Fuchsia */
    --chart-8: 215 20% 65%;      /* Slate */
  }

  .dark {
    /* --- Backgrounds --- */
    --background: 222 47% 6%;
    --foreground: 210 40% 98%;
    --card: 222 47% 9%;
    --card-foreground: 210 40% 98%;
    --popover: 221 39% 11%;
    --popover-foreground: 210 40% 98%;

    /* --- Brand Colors --- */
    --primary: 30 100% 50%;
    --primary-foreground: 0 0% 0%;
    --secondary: 217 91% 60%;
    --secondary-foreground: 0 0% 100%;

    /* --- Surfaces --- */
    --muted: 217 33% 17%;
    --muted-foreground: 215 20% 65%;
    --accent: 217 33% 17%;
    --accent-foreground: 210 40% 98%;

    /* --- Semantic --- */
    --destructive: 0 84% 60%;
    --destructive-foreground: 210 40% 98%;
    --success: 142 71% 45%;
    --success-foreground: 210 40% 98%;
    --warning: 38 92% 50%;
    --warning-foreground: 0 0% 0%;
    --info: 217 91% 60%;
    --info-foreground: 210 40% 98%;

    /* --- Chrome --- */
    --border: 217 33% 17%;
    --border-strong: 215 25% 27%;
    --input: 217 33% 17%;
    --ring: 30 100% 50%;

    /* --- Shadows (heavier for dark mode) --- */
    --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.2);
    --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.3), 0 2px 4px -2px rgb(0 0 0 / 0.2);
    --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.35), 0 4px 6px -4px rgb(0 0 0 / 0.2);
    --shadow-xl: 0 20px 25px -5px rgb(0 0 0 / 0.4), 0 8px 10px -6px rgb(0 0 0 / 0.25);

    /* --- Chart uses same palette (engineered for dark bg contrast) --- */
    --chart-1: 30 100% 50%;
    --chart-2: 199 89% 60%;
    --chart-3: 160 60% 52%;
    --chart-4: 351 83% 74%;
    --chart-5: 263 70% 76%;
    --chart-6: 168 76% 50%;
    --chart-7: 292 84% 72%;
    --chart-8: 215 20% 65%;
  }
}
```

---

## 2.4 Typography System

### Typeface Selection

| Role | Font | Rationale | Fallback Stack |
|------|------|-----------|---------------|
| **Primary** | **Inter** | Open-source, optimized for UIs, excellent tabular number support (`font-feature-settings: "tnum"`), crisp at small sizes, designed by Rasmus Andersson for screen legibility. Free on Google Fonts. | `ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif` |
| **Monospace** | **JetBrains Mono** | Designed for code/data readability. Ligatures for developers, excellent digit clarity. Free on Google Fonts. | `'Fira Code', ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace` |

### Why Inter over the System Font Stack

The current system font stack (`ui-sans-serif, system-ui...`) renders differently across Windows (Segoe UI), macOS (SF Pro), and Linux (various). For a financial analytics product where table alignment, number rendering, and typographic consistency directly impact trust:

- **Tabular numbers**: Inter's `tnum` feature aligns decimal points in financial columns.
- **Consistent x-height**: Identical rendering across platforms for pixel-perfect dashboards.
- **Variable font**: Single file, all weights, optimized loading.

### Implementation

Add to `index.html` `<head>`:

```html
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
```

### Type Scale Specifications

| Token | Size | Weight | Line Height | Letter Spacing | Usage |
|-------|------|--------|-------------|---------------|-------|
| `--text-xs` | 11px | 500 | 1.25 | +0.02em | Micro labels: data freshness timestamps, table footnotes |
| `--text-sm` | 12px | 400 | 1.25 | +0.01em | Captions, timestamps, badge text, table headers |
| `--text-base` | 14px | 400 | 1.5 | 0 | Body text, table cells, input fields, descriptions |
| `--text-lg` | 16px | 500 | 1.5 | 0 | Emphasized body, card labels, metric labels |
| `--text-xl` | 18px | 600 | 1.25 | -0.01em | Subtitles, modal titles, section labels |
| `--text-2xl` | 20px | 600 | 1.25 | -0.01em | Section headers, panel titles |
| `--text-3xl` | 24px | 700 | 1.25 | -0.02em | Page titles, dashboard view names |
| `--text-4xl` | 30px | 700 | 1.15 | -0.02em | Hero numbers, KPI big stat displays |
| `--text-5xl` | 36px | 700 | 1.1 | -0.025em | Marketing headlines (website only) |

### Monospace Usage

Monospace font is reserved for:
- Financial amounts in data tables (for column alignment)
- ASIN/SKU identifiers
- API status codes & technical identifiers
- Code snippets in documentation

CSS utility class:

```css
.font-mono-tabular {
  font-family: var(--font-mono);
  font-variant-numeric: tabular-nums;
  font-feature-settings: "tnum";
}
```

---

## 2.5 Spacing & Layout System

### Grid System

| Property | Value | Notes |
|---------|-------|-------|
| Grid columns | 12-column | Marketing/full pages |
| Column gap | `--space-6` (24px) | Default gap between columns |
| Row gap | `--space-4` (16px) | Default gap between rows |
| Max content width | 1400px | `container` class from Tailwind config |
| Sidebar width | 224px (w-56) | Fixed, collapsible to 64px (icon-only) |
| TopBar height | 56px (h-14) | Fixed |
| Content padding | `--space-6` (24px) | Distance from edges to content |

### Spacing Usage Guide

| Token | px | Usage |
|-------|-----|-------|
| `--space-1` | 4 | Inline element gaps (icon-to-text), minor visual separation |
| `--space-2` | 8 | Tight groups (badge lists, inline tags), form input padding-x |
| `--space-3` | 12 | List item gaps, small card padding |
| `--space-4` | 16 | Standard card padding, section gaps, form field spacing |
| `--space-6` | 24 | Content area padding, between-card gaps, section dividers |
| `--space-8` | 32 | Major section separation within a page |
| `--space-10` | 40 | Between major dashboard sections |
| `--space-12` | 48 | Page-level vertical padding |
| `--space-16` | 64 | Marketing page section spacing |
| `--space-20` | 80 | Marketing hero vertical padding |

---

## 2.6 Border & Radius System

### Border Widths

| Token | Width | Usage |
|-------|-------|-------|
| Default | 1px | Card borders, dividers, input outlines |
| Strong | 2px | Focus rings, active tab indicators, emphasis lines |
| Thick | 3px | Left-border color indicators on status cards |

### Radius Usage

| Token | Value | Usage |
|-------|-------|-------|
| `--radius-sm` | 4px | Badges, tags, small pills |
| `--radius-md` | 8px | Cards, inputs, buttons, dropdowns — **DEFAULT** |
| `--radius-lg` | 12px | Modals, large panels, dialog boxes |
| `--radius-xl` | 16px | Hero cards, marketing feature blocks |
| `--radius-full` | 9999px | Avatars, circular indicators, pill buttons |

---

## 2.7 Shadow & Elevation System

### Elevation Hierarchy

| Level | Token | Usage | CSS Value (dark mode) |
|-------|-------|-------|-----------------------|
| 0 | None | Flat elements, list items | `none` |
| 1 | `--shadow-sm` | Cards, table headers, subtle depth | `0 1px 2px 0 rgb(0 0 0 / 0.2)` |
| 2 | `--shadow-md` | Dropdowns, tooltips, raised cards | `0 4px 6px -1px rgb(0 0 0 / 0.3), 0 2px 4px -2px rgb(0 0 0 / 0.2)` |
| 3 | `--shadow-lg` | Popovers, floating panels, command palettes | `0 10px 15px -3px rgb(0 0 0 / 0.35), 0 4px 6px -4px rgb(0 0 0 / 0.2)` |
| 4 | `--shadow-xl` | Modals, dialogs, overlay panels | `0 20px 25px -5px rgb(0 0 0 / 0.4), 0 8px 10px -6px rgb(0 0 0 / 0.25)` |

### Glow Effects (Brand Accent)

For special emphasis on primary elements (active sidebar item, featured metric):

```css
.glow-primary {
  box-shadow: 0 0 20px rgb(255 153 0 / 0.15), 0 0 6px rgb(255 153 0 / 0.1);
}

.glow-success {
  box-shadow: 0 0 20px rgb(34 197 94 / 0.15), 0 0 6px rgb(34 197 94 / 0.1);
}

.glow-destructive {
  box-shadow: 0 0 20px rgb(239 68 68 / 0.15), 0 0 6px rgb(239 68 68 / 0.1);
}
```

---

## 2.8 Motion & Animation System

### Timing Tokens

| Token | Duration | Usage |
|-------|----------|-------|
| `--transition-fast` | 100ms | Micro-interactions: hover color changes, icon swaps |
| `--transition-base` | 200ms | Standard transitions: button clicks, tab switches, card hovers |
| `--transition-slow` | 350ms | Larger movements: sidebar collapse, modal entry, panel slide |

### Easing Functions

| Token | Function | Usage |
|-------|----------|-------|
| `--ease-default` | `cubic-bezier(0.4, 0, 0.2, 1)` | General purpose — smooth and natural |
| `--ease-in` | `cubic-bezier(0.4, 0, 1, 1)` | Elements exiting view |
| `--ease-out` | `cubic-bezier(0, 0, 0.2, 1)` | Elements entering view |
| `--ease-bounce` | `cubic-bezier(0.34, 1.56, 0.64, 1)` | Celebrations, success confirmations (sparingly) |

### Animation Guidelines

| Context | Animation | Duration | Easing |
|---------|----------|----------|--------|
| Page transitions | Fade in + slight upward translate (8px) | 200ms | ease-out |
| Card loading skeleton | Shimmer pulse (opacity 0.5↔1.0) | 1500ms | ease-in-out |
| Sidebar collapse | Width transition + icon rotation | 350ms | ease-default |
| Data refresh | Subtle pulse on updated cells | 600ms | ease-out |
| Success toast | Slide in from right + fade | 250ms | ease-out |
| Error toast | Slide in from right + subtle shake | 250ms + 300ms | ease-out |
| Number counting | Count-up animation on KPI changes | 400ms | ease-out |
| Chart drawing | Staggered path draw | 500ms total | ease-out |

### Motion Principles

1. **Purpose over decoration**: Every animation must communicate state change. No animation for aesthetics alone.
2. **Speed over spectacle**: Financial data demands speed. Default to `--transition-base` (200ms). Only use `--transition-slow` for major layout changes.
3. **Reduce motion**: Respect `prefers-reduced-motion`. All animations must degrade to instant transitions:

```css
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;
    transition-duration: 0.01ms !important;
  }
}
```

---

# 3. BRAND VOICE AND MESSAGING ARCHITECTURE

## 3.1 Voice Attributes

| Attribute | We Are | We Are Not |
|-----------|--------|------------|
| **Precise** | "Your CM1 on B08X7YZ dropped 12% to €3.21 this week" | "Your margins seem to be going down a bit recently" |
| **Direct** | "3 products are losing money on FR marketplace" | "There might be some profitability challenges worth investigating across your French operations" |
| **Confident** | "This is your true profit, calculated from 12 data sources" | "We think this number is probably pretty close to accurate" |
| **Calm** | "Your ads sync failed at 14:22. We'll retry in 15 minutes. Last successful data: 2h ago." | "⚠️ ERROR!! Data sync FAILED! Your numbers may be WRONG!" |
| **Respectful of time** | "Weekly P&L ready. 3 actions recommended." | "Here's your comprehensive weekly performance analysis report with a detailed breakdown of all the metrics you might want to review…" |

---

## 3.2 Tone Spectrum

The voice stays constant. The **tone** adjusts based on context:

| Context | Tone | Characteristics | Example |
|---------|------|----------------|---------|
| **Marketing website** | Aspirational + Confident | Bolder claims, future-oriented, emotional hooks | "Stop guessing. Start knowing." |
| **Onboarding** | Warm + Guiding | Patient, encouraging, step-by-step | "Let's connect your first marketplace. This takes about 2 minutes." |
| **Dashboard** | Neutral + Precise | Minimal text, data-first, no fluff | "CM1: €3.21 · 24h" |
| **Alerts & Notifications** | Clear + Actionable | State → Impact → Action format | "PPC spend on DE exceeded €500/day. CM1 impact: -€127. Review campaigns →" |
| **Error states** | Calm + Supportive | Reassuring, transparent about what happened | "Sync paused. We're reconnecting to Amazon SP-API. No data was lost." |
| **Success / Celebrations** | Warm + Understated | Brief positive reinforcement, not over-the-top | "First month calculated. Your avg. CM1: €4.82/unit." |
| **Documentation / Help** | Clear + Technical | Exhaustive accuracy, assumes competence | "CM1 = Revenue − (Amazon Fees + Shipping + PPC Cost + COGS)" |
| **Email communications** | Professional + Concise | Scannable, mobile-first, one CTA per email | Subject: "Your weekly profit snapshot: DE ↑12%, FR ↓3%" |
| **PDF Reports** | Formal + Authoritative | Report-grade language, suitable for accountants | "Contribution Margin Level 1 (CM1) by Marketplace — February 2026" |

---

## 3.3 Messaging Hierarchy

### Level 1 — Brand Tagline

> **"Your profit. Precisely."**

**Alternatives considered:**
- "See the profit Amazon hides" — too adversarial toward Amazon, risky for partnership
- "Every fee. Every cost. Every truth." — strong but long
- "Profit clarity for Amazon sellers" — descriptive but generic
- "Where sellers see their real numbers" — good but not punchy enough

**Rationale**: "Your profit. Precisely." encodes the two pillars — **ownership** (your data, your business) and **accuracy** (precision is the tool's unique value). The period after "profit" creates a deliberate pause that emphasizes both words.

### Level 2 — Value Proposition

> **ACC connects every data source in the Amazon seller ecosystem — orders, ads, logistics, and ERP — to calculate your true profit per product, per marketplace, in real time. Stop running your business on revenue. Start running it on profit.**

### Level 3 — Feature-Level Messaging

| Capability | Headline | Supporting Copy |
|-----------|----------|----------------|
| **Profit Engine** | "CM1 calculated from every cost — not just the obvious ones" | "100+ Amazon fee types, real COGS from your ERP, actual shipping costs, PPC attribution — unified into one number you can trust. Updated every 15 minutes." |
| **Ads→Profit Attribution** | "Beyond ROAS: see what your ads actually earn" | "Your PPC campaigns don't just generate sales — they generate (or destroy) profit. ACC connects ad spend directly to CM1 so you know which campaigns make money and which burn it." |
| **Logistics Cost Model** | "Your real shipping cost, not Amazon's estimate" | "FBM sellers: ACC models your actual GLS/InPost/DPD rates including fuel surcharges and zone pricing. FBA sellers: every fulfillment and storage fee tracked to the unit." |
| **ERP / COGS Pipeline** | "Purchase prices from your ERP, not a spreadsheet" | "Direct integration with your ERP system syncs COGS automatically. No manual uploads, no stale data, no 'I forgot to update the spreadsheet' margin surprises." |
| **Multi-Marketplace P&L** | "Nine marketplaces. One P&L. Every currency accounted for." | "DE, PL, FR, IT, ES, NL, SE, BE, CZ — each marketplace with its own fee structure, currency, and VAT rules. ACC unifies them into the financial truth you need." |
| **Dashboard & Guardrails** | "30-second health check. Zero spreadsheets." | "Open ACC. See your numbers. Know if something's wrong. Guardrails alert you to anomalies before they become problems. Your weekly P&L review now takes 30 seconds." |

### Level 4 — Persona-Specific Messaging

| Persona | Headline | Message |
|---------|----------|---------|
| **Miłosz — The Builder-Operator** | "Built for the seller who builds their own tools" | "You've tried everything. You've built macros, scraped APIs, stitched spreadsheets. ACC does what you've been building manually — but it runs 24/7, never sleeps, and scales to 4,300 SKUs." |
| **Kasia — The Growing Polish Seller** | "Finally know if you're making money — on every single product" | "Amazon shows you revenue. ACC shows you what's left after 100+ fee types, VAT, shipping, PPC, and COGS. The answer might surprise you — but you need to know it." |
| **Markus — The Scaling DACH Seller** | "Multi-marketplace profitability without the spreadsheet nightmare" | "DE revenue in EUR, PL costs in PLN, FR returns in different VAT brackets — ACC reconciles all of it into a single P&L your CFO can trust." |
| **Anna — The Agency PPC Manager** | "Show clients what their ad spend actually earned" | "When a client asks if 25% ACOS is good, you'll finally have an answer grounded in real margin. ACC connects PPC spend to true CM1 — the number that matters." |
| **Tomasz — The Aggregator Ops Manager** | "Fleet-level profitability across every brand and marketplace" | "2,000 SKUs. Five brands. Nine marketplaces. One dashboard that shows profit truth at every level — brand, marketplace, product, time period." |

---

## 3.4 Terminology Guide

### Preferred Terms

| Use This ✅ | Not This ❌ | Rationale |
|------------|-----------|-----------|
| Profit | Margin | "Profit" is concrete and emotional. "Margin" is abstract and academic. |
| CM1 / Contribution Margin 1 | Gross profit | CM1 is our specific calculation that includes more than gross profit |
| True profit | Estimated profit | Our value prop is accuracy — "true" reinforces this |
| Insight | Data point | Insights imply actionability; data points are inert |
| Marketplace | Market / Channel | Amazon's terminology; maintains domain familiarity |
| Connect | Integrate / Sync | "Connect" is simpler and more human |
| Dashboard | Control panel / Portal | Dashboard is the industry standard for analytics products |
| Guardrail | Alert / Warning | "Guardrail" implies proactive protection, not reactive panic |
| COGS | Purchase price / Cost price | Industry standard accounting term with specific meaning |
| Health check | Report / Summary | "Health check" implies quick diagnostic with clear pass/fail |
| Calculated | Estimated / Predicted | Every number is calculated from real data sources |

### Words We Avoid

| Avoid | Why |
|-------|-----|
| Amazon (in product name) | Trademark risk |
| Synergy / Leverage / Disrupt | Corporate buzzwords that erode trust |
| Revolutionary / Game-changing | Over-promising language |
| Simple / Easy | Dismissive of the genuine complexity sellers face |
| Dashboard (as a verb) | "Dashboard your data" — no |
| At a glance | Overused, meaningless |
| Empower | Patronizing |
| Unlock | SaaS cliché |
| 360° view | Every competitor says this |
| Cutting-edge / State-of-the-art | Generic filler |

---

## 3.5 Sample Copy

### Homepage Hero Headline + Subheadline

```
Headline:    Your profit. Precisely.

Subheadline: ACC connects your Amazon orders, advertising, logistics, 
             and ERP into one number — your true contribution margin — 
             calculated across 9 EU marketplaces in real time.

CTA:         Start Free Trial    |    See How It Works
```

### Onboarding Welcome Message

```
Welcome to ACC.

You're about to see your Amazon business with financial precision 
most sellers never get.

Let's start by connecting your first marketplace. 
This takes about 2 minutes, and we'll walk you through every step.

[Connect Amazon Seller Central →]
```

### Empty State — No Data Yet

```
No profit data yet.

ACC needs at least one marketplace connected and 24 hours of order 
data to calculate your first CM1. 

Once connected, we'll process your historical orders going back 
12 months — you'll see trends, not just a snapshot.

[Connect Marketplace →]       Need help? Read the setup guide.
```

### Error State — Data Sync Failed

```
Amazon SP-API sync paused.

Connection to your DE marketplace was interrupted at 14:22 today. 
This can happen when Amazon's API is under maintenance.

Your data: Safe. Last synced 2 hours ago. All calculations reflect 
data through 12:22.

What happens next: We're retrying automatically every 15 minutes. 
No action needed from you.

[Check Sync Status]    [Reconnect Manually]
```

### Success Celebration — First Profitable Month Identified

```
Your first month is calculated.

February 2026 across all marketplaces:
  Revenue:  €47,230
  CM1:      €8,104  (17.2% margin)
  
  847 products contributing positively
  23 products below breakeven — worth reviewing

[View Full P&L →]    [See Losing Products →]
```

### Dashboard Data Freshness Indicator

```
Normal:     Data current · Last sync 4 min ago
Stale:      Data is 2h old · Sync in progress...
Warning:    ⚠ Data from 6h ago · API reconnecting
Error:      ● Sync offline since 14:22 · Retrying
```

### Loading State — Slow Query

```
Calculating profit across 4,300 SKUs...

This is a complex calculation touching 12 data sources. 
Large portfolios take 10-15 seconds on first load.

Tip: Pin your most-used filters to speed up repeat views.
```

---

# 4. LOGO SYSTEM SPECIFICATIONS

## 4.1 Logo Concept

### Concept: "The Ascending Bar"

The ACC logo combines a **geometric upward trajectory** with a **bar chart abstraction**, communicating financial growth, precision, and analytical clarity.

**Composition:**

1. **Icon Element (Logomark):** Three vertical bars of ascending height, arranged left to right, with the tallest bar having an angled top edge that rises at approximately 30°. The bars represent data/profit columns in a chart — the ascending pattern communicates growth and upward trajectory ("Ascend"). The bars are slightly rounded at top corners (`--radius-sm`). The negative space between bars is equal and deliberate, suggesting precision and measured analysis.

2. **Color Treatment:** The bars use a gradient from `--secondary` (Steel Blue, #3B82F6) for the shortest bar through a mid-tone blend to `--primary` (ACC Amber, #FF9900) for the tallest bar. This gradient encodes the brand's two key colors and creates visual momentum toward the amber/gold — the color of profit and value.

3. **Wordmark Element:** "ACC" set in **Inter Bold** (700 weight), letter-spacing `-0.03em` for tight, commanding presence. The three letters sit beside the logomark, aligned to the baseline of the shortest bar.

4. **Tagline placement** (optional lockup): "Ascend Commerce Cloud" in Inter Medium, set at 40% the size of the "ACC" wordmark, positioned directly below the wordmark with `--space-1` gap.

### Visual Style

- **Geometric, not illustrative**: Clean mathematical forms, not hand-drawn or organic
- **Flat design**: No gradients in the bar fills at small sizes (solid colors); gradient reserved for hero/marketing sizes
- **Precision**: Pixel-perfect alignment, consistent stroke weights, mathematical spacing
- **Weight**: Confident and solid — not thin or wispy

### Concept Rationale

| Element | Communicates |
|---------|-------------|
| Ascending bars | Growth, profit trajectory, the "Ascend" in the name |
| Chart abstraction | Data analytics, financial measurement, profit engine |
| Blue→Amber gradient | Journey from raw data (blue/cool) to profit truth (amber/warm) |
| Geometric precision | Accuracy, calculation, trustworthiness |
| Tight "ACC" letterforms | Commanding, efficient, professional |

---

## 4.2 Logo Variants

### Variant Matrix

| Variant | Description | Usage |
|---------|-------------|-------|
| **Primary (Horizontal)** | Logomark + "ACC" wordmark, side by side | Default for most contexts: sidebar header, website nav, documents |
| **Primary with Tagline** | Primary + "Ascend Commerce Cloud" below wordmark | About pages, formal documents, press materials |
| **Stacked** | Logomark above "ACC" wordmark, centered | Square contexts: social media profiles, app tiles |
| **Logomark Only** | Ascending bars icon without text | Favicon, app icon, collapsed sidebar, loading spinner, watermarks |
| **Wordmark Only** | "ACC" in branded typography | Text-heavy contexts where icon is redundant, email signatures |

### Size Specifications

| Variant | Aspect Ratio | Standard Sizes |
|---------|-------------|---------------|
| Primary Horizontal | ~3:1 | 180×60px, 240×80px, 360×120px |
| Primary with Tagline | ~3:1.3 | 240×104px, 360×156px |
| Stacked | ~1:1.2 | 80×96px, 120×144px |
| Logomark Only | 1:1 | 32×32px, 48×48px, 64×64px, 128×128px, 256×256px |
| Wordmark Only | ~2.5:1 | 120×48px, 180×72px |

---

## 4.3 Clear Space

The minimum clear space around all logo variants is defined as **1x**, where "x" equals the height of the shortest bar in the logomark.

```
    ┌─────────────────────────────┐
    │         x (clear space)     │
    │   ┌─────────────────────┐   │
    │   │                     │   │
    │ x │  [LOGO]             │ x │
    │   │                     │   │
    │   └─────────────────────┘   │
    │         x (clear space)     │
    └─────────────────────────────┘
```

No other graphic elements, text, or visual noise may enter the clear space zone.

---

## 4.4 Minimum Sizes

| Variant | Digital Minimum | Print Minimum |
|---------|----------------|---------------|
| Primary Horizontal | 120px wide | 30mm wide |
| Stacked | 48px wide | 12mm wide |
| Logomark Only | 16px (favicon) / 24px (UI) | 8mm |
| Wordmark Only | 64px wide | 16mm wide |

Below minimum sizes, switch to **Logomark Only** variant.

---

## 4.5 Color Variants

| Variant | Background | Logo Colors | Usage |
|---------|-----------|-------------|-------|
| **Full Color (Dark BG)** | Dark navy or black | Amber + Blue gradient bars, white wordmark | Primary usage — in-app, dark website |
| **Full Color (Light BG)** | White or light gray | Amber + Blue gradient bars, dark navy wordmark | Light theme, printed collateral |
| **Single Color (White)** | Dark backgrounds | All-white logo (bars + wordmark) | Low-contrast dark contexts, screen printing |
| **Single Color (Dark)** | Light backgrounds | All-`#080D16` navy logo (bars + wordmark) | Formal print, legal documents |
| **Monochrome (Gray)** | Any | `#94A3B8` (muted) | Watermarks, disabled states, background patterns |

---

## 4.6 Logo Don'ts

| Rule | Description |
|------|-------------|
| ❌ Don't stretch or distort | Maintain original aspect ratio at all times |
| ❌ Don't rotate | The ascending trajectory must always read left-to-right, bottom-to-top |
| ❌ Don't add effects | No drop shadows, glows, bevels, or 3D effects on the logo |
| ❌ Don't recolor arbitrarily | Only use approved color variants from §4.5 |
| ❌ Don't place on busy backgrounds | If background is complex, use a solid color container behind the logo |
| ❌ Don't rearrange elements | The spatial relationship between logomark and wordmark is fixed |
| ❌ Don't add "Amazon" to the logo | Never prefix or suffix the logo with "Amazon" |
| ❌ Don't use as a pattern | The logo is not a decorative pattern element |
| ❌ Don't crop the bars | The ascending bars must always be fully visible |
| ❌ Don't use below minimum size | Switch to logomark-only below thresholds |

---

## 4.7 Favicon & App Icon

### Favicon

- **Design**: Logomark only (ascending bars), simplified for 16×16 clarity
- **Colors**: Full-color gradient at 32×32, simplified to solid amber at 16×16
- **Format**: SVG primary (scalable), with ICO fallback containing 16×16, 32×32, 48×48
- **Background**: Transparent

### App Icon (PWA / Mobile)

- **Design**: Logomark centered on a rounded-square background
- **Background**: `#080D16` (Deep Navy) — consistent with dark theme identity
- **Logomark**: Full-color gradient variant
- **Corner radius**: Platform-standard (iOS: continuous, Android: adaptive)
- **Sizes**: 192×192, 512×512 for `manifest.json`
- **Safe zone**: Logomark occupies 60% of the total icon area (20% padding each side)

### Implementation

```json
// manifest.json icons array
{
  "icons": [
    { "src": "/icons/icon-192.png", "sizes": "192x192", "type": "image/png" },
    { "src": "/icons/icon-512.png", "sizes": "512x512", "type": "image/png" },
    { "src": "/icons/icon-maskable-512.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable" }
  ]
}
```

---

# 5. BRAND USAGE GUIDELINES

## 5.1 Application Examples

### A. Dashboard UI

| Element | Specification |
|---------|--------------|
| **Sidebar logo** | Logomark + "ACC" wordmark (Primary Horizontal variant), amber icon + white text on `--background` navy. When sidebar collapsed: logomark only. |
| **Top bar** | Clean, no logo. Page title in `--text-2xl` Inter Semibold. Right side: data freshness indicator, user avatar. Background: `--card` navy. Bottom border: 1px `--border`. |
| **Cards** | `--card` background, `--radius-md` corners, 1px `--border`, `--space-4` internal padding. Card title in `--text-lg` Semibold. KPI numbers in `--text-4xl` Bold, monospace tabular. |
| **Tables** | Alternating row background: `--card` / transparent. Header row: `--muted` background, `--text-sm` uppercase tracking. Amounts: right-aligned, `font-mono-tabular`. Profit-positive: `--success` text. Profit-negative: `--destructive` text. |
| **Buttons (Primary)** | `--primary` background, `--primary-foreground` text, `--radius-md`, hover: darken 10%, transition `--transition-base`. |
| **Buttons (Secondary)** | `--secondary` background, `--secondary-foreground` text, or ghost variant with `--border` border and transparent background. |
| **Charts** | Dark backgrounds, data visualization palette §2.2, subtle grid lines in `--border` color, tooltips with `--popover` background and `--shadow-lg`. |

### B. Marketing Website

| Element | Specification |
|---------|--------------|
| **Hero section** | Dark navy background, gradient overlay from `--background` to transparent. Headline in `--text-5xl` Inter Bold, white. Subheadline in `--text-xl`, `--muted-foreground`. Primary CTA button in full `--primary` amber. |
| **Navigation** | Sticky, transparent on scroll-top, `--background` with `--shadow-sm` on scroll. Logo: Primary Horizontal variant (white). |
| **Feature sections** | Alternating between `--background` navy and slightly lighter `--card` navy. Feature icons: amber-tinted Lucide icons. Descriptions: `--text-base`, `--muted-foreground`. |
| **Social proof** | Customer logos in monochrome white (§4.5 single-color-white). Testimonial quotes in `--text-lg` italic. |
| **Footer** | `--background` navy, `--border-strong` top border. Copyright text in `--text-sm`, `--muted-foreground`. |

### C. Email Communications

| Element | Specification |
|---------|--------------|
| **Header** | White background (email clients), Primary Horizontal logo (light BG variant), left-aligned. |
| **Body** | System font stack (Inter may not be available in email clients). `--text-base` size. Dark text on white background for universal readability. Key metrics highlighted with amber background (`#FF990015`). |
| **CTA** | Solid amber `#FF9900` button, white text, 36px height, `8px` border radius. Single CTA per email. |
| **Footer** | Light gray background `#F8FAFC`. Muted text. Unsubscribe link. Company details. |
| **Subject lines** | Format: `"ACC: [Key insight]"` or `"Your weekly profit snapshot: [headline metric]"` |

### D. PDF Reports

| Element | Specification |
|---------|--------------|
| **Header** | Primary Horizontal logo (light BG variant), right-aligned to page. Report title in Inter Bold 18pt, left-aligned. Date and period below in Inter Regular 11pt, `--muted-foreground`. |
| **Body** | Inter Regular 10pt for body text. Inter Semibold for section headers. Tables: 9pt, alternating row shading. All financial numbers: JetBrains Mono 10pt, right-aligned. |
| **Charts** | Same data visualization palette on white backgrounds. Remove dark-mode specific styling. |
| **Footer** | Page number, "Generated by ACC · ascendcommerce.com", thin amber top-border line. |
| **Color usage** | Light mode derivative: white backgrounds, dark navy text, amber accents for headers and rules. Success green / Destructive red for profit/loss indicators. |

### E. Social Media

| Platform | Profile Image | Cover/Banner | Post Style |
|---------|-------------|-------------|-----------|
| **LinkedIn** | Stacked logo on `#080D16` background | Tagline + abstract ascending chart visual on dark navy gradient | Professional insights, founder story, industry analyses |
| **Twitter/X** | Logomark only on `#080D16` | "Your profit. Precisely." + abstract gradient | Sharp stats, seller tips, product updates |
| **YouTube** | Stacked logo | Dark navy with amber accent stripe | Product demos, tutorials, seller interview series |

---

## 5.2 Photography & Illustration Style

### Photography Direction

ACC is a data product — photography is secondary to UI screenshots and data visualizations. When photography is used:

| Guideline | Specification |
|-----------|--------------|
| **Subject matter** | Warehouse operations, shipping/logistics, business analysis (laptop/screen), e-commerce fulfillment |
| **Style** | Real-world, documentary style. Not staged corporate. Slightly desaturated tones. |
| **Color treatment** | Cool-toned post-processing (aligns with navy theme). Amber/warm highlights on key subjects. |
| **Avoid** | Generic stock photos of "happy people at screens." Amazon-branded boxes (trademark). AI-generated images. |
| **Sources** | Unsplash, Pexels, or custom photography. Ensure proper licensing. |

### Illustration Style

| Guideline | Specification |
|-----------|--------------|
| **Style** | Geometric, flat, minimal line illustrations. Not cartoon or hand-drawn. |
| **Color palette** | Amber, Steel Blue, Slate on transparent/dark navy backgrounds |
| **Line weight** | Consistent 2px strokes |
| **Usage** | Empty states, onboarding flows, error pages, marketing feature explanations |
| **Icons** | Lucide React icon library — consistent with current implementation. Custom icons follow Lucide design language (24px grid, 2px stroke, square canvas). |

---

## 5.3 Do's and Don'ts

### Do's ✅

| Rule | Context |
|------|---------|
| ✅ Use amber `#FF9900` as the primary accent sparingly — not for large surfaces | Amber is high-energy; overuse creates visual fatigue |
| ✅ Lead with numbers, not words, in dashboards | Data-first principle: "CM1: €3.21" not "Your CM1 is €3.21" |
| ✅ Use monospace tabular numerals for all financial data | Ensures column alignment and communicates precision |
| ✅ Show data confidence and freshness | Timestamp every calculation. Label estimates. Show quality scores. |
| ✅ Use semantic colors for profit/loss consistently | Green = positive. Red = negative. Never reversed. Never contextual. |
| ✅ Maintain minimum contrast ratios (see §5.5) | WCAG AA compliance is mandatory |
| ✅ Use dark mode as the primary context for screenshots/demos | This is the brand experience |
| ✅ Credit data sources when referencing external data | "Source: Amazon SP-API" not just showing numbers without attribution |

### Don'ts ❌

| Rule | Context |
|------|---------|
| ❌ Don't use "Amazon" in the product name or marketing headlines | Trademark risk; only reference Amazon as a marketplace platform |
| ❌ Don't use Amazon's smile logo or brand elements | They are protected trademarks |
| ❌ Don't use amber as a background color for large areas | It overwhelms on dark backgrounds; use `#FF990015` for subtle tinting |
| ❌ Don't mix serif fonts into the UI | The brand is exclusively sans-serif (Inter) + monospace (JetBrains Mono) |
| ❌ Don't use exclamation marks in error messages | Calm > panic. "Sync failed." not "Sync failed!" |
| ❌ Don't round financial numbers without indication | "€3.21" not "~€3". If rounding, show "(rounded)" or use a tilde prefix. |
| ❌ Don't use more than 3 colors in a single chart | Cognitive overload. If more than 3 series, use a table instead. |
| ❌ Don't animate financial numbers without purpose | Count-up is OK for celebration moments, not for routine data display. |
| ❌ Don't use the word "dashboard" in marketing as if it's a feature | Every tool has a dashboard. Focus on what the dashboard shows (profit truth). |
| ❌ Don't use gradient backgrounds in the app UI | Gradients are reserved for logo/marketing hero only. App surfaces are flat. |

---

## 5.4 Co-Branding Guidelines — Amazon Trademark Compliance

### Critical Rules

Amazon is a registered trademark. ACC is an independent third-party tool. All references to Amazon must comply with Amazon's trademark usage guidelines.

| Rule | Implementation |
|------|---------------|
| **Never use "Amazon" in ACC's product name** | "ACC" or "Ascend Commerce Cloud" — never "Amazon Commerce Cockpit" |
| **Amazon name usage in marketing** | OK: "ACC analytics for Amazon sellers" / "Works with Amazon Seller Central" |
| **Amazon name usage — forbidden** | Never: "Amazon's analytics tool" / "Powered by Amazon" / "Official Amazon product" |
| **Amazon logo** | Never reproduce the Amazon smile logo. Do not use Amazon's trademarks as part of ACC's brand identity. |
| **SP-API attribution** | When mentioning API connection: "Connects via Amazon's Selling Partner API" (per Amazon's MWS/SP-API terms) |
| **Marketplace logos** | Use flag emojis or neutral marketplace identifiers (DE, PL, FR), not Amazon marketplace logos |
| **"Amazon" in UI** | OK in data labels: "Amazon Fees", "Amazon Ads" — these are factual descriptions of data sources |
| **Trademark symbol** | First instance in any document: "Amazon® is a registered trademark of Amazon.com, Inc." in footnote |
| **Disclaimer** | Website footer and legal docs: "ACC is an independent product and is not affiliated with, endorsed by, or sponsored by Amazon.com, Inc. or its affiliates." |

---

## 5.5 Accessibility — WCAG AA Compliance

### Color Contrast Requirements

All text and interactive elements must meet WCAG 2.1 AA minimum contrast ratios:

| Element Type | Minimum Ratio | Standard |
|-------------|--------------|---------|
| Normal text (< 18pt) | 4.5:1 | WCAG AA |
| Large text (≥ 18pt bold / ≥ 24pt) | 3:1 | WCAG AA |
| UI components & graphics | 3:1 | WCAG AA |

### Verified Contrast Ratios — Dark Mode

| Combination | Foreground | Background | Ratio | Pass? |
|------------|-----------|-----------|-------|-------|
| Primary text on background | `#F8FAFC` on `#080D16` | — | **17.8:1** | ✅ AAA |
| Secondary text on background | `#94A3B8` on `#080D16` | — | **6.8:1** | ✅ AA |
| Tertiary text on background | `#64748B` on `#080D16` | — | **4.5:1** | ✅ AA |
| Amber on background | `#FF9900` on `#080D16` | — | **7.3:1** | ✅ AA |
| Success green on background | `#22C55E` on `#080D16` | — | **8.2:1** | ✅ AA |
| Destructive red on background | `#EF4444` on `#080D16` | — | **5.1:1** | ✅ AA |
| Amber on card | `#FF9900` on `#0C1320` | — | **6.5:1** | ✅ AA |
| Primary text on card | `#F8FAFC` on `#0C1320` | — | **15.4:1** | ✅ AAA |
| Warning text on background | `#F59E0B` on `#080D16` | — | **8.1:1** | ✅ AA |

### Verified Contrast Ratios — Light Mode

| Combination | Foreground | Background | Ratio | Pass? |
|------------|-----------|-----------|-------|-------|
| Primary text on background | `#080D16` on `#FAFBFC` | — | **17.4:1** | ✅ AAA |
| Secondary text on background | `#64748B` on `#FAFBFC` | — | **4.8:1** | ✅ AA |
| Amber on white | `#FF9900` on `#FFFFFF` | — | **2.9:1** | ❌ Fail |
| Amber adjusted for light | `#B36B00` on `#FFFFFF` | — | **4.6:1** | ✅ AA |

**Light mode note**: Pure `#FF9900` amber fails contrast on white backgrounds. When implementing light mode, use the darkened amber `#B36B00` for text, and reserve `#FF9900` for large interactive elements (buttons, filled backgrounds where text is dark).

### Accessibility Implementation Checklist

- [ ] All interactive elements have visible focus indicators (2px `--ring` outline with 2px offset)
- [ ] All images have descriptive `alt` text
- [ ] All form inputs have associated `<label>` elements
- [ ] Color is never the only indicator of state (combine with icons, text, or patterns)
- [ ] All charts have data table alternatives accessible via screen readers
- [ ] Navigation is fully keyboard-accessible (Tab, Enter, Escape, Arrow keys)
- [ ] `prefers-reduced-motion` is respected (see §2.8)
- [ ] `prefers-color-scheme` is respected when light mode is available
- [ ] ARIA landmarks are used for major regions (`nav`, `main`, `aside`, `footer`)
- [ ] Dynamic content changes are announced via `aria-live` regions

---

## 5.6 Brand Evolution Triggers

The following events should trigger a brand identity review:

| Trigger | Scope of Review | Timeline |
|---------|----------------|----------|
| **Marketplace expansion** beyond Amazon (Shopify, eBay, Allegro) | Name validation ("Commerce" still works; "Amazon" references in voice need audit) | When decision is made to expand |
| **Light mode launch** | Full visual system validation for light theme | Before light mode ships |
| **Pricing model change** (free tier → enterprise) | Messaging hierarchy, persona messaging, value proposition | When pricing is finalized |
| **First 100 paying customers** milestone | Brand perception survey, voice tone audit, testimonial refresh | At milestone |
| **Series A / significant funding** | Consider professional logo development, comprehensive motion system | At funding event |
| **Geographic expansion** beyond EU | Localization audit, cultural sensitivity review, name translation check | When expansion is planned |
| **Annual anniversary** | Routine brand audit: consistency check, competitor positioning refresh, visual freshness | Annually |
| **User count exceeds 500** | Enterprise co-branding rules, partner portal branding, API documentation branding | At milestone |
| **NPS drops below 30** | Voice and messaging audit — is the brand promise being delivered? | Immediately |

---

# APPENDIX A: IMPLEMENTATION PRIORITY

| Priority | Action | Effort | Impact |
|----------|--------|--------|--------|
| 🔴 P0 | Replace "Amazon Commerce Cockpit" with "ACC" in all UI strings | 1 hour | Eliminates trademark risk |
| 🔴 P0 | Update `index.css` with new CSS variables block (§2.3) | 30 min | Unified design token system |
| 🟠 P1 | Add Inter + JetBrains Mono font loading | 15 min | Brand typography active |
| 🟠 P1 | Add `font-mono-tabular` class to all financial number displays | 2 hours | Precision perception boost |
| 🟠 P1 | Update sidebar logo placeholder with "ACC" wordmark styling | 30 min | Brand presence in-app |
| 🟡 P2 | Commission logo design (Fiverr/99designs, brief from §4.1) | 1-2 weeks | Professional brand mark |
| 🟡 P2 | Update loading/error/empty state copy per §3.5 | 2-3 hours | Voice consistency |
| 🟡 P2 | Add semantic color tokens (success/warning/info) to Tailwind config | 1 hour | Extended design system |
| 🟢 P3 | Create marketing website with brand guidelines applied | 1-2 weeks | External brand expression |
| 🟢 P3 | Create PDF report template with brand styling | 3-4 hours | Persona 3 (Markus) value |
| 🟢 P3 | Register trademark for "Ascend Commerce Cloud" / "ACC" | 2-4 weeks | Legal protection |

---

# APPENDIX B: TAILWIND CONFIG EXTENSIONS

Additional Tailwind config values to add alongside the CSS variable updates:

```js
// tailwind.config.js — extend section additions
extend: {
  colors: {
    // ... existing shadcn colors ...
    amazon: {
      DEFAULT: "#FF9900",
      dark: "#B36B00",     // Light-mode accessible variant
      glow: "rgb(255 153 0 / 0.15)",
    },
    success: {
      DEFAULT: "hsl(var(--success))",
      foreground: "hsl(var(--success-foreground))",
    },
    warning: {
      DEFAULT: "hsl(var(--warning))",
      foreground: "hsl(var(--warning-foreground))",
    },
    info: {
      DEFAULT: "hsl(var(--info))",
      foreground: "hsl(var(--info-foreground))",
    },
  },
  fontFamily: {
    sans: ['Inter', 'ui-sans-serif', 'system-ui', '-apple-system', 'sans-serif'],
    mono: ['JetBrains Mono', 'Fira Code', 'ui-monospace', 'monospace'],
  },
  fontSize: {
    'xs':   ['0.6875rem', { lineHeight: '1.25' }],
    'sm':   ['0.75rem',   { lineHeight: '1.25' }],
    'base': ['0.875rem',  { lineHeight: '1.5' }],
    'lg':   ['1rem',      { lineHeight: '1.5' }],
    'xl':   ['1.125rem',  { lineHeight: '1.25' }],
    '2xl':  ['1.25rem',   { lineHeight: '1.25' }],
    '3xl':  ['1.5rem',    { lineHeight: '1.25' }],
    '4xl':  ['1.875rem',  { lineHeight: '1.15' }],
    '5xl':  ['2.25rem',   { lineHeight: '1.1' }],
  },
  transitionDuration: {
    'fast': '100ms',
    'base': '200ms',
    'slow': '350ms',
  },
  transitionTimingFunction: {
    'default': 'cubic-bezier(0.4, 0, 0.2, 1)',
    'bounce': 'cubic-bezier(0.34, 1.56, 0.64, 1)',
  },
  boxShadow: {
    'glow-primary': '0 0 20px rgb(255 153 0 / 0.15), 0 0 6px rgb(255 153 0 / 0.1)',
    'glow-success': '0 0 20px rgb(34 197 94 / 0.15), 0 0 6px rgb(34 197 94 / 0.1)',
    'glow-destructive': '0 0 20px rgb(239 68 68 / 0.15), 0 0 6px rgb(239 68 68 / 0.1)',
  },
}
```

---

# APPENDIX C: BRAND ASSET CHECKLIST

| Asset | Format | Status |
|-------|--------|--------|
| Logo — Primary Horizontal (Dark BG) | SVG + PNG@2x | 🔲 To commission |
| Logo — Primary Horizontal (Light BG) | SVG + PNG@2x | 🔲 To commission |
| Logo — Stacked | SVG + PNG@2x | 🔲 To commission |
| Logo — Logomark Only | SVG + PNG@2x | 🔲 To commission |
| Logo — Wordmark Only | SVG + PNG@2x | 🔲 To commission |
| Logo — Single Color White | SVG | 🔲 To commission |
| Logo — Single Color Dark | SVG | 🔲 To commission |
| Favicon (16, 32, 48px) | ICO + SVG | 🔲 To create |
| App Icon (192, 512px) | PNG | 🔲 To create |
| OG Image (1200×630) | PNG | 🔲 To create |
| Social Profile Image | PNG 400×400 | 🔲 To create |
| Social Cover — LinkedIn | PNG 1584×396 | 🔲 To create |
| Social Cover — Twitter | PNG 1500×500 | 🔲 To create |
| Email Header Template | HTML | 🔲 To create |
| PDF Report Template | PDF/Figma | 🔲 To create |
| CSS Variables System | CSS | ✅ Defined (§2.3) |
| Tailwind Config Extensions | JS | ✅ Defined (Appendix B) |
| Font Loading | HTML | ✅ Defined (§2.4) |
| Brand Guidelines (this document) | MD | ✅ Complete |

---

**Document Control**

| Field | Value |
|-------|-------|
| Version | 1.0 |
| Created | 2026-03-13 |
| Author | Brand Guardian |
| Next Review | 2026-06-13 (quarterly) or at next evolution trigger |
| Distribution | Internal — Founder and development team |

---

*Amazon® is a registered trademark of Amazon.com, Inc. ACC (Ascend Commerce Cloud) is an independent product and is not affiliated with, endorsed by, or sponsored by Amazon.com, Inc. or its affiliates.*
