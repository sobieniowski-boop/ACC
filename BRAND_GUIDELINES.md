# ACC — Brand Guidelines

> Version: 2026-03-12 | Product: Amazon Commerce Cockpit (ACC)
> Theme: Dark-first, Amazon Orange accent

---

## 1. Brand Identity

### Product Name

| Usage | Format |
|---|---|
| **Full Name** | Amazon Commerce Cockpit |
| **Abbreviation** | ACC |
| **URL/Technical** | acc |
| **Tagline** | *Multi-marketplace intelligence platform* |

### Brand Positioning

ACC is an internal operations platform for managing Amazon marketplace presence across 8 European markets. The brand conveys:
- **Professional** — enterprise-grade e-commerce analytics
- **Efficient** — data-driven decision making
- **Trustworthy** — accurate financial and inventory data
- **Amazon-aligned** — visual affinity with Amazon ecosystem

---

## 2. Color Palette

### Primary Colors

| Color | Hex | HSL | Usage |
|---|---|---|---|
| **Amazon Orange** | `#FF9900` | `hsl(30, 100%, 50%)` | Primary brand color, CTAs, active states, focus rings |
| **Amazon Orange Dark** | `#E88900` | `hsl(30, 100%, 45%)` | Hover state for primary color |
| **Background Dark** | `#080D16` | `hsl(222, 47%, 6%)` | Page background (dark mode) |
| **Card Dark** | `#0C1320` | `hsl(222, 47%, 9%)` | Card/panel surfaces |

### Secondary Colors

| Color | Hex | HSL | Usage |
|---|---|---|---|
| **Navy** | `#1E293B` | `hsl(217.2, 32.6%, 17.5%)` | Secondary surfaces, borders, muted |
| **Text Light** | `#F8FAFC` | `hsl(210, 40%, 98%)` | Primary text (dark mode) |
| **Text Muted** | `#94A3B8` | `hsl(215, 20.2%, 65.1%)` | Secondary text, labels |

### Status Colors

| Status | Color | Hex | Usage |
|---|---|---|---|
| **Success** | Green 500 | `#22C55E` | Positive metrics, completed states |
| **Warning** | Yellow 500 | `#EAB308` | Warnings, pending states |
| **Error** | Red 500 | `#EF4444` | Errors, failed states, loss indicators |
| **Info** | Muted foreground | `#94A3B8` | Neutral information |

### Color Don'ts

- ❌ Do not use Amazon Orange for error states
- ❌ Do not use pure white (`#FFFFFF`) as background in dark mode
- ❌ Do not combine orange text on red background
- ❌ Do not use colors outside the defined palette for data visualization

---

## 3. Typography

### Font Family

System font stack (no custom fonts):

```
ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
"Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif
```

### Type Scale

| Level | Size | Weight | Class | Usage |
|---|---|---|---|---|
| **Hero Number** | 30px | Bold (700) | `text-3xl font-bold` | Dashboard KPI big numbers |
| **Page Title** | 24px | Bold (700) | `text-2xl font-bold` | Page headings |
| **Section Title** | 20px | Semibold (600) | `text-xl font-semibold` | Card titles |
| **Subtitle** | 18px | Semibold (600) | `text-lg font-semibold` | Section headers |
| **Body** | 14px | Normal (400) | `text-sm` | Default body text |
| **Label** | 14px | Medium (500) | `text-sm font-medium` | Form labels, table headers |
| **Caption** | 12px | Normal (400) | `text-xs` | Badges, timestamps, footnotes |

### Type Hierarchy Example

```
Dashboard                        ← text-2xl font-bold text-foreground
├── Revenue Card                 ← text-xl font-semibold text-foreground
│   ├── 145 234 zł              ← text-3xl font-bold text-foreground
│   └── +12.3% vs last month    ← text-sm text-green-500
├── Orders Card
│   ├── 2 341                   ← text-3xl font-bold text-foreground
│   └── -3.1% vs last month    ← text-sm text-red-500
└── Table Header                 ← text-sm font-medium text-muted-foreground
    └── Table Cell               ← text-sm text-foreground
```

---

## 4. Iconography

### Icon Library

**Lucide React** — consistent stroke-based icon set.

### Standard Sizes

| Context | Size | Class |
|---|---|---|
| **Sidebar/Navigation** | 16×16 | `h-4 w-4` |
| **Inline (buttons, badges)** | 16×16 | `h-4 w-4` |
| **Card headers** | 20×20 | `h-5 w-5` |
| **Empty states** | 48×48 | `h-12 w-12` |

### Icon Color Rules

| Context | Color |
|---|---|
| Active navigation | `text-amazon` (#FF9900) |
| Inactive navigation | `text-muted-foreground` |
| Button icons | Inherit from button text |
| Status icons | Match status color (green/yellow/red) |

### Common Icons

| Domain | Icon |
|---|---|
| Dashboard | `LayoutDashboard` |
| Orders/Profit | `DollarSign`, `TrendingUp` |
| Inventory | `Package`, `Warehouse` |
| Ads | `Megaphone` |
| Content | `FileText`, `Pen` |
| Settings | `Settings`, `Cog` |
| Alerts | `Bell`, `AlertTriangle` |
| Strategy | `Target`, `Lightbulb` |

---

## 5. Layout & Spacing

### Page Layout

```
┌─────────────────────────────────────────────────────┐
│ Sidebar (w-56)  │  TopBar (h-14)                    │
│                 ├────────────────────────────────────│
│  Logo           │                                    │
│  Nav Groups     │  Page Content                      │
│  ...            │  (padding: p-6)                    │
│                 │                                    │
│  Settings       │                                    │
└─────────────────┴────────────────────────────────────┘
```

### Spacing Scale

| Value | Pixels | Usage |
|---|---|---|
| `gap-2` | 8px | Tight inline spacing |
| `gap-4` | 16px | Standard card gap |
| `gap-6` | 24px | Section spacing |
| `p-4` | 16px | Card internal padding |
| `p-6` | 24px | Page content padding |
| `mb-6` | 24px | Section bottom margin |

### Card Pattern

```jsx
<Card>                          {/* bg-card rounded-lg border border-border */}
  <CardHeader>                  {/* p-6 pb-2 */}
    <CardTitle>Revenue</CardTitle>
    <CardDescription>Monthly overview</CardDescription>
  </CardHeader>
  <CardContent>                 {/* p-6 pt-0 */}
    {/* Content */}
  </CardContent>
</Card>
```

---

## 6. Data Visualization

### Chart Colors

| Series | Color | Token |
|---|---|---|
| Primary series | `#FF9900` | Amazon Orange |
| Secondary series | `#F8FAFC` | Foreground |
| Tertiary series | `#94A3B8` | Muted |
| Grid lines | `#1E293B` | Border |
| Axis labels | `#94A3B8` | Muted foreground |

### Number Formatting

| Type | Format | Example |
|---|---|---|
| Currency (PLN) | `Intl.NumberFormat("pl-PL", { currency: "PLN" })` | `145 234 zł` |
| Percentage | `value.toFixed(1) + "%"` | `12.3%` |
| Delta (positive) | `"+" + value.toFixed(1) + "%"` | `+5.2%` |
| Delta (negative) | `value.toFixed(1) + "%"` | `-3.1%` |
| Count | `Intl.NumberFormat("pl-PL")` | `2 341` |

### Status Indicators

| State | Visual | Example |
|---|---|---|
| **Positive** | Green text, `↑` icon | `+12.3%` in `text-green-500` |
| **Negative** | Red text, `↓` icon | `-3.1%` in `text-red-500` |
| **Neutral** | Muted text | `0.0%` in `text-muted-foreground` |
| **Active/Brand** | Orange badge | `bg-amazon/10 text-amazon` |

---

## 7. Component Patterns

### Button Hierarchy

| Variant | Usage | Example |
|---|---|---|
| `default` (primary) | Primary action | "Save", "Submit", "Run Job" |
| `outline` | Secondary action | "Cancel", "Export" |
| `ghost` | Tertiary/inline action | "View Details", icon buttons |
| `destructive` | Dangerous action | "Delete", "Revoke" |
| `link` | Navigation link styled as button | "View all" |

### Table Pattern

```
┌──────────────┬──────────┬─────────────┬──────────┐
│ SKU          │ Revenue  │ Margin      │ Status   │
│ text-sm      │ text-sm  │ text-sm     │ Badge    │
│ font-medium  │ right    │ green/red   │          │
├──────────────┼──────────┼─────────────┼──────────┤
│ ABC-123-DE   │ 1 234 zł │ +12.3%     │ ✅ Active │
│ DEF-456-PL   │   567 zł │  -3.1%     │ ⚠️ Low   │
└──────────────┴──────────┴─────────────┴──────────┘
```

### Badge Variants

| Variant | Styling | Usage |
|---|---|---|
| `default` | `bg-primary text-primary-foreground` | Primary status |
| `secondary` | `bg-secondary text-secondary-foreground` | Neutral/default |
| `destructive` | `bg-destructive text-destructive-foreground` | Error/failed |
| `outline` | `border text-foreground` | Informational |

---

## 8. Dark Mode Rules

The application runs **exclusively in dark mode** (hardcoded `class="dark"` on `<html>`).

| Rule |
|---|
| All surfaces use `bg-background` (`#080D16`) or `bg-card` (`#0C1320`) |
| Text defaults to `text-foreground` (`#F8FAFC`) |
| Secondary text uses `text-muted-foreground` (`#94A3B8`) |
| Borders use `border-border` (`#1E293B`) |
| Focus rings use Amazon Orange |
| Scrollbars styled to match dark theme |
| Charts use dark-friendly color palette |

---

## 9. Marketplace Identifiers

| Market | Code | Flag | Currency |
|---|---|---|---|
| Germany | DE | 🇩🇪 | EUR |
| Poland | PL | 🇵🇱 | PLN |
| France | FR | 🇫🇷 | EUR |
| Italy | IT | 🇮🇹 | EUR |
| Spain | ES | 🇪🇸 | EUR |
| Netherlands | NL | 🇳🇱 | EUR |
| Sweden | SE | 🇸🇪 | SEK |
| Belgium | BE | 🇧🇪 | EUR |

---

## 10. Logo & Branding Elements

| Element | Description |
|---|---|
| **Logo Position** | Top-left corner of sidebar, `h-14` container |
| **Logo Icon** | Lucide icon in `text-amazon` color |
| **Logo Text** | "ACC" in `text-sm font-semibold` |
| **Favicon** | Standard Vite favicon (to be customized) |

### Future Branding Work

- [ ] Custom SVG logo (Amazon orange + dark navy)
- [ ] Custom favicon matching brand
- [ ] Loading screen with brand animation
- [ ] Email template styling with brand colors
- [ ] PDF export header/footer with brand identity
