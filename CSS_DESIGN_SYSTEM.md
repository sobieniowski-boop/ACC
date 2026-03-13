# ACC — CSS Design System

> Version: 2026-03-12 | Framework: Tailwind CSS 3.4 + shadcn/ui
> Theme: HSL CSS Variables | Dark Mode: Class-based (always dark)

---

## 1. Stack Overview

| Layer | Technology | Version |
|---|---|---|
| **CSS Framework** | Tailwind CSS | ^3.4.16 |
| **Component Library** | shadcn/ui | Manual install (12 components) |
| **Primitives** | Radix UI | 10 packages |
| **Icons** | Lucide React | ^0.460.0 |
| **Charts** | Recharts | ^2.14.1 |
| **Animations** | tailwindcss-animate | ^1.0.7 |
| **Class Utilities** | clsx + tailwind-merge | ^2.1.1 / ^2.5.5 |
| **Variants** | class-variance-authority (CVA) | ^0.7.1 |
| **PostCSS** | autoprefixer + tailwindcss | — |

---

## 2. Design Tokens (CSS Custom Properties)

### 2.1 Color Tokens — Light Mode (`:root`)

| Token | HSL | Hex Approx | Usage |
|---|---|---|---|
| `--background` | `0 0% 100%` | `#FFFFFF` | Page background |
| `--foreground` | `222.2 84% 4.9%` | `#0A0F1A` | Default text |
| `--card` | `0 0% 100%` | `#FFFFFF` | Card surfaces |
| `--card-foreground` | `222.2 84% 4.9%` | `#0A0F1A` | Card text |
| `--popover` | `0 0% 100%` | `#FFFFFF` | Dropdown/popover bg |
| `--popover-foreground` | `222.2 84% 4.9%` | `#0A0F1A` | Dropdown text |
| `--primary` | `30 100% 50%` | `#FF9900` | **Amazon Orange** |
| `--primary-foreground` | `0 0% 100%` | `#FFFFFF` | Text on primary |
| `--secondary` | `210 40% 96.1%` | `#F0F4F8` | Secondary surface |
| `--secondary-foreground` | `222.2 47.4% 11.2%` | `#1A2332` | Text on secondary |
| `--muted` | `210 40% 96.1%` | `#F0F4F8` | Muted background |
| `--muted-foreground` | `215.4 16.3% 46.9%` | `#6B7A8D` | Muted text |
| `--accent` | `210 40% 96.1%` | `#F0F4F8` | Accent highlight |
| `--accent-foreground` | `222.2 47.4% 11.2%` | `#1A2332` | Text on accent |
| `--destructive` | `0 84.2% 60.2%` | `#EF4444` | Error/danger |
| `--destructive-foreground` | `210 40% 98%` | `#F8FAFC` | Text on destructive |
| `--border` | `214.3 31.8% 91.4%` | `#E2E8F0` | Border color |
| `--input` | `214.3 31.8% 91.4%` | `#E2E8F0` | Input border |
| `--ring` | `30 100% 50%` | `#FF9900` | Focus ring |
| `--radius` | `0.5rem` | 8px | Border radius base |

### 2.2 Color Tokens — Dark Mode (`.dark`)

| Token | HSL | Hex Approx | Usage |
|---|---|---|---|
| `--background` | `222 47% 6%` | `#080D16` | Page background |
| `--foreground` | `210 40% 98%` | `#F8FAFC` | Default text |
| `--card` | `222 47% 9%` | `#0C1320` | Card surfaces |
| `--card-foreground` | `210 40% 98%` | `#F8FAFC` | Card text |
| `--popover` | `222 47% 9%` | `#0C1320` | Dropdown/popover bg |
| `--popover-foreground` | `210 40% 98%` | `#F8FAFC` | Dropdown text |
| `--primary` | `30 100% 50%` | `#FF9900` | **Amazon Orange** |
| `--primary-foreground` | `0 0% 0%` | `#000000` | Text on primary |
| `--secondary` | `217.2 32.6% 17.5%` | `#1E293B` | Secondary surface |
| `--secondary-foreground` | `210 40% 98%` | `#F8FAFC` | Text on secondary |
| `--muted` | `217.2 32.6% 17.5%` | `#1E293B` | Muted background |
| `--muted-foreground` | `215 20.2% 65.1%` | `#94A3B8` | Muted text |
| `--accent` | `217.2 32.6% 17.5%` | `#1E293B` | Accent highlight |
| `--accent-foreground` | `210 40% 98%` | `#F8FAFC` | Text on accent |
| `--destructive` | `0 62.8% 30.6%` | `#7F1D1D` | Error dark |
| `--destructive-foreground` | `210 40% 98%` | `#F8FAFC` | Text on destructive |
| `--border` | `217.2 32.6% 17.5%` | `#1E293B` | Border color |
| `--input` | `217.2 32.6% 17.5%` | `#1E293B` | Input border |
| `--ring` | `30 100% 50%` | `#FF9900` | Focus ring |

### 2.3 Custom Brand Token

```js
// tailwind.config.js
colors: {
  amazon: {
    DEFAULT: "#FF9900",
    dark: "#E88900",
  }
}
```

Usage: `text-amazon`, `bg-amazon`, `bg-amazon/10`, `border-amazon`, `hover:text-amazon`

---

## 3. Typography

### Font Stack

System font (Tailwind default — no custom fonts loaded):

```css
font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
  "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif;
```

### Font Features

```css
body {
  font-feature-settings: "rlig" 1, "calt" 1;
}
```

### Size Scale (Tailwind defaults)

| Class | Size | Usage |
|---|---|---|
| `text-xs` | 12px | Badges, footnotes |
| `text-sm` | 14px | Body text, table cells, sidebar items |
| `text-base` | 16px | Standard body |
| `text-lg` | 18px | Section headers |
| `text-xl` | 20px | Card titles |
| `text-2xl` | 24px | Page titles |
| `text-3xl` | 30px | Dashboard hero numbers |

### Weight Scale

| Class | Weight | Usage |
|---|---|---|
| `font-normal` | 400 | Body text |
| `font-medium` | 500 | Table headers, labels |
| `font-semibold` | 600 | Card titles, sidebar logo |
| `font-bold` | 700 | KPI values, emphasis |

---

## 4. Spacing & Layout

### Container

```js
container: {
  center: true,
  padding: "2rem",
  screens: { "2xl": "1400px" }
}
```

### Border Radius

| Token | Value | Tailwind Class |
|---|---|---|
| `--radius` | `0.5rem` (8px) | `rounded-lg` uses this |
| `lg` | `var(--radius)` | 8px |
| `md` | `calc(var(--radius) - 2px)` | 6px |
| `sm` | `calc(var(--radius) - 4px)` | 4px |

### Grid System

- Sidebar: fixed `w-56` (224px)
- Content: `flex-1 overflow-auto`
- Cards: CSS Grid with `gap-4` or `gap-6`
- Tables: full-width with horizontal scroll

---

## 5. shadcn/ui Components (12 installed)

| Component | Radix Primitive | Variants |
|---|---|---|
| **Badge** | — | `default`, `secondary`, `destructive`, `outline` |
| **Button** | Slot | `default`, `destructive`, `outline`, `secondary`, `ghost`, `link` × `default`, `sm`, `lg`, `icon` |
| **Card** | — | `Card`, `CardHeader`, `CardTitle`, `CardDescription`, `CardContent`, `CardFooter` |
| **Dialog** | Dialog | `DialogTrigger`, `DialogContent`, `DialogHeader`, `DialogFooter`, `DialogTitle`, `DialogDescription` |
| **Input** | — | Standard text input with focus ring |
| **Label** | Label | Standard form label |
| **Progress** | Progress | Animated bar with `bg-primary` fill |
| **Select** | Select | `SelectTrigger`, `SelectContent`, `SelectItem`, `SelectValue` |
| **Separator** | Separator | Horizontal/vertical divider |
| **Skeleton** | — | Loading placeholder with `animate-pulse` |
| **Table** | — | `Table`, `TableHeader`, `TableBody`, `TableRow`, `TableHead`, `TableCell` |
| **Tooltip** | Tooltip | `TooltipProvider`, `TooltipTrigger`, `TooltipContent` |

---

## 6. Color Palette Usage Patterns

### Semantic Colors

| Purpose | Light Mode | Dark Mode | Classes |
|---|---|---|---|
| **Brand/Primary** | `#FF9900` | `#FF9900` | `text-amazon`, `bg-primary` |
| **Page Background** | `#FFFFFF` | `#080D16` | `bg-background` |
| **Card Background** | `#FFFFFF` | `#0C1320` | `bg-card` |
| **Text Primary** | `#0A0F1A` | `#F8FAFC` | `text-foreground` |
| **Text Muted** | `#6B7A8D` | `#94A3B8` | `text-muted-foreground` |
| **Border** | `#E2E8F0` | `#1E293B` | `border-border` |
| **Error** | `#EF4444` | `#7F1D1D` | `bg-destructive`, `text-destructive` |
| **Success** | green-500 | green-500 | `text-green-500` (not tokenized) |
| **Warning** | yellow-500 | yellow-500 | `text-yellow-500` (not tokenized) |

### Status Colors (Data Tables)

```
✅ Active/Success → text-green-500
⚠️ Warning/Pending → text-yellow-500
🔴 Error/Failed → text-destructive / text-red-500
📊 Info/Neutral → text-muted-foreground
🟠 Brand/Action → text-amazon
```

### Active State (Sidebar)

```jsx
// Active link
<a className="bg-amazon/10 text-amazon">
  <Icon className="h-4 w-4" />
  <span>Active Item</span>
</a>

// Inactive link
<a className="text-muted-foreground hover:bg-accent hover:text-foreground">
  <Icon className="h-4 w-4" />
  <span>Inactive Item</span>
</a>
```

---

## 7. Animations

### Keyframes (tailwind.config.js)

```js
keyframes: {
  "accordion-down": {
    from: { height: "0" },
    to: { height: "var(--radix-accordion-content-height)" },
  },
  "accordion-up": {
    from: { height: "var(--radix-accordion-content-height)" },
    to: { height: "0" },
  },
}
```

### tailwindcss-animate Plugin

Provides: `animate-in`, `animate-out`, `fade-in`, `fade-out`, `zoom-in`, `zoom-out`, `slide-in-from-*`, `slide-out-to-*`

Used by shadcn/ui Dialog, Select, Tooltip for enter/exit animations.

---

## 8. Scrollbar Styling

```css
/* Custom scrollbar — index.css */
*::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}
*::-webkit-scrollbar-track {
  background: hsl(var(--background));
}
*::-webkit-scrollbar-thumb {
  background: hsl(var(--muted));
  border-radius: 3px;
}
```

---

## 9. Code Splitting (Vite)

```js
manualChunks: {
  "vendor-react": ["react", "react-dom", "react-router-dom"],
  "vendor-query": ["@tanstack/react-query"],
  "vendor-charts": ["recharts"],
  "vendor-ui": ["@radix-ui/react-dialog", "@radix-ui/react-select", ...],
}
```

Chunk size warning: 500KB limit.

---

## 10. Utility Functions

### `cn()` — Class Name Merger

```typescript
import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
```

Used everywhere for conditional + merged class names:

```tsx
<div className={cn("bg-card rounded-lg p-4", isActive && "border-amazon")} />
```

### Formatters

```typescript
formatPLN(value)   → "1 234 zł"     // Intl.NumberFormat "pl-PL" PLN
formatPct(value)   → "12.3%"        // Fixed 1 decimal
formatDelta(delta) → "+5.2%" / "-3.1%"
```

---

## 11. Dark Mode Configuration

| Aspect | Configuration |
|---|---|
| **Strategy** | Class-based (`darkMode: ["class"]`) |
| **Default** | Always dark (`<html class="dark">` in index.html) |
| **Toggle** | None — no theme switcher implemented |
| **Persistence** | N/A — hardcoded to dark |

To add theme switching in the future:
1. Install `next-themes` or create custom `useTheme` hook
2. Store preference in `localStorage`
3. Add toggle button to TopBar
4. Remove hardcoded `class="dark"` from index.html

---

## 12. Icon System

**Library**: Lucide React (`^0.460.0`)

**Standard Size**: `h-4 w-4` (16×16px) for sidebar and inline icons

```tsx
import { LayoutDashboard, Package, DollarSign, Settings } from "lucide-react";

<LayoutDashboard className="h-4 w-4" />
<Package className="h-4 w-4 text-amazon" />
```

---

## 13. Chart Theming (Recharts)

Charts use the CSS variable colors for consistency:

```tsx
<LineChart>
  <Line stroke="hsl(30, 100%, 50%)" />   {/* Amazon Orange */}
  <Line stroke="hsl(210, 40%, 98%)" />    {/* Foreground */}
  <CartesianGrid stroke="hsl(217.2, 32.6%, 17.5%)" /> {/* Border */}
</LineChart>
```
