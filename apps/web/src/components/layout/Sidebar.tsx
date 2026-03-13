import { useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import {
  LayoutDashboard,
  TrendingUp,
  Bell,
  Settings,
  Play,
  ShoppingCart,
  Tag,
  CalendarDays,
  Warehouse,
  Megaphone,
  Brain,
  GitBranch,
  BarChart3,
  AlertTriangle,
  Database,
  FileSpreadsheet,
  ListChecks,
  FileText,
  Landmark,
  Receipt,
  ShieldCheck,
  Images,
  Upload,
  Boxes,
  ClipboardList,
  Container,
  ShieldAlert,
  PackagePlus,
  Gauge,
  ChevronDown,
  ChevronRight,
  Calculator,
  PieChart,
  Shield,
  Globe,
  Rocket,
  FlaskConical,
  Scale,
  Undo2,
  Repeat,
  Building2,
  Key,
  Briefcase,
  type LucideIcon,
} from "lucide-react";
import { cn } from "@/lib/utils";

/* ------------------------------------------------------------------ */
/*  Navigation structure                                               */
/* ------------------------------------------------------------------ */

interface NavItem {
  to: string;
  icon: LucideIcon;
  label: string;
}

interface NavGroup {
  key: string;
  label: string;
  icon: LucideIcon;
  prefix: string;          // auto-expand when route starts with this
  items: NavItem[];
}

type NavEntry = NavItem | NavGroup;

function isGroup(entry: NavEntry): entry is NavGroup {
  return "items" in entry;
}

const navigation: NavEntry[] = [
  { to: "/dashboard", icon: LayoutDashboard, label: "Dashboard" },

  {
    key: "executive",
    label: "Executive",
    icon: Shield,
    prefix: "/exec",
    items: [
      { to: "/exec/overview", icon: Shield, label: "Command Center" },
      { to: "/exec/products", icon: BarChart3, label: "Products" },
      { to: "/exec/marketplaces", icon: Globe, label: "Marketplaces" },
    ],
  },

  {
    key: "strategy",
    label: "Strategy",
    icon: Rocket,
    prefix: "/strategy",
    items: [
      { to: "/strategy/overview", icon: Rocket, label: "Growth Engine" },
      { to: "/strategy/opportunities", icon: TrendingUp, label: "Opportunities" },
      { to: "/strategy/playbooks", icon: ClipboardList, label: "Playbooks" },
      { to: "/strategy/market-expansion", icon: Globe, label: "Market Expansion" },
      { to: "/strategy/bundles", icon: PackagePlus, label: "Bundles" },
      { to: "/strategy/experiments", icon: FlaskConical, label: "Experiments" },
      { to: "/strategy/outcomes", icon: BarChart3, label: "Outcomes" },
      { to: "/strategy/learning", icon: Brain, label: "Learning" },
    ],
  },

  {
    key: "seasonality",
    label: "Seasonality",
    icon: CalendarDays,
    prefix: "/seasonality",
    items: [
      { to: "/seasonality/overview", icon: CalendarDays, label: "Dashboard" },
      { to: "/seasonality/map", icon: BarChart3, label: "Heatmap" },
      { to: "/seasonality/entities", icon: ListChecks, label: "Entities" },
      { to: "/seasonality/clusters", icon: Boxes, label: "Clusters" },
      { to: "/seasonality/opportunities", icon: TrendingUp, label: "Opportunities" },
      { to: "/seasonality/settings", icon: Settings, label: "Settings" },
    ],
  },

  {
    key: "profit",
    label: "Profit",
    icon: TrendingUp,
    prefix: "/profit",
    items: [
      { to: "/profit/overview", icon: PieChart, label: "Dashboard" },
      { to: "/profit/products", icon: BarChart3, label: "Products" },
      { to: "/profit/orders", icon: ShoppingCart, label: "Orders" },
      { to: "/profit/loss-orders", icon: AlertTriangle, label: "Loss Analysis" },
      { to: "/profit/fee-breakdown", icon: BarChart3, label: "Fee Breakdown" },
      { to: "/profit/simulator", icon: Calculator, label: "Price Simulator" },
      { to: "/profit/data-quality", icon: Database, label: "Data Quality" },
      { to: "/profit/tasks", icon: ListChecks, label: "Tasks" },
    ],
  },

  {
    key: "pricing",
    label: "Cennik & Plan",
    icon: Tag,
    prefix: "/pricing",
    items: [
      { to: "/pricing", icon: Tag, label: "Cennik & Buy Box" },
      { to: "/pricing/repricing", icon: Repeat, label: "Repricing Engine" },
      { to: "/planning", icon: CalendarDays, label: "Planowanie" },
    ],
  },

  {
    key: "inventory360",
    label: "Inventory 360",
    icon: Boxes,
    prefix: "/inventory",
    items: [
      { to: "/inventory/overview", icon: Gauge, label: "Dashboard" },
      { to: "/inventory/all", icon: Boxes, label: "Manage All" },
      { to: "/inventory/families", icon: GitBranch, label: "Families" },
      { to: "/inventory/drafts", icon: ClipboardList, label: "Drafts" },
      { to: "/inventory/jobs", icon: Play, label: "Jobs" },
      { to: "/inventory/settings", icon: Settings, label: "Settings" },
      { to: "/inventory/risk", icon: AlertTriangle, label: "Risk Engine" },
    ],
  },

  {
    key: "finance",
    label: "Finance",
    icon: Landmark,
    prefix: "/finance",
    items: [
      { to: "/finance/dashboard", icon: Landmark, label: "Dashboard" },
      { to: "/finance/ledger", icon: Receipt, label: "Ledger" },
      { to: "/finance/reconciliation", icon: ListChecks, label: "Reconciliation" },
    ],
  },

  {
    key: "tax",
    label: "Tax Compliance",
    icon: Scale,
    prefix: "/tax",
    items: [
      { to: "/tax/overview", icon: Scale, label: "Overview" },
      { to: "/tax/classification", icon: ListChecks, label: "VAT Classification" },
      { to: "/tax/oss", icon: Globe, label: "OSS / VIU-DO" },
      { to: "/tax/local-vat", icon: Landmark, label: "Local VAT" },
      { to: "/tax/fba-movements", icon: Container, label: "FBA Movements" },
      { to: "/tax/evidence", icon: ShieldCheck, label: "Evidence Control" },
      { to: "/tax/reconciliation", icon: Receipt, label: "Reconciliation" },
      { to: "/tax/filing-readiness", icon: BarChart3, label: "Filing Readiness" },
      { to: "/tax/audit-archive", icon: FileText, label: "Audit Archive" },
      { to: "/tax/settings", icon: Settings, label: "Settings" },
    ],
  },

  {
    key: "fba",
    label: "Magazyn & FBA",
    icon: Warehouse,
    prefix: "/fba",
    items: [
      { to: "/fba/overview", icon: Gauge, label: "FBA Overview" },
      { to: "/fba/inventory", icon: Boxes, label: "FBA Inventory" },
      { to: "/fba/replenishment", icon: ClipboardList, label: "Replenishment" },
      { to: "/fba/inbound", icon: Container, label: "Inbound" },
      { to: "/fba/aged-stranded", icon: ShieldAlert, label: "Aged / Stranded" },
      { to: "/fba/bundles", icon: PackagePlus, label: "Bundles" },
      { to: "/fba/kpi-scorecard", icon: BarChart3, label: "Scorecard" },
      { to: "/fba/returns", icon: Undo2, label: "Returns" },
      { to: "/fba/fee-audit", icon: Calculator, label: "Fee Audit" },
      { to: "/fba/refund-anomalies", icon: AlertTriangle, label: "Refund Anomalies" },
    ],
  },

  { to: "/ads", icon: Megaphone, label: "Reklamy" },

  {
    key: "content",
    label: "Content",
    icon: FileText,
    prefix: "/content",
    items: [
      { to: "/content/studio", icon: FileText, label: "Content Studio" },
      { to: "/content/compliance", icon: ShieldCheck, label: "Compliance" },
      { to: "/content/assets", icon: Images, label: "Zdjęcia & Pliki" },
      { to: "/content/publish", icon: Upload, label: "Publikacja" },
      { to: "/content/scores", icon: BarChart3, label: "Content Score" },
      { to: "/content/ab-testing", icon: FlaskConical, label: "A/B Testing" },
    ],
  },

  {
    key: "tools",
    label: "AI & Narzędzia",
    icon: Brain,
    prefix: "/ai",
    items: [
      { to: "/ai", icon: Brain, label: "AI Insights" },
      { to: "/families", icon: GitBranch, label: "Family Mapper" },
      { to: "/import-products", icon: FileSpreadsheet, label: "Import Products" },
    ],
  },

  {
    key: "operator",
    label: "Operator",
    icon: Briefcase,
    prefix: "/operator",
    items: [
      { to: "/operator/console", icon: Briefcase, label: "Konsola Operatora" },
      { to: "/operator/accounts", icon: Building2, label: "Account Hub" },
    ],
  },

  {
    key: "system",
    label: "System",
    icon: Bell,
    prefix: "/system",
    items: [
      { to: "/alerts", icon: Bell, label: "Alerty" },
      { to: "/jobs", icon: Play, label: "Jobs" },
      { to: "/system/netfox-health", icon: Database, label: "Netfox Health" },
      { to: "/system/guardrails", icon: ShieldCheck, label: "Guardrails" },
      { to: "/system/sqs-topology", icon: Repeat, label: "Event Topology" },
      { to: "/system/event-wiring", icon: GitBranch, label: "Event Wiring" },
    ],
  },
];

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

export default function Sidebar() {
  const location = useLocation();

  // Auto-expand groups whose prefix matches current route
  const initialOpen = new Set<string>();
  for (const entry of navigation) {
    if (isGroup(entry)) {
      const matchesPrefix = location.pathname.startsWith(entry.prefix);
      const matchesAny = entry.items.some(
        (i) => location.pathname === i.to || location.pathname.startsWith(i.to + "/"),
      );
      if (matchesPrefix || matchesAny) initialOpen.add(entry.key);
    }
  }

  const [open, setOpen] = useState<Set<string>>(initialOpen);

  const toggle = (key: string) =>
    setOpen((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  return (
    <aside className="flex w-56 flex-col border-r border-border bg-card">
      {/* Logo */}
      <div className="flex h-14 items-center gap-2 border-b border-border px-4">
        <ShoppingCart className="h-6 w-6 text-amazon" />
        <span className="text-sm font-semibold tracking-tight">
          Command Center
        </span>
      </div>

      {/* Navigation */}
      <nav className="flex-1 space-y-0.5 p-3 overflow-y-auto">
        {navigation.map((entry) =>
          isGroup(entry) ? (
            <SidebarGroup
              key={entry.key}
              group={entry}
              isOpen={open.has(entry.key)}
              onToggle={() => toggle(entry.key)}
            />
          ) : (
            <SidebarLink key={entry.to} item={entry} />
          ),
        )}
      </nav>

      {/* Footer */}
      <div className="border-t border-border p-3">
        <NavLink
          to="/system/settings"
          className="flex items-center gap-3 rounded-md px-3 py-2 text-sm text-muted-foreground hover:bg-accent hover:text-foreground"
        >
          <Settings className="h-4 w-4" />
          Settings
        </NavLink>
      </div>
    </aside>
  );
}

/* ------------------------------------------------------------------ */
/*  Sub-components                                                     */
/* ------------------------------------------------------------------ */

function SidebarLink({ item }: { item: NavItem }) {
  return (
    <NavLink
      to={item.to}
      end={item.to === "/profit"}
      className={({ isActive }) =>
        cn(
          "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
          isActive
            ? "bg-amazon/10 text-amazon"
            : "text-muted-foreground hover:bg-accent hover:text-foreground",
        )
      }
    >
      <item.icon className="h-4 w-4 shrink-0" />
      {item.label}
    </NavLink>
  );
}

function SidebarGroup({
  group,
  isOpen,
  onToggle,
}: {
  group: NavGroup;
  isOpen: boolean;
  onToggle: () => void;
}) {
  const location = useLocation();
  const isActive = group.items.some(
    (i) => location.pathname === i.to || location.pathname.startsWith(i.to + "/"),
  );

  const Chevron = isOpen ? ChevronDown : ChevronRight;

  return (
    <div>
      <button
        type="button"
        onClick={onToggle}
        className={cn(
          "flex w-full items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
          isActive
            ? "text-amazon"
            : "text-muted-foreground hover:bg-accent hover:text-foreground",
        )}
      >
        <group.icon className="h-4 w-4 shrink-0" />
        <span className="flex-1 text-left">{group.label}</span>
        <Chevron className="h-3.5 w-3.5 shrink-0 opacity-50" />
      </button>

      {isOpen && (
        <div className="ml-3 border-l border-border/50 pl-2 space-y-0.5">
          {group.items.map((item) => (
            <SidebarLink key={item.to} item={item} />
          ))}
        </div>
      )}
    </div>
  );
}
