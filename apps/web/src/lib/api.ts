import axios from "axios";
import type { AxiosError, InternalAxiosRequestConfig } from "axios";
import { useAuthStore } from "@/store/authStore";

export const api = axios.create({
  baseURL: "/api/v1",
  headers: { "Content-Type": "application/json" },
  timeout: 120_000,
});

// Attach Bearer token
api.interceptors.request.use((config) => {
  const token = useAuthStore.getState().accessToken;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// ---------------------------------------------------------------------------
// Refresh-token mutex: only ONE refresh request runs at a time.
// All other 401'd requests wait for the same promise, then retry.
// ---------------------------------------------------------------------------
let refreshPromise: Promise<string> | null = null;

async function refreshAccessToken(): Promise<string> {
  const { refreshToken, setTokens, logout } = useAuthStore.getState();
  if (!refreshToken) {
    logout();
    throw new Error("no_refresh_token");
  }
  try {
    const { data } = await axios.post("/api/v1/auth/refresh", {
      refresh_token: refreshToken,
    });
    setTokens(data.access_token, data.refresh_token);
    return data.access_token as string;
  } catch (refreshErr) {
    logout();
    throw refreshErr;
  }
}

// Auto-refresh on 401 (with mutex)
api.interceptors.response.use(
  (r) => r,
  async (err: AxiosError) => {
    const original = err.config as InternalAxiosRequestConfig & { _retry?: boolean };
    const status = err.response?.status;

    // 403 — permission denied (no retry)
    if (status === 403) {
      console.warn("[API] 403 Forbidden:", original?.url);
      return Promise.reject(err);
    }

    // Only attempt refresh on 401, if we haven't retried this request yet
    if (status !== 401 || !original || original._retry) {
      return Promise.reject(err);
    }

    original._retry = true;

    try {
      // Mutex: reuse an in-flight refresh, or start a new one.
      if (!refreshPromise) {
        refreshPromise = refreshAccessToken().finally(() => {
          refreshPromise = null;
        });
      }
      const newToken = await refreshPromise;
      original.headers.Authorization = `Bearer ${newToken}`;
      return api(original);
    } catch {
      // refresh failed — already logged out in refreshAccessToken()
      return Promise.reject(err);
    }
  }
);

// ---- API calls ----

export interface KPISummary {
  date_from: string;
  date_to: string;
  last_sync?: string | null;
  total_revenue_pln: number;
  total_orders: number;
  total_units: number;
  total_cm1_pln: number;
  total_cm1_percent: number;
  total_cm2_pln: number;
  total_cm2_percent: number;
  total_overhead_pln: number;
  total_net_profit_pln: number;
  total_net_profit_percent: number;
  total_ads_spend_pln: number;
  total_acos?: number | null;
  total_tacos?: number | null;
  avg_order_value_pln: number;
  total_courier_cost_pln: number;
  total_return_rate_pct?: number | null;
  total_refund_pln: number;
  total_refund_units: number;
  fbm_logistics_by_mkt: { mkt: string; avg_cost: number; coverage_pct: number; billing_pct: number; billing_avg_cost?: number }[];
  fbm_coverage_pct?: number | null;
  fbm_billing_pct?: number | null;
  fba_orders: number;
  fbm_orders: number;
  fba_units: number;
  fbm_units: number;
  fba_units_per_order?: number | null;
  fbm_units_per_order?: number | null;
  revenue_delta_pct?: number;
  orders_delta_pct?: number;
  cm1_delta_pct?: number;
  active_alerts_count: number;
  critical_alerts_count: number;
  by_marketplace: MarketplaceKPI[];
}

export interface MarketplaceKPI {
  marketplace_id: string;
  marketplace_code: string;
  revenue_pln: number;
  orders: number;
  units: number;
  cm1_pln: number;
  cm1_percent: number;
  cm2_pln: number;
  cm2_percent: number;
  overhead_pln: number;
  net_profit_pln: number;
  net_profit_percent: number;
  acos?: number;
  ads_spend_pln: number;
  avg_order_value_pln: number;
  courier_cost_pln: number;
  return_rate_pct?: number | null;
  refund_units: number;
  refund_pln: number;
}

export interface RevenueChartPoint {
  date: string;
  revenue_pln: number;
  cm1_pln: number;
  orders: number;
}

export interface OrderLine {
  sku?: string;
  asin?: string;
  title?: string;
  title_pl?: string;
  quantity: number;
  item_price?: number;
  currency: string;
  purchase_price_pln?: number;
  cogs_pln?: number;
  fba_fee_pln?: number;
  referral_fee_pln?: number;
}

export interface ProfitOrder {
  id: string;
  amazon_order_id: string;
  marketplace_id: string;
  marketplace_code?: string;
  purchase_date: string;
  status: string;
  fulfillment_channel?: string;
  order_total?: number;
  currency: string;
  revenue_pln?: number;
  cogs_pln?: number;
  amazon_fees_pln?: number;
  ads_cost_pln?: number;
  logistics_pln?: number;
  contribution_margin_pln?: number;
  cm1_percent?: number;
  lines: OrderLine[];
}

export interface Alert {
  id: string;
  title: string;
  detail?: string;
  detail_json?: Record<string, unknown>;
  context_json?: Record<string, unknown>;
  severity: "info" | "warning" | "critical";
  marketplace_id?: string;
  sku?: string;
  is_read: boolean;
  is_resolved: boolean;
  triggered_at: string;
}

export interface JobRun {
  id: string;
  job_type: string;
  status: string;
  progress_pct: number;
  progress_message?: string;
  records_processed?: number;
  error_message?: string;
  created_at: string;
  duration_seconds?: number;
}

export interface JobListResponse {
  total: number;
  items: JobRun[];
}

// KPI
export interface MarketplaceOption {
  marketplace_id: string;
  code: string;
  country: string;
  order_count: number;
}

export const getKPISummary = (params: Record<string, string>) =>
  api.get<KPISummary>("/kpi/summary", { params }).then((r) => r.data);

export const getRevenueChart = (params: Record<string, string>) =>
  api.get<{ points: RevenueChartPoint[] }>("/kpi/chart/revenue", { params }).then((r) => r.data);

export const getMarketplaces = () =>
  api.get<MarketplaceOption[]>("/kpi/marketplaces").then((r) => r.data);

export interface TopDriverItem {
  sku: string;
  asin: string | null;
  title: string;
  internal_sku: string | null;
  units: number;
  revenue_pln: number;
  cogs_pln: number;
  cm1_pln: number;
  cm1_percent: number;
}

export interface TopDriversResponse {
  date_from: string;
  date_to: string;
  drivers: TopDriverItem[];
  leaks: TopDriverItem[];
}

export const getTopDrivers = (params: Record<string, string>) =>
  api.get<TopDriversResponse>("/kpi/top-drivers", { params }).then((r) => r.data);

export interface DashboardAlert {
  id: string;
  title: string;
  detail?: string;
  severity: string;
  marketplace_id?: string;
  sku?: string;
  is_read: boolean;
  triggered_at: string | null;
}

export const getRecentAlerts = (limit = 5) =>
  api.get<DashboardAlert[]>("/kpi/recent-alerts", { params: { limit } }).then((r) => r.data);

export const exportProfitCSV = (params: Record<string, string>) =>
  api.get("/profit/export", { params, responseType: "blob" }).then((r) => {
    const url = window.URL.createObjectURL(new Blob([r.data]));
    const a = document.createElement("a");
    a.href = url;
    a.download = `profit_export.csv`;
    a.click();
    window.URL.revokeObjectURL(url);
  });

// Profit
export const getProfitOrders = (params: Record<string, string | number>) =>
  api.get<{ total: number; page: number; page_size: number; pages: number; items: ProfitOrder[] }>("/profit/orders", { params }).then((r) => r.data);

// Alerts
export const getAlerts = (params?: Record<string, unknown>) =>
  api.get<{ total: number; unread: number; critical_count: number; items: Alert[] }>("/alerts", { params }).then((r) => r.data);

export const markAlertRead = (id: string) =>
  api.post(`/alerts/${id}/read`).then((r) => r.data);

export const resolveAlert = (id: string) =>
  api.post(`/alerts/${id}/resolve`).then((r) => r.data);

// Alert Rules
export interface AlertRule {
  id: string;
  name: string;
  description?: string;
  rule_type: string;
  marketplace_id?: string;
  sku?: string;
  category?: string;
  threshold_value?: number;
  threshold_operator?: string;
  severity: string;
  is_active: boolean;
  created_by?: string;
  created_at: string;
}

export interface AlertRuleCreate {
  name: string;
  description?: string;
  rule_type: string;
  marketplace_id?: string;
  sku?: string;
  category?: string;
  threshold_value?: number;
  threshold_operator?: string;
  severity?: string;
  is_active?: boolean;
}

export const getAlertRules = () =>
  api.get<AlertRule[]>("/alerts/rules").then((r) => r.data);

export const createAlertRule = (payload: AlertRuleCreate) =>
  api.post<AlertRule>("/alerts/rules", payload).then((r) => r.data);

export const deleteAlertRule = (ruleId: string) =>
  api.delete(`/alerts/rules/${ruleId}`);

// Jobs
export const getJobs = (params?: Record<string, unknown>) =>
  api.get<{ total: number; items: JobRun[] }>("/jobs", { params }).then((r) => r.data);

export const runJob = (job_type: string, marketplace_id?: string) =>
  api.post<JobRun>("/jobs/run", { job_type, marketplace_id }).then((r) => r.data);

// Auth
export const login = (email: string, password: string) =>
  api.post<{ access_token: string; refresh_token: string }>("/auth/token", { email, password }).then((r) => r.data);

export const getMe = () =>
  api.get("/auth/me").then((r) => r.data);

// ---------------------------------------------------------------------------
// Pricing
// ---------------------------------------------------------------------------
export interface OfferPrice {
  id: number;
  marketplace_id: string;
  marketplace_code: string;
  sku: string;
  asin: string;
  current_price: number;
  currency: string;
  buybox_price?: number;
  has_buybox: boolean;
  status: string;
  fulfillment_channel: string;
  fba_fee?: number;
  referral_fee_rate?: number;
  updated_at: string;
}

export interface BuyBoxStats {
  marketplace_id: string;
  marketplace_code: string;
  total_active_offers: number;
  buybox_wins: number;
  buybox_win_pct: number;
  avg_price_gap?: number;
  active_offers: number;
  inactive_offers: number;
  fba_offers: number;
  fbm_offers: number;
  last_sync?: string;
}

export const getPricingOffers = (params?: Record<string, unknown>) =>
  api.get<{ items: OfferPrice[]; total: number; page: number; page_size: number }>(
    "/pricing/offers", { params }
  ).then((r) => r.data);

export const getBuyBoxStats = () =>
  api.get<BuyBoxStats[]>("/pricing/buybox-stats").then((r) => r.data);

// ---------------------------------------------------------------------------
// Buy Box Radar — Competitor Intelligence (Sprint 12)
// ---------------------------------------------------------------------------
export interface CompetitorOffer {
  seller_id: string;
  listing_price: number | null;
  shipping_price: number | null;
  landed_price: number | null;
  is_fba: boolean;
  is_buybox_winner: boolean;
  feedback_rating: number | null;
  observed_at: string;
}

export interface LandscapeEntry {
  asin: string;
  marketplace_id: string;
  total_sellers: number;
  fba_sellers: number;
  fbm_sellers: number;
  min_price: number | null;
  max_price: number | null;
  avg_price: number | null;
  buybox_winner_seller_id: string | null;
}

export interface CompetitorPriceDay {
  date: string;
  unique_sellers: number;
  min_price: number | null;
  max_price: number | null;
  avg_price: number | null;
  fba_offers: number;
  fbm_offers: number;
}

export interface BuyBoxDashboard {
  asins_tracked: number;
  total_snapshots: number;
  overall_win_rate: number | null;
  winners: unknown[];
  losers: unknown[];
  trend_direction: string;
}

export const getBuyBoxDashboard = (params?: { marketplace_id?: string; days?: number }) =>
  api.get<BuyBoxDashboard>("/buybox-radar/dashboard", { params }).then((r) => r.data);

export const getBuyBoxLandscape = (params?: { marketplace_id?: string; hours?: number; limit?: number }) =>
  api.get<{ count: number; landscape: LandscapeEntry[] }>("/buybox-radar/landscape", { params }).then((r) => r.data);

export const getCompetitorPriceHistory = (asin: string, marketplace_id: string, days = 30, seller_id?: string) =>
  api.get<{ asin: string; history: CompetitorPriceDay[] }>("/buybox-radar/competitors/" + encodeURIComponent(asin) + "/history", {
    params: { marketplace_id, days, ...(seller_id ? { seller_id } : {}) },
  }).then((r) => r.data);

export const getCompetitorLandscapeForAsin = (asin: string, marketplace_id: string, hours = 24) =>
  api.get<{ asin: string; offers: CompetitorOffer[] }>("/buybox-radar/competitors/" + encodeURIComponent(asin), {
    params: { marketplace_id, hours },
  }).then((r) => r.data);

export const getBuyBoxAlerts = (params?: { marketplace_id?: string; days?: number; limit?: number }) =>
  api.get<{ count: number; alerts: unknown[] }>("/buybox-radar/alerts", { params }).then((r) => r.data);

export const triggerCompetitorCapture = (marketplace_id: string, asin_limit = 50) =>
  api.post<{ marketplace_id: string; asins_sampled: number; offers_recorded: number; status: string }>(
    "/buybox-radar/capture-competitors", null, { params: { marketplace_id, asin_limit } }
  ).then((r) => r.data);

// ---------------------------------------------------------------------------
// Inventory Risk Engine (Sprint 13)
// ---------------------------------------------------------------------------
export interface InventoryRiskScore {
  seller_sku: string;
  asin: string | null;
  marketplace_id: string;
  score_date: string | null;
  stockout_prob_7d: number | null;
  stockout_prob_14d: number | null;
  stockout_prob_30d: number | null;
  days_cover: number | null;
  velocity_7d: number | null;
  velocity_30d: number | null;
  velocity_cv: number | null;
  units_available: number;
  overstock_holding_cost_pln: number | null;
  storage_fee_30d_pln: number | null;
  capital_tie_up_pln: number | null;
  excess_units: number;
  excess_value_pln: number | null;
  aging_risk_pln: number | null;
  aged_90_plus_units: number;
  aged_90_plus_value_pln: number | null;
  projected_aged_90_30d: number;
  risk_tier: string;
  risk_score: number;
}

export interface InventoryRiskWatchlistItem {
  seller_sku: string;
  asin: string | null;
  marketplace_id: string;
  stockout_prob_7d: number | null;
  stockout_prob_14d: number | null;
  stockout_prob_30d: number | null;
  days_cover: number | null;
  velocity_30d: number | null;
  units_available: number;
}

export interface InventoryRiskOverstockItem {
  seller_sku: string;
  asin: string | null;
  marketplace_id: string;
  overstock_holding_cost_pln: number | null;
  storage_fee_30d_pln: number | null;
  capital_tie_up_pln: number | null;
  excess_units: number;
  excess_value_pln: number | null;
  days_cover: number | null;
  velocity_30d: number | null;
}

export interface InventoryRiskDashboard {
  total_skus: number;
  critical: number;
  high: number;
  medium: number;
  low: number;
  avg_stockout_prob_7d: number | null;
  total_holding_cost_pln: number;
  total_aging_risk_pln: number;
  total_excess_value_pln: number;
  avg_risk_score: number | null;
}

export interface InventoryRiskHistoryDay {
  score_date: string;
  risk_score: number;
  risk_tier: string;
  stockout_prob_7d: number;
  overstock_total_pln: number;
  aging_risk_pln: number;
}

export const getInventoryRiskDashboard = (params?: { marketplace_id?: string; days?: number }) =>
  api.get<InventoryRiskDashboard>("/inventory-risk/dashboard", { params }).then((r) => r.data);

export const getInventoryRiskScores = (params?: {
  marketplace_id?: string; risk_tier?: string; limit?: number; offset?: number;
  sort_by?: string; sort_dir?: string;
}) =>
  api.get<{ items: InventoryRiskScore[]; total: number; limit: number; offset: number }>("/inventory-risk/scores", { params }).then((r) => r.data);

export const getInventoryRiskHistory = (seller_sku: string, marketplace_id: string, days = 30) =>
  api.get<{ seller_sku: string; history: InventoryRiskHistoryDay[] }>(
    "/inventory-risk/history/" + encodeURIComponent(seller_sku), { params: { marketplace_id, days } }
  ).then((r) => r.data);

export const getStockoutWatchlist = (params?: { marketplace_id?: string; threshold?: number; limit?: number }) =>
  api.get<{ count: number; threshold: number; items: InventoryRiskWatchlistItem[] }>("/inventory-risk/stockout-watchlist", { params }).then((r) => r.data);

export const getOverstockReport = (params?: { marketplace_id?: string; min_cost_pln?: number; limit?: number }) =>
  api.get<{ count: number; items: InventoryRiskOverstockItem[] }>("/inventory-risk/overstock-report", { params }).then((r) => r.data);

export const triggerInventoryRiskCompute = (marketplace_id?: string, target_date?: string) =>
  api.post<{ upserted: number; date: string }>(
    "/inventory-risk/compute", null, { params: { ...(marketplace_id ? { marketplace_id } : {}), ...(target_date ? { date: target_date } : {}) } }
  ).then((r) => r.data);

// ---------------------------------------------------------------------------
// Inventory Risk Engine — Sprint 14: Replenishment & Alerts
// ---------------------------------------------------------------------------
export interface ReplenishmentPlanItem {
  seller_sku: string;
  asin: string | null;
  marketplace_id: string;
  plan_date: string;
  risk_score: number;
  risk_tier: string;
  stockout_prob_7d: number | null;
  days_cover: number | null;
  velocity_7d: number | null;
  velocity_30d: number | null;
  velocity_trend: string;
  velocity_change_pct: number | null;
  suggested_reorder_qty: number;
  reorder_urgency: string;
  target_days: number;
  lead_time_days: number;
  safety_stock_days: number;
  estimated_stockout_date: string | null;
  overstock_holding_cost_pln: number | null;
  aging_risk_pln: number | null;
  units_available: number;
  acknowledged_at: string | null;
  acknowledged_by: string | null;
}

export interface RiskAlert {
  id: number;
  seller_sku: string;
  marketplace_id: string;
  alert_type: string;
  severity: string;
  title: string;
  detail: string | null;
  current_value: number | null;
  previous_value: number | null;
  threshold_value: number | null;
  risk_score: number | null;
  risk_tier: string | null;
  created_at: string;
  resolved_at: string | null;
}

export interface VelocityTrendDay {
  score_date: string;
  velocity_7d: number | null;
  velocity_30d: number | null;
  velocity_trend: string | null;
  velocity_change_pct: number | null;
  risk_score: number | null;
  risk_tier: string | null;
  days_cover: number | null;
}

export const getReplenishmentPlan = (params?: {
  marketplace_id?: string; urgency?: string; limit?: number; offset?: number;
  sort_by?: string; sort_dir?: string;
}) =>
  api.get<{ items: ReplenishmentPlanItem[]; total: number; limit: number; offset: number }>(
    "/inventory-risk/replenishment-plan", { params }
  ).then((r) => r.data);

export const acknowledgeReplenishment = (seller_sku: string, marketplace_id: string, acknowledged_by: string) =>
  api.post<{ acknowledged: boolean }>(
    "/inventory-risk/replenishment-plan/acknowledge", null,
    { params: { seller_sku, marketplace_id, acknowledged_by } }
  ).then((r) => r.data);

export const getRiskAlerts = (params?: {
  marketplace_id?: string; alert_type?: string; include_resolved?: boolean; limit?: number; offset?: number;
}) =>
  api.get<{ items: RiskAlert[]; total: number; limit: number; offset: number }>(
    "/inventory-risk/alerts", { params }
  ).then((r) => r.data);

export const resolveRiskAlert = (alert_id: number) =>
  api.post<{ resolved: boolean }>(`/inventory-risk/alerts/${alert_id}/resolve`).then((r) => r.data);

export const getVelocityTrends = (seller_sku: string, marketplace_id: string, days = 30) =>
  api.get<{ seller_sku: string; days: number; trends: VelocityTrendDay[] }>(
    "/inventory-risk/trends/" + encodeURIComponent(seller_sku), { params: { marketplace_id, days } }
  ).then((r) => r.data);

// ---------------------------------------------------------------------------
// Planning
// ---------------------------------------------------------------------------
export interface PlanLine {
  id: number;
  plan_id: number;
  marketplace_id: string;
  marketplace_code: string;
  target_revenue_pln: number;
  target_orders: number;
  target_acos_pct: number;
  target_cm_pct: number;
  budget_ads_pln: number;
  actual_revenue_pln?: number;
  actual_orders?: number;
  actual_acos_pct?: number;
  actual_cm_pct?: number;
  revenue_attainment_pct?: number;
}

export interface PlanMonth {
  id: number;
  year: number;
  month: number;
  month_label: string;
  status: string;
  total_target_revenue_pln: number;
  total_target_budget_ads_pln: number;
  total_actual_revenue_pln?: number;
  revenue_attainment_pct?: number;
  lines: PlanLine[];
  created_by?: string;
  created_at?: string;
}

export interface PlanLineCreate {
  marketplace_id: string;
  target_revenue_pln: number;
  target_orders: number;
  target_acos_pct: number;
  target_cm_pct: number;
  budget_ads_pln: number;
}

export interface PlanMonthCreate {
  year: number;
  month: number;
  lines: PlanLineCreate[];
}

export interface PlanVsActualRow {
  month_label: string;
  target_revenue_pln: number;
  actual_revenue_pln: number;
  revenue_attainment_pct: number;
  target_cm_pct: number;
  actual_cm_pct: number;
  target_acos_pct: number;
  actual_acos_pct: number;
}

export const getPlanMonths = (year?: number) =>
  api.get<PlanMonth[]>("/planning/months", { params: year ? { year } : undefined }).then((r) => r.data);

export const getPlanVsActual = (year: number) =>
  api.get<{ rows: PlanVsActualRow[]; ytd_target_revenue_pln: number; ytd_actual_revenue_pln: number; ytd_attainment_pct: number }>(
    "/planning/vs-actual", { params: { year } }
  ).then((r) => r.data);

export const createPlanMonth = (payload: PlanMonthCreate) =>
  api.post<PlanMonth>("/planning/months", payload).then((r) => r.data);

export const updatePlanStatus = (planId: number, status: string) =>
  api.patch<PlanMonth>(`/planning/months/${planId}/status`, { status }).then((r) => r.data);

export const deletePlanMonth = (planId: number) =>
  api.delete(`/planning/months/${planId}`).then((r) => r.data);

// ---------------------------------------------------------------------------
// Inventory
// ---------------------------------------------------------------------------
export interface InventoryItem {
  id: number;
  snapshot_date: string;
  marketplace_id: string;
  marketplace_code: string;
  sku: string;
  asin?: string;
  qty_fulfillable: number;
  qty_reserved: number;
  qty_inbound: number;
  qty_unfulfillable: number;
  qty_total: number;
  days_of_inventory?: number;
  velocity_30d?: number;
  inventory_value_pln?: number;
  status: string;
}

export interface InventorySummary {
  total_skus: number;
  critical_count: number;
  low_count: number;
  overstock_count: number;
  total_value_pln: number;
  avg_doi: number;
}

export interface OpenPO {
  sku: string;
  product_name?: string;
  order_date?: string;
  expected_delivery?: string;
  qty_ordered: number;
  qty_received: number;
  qty_open: number;
  days_until_delivery?: number;
}

export interface ReorderSuggestion {
  sku: string;
  product_name?: string;
  current_doi: number;
  velocity_30d: number;
  suggested_qty: number;
  suggested_order_date: string;
  urgency: string;
  reason: string;
}

export const getInventory = (params?: Record<string, unknown>) =>
  api.get<{ items: InventoryItem[]; total: number; page: number; page_size: number; summary: InventorySummary }>(
    "/inventory/", { params }
  ).then((r) => r.data);

export const getOpenPOs = (sku?: string) =>
  api.get<OpenPO[]>("/inventory/open-pos", { params: sku ? { sku } : undefined }).then((r) => r.data);

export const getReorderSuggestions = (marketplace_id?: string) =>
  api.get<ReorderSuggestion[]>("/inventory/reorder-suggestions", {
    params: marketplace_id ? { marketplace_id } : undefined,
  }).then((r) => r.data);

// ---------------------------------------------------------------------------
// Manage All Inventory
// ---------------------------------------------------------------------------

export interface ManageInventoryCoverageItem {
  key: string;
  label: string;
  pct: number;
  status: string;
  note?: string | null;
}

export interface ManageInventoryMetric {
  label: string;
  value: number;
  unit?: string | null;
  delta_pct?: number | null;
  status: string;
}

export interface ManageInventoryDecisionItem {
  sku: string;
  asin?: string | null;
  marketplace_id: string;
  marketplace_code: string;
  title_preferred?: string | null;
  brand?: string | null;
  category?: string | null;
  product_type?: string | null;
  fulfillment_badge: string;
  listing_status: string;
  suppression_reason?: string | null;
  local_parent_asin?: string | null;
  local_theme?: string | null;
  family_health: string;
  global_family_status: string;
  fba_on_hand: number;
  fba_available: number;
  inbound: number;
  reserved: number;
  fbm_on_hand: number;
  velocity_7d_units: number;
  velocity_30d_units: number;
  days_cover?: number | null;
  stockout_risk_badge: string;
  overstock_risk_badge: string;
  stranded_units: number;
  stranded_value_pln: number;
  aged_90_plus_units: number;
  aged_90_plus_value_pln: number;
  sessions_7d?: number | null;
  sessions_30d?: number | null;
  page_views_7d?: number | null;
  page_views_30d?: number | null;
  orders_7d: number;
  units_ordered_7d: number;
  unit_session_pct_7d?: number | null;
  unit_session_pct_30d?: number | null;
  sessions_delta_pct?: number | null;
  cvr_delta_pct?: number | null;
  demand_vs_supply_badge: string;
  traffic_coverage_flag: boolean;
  inventory_freshness?: string | null;
  last_change_at?: string | null;
  notes_indicator: boolean;
  internal_sku?: string | null;
  ean?: string | null;
  parent_asin?: string | null;
}

export interface ManageInventoryFamilySummary {
  marketplace_code: string;
  parent_asin: string;
  children_count: number;
  theme?: string | null;
  coverage_vs_de_pct?: number | null;
  missing_children: number;
  extra_children: number;
  conflicts_count: number;
  missing_required_attrs_count: number;
  confidence_avg?: number | null;
  status: string;
  updated_at?: string | null;
}

export interface ManageInventoryOverviewResponse {
  metrics: ManageInventoryMetric[];
  coverage: ManageInventoryCoverageItem[];
  top_high_demand_low_supply: ManageInventoryDecisionItem[];
  top_cvr_crash: ManageInventoryDecisionItem[];
  top_suppressed_high_sessions: ManageInventoryDecisionItem[];
  recently_changed_families: ManageInventoryFamilySummary[];
  generated_at: string;
}

export interface ManageInventoryListResponse {
  items: ManageInventoryDecisionItem[];
  total: number;
  snapshot_date?: string | null;
  coverage: ManageInventoryCoverageItem[];
}

export interface ManageInventoryTimelinePoint {
  date: string;
  sessions?: number | null;
  page_views?: number | null;
  units: number;
  orders: number;
  revenue: number;
  unit_session_pct?: number | null;
  on_hand?: number | null;
  available?: number | null;
  inbound?: number | null;
}

export interface ManageInventorySkuDetailResponse {
  item: ManageInventoryDecisionItem;
  inventory_timeline: ManageInventoryTimelinePoint[];
  traffic_timeline: ManageInventoryTimelinePoint[];
  family_context: Record<string, unknown>;
  issues: string[];
  change_history: Array<{
    event_type: string;
    actor?: string | null;
    payload_json: Record<string, unknown>;
    created_at: string;
  }>;
  coverage: ManageInventoryCoverageItem[];
}

export interface ManageInventoryFamilyChildItem {
  child_asin?: string | null;
  child_sku?: string | null;
  master_key?: string | null;
  key_type?: string | null;
  variant_attributes: Record<string, unknown>;
  current_parent_asin?: string | null;
  proposed_parent_asin?: string | null;
  match_type?: string | null;
  confidence?: number | null;
  warnings: string[];
}

export interface ManageInventoryFamilyListResponse {
  items: ManageInventoryFamilySummary[];
  total: number;
}

export interface ManageInventoryFamilyDetailResponse {
  marketplace_code: string;
  parent_asin: string;
  theme?: string | null;
  status: string;
  current_children: ManageInventoryFamilyChildItem[];
  proposed_children: ManageInventoryFamilyChildItem[];
  coverage_vs_de_pct?: number | null;
  issues: string[];
}

export interface ManageInventoryDraftItem {
  id: string;
  draft_type: string;
  marketplace_id?: string | null;
  marketplace_code?: string | null;
  affected_parent_asin?: string | null;
  affected_sku?: string | null;
  validation_status: string;
  approval_status: string;
  apply_status: string;
  created_by?: string | null;
  created_at: string;
  approved_by?: string | null;
  approved_at?: string | null;
  apply_started_at?: string | null;
  applied_at?: string | null;
  rolled_back_at?: string | null;
  payload_json: Record<string, unknown>;
  snapshot_before_json: Record<string, unknown>;
  snapshot_after_json: Record<string, unknown>;
  validation_errors: string[];
}

export interface ManageInventoryDraftListResponse {
  items: ManageInventoryDraftItem[];
  total: number;
}

export interface ManageInventoryDraftActionResponse {
  draft: ManageInventoryDraftItem;
  events: Array<{
    event_type: string;
    actor?: string | null;
    payload_json: Record<string, unknown>;
    created_at: string;
  }>;
}

export interface ManageInventoryJobItem {
  id: string;
  job_type: string;
  status: string;
  progress_pct: number;
  progress_message?: string | null;
  records_processed?: number | null;
  error_message?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  created_at?: string | null;
  duration_seconds?: number | null;
}

export interface ManageInventoryJobListResponse {
  items: ManageInventoryJobItem[];
  total: number;
  latest_by_type: ManageInventoryJobItem[];
}

export interface ManageInventorySettings {
  thresholds: Record<string, number>;
  theme_requirements: Record<string, string[]>;
  apply_safety: Record<string, unknown>;
  traffic_schedule: Record<string, unknown>;
  saved_views_enabled: boolean;
  updated_at?: string | null;
}

export interface ManageInventoryDraftCreatePayload {
  draft_type: string;
  marketplace_id?: string | null;
  affected_parent_asin?: string | null;
  affected_sku?: string | null;
  payload_json?: Record<string, unknown>;
  snapshot_before_json?: Record<string, unknown>;
  created_by?: string | null;
}

export const getManageInventoryOverview = (params?: Record<string, unknown>) =>
  api.get<ManageInventoryOverviewResponse>("/inventory/overview", { params }).then((r) => r.data);

export const getManageInventoryAll = (params?: Record<string, unknown>) =>
  api.get<ManageInventoryListResponse>("/inventory/all", { params }).then((r) => r.data);

export const getManageInventorySkuDetail = (sku: string, marketplace_id?: string) =>
  api.get<ManageInventorySkuDetailResponse>(`/inventory/sku/${encodeURIComponent(sku)}`, {
    params: marketplace_id ? { marketplace_id } : undefined,
  }).then((r) => r.data);

export const getManageInventoryFamilies = (params?: Record<string, unknown>) =>
  api.get<ManageInventoryFamilyListResponse>("/inventory/families", { params }).then((r) => r.data);

export const getManageInventoryFamilyDetail = (parentAsin: string, marketplace?: string) =>
  api.get<ManageInventoryFamilyDetailResponse>(`/inventory/families/${encodeURIComponent(parentAsin)}`, {
    params: marketplace ? { marketplace } : undefined,
  }).then((r) => r.data);

export const getManageInventoryDrafts = () =>
  api.get<ManageInventoryDraftListResponse>("/inventory/drafts").then((r) => r.data);

export const createManageInventoryDraft = (payload: ManageInventoryDraftCreatePayload) =>
  api.post<ManageInventoryDraftItem>("/inventory/drafts", payload).then((r) => r.data);

export const validateManageInventoryDraft = (draftId: string) =>
  api.post<ManageInventoryDraftActionResponse>(`/inventory/drafts/${draftId}/validate`).then((r) => r.data);

export const approveManageInventoryDraft = (draftId: string) =>
  api.post<ManageInventoryDraftActionResponse>(`/inventory/drafts/${draftId}/approve`).then((r) => r.data);

export const applyManageInventoryDraft = (draftId: string) =>
  api.post<ManageInventoryDraftActionResponse>(`/inventory/drafts/${draftId}/apply`).then((r) => r.data);

export const rollbackManageInventoryDraft = (draftId: string) =>
  api.post<ManageInventoryDraftActionResponse>(`/inventory/drafts/${draftId}/rollback`).then((r) => r.data);

export const getManageInventoryJobs = () =>
  api.get<ManageInventoryJobListResponse>("/inventory/jobs").then((r) => r.data);

export const runManageInventoryJob = (
  job_type:
    | "inventory_sync_listings"
    | "inventory_sync_snapshots"
    | "inventory_sync_sales_traffic"
    | "inventory_compute_rollups"
    | "inventory_run_alerts",
) => api.post<ManageInventoryJobItem>("/inventory/jobs/run", null, { params: { job_type } }).then((r) => r.data);

export const getManageInventorySettings = () =>
  api.get<ManageInventorySettings>("/inventory/settings").then((r) => r.data);

export const updateManageInventorySettings = (payload: Partial<ManageInventorySettings>) =>
  api.put<ManageInventorySettings>("/inventory/settings", payload).then((r) => r.data);

// ---------------------------------------------------------------------------
// Ads
// ---------------------------------------------------------------------------
export interface AdsSummary {
  period_days: number;
  total_spend_pln: number;
  total_sales_pln: number;
  total_orders: number;
  total_clicks?: number;
  avg_acos: number;
  avg_roas: number;
  avg_cpc: number;
  avg_ctr: number;
}

export interface AdsChartPoint {
  report_date: string;
  spend_pln: number;
  sales_pln: number;
  acos: number;
  roas: number;
  orders: number;
}

export interface TopCampaign {
  campaign_id: string;
  campaign_name: string;
  marketplace_code: string;
  total_spend_pln: number;
  total_sales_pln: number;
  avg_acos: number;
  avg_roas: number;
  orders: number;
  efficiency_score: number;
}

export const getAdsSummary = (days = 30, marketplace_id?: string) =>
  api.get<AdsSummary>("/ads/summary", { params: { days, ...(marketplace_id ? { marketplace_id } : {}) } }).then((r) => r.data);

export const getAdsChart = (days = 30, marketplace_id?: string) =>
  api.get<{ points: AdsChartPoint[] }>("/ads/chart", { params: { days, ...(marketplace_id ? { marketplace_id } : {}) } }).then((r) => r.data);

export const getTopCampaigns = (days = 30, marketplace_id?: string) =>
  api.get<TopCampaign[]>("/ads/top-campaigns", { params: { days, ...(marketplace_id ? { marketplace_id } : {}) } }).then((r) => r.data);

// ---------------------------------------------------------------------------
// AI Recommendations
// ---------------------------------------------------------------------------
export interface AIRecommendation {
  id: number;
  rec_type: string;
  title: string;
  summary: string;
  action_items: string[];
  confidence_score: number;
  model_used: string;
  status: string;
  sku?: string;
  marketplace_id?: string;
  expected_impact_pln?: number;
  created_at: string;
}

export interface AIInsightSummary {
  total_recommendations: number;
  new_count: number;
  accepted_count: number;
  dismissed_count: number;
  total_expected_impact_pln: number;
  top_rec?: AIRecommendation;
  last_generated_at?: string;
}

export const getAIRecommendations = (params?: Record<string, unknown>) =>
  api.get<{ items: AIRecommendation[]; total: number; new_count: number }>(
    "/ai/recommendations", { params }
  ).then((r) => r.data);

export const getAISummary = () =>
  api.get<AIInsightSummary>("/ai/summary").then((r) => r.data);

export const generateAIRec = (rec_type: string, sku?: string, marketplace_id?: string) =>
  api.post<AIRecommendation>("/ai/generate", { rec_type, sku, marketplace_id }).then((r) => r.data);

export const updateAIRecStatus = (id: number, status: "accepted" | "dismissed") =>
  api.patch<AIRecommendation>(`/ai/recommendations/${id}`, { status }).then((r) => r.data);

// ---------------------------------------------------------------------------
// Family Mapper
// ---------------------------------------------------------------------------
export interface FamilySummary {
  id: number;
  de_parent_asin: string;
  brand: string | null;
  category: string | null;
  product_type: string | null;
  variation_theme_de: string | null;
  children_count: number;
  marketplaces_mapped: number;
  de_sales_qty: number;
}

export interface FamilyListResponse {
  items: FamilySummary[];
  total: number;
  page: number;
  page_size: number;
}

export interface FamilyChild {
  id: number;
  master_key: string;
  key_type: string;
  de_child_asin: string;
  sku_de: string | null;
  ean_de: string | null;
  attributes: Record<string, string> | null;
}

export interface ChildMarketLink {
  global_family_id: number;
  master_key: string;
  marketplace: string;
  target_child_asin: string | null;
  current_parent_asin: string | null;
  match_type: string;
  confidence: number;
  status: string;
  reasons: string[] | null;
}

export interface FamilyCoverage {
  global_family_id: number;
  marketplace: string;
  de_children_count: number;
  matched_children_count: number;
  coverage_pct: number;
  missing_children_count: number;
  extra_children_count: number;
  theme_mismatch: boolean;
  confidence_avg: number;
}

export interface FamilyIssue {
  id: number;
  global_family_id: number;
  marketplace: string | null;
  issue_type: string;
  severity: string;
  payload: Record<string, unknown> | null;
}

export interface FamilyDetail {
  id: number;
  de_parent_asin: string;
  brand: string | null;
  category: string | null;
  product_type: string | null;
  variation_theme_de: string | null;
  created_at: string;
  children: FamilyChild[];
  market_links: { marketplace: string; target_parent_asin: string | null; status: string; confidence_avg: number }[];
}

export interface FixPackage {
  id: number;
  marketplace: string;
  global_family_id: number;
  action_plan: Record<string, unknown>;
  status: string;
  generated_at: string | null;
  approved_by: string | null;
  approved_at: string | null;
  applied_at: string | null;
}

export interface FixPackageListResponse {
  items: FixPackage[];
  total: number;
  page: number;
  page_size: number;
}

export interface ReviewQueueItem {
  global_family_id: number;
  de_parent_asin: string;
  brand: string | null;
  marketplace: string;
  master_key: string;
  de_child_asin: string | null;
  target_child_asin: string | null;
  match_type: string;
  confidence: number;
  status: string;
}

export interface ReviewQueueResponse {
  items: ReviewQueueItem[];
  total: number;
  page: number;
  page_size: number;
}

export interface MarketplaceInfo {
  marketplace_id: string;
  code: string;
  name: string;
  currency: string;
  tz: string;
}

export interface TriggerResult {
  status: string;
  result: Record<string, unknown>;
}

export interface RebuildStatus {
  running: boolean;
  phase: string;
  detail?: string;
}

export const getFamilies = (params?: Record<string, unknown>) =>
  api.get<FamilyListResponse>("/families", { params }).then((r) => r.data);

export const getFamily = (id: number) =>
  api.get<FamilyDetail>(`/families/${id}`).then((r) => r.data);

export const getFamilyChildren = (id: number) =>
  api.get<FamilyChild[]>(`/families/${id}/children`).then((r) => r.data);

export const getFamilyLinks = (id: number, marketplace?: string) =>
  api.get<ChildMarketLink[]>(`/families/${id}/links`, { params: marketplace ? { marketplace } : {} }).then((r) => r.data);

export const updateLinkStatus = (familyId: number, body: { status: string; master_key: string; marketplace: string }) =>
  api.put(`/families/${familyId}/links/status`, body).then((r) => r.data);

export const getFamilyCoverage = (id: number) =>
  api.get<FamilyCoverage[]>(`/families/${id}/coverage`).then((r) => r.data);

export const getFamilyIssues = (id: number) =>
  api.get<FamilyIssue[]>(`/families/${id}/issues`).then((r) => r.data);

export const getRebuildStatus = () =>
  api.get<RebuildStatus>("/families/trigger/rebuild-status").then((r) => r.data);

export const triggerRebuildDE = (maxParents = 200, brand?: string, onlyMissing?: boolean) =>
  api.post<TriggerResult>("/families/trigger/rebuild-de", null, {
    params: {
      max_parents: maxParents,
      ...(brand ? { brand } : {}),
      ...(onlyMissing !== undefined ? { only_missing: onlyMissing } : {}),
    },
  }).then((r) => r.data);

export const triggerSyncMP = (marketplaceIds?: string, familyIds?: string) =>
  api.post<TriggerResult>("/families/trigger/sync-mp", null, {
    params: {
      ...(marketplaceIds ? { marketplace_ids: marketplaceIds } : {}),
      ...(familyIds ? { family_ids: familyIds } : {}),
    },
  }).then((r) => r.data);

export const triggerMatching = (marketplaceIds?: string, familyIds?: string) =>
  api.post<TriggerResult>("/families/trigger/matching", null, {
    params: {
      ...(marketplaceIds ? { marketplace_ids: marketplaceIds } : {}),
      ...(familyIds ? { family_ids: familyIds } : {}),
    },
  }).then((r) => r.data);

export const getReviewQueue = (params?: Record<string, unknown>) =>
  api.get<ReviewQueueResponse>("/families/review", { params }).then((r) => r.data);

export const getFixPackages = (params?: Record<string, unknown>) =>
  api.get<FixPackageListResponse>("/families/fix-packages", { params }).then((r) => r.data);

export const generateFixPackages = (familyId?: number, marketplace?: string) =>
  api.post<TriggerResult>("/families/fix-packages/generate", null, {
    params: { ...(familyId ? { family_id: familyId } : {}), ...(marketplace ? { marketplace } : {}) },
  }).then((r) => r.data);

export const approveFixPackage = (pkgId: number, approvedBy: string) =>
  api.post(`/families/fix-packages/${pkgId}/approve`, { approved_by: approvedBy }).then((r) => r.data);

export const getFamilyMarketplaces = () =>
  api.get<MarketplaceInfo[]>("/families/marketplaces").then((r) => r.data);

// ---------------------------------------------------------------------------
// Family Restructure Analysis
// ---------------------------------------------------------------------------
export interface RestructureChildDE {
  asin: string;
  master_key: string;
  key_type: string;
  sku_de: string | null;
  ean_de: string | null;
  attributes: Record<string, string> | null;
}

export interface RestructureChildTarget {
  asin: string;
  sku: string | null;
  ean: string | null;
  current_parent_asin: string | null;
  variation_theme: string | null;
  attributes: Record<string, string> | null;
  reason?: string;
  current_parent?: string | null;
}

export interface RestructureForeignParent {
  parent_asin: string;
  children_count: number;
  children_asins: string[];
}

export interface RestructureAction {
  action: string;
  target?: string;
  child_asin?: string;
  from_parent?: string | null;
  to_parent?: string;
  marketplace: string;
  marketplace_id: string;
  affected_children?: number;
  note: string;
}

export interface RestructureAnalysis {
  verdict: "aligned" | "needs_restructure" | "no_data";
  marketplace: string;
  marketplace_id: string;
  de_canonical: {
    family_id: number;
    de_parent_asin: string;
    brand: string | null;
    category: string | null;
    product_type: string | null;
    variation_theme_de: string | null;
    children: RestructureChildDE[];
    children_count: number;
  };
  target_state: {
    children_found: number;
    children_aligned?: number;
    children_misaligned?: number;
    parent_asins: Record<string, number>;
    variation_themes?: string[];
    children: RestructureChildTarget[];
  };
  foreign_parents?: RestructureForeignParent[];
  children_to_reassign?: RestructureChildTarget[];
  missing_children?: RestructureChildDE[];
  extra_children?: RestructureChildTarget[];
  summary: string;
  actions: RestructureAction[];
  error?: string;
}

export interface RestructureAllResult {
  family_id: number;
  total_marketplaces: number;
  aligned: number;
  needs_restructure: number;
  no_data: number;
  results: Record<string, RestructureAnalysis>;
}

export const analyzeRestructure = (familyId: number, marketplaceId: string) =>
  api.post<RestructureAnalysis>(`/families/${familyId}/analyze-restructure`, null, {
    params: { marketplace_id: marketplaceId },
  }).then((r) => r.data);

export const analyzeRestructureAll = (familyId: number) =>
  api.post<RestructureAllResult>(`/families/${familyId}/analyze-restructure-all`).then((r) => r.data);

// Execute Restructure

export interface ExecuteRestructureStep {
  action: string;
  asin?: string;
  sku?: string | null;
  status: string;
  reason?: string;
  error?: string;
  from_parent?: string;
  to_parent?: string;
  to_parent_sku?: string;
  de_parent_sku?: string;
  children_count?: number;
  submission_id?: string;
  product_type?: string;
  issues?: unknown[];
  // variation theme validation
  desired_theme?: string;
  effective_theme?: string;
  allowed_themes?: string[];
  // translation
  target_language?: string;
  translated_fields?: string[];
  // child attr audit
  sample_checked?: number;
  missing_color?: number;
  missing_size?: number;
  children?: { sku: string; color?: string; size?: string; has_color: boolean; has_size: boolean; error?: string }[];
  // PIM enrichment
  dry_run?: boolean;
  total_missing?: number;
  pim_found?: number;
  patched?: number;
}

export interface ExecuteRestructureResult {
  status: string;
  dry_run?: boolean;
  family_id?: number;
  marketplace?: string;
  marketplace_id?: string;
  de_parent_asin?: string;
  de_parent_sku?: string;
  parent_on_target?: boolean;
  parent_skus_used?: string[];
  product_type_detected?: string;
  variation_theme?: string;
  child_attr_audit?: {
    missing_color: number;
    missing_size: number;
    sample_checked: number;
  };
  pim_enrichment?: {
    total_missing: number;
    pim_found: number;
    patched: number;
  } | null;
  total_steps?: number;
  children_planned?: number;
  children_actionable?: number;
  children_skipped?: number;
  errors: number;
  steps: ExecuteRestructureStep[];
  analysis?: RestructureAnalysis;
  message?: string;
  error?: string;
}

export interface ExecuteRestructureStartResponse {
  status: "started";
  run_id: string;
}

export interface ExecuteRestructureRunStatus {
  run_id: string;
  family_id: number;
  marketplace_id: string;
  marketplace?: string;
  dry_run: boolean;
  status: string;
  progress_pct: number;
  children_total: number;
  children_done: number;
  progress_message?: string;
  result?: ExecuteRestructureResult | null;
  error_message?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  finished_at?: string | null;
}

export const executeRestructure = (
  familyId: number,
  marketplaceId: string,
  dryRun: boolean = false,
) =>
  api.post<ExecuteRestructureResult>(
    `/families/${familyId}/execute-restructure`,
    null,
    { params: { marketplace_id: marketplaceId, dry_run: dryRun } },
  ).then((r) => r.data);

export const executeRestructureStart = (
  familyId: number,
  marketplaceId: string,
  dryRun: boolean = false,
) =>
  api.post<ExecuteRestructureStartResponse>(
    `/families/${familyId}/execute-restructure/start`,
    null,
    { params: { marketplace_id: marketplaceId, dry_run: dryRun } },
  ).then((r) => r.data);

export const getExecuteRestructureStatus = (
  familyId: number,
  marketplaceId: string,
  runId?: string,
) =>
  api.get<ExecuteRestructureRunStatus>(
    `/families/${familyId}/execute-restructure/status`,
    { params: { marketplace_id: marketplaceId, ...(runId ? { run_id: runId } : {}) } },
  ).then((r) => r.data);

// ---------------------------------------------------------------------------
// Import Products
// ---------------------------------------------------------------------------
export interface ImportProductItem {
  id: number;
  sku: string;
  nazwa_pelna?: string;
  kod_k?: string;
  kod_importu?: string;
  aktywny?: boolean;
  data_pierwszej_dostawy?: string;
  stan_magazynowy?: number;
  w_tym_fba?: number;
  sprzedaz_30d?: number;
  amazon_30d?: number;
  fba_30d?: number;
  allegro_30d?: number;
  sklep_30d?: number;
  inne_30d?: number;
  zasieg_dni?: number;
  estymacja_braku_stanu?: string;
  dynamika_10_30?: number;
  data_ostatniej_dostawy?: string;
  ilosc_ostatniej_dostawy?: number;
  cena_zakupu?: number;
  wartosc_magazynu?: number;
  srednia_cena_sprzedazy_30d?: number;
  srednia_marza?: number;
  marza?: number;
  miejsc_paletowych?: number;
  koszt_skladowania_1szt_30d?: number;
  koszt_skladowania_zapasu_30d?: number;
  nasycenie_12m?: number;
  data_dostawy?: string;
  tempo_pokrycie_150d?: number;
  sprzedaz_12m?: number;
  filtr?: string;
  mix?: string;
  is_import: boolean;
  uploaded_at: string;
  updated_at: string;
  // Amazon metrics (last 30 days, same logic as profit engine)
  amz_units_30d?: number;
  amz_orders_30d?: number;
  amz_revenue_pln_30d?: number;
  amz_cogs_pln_30d?: number;
  amz_fees_pln_30d?: number;
  amz_cm1_pln_30d?: number;
  amz_cm1_pct_30d?: number;
  amz_avg_price_pln?: number;
  amz_cogs_coverage_pct?: number;
  amz_lines_with_cogs?: number;
  amz_total_lines?: number;
}

export interface ImportProductsSummary {
  total_products: number;
  active_count: number;
  // Holding (CEO Excel)
  holding_total_stock: number;
  holding_stock_value: number;
  holding_avg_margin: number;
  holding_sales_30d: number;
  // Amazon (our data, last 30 days)
  amz_units_30d: number;
  amz_orders_30d: number;
  amz_revenue_30d: number;
  amz_cogs_30d: number;
  amz_fees_30d: number;
  amz_cm1_30d: number;
  amz_cm1_pct_30d: number;
  amz_products_with_sales: number;
}

export interface ImportFilterOptions {
  kod_importu: string[];
}

export interface ImportUploadResult {
  status: string;
  message: string;
  inserted: number;
  updated: number;
  total: number;
  filename: string;
}

export interface ImportSkusResponse {
  skus: string[];
  count: number;
}

export const uploadImportProducts = (file: File) => {
  const formData = new FormData();
  formData.append("file", file);
  return api
    .post<ImportUploadResult>("/import-products/upload", formData, {
      headers: { "Content-Type": "multipart/form-data" },
    })
    .then((r) => r.data);
};

export const getImportProducts = (params?: Record<string, unknown>) =>
  api
    .get<{
      items: ImportProductItem[];
      total: number;
      page: number;
      page_size: number;
      pages: number;
    }>("/import-products", { params })
    .then((r) => r.data);

export const getImportProductsSummary = () =>
  api.get<ImportProductsSummary>("/import-products/summary").then((r) => r.data);

export const getImportFilterOptions = () =>
  api.get<ImportFilterOptions>("/import-products/filter-options").then((r) => r.data);

export const getImportSkus = () =>
  api.get<ImportSkusResponse>("/import-products/skus").then((r) => r.data);

// ---------------------------------------------------------------------------
// Profit V2 (product profit table)
// ---------------------------------------------------------------------------
export interface ProductProfitItem {
  entity_type?: "sku" | "asin" | "parent";
  group_key?: string | null;
  sku: string;
  sample_sku?: string | null;
  asin?: string;
  parent_asin?: string | null;
  marketplace_id: string;
  marketplace_code: string;
  title?: string;
  brand?: string;
  category?: string;
  internal_sku?: string;
  fulfillment_channel: string;
  units: number;
  order_count: number;
  sku_count?: number;
  child_count?: number;
  revenue_pln: number;
  shipping_charge_pln?: number;
  cogs_per_unit: number;
  fees_per_unit: number;
  revenue_per_unit: number;
  cogs_pln: number;
  amazon_fees_pln: number;
  fba_fee_pln: number;
  referral_fee_pln: number;
  logistics_pln?: number;
  cm1_profit: number;
  cm1_percent: number;
  ads_cost_pln?: number;
  returns_net_pln?: number;
  refund_gross_pln?: number;
  return_handling_pln?: number;
  fba_storage_fee_pln?: number;
  fba_aged_fee_pln?: number;
  fba_removal_fee_pln?: number;
  fba_liquidation_fee_pln?: number;
  refund_finance_pln?: number;
  shipping_surcharge_pln?: number;
  fba_inbound_fee_pln?: number;
  promo_cost_pln?: number;
  warehouse_loss_pln?: number;
  amazon_other_fee_pln?: number;
  cm2_profit?: number;
  cm2_percent?: number;
  overhead_allocated_pln?: number;
  overhead_allocation_method?: string;
  overhead_confidence_pct?: number;
  np_profit?: number;
  np_percent?: number;
  cogs_coverage_pct: number;
  fees_coverage_pct: number;
  shipping_match_pct?: number;
  finance_match_pct?: number;
  return_rate?: number;
  tacos?: number;
  days_of_cover?: number;
  confidence_score: number;
  loss_orders_pct: number;
  flags?: string[];
  is_import?: boolean;
  refund_orders?: number;
  refund_units?: number;
  refund_cost_pln?: number;
  return_cogs_recovered_pln?: number;
  return_cogs_write_off_pln?: number;
  return_cogs_pending_pln?: number;
  cm1_adjusted?: number;
}

export interface ProductProfitSummary {
  total_revenue_pln: number;
  total_cogs_pln: number;
  total_fees_pln: number;
  total_cm1_pln: number;
  total_cm1_pct: number;
  total_ads_cost_pln?: number;
  total_logistics_pln?: number;
  total_cm2_pln?: number;
  total_cm2_pct?: number;
  total_np_pln?: number;
  total_np_pct?: number;
  total_returns_net_pln?: number;
  total_refund_gross_pln?: number;
  total_return_handling_pln?: number;
  total_fba_storage_fee_pln?: number;
  total_fba_aged_fee_pln?: number;
  total_fba_removal_fee_pln?: number;
  total_fba_liquidation_fee_pln?: number;
  total_overhead_allocated_pln?: number;
  overhead_allocation_method?: string;
  overhead_confidence_pct?: number;
  total_units: number;
  avg_confidence: number;
  // Refund info — Shipped+refund orders (included in profit as cost)
  refund_shipped_orders?: number;
  refund_shipped_units?: number;
  refund_shipped_cost_pln?: number;
  // Refund info — Return status orders (excluded from Shipped filter)
  refund_orders_excluded?: number;
  refund_full_count?: number;
  refund_partial_count?: number;
  refund_total_pln?: number;
  // Return tracker — aggregate COGS impact
  total_return_cogs_recovered_pln?: number;
  total_return_cogs_write_off_pln?: number;
  total_return_cogs_pending_pln?: number;
}

export interface ProductProfitTableResponse {
  total: number;
  page: number;
  page_size: number;
  pages: number;
  summary: ProductProfitSummary;
  items: ProductProfitItem[];
  warnings?: string[];
}

export const getProductProfitTable = (params: Record<string, unknown>) =>
  api
    .get<ProductProfitTableResponse>("/profit/v2/products", { params })
    .then((r) => r.data);

export interface ProductWhatIfItem {
  entity_type?: "offer" | "asin" | "parent";
  group_key?: string | null;
  sku: string;
  sample_sku?: string | null;
  asin?: string | null;
  parent_asin?: string | null;
  marketplace_id: string;
  marketplace_code: string;
  title?: string | null;
  brand?: string | null;
  category?: string | null;
  internal_sku?: string | null;
  fulfillment_channel: string;
  offer_status: string;
  offer_currency: string;
  offer_price: number;
  offer_price_pln: number;
  scenario_qty: number;
  offer_count?: number;
  sku_count?: number;
  child_count?: number;
  suggested_pack_qty: number;
  packages_count: number;
  plan_logistics_pln: number;
  observed_logistics_pln: number;
  decision_logistics_pln: number;
  logistics_gap_pct?: number | null;
  logistics_decision_rule: string;
  logistics_plan_source: string;
  logistics_observed_source: string;
  logistics_observed_samples: number;
  execution_drift: boolean;
  estimated_shipping_charge_pln: number;
  estimated_logistics_pln: number;
  estimated_ads_pln: number;
  estimated_returns_net_pln?: number;
  estimated_fba_storage_fee_pln?: number;
  estimated_fba_aged_fee_pln?: number;
  estimated_fba_removal_fee_pln?: number;
  estimated_fba_liquidation_fee_pln?: number;
  overhead_allocated_pln?: number;
  overhead_allocation_method?: string;
  overhead_confidence_pct?: number;
  cogs_per_unit_pln: number;
  fba_fee_per_unit_pln: number;
  referral_fee_per_unit_pln: number;
  revenue_pln: number;
  cogs_pln: number;
  amazon_fees_pln: number;
  cm1_profit: number;
  cm1_percent: number;
  cm2_profit: number;
  cm2_percent: number;
  np_profit?: number;
  np_percent?: number;
  history_orders: number;
  history_units: number;
  single_order_samples: number;
  confidence_score: number;
  cogs_source: string;
  fba_fee_source: string;
  referral_fee_source: string;
  logistics_source: string;
  shipping_charge_source: string;
  shipping_charge_mode?: string;
  pack_suggestion_source: string;
  flags: string[];
}

export interface ProductWhatIfSummary {
  summary_scope: string;
  total_revenue_pln: number;
  total_cogs_pln: number;
  total_fees_pln: number;
  total_logistics_pln: number;
  total_shipping_charge_pln: number;
  total_ads_pln: number;
  total_returns_net_pln?: number;
  total_fba_storage_fee_pln?: number;
  total_fba_aged_fee_pln?: number;
  total_fba_removal_fee_pln?: number;
  total_fba_liquidation_fee_pln?: number;
  total_overhead_allocated_pln?: number;
  total_cm1_pln: number;
  total_cm2_pln: number;
  total_np_pln?: number;
  total_cm1_pct: number;
  total_cm2_pct: number;
  total_np_pct?: number;
  total_offers: number;
  avg_confidence: number;
}

export interface ProductWhatIfResponse {
  total: number;
  page: number;
  page_size: number;
  pages: number;
  scenario_qty: number;
  include_shipping_charge: boolean;
  summary: ProductWhatIfSummary;
  items: ProductWhatIfItem[];
}

export const getProductWhatIfTable = (params: Record<string, unknown>) =>
  api
    .get<ProductWhatIfResponse>("/profit/v2/what-if", { params })
    .then((r) => r.data);

export const exportProductProfitXlsx = (params: Record<string, unknown>) =>
  api.get("/profit/v2/products/export.xlsx", { params, responseType: "blob" }).then((r) => {
    const url = window.URL.createObjectURL(new Blob([r.data]));
    const a = document.createElement("a");
    a.href = url;
    a.download = `product_profit_export.xlsx`;
    a.click();
    window.URL.revokeObjectURL(url);
  });

export interface DrilldownItem {
  amazon_order_id: string;
  marketplace_id: string;
  marketplace_code: string;
  purchase_date: string;
  fulfillment_channel: string;
  sku?: string;
  asin?: string;
  title?: string;
  qty: number;
  currency: string;
  fx_rate: number;
  item_price: number;
  item_tax: number;
  promo_discount: number;
  revenue_pln: number;
  shipping_charge_pln: number;
  cogs_pln: number;
  fba_fee_pln: number;
  referral_fee_pln: number;
  amazon_fees_pln: number;
  logistics_pln: number;
  cm1_profit: number;
  cm1_percent: number;
  purchase_price_pln: number;
  price_source?: string;
  cost_source: string;
  is_refund?: boolean;
  refund_type?: string;
  refund_amount_pln?: number;
}

export interface ProductDrilldownResponse {
  sku: string;
  total: number;
  page: number;
  page_size: number;
  pages: number;
  summary: {
    revenue_pln: number;
    shipping_charge_pln: number;
    cogs_pln: number;
    fees_pln: number;
    logistics_pln: number;
    cm1_pln: number;
    cm1_pct: number;
    units: number;
  };
  items: DrilldownItem[];
}

export const getProductDrilldown = (params: Record<string, unknown>) =>
  api
    .get<ProductDrilldownResponse>("/profit/v2/drilldown", { params })
    .then((r) => r.data);

export interface LossOrderItem {
  amazon_order_id: string;
  marketplace_id: string;
  marketplace_code: string;
  purchase_date: string;
  fulfillment_channel: string;
  sku?: string;
  asin?: string;
  title?: string;
  product_title?: string;
  qty: number;
  currency: string;
  revenue_pln: number;
  shipping_charge_pln: number;
  cogs_pln: number;
  amazon_fees_pln: number;
  logistics_pln: number;
  cm1_profit: number;
  cm1_percent: number;
  primary_loss_driver: string;
  driver_amount: number;
  confidence_score?: number;
}

export interface LossOrdersResponse {
  total: number;
  page: number;
  page_size: number;
  pages: number;
  total_loss_pln: number;
  items: LossOrderItem[];
}

export const getLossOrders = (params: Record<string, unknown>) =>
  api.get<LossOrdersResponse>("/profit/v2/loss-orders", { params }).then((r) => r.data);

// Fee Breakdown (Granular P&L)
export interface FeeBreakdownLine {
  line_type: string;
  charge_type: string;
  category: string;
  description: string;
  profit_layer: string;
  profit_bucket?: string;
  amount_pln: number;
  txn_count: number;
  pct_of_revenue: number;
  source: string;
}

export interface FeeBreakdownResponse {
  date_from: string;
  date_to: string;
  marketplace_id?: string;
  sku?: string;
  total_lines: number;
  summary: {
    revenue_pln: number;
    cogs_pln: number;
    cm1_pln: number;
    cm2_pln: number;
    np_pln: number;
    units: number;
  };
  lines: FeeBreakdownLine[];
}

export const getFeeBreakdown = (params: Record<string, unknown>) =>
  api.get<FeeBreakdownResponse>("/profit/v2/fee-breakdown", { params }).then((r) => r.data);

export interface DataQualityResponse {
  period: { date_from: string; date_to: string };
  overview: {
    total_order_lines: number;
    distinct_orders: number;
    distinct_skus: number;
    cogs_coverage_pct: number;
    purchase_price_coverage_pct: number;
    fba_fee_coverage_pct: number;
    referral_fee_coverage_pct: number;
    product_mapping_pct: number;
    finance_match_pct: number;
    fx_rate_coverage: string;
  };
  missing_cogs_top: Array<{
    sku: string;
    asin?: string;
    internal_sku?: string;
    ean?: string;
    units: number;
    revenue_orig: number;
    line_count: number;
    current_price_pln?: number | null;
    current_price_source?: string | null;
    hard_suggestion?: {
      suggested_internal_sku?: string | null;
      suggested_price_pln?: number | null;
      source_type: string;
      source_label: string;
      note?: string | null;
      is_hard_source: boolean;
    } | null;
    ai_candidate?: {
      matched_internal_sku: string;
      matched_title?: string | null;
      confidence: number;
      reasoning?: string | null;
      hard_price_pln?: number | null;
      hard_price_source?: string | null;
    } | null;
  }>;
  by_marketplace: Array<{
    marketplace_id: string;
    marketplace_code: string;
    total_lines: number;
    cogs_coverage_pct: number;
    fees_coverage_pct: number;
  }>;
}

export const getDataQuality = (params: Record<string, unknown>) =>
  api.get<DataQualityResponse>("/profit/v2/data-quality", { params }).then((r) => r.data);

// Purchase price manual upsert
export interface PurchasePriceUpsertRequest {
  internal_sku: string;
  netto_price_pln: number;
}

export interface PurchasePriceUpsertResponse {
  internal_sku: string;
  netto_price_pln: number;
  status: "created" | "updated";
}

export const upsertPurchasePrice = (data: PurchasePriceUpsertRequest) =>
  api.post<PurchasePriceUpsertResponse>("/profit/v2/purchase-price", data).then((r) => r.data);

// Map SKU → internal_sku + set purchase price in one call
export interface MapAndPriceRequest {
  sku: string;
  internal_sku: string;
  netto_price_pln: number;
}

export interface MapAndPriceResponse {
  sku: string;
  internal_sku: string;
  netto_price_pln: number;
  mapped_products: number;
  price_status: "created" | "updated";
}

export const mapAndPrice = (data: MapAndPriceRequest) =>
  api.post<MapAndPriceResponse>("/profit/v2/map-and-price", data).then((r) => r.data);

// AI Product Match Suggestions
export interface AIMatchRunResponse {
  status: "ok" | "partial" | "error";
  unmapped_count: number;
  batches_processed: number;
  gpt_results: number;
  suggestions_saved: number;
  errors_count: number;
  error_code?: string | null;
  error_summary?: string | null;
  message: string;
}

export interface BOMComponent {
  internal_sku?: string;
  name?: string;
  qty: number;
  unit_price_pln?: number;
}

export interface AIMatchSuggestionItem {
  id: number;
  unmapped_sku: string;
  unmapped_asin?: string;
  unmapped_title?: string;
  matched_internal_sku?: string;
  matched_title?: string;
  matched_sku?: string;
  confidence: number;
  reasoning?: string;
  quantity_in_bundle: number;
  unit_price_pln?: number;
  total_price_pln?: number;
  bom: BOMComponent[];
  status: string;
  created_at: string;
}

export interface AIMatchSuggestionsResponse {
  total: number;
  page: number;
  page_size: number;
  pages: number;
  items: AIMatchSuggestionItem[];
}

export interface AIMatchActionResponse {
  id: number;
  status: string;
  unmapped_sku: string;
  matched_internal_sku?: string;
  mapped_products?: number;
  price_status?: string;
}

export const runAIMatching = () =>
  api.post<AIMatchRunResponse>("/profit/v2/ai-match/run").then((r) => r.data);

export const getAIMatchSuggestions = (params: { status?: string; page?: number; page_size?: number }) =>
  api.get<AIMatchSuggestionsResponse>("/profit/v2/ai-match/suggestions", { params }).then((r) => r.data);

export const approveAIMatch = (id: number) =>
  api.post<AIMatchActionResponse>(`/profit/v2/ai-match/${id}/approve`).then((r) => r.data);

export const rejectAIMatch = (id: number) =>
  api.post<AIMatchActionResponse>(`/profit/v2/ai-match/${id}/reject`).then((r) => r.data);

export interface ProductTaskCreate {
  task_type: "pricing" | "content" | "watchlist";
  sku: string;
  marketplace_id?: string;
  title?: string;
  note?: string;
  owner?: string;
  source_page?: string;
  payload_json?: string;
}

export interface ProductTaskItem {
  id: string;
  task_type: string;
  sku: string;
  marketplace_id?: string;
  status: string;
  title?: string;
  note?: string;
  owner?: string;
  source_page?: string;
  created_at: string;
}

export const createProductTask = (payload: ProductTaskCreate) =>
  api.post<ProductTaskItem>("/profit/v2/tasks", payload).then((r) => r.data);

export interface ProductTaskListResponse {
  total: number;
  page: number;
  page_size: number;
  pages: number;
  items: ProductTaskItem[];
}

export interface ProductTaskUpdate {
  status?: "open" | "investigating" | "resolved";
  owner?: string;
  title?: string;
  note?: string;
}

export interface ProductTaskCommentCreate {
  comment: string;
  author?: string;
}

export interface ProductTaskCommentItem {
  id: number;
  task_id: string;
  comment: string;
  author?: string;
  created_at: string;
}

export const getProductTasks = (params?: Record<string, unknown>) =>
  api.get<ProductTaskListResponse>("/profit/v2/tasks", { params }).then((r) => r.data);

export const updateProductTask = (taskId: string, payload: ProductTaskUpdate) =>
  api.patch<ProductTaskItem>(`/profit/v2/tasks/${taskId}`, payload).then((r) => r.data);

export const getProductTaskComments = (taskId: string) =>
  api.get<ProductTaskCommentItem[]>(`/profit/v2/tasks/${taskId}/comments`).then((r) => r.data);

export const addProductTaskComment = (taskId: string, payload: ProductTaskCommentCreate) =>
  api.post<ProductTaskCommentItem>(`/profit/v2/tasks/${taskId}/comments`, payload).then((r) => r.data);

export interface TaskOwnerRuleCreate {
  owner: string;
  priority?: number;
  task_type?: string;
  marketplace_id?: string;
  brand?: string;
  is_active?: boolean;
}

export interface TaskOwnerRuleItem {
  id: number;
  owner: string;
  priority: number;
  task_type?: string;
  marketplace_id?: string;
  brand?: string;
  is_active: boolean;
  created_at: string;
}

export const getTaskOwnerRules = () =>
  api.get<TaskOwnerRuleItem[]>("/profit/v2/tasks/owner-rules").then((r) => r.data);

export const createTaskOwnerRule = (payload: TaskOwnerRuleCreate) =>
  api.post<TaskOwnerRuleItem>("/profit/v2/tasks/owner-rules", payload).then((r) => r.data);

export const deleteTaskOwnerRule = (ruleId: number) =>
  api.delete(`/profit/v2/tasks/owner-rules/${ruleId}`);

// ---------------------------------------------------------------------------
// Content Ops
// ---------------------------------------------------------------------------
export interface ContentOnboardPreflightRequest {
  sku_list: string[];
  main_market: string;
  target_markets: string[];
  auto_create_tasks?: boolean;
}

export interface ContentOnboardPreflightItem {
  sku: string;
  asin?: string | null;
  ean?: string | null;
  brand?: string | null;
  title?: string | null;
  pim_score: number;
  family_coverage_pct: number;
  blockers: string[];
  warnings: string[];
  recommended_actions: string[];
  tasks_created: string[];
}

export interface ContentOnboardPreflightResponse {
  main_market: string;
  target_markets: string[];
  items: ContentOnboardPreflightItem[];
  generated_at: string;
}

export interface ContentOnboardCatalogMatch {
  asin: string;
  title?: string | null;
  brand?: string | null;
  product_type?: string | null;
  image_url?: string | null;
}

export interface ContentOnboardCatalogResponse {
  query: string;
  marketplace: string;
  total: number;
  matches: ContentOnboardCatalogMatch[];
}

export interface ContentOnboardRestrictionResponse {
  asin: string;
  marketplace: string;
  can_list: boolean;
  requires_approval: boolean;
  reasons: string[];
}

export interface ContentPublishPushRequest {
  marketplaces: string[];
  selection: "approved" | "draft";
  sku_filter?: string[];
  version_ids?: string[];
  mode: "preview" | "confirm";
  idempotency_key?: string;
}

export interface ContentPublishJobItem {
  id: string;
  job_type: string;
  marketplaces: string[];
  selection_mode: "approved" | "draft";
  status: string;
  progress_pct: number;
  log_json: Record<string, unknown>;
  artifact_url?: string | null;
  created_by?: string | null;
  created_at: string;
  finished_at?: string | null;
}

export interface ContentPublishPushAccepted {
  job: ContentPublishJobItem;
  queued: boolean;
  detail: string;
}

export interface ContentPublishJobsResponse {
  total: number;
  page: number;
  page_size: number;
  pages: number;
  items: ContentPublishJobItem[];
}

export const runContentOnboardPreflight = (payload: ContentOnboardPreflightRequest) =>
  api.post<ContentOnboardPreflightResponse>("/content/onboard/preflight", payload).then((r) => r.data);

export const searchContentCatalogByEan = (ean: string, marketplace = "DE") =>
  api
    .get<ContentOnboardCatalogResponse>("/content/onboard/catalog/search-by-ean", { params: { ean, marketplace } })
    .then((r) => r.data);

export const checkContentRestrictions = (asin: string, marketplace = "DE") =>
  api
    .get<ContentOnboardRestrictionResponse>("/content/onboard/restrictions/check", { params: { asin, marketplace } })
    .then((r) => r.data);

export const pushContentPublish = (payload: ContentPublishPushRequest) =>
  api.post<ContentPublishJobItem | ContentPublishPushAccepted>("/content/publish/push", payload).then((r) => r.data);

export const getContentPublishJobs = (params?: Record<string, unknown>) =>
  api.get<ContentPublishJobsResponse>("/content/publish/jobs", { params }).then((r) => r.data);

export interface ContentTaskItem {
  id: string;
  type: "create_listing" | "refresh_content" | "fix_policy" | "expand_marketplaces";
  sku: string;
  asin?: string | null;
  marketplace_id?: string | null;
  priority: "p0" | "p1" | "p2" | "p3";
  owner?: string | null;
  due_date?: string | null;
  status: "open" | "investigating" | "resolved";
  tags_json: Record<string, unknown>;
  title?: string | null;
  note?: string | null;
  source_page?: string | null;
  created_by?: string | null;
  created_at: string;
  updated_at: string;
}

export interface ContentTaskListResponse {
  total: number;
  page: number;
  page_size: number;
  pages: number;
  items: ContentTaskItem[];
}

export interface ContentFieldsPayload {
  title?: string | null;
  bullets?: string[];
  description?: string | null;
  keywords?: string | null;
  special_features?: string[];
  attributes_json?: Record<string, unknown>;
  aplus_json?: Record<string, unknown>;
  compliance_notes?: string | null;
}

export interface ContentVersionItem {
  id: string;
  sku: string;
  asin?: string | null;
  marketplace_id: string;
  version_no: number;
  status: "draft" | "review" | "approved" | "published";
  fields: ContentFieldsPayload;
  created_by?: string | null;
  created_at: string;
  approved_by?: string | null;
  approved_at?: string | null;
  published_at?: string | null;
  parent_version_id?: string | null;
}

export interface ContentVersionListResponse {
  sku: string;
  marketplace_id: string;
  items: ContentVersionItem[];
}

export interface ContentPolicyRule {
  id?: string;
  name: string;
  pattern: string;
  severity: "critical" | "major" | "minor";
  applies_to_json?: Record<string, unknown>;
  is_active: boolean;
}

export interface ContentPolicyCheckResponse {
  version_id: string;
  passed: boolean;
  critical_count: number;
  major_count: number;
  minor_count: number;
  findings: Array<{
    rule_id?: string;
    rule_name?: string;
    severity: "critical" | "major" | "minor";
    field: string;
    message: string;
    snippet?: string | null;
  }>;
  checked_at: string;
  checker_version: string;
}

export interface ContentAssetItem {
  id: string;
  filename: string;
  mime: string;
  content_hash: string;
  storage_path: string;
  metadata_json: Record<string, unknown>;
  status: "approved" | "deprecated" | "draft";
  uploaded_by?: string | null;
  uploaded_at: string;
}

export interface ContentAssetListResponse {
  total: number;
  page: number;
  page_size: number;
  pages: number;
  items: ContentAssetItem[];
}

export interface ContentProductTypeMapRule {
  id?: string;
  marketplace_id?: string | null;
  brand?: string | null;
  category?: string | null;
  subcategory?: string | null;
  product_type: string;
  required_attrs: string[];
  priority: number;
  is_active: boolean;
}

export const getContentTasks = (params?: Record<string, unknown>) =>
  api.get<ContentTaskListResponse>("/content/tasks", { params }).then((r) => r.data);

export const createContentTask = (payload: {
  type: "create_listing" | "refresh_content" | "fix_policy" | "expand_marketplaces";
  sku: string;
  asin?: string;
  marketplace_id?: string;
  priority?: "p0" | "p1" | "p2" | "p3";
  owner?: string;
  due_date?: string;
  tags_json?: Record<string, unknown>;
  title?: string;
  note?: string;
  source_page?: string;
}) => api.post<ContentTaskItem>("/content/tasks", payload).then((r) => r.data);

export const updateContentTask = (
  taskId: string,
  payload: {
    status?: "open" | "investigating" | "resolved";
    owner?: string;
    priority?: "p0" | "p1" | "p2" | "p3";
    due_date?: string;
    title?: string;
    note?: string;
  }
) => api.patch<ContentTaskItem>(`/content/tasks/${taskId}`, payload).then((r) => r.data);

export const bulkUpdateContentTasks = (payload: {
  task_ids: string[];
  status?: "open" | "investigating" | "resolved";
  owner?: string;
  priority?: "p0" | "p1" | "p2" | "p3";
}) => api.post<{ updated_count: number; task_ids: string[] }>("/content/tasks/bulk-update", payload).then((r) => r.data);

export const getContentVersions = (sku: string, marketplaceId: string) =>
  api.get<ContentVersionListResponse>(`/content/${encodeURIComponent(sku)}/${encodeURIComponent(marketplaceId)}/versions`).then((r) => r.data);

export const createContentVersion = (sku: string, marketplaceId: string, payload: { asin?: string; base_version_id?: string; fields?: ContentFieldsPayload }) =>
  api.post<ContentVersionItem>(`/content/${encodeURIComponent(sku)}/${encodeURIComponent(marketplaceId)}/versions`, payload).then((r) => r.data);

export const updateContentVersion = (versionId: string, payload: { fields: ContentFieldsPayload }) =>
  api.put<ContentVersionItem>(`/content/versions/${versionId}`, payload).then((r) => r.data);

export const submitContentVersionReview = (versionId: string) =>
  api.post<ContentVersionItem>(`/content/versions/${versionId}/submit-review`).then((r) => r.data);

export const approveContentVersion = (versionId: string) =>
  api.post<ContentVersionItem>(`/content/versions/${versionId}/approve`).then((r) => r.data);

export const getContentPolicyRules = () =>
  api.get<ContentPolicyRule[]>("/content/policy/rules").then((r) => r.data);

export const upsertContentPolicyRules = (rules: ContentPolicyRule[]) =>
  api.put<ContentPolicyRule[]>("/content/policy/rules", { rules }).then((r) => r.data);

export const checkContentPolicy = (versionId: string) =>
  api.post<ContentPolicyCheckResponse>("/content/policy/check", { version_id: versionId }).then((r) => r.data);

export const getContentAssets = (params?: Record<string, unknown>) =>
  api.get<ContentAssetListResponse>("/content/assets", { params }).then((r) => r.data);

export const uploadContentAsset = (payload: { filename: string; mime: string; content_base64: string; metadata_json?: Record<string, unknown> }) =>
  api.post<ContentAssetItem>("/content/assets/upload", payload).then((r) => r.data);

export const linkContentAsset = (
  assetId: string,
  payload: { sku: string; asin?: string; marketplace_id?: string; role: "main_image" | "manual" | "cert" | "aplus" | "lifestyle" | "infographic" | "other"; status?: "approved" | "deprecated" | "draft" }
) => api.post(`/content/assets/${assetId}/link`, payload).then((r) => r.data);

export const createContentPublishPackage = (payload: {
  marketplaces: string[];
  selection?: "approved" | "draft";
  format?: "xlsx" | "csv";
  sku_filter?: string[];
}) => api.post<ContentPublishJobItem>("/content/publish/package", payload).then((r) => r.data);

export const getContentProductTypeMappings = () =>
  api.get<ContentProductTypeMapRule[]>("/content/publish/product-type-mappings").then((r) => r.data);

export const upsertContentProductTypeMappings = (rules: ContentProductTypeMapRule[]) =>
  api.put<ContentProductTypeMapRule[]>("/content/publish/product-type-mappings", { rules }).then((r) => r.data);

export interface ContentProductTypeDefinitionItem {
  id?: string;
  marketplace_id: string;
  marketplace_code: string;
  product_type: string;
  requirements_json: Record<string, unknown>;
  required_attrs: string[];
  refreshed_at: string;
  source: string;
}

export const getContentProductTypeDefinitions = (params?: { marketplace?: string; product_type?: string }) =>
  api.get<ContentProductTypeDefinitionItem[]>("/content/publish/product-type-definitions", { params }).then((r) => r.data);

export const refreshContentProductTypeDefinition = (payload: {
  marketplace: string;
  product_type: string;
  force_refresh?: boolean;
}) =>
  api.post<ContentProductTypeDefinitionItem>("/content/publish/product-type-definitions/refresh", payload).then((r) => r.data);

export interface ContentAttributeMapRule {
  id?: string;
  marketplace_id?: string | null;
  product_type?: string | null;
  source_field: string;
  target_attribute: string;
  transform?: "identity" | "stringify" | "upper" | "lower" | "trim";
  priority: number;
  is_active: boolean;
}

export const getContentAttributeMappings = () =>
  api.get<ContentAttributeMapRule[]>("/content/publish/attribute-mappings").then((r) => r.data);

export const upsertContentAttributeMappings = (rules: ContentAttributeMapRule[]) =>
  api.put<ContentAttributeMapRule[]>("/content/publish/attribute-mappings", { rules }).then((r) => r.data);

export interface ContentPublishCoverageRow {
  marketplace_id: string;
  category?: string | null;
  product_type: string;
  total_candidates: number;
  fully_covered: number;
  coverage_pct: number;
  missing_required_top: string[];
}

export interface ContentPublishCoverageResponse {
  generated_at: string;
  items: ContentPublishCoverageRow[];
}

export const getContentPublishCoverage = (marketplaces: string, selection: "approved" | "draft" = "approved") =>
  api.get<ContentPublishCoverageResponse>("/content/publish/coverage", { params: { marketplaces, selection } }).then((r) => r.data);

export interface ContentPublishMappingSuggestionItem {
  marketplace_id: string;
  product_type: string;
  missing_attribute: string;
  suggested_source_field?: string | null;
  confidence: number;
  candidates: string[];
  affected_skus: number;
}

export interface ContentPublishMappingSuggestionsResponse {
  generated_at: string;
  items: ContentPublishMappingSuggestionItem[];
}

export const getContentPublishMappingSuggestions = (marketplaces: string, selection: "approved" | "draft" = "approved", limit = 100) =>
  api
    .get<ContentPublishMappingSuggestionsResponse>("/content/publish/mapping-suggestions", { params: { marketplaces, selection, limit } })
    .then((r) => r.data);

export interface ContentPublishMappingApplyResponse {
  generated_at: string;
  dry_run: boolean;
  evaluated: number;
  created: number;
  skipped: number;
  items: Array<Record<string, unknown>>;
}

export const applyContentPublishMappingSuggestions = (payload: {
  marketplaces: string[];
  selection?: "approved" | "draft";
  min_confidence?: number;
  limit?: number;
  dry_run?: boolean;
}) =>
  api.post<ContentPublishMappingApplyResponse>("/content/publish/mapping-suggestions/apply", payload).then((r) => r.data);

export interface ContentPublishQueueHealthResponse {
  generated_at: string;
  queued_total: number;
  queued_stale_30m: number;
  running_total: number;
  retry_in_progress: number;
  failed_last_24h: number;
  max_retry_reached_last_24h: number;
  thresholds: Record<string, number>;
}

export const getContentPublishQueueHealth = (stale_minutes = 30) =>
  api.get<ContentPublishQueueHealthResponse>("/content/publish/queue-health", { params: { stale_minutes } }).then((r) => r.data);

export interface ContentPublishRetryRequest {
  sku_filter?: string[];
  failed_only?: boolean;
  idempotency_key?: string;
}

export const retryContentPublishJob = (jobId: string, payload: ContentPublishRetryRequest) =>
  api.post<ContentPublishPushAccepted>(`/content/publish/jobs/${jobId}/retry`, payload).then((r) => r.data);

export interface ContentOpsHealthResponse {
  generated_at: string;
  queue_health: ContentPublishQueueHealthResponse;
  compliance_backlog: Record<string, number>;
  tasks_health: Record<string, number>;
  data_quality_cards: Array<{ key: string; value: number; unit: string; note?: string | null }>;
}

export const getContentOpsHealth = () =>
  api.get<ContentOpsHealthResponse>("/content/health").then((r) => r.data);

export interface NetfoxSessionHealthItem {
  session_id: number;
  login_name?: string | null;
  host_name?: string | null;
  program_name?: string | null;
  status?: string | null;
  database_name?: string | null;
  login_time?: string | null;
  last_request_start_time?: string | null;
  last_request_end_time?: string | null;
}

export interface NetfoxSessionHealthResponse {
  ok: boolean;
  session_count: number | null;
  error?: string;
  items: NetfoxSessionHealthItem[];
}

export const getNetfoxSessionHealth = () =>
  api.get<NetfoxSessionHealthResponse>("/health/netfox-sessions").then((r) => r.data);

export interface OrderSyncHealthItem {
  marketplace_id: string;
  marketplace_code: string;
  status: string;
  gap_minutes?: number | null;
  last_finished_at?: string | null;
  last_successful_window_from?: string | null;
  last_successful_window_to?: string | null;
  last_orders_count: number;
  last_error?: string | null;
}

export interface OrderSyncHealthResponse {
  ok: boolean;
  status: string;
  stale_minutes?: number;
  error?: string;
  items: OrderSyncHealthItem[];
}

export const getOrderSyncHealth = () =>
  api.get<OrderSyncHealthResponse>("/health/order-sync").then((r) => r.data);

export interface ContentComplianceQueueItem {
  version_id: string;
  sku: string;
  marketplace_id: string;
  version_no: number;
  version_status: "draft" | "review" | "approved" | "published";
  critical_count: number;
  major_count: number;
  minor_count: number;
  findings: Array<Record<string, unknown>>;
  checked_at: string;
}

export interface ContentComplianceQueueResponse {
  total: number;
  page: number;
  page_size: number;
  pages: number;
  items: ContentComplianceQueueItem[];
}

export const getContentComplianceQueue = (params?: Record<string, unknown>) =>
  api.get<ContentComplianceQueueResponse>("/content/compliance/queue", { params }).then((r) => r.data);

export interface ContentImpactPoint {
  label: string;
  units: number;
  revenue: number;
  impact_margin_pln: number;
  refunds: number;
  return_rate: number;
  sessions?: number | null;
  cvr?: number | null;
}

export interface ContentImpactResponse {
  sku: string;
  marketplace_id: string;
  range_days: number;
  before: ContentImpactPoint;
  after: ContentImpactPoint;
  delta: ContentImpactPoint;
  baseline_expected: ContentImpactPoint;
  delta_vs_baseline: ContentImpactPoint;
  impact_signal: "negative" | "neutral" | "positive";
  confidence_score: number;
  baseline_hint: string;
  negative_impact: boolean;
  generated_at: string;
}

export const getContentImpact = (params: { sku: string; marketplace: string; range?: number }) =>
  api.get<ContentImpactResponse>("/content/impact", { params }).then((r) => r.data);

export interface ContentDiffField {
  field: string;
  main_value: unknown;
  target_value: unknown;
  change_type: "added" | "removed" | "changed" | "same";
}

export interface ContentDiffResponse {
  sku: string;
  main_market: string;
  target_market: string;
  version_main?: string | null;
  version_target?: string | null;
  fields: ContentDiffField[];
  created_at: string;
}

export interface ContentSyncResponse {
  sku: string;
  from_market: string;
  to_markets: string[];
  drafts_created: number;
  skipped: number;
  warnings: string[];
}

export const getContentDiff = (sku: string, params: { main: string; target: string; version_main?: string; version_target?: string }) =>
  api.get<ContentDiffResponse>(`/content/${encodeURIComponent(sku)}/diff`, { params }).then((r) => r.data);

export const syncContent = (
  sku: string,
  payload: { fields: string[]; from_market: string; to_markets: string[]; overwrite_mode?: "missing_only" | "force" }
) => api.post<ContentSyncResponse>(`/content/${encodeURIComponent(sku)}/sync`, payload).then((r) => r.data);

export interface ContentDataQualityCard {
  key: string;
  value: number;
  unit: string;
  note?: string | null;
}

export interface ContentDataQualityResponse {
  cards: ContentDataQualityCard[];
  missing_title: Array<Record<string, unknown>>;
  missing_bullets: Array<Record<string, unknown>>;
  missing_description: Array<Record<string, unknown>>;
  generated_at: string;
}

export const getContentDataQuality = () =>
  api.get<ContentDataQualityResponse>("/content/data-quality").then((r) => r.data);

// ---------------------------------------------------------------------------
// FBA Ops
// ---------------------------------------------------------------------------
export interface FbaOverviewMetric {
  label: string;
  value: number;
  unit?: string;
  trend?: number;
  status: string;
}

export interface FbaRiskItem {
  sku: string;
  asin?: string | null;
  internal_sku?: string | null;
  ean?: string | null;
  parent_asin?: string | null;
  title_preferred?: string | null;
  marketplace_id: string;
  marketplace_code: string;
  brand?: string | null;
  category?: string | null;
  on_hand: number;
  inbound: number;
  reserved: number;
  units_available: number;
  velocity_7d: number;
  velocity_30d: number;
  days_cover?: number | null;
  target_days: number;
  stockout_risk: string;
  overstock_risk: string;
  aged_90_plus_units: number;
  aged_90_plus_value_pln: number;
  stranded_units: number;
  stranded_value_pln: number;
  last_restock_date?: string | null;
  next_inbound_eta?: string | null;
}

export interface FbaInboundShipmentItem {
  shipment_id: string;
  shipment_name?: string | null;
  marketplace_id?: string | null;
  marketplace_code?: string | null;
  from_warehouse?: string | null;
  status: string;
  created_at?: string | null;
  last_update_at?: string | null;
  units_planned: number;
  units_received: number;
  variance_units: number;
  first_receive_at?: string | null;
  closed_at?: string | null;
  days_in_status: number;
  problems: string[];
}

export interface FbaInboundShipmentLineItem {
  sku: string;
  asin?: string | null;
  internal_sku?: string | null;
  ean?: string | null;
  parent_asin?: string | null;
  title_preferred?: string | null;
  qty_planned: number;
  qty_received: number;
  variance_units: number;
  payload_json: Record<string, unknown>;
}

export interface FbaOverviewResponse {
  metrics: FbaOverviewMetric[];
  top_stockout_risks: FbaRiskItem[];
  top_aged_value_skus: FbaAgedItem[];
  inbound_delays: FbaInboundShipmentItem[];
  snapshot_date?: string | null;
}

export interface FbaReportDiagnosticItem {
  report_type: string;
  fetch_mode: string;
  request_status?: string | null;
  selected_status?: string | null;
  fallback_source?: string | null;
  detail_json: Record<string, unknown>;
  created_at: string;
}

export interface FbaMarketplaceDiagnosticItem {
  marketplace_id: string;
  marketplace_code: string;
  planning?: FbaReportDiagnosticItem | null;
  stranded?: FbaReportDiagnosticItem | null;
  inventory_api?: FbaReportDiagnosticItem | null;
}

export interface FbaReportDiagnosticsResponse {
  generated_at: string;
  items: FbaMarketplaceDiagnosticItem[];
}

export interface FbaInventoryResponse {
  items: FbaRiskItem[];
  total: number;
  snapshot_date?: string | null;
}

export interface FbaInventoryTimelinePoint {
  date: string;
  on_hand: number;
  inbound: number;
  reserved: number;
  units_sold: number;
}

export interface FbaInventoryDetailResponse {
  item: FbaRiskItem;
  inventory_timeline: FbaInventoryTimelinePoint[];
  sales_timeline: FbaInventoryTimelinePoint[];
  notes: { at: string; type: string; message: string }[];
}

export interface FbaReplenishmentSuggestion {
  sku: string;
  asin?: string | null;
  title_preferred?: string | null;
  marketplace_id: string;
  marketplace_code: string;
  brand?: string | null;
  category?: string | null;
  current_days_cover?: number | null;
  target_days_cover: number;
  lead_time_days: number;
  safety_stock_days: number;
  suggested_qty: number;
  suggested_ship_week: string;
  urgency: string;
  exceptions: string[];
}

export interface FbaReplenishmentResponse {
  items: FbaReplenishmentSuggestion[];
  total: number;
}

export interface FbaAgedItem {
  sku: string;
  asin?: string | null;
  title_preferred?: string | null;
  marketplace_id: string;
  marketplace_code: string;
  aged_90_plus_units: number;
  aged_90_plus_value_pln: number;
  storage_fee_impact_estimate_pln: number;
  recommended_action: string;
}

export interface FbaStrandedItem {
  sku: string;
  asin?: string | null;
  title_preferred?: string | null;
  marketplace_id: string;
  marketplace_code: string;
  stranded_units: number;
  stranded_value_pln: number;
  reason?: string | null;
  recommended_action: string;
}

export interface FbaInboundShipmentsResponse {
  items: FbaInboundShipmentItem[];
  total: number;
  by_status: Record<string, number>;
}

export interface FbaInboundShipmentDetailResponse {
  shipment: FbaInboundShipmentItem;
  lines: FbaInboundShipmentLineItem[];
}

export interface FbaScorecardResponse {
  quarter: string;
  data_ready: boolean;
  score: number;
  score_pct_of_target: number;
  safety_gate_passed: boolean;
  explanation: string;
  kpis: Record<string, number>;
  factors: Record<string, number>;
  weights: Record<string, number>;
  components: FbaKpiComponent[];
  missing_inputs: string[];
}

export interface FbaKpiComponent {
  key: string;
  label: string;
  unit: string;
  direction: string;
  weight: number;
  actual?: number | null;
  alarm?: number | null;
  target?: number | null;
  good?: number | null;
  factor: number;
  score_contribution: number;
  data_ready: boolean;
  note?: string | null;
}

export interface FbaShipmentPlanItem {
  id: string;
  quarter: string;
  marketplace_id?: string | null;
  marketplace_code?: string | null;
  shipment_id?: string | null;
  plan_week_start: string;
  planned_ship_date?: string | null;
  planned_units: number;
  actual_ship_date?: string | null;
  actual_units?: number | null;
  tolerance_pct: number;
  status: string;
  owner?: string | null;
  notes_json: Record<string, unknown>;
  updated_at: string;
}

export interface FbaCaseItem {
  id: string;
  case_type: string;
  marketplace_id?: string | null;
  marketplace_code?: string | null;
  entity_type?: string | null;
  entity_id?: string | null;
  sku?: string | null;
  detected_date: string;
  close_date?: string | null;
  owner?: string | null;
  status: string;
  root_cause?: string | null;
  payload_json: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface FbaCaseEventItem {
  id: string;
  case_id: string;
  event_type: string;
  event_at: string;
  actor?: string | null;
  payload_json: Record<string, unknown>;
}

export interface FbaCaseTimelineResponse {
  case: FbaCaseItem;
  events: FbaCaseEventItem[];
}

export interface FbaLaunchItem {
  id: string;
  quarter: string;
  launch_type: string;
  sku?: string | null;
  bundle_id?: string | null;
  marketplace_id?: string | null;
  marketplace_code?: string | null;
  planned_go_live_date?: string | null;
  actual_go_live_date?: string | null;
  live_stable_at?: string | null;
  incident_free: boolean;
  vine_eligible: boolean;
  vine_eligible_at?: string | null;
  vine_submitted_at?: string | null;
  owner?: string | null;
  status: string;
  payload_json: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface FbaInitiativeItem {
  id: string;
  quarter: string;
  initiative_type: string;
  title: string;
  sku?: string | null;
  bundle_id?: string | null;
  owner?: string | null;
  status: string;
  planned: boolean;
  approved: boolean;
  live_stable_at?: string | null;
  created_at: string;
  updated_at: string;
}

export const getFbaOverview = () =>
  api.get<FbaOverviewResponse>("/fba/overview").then((r) => r.data);

export const getFbaReportDiagnostics = (lookback_hours = 48) =>
  api.get<FbaReportDiagnosticsResponse>("/fba/diagnostics/report-status", { params: { lookback_hours } }).then((r) => r.data);

export const getFbaInventory = (params?: Record<string, unknown>) =>
  api.get<FbaInventoryResponse>("/fba/inventory", { params }).then((r) => r.data);

export const getFbaInventoryDetail = (sku: string, marketplace_id?: string) =>
  api.get<FbaInventoryDetailResponse>(`/fba/inventory/${encodeURIComponent(sku)}`, {
    params: marketplace_id ? { marketplace_id } : undefined,
  }).then((r) => r.data);

export const getFbaReplenishmentSuggestions = (params?: Record<string, unknown>) =>
  api.get<FbaReplenishmentResponse>("/fba/replenishment/suggestions", { params }).then((r) => r.data);

export const getFbaInboundShipments = (params?: Record<string, unknown>) =>
  api.get<FbaInboundShipmentsResponse>("/fba/inbound/shipments", { params }).then((r) => r.data);

export const getFbaInboundShipmentDetail = (shipmentId: string) =>
  api.get<FbaInboundShipmentDetailResponse>(`/fba/inbound/shipments/${encodeURIComponent(shipmentId)}`).then((r) => r.data);

export const getFbaAgedItems = () =>
  api.get<FbaAgedItem[]>("/fba/aged").then((r) => r.data);

export const getFbaStrandedItems = () =>
  api.get<FbaStrandedItem[]>("/fba/stranded").then((r) => r.data);

export const getFbaScorecard = (quarter: string) =>
  api.get<FbaScorecardResponse>("/fba/kpi/scorecard", { params: { quarter } }).then((r) => r.data);

export const runFbaJob = (job_type: "sync_fba_inventory" | "sync_fba_inbound" | "run_fba_alerts" | "recompute_fba_replenishment" | "sync_fba_reconciliation") =>
  api.post<JobRun>("/fba/jobs/run", null, { params: { job_type } }).then((r) => r.data);

export type FbaRegisterType = "shipment_plan" | "case" | "launch" | "initiative";

// ---------------------------------------------------------------------------
// Profitability Module
// ---------------------------------------------------------------------------

export interface ProfitabilityKPI {
  total_revenue_pln: number;
  total_cm1_pln: number;
  total_cm2_pln: number;
  total_profit_pln: number;
  profit_tier: string;
  total_margin_pct: number;
  cm1_margin_pct: number;
  total_orders: number;
  total_units: number;
  total_ad_spend_pln: number;
  ad_spend_share_pct: number;
  tacos_pct: number;
  total_refund_pln: number;
  return_rate_pct: number;
}

export interface SkuRankItem {
  sku: string;
  asin?: string;
  marketplace_id: string;
  marketplace_code?: string;
  revenue_pln: number;
  profit_pln: number;
  margin_pct?: number;
  units: number;
  acos_pct?: number;
  return_rate_pct?: number;
}

export interface ProfitabilityLossOrderItem {
  amazon_order_id: string;
  marketplace_id: string;
  marketplace_code?: string;
  purchase_date: string;
  sku?: string;
  revenue_pln: number;
  profit_pln: number;
  margin_pct?: number;
}

export interface DataFreshnessInfo {
  rollup_recomputed_at?: string | null;
  cache_age_seconds?: number | null;
  rollup_covers?: { date_from?: string | null; date_to?: string | null } | null;
  data_source?: "rollup" | "live" | "mixed" | null;
}

export interface ProfitabilityOverview {
  kpi: ProfitabilityKPI;
  best_skus: SkuRankItem[];
  worst_skus: SkuRankItem[];
  loss_orders: ProfitabilityLossOrderItem[];
  warnings?: string[];
  data_freshness?: DataFreshnessInfo | null;
}

export interface ProfitabilityOrderItem {
  amazon_order_id: string;
  marketplace_id: string;
  marketplace_code?: string;
  purchase_date: string;
  sku?: string;
  asin?: string;
  sku_count?: number;
  all_skus?: string;
  revenue_pln: number;
  amazon_fees_pln: number;
  fba_fees_pln: number;
  logistics_pln: number;
  cogs_pln: number;
  ad_cost_pln: number;
  refund_pln: number;
  profit_pln: number;
  margin_pct?: number;
}

export interface ProfitabilityProductItem {
  sku: string;
  asin?: string;
  marketplace_id: string;
  marketplace_code?: string;
  units: number;
  orders: number;
  revenue_pln: number;
  cogs_pln: number;
  amazon_fees_pln: number;
  logistics_pln: number;
  ad_spend_pln: number;
  refund_pln: number;
  cm1_pln: number;
  cm2_pln: number;
  profit_pln: number;
  margin_pct?: number;
  acos_pct?: number;
  return_rate_pct?: number;
}

export interface MarketplaceProfitabilityItem {
  marketplace_id: string;
  marketplace_code?: string;
  total_orders: number;
  total_units: number;
  unique_skus: number;
  revenue_pln: number;
  cm1_pln: number;
  cm2_pln: number;
  profit_pln: number;
  margin_pct?: number;
  ad_spend_pln: number;
  acos_pct?: number;
  return_rate_pct?: number;
}

export interface PriceSimulatorResult {
  sale_price: number;
  purchase_cost: number;
  shipping_cost: number;
  amazon_fee: number;
  fba_fee: number;
  ad_cost: number;
  total_cost: number;
  profit: number;
  margin_pct: number;
  breakeven_price: number;
  currency: string;
  fx_rate: number;
}

export const getProfitabilityOverview = (params: Record<string, string>) =>
  api.get<ProfitabilityOverview>("/profit/v2/overview", { params }).then((r) => r.data);

export const getProfitabilityOrders = (params: Record<string, string | number>) =>
  api.get<{ total: number; page: number; page_size: number; pages: number; items: ProfitabilityOrderItem[] }>(
    "/profit/v2/orders", { params }
  ).then((r) => r.data);

export interface OrderLineDetail {
  sku: string;
  asin: string | null;
  title: string | null;
  quantity: number;
  item_price: number;
  item_tax: number;
  promo_discount: number;
  currency: string | null;
  referral_fee_pln: number;
  fba_fee_pln: number;
  cogs_pln: number;
  purchase_price_pln: number;
  price_source: string | null;
  line_profit_pln: number;
}

export interface OrderDetailResponse {
  amazon_order_id: string;
  order: {
    amazon_order_id: string;
    marketplace_id: string;
    marketplace_code: string;
    purchase_date: string;
    revenue_pln: number;
    amazon_fees_pln: number | null;
    cogs_pln: number;
    status: string;
  } | null;
  lines: OrderLineDetail[];
}

export const getOrderDetail = (orderId: string) =>
  api.get<OrderDetailResponse>("/profit/v2/order-detail", { params: { order_id: orderId } }).then((r) => r.data);

export const getProfitabilityProducts = (params: Record<string, string | number>) =>
  api.get<{ total: number; page: number; page_size: number; pages: number; items: ProfitabilityProductItem[] }>(
    "/profit/v2/sku-rollup", { params }
  ).then((r) => r.data);

export const getMarketplaceProfitability = (params: Record<string, string>) =>
  api.get<{ items: MarketplaceProfitabilityItem[] }>("/profit/v2/marketplace-rollup", { params }).then((r) => r.data);

export const simulatePrice = (payload: {
  sale_price: number;
  purchase_cost: number;
  shipping_cost?: number;
  amazon_fee_pct?: number;
  fba_fee?: number;
  ad_cost?: number;
  currency?: string;
  fx_rate?: number;
}) => api.post<PriceSimulatorResult>("/profit/v2/simulate", payload).then((r) => r.data);

export const triggerProfitabilityRecompute = (days_back = 7) =>
  api.post<{ sku_rows_upserted: number; marketplace_rows_upserted: number }>(
    "/profit/v2/recompute", null, { params: { days_back } }
  ).then((r) => r.data);

export const importFbaRegister = (file: File, registerType: FbaRegisterType, quarter?: string) => {
  const form = new FormData();
  form.append("file", file);
  const params: Record<string, string> = { register_type: registerType };
  if (quarter) params.quarter = quarter;
  return api.post<{ register_type: string; imported: number; skipped: number; errors: string[] }>(
    "/fba/registers/import", form, { params, headers: { "Content-Type": "multipart/form-data" } }
  ).then((r) => r.data);
};

export const syncFbaReconciliation = () =>
  api.post<{ reconciliation: { upserted: number }; shipment_plan: { updated: number } }>("/fba/reconciliation/sync").then((r) => r.data);

export const getFbaShipmentPlans = (params?: Record<string, unknown>) =>
  api.get<{ total: number; items: FbaShipmentPlanItem[] }>("/fba/shipment-plans", { params }).then((r) => r.data);

export const createFbaShipmentPlan = (payload: Record<string, unknown>) =>
  api.post<FbaShipmentPlanItem>("/fba/shipment-plans", payload).then((r) => r.data);

export const updateFbaShipmentPlan = (id: string, payload: Record<string, unknown>) =>
  api.patch<FbaShipmentPlanItem>(`/fba/shipment-plans/${id}`, payload).then((r) => r.data);

export const deleteFbaShipmentPlan = (id: string) =>
  api.delete(`/fba/shipment-plans/${id}`);

export const getFbaCases = (params?: Record<string, unknown>) =>
  api.get<{ total: number; items: FbaCaseItem[] }>("/fba/cases", { params }).then((r) => r.data);

export const createFbaCase = (payload: Record<string, unknown>) =>
  api.post<FbaCaseItem>("/fba/cases", payload).then((r) => r.data);

export const updateFbaCase = (id: string, payload: Record<string, unknown>) =>
  api.patch<FbaCaseItem>(`/fba/cases/${id}`, payload).then((r) => r.data);

export const deleteFbaCase = (id: string) =>
  api.delete(`/fba/cases/${id}`);

export const getFbaCaseTimeline = (id: string) =>
  api.get<FbaCaseTimelineResponse>(`/fba/cases/${id}/timeline`).then((r) => r.data);

export const addFbaCaseComment = (id: string, payload: { comment: string; author?: string }) =>
  api.post<FbaCaseTimelineResponse>(`/fba/cases/${id}/comments`, payload).then((r) => r.data);

export const updateFbaCaseComment = (id: string, eventId: string, payload: { comment: string; author?: string }) =>
  api.put<FbaCaseTimelineResponse>(`/fba/cases/${id}/comments/${eventId}`, payload).then((r) => r.data);

export const deleteFbaCaseComment = (id: string, eventId: string, author?: string) =>
  api.delete<FbaCaseTimelineResponse>(`/fba/cases/${id}/comments/${eventId}`, { params: author ? { author } : undefined }).then((r) => r.data);

export const getFbaLaunches = (params?: Record<string, unknown>) =>
  api.get<{ total: number; items: FbaLaunchItem[] }>("/fba/launches", { params }).then((r) => r.data);

export const createFbaLaunch = (payload: Record<string, unknown>) =>
  api.post<FbaLaunchItem>("/fba/launches", payload).then((r) => r.data);

export const updateFbaLaunch = (id: string, payload: Record<string, unknown>) =>
  api.patch<FbaLaunchItem>(`/fba/launches/${id}`, payload).then((r) => r.data);

export const deleteFbaLaunch = (id: string) =>
  api.delete(`/fba/launches/${id}`);

export const getFbaInitiatives = (params?: Record<string, unknown>) =>
  api.get<{ total: number; items: FbaInitiativeItem[] }>("/fba/initiatives", { params }).then((r) => r.data);

export const createFbaInitiative = (payload: Record<string, unknown>) =>
  api.post<FbaInitiativeItem>("/fba/initiatives", payload).then((r) => r.data);

export const updateFbaInitiative = (id: string, payload: Record<string, unknown>) =>
  api.patch<FbaInitiativeItem>(`/fba/initiatives/${id}`, payload).then((r) => r.data);

export const deleteFbaInitiative = (id: string) =>
  api.delete(`/fba/initiatives/${id}`);

export interface FinanceAccountItem {
  account_code: string;
  name: string;
  account_type: string;
  parent_code?: string | null;
  is_active: boolean;
}

export interface FinanceTaxCodeItem {
  code: string;
  vat_rate: number;
  oss_flag: boolean;
  country?: string | null;
  description?: string | null;
  is_active: boolean;
}

export interface FinanceLedgerEntryItem {
  id: string;
  entry_date: string;
  source: string;
  source_ref: string;
  marketplace_id?: string | null;
  marketplace_code?: string | null;
  settlement_id?: string | null;
  financial_event_group_id?: string | null;
  amazon_order_id?: string | null;
  transaction_type?: string | null;
  charge_type?: string | null;
  currency: string;
  amount: number;
  fx_rate: number;
  amount_base: number;
  base_currency: string;
  account_code: string;
  tax_code?: string | null;
  country?: string | null;
  sku?: string | null;
  asin?: string | null;
  description?: string | null;
  tags_json: Record<string, unknown>;
  reversed_entry_id?: string | null;
}

export interface FinanceLedgerResponse {
  items: FinanceLedgerEntryItem[];
  total: number;
}

export interface FinancePayoutReconciliationItem {
  settlement_id: string;
  financial_event_group_id?: string | null;
  marketplace_id?: string | null;
  marketplace_code?: string | null;
  currency: string;
  total_amount: number;
  total_amount_base: number;
  transaction_count: number;
  posted_from?: string | null;
  posted_to?: string | null;
  id?: string | null;
  status: string;
  bank_line_id?: string | null;
  matched_amount?: number | null;
  diff_amount?: number | null;
  notes?: string | null;
  bank_date?: string | null;
  bank_amount?: number | null;
  bank_currency?: string | null;
  reference?: string | null;
}

export interface FinancePayoutReconciliationResponse {
  items: FinancePayoutReconciliationItem[];
  total: number;
}

export interface FinanceAutoMatchOut {
  matched: number;
  settlements: number;
}

export const getFinanceJobs = (params?: Record<string, unknown>) =>
  api.get<JobListResponse>("/finance/jobs", { params }).then((r) => r.data);

export interface FinanceSyncDiagnosticItem {
  financial_event_group_id: string;
  marketplace_id?: string | null;
  marketplace_code?: string | null;
  processing_status?: string | null;
  fund_transfer_status?: string | null;
  group_start?: string | null;
  group_end?: string | null;
  first_posted_at?: string | null;
  last_posted_at?: string | null;
  last_row_count: number;
  payload_signature?: string | null;
  event_type_counts_json: Record<string, number>;
  last_synced_at?: string | null;
  open_refresh_after?: string | null;
  open_age_hours: number;
  cost_score: number;
  sync_state: string;
}

export interface FinanceSyncDiagnosticsResponse {
  latest_watermark_from?: string | null;
  tracked_open_groups: number;
  items: FinanceSyncDiagnosticItem[];
}

export const getFinanceSyncDiagnostics = (params?: Record<string, unknown>) =>
  api.get<FinanceSyncDiagnosticsResponse>("/finance/sync/diagnostics", { params }).then((r) => r.data);

export interface FinanceCompletenessMarketplaceItem {
  marketplace_id: string;
  marketplace_code: string;
  order_days: number;
  finance_days: number;
  day_coverage_pct: number;
  orders_total: number;
  orders_with_finance: number;
  order_coverage_pct: number;
  status: string;
  note?: string | null;
}

export interface FinanceCompletenessResponse {
  date_from: string;
  date_to: string;
  overall_status: string;
  partial: boolean;
  note?: string | null;
  marketplaces: FinanceCompletenessMarketplaceItem[];
}

export const getFinanceCompleteness = (params?: Record<string, unknown>) =>
  api.get<FinanceCompletenessResponse>("/finance/sync/completeness", { params }).then((r) => r.data);

export interface FinanceGapDiagnosticsMarketplaceItem {
  marketplace_id: string;
  marketplace_code: string;
  tracked_groups: number;
  groups_with_rows: number;
  imported_rows: number;
  imported_orders: number;
  unmapped_rows: number;
  missing_order_rows: number;
  missing_order_distinct_orders: number;
  event_type_counts: Record<string, number>;
  first_group_start?: string | null;
  last_group_end?: string | null;
  order_days: number;
  finance_days: number;
  day_coverage_pct: number;
  order_coverage_pct: number;
  imported_transaction_type_counts: Record<string, number>;
  by_age_bucket: Array<{
    key: string;
    orders_total: number;
    orders_with_finance: number;
    coverage_pct: number;
  }>;
  by_fulfillment_channel: Array<{
    key: string;
    orders_total: number;
    orders_with_finance: number;
    coverage_pct: number;
  }>;
  missing_order_age_bucket_counts: Record<string, number>;
  missing_order_transaction_type_counts: Record<string, number>;
  missing_order_likely_cause?: string | null;
  likely_gap_driver?: string | null;
  gap_reason: string;
  note?: string | null;
}

export interface FinanceGapDiagnosticsResponse {
  date_from: string;
  date_to: string;
  note: string;
  marketplaces: FinanceGapDiagnosticsMarketplaceItem[];
}

export const getFinanceGapDiagnostics = (params?: Record<string, unknown>) =>
  api.get<FinanceGapDiagnosticsResponse>("/finance/sync/gap-diagnostics", { params }).then((r) => r.data);

export interface FinanceRevenueIntegrityResponse {
  date_from: string;
  date_to: string;
  total_orders: number;
  active_orders: number;
  canceled_orders: number;
  missing_revenue_total: number;
  missing_revenue_active: number;
  missing_revenue_shipped: number;
  missing_revenue_unshipped: number;
  missing_order_total_total: number;
  missing_order_total_active: number;
  missing_order_total_shipped: number;
  missing_order_total_unshipped: number;
  shipped_missing_revenue_zero_line_headers: number;
  unshipped_missing_revenue_zero_line_headers: number;
  missing_revenue_by_status: Record<string, number>;
  missing_order_total_by_status: Record<string, number>;
  note: string;
}

export interface FinanceDashboardSectionItem {
  key: string;
  label: string;
  status: string;
  note?: string | null;
}

export interface FinanceDashboardJobItem {
  id: string;
  job_type: string;
  status: string;
  progress_pct: number;
  progress_message?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  records_processed?: number | null;
}

export interface FinanceOrderSyncHealthItem {
  marketplace_id?: string | null;
  marketplace_code?: string | null;
  status: string;
  gap_minutes?: number | null;
  note?: string | null;
}

export interface FinanceOrderSyncHealthResponse {
  ok: boolean;
  status: string;
  error?: string | null;
  items: FinanceOrderSyncHealthItem[];
}

export interface FinanceDashboardResponse {
  date_from?: string | null;
  date_to?: string | null;
  revenue_base: number;
  fees_base: number;
  vat_base: number;
  profit_proxy: number;
  unmatched_payouts: number;
  ledger_rows: number;
  settlement_rows: number;
  payout_rows: number;
  bank_line_rows: number;
  completeness_status: string;
  partial: boolean;
  note?: string | null;
  sections: FinanceDashboardSectionItem[];
  recent_jobs: FinanceDashboardJobItem[];
  completeness?: FinanceCompletenessResponse | null;
  gap_diagnostics?: FinanceGapDiagnosticsResponse | null;
  order_revenue_integrity?: FinanceRevenueIntegrityResponse | null;
  sync_diagnostics?: FinanceSyncDiagnosticsResponse | null;
  payout_reconciliation?: FinancePayoutReconciliationResponse | null;
  order_sync?: FinanceOrderSyncHealthResponse | null;
}

export const getFinanceOrderRevenueIntegrity = (params?: Record<string, unknown>) =>
  api
    .get<FinanceRevenueIntegrityResponse>("/finance/sync/order-revenue-integrity", { params })
    .then((r) => r.data);

export const getFinanceDashboard = (params?: Record<string, unknown>) =>
  api.get<FinanceDashboardResponse>("/finance/dashboard", { params }).then((r) => r.data);

export const getFinanceLedger = (params?: Record<string, unknown>) =>
  api.get<FinanceLedgerResponse>("/finance/ledger", { params }).then((r) => r.data);

export const createFinanceManualLedgerEntry = (payload: Record<string, unknown>) =>
  api.post<{ id: string }>("/finance/ledger/manual", payload).then((r) => r.data);

export const reverseFinanceLedgerEntry = (entryId: string) =>
  api.post<{ id: string; reversed_entry_id?: string | null }>(`/finance/ledger/reverse/${entryId}`).then((r) => r.data);

export const getFinanceAccounts = () =>
  api.get<FinanceAccountItem[]>("/finance/accounts").then((r) => r.data);

export const getFinanceTaxCodes = () =>
  api.get<FinanceTaxCodeItem[]>("/finance/tax-codes").then((r) => r.data);

export const getFinancePayoutReconciliation = (params?: Record<string, unknown>) =>
  api.get<FinancePayoutReconciliationResponse>("/finance/reconcile/payouts", { params }).then((r) => r.data);

export const autoMatchFinancePayouts = () =>
  api.post<FinanceAutoMatchOut>("/finance/reconcile/payouts/auto-match").then((r) => r.data);

export const runFinanceImportTransactions = (days_back = 30, marketplace_id?: string) =>
  api.post<JobRun>("/finance/import/amazon/transactions", { days_back, ...(marketplace_id ? { marketplace_id } : {}) }).then((r) => r.data);

export const runFinancePrepareSettlements = () =>
  api.post<JobRun>("/finance/import/amazon/settlements").then((r) => r.data);

export const runFinanceGenerateLedger = (days_back = 90, marketplace_id?: string) =>
  api.post<JobRun>("/finance/jobs/run-ledger", { days_back, ...(marketplace_id ? { marketplace_id } : {}) }).then((r) => r.data);

export const runFinanceReconciliation = () =>
  api.post<JobRun>("/finance/jobs/run-reconciliation").then((r) => r.data);

// ---------------------------------------------------------------------------
// Executive Command Center
// ---------------------------------------------------------------------------

export interface ExecKPI {
  revenue_pln: number;
  cm1_pln: number;
  cm2_pln: number;
  profit_pln: number;
  margin_pct: number;
  orders: number;
  units: number;
  ad_spend_pln: number;
  acos_pct?: number;
  return_rate_pct?: number;
  revenue_growth_pct?: number;
  profit_growth_pct?: number;
}

export interface ExecHealthScore {
  period_date: string;
  revenue_score: number;
  profit_score: number;
  demand_score: number;
  inventory_score: number;
  operations_score: number;
  overall_score: number;
}

export interface ExecHealthLabel {
  score: number;
  label: string;
  color: string;
}

export interface ExecOpportunity {
  id: number;
  opp_type: string;
  category: string;
  priority: string;
  marketplace_id?: string;
  marketplace_code?: string;
  sku?: string;
  title: string;
  description?: string;
  impact_estimate?: number;
  confidence?: number;
  is_active: boolean;
  created_at?: string;
}

export interface ExecOverviewResponse {
  kpi: ExecKPI;
  kpi_prev?: ExecKPI;
  health?: ExecHealthScore;
  health_label?: ExecHealthLabel;
  risks: ExecOpportunity[];
  growth: ExecOpportunity[];
  best_skus: { sku: string; asin?: string; marketplace_id: string; marketplace_code?: string; revenue_pln: number; cm1_pln: number; cm2_pln: number; profit_pln: number; margin_pct: number; units: number }[];
  worst_skus: { sku: string; asin?: string; marketplace_id: string; marketplace_code?: string; revenue_pln: number; cm1_pln: number; cm2_pln: number; profit_pln: number; margin_pct: number; units: number }[];
}

export interface ExecProductItem {
  sku: string;
  asin?: string;
  marketplace_id: string;
  marketplace_code?: string;
  revenue_pln: number;
  cm1_pln: number;
  cm2_pln: number;
  profit_pln: number;
  margin_pct?: number;
  units: number;
  sessions?: number;
  cvr_pct?: number;
  return_rate_pct?: number;
  acos_pct?: number;
  inventory_risk?: string;
}

export interface ExecMarketplaceItem {
  marketplace_id: string;
  marketplace_code?: string;
  revenue_pln: number;
  cm1_pln: number;
  cm2_pln: number;
  profit_pln: number;
  margin_pct?: number;
  orders: number;
  units: number;
  sessions?: number;
  cvr_pct?: number;
  acos_pct?: number;
  return_rate_pct?: number;
  health_score?: number;
}

export const getExecOverview = (params: Record<string, string>) =>
  api.get<ExecOverviewResponse>("/executive/overview", { params }).then((r) => r.data);

export const getExecProducts = (params: Record<string, string | number>) =>
  api.get<{ total: number; page: number; page_size: number; pages: number; items: ExecProductItem[] }>(
    "/executive/products", { params }
  ).then((r) => r.data);

export const getExecMarketplaces = (params: Record<string, string>) =>
  api.get<{ items: ExecMarketplaceItem[] }>("/executive/marketplaces", { params }).then((r) => r.data);

export const triggerExecRecompute = (days_back = 7) =>
  api.post<{ metrics_rows: number; health_computed: boolean; opportunities_found: number; risks_found: number }>(
    "/executive/recompute", null, { params: { days_back }, timeout: 300_000 }
  ).then((r) => r.data);

/* ═══════════════════════════════════════════════════════════════════
   Strategy / Growth Engine
   ═══════════════════════════════════════════════════════════════════ */

export interface GrowthOpportunity {
  id: number;
  opportunity_type: string;
  marketplace_id?: string;
  marketplace_code?: string;
  sku?: string;
  asin?: string;
  parent_asin?: string;
  family_id?: number;
  title: string;
  description?: string;
  root_cause?: string;
  recommendation?: string;
  priority_score: number;
  priority_label?: string;
  confidence_score: number;
  estimated_revenue_uplift?: number;
  estimated_profit_uplift?: number;
  estimated_margin_uplift?: number;
  estimated_units_uplift?: number;
  effort_score?: number;
  owner_role?: string;
  blocker_json?: unknown;
  source_signals_json?: unknown;
  status: string;
  created_at?: string;
  updated_at?: string;
}

export interface StrategyKPI {
  total_revenue_uplift: number;
  total_profit_uplift: number;
  total_opportunities: number;
  do_now_count: number;
  this_week_count: number;
  this_month_count: number;
  blocked_count: number;
  completed_30d: number;
  completed_impact_30d: number;
}

export interface TypeBreakdown {
  opportunity_type: string;
  count: number;
  revenue_uplift: number;
  profit_uplift: number;
}

export interface MarketBreakdown {
  marketplace_id: string;
  marketplace_code?: string;
  count: number;
  revenue_uplift: number;
  profit_uplift: number;
}

export interface OwnerBreakdown {
  owner_role: string;
  count: number;
}

export interface StrategyOverviewResponse {
  kpi: StrategyKPI;
  by_type: TypeBreakdown[];
  by_market: MarketBreakdown[];
  by_owner: OwnerBreakdown[];
  top_priorities: GrowthOpportunity[];
  do_now: GrowthOpportunity[];
  this_week: GrowthOpportunity[];
  blocked: GrowthOpportunity[];
}

export interface OpportunityLogEntry {
  id: number;
  opportunity_id: number;
  action: string;
  actor?: string;
  note?: string;
  created_at?: string;
}

export interface OpportunityDetailResponse {
  opportunity: GrowthOpportunity;
  timeline: OpportunityLogEntry[];
}

export interface MarketExpansionItem {
  family_id?: number;
  parent_asin?: string;
  sku?: string;
  source_marketplace: string;
  target_marketplace: string;
  source_revenue: number;
  source_profit: number;
  readiness_score: number;
  readiness_label: string;
  missing_components?: string[];
  estimated_revenue_uplift: number;
  estimated_profit_uplift: number;
  confidence: number;
}

export interface BundleCandidate {
  id?: number;
  sku_a: string;
  sku_b?: string;
  proposed_bundle_sku?: string;
  marketplace_id?: string;
  est_margin?: number;
  est_profit_uplift?: number;
  confidence: number;
  blocker?: string;
  action?: string;
}

export interface StrategyExperiment {
  id: number;
  opportunity_id?: number;
  experiment_type: string;
  marketplace_id?: string;
  sku?: string;
  asin?: string;
  hypothesis: string;
  owner?: string;
  status: string;
  start_date?: string;
  end_date?: string;
  success_metric?: string;
  baseline_value?: number;
  result_value?: number;
  result_summary?: string;
  created_at?: string;
  updated_at?: string;
}

export interface PlaybookStep {
  seq: number;
  action: string;
  owner_role?: string;
  details?: string;
}

export interface StrategyPlaybook {
  id: string;
  name: string;
  description: string;
  trigger_condition: string;
  opportunity_types: string[];
  steps: PlaybookStep[];
  metrics_to_monitor: string[];
  expected_time_to_impact?: string;
}

// ── Strategy API functions ───────────────────────────────────────

export const getStrategyOverview = () =>
  api.get<StrategyOverviewResponse>("/strategy/overview").then((r) => r.data);

export const getStrategyOpportunities = (params: Record<string, string | number>) =>
  api.get<{ items: GrowthOpportunity[]; total: number; pages: number }>(
    "/strategy/opportunities", { params }
  ).then((r) => r.data);

export const getStrategyOpportunityDetail = (id: number) =>
  api.get<OpportunityDetailResponse>(`/strategy/opportunities/${id}`).then((r) => r.data);

export const acceptOpportunity = (id: number, note?: string) =>
  api.post(`/strategy/opportunities/${id}/accept`, note ? { note } : null).then((r) => r.data);

export const rejectOpportunity = (id: number, note?: string) =>
  api.post(`/strategy/opportunities/${id}/reject`, note ? { note } : null).then((r) => r.data);

export const completeOpportunity = (id: number, note?: string) =>
  api.post(`/strategy/opportunities/${id}/complete`, note ? { note } : null).then((r) => r.data);

export const getStrategyPlaybooks = () =>
  api.get<{ playbooks: StrategyPlaybook[] }>("/strategy/playbooks").then((r) => r.data);

export const getMarketExpansion = () =>
  api.get<{ items: MarketExpansionItem[]; total: number }>("/strategy/market-expansion").then((r) => r.data);

export const getStrategyBundles = () =>
  api.get<{ bundles: BundleCandidate[]; variant_gaps: Record<string, unknown>[] }>(
    "/strategy/bundles"
  ).then((r) => r.data);

export const getStrategyExperiments = (status?: string) =>
  api.get<{ items: StrategyExperiment[]; total: number }>(
    "/strategy/experiments", { params: status ? { status } : {} }
  ).then((r) => r.data);

export const createStrategyExperiment = (data: Omit<StrategyExperiment, "id" | "status" | "created_at" | "updated_at" | "baseline_value" | "result_value" | "result_summary">) =>
  api.post("/strategy/experiments", data).then((r) => r.data);

export const runStrategyDetection = (days_back = 30) =>
  api.post<{ opportunities_found: number; elapsed_sec: number; details: Record<string, number> }>(
    "/strategy/jobs/run", { job_type: "detect_all", days_back }, { timeout: 300_000 }
  ).then((r) => r.data);

// ═══════════════════════════════════════════════════════════════════
//  Decision Intelligence — Outcomes & Learning
// ═══════════════════════════════════════════════════════════════════

export interface OutcomeExecution {
  execution_id: number;
  opportunity_id: number;
  entity_type: string;
  entity_id: string;
  action_type: string;
  executed_by: string | null;
  executed_at: string | null;
  monitoring_start: string | null;
  monitoring_end: string | null;
  status: "monitoring" | "evaluated" | "expired";
  opportunity_type: string;
  marketplace_id: string | null;
  marketplace_code: string | null;
  sku: string | null;
  title: string | null;
  product_title: string | null;
  expected_profit: number;
  success_score: number | null;
  success_label: string | null;
  impact_score: number | null;
  confidence_adjustment: number | null;
  monitoring_days: number | null;
  evaluated_at: string | null;
  actual_metrics: Record<string, number> | null;
  delta: Record<string, number> | null;
}

export interface OutcomeWindow {
  id: number;
  monitoring_days: number;
  actual_metrics: Record<string, number> | null;
  expected_metrics: Record<string, number> | null;
  delta: Record<string, number> | null;
  success_score: number | null;
  success_label: string | null;
  impact_score: number | null;
  confidence_adjustment: number | null;
  evaluated_at: string | null;
}

export interface ExecutionDetail {
  execution: {
    id: number;
    opportunity_id: number;
    entity_type: string;
    entity_id: string;
    action_type: string;
    executed_by: string | null;
    executed_at: string | null;
    baseline_metrics: Record<string, number>;
    expected_metrics: Record<string, number>;
    monitoring_start: string | null;
    monitoring_end: string | null;
    status: string;
    opportunity_type: string;
    marketplace_id: string | null;
    sku: string | null;
    title: string | null;
    product_title: string | null;
  };
  outcomes: OutcomeWindow[];
}

export interface LearningEntry {
  opportunity_type: string;
  sample_size: number;
  avg_expected_profit: number;
  avg_actual_profit: number;
  prediction_accuracy: number;
  avg_success_score: number;
  confidence_adjustment: number;
  win_rate: number;
  avg_roi: number | null;
  last_updated: string | null;
}

export interface ModelAdjustment {
  opportunity_type: string;
  impact_weight_adjustment: number;
  confidence_weight_adjustment: number;
  priority_weight_adjustment: number;
  reason: string;
  updated_at: string | null;
}

export interface LearningDashboard {
  learning: LearningEntry[];
  adjustments: ModelAdjustment[];
  summary: {
    types_tracked: number;
    avg_prediction_accuracy: number;
    avg_win_rate: number;
    total_evaluations: number;
    avg_roi: number;
  };
}

export interface WeeklyReport {
  period_start: string;
  period_end: string;
  top_performing: Array<{
    opportunity_type: string;
    sku: string | null;
    marketplace_code: string | null;
    title: string | null;
    product_title: string | null;
    success_score: number;
    impact_score: number;
    profit_delta: number;
    expected_profit: number;
  }>;
  worst_performing: Array<{
    opportunity_type: string;
    sku: string | null;
    marketplace_code: string | null;
    title: string | null;
    product_title: string | null;
    success_score: number;
    impact_score: number;
    profit_delta: number;
    expected_profit: number;
  }>;
  prediction_accuracy: number;
  total_evaluated: number;
  total_success: number;
  insights: string[];
}

export interface OpportunityExecution {
  execution_id: number;
  action_type: string;
  executed_by: string | null;
  executed_at: string | null;
  baseline_metrics: Record<string, number> | null;
  expected_metrics: Record<string, number> | null;
  monitoring_start: string | null;
  monitoring_end: string | null;
  status: string;
  outcomes: OutcomeWindow[];
}

// ── API functions ───────────────────────────────────────────────

export const getDecisionOutcomes = (params: {
  page?: number;
  page_size?: number;
  opportunity_type?: string;
  marketplace_id?: string;
  min_success?: number;
  max_success?: number;
  status?: string;
}) =>
  api.get<{ items: OutcomeExecution[]; total: number; pages: number }>(
    "/strategy/decisions/outcomes", { params }
  ).then((r) => r.data);

export const getExecutionDetail = (executionId: number) =>
  api.get<ExecutionDetail>(
    `/strategy/decisions/outcomes/${executionId}`
  ).then((r) => r.data);

export const getOpportunityOutcomes = (opportunityId: number) =>
  api.get<OpportunityExecution[]>(
    `/strategy/decisions/outcomes/opportunity/${opportunityId}`
  ).then((r) => r.data);

export const createExecution = (data: {
  opportunity_id: number;
  action_type: string;
  executed_by?: string;
  entity_type?: string;
  entity_id?: string;
  monitoring_days?: number;
}) =>
  api.post("/strategy/decisions/executions", data).then((r) => r.data);

export const getLearningDashboard = () =>
  api.get<LearningDashboard>("/strategy/decisions/learning").then((r) => r.data);

export const getWeeklyReport = () =>
  api.get<WeeklyReport>("/strategy/decisions/learning/report").then((r) => r.data);

export const triggerOutcomeEvaluation = () =>
  api.post("/strategy/decisions/evaluate").then((r) => r.data);

export const triggerLearningAggregation = () =>
  api.post("/strategy/decisions/aggregate").then((r) => r.data);

export const triggerModelRecalibration = () =>
  api.post("/strategy/decisions/recalibrate").then((r) => r.data);

// ════════════════════════════════════════════════════════════════════
// SEASONALITY & DEMAND INTELLIGENCE
// ════════════════════════════════════════════════════════════════════

export interface SeasonalityMonthIndex {
  month: number;
  demand_index: number | null;
  sales_index: number | null;
  profit_index: number | null;
  search_demand_index?: number | null;
}

export interface SeasonalityProfile {
  id: number;
  marketplace: string;
  entity_type: string;
  entity_id: string;
  seasonality_class: string;
  demand_strength_score: number;
  sales_strength_score: number;
  profit_strength_score: number;
  evergreen_score: number;
  volatility_score: number;
  seasonality_confidence_score: number;
  peak_months: number[];
  ramp_months: number[];
  decay_months: number[];
  season_length_months: number | null;
  demand_vs_sales_gap: number | null;
  sales_vs_profit_gap: number | null;
  updated_at: string | null;
}

export interface SeasonalityMonthlyMetric {
  id: number;
  marketplace: string;
  entity_type: string;
  entity_id: string;
  year: number;
  month: number;
  sessions: number | null;
  page_views: number | null;
  clicks: number | null;
  impressions: number | null;
  purchases: number | null;
  units: number | null;
  orders: number | null;
  revenue: number | null;
  profit_cm1: number | null;
  profit_cm2: number | null;
  profit_np: number | null;
  unit_session_pct: number | null;
  ad_spend: number | null;
  refunds: number | null;
  stockout_days: number | null;
  suppression_days: number | null;
}

export interface SeasonalityOpportunity {
  id: number;
  marketplace: string;
  entity_type: string;
  entity_id: string;
  opportunity_type: string;
  title: string;
  description: string;
  priority_score: number;
  confidence_score: number;
  estimated_revenue_uplift: number | null;
  estimated_profit_uplift: number | null;
  recommended_start_date: string | null;
  status: string;
  source_signals: Record<string, unknown> | null;
  created_at: string | null;
}

export interface SeasonalityMapRow {
  entity_type: string;
  entity_id: string;
  marketplace: string;
  product_title?: string | null;
  indices: SeasonalityMonthIndex[];
  seasonality_class: string;
  peak_months: number[];
  strength_score: number;
  confidence_score: number;
  evergreen_score: number;
  volatility_score: number;
}

export interface SeasonalityOverviewKPI {
  total_entities: number;
  sku_count: number;
  category_count: number;
  seasonal_categories: number;
  evergreen_categories: number;
  strongest_upcoming_season: Record<string, unknown> | null;
  highest_demand_ramp: Record<string, unknown> | null;
  biggest_execution_gap: Record<string, unknown> | null;
  biggest_profit_opportunity: Record<string, unknown> | null;
  search_terms_count: number;
  search_blended_count: number;
}

export interface SeasonalityOverviewResponse {
  kpi: SeasonalityOverviewKPI;
  marketplace_heatmap: Record<string, unknown>[];
  class_distribution: Record<string, number>;
  upcoming_opportunities: Record<string, unknown>[];
  peak_calendar: Record<string, unknown>[];
}

export interface SeasonalityCluster {
  id: number;
  cluster_name: string;
  description: string | null;
  rules_json: Record<string, unknown> | null;
  members_count: number;
  created_by: string | null;
  created_at: string | null;
  seasonality_class?: string | null;
  peak_months?: number[];
  confidence?: number | null;
}

export interface SeasonalityEntityDetail {
  profile: SeasonalityProfile;
  monthly_metrics: SeasonalityMonthlyMetric[];
  indices: SeasonalityMonthIndex[];
  demand_vs_execution_gap: Record<string, unknown>;
  marketplace_comparison: Record<string, unknown>[];
}

export interface SeasonalitySettingsResponse {
  settings: Record<string, string>;
}

// --- Seasonality API functions ---

export const getSeasonalityOverview = (params?: { marketplace?: string }) =>
  api.get<SeasonalityOverviewResponse>("/seasonality/overview", { params }).then((r) => r.data);

export const getSeasonalityMap = (params?: {
  entity_type?: string; marketplace?: string; seasonality_class?: string;
  page?: number; page_size?: number;
}) =>
  api.get<{
    items: SeasonalityMapRow[];
    total: number;
    page: number;
    page_size: number;
    search_demand_curves?: { marketplace: string; month: number; demand_index: number; terms_count: number }[];
    available_filters?: { entity_types: string[]; marketplaces: string[]; classes: string[] };
  }>(
    "/seasonality/map", { params }
  ).then((r) => r.data);

export const getSeasonalityEntities = (params?: {
  entity_type?: string; marketplace?: string; seasonality_class?: string;
  sort?: string; page?: number; page_size?: number;
}) =>
  api.get<{ items: SeasonalityProfile[]; total: number; page: number; page_size: number }>(
    "/seasonality/entities", { params }
  ).then((r) => r.data);

export const getSeasonalityEntityDetail = (entityType: string, entityId: string, marketplace?: string) =>
  api.get<SeasonalityEntityDetail>(
    `/seasonality/entity/${entityType}/${entityId}`, { params: { marketplace } }
  ).then((r) => r.data);

export const getSeasonalityOpportunities = (params?: {
  marketplace?: string; opportunity_type?: string; status?: string;
  entity_type?: string; page?: number; page_size?: number;
}) =>
  api.get<{ items: SeasonalityOpportunity[]; total: number; page: number; page_size: number }>(
    "/seasonality/opportunities", { params }
  ).then((r) => r.data);

export const acceptSeasonalityOpportunity = (id: number) =>
  api.post(`/seasonality/opportunities/${id}/accept`).then((r) => r.data);

export const rejectSeasonalityOpportunity = (id: number) =>
  api.post(`/seasonality/opportunities/${id}/reject`).then((r) => r.data);

export const getSeasonalityClusters = () =>
  api.get<SeasonalityCluster[]>("/seasonality/clusters").then((r) => r.data);

export const getSeasonalityClusterDetail = (id: number) =>
  api.get<SeasonalityCluster>(`/seasonality/clusters/${id}`).then((r) => r.data);

export const createSeasonalityCluster = (data: {
  cluster_name: string; description?: string; rules_json?: Record<string, unknown>;
  members?: Array<{ sku?: string; asin?: string; product_type?: string; category?: string }>;
}) =>
  api.post("/seasonality/clusters", data).then((r) => r.data);

export const updateSeasonalityCluster = (id: number, data: Record<string, unknown>) =>
  api.put(`/seasonality/clusters/${id}`, data).then((r) => r.data);

export const getSeasonalitySettings = () =>
  api.get<SeasonalitySettingsResponse>("/seasonality/settings").then((r) => r.data);

export const updateSeasonalitySettings = (data: Record<string, string>) =>
  api.put<SeasonalitySettingsResponse>("/seasonality/settings", data).then((r) => r.data);

export const runSeasonalityJob = (jobType: string) =>
  api.post("/seasonality/jobs/run", null, { params: { job_type: jobType }, timeout: 300_000 }).then((r) => r.data);

// ── Tax Compliance ──────────────────────────────────────────────────

export const getTaxOverview = () =>
  api.get("/tax/overview").then((r) => r.data);

export const getTaxVatEvents = (params?: Record<string, unknown>) =>
  api.get("/tax/vat-events", { params }).then((r) => r.data);

export const recomputeClassification = (params?: Record<string, unknown>) =>
  api.post("/tax/classification/recompute", null, { params }).then((r) => r.data);

export const overrideVatClassification = (eventId: number, newClassification: string, reviewer?: string) =>
  api.post(`/tax/vat-events/${eventId}/override-classification`, null, { params: { new_classification: newClassification, reviewer } }).then((r) => r.data);

export const getTaxOssOverview = () =>
  api.get("/tax/oss/overview").then((r) => r.data);

export const getTaxOssPeriod = (year: number, quarter: number) =>
  api.get(`/tax/oss/period/${year}/${quarter}`).then((r) => r.data);

export const buildOssPeriod = (year?: number, quarter?: number) =>
  api.post("/tax/oss/build-period", null, { params: { year, quarter } }).then((r) => r.data);

export const getTaxOssCorrections = (year?: number) =>
  api.get("/tax/oss/corrections", { params: { year } }).then((r) => r.data);

export const getTaxEvidenceList = (params?: Record<string, unknown>) =>
  api.get("/tax/evidence", { params }).then((r) => r.data);

export const getTaxEvidenceSummary = (params?: Record<string, unknown>) =>
  api.get("/tax/evidence/summary", { params }).then((r) => r.data);

export const getTaxLocalVat = (params?: Record<string, unknown>) =>
  api.get("/tax/local-vat", { params }).then((r) => r.data);

export const getTaxLocalVatSummary = () =>
  api.get("/tax/local-vat/summary").then((r) => r.data);

export const getTaxFbaMovements = (params?: Record<string, unknown>) =>
  api.get("/tax/fba-movements", { params }).then((r) => r.data);

export const getTaxFbaMovementsSummary = () =>
  api.get("/tax/fba-movements/summary").then((r) => r.data);

export const getTaxReconciliation = (params?: Record<string, unknown>) =>
  api.get("/tax/reconciliation/amazon", { params }).then((r) => r.data);

export const runTaxReconciliation = (daysBack?: number) =>
  api.post("/tax/reconciliation/run", null, { params: { days_back: daysBack } }).then((r) => r.data);

export const getTaxReconciliationSummary = () =>
  api.get("/tax/reconciliation/summary").then((r) => r.data);

export const getTaxFilingReadiness = (periodRef?: string) =>
  api.get("/tax/filing-readiness", { params: { period_ref: periodRef } }).then((r) => r.data);

export const getTaxFilingBlockers = (params?: Record<string, unknown>) =>
  api.get("/tax/filing-readiness/blockers", { params }).then((r) => r.data);

export const getTaxAuditArchive = () =>
  api.get("/tax/audit-archive").then((r) => r.data);

export const generateTaxAuditPack = (periodType?: string, periodRef?: string) =>
  api.post("/tax/audit-pack/generate", null, { params: { period_type: periodType, period_ref: periodRef } }).then((r) => r.data);

export const getTaxComplianceIssues = (params?: Record<string, unknown>) =>
  api.get("/tax/compliance-issues", { params }).then((r) => r.data);

export const assignTaxIssue = (issueId: number, owner: string) =>
  api.post(`/tax/compliance-issues/${issueId}/assign`, null, { params: { owner } }).then((r) => r.data);

export const resolveTaxIssue = (issueId: number, resolver?: string) =>
  api.post(`/tax/compliance-issues/${issueId}/resolve`, null, { params: { resolver } }).then((r) => r.data);

export const detectTaxIssues = (daysBack?: number) =>
  api.post("/tax/detect-issues", null, { params: { days_back: daysBack } }).then((r) => r.data);

export const getTaxVatRates = () =>
  api.get("/tax/vat-rates").then((r) => r.data);

export const upsertVatRate = (country: string, rateType: string, rate: number, validFrom?: string) =>
  api.post("/tax/vat-rates/upsert", null, { params: { country, rate_type: rateType, rate, valid_from: validFrom } }).then((r) => r.data);

export const syncEcbRates = (daysBack?: number) =>
  api.post("/tax/ecb-rates/sync", null, { params: { days_back: daysBack } }).then((r) => r.data);

export const runTaxPipeline = (daysBack?: number) =>
  api.post("/tax/pipeline/run", null, { params: { days_back: daysBack } }).then((r) => r.data);

// ── Returns Tracker ──

export interface ReturnItem {
  id: number;
  amazon_order_id: string;
  sku: string | null;
  asin: string | null;
  marketplace_id: string;
  marketplace_code: string;
  refund_date: string | null;
  refund_type: string;
  refund_amount_pln: number;
  return_date: string | null;
  return_reason: string;
  disposition: string;
  quantity: number;
  financial_status: string;
  cogs_pln: number;
  cogs_recovered_pln: number;
  write_off_pln: number;
  manual_status: string | null;
  manual_note: string | null;
  manual_updated_by: string | null;
  source: string;
  created_at: string | null;
}

export interface ReturnsDashboardSummary {
  total_items: number;
  total_units: number;
  total_orders: number;
  total_refund_pln: number;
  total_cogs_at_risk_pln: number;
  cogs_recovered_pln: number;
  cogs_write_off_pln: number;
  cogs_pending_pln: number;
  sellable_count: number;
  damaged_count: number;
  pending_count: number;
  lost_count: number;
  reimbursed_count: number;
  sellable_rate_pct: number;
  net_loss_pln: number;
}

export interface ReturnsDashboard {
  period: { date_from: string; date_to: string };
  summary: ReturnsDashboardSummary;
  by_marketplace: unknown[];
  top_returned_skus: unknown[];
  pending_items: unknown[];
}

export const getReturnsDashboard = (params: Record<string, string>) =>
  api.get<ReturnsDashboard>("/returns/dashboard", { params }).then((r) => r.data);

export const getReturnsItems = (params: Record<string, string | number>) =>
  api.get<{ total: number; page: number; page_size: number; pages: number; items: ReturnItem[] }>(
    "/returns/items", { params }
  ).then((r) => r.data);

// ── FBA Fee Audit ──

export interface FbaFeeAnomalyPeriod {
  week_start: string;
  week_end: string | null;
  order_count: number;
  avg_fee: number;
  min_fee: number | null;
  max_fee: number | null;
}

export interface FbaFeeAnomaly {
  sku: string;
  asin: string | null;
  title: string | null;
  internal_sku: string | null;
  parent_asin: string | null;
  currency: string;
  current_period: FbaFeeAnomalyPeriod;
  previous_period: FbaFeeAnomalyPeriod;
  fee_ratio: number;
  estimated_overcharge: number;
  severity: string;
  recommendation: string | null;
}

export interface FbaFeeAnomalyResponse {
  anomalies: FbaFeeAnomaly[];
  total_anomalies: number;
  total_estimated_overcharge_eur: number;
  overcharge_by_currency: Record<string, number>;
  scan_period: { date_from: string | null; date_to: string | null };
}

export interface FbaOverchargedOrder {
  order_id: string;
  date: string;
  actual_fee: number;
  expected_fee: number;
  overcharge: number;
}

export interface FbaSkuOvercharge {
  sku: string;
  asin: string | null;
  title: string | null;
  internal_sku: string | null;
  currency: string;
  total_charges: number;
  median_fee: number;
  threshold: number;
  overcharged_order_count: number;
  estimated_overcharge: number;
  estimated_overcharge_eur: number;
  overcharged_orders: FbaOverchargedOrder[];
  severity: string;
}

export interface FbaOverchargeSummaryResponse {
  items: FbaSkuOvercharge[];
  total_skus_affected: number;
  total_affected_orders: number;
  total_estimated_overcharge_eur: number;
  overcharge_by_currency: Record<string, number>;
  scan_date: string;
  filters: Record<string, unknown>;
}

export const getFbaFeeAnomalies = (params: Record<string, string | number>) =>
  api.get<FbaFeeAnomalyResponse>("/fba/fee-audit/anomalies", { params }).then((r) => r.data);

export const getFbaFeeOvercharges = (params: Record<string, string | number>) =>
  api.get<FbaOverchargeSummaryResponse>("/fba/fee-audit/overcharges", { params }).then((r) => r.data);

// ── Guardrails ──

export interface GuardrailCheckResult {
  check_name: string;
  severity: string;
  message: string | null;
  value: number | null;
  threshold: number | null;
  query_used?: string;
  elapsed_ms: number | null;
  checked_at: string | null;
}

export interface GuardrailsRunAllResponse {
  status: string;
  elapsed_ms: number;
  summary: { ok: number; warning: number; critical: number; unknown: number };
  total_checks: number;
  checks: GuardrailCheckResult[];
}

export interface GuardrailsSummaryResponse {
  status: string;
  hours: number;
  summary: { ok: number; warning: number; critical: number; unknown: number };
  latest_per_check: Record<string, GuardrailCheckResult>;
  total_records: number;
}

export interface GuardrailHistoryPoint {
  severity: string;
  value: number | null;
  threshold: number | null;
  elapsed_ms: number | null;
  checked_at: string | null;
}

export interface GuardrailHistoryResponse {
  check_name: string;
  days: number;
  data_points: number;
  history: GuardrailHistoryPoint[];
}

export const getGuardrailsRunAll = () =>
  api.get<GuardrailsRunAllResponse>("/guardrails").then((r) => r.data);

export const getGuardrailsSummary = (params: { hours: number }) =>
  api.get<GuardrailsSummaryResponse>("/guardrails/summary", { params }).then((r) => r.data);

export const getGuardrailsHistory = (params: { check_name: string; days?: number }) =>
  api.get<GuardrailHistoryResponse>("/guardrails/history", { params }).then((r) => r.data);

// ---------------------------------------------------------------------------
// Repricing Decision Engine — Sprint 15 + Sprint 16
// ---------------------------------------------------------------------------

export interface RepricingStrategy {
  id: number;
  seller_sku: string | null;
  marketplace_id: string | null;
  strategy_type: string;
  is_active: boolean;
  parameters: Record<string, unknown>;
  min_price: number | null;
  max_price: number | null;
  min_margin_pct: number | null;
  max_daily_change_pct: number | null;
  requires_approval: boolean;
  priority: number;
  created_at: string;
  updated_at: string;
}

export interface RepricingExecution {
  id: number;
  seller_sku: string;
  asin: string | null;
  marketplace_id: string;
  strategy_id: number | null;
  strategy_type: string;
  current_price: number | null;
  target_price: number;
  final_price: number | null;
  price_change: number | null;
  price_change_pct: number | null;
  estimated_margin_pct: number | null;
  buybox_price: number | null;
  competitor_lowest: number | null;
  reason_code: string;
  reason_text: string | null;
  guardrail_applied: string | null;
  status: string;
  approved_by: string | null;
  approved_at: string | null;
  executed_at: string | null;
  error_message: string | null;
  created_at: string;
  expires_at: string | null;
  feed_id: string | null;
  auto_approved: boolean;
}

export interface RepricingDashboard {
  strategies_total: number;
  strategies_active: number;
  strategy_types: number;
  proposed: number;
  approved: number;
  executed: number;
  rejected: number;
  total_executions_30d: number;
  avg_proposed_change_pct: number | null;
}

export interface RepricingAnalyticsTrend {
  date: string;
  marketplace_id: string | null;
  proposals_created: number;
  proposals_approved: number;
  proposals_rejected: number;
  proposals_expired: number;
  executions_submitted: number;
  executions_succeeded: number;
  executions_failed: number;
  auto_approved_count: number;
  avg_price_change_pct: number | null;
  avg_margin_after: number | null;
  total_revenue_impact: number | null;
}

export interface RepricingStrategyAnalytics {
  strategy_type: string;
  total: number;
  executed: number;
  rejected: number;
  pending: number;
  avg_change_pct: number | null;
  avg_margin: number | null;
}

export const getRepricingDashboard = (params?: { marketplace_id?: string }) =>
  api.get<RepricingDashboard>("/repricing/dashboard", { params }).then((r) => r.data);

export const getRepricingStrategies = (params?: { marketplace_id?: string; active_only?: boolean; limit?: number; offset?: number }) =>
  api.get<{ items: RepricingStrategy[]; total: number; limit: number; offset: number }>("/repricing/strategies", { params }).then((r) => r.data);

export const getRepricingStrategy = (id: number) =>
  api.get<RepricingStrategy>(`/repricing/strategies/${id}`).then((r) => r.data);

export const createRepricingStrategy = (data: {
  strategy_type: string;
  seller_sku?: string;
  marketplace_id?: string;
  parameters?: Record<string, unknown>;
  min_price?: number;
  max_price?: number;
  min_margin_pct?: number;
  max_daily_change_pct?: number;
  requires_approval?: boolean;
  is_active?: boolean;
  priority?: number;
}) =>
  api.post<{ status: string }>("/repricing/strategies", data).then((r) => r.data);

export const deleteRepricingStrategy = (id: number) =>
  api.delete<{ status: string; strategy_id: number }>(`/repricing/strategies/${id}`).then((r) => r.data);

export const getRepricingExecutions = (params?: { marketplace_id?: string; status?: string; limit?: number; offset?: number }) =>
  api.get<{ items: RepricingExecution[]; total: number; limit: number; offset: number }>("/repricing/executions", { params }).then((r) => r.data);

export const getRepricingExecutionHistory = (seller_sku: string, marketplace_id: string, days = 30) =>
  api.get<{ seller_sku: string; marketplace_id: string; history: RepricingExecution[] }>(
    `/repricing/executions/history/${encodeURIComponent(seller_sku)}`, { params: { marketplace_id, days } }
  ).then((r) => r.data);

export const approveRepricingExecution = (id: number) =>
  api.post<{ status: string; execution_id: number }>(`/repricing/executions/${id}/approve`).then((r) => r.data);

export const rejectRepricingExecution = (id: number) =>
  api.post<{ status: string; execution_id: number }>(`/repricing/executions/${id}/reject`).then((r) => r.data);

export const triggerRepricingCompute = (marketplace_id?: string) =>
  api.post<{ proposals_created: number }>("/repricing/compute", null, {
    params: marketplace_id ? { marketplace_id } : {},
  }).then((r) => r.data);

// Sprint 16 — Bulk operations
export const bulkApproveRepricingExecutions = (execution_ids: number[], approved_by = "operator") =>
  api.post<{ approved: number; skipped: number }>("/repricing/executions/bulk-approve", { execution_ids, approved_by }).then((r) => r.data);

export const bulkRejectRepricingExecutions = (execution_ids: number[]) =>
  api.post<{ rejected: number; skipped: number }>("/repricing/executions/bulk-reject", { execution_ids }).then((r) => r.data);

// Sprint 16 — Auto-execution
export const autoApproveRepricingExecutions = (marketplace_id?: string) =>
  api.post<{ auto_approved: number }>("/repricing/executions/auto-approve", null, {
    params: marketplace_id ? { marketplace_id } : {},
  }).then((r) => r.data);

export const executeRepricingPrices = (marketplace_id: string) =>
  api.post<{ marketplace_id: string; submitted: number; feed_id: string | null; feed_status: string; error: string | null }>(
    "/repricing/executions/execute", null, { params: { marketplace_id } }
  ).then((r) => r.data);

// Sprint 16 — Analytics
export const triggerRepricingAnalytics = (marketplace_id?: string) =>
  api.post<Record<string, unknown>>("/repricing/analytics/compute", null, {
    params: marketplace_id ? { marketplace_id } : {},
  }).then((r) => r.data);

export const getRepricingAnalyticsTrend = (params?: { days?: number; marketplace_id?: string }) =>
  api.get<RepricingAnalyticsTrend[]>("/repricing/analytics/trend", { params }).then((r) => r.data);

export const getRepricingAnalyticsByStrategy = (params?: { days?: number; marketplace_id?: string }) =>
  api.get<RepricingStrategyAnalytics[]>("/repricing/analytics/by-strategy", { params }).then((r) => r.data);

// ════════════════════════════════════════════════════════════════════
// CONTENT OPTIMIZATION — Sprint 17
// ════════════════════════════════════════════════════════════════════

export interface ContentScore {
  id: number;
  seller_sku: string;
  asin: string | null;
  marketplace_id: string;
  total_score: number;
  title_score: number;
  bullet_score: number;
  description_score: number;
  keyword_score: number;
  image_score: number;
  aplus_score: number;
  title_length: number | null;
  bullet_count: number | null;
  avg_bullet_len: number | null;
  description_length: number | null;
  keyword_length: number | null;
  image_count: number | null;
  has_aplus: boolean;
  issues: string[];
  recommendations: string[];
  score_version: number;
  scored_at: string;
}

export interface ContentScoreDistribution {
  distribution: { poor: number; below_avg: number; average: number; good: number; excellent: number };
  total: number;
  avg_score: number | null;
  avg_by_component: {
    title: number | null;
    bullets: number | null;
    description: number | null;
    keywords: number | null;
    images: number | null;
    aplus: number | null;
  };
}

export interface ContentScoreHistory {
  date: string;
  total_score: number;
  title_score: number;
  bullet_score: number;
  description_score: number;
  keyword_score: number;
  image_score: number;
  aplus_score: number;
}

export interface SeoAnalysis {
  id: number;
  seller_sku: string;
  asin: string | null;
  marketplace_id: string;
  seo_score: number;
  keyword_coverage_pct: number | null;
  missing_keywords: { search_term: string; rank: number | null; click_share: number | null; conversion_share: number | null }[];
  top_search_terms: { search_term: string; rank: number | null; in_content: boolean }[];
  keyword_density: Record<string, number>;
  title_keyword_count: number;
  title_has_brand: boolean;
  title_has_primary_kw: boolean;
  analyzed_at: string;
}

export interface ContentOpportunity {
  seller_sku: string;
  asin: string | null;
  marketplace_id: string;
  total_score: number;
  title_score: number;
  bullet_score: number;
  description_score: number;
  keyword_score: number;
  image_score: number;
  aplus_score: number;
  issues: string[];
  recommendations: string[];
}

export const getContentScores = (params?: {
  marketplace_id?: string;
  min_score?: number;
  max_score?: number;
  limit?: number;
  offset?: number;
}) =>
  api.get<{ items: ContentScore[]; total: number; limit: number; offset: number }>("/content-optimization/scores", { params }).then((r) => r.data);

export const getContentScoreForSku = (seller_sku: string, marketplace_id: string) =>
  api.get<ContentScore>(`/content-optimization/scores/${encodeURIComponent(seller_sku)}`, { params: { marketplace_id } }).then((r) => r.data);

export const getContentScoreDistribution = (marketplace_id?: string) =>
  api.get<ContentScoreDistribution>("/content-optimization/distribution", { params: marketplace_id ? { marketplace_id } : {} }).then((r) => r.data);

export const getContentOpportunities = (params?: { marketplace_id?: string; limit?: number }) =>
  api.get<ContentOpportunity[]>("/content-optimization/opportunities", { params }).then((r) => r.data);

export const getContentScoreHistory = (seller_sku: string, marketplace_id: string, days = 30) =>
  api.get<ContentScoreHistory[]>(`/content-optimization/history/${encodeURIComponent(seller_sku)}`, { params: { marketplace_id, days } }).then((r) => r.data);

export const getSeoAnalysis = (seller_sku: string, marketplace_id: string) =>
  api.get<SeoAnalysis>(`/content-optimization/seo/${encodeURIComponent(seller_sku)}`, { params: { marketplace_id } }).then((r) => r.data);

export const triggerContentScoring = (marketplace_id: string, limit = 500) =>
  api.post<{ marketplace_id: string; listings_scored: number; avg_score: number }>("/content-optimization/compute", null, { params: { marketplace_id, limit } }).then((r) => r.data);

// ── Sprint 18 – Content A/B Testing & Multi-language ───────────────

export interface MultilangJob {
  id: number;
  seller_sku: string;
  asin: string | null;
  source_marketplace_id: string;
  target_marketplace_id: string;
  target_language: string;
  status: string;
  source_version_id: string | null;
  target_version_id: string | null;
  model: string | null;
  quality_score: number | null;
  quality_issues: string[];
  policy_flags: string[];
  error_message: string | null;
  created_at: string;
  completed_at: string | null;
}

export interface MultilangCoverage {
  seller_sku: string;
  source_marketplace_id: string;
  markets: { marketplace_id: string; language: string; status: string; quality_score: number | null }[];
}

export interface ContentExperiment {
  id: number;
  name: string;
  seller_sku: string;
  marketplace_id: string;
  status: string;
  hypothesis: string | null;
  metric_primary: string;
  start_date: string | null;
  end_date: string | null;
  winner_variant_id: number | null;
  created_by: string | null;
  created_at: string;
  concluded_at: string | null;
  variants?: ContentVariant[];
}

export interface ContentVariant {
  id: number;
  experiment_id: number;
  label: string;
  version_id: string | null;
  is_control: boolean;
  impressions: number;
  clicks: number;
  orders: number;
  revenue: number;
  conversion_rate: number;
  ctr: number;
  content_score: number | null;
  created_at: string;
}

export interface ExperimentSummary {
  total: number;
  draft: number;
  running: number;
  concluded: number;
  cancelled: number;
}

export const getMultilangJobs = (params?: { seller_sku?: string; source_marketplace_id?: string; status?: string; limit?: number; offset?: number }) =>
  api.get<MultilangJob[]>("/content-optimization/multilang/jobs", { params }).then((r) => r.data);

export const generateMultilang = (body: { seller_sku: string; source_marketplace_id: string; asin?: string; target_markets?: string[] }) =>
  api.post("/content-optimization/multilang/generate", body).then((r) => r.data);

export const getMultilangCoverage = (seller_sku: string, source_marketplace_id: string) =>
  api.get<MultilangCoverage>(`/content-optimization/multilang/coverage/${encodeURIComponent(seller_sku)}`, { params: { source_marketplace_id } }).then((r) => r.data);

export const getExperiments = (params?: { marketplace_id?: string; seller_sku?: string; status?: string; limit?: number; offset?: number }) =>
  api.get<ContentExperiment[]>("/content-optimization/experiments", { params }).then((r) => r.data);

export const getExperimentSummary = (marketplace_id?: string) =>
  api.get<ExperimentSummary>("/content-optimization/experiments/summary", { params: { marketplace_id } }).then((r) => r.data);

export const getExperiment = (id: number) =>
  api.get<ContentExperiment>(`/content-optimization/experiments/${id}`).then((r) => r.data);

export const createExperiment = (body: { name: string; seller_sku: string; marketplace_id: string; hypothesis?: string; metric_primary?: string }) =>
  api.post<ContentExperiment>("/content-optimization/experiments", body).then((r) => r.data);

export const addVariant = (experimentId: number, body: { label: string; version_id?: string; is_control?: boolean; content_score?: number }) =>
  api.post<ContentVariant>(`/content-optimization/experiments/${experimentId}/variants`, body).then((r) => r.data);

export const startExperiment = (experimentId: number) =>
  api.post(`/content-optimization/experiments/${experimentId}/start`).then((r) => r.data);

export const concludeExperiment = (experimentId: number) =>
  api.post(`/content-optimization/experiments/${experimentId}/conclude`).then((r) => r.data);

export const recordVariantMetrics = (variantId: number, body: { impressions?: number; clicks?: number; orders?: number; revenue?: number }) =>
  api.post(`/content-optimization/experiments/variants/${variantId}/metrics`, body).then((r) => r.data);

// ── Sprint 19 – SQS Queue Topology ────────────────────────────────

export interface SqsQueueTopology {
  id: number;
  domain: string;
  queue_url: string;
  queue_arn: string | null;
  dlq_url: string | null;
  dlq_arn: string | null;
  region: string;
  max_receive_count: number;
  visibility_timeout_seconds: number;
  message_retention_days: number;
  polling_interval_seconds: number;
  batch_size: number;
  enabled: boolean;
  status: string;
  messages_received: number;
  messages_processed: number;
  messages_failed: number;
  messages_dlq: number;
  last_poll_at: string | null;
  last_error: string | null;
  created_at: string;
  updated_at: string | null;
}

export interface DlqEntry {
  id: number;
  domain: string;
  queue_url: string;
  message_id: string;
  receipt_handle: string | null;
  body: string | null;
  approximate_receive_count: number;
  original_event_id: string | null;
  error_message: string | null;
  status: string;
  resolution: string | null;
  resolved_by: string | null;
  resolved_at: string | null;
  created_at: string;
}

export interface DlqSummary {
  total: number;
  unresolved: number;
  replayed: number;
  discarded: number;
  investigating: number;
}

export interface TopologyHealth {
  total_queues: number;
  active_queues: number;
  error_queues: number;
  disabled_queues: number;
  total_received: number;
  total_processed: number;
  total_failed: number;
  total_dlq: number;
  unresolved_dlq: number;
}

export interface RoutingTable {
  routes: Record<string, string>;
  domains: Record<string, string[]>;
  total_types: number;
  total_domains: number;
}

export const getSqsQueues = () =>
  api.get<SqsQueueTopology[]>("/sqs-topology/queues").then((r) => r.data);

export const getSqsQueue = (domain: string) =>
  api.get<SqsQueueTopology>(`/sqs-topology/queues/${encodeURIComponent(domain)}`).then((r) => r.data);

export const registerSqsQueue = (body: { domain: string; queue_url: string; dlq_url?: string; region?: string }) =>
  api.post("/sqs-topology/queues", body).then((r) => r.data);

export const updateSqsQueueStatus = (domain: string, body: { enabled?: boolean; status?: string }) =>
  api.patch(`/sqs-topology/queues/${encodeURIComponent(domain)}/status`, body).then((r) => r.data);

export const getTopologyHealth = () =>
  api.get<TopologyHealth>("/sqs-topology/health").then((r) => r.data);

export const getRoutingTable = () =>
  api.get<RoutingTable>("/sqs-topology/routing").then((r) => r.data);

export const pollDomainQueue = (domain: string, maxMessages?: number) =>
  api.post(`/sqs-topology/poll/${encodeURIComponent(domain)}`, null, { params: { max_messages: maxMessages } }).then((r) => r.data);

export const pollAllQueues = () =>
  api.post("/sqs-topology/poll-all").then((r) => r.data);

export const seedTopology = (body?: { base_queue_url?: string; region?: string }) =>
  api.post("/sqs-topology/seed", body ?? {}).then((r) => r.data);

export const getDlqEntries = (params?: { domain?: string; status?: string; limit?: number; offset?: number }) =>
  api.get<{ items: DlqEntry[]; total: number; limit: number; offset: number }>("/sqs-topology/dlq", { params }).then((r) => r.data);

export const getDlqSummary = () =>
  api.get<DlqSummary>("/sqs-topology/dlq/summary").then((r) => r.data);

export const resolveDlqEntry = (id: number, body: { resolution: string; resolved_by?: string }) =>
  api.post(`/sqs-topology/dlq/${id}/resolve`, body).then((r) => r.data);

// ── Sprint 20 – Event Wiring & Replay ─────────────────────────────

export interface EventWireConfig {
  id: number;
  module_name: string;
  event_domain: string;
  event_action: string;
  handler_name: string;
  description: string | null;
  enabled: boolean;
  priority: number;
  timeout_seconds: number;
  created_at: string;
  updated_at: string | null;
}

export interface WiringHealth {
  total_wires: number;
  enabled_wires: number;
  disabled_wires: number;
  domains_covered: number;
  modules_wired: number;
  domain_coverage: { domain: string; wire_count: number; enabled_count: number }[];
  unwired_domains: string[];
}

export interface ReplayJob {
  id: number;
  replay_type: string;
  scope_domain: string | null;
  scope_action: string | null;
  scope_event_ids: string | null;
  scope_since: string | null;
  scope_until: string | null;
  events_matched: number;
  events_replayed: number;
  events_processed: number;
  events_failed: number;
  status: string;
  triggered_by: string | null;
  error_message: string | null;
  started_at: string;
  completed_at: string | null;
}

export interface ReplaySummary {
  total_jobs: number;
  completed: number;
  failed: number;
  running: number;
  pending: number;
  total_events_replayed: number;
  total_events_processed: number;
  total_events_failed: number;
}

export const getEventWires = (params?: { module_name?: string; event_domain?: string; enabled_only?: boolean }) =>
  api.get<EventWireConfig[]>("/event-wiring/wires", { params }).then((r) => r.data);

export const registerEventWire = (body: { module_name: string; event_domain: string; event_action?: string; handler_name: string; description?: string }) =>
  api.post("/event-wiring/wires", body).then((r) => r.data);

export const toggleEventWire = (handlerName: string, enabled: boolean) =>
  api.patch(`/event-wiring/wires/${encodeURIComponent(handlerName)}/toggle`, { enabled }).then((r) => r.data);

export const deleteEventWire = (handlerName: string) =>
  api.delete(`/event-wiring/wires/${encodeURIComponent(handlerName)}`).then((r) => r.data);

export const seedEventWiring = () =>
  api.post("/event-wiring/wires/seed").then((r) => r.data);

export const getWiringHealth = () =>
  api.get<WiringHealth>("/event-wiring/health").then((r) => r.data);

export const registerDomainHandlers = () =>
  api.post("/event-wiring/register-handlers").then((r) => r.data);

export const replayEvents = (body: { event_domain?: string; notification_type?: string; event_ids?: string[]; since?: string; until?: string; limit?: number; triggered_by?: string }) =>
  api.post("/event-wiring/replay", body).then((r) => r.data);

export const replayDlqEntries = (body: { domain?: string; entry_ids?: number[]; triggered_by?: string }) =>
  api.post("/event-wiring/replay/dlq", body).then((r) => r.data);

export const getReplayJobs = (params?: { status?: string; replay_type?: string; limit?: number; offset?: number }) =>
  api.get<{ items: ReplayJob[]; total: number }>("/event-wiring/replay/jobs", { params }).then((r) => r.data);

export const getReplaySummary = () =>
  api.get<ReplaySummary>("/event-wiring/replay/summary").then((r) => r.data);

export const pollTopologyQueues = () =>
  api.post("/event-wiring/poll-topology").then((r) => r.data);

// ─── Sprint 21: Refund Anomaly Engine ──────────────────────────────

export interface RefundAnomaly {
  id: number;
  sku: string;
  asin: string | null;
  marketplace_id: string;
  anomaly_type: string;
  detection_date: string | null;
  period_start: string | null;
  period_end: string | null;
  baseline_rate: number;
  current_rate: number;
  spike_ratio: number;
  refund_count: number;
  order_count: number;
  refund_amount_pln: number;
  estimated_loss_pln: number;
  severity: string;
  status: string;
  resolution_note: string | null;
  resolved_by: string | null;
  resolved_at: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface SerialReturner {
  id: number;
  buyer_identifier: string;
  marketplace_id: string;
  detection_date: string | null;
  return_count: number;
  order_count: number;
  return_rate: number;
  total_refund_pln: number;
  avg_refund_pln: number;
  first_return_date: string | null;
  last_return_date: string | null;
  top_skus: string | null;
  risk_score: number;
  risk_tier: string;
  status: string;
  notes: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface ReimbursementCase {
  id: number;
  case_type: string;
  sku: string;
  asin: string | null;
  marketplace_id: string;
  amazon_order_id: string | null;
  fnsku: string | null;
  quantity: number;
  estimated_value_pln: number;
  evidence_summary: string | null;
  amazon_case_id: string | null;
  status: string;
  filed_at: string | null;
  resolved_at: string | null;
  reimbursed_amount_pln: number;
  resolution_note: string | null;
  created_at: string | null;
}

export interface AnomalyDashboard {
  anomalies: {
    total: number;
    open: number;
    critical_open: number;
    high_open: number;
    total_estimated_loss_pln: number;
    open_estimated_loss_pln: number;
  };
  serial_returners: {
    total_active: number;
    critical: number;
    high: number;
    total_refund_exposure_pln: number;
  };
  reimbursements: {
    total_cases: number;
    pending: number;
    filed: number;
    paid: number;
    total_estimated_value_pln: number;
    total_reimbursed_pln: number;
  };
}

export interface PaginatedList<T> {
  items: T[];
  total: number;
  limit: number;
  offset: number;
}

export const getAnomalyDashboard = () =>
  api.get<AnomalyDashboard>("/refund-anomaly/dashboard").then((r) => r.data);

export const triggerAnomalyScan = (body?: { marketplace_id?: string }) =>
  api.post("/refund-anomaly/scan", body ?? {}).then((r) => r.data);

export const getRefundAnomalies = (params?: {
  anomaly_type?: string;
  severity?: string;
  status?: string;
  marketplace_id?: string;
  sku?: string;
  limit?: number;
  offset?: number;
}) => api.get<PaginatedList<RefundAnomaly>>("/refund-anomaly/anomalies", { params }).then((r) => r.data);

export const updateAnomalyStatus = (anomalyId: number, body: { status: string; resolution_note?: string; resolved_by?: string }) =>
  api.put(`/refund-anomaly/anomalies/${anomalyId}/status`, body).then((r) => r.data);

export const getSerialReturners = (params?: {
  risk_tier?: string;
  status?: string;
  marketplace_id?: string;
  limit?: number;
  offset?: number;
}) => api.get<PaginatedList<SerialReturner>>("/refund-anomaly/serial-returners", { params }).then((r) => r.data);

export const updateReturnerStatus = (returnerId: number, body: { status: string; notes?: string }) =>
  api.put(`/refund-anomaly/serial-returners/${returnerId}/status`, body).then((r) => r.data);

export const getReimbursementCases = (params?: {
  case_type?: string;
  status?: string;
  marketplace_id?: string;
  limit?: number;
  offset?: number;
}) => api.get<PaginatedList<ReimbursementCase>>("/refund-anomaly/reimbursement-cases", { params }).then((r) => r.data);

export const updateCaseStatus = (caseId: number, body: { status: string; amazon_case_id?: string; reimbursed_amount_pln?: number; resolution_note?: string }) =>
  api.put(`/refund-anomaly/reimbursement-cases/${caseId}/status`, body).then((r) => r.data);

// ── Sprint 22: Detail, trend, and export endpoints ──

export interface AnomalyTrendPoint {
  week_start: string | null;
  anomaly_type: string;
  count: number;
  critical_count: number;
  high_count: number;
  total_loss_pln: number;
}

export const getAnomalyDetail = (id: number) =>
  api.get<RefundAnomaly>(`/refund-anomaly/anomalies/${id}`).then((r) => r.data);

export const getReturnerDetail = (id: number) =>
  api.get<SerialReturner>(`/refund-anomaly/serial-returners/${id}`).then((r) => r.data);

export const getCaseDetail = (id: number) =>
  api.get<ReimbursementCase>(`/refund-anomaly/reimbursement-cases/${id}`).then((r) => r.data);

export const getAnomalyTrends = (params?: { days?: number; anomaly_type?: string; marketplace_id?: string }) =>
  api.get<AnomalyTrendPoint[]>("/refund-anomaly/trends", { params }).then((r) => r.data);

export const exportAnomaliesCsv = (params?: { anomaly_type?: string; severity?: string; status?: string; marketplace_id?: string }) =>
  api.get("/refund-anomaly/anomalies/export/csv", { params, responseType: "blob" }).then((r) => r.data);

export const exportReturnersCsv = (params?: { risk_tier?: string; status?: string; marketplace_id?: string }) =>
  api.get("/refund-anomaly/serial-returners/export/csv", { params, responseType: "blob" }).then((r) => r.data);

export const exportCasesCsv = (params?: { case_type?: string; status?: string; marketplace_id?: string }) =>
  api.get("/refund-anomaly/reimbursement-cases/export/csv", { params, responseType: "blob" }).then((r) => r.data);

// ── Sprint 23-24: Operator Console v2 ────────────────────────────

export interface FeedItem {
  source: string;
  source_id: string;
  title: string;
  description: string | null;
  severity: string;
  marketplace_id: string | null;
  sku: string | null;
  asin: string | null;
  status: string;
  created_at: string;
}

export interface FeedResponse {
  items: FeedItem[];
  total: number;
  page: number;
  page_size: number;
}

export interface OperatorCase {
  id: number;
  title: string;
  description: string | null;
  category: string;
  priority: string;
  status: string;
  marketplace_id: string | null;
  sku: string | null;
  asin: string | null;
  source_type: string | null;
  source_id: string | null;
  assigned_to: string | null;
  resolution_note: string | null;
  resolved_by: string | null;
  resolved_at: string | null;
  due_date: string | null;
  tags: string | null;
  created_at: string;
  updated_at: string;
}

export interface ActionQueueItem {
  id: number;
  action_type: string;
  title: string;
  description: string | null;
  marketplace_id: string | null;
  sku: string | null;
  asin: string | null;
  payload: Record<string, unknown> | null;
  risk_level: string;
  requires_approval: boolean;
  status: string;
  requested_by: string;
  approved_by: string | null;
  approved_at: string | null;
  rejected_by: string | null;
  rejected_at: string | null;
  rejection_reason: string | null;
  executed_at: string | null;
  execution_result: string | null;
  error_message: string | null;
  expires_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface OperatorDashboard {
  alerts: { total: number; critical: number; unresolved: number };
  system_alerts: { total: number; critical: number };
  anomalies: { total: number; critical: number; open: number };
  cases: { total: number; open: number; critical: number };
  action_queue: { total: number; pending_approval: number };
}

export const getOperatorDashboard = () =>
  api.get<OperatorDashboard>("/operator-console/dashboard").then((r) => r.data);

export const getUnifiedFeed = (params?: { days?: number; severity?: string; marketplace_id?: string; source?: string; page?: number; page_size?: number }) =>
  api.get<FeedResponse>("/operator-console/feed", { params }).then((r) => r.data);

export const getFeedSummary = (params?: { days?: number }) =>
  api.get<OperatorDashboard>("/operator-console/feed/summary", { params }).then((r) => r.data);

export const getOperatorCases = (params?: { status?: string; category?: string; priority?: string; assigned_to?: string; marketplace_id?: string; page?: number; page_size?: number }) =>
  api.get<{ items: OperatorCase[]; total: number; page: number; page_size: number }>("/operator-console/cases", { params }).then((r) => r.data);

export const getOperatorCase = (caseId: number) =>
  api.get<OperatorCase>(`/operator-console/cases/${caseId}`).then((r) => r.data);

export const createOperatorCase = (data: { title: string; description?: string; category?: string; priority?: string; marketplace_id?: string; sku?: string; asin?: string; assigned_to?: string; due_date?: string; tags?: string }) =>
  api.post("/operator-console/cases", data).then((r) => r.data);

export const updateOperatorCase = (caseId: number, data: { status?: string; priority?: string; assigned_to?: string; resolution_note?: string; resolved_by?: string }) =>
  api.patch(`/operator-console/cases/${caseId}`, data).then((r) => r.data);

export const getActionQueue = (params?: { status?: string; action_type?: string; marketplace_id?: string; page?: number; page_size?: number }) =>
  api.get<{ items: ActionQueueItem[]; total: number; page: number; page_size: number }>("/operator-console/actions", { params }).then((r) => r.data);

export const getActionQueueItem = (actionId: number) =>
  api.get<ActionQueueItem>(`/operator-console/actions/${actionId}`).then((r) => r.data);

export const submitAction = (data: { action_type: string; title: string; description?: string; marketplace_id?: string; sku?: string; asin?: string; payload?: Record<string, unknown>; risk_level?: string; requested_by: string; expires_hours?: number }) =>
  api.post("/operator-console/actions", data).then((r) => r.data);

export const approveAction = (actionId: number, data: { approved_by: string }) =>
  api.post(`/operator-console/actions/${actionId}/approve`, data).then((r) => r.data);

export const rejectAction = (actionId: number, data: { rejected_by: string; reason?: string }) =>
  api.post(`/operator-console/actions/${actionId}/reject`, data).then((r) => r.data);

// ── Sprint 25-26: Account Hub (Multi-seller) ────────────────────

export interface SellerAccount {
  id: number;
  seller_id: string;
  name: string;
  company_name: string | null;
  marketplace_ids: string[];
  primary_marketplace: string | null;
  region: string;
  status: string;
  notes: string | null;
  created_at: string;
  updated_at: string;
}

export interface SellerCredentialMeta {
  id: number;
  credential_type: string;
  credential_key: string;
  is_valid: boolean;
  expires_at: string | null;
  last_validated_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface SellerPermission {
  id: number;
  user_email: string;
  seller_account_id?: number;
  permission_level: string;
  granted_by: string;
  granted_at: string;
  seller_id?: string;
  seller_name?: string;
}

export interface AccountHubDashboard {
  sellers: { total: number; active: number; onboarding: number; suspended: number };
  users_with_access: number;
  valid_credentials: number;
}

export interface CredentialValidation {
  seller_account_id: number;
  valid: boolean;
  missing_keys: string[];
  total_credentials: number;
}

export interface SchedulerStatus {
  seller_account_id: number;
  seller_id: string;
  name: string;
  status: string;
  jobs_last_24h: number;
  last_job_at: string | null;
}

export const getAccountHubDashboard = () =>
  api.get<AccountHubDashboard>("/account-hub/dashboard").then((r) => r.data);

export const getSellerAccounts = (params?: { status?: string; page?: number; page_size?: number }) =>
  api.get<{ items: SellerAccount[]; total: number; page: number; page_size: number }>("/account-hub/sellers", { params }).then((r) => r.data);

export const getSellerAccount = (id: number) =>
  api.get<SellerAccount>(`/account-hub/sellers/${id}`).then((r) => r.data);

export const createSellerAccount = (data: { seller_id: string; name: string; company_name?: string; marketplace_ids?: string[]; primary_marketplace?: string; region?: string; notes?: string }) =>
  api.post("/account-hub/sellers", data).then((r) => r.data);

export const updateSellerAccount = (id: number, data: { name?: string; company_name?: string; marketplace_ids?: string[]; primary_marketplace?: string; region?: string; status?: string; notes?: string }) =>
  api.patch(`/account-hub/sellers/${id}`, data).then((r) => r.data);

export const getSellerCredentials = (sellerAccountId: number) =>
  api.get<SellerCredentialMeta[]>(`/account-hub/sellers/${sellerAccountId}/credentials`).then((r) => r.data);

export const storeSellerCredential = (sellerAccountId: number, data: { credential_type: string; credential_key: string; plaintext_value: string; expires_at?: string }) =>
  api.post(`/account-hub/sellers/${sellerAccountId}/credentials`, data).then((r) => r.data);

export const revokeSellerCredential = (credentialId: number) =>
  api.delete(`/account-hub/credentials/${credentialId}`).then((r) => r.data);

export const validateSellerCredentials = (sellerAccountId: number) =>
  api.get<CredentialValidation>(`/account-hub/sellers/${sellerAccountId}/credentials/validate`).then((r) => r.data);

export const getSellerPermissions = (sellerAccountId: number) =>
  api.get<SellerPermission[]>(`/account-hub/sellers/${sellerAccountId}/permissions`).then((r) => r.data);

export const grantSellerPermission = (sellerAccountId: number, data: { user_email: string; permission_level?: string; granted_by: string }) =>
  api.post(`/account-hub/sellers/${sellerAccountId}/permissions`, data).then((r) => r.data);

export const revokeSellerPermission = (sellerAccountId: number, data: { user_email: string }) =>
  api.delete(`/account-hub/sellers/${sellerAccountId}/permissions`, { data }).then((r) => r.data);

export const getUserPermissions = (userEmail: string) =>
  api.get<SellerPermission[]>(`/account-hub/users/${encodeURIComponent(userEmail)}/permissions`).then((r) => r.data);

export const getSellerSchedulerStatus = () =>
  api.get<SchedulerStatus[]>("/account-hub/scheduler-status").then((r) => r.data);

// ---------------------------------------------------------------------------
// Intelligence Hub (Sprint 4 — Unified Intelligence Dashboard)
// ---------------------------------------------------------------------------
export interface IntelligenceFunnelResponse {
  funnel: {
    detected: number;
    do_now: number;
    this_week: number;
    this_month: number;
    blocked: number;
    completed_30d: number;
  };
  model_quality: {
    types_tracked: number;
    avg_prediction_accuracy: number;
    avg_win_rate: number;
    total_evaluations: number;
    avg_roi: number;
  } | null;
  by_type: { opportunity_type: string; count: number; pct: number }[];
}

export const getIntelligenceDashboard = (params?: Record<string, string>) =>
  api.get<Record<string, unknown>>("/intelligence/dashboard", { params }).then((r) => r.data);

export const getIntelligenceFunnel = (params?: { marketplace_id?: string }) =>
  api.get<IntelligenceFunnelResponse>("/intelligence/funnel", { params }).then((r) => r.data);

export const getIntelligenceForecastAccuracy = (params?: { opportunity_type?: string }) =>
  api.get<Record<string, unknown>>("/intelligence/forecast-accuracy", { params }).then((r) => r.data);
