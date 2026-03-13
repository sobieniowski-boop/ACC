import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { Suspense, lazy } from "react";
import { useAuthStore } from "@/store/authStore";
import Layout from "@/components/layout/Layout";
import LoginPage from "@/pages/Login";

// ── Lazy-loaded page components (code-split per module) ──
const DashboardPage = lazy(() => import("@/pages/Dashboard"));
const ProfitExplorerPage = lazy(() => import("@/pages/ProfitExplorer"));
const JobsPage = lazy(() => import("@/pages/Jobs"));
const AlertsPage = lazy(() => import("@/pages/Alerts"));
const PricingPage = lazy(() => import("@/pages/Pricing"));
const PlanningPage = lazy(() => import("@/pages/Planning"));
const AdsPage = lazy(() => import("@/pages/Ads"));
const AIRecommendationsPage = lazy(() => import("@/pages/AIRecommendations"));
const FamilyMapperPage = lazy(() => import("@/pages/FamilyMapper"));
const FamilyDetailPage = lazy(() => import("@/pages/FamilyDetail"));
const ReviewQueuePage = lazy(() => import("@/pages/ReviewQueue"));
const FixPackagesPage = lazy(() => import("@/pages/FixPackages"));
const ProductProfitTablePage = lazy(() => import("@/pages/ProductProfitTable"));
const ProductDrilldownPage = lazy(() => import("@/pages/ProductDrilldown"));
const FeeBreakdownPage = lazy(() => import("@/pages/FeeBreakdown"));
const LossOrdersPage = lazy(() => import("@/pages/LossOrders"));
const DataQualityPage = lazy(() => import("@/pages/DataQuality"));
const ProductTasksPage = lazy(() => import("@/pages/ProductTasks"));
const ImportProductsPage = lazy(() => import("@/pages/ImportProductsPage"));
const ContentStudioPage = lazy(() => import("@/pages/ContentStudio"));
const ContentCompliancePage = lazy(() => import("@/pages/ContentCompliance"));
const ContentAssetsPage = lazy(() => import("@/pages/ContentAssets"));
const ContentPublishPage = lazy(() => import("@/pages/ContentPublish"));
const NetfoxHealthPage = lazy(() => import("@/pages/NetfoxHealth"));
const FbaOverviewPage = lazy(() => import("@/pages/FbaOverview"));
const FbaInventoryPage = lazy(() => import("@/pages/FbaInventory"));
const FbaReplenishmentPage = lazy(() => import("@/pages/FbaReplenishment"));
const FbaInboundPage = lazy(() => import("@/pages/FbaInbound"));
const FbaAgedStrandedPage = lazy(() => import("@/pages/FbaAgedStranded"));
const FbaBundlesPage = lazy(() => import("@/pages/FbaBundles"));
const FbaScorecardPage = lazy(() => import("@/pages/FbaScorecard"));
const ProfitOverviewPage = lazy(() => import("@/pages/ProfitOverview"));
const ProfitabilityOrdersPage = lazy(() => import("@/pages/ProfitabilityOrders"));
const ProfitabilityProductsPage = lazy(() => import("@/pages/ProfitabilityProducts"));
const PriceSimulatorPage = lazy(() => import("@/pages/PriceSimulator"));
const FinanceDashboardPage = lazy(() => import("@/pages/FinanceDashboard"));
const FinanceLedgerPage = lazy(() => import("@/pages/FinanceLedger"));
const FinanceReconciliationPage = lazy(() => import("@/pages/FinanceReconciliation"));
const InventoryOverviewPage = lazy(() => import("@/pages/InventoryOverview"));
const ManageAllInventoryPage = lazy(() => import("@/pages/ManageAllInventory"));
const InventoryFamiliesPage = lazy(() => import("@/pages/InventoryFamilies"));
const InventoryDraftsPage = lazy(() => import("@/pages/InventoryDrafts"));
const InventoryJobsPage = lazy(() => import("@/pages/InventoryJobs"));
const InventorySettingsPage = lazy(() => import("@/pages/InventorySettings"));
const ExecOverviewPage = lazy(() => import("@/pages/ExecOverview"));
const ExecProductsPage = lazy(() => import("@/pages/ExecProducts"));
const ExecMarketplacesPage = lazy(() => import("@/pages/ExecMarketplaces"));
const StrategyOverviewPage = lazy(() => import("@/pages/StrategyOverview"));
const StrategyOpportunitiesPage = lazy(() => import("@/pages/StrategyOpportunities"));
const StrategyPlaybooksPage = lazy(() => import("@/pages/StrategyPlaybooks"));
const StrategyMarketExpansionPage = lazy(() => import("@/pages/StrategyMarketExpansion"));
const StrategyBundlesPage = lazy(() => import("@/pages/StrategyBundles"));
const StrategyExperimentsPage = lazy(() => import("@/pages/StrategyExperiments"));
const StrategyOutcomesPage = lazy(() => import("@/pages/StrategyOutcomes"));
const StrategyLearningPage = lazy(() => import("@/pages/StrategyLearning"));
const SeasonalityOverviewPage = lazy(() => import("@/pages/SeasonalityOverview"));
const SeasonalityMapPage = lazy(() => import("@/pages/SeasonalityMap"));
const SeasonalityEntitiesPage = lazy(() => import("@/pages/SeasonalityEntities"));
const SeasonalityEntityDetailPage = lazy(() => import("@/pages/SeasonalityEntityDetail"));
const SeasonalityClustersPage = lazy(() => import("@/pages/SeasonalityClusters"));
const SeasonalityOpportunitiesPage = lazy(() => import("@/pages/SeasonalityOpportunities"));
const SeasonalitySettingsPage = lazy(() => import("@/pages/SeasonalitySettings"));
const TaxOverviewPage = lazy(() => import("@/pages/TaxOverview"));
const TaxVatClassificationPage = lazy(() => import("@/pages/TaxVatClassification"));
const TaxOssPage = lazy(() => import("@/pages/TaxOss"));
const TaxLocalVatPage = lazy(() => import("@/pages/TaxLocalVat"));
const TaxFbaMovementsPage = lazy(() => import("@/pages/TaxFbaMovements"));
const TaxEvidencePage = lazy(() => import("@/pages/TaxEvidence"));
const TaxReconciliationPage = lazy(() => import("@/pages/TaxReconciliation"));
const TaxFilingReadinessPage = lazy(() => import("@/pages/TaxFilingReadiness"));
const TaxAuditArchivePage = lazy(() => import("@/pages/TaxAuditArchive"));
const TaxSettingsPage = lazy(() => import("@/pages/TaxSettings"));
const ReturnsTrackerPage = lazy(() => import("@/pages/ReturnsTracker"));
const FbaFeeAuditPage = lazy(() => import("@/pages/FbaFeeAudit"));
const GuardrailsDashboardPage = lazy(() => import("@/pages/GuardrailsDashboard"));
const CompetitorRadarPage = lazy(() => import("@/pages/CompetitorRadar"));
const InventoryRiskPage = lazy(() => import("@/pages/InventoryRisk"));
const RepricingEnginePage = lazy(() => import("@/pages/RepricingEngine"));
const ContentScoresPage = lazy(() => import("@/pages/ContentScores"));
const ContentABTestingPage = lazy(() => import("@/pages/ContentABTesting"));
const SqsTopologyPage = lazy(() => import("@/pages/SqsTopology"));
const EventWiringPage = lazy(() => import("@/pages/EventWiring"));
const RefundAnomaliesPage = lazy(() => import("@/pages/RefundAnomalies"));
const OperatorConsolePage = lazy(() => import("@/pages/OperatorConsole"));
const AccountHubPage = lazy(() => import("@/pages/AccountHub"));

function PrivateRoute({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((s) => s.accessToken);
  return token ? <>{children}</> : <Navigate to="/login" replace />;
}

function LoadingFallback() {
  return (
    <div className="flex items-center justify-center h-64">
      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary" />
    </div>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <Suspense fallback={<LoadingFallback />}>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route
            path="/"
            element={
              <PrivateRoute>
                <Layout />
              </PrivateRoute>
            }
          >
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard" element={<DashboardPage />} />
          <Route path="exec/overview" element={<ExecOverviewPage />} />
          <Route path="exec/products" element={<ExecProductsPage />} />
          <Route path="exec/marketplaces" element={<ExecMarketplacesPage />} />
          <Route path="strategy/overview" element={<StrategyOverviewPage />} />
          <Route path="strategy/opportunities" element={<StrategyOpportunitiesPage />} />
          <Route path="strategy/playbooks" element={<StrategyPlaybooksPage />} />
          <Route path="strategy/market-expansion" element={<StrategyMarketExpansionPage />} />
          <Route path="strategy/bundles" element={<StrategyBundlesPage />} />
          <Route path="strategy/experiments" element={<StrategyExperimentsPage />} />
          <Route path="strategy/outcomes" element={<StrategyOutcomesPage />} />
          <Route path="strategy/learning" element={<StrategyLearningPage />} />
          <Route path="seasonality/overview" element={<SeasonalityOverviewPage />} />
          <Route path="seasonality/map" element={<SeasonalityMapPage />} />
          <Route path="seasonality/entities" element={<SeasonalityEntitiesPage />} />
          <Route path="seasonality/entity/:type/:id" element={<SeasonalityEntityDetailPage />} />
          <Route path="seasonality/clusters" element={<SeasonalityClustersPage />} />
          <Route path="seasonality/opportunities" element={<SeasonalityOpportunitiesPage />} />
          <Route path="seasonality/settings" element={<SeasonalitySettingsPage />} />
          <Route path="profit/overview" element={<ProfitOverviewPage />} />
          <Route path="profit" element={<ProfitExplorerPage />} />
          <Route path="profit/products" element={<ProductProfitTablePage />} />
          <Route path="profit/drilldown" element={<ProductDrilldownPage />} />
          <Route path="profit/fee-breakdown" element={<FeeBreakdownPage />} />
          <Route path="profit/loss-orders" element={<LossOrdersPage />} />
          <Route path="profit/data-quality" element={<DataQualityPage />} />
          <Route path="profit/tasks" element={<ProductTasksPage />} />
          <Route path="profit/orders" element={<ProfitabilityOrdersPage />} />
          <Route path="profit/profitability-products" element={<ProfitabilityProductsPage />} />
          <Route path="profit/simulator" element={<PriceSimulatorPage />} />
          <Route path="jobs" element={<JobsPage />} />
          <Route path="alerts" element={<AlertsPage />} />
          <Route path="pricing" element={<PricingPage />} />
          <Route path="pricing/competitors" element={<CompetitorRadarPage />} />
          <Route path="pricing/repricing" element={<RepricingEnginePage />} />
          <Route path="planning" element={<PlanningPage />} />
          <Route path="inventory" element={<Navigate to="/inventory/overview" replace />} />
          <Route path="inventory/overview" element={<InventoryOverviewPage />} />
          <Route path="inventory/all" element={<ManageAllInventoryPage />} />
          <Route path="inventory/families" element={<InventoryFamiliesPage />} />
          <Route path="inventory/drafts" element={<InventoryDraftsPage />} />
          <Route path="inventory/jobs" element={<InventoryJobsPage />} />
          <Route path="inventory/settings" element={<InventorySettingsPage />} />
          <Route path="inventory/risk" element={<InventoryRiskPage />} />
          <Route path="ads" element={<AdsPage />} />
          <Route path="ai" element={<AIRecommendationsPage />} />
          <Route path="import-products" element={<ImportProductsPage />} />
          <Route path="content/studio" element={<ContentStudioPage />} />
          <Route path="content-ops" element={<Navigate to="/content/studio?tab=onboard" replace />} />
          <Route path="content/dashboard" element={<Navigate to="/content/studio?tab=overview" replace />} />
          <Route path="content/editor" element={<Navigate to="/content/studio?tab=editor" replace />} />
          <Route path="content/health" element={<Navigate to="/content/studio?tab=overview" replace />} />
          <Route path="content/compliance" element={<ContentCompliancePage />} />
          <Route path="content/assets" element={<ContentAssetsPage />} />
          <Route path="content/publish" element={<ContentPublishPage />} />
          <Route path="content/scores" element={<ContentScoresPage />} />
          <Route path="content/ab-testing" element={<ContentABTestingPage />} />
          <Route path="system/netfox-health" element={<NetfoxHealthPage />} />
          <Route path="system/guardrails" element={<GuardrailsDashboardPage />} />
          <Route path="system/sqs-topology" element={<SqsTopologyPage />} />
          <Route path="system/event-wiring" element={<EventWiringPage />} />
          <Route path="system/settings" element={<Navigate to="/dashboard" replace />} />
          <Route path="fba/overview" element={<FbaOverviewPage />} />
          <Route path="fba/inventory" element={<FbaInventoryPage />} />
          <Route path="fba/replenishment" element={<FbaReplenishmentPage />} />
          <Route path="fba/inbound" element={<FbaInboundPage />} />
          <Route path="fba/aged-stranded" element={<FbaAgedStrandedPage />} />
          <Route path="fba/bundles" element={<FbaBundlesPage />} />
          <Route path="fba/kpi-scorecard" element={<FbaScorecardPage />} />
          <Route path="fba/returns" element={<ReturnsTrackerPage />} />
          <Route path="fba/fee-audit" element={<FbaFeeAuditPage />} />
          <Route path="fba/refund-anomalies" element={<RefundAnomaliesPage />} />
          <Route path="finance/dashboard" element={<FinanceDashboardPage />} />
          <Route path="finance/ledger" element={<FinanceLedgerPage />} />
          <Route path="finance/reconciliation" element={<FinanceReconciliationPage />} />
          <Route path="tax/overview" element={<TaxOverviewPage />} />
          <Route path="tax/classification" element={<TaxVatClassificationPage />} />
          <Route path="tax/oss" element={<TaxOssPage />} />
          <Route path="tax/local-vat" element={<TaxLocalVatPage />} />
          <Route path="tax/fba-movements" element={<TaxFbaMovementsPage />} />
          <Route path="tax/evidence" element={<TaxEvidencePage />} />
          <Route path="tax/reconciliation" element={<TaxReconciliationPage />} />
          <Route path="tax/filing-readiness" element={<TaxFilingReadinessPage />} />
          <Route path="tax/audit-archive" element={<TaxAuditArchivePage />} />
          <Route path="tax/settings" element={<TaxSettingsPage />} />
          <Route path="families" element={<FamilyMapperPage />} />
          <Route path="families/review" element={<ReviewQueuePage />} />
          <Route path="families/fix-packages" element={<FixPackagesPage />} />
          <Route path="families/:id" element={<FamilyDetailPage />} />
          <Route path="operator/console" element={<OperatorConsolePage />} />
          <Route path="operator/accounts" element={<AccountHubPage />} />
        </Route>
        </Routes>
      </Suspense>
    </BrowserRouter>
  );
}
