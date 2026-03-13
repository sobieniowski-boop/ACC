from fastapi import APIRouter

from app.api.v1.alerts import router as alerts_router
from app.api.v1.auth import router as auth_router
from app.api.v1.jobs import router as jobs_router
from app.api.v1.planning import router as planning_router
from app.api.v1.profit import router as profit_router
from app.api.v1.profit_v2 import router as profit_v2_router
from app.api.v1.routes_health import router as health_router
from app.api.v1.kpi import router as kpi_router
from app.api.v1.pricing import router as pricing_router
from app.api.v1.inventory_routes import router as inventory_router
from app.api.v1.ads import router as ads_router
from app.api.v1.ai_rec import router as ai_router
from app.api.v1.audit import router as audit_router
from app.api.v1.families import router as families_router
from app.api.v1.import_products import router as import_products_router
from app.api.v1.content_ops import router as content_ops_router
from app.api.v1.fba_ops import router as fba_ops_router
from app.api.v1.finance_center import router as finance_center_router
from app.api.v1.manage_inventory import router as manage_inventory_router
from app.api.v1.inventory_taxonomy import router as inventory_taxonomy_router
from app.api.v1.returns import router as returns_router
from app.api.v1.gls import router as gls_router
from app.api.v1.dhl import router as dhl_router
from app.api.v1.courier import router as courier_router
from app.api.v1.profitability import router as profitability_router
from app.api.v1.executive import router as executive_router
from app.api.v1.strategy import router as strategy_router
from app.api.v1.outcomes import router as outcomes_router
from app.api.v1.seasonality import router as seasonality_router
from app.api.v1.tax_compliance import router as tax_compliance_router
from app.api.v1.guardrails import router as guardrails_router
from app.api.v1.notifications import router as notifications_router
from app.api.v1.listing_state import router as listing_state_router
from app.api.v1.catalog_definitions import router as catalog_definitions_router
from app.api.v1.pricing_state import router as pricing_state_router
from app.api.v1.backbone import router as backbone_router
from app.api.v1.catalog_health import router as catalog_health_router
from app.api.v1.buybox_radar import router as buybox_radar_router
from app.api.v1.inventory_risk import router as inventory_risk_router
from app.api.v1.repricing import router as repricing_router
from app.api.v1.content_optimization import router as content_optimization_router
from app.api.v1.content_ab_testing import router as content_ab_testing_router
from app.api.v1.sqs_topology import router as sqs_topology_router
from app.api.v1.event_wiring import router as event_wiring_router
from app.api.v1.refund_anomaly import router as refund_anomaly_router
from app.api.v1.operator_console import router as operator_console_router
from app.api.v1.account_hub import router as account_hub_router
from app.api.v1.system import router as system_router
from app.api.v1.intelligence import router as intelligence_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(auth_router)
api_router.include_router(profit_router)
api_router.include_router(profit_v2_router)
api_router.include_router(alerts_router)
api_router.include_router(jobs_router)
api_router.include_router(planning_router)
api_router.include_router(kpi_router)
api_router.include_router(pricing_router)
api_router.include_router(inventory_router)
api_router.include_router(ads_router)
api_router.include_router(ai_router)
api_router.include_router(audit_router)
api_router.include_router(families_router)
api_router.include_router(import_products_router)
api_router.include_router(content_ops_router)
api_router.include_router(fba_ops_router)
api_router.include_router(finance_center_router)
api_router.include_router(manage_inventory_router)
api_router.include_router(inventory_taxonomy_router)
api_router.include_router(returns_router)
api_router.include_router(gls_router)
api_router.include_router(dhl_router)
api_router.include_router(courier_router)
api_router.include_router(profitability_router)
api_router.include_router(executive_router)
api_router.include_router(strategy_router)
api_router.include_router(outcomes_router)
api_router.include_router(seasonality_router)
api_router.include_router(tax_compliance_router)
api_router.include_router(guardrails_router)
api_router.include_router(notifications_router)
api_router.include_router(listing_state_router)
api_router.include_router(catalog_definitions_router)
api_router.include_router(pricing_state_router)
api_router.include_router(backbone_router)
api_router.include_router(catalog_health_router)
api_router.include_router(buybox_radar_router)
api_router.include_router(inventory_risk_router)
api_router.include_router(repricing_router)
api_router.include_router(content_optimization_router)
api_router.include_router(content_ab_testing_router)
api_router.include_router(sqs_topology_router)
api_router.include_router(event_wiring_router)
api_router.include_router(refund_anomaly_router)
api_router.include_router(operator_console_router)
api_router.include_router(account_hub_router)
api_router.include_router(system_router)
api_router.include_router(intelligence_router)
