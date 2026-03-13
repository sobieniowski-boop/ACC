"""SQLAlchemy models package — import all so Alembic can detect them."""
from app.models.user import User
from app.models.marketplace import Marketplace
from app.models.product import Product
from app.models.offer import Offer
from app.models.order import AccOrder, OrderLine
from app.models.inventory import InventorySnapshot
from app.models.finance import FinanceTransaction
from app.models.ads import AdsCampaign, AdsCampaignDay
from app.models.plan import PlanMonth, PlanLine
from app.models.alert import Alert, AlertRule
from app.models.ai import AIRecommendation
from app.models.job import JobRun
from app.models.exchange_rate import ExchangeRate
from app.models.purchase_price import PurchasePrice
from app.models.shipment import (
    AccShipment,
    AccShipmentOrderLink,
    AccShipmentEvent,
    AccShipmentPod,
    AccShipmentCost,
    AccOrderCourierRelation,
    AccShipmentOutcomeFact,
    AccOrderLogisticsFact,
    AccOrderLogisticsShadow,
)
from app.models.family import (
    GlobalFamily,
    GlobalFamilyChild,
    MarketplaceListingChild,
    GlobalFamilyChildMarketLink,
    GlobalFamilyMarketLink,
    FamilyCoverageCache,
    FamilyIssuesCache,
    FamilyFixPackage,
    FamilyFixJob,
)

__all__ = [
    "User",
    "Marketplace",
    "Product",
    "Offer",
    "AccOrder",
    "OrderLine",
    "InventorySnapshot",
    "FinanceTransaction",
    "AdsCampaign",
    "AdsCampaignDay",
    "PlanMonth",
    "PlanLine",
    "Alert",
    "AlertRule",
    "AIRecommendation",
    "JobRun",
    "ExchangeRate",
    "PurchasePrice",
    "AccShipment",
    "AccShipmentOrderLink",
    "AccShipmentEvent",
    "AccShipmentPod",
    "AccShipmentCost",
    "AccOrderCourierRelation",
    "AccShipmentOutcomeFact",
    "AccOrderLogisticsFact",
    "AccOrderLogisticsShadow",
    "GlobalFamily",
    "GlobalFamilyChild",
    "MarketplaceListingChild",
    "GlobalFamilyChildMarketLink",
    "GlobalFamilyMarketLink",
    "FamilyCoverageCache",
    "FamilyIssuesCache",
    "FamilyFixPackage",
    "FamilyFixJob",
]
