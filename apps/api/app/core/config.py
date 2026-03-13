"""Application configuration — loaded from .env."""
from __future__ import annotations

from pathlib import Path
import json
from typing import List
from urllib.parse import quote_plus

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

# config.py lives at apps/api/app/core/ — go 4 levels up to reach project root
_ENV_FILE = Path(__file__).parents[4] / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # App
    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"
    SECRET_KEY: str = "change-me"
    DEFAULT_ACTOR: str = "system"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 480
    REFRESH_TOKEN_EXPIRE_DAYS: int = 30
    CORS_ORIGINS: List[str] = [
        "http://localhost:3010",
        "http://localhost:5173",
        "http://192.168.49.97:3010",
    ]

    # Redis
    REDIS_URL: str = "redis://localhost:6380/0"

    # Amazon SP-API
    SP_API_CLIENT_ID: str = ""
    SP_API_CLIENT_SECRET: str = ""
    SP_API_REFRESH_TOKEN: str = ""
    SP_API_SELLER_ID: str = "A1O0H08K2DYVHX"
    SP_API_REGION: str = "eu"
    SP_API_SANDBOX: bool = False
    SP_API_PRIMARY_MARKETPLACE: str = "A1PA6795UKMFR9"

    # Event Backbone / Notifications
    SQS_QUEUE_URL: str = ""
    SQS_REGION: str = "eu-west-1"
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    NOTIFICATION_INTAKE_SECRET: str = ""  # HMAC-SHA256 for intake auth

    # MSSQL — ACC own database (Azure SQL or local)
    # This is the PRIMARY database for all acc_* tables (full read+write)
    MSSQL_SERVER: str = "192.168.230.120"
    MSSQL_PORT: int = 11901
    MSSQL_USER: str = "msobieniowski"
    MSSQL_PASSWORD: str = ""
    MSSQL_DATABASE: str = "NetfoxAnalityka"

    # MSSQL — Netfox ERP (read-only access to NetfoxAnalityka + NetfoxDistribution)
    # Used for: ITJK_* tables, tw_*, dok_*, Holding COGS, courier costs
    # Falls back to MSSQL_* values if not set (backward compat)
    NETFOX_MSSQL_SERVER: str = ""
    NETFOX_MSSQL_PORT: int = 0
    NETFOX_MSSQL_USER: str = ""
    NETFOX_MSSQL_PASSWORD: str = ""
    NETFOX_MSSQL_DATABASE: str = ""

    @property
    def mssql_enabled(self) -> bool:
        """True when MSSQL credentials are configured."""
        return bool(self.MSSQL_USER and self.MSSQL_PASSWORD)

    @property
    def netfox_enabled(self) -> bool:
        """True when Netfox ERP read-only credentials are configured."""
        server = self.NETFOX_MSSQL_SERVER or self.MSSQL_SERVER
        user = self.NETFOX_MSSQL_USER or self.MSSQL_USER
        pwd = self.NETFOX_MSSQL_PASSWORD or self.MSSQL_PASSWORD
        return bool(server and user and pwd)

    @property
    def use_pymssql(self) -> bool:
        """True when Azure SQL is configured (pymssql needed for TLS 1.2)."""
        return "database.windows.net" in self.MSSQL_SERVER

    @computed_field
    @property
    def _odbc_driver(self) -> str:  # noqa: N802
        """Wybiera najlepszy dostępny sterownik ODBC dla MSSQL."""
        try:
            import pyodbc
            available = pyodbc.drivers()
            for preferred in [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "SQL Server",
            ]:
                if preferred in available:
                    return preferred
        except Exception:
            pass
        return "ODBC Driver 18 for SQL Server"  # fallback dla Dockera

    @computed_field
    @property
    def DATABASE_URL(self) -> str:  # noqa: N802
        """Async MSSQL URL dla SQLAlchemy (aioodbc)."""
        pwd = quote_plus(self.MSSQL_PASSWORD)
        user = quote_plus(self.MSSQL_USER)
        driver = quote_plus(self._odbc_driver)
        extras = (
            "&TrustServerCertificate=yes&async_execution=True&MARS_Connection=yes"
            if "18" in self._odbc_driver or "17" in self._odbc_driver
            else ""
        )
        return (
            f"mssql+aioodbc://{user}:{pwd}@"
            f"{self.MSSQL_SERVER},{self.MSSQL_PORT}/{self.MSSQL_DATABASE}"
            f"?driver={driver}{extras}"
        )

    @computed_field
    @property
    def DATABASE_URL_SYNC(self) -> str:  # noqa: N802
        """Sync MSSQL URL dla Alembic i pyodbc."""
        pwd = quote_plus(self.MSSQL_PASSWORD)
        user = quote_plus(self.MSSQL_USER)
        driver = quote_plus(self._odbc_driver)
        extras = (
            "&TrustServerCertificate=yes&MARS_Connection=yes"
            if "18" in self._odbc_driver or "17" in self._odbc_driver
            else ""
        )
        return (
            f"mssql+pyodbc://{user}:{pwd}@"
            f"{self.MSSQL_SERVER},{self.MSSQL_PORT}/{self.MSSQL_DATABASE}"
            f"?driver={driver}{extras}"
        )

    @property
    def mssql_connection_string(self) -> str:
        """Natywny connection string pyodbc (dla netfox.py i discover_mssql_schema.py).

        When MSSQL_SERVER points to Azure SQL (*.database.windows.net), use
        connect_acc() / pymssql_compat instead — this string won't work without
        ODBC Driver 17/18.
        """
        trust = "TrustServerCertificate=yes;MARS_Connection=yes;" if "18" in self._odbc_driver or "17" in self._odbc_driver else ""
        return (
            f"DRIVER={{{self._odbc_driver}}};"
            f"SERVER={self.MSSQL_SERVER},{self.MSSQL_PORT};"
            f"DATABASE={self.MSSQL_DATABASE};"
            f"UID={self.MSSQL_USER};"
            f"PWD={self.MSSQL_PASSWORD};"
            f"{trust}"
        )

    @property
    def netfox_connection_string(self) -> str:
        """pyodbc connection string for Netfox ERP (read-only, old driver OK).

        Falls back to MSSQL_* values for backward compat (when not yet split).
        """
        server = self.NETFOX_MSSQL_SERVER or self.MSSQL_SERVER
        port = self.NETFOX_MSSQL_PORT or self.MSSQL_PORT
        user = self.NETFOX_MSSQL_USER or self.MSSQL_USER
        pwd = self.NETFOX_MSSQL_PASSWORD or self.MSSQL_PASSWORD
        db = self.NETFOX_MSSQL_DATABASE or self.MSSQL_DATABASE
        trust = "TrustServerCertificate=yes;" if "18" in self._odbc_driver or "17" in self._odbc_driver else ""
        return (
            f"DRIVER={{{self._odbc_driver}}};"
            f"SERVER={server},{port};"
            f"DATABASE={db};"
            f"UID={user};"
            f"PWD={pwd};"
            f"{trust}"
        )

    # OpenAI
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-5.2"
    OPENAI_MAX_TOKENS: int = 4096

    # ProductOnboard bridge for Content Ops (restrictions/catalog/push proxy)
    PRODUCTONBOARD_BASE_URL: str = ""
    PRODUCTONBOARD_API_KEY: str = ""
    PRODUCTONBOARD_TIMEOUT_SEC: int = 20
    PRODUCTONBOARD_RESTRICTIONS_PATH: str = "/api/productonboard/restrictions/check"
    PRODUCTONBOARD_CATALOG_BY_EAN_PATH: str = "/api/productonboard/catalog/search-by-ean"
    PRODUCTONBOARD_PUSH_PATH: str = "/api/productonboard/acc-content/push"

    # Celery
    CELERY_BROKER_URL: str = "redis://localhost:6380/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6380/2"
    SCHEDULER_ENABLED: bool = True
    WORKER_EXECUTION_ENABLED: bool = False
    JOB_CANARY_MODE: str = "courier_fba"  # off | courier_fba | all
    JOB_QUEUE_ROUTING_JSON: str = ""
    WORKER_DB_HEAVY_MAX: int = 3
    WORKER_DB_HEAVY_LEASE_SEC: int = 14400
    WORKER_CONCURRENCY_COURIER_HEAVY: int = 1
    WORKER_CONCURRENCY_INVENTORY_HEAVY: int = 1
    WORKER_CONCURRENCY_FINANCE_HEAVY: int = 1
    WORKER_CONCURRENCY_FBA_MEDIUM: int = 2
    WORKER_CONCURRENCY_CORE_MEDIUM: int = 2
    WORKER_CONCURRENCY_LIGHT_DEFAULT: int = 4

    # Sentry
    SENTRY_DSN: str = ""

    # Currency
    BASE_CURRENCY: str = "PLN"
    PROFIT_USE_LOGISTICS_FACT: bool = False

    # BaseLinker Distribution API (read-only order/package cache for courier linking)
    BASELINKER_API: str = "https://api.baselinker.com/connector.php"
    BASELINKER_DISTRIBUTION_TOKEN: str = ""
    BASELINKER_TIMEOUT_SEC: int = 30
    BASELINKER_DISTRIBUTION_PAGE_SLEEP_SEC: float = 0.35
    BASELINKER_DISTRIBUTION_PACKAGE_SLEEP_SEC: float = 0.12
    BASELINKER_DISTRIBUTION_SYNC_ENABLED: bool = False
    BASELINKER_DISTRIBUTION_SYNC_HOUR: int = 1
    BASELINKER_DISTRIBUTION_SYNC_MINUTE: int = 55
    BASELINKER_DISTRIBUTION_SYNC_LOOKBACK_DAYS: int = 2
    BASELINKER_DISTRIBUTION_SYNC_LIMIT_ORDERS: int = 500
    BASELINKER_DISTRIBUTION_SYNC_INCLUDE_PACKAGES: bool = True

    @property
    def baselinker_distribution_enabled(self) -> bool:
        """True when BaseLinker Distribution API is configured for read-only cache sync."""
        return bool(self.BASELINKER_API and self.BASELINKER_DISTRIBUTION_TOKEN)

    # Ergonode PIM
    ERGONODE_API_URL: str = ""
    ERGONODE_USERNAME: str = ""
    ERGONODE_PASSWORD: str = ""
    ERGONODE_API_KEY: str = ""

    # Google Sheet CSV with EAN->SKU mapping fallback
    GSHEET_EAN_CSV_URL: str = ""
    # Legacy alias kept for backward compatibility with existing .env files
    GSHEET_ALLEGRO_CSV_URL: str = ""
    # Amazon listing registry Google Sheet CSV (Merchant SKU / EAN / ASIN / Nr art.)
    GSHEET_AMAZON_LISTING_CSV_URL: str = (
        "https://docs.google.com/spreadsheets/d/"
        "1rRBVZUTwqYcGYZRSp28mIWXw7gMfvqes0apEE_hdpjo/export?format=csv&gid=400534387"
    )

    # Purchase prices XLSX (Oficjalne ceny zakupu)
    PURCHASE_PRICES_XLSX_PATH: str = r"N:\Analityka\00. Oficjalne ceny zakupu dla sprzedaży.xlsx"

    # Amazon Ads API
    AMAZON_ADS_CLIENT_ID: str = ""
    AMAZON_ADS_CLIENT_SECRET: str = ""
    AMAZON_ADS_REFRESH_TOKEN: str = ""
    AMAZON_ADS_REGION: str = "EU"  # EU, NA, FE

    @property
    def amazon_ads_enabled(self) -> bool:
        """True when Amazon Ads API credentials are configured."""
        return bool(self.AMAZON_ADS_CLIENT_ID and self.AMAZON_ADS_REFRESH_TOKEN)

    # Taxonomy nightly sync (brand/category gap fill)
    TAXONOMY_SYNC_ENABLED: bool = True
    TAXONOMY_SYNC_HOUR: int = 2
    TAXONOMY_SYNC_MINUTE: int = 25
    TAXONOMY_SYNC_LIMIT: int = 10000
    TAXONOMY_SYNC_AUTO_APPLY: bool = True
    TAXONOMY_SYNC_MIN_CONFIDENCE: float = 0.95

    # GLS Group API (OAuth2 + Tracking + Shipments)
    GLS_CLIENT_ID: str = ""
    GLS_CLIENT_SECRET: str = ""
    GLS_SANDBOX: bool = False  # True → api-sandbox.gls-group.net

    @property
    def gls_api_enabled(self) -> bool:
        """True when GLS API credentials are configured."""
        return bool(self.GLS_CLIENT_ID and self.GLS_CLIENT_SECRET)

    # GLS Poland ADE WebAPI (SOAP) — parcel creation, tracking, labels
    GLS_ADE_WSDL_URL: str = "https://adeplus.gls-poland.com/adeplus/pm1/ade_webapi2.php?wsdl"
    GLS_ADE_USERNAME: str = ""
    GLS_ADE_PASSWORD: str = ""
    GLS_BILLING_ROOT_PATH: str = r"N:\KURIERZY\GLS POLSKA"
    GLS_BILLING_BL_MAP_PATH: str = r"N:\KURIERZY\GLS POLSKA\GLS - BL.xlsx"
    GLS_BILLING_CORRECTIONS_PATH: str = r"N:\KURIERZY\GLS POLSKA\Korekty kosztowe"
    GLS_LOGISTICS_SYNC_ENABLED: bool = True
    GLS_LOGISTICS_SYNC_HOUR: int = 0
    GLS_LOGISTICS_SYNC_MINUTE: int = 20
    GLS_LOGISTICS_SYNC_LOOKBACK_DAYS: int = 60
    GLS_LOGISTICS_SYNC_LIMIT_SHIPMENTS: int = 50000
    GLS_LOGISTICS_SYNC_LIMIT_ORDERS: int = 50000
    COURIER_BILLING_VERIFY_ENABLED: bool = True
    COURIER_BILLING_VERIFY_HOUR: int = 6
    COURIER_BILLING_VERIFY_MINUTE: int = 10

    @property
    def gls_ade_enabled(self) -> bool:
        """True when GLS Poland ADE credentials are configured."""
        return bool(self.GLS_ADE_USERNAME and self.GLS_ADE_PASSWORD)

    # DHL24 WebAPI2 (SOAP) - read-only shipment registry, tracking, POD, pricing
    DHL24_API_BASE_URL: str = "https://dhl24.com.pl/webapi2/provider/service.html?ws=1"
    DHL24_API_USERNAME: str = ""
    DHL24_API_PASSWORD: str = ""
    DHL24_PARCELSHOP_USERNAME: str = ""
    DHL24_PARCELSHOP_PASSWORD: str = ""
    DHL24_TIMEOUT_SEC: int = 30
    DHL24_ENABLED: bool = True
    DHL24_WRITE_ENABLED: bool = False
    DHL_BILLING_ROOT_PATH: str = r"N:\KURIERZY\DHL"
    DHL_BILLING_JJ_PATH: str = r"N:\KURIERZY\DHL\JJ"
    DHL_BILLING_MANIFEST_PATH: str = ""
    DHL_LOGISTICS_SYNC_ENABLED: bool = True
    DHL_LOGISTICS_SYNC_HOUR: int = 0
    DHL_LOGISTICS_SYNC_MINUTE: int = 5
    DHL_LOGISTICS_SYNC_LOOKBACK_DAYS: int = 60
    DHL_LOGISTICS_SYNC_LIMIT_SHIPMENTS: int = 50000
    DHL_LOGISTICS_SYNC_LIMIT_ORDERS: int = 50000
    DHL_LOGISTICS_SYNC_ALLOW_ESTIMATED: bool = True

    COURIER_ESTIMATION_SYNC_ENABLED: bool = True
    COURIER_ESTIMATION_SYNC_HOUR: int = 0
    COURIER_ESTIMATION_SYNC_MINUTE: int = 40
    COURIER_ESTIMATION_SYNC_LOOKBACK_DAYS: int = 45
    COURIER_ESTIMATION_HORIZON_DAYS: int = 180
    COURIER_ESTIMATION_MIN_SAMPLES: int = 10
    COURIER_ESTIMATION_LIMIT_SHIPMENTS: int = 20000
    COURIER_RECONCILE_LIMIT_SHIPMENTS: int = 50000
    COURIER_ESTIMATION_REFRESH_EXISTING: bool = False

    COURIER_READINESS_SLA_ENABLED: bool = True
    COURIER_READINESS_SLA_BUFFER_DAYS: int = 45

    @property
    def dhl24_api_enabled(self) -> bool:
        """True when DHL24 API credentials are configured and feature is enabled."""
        return bool(
            self.DHL24_ENABLED
            and self.DHL24_API_USERNAME
            and self.DHL24_API_PASSWORD
        )

    @property
    def dhl24_parcelshop_enabled(self) -> bool:
        """True when DHL24 Parcelshop credentials are configured."""
        return bool(
            self.DHL24_ENABLED
            and self.DHL24_PARCELSHOP_USERNAME
            and self.DHL24_PARCELSHOP_PASSWORD
        )

    @property
    def job_queue_routing(self) -> dict[str, str]:
        """Optional JSON map: job_type -> queue."""
        raw = str(self.JOB_QUEUE_ROUTING_JSON or "").strip()
        if not raw:
            return {}
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items() if str(k).strip() and str(v).strip()}
        except Exception:
            return {}
        return {}


settings = Settings()

# ---------------------------------------------------------------------------
# Marketplace registry — active KADAX marketplaces (DE, FR, IT, ES, NL, BE, PL, SE, IE)
# ---------------------------------------------------------------------------
MARKETPLACE_REGISTRY = {
    "A1PA6795UKMFR9": {"code": "DE", "name": "Amazon.de",  "currency": "EUR", "tz": "Europe/Berlin",     "brand_owner": True},
    "A1C3SOZRARQ6R3": {"code": "PL", "name": "Amazon.pl",  "currency": "PLN", "tz": "Europe/Warsaw",     "brand_owner": False},
    "A1RKKUPIHCS9HS": {"code": "ES", "name": "Amazon.es",  "currency": "EUR", "tz": "Europe/Madrid",     "brand_owner": True},
    "A13V1IB3VIYZZH": {"code": "FR", "name": "Amazon.fr",  "currency": "EUR", "tz": "Europe/Paris",      "brand_owner": True},
    "A1805IZSGTT6HS": {"code": "NL", "name": "Amazon.nl",  "currency": "EUR", "tz": "Europe/Amsterdam",  "brand_owner": True},
    "APJ6JRA9NG5V4":  {"code": "IT", "name": "Amazon.it",  "currency": "EUR", "tz": "Europe/Rome",       "brand_owner": True},
    "A2NODRKZP88ZB9": {"code": "SE", "name": "Amazon.se",  "currency": "SEK", "tz": "Europe/Stockholm",  "brand_owner": True},
    "AMEN7PMS3EDWL":  {"code": "BE", "name": "Amazon.be",  "currency": "EUR", "tz": "Europe/Brussels",   "brand_owner": False},
    "A28R8C7NBKEWEA": {"code": "IE", "name": "Amazon.ie",  "currency": "EUR", "tz": "Europe/Dublin",     "brand_owner": False},
}

# ---------------------------------------------------------------------------
# Renewed / Amazon-graded SKU filter
# SKUs like amzn.gr.* and amazon.found* are customer-returned items resold.
# Include them in profitability & inventory, EXCLUDE from recommendations,
# analysis, suggestions, seasonality, strategy, and content ops.
# ---------------------------------------------------------------------------
RENEWED_SKU_PATTERNS = ("amzn.gr.", "amazon.found")

RENEWED_SKU_SQL_FILTER = (
    "sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'"
)


def is_renewed_sku(sku: str | None) -> bool:
    """Return True if SKU is a renewed / Amazon-graded product."""
    s = str(sku or "").strip().lower()
    return any(s.startswith(p) for p in RENEWED_SKU_PATTERNS)
