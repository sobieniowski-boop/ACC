from __future__ import annotations

from contextlib import asynccontextmanager
import logging

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from app.api.v1.router import api_router
from app.api.ws import ws_router
from app.connectors.mssql import ensure_v2_schema
from app.core.config import settings
from app.core.logging_config import setup_logging

setup_logging()

log = logging.getLogger("amazon-acc")

# ── Sentry error tracking ──
if settings.SENTRY_DSN:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        environment=settings.APP_ENV,
        traces_sample_rate=0.1 if settings.APP_ENV == "production" else 0.0,
        integrations=[
            StarletteIntegration(),
            FastApiIntegration(),
        ],
        send_default_pii=False,
    )
    log.info("sentry.initialized (env=%s)", settings.APP_ENV)
else:
    log.info("sentry.disabled (SENTRY_DSN not set)")

# ── Startup validation for critical environment variables ──
_missing_vars = []
if not settings.MSSQL_PASSWORD:
    _missing_vars.append("MSSQL_PASSWORD")
if not settings.SP_API_CLIENT_ID:
    _missing_vars.append("SP_API_CLIENT_ID")
if not settings.SP_API_CLIENT_SECRET:
    _missing_vars.append("SP_API_CLIENT_SECRET")
if not settings.SP_API_REFRESH_TOKEN:
    _missing_vars.append("SP_API_REFRESH_TOKEN")
if not settings.SECRET_KEY or settings.SECRET_KEY == "change-me":
    _missing_vars.append("SECRET_KEY")
if _missing_vars:
    log.warning("config.missing_critical_vars: %s — some features will be degraded", ", ".join(_missing_vars))


@asynccontextmanager
async def lifespan(_: FastAPI):
    # ── startup ──
    try:
        await run_in_threadpool(ensure_v2_schema)
        log.info("mssql.v2_schema_ready")
    except Exception as exc:
        log.warning("mssql.v2_schema_failed: %s", exc)
    try:
        from app.services.fba_ops import ensure_fba_schema

        await run_in_threadpool(ensure_fba_schema)
        log.info("mssql.fba_schema_ready")
    except Exception as exc:
        log.warning("mssql.fba_schema_failed: %s", exc)
    try:
        from app.services.finance_center import ensure_finance_center_schema

        await run_in_threadpool(ensure_finance_center_schema)
        log.info("mssql.finance_center_schema_ready")
    except Exception as exc:
        log.warning("mssql.finance_center_schema_failed: %s", exc)
    try:
        from app.services.amazon_listing_registry import ensure_amazon_listing_registry_schema

        await run_in_threadpool(ensure_amazon_listing_registry_schema)
        log.info("mssql.amazon_listing_registry_schema_ready")
    except Exception as exc:
        log.warning("mssql.amazon_listing_registry_schema_failed: %s", exc)
    try:
        from app.services.manage_inventory import ensure_manage_inventory_schema

        await run_in_threadpool(ensure_manage_inventory_schema)
        log.info("mssql.manage_inventory_schema_ready")
    except Exception as exc:
        log.warning("mssql.manage_inventory_schema_failed: %s", exc)
    try:
        from app.services.dhl_integration import ensure_dhl_schema

        await run_in_threadpool(ensure_dhl_schema)
        log.info("mssql.dhl_schema_ready")
    except Exception as exc:
        log.warning("mssql.dhl_schema_failed: %s", exc)
    try:
        from app.services.gls_integration import ensure_gls_schema

        await run_in_threadpool(ensure_gls_schema)
        log.info("mssql.gls_schema_ready")
    except Exception as exc:
        log.warning("mssql.gls_schema_failed: %s", exc)
    try:
        from app.services.bl_distribution_cache import ensure_bl_distribution_cache_schema

        await run_in_threadpool(ensure_bl_distribution_cache_schema)
        log.info("mssql.bl_distribution_cache_schema_ready")
    except Exception as exc:
        log.warning("mssql.bl_distribution_cache_schema_failed: %s", exc)
    try:
        from app.services.sellerboard_history import ensure_sellerboard_history_schema

        await run_in_threadpool(ensure_sellerboard_history_schema)
        log.info("mssql.sellerboard_history_schema_ready")
    except Exception as exc:
        log.warning("mssql.sellerboard_history_schema_failed: %s", exc)
    try:
        from app.services.profit_engine import ensure_profit_data_quality_schema

        await run_in_threadpool(ensure_profit_data_quality_schema)
        log.info("mssql.profit_data_quality_schema_ready")
    except Exception as exc:
        log.warning("mssql.profit_data_quality_schema_failed: %s", exc)
    try:
        from app.services.profit_engine import ensure_profit_tkl_cache_schema

        await run_in_threadpool(ensure_profit_tkl_cache_schema)
        log.info("mssql.profit_tkl_cache_schema_ready")
    except Exception as exc:
        log.warning("mssql.profit_tkl_cache_schema_failed: %s", exc)

    try:
        from app.services.controlling import ensure_controlling_tables

        await run_in_threadpool(ensure_controlling_tables)
        log.info("mssql.controlling_tables_ready")
    except Exception as exc:
        log.warning("mssql.controlling_tables_failed: %s", exc)
    try:
        from app.services.tax_compliance import ensure_tax_compliance_schema

        await run_in_threadpool(ensure_tax_compliance_schema)
        log.info("mssql.tax_compliance_schema_ready")
    except Exception as exc:
        log.warning("mssql.tax_compliance_schema_failed: %s", exc)
    try:
        from app.services.event_backbone import ensure_event_backbone_schema

        await run_in_threadpool(ensure_event_backbone_schema)
        log.info("mssql.event_backbone_schema_ready")
    except Exception as exc:
        log.warning("mssql.event_backbone_schema_failed: %s", exc)
    try:
        from app.services.listing_state import ensure_listing_state_schema

        await run_in_threadpool(ensure_listing_state_schema)
        log.info("mssql.listing_state_schema_ready")
    except Exception as exc:
        log.warning("mssql.listing_state_schema_failed: %s", exc)
    try:
        from app.intelligence.catalog_health import ensure_catalog_health_schema

        await run_in_threadpool(ensure_catalog_health_schema)
        log.info("mssql.catalog_health_schema_ready")
    except Exception as exc:
        log.warning("mssql.catalog_health_schema_failed: %s", exc)
    try:
        from app.services.ptd_cache import ensure_ptd_cache_schema

        await run_in_threadpool(ensure_ptd_cache_schema)
        log.info("mssql.ptd_cache_schema_ready")
    except Exception as exc:
        log.warning("mssql.ptd_cache_schema_failed: %s", exc)
    try:
        from app.services.pricing_state import ensure_pricing_state_schema

        await run_in_threadpool(ensure_pricing_state_schema)
        log.info("mssql.pricing_state_schema_ready")
    except Exception as exc:
        log.warning("mssql.pricing_state_schema_failed: %s", exc)
    try:
        from app.intelligence.event_wiring import ensure_wiring_schema

        await run_in_threadpool(ensure_wiring_schema)
        log.info("mssql.event_wiring_schema_ready")
    except Exception as exc:
        log.warning("mssql.event_wiring_schema_failed: %s", exc)
    try:
        from app.intelligence.sqs_topology import ensure_topology_schema

        await run_in_threadpool(ensure_topology_schema)
        log.info("mssql.sqs_topology_schema_ready")
    except Exception as exc:
        log.warning("mssql.sqs_topology_schema_failed: %s", exc)
    try:
        from app.services.listing_state import register_backbone_handlers

        register_backbone_handlers()
        log.info("listing_state.handlers_registered")
    except Exception as exc:
        log.warning("listing_state.handlers_registration_failed: %s", exc)
    try:
        from app.services.pricing_state import register_pricing_backbone_handler

        register_pricing_backbone_handler()
        log.info("pricing_state.handlers_registered")
    except Exception as exc:
        log.warning("pricing_state.handlers_registration_failed: %s", exc)
    try:
        from app.intelligence.event_wiring import register_all_domain_handlers

        register_all_domain_handlers()
        log.info("event_wiring.domain_handlers_registered")
    except Exception as exc:
        log.warning("event_wiring.domain_handlers_registration_failed: %s", exc)

    # Optional in-process scheduler (kept behind feature flag for API/scheduler split).
    from app.scheduler import start_scheduler, stop_scheduler
    _scheduler_started = False
    if settings.SCHEDULER_ENABLED:
        try:
            from app.core.scheduler_lock import scheduler_lock
            acquired = await scheduler_lock.acquire()
            if acquired:
                start_scheduler()
                _scheduler_started = True
                log.info("scheduler.started_ok (leader=%s)", scheduler_lock.worker_id)
            else:
                log.info(
                    "scheduler.skipped_not_leader (worker=%s)",
                    scheduler_lock.worker_id,
                )
        except Exception as exc:
            log.warning("scheduler.start_failed: %s", exc)
    else:
        log.info("scheduler.disabled_by_config")

    yield

    # ── shutdown ──
    if settings.SCHEDULER_ENABLED and _scheduler_started:
        try:
            stop_scheduler()
        except Exception:
            pass
        try:
            from app.core.scheduler_lock import scheduler_lock
            await scheduler_lock.release()
        except Exception:
            pass
    # Close Redis connection pool to avoid socket leaks.
    try:
        from app.core.redis_client import close_redis
        await close_redis()
    except Exception:
        pass


app = FastAPI(
    title="Amazon Command Center",
    description="KADAX e-commerce analytics: Amazon SP-API + MSSQL NetfoxAnalityka",
    version="2.1.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── Prometheus Metrics ──
from app.core.metrics import setup_metrics  # noqa: E402
setup_metrics(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Correlation-ID", "X-ACC-Signature"],
)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        if settings.APP_ENV == "production":
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return response


app.add_middleware(SecurityHeadersMiddleware)

from app.platform.middleware import CorrelationIdMiddleware  # noqa: E402
app.add_middleware(CorrelationIdMiddleware)

# ── Request Logging (po CorrelationId, żeby mieć correlation_id w logach) ──
from app.platform.middleware.request_logging import RequestLoggingMiddleware  # noqa: E402
app.add_middleware(RequestLoggingMiddleware)

# ── Security Hardening ──
from app.core.security_hardening import setup_security  # noqa: E402
setup_security(app)

# ── Standardowe wyjątki aplikacyjne ──
from app.core.exceptions import register_exception_handlers  # noqa: E402
register_exception_handlers(app)


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    log.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})

app.include_router(api_router, prefix="/api/v1")
app.include_router(ws_router)


@app.get("/health", tags=["system"])
async def health() -> dict[str, str]:
    return {"status": "ok", "app": "amazon-command-center", "env": settings.APP_ENV}
