"""Scheduler package — domain-decomposed job registration.

The old monolithic scheduler.py (1875 lines) is being decomposed into
domain-specific modules. Each domain module exports a `register(scheduler)`
function that adds its jobs.

Usage (in app/scheduler.py thin orchestrator):
    from app.platform.scheduler import register_all_domains
    register_all_domains(scheduler)
"""
from __future__ import annotations

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.platform.scheduler.registry import JobRegistry

_registry = JobRegistry()


def register_all_domains(scheduler: AsyncIOScheduler) -> None:
    """Register jobs from all domain modules."""
    from app.platform.scheduler import (
        orders,
        finance,
        inventory,
        ads,
        profit,
        content,
        logistics,
        strategy,
        seasonality,
        catalog_health,
        buybox_radar,
        inventory_risk,
        repricing,
        system,
    )

    for domain_module in [
        orders, finance, inventory, ads, profit, content,
        logistics, strategy, seasonality, catalog_health, buybox_radar,
        inventory_risk, repricing, system,
    ]:
        domain_module.register(scheduler)
