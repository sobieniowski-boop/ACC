#!/usr/bin/env python3
"""
Seed script — inserts:
  - Marketplaces from MARKETPLACE_REGISTRY
  - 1 admin user (admin@acc.local / Admin1234!)
  - Demo alert rules
  - NBP EUR/PLN exchange rates for last 90 days (static)

Run from apps/api/:
    python scripts/seed_demo.py
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, timedelta

from sqlalchemy import select
from app.core.config import MARKETPLACE_REGISTRY
from app.core.database import AsyncSessionLocal
from app.core.security import hash_password
from app.models.user import User
from app.models.marketplace import Marketplace
from app.models.alert import AlertRule
from app.models.exchange_rate import ExchangeRate


async def seed():
    async with AsyncSessionLocal() as db:

        # --- Marketplaces ---
        print("Seeding marketplaces…")
        for mkt_id, data in MARKETPLACE_REGISTRY.items():
            existing = await db.execute(select(Marketplace).where(Marketplace.id == mkt_id))
            if not existing.scalar_one_or_none():
                db.add(Marketplace(
                    id=mkt_id,
                    code=data["code"],
                    name=data["name"],
                    currency=data["currency"],
                    timezone=data["tz"],
                ))
        await db.flush()

        # --- Admin user ---
        print("Seeding admin user…")
        existing = await db.execute(select(User).where(User.email == "admin@acc.local"))
        if not existing.scalar_one_or_none():
            db.add(User(
                email="admin@acc.local",
                full_name="Administrator",
                hashed_password=hash_password("Admin1234!"),
                role="admin",
                is_superuser=True,
            ))

        # --- Demo director user ---
        existing = await db.execute(select(User).where(User.email == "director@acc.local"))
        if not existing.scalar_one_or_none():
            db.add(User(
                email="director@acc.local",
                full_name="E-commerce Director",
                hashed_password=hash_password("Director1234!"),
                role="director",
            ))

        await db.flush()

        # --- Alert rules ---
        print("Seeding alert rules…")
        rules = [
            dict(
                name="BuyBox Lost",
                rule_type="buybox_lost",
                description="Triggered when a listing loses the Buy Box",
                severity="warning",
            ),
            dict(
                name="Stock Critical (≤7 days)",
                rule_type="stock_low",
                description="FBA stock covers less than 7 days of sales",
                threshold_value=7,
                threshold_operator="lte",
                severity="critical",
            ),
            dict(
                name="Negative CM1",
                rule_type="cm_negative",
                description="Order contribution margin is negative",
                threshold_value=0,
                threshold_operator="lt",
                severity="critical",
            ),
            dict(
                name="ACoS > 40%",
                rule_type="acos_high",
                description="Campaign ACoS exceeds 40%",
                threshold_value=40,
                threshold_operator="gt",
                severity="warning",
            ),
        ]
        for rule_data in rules:
            existing = await db.execute(
                select(AlertRule).where(AlertRule.rule_type == rule_data["rule_type"])
            )
            if not existing.scalar_one_or_none():
                db.add(AlertRule(**rule_data))

        await db.flush()

        # --- Exchange rates (approx NBP) ---
        print("Seeding exchange rates…")
        FX = {
            "EUR": 4.25,
            "GBP": 5.10,
            "SEK": 0.385,
            "AED": 1.16,
            "SAR": 1.13,
            "TRY": 0.13,
        }
        today = date.today()
        for i in range(90):
            rate_date = today - timedelta(days=i)
            for currency, rate in FX.items():
                existing = await db.execute(
                    select(ExchangeRate).where(
                        ExchangeRate.rate_date == rate_date,
                        ExchangeRate.currency == currency,
                    )
                )
                if not existing.scalar_one_or_none():
                    db.add(ExchangeRate(
                        rate_date=rate_date,
                        currency=currency,
                        rate_to_pln=rate,
                        source="seed",
                    ))

        await db.commit()
        print("✓ Seed complete.")


if __name__ == "__main__":
    asyncio.run(seed())
