#!/usr/bin/env python3
"""Quick runner: sync exchange rates from NBP API (2 years)."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.sync_service import sync_exchange_rates

async def main():
    print("=== Sync Exchange Rates (NBP) — 730 days ===")
    total = await sync_exchange_rates(days_back=730)
    print(f"Done! Inserted {total} exchange rate records.")

if __name__ == "__main__":
    asyncio.run(main())
