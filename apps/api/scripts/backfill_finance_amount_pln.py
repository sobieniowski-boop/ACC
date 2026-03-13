"""Backfill amount_pln in acc_finance_transaction using exchange rates.

Processes in batches of 50K to avoid lock escalation / timeout.
Uses the same FX lookup logic as profit_engine (closest rate_date <= posted_date).

Usage:
    cd C:\ACC\apps\api
    python scripts/backfill_finance_amount_pln.py
"""
import sys
import time

sys.path.insert(0, ".")
from app.core.db_connection import connect_acc

BATCH_SIZE = 25_000
FX_FALLBACK = {
    "EUR": 4.25, "GBP": 5.10, "SEK": 0.39,
    "AED": 1.16, "SAR": 1.13, "TRY": 0.13, "PLN": 1.0,
}


def backfill():
    conn = connect_acc(autocommit=False, timeout=300)
    cur = conn.cursor()

    print("Starting backfill (skipping COUNT to avoid timeout on 1.5M rows)...")
    total = 1_600_000  # approximate

    updated = 0
    t0 = time.monotonic()

    while True:
        # Update batch using OUTER APPLY for FX rate lookup
        cur.execute(f"""
            UPDATE TOP ({BATCH_SIZE}) ft
            SET ft.amount_pln = ROUND(
                ft.amount * ISNULL(
                    fx.rate_to_pln,
                    CASE ft.currency
                        WHEN 'EUR' THEN 4.25 WHEN 'GBP' THEN 5.10
                        WHEN 'SEK' THEN 0.39 WHEN 'AED' THEN 1.16
                        WHEN 'SAR' THEN 1.13 WHEN 'TRY' THEN 0.13
                        WHEN 'PLN' THEN 1.0  ELSE 4.25
                    END
                ), 4),
                ft.exchange_rate = ISNULL(
                    fx.rate_to_pln,
                    CASE ft.currency
                        WHEN 'EUR' THEN 4.25 WHEN 'GBP' THEN 5.10
                        WHEN 'SEK' THEN 0.39 WHEN 'AED' THEN 1.16
                        WHEN 'SAR' THEN 1.13 WHEN 'TRY' THEN 0.13
                        WHEN 'PLN' THEN 1.0  ELSE 4.25
                    END
                )
            FROM acc_finance_transaction ft
            OUTER APPLY (
                SELECT TOP 1 er.rate_to_pln
                FROM acc_exchange_rate er
                WHERE er.currency = ft.currency
                  AND er.rate_date <= CAST(ft.posted_date AS DATE)
                ORDER BY er.rate_date DESC
            ) fx
            WHERE ft.amount_pln IS NULL OR ft.amount_pln = 0
        """)

        batch_count = cur.rowcount
        conn.commit()
        updated += batch_count

        elapsed = time.monotonic() - t0
        rate = updated / elapsed if elapsed > 0 else 0
        print(f"  Updated {updated:,} / {total:,}  ({batch_count} this batch, {rate:.0f} rows/s)")

        if batch_count == 0:
            break

    elapsed = time.monotonic() - t0
    print(f"\nDone: {updated:,} records updated in {elapsed:.1f}s")

    # Verify
    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction WHERE amount_pln IS NULL OR amount_pln = 0")
    remaining = int(cur.fetchone()[0] or 0)
    print(f"Remaining with amount_pln=0: {remaining:,}")

    cur.execute("""
        SELECT currency,
               COUNT(*) AS cnt,
               ROUND(SUM(amount_pln), 2) AS total_pln,
               ROUND(AVG(exchange_rate), 4) AS avg_fx
        FROM acc_finance_transaction
        GROUP BY currency
        ORDER BY COUNT(*) DESC
    """)
    print("\nCurrency breakdown after backfill:")
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]:,} rows, total_pln={r[2]:,.2f}, avg_fx={r[3]}")

    conn.close()


if __name__ == "__main__":
    backfill()
