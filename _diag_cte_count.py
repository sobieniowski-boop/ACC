import sys
sys.path.insert(0, r"C:\ACC\apps\api")
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

_FBA_CHARGE_TYPES = ['FBAPerUnitFulfillmentFee', 'FBAPerOrderFulfillmentFee', 'FBAWeightBasedFee']
_FBA_TYPES_SQL = ','.join(f"'{t}'" for t in _FBA_CHARGE_TYPES)

cur.execute(f"""
    WITH base AS (
        SELECT
            ft.sku,
            ft.currency,
            COUNT(*) OVER (PARTITION BY ft.sku, ft.currency) AS total_charges,
            MIN(ABS(ft.amount)) OVER (PARTITION BY ft.sku, ft.currency) AS min_fee,
            MAX(ABS(ft.amount)) OVER (PARTITION BY ft.sku, ft.currency) AS max_fee
        FROM acc_finance_transaction ft WITH (NOLOCK)
        WHERE ft.charge_type IN ({_FBA_TYPES_SQL})
          AND ft.sku IS NOT NULL
    ),
    suspicious AS (
        SELECT sku, currency, total_charges
        FROM base
        WHERE total_charges >= 5
          AND max_fee > min_fee * 1.5
    )
    SELECT COUNT(*) AS cnt FROM suspicious
""")
row = cur.fetchone()
print("Total suspicious rows:", row[0])

# Also count distinct SKUs
cur.execute(f"""
    WITH base AS (
        SELECT
            ft.sku,
            ft.currency,
            COUNT(*) OVER (PARTITION BY ft.sku, ft.currency) AS total_charges,
            MIN(ABS(ft.amount)) OVER (PARTITION BY ft.sku, ft.currency) AS min_fee,
            MAX(ABS(ft.amount)) OVER (PARTITION BY ft.sku, ft.currency) AS max_fee
        FROM acc_finance_transaction ft WITH (NOLOCK)
        WHERE ft.charge_type IN ({_FBA_TYPES_SQL})
          AND ft.sku IS NOT NULL
    ),
    suspicious AS (
        SELECT sku, currency
        FROM base
        WHERE total_charges >= 5
          AND max_fee > min_fee * 1.5
    )
    SELECT COUNT(DISTINCT sku) AS distinct_skus FROM suspicious
""")
row = cur.fetchone()
print("Distinct suspicious SKUs:", row[0])

conn.close()
