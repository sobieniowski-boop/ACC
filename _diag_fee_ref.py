"""Check acc_fba_fee_reference and acc_offer_fee_expected for fee source."""
import sys; sys.path.insert(0, ".")
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=15)
cur = conn.cursor()

print("=== acc_fba_fee_reference columns ===")
cur.execute("""
    SELECT c.name, t.name as type
    FROM sys.columns c
    JOIN sys.types t ON t.system_type_id = c.system_type_id AND t.user_type_id = c.user_type_id
    JOIN sys.tables tb ON tb.object_id = c.object_id
    WHERE tb.name = 'acc_fba_fee_reference'
    ORDER BY c.column_id
""")
for row in cur.fetchall():
    print(f"  {row[0]:30s} ({row[1]})")

print("\n=== acc_fba_fee_reference sample ===")
cur.execute("SELECT TOP 5 * FROM dbo.acc_fba_fee_reference WITH (NOLOCK)")
cols = [c[0] for c in cur.description]
for row in cur.fetchall():
    d = dict(zip(cols, row))
    print(f"  {d}")

print(f"\n=== acc_fba_fee_reference count ===")
cur.execute("SELECT COUNT(*) FROM dbo.acc_fba_fee_reference WITH (NOLOCK)")
print(f"  {cur.fetchone()[0]} rows")

print(f"\n=== acc_offer_fee_expected columns ===")
cur.execute("""
    SELECT c.name, t.name as type
    FROM sys.columns c
    JOIN sys.types t ON t.system_type_id = c.system_type_id AND t.user_type_id = c.user_type_id
    JOIN sys.tables tb ON tb.object_id = c.object_id
    WHERE tb.name = 'acc_offer_fee_expected'
    ORDER BY c.column_id
""")
for row in cur.fetchall():
    print(f"  {row[0]:30s} ({row[1]})")

print("\n=== acc_offer_fee_expected sample ===")
cur.execute("SELECT TOP 5 * FROM dbo.acc_offer_fee_expected WITH (NOLOCK)")
cols = [c[0] for c in cur.description]
for row in cur.fetchall():
    d = dict(zip(cols, row))
    print(f"  {d}")

print(f"\n=== acc_offer_fee_expected count ===")
cur.execute("SELECT COUNT(*) FROM dbo.acc_offer_fee_expected WITH (NOLOCK)")
print(f"  {cur.fetchone()[0]} rows")

# KEY QUESTION: How does order_pipeline stamp fees? Check step 10
# There must be another source besides acc_finance_transaction
print("\n=== Check if fees come from acc_fba_fee_reference ===")
cur.execute("""
    SELECT TOP 3 
        ol.sku, ol.referral_fee_pln, ol.fba_fee_pln, 
        fr.referral_fee_amount, fr.fba_fulfilment_amount,
        o.purchase_date, o.marketplace_id
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    LEFT JOIN dbo.acc_fba_fee_reference fr WITH (NOLOCK)
        ON fr.asin = ol.asin AND fr.marketplace_id = o.marketplace_id
    WHERE o.purchase_date >= '2026-02-15' AND o.purchase_date < '2026-02-20'
      AND o.marketplace_id = 'A1PA6795UKMFR9'
      AND ol.referral_fee_pln IS NOT NULL AND ol.referral_fee_pln > 0
    ORDER BY o.purchase_date DESC
""")
cols = [c[0] for c in cur.description]
for row in cur.fetchall():
    d = dict(zip(cols, row))
    print(f"  sku={d['sku']} | ref_on_line={d['referral_fee_pln']} | ref_reference={d.get('referral_fee_amount')} | fba_on_line={d['fba_fee_pln']} | fba_reference={d.get('fba_fulfilment_amount')}")

conn.close()
