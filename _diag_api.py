"""Quick API diagnostic for Product Profit Table, Profit Explorer, and Fee Breakdown."""
import requests, json, time

BASE = "http://127.0.0.1:8000/api/v1"

def test_endpoint(name, url, params=None):
    print(f"\n{'='*60}")
    print(f"TEST: {name}")
    print(f"URL: {url}")
    t0 = time.time()
    try:
        r = requests.get(url, params=params, timeout=90)
        elapsed = time.time() - t0
        print(f"Status: {r.status_code} in {elapsed:.1f}s")
        if r.status_code == 200:
            d = r.json()
            if isinstance(d, dict):
                print(f"Keys: {list(d.keys())}")
                if "total" in d:
                    print(f"Total: {d['total']}")
                if "items" in d:
                    print(f"Items count: {len(d['items'])}")
                    if d["items"]:
                        print(f"First item keys: {list(d['items'][0].keys()) if isinstance(d['items'][0], dict) else 'N/A'}")
                if "lines" in d:
                    print(f"Lines count: {len(d['lines'])}")
                if "summary" in d:
                    print(f"Summary: {json.dumps(d['summary'], default=str)}")
            elif isinstance(d, list):
                print(f"Array length: {len(d)}")
        else:
            print(f"Error: {r.text[:500]}")
    except Exception as e:
        print(f"EXCEPTION: {e}")

# 1. Product Profit Table (v2)
test_endpoint(
    "Product Profit Table (v2)",
    f"{BASE}/profit/v2/products",
    {"date_from": "2026-02-09", "date_to": "2026-03-10", "page": 1, "page_size": 5, "profit_mode": "cm1", "group_by": "asin_marketplace"},
)

# 2. Profit Explorer (legacy v1 orders)
test_endpoint(
    "Profit Explorer (v1 orders)",
    f"{BASE}/profit/orders",
    {"date_from": "2026-02-09", "date_to": "2026-03-10", "page": 1, "page_size": 5},
)

# 3. Fee Breakdown (Granularny P&L)
test_endpoint(
    "Fee Breakdown (Granularny P&L)",
    f"{BASE}/profit/v2/fee-breakdown",
    {"date_from": "2026-02-09", "date_to": "2026-03-10"},
)

# 4. Check for duplicate charge_types in fee breakdown
print(f"\n{'='*60}")
print("DUPLICATE CHECK: Fee Breakdown charge_types")
try:
    r = requests.get(f"{BASE}/profit/v2/fee-breakdown", params={"date_from": "2026-02-09", "date_to": "2026-03-10"}, timeout=90)
    if r.status_code == 200:
        d = r.json()
        lines = d.get("lines", [])
        seen = {}
        for i, line in enumerate(lines):
            ct = line.get("charge_type", "")
            if ct in seen:
                print(f"  DUPLICATE: '{ct}' at index {seen[ct]} and {i}")
                print(f"    First:  {json.dumps(lines[seen[ct]], default=str)[:200]}")
                print(f"    Second: {json.dumps(line, default=str)[:200]}")
            else:
                seen[ct] = i
except Exception as e:
    print(f"Error: {e}")

# 5. Check distinct charge_types in DB for "Paid" or "SAS"
print(f"\n{'='*60}")
print("DB CHECK: Finance transactions with 'Paid' or 'Service' in charge_type")
try:
    from app.core.db_connection import connect_acc
    conn = connect_acc(autocommit=False, timeout=15)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT charge_type, transaction_type, COUNT(*) as cnt, 
               ROUND(SUM(ISNULL(amount_pln, amount)), 2) as total_pln
        FROM dbo.acc_finance_transaction WITH (NOLOCK)
        WHERE charge_type LIKE '%aid%' OR charge_type LIKE '%ervice%' OR charge_type LIKE '%ubscription%' OR charge_type LIKE '%SAS%'
        GROUP BY charge_type, transaction_type
        ORDER BY charge_type
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:40s} | {row[1]:30s} | cnt={row[2]:5d} | total={float(row[3]):>12.2f} PLN")
    conn.close()
except Exception as e:
    print(f"  DB Error: {e}")

# 6. Check fee coverage - which orders are missing fees
print(f"\n{'='*60}")
print("DB CHECK: Fee coverage by marketplace")
try:
    from app.core.db_connection import connect_acc
    conn = connect_acc(autocommit=False, timeout=15)
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            o.marketplace_id,
            COUNT(*) AS total_lines,
            SUM(CASE WHEN ol.fba_fee_pln IS NOT NULL AND ol.fba_fee_pln != 0 THEN 1 ELSE 0 END) AS with_fba_fee,
            SUM(CASE WHEN ol.referral_fee_pln IS NOT NULL AND ol.referral_fee_pln != 0 THEN 1 ELSE 0 END) AS with_referral_fee,
            SUM(CASE WHEN (ol.fba_fee_pln IS NOT NULL AND ol.fba_fee_pln != 0) 
                      AND (ol.referral_fee_pln IS NOT NULL AND ol.referral_fee_pln != 0) THEN 1 ELSE 0 END) AS with_both_fees
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE o.purchase_date >= '2026-02-09' AND o.purchase_date < '2026-03-11'
          AND o.status = 'Shipped'
          AND o.amazon_order_id NOT LIKE 'S02-%%'
          AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
          AND ol.quantity_ordered > 0
        GROUP BY o.marketplace_id
        ORDER BY total_lines DESC
    """)
    print(f"  {'MP_ID':20s} | {'Lines':>6s} | {'FBA%':>6s} | {'Ref%':>6s} | {'Both%':>6s}")
    print(f"  {'-'*20} | {'-'*6} | {'-'*6} | {'-'*6} | {'-'*6}")
    for row in cur.fetchall():
        mp, total, fba, ref, both = row[0], row[1], row[2], row[3], row[4]
        print(f"  {mp:20s} | {total:6d} | {fba/total*100:5.1f}% | {ref/total*100:5.1f}% | {both/total*100:5.1f}%")
    conn.close()
except Exception as e:
    print(f"  DB Error: {e}")

# 7. Check how fees get stamped on order_lines - source analysis
print(f"\n{'='*60}")
print("DB CHECK: Sample NL orders without fees")
try:
    from app.core.db_connection import connect_acc
    conn = connect_acc(autocommit=False, timeout=15)
    cur = conn.cursor()
    cur.execute("""
        SELECT TOP 10
            o.amazon_order_id, o.marketplace_id, o.fulfillment_channel,
            ol.sku, ol.item_price, ol.fba_fee_pln, ol.referral_fee_pln, o.purchase_date
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE o.marketplace_id = 'A1805IZSGTT6HS'
          AND o.purchase_date >= '2026-02-09'
          AND o.status = 'Shipped'
          AND ol.quantity_ordered > 0
          AND (ol.referral_fee_pln IS NULL OR ol.referral_fee_pln = 0)
        ORDER BY o.purchase_date DESC
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:20s} | {row[1]} | {row[2]:3s} | {row[3]:20s} | price={row[4]} | fba={row[5]} | ref={row[6]} | date={row[7]}")
    conn.close()
except Exception as e:
    print(f"  DB Error: {e}")

print(f"\n{'='*60}")
print("DONE")
