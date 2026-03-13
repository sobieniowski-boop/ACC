import pymssql, os
from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")

MP = {
    'A1PA6795UKMFR9':'DE','A1RKKUPIHCS9HS':'ES','AMEN7PMS3EDWL':'BE',
    'APJ6JRA9NG5V4':'IT','A2NODRKZP88ZB9':'SE','A13V1IB3VIYZZH':'FR',
    'A1805IZSGTT6HS':'NL','A1C3SOZRARQ6R3':'PL','A28R8C7NBKEWEA':'IE',
}

conn = pymssql.connect(
    server=os.getenv("MSSQL_SERVER"), user=os.getenv("MSSQL_USER"),
    password=os.getenv("MSSQL_PASSWORD"), database=os.getenv("MSSQL_DATABASE"),
    port=1433, login_timeout=60, timeout=120, tds_version="7.3",
)
cur = conn.cursor()

# 1) per marketplace
print("=== FBAStorageFee by marketplace ===")
cur.execute("""
SELECT marketplace_id, COUNT(*) cnt, SUM(CAST(amount AS FLOAT)) eur
FROM acc_finance_transaction
WHERE charge_type='FBAStorageFee'
GROUP BY marketplace_id
ORDER BY SUM(CAST(amount AS FLOAT))
""")
for r in cur.fetchall():
    code = MP.get(r[0], r[0] or "NULL")
    print(f"  {code:<4} | {r[1]:>5} recs | {r[2]:>12,.2f} EUR")

# 2) sample big entries
print("\n=== Top 20 largest single FBAStorageFee entries ===")
cur.execute("""
SELECT TOP 20 CAST(posted_date AS DATE), marketplace_id, sku, 
       CAST(amount AS FLOAT) eur, CAST(amount_pln AS FLOAT) pln
FROM acc_finance_transaction
WHERE charge_type='FBAStorageFee'
ORDER BY CAST(amount AS FLOAT)
""")
for r in cur.fetchall():
    code = MP.get(r[1], r[1] or "?")
    pln_val = r[4] or 0
    print(f"  {str(r[0]):<12} {code:<4} sku={str(r[2]):<20} {r[3]:>10,.2f} EUR  {pln_val:>10,.2f} PLN")

# 3) check FBALongTermStorageFee
print("\n=== FBALongTermStorageFee ===")
cur.execute("""
SELECT CAST(posted_date AS DATE), COUNT(*), SUM(CAST(amount AS FLOAT))
FROM acc_finance_transaction
WHERE charge_type='FBALongTermStorageFee'
GROUP BY CAST(posted_date AS DATE)
ORDER BY CAST(posted_date AS DATE)
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<12} | {r[1]:>5} recs | {r[2]:>12,.2f} EUR")

conn.close()
print("\nDONE")
