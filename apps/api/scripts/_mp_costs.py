import pymssql, os
from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")
conn = pymssql.connect(
    server=os.getenv("MSSQL_SERVER"), user=os.getenv("MSSQL_USER"),
    password=os.getenv("MSSQL_PASSWORD"), database=os.getenv("MSSQL_DATABASE"),
    port=int(os.getenv("MSSQL_PORT","1433")), tds_version="7.3",
    login_timeout=30, timeout=120,
)
c = conn.cursor()
MP = {
    "A1PA6795UKMFR9": "DE", "A1RKKUPIHCS9HS": "ES", "A13V1IB3VIYBER": "FR",
    "APJ6JRA9NG5V4": "IT", "A1805I8KKMJY53": "NL", "A2NODRKZP88ZB9": "SE",
    "A1C3SOZRARQ6R3": "PL", "AMEN7PMS3EDWL": "BE", "A33AVAJ2PDY3EV": "TR",
    "A21TJRUUN4KGV": "IN", "ATVPDKIKX0DER": "US", "A1F83G8C2ARO7P": "UK",
}
SKIP = ("Principal","Tax","Commission","FBAPerUnitFulfillmentFee",
        "ShippingCharge","ShippingTax","DigitalServicesFee",
        "ReserveDebit","ReserveCredit","FailedDisbursement")

# 1. All finance per marketplace
c.execute("SELECT ISNULL(marketplace_id,'NONE'), COUNT(*), SUM(CAST(amount AS FLOAT)) FROM acc_finance_transaction GROUP BY marketplace_id ORDER BY ABS(SUM(CAST(amount AS FLOAT))) DESC")
print("=== ALL finance transactions per marketplace ===")
for r in c.fetchall():
    mp = str(r[0])
    name = MP.get(mp, "?")
    t = r[2] or 0
    print(f"  {mp:20s} ({name:2s}) | cnt={r[1]:8d} | total={t:14.2f} EUR")

# 2. CM2/NP costs per marketplace (excluding revenue + CM1 fees + cash-flow)
skip_clause = ",".join(f"'{s}'" for s in SKIP)
sql = f"""
SELECT ISNULL(marketplace_id,'NONE'), COUNT(*), SUM(CAST(amount AS FLOAT))
FROM acc_finance_transaction
WHERE charge_type NOT IN ({skip_clause})
GROUP BY marketplace_id
ORDER BY ABS(SUM(CAST(amount AS FLOAT))) DESC
"""
c.execute(sql)
print("\n=== CM2/NP cost transactions per marketplace ===")
for r in c.fetchall():
    mp = str(r[0])
    name = MP.get(mp, "?")
    t = r[2] or 0
    print(f"  {mp:20s} ({name:2s}) | cnt={r[1]:8d} | cost={t:14.2f} EUR")

# 3. Detailed breakdown for top marketplaces
sql2 = f"""
SELECT ISNULL(marketplace_id,'NONE'), charge_type, COUNT(*), SUM(CAST(amount AS FLOAT))
FROM acc_finance_transaction
WHERE charge_type NOT IN ({skip_clause})
GROUP BY marketplace_id, charge_type
ORDER BY marketplace_id, ABS(SUM(CAST(amount AS FLOAT))) DESC
"""
c.execute(sql2)
print("\n=== CM2/NP costs breakdown per marketplace ===")
cur = None
for r in c.fetchall():
    mp = str(r[0])
    if mp != cur:
        name = MP.get(mp, "?")
        print(f"\n  --- {mp} ({name}) ---")
        cur = mp
    t = r[3] or 0
    print(f"    {str(r[1]):45s} | cnt={r[2]:6d} | {t:12.2f} EUR")

conn.close()
print("\n--- DONE ---")
