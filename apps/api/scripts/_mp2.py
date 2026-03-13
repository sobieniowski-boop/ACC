import pymssql, os, sys
from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")

def get_conn():
    return pymssql.connect(
        server=os.getenv("MSSQL_SERVER"), user=os.getenv("MSSQL_USER"),
        password=os.getenv("MSSQL_PASSWORD"), database=os.getenv("MSSQL_DATABASE"),
        port=int(os.getenv("MSSQL_PORT","1433")), tds_version="7.3",
        login_timeout=60, timeout=300,
    )

MP = {
    "A1PA6795UKMFR9": "DE", "A1RKKUPIHCS9HS": "ES", "A13V1IB3VIYBER": "FR",
    "APJ6JRA9NG5V4": "IT", "A1805I8KKMJY53": "NL", "A2NODRKZP88ZB9": "SE",
    "A1C3SOZRARQ6R3": "PL", "AMEN7PMS3EDWL": "BE", "A33AVAJ2PDY3EV": "TR",
}

out = []

# Query 1: All finance per marketplace
conn = get_conn()
c = conn.cursor()
c.execute("""
SELECT ISNULL(marketplace_id,'NONE'), COUNT(*), SUM(CAST(amount AS FLOAT))
FROM acc_finance_transaction
GROUP BY marketplace_id
ORDER BY ABS(SUM(CAST(amount AS FLOAT))) DESC
""")
out.append("=== ALL finance per marketplace ===")
for r in c.fetchall():
    mp = str(r[0]); name = MP.get(mp,"?"); t = r[2] or 0
    out.append(f"  {mp:20s} ({name:2s}) | cnt={r[1]:8d} | {t:14.2f} EUR")
conn.close()

# Query 2: CM2/NP costs per marketplace
conn = get_conn()
c = conn.cursor()
c.execute("""
SELECT ISNULL(marketplace_id,'NONE'), COUNT(*), SUM(CAST(amount AS FLOAT))
FROM acc_finance_transaction
WHERE charge_type NOT IN (
  'Principal','Tax','Commission','FBAPerUnitFulfillmentFee',
  'ShippingCharge','ShippingTax','DigitalServicesFee',
  'ReserveDebit','ReserveCredit','FailedDisbursement'
)
GROUP BY marketplace_id
ORDER BY ABS(SUM(CAST(amount AS FLOAT))) DESC
""")
out.append("\n=== CM2/NP costs per marketplace ===")
for r in c.fetchall():
    mp = str(r[0]); name = MP.get(mp,"?"); t = r[2] or 0
    out.append(f"  {mp:20s} ({name:2s}) | cnt={r[1]:8d} | cost={t:14.2f} EUR")
conn.close()

# Query 3: Detail per marketplace
conn = get_conn()
c = conn.cursor()
c.execute("""
SELECT ISNULL(marketplace_id,'NONE'), charge_type, COUNT(*), SUM(CAST(amount AS FLOAT))
FROM acc_finance_transaction
WHERE charge_type NOT IN (
  'Principal','Tax','Commission','FBAPerUnitFulfillmentFee',
  'ShippingCharge','ShippingTax','DigitalServicesFee',
  'ReserveDebit','ReserveCredit','FailedDisbursement'
)
GROUP BY marketplace_id, charge_type
ORDER BY marketplace_id, ABS(SUM(CAST(amount AS FLOAT))) DESC
""")
out.append("\n=== Detail per marketplace ===")
cur = None
for r in c.fetchall():
    mp = str(r[0])
    if mp != cur:
        name = MP.get(mp,"?")
        out.append(f"\n  --- {mp} ({name}) ---")
        cur = mp
    t = r[3] or 0
    out.append(f"    {str(r[1]):45s} | cnt={r[2]:6d} | {t:12.2f} EUR")
conn.close()

result = "\n".join(out)
with open("C:/ACC/mp_costs_result.txt", "w", encoding="utf-8") as f:
    f.write(result)
print(result)
print("\n--- DONE ---")
