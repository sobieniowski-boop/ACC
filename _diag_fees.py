"""Diagnose fee sources and Finance API gap."""
import pymssql
from app.core.config import settings

c = pymssql.connect(
    server=settings.MSSQL_SERVER,
    user=settings.MSSQL_USER,
    password=settings.MSSQL_PASSWORD,
    database=settings.MSSQL_DATABASE,
    port=1433, login_timeout=15, timeout=30
)
cur = c.cursor()

# 1. Subscription charge detail
cur.execute(
    "SELECT charge_type, transaction_type, amount_pln, amount, currency, "
    "posted_date, marketplace_id "
    "FROM dbo.acc_finance_transaction WHERE charge_type = 'Subscription'"
)
print("=== Subscription txn ===")
for r in cur.fetchall():
    print(f"  type={r[0]}, txn={r[1]}, pln={r[2]}, orig={r[3]}, curr={r[4]}, posted={r[5]}, mp={r[6]}")

# 2. DigitalServicesFee samples
cur.execute(
    "SELECT TOP 3 charge_type, transaction_type, amount_pln, posted_date, marketplace_id "
    "FROM dbo.acc_finance_transaction WHERE charge_type = 'DigitalServicesFee' "
    "ORDER BY posted_date DESC"
)
print("\n=== DigitalServicesFee samples ===")
for r in cur.fetchall():
    print(f"  pln={float(r[2]):.2f}, posted={r[3]}, mp={r[4]}")

# 3. Event groups expected vs actual
cur.execute(
    "SELECT CAST(YEAR(group_start) AS VARCHAR) + '-' + "
    "RIGHT('0'+CAST(MONTH(group_start) AS VARCHAR),2) as mo, "
    "SUM(last_row_count) expected "
    "FROM dbo.acc_fin_event_group_sync "
    "GROUP BY CAST(YEAR(group_start) AS VARCHAR) + '-' + "
    "RIGHT('0'+CAST(MONTH(group_start) AS VARCHAR),2) "
    "ORDER BY mo"
)
groups = cur.fetchall()

cur.execute(
    "SELECT CAST(YEAR(posted_date) AS VARCHAR) + '-' + "
    "RIGHT('0'+CAST(MONTH(posted_date) AS VARCHAR),2) as mo, "
    "COUNT(*) actual "
    "FROM dbo.acc_finance_transaction "
    "GROUP BY CAST(YEAR(posted_date) AS VARCHAR) + '-' + "
    "RIGHT('0'+CAST(MONTH(posted_date) AS VARCHAR),2)"
)
actual_map = {r[0]: int(r[1]) for r in cur.fetchall()}

print("\n=== Event Groups vs Transactions fill rate ===")
total_exp = 0
total_act = 0
for r in groups:
    mo = r[0]
    exp = int(r[1])
    act = actual_map.get(mo, 0)
    pct = 100 * act / exp if exp > 0 else 0
    marker = "OK" if pct > 50 else "*** GAP ***"
    print(f"  {mo}: expected={exp:>8d}  actual={act:>8d}  fill={pct:>5.1f}%  {marker}")
    total_exp += exp
    total_act += act
print(f"  TOTAL: expected={total_exp:>8d}  actual={total_act:>8d}  fill={100*total_act/total_exp:.1f}%")

# 4. SB sync state
cur.execute("SELECT * FROM dbo.acc_sb_order_line_sync_state ORDER BY 1 DESC")
cols = [d[0] for d in cur.description]
print("\n=== SB sync state ===")
for r in cur.fetchall():
    print(f"  {dict(zip(cols, r))}")

c.close()

# 5. FEE_REGISTRY check
from app.core.fee_taxonomy import FEE_REGISTRY
print("\n=== FEE_REGISTRY entries for service fees ===")
for ct in ['Subscription', 'DigitalServicesFee', 'DigitalServicesFeeFBA',
           'PaidServicesFee', 'PaidServicesRefund']:
    entry = FEE_REGISTRY.get(ct)
    print(f"  {ct}: {entry}")
