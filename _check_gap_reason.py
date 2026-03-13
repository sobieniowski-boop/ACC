"""Check why Feb 2025 - Feb 2026 gap isn't filling."""
import sys, os
sys.path.insert(0, r"C:\ACC\apps\api")
os.chdir(r"C:\ACC\apps\api")
from dotenv import load_dotenv; load_dotenv("C:/ACC/.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=True, timeout=10)
cur = c.cursor()

# Check group sync entries by date range
cur.execute("""
    SELECT 
        CONVERT(VARCHAR(7), group_start, 120) m,
        processing_status,
        fund_transfer_status,
        COUNT(*) cnt,
        SUM(last_row_count) total_rows,
        SUM(CASE WHEN last_row_count = 0 THEN 1 ELSE 0 END) empty_groups,
        SUM(CASE WHEN last_row_count > 0 THEN 1 ELSE 0 END) has_rows
    FROM acc_fin_event_group_sync WITH (NOLOCK)
    WHERE group_start IS NOT NULL
    GROUP BY CONVERT(VARCHAR(7), group_start, 120), processing_status, fund_transfer_status
    ORDER BY m, processing_status
""")
print("Month     | Status          | FundXfer     | Groups | TotalRows | Empty | HasRows")
print("-" * 95)
for r in cur.fetchall():
    m = str(r[0] or '?')
    ps = str(r[1] or '?')
    ft = str(r[2] or '?')
    cnt = int(r[3])
    tr = int(r[4] or 0)
    eg = int(r[5])
    hr = int(r[6])
    print(f"{m:10}| {ps:16}| {ft:13}| {cnt:>6} | {tr:>9,} | {eg:>5} | {hr:>7}")

print("\n" + "=" * 50)
print("Groups with group_start in Feb-Dec 2025 + terminal + 0 rows:")
cur.execute("""
    SELECT financial_event_group_id, 
           CONVERT(VARCHAR(10), group_start, 120) gs,
           CONVERT(VARCHAR(10), group_end, 120) ge,
           processing_status, fund_transfer_status, last_row_count
    FROM acc_fin_event_group_sync WITH (NOLOCK)
    WHERE group_start >= '2025-02-01' AND group_start < '2026-01-01'
      AND processing_status = 'Closed'
    ORDER BY group_start
""")
rows = cur.fetchall()
print(f"Found {len(rows)} groups")
for r in rows[:20]:
    gid = str(r[0])[:20]
    gs = str(r[1])
    ge = str(r[2])
    ps = str(r[3])
    ft = str(r[4])
    rc = int(r[5] or 0)
    print(f"  {gid}... | {gs} -> {ge} | {ps}/{ft} | rows={rc}")
if len(rows) > 20:
    print(f"  ... and {len(rows)-20} more")

# Also check: how many actual transaction rows match these groups?
cur.execute("""
    SELECT g.financial_event_group_id,
           CONVERT(VARCHAR(10), g.group_start, 120) gs,
           g.last_row_count,
           COUNT(t.id) actual_rows
    FROM acc_fin_event_group_sync g WITH (NOLOCK)
    LEFT JOIN acc_finance_transaction t WITH (NOLOCK) 
        ON t.financial_event_group_id = g.financial_event_group_id
    WHERE g.group_start >= '2025-02-01' AND g.group_start < '2026-01-01'
      AND g.processing_status = 'Closed'
    GROUP BY g.financial_event_group_id, g.group_start, g.last_row_count
    ORDER BY g.group_start
""")
print("\nFeb-Dec 2025 groups: tracked vs actual rows:")
for r in cur.fetchall()[:15]:
    gid = str(r[0])[:20]
    gs = str(r[1])
    tracked = int(r[2] or 0)
    actual = int(r[3])
    flag = " *** MISMATCH" if tracked > 0 and actual == 0 else ""
    print(f"  {gid}... | start={gs} | tracked={tracked} | actual={actual}{flag}")

c.close()
