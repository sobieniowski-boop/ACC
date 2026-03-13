import sys, os
sys.path.insert(0, r"C:\ACC\apps\api")
os.chdir(r"C:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")
from app.core.db_connection import connect_acc
c = connect_acc(autocommit=True, timeout=10)
cur = c.cursor()
cur.execute("""
    SELECT COUNT(*) total,
           SUM(CASE WHEN processing_status = 'Closed' THEN 1 ELSE 0 END) closed_g
    FROM acc_fin_event_group_sync WITH (NOLOCK)
""")
r = cur.fetchone()
print(f"Groups: {r[0]} total, {r[1]} closed")
c.close()
