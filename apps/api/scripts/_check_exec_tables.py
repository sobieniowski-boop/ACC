import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()
tables = ['executive_daily_metrics', 'executive_health_score', 'executive_opportunities']
for t in tables:
    cur.execute("SELECT COUNT(*) FROM " + t)
    row = cur.fetchone()
    print(t + ": " + str(row[0]) + " rows")
cur.close()
conn.close()
