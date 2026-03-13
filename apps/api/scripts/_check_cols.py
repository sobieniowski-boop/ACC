from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()
cur.execute("SELECT TOP 1 * FROM acc_product")
cols = [d[0] for d in cur.description]
print("Columns:", cols)
conn.close()
