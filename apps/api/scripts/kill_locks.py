"""Kill blocking sessions."""
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import pymssql

conn2 = pymssql.connect(
    server=os.environ["MSSQL_SERVER"],
    user=os.environ["MSSQL_USER"],
    password=os.environ["MSSQL_PASSWORD"],
    database=os.environ.get("MSSQL_DB", "ACC"),
    autocommit=True,
    tds_version="7.3",
)
cur2 = conn2.cursor()

# Kill specific sessions holding app locks
for sid in [71, 52]:
    try:
        cur2.execute(f"KILL {sid}")
        print(f"Killed session {sid}")
    except Exception as e:
        print(f"Could not kill {sid}: {e}")

conn2.close()
print("Done.")
