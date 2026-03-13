import sys, os, traceback
logf = open(r"C:\ACC\apps\api\debug2_result.txt", "w")

def log(msg):
    logf.write(msg + "\n")
    logf.flush()

try:
    sys.path.insert(0, r"C:\ACC\apps\api")
    log("Step 1: path OK")
    
    from app.core.config import settings
    log(f"Step 2: config OK, conn_str starts with: {settings.mssql_connection_string[:80]}")
    
    import pyodbc
    log("Step 3: pyodbc OK, connecting...")
    
    conn = pyodbc.connect(settings.mssql_connection_string, autocommit=False, timeout=10)
    log("Step 4: connected!")
    
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM acc_order")
    count = cur.fetchone()[0]
    log(f"Step 5: {count} orders in DB")
    cur.close()
    conn.close()
    log("ALL OK")
except Exception as e:
    log(f"ERROR: {e}")
    traceback.print_exc(file=logf)
finally:
    logf.close()
