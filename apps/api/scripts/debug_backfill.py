import sys, os, traceback
try:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    print("Step 1: sys.path OK", flush=True)
    
    import pyodbc
    print("Step 2: pyodbc OK", flush=True)
    
    import logging
    print("Step 3: logging OK", flush=True)
    
    from app.core.config import settings, MARKETPLACE_REGISTRY
    print(f"Step 4: config OK, {len(MARKETPLACE_REGISTRY)} mkts", flush=True)
    
    from app.connectors.amazon_sp_api.orders import OrdersClient
    print("Step 5: OrdersClient OK", flush=True)
    
    # Test logging to file
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backfill_debug.log"), encoding="utf-8"),
        ],
    )
    logger = logging.getLogger("test")
    logger.info("Logging works!")
    print("Step 6: logging to file OK", flush=True)
    
    conn = pyodbc.connect(settings.mssql_connection_string, autocommit=False)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM acc_order")
    count = cur.fetchone()[0]
    print(f"Step 7: DB OK, {count} orders", flush=True)
    cur.close()
    conn.close()
    
    print("ALL CHECKS PASSED", flush=True)
except Exception as e:
    print(f"FAILED: {e}", flush=True)
    traceback.print_exc()
