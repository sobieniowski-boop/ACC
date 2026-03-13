import json
import sys
sys.path.insert(0, 'C:/ACC/apps/api')
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()
cur.execute("""
    SELECT TOP 1 error_message, result_json 
    FROM dbo.family_restructure_run 
    WHERE family_id=1367 AND marketplace_id='A13V1IB3VIYZZH' 
    ORDER BY created_at DESC
""")
r = cur.fetchone()

if r:
    print("Error message:", r[0] if r[0] else "None")
    if r[1]:
        result = json.loads(r[1])
        print("Result status:", result.get('status'))
        print("Result error:", result.get('error'))
        
        error_steps = [s for s in result.get('steps', []) if s.get('status') in ['error', 'conflict', 'failed']]
        print(f"\nError/conflict steps: {len(error_steps)}")
        for s in error_steps[:5]:
            print(f"  - {s.get('action')}: {s.get('status')}")
            print(f"    {s.get('reason', s.get('error', ''))}")
        
        # Check for CHECK_ASIN_CONFLICT step
        asin_check = [s for s in result.get('steps', []) if s.get('action') == 'CHECK_ASIN_CONFLICT']
        if asin_check:
            print(f"\nCHECK_ASIN_CONFLICT steps: {len(asin_check)}")
            for s in asin_check:
                print(f"  Status: {s.get('status')}, Reason: {s.get('reason', '')}")
else:
    print("No runs found")

conn.close()
