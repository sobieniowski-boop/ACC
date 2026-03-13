import sys
sys.path.insert(0, r'C:\ACC\apps\api')

from app.core.db_connection import connect_acc
import json

conn = connect_acc(autocommit=True)
cur = conn.cursor()

# Get latest run
cur.execute("""
    SELECT TOP 1 
        run_id, status, error_message, result_json, created_at, progress_pct
    FROM dbo.family_restructure_run WITH (NOLOCK)
    WHERE family_id = 1367
      AND marketplace_id = 'A13V1IB3VIYZZH'
    ORDER BY created_at DESC
""")

r = cur.fetchone()
if r:
    run_id, status, error_msg, result_json, created_at, progress_pct = r
    print(f"Run ID: {run_id}")
    print(f"Status: {status}")
    print(f"Progress: {progress_pct}%")
    print(f"Error: {error_msg if error_msg else '✅ NONE'}")
    print(f"Created: {created_at}")
    print()
    
    if result_json:
        result = json.loads(result_json)
        print(f"Result keys: {list(result.keys())}")
        if 'total_steps' in result:
            print(f"Total steps: {result['total_steps']}")
        if 'errors' in result:
            print(f"Errors: {result['errors']}")
        if 'children_actionable' in result:
            print(f"Children processed: {result.get('children_actionable', 0)}")
        
        # Show last 3 steps
        if 'steps' in result:
            steps = result['steps']
            print(f"\n📋 Last 5 steps:")
            for step in steps[-5:]:
                action = step.get('action', '?')
                status_val = step.get('status', '?')
                reason = step.get('reason', '')
                print(f"  - {action}: {status_val} {reason}")

conn.close()
