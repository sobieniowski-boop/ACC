import sys
sys.path.insert(0, r'C:\ACC\apps\api')

from app.core.db_connection import connect_acc
import json

conn = connect_acc(autocommit=True)
cur = conn.cursor()

# Get latest run
cur.execute("""
    SELECT TOP 1 
        run_id, status, result_json, progress_pct
    FROM dbo.family_restructure_run WITH (NOLOCK)
    WHERE family_id = 1367
      AND marketplace_id = 'A13V1IB3VIYZZH'
    ORDER BY created_at DESC
""")

r = cur.fetchone()
if r:
    run_id, status, result_json, progress_pct = r
    print(f"Run ID: {run_id}")
    print(f"Status: {status}")
    print(f"Progress: {progress_pct}%")
    print()
    
    if result_json:
        try:
            result = json.loads(result_json)
            if 'steps' in result:
                steps = result['steps']
                print(f"Total steps completed: {len(steps)}")
                
                # Show action summary
                actions = {}
                for step in steps:
                    action = step.get('action', '?')
                    status_val = step.get('status', '?')
                    if action not in actions:
                        actions[action] = []
                    actions[action].append(status_val)
                
                print("\n📊 Actions executed:")
                for action, statuses in sorted(actions.items()):
                    print(f"  - {action}: {len(statuses)} times ({', '.join(set(statuses))})")
                
                # Show last 5 steps
                print(f"\n📋 Last 5 steps:")
                for step in steps[-5:]:
                    action = step.get('action', '?')
                    status_val = step.get('status', '?')
                    reason = step.get('reason', '')
                    error = step.get('error', '')
                    if reason:
                        print(f"  - {action}: {status_val} ({reason})")
                    elif error:
                        print(f"  - {action}: {status_val} - ERROR: {error[:100]}")
                    else:
                        print(f"  - {action}: {status_val}")
        except Exception as e:
            print(f"Error parsing result: {e}")

conn.close()
