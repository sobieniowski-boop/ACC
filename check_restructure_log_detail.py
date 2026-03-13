import json, sys
sys.path.insert(0, r'C:\ACC\apps\api')
from app.core.db_connection import connect_acc

FR_MARKETPLACE = "A13V1IB3VIYZZH"
ACIER_SKUS = {"FBA_5903699409312","FBA_5903699409329","FBA_5903699409336","FBA_5903699409343",
              "FBA_5903699409350","FBA_5903699409398","FBA_5903699409381","FBA_5903699409367","FBA_5903699409374"}
STANDARD_CONFLICT_SKUS = {"FBA_5902730382072","FBA_5902730382089","FBA_5902730382096","FBA_5902730382102"}

conn = connect_acc(autocommit=True)
cur = conn.cursor()

cur.execute("""
    SELECT TOP 1 steps_json
    FROM dbo.family_restructure_log WITH (NOLOCK)
    WHERE family_id = 1367 AND marketplace_id = ?
    ORDER BY executed_at DESC
""", [FR_MARKETPLACE])
r = cur.fetchone()
steps = json.loads(r[0]) if r and r[0] else []

# Show ALL steps for Acier or conflicting SKUs
print(f"Total steps: {len(steps)}")
all_skus = ACIER_SKUS | STANDARD_CONFLICT_SKUS

for s in steps:
    sku = s.get("sku", "")
    if sku in all_skus:
        action = s.get("action", "?")
        asin = s.get("asin", "")
        status = s.get("result", {}).get("status", "?") if isinstance(s.get("result"), dict) else "?"
        print(f"\n[{action}] sku={sku}, asin={asin}, status={status}")
        if s.get("patches"):
            print(f"  patches: {json.dumps(s['patches'], ensure_ascii=False)}")
        if s.get("body"):
            body = s["body"]
            # Show color/size related attrs only
            attrs = body.get("attributes", {})
            relevant = {}
            for k in ["color", "size", "color_name", "size_name", "child_parent_sku_relationship", "parentage_level"]:
                if k in attrs:
                    relevant[k] = attrs[k]
            if relevant:
                print(f"  body.attrs (relevant): {json.dumps(relevant, ensure_ascii=False)}")

# Also check ENRICH_FROM_DE step
print("\n\n=== ENRICH_FROM_DE details ===")
for s in steps:
    if s.get("action") == "ENRICH_FROM_DE":
        print(json.dumps(s, ensure_ascii=False)[:2000])

# Also check targeted_repair logs
print("\n\n=== TARGETED REPAIR LOGS ===")
cur.execute("""
    SELECT TOP 3 executed_at, steps_json
    FROM dbo.family_restructure_log WITH (NOLOCK)
    WHERE family_id = 1367 AND marketplace_id = ?
    ORDER BY executed_at ASC
""", [FR_MARKETPLACE])
for row in cur.fetchall():
    steps2 = json.loads(row[1]) if row[1] else []
    has_repair = any(s.get("action", "").startswith("REPAIR") or s.get("action", "").startswith("TARGETED") for s in steps2)
    print(f"\nLog {row[0]}: {len(steps2)} steps, has_repair={has_repair}")
    for s in steps2:
        sku = s.get("sku", "")
        if sku in all_skus:
            print(f"  [{s.get('action')}] {sku}: patches={json.dumps(s.get('patches',''))[:300]}")

conn.close()
