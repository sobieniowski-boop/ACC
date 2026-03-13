import sys, time, traceback
sys.path.insert(0, r"C:\ACC\apps\api")

from app.services.fba_ops.fba_fee_audit import get_overcharge_summary

print("Calling get_overcharge_summary...")
t0 = time.time()
try:
    result = get_overcharge_summary()
    elapsed = time.time() - t0
    print(f"OK in {elapsed:.1f}s")
    print(f"total_skus_affected: {result.get('total_skus_affected')}")
    print(f"total_estimated_overcharge_eur: {result.get('total_estimated_overcharge_eur')}")
    if result.get("items"):
        first = result["items"][0]
        print(f"First item keys: {sorted(first.keys())}")
        print(f"estimated_overcharge_eur present: {'estimated_overcharge_eur' in first}")
except Exception as e:
    elapsed = time.time() - t0
    print(f"ERROR after {elapsed:.1f}s: {e}")
    traceback.print_exc()
