"""Run only profiles + opportunities (steps 3-4)."""
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.seasonality_service import recompute_profiles
from app.services.seasonality_opportunity_engine import detect_seasonality_opportunities

print("Step 3/4: Recomputing profiles...")
result3 = recompute_profiles()
print(f"  => {result3}")

print("Step 4/4: Detecting opportunities...")
result4 = detect_seasonality_opportunities()
print(f"  => {result4}")

print("\nDone!")
