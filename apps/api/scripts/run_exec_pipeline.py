import sys, os
# Ensure the api directory is on path and is the cwd
api_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, api_dir)
os.chdir(api_dir)

# Force settings to load before anything else
from app.core.config import settings
print("mssql_enabled:", settings.mssql_enabled)

from app.services.executive_service import run_executive_pipeline

result = run_executive_pipeline(days_back=180)
for k, v in result.items():
    print(f"  {k}: {v}")
print("Done!")
