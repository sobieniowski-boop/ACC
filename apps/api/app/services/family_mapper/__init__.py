"""Family Mapper services package — DE Canonical → EU variation mapping."""
from app.services.family_mapper.master_key import build_master_key
from app.services.family_mapper.de_builder import rebuild_de_canonical, get_rebuild_status
from app.services.family_mapper.marketplace_sync import sync_marketplace_listings
from app.services.family_mapper.matching import run_matching
from app.services.family_mapper.coverage import recompute_coverage
from app.services.family_mapper.fix_package import generate_fix_package
from app.services.family_mapper.restructure import (
    analyze_restructure,
    analyze_restructure_all_marketplaces,
    create_restructure_run,
    execute_restructure,
    finish_restructure_run,
    get_restructure_run_status,
    update_restructure_run_progress,
)

__all__ = [
    "build_master_key",
    "rebuild_de_canonical",
    "get_rebuild_status",
    "sync_marketplace_listings",
    "run_matching",
    "recompute_coverage",
    "generate_fix_package",
    "analyze_restructure",
    "analyze_restructure_all_marketplaces",
    "create_restructure_run",
    "execute_restructure",
    "finish_restructure_run",
    "get_restructure_run_status",
    "update_restructure_run_progress",
]
