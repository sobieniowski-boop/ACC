"""Content Ops service package - backward-compatible re-exports.

All public symbols from the original content_ops.py monolith are re-exported
so that ``from app.services.content_ops import X`` keeps working everywhere.
"""
from __future__ import annotations

# ── helpers / constants ────────────────────────────────────────────────────
from ._helpers import (                                     # noqa: F401
    ContentOpsNotImplementedError,
    ensure_v2_schema,
    get_brand_for_sku,
    link_asset,
    list_assets,
    upload_asset,
    # internal helpers used by tests
    _connect,
    _fetchall_dict,
    _run_async,
)

# ── tasks ──────────────────────────────────────────────────────────────────
from .tasks import (                                        # noqa: F401
    bulk_update_content_tasks,
    create_content_task,
    list_content_tasks,
    update_content_task,
)

# ── versions ───────────────────────────────────────────────────────────────
from .versions import (                                     # noqa: F401
    approve_version,
    create_version,
    get_content_diff,
    list_versions,
    submit_version_review,
    sync_content,
    update_version,
)

# ── publish ────────────────────────────────────────────────────────────────
from .publish import (                                      # noqa: F401
    create_publish_package,
    create_publish_push,
    evaluate_publish_queue_alerts,
    get_publish_queue_health,
    list_publish_jobs,
    process_queued_publish_jobs,
    retry_publish_job,
)

# ── catalog ────────────────────────────────────────────────────────────────
from .catalog import (                                      # noqa: F401
    apply_publish_mapping_suggestions,
    get_publish_coverage,
    get_publish_mapping_suggestions,
    list_attribute_mappings,
    list_product_type_definitions,
    list_product_type_mappings,
    refresh_product_type_definition,
    upsert_attribute_mappings,
    upsert_product_type_mappings,
)

# ── policy ─────────────────────────────────────────────────────────────────
from .policy import (                                       # noqa: F401
    get_content_data_quality,
    get_content_impact,
    list_compliance_queue,
    list_policy_rules,
    policy_check,
    upsert_policy_rules,
)

# ── compliance ─────────────────────────────────────────────────────────────
from .compliance import (                                   # noqa: F401
    ai_generate,
    get_content_ops_health,
    onboard_catalog_search_by_ean,
    onboard_restrictions_check,
    run_onboard_preflight,
    verify_content_quality,
)
