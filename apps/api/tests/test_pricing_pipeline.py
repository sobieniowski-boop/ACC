"""Integration test — full pricing pipeline (snapshot → rule → evaluate → recommendation).

Scenario:
  1. Insert a synthetic pricing snapshot with our_price below a floor.
  2. Create a floor_price rule for that SKU.
  3. Run rule evaluation for the marketplace.
  4. Verify a recommendation was generated with correct attributes.
  5. Clean up all synthetic data.
"""
from __future__ import annotations

import uuid

import pytest

from app.core.db_connection import connect_acc
from app.services.pricing_state import record_snapshot
from app.services.pricing_rules import (
    upsert_rule,
    evaluate_rules_for_marketplace,
    get_pending_recommendations,
    delete_rule,
)

# Synthetic identifiers — unique per run to avoid collision with real data.
_RUN_ID = uuid.uuid4().hex[:8]
_SKU = f"_TEST_PIPE_{_RUN_ID}"
_ASIN = f"B0TEST{_RUN_ID[:4].upper()}"
_MKT = "A1PA6795UKMFR9"          # DE marketplace (always present in MARKETPLACE_REGISTRY)
_OUR_PRICE = 9.99
_FLOOR = 15.00                     # floor > our_price  ⇒  violation expected
_BUYBOX = 14.50


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cleanup(snap_id: int | None, rule_id: int | None):
    """Remove every row created by this test run."""
    conn = connect_acc(autocommit=False, timeout=30)
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")
        # recommendations (referencing the snapshot or rule)
        cur.execute(
            "DELETE FROM dbo.acc_pricing_recommendation WHERE seller_sku = ? AND marketplace_id = ?",
            (_SKU, _MKT),
        )
        # rule
        if rule_id:
            cur.execute("DELETE FROM dbo.acc_pricing_rule WHERE id = ?", (rule_id,))
        # snapshot
        if snap_id:
            cur.execute("DELETE FROM dbo.acc_pricing_snapshot WHERE id = ?", (snap_id,))
        conn.commit()
    finally:
        conn.close()


def _find_rule_id() -> int | None:
    """Look up the rule we created (needed for cleanup if upsert didn't return id)."""
    conn = connect_acc(autocommit=False, timeout=30)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id FROM dbo.acc_pricing_rule WITH (NOLOCK)
            WHERE seller_sku = ? AND marketplace_id = ? AND rule_type = 'floor_price'
        """, (_SKU, _MKT))
        row = cur.fetchone()
        return int(row[0]) if row else None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPricingPipeline:
    """End-to-end pricing pipeline validation."""

    snap_id: int | None = None
    rule_id: int | None = None

    @classmethod
    def setup_class(cls):
        """Pre-clean in case a previous run left orphan data."""
        rid = _find_rule_id()
        _cleanup(None, rid)

    @classmethod
    def teardown_class(cls):
        """Always clean up, even on failure."""
        _cleanup(cls.snap_id, cls.rule_id)

    # -- 1. Insert synthetic snapshot ------------------------------------

    def test_01_insert_snapshot(self):
        snap_id = record_snapshot(
            _SKU, _MKT,
            asin=_ASIN,
            our_price=_OUR_PRICE,
            buybox_price=_BUYBOX,
            has_buybox=False,
            lowest_price_new=_BUYBOX,
            num_offers_new=3,
            source="integration_test",
        )
        assert snap_id > 0, "record_snapshot must return positive id"
        TestPricingPipeline.snap_id = snap_id

    # -- 2. Create floor_price rule --------------------------------------

    def test_02_create_floor_rule(self):
        result = upsert_rule(
            "floor_price",
            seller_sku=_SKU,
            marketplace_id=_MKT,
            floor_price=_FLOOR,
            strategy="monitor",
            is_active=True,
            priority=1,
        )
        assert result["status"] == "upserted"

        rule_id = _find_rule_id()
        assert rule_id is not None, "floor_price rule must exist after upsert"
        TestPricingPipeline.rule_id = rule_id

    # -- 3. Run evaluation -----------------------------------------------

    def test_03_evaluate_rules(self):
        result = evaluate_rules_for_marketplace(_MKT)
        assert result["evaluated"] > 0, "at least our synthetic SKU should be evaluated"
        assert result["recommendations_created"] > 0, "floor violation should generate recommendation"

    # -- 4. Verify recommendation ----------------------------------------

    def test_04_recommendation_generated(self):
        recs = get_pending_recommendations(marketplace_id=_MKT, limit=500)
        matching = [r for r in recs if r["seller_sku"] == _SKU]
        assert len(matching) >= 1, f"expected recommendation for {_SKU}"

        rec = matching[0]

        # --- assertions from prompt ---
        assert rec["status"] == "pending"
        assert rec["confidence"] is not None and rec["confidence"] > 0
        assert rec["recommended_price"] != rec["current_price"], (
            f"recommended_price ({rec['recommended_price']}) must differ from current_price ({rec['current_price']})"
        )

        # --- extra sanity checks ---
        assert rec["reason_code"] == "price_below_floor"
        assert rec["recommended_price"] == _FLOOR
        assert rec["current_price"] == _OUR_PRICE
        assert rec["rule_id"] == TestPricingPipeline.rule_id
        assert rec["snapshot_id"] == TestPricingPipeline.snap_id
