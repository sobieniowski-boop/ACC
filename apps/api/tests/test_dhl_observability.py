from __future__ import annotations

from app.services.dhl_observability import _build_unmatched_reasons


def test_build_unmatched_reasons_accumulates_missing_flags():
    reasons = _build_unmatched_reasons(
        {
            "has_primary_link": 0,
            "parcel_map_rows": 0,
            "billing_line_rows": 0,
            "cost_source": None,
            "is_estimated": 0,
        }
    )

    assert reasons == [
        "missing_order_link",
        "missing_parcel_map",
        "missing_billing_lines",
        "missing_shipment_cost",
    ]


def test_build_unmatched_reasons_marks_estimated_only():
    reasons = _build_unmatched_reasons(
        {
            "has_primary_link": 1,
            "parcel_map_rows": 2,
            "billing_line_rows": 3,
            "cost_source": "dhl_get_price",
            "is_estimated": 1,
        }
    )

    assert reasons == ["estimated_only"]
