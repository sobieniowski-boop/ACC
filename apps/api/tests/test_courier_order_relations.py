from __future__ import annotations

from datetime import date

from app.services.courier_order_relations import (
    CandidateOrder,
    SourceOrder,
    _detect_relation,
    _distribution_order_carrier_predicate,
)


def _source_order() -> SourceOrder:
    source = SourceOrder(
        amazon_order_id="405-1234567-1234567",
        acc_order_id="00000000-0000-0000-0000-000000000111",
        purchase_date=date(2026, 2, 5),
        reference_tokens={"405 1234567 1234567", "405-1234567-1234567"},
    )
    source.add_identity(
        distribution_order_id=1001,
        resolved_bl_order_id=1001,
        email="client@example.com",
        phone="500600700",
        fullname="jan kowalski",
        country="PL",
    )
    return source


def test_detect_relation_marks_explicit_reshipment_as_strong():
    decision = _detect_relation(
        carrier="GLS",
        source=_source_order(),
        candidate=CandidateOrder(
            order_id=2002,
            resolved_bl_order_id=2002,
            external_order_id=None,
            order_source="blconnect",
            order_source_id=645,
            order_date=date(2026, 2, 9),
            country="PL",
            email="client@example.com",
            phone="500600700",
            fullname="jan kowalski",
            context_text="ponowna dosyl brakujacego produktu do 405 1234567 1234567",
        ),
        replacement_flags=set(),
        lookahead_days=30,
    )

    assert decision is not None
    assert decision.relation_type == "reshipment"
    assert decision.is_strong is True
    assert decision.confidence >= 0.95


def test_detect_relation_keeps_same_customer_follow_up_as_weak_only():
    decision = _detect_relation(
        carrier="DHL",
        source=_source_order(),
        candidate=CandidateOrder(
            order_id=2003,
            resolved_bl_order_id=2003,
            external_order_id=None,
            order_source="blconnect",
            order_source_id=645,
            order_date=date(2026, 2, 8),
            country="PL",
            email="client@example.com",
            phone="",
            fullname="",
            context_text="regularny follow up klienta bez slow kluczowych",
        ),
        replacement_flags=set(),
        lookahead_days=30,
    )

    assert decision is not None
    assert decision.relation_type == "same_customer_follow_up"
    assert decision.is_strong is False
    assert 0.62 <= decision.confidence < 0.95


def test_distribution_order_carrier_predicate_uses_charindex_not_like():
    sql = _distribution_order_carrier_predicate("dco", "DHL").lower()

    assert "charindex('dhl'" in sql
    assert " like " not in sql
