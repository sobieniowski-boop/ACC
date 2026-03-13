from __future__ import annotations

from app.services.bl_order_lookup import ResolvedBlOrder, resolve_bl_orders_to_acc_orders


def test_resolve_bl_orders_to_acc_orders_without_netfox_fallback(monkeypatch):
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_distribution_to_holding_map",
        lambda cur, *, bl_order_ids: {},
    )
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_cached_external_orders",
        lambda cur, *, bl_order_ids: {100: "AMZ-100"},
    )
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_distribution_external_orders",
        lambda cur, *, bl_order_ids: {},
    )
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_acc_orders",
        lambda cur, *, external_order_ids: {
            "AMZ-100": ("AMZ-100", "acc-100"),
        },
    )

    result = resolve_bl_orders_to_acc_orders(object(), bl_order_ids=[100, 200])

    assert result == {
        100: ResolvedBlOrder(bl_order_id=100, amazon_order_id="AMZ-100", acc_order_id="acc-100"),
    }


def test_resolve_bl_orders_to_acc_orders_skips_external_orders_missing_in_acc(monkeypatch):
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_distribution_to_holding_map",
        lambda cur, *, bl_order_ids: {},
    )
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_cached_external_orders",
        lambda cur, *, bl_order_ids: {100: "AMZ-100", 200: "AMZ-200"},
    )
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_distribution_external_orders",
        lambda cur, *, bl_order_ids: {},
    )
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_acc_orders",
        lambda cur, *, external_order_ids: {"AMZ-100": ("AMZ-100", "acc-100")},
    )

    result = resolve_bl_orders_to_acc_orders(object(), bl_order_ids=[100, 200])

    assert result == {
        100: ResolvedBlOrder(bl_order_id=100, amazon_order_id="AMZ-100", acc_order_id="acc-100"),
    }


def test_resolve_bl_orders_to_acc_orders_bridges_distribution_order_via_holding(monkeypatch):
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_distribution_to_holding_map",
        lambda cur, *, bl_order_ids: {25558607: 301405405},
    )
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_cached_external_orders",
        lambda cur, *, bl_order_ids: {301405405: "AMZ-301405405"},
    )
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_distribution_external_orders",
        lambda cur, *, bl_order_ids: {},
    )
    monkeypatch.setattr(
        "app.services.bl_order_lookup._load_acc_orders",
        lambda cur, *, external_order_ids: {
            "AMZ-301405405": ("AMZ-301405405", "acc-bridge"),
        },
    )

    result = resolve_bl_orders_to_acc_orders(object(), bl_order_ids=[25558607])

    assert result == {
        25558607: ResolvedBlOrder(
            bl_order_id=301405405,
            amazon_order_id="AMZ-301405405",
            acc_order_id="acc-bridge",
        ),
    }
