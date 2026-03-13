from __future__ import annotations

import json

from app.connectors.dhl24_api.models import DHL24LabelDataResult
from app.services.dhl_cost_sync import ShipmentCostTarget, _extract_identifiers, _label_data_priceable
from app.services.dhl_logistics_aggregation import _classify_shadow_status


def test_label_data_from_dict_supports_legacy_payload_shape():
    payload = {
        "shipment_id": "11122223333",
        "primary_waybill_number": "JJD000000000001234567",
        "service_product": "AH",
        "shipper_name": "KADAX",
        "shipper_country": "PL",
        "receiver_name": "Jan Kowalski",
        "receiver_country": "PL",
        "pieces": [
            {
                "routing_barcode": "RB-1",
                "blp_piece_id": "PIECE-1",
                "piece_type": "PACKAGE",
                "weight": 2.0,
                "quantity": 1,
            }
        ],
    }
    result = DHL24LabelDataResult.from_dict(payload)
    assert result is not None
    assert result.service is not None
    assert result.service.product == "AH"
    assert result.shipper is not None
    assert result.shipper.name == "KADAX"
    assert result.receiver is not None
    assert result.receiver.country == "PL"


def test_extract_identifiers_uses_payload_tokens():
    payload = {
        "label_data": {
            "shipment_id": "11122223333",
            "primary_waybill_number": "JJD000000000001234567",
            "pieces": [
                {
                    "routing_barcode": "RB-1",
                    "blp_piece_id": "PIECE-1",
                }
            ],
        },
        "piece_shipments": [
            {
                "cedex_number": "CEDEX-1",
                "packages": [{"package_number": "TRACK-1"}],
            }
        ],
    }
    shipment = ShipmentCostTarget(
        shipment_id="00000000-0000-0000-0000-000000000001",
        shipment_number="SHIP-1",
        tracking_number=None,
        piece_id=None,
        cedex_number=None,
        source_payload_json=json.dumps(payload),
    )
    identifiers = _extract_identifiers(shipment)
    values = {item.value for item in identifiers}
    assert "SHIP-1" in values
    assert "JJD000000000001234567" in values
    assert "RB-1" in values
    assert "PIECE-1" in values
    assert "TRACK-1" in values


def test_label_data_priceable_requires_billing_and_address():
    label_data = DHL24LabelDataResult.from_dict(
        {
            "shipment_id": "11122223333",
            "billing": {
                "shipping_payment_type": "SHIPPER",
                "billing_account_number": "123456",
            },
            "service": {
                "product": "AH",
            },
            "shipper": {
                "country": "PL",
                "postal_code": "42-600",
                "city": "Tarnowskie Gory",
                "street": "Magazynowa",
                "house_number": "7",
            },
            "receiver": {
                "country": "PL",
                "postal_code": "00-001",
                "city": "Warsaw",
                "street": "Marszalkowska",
                "house_number": "1",
            },
            "pieces": [
                {
                    "piece_type": "PACKAGE",
                    "weight": 2.0,
                }
            ],
        }
    )
    assert _label_data_priceable(label_data) is True


def test_shadow_status_classification():
    assert _classify_shadow_status(0.0, 0.0) == "match_zero"
    assert _classify_shadow_status(10.0, 10.02) == "match"
    assert _classify_shadow_status(12.0, 0.0) == "legacy_only"
    assert _classify_shadow_status(0.0, 9.0) == "shadow_only"
    assert _classify_shadow_status(12.0, 9.0) == "delta"
