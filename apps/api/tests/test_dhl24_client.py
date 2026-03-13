from __future__ import annotations

from xml.etree import ElementTree as ET

from app.connectors.dhl24_api.models import (
    parse_binary_document,
    parse_labels_data_list,
    parse_price_result,
    parse_piece_shipments,
    parse_shipment_basic_list,
    parse_track_and_trace,
)


def test_parse_shipment_basic_list():
    root = ET.fromstring(
        """
        <getMyShipmentsResult>
            <item>
                <shipmentId>11122223333</shipmentId>
                <created>2026-03-05</created>
                <shipper><name>KADAX</name></shipper>
                <receiver><name>Jan Kowalski</name></receiver>
                <orderStatus>DELIVERED</orderStatus>
            </item>
        </getMyShipmentsResult>
        """
    )
    items = parse_shipment_basic_list(root)
    assert len(items) == 1
    assert items[0].shipment_id == "11122223333"
    assert items[0].shipper_name == "KADAX"
    assert items[0].receiver_name == "Jan Kowalski"
    assert items[0].order_status == "DELIVERED"


def test_parse_track_and_trace():
    root = ET.fromstring(
        """
        <getTrackAndTraceInfoResult>
            <shipmentId>11122223333</shipmentId>
            <receivedBy>Jan Kowalski</receivedBy>
            <events>
                <item>
                    <status>DOR</status>
                    <description>Delivered</description>
                    <terminal>Warsaw</terminal>
                    <timestamp>2026-03-05 10:15:00</timestamp>
                </item>
                <item>
                    <status>MAG</status>
                    <description>In depot</description>
                    <terminal>Lodz</terminal>
                    <timestamp>2026-03-04 23:40:00</timestamp>
                </item>
            </events>
        </getTrackAndTraceInfoResult>
        """
    )
    result = parse_track_and_trace(root)
    assert result.shipment_id == "11122223333"
    assert result.received_by == "Jan Kowalski"
    assert len(result.events) == 2
    assert result.events[0].status == "DOR"
    assert result.events[0].description == "Delivered"


def test_parse_binary_document():
    root = ET.fromstring(
        """
        <getEpodResult>
            <scanData>JVBERi0xLjQKJ...</scanData>
            <scanMimeType>application/pdf</scanMimeType>
        </getEpodResult>
        """
    )
    result = parse_binary_document(root)
    assert result.has_content is True
    assert result.mime_type == "application/pdf"
    assert result.content_base64 == "JVBERi0xLjQKJ..."


def test_parse_piece_shipments():
    root = ET.fromstring(
        """
        <getPieceIdResult>
            <shipments>
                <item>
                    <shipmentNumber>SHIP-1</shipmentNumber>
                    <cedexNumber>CEDEX-1</cedexNumber>
                    <packages>
                        <item>
                            <packageNumber>TRACK-1</packageNumber>
                            <productType>AH</productType>
                            <weightReal>2.5</weightReal>
                            <weighVolumetric>3.0</weighVolumetric>
                            <width>20</width>
                            <length>30</length>
                            <height>10</height>
                        </item>
                    </packages>
                </item>
            </shipments>
        </getPieceIdResult>
        """
    )
    items = parse_piece_shipments(root)
    assert len(items) == 1
    assert items[0].shipment_number == "SHIP-1"
    assert items[0].packages[0].package_number == "TRACK-1"
    assert items[0].packages[0].weight_real == 2.5


def test_parse_labels_data_list():
    root = ET.fromstring(
        """
        <getLabelsDataResult>
            <item>
                <shipmentId>11122223333</shipmentId>
                <primaryWaybillNumber>JJD000000000001234567</primaryWaybillNumber>
                <dispatchNotificationNumber>DISP-1</dispatchNotificationNumber>
                <reference>405-1234567-1234567</reference>
                <billing>
                    <shippingPaymentType>SHIPPER</shippingPaymentType>
                    <billingAccountNumber>123456</billingAccountNumber>
                    <paymentType>BANK_TRANSFER</paymentType>
                    <costsCenter>KDX</costsCenter>
                </billing>
                <service>
                    <product>AH</product>
                    <proofOfDelivery>true</proofOfDelivery>
                    <insurance>false</insurance>
                </service>
                <shipper>
                    <name>KADAX</name>
                    <country>PL</country>
                    <postalCode>42-600</postalCode>
                    <city>Tarnowskie Gory</city>
                    <street>Magazynowa</street>
                    <houseNumber>7</houseNumber>
                </shipper>
                <receiver>
                    <name>Jan Kowalski</name>
                    <country>PL</country>
                    <postalCode>00-001</postalCode>
                    <city>Warsaw</city>
                    <street>Marszalkowska</street>
                    <houseNumber>1</houseNumber>
                    <addressType>B</addressType>
                </receiver>
                <pieceList>
                    <item>
                        <routingBarcode>RB-1</routingBarcode>
                        <blpPieceId>PIECE-1</blpPieceId>
                        <type>PACKAGE</type>
                        <width>20</width>
                        <height>10</height>
                        <length>30</length>
                        <weight>2.0</weight>
                        <quantity>1</quantity>
                        <nonStandard>false</nonStandard>
                        <euroReturn>false</euroReturn>
                    </item>
                </pieceList>
            </item>
        </getLabelsDataResult>
        """
    )
    items = parse_labels_data_list(root)
    assert len(items) == 1
    assert items[0].shipment_id == "11122223333"
    assert items[0].primary_waybill_number == "JJD000000000001234567"
    assert items[0].reference == "405-1234567-1234567"
    assert items[0].service_product == "AH"
    assert items[0].billing is not None
    assert items[0].billing.shipping_payment_type == "SHIPPER"
    assert items[0].billing.billing_account_number == "123456"
    assert items[0].service is not None
    assert items[0].service.proof_of_delivery is True
    assert items[0].shipper is not None
    assert items[0].shipper.postal_code == "42-600"
    assert items[0].receiver is not None
    assert items[0].receiver.address_type == "B"
    assert items[0].pieces[0].blp_piece_id == "PIECE-1"
    assert items[0].pieces[0].width == 20
    assert items[0].pieces[0].non_standard is False


def test_parse_price_result():
    root = ET.fromstring(
        """
        <getPriceResult>
            <price>15.50</price>
            <fuelSurcharge>2.25</fuelSurcharge>
        </getPriceResult>
        """
    )
    result = parse_price_result(root)
    assert result.price == 15.5
    assert result.fuel_surcharge == 2.25
