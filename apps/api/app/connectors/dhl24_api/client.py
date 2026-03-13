from __future__ import annotations

import time
from datetime import date, timedelta
from html import escape
from typing import Any
from xml.etree import ElementTree as ET

import httpx
import structlog

from app.connectors.dhl24_api.errors import DHL24APIError, DHL24ConfigError
from app.connectors.dhl24_api.models import (
    DHL24BinaryDocument,
    DHL24LabelDataResult,
    DHL24PriceResult,
    DHL24PieceShipment,
    DHL24ShipmentBasic,
    DHL24TrackAndTraceResult,
    parse_binary_document,
    parse_labels_data_list,
    parse_price_result,
    parse_piece_shipments,
    parse_shipment_basic_list,
    parse_track_and_trace,
)
from app.core.config import settings

log = structlog.get_logger(__name__)

_SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
_SERVICE_NS = "https://dhl24.com.pl/webapi2/provider/service.html?ws=1"
_MAX_RETRIES = 3
_RETRY_DELAY_SEC = 2.0


def _first_text(root: ET.Element, name: str) -> str | None:
    node = root.find(f".//{{*}}{name}")
    if node is None or node.text is None:
        return None
    value = node.text.strip()
    return value or None


def _xml_bool(value: bool | None) -> str | None:
    if value is None:
        return None
    return "true" if value else "false"


def _xml_tag(name: str, value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        text = _xml_bool(value)
    else:
        text = str(value).strip()
    if not text:
        return ""
    return f"<{name}>{escape(text)}</{name}>"


class DHL24Client:
    """Minimal SOAP client for DHL24 WebAPI2 read-only methods."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout_sec: int | None = None,
    ):
        self.base_url = base_url or settings.DHL24_API_BASE_URL
        self.username = username if username is not None else settings.DHL24_API_USERNAME
        self.password = password if password is not None else settings.DHL24_API_PASSWORD
        self.timeout_sec = timeout_sec if timeout_sec is not None else int(settings.DHL24_TIMEOUT_SEC or 30)

    @property
    def is_configured(self) -> bool:
        return bool(self.username and self.password and settings.DHL24_ENABLED)

    def _auth_xml(self) -> str:
        if not self.is_configured:
            raise DHL24ConfigError("DHL24 API not configured - set DHL24_API_USERNAME + DHL24_API_PASSWORD in .env")
        return (
            "<authData>"
            f"<username>{escape(self.username)}</username>"
            f"<password>{escape(self.password)}</password>"
            "</authData>"
        )

    def _build_envelope(self, operation: str, body_xml: str) -> str:
        return (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<soapenv:Envelope '
            'xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
            f'xmlns:ns="{_SERVICE_NS}">'
            "<soapenv:Body>"
            f"<ns:{operation}>"
            f"{body_xml}"
            f"</ns:{operation}>"
            "</soapenv:Body>"
            "</soapenv:Envelope>"
        )

    def _parse_xml(self, payload: str) -> ET.Element:
        try:
            root = ET.fromstring(payload)
        except ET.ParseError as exc:
            raise DHL24APIError(f"DHL24 returned invalid XML: {exc}") from exc

        fault = root.find(f".//{{{_SOAP_NS}}}Fault")
        if fault is None:
            fault = root.find(".//Fault")
        if fault is not None:
            fault_code = _first_text(fault, "faultcode")
            fault_string = _first_text(fault, "faultstring") or "Unknown SOAP fault"
            raise DHL24APIError(
                f"DHL24 SOAP fault: {fault_string}",
                fault_code=fault_code,
            )
        return root

    def _soap_call(self, operation: str, body_xml: str) -> ET.Element:
        envelope = self._build_envelope(operation, body_xml)
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "Accept": "text/xml",
            "SOAPAction": f"{_SERVICE_NS}#{operation}",
        }

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                with httpx.Client(timeout=self.timeout_sec) as http:
                    response = http.post(self.base_url, content=envelope.encode("utf-8"), headers=headers)
            except httpx.HTTPError as exc:
                if attempt < _MAX_RETRIES:
                    log.warning("dhl24.http_retry", operation=operation, attempt=attempt, error=str(exc))
                    time.sleep(_RETRY_DELAY_SEC * attempt)
                    continue
                raise DHL24APIError(f"DHL24 HTTP error on {operation}: {exc}") from exc

            if response.status_code == 200:
                return self._parse_xml(response.text)

            if response.status_code >= 500 and attempt < _MAX_RETRIES:
                log.warning(
                    "dhl24.5xx_retry",
                    operation=operation,
                    attempt=attempt,
                    status=response.status_code,
                )
                time.sleep(_RETRY_DELAY_SEC * attempt)
                continue

            raise DHL24APIError(
                f"DHL24 API error: HTTP {response.status_code} on {operation}",
                status_code=response.status_code,
            )

        raise DHL24APIError(f"DHL24 API: max retries exceeded for {operation}")

    def get_version(self) -> str:
        root = self._soap_call("getVersion", "")
        return _first_text(root, "getVersionResult") or ""

    def get_my_shipments(
        self,
        *,
        created_from: date,
        created_to: date,
        offset: int = 0,
    ) -> list[DHL24ShipmentBasic]:
        body = (
            f"{self._auth_xml()}"
            f"<createdFrom>{created_from.isoformat()}</createdFrom>"
            f"<createdTo>{created_to.isoformat()}</createdTo>"
            f"<offset>{int(offset)}</offset>"
        )
        root = self._soap_call("getMyShipments", body)
        result = root.find(".//{*}getMyShipmentsResult")
        return parse_shipment_basic_list(result)

    def get_my_shipments_count(
        self,
        *,
        created_from: date,
        created_to: date,
    ) -> int:
        body = (
            f"{self._auth_xml()}"
            f"<createdFrom>{created_from.isoformat()}</createdFrom>"
            f"<createdTo>{created_to.isoformat()}</createdTo>"
        )
        root = self._soap_call("getMyShipmentsCount", body)
        value = _first_text(root, "getMyShipmentsCountResult")
        try:
            return int(value or 0)
        except ValueError:
            raise DHL24APIError(f"DHL24 returned invalid shipment count: {value!r}") from None

    def get_track_and_trace_info(self, shipment_id: str) -> DHL24TrackAndTraceResult:
        body = (
            f"{self._auth_xml()}"
            f"<shipmentId>{escape(shipment_id)}</shipmentId>"
        )
        root = self._soap_call("getTrackAndTraceInfo", body)
        result = root.find(".//{*}getTrackAndTraceInfoResult")
        return parse_track_and_trace(result)

    def get_shipment_scan(self, shipment_id: str) -> DHL24BinaryDocument:
        body = (
            f"{self._auth_xml()}"
            f"<shipmentId>{escape(shipment_id)}</shipmentId>"
        )
        root = self._soap_call("getShipmentScan", body)
        result = root.find(".//{*}getShipmentScanResult")
        return parse_binary_document(result)

    def get_epod(self, shipment_id: str) -> DHL24BinaryDocument:
        body = (
            f"{self._auth_xml()}"
            f"<shipmentId>{escape(shipment_id)}</shipmentId>"
        )
        root = self._soap_call("getEpod", body)
        result = root.find(".//{*}getEpodResult")
        return parse_binary_document(result)

    def get_piece_id(
        self,
        *,
        shipment_number: str | None = None,
        cedex_number: str | None = None,
        package_number: str | None = None,
    ) -> list[DHL24PieceShipment]:
        if not any([shipment_number, cedex_number, package_number]):
            raise DHL24APIError("getPieceId requires shipment_number, cedex_number or package_number")

        body = (
            f"{self._auth_xml()}"
            "<request><items><item>"
            f"<shipmentNumber>{escape(shipment_number or '')}</shipmentNumber>"
            f"<cedexNumber>{escape(cedex_number or '')}</cedexNumber>"
            f"<packageNumber>{escape(package_number or '')}</packageNumber>"
            "</item></items></request>"
        )
        root = self._soap_call("getPieceId", body)
        result = root.find(".//{*}getPieceIdResult")
        return parse_piece_shipments(result)

    def get_labels_data(self, shipment_ids: list[str]) -> list[DHL24LabelDataResult]:
        clean_ids = [str(item).strip() for item in shipment_ids if str(item or "").strip()]
        if not clean_ids:
            return []
        items_xml = "".join(
            f"<item><shipmentId>{escape(shipment_id)}</shipmentId></item>"
            for shipment_id in clean_ids
        )
        body = (
            f"{self._auth_xml()}"
            f"<itemsToLabelData>{items_xml}</itemsToLabelData>"
        )
        root = self._soap_call("getLabelsData", body)
        result = root.find(".//{*}getLabelsDataResult")
        return parse_labels_data_list(result)

    def _build_price_request_xml(self, label_data: DHL24LabelDataResult) -> str:
        payer_type = None
        account_number = None
        if label_data.billing:
            payer_type = (
                label_data.billing.shipping_payment_type
                or label_data.billing.payment_type
            )
            account_number = label_data.billing.billing_account_number

        payment_xml = ""
        if payer_type or account_number:
            payment_xml = (
                "<payment>"
                f"{_xml_tag('accountNumber', account_number)}"
                f"{_xml_tag('payerType', payer_type)}"
                "</payment>"
            )

        shipper_xml = ""
        if label_data.shipper:
            shipper_xml = (
                "<shipper>"
                f"{_xml_tag('country', label_data.shipper.country)}"
                f"{_xml_tag('name', label_data.shipper.name)}"
                f"{_xml_tag('postalCode', label_data.shipper.postal_code)}"
                f"{_xml_tag('city', label_data.shipper.city)}"
                f"{_xml_tag('street', label_data.shipper.street)}"
                f"{_xml_tag('houseNumber', label_data.shipper.house_number)}"
                f"{_xml_tag('apartmentNumber', label_data.shipper.apartment_number)}"
                "</shipper>"
            )

        receiver_xml = ""
        if label_data.receiver:
            receiver_xml = (
                "<receiver>"
                f"{_xml_tag('country', label_data.receiver.country)}"
                f"{_xml_tag('addressType', label_data.receiver.address_type)}"
                f"{_xml_tag('name', label_data.receiver.name)}"
                f"{_xml_tag('postalCode', label_data.receiver.postal_code)}"
                f"{_xml_tag('city', label_data.receiver.city)}"
                f"{_xml_tag('street', label_data.receiver.street)}"
                f"{_xml_tag('houseNumber', label_data.receiver.house_number)}"
                f"{_xml_tag('apartmentNumber', label_data.receiver.apartment_number)}"
                "</receiver>"
            )

        service_xml = ""
        if label_data.service:
            service_xml = (
                "<service>"
                f"{_xml_tag('product', label_data.service.product)}"
                f"{_xml_tag('deliveryEvening', _xml_bool(label_data.service.delivery_evening))}"
                f"{_xml_tag('deliveryOnSaturday', _xml_bool(label_data.service.delivery_on_saturday))}"
                f"{_xml_tag('pickupOnSaturday', _xml_bool(label_data.service.pickup_on_saturday))}"
                f"{_xml_tag('collectOnDelivery', _xml_bool(label_data.service.collect_on_delivery))}"
                f"{_xml_tag('collectOnDeliveryValue', label_data.service.collect_on_delivery_value)}"
                f"{_xml_tag('insurance', _xml_bool(label_data.service.insurance))}"
                f"{_xml_tag('insuranceValue', label_data.service.insurance_value)}"
                f"{_xml_tag('returnOnDelivery', _xml_bool(label_data.service.return_on_delivery))}"
                f"{_xml_tag('proofOfDelivery', _xml_bool(label_data.service.proof_of_delivery))}"
                f"{_xml_tag('selfCollect', _xml_bool(label_data.service.self_collect))}"
                f"{_xml_tag('predeliveryInformation', _xml_bool(label_data.service.predelivery_information))}"
                f"{_xml_tag('ageVer', _xml_bool(label_data.service.age_ver))}"
                "</service>"
            )

        pieces_xml = "".join(
            (
                "<item>"
                f"{_xml_tag('type', piece.piece_type)}"
                f"{_xml_tag('width', piece.width)}"
                f"{_xml_tag('height', piece.height)}"
                f"{_xml_tag('length', piece.length)}"
                f"{_xml_tag('weight', piece.weight)}"
                f"{_xml_tag('quantity', piece.quantity)}"
                f"{_xml_tag('nonStandard', _xml_bool(piece.non_standard))}"
                f"{_xml_tag('euroReturn', _xml_bool(piece.euro_return))}"
                "</item>"
            )
            for piece in label_data.pieces
        )
        piece_list_xml = f"<pieceList>{pieces_xml}</pieceList>" if pieces_xml else ""

        return (
            "<shipment>"
            f"{payment_xml}"
            f"{shipper_xml}"
            f"{receiver_xml}"
            f"{service_xml}"
            f"{piece_list_xml}"
            "</shipment>"
        )

    def get_price(self, label_data: DHL24LabelDataResult) -> DHL24PriceResult:
        body = f"{self._auth_xml()}{self._build_price_request_xml(label_data)}"
        root = self._soap_call("getPrice", body)
        result = root.find(".//{*}getPriceResult")
        return parse_price_result(result)

    def health_check(self) -> dict[str, Any]:
        started = time.perf_counter()
        response: dict[str, Any] = {
            "ok": False,
            "configured": self.is_configured,
            "base_url": self.base_url,
            "write_enabled": bool(settings.DHL24_WRITE_ENABLED),
        }
        try:
            response["version"] = self.get_version()
            if not self.is_configured:
                response["error"] = "DHL24 API not configured - set DHL24_API_USERNAME + DHL24_API_PASSWORD in .env"
                response["latency_ms"] = round((time.perf_counter() - started) * 1000, 1)
                return response

            today = date.today()
            count = self.get_my_shipments_count(
                created_from=today - timedelta(days=1),
                created_to=today,
            )
            response["shipments_probe_count"] = count
            response["ok"] = True
        except Exception as exc:
            response["error"] = str(exc)

        response["latency_ms"] = round((time.perf_counter() - started) * 1000, 1)
        return response
