from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from xml.etree import ElementTree as ET


def _find(node: ET.Element | None, name: str) -> ET.Element | None:
    if node is None:
        return None
    return node.find(f".//{{*}}{name}")


def _children(node: ET.Element | None, local_name: str) -> list[ET.Element]:
    if node is None:
        return []
    matches: list[ET.Element] = []
    for child in list(node):
        tag = child.tag.split("}", 1)[-1] if "}" in child.tag else child.tag
        if tag == local_name:
            matches.append(child)
    return matches


def _text(node: ET.Element | None, name: str) -> str | None:
    target = _find(node, name)
    if target is None or target.text is None:
        return None
    value = target.text.strip()
    return value or None


def _to_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: str | bool | None) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"}:
        return False
    return None


@dataclass
class DHL24ShipmentBasic:
    shipment_id: str
    created: str | None = None
    shipper_name: str | None = None
    receiver_name: str | None = None
    order_status: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "shipment_id": self.shipment_id,
            "created": self.created,
            "shipper_name": self.shipper_name,
            "receiver_name": self.receiver_name,
            "order_status": self.order_status,
        }


@dataclass
class DHL24TrackingEvent:
    status: str | None = None
    description: str | None = None
    terminal: str | None = None
    timestamp: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "description": self.description,
            "terminal": self.terminal,
            "timestamp": self.timestamp,
        }


@dataclass
class DHL24TrackAndTraceResult:
    shipment_id: str
    received_by: str | None = None
    events: list[DHL24TrackingEvent] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "shipment_id": self.shipment_id,
            "received_by": self.received_by,
            "events": [event.to_dict() for event in self.events],
        }


@dataclass
class DHL24BinaryDocument:
    mime_type: str | None = None
    content_base64: str | None = None

    @property
    def has_content(self) -> bool:
        return bool(self.content_base64)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mime_type": self.mime_type,
            "content_base64": self.content_base64,
            "has_content": self.has_content,
        }


@dataclass
class DHL24Piece:
    package_number: str | None = None
    product_type: str | None = None
    weight_real: float | None = None
    weight_volumetric: float | None = None
    width: int | None = None
    length: int | None = None
    height: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_number": self.package_number,
            "product_type": self.product_type,
            "weight_real": self.weight_real,
            "weight_volumetric": self.weight_volumetric,
            "width": self.width,
            "length": self.length,
            "height": self.height,
        }


@dataclass
class DHL24PieceShipment:
    shipment_number: str | None = None
    cedex_number: str | None = None
    packages: list[DHL24Piece] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "shipment_number": self.shipment_number,
            "cedex_number": self.cedex_number,
            "packages": [package.to_dict() for package in self.packages],
        }


@dataclass
class DHL24LabelBilling:
    shipping_payment_type: str | None = None
    billing_account_number: str | None = None
    payment_type: str | None = None
    costs_center: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "shipping_payment_type": self.shipping_payment_type,
            "billing_account_number": self.billing_account_number,
            "payment_type": self.payment_type,
            "costs_center": self.costs_center,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> DHL24LabelBilling | None:
        if not payload:
            return None
        return cls(
            shipping_payment_type=str(payload.get("shipping_payment_type") or "") or None,
            billing_account_number=str(payload.get("billing_account_number") or "") or None,
            payment_type=str(payload.get("payment_type") or "") or None,
            costs_center=str(payload.get("costs_center") or "") or None,
        )


@dataclass
class DHL24LabelAddress:
    name: str | None = None
    country: str | None = None
    postal_code: str | None = None
    city: str | None = None
    street: str | None = None
    house_number: str | None = None
    apartment_number: str | None = None
    contact_person: str | None = None
    contact_phone: str | None = None
    contact_email: str | None = None
    preaviso_phone: str | None = None
    preaviso_email: str | None = None
    preaviso_person: str | None = None
    address_type: str | None = None
    is_packstation: bool | None = None
    is_postfiliale: bool | None = None
    postnummer: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "country": self.country,
            "postal_code": self.postal_code,
            "city": self.city,
            "street": self.street,
            "house_number": self.house_number,
            "apartment_number": self.apartment_number,
            "contact_person": self.contact_person,
            "contact_phone": self.contact_phone,
            "contact_email": self.contact_email,
            "preaviso_phone": self.preaviso_phone,
            "preaviso_email": self.preaviso_email,
            "preaviso_person": self.preaviso_person,
            "address_type": self.address_type,
            "is_packstation": self.is_packstation,
            "is_postfiliale": self.is_postfiliale,
            "postnummer": self.postnummer,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> DHL24LabelAddress | None:
        if not payload:
            return None
        return cls(
            name=str(payload.get("name") or "") or None,
            country=str(payload.get("country") or "") or None,
            postal_code=str(payload.get("postal_code") or "") or None,
            city=str(payload.get("city") or "") or None,
            street=str(payload.get("street") or "") or None,
            house_number=str(payload.get("house_number") or "") or None,
            apartment_number=str(payload.get("apartment_number") or "") or None,
            contact_person=str(payload.get("contact_person") or "") or None,
            contact_phone=str(payload.get("contact_phone") or "") or None,
            contact_email=str(payload.get("contact_email") or "") or None,
            preaviso_phone=str(payload.get("preaviso_phone") or "") or None,
            preaviso_email=str(payload.get("preaviso_email") or "") or None,
            preaviso_person=str(payload.get("preaviso_person") or "") or None,
            address_type=str(payload.get("address_type") or "") or None,
            is_packstation=_to_bool(payload.get("is_packstation")),
            is_postfiliale=_to_bool(payload.get("is_postfiliale")),
            postnummer=str(payload.get("postnummer") or "") or None,
        )


@dataclass
class DHL24LabelService:
    product: str | None = None
    delivery_evening: bool | None = None
    delivery_on_saturday: bool | None = None
    pickup_on_saturday: bool | None = None
    collect_on_delivery: bool | None = None
    collect_on_delivery_value: float | None = None
    collect_on_delivery_form: str | None = None
    collect_on_delivery_reference: str | None = None
    insurance: bool | None = None
    insurance_value: float | None = None
    return_on_delivery: bool | None = None
    return_on_delivery_reference: str | None = None
    proof_of_delivery: bool | None = None
    self_collect: bool | None = None
    predelivery_information: bool | None = None
    delivery_to_neighbour: bool | None = None
    preaviso: bool | None = None
    additional_service: bool | None = None
    e_rodemail: str | None = None
    age_ver: bool | None = None
    vip: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "product": self.product,
            "delivery_evening": self.delivery_evening,
            "delivery_on_saturday": self.delivery_on_saturday,
            "pickup_on_saturday": self.pickup_on_saturday,
            "collect_on_delivery": self.collect_on_delivery,
            "collect_on_delivery_value": self.collect_on_delivery_value,
            "collect_on_delivery_form": self.collect_on_delivery_form,
            "collect_on_delivery_reference": self.collect_on_delivery_reference,
            "insurance": self.insurance,
            "insurance_value": self.insurance_value,
            "return_on_delivery": self.return_on_delivery,
            "return_on_delivery_reference": self.return_on_delivery_reference,
            "proof_of_delivery": self.proof_of_delivery,
            "self_collect": self.self_collect,
            "predelivery_information": self.predelivery_information,
            "delivery_to_neighbour": self.delivery_to_neighbour,
            "preaviso": self.preaviso,
            "additional_service": self.additional_service,
            "e_rodemail": self.e_rodemail,
            "age_ver": self.age_ver,
            "vip": self.vip,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> DHL24LabelService | None:
        if not payload:
            return None
        return cls(
            product=str(payload.get("product") or "") or None,
            delivery_evening=_to_bool(payload.get("delivery_evening")),
            delivery_on_saturday=_to_bool(payload.get("delivery_on_saturday")),
            pickup_on_saturday=_to_bool(payload.get("pickup_on_saturday")),
            collect_on_delivery=_to_bool(payload.get("collect_on_delivery")),
            collect_on_delivery_value=_to_float(payload.get("collect_on_delivery_value")),
            collect_on_delivery_form=str(payload.get("collect_on_delivery_form") or "") or None,
            collect_on_delivery_reference=str(payload.get("collect_on_delivery_reference") or "") or None,
            insurance=_to_bool(payload.get("insurance")),
            insurance_value=_to_float(payload.get("insurance_value")),
            return_on_delivery=_to_bool(payload.get("return_on_delivery")),
            return_on_delivery_reference=str(payload.get("return_on_delivery_reference") or "") or None,
            proof_of_delivery=_to_bool(payload.get("proof_of_delivery")),
            self_collect=_to_bool(payload.get("self_collect")),
            predelivery_information=_to_bool(payload.get("predelivery_information")),
            delivery_to_neighbour=_to_bool(payload.get("delivery_to_neighbour")),
            preaviso=_to_bool(payload.get("preaviso")),
            additional_service=_to_bool(payload.get("additional_service")),
            e_rodemail=str(payload.get("e_rodemail") or "") or None,
            age_ver=_to_bool(payload.get("age_ver")),
            vip=_to_bool(payload.get("vip")),
        )


@dataclass
class DHL24LabelPiece:
    routing_barcode: str | None = None
    blp_piece_id: str | None = None
    piece_type: str | None = None
    width: int | None = None
    height: int | None = None
    length: int | None = None
    weight: float | None = None
    quantity: int | None = None
    non_standard: bool | None = None
    euro_return: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "routing_barcode": self.routing_barcode,
            "blp_piece_id": self.blp_piece_id,
            "piece_type": self.piece_type,
            "width": self.width,
            "height": self.height,
            "length": self.length,
            "weight": self.weight,
            "quantity": self.quantity,
            "non_standard": self.non_standard,
            "euro_return": self.euro_return,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> DHL24LabelPiece | None:
        if not payload:
            return None
        return cls(
            routing_barcode=str(payload.get("routing_barcode") or "") or None,
            blp_piece_id=str(payload.get("blp_piece_id") or "") or None,
            piece_type=str(payload.get("piece_type") or "") or None,
            width=_to_int(payload.get("width")),
            height=_to_int(payload.get("height")),
            length=_to_int(payload.get("length")),
            weight=_to_float(payload.get("weight")),
            quantity=_to_int(payload.get("quantity")),
            non_standard=_to_bool(payload.get("non_standard")),
            euro_return=_to_bool(payload.get("euro_return")),
        )


@dataclass
class DHL24LabelDataResult:
    shipment_id: str
    primary_waybill_number: str | None = None
    dispatch_notification_number: str | None = None
    label_header: str | None = None
    reference: str | None = None
    content: str | None = None
    comment: str | None = None
    billing: DHL24LabelBilling | None = None
    service: DHL24LabelService | None = None
    shipper: DHL24LabelAddress | None = None
    receiver: DHL24LabelAddress | None = None
    pieces: list[DHL24LabelPiece] = field(default_factory=list)

    @property
    def service_product(self) -> str | None:
        return self.service.product if self.service else None

    @property
    def shipper_name(self) -> str | None:
        return self.shipper.name if self.shipper else None

    @property
    def shipper_country(self) -> str | None:
        return self.shipper.country if self.shipper else None

    @property
    def receiver_name(self) -> str | None:
        return self.receiver.name if self.receiver else None

    @property
    def receiver_country(self) -> str | None:
        return self.receiver.country if self.receiver else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "shipment_id": self.shipment_id,
            "primary_waybill_number": self.primary_waybill_number,
            "dispatch_notification_number": self.dispatch_notification_number,
            "label_header": self.label_header,
            "reference": self.reference,
            "content": self.content,
            "comment": self.comment,
            "service_product": self.service_product,
            "shipper_name": self.shipper_name,
            "shipper_country": self.shipper_country,
            "receiver_name": self.receiver_name,
            "receiver_country": self.receiver_country,
            "billing": self.billing.to_dict() if self.billing else None,
            "service": self.service.to_dict() if self.service else None,
            "shipper": self.shipper.to_dict() if self.shipper else None,
            "receiver": self.receiver.to_dict() if self.receiver else None,
            "pieces": [piece.to_dict() for piece in self.pieces],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> DHL24LabelDataResult | None:
        if not payload:
            return None

        billing = DHL24LabelBilling.from_dict(payload.get("billing"))
        service = DHL24LabelService.from_dict(payload.get("service"))
        shipper = DHL24LabelAddress.from_dict(payload.get("shipper"))
        receiver = DHL24LabelAddress.from_dict(payload.get("receiver"))

        if shipper is None and any(payload.get(key) for key in ("shipper_name", "shipper_country")):
            shipper = DHL24LabelAddress(
                name=str(payload.get("shipper_name") or "") or None,
                country=str(payload.get("shipper_country") or "") or None,
            )
        if receiver is None and any(payload.get(key) for key in ("receiver_name", "receiver_country")):
            receiver = DHL24LabelAddress(
                name=str(payload.get("receiver_name") or "") or None,
                country=str(payload.get("receiver_country") or "") or None,
            )
        if service is None and payload.get("service_product"):
            service = DHL24LabelService(product=str(payload.get("service_product") or "") or None)

        pieces: list[DHL24LabelPiece] = []
        for item in payload.get("pieces") or []:
            piece = DHL24LabelPiece.from_dict(item if isinstance(item, dict) else None)
            if piece:
                pieces.append(piece)

        shipment_id = str(payload.get("shipment_id") or "").strip()
        if not shipment_id:
            return None

        return cls(
            shipment_id=shipment_id,
            primary_waybill_number=str(payload.get("primary_waybill_number") or "") or None,
            dispatch_notification_number=str(payload.get("dispatch_notification_number") or "") or None,
            label_header=str(payload.get("label_header") or "") or None,
            reference=str(payload.get("reference") or "") or None,
            content=str(payload.get("content") or "") or None,
            comment=str(payload.get("comment") or "") or None,
            billing=billing,
            service=service,
            shipper=shipper,
            receiver=receiver,
            pieces=pieces,
        )


@dataclass
class DHL24PriceResult:
    price: float | None = None
    fuel_surcharge: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "price": self.price,
            "fuel_surcharge": self.fuel_surcharge,
        }


def parse_shipment_basic_list(result_node: ET.Element | None) -> list[DHL24ShipmentBasic]:
    items: list[DHL24ShipmentBasic] = []
    for item in _children(result_node, "item"):
        shipper = _find(item, "shipper")
        receiver = _find(item, "receiver")
        shipment_id = _text(item, "shipmentId") or ""
        if not shipment_id:
            continue
        items.append(
            DHL24ShipmentBasic(
                shipment_id=shipment_id,
                created=_text(item, "created"),
                shipper_name=_text(shipper, "name"),
                receiver_name=_text(receiver, "name"),
                order_status=_text(item, "orderStatus"),
            )
        )
    return items


def parse_track_and_trace(result_node: ET.Element | None) -> DHL24TrackAndTraceResult:
    shipment_id = _text(result_node, "shipmentId") or ""
    received_by = _text(result_node, "receivedBy")
    events_node = _find(result_node, "events")
    events: list[DHL24TrackingEvent] = []
    for item in _children(events_node, "item"):
        events.append(
            DHL24TrackingEvent(
                status=_text(item, "status"),
                description=_text(item, "description"),
                terminal=_text(item, "terminal"),
                timestamp=_text(item, "timestamp"),
            )
        )
    return DHL24TrackAndTraceResult(
        shipment_id=shipment_id,
        received_by=received_by,
        events=events,
    )


def parse_binary_document(result_node: ET.Element | None) -> DHL24BinaryDocument:
    return DHL24BinaryDocument(
        mime_type=_text(result_node, "scanMimeType"),
        content_base64=_text(result_node, "scanData"),
    )


def parse_piece_shipments(result_node: ET.Element | None) -> list[DHL24PieceShipment]:
    shipments: list[DHL24PieceShipment] = []
    shipments_node = _find(result_node, "shipments")
    for shipment_node in _children(shipments_node, "item"):
        packages_node = _find(shipment_node, "packages")
        packages: list[DHL24Piece] = []
        for package_node in _children(packages_node, "item"):
            packages.append(
                DHL24Piece(
                    package_number=_text(package_node, "packageNumber"),
                    product_type=_text(package_node, "productType"),
                    weight_real=_to_float(_text(package_node, "weightReal")),
                    weight_volumetric=_to_float(_text(package_node, "weighVolumetric")),
                    width=_to_int(_text(package_node, "width")),
                    length=_to_int(_text(package_node, "length")),
                    height=_to_int(_text(package_node, "height")),
                )
            )
        shipments.append(
            DHL24PieceShipment(
                shipment_number=_text(shipment_node, "shipmentNumber"),
                cedex_number=_text(shipment_node, "cedexNumber"),
                packages=packages,
            )
        )
    return shipments


def _parse_label_address(node: ET.Element | None) -> DHL24LabelAddress | None:
    if node is None:
        return None
    return DHL24LabelAddress(
        name=_text(node, "name"),
        country=_text(node, "country"),
        postal_code=_text(node, "postalCode"),
        city=_text(node, "city"),
        street=_text(node, "street"),
        house_number=_text(node, "houseNumber"),
        apartment_number=_text(node, "apartmentNumber"),
        contact_person=_text(node, "contactPerson"),
        contact_phone=_text(node, "contactPhone"),
        contact_email=_text(node, "contactEmail"),
        preaviso_phone=_text(node, "preavisoPhone"),
        preaviso_email=_text(node, "preavisoEmail"),
        preaviso_person=_text(node, "preavisoPerson"),
        address_type=_text(node, "addressType"),
        is_packstation=_to_bool(_text(node, "isPackstation")),
        is_postfiliale=_to_bool(_text(node, "isPostfiliale")),
        postnummer=_text(node, "postnummer"),
    )


def _parse_label_billing(node: ET.Element | None) -> DHL24LabelBilling | None:
    if node is None:
        return None
    return DHL24LabelBilling(
        shipping_payment_type=_text(node, "shippingPaymentType"),
        billing_account_number=_text(node, "billingAccountNumber"),
        payment_type=_text(node, "paymentType"),
        costs_center=_text(node, "costsCenter"),
    )


def _parse_label_service(node: ET.Element | None) -> DHL24LabelService | None:
    if node is None:
        return None
    return DHL24LabelService(
        product=_text(node, "product"),
        delivery_evening=_to_bool(_text(node, "deliveryEvening")),
        delivery_on_saturday=_to_bool(_text(node, "deliveryOnSaturday")),
        pickup_on_saturday=_to_bool(_text(node, "pickupOnSaturday")),
        collect_on_delivery=_to_bool(_text(node, "collectOnDelivery")),
        collect_on_delivery_value=_to_float(_text(node, "collectOnDeliveryValue")),
        collect_on_delivery_form=_text(node, "collectOnDeliveryForm"),
        collect_on_delivery_reference=_text(node, "collectOnDeliveryReference"),
        insurance=_to_bool(_text(node, "insurance")),
        insurance_value=_to_float(_text(node, "insuranceValue")),
        return_on_delivery=_to_bool(_text(node, "returnOnDelivery")),
        return_on_delivery_reference=_text(node, "returnOnDeliveryReference"),
        proof_of_delivery=_to_bool(_text(node, "proofOfDelivery")),
        self_collect=_to_bool(_text(node, "selfCollect")),
        predelivery_information=_to_bool(_text(node, "predeliveryInformation")),
        delivery_to_neighbour=_to_bool(_text(node, "deliveryToNeighbour")),
        preaviso=_to_bool(_text(node, "preaviso")),
        additional_service=_to_bool(_text(node, "additionalService")),
        e_rodemail=_text(node, "eRodemail"),
        age_ver=_to_bool(_text(node, "ageVer")),
        vip=_to_bool(_text(node, "vip")),
    )


def parse_labels_data_list(result_node: ET.Element | None) -> list[DHL24LabelDataResult]:
    items: list[DHL24LabelDataResult] = []
    for item in _children(result_node, "item"):
        shipment_id = _text(item, "shipmentId") or ""
        if not shipment_id:
            continue
        piece_list = _find(item, "pieceList")
        pieces: list[DHL24LabelPiece] = []
        for piece_node in _children(piece_list, "item"):
            pieces.append(
                DHL24LabelPiece(
                    routing_barcode=_text(piece_node, "routingBarcode"),
                    blp_piece_id=_text(piece_node, "blpPieceId"),
                    piece_type=_text(piece_node, "type"),
                    width=_to_int(_text(piece_node, "width")),
                    height=_to_int(_text(piece_node, "height")),
                    length=_to_int(_text(piece_node, "length")),
                    weight=_to_float(_text(piece_node, "weight")),
                    quantity=_to_int(_text(piece_node, "quantity")),
                    non_standard=_to_bool(_text(piece_node, "nonStandard")),
                    euro_return=_to_bool(_text(piece_node, "euroReturn")),
                )
            )
        items.append(
            DHL24LabelDataResult(
                shipment_id=shipment_id,
                primary_waybill_number=_text(item, "primaryWaybillNumber"),
                dispatch_notification_number=_text(item, "dispatchNotificationNumber"),
                label_header=_text(item, "labelHeader"),
                reference=_text(item, "reference"),
                content=_text(item, "content"),
                comment=_text(item, "comment"),
                billing=_parse_label_billing(_find(item, "billing")),
                service=_parse_label_service(_find(item, "service")),
                shipper=_parse_label_address(_find(item, "shipper")),
                receiver=_parse_label_address(_find(item, "receiver")),
                pieces=pieces,
            )
        )
    return items


def parse_price_result(result_node: ET.Element | None) -> DHL24PriceResult:
    return DHL24PriceResult(
        price=_to_float(_text(result_node, "price")),
        fuel_surcharge=_to_float(_text(result_node, "fuelSurcharge")),
    )
