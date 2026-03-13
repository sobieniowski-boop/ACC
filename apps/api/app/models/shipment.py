"""Shipment-centric logistics models."""
from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class AccShipment(Base):
    __tablename__ = "acc_shipment"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    carrier: Mapped[str] = mapped_column(String(16), default="DHL", index=True)
    carrier_account: Mapped[str | None] = mapped_column(String(64), nullable=True)
    shipment_number: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    piece_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    tracking_number: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    cedex_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    service_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ship_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at_carrier: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    received_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_delivered: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    recipient_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    recipient_country: Mapped[str | None] = mapped_column(String(8), nullable=True)
    shipper_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    shipper_country: Mapped[str | None] = mapped_column(String(8), nullable=True)
    source_system: Mapped[str] = mapped_column(String(32), default="dhl_webapi2")
    source_payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_payload_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_sync_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AccShipmentOrderLink(Base):
    __tablename__ = "acc_shipment_order_link"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    shipment_id: Mapped[uuid.UUID] = mapped_column(index=True)
    amazon_order_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    acc_order_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True, index=True)
    bl_order_id: Mapped[int | None] = mapped_column(nullable=True, index=True)
    link_method: Mapped[str] = mapped_column(String(64), default="unknown")
    link_confidence: Mapped[float] = mapped_column(Numeric(9, 4), default=0)
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AccShipmentEvent(Base):
    __tablename__ = "acc_shipment_event"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    shipment_id: Mapped[uuid.UUID] = mapped_column(index=True)
    event_code: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    event_label: Mapped[str | None] = mapped_column(String(500), nullable=True)
    event_terminal: Mapped[str | None] = mapped_column(String(255), nullable=True)
    event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    location_city: Mapped[str | None] = mapped_column(String(255), nullable=True)
    location_country: Mapped[str | None] = mapped_column(String(8), nullable=True)
    raw_payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AccShipmentPod(Base):
    __tablename__ = "acc_shipment_pod"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    shipment_id: Mapped[uuid.UUID] = mapped_column(index=True)
    pod_type: Mapped[str] = mapped_column(String(32), default="epod")
    available: Mapped[bool] = mapped_column(Boolean, default=False)
    mime_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    document_base64: Mapped[str | None] = mapped_column(Text, nullable=True)
    document_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    raw_payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    downloaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_sync_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AccShipmentCost(Base):
    __tablename__ = "acc_shipment_cost"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    shipment_id: Mapped[uuid.UUID] = mapped_column(index=True)
    cost_source: Mapped[str] = mapped_column(String(64), index=True)
    currency: Mapped[str] = mapped_column(String(8), default="PLN")
    net_amount: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    fuel_amount: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    toll_amount: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    gross_amount: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    invoice_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    invoice_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    billing_period: Mapped[str | None] = mapped_column(String(32), nullable=True)
    is_estimated: Mapped[bool] = mapped_column(Boolean, default=False)
    raw_payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AccOrderCourierRelation(Base):
    __tablename__ = "acc_order_courier_relation"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    carrier: Mapped[str] = mapped_column(String(16), index=True)
    source_amazon_order_id: Mapped[str] = mapped_column(String(80), index=True)
    source_acc_order_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True, index=True)
    source_distribution_order_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    source_bl_order_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    source_purchase_date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    related_distribution_order_id: Mapped[int] = mapped_column(BigInteger, index=True)
    related_bl_order_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    related_external_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    related_order_source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    related_order_source_id: Mapped[int | None] = mapped_column(nullable=True)
    related_order_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    relation_type: Mapped[str] = mapped_column(String(32), index=True)
    detection_method: Mapped[str] = mapped_column(String(64))
    confidence: Mapped[float] = mapped_column(Numeric(9, 4), default=0)
    is_strong: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    evidence_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AccShipmentOutcomeFact(Base):
    __tablename__ = "acc_shipment_outcome_fact"

    shipment_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    carrier: Mapped[str] = mapped_column(String(16), index=True)
    ship_month: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)
    amazon_order_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    acc_order_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True, index=True)
    bl_order_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    primary_link_method: Mapped[str | None] = mapped_column(String(64), nullable=True)
    relation_type: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    relation_confidence: Mapped[float | None] = mapped_column(Numeric(9, 4), nullable=True)
    outcome_code: Mapped[str] = mapped_column(String(32), index=True)
    outcome_confidence: Mapped[float] = mapped_column(Numeric(9, 4), default=0)
    cost_reason: Mapped[str] = mapped_column(String(32), index=True)
    cost_reason_confidence: Mapped[float] = mapped_column(Numeric(9, 4), default=0)
    classifier_version: Mapped[str] = mapped_column(String(32), default="courier_semantics_v1")
    evidence_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AccOrderLogisticsFact(Base):
    __tablename__ = "acc_order_logistics_fact"

    amazon_order_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    calc_version: Mapped[str] = mapped_column(String(32), primary_key=True, default="dhl_v1")
    acc_order_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True, index=True)
    shipments_count: Mapped[int] = mapped_column(default=0)
    delivered_shipments_count: Mapped[int] = mapped_column(default=0)
    actual_shipments_count: Mapped[int] = mapped_column(default=0)
    estimated_shipments_count: Mapped[int] = mapped_column(default=0)
    total_logistics_pln: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    last_delivery_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_system: Mapped[str] = mapped_column(String(32), default="shipment_aggregate")
    calculated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AccOrderLogisticsShadow(Base):
    __tablename__ = "acc_order_logistics_shadow"

    amazon_order_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    calc_version: Mapped[str] = mapped_column(String(32), primary_key=True, default="dhl_v1")
    acc_order_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True, index=True)
    legacy_logistics_pln: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    shadow_logistics_pln: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    delta_pln: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    delta_abs_pln: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    shipments_count: Mapped[int] = mapped_column(default=0)
    actual_shipments_count: Mapped[int] = mapped_column(default=0)
    estimated_shipments_count: Mapped[int] = mapped_column(default=0)
    comparison_status: Mapped[str] = mapped_column(String(32), default="unknown", index=True)
    calculated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AccCourierCostEstimate(Base):
    __tablename__ = "acc_courier_cost_estimate"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    shipment_id: Mapped[uuid.UUID] = mapped_column(index=True)
    carrier: Mapped[str] = mapped_column(String(16), index=True)
    amazon_order_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    estimator_name: Mapped[str] = mapped_column(String(64), default="courier_hist_v1")
    model_version: Mapped[str] = mapped_column(String(32), default="courier_hist_v1")
    horizon_days: Mapped[int] = mapped_column(default=180)
    min_samples: Mapped[int] = mapped_column(default=10)
    bucket_key: Mapped[str] = mapped_column(String(300))
    sample_count: Mapped[int] = mapped_column(default=0)
    estimated_amount_pln: Mapped[float] = mapped_column(Numeric(18, 4), default=0)
    estimated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    status: Mapped[str] = mapped_column(String(24), default="estimated", index=True)
    reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    actual_amount_pln: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    abs_error_pln: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    ape_pct: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    replaced_by_cost_source: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
