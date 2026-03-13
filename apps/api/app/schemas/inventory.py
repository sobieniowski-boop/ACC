"""Pydantic schemas — Inventory module."""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict


class InventorySnapshotOut(BaseModel):
    id: int
    snapshot_date: date
    marketplace_id: str
    marketplace_code: str
    sku: str
    asin: Optional[str] = None
    product_name: Optional[str] = None
    qty_fulfillable: int
    qty_reserved: int
    qty_inbound: int
    qty_unfulfillable: int
    qty_total: int
    days_of_inventory: Optional[int] = None   # DOI
    velocity_30d: Optional[float] = None      # units/day
    reorder_point: Optional[int] = None
    inventory_value_pln: Optional[float] = None
    status: str                               # ok | low | critical | overstock

    model_config = ConfigDict(from_attributes=True)


class InventoryListResponse(BaseModel):
    items: list[InventorySnapshotOut]
    total: int
    page: int
    page_size: int
    summary: "InventorySummary"


class InventorySummary(BaseModel):
    total_skus: int
    critical_count: int        # DOI < 7
    low_count: int             # DOI 7-14
    overstock_count: int       # DOI > 90
    total_value_pln: float
    avg_doi: float


class OpenPOOut(BaseModel):
    sku: str
    product_name: Optional[str] = None
    order_date: Optional[date] = None
    expected_delivery: Optional[date] = None
    qty_ordered: int
    qty_received: int
    qty_open: int
    days_until_delivery: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


class ReorderSuggestionOut(BaseModel):
    """Auto-generated reorder suggestion."""
    sku: str
    product_name: Optional[str] = None
    current_doi: int
    velocity_30d: float
    suggested_qty: int
    suggested_order_date: date
    urgency: str             # critical | high | medium | low
    reason: str
