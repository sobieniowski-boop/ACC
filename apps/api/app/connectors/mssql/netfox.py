"""
MSSQL NetfoxAnalityka connector — reads cost / ERP data.

Column-name mapping is centralised in SCHEMA at the top of this file.
Run scripts/discover_mssql_schema.py to inspect the actual DB schema
and update the mappings below if they differ.

Common Polish ERP naming (Comarch XL / Optima based on Netfox):
  Kartoteki         — product master (Symbol=SKU, EAN, CenaZakupu)
  Stany             — stock levels   (Stan=qty)
  Zamowienia        — purchase order headers
  ZamowieniaPozycje — PO lines
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pyodbc
import pandas as pd
import structlog

from app.core.config import settings
from app.core.db_connection import connect_netfox

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# SCHEMA MAPPING — edit these constants to match actual NetfoxAnalityka schema.
# Run scripts/discover_mssql_schema.py to discover column names.
# ---------------------------------------------------------------------------
@dataclass
class _SchemaMap:
    # dbo.Kartoteki (product master)
    tbl_products: str = "dbo.Kartoteki"
    col_sku: str = "Symbol"
    col_ean: str = "EAN"
    col_purchase_price: str = "CenaZakupu"
    col_active_flag: str = "Aktywny"
    col_product_id: str = "Id"
    col_product_name: str = "Nazwa"

    # dbo.Stany (stock levels)
    tbl_stock: str = "dbo.Stany"
    col_stock_qty: str = "Stan"
    col_stock_warehouse_id: str = "MagazynId"
    col_stock_product_fk: str = "KartotekaId"

    # dbo.Zamowienia (PO headers)
    tbl_po_header: str = "dbo.Zamowienia"
    col_po_id: str = "Id"
    col_po_order_date: str = "DataZamowienia"
    col_po_delivery_date: str = "DataDostawy"
    col_po_status: str = "Status"
    po_status_cancelled: str = "'Anulowane'"
    po_status_done: str = "'Zrealizowane'"

    # dbo.ZamowieniaPozycje (PO lines)
    tbl_po_lines: str = "dbo.ZamowieniaPozycje"
    col_po_line_po_fk: str = "ZamowienieId"
    col_po_line_product_fk: str = "KartotekaId"
    col_po_line_qty: str = "Ilosc"
    col_po_line_qty_received: str = "IloscZrealizowana"


SCHEMA = _SchemaMap()
# ---------------------------------------------------------------------------


def _get_conn() -> pyodbc.Connection:
    log.debug("mssql.connect.netfox", server=settings.NETFOX_MSSQL_SERVER or settings.MSSQL_SERVER)
    return connect_netfox(autocommit=False, timeout=15)


def query_df(sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
    """Execute a SELECT and return a DataFrame. Uses synchronous pyodbc."""
    conn = _get_conn()
    try:
        df = pd.read_sql(sql, conn, params=params or ())
        log.debug("mssql.query_ok", rows=len(df))
        return df
    except pyodbc.Error as exc:
        log.error("mssql.query_error", error=str(exc), sql=sql[:200])
        raise
    finally:
        conn.close()


def test_connection() -> bool:
    """Quick connectivity check — returns True if MSSQL is reachable."""
    try:
        _get_conn().close()
        return True
    except Exception as exc:
        log.warning("mssql.unreachable", error=str(exc))
        return False


# ---------------------------------------------------------------------------
# Public query helpers
# ---------------------------------------------------------------------------

def get_product_costs(skus: Optional[list[str]] = None) -> pd.DataFrame:
    """
    Return product purchase costs from ERP.

    Returns DataFrame with columns:
        sku, ean, product_name, netto_purchase_price_pln, vat_rate
    """
    s = SCHEMA
    base_sql = f"""
        SELECT
            k.{s.col_sku}            AS sku,
            k.{s.col_ean}            AS ean,
            k.{s.col_product_name}   AS product_name,
            k.{s.col_purchase_price} AS netto_purchase_price_pln,
            23.0                     AS vat_rate
        FROM {s.tbl_products} k
        WHERE k.{s.col_active_flag} = 1
    """
    if skus:
        placeholders = ",".join("?" for _ in skus)
        base_sql += f" AND k.{s.col_sku} IN ({placeholders})"
        return query_df(base_sql, tuple(skus))
    return query_df(base_sql)


def get_warehouse_stock(
    skus: Optional[list[str]] = None,
    warehouse_ids: Optional[list[int]] = None,
) -> pd.DataFrame:
    """
    Return warehouse stock levels from ERP.

    Returns DataFrame with columns:
        sku, product_name, qty_on_hand
    """
    s = SCHEMA
    where_clauses: list[str] = []
    params: list[Any] = []

    if warehouse_ids:
        ph = ",".join("?" for _ in warehouse_ids)
        where_clauses.append(f"s.{s.col_stock_warehouse_id} IN ({ph})")
        params.extend(warehouse_ids)

    if skus:
        ph = ",".join("?" for _ in skus)
        where_clauses.append(f"k.{s.col_sku} IN ({ph})")
        params.extend(skus)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    sql = f"""
        SELECT
            k.{s.col_sku}            AS sku,
            k.{s.col_product_name}   AS product_name,
            SUM(s.{s.col_stock_qty}) AS qty_on_hand
        FROM {s.tbl_stock} s
        JOIN {s.tbl_products} k
            ON k.{s.col_product_id} = s.{s.col_stock_product_fk}
        {where_sql}
        GROUP BY k.{s.col_sku}, k.{s.col_product_name}
    """
    return query_df(sql, tuple(params) if params else None)


def get_open_purchase_orders(skus: Optional[list[str]] = None) -> pd.DataFrame:
    """
    Return open PO lines from ERP for reorder planning.

    Returns DataFrame with columns:
        sku, product_name, order_date, expected_delivery,
        qty_ordered, qty_received, qty_open
    """
    s = SCHEMA
    sku_filter = ""
    params: tuple = ()
    if skus:
        ph = ",".join("?" for _ in skus)
        sku_filter = f" AND k.{s.col_sku} IN ({ph})"
        params = tuple(skus)

    sql = f"""
        SELECT
            k.{s.col_sku}                       AS sku,
            k.{s.col_product_name}              AS product_name,
            z.{s.col_po_order_date}             AS order_date,
            z.{s.col_po_delivery_date}          AS expected_delivery,
            zp.{s.col_po_line_qty}              AS qty_ordered,
            zp.{s.col_po_line_qty_received}     AS qty_received,
            (zp.{s.col_po_line_qty} - zp.{s.col_po_line_qty_received}) AS qty_open
        FROM {s.tbl_po_lines} zp
        JOIN {s.tbl_po_header} z
            ON z.{s.col_po_id} = zp.{s.col_po_line_po_fk}
        JOIN {s.tbl_products} k
            ON k.{s.col_product_id} = zp.{s.col_po_line_product_fk}
        WHERE z.{s.col_po_status} NOT IN (
            {s.po_status_cancelled}, {s.po_status_done}
        )
          AND zp.{s.col_po_line_qty_received} < zp.{s.col_po_line_qty}
          {sku_filter}
        ORDER BY z.{s.col_po_delivery_date}
    """
    return query_df(sql, params if params else None)


def get_products_with_stock(skus: Optional[list[str]] = None) -> pd.DataFrame:
    """
    Join product costs + stock in a single call.
    Used by profit service to enrich orders with COGS and on-hand qty.
    """
    costs = get_product_costs(skus)
    stock = get_warehouse_stock(skus)
    if costs.empty:
        return pd.DataFrame(
            columns=["sku", "ean", "product_name",
                     "netto_purchase_price_pln", "vat_rate", "qty_on_hand"]
        )
    merged = costs.merge(stock[["sku", "qty_on_hand"]], on="sku", how="left")
    merged["qty_on_hand"] = merged["qty_on_hand"].fillna(0).astype(int)
    return merged
