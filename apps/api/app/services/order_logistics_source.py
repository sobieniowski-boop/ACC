from __future__ import annotations

from typing import Any

from sqlalchemy import Numeric, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.shipment import AccOrderLogisticsFact


def profit_uses_logistics_fact() -> bool:
    return bool(getattr(settings, "PROFIT_USE_LOGISTICS_FACT", False))


def profit_logistics_join_sql(*, order_alias: str = "o", fact_alias: str = "olf") -> str:
    """Return SQL fragment that yields exactly one logistics-fact row per order.

    Uses OUTER APPLY (SELECT TOP 1 ... ORDER BY calculated_at DESC) to avoid
    row multiplication when multiple calc_version rows exist for the same
    amazon_order_id (e.g. dhl_v1 + gls_v1).
    """
    if not profit_uses_logistics_fact():
        return ""
    return (
        f"OUTER APPLY (\n"
        f"    SELECT TOP 1 olf_inner.total_logistics_pln\n"
        f"    FROM dbo.acc_order_logistics_fact olf_inner WITH (NOLOCK)\n"
        f"    WHERE olf_inner.amazon_order_id = {order_alias}.amazon_order_id\n"
        f"    ORDER BY olf_inner.calculated_at DESC\n"
        f") {fact_alias}"
    )


def profit_logistics_value_sql(*, order_alias: str = "o", fact_alias: str = "olf") -> str:
    legacy_expr = f"CAST(ISNULL({order_alias}.logistics_pln, 0) AS FLOAT)"
    if not profit_uses_logistics_fact():
        return legacy_expr
    return (
        f"ISNULL(CAST({fact_alias}.total_logistics_pln AS FLOAT), {legacy_expr})"
    )


def _logistics_dedup_subquery(*, order_model: Any) -> Any:
    """Build a SQLAlchemy subquery that picks the latest logistics fact per order.

    This deduplicates acc_order_logistics_fact rows where the same
    amazon_order_id has multiple calc_version entries.
    """
    from sqlalchemy import literal_column
    from sqlalchemy.sql import expression as sa_expr

    return (
        select(
            AccOrderLogisticsFact.amazon_order_id,
            AccOrderLogisticsFact.total_logistics_pln,
        )
        .distinct(AccOrderLogisticsFact.amazon_order_id)
        .where(AccOrderLogisticsFact.amazon_order_id == order_model.amazon_order_id)
        .order_by(
            AccOrderLogisticsFact.amazon_order_id,
            AccOrderLogisticsFact.calculated_at.desc(),
        )
        .correlate(order_model)
        .limit(1)
        .lateral("olf_latest")
    )


def profit_logistics_join_sqla(statement: Any, *, order_model: Any, fact_model: Any | None = None) -> Any:
    """Join the latest logistics fact per order (1 row max) via a lateral subquery.

    When a dedicated *fact_model* alias is provided (kpi.py pattern), we fall
    back to a ROW_NUMBER-based dedup subquery joined on amazon_order_id so
    that the caller's alias columns remain accessible.
    """
    if not profit_uses_logistics_fact():
        return statement
    # Build a deduplication subquery (ROW_NUMBER) that the caller can reference.
    dedup = (
        select(
            AccOrderLogisticsFact.amazon_order_id.label("amazon_order_id"),
            AccOrderLogisticsFact.total_logistics_pln.label("total_logistics_pln"),
            func.row_number()
            .over(
                partition_by=AccOrderLogisticsFact.amazon_order_id,
                order_by=AccOrderLogisticsFact.calculated_at.desc(),
            )
            .label("_rn"),
        )
    ).subquery("_olf_dedup")

    latest = (
        select(
            dedup.c.amazon_order_id,
            dedup.c.total_logistics_pln,
        )
        .where(dedup.c._rn == 1)
    ).subquery("_olf_latest")

    if fact_model is not None:
        # The caller created an aliased(AccOrderLogisticsFact) and references
        # its columns (e.g. fact_model.total_logistics_pln) in expressions built
        # *before* calling this function.  We cannot swap the alias out, so we
        # join the dedup subquery aliased under the same name.
        from sqlalchemy.orm import aliased as sa_aliased

        # Re-alias the subquery with the same name to keep column references.
        return statement.outerjoin(
            latest,
            latest.c.amazon_order_id == order_model.amazon_order_id,
        )

    return statement.outerjoin(
        latest,
        latest.c.amazon_order_id == order_model.amazon_order_id,
    )


def profit_logistics_value_sqla(*, order_model: Any, fact_model: Any | None = None) -> Any:
    """Return a scalar expression for the logistics PLN value.

    When *fact_model* is provided the caller **must** also call
    ``profit_logistics_join_sqla`` with the same *fact_model* to attach the
    ``_olf_latest`` subquery.  In that case we return a column reference to
    the already-joined subquery (safe inside ``SUM`` / ``GROUP BY``).

    When *fact_model* is ``None`` we use a correlated scalar subquery — fine
    for row-level access but **not** inside aggregate functions on SQL Server.
    """
    from sqlalchemy import literal_column

    legacy_expr = func.coalesce(order_model.logistics_pln, cast(0, Numeric(18, 4)))
    if not profit_uses_logistics_fact():
        return legacy_expr

    if fact_model is not None:
        # Reference the column from the _olf_latest subquery built by
        # profit_logistics_join_sqla — avoids correlated subquery inside SUM.
        joined_col = literal_column("_olf_latest.total_logistics_pln")
        return func.coalesce(cast(joined_col, Numeric(18, 4)), legacy_expr)

    # Fallback: correlated scalar subquery (row-level only).
    latest_val = (
        select(AccOrderLogisticsFact.total_logistics_pln)
        .where(AccOrderLogisticsFact.amazon_order_id == order_model.amazon_order_id)
        .order_by(AccOrderLogisticsFact.calculated_at.desc())
        .limit(1)
        .correlate(order_model)
        .scalar_subquery()
    )
    return func.coalesce(cast(latest_val, Numeric(18, 4)), legacy_expr)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


async def resolve_profit_logistics_pln(
    db: AsyncSession,
    *,
    amazon_order_id: str | None,
    legacy_logistics_pln: Any,
) -> float:
    legacy_value = _to_float(legacy_logistics_pln)
    if not profit_uses_logistics_fact() or not amazon_order_id:
        return legacy_value

    result = await db.execute(
        select(AccOrderLogisticsFact.total_logistics_pln)
        .where(AccOrderLogisticsFact.amazon_order_id == amazon_order_id)
        .order_by(AccOrderLogisticsFact.calculated_at.desc())
        .limit(1)
    )
    fact_value = result.scalar_one_or_none()
    if fact_value is None:
        return legacy_value
    return _to_float(fact_value, legacy_value)
