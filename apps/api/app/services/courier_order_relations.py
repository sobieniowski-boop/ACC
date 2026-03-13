from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from app.core.db_connection import connect_acc
from app.services.bl_distribution_cache import ensure_bl_distribution_cache_schema
from app.services.dhl_integration import ensure_dhl_schema
from app.services.sellerboard_history import ensure_sellerboard_history_schema

_DEFAULT_CARRIERS = ("DHL", "GLS")
_DEFAULT_LOOKAHEAD_DAYS = 30
_RELATION_CLASSIFIER_VERSION = "courier_relation_v1"
_REPLACEMENT_KEYWORDS = (
    "replacement",
    "replace",
    "exchange",
    "wymian",
    "reklamac",
    "uszkodz",
    "damage",
    "defect",
)
_RESHIPMENT_KEYWORDS = (
    "reship",
    "resend",
    "ponown",
    "dosyl",
    "doposl",
    "uzupeln",
    "missing",
    "brak",
    "druga paczk",
    "second pack",
    "second parc",
)


def _connect():
    return connect_acc(autocommit=False, timeout=90)


def _month_start(token: str) -> date:
    year_str, month_str = str(token or "").strip().split("-", 1)
    return date(int(year_str), int(month_str), 1)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _default_months() -> list[str]:
    today = date.today()
    out: list[str] = []
    for offset in range(3, 0, -1):
        month_value = today.month - offset
        year_value = today.year
        while month_value <= 0:
            month_value += 12
            year_value -= 1
        out.append(f"{year_value}-{month_value:02d}")
    return out


def _normalize_months(months: list[str] | None) -> list[str]:
    out: list[str] = []
    for raw in (months or _default_months()):
        token = str(raw or "").strip()
        if not token:
            continue
        _month_start(token)
        out.append(token)
    if not out:
        raise ValueError("months list cannot be empty")
    return out


def _normalize_carriers(carriers: list[str] | None) -> list[str]:
    out = [str(item or "").strip().upper() for item in (carriers or list(_DEFAULT_CARRIERS)) if str(item or "").strip()]
    if not out:
        raise ValueError("carriers list cannot be empty")
    for carrier in out:
        if carrier not in {"DHL", "GLS"}:
            raise ValueError(f"Unsupported carrier '{carrier}'")
    return out


def _contains_ci(expression: str, needle: str) -> str:
    return f"CHARINDEX('{needle}', LOWER(ISNULL({expression}, ''))) > 0"


def _distribution_order_carrier_predicate(alias: str, carrier: str) -> str:
    if carrier == "DHL":
        return (
            f"({_contains_ci(f'{alias}.delivery_method', 'dhl')} "
            f"OR {_contains_ci(f'{alias}.delivery_package_module', 'dhl')})"
        )
    return (
        f"({_contains_ci(f'{alias}.delivery_method', 'gls')} "
        f"OR {_contains_ci(f'{alias}.delivery_package_module', 'gls')})"
    )


def _ascii_text(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_only = re.sub(r"[^a-z0-9]+", " ", ascii_only)
    return re.sub(r"\s+", " ", ascii_only).strip()


def _normalize_email(value: Any) -> str:
    return _ascii_text(value)


def _normalize_phone(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) > 9:
        return digits[-9:]
    return digits


def _normalize_name(value: Any) -> str:
    return _ascii_text(value)


def _normalize_country(value: Any) -> str:
    return str(value or "").strip().upper()[:8]


def _month_token_from_date(value: date | None) -> str | None:
    if value is None:
        return None
    return f"{value.year}-{value.month:02d}"


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


@dataclass
class SourceOrder:
    amazon_order_id: str
    acc_order_id: str | None
    purchase_date: date
    distribution_order_ids: set[int] = field(default_factory=set)
    resolved_bl_order_ids: set[int] = field(default_factory=set)
    emails: set[str] = field(default_factory=set)
    phones: set[str] = field(default_factory=set)
    name_countries: set[tuple[str, str]] = field(default_factory=set)
    reference_tokens: set[str] = field(default_factory=set)

    def add_identity(
        self,
        *,
        distribution_order_id: int | None,
        resolved_bl_order_id: int | None,
        email: str,
        phone: str,
        fullname: str,
        country: str,
    ) -> None:
        if distribution_order_id is not None:
            self.distribution_order_ids.add(int(distribution_order_id))
            self.reference_tokens.add(str(int(distribution_order_id)))
        if resolved_bl_order_id is not None:
            self.resolved_bl_order_ids.add(int(resolved_bl_order_id))
            self.reference_tokens.add(str(int(resolved_bl_order_id)))
        if email:
            self.emails.add(email)
        if phone:
            self.phones.add(phone)
        if fullname and country:
            self.name_countries.add((fullname, country))


@dataclass(frozen=True)
class CandidateOrder:
    order_id: int
    resolved_bl_order_id: int
    external_order_id: str | None
    order_source: str | None
    order_source_id: int | None
    order_date: date | None
    country: str
    email: str
    phone: str
    fullname: str
    context_text: str


@dataclass(frozen=True)
class RelationDecision:
    carrier: str
    source_amazon_order_id: str
    source_acc_order_id: str | None
    source_distribution_order_id: int | None
    source_bl_order_id: int | None
    source_purchase_date: date
    related_distribution_order_id: int
    related_bl_order_id: int
    related_external_order_id: str | None
    related_order_source: str | None
    related_order_source_id: int | None
    related_order_date: date | None
    relation_type: str
    detection_method: str
    confidence: float
    is_strong: bool
    evidence: dict[str, Any]


def _keyword_hits(text: str) -> dict[str, list[str]]:
    return {
        "replacement": [keyword for keyword in _REPLACEMENT_KEYWORDS if keyword in text],
        "reshipment": [keyword for keyword in _RESHIPMENT_KEYWORDS if keyword in text],
    }


def _detect_relation(
    *,
    carrier: str,
    source: SourceOrder,
    candidate: CandidateOrder,
    replacement_flags: set[str],
    lookahead_days: int,
) -> RelationDecision | None:
    if candidate.order_id in source.distribution_order_ids:
        return None
    if candidate.resolved_bl_order_id in source.resolved_bl_order_ids:
        return None
    if candidate.external_order_id and candidate.external_order_id == source.amazon_order_id:
        return None
    if candidate.order_date is None:
        return None

    days_delta = (candidate.order_date - source.purchase_date).days
    if days_delta < -2 or days_delta > lookahead_days:
        return None

    same_email = bool(candidate.email and candidate.email in source.emails)
    same_phone = bool(candidate.phone and candidate.phone in source.phones)
    same_name_country = bool(candidate.fullname and candidate.country and (candidate.fullname, candidate.country) in source.name_countries)
    if not (same_email or same_phone or same_name_country):
        return None

    keyword_hits = _keyword_hits(candidate.context_text)
    explicit_replacement = bool(keyword_hits["replacement"])
    explicit_reshipment = bool(keyword_hits["reshipment"])
    mentioned_source = any(token and token in candidate.context_text for token in source.reference_tokens)
    candidate_is_replacement = bool(candidate.external_order_id and candidate.external_order_id in replacement_flags)

    signal_score = 0.0
    signal_parts: list[str] = []
    if same_email:
        signal_score += 0.18
        signal_parts.append("email")
    if same_phone:
        signal_score += 0.16
        signal_parts.append("phone")
    if same_name_country:
        signal_score += 0.12
        signal_parts.append("name_country")
    if days_delta <= 7:
        signal_score += 0.05
    elif days_delta <= 14:
        signal_score += 0.03
    else:
        signal_score += 0.01
    if mentioned_source:
        signal_score += 0.12
        signal_parts.append("source_ref")
    if candidate_is_replacement:
        signal_score += 0.15
        signal_parts.append("sellerboard_replacement")

    if explicit_replacement or candidate_is_replacement:
        relation_type = "replacement_order"
        confidence = 0.58 + signal_score + (0.25 if explicit_replacement else 0.0)
    elif explicit_reshipment or mentioned_source:
        relation_type = "reshipment"
        confidence = 0.56 + signal_score + (0.22 if explicit_reshipment else 0.0)
    else:
        relation_type = "same_customer_follow_up"
        confidence = 0.43 + signal_score

    confidence = min(0.99, round(confidence, 4))
    if relation_type in {"replacement_order", "reshipment"} and confidence < 0.80:
        return None
    if relation_type == "same_customer_follow_up" and confidence < 0.62:
        return None

    is_strong = relation_type in {"replacement_order", "reshipment"} and confidence >= 0.95
    detection_tokens: list[str] = []
    if explicit_replacement:
        detection_tokens.append("replacement_keyword")
    if explicit_reshipment:
        detection_tokens.append("reshipment_keyword")
    detection_tokens.extend(signal_parts)
    if not detection_tokens:
        detection_tokens.append("same_customer")

    source_distribution_order_id = min(source.distribution_order_ids) if source.distribution_order_ids else None
    source_bl_order_id = min(source.resolved_bl_order_ids) if source.resolved_bl_order_ids else None
    return RelationDecision(
        carrier=carrier,
        source_amazon_order_id=source.amazon_order_id,
        source_acc_order_id=source.acc_order_id,
        source_distribution_order_id=source_distribution_order_id,
        source_bl_order_id=source_bl_order_id,
        source_purchase_date=source.purchase_date,
        related_distribution_order_id=candidate.order_id,
        related_bl_order_id=candidate.resolved_bl_order_id,
        related_external_order_id=candidate.external_order_id,
        related_order_source=candidate.order_source,
        related_order_source_id=candidate.order_source_id,
        related_order_date=candidate.order_date,
        relation_type=relation_type,
        detection_method="+".join(detection_tokens)[:64],
        confidence=confidence,
        is_strong=is_strong,
        evidence={
            "classifier_version": _RELATION_CLASSIFIER_VERSION,
            "days_delta": days_delta,
            "matched_signals": {
                "email": same_email,
                "phone": same_phone,
                "name_country": same_name_country,
                "source_reference": mentioned_source,
            },
            "keyword_hits": keyword_hits,
            "candidate_is_replacement_order": candidate_is_replacement,
        },
    )


def _load_source_orders(cur, *, month_start_value: date, month_end_value: date) -> dict[str, SourceOrder]:
    cur.execute(
        """
SELECT
    o.amazon_order_id,
    CAST(o.id AS NVARCHAR(40)) AS acc_order_id,
    CAST(o.purchase_date AS DATE) AS purchase_date,
    dco.order_id,
    COALESCE(dm.holding_order_id, dco.order_id) AS resolved_bl_order_id,
    dco.delivery_country_code,
    dco.delivery_fullname,
    dco.email,
    dco.phone
FROM dbo.acc_order o WITH (NOLOCK)
LEFT JOIN dbo.acc_bl_distribution_order_cache dco WITH (NOLOCK)
  ON dco.external_order_id = o.amazon_order_id
LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.dis_order_id = dco.order_id
WHERE o.fulfillment_channel = 'MFN'
  AND CAST(o.purchase_date AS DATE) >= ?
  AND CAST(o.purchase_date AS DATE) < ?
        """,
        [month_start_value.isoformat(), month_end_value.isoformat()],
    )
    out: dict[str, SourceOrder] = {}
    for row in cur.fetchall():
        amazon_order_id = str(row[0] or "").strip()
        if not amazon_order_id:
            continue
        purchase_date = row[2]
        if purchase_date is None:
            continue
        source = out.get(amazon_order_id)
        if source is None:
            source = SourceOrder(
                amazon_order_id=amazon_order_id,
                acc_order_id=str(row[1]).strip() if row[1] else None,
                purchase_date=purchase_date,
                reference_tokens={_ascii_text(amazon_order_id), amazon_order_id.lower()},
            )
            out[amazon_order_id] = source
        source.add_identity(
            distribution_order_id=int(row[3]) if row[3] is not None else None,
            resolved_bl_order_id=int(row[4]) if row[4] is not None else None,
            email=_normalize_email(row[7]),
            phone=_normalize_phone(row[8]),
            fullname=_normalize_name(row[6]),
            country=_normalize_country(row[5]),
        )
    return out


def _load_candidate_orders(
    cur,
    *,
    carrier: str,
    date_from: date,
    date_to: date,
) -> list[CandidateOrder]:
    carrier_predicate = _distribution_order_carrier_predicate("dco", carrier)
    cur.execute(
        f"""
SELECT
    dco.order_id,
    COALESCE(dm.holding_order_id, dco.order_id) AS resolved_bl_order_id,
    dco.external_order_id,
    dco.order_source,
    dco.order_source_id,
    CAST(COALESCE(dco.date_confirmed, dco.date_add) AS DATE) AS order_date,
    dco.delivery_country_code,
    dco.delivery_fullname,
    dco.email,
    dco.phone,
    dco.admin_comments,
    dco.extra_field_1,
    dco.extra_field_2
FROM dbo.acc_bl_distribution_order_cache dco WITH (NOLOCK)
LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.dis_order_id = dco.order_id
WHERE CAST(COALESCE(dco.date_confirmed, dco.date_add) AS DATE) >= ?
  AND CAST(COALESCE(dco.date_confirmed, dco.date_add) AS DATE) < ?
  AND {carrier_predicate}
        """,
        [date_from.isoformat(), date_to.isoformat()],
    )
    out: list[CandidateOrder] = []
    for row in cur.fetchall():
        order_id = int(row[0] or 0)
        if order_id <= 0:
            continue
        context_text = _ascii_text(" ".join(str(row[idx] or "") for idx in range(10, 13)))
        out.append(
            CandidateOrder(
                order_id=order_id,
                resolved_bl_order_id=int(row[1] or order_id),
                external_order_id=str(row[2]).strip() if row[2] else None,
                order_source=str(row[3]).strip() if row[3] else None,
                order_source_id=int(row[4]) if row[4] is not None else None,
                order_date=row[5],
                country=_normalize_country(row[6]),
                fullname=_normalize_name(row[7]),
                email=_normalize_email(row[8]),
                phone=_normalize_phone(row[9]),
                context_text=context_text,
            )
        )
    return out


def _load_replacement_flags(cur, *, date_from: date, date_to: date) -> set[str]:
    cur.execute(
        """
SELECT amazon_order_id
FROM dbo.acc_sb_order_line_staging WITH (NOLOCK)
WHERE is_replacement_order = 1
  AND CAST(purchase_date AS DATE) >= ?
  AND CAST(purchase_date AS DATE) < ?
GROUP BY amazon_order_id
        """,
        [date_from.isoformat(), date_to.isoformat()],
    )
    return {str(row[0] or "").strip() for row in cur.fetchall() if str(row[0] or "").strip()}


def refresh_courier_order_relations(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
    lookahead_days: int = _DEFAULT_LOOKAHEAD_DAYS,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_dhl_schema()
    ensure_bl_distribution_cache_schema()
    ensure_sellerboard_history_schema()

    from app.connectors.mssql.mssql_store import set_job_progress

    months_norm = _normalize_months(months)
    carriers_norm = _normalize_carriers(carriers)
    lookahead_days_safe = max(1, int(lookahead_days or _DEFAULT_LOOKAHEAD_DAYS))
    total_pairs = max(1, len(months_norm) * len(carriers_norm))

    result: dict[str, Any] = {
        "months": months_norm,
        "carriers": carriers_norm,
        "lookahead_days": lookahead_days_safe,
        "rows_written": 0,
        "rows_deleted": 0,
        "items": [],
        "matrix": {month: {} for month in months_norm},
    }

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET DEADLOCK_PRIORITY LOW")
        cur.execute("SET LOCK_TIMEOUT 10000")

        pair_index = 0
        for month_token in months_norm:
            month_start_value = _month_start(month_token)
            month_end_value = _next_month(month_start_value)
            candidate_window_to = month_end_value + timedelta(days=lookahead_days_safe + 1)
            replacement_window_to = month_end_value + timedelta(days=lookahead_days_safe + 1)
            source_orders = _load_source_orders(cur, month_start_value=month_start_value, month_end_value=month_end_value)
            replacement_flags = _load_replacement_flags(cur, date_from=month_start_value, date_to=replacement_window_to)
            for carrier in carriers_norm:
                pair_index += 1
                candidates = _load_candidate_orders(
                    cur,
                    carrier=carrier,
                    date_from=month_start_value,
                    date_to=candidate_window_to,
                )
                by_email: dict[str, list[CandidateOrder]] = defaultdict(list)
                by_phone: dict[str, list[CandidateOrder]] = defaultdict(list)
                by_name_country: dict[tuple[str, str], list[CandidateOrder]] = defaultdict(list)
                for candidate in candidates:
                    if candidate.email:
                        by_email[candidate.email].append(candidate)
                    if candidate.phone:
                        by_phone[candidate.phone].append(candidate)
                    if candidate.fullname and candidate.country:
                        by_name_country[(candidate.fullname, candidate.country)].append(candidate)

                decisions: dict[tuple[str, str, int, str], RelationDecision] = {}
                for source in source_orders.values():
                    scoped_candidates: dict[int, CandidateOrder] = {}
                    for email in source.emails:
                        for candidate in by_email.get(email, []):
                            scoped_candidates[candidate.order_id] = candidate
                    for phone in source.phones:
                        for candidate in by_phone.get(phone, []):
                            scoped_candidates[candidate.order_id] = candidate
                    for name_country in source.name_countries:
                        for candidate in by_name_country.get(name_country, []):
                            scoped_candidates[candidate.order_id] = candidate

                    for candidate in scoped_candidates.values():
                        decision = _detect_relation(
                            carrier=carrier,
                            source=source,
                            candidate=candidate,
                            replacement_flags=replacement_flags,
                            lookahead_days=lookahead_days_safe,
                        )
                        if decision is None:
                            continue
                        key = (
                            decision.carrier,
                            decision.source_amazon_order_id,
                            decision.related_distribution_order_id,
                            decision.relation_type,
                        )
                        existing = decisions.get(key)
                        if existing is None or decision.confidence > existing.confidence:
                            decisions[key] = decision

                cur.execute(
                    """
DELETE FROM dbo.acc_order_courier_relation
WHERE carrier = ?
  AND source_purchase_date >= ?
  AND source_purchase_date < ?
                    """,
                    [carrier, month_start_value.isoformat(), month_end_value.isoformat()],
                )
                rows_deleted = max(0, int(cur.rowcount or 0))
                rows_to_write = list(decisions.values())
                if rows_to_write:
                    insert_sql = """
INSERT INTO dbo.acc_order_courier_relation (
    id, carrier, source_amazon_order_id, source_acc_order_id,
    source_distribution_order_id, source_bl_order_id, source_purchase_date,
    related_distribution_order_id, related_bl_order_id, related_external_order_id,
    related_order_source, related_order_source_id, related_order_date,
    relation_type, detection_method, confidence, is_strong, evidence_json,
    created_at, updated_at
)
VALUES (
    NEWID(), ?, ?, CASE WHEN ? IS NULL OR ? = '' THEN NULL ELSE CAST(? AS UNIQUEIDENTIFIER) END,
    ?, ?, ?,
    ?, ?, ?,
    ?, ?, ?,
    ?, ?, ?, ?, ?,
    SYSUTCDATETIME(), SYSUTCDATETIME()
)
                    """
                    params = [
                        [
                            row.carrier,
                            row.source_amazon_order_id,
                            row.source_acc_order_id,
                            row.source_acc_order_id,
                            row.source_acc_order_id,
                            row.source_distribution_order_id,
                            row.source_bl_order_id,
                            row.source_purchase_date.isoformat(),
                            row.related_distribution_order_id,
                            row.related_bl_order_id,
                            row.related_external_order_id,
                            row.related_order_source,
                            row.related_order_source_id,
                            row.related_order_date.isoformat() if row.related_order_date else None,
                            row.relation_type,
                            row.detection_method,
                            row.confidence,
                            1 if row.is_strong else 0,
                            json.dumps(row.evidence, ensure_ascii=True),
                        ]
                        for row in rows_to_write
                    ]
                    if hasattr(cur, "fast_executemany"):
                        cur.fast_executemany = True
                    cur.executemany(insert_sql, params)
                    if hasattr(cur, "fast_executemany"):
                        cur.fast_executemany = False

                conn.commit()
                summary = {
                    "month": month_token,
                    "carrier": carrier,
                    "source_orders": len(source_orders),
                    "candidate_orders": len(candidates),
                    "relations_total": len(rows_to_write),
                    "strong_relations": sum(1 for row in rows_to_write if row.is_strong),
                    "replacement_relations": sum(1 for row in rows_to_write if row.relation_type == "replacement_order"),
                    "reshipment_relations": sum(1 for row in rows_to_write if row.relation_type == "reshipment"),
                    "weak_follow_up_relations": sum(1 for row in rows_to_write if row.relation_type == "same_customer_follow_up"),
                }
                result["rows_deleted"] += rows_deleted
                result["rows_written"] += len(rows_to_write)
                result["items"].append(summary)
                result["matrix"][month_token][carrier] = summary

                if job_id:
                    set_job_progress(
                        job_id,
                        progress_pct=min(95, 10 + int((pair_index / total_pairs) * 80)),
                        records_processed=result["rows_written"],
                        message=f"Courier order relations {month_token} {carrier}",
                    )
        return result
    finally:
        conn.close()


def get_courier_order_relations(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
    only_strong: bool = False,
    limit: int = 200,
) -> dict[str, Any]:
    ensure_dhl_schema()

    months_norm = _normalize_months(months)
    carriers_norm = _normalize_carriers(carriers)
    month_starts = [_month_start(token) for token in months_norm]
    date_from = min(month_starts)
    date_to = max(_next_month(item) for item in month_starts)
    carrier_placeholders = ",".join("?" for _ in carriers_norm)

    conn = _connect()
    try:
        cur = conn.cursor()
        sql = f"""
SELECT
    carrier,
    source_amazon_order_id,
    CAST(source_acc_order_id AS NVARCHAR(40)) AS source_acc_order_id,
    source_distribution_order_id,
    source_bl_order_id,
    source_purchase_date,
    related_distribution_order_id,
    related_bl_order_id,
    related_external_order_id,
    related_order_source,
    related_order_source_id,
    related_order_date,
    relation_type,
    detection_method,
    CAST(confidence AS FLOAT) AS confidence,
    is_strong,
    evidence_json
FROM dbo.acc_order_courier_relation WITH (NOLOCK)
WHERE source_purchase_date >= ?
  AND source_purchase_date < ?
  AND carrier IN ({carrier_placeholders})
        """
        params: list[Any] = [date_from.isoformat(), date_to.isoformat(), *carriers_norm]
        if only_strong:
            sql += "\n  AND is_strong = 1"
        sql += "\nORDER BY source_purchase_date ASC, carrier ASC, confidence DESC, source_amazon_order_id ASC"
        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    matrix: dict[str, dict[str, Any]] = {month: {} for month in months_norm}
    for row in rows:
        month_token = _month_token_from_date(row[5])
        if month_token not in matrix:
            continue
        try:
            evidence = json.loads(row[16]) if row[16] else {}
        except Exception:
            evidence = {}
        item = {
            "month": month_token,
            "carrier": str(row[0]),
            "source_amazon_order_id": str(row[1]),
            "source_acc_order_id": str(row[2]) if row[2] else None,
            "source_distribution_order_id": int(row[3]) if row[3] is not None else None,
            "source_bl_order_id": int(row[4]) if row[4] is not None else None,
            "source_purchase_date": str(row[5]),
            "related_distribution_order_id": int(row[6]),
            "related_bl_order_id": int(row[7]) if row[7] is not None else None,
            "related_external_order_id": str(row[8]) if row[8] else None,
            "related_order_source": str(row[9]) if row[9] else None,
            "related_order_source_id": int(row[10]) if row[10] is not None else None,
            "related_order_date": str(row[11]) if row[11] else None,
            "relation_type": str(row[12]),
            "detection_method": str(row[13]),
            "confidence": round(_to_float(row[14]), 4),
            "is_strong": bool(row[15]),
            "evidence": evidence if isinstance(evidence, dict) else {},
        }
        items.append(item)

    summary_counts: dict[tuple[str, str], dict[str, Any]] = {}
    for item in items:
        key = (item["month"], item["carrier"])
        bucket = summary_counts.setdefault(
            key,
            {
                "relations_total": 0,
                "strong_relations": 0,
                "replacement_relations": 0,
                "reshipment_relations": 0,
                "weak_follow_up_relations": 0,
            },
        )
        bucket["relations_total"] += 1
        if item["is_strong"]:
            bucket["strong_relations"] += 1
        if item["relation_type"] == "replacement_order":
            bucket["replacement_relations"] += 1
        elif item["relation_type"] == "reshipment":
            bucket["reshipment_relations"] += 1
        elif item["relation_type"] == "same_customer_follow_up":
            bucket["weak_follow_up_relations"] += 1

    summary_items: list[dict[str, Any]] = []
    for month in months_norm:
        for carrier in carriers_norm:
            summary = {
                "month": month,
                "carrier": carrier,
                **summary_counts.get(
                    (month, carrier),
                    {
                        "relations_total": 0,
                        "strong_relations": 0,
                        "replacement_relations": 0,
                        "reshipment_relations": 0,
                        "weak_follow_up_relations": 0,
                    },
                ),
            }
            matrix[month][carrier] = summary
            summary_items.append(summary)

    return {
        "months": months_norm,
        "carriers": carriers_norm,
        "rows": len(items),
        "summary": summary_items,
        "items": items[: max(1, int(limit or 1))],
        "matrix": matrix,
    }
