"""Content Ops - Amazon policy checks, rules, listing health, impact analytics."""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pyodbc

from app.connectors.mssql.mssql_store import ensure_v2_schema
from app.core.config import settings
from ._helpers import (
    _connect, _fetchall_dict, _json_load,
    _normalize_policy_severity,
    _normalize_version_status,
)


def policy_check(*, version_id: str):
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 id, fields_json
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (version_id,),
        )
        version_rows = _fetchall_dict(cur)
        if not version_rows:
            raise ValueError("version not found")

        fields_payload = _json_load(version_rows[0].get("fields_json"))

        cur.execute(
            """
            SELECT id, name, pattern, severity, applies_to_json
            FROM dbo.acc_co_policy_rules WITH (NOLOCK)
            WHERE is_active = 1
            ORDER BY created_at ASC
            """
        )
        rules = _fetchall_dict(cur)

        findings: list[dict[str, Any]] = []
        critical_count = 0
        major_count = 0
        minor_count = 0

        for rule in rules:
            severity = _normalize_policy_severity(str(rule.get("severity") or "minor"))
            pattern = str(rule.get("pattern") or "").strip()
            if not pattern:
                continue
            try:
                regex = re.compile(pattern, flags=re.IGNORECASE)
            except re.error:
                # Skip invalid pattern in checker run; rule should be fixed via rules endpoint.
                continue

            applies_to = _json_load(rule.get("applies_to_json"))
            target_fields = _extract_rule_fields(applies_to)
            for field_name in target_fields:
                for text_value in _collect_field_texts(fields_payload, field_name):
                    match = regex.search(text_value)
                    if not match:
                        continue
                    snippet_start = max(0, match.start() - 20)
                    snippet_end = min(len(text_value), match.end() + 20)
                    findings.append(
                        {
                            "rule_id": str(rule.get("id")),
                            "rule_name": rule.get("name"),
                            "severity": severity,
                            "field": field_name,
                            "message": f"Rule matched: {rule.get('name')}",
                            "snippet": text_value[snippet_start:snippet_end],
                        }
                    )
                    if severity == "critical":
                        critical_count += 1
                    elif severity == "major":
                        major_count += 1
                    else:
                        minor_count += 1

        passed = critical_count == 0
        check_id = str(uuid.uuid4())
        results_payload = {
            "critical_count": critical_count,
            "major_count": major_count,
            "minor_count": minor_count,
            "findings": findings,
        }

        cur.execute(
            """
            INSERT INTO dbo.acc_co_policy_checks
                (id, version_id, results_json, passed, checker_version)
            VALUES
                (?, CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?)
            """,
            (
                check_id,
                version_id,
                json.dumps(results_payload, ensure_ascii=True),
                1 if passed else 0,
                "policy-lint-v1",
            ),
        )
        conn.commit()

        cur.execute(
            """
            SELECT TOP 1 checked_at
            FROM dbo.acc_co_policy_checks WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (check_id,),
        )
        checked_row = cur.fetchone()
        checked_at = checked_row[0] if checked_row else None
        return {
            "version_id": version_id,
            "passed": passed,
            "critical_count": critical_count,
            "major_count": major_count,
            "minor_count": minor_count,
            "findings": findings,
            "checked_at": checked_at,
            "checker_version": "policy-lint-v1",
        }
    finally:
        conn.close()


def list_policy_rules():
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id, name, pattern, severity, applies_to_json, is_active, created_by, created_at
            FROM dbo.acc_co_policy_rules WITH (NOLOCK)
            ORDER BY created_at DESC
            """
        )
        rows = _fetchall_dict(cur)
        return [
            {
                "id": str(r["id"]),
                "name": r["name"],
                "pattern": r["pattern"],
                "severity": _normalize_policy_severity(str(r.get("severity") or "minor")),
                "applies_to_json": _json_load(r.get("applies_to_json")),
                "is_active": bool(r.get("is_active")),
                "created_by": r.get("created_by"),
                "created_at": r.get("created_at"),
            }
            for r in rows
        ]
    finally:
        conn.close()


def upsert_policy_rules(*, payload: dict):
    ensure_v2_schema()
    rules = payload.get("rules")
    if not isinstance(rules, list):
        raise ValueError("rules must be a list")

    conn = _connect()
    try:
        cur = conn.cursor()
        for rule in rules:
            if not isinstance(rule, dict):
                raise ValueError("each rule must be an object")
            name = str(rule.get("name") or "").strip()
            pattern = str(rule.get("pattern") or "").strip()
            if not name:
                raise ValueError("rule name is required")
            if not pattern:
                raise ValueError("rule pattern is required")
            severity = _normalize_policy_severity(str(rule.get("severity") or "minor"))
            applies_to = rule.get("applies_to_json") or {}
            is_active = bool(rule.get("is_active", True))
            rule_id = (rule.get("id") or "").strip()

            if rule_id:
                cur.execute(
                    """
                    UPDATE dbo.acc_co_policy_rules
                    SET name = ?,
                        pattern = ?,
                        severity = ?,
                        applies_to_json = ?,
                        is_active = ?
                    WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                    """,
                    (
                        name,
                        pattern,
                        severity,
                        json.dumps(applies_to, ensure_ascii=True),
                        1 if is_active else 0,
                        rule_id,
                    ),
                )
                if cur.rowcount == 0:
                    raise ValueError("policy rule not found")
            else:
                cur.execute(
                    """
                    INSERT INTO dbo.acc_co_policy_rules
                        (id, name, pattern, severity, applies_to_json, is_active, created_by)
                    VALUES
                        (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        name,
                        pattern,
                        severity,
                        json.dumps(applies_to, ensure_ascii=True),
                        1 if is_active else 0,
                        settings.DEFAULT_ACTOR,
                    ),
                )

        conn.commit()
        return list_policy_rules()
    finally:
        conn.close()


def _severity_counts(results_json: Any) -> tuple[int, int, int]:
    data = _json_load(results_json)
    critical = int(data.get("critical_count") or 0)
    major = int(data.get("major_count") or 0)
    minor = int(data.get("minor_count") or 0)
    if critical or major or minor:
        return critical, major, minor
    findings = data.get("findings")
    if isinstance(findings, list):
        for item in findings:
            if not isinstance(item, dict):
                continue
            sev = str(item.get("severity") or "").lower()
            if sev == "critical":
                critical += 1
            elif sev == "major":
                major += 1
            elif sev == "minor":
                minor += 1
    return critical, major, minor


def list_compliance_queue(*, severity: str | None = "critical", page: int = 1, page_size: int = 50):
    ensure_v2_schema()
    safe_page_size = max(1, min(page_size, 200))
    safe_page = max(1, page)
    offset = (safe_page - 1) * safe_page_size
    threshold = str(severity or "critical").strip().lower()
    if threshold not in {"critical", "major", "minor"}:
        threshold = "critical"

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            WITH latest AS (
                SELECT
                    pc.version_id,
                    pc.results_json,
                    pc.checked_at,
                    ROW_NUMBER() OVER (PARTITION BY pc.version_id ORDER BY pc.checked_at DESC) AS rn
                FROM dbo.acc_co_policy_checks pc WITH (NOLOCK)
            )
            SELECT
                CAST(v.id AS NVARCHAR(40)) AS version_id,
                v.sku,
                v.marketplace_id,
                v.version_no,
                v.status,
                l.results_json,
                l.checked_at
            FROM latest l
            JOIN dbo.acc_co_versions v WITH (NOLOCK) ON v.id = l.version_id
            WHERE l.rn = 1
            ORDER BY l.checked_at DESC
            """
        )
        rows = _fetchall_dict(cur)
        items: list[dict[str, Any]] = []
        for row in rows:
            critical, major, minor = _severity_counts(row.get("results_json"))
            include = (
                (threshold == "critical" and critical > 0)
                or (threshold == "major" and (critical > 0 or major > 0))
                or (threshold == "minor" and (critical > 0 or major > 0 or minor > 0))
            )
            if not include:
                continue
            parsed = _json_load(row.get("results_json"))
            findings = parsed.get("findings") if isinstance(parsed.get("findings"), list) else []
            items.append(
                {
                    "version_id": str(row.get("version_id") or ""),
                    "sku": row.get("sku"),
                    "marketplace_id": row.get("marketplace_id"),
                    "version_no": int(row.get("version_no") or 0),
                    "version_status": str(row.get("status") or "draft"),
                    "critical_count": critical,
                    "major_count": major,
                    "minor_count": minor,
                    "findings": findings,
                    "checked_at": row.get("checked_at"),
                }
            )

        total = len(items)
        pages = math.ceil(total / safe_page_size) if total else 0
        sliced = items[offset: offset + safe_page_size]
        return {"total": total, "page": safe_page, "page_size": safe_page_size, "pages": pages, "items": sliced}
    finally:
        conn.close()


def _impact_aggregate(cur: pyodbc.Cursor, *, sku: str, marketplace_id: str, from_date: date, to_date: date) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
            SUM(ISNULL(quantity, 0)) AS units,
            SUM(ISNULL(revenue_net, 0)) AS revenue,
            SUM(ISNULL(cogs, 0)) AS cogs,
            SUM(ISNULL(transport, 0)) AS transport
        FROM dbo.acc_al_profit_snapshot WITH (NOLOCK)
        WHERE sku = ?
          AND channel = ?
          AND sales_date >= ?
          AND sales_date <= ?
        """,
        (sku, marketplace_id, from_date, to_date),
    )
    row = cur.fetchone()
    units = int(float(row[0] or 0)) if row else 0
    revenue = float(row[1] or 0) if row else 0.0
    cogs = float(row[2] or 0) if row else 0.0
    transport = float(row[3] or 0) if row else 0.0
    return {
        "units": units,
        "revenue": round(revenue, 2),
        "impact_margin_pln": round(revenue - cogs - transport, 2),
        "refunds": 0.0,
        "return_rate": 0.0,
        "sessions": None,
        "cvr": None,
    }


def _impact_daily_map(
    cur: pyodbc.Cursor,
    *,
    sku: str,
    marketplace_id: str,
    from_date: date,
    to_date: date,
) -> dict[date, dict[str, float]]:
    cur.execute(
        """
        SELECT
            sales_date,
            SUM(ISNULL(quantity, 0)) AS units,
            SUM(ISNULL(revenue_net, 0)) AS revenue,
            SUM(ISNULL(cogs, 0)) AS cogs,
            SUM(ISNULL(transport, 0)) AS transport
        FROM dbo.acc_al_profit_snapshot WITH (NOLOCK)
        WHERE sku = ?
          AND channel = ?
          AND sales_date >= ?
          AND sales_date <= ?
        GROUP BY sales_date
        """,
        (sku, marketplace_id, from_date, to_date),
    )
    rows = cur.fetchall()
    out: dict[date, dict[str, float]] = {}
    for row in rows:
        day = row[0]
        units = float(row[1] or 0)
        revenue = float(row[2] or 0)
        cogs = float(row[3] or 0)
        transport = float(row[4] or 0)
        out[day] = {
            "units": units,
            "revenue": revenue,
            "impact_margin_pln": revenue - cogs - transport,
        }
    return out


def _impact_sum_period(
    daily_map: dict[date, dict[str, float]],
    *,
    from_date: date,
    to_date: date,
) -> dict[str, Any]:
    units = 0.0
    revenue = 0.0
    cm1 = 0.0
    day = from_date
    while day <= to_date:
        m = daily_map.get(day)
        if m:
            units += float(m.get("units") or 0)
            revenue += float(m.get("revenue") or 0)
            cm1 += float(m.get("impact_margin_pln") or 0)
        day += timedelta(days=1)
    return {
        "units": int(round(units)),
        "revenue": round(revenue, 2),
        "impact_margin_pln": round(cm1, 2),
        "refunds": 0.0,
        "return_rate": 0.0,
        "sessions": None,
        "cvr": None,
    }


def _impact_optional_metrics_aggregate(
    cur: pyodbc.Cursor,
    *,
    sku: str,
    marketplace_id: str,
    from_date: date,
    to_date: date,
) -> dict[str, Any]:
    sessions = None
    units_ordered = None
    refunds = None
    return_rate = None
    cvr = None

    try:
        cur.execute(
            """
            IF OBJECT_ID('dbo.acc_listing_traffic_daily', 'U') IS NOT NULL
                SELECT
                    SUM(ISNULL(sessions, 0)) AS sessions,
                    SUM(ISNULL(units_ordered, 0)) AS units_ordered
                FROM dbo.acc_listing_traffic_daily WITH (NOLOCK)
                WHERE sku = ?
                  AND marketplace_id = ?
                  AND report_date >= ?
                  AND report_date <= ?
            ELSE
                SELECT CAST(NULL AS FLOAT) AS sessions, CAST(NULL AS FLOAT) AS units_ordered
            """,
            (sku, marketplace_id, from_date, to_date),
        )
        row = cur.fetchone()
        if row:
            sessions = float(row[0]) if row[0] is not None else None
            units_ordered = float(row[1]) if row[1] is not None else None
    except Exception:
        sessions = None
        units_ordered = None

    try:
        cur.execute(
            """
            IF OBJECT_ID('dbo.acc_returns_reason_daily', 'U') IS NOT NULL
                SELECT
                    SUM(ISNULL(refund_amount, 0)) AS refunds,
                    SUM(ISNULL(return_units, 0)) AS return_units,
                    SUM(ISNULL(shipped_units, 0)) AS shipped_units
                FROM dbo.acc_returns_reason_daily WITH (NOLOCK)
                WHERE sku = ?
                  AND marketplace_id = ?
                  AND report_date >= ?
                  AND report_date <= ?
            ELSE
                SELECT CAST(NULL AS FLOAT) AS refunds, CAST(NULL AS FLOAT) AS return_units, CAST(NULL AS FLOAT) AS shipped_units
            """,
            (sku, marketplace_id, from_date, to_date),
        )
        row = cur.fetchone()
        if row:
            refunds = float(row[0]) if row[0] is not None else None
            return_units = float(row[1]) if row[1] is not None else None
            shipped_units = float(row[2]) if row[2] is not None else None
            if return_units is not None and shipped_units and shipped_units > 0:
                return_rate = round(return_units / shipped_units, 4)
    except Exception:
        refunds = None
        return_rate = None

    if sessions is not None and sessions > 0 and units_ordered is not None:
        cvr = round(units_ordered / sessions, 4)

    return {
        "sessions": int(round(sessions)) if sessions is not None else None,
        "cvr": cvr,
        "refunds": round(float(refunds or 0), 2) if refunds is not None else 0.0,
        "return_rate": float(return_rate or 0),
    }


def get_content_impact(*, sku: str, marketplace: str, range_days: int = 14):
    ensure_v2_schema()
    sku_value = str(sku or "").strip()
    if not sku_value:
        raise ValueError("sku is required")
    market_id = _marketplace_to_id(str(marketplace or "").strip().upper())
    if not market_id:
        raise ValueError("marketplace is required")
    safe_range = max(7, min(int(range_days or 14), 90))

    now = datetime.now(timezone.utc)
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 published_at
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE sku = ?
              AND marketplace_id = ?
              AND published_at IS NOT NULL
            ORDER BY published_at DESC
            """,
            (sku_value, market_id),
        )
        row = cur.fetchone()
        pivot = row[0].date() if row and row[0] else now.date()
        after_from = max(pivot - timedelta(days=safe_range - 1), now.date() - timedelta(days=safe_range - 1))
        after_to = now.date()
        before_to = after_from - timedelta(days=1)
        before_from = before_to - timedelta(days=safe_range - 1)

        data_from = before_from - timedelta(days=56)
        daily_map = _impact_daily_map(
            cur,
            sku=sku_value,
            marketplace_id=market_id,
            from_date=data_from,
            to_date=after_to,
        )
        before = _impact_sum_period(daily_map, from_date=before_from, to_date=before_to)
        after = _impact_sum_period(daily_map, from_date=after_from, to_date=after_to)
        before_opt = _impact_optional_metrics_aggregate(
            cur,
            sku=sku_value,
            marketplace_id=market_id,
            from_date=before_from,
            to_date=before_to,
        )
        after_opt = _impact_optional_metrics_aggregate(
            cur,
            sku=sku_value,
            marketplace_id=market_id,
            from_date=after_from,
            to_date=after_to,
        )
        before["sessions"] = before_opt["sessions"]
        before["cvr"] = before_opt["cvr"]
        before["refunds"] = before_opt["refunds"]
        before["return_rate"] = before_opt["return_rate"]
        after["sessions"] = after_opt["sessions"]
        after["cvr"] = after_opt["cvr"]
        after["refunds"] = after_opt["refunds"]
        after["return_rate"] = after_opt["return_rate"]
    finally:
        conn.close()

    delta = {
        "label": "delta",
        "units": after["units"] - before["units"],
        "revenue": round(after["revenue"] - before["revenue"], 2),
        "impact_margin_pln": round(after["impact_margin_pln"] - before["impact_margin_pln"], 2),
        "refunds": round(after["refunds"] - before["refunds"], 2),
        "return_rate": round(after["return_rate"] - before["return_rate"], 4),
        "sessions": None,
        "cvr": None,
    }

    baseline_units = 0.0
    baseline_revenue = 0.0
    baseline_cm1 = 0.0
    sample_points = 0
    day = after_from
    while day <= after_to:
        weekday_samples: list[dict[str, float]] = []
        for w in range(1, 9):
            hday = day - timedelta(days=7 * w)
            sample = daily_map.get(hday)
            if sample:
                weekday_samples.append(sample)
        if weekday_samples:
            sample_points += 1
            baseline_units += sum(float(s.get("units") or 0) for s in weekday_samples) / len(weekday_samples)
            baseline_revenue += sum(float(s.get("revenue") or 0) for s in weekday_samples) / len(weekday_samples)
            baseline_cm1 += sum(float(s.get("impact_margin_pln") or 0) for s in weekday_samples) / len(weekday_samples)
        day += timedelta(days=1)

    baseline_expected = {
        "label": "baseline_expected",
        "units": int(round(baseline_units)),
        "revenue": round(baseline_revenue, 2),
        "impact_margin_pln": round(baseline_cm1, 2),
        "refunds": 0.0,
        "return_rate": 0.0,
        "sessions": None,
        "cvr": None,
    }
    delta_vs_baseline = {
        "label": "delta_vs_baseline",
        "units": int(after["units"] - baseline_expected["units"]),
        "revenue": round(after["revenue"] - baseline_expected["revenue"], 2),
        "impact_margin_pln": round(after["impact_margin_pln"] - baseline_expected["impact_margin_pln"], 2),
        "refunds": 0.0,
        "return_rate": 0.0,
        "sessions": None,
        "cvr": None,
    }

    denominator = max(50.0, abs(float(baseline_expected["impact_margin_pln"])))
    negative_threshold = max(100.0, denominator * 0.2)
    positive_threshold = max(100.0, denominator * 0.2)
    if delta_vs_baseline["impact_margin_pln"] <= -negative_threshold:
        impact_signal = "negative"
    elif delta_vs_baseline["impact_margin_pln"] >= positive_threshold:
        impact_signal = "positive"
    else:
        impact_signal = "neutral"

    confidence_score = round(
        min(
            100.0,
            (sample_points / max(1, safe_range)) * 70.0
            + (min(abs(baseline_expected["revenue"]), 10000.0) / 10000.0) * 30.0,
        ),
        2,
    )
    negative_impact = impact_signal == "negative" and confidence_score >= 55 and abs(delta_vs_baseline["impact_margin_pln"]) >= negative_threshold

    return {
        "sku": sku_value,
        "marketplace_id": market_id,
        "range_days": safe_range,
        "before": {"label": f"before_{safe_range}d", **before},
        "after": {"label": f"after_{safe_range}d", **after},
        "delta": delta,
        "baseline_expected": baseline_expected,
        "delta_vs_baseline": delta_vs_baseline,
        "impact_signal": impact_signal,
        "confidence_score": confidence_score,
        "baseline_hint": "Baseline uses same-weekday trailing 8-week window; sessions/CVR still pending source integration.",
        "negative_impact": bool(negative_impact),
        "generated_at": datetime.now(timezone.utc),
    }


def get_content_data_quality():
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dbo.acc_co_versions WITH (NOLOCK)")
        total = int((cur.fetchone() or [0])[0] or 0)

        def _coverage(expr: str) -> float:
            if total <= 0:
                return 0.0
            cur.execute(f"SELECT COUNT(*) FROM dbo.acc_co_versions WITH (NOLOCK) WHERE {expr}")
            cnt = int((cur.fetchone() or [0])[0] or 0)
            return round((cnt * 100.0) / total, 2)

        title_cov = _coverage(
            "ISJSON(fields_json) = 1 AND LTRIM(RTRIM(ISNULL(JSON_VALUE(fields_json, '$.title'), ''))) <> ''"
        )
        bullets_cov = _coverage(
            "ISJSON(fields_json) = 1 AND JSON_QUERY(fields_json, '$.bullets') IS NOT NULL AND JSON_QUERY(fields_json, '$.bullets') <> '[]'"
        )
        desc_cov = _coverage(
            "ISJSON(fields_json) = 1 AND LTRIM(RTRIM(ISNULL(JSON_VALUE(fields_json, '$.description'), ''))) <> ''"
        )

        cur.execute(
            """
            SELECT TOP 20
                sku, marketplace_id, version_no, created_at
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE ISJSON(fields_json) = 1
              AND LTRIM(RTRIM(ISNULL(JSON_VALUE(fields_json, '$.title'), ''))) = ''
            ORDER BY created_at DESC
            """
        )
        missing_title = _fetchall_dict(cur)

        cur.execute(
            """
            SELECT TOP 20
                sku, marketplace_id, version_no, created_at
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE ISJSON(fields_json) = 1
              AND (JSON_QUERY(fields_json, '$.bullets') IS NULL OR JSON_QUERY(fields_json, '$.bullets') = '[]')
            ORDER BY created_at DESC
            """
        )
        missing_bullets = _fetchall_dict(cur)

        cur.execute(
            """
            SELECT TOP 20
                sku, marketplace_id, version_no, created_at
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE ISJSON(fields_json) = 1
              AND LTRIM(RTRIM(ISNULL(JSON_VALUE(fields_json, '$.description'), ''))) = ''
            ORDER BY created_at DESC
            """
        )
        missing_description = _fetchall_dict(cur)

        cards = [
            {"key": "title_coverage", "value": title_cov, "unit": "pct", "note": "Share of versions with non-empty title."},
            {"key": "bullets_coverage", "value": bullets_cov, "unit": "pct", "note": "Share of versions with bullets array."},
            {"key": "description_coverage", "value": desc_cov, "unit": "pct", "note": "Share of versions with non-empty description."},
        ]
        return {
            "cards": cards,
            "missing_title": missing_title,
            "missing_bullets": missing_bullets,
            "missing_description": missing_description,
            "generated_at": datetime.now(timezone.utc),
        }
    finally:
        conn.close()

