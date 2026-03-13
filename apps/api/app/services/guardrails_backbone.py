"""Dead-letter monitoring & alerting for the ACC Event Backbone.

Provides:
- ``check_backbone_dead_letters()``   — guardrail check for failed events
- ``check_backbone_pending_depth()``  — guardrail for pending-event buildup
- ``check_backbone_processing_rate()`` — events/hour throughput
- ``send_backbone_alert()``            — CRITICAL log + ``acc_system_alert`` row
- ``get_backbone_health_summary()``    — aggregated health snapshot for API

All functions follow the existing guardrails pattern:
read-only, fail-open, idempotent, observable.
"""
from __future__ import annotations

import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import structlog

from app.core.db_connection import connect_acc
from app.services.guardrails import (
    GuardrailResult,
    Severity,
    _run_scalar,
    _run_rows,
    _timed,
)

log = structlog.get_logger(__name__)

# ── Thresholds ──────────────────────────────────────────────────────────────
DEAD_LETTER_ALERT_THRESHOLD: int = 5   # failed events in last hour
PENDING_DEPTH_WARNING: int = 500
PENDING_DEPTH_CRITICAL: int = 2000

# ── SQL ─────────────────────────────────────────────────────────────────────

_FAILED_LAST_HOUR_SQL = """\
SELECT COUNT(*)
FROM acc_event_log WITH (NOLOCK)
WHERE status = 'failed'
  AND received_at > DATEADD(HOUR, -1, GETUTCDATE())
"""

_PENDING_DEPTH_SQL = """\
SELECT COUNT(*)
FROM acc_event_log WITH (NOLOCK)
WHERE status = 'received'
"""

_PROCESSING_RATE_SQL = """\
SELECT COUNT(*)
FROM acc_event_log WITH (NOLOCK)
WHERE status = 'processed'
  AND processed_at > DATEADD(HOUR, -1, GETUTCDATE())
"""

_FAILED_DETAIL_SQL = """\
SELECT TOP 20
    event_id, notification_type, event_domain, error_message,
    retry_count, received_at
FROM acc_event_log WITH (NOLOCK)
WHERE status = 'failed'
  AND received_at > DATEADD(HOUR, -1, GETUTCDATE())
ORDER BY received_at DESC
"""

# ── acc_system_alert DDL (auto-created on first alert) ──────────────────────

_SYSTEM_ALERT_DDL = """\
IF OBJECT_ID('dbo.acc_system_alert', 'U') IS NULL
CREATE TABLE dbo.acc_system_alert (
    id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    alert_type  VARCHAR(100)  NOT NULL,
    severity    VARCHAR(20)   NOT NULL,
    message     NVARCHAR(2000) NOT NULL,
    details     NVARCHAR(MAX) NULL,
    created_at  DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    INDEX ix_system_alert_type_created (alert_type, created_at)
)
"""


# ═══════════════════════════════════════════════════════════════════════════
#  Guardrail checks
# ═══════════════════════════════════════════════════════════════════════════

def check_backbone_dead_letters() -> GuardrailResult:
    """Count failed events in the last hour; alert if > threshold."""
    sql = _FAILED_LAST_HOUR_SQL
    try:
        count, ms = _timed(_run_scalar, sql)
        count = int(count or 0)
        if count > DEAD_LETTER_ALERT_THRESHOLD:
            sev = Severity.CRITICAL
            msg = f"{count} failed events in last hour (threshold {DEAD_LETTER_ALERT_THRESHOLD})"
        elif count > 0:
            sev = Severity.WARNING
            msg = f"{count} failed event(s) in last hour"
        else:
            sev = Severity.OK
            msg = "No failed events in last hour"
        return GuardrailResult(
            "backbone_dead_letters", sev, msg,
            value=count, threshold=DEAD_LETTER_ALERT_THRESHOLD,
            query_used=sql, elapsed_ms=ms,
        )
    except Exception as exc:
        return GuardrailResult(
            "backbone_dead_letters", Severity.UNKNOWN,
            str(exc)[:200], query_used=sql,
        )


def check_backbone_pending_depth() -> GuardrailResult:
    """Count events still in 'received' status."""
    sql = _PENDING_DEPTH_SQL
    try:
        depth, ms = _timed(_run_scalar, sql)
        depth = int(depth or 0)
        if depth >= PENDING_DEPTH_CRITICAL:
            sev = Severity.CRITICAL
        elif depth >= PENDING_DEPTH_WARNING:
            sev = Severity.WARNING
        else:
            sev = Severity.OK
        return GuardrailResult(
            "backbone_pending_depth", sev,
            f"{depth} events pending processing",
            value=depth, threshold=PENDING_DEPTH_WARNING,
            query_used=sql, elapsed_ms=ms,
        )
    except Exception as exc:
        return GuardrailResult(
            "backbone_pending_depth", Severity.UNKNOWN,
            str(exc)[:200], query_used=sql,
        )


def check_backbone_processing_rate() -> GuardrailResult:
    """Events processed in the last hour (throughput metric)."""
    sql = _PROCESSING_RATE_SQL
    try:
        rate, ms = _timed(_run_scalar, sql)
        rate = int(rate or 0)
        sev = Severity.OK  # informational — no threshold
        return GuardrailResult(
            "backbone_processing_rate", sev,
            f"{rate} events processed in last hour",
            value=rate, query_used=sql, elapsed_ms=ms,
        )
    except Exception as exc:
        return GuardrailResult(
            "backbone_processing_rate", Severity.UNKNOWN,
            str(exc)[:200], query_used=sql,
        )


# ═══════════════════════════════════════════════════════════════════════════
#  Alert handler
# ═══════════════════════════════════════════════════════════════════════════

def send_backbone_alert(
    message: str,
    *,
    details: str | None = None,
    severity: str = "critical",
) -> dict:
    """Log a CRITICAL event and persist an alert row in acc_system_alert.

    Returns {"status": "stored", "alert_type": ...} on success.
    """
    log.critical(
        "event_backbone.alert",
        alert_type="event_backbone_failure",
        severity=severity,
        message=message,
    )

    try:
        conn = connect_acc(timeout=10)
        try:
            cur = conn.cursor()
            # Ensure table exists
            cur.execute(_SYSTEM_ALERT_DDL)
            conn.commit()
            cur.execute(
                """
                INSERT INTO dbo.acc_system_alert
                    (alert_type, severity, message, details, created_at)
                VALUES ('event_backbone_failure', ?, ?, ?, SYSUTCDATETIME())
                """,
                (severity, message[:2000], (details or "")[:4000] or None),
            )
            conn.commit()
            cur.close()
        finally:
            conn.close()
        return {"status": "stored", "alert_type": "event_backbone_failure"}
    except Exception as exc:
        log.error("backbone_alert.persist_failed", error=str(exc))
        return {"status": "log_only", "error": str(exc)}


# ═══════════════════════════════════════════════════════════════════════════
#  Evaluate & auto-alert
# ═══════════════════════════════════════════════════════════════════════════

def evaluate_and_alert() -> list[GuardrailResult]:
    """Run all backbone guardrail checks; fire alert if dead-letter threshold breached.

    Designed to be called from the scheduler (e.g. every 5 min).
    """
    results = [
        check_backbone_dead_letters(),
        check_backbone_pending_depth(),
        check_backbone_processing_rate(),
    ]

    dead_letter_check = results[0]
    if (
        dead_letter_check.severity == Severity.CRITICAL
        and isinstance(dead_letter_check.value, (int, float))
        and dead_letter_check.value > DEAD_LETTER_ALERT_THRESHOLD
    ):
        # Gather detail for the alert body
        try:
            detail_rows = _run_rows(_FAILED_DETAIL_SQL)
            detail_text = "\n".join(
                f"  {r[0][:16]}… type={r[1]} domain={r[2]} err={str(r[3])[:120]} retries={r[4]}"
                for r in detail_rows
            )
        except Exception:
            detail_text = "(could not fetch detail)"

        send_backbone_alert(
            dead_letter_check.message,
            details=detail_text,
        )

    return results


# ═══════════════════════════════════════════════════════════════════════════
#  Health summary (for the /backbone/health endpoint)
# ═══════════════════════════════════════════════════════════════════════════

def get_backbone_health_summary() -> dict:
    """Aggregate health snapshot for the backbone health endpoint.

    Returns:
        {
            "failed_last_hour": int,
            "pending_events": int,
            "processing_rate": int,
            "circuit_breakers": [...],
            "alert_threshold": int,
            "status": "healthy" | "degraded" | "critical",
        }
    """
    t0 = time.perf_counter()

    failed = 0
    pending = 0
    rate = 0

    try:
        failed = int(_run_scalar(_FAILED_LAST_HOUR_SQL) or 0)
    except Exception:
        pass
    try:
        pending = int(_run_scalar(_PENDING_DEPTH_SQL) or 0)
    except Exception:
        pass
    try:
        rate = int(_run_scalar(_PROCESSING_RATE_SQL) or 0)
    except Exception:
        pass

    # Circuit breakers from event_backbone
    try:
        from app.services.event_backbone import get_handler_health
        circuits = get_handler_health()
    except Exception:
        circuits = []

    open_circuits = [c["handler_name"] for c in circuits if c.get("circuit_open")]

    if failed > DEAD_LETTER_ALERT_THRESHOLD or open_circuits:
        status = "critical"
    elif failed > 0 or pending >= PENDING_DEPTH_WARNING:
        status = "degraded"
    else:
        status = "healthy"

    return {
        "status": status,
        "failed_last_hour": failed,
        "pending_events": pending,
        "processing_rate": rate,
        "alert_threshold": DEAD_LETTER_ALERT_THRESHOLD,
        "open_circuits": open_circuits,
        "circuit_breakers": circuits,
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
    }
