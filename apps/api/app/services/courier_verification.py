from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.core.db_connection import connect_acc


def _connect():
    return connect_acc(autocommit=False, timeout=30)


@dataclass(frozen=True)
class CarrierVerificationScope:
    carrier: str
    import_table: str
    import_root: str
    file_glob: str


_VERIFY_SPECS = {
    "DHL": CarrierVerificationScope(
        carrier="DHL",
        import_table="acc_dhl_import_file",
        import_root=settings.DHL_BILLING_ROOT_PATH,
        file_glob="DHL_Dokument nr *.xlsx",
    ),
    "GLS": CarrierVerificationScope(
        carrier="GLS",
        import_table="acc_gls_import_file",
        import_root=settings.GLS_BILLING_ROOT_PATH,
        file_glob="GLS_*.csv",
    ),
}


def ensure_courier_verification_schema() -> None:
    """No-op: schema managed by Alembic migration eb013."""
    pass


def _discover_expected_files(root: str, *, pattern: str, billing_period: str | None) -> list[str]:
    root_path = Path(root)
    if not root_path.exists():
        return []
    files = sorted(path.resolve() for path in root_path.rglob(pattern))
    if billing_period:
        token = billing_period.strip()
        files = [path for path in files if token in str(path)]
    return [str(path) for path in files]


def _load_import_states(
    cur,
    *,
    table_name: str,
) -> dict[str, dict[str, Any]]:
    cur.execute(
        f"""
        SELECT file_path, status, document_number, rows_imported, error_message
        FROM dbo.{table_name} WITH (NOLOCK)
        WHERE source_kind = 'invoice'
        """
    )
    result: dict[str, dict[str, Any]] = {}
    for row in cur.fetchall():
        result[str(row[0])] = {
            "status": str(row[1] or ""),
            "document_number": str(row[2] or "") or None,
            "rows_imported": int(row[3] or 0),
            "error_message": str(row[4] or "") or None,
        }
    return result


def _load_dhl_manifest_doc_gaps(cur, *, billing_period: str | None) -> dict[str, Any]:
    where = ["source_manifest_file IS NOT NULL"]
    params: list[Any] = []
    if billing_period:
        where.append("CONVERT(CHAR(7), issue_date, 126) = ?")
        params.append(billing_period.replace(".", "-"))

    cur.execute(
        f"""
        SELECT document_number, source_file
        FROM dbo.acc_dhl_billing_document WITH (NOLOCK)
        WHERE {' AND '.join(where)}
        """,
        params,
    )
    expected_docs: set[str] = set()
    imported_docs: set[str] = set()
    for row in cur.fetchall():
        doc = str(row[0] or "").strip()
        if not doc:
            continue
        expected_docs.add(doc)
        if row[1]:
            imported_docs.add(doc)
    missing_docs = sorted(expected_docs - imported_docs)
    return {
        "manifest_expected_count": len(expected_docs),
        "manifest_imported_count": len(imported_docs),
        "manifest_missing_count": len(missing_docs),
        "manifest_missing_docs_sample": missing_docs[:50],
    }


def _upsert_audit_row(
    cur,
    *,
    audit_type: str,
    carrier: str,
    scope_key: str,
    trigger_source: str,
    payload: dict[str, Any],
) -> None:
    cur.execute(
        """
        SELECT CAST(id AS NVARCHAR(40))
        FROM dbo.acc_courier_audit_log WITH (NOLOCK)
        WHERE audit_type = ?
          AND carrier = ?
          AND scope_key = ?
          AND trigger_source = ?
        """,
        (audit_type, carrier, scope_key, trigger_source),
    )
    row = cur.fetchone()
    detail_json = json.dumps(payload.get("detail_json") or {}, ensure_ascii=True)
    if row and row[0]:
        cur.execute(
            """
            UPDATE dbo.acc_courier_audit_log
            SET
                status = ?,
                expected_count = ?,
                imported_count = ?,
                failed_count = ?,
                missing_count = ?,
                extra_count = ?,
                matched_count = ?,
                detail_json = ?,
                updated_at = SYSUTCDATETIME()
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (
                payload["status"],
                payload["expected_count"],
                payload["imported_count"],
                payload["failed_count"],
                payload["missing_count"],
                payload["extra_count"],
                payload["matched_count"],
                detail_json,
                str(row[0]),
            ),
        )
        return

    cur.execute(
        """
        INSERT INTO dbo.acc_courier_audit_log
        (
            id, audit_type, carrier, scope_key, status, expected_count, imported_count,
            failed_count, missing_count, extra_count, matched_count, detail_json, trigger_source
        )
        VALUES
        (
            CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            str(uuid.uuid4()),
            audit_type,
            carrier,
            scope_key,
            payload["status"],
            payload["expected_count"],
            payload["imported_count"],
            payload["failed_count"],
            payload["missing_count"],
            payload["extra_count"],
            payload["matched_count"],
            detail_json,
            trigger_source,
        ),
    )


def verify_courier_billing_completeness(
    *,
    carrier: str | None = None,
    billing_period: str | None = None,
    trigger_source: str = "job",
) -> dict[str, Any]:
    ensure_courier_verification_schema()

    carriers = [carrier.upper()] if carrier else list(_VERIFY_SPECS.keys())
    scope_key = billing_period or "__all__"
    stats: dict[str, Any] = {
        "status": "ok",
        "billing_period": billing_period,
        "audits_written": 0,
        "items": [],
    }

    conn = _connect()
    try:
        cur = conn.cursor()
        for carrier_code in carriers:
            spec = _VERIFY_SPECS.get(carrier_code)
            if spec is None:
                raise RuntimeError(f"Unsupported carrier '{carrier_code}'")

            expected_files = _discover_expected_files(
                spec.import_root,
                pattern=spec.file_glob,
                billing_period=billing_period,
            )
            import_states = _load_import_states(cur, table_name=spec.import_table)
            expected_set = set(expected_files)
            imported_set = {
                path
                for path, meta in import_states.items()
                if meta.get("status") == "imported" and (not billing_period or billing_period in path)
            }
            failed_set = {
                path
                for path, meta in import_states.items()
                if meta.get("status") == "failed" and (not billing_period or billing_period in path)
            }
            missing_set = expected_set - imported_set
            extra_set = imported_set - expected_set

            detail_json: dict[str, Any] = {
                "carrier": carrier_code,
                "expected_files_sample": sorted(expected_set)[:20],
                "missing_files_sample": sorted(missing_set)[:20],
                "failed_files_sample": sorted(failed_set)[:20],
                "extra_files_sample": sorted(extra_set)[:20],
            }
            if carrier_code == "DHL":
                detail_json.update(_load_dhl_manifest_doc_gaps(cur, billing_period=billing_period))

            status = "ok"
            if missing_set or failed_set:
                status = "critical" if not imported_set else "partial"
            elif carrier_code == "DHL" and int(detail_json.get("manifest_missing_count") or 0) > 0:
                status = "partial"

            payload = {
                "status": status,
                "expected_count": len(expected_set),
                "imported_count": len(imported_set),
                "failed_count": len(failed_set),
                "missing_count": len(missing_set),
                "extra_count": len(extra_set),
                "matched_count": max(len(expected_set) - len(missing_set), 0),
                "detail_json": detail_json,
            }
            _upsert_audit_row(
                cur,
                audit_type="billing_completeness",
                carrier=carrier_code,
                scope_key=scope_key,
                trigger_source=trigger_source,
                payload=payload,
            )
            stats["audits_written"] += 1
            stats["items"].append(
                {
                    "carrier": carrier_code,
                    "scope_key": scope_key,
                    **payload,
                }
            )
            if status != "ok":
                stats["status"] = "warning"

        conn.commit()
        return stats
    finally:
        conn.close()
