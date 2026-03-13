"""Content Ops - shared helpers, constants, normalizers, assets, native API."""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any

import pyodbc
import httpx

from app.connectors.mssql.mssql_store import ensure_v2_schema  # noqa: F401
from app.connectors.amazon_sp_api.client import SPAPIClient
from app.connectors.amazon_sp_api.catalog import CatalogClient
from app.core.config import settings, MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc


class ContentOpsNotImplementedError(NotImplementedError):
    """Raised by scaffold methods that are not implemented yet."""


_TASK_TYPES = {"create_listing", "refresh_content", "fix_policy", "expand_marketplaces"}

_TASK_STATUSES = {"open", "investigating", "resolved"}

_TASK_PRIORITIES = {"p0", "p1", "p2", "p3"}

_POLICY_SEVERITIES = {"critical", "major", "minor"}

_ASSET_STATUSES = {"approved", "deprecated", "draft"}

_ASSET_ROLES = {"main_image", "manual", "cert", "aplus", "lifestyle", "infographic", "other"}

_ALLOWED_ASSET_MIME = {
    "image/jpeg",
    "image/png",
    "image/webp",
    "application/pdf",
    "text/plain",
}

_PUBLISH_SELECTIONS = {"approved", "draft"}

_PUBLISH_FORMATS = {"xlsx", "csv"}

_ATTRIBUTE_TRANSFORMS = {"identity", "stringify", "upper", "lower", "trim"}

_AI_BANNED_CLAIMS = [
    r"\bcure(s|d)?\b",
    r"\bheal(s|ed|ing)?\b",
    r"\bguarantee(s|d)?\b",
    r"\b100%\b",
    r"\bbest\b",
    r"\b#1\b",
]

_POLISH_LEAK_PATTERNS = [
    r"\b(jest|sa|dla|oraz|lub|moze|ktory|ktora|ktore)\b",
    r"\b(bardzo|tylko|takze|rowniez|jednak|wiec|dlatego)\b",
    r"\b(produkt|nasz|nasza|nasze|twoj|twoja|twoje)\b",
    r"\b(swietny|doskonaly|idealny|najlepszy|perfekcyjny)\b",
]

_DEFAULT_CONTENT_MARKETS = ["DE", "FR", "IT", "ES", "NL", "PL", "SE", "BE"]

_MARKET_CODE_TO_ID = {
    (v.get("code") or "").upper(): k
    for k, v in MARKETPLACE_REGISTRY.items()
    if isinstance(v, dict) and v.get("code")
}

_PRODUCT_TYPE_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("PLANTER", ("planter", "donicz", "blumentopf", "cache-pot", "maceta", "vaso")),
    ("SLED", ("sled", "sanki", "schlitten", "trineo", "slitta", "slede", "slade")),
    ("KITCHEN", ("kitchen", "kuch", "kochen", "pan", "pfanne", "garnek", "knife", "messer")),
    ("LAWN_AND_GARDEN", ("garden", "garten", "ogrod", "lawn", "rasen", "sekator", "rake", "grabie")),
    ("FURNITURE", ("furniture", "mebel", "mobel", "chair", "stuhl", "table", "tisch", "regal")),
    ("TOOL", ("tool", "narzedz", "werkzeug", "drill", "bohrer", "hammer", "mlotek")),
    ("PET_SUPPLIES", ("pet", "zwier", "haustier", "dog", "hund", "cat", "katze")),
    ("LIGHTING", ("lamp", "lampe", "led", "lighting", "oswietl", "beleuchtung")),
    ("HOME", ("home", "dom", "house", "haus", "storage", "organiz", "kosz", "basket")),
]

_PUBLISH_RETRY_BASE_MINUTES = 5

_PUBLISH_RETRY_MAX = 3



def _connect():
    return connect_acc(autocommit=False, timeout=20)


def _fetchall_dict(cur: pyodbc.Cursor) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _normalize_status(status: str) -> str:
    value = (status or "").strip().lower()
    if value not in _TASK_STATUSES:
        raise ValueError("status must be one of: open, investigating, resolved")
    return value


def _normalize_priority(priority: str) -> str:
    value = (priority or "").strip().lower()
    if value not in _TASK_PRIORITIES:
        raise ValueError("priority must be one of: p0, p1, p2, p3")
    return value


def _normalize_task_type(task_type: str) -> str:
    value = (task_type or "").strip().lower()
    if value not in _TASK_TYPES:
        raise ValueError(
            "type must be one of: create_listing, refresh_content, fix_policy, expand_marketplaces"
        )
    return value


def _status_transition_allowed(current_status: str, next_status: str) -> bool:
    if current_status == next_status:
        return True
    allowed = {
        "open": {"investigating"},
        "investigating": {"resolved"},
        "resolved": set(),
    }
    return next_status in allowed.get(current_status, set())


def _json_load(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    text = str(value).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _detect_brand_for_sku(cur: pyodbc.Cursor, sku: str) -> str | None:
    cur.execute(
        """
        SELECT TOP 1 p.brand
        FROM dbo.acc_product p WITH (NOLOCK)
        WHERE (p.internal_sku = ? OR p.sku = ?)
          AND p.brand IS NOT NULL
          AND LTRIM(RTRIM(p.brand)) <> ''
        """,
        (sku, sku),
    )
    row = cur.fetchone()
    if row and row[0]:
        return str(row[0]).strip()
    return None


# ── Canonical product resolution (S8.4) ──────────────────────────

_CO_INTERNAL_SKU_COL_ENSURED = False


def _ensure_co_internal_sku_columns() -> None:
    """Add internal_sku to content-ops tables if not present (idempotent)."""
    global _CO_INTERNAL_SKU_COL_ENSURED
    if _CO_INTERNAL_SKU_COL_ENSURED:
        return
    conn = _connect()
    try:
        cur = conn.cursor()
        for tbl in ("acc_co_tasks", "acc_co_versions", "acc_co_asset_links"):
            cur.execute(f"""
                IF NOT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{tbl}' AND COLUMN_NAME = 'internal_sku'
                )
                ALTER TABLE dbo.{tbl} ADD internal_sku NVARCHAR(64) NULL;
            """)
        conn.commit()
        _CO_INTERNAL_SKU_COL_ENSURED = True
    except Exception:
        pass  # non-critical — table may not yet exist at import time
    finally:
        conn.close()


def resolve_internal_sku(
    cur: pyodbc.Cursor,
    sku: str,
    marketplace_id: str | None,
) -> str | None:
    """Resolve seller_sku → internal_sku via acc_marketplace_presence."""
    if not sku or not marketplace_id:
        return None
    cur.execute("""
        SELECT TOP 1 internal_sku
        FROM dbo.acc_marketplace_presence WITH (NOLOCK)
        WHERE seller_sku = ? AND marketplace_id = ?
    """, (sku, marketplace_id))
    row = cur.fetchone()
    return row[0] if row else None


def get_brand_for_sku(*, sku: str) -> str | None:
    sku_value = str(sku or "").strip()
    if not sku_value:
        return None
    conn = _connect()
    try:
        cur = conn.cursor()
        return _detect_brand_for_sku(cur, sku_value)
    finally:
        conn.close()


def _map_task_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "type": row["task_type"],
        "sku": row["sku"],
        "asin": row.get("asin"),
        "internal_sku": row.get("internal_sku"),
        "marketplace_id": row.get("marketplace_id"),
        "priority": row.get("priority"),
        "owner": row.get("owner"),
        "due_date": row.get("due_date"),
        "status": row.get("status"),
        "tags_json": _json_load(row.get("tags_json")),
        "title": row.get("title"),
        "note": row.get("note"),
        "source_page": row.get("source_page"),
        "created_by": row.get("created_by"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def _map_version_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "sku": row["sku"],
        "asin": row.get("asin"),
        "internal_sku": row.get("internal_sku"),
        "marketplace_id": row["marketplace_id"],
        "version_no": int(row["version_no"] or 0),
        "status": row["status"],
        "fields": _json_load(row.get("fields_json")),
        "created_by": row.get("created_by"),
        "created_at": row.get("created_at"),
        "approved_by": row.get("approved_by"),
        "approved_at": row.get("approved_at"),
        "published_at": row.get("published_at"),
        "parent_version_id": str(row["parent_version_id"]) if row.get("parent_version_id") else None,
    }


def _normalize_version_status(status: str) -> str:
    value = (status or "").strip().lower()
    if value not in {"draft", "review", "approved", "published"}:
        raise ValueError("version status must be one of: draft, review, approved, published")
    return value


def _normalize_policy_severity(severity: str) -> str:
    value = (severity or "").strip().lower()
    if value not in _POLICY_SEVERITIES:
        raise ValueError("policy severity must be one of: critical, major, minor")
    return value


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False


def _normalize_asset_status(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in _ASSET_STATUSES:
        raise ValueError("asset status must be one of: approved, deprecated, draft")
    return normalized


def _normalize_asset_role(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in _ASSET_ROLES:
        raise ValueError("asset role must be one of: main_image, manual, cert, aplus, lifestyle, infographic, other")
    return normalized


def _safe_filename(filename: str) -> str:
    base = Path(filename or "").name.strip()
    if not base:
        raise ValueError("filename is required")
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in base)


def _map_asset_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "filename": row["filename"],
        "mime": row["mime"],
        "content_hash": row["content_hash"],
        "storage_path": row["storage_path"],
        "metadata_json": _json_load(row.get("metadata_json")),
        "status": _normalize_asset_status(str(row.get("status") or "approved")),
        "uploaded_by": row.get("uploaded_by"),
        "uploaded_at": row.get("uploaded_at"),
    }


def _json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(v) for v in parsed]
    except Exception:
        pass
    return []


def _map_publish_job_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "job_type": row["job_type"],
        "marketplaces": _json_list(row.get("marketplaces_json")),
        "selection_mode": row["selection_mode"],
        "status": row["status"],
        "progress_pct": float(row.get("progress_pct") or 0),
        "log_json": _json_load(row.get("log_json")),
        "artifact_url": row.get("artifact_url"),
        "created_by": row.get("created_by"),
        "created_at": row.get("created_at"),
        "finished_at": row.get("finished_at"),
    }


def _normalize_publish_selection(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in _PUBLISH_SELECTIONS:
        raise ValueError("selection must be one of: approved, draft")
    return normalized


def _normalize_publish_format(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in _PUBLISH_FORMATS:
        raise ValueError("format must be one of: xlsx, csv")
    return normalized


def upload_asset(*, payload: dict):
    ensure_v2_schema()
    filename = _safe_filename(str(payload.get("filename") or ""))
    mime = str(payload.get("mime") or "").strip().lower()
    if not mime:
        raise ValueError("mime is required")
    if mime not in _ALLOWED_ASSET_MIME:
        raise ValueError("unsupported mime type")

    content_base64 = str(payload.get("content_base64") or "").strip()
    if not content_base64:
        raise ValueError("content_base64 is required")
    metadata = payload.get("metadata_json") or {}
    if not isinstance(metadata, dict):
        raise ValueError("metadata_json must be an object")

    try:
        raw = base64.b64decode(content_base64, validate=True)
    except Exception as exc:
        raise ValueError("content_base64 is invalid") from exc
    if not raw:
        raise ValueError("decoded asset content is empty")
    if len(raw) > 20 * 1024 * 1024:
        raise ValueError("asset exceeds max size 20MB")

    content_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    now = datetime.now(timezone.utc)
    storage_path = f"content-assets/{now:%Y/%m}/{content_hash[7:19]}_{filename}"

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1
                id, filename, mime, content_hash, storage_path, metadata_json, status, uploaded_by, uploaded_at
            FROM dbo.acc_co_assets WITH (NOLOCK)
            WHERE content_hash = ?
            """,
            (content_hash,),
        )
        existing = _fetchall_dict(cur)
        if existing:
            return _map_asset_row(existing[0])

        asset_id = str(uuid.uuid4())
        status_value = _normalize_asset_status(str(metadata.get("status", "approved")))
        cur.execute(
            """
            INSERT INTO dbo.acc_co_assets
                (id, filename, mime, content_hash, storage_path, metadata_json, status, uploaded_by)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                filename,
                mime,
                content_hash,
                storage_path,
                json.dumps(metadata, ensure_ascii=True),
                status_value,
                settings.DEFAULT_ACTOR,
            ),
        )
        conn.commit()

        cur.execute(
            """
            SELECT TOP 1
                id, filename, mime, content_hash, storage_path, metadata_json, status, uploaded_by, uploaded_at
            FROM dbo.acc_co_assets WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (asset_id,),
        )
        return _map_asset_row(_fetchall_dict(cur)[0])
    finally:
        conn.close()


def list_assets(
    *,
    sku: Optional[str] = None,
    tag: Optional[str] = None,
    role: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
):
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []

        if status:
            where.append("a.status = ?")
            params.append(_normalize_asset_status(status))

        if sku:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM dbo.acc_co_asset_links l WITH (NOLOCK)
                    WHERE l.asset_id = a.id
                      AND l.sku = ?
                )
                """
            )
            params.append(sku)

        if role:
            where.append(
                """
                EXISTS (
                    SELECT 1 FROM dbo.acc_co_asset_links l WITH (NOLOCK)
                    WHERE l.asset_id = a.id
                      AND l.role = ?
                )
                """
            )
            params.append(_normalize_asset_role(role))

        if tag:
            where.append("ISNULL(a.metadata_json, '') LIKE ?")
            params.append(f"%{tag}%")

        where_sql = " AND ".join(where)
        safe_page_size = max(1, min(page_size, 200))
        safe_page = max(1, page)
        offset = (safe_page - 1) * safe_page_size

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_co_assets a WITH (NOLOCK) WHERE {where_sql}", params)
        total = int(cur.fetchone()[0] or 0)
        pages = math.ceil(total / safe_page_size) if total else 0

        cur.execute(
            f"""
            SELECT
                a.id, a.filename, a.mime, a.content_hash, a.storage_path, a.metadata_json,
                a.status, a.uploaded_by, a.uploaded_at
            FROM dbo.acc_co_assets a WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY a.uploaded_at DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            (*params, offset, safe_page_size),
        )
        rows = _fetchall_dict(cur)
        return {
            "total": total,
            "page": safe_page,
            "page_size": safe_page_size,
            "pages": pages,
            "items": [_map_asset_row(r) for r in rows],
        }
    finally:
        conn.close()


def link_asset(*, asset_id: str, payload: dict):
    ensure_v2_schema()
    sku = str(payload.get("sku") or "").strip()
    if not sku:
        raise ValueError("sku is required")
    role = _normalize_asset_role(str(payload.get("role") or ""))
    status = _normalize_asset_status(str(payload.get("status") or "approved"))
    asin = (payload.get("asin") or "").strip() or None
    marketplace_id = (payload.get("marketplace_id") or "").strip() or None

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM dbo.acc_co_assets WITH (NOLOCK) WHERE id = CAST(? AS UNIQUEIDENTIFIER)",
            (asset_id,),
        )
        if int((cur.fetchone() or [0])[0] or 0) == 0:
            raise ValueError("asset not found")

        link_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO dbo.acc_co_asset_links
                (id, asset_id, sku, asin, marketplace_id, role, status)
            VALUES
                (?, CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?)
            """,
            (
                link_id,
                asset_id,
                sku,
                asin,
                marketplace_id,
                role,
                status,
            ),
        )
        conn.commit()

        cur.execute(
            """
            SELECT TOP 1 id, asset_id, sku, asin, marketplace_id, role, status, created_at
            FROM dbo.acc_co_asset_links WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (link_id,),
        )
        row = _fetchall_dict(cur)[0]
        return {
            "id": str(row["id"]),
            "asset_id": str(row["asset_id"]),
            "sku": row["sku"],
            "asin": row.get("asin"),
            "marketplace_id": row.get("marketplace_id"),
            "role": _normalize_asset_role(str(row.get("role") or "other")),
            "status": _normalize_asset_status(str(row.get("status") or "approved")),
            "created_at": row.get("created_at"),
        }
    finally:
        conn.close()


def _bridge_base_url() -> str:
    return str(getattr(settings, "PRODUCTONBOARD_BASE_URL", "") or "").strip().rstrip("/")


def _bridge_enabled() -> bool:
    return bool(_bridge_base_url())


def _bridge_headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    api_key = str(getattr(settings, "PRODUCTONBOARD_API_KEY", "") or "").strip()
    if api_key:
        headers["X-API-Key"] = api_key
    return headers


def _bridge_timeout() -> float:
    return float(getattr(settings, "PRODUCTONBOARD_TIMEOUT_SEC", 20) or 20)


def _spapi_ready() -> bool:
    return bool(
        str(settings.SP_API_CLIENT_ID or "").strip()
        and str(settings.SP_API_CLIENT_SECRET or "").strip()
        and str(settings.SP_API_REFRESH_TOKEN or "").strip()
        and str(settings.SP_API_SELLER_ID or "").strip()
    )


def _marketplace_to_id(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.upper().startswith("A") and len(raw) >= 10:
        return raw
    return _MARKET_CODE_TO_ID.get(raw.upper(), raw)


def _marketplace_to_code(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.upper() in _MARKET_CODE_TO_ID:
        return raw.upper()
    info = MARKETPLACE_REGISTRY.get(raw)
    if info and isinstance(info, dict) and info.get("code"):
        return str(info["code"]).upper()
    return raw.upper()


def _run_async(coro):
    return asyncio.run(coro)


def _native_catalog_search_by_ean(ean: str, marketplace: str) -> dict[str, Any]:
    if not _spapi_ready():
        raise RuntimeError("SP-API not configured for native catalog check")
    mp_id = _marketplace_to_id(marketplace)
    if not mp_id:
        raise ValueError("marketplace is required")
    ean_value = str(ean or "").strip()
    if not ean_value:
        raise ValueError("ean is required")

    async def _call():
        client = CatalogClient(marketplace_id=mp_id)
        items = await client.search_items(
            identifiers=[ean_value],
            identifiers_type="EAN",
            included_data="summaries,images,identifiers",
            page_size=10,
        )
        out_matches: list[dict[str, Any]] = []
        for item in items:
            asin = str(item.get("asin") or "").strip()
            if not asin:
                continue
            title = None
            brand = None
            product_type = None
            image_url = None

            for summary_group in item.get("summaries", []):
                if summary_group.get("marketplaceId") == mp_id:
                    title = summary_group.get("itemName")
                    brand = summary_group.get("brandName")
                    product_type = summary_group.get("productType")
                    break

            for image_group in item.get("images", []):
                if image_group.get("marketplaceId") == mp_id:
                    imgs = image_group.get("images") or []
                    if imgs:
                        image_url = imgs[0].get("link")
                    break

            out_matches.append(
                {
                    "asin": asin,
                    "title": title,
                    "brand": brand,
                    "product_type": product_type,
                    "image_url": image_url,
                }
            )

        return {
            "query": ean_value,
            "marketplace": _marketplace_to_code(marketplace),
            "total": len(out_matches),
            "matches": out_matches,
        }

    return _run_async(_call())


def _native_restrictions_check(asin: str, marketplace: str) -> dict[str, Any]:
    if not _spapi_ready():
        raise RuntimeError("SP-API not configured for native restrictions check")
    asin_value = str(asin or "").strip()
    if not asin_value:
        raise ValueError("asin is required")
    mp_id = _marketplace_to_id(marketplace)
    if not mp_id:
        raise ValueError("marketplace is required")

    async def _call():
        client = SPAPIClient(marketplace_id=mp_id)
        payload = await client.get(
            "/listings/2021-08-01/restrictions",
            params={
                "asin": asin_value,
                "sellerId": settings.SP_API_SELLER_ID,
                "marketplaceIds": mp_id,
                "conditionType": "new_new",
            },
        )
        restrictions = payload.get("restrictions") if isinstance(payload, dict) else []
        restrictions = restrictions if isinstance(restrictions, list) else []

        reasons: list[str] = []
        requires_approval = False
        if restrictions:
            for r in restrictions:
                reason_items = r.get("reasons") if isinstance(r, dict) else None
                if isinstance(reason_items, list):
                    for reason in reason_items:
                        if not isinstance(reason, dict):
                            continue
                        code = str(reason.get("reasonCode") or "").strip()
                        message = str(reason.get("message") or "").strip()
                        if code:
                            reasons.append(code if not message else f"{code}: {message}")
                        elif message:
                            reasons.append(message)
                        txt = f"{code} {message}".lower()
                        if "approval" in txt:
                            requires_approval = True
                else:
                    txt = json.dumps(r, ensure_ascii=True)
                    reasons.append(txt[:200])

        can_list = len(restrictions) == 0
        return {
            "asin": asin_value,
            "marketplace": _marketplace_to_code(marketplace),
            "can_list": can_list,
            "requires_approval": requires_approval,
            "reasons": reasons,
        }

    return _run_async(_call())


def _language_tag_for_market(market: str) -> str:
    code = _marketplace_to_code(market)
    mapping = {
        "DE": "de_DE",
        "FR": "fr_FR",
        "IT": "it_IT",
        "ES": "es_ES",
        "NL": "nl_NL",
        "PL": "pl_PL",
        "SE": "sv_SE",
        "BE": "nl_BE",
    }
    return mapping.get(code, "en_GB")


def _native_push_listing_content(
    *,
    marketplace: str,
    sku: str,
    fields: dict[str, Any],
    category_hint: str | None = None,
    subcategory_hint: str | None = None,
    brand_hint: str | None = None,
    product_type_override: str | None = None,
    required_attrs: list[str] | None = None,
) -> dict[str, Any]:
    if not _spapi_ready():
        raise RuntimeError("SP-API not configured for native push")
    mp_id = _marketplace_to_id(marketplace)
    if not mp_id:
        raise ValueError("marketplace is required")
    sku_value = str(sku or "").strip()
    if not sku_value:
        raise ValueError("sku is required")

    lang = _language_tag_for_market(marketplace)
    patches: list[dict[str, Any]] = []

    title = str(fields.get("title") or "").strip()
    if title:
        patches.append(
            {
                "op": "replace",
                "path": "/attributes/item_name",
                "value": [{"value": title, "language_tag": lang, "marketplace_id": mp_id}],
            }
        )

    bullets = fields.get("bullets")
    if isinstance(bullets, list):
        for idx, bullet in enumerate(bullets[:5], start=1):
            text = str(bullet or "").strip()
            if text:
                patches.append(
                    {
                        "op": "replace",
                        "path": f"/attributes/bullet_point#{idx}",
                        "value": [{"value": text, "language_tag": lang, "marketplace_id": mp_id}],
                    }
                )

    description = str(fields.get("description") or "").strip()
    if description:
        patches.append(
            {
                "op": "replace",
                "path": "/attributes/product_description",
                "value": [{"value": description, "language_tag": lang, "marketplace_id": mp_id}],
            }
        )

    keywords = str(fields.get("keywords") or "").strip()
    if keywords:
        patches.append(
            {
                "op": "replace",
                "path": "/attributes/generic_keyword",
                "value": [{"value": keywords, "language_tag": lang, "marketplace_id": mp_id}],
            }
        )

    if not patches:
        raise ValueError("no_pushable_fields")

    attrs = fields.get("attributes_json") if isinstance(fields.get("attributes_json"), dict) else {}
    attrs = attrs if isinstance(attrs, dict) else {}

    if product_type_override:
        product_type = str(product_type_override).strip().upper()
    else:
        product_type, _, _ = _resolve_native_product_type_and_requirements(
            sku=sku_value,
            fields=fields,
            category_hint=category_hint,
            subcategory_hint=subcategory_hint,
            brand_hint=brand_hint,
            marketplace=marketplace,
            mapping_rules=None,
        )

    required_list = [str(x).strip() for x in (required_attrs or []) if str(x).strip()]
    missing_required: list[str] = []
    required_set = {str(x).strip().lower() for x in required_list if str(x).strip()}
    for attr_name in required_list:
        attr_value = attrs.get(attr_name)
        if attr_value is None or (isinstance(attr_value, str) and not attr_value.strip()):
            missing_required.append(attr_name)
        else:
            normalized_values = _normalize_attribute_values(value=attr_value, lang=lang, mp_id=mp_id)
            if not normalized_values:
                missing_required.append(attr_name)
                continue
            patches.append(
                {
                    "op": "replace",
                    "path": f"/attributes/{attr_name}",
                    "value": normalized_values,
                }
            )

    if missing_required:
        raise ValueError(f"missing_required_attributes:{','.join(missing_required)}")

    for attr_name, attr_value in attrs.items():
        attr_key = str(attr_name).strip()
        if not attr_key:
            continue
        if _is_reserved_content_attr(attr_key):
            continue
        if attr_key.lower() in required_set:
            continue
        normalized_values = _normalize_attribute_values(value=attr_value, lang=lang, mp_id=mp_id)
        if not normalized_values:
            continue
        patches.append(
            {
                "op": "replace",
                "path": f"/attributes/{attr_key}",
                "value": normalized_values,
            }
        )

    async def _call():
        client = SPAPIClient(marketplace_id=mp_id)
        body = {"productType": product_type, "patches": patches}
        return await client.post(
            f"/listings/2021-08-01/items/{settings.SP_API_SELLER_ID}/{sku_value}"
            f"?marketplaceIds={mp_id}",
            body=body,
        )

    return _run_async(_call())


def _bridge_get_json(path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    base = _bridge_base_url()
    if not base:
        raise RuntimeError("ProductOnboard bridge is not configured")
    url = urljoin(f"{base}/", path.lstrip("/"))
    with httpx.Client(timeout=_bridge_timeout()) as client:
        resp = client.get(url, params=params, headers=_bridge_headers())
    try:
        payload = resp.json() if resp.content else {}
    except Exception:
        payload = {}
    return resp.status_code, payload if isinstance(payload, dict) else {}


def _bridge_post_json(path: str, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    base = _bridge_base_url()
    if not base:
        raise RuntimeError("ProductOnboard bridge is not configured")
    url = urljoin(f"{base}/", path.lstrip("/"))
    with httpx.Client(timeout=max(_bridge_timeout(), 60.0)) as client:
        resp = client.post(url, json=payload, headers=_bridge_headers())
    try:
        body = resp.json() if resp.content else {}
    except Exception:
        body = {}
    return resp.status_code, body if isinstance(body, dict) else {}

