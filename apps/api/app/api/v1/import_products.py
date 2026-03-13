"""API routes — Import Products (CEO's Excel upload + listing with Amazon metrics)."""
from __future__ import annotations

import math
from pathlib import Path
import tempfile
from typing import Optional
import uuid

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.concurrency import run_in_threadpool

import structlog

from app.connectors.mssql import enqueue_job
from app.core.config import settings
from app.schemas.jobs import JobRunOut

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/import-products", tags=["import-products"])


@router.post("/upload", response_model=JobRunOut, status_code=202)
async def upload_import_products(file: UploadFile = File(...)):
    """Upload the CEO's import products Excel file.

    Accepts .xlsx files. Parses headers from row 3.
    Upserts all products into acc_import_products table.
    """
    if not file.filename:
        raise HTTPException(400, "No file provided")

    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in ("xlsx", "xls"):
        raise HTTPException(
            400,
            f"Invalid file type '.{ext}'. Expected .xlsx or .xls",
        )

    contents = await file.read()
    if len(contents) == 0:
        raise HTTPException(400, "Empty file")

    if len(contents) > 200 * 1024 * 1024:  # 200 MB limit
        raise HTTPException(400, "File too large (max 200 MB)")

    try:
        suffix = "." + ext if ext else ""
        staging_dir = Path(tempfile.gettempdir()) / "acc_job_uploads" / "import_products"
        staging_dir.mkdir(parents=True, exist_ok=True)
        staged_path = staging_dir / f"{uuid.uuid4().hex}{suffix}"
        staged_path.write_bytes(contents)

        return await run_in_threadpool(
            enqueue_job,
            job_type="import_products_upload",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params={
                "file_path": str(staged_path),
                "filename": file.filename,
            },
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        log.error("import_products.upload_error", error=str(e))
        raise HTTPException(500, f"Error processing file: {str(e)}")


@router.get("")
async def list_import_products(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    sku_search: Optional[str] = Query(None),
    aktywny: Optional[bool] = Query(None),
    kod_importu: Optional[str] = Query(None),
    has_amazon_sales: Optional[bool] = Query(None),
    min_zasieg: Optional[int] = Query(None),
    max_zasieg: Optional[int] = Query(None),
    sort_by: str = Query("sku"),
    sort_dir: str = Query("asc"),
):
    """List import products with Amazon metrics, pagination and filtering."""
    from app.services.import_products import get_import_products_list

    try:
        return await run_in_threadpool(
            get_import_products_list,
            page=page,
            page_size=page_size,
            sku_search=sku_search,
            aktywny=aktywny,
            kod_importu=kod_importu,
            has_amazon_sales=has_amazon_sales,
            min_zasieg=min_zasieg,
            max_zasieg=max_zasieg,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
    except Exception as e:
        log.error("import_products.list_error", error=str(e))
        raise HTTPException(500, f"Error fetching import products: {str(e)}")


@router.get("/summary")
async def import_products_summary():
    """Summary statistics for import products — Holding + Amazon."""
    from app.services.import_products import get_import_products_summary

    try:
        return await run_in_threadpool(get_import_products_summary)
    except Exception as e:
        log.error("import_products.summary_error", error=str(e))
        raise HTTPException(500, str(e))


@router.get("/filter-options")
async def import_products_filter_options():
    """Return distinct filter values (kod_importu, etc.) for the UI."""
    from app.services.import_products import get_import_filter_options

    try:
        return await run_in_threadpool(get_import_filter_options)
    except Exception as e:
        log.error("import_products.filter_options_error", error=str(e))
        raise HTTPException(500, str(e))


@router.get("/skus")
async def import_product_skus():
    """Return flat list of SKUs flagged as import.

    Used by the frontend to flag import products in dashboards.
    """
    from app.services.import_products import get_import_skus

    try:
        skus = await run_in_threadpool(get_import_skus)
        return {"skus": sorted(skus), "count": len(skus)}
    except Exception as e:
        log.error("import_products.skus_error", error=str(e))
        raise HTTPException(500, str(e))
