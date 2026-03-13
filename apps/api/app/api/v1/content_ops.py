"""Content Ops Studio API scaffold (P0)."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi import Depends

from app.connectors.mssql import enqueue_job
from app.core.security import require_analyst, require_ops
from app.core.config import MARKETPLACE_REGISTRY, settings
from app.schemas.jobs import JobRunOut
from app.schemas.content_ops import (
    AIContentGenerateRequest,
    AIContentGenerateResponse,
    AssetItem,
    AssetLinkCreate,
    AssetLinkItem,
    AssetListResponse,
    AssetUploadRequest,
    ContentDiffResponse,
    ContentSyncRequest,
    ContentSyncResponse,
    ContentTaskCreate,
    ContentTaskItem,
    ContentTaskListResponse,
    ContentTaskBulkUpdateRequest,
    ContentTaskBulkUpdateResponse,
    ContentTaskUpdate,
    ContentDataQualityResponse,
    ContentOnboardPreflightRequest,
    ContentOnboardPreflightResponse,
    ContentOnboardRestrictionResponse,
    ContentOnboardCatalogResponse,
    ContentImpactResponse,
    ContentComplianceQueueResponse,
    ContentProductTypeMapRule,
    ContentProductTypeMapUpsertRequest,
    ContentProductTypeDefinitionItem,
    ContentProductTypeDefinitionRefreshRequest,
    ContentAttributeMapRule,
    ContentAttributeMapUpsertRequest,
    ContentPublishCoverageResponse,
    ContentPublishMappingSuggestionsResponse,
    ContentPublishQueueHealthResponse,
    ContentPublishRetryRequest,
    ContentPublishMappingApplyRequest,
    ContentPublishMappingApplyResponse,
    ContentOpsHealthResponse,
    ContentQAVerifyRequest,
    ContentQAVerifyResponse,
    ContentVersionCreate,
    ContentVersionItem,
    ContentVersionListResponse,
    ContentVersionUpdate,
    PolicyCheckRequest,
    PolicyCheckResponse,
    PolicyRuleItem,
    PolicyRulesUpsertRequest,
    PublishJobItem,
    PublishJobsResponse,
    PublishPushRequest,
    PublishPushAcceptedResponse,
    PublishPackageRequest,
)
from app.services.content_ops import ContentOpsNotImplementedError

router = APIRouter(
    prefix="/content",
    tags=["content-ops"],
    dependencies=[Depends(require_analyst)],
)


def _raise_service_error(exc: Exception) -> None:
    if isinstance(exc, ContentOpsNotImplementedError):
        raise HTTPException(status_code=501, detail=str(exc)) from exc
    if isinstance(exc, ValueError):
        msg = str(exc)
        status_code = 404 if "not found" in msg.lower() else 400
        raise HTTPException(status_code=status_code, detail=msg) from exc
    raise HTTPException(status_code=500, detail=f"Content Ops request failed: {exc}") from exc


async def _enforce_scope(
    *,
    user: dict,
    marketplaces: list[str] | None = None,
    skus: list[str] | None = None,
) -> None:
    id_to_code = {
        str(k).strip().upper(): str(v.get("code") or "").strip().upper()
        for k, v in MARKETPLACE_REGISTRY.items()
        if isinstance(v, dict) and v.get("code")
    }
    code_to_id = {v: k for k, v in id_to_code.items()}

    def _market_tokens(value: str) -> set[str]:
        raw = str(value or "").strip().upper()
        if not raw:
            return set()
        out = {raw}
        if raw in id_to_code:
            out.add(id_to_code[raw])
        if raw in code_to_id:
            out.add(code_to_id[raw])
        return out

    allowed_marketplaces = [str(x).strip().upper() for x in (user.get("allowed_marketplaces") or []) if str(x).strip()]
    allowed_tokens: set[str] = set()
    for mp in allowed_marketplaces:
        allowed_tokens.update(_market_tokens(mp))
    allowed_brands = [str(x).strip().lower() for x in (user.get("allowed_brands") or []) if str(x).strip()]
    if allowed_marketplaces and marketplaces:
        for mp in marketplaces:
            mpv = str(mp or "").strip().upper()
            tokens = _market_tokens(str(mp or ""))
            if tokens and not (tokens & allowed_tokens):
                raise HTTPException(status_code=403, detail=f"Marketplace scope denied: {mpv}")
    if allowed_brands and skus:
        from app.services.content_ops import get_brand_for_sku

        for sku in skus:
            sv = str(sku or "").strip()
            if not sv:
                continue
            brand = await run_in_threadpool(get_brand_for_sku, sku=sv)
            bv = str(brand or "").strip().lower()
            if bv and bv not in allowed_brands:
                raise HTTPException(status_code=403, detail=f"Brand scope denied for sku={sv}")


@router.get("/tasks", response_model=ContentTaskListResponse)
async def get_content_tasks(
    status: Optional[str] = Query(default=None),
    owner: Optional[str] = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    type: Optional[str] = Query(default=None),
    priority: Optional[str] = Query(default=None),
    sku_search: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    _user=Depends(require_analyst),
):
    from app.services.content_ops import list_content_tasks

    try:
        if marketplace_id:
            await _enforce_scope(user=_user, marketplaces=[marketplace_id])
        return await run_in_threadpool(
            list_content_tasks,
            status=status,
            owner=owner,
            marketplace_id=marketplace_id,
            task_type=type,
            priority=priority,
            sku_search=sku_search,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/tasks", response_model=ContentTaskItem, status_code=201)
async def post_content_task(payload: ContentTaskCreate, _user=Depends(require_ops)):
    from app.services.content_ops import create_content_task

    try:
        await _enforce_scope(
            user=_user,
            marketplaces=[payload.marketplace_id] if payload.marketplace_id else [],
            skus=[payload.sku],
        )
        return await run_in_threadpool(create_content_task, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.patch("/tasks/{task_id}", response_model=ContentTaskItem)
async def patch_content_task(task_id: str, payload: ContentTaskUpdate, _user=Depends(require_ops)):
    from app.services.content_ops import update_content_task

    try:
        return await run_in_threadpool(
            update_content_task,
            task_id=task_id,
            payload=payload.model_dump(exclude_none=True),
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/tasks/bulk-update", response_model=ContentTaskBulkUpdateResponse)
async def post_content_tasks_bulk_update(
    payload: ContentTaskBulkUpdateRequest,
    _user=Depends(require_ops),
):
    from app.services.content_ops import bulk_update_content_tasks

    try:
        return await run_in_threadpool(bulk_update_content_tasks, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/{sku}/{marketplace_id}/versions", response_model=ContentVersionListResponse)
async def get_content_versions(sku: str, marketplace_id: str, _user=Depends(require_analyst)):
    from app.services.content_ops import list_versions

    try:
        await _enforce_scope(user=_user, marketplaces=[marketplace_id], skus=[sku])
        return await run_in_threadpool(list_versions, sku=sku, marketplace_id=marketplace_id)
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/{sku}/{marketplace_id}/versions", response_model=ContentVersionItem, status_code=201)
async def post_content_version(
    sku: str,
    marketplace_id: str,
    payload: ContentVersionCreate,
    _user=Depends(require_ops),
):
    from app.services.content_ops import create_version

    try:
        await _enforce_scope(user=_user, marketplaces=[marketplace_id], skus=[sku])
        return await run_in_threadpool(
            create_version,
            sku=sku,
            marketplace_id=marketplace_id,
            payload=payload.model_dump(),
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.put("/versions/{version_id}", response_model=ContentVersionItem)
async def put_content_version(
    version_id: str,
    payload: ContentVersionUpdate,
    _user=Depends(require_ops),
):
    from app.services.content_ops import update_version

    try:
        return await run_in_threadpool(update_version, version_id=version_id, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/versions/{version_id}/submit-review", response_model=ContentVersionItem)
async def submit_content_version_for_review(version_id: str, _user=Depends(require_ops)):
    from app.services.content_ops import submit_version_review

    try:
        return await run_in_threadpool(submit_version_review, version_id=version_id)
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/versions/{version_id}/approve", response_model=ContentVersionItem)
async def approve_content_version(version_id: str, _user=Depends(require_ops)):
    from app.services.content_ops import approve_version

    try:
        return await run_in_threadpool(approve_version, version_id=version_id)
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/policy/check", response_model=PolicyCheckResponse)
async def post_policy_check(payload: PolicyCheckRequest, _user=Depends(require_ops)):
    from app.services.content_ops import policy_check

    try:
        return await run_in_threadpool(policy_check, version_id=payload.version_id)
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/policy/rules", response_model=list[PolicyRuleItem])
async def get_policy_rules():
    from app.services.content_ops import list_policy_rules

    try:
        return await run_in_threadpool(list_policy_rules)
    except Exception as exc:
        _raise_service_error(exc)


@router.put("/policy/rules", response_model=list[PolicyRuleItem])
async def put_policy_rules(payload: PolicyRulesUpsertRequest, _user=Depends(require_ops)):
    from app.services.content_ops import upsert_policy_rules

    try:
        return await run_in_threadpool(upsert_policy_rules, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/assets/upload", response_model=AssetItem, status_code=201)
async def post_asset_upload(payload: AssetUploadRequest, _user=Depends(require_ops)):
    from app.services.content_ops import upload_asset

    try:
        return await run_in_threadpool(upload_asset, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/assets", response_model=AssetListResponse)
async def get_assets(
    sku: Optional[str] = Query(default=None),
    tag: Optional[str] = Query(default=None),
    role: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    from app.services.content_ops import list_assets

    try:
        return await run_in_threadpool(
            list_assets,
            sku=sku,
            tag=tag,
            role=role,
            status=status,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/assets/{asset_id}/link", response_model=AssetLinkItem, status_code=201)
async def post_asset_link(asset_id: str, payload: AssetLinkCreate, _user=Depends(require_ops)):
    from app.services.content_ops import link_asset

    try:
        return await run_in_threadpool(link_asset, asset_id=asset_id, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/publish/package", response_model=PublishJobItem)
async def post_publish_package(payload: PublishPackageRequest, _user=Depends(require_ops)):
    from app.services.content_ops import create_publish_package

    try:
        await _enforce_scope(user=_user, marketplaces=payload.marketplaces, skus=payload.sku_filter)
        return await run_in_threadpool(create_publish_package, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/publish/jobs", response_model=PublishJobsResponse)
async def get_publish_jobs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    from app.services.content_ops import list_publish_jobs

    try:
        return await run_in_threadpool(list_publish_jobs, page=page, page_size=page_size)
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/publish/push", response_model=PublishJobItem | PublishPushAcceptedResponse)
async def post_publish_push(payload: PublishPushRequest, _user=Depends(require_ops)):
    from app.services.content_ops import create_publish_push

    try:
        await _enforce_scope(user=_user, marketplaces=payload.marketplaces, skus=payload.sku_filter)
        return await run_in_threadpool(create_publish_push, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/{sku}/diff", response_model=ContentDiffResponse)
async def get_diff(
    sku: str,
    main: str = Query(...),
    target: str = Query(...),
    version_main: Optional[str] = Query(default=None),
    version_target: Optional[str] = Query(default=None),
):
    from app.services.content_ops import get_content_diff

    try:
        return await run_in_threadpool(
            get_content_diff,
            sku=sku,
            main=main,
            target=target,
            version_main=version_main,
            version_target=version_target,
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/{sku}/sync", response_model=ContentSyncResponse)
async def post_sync(sku: str, payload: ContentSyncRequest, _user=Depends(require_ops)):
    from app.services.content_ops import sync_content

    try:
        await _enforce_scope(user=_user, marketplaces=[payload.from_market, *payload.to_markets], skus=[sku])
        return await run_in_threadpool(sync_content, sku=sku, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/ai/generate", response_model=AIContentGenerateResponse)
async def post_ai_generate(payload: AIContentGenerateRequest, _user=Depends(require_ops)):
    from app.services.content_ops import ai_generate

    try:
        return await run_in_threadpool(ai_generate, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/onboard/preflight", response_model=ContentOnboardPreflightResponse)
async def post_onboard_preflight(payload: ContentOnboardPreflightRequest, _user=Depends(require_ops)):
    from app.services.content_ops import run_onboard_preflight

    try:
        await _enforce_scope(
            user=_user,
            marketplaces=[payload.main_market, *payload.target_markets],
            skus=payload.sku_list,
        )
        return await run_in_threadpool(run_onboard_preflight, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/qa/verify", response_model=ContentQAVerifyResponse)
async def post_qa_verify(payload: ContentQAVerifyRequest, _user=Depends(require_ops)):
    from app.services.content_ops import verify_content_quality

    try:
        return await run_in_threadpool(verify_content_quality, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/onboard/restrictions/check", response_model=ContentOnboardRestrictionResponse)
async def get_onboard_restrictions_check(asin: str, marketplace: str = Query(default="DE")):
    from app.services.content_ops import onboard_restrictions_check

    try:
        return await run_in_threadpool(onboard_restrictions_check, asin=asin, marketplace=marketplace)
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/onboard/catalog/search-by-ean", response_model=ContentOnboardCatalogResponse)
async def get_onboard_catalog_search_by_ean(ean: str, marketplace: str = Query(default="DE")):
    from app.services.content_ops import onboard_catalog_search_by_ean

    try:
        return await run_in_threadpool(onboard_catalog_search_by_ean, ean=ean, marketplace=marketplace)
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/publish/product-type-mappings", response_model=list[ContentProductTypeMapRule])
async def get_publish_product_type_mappings():
    from app.services.content_ops import list_product_type_mappings

    try:
        return await run_in_threadpool(list_product_type_mappings)
    except Exception as exc:
        _raise_service_error(exc)


@router.put("/publish/product-type-mappings", response_model=list[ContentProductTypeMapRule])
async def put_publish_product_type_mappings(payload: ContentProductTypeMapUpsertRequest, _user=Depends(require_ops)):
    from app.services.content_ops import upsert_product_type_mappings

    try:
        return await run_in_threadpool(upsert_product_type_mappings, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/publish/product-type-definitions", response_model=list[ContentProductTypeDefinitionItem])
async def get_publish_product_type_definitions(
    marketplace: Optional[str] = Query(default=None),
    product_type: Optional[str] = Query(default=None),
):
    from app.services.content_ops import list_product_type_definitions

    try:
        return await run_in_threadpool(
            list_product_type_definitions,
            marketplace=marketplace,
            product_type=product_type,
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/publish/product-type-definitions/refresh", response_model=ContentProductTypeDefinitionItem)
async def post_publish_product_type_definitions_refresh(
    payload: ContentProductTypeDefinitionRefreshRequest,
    _user=Depends(require_ops),
):
    from app.services.content_ops import refresh_product_type_definition

    try:
        return await run_in_threadpool(refresh_product_type_definition, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/publish/product-type-definitions/refresh-job", response_model=JobRunOut, status_code=202)
async def post_publish_product_type_definitions_refresh_job(
    payload: ContentProductTypeDefinitionRefreshRequest,
    _user=Depends(require_ops),
):
    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="content_refresh_product_type_definition",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params=payload.model_dump(),
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/publish/attribute-mappings", response_model=list[ContentAttributeMapRule])
async def get_publish_attribute_mappings():
    from app.services.content_ops import list_attribute_mappings

    try:
        return await run_in_threadpool(list_attribute_mappings)
    except Exception as exc:
        _raise_service_error(exc)


@router.put("/publish/attribute-mappings", response_model=list[ContentAttributeMapRule])
async def put_publish_attribute_mappings(
    payload: ContentAttributeMapUpsertRequest,
    _user=Depends(require_ops),
):
    from app.services.content_ops import upsert_attribute_mappings

    try:
        return await run_in_threadpool(upsert_attribute_mappings, payload=payload.model_dump())
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/publish/coverage", response_model=ContentPublishCoverageResponse)
async def get_publish_coverage(
    marketplaces: str = Query(..., description="Comma separated marketplace codes, e.g. DE,FR,IT"),
    selection: str = Query(default="approved"),
):
    from app.services.content_ops import get_publish_coverage

    try:
        return await run_in_threadpool(
            get_publish_coverage,
            marketplaces=marketplaces,
            selection=selection,
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/publish/mapping-suggestions", response_model=ContentPublishMappingSuggestionsResponse)
async def get_publish_mapping_suggestions(
    marketplaces: str = Query(..., description="Comma separated marketplace codes, e.g. DE,FR,IT"),
    selection: str = Query(default="approved"),
    limit: int = Query(default=100, ge=1, le=500),
):
    from app.services.content_ops import get_publish_mapping_suggestions

    try:
        return await run_in_threadpool(
            get_publish_mapping_suggestions,
            marketplaces=marketplaces,
            selection=selection,
            limit=limit,
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/publish/queue-health", response_model=ContentPublishQueueHealthResponse)
async def get_publish_queue_health(
    stale_minutes: int = Query(default=30, ge=5, le=240),
):
    from app.services.content_ops import get_publish_queue_health

    try:
        return await run_in_threadpool(
            get_publish_queue_health,
            stale_minutes=stale_minutes,
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/publish/circuit-breaker")
async def get_publish_circuit_breaker_state():
    from app.core.circuit_breaker import get_state
    return await get_state()


@router.post("/publish/circuit-breaker/reset")
async def post_publish_circuit_breaker_reset(_user=Depends(require_ops)):
    from app.core.circuit_breaker import force_reset
    await force_reset()
    return {"status": "reset", "detail": "Circuit breaker has been manually reset."}


@router.post("/publish/jobs/{job_id}/retry", response_model=PublishPushAcceptedResponse)
async def post_publish_job_retry(
    job_id: str,
    payload: ContentPublishRetryRequest,
    _user=Depends(require_ops),
):
    from app.services.content_ops import retry_publish_job

    try:
        return await run_in_threadpool(
            retry_publish_job,
            job_id=job_id,
            payload=payload.model_dump(),
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/publish/mapping-suggestions/apply", response_model=ContentPublishMappingApplyResponse)
async def post_publish_mapping_suggestions_apply(
    payload: ContentPublishMappingApplyRequest,
    _user=Depends(require_ops),
):
    from app.services.content_ops import apply_publish_mapping_suggestions

    try:
        return await run_in_threadpool(
            apply_publish_mapping_suggestions,
            payload=payload.model_dump(),
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.post("/publish/mapping-suggestions/apply-job", response_model=JobRunOut, status_code=202)
async def post_publish_mapping_suggestions_apply_job(
    payload: ContentPublishMappingApplyRequest,
    _user=Depends(require_ops),
):
    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="content_apply_publish_mapping_suggestions",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params=payload.model_dump(),
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/health", response_model=ContentOpsHealthResponse)
async def get_content_ops_health():
    from app.services.content_ops import get_content_ops_health

    try:
        return await run_in_threadpool(get_content_ops_health)
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/compliance/queue", response_model=ContentComplianceQueueResponse)
async def get_content_compliance_queue(
    severity: Optional[str] = Query(default="critical"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    from app.services.content_ops import list_compliance_queue

    try:
        return await run_in_threadpool(
            list_compliance_queue,
            severity=severity,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/impact", response_model=ContentImpactResponse)
async def get_content_impact(
    sku: str = Query(...),
    marketplace: str = Query(...),
    range: int = Query(default=14, ge=7, le=90),
    _user=Depends(require_analyst),
):
    from app.services.content_ops import get_content_impact

    try:
        await _enforce_scope(user=_user, marketplaces=[marketplace], skus=[sku])
        return await run_in_threadpool(
            get_content_impact,
            sku=sku,
            marketplace=marketplace,
            range_days=range,
        )
    except Exception as exc:
        _raise_service_error(exc)


@router.get("/data-quality", response_model=ContentDataQualityResponse)
async def get_content_data_quality():
    from app.services.content_ops import get_content_data_quality

    try:
        return await run_in_threadpool(get_content_data_quality)
    except Exception as exc:
        _raise_service_error(exc)
