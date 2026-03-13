"""Account Hub API — Multi-seller support — Sprint 25-26.

Seller account CRUD, credential vault management (no plaintext
returned), permission management, and scheduling status.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from app.intelligence import account_hub as ah

router = APIRouter(prefix="/account-hub", tags=["account-hub"])


# ── Pydantic schemas ─────────────────────────────────────────────────

class SellerCreate(BaseModel):
    seller_id: str = Field(..., max_length=30)
    name: str = Field(..., max_length=200)
    company_name: Optional[str] = None
    marketplace_ids: Optional[list[str]] = None
    primary_marketplace: Optional[str] = None
    region: str = Field(default="eu", max_length=10)
    notes: Optional[str] = None


class SellerUpdate(BaseModel):
    name: Optional[str] = None
    company_name: Optional[str] = None
    marketplace_ids: Optional[list[str]] = None
    primary_marketplace: Optional[str] = None
    region: Optional[str] = None
    status: Optional[str] = None
    notes: Optional[str] = None


class CredentialStore(BaseModel):
    credential_type: str = Field(..., max_length=20)
    credential_key: str = Field(..., max_length=80)
    plaintext_value: str = Field(..., min_length=1)
    expires_at: Optional[str] = None


class PermissionGrant(BaseModel):
    user_email: str = Field(..., max_length=200)
    permission_level: str = Field(default="read_only", max_length=20)
    granted_by: str = Field(..., max_length=120)


class PermissionRevoke(BaseModel):
    user_email: str = Field(..., max_length=200)


# ── Dashboard ────────────────────────────────────────────────────────

@router.get("/dashboard")
async def get_dashboard():
    try:
        return await run_in_threadpool(ah.get_account_hub_dashboard)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Seller Accounts ──────────────────────────────────────────────────

@router.get("/sellers")
async def list_sellers(
    status: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        return await run_in_threadpool(
            ah.list_seller_accounts, status=status, page=page, page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sellers/{seller_account_id}")
async def get_seller(seller_account_id: int):
    try:
        result = await run_in_threadpool(ah.get_seller_account, seller_account_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Seller account not found")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sellers", status_code=201)
async def create_seller(body: SellerCreate):
    try:
        return await run_in_threadpool(
            ah.create_seller_account,
            seller_id=body.seller_id,
            name=body.name,
            company_name=body.company_name,
            marketplace_ids=body.marketplace_ids,
            primary_marketplace=body.primary_marketplace,
            region=body.region,
            notes=body.notes,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.patch("/sellers/{seller_account_id}")
async def update_seller(seller_account_id: int, body: SellerUpdate):
    try:
        result = await run_in_threadpool(
            ah.update_seller_account,
            seller_account_id,
            name=body.name,
            company_name=body.company_name,
            marketplace_ids=body.marketplace_ids,
            primary_marketplace=body.primary_marketplace,
            region=body.region,
            status=body.status,
            notes=body.notes,
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Seller account not found")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Credential Vault ─────────────────────────────────────────────────

@router.get("/sellers/{seller_account_id}/credentials")
async def get_credentials(seller_account_id: int):
    """List credential metadata (no plaintext values returned)."""
    try:
        return await run_in_threadpool(ah.list_credentials, seller_account_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sellers/{seller_account_id}/credentials")
async def store_credential(seller_account_id: int, body: CredentialStore):
    try:
        return await run_in_threadpool(
            ah.store_credential,
            seller_account_id=seller_account_id,
            credential_type=body.credential_type,
            credential_key=body.credential_key,
            plaintext_value=body.plaintext_value,
            expires_at=body.expires_at,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/credentials/{credential_id}", status_code=204)
async def revoke_credential(credential_id: int):
    try:
        ok = await run_in_threadpool(ah.revoke_credential, credential_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Credential not found")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sellers/{seller_account_id}/credentials/validate")
async def validate_credentials(seller_account_id: int):
    try:
        return await run_in_threadpool(ah.validate_seller_credentials, seller_account_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Permissions ──────────────────────────────────────────────────────

@router.get("/sellers/{seller_account_id}/permissions")
async def get_seller_permissions(seller_account_id: int):
    try:
        return await run_in_threadpool(ah.list_seller_permissions, seller_account_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sellers/{seller_account_id}/permissions")
async def grant_permission(seller_account_id: int, body: PermissionGrant):
    try:
        return await run_in_threadpool(
            ah.grant_permission,
            user_email=body.user_email,
            seller_account_id=seller_account_id,
            permission_level=body.permission_level,
            granted_by=body.granted_by,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/sellers/{seller_account_id}/permissions")
async def revoke_permission_endpoint(seller_account_id: int, body: PermissionRevoke):
    try:
        ok = await run_in_threadpool(
            ah.revoke_permission, user_email=body.user_email, seller_account_id=seller_account_id,
        )
        if not ok:
            raise HTTPException(status_code=404, detail="Permission not found")
        return {"revoked": True}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/users/{user_email}/permissions")
async def get_user_permissions(user_email: str):
    try:
        return await run_in_threadpool(ah.list_user_permissions, user_email)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Scheduler Status ─────────────────────────────────────────────────

@router.get("/scheduler-status")
async def get_scheduler_status():
    try:
        return await run_in_threadpool(ah.get_seller_scheduler_status)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
