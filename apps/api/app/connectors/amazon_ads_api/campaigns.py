"""Amazon Ads API — Campaigns (SP, SB, SD).

Fetches campaign metadata from all three ad types:
- Sponsored Products (SP): /sp/campaigns/list  (v3)
- Sponsored Brands  (SB): /sb/v4/campaigns/list
- Sponsored Display (SD): /sd/campaigns  (v3)

All use POST with body filters (the new v3/v4 endpoints).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import structlog

from app.connectors.amazon_ads_api.client import AdsAPIClient

log = structlog.get_logger(__name__)


@dataclass
class AdsCampaignInfo:
    """Normalized campaign data across SP/SB/SD."""
    campaign_id: str
    campaign_name: str
    ad_type: str  # SP, SB, SD
    state: str  # ENABLED, PAUSED, ARCHIVED
    targeting_type: Optional[str]  # AUTO, MANUAL (SP only)
    daily_budget: Optional[float]
    start_date: Optional[str]
    end_date: Optional[str]


async def list_sp_campaigns(profile_id: int) -> list[AdsCampaignInfo]:
    """List all Sponsored Products campaigns via POST /sp/campaigns/list."""
    client = AdsAPIClient(profile_id=profile_id)
    campaigns: list[AdsCampaignInfo] = []
    next_token: Optional[str] = None

    # SP v3 requires versioned Accept/Content-Type
    sp_headers = {
        "Accept": "application/vnd.spCampaign.v3+json",
        "Content-Type": "application/vnd.spCampaign.v3+json",
    }

    while True:
        body: dict[str, Any] = {
            "maxResults": 100,
            "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
        }
        if next_token:
            body["nextToken"] = next_token

        try:
            resp = await client.post("/sp/campaigns/list", body=body, extra_headers=sp_headers)
        except Exception as exc:
            log.error("ads_api.sp_campaigns.error", profile_id=profile_id, error=str(exc))
            break

        for c in resp.get("campaigns", []):
            budget = c.get("budget", {})
            campaigns.append(AdsCampaignInfo(
                campaign_id=str(c["campaignId"]),
                campaign_name=c.get("name", ""),
                ad_type="SP",
                state=c.get("state", "ENABLED"),
                targeting_type=c.get("targetingType"),  # AUTO or MANUAL
                daily_budget=budget.get("budget"),
                start_date=c.get("startDate"),
                end_date=c.get("endDate"),
            ))

        next_token = resp.get("nextToken")
        if not next_token:
            break

    log.info("ads_api.sp_campaigns.fetched", profile_id=profile_id, count=len(campaigns))
    return campaigns


async def list_sb_campaigns(profile_id: int) -> list[AdsCampaignInfo]:
    """List all Sponsored Brands campaigns (v4 with fallback to legacy GET)."""
    client = AdsAPIClient(profile_id=profile_id)
    campaigns: list[AdsCampaignInfo] = []

    # --- Try v4 POST endpoint first ---
    sb_headers = {
        "Accept": "application/vnd.sbCampaignResource.v4+json",
        "Content-Type": "application/vnd.sbCampaignResource.v4+json",
    }
    next_token: Optional[str] = None
    v4_ok = True

    while v4_ok:
        body: dict[str, Any] = {
            "maxResults": 100,
            "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
        }
        if next_token:
            body["nextToken"] = next_token

        try:
            resp = await client.post(
                "/sb/v4/campaigns/list", body=body, extra_headers=sb_headers,
            )
        except Exception as exc:
            err_msg = str(exc)
            if "404" in err_msg or "Not Found" in err_msg:
                v4_ok = False
                break
            log.error("ads_api.sb_campaigns.v4_error", profile_id=profile_id, error=err_msg)
            v4_ok = False
            break

        for c in resp.get("campaigns", []):
            raw_budget = c.get("budget")
            if isinstance(raw_budget, dict):
                daily_budget = raw_budget.get("budget")
            else:
                daily_budget = raw_budget  # float or None
            campaigns.append(AdsCampaignInfo(
                campaign_id=str(c["campaignId"]),
                campaign_name=c.get("name", ""),
                ad_type="SB",
                state=c.get("state", "ENABLED"),
                targeting_type=None,
                daily_budget=daily_budget,
                start_date=c.get("startDate"),
                end_date=c.get("endDate"),
            ))

        next_token = resp.get("nextToken")
        if not next_token:
            break

    # --- Fallback: legacy GET /sb/campaigns ---
    if not v4_ok and not campaigns:
        try:
            legacy = await client.get(
                "/sb/campaigns",
                params={"stateFilter": "enabled,paused,archived"},
            )
            if isinstance(legacy, list):
                for c in legacy:
                    campaigns.append(AdsCampaignInfo(
                        campaign_id=str(c["campaignId"]),
                        campaign_name=c.get("name", ""),
                        ad_type="SB",
                        state=str(c.get("state", "enabled")).upper(),
                        targeting_type=None,
                        daily_budget=c.get("budget"),
                        start_date=c.get("startDate"),
                        end_date=c.get("endDate"),
                    ))
        except Exception as exc2:
            log.error("ads_api.sb_campaigns.legacy_error", profile_id=profile_id, error=str(exc2))

    log.info("ads_api.sb_campaigns.fetched", profile_id=profile_id, count=len(campaigns))
    return campaigns


async def list_sd_campaigns(profile_id: int) -> list[AdsCampaignInfo]:
    """List all Sponsored Display campaigns via GET /sd/campaigns."""
    client = AdsAPIClient(profile_id=profile_id)
    campaigns: list[AdsCampaignInfo] = []

    try:
        resp = await client.get("/sd/campaigns", params={"stateFilter": "enabled,paused,archived"})
    except Exception as exc:
        log.error("ads_api.sd_campaigns.error", profile_id=profile_id, error=str(exc))
        return campaigns

    if isinstance(resp, list):
        for c in resp:
            campaigns.append(AdsCampaignInfo(
                campaign_id=str(c["campaignId"]),
                campaign_name=c.get("name", ""),
                ad_type="SD",
                state=c.get("state", "enabled").upper(),
                targeting_type=c.get("tactic"),  # T00020 = remarketing, etc.
                daily_budget=c.get("budget"),
                start_date=c.get("startDate"),
                end_date=c.get("endDate"),
            ))

    log.info("ads_api.sd_campaigns.fetched", profile_id=profile_id, count=len(campaigns))
    return campaigns


async def list_all_campaigns(profile_id: int) -> list[AdsCampaignInfo]:
    """Fetch campaigns from all three ad types for a given profile."""
    import asyncio
    sp, sb, sd = await asyncio.gather(
        list_sp_campaigns(profile_id),
        list_sb_campaigns(profile_id),
        list_sd_campaigns(profile_id),
        return_exceptions=True,
    )

    all_campaigns: list[AdsCampaignInfo] = []
    for result, label in [(sp, "SP"), (sb, "SB"), (sd, "SD")]:
        if isinstance(result, Exception):
            log.error("ads_api.campaigns.error", ad_type=label, error=str(result))
        else:
            all_campaigns.extend(result)

    log.info("ads_api.campaigns.total", profile_id=profile_id, count=len(all_campaigns))
    return all_campaigns
