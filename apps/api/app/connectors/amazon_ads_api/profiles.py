"""Amazon Ads API — Profiles.

Each Amazon Ads account has one profile per marketplace (country).
Profile ID is required as Amazon-Advertising-API-Scope header for all requests.

GET /v2/profiles → list of profiles for the authorized advertiser.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import structlog

from app.connectors.amazon_ads_api.client import AdsAPIClient
from app.core.config import MARKETPLACE_REGISTRY

log = structlog.get_logger(__name__)

# Mapping from Ads API countryCode → SP-API marketplace_id
_COUNTRY_TO_MARKETPLACE: dict[str, str] = {}
for mp_id, info in MARKETPLACE_REGISTRY.items():
    _COUNTRY_TO_MARKETPLACE[info["code"]] = mp_id
# Amazon Ads API returns "UK" instead of "GB"
_COUNTRY_TO_MARKETPLACE["UK"] = _COUNTRY_TO_MARKETPLACE.get("GB", "")


@dataclass
class AdsProfile:
    """Represents an Amazon Advertising profile."""
    profile_id: int
    country_code: str
    marketplace_id: str  # SP-API marketplace ID
    currency: str
    account_type: str  # seller or vendor
    account_name: str
    account_id: str  # Amazon seller/vendor ID


async def list_profiles() -> list[AdsProfile]:
    """Fetch all advertising profiles for the authorized account.

    Returns profiles mapped to our MARKETPLACE_REGISTRY.
    """
    client = AdsAPIClient()  # no profile_id needed for this call
    raw = await client.get("/v2/profiles")

    profiles: list[AdsProfile] = []
    for p in raw:
        country_code = p.get("countryCode", "").upper()
        # Map to SP-API marketplace ID
        marketplace_id = _COUNTRY_TO_MARKETPLACE.get(country_code)
        if not marketplace_id:
            log.debug("ads_api.profile.unmapped_country", country=country_code, profile_id=p["profileId"])
            continue

        account_info = p.get("accountInfo", {})
        profiles.append(AdsProfile(
            profile_id=p["profileId"],
            country_code=country_code,
            marketplace_id=marketplace_id,
            currency=p.get("currencyCode", "EUR"),
            account_type=account_info.get("type", "seller"),
            account_name=account_info.get("name", ""),
            account_id=account_info.get("id", ""),
        ))
        log.info(
            "ads_api.profile.found",
            profile_id=p["profileId"],
            country=country_code,
            marketplace=marketplace_id,
        )

    log.info("ads_api.profiles.total", count=len(profiles))
    return profiles


async def get_profile_map() -> dict[str, AdsProfile]:
    """Return {marketplace_id: AdsProfile} mapping.

    Makes it easy to look up profile_id when you know which marketplace to query.
    """
    profiles = await list_profiles()
    return {p.marketplace_id: p for p in profiles}
