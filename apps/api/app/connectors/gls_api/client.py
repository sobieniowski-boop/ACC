"""
GLS Group Track And Trace V1 Client.

Uses the public T&T API (GET-based) for parcel tracking:
  - GET /tracking/simple/trackids/{unitnos}     — track by parcel numbers (max 10)
  - GET /tracking/simple/references/{references} — track by references (max 10)
  - GET /tracking/deliveryinfo/parcelid/{unitno}  — delivery info (parcel shop etc.)
  - GET /tracking/events/codes                    — list all event codes

Base URLs:
  Production: https://api.gls-group.net/track-and-trace-v1/
  Sandbox:    https://api-sandbox.gls-group.net/track-and-trace-v1/

Rate limit: 500 requests/day (default). Contact GLS for higher limits.

All methods auto-authenticate via GLSAuth (OAuth2 client_credentials).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx
import structlog

from app.connectors.gls_api.auth import GLSAuth, GLSAuthError

log = structlog.get_logger(__name__)

_MAX_RETRIES = 3
_RETRY_DELAY_SEC = 2.0
_REQUEST_TIMEOUT_SEC = 30.0
# T&T V1 allows max 10 parcel IDs per request
_MAX_PARCELS_PER_REQUEST = 10


@dataclass
class GLSTrackingEvent:
    """Single tracking event from GLS Track & Trace V1."""

    timestamp: datetime | None
    description: str
    city: str
    postal_code: str
    country: str
    event_code: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "description": self.description,
            "city": self.city,
            "postal_code": self.postal_code,
            "country": self.country,
            "event_code": self.event_code,
        }


@dataclass
class GLSTrackingResult:
    """Tracking result for a parcel from T&T V1."""

    parcel_number: str
    unitno: str
    status: str  # PLANNEDPICKUP, INTRANSIT, DELIVERED, DELIVEREDPS, etc.
    status_datetime: str
    events: list[GLSTrackingEvent]
    error_code: str | None = None
    error_message: str | None = None

    @property
    def is_delivered(self) -> bool:
        return self.status.upper() in ("DELIVERED", "DELIVEREDPS")

    @property
    def latest_event(self) -> GLSTrackingEvent | None:
        return self.events[0] if self.events else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "parcel_number": self.parcel_number,
            "unitno": self.unitno,
            "status": self.status,
            "status_datetime": self.status_datetime,
            "is_delivered": self.is_delivered,
            "events_count": len(self.events),
            "events": [e.to_dict() for e in self.events],
            "error_code": self.error_code,
            "error_message": self.error_message,
        }


@dataclass
class GLSClient:
    """
    GLS Track And Trace V1 client.

    Wraps OAuth2 auth, HTTP transport and T&T response parsing.
    Auto-retries on 401 (token expired) and 429 (rate limit).
    """

    client_id: str = ""
    client_secret: str = ""
    sandbox: bool = False
    _auth: GLSAuth = field(init=False, repr=False)

    def __post_init__(self):
        if not self.client_id or not self.client_secret:
            from app.core.config import settings
            self.client_id = settings.GLS_CLIENT_ID
            self.client_secret = settings.GLS_CLIENT_SECRET
            self.sandbox = settings.GLS_SANDBOX

        self._auth = GLSAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            sandbox=self.sandbox,
        )

    @property
    def base_url(self) -> str:
        """Base URL for Track And Trace V1 API."""
        if self.sandbox:
            return "https://api-sandbox.gls-group.net/track-and-trace-v1"
        return "https://api.gls-group.net/track-and-trace-v1"

    @property
    def is_configured(self) -> bool:
        return bool(self.client_id and self.client_secret)

    # ──────────────────────────────────────────────────────────────
    # HTTP layer
    # ──────────────────────────────────────────────────────────────

    def _request(
        self,
        path: str,
        *,
        params: dict[str, str] | None = None,
        language: str = "EN",
    ) -> dict[str, Any]:
        """
        GET request to T&T V1 with auto-retry on 401/429/5xx.
        """
        url = f"{self.base_url}{path}"

        for attempt in range(1, _MAX_RETRIES + 1):
            headers = {
                **self._auth.get_auth_header(),
                "Accept": "application/json",
                "Accept-Language": language,
            }

            try:
                with httpx.Client(timeout=_REQUEST_TIMEOUT_SEC) as http:
                    response = http.get(url, params=params, headers=headers)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 401 and attempt < _MAX_RETRIES:
                    log.warning("gls.client.401_retry", attempt=attempt, path=path)
                    self._auth.invalidate()
                    time.sleep(0.5)
                    continue

                if response.status_code == 429 and attempt < _MAX_RETRIES:
                    retry_after = int(response.headers.get("Retry-After", _RETRY_DELAY_SEC))
                    log.warning("gls.client.429_retry", attempt=attempt, retry_after=retry_after, path=path)
                    time.sleep(retry_after)
                    continue

                if response.status_code >= 500 and attempt < _MAX_RETRIES:
                    log.warning("gls.client.5xx_retry", status=response.status_code, attempt=attempt, path=path)
                    time.sleep(_RETRY_DELAY_SEC * attempt)
                    continue

                body = response.text[:500]
                log.error("gls.client.error", status=response.status_code, path=path, body=body)
                raise GLSAPIError(
                    f"GLS API error: HTTP {response.status_code} on {path} — {body}",
                    status_code=response.status_code,
                )

            except httpx.HTTPError as exc:
                if attempt < _MAX_RETRIES:
                    log.warning("gls.client.http_retry", attempt=attempt, path=path, error=str(exc))
                    time.sleep(_RETRY_DELAY_SEC * attempt)
                    continue
                raise GLSAPIError(f"GLS API HTTP error: {exc}") from exc

        raise GLSAPIError(f"GLS API: max retries exceeded for {path}")

    # ──────────────────────────────────────────────────────────────
    # Tracking API — Track And Trace V1
    # ──────────────────────────────────────────────────────────────

    def track(self, parcel_number: str, *, language: str = "EN") -> GLSTrackingResult:
        """
        Track a single parcel by its GLS parcel number (unitno).

        GET /tracking/simple/trackids/{unitnos}
        """
        if not self.is_configured:
            raise GLSAPIError("GLS API not configured — set GLS_CLIENT_ID/SECRET in .env")

        log.debug("gls.track", parcel=parcel_number)
        data = self._request(f"/tracking/simple/trackids/{parcel_number}", language=language)
        results = self._parse_tnt_response(data)
        if not results:
            raise GLSAPIError(f"No tracking data for parcel {parcel_number}", status_code=404)
        return results[0]

    def track_multi(
        self,
        parcel_numbers: list[str],
        *,
        language: str = "EN",
    ) -> list[GLSTrackingResult]:
        """
        Track up to 10 parcels in a single request.

        GET /tracking/simple/trackids/{unitno1},{unitno2},...
        """
        if not self.is_configured:
            raise GLSAPIError("GLS API not configured — set GLS_CLIENT_ID/SECRET in .env")
        if len(parcel_numbers) > _MAX_PARCELS_PER_REQUEST:
            raise GLSAPIError(f"Max {_MAX_PARCELS_PER_REQUEST} parcels per request")

        joined = ",".join(parcel_numbers)
        data = self._request(f"/tracking/simple/trackids/{joined}", language=language)
        return self._parse_tnt_response(data)

    def track_batch(
        self,
        parcel_numbers: list[str],
        *,
        language: str = "EN",
    ) -> dict[str, GLSTrackingResult | None]:
        """
        Track any number of parcels. Chunks into groups of 10 (API limit).

        Returns dict[parcel_number → result_or_None].
        """
        if not self.is_configured:
            raise GLSAPIError("GLS API not configured — set GLS_CLIENT_ID/SECRET in .env")

        results: dict[str, GLSTrackingResult | None] = {pn: None for pn in parcel_numbers}

        for i in range(0, len(parcel_numbers), _MAX_PARCELS_PER_REQUEST):
            chunk = parcel_numbers[i : i + _MAX_PARCELS_PER_REQUEST]
            try:
                chunk_results = self.track_multi(chunk, language=language)
                for r in chunk_results:
                    results[r.parcel_number] = r
            except GLSAPIError as exc:
                log.warning("gls.track_batch.chunk_error", chunk=chunk, error=str(exc))
            # Respect rate limits
            if i + _MAX_PARCELS_PER_REQUEST < len(parcel_numbers):
                time.sleep(0.5)

        return results

    def track_by_reference(
        self,
        references: list[str],
        *,
        language: str = "EN",
    ) -> list[GLSTrackingResult]:
        """
        Track parcels by references (parcel number, track ID, notification card ID).

        GET /tracking/simple/references/{ref1},{ref2},...
        Max 10 references. One reference may return multiple parcels.
        """
        if not self.is_configured:
            raise GLSAPIError("GLS API not configured — set GLS_CLIENT_ID/SECRET in .env")
        if len(references) > _MAX_PARCELS_PER_REQUEST:
            raise GLSAPIError(f"Max {_MAX_PARCELS_PER_REQUEST} references per request")

        joined = ",".join(references)
        data = self._request(f"/tracking/simple/references/{joined}", language=language)
        return self._parse_tnt_response(data)

    def get_delivery_info(
        self,
        unitno: str,
        postal_code: str,
    ) -> dict[str, Any]:
        """
        Get delivery info (e.g. parcel shop) for a parcel.

        GET /tracking/deliveryinfo/parcelid/{unitno}?originaldestinationpostalcode=...
        """
        if not self.is_configured:
            raise GLSAPIError("GLS API not configured — set GLS_CLIENT_ID/SECRET in .env")

        return self._request(
            f"/tracking/deliveryinfo/parcelid/{unitno}",
            params={"originaldestinationpostalcode": postal_code},
        )

    def get_event_codes(self, *, language: str = "EN") -> list[dict[str, str]]:
        """
        Get all track & trace event codes with descriptions.

        GET /tracking/events/codes
        """
        data = self._request("/tracking/events/codes", language=language)
        return data.get("eventsCodes", [])

    # ──────────────────────────────────────────────────────────────
    # Response parsing
    # ──────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_tnt_response(data: dict[str, Any]) -> list[GLSTrackingResult]:
        """Parse T&T V1 ParcelsResponseDTO into structured results."""
        results: list[GLSTrackingResult] = []

        for parcel in data.get("parcels", []):
            # Error-only parcel (no unitno)
            error_code = parcel.get("errorCode")
            error_message = parcel.get("errorMessage")

            events: list[GLSTrackingEvent] = []
            for ev in parcel.get("events", []):
                ts = None
                ts_str = ev.get("eventDateTime")
                if ts_str:
                    try:
                        ts = datetime.fromisoformat(ts_str)
                    except (ValueError, TypeError):
                        pass

                events.append(GLSTrackingEvent(
                    timestamp=ts,
                    description=ev.get("description", ""),
                    city=ev.get("city", ""),
                    postal_code=ev.get("postalCode", ""),
                    country=ev.get("country", ""),
                    event_code=ev.get("code", ""),
                ))

            results.append(GLSTrackingResult(
                parcel_number=parcel.get("requested", ""),
                unitno=parcel.get("unitno", ""),
                status=parcel.get("status", "UNKNOWN"),
                status_datetime=parcel.get("statusDateTime", ""),
                events=events,
                error_code=error_code,
                error_message=error_message,
            ))

        return results

    # ──────────────────────────────────────────────────────────────
    # Health check
    # ──────────────────────────────────────────────────────────────

    def health_check(self) -> dict[str, Any]:
        """Verify GLS T&T API connectivity by fetching event codes."""
        if not self.is_configured:
            return {
                "ok": False,
                "error": "GLS API not configured — set GLS_CLIENT_ID + GLS_CLIENT_SECRET in .env",
            }

        try:
            codes = self.get_event_codes()
            return {
                "ok": True,
                "sandbox": self.sandbox,
                "base_url": self.base_url,
                "event_codes_count": len(codes),
            }
        except (GLSAuthError, GLSAPIError) as exc:
            return {
                "ok": False,
                "error": str(exc),
                "sandbox": self.sandbox,
            }


class GLSAPIError(Exception):
    """Raised when a GLS API call fails."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code
