"""
GLS Cost Center Posting API v1 Client.

OData REST service for creating Cost Center postings in SAP.
Write requests require a CSRF token (fetched automatically).

Base URLs:
  Production: https://api.gls-group.net/finance/billing/v1/cost-center-posting
  Sandbox:    https://api-sandbox.gls-group.net/finance/billing/v1/cost-center-posting
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import httpx
import structlog

from app.connectors.gls_api.auth import GLSAuth, GLSAuthError

log = structlog.get_logger(__name__)

_REQUEST_TIMEOUT_SEC = 30.0
_MAX_RETRIES = 2


@dataclass
class CostCenterItem:
    """Single line item for a cost center posting."""

    sender_cost_center: str      # SendCctr — e.g. "6400"
    receiver_cost_center: str    # RecCctr — e.g. "1600"
    amount: str                  # ValueTcur — e.g. "100.15"
    currency: str                # TransCurrIso — e.g. "EUR"
    cost_element: str            # CostElem — e.g. "628002"
    item_text: str = ""          # SegText — e.g. "DE01"

    def to_odata(self) -> dict[str, str]:
        d: dict[str, str] = {
            "SendCctr": self.sender_cost_center,
            "RecCctr": self.receiver_cost_center,
            "ValueTcur": self.amount,
            "TransCurrIso": self.currency,
            "CostElem": self.cost_element,
        }
        if self.item_text:
            d["SegText"] = self.item_text
        return d


@dataclass
class CostCenterPostingResult:
    """Result of a cost center posting."""

    success: bool
    transaction_id: str
    request_id: str | None = None
    doc_no: str | None = None
    raw: dict[str, Any] = field(default_factory=lambda: {})
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "transaction_id": self.transaction_id,
            "request_id": self.request_id,
            "doc_no": self.doc_no,
            "error": self.error,
        }


@dataclass
class GLSCostCenterClient:
    """
    GLS Cost Center Posting API v1 client.

    Handles OAuth2 auth + CSRF token flow required for OData write operations.
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
        if self.sandbox:
            return "https://api-sandbox.gls-group.net/finance/billing/v1/cost-center-posting"
        return "https://api.gls-group.net/finance/billing/v1/cost-center-posting"

    @property
    def is_configured(self) -> bool:
        return bool(self.client_id and self.client_secret)

    # ──────────────────────────────────────────────────────────────
    # CSRF token
    # ──────────────────────────────────────────────────────────────

    def _fetch_csrf_token(self, http: httpx.Client) -> tuple[str, httpx.Cookies]:
        """
        Fetch CSRF token via GET with X-CSRF-Token: fetch header.

        Returns (csrf_token, cookies) — cookies must be sent with the POST.
        """
        headers = {
            **self._auth.get_auth_header(),
            "X-CSRF-Token": "fetch",
            "Accept": "application/json",
        }

        response = http.get(f"{self.base_url}/ ", headers=headers)

        if response.status_code not in (200, 204):
            raise CostCenterAPIError(
                f"CSRF token fetch failed: HTTP {response.status_code} — {response.text[:300]}",
                status_code=response.status_code,
            )

        csrf_token = response.headers.get("x-csrf-token", "")
        if not csrf_token:
            raise CostCenterAPIError("CSRF token not found in response headers")

        log.debug("gls.cost_center.csrf_ok", token_len=len(csrf_token))
        return csrf_token, response.cookies

    # ──────────────────────────────────────────────────────────────
    # Post transaction
    # ──────────────────────────────────────────────────────────────

    def post_transaction(
        self,
        *,
        transaction_id: str,
        process_code: str,
        username: str,
        doc_date: str,
        posting_date: str,
        items: list[CostCenterItem],
        doc_header_text: str = "",
    ) -> CostCenterPostingResult:
        """
        Create a Cost Center posting in SAP.

        Args:
            transaction_id: Unique ID (max 32 chars), e.g. "ACC_20260305_001"
            process_code:   Process code (max 10 chars), e.g. "CT"
            username:       SAP username (max 12 chars)
            doc_date:       Document date, ISO format "YYYY-MM-DD"
            posting_date:   Posting date, ISO format "YYYY-MM-DD"
            items:          Line items (at least one)
            doc_header_text: Optional header text (max 50 chars)
        """
        if not self.is_configured:
            raise CostCenterAPIError("GLS API not configured")
        if not items:
            raise CostCenterAPIError("At least one line item is required")

        header: dict[str, str] = {
            "Username": username,
            "DocdateExt": doc_date,
            "PostgdateExt": posting_date,
        }
        if doc_header_text:
            header["DocHdrTx"] = doc_header_text

        payload: dict[str, Any] = {
            "TransactionId": transaction_id,
            "ProcessCode": process_code,
            "Header": header,
            "Items": [item.to_odata() for item in items],
        }

        log.info(
            "gls.cost_center.post",
            transaction_id=transaction_id,
            items_count=len(items),
        )

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                with httpx.Client(timeout=_REQUEST_TIMEOUT_SEC) as http:
                    csrf_token, cookies = self._fetch_csrf_token(http)

                    headers = {
                        **self._auth.get_auth_header(),
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "X-CSRF-Token": csrf_token,
                    }

                    response = http.post(
                        f"{self.base_url}/TransactionSet",
                        json=payload,
                        headers=headers,
                        cookies=cookies,
                    )

                if response.status_code == 201:
                    data = response.json()
                    d = data.get("d", data)
                    return CostCenterPostingResult(
                        success=True,
                        transaction_id=d.get("TransactionId", transaction_id),
                        request_id=d.get("RequestId"),
                        doc_no=d.get("DocNo"),
                        raw=data,
                    )

                if response.status_code == 401 and attempt < _MAX_RETRIES:
                    log.warning("gls.cost_center.401_retry", attempt=attempt)
                    self._auth.invalidate()
                    time.sleep(0.5)
                    continue

                body = response.text[:500]
                log.error(
                    "gls.cost_center.error",
                    status=response.status_code,
                    body=body,
                )
                return CostCenterPostingResult(
                    success=False,
                    transaction_id=transaction_id,
                    error=f"HTTP {response.status_code}: {body}",
                    raw={"status": response.status_code, "body": body},
                )

            except httpx.HTTPError as exc:
                if attempt < _MAX_RETRIES:
                    log.warning("gls.cost_center.http_retry", attempt=attempt, error=str(exc))
                    time.sleep(1)
                    continue
                return CostCenterPostingResult(
                    success=False,
                    transaction_id=transaction_id,
                    error=f"HTTP error: {exc}",
                )

        return CostCenterPostingResult(
            success=False,
            transaction_id=transaction_id,
            error="Max retries exceeded",
        )

    # ──────────────────────────────────────────────────────────────
    # Health check
    # ──────────────────────────────────────────────────────────────

    def health_check(self) -> dict[str, Any]:
        """Check connectivity by fetching a CSRF token."""
        if not self.is_configured:
            return {"ok": False, "error": "GLS API not configured"}

        try:
            with httpx.Client(timeout=_REQUEST_TIMEOUT_SEC) as http:
                csrf_token, _ = self._fetch_csrf_token(http)
            return {
                "ok": True,
                "sandbox": self.sandbox,
                "base_url": self.base_url,
                "csrf_token_length": len(csrf_token),
            }
        except (GLSAuthError, CostCenterAPIError) as exc:
            return {"ok": False, "error": str(exc), "sandbox": self.sandbox}


class CostCenterAPIError(Exception):
    """Raised when Cost Center Posting API call fails."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code
