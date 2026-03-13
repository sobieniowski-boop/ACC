"""
GLS Group OAuth2 Authentication for ShipIT Farm API.

Implements RFC 6749 client_credentials flow against GLS Authentication API v2.
Tokens are cached in-memory and auto-refreshed 5 minutes before expiry.

Endpoints:
  Sandbox:    https://api-sandbox.gls-group.net/oauth2/v2/token
  Production: https://api.gls-group.net/oauth2/v2/token
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Optional

import httpx
import structlog

log = structlog.get_logger(__name__)

# Token refresh margin — renew 5 min before actual expiry
_REFRESH_MARGIN_SEC = 300

# GLS API base URLs
GLS_SANDBOX_BASE = "https://api-sandbox.gls-group.net"
GLS_PRODUCTION_BASE = "https://api.gls-group.net"


@dataclass
class _CachedToken:
    """In-memory token cache entry."""

    access_token: str
    token_type: str
    expires_at: float  # time.monotonic() based
    scope: str = ""

    @property
    def is_valid(self) -> bool:
        return time.monotonic() < (self.expires_at - _REFRESH_MARGIN_SEC)


@dataclass
class GLSAuth:
    """
    GLS OAuth2 client_credentials authenticator.

    Args:
        client_id:     GLS API App client ID
        client_secret: GLS API App client secret
        sandbox:       Use sandbox environment (default: False)
    """

    client_id: str
    client_secret: str
    sandbox: bool = False
    _token: Optional[_CachedToken] = field(default=None, init=False, repr=False)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    @property
    def base_url(self) -> str:
        return GLS_SANDBOX_BASE if self.sandbox else GLS_PRODUCTION_BASE

    @property
    def token_url(self) -> str:
        return f"{self.base_url}/oauth2/v2/token"

    def get_access_token(self) -> str:
        """
        Return a valid access token, refreshing if needed.

        Thread-safe — uses a lock to prevent concurrent token requests.
        """
        with self._lock:
            if self._token and self._token.is_valid:
                return self._token.access_token
            return self._request_token()

    def _request_token(self) -> str:
        """
        Request a new OAuth2 access token from GLS.

        Uses HTTP Basic Auth (client_id:client_secret) with
        grant_type=client_credentials as form parameter.
        """
        log.info(
            "gls.auth.token_request",
            url=self.token_url,
            sandbox=self.sandbox,
        )

        try:
            with httpx.Client(timeout=30.0) as http:
                response = http.post(
                    self.token_url,
                    auth=(self.client_id, self.client_secret),
                    data={"grant_type": "client_credentials"},
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Accept": "application/json",
                    },
                )

            if response.status_code != 200:
                body = response.text[:500]
                log.error(
                    "gls.auth.token_error",
                    status=response.status_code,
                    body=body,
                )
                raise GLSAuthError(
                    f"GLS token request failed: HTTP {response.status_code} — {body}"
                )

            data = response.json()
            access_token = data["access_token"]
            expires_in = int(data.get("expires_in", 14400))  # default 4h
            token_type = data.get("token_type", "Bearer")
            scope = data.get("scope", "")

            self._token = _CachedToken(
                access_token=access_token,
                token_type=token_type,
                expires_at=time.monotonic() + expires_in,
                scope=scope,
            )

            log.info(
                "gls.auth.token_ok",
                expires_in=expires_in,
                scope=scope,
            )
            return access_token

        except httpx.HTTPError as exc:
            log.error("gls.auth.http_error", error=str(exc))
            raise GLSAuthError(f"GLS auth HTTP error: {exc}") from exc

    def invalidate(self) -> None:
        """Force token refresh on next call."""
        with self._lock:
            self._token = None
            log.debug("gls.auth.token_invalidated")

    def get_auth_header(self) -> dict[str, str]:
        """Return Authorization header dict ready for httpx requests."""
        token = self.get_access_token()
        return {"Authorization": f"Bearer {token}"}


class GLSAuthError(Exception):
    """Raised when GLS authentication fails."""
