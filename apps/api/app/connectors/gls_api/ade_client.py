"""
GLS Poland ADE WebAPI2 SOAP Client.

SOAP (Document/Literal) client for GLS Poland parcel management:
  - Session-based auth (adeLogin / adeLogout), 30-min TTL, max 10 concurrent
  - Parcel search (adePickup_ParcelNumberSearch) — track Polish parcels
  - Tracking ID (adeTrackID_Get) — get tracking URL/ID
  - POD retrieval (adePOD_Get) — proof of delivery PDF (base64)
  - Preparing box (insert consignment, get labels)
  - Pickup (create, get consignment details, labels)
  - Services query (allowed services, max COD, parcel weights)

Requires: pip install zeep

Config (.env):
  GLS_ADE_WSDL_URL=https://adeplus.gls-poland.com/adeplus/pm1/ade_webapi2.php?wsdl
  GLS_ADE_USERNAME=...
  GLS_ADE_PASSWORD=...
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

import structlog
from zeep import Client as ZeepClient
from zeep.exceptions import Fault, TransportError

from app.core.config import settings

log = structlog.get_logger(__name__)

# Session TTL: 30 min from GLS docs, refresh 2 min early
_SESSION_TTL_SEC = 30 * 60
_SESSION_REFRESH_MARGIN_SEC = 2 * 60


class ADEError(Exception):
    """Base error for GLS ADE WebAPI."""

    def __init__(self, message: str, fault_code: str | None = None):
        self.message = message
        self.fault_code = fault_code
        super().__init__(message)


class ADEAuthError(ADEError):
    """Authentication/session error."""


@dataclass
class _CachedSession:
    """In-memory ADE session cache."""

    session_id: str
    created_at: float  # time.monotonic()

    @property
    def is_valid(self) -> bool:
        elapsed = time.monotonic() - self.created_at
        return elapsed < (_SESSION_TTL_SEC - _SESSION_REFRESH_MARGIN_SEC)


class GLSADEClient:
    """
    GLS Poland ADE WebAPI2 SOAP client.

    Thread-safe session management with auto-login/re-login.
    Rate limit: max 20 calls/sec (enforced by GLS server-side).

    Usage:
        client = GLSADEClient()
        result = client.search_parcel("12345678901")
        client.close()
    """

    def __init__(
        self,
        wsdl_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ):
        self._wsdl_url = wsdl_url or settings.GLS_ADE_WSDL_URL
        self._username = username or settings.GLS_ADE_USERNAME
        self._password = password or settings.GLS_ADE_PASSWORD
        self._session: _CachedSession | None = None
        self._lock = Lock()
        self._zeep: ZeepClient | None = None

    def _get_zeep(self) -> ZeepClient:
        if self._zeep is None:
            self._zeep = ZeepClient(self._wsdl_url)
        return self._zeep

    def _get_session(self) -> str:
        """Return a valid session ID, logging in if needed. Thread-safe."""
        with self._lock:
            if self._session and self._session.is_valid:
                return self._session.session_id
            return self._login()

    def _login(self) -> str:
        """Authenticate and cache session ID."""
        zeep = self._get_zeep()
        try:
            result = zeep.service.adeLogin(
                user_name=self._username,
                user_password=self._password,
            )
            session_id = result.session if hasattr(result, "session") else str(result)
            self._session = _CachedSession(
                session_id=session_id,
                created_at=time.monotonic(),
            )
            log.info("gls_ade.login_ok", username=self._username)
            return session_id
        except Fault as exc:
            log.error("gls_ade.login_fault", fault=str(exc))
            raise ADEAuthError(f"ADE login failed: {exc}", fault_code=str(exc.code) if exc.code else None)
        except TransportError as exc:
            log.error("gls_ade.login_transport_error", error=str(exc))
            raise ADEError(f"ADE transport error: {exc}")

    def _call(self, method_name: str, **kwargs: Any) -> Any:
        """
        Call an ADE method with automatic session injection and retry on expired session.
        """
        session = self._get_session()
        zeep = self._get_zeep()
        method = getattr(zeep.service, method_name)
        try:
            return method(session=session, **kwargs)
        except Fault as exc:
            fault_str = str(exc).lower()
            # Session expired — re-login once and retry
            if "session" in fault_str or "login" in fault_str or "auth" in fault_str:
                log.warning("gls_ade.session_expired", method=method_name)
                with self._lock:
                    self._session = None
                session = self._get_session()
                return method(session=session, **kwargs)
            raise ADEError(f"ADE SOAP fault in {method_name}: {exc}", fault_code=str(exc.code) if exc.code else None)
        except TransportError as exc:
            raise ADEError(f"ADE transport error in {method_name}: {exc}")

    # ── Tracking / Search ─────────────────────────────────────────

    def search_parcel(self, parcel_number: str) -> dict[str, Any]:
        """
        Search for a parcel by number (adePickup_ParcelNumberSearch).

        Returns consignment data for the parcel.
        """
        result = self._call("adePickup_ParcelNumberSearch", number=parcel_number)
        return self._serialize(result)

    def get_track_id(self, parcel_number: str) -> str:
        """
        Get tracking ID/URL for a parcel (adeTrackID_Get).

        Returns the tracking ID string.
        """
        result = self._call("adeTrackID_Get", number=parcel_number)
        if hasattr(result, "track_id"):
            return result.track_id
        return str(result)

    def get_pod(self, parcel_number: str) -> str | None:
        """
        Get Proof of Delivery PDF (base64-encoded) for a parcel.

        Returns base64 string of PDF, or None if not available.
        """
        result = self._call("adePOD_Get", number=parcel_number)
        if hasattr(result, "file_pdf"):
            return result.file_pdf if result.file_pdf else None
        return str(result) if result else None

    # ── Preparing Box ─────────────────────────────────────────────

    def preparing_box_insert(self, consign_data: dict[str, Any]) -> dict[str, Any]:
        """
        Insert a consignment into preparing box.

        consign_data should match cConsign structure (rname1, rcountry, etc.)
        """
        zeep = self._get_zeep()
        ns = self._wsdl_url
        consign_type = zeep.get_type(f"{{{ns}}}cConsign")
        consign_obj = consign_type(**consign_data)
        result = self._call("adePreparingBox_Insert", consign_prep_data=consign_obj)
        return self._serialize(result)

    def preparing_box_get_consign_ids(self, id_start: int = 0) -> list[int]:
        """Get IDs of consignments in preparing box."""
        result = self._call("adePreparingBox_GetConsignIDs", id_start=id_start)
        return self._extract_ids(result)

    def preparing_box_get_consign(self, consign_id: int) -> dict[str, Any]:
        """Get consignment details from preparing box."""
        result = self._call("adePreparingBox_GetConsign", id=consign_id)
        return self._serialize(result)

    def preparing_box_get_labels(self, consign_id: int, mode: str = "one_label_on_a4_lt_pdf") -> str:
        """Get consignment labels from preparing box. Returns base64 PDF."""
        result = self._call("adePreparingBox_GetConsignLabels", id=consign_id, mode=mode)
        return self._serialize(result)

    def preparing_box_delete(self, consign_id: int) -> Any:
        """Delete a consignment from preparing box."""
        return self._call("adePreparingBox_DeleteConsign", id=consign_id)

    # ── Pickup ────────────────────────────────────────────────────

    def pickup_create(self, desc: str = "") -> dict[str, Any]:
        """
        Create (close) a pickup from current preparing box.

        Moves all consignments from preparing box to pickup.
        """
        result = self._call("adePickup_Create", desc=desc)
        return self._serialize(result)

    def pickup_get_ids(self, id_start: int = 0) -> list[int]:
        """Get pickup IDs."""
        result = self._call("adePickup_GetIDs", id_start=id_start)
        return self._extract_ids(result)

    def pickup_get(self, pickup_id: int) -> dict[str, Any]:
        """Get pickup details."""
        result = self._call("adePickup_Get", id=pickup_id)
        return self._serialize(result)

    def pickup_get_consign_ids(self, pickup_id: int, id_start: int = 0) -> list[int]:
        """Get consignment IDs within a pickup."""
        result = self._call("adePickup_GetConsignIDs", id=pickup_id, id_start=id_start)
        return self._extract_ids(result)

    def pickup_get_consign(self, consign_id: int) -> dict[str, Any]:
        """Get a consignment from a pickup."""
        result = self._call("adePickup_GetConsign", id=consign_id)
        return self._serialize(result)

    def pickup_get_labels(self, pickup_id: int, mode: str = "one_label_on_a4_lt_pdf") -> str:
        """Get all labels for a pickup. Returns base64 PDF."""
        result = self._call("adePickup_GetLabels", id=pickup_id, mode=mode)
        return self._serialize(result)

    def pickup_get_consign_labels(self, consign_id: int, mode: str = "one_label_on_a4_lt_pdf") -> str:
        """Get labels for a specific consignment in a pickup."""
        result = self._call("adePickup_GetConsignLabels", id=consign_id, mode=mode)
        return self._serialize(result)

    def pickup_get_receipt(self, pickup_id: int) -> str:
        """Get pickup receipt (base64 PDF)."""
        result = self._call("adePickup_GetReceipt", id=pickup_id)
        return self._serialize(result)

    # ── Services ──────────────────────────────────────────────────

    def get_allowed_services(self) -> dict[str, Any]:
        """Get list of services allowed for this account."""
        result = self._call("adeServices_GetAllowed")
        return self._serialize(result)

    def get_max_cod(self) -> dict[str, Any]:
        """Get maximum COD amounts."""
        result = self._call("adeServices_GetMaxCOD")
        return self._serialize(result)

    def get_max_parcel_weights(self) -> dict[str, Any]:
        """Get maximum parcel weights by service."""
        result = self._call("adeServices_GetMaxParcelWeights")
        return self._serialize(result)

    # ── Utility ───────────────────────────────────────────────────

    def get_city(self, zipcode: str) -> str | None:
        """Lookup city name by Polish ZIP code."""
        result = self._call("adeZip_GetCity", zipcode=zipcode)
        if hasattr(result, "city"):
            return result.city
        return str(result) if result else None

    def order_courier(self) -> dict[str, Any]:
        """Order courier pickup (adeCourier_Order)."""
        result = self._call("adeCourier_Order")
        return self._serialize(result)

    # ── Session Management ────────────────────────────────────────

    def close(self) -> None:
        """Explicitly logout and clear session."""
        if self._session and self._session.is_valid:
            try:
                zeep = self._get_zeep()
                zeep.service.adeLogout(session=self._session.session_id)
                log.info("gls_ade.logout_ok")
            except Exception:
                pass  # Best-effort logout
        with self._lock:
            self._session = None

    def health_check(self) -> dict[str, Any]:
        """
        Check ADE WebAPI connectivity: login → get allowed services → logout.
        """
        try:
            session = self._get_session()
            services = self.get_allowed_services()
            return {
                "ok": True,
                "wsdl_url": self._wsdl_url,
                "username": self._username,
                "session_active": True,
                "services": services,
            }
        except ADEError as exc:
            return {
                "ok": False,
                "wsdl_url": self._wsdl_url,
                "username": self._username,
                "error": str(exc),
            }

    # ── Helpers ───────────────────────────────────────────────────

    @staticmethod
    def _serialize(obj: Any) -> Any:
        """Convert zeep response objects to plain dicts/lists."""
        if obj is None:
            return None
        if isinstance(obj, (str, int, float, bool)):
            return obj
        if isinstance(obj, (list, tuple)):
            return [GLSADEClient._serialize(item) for item in obj]
        if hasattr(obj, "__dict__"):
            return {
                k: GLSADEClient._serialize(v)
                for k, v in obj.__dict__.items()
                if not k.startswith("_")
            }
        # zeep CompoundValue — iterate over elements
        try:
            from collections import OrderedDict
            if isinstance(obj, OrderedDict):
                return {k: GLSADEClient._serialize(v) for k, v in obj.items()}
        except Exception:
            pass
        return str(obj)

    @staticmethod
    def _extract_ids(result: Any) -> list[int]:
        """Extract list of integer IDs from a zeep array response."""
        if result is None:
            return []
        if isinstance(result, (list, tuple)):
            return [int(x) for x in result]
        if hasattr(result, "items"):
            items = result.items
            if items is None:
                return []
            return [int(x) for x in items]
        return []
