from __future__ import annotations


class DHL24Error(Exception):
    """Base error for DHL24 WebAPI2."""


class DHL24ConfigError(DHL24Error):
    """Raised when DHL24 credentials are missing."""


class DHL24APIError(DHL24Error):
    """Raised when DHL24 API returns an error or invalid response."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        fault_code: str | None = None,
    ):
        self.status_code = status_code
        self.fault_code = fault_code
        super().__init__(message)
