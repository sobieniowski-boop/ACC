"""Ustandaryzowane wyjątki i schemat odpowiedzi błędów.

Wszystkie endpointy powinny korzystać z tych wyjątków zamiast
ręcznego zwracania JSONResponse z różnymi formatami.

Klasy wyjątków:
    AppException          — bazowy wyjątek aplikacyjny
    NotFoundError         — 404
    ValidationError       — 422
    ConflictError         — 409
    ForbiddenError        — 403
    ServiceUnavailableError — 503
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from pydantic import BaseModel
from starlette.responses import JSONResponse

from app.platform.middleware import correlation_id_var


# ── Schemat odpowiedzi błędów ──


class ErrorDetail(BaseModel):
    """Pojedynczy szczegół błędu (np. pole walidacji)."""
    loc: list[str] | None = None
    msg: str
    type: str | None = None


class ErrorBody(BaseModel):
    """Główna struktura błędu w odpowiedzi API."""
    code: str
    message: str
    details: list[ErrorDetail] | None = None
    correlation_id: str | None = None


class ErrorResponse(BaseModel):
    """Koperta odpowiedzi błędu — zawsze pod kluczem 'error'."""
    error: ErrorBody


# ── Hierarchia wyjątków ──


class AppException(Exception):
    """Bazowy wyjątek aplikacyjny z kodem HTTP i strukturyzowanym błędem."""

    def __init__(
        self,
        status_code: int = 500,
        code: str = "INTERNAL_ERROR",
        message: str = "Wewnętrzny błąd serwera",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details
        super().__init__(message)


class NotFoundError(AppException):
    def __init__(
        self,
        message: str = "Zasób nie został znaleziony",
        code: str = "NOT_FOUND",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(status_code=404, code=code, message=message, details=details)


class ValidationError(AppException):
    def __init__(
        self,
        message: str = "Błąd walidacji danych wejściowych",
        code: str = "VALIDATION_ERROR",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(status_code=422, code=code, message=message, details=details)


class ConflictError(AppException):
    def __init__(
        self,
        message: str = "Konflikt — zasób już istnieje lub jest w niespójnym stanie",
        code: str = "CONFLICT",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(status_code=409, code=code, message=message, details=details)


class ForbiddenError(AppException):
    def __init__(
        self,
        message: str = "Brak uprawnień do tej operacji",
        code: str = "FORBIDDEN",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(status_code=403, code=code, message=message, details=details)


class ServiceUnavailableError(AppException):
    def __init__(
        self,
        message: str = "Usługa tymczasowo niedostępna",
        code: str = "SERVICE_UNAVAILABLE",
        details: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(status_code=503, code=code, message=message, details=details)


# ── Handler rejestrowany w FastAPI ──


def _build_error_response(exc: AppException) -> JSONResponse:
    """Buduje ustandaryzowaną odpowiedź JSON z wyjątku AppException."""
    cid = correlation_id_var.get() or None

    detail_items: list[ErrorDetail] | None = None
    if exc.details:
        detail_items = [
            ErrorDetail(
                loc=d.get("loc"),
                msg=d.get("msg", ""),
                type=d.get("type"),
            )
            for d in exc.details
        ]

    body = ErrorResponse(
        error=ErrorBody(
            code=exc.code,
            message=exc.message,
            details=detail_items,
            correlation_id=cid,
        )
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=body.model_dump(exclude_none=True),
    )


async def app_exception_handler(_request: Request, exc: AppException) -> JSONResponse:
    """FastAPI exception handler dla AppException i podklas."""
    return _build_error_response(exc)


def register_exception_handlers(app: FastAPI) -> None:
    """Rejestruje handlery wyjątków w instancji FastAPI."""
    app.add_exception_handler(AppException, app_exception_handler)  # type: ignore[arg-type]
