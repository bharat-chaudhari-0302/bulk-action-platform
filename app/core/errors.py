"""Domain exceptions and RFC 7807 (problem+json) error responses."""

from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.core.logging import get_logger

log = get_logger(__name__)


class AppError(Exception):
    """Base class for expected, client-facing failures."""

    status_code: int = status.HTTP_400_BAD_REQUEST
    title: str = "Request failed"
    code: str = "app_error"

    def __init__(self, detail: str, **extra: Any) -> None:
        super().__init__(detail)
        self.detail = detail
        self.extra = extra


class NotFoundError(AppError):
    status_code = status.HTTP_404_NOT_FOUND
    title = "Resource not found"
    code = "not_found"


class ValidationError(AppError):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    title = "Invalid request"
    code = "validation_error"


class ConflictError(AppError):
    status_code = status.HTTP_409_CONFLICT
    title = "Conflicting state"
    code = "conflict"


class RateLimitError(AppError):
    status_code = status.HTTP_429_TOO_MANY_REQUESTS
    title = "Rate limit exceeded"
    code = "rate_limited"

    def __init__(self, detail: str, retry_after_seconds: float = 60.0, **extra: Any) -> None:
        super().__init__(detail, **extra)
        self.retry_after_seconds = retry_after_seconds


def _problem(
    status_code: int, title: str, code: str, detail: str, **extra: Any
) -> dict[str, Any]:
    return {"type": f"about:blank#{code}", "title": title, "status": status_code,
            "code": code, "detail": detail, **extra}


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _app_error(_: Request, exc: AppError) -> JSONResponse:
        headers = {}
        if isinstance(exc, RateLimitError):
            headers["Retry-After"] = str(int(exc.retry_after_seconds))
        return JSONResponse(
            status_code=exc.status_code,
            content=_problem(exc.status_code, exc.title, exc.code, exc.detail, **exc.extra),
            headers=headers,
        )

    @app.exception_handler(RequestValidationError)
    async def _request_validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=_problem(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                "Invalid request",
                "validation_error",
                "The request body failed schema validation.",
                errors=jsonable_encoder(exc.errors()),
            ),
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled_error", path=request.url.path, error=str(exc))
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_problem(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Internal server error",
                "internal_error",
                "An unexpected error occurred.",
            ),
        )
