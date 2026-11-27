"""
ScholarAssist — API Middleware
"""
from collections import defaultdict, deque
import math
import time
from typing import Callable

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import structlog

from src.config.settings import get_settings

logger = structlog.get_logger(__name__)


class LogRequestsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time

        logger.info(
            "api.request",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            process_time_ms=round(process_time * 1000, 2),
        )
        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Sliding-window per-client rate limiter enforcing configured request quotas.
    Exempts health checks and attaches standard X-RateLimit-* headers.
    """

    def __init__(
        self,
        app,
        max_requests: int | None = None,
        window_seconds: int | None = None,
    ) -> None:
        super().__init__(app)
        api_cfg = get_settings().api
        self.max_requests = max_requests if max_requests is not None else api_cfg.rate_limit_requests
        self.window_seconds = window_seconds if window_seconds is not None else api_cfg.rate_limit_window_seconds
        self._buckets: dict[str, deque[float]] = defaultdict(deque)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.url.path in ("/v1/health", "/docs", "/redoc", "/openapi.json"):
            return await call_next(request)

        client_key = (
            request.headers.get("X-API-Key")
            or (request.client.host if request.client else "anonymous")
        )
        now = time.monotonic()
        cutoff = now - self.window_seconds
        window = self._buckets[client_key]
        while window and window[0] <= cutoff:
            window.popleft()

        if len(window) >= self.max_requests:
            retry_after = max(1, math.ceil(window[0] + self.window_seconds - now))
            logger.warning("api.rate_limited", client=client_key, path=request.url.path)
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded. Please retry later."},
                headers={
                    "Retry-After": str(retry_after),
                    "X-RateLimit-Limit": str(self.max_requests),
                    "X-RateLimit-Remaining": "0",
                },
            )

        window.append(now)
        remaining = max(0, self.max_requests - len(window))
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(self.max_requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        return response
