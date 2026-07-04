"""
ScholarAssist — API Middleware
"""
import time
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
import structlog

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
    Dummy rate limit middleware. In production, this uses Redis to enforce
    limits based on API keys or IP addresses.
    """
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # TODO: Implement Redis token bucket here
        return await call_next(request)
