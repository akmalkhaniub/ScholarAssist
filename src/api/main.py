"""
ScholarAssist — FastAPI Entrypoint

Configures the FastAPI application, registers routers, middleware, and
exception handlers.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import structlog

from src.api.middleware import RateLimitMiddleware, LogRequestsMiddleware
from src.api.routers import records, citations, provenance, authors, venues, health, quality
from src.config.settings import get_settings

logger = structlog.get_logger(__name__)

settings = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Startup and shutdown events."""
    logger.info("api.startup", version=settings.api.version, env=settings.env.value)
    
    # Initialize OpenSearch and Redis connections here if needed globally.
    # Currently handled via dependencies injection per request.
    
    yield
    
    logger.info("api.shutdown")

app = FastAPI(
    title=settings.api.title,
    description=settings.api.description,
    version=settings.api.version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# --- Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.api.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(LogRequestsMiddleware)

# --- Routers ---
app.include_router(records.router, prefix="/v1/records", tags=["Records"])
app.include_router(citations.router, prefix="/v1/records", tags=["Citations"])
app.include_router(provenance.router, prefix="/v1/records", tags=["Provenance"])
app.include_router(authors.router, prefix="/v1/authors", tags=["Authors"])
app.include_router(venues.router, prefix="/v1/venues", tags=["Venues"])
app.include_router(quality.router, prefix="/v1/quality", tags=["Data Quality"])
app.include_router(health.router, prefix="/v1", tags=["Health & Status"])
