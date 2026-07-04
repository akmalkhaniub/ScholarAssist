"""
ScholarAssist — Health Router

Endpoints for checking system health.
"""

from fastapi import APIRouter, Depends, HTTPException
from opensearchpy import OpenSearch
import structlog

from src.api.dependencies import get_opensearch_client
from src.config.settings import get_settings

router = APIRouter()
logger = structlog.get_logger(__name__)

@router.get("/health")
async def check_health(os_client: OpenSearch = Depends(get_opensearch_client)):
    """
    Check the health of the API and backing services (OpenSearch).
    """
    status = {
        "status": "ok",
        "api_version": get_settings().api.version,
        "services": {
            "opensearch": "unknown"
        }
    }
    
    try:
        # Check OpenSearch connection
        if os_client.ping():
            status["services"]["opensearch"] = "ok"
        else:
            status["status"] = "degraded"
            status["services"]["opensearch"] = "failed"
            logger.warning("api.healthcheck.opensearch_ping_failed")
    except Exception as e:
        status["status"] = "degraded"
        status["services"]["opensearch"] = "error"
        status["services"]["opensearch_error"] = str(e)
        logger.error("api.healthcheck.opensearch_error", error=str(e))
        
    if status["status"] != "ok":
        # We can return 503 if we want load balancers to take the node out of rotation
        # but 200 with degraded status is also common for informational endpoints.
        pass

    return status
