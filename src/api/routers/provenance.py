"""
ScholarAssist — Provenance Router

Endpoints for inspecting the data lineage of a given Golden Record.
"""

from fastapi import APIRouter, Depends, HTTPException
from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError

from src.api.dependencies import get_opensearch_client
from src.config.settings import get_settings

router = APIRouter()

@router.get("/{record_id}/provenance")
async def get_provenance(
    record_id: str,
    os_client: OpenSearch = Depends(get_opensearch_client)
):
    """
    Retrieve the field-level source provenance for a specific Golden Record.
    """
    settings = get_settings()
    try:
        response = os_client.get(
            index=settings.opensearch.index_alias, 
            id=record_id,
            _source_includes=["source_provenance", "merged_provider_ids"]
        )
        return {
            "golden_record_id": record_id,
            "provenance": response["_source"].get("source_provenance", {}),
            "merged_provider_ids": response["_source"].get("merged_provider_ids", [])
        }
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Record not found")
