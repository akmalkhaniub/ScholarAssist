"""
ScholarAssist — Citations Router

Endpoints for retrieving citation networks (papers citing a given paper, or referenced by a given paper).
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError

from src.api.dependencies import get_opensearch_client
from src.api.models.record import ScholarRecord, SearchResponse
from src.config.settings import get_settings

router = APIRouter()

@router.get("/{record_id}/citations", response_model=SearchResponse)
async def get_citations(
    record_id: str,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    os_client: OpenSearch = Depends(get_opensearch_client)
):
    """
    Retrieve records that cite the given record_id.
    This searches for the given record_id in the 'references' array of other documents.
    """
    settings = get_settings()
    index_alias = settings.opensearch.index_alias

    # We want to find documents where `references` contains `record_id`
    query_body = {
        "query": {
            "term": {
                "references": record_id
            }
        },
        "from": (page - 1) * size,
        "size": size,
        "track_total_hits": True,
    }

    try:
        response = os_client.search(index=index_alias, body=query_body)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
    hits = response["hits"]["hits"]
    total = response["hits"]["total"]["value"]

    results = [ScholarRecord(**hit["_source"]) for hit in hits]

    return SearchResponse(
        total_hits=total,
        page=page,
        size=size,
        results=results
    )
