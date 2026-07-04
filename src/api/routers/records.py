"""
ScholarAssist — Records Router

Endpoints for searching and retrieving unified Golden Records.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError

from src.api.dependencies import get_opensearch_client
from src.api.models.record import ScholarRecord, SearchResponse
from src.config.settings import get_settings

router = APIRouter()


@router.get("/search", response_model=SearchResponse)
async def search_records(
    q: str = Query(..., description="Full-text search query (Lucene syntax supported)"),
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(20, ge=1, le=100, description="Results per page"),
    year_start: Optional[int] = Query(None, description="Filter by publication year (start)"),
    year_end: Optional[int] = Query(None, description="Filter by publication year (end)"),
    os_client: OpenSearch = Depends(get_opensearch_client),
):
    """
    Search across all normalized Golden Records.
    Supports title, author, venue, and abstract matching.
    """
    settings = get_settings()
    index_alias = settings.opensearch.index_alias

    # Build OpenSearch Query DSL
    must_clauses = [{"query_string": {"query": q, "default_field": "title"}}]
    
    if year_start or year_end:
        range_clause = {}
        if year_start:
            range_clause["gte"] = year_start
        if year_end:
            range_clause["lte"] = year_end
        must_clauses.append({"range": {"publication_year": range_clause}})

    query_body = {
        "query": {"bool": {"must": must_clauses}},
        "from": (page - 1) * size,
        "size": size,
        "track_total_hits": True,
    }

    try:
        response = os_client.search(index=index_alias, body=query_body)
    except NotFoundError:
        raise HTTPException(status_code=503, detail="Search index is not currently available.")
    
    hits = response["hits"]["hits"]
    total = response["hits"]["total"]["value"]

    results = []
    for hit in hits:
        source = hit["_source"]
        results.append(ScholarRecord(**source))

    return SearchResponse(
        total_hits=total,
        page=page,
        size=size,
        results=results
    )


@router.get("/{record_id}", response_model=ScholarRecord)
async def get_record(
    record_id: str,
    os_client: OpenSearch = Depends(get_opensearch_client)
):
    """
    Retrieve a specific Golden Record by its ID.
    """
    settings = get_settings()
    try:
        response = os_client.get(index=settings.opensearch.index_alias, id=record_id)
        return ScholarRecord(**response["_source"])
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Record not found")
