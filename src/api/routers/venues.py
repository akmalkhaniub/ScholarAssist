"""
ScholarAssist — Venues Router

Endpoints for searching and retrieving venue/journal information.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from opensearchpy import OpenSearch

from src.api.dependencies import get_opensearch_client
from src.config.settings import get_settings

router = APIRouter()

@router.get("/search")
async def search_venues(
    name: str = Query(..., description="Venue name to search for"),
    size: int = Query(20, ge=1, le=100),
    os_client: OpenSearch = Depends(get_opensearch_client),
):
    """
    Search for venues/journals by name using OpenSearch aggregations to return unique venues.
    """
    settings = get_settings()
    index_alias = settings.opensearch.index_alias

    query_body = {
        "query": {
            "match": {
                "venue.name": name
            }
        },
        "aggs": {
            "unique_venues": {
                "terms": {
                    "field": "venue.name.keyword",
                    "size": size
                },
                "aggs": {
                    "top_venue_hits": {
                        "top_hits": {
                            "_source": ["venue"],
                            "size": 1
                        }
                    }
                }
            }
        },
        "size": 0
    }

    try:
        response = os_client.search(index=index_alias, body=query_body)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
    buckets = response.get("aggregations", {}).get("unique_venues", {}).get("buckets", [])
    
    results = []
    for bucket in buckets:
        hit = bucket["top_venue_hits"]["hits"]["hits"][0]["_source"]["venue"]
        # Add a paper count for this venue based on the doc_count
        hit["paper_count"] = bucket["doc_count"]
        results.append(hit)

    return {
        "query": name,
        "results": results
    }
