"""
ScholarAssist — Authors Router

Endpoints for searching and retrieving author information across records.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from opensearchpy import OpenSearch
from typing import Optional

from src.api.dependencies import get_opensearch_client
from src.config.settings import get_settings

router = APIRouter()

@router.get("/search")
async def search_authors(
    name: str = Query(..., description="Author name to search for"),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    os_client: OpenSearch = Depends(get_opensearch_client),
):
    """
    Search for authors by name. This aggregates unique authors matching the query.
    """
    settings = get_settings()
    index_alias = settings.opensearch.index_alias

    # Use a nested query to search inside the authors array
    query_body = {
        "query": {
            "nested": {
                "path": "authors",
                "query": {
                    "match": {
                        "authors.name": name
                    }
                }
            }
        },
        # We use aggregations to return unique author entities rather than the papers they wrote
        "aggs": {
            "unique_authors": {
                "nested": {
                    "path": "authors"
                },
                "aggs": {
                    "filter_authors": {
                        "filter": {
                            "match": {
                                "authors.name": name
                            }
                        },
                        "aggs": {
                            "author_names": {
                                "terms": {
                                    "field": "authors.name.keyword",
                                    "size": size
                                },
                                "aggs": {
                                    "top_author_hits": {
                                        "top_hits": {
                                            "size": 1
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "size": 0 # We only care about the aggregations here
    }

    try:
        response = os_client.search(index=index_alias, body=query_body)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
    buckets = response.get("aggregations", {}).get("unique_authors", {}).get("filter_authors", {}).get("author_names", {}).get("buckets", [])
    
    results = []
    for bucket in buckets:
        # Extract the first hit from the top_hits aggregation for this author
        hit = bucket["top_author_hits"]["hits"]["hits"][0]["_source"]
        results.append(hit)

    return {
        "query": name,
        "results": results
    }
