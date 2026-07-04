"""
ScholarAssist — Data Quality Router

Endpoints for returning aggregate data quality metrics from OpenSearch.
"""

from fastapi import APIRouter, Depends, HTTPException
from opensearchpy import OpenSearch

from src.api.dependencies import get_opensearch_client
from src.config.settings import get_settings

router = APIRouter()

@router.get("/metrics")
async def get_quality_metrics(os_client: OpenSearch = Depends(get_opensearch_client)):
    """
    Retrieve real-time data quality metrics from the indexed Golden Records.
    Metrics include the total record count and the number of records missing DOIs or titles.
    """
    settings = get_settings()
    index_alias = settings.opensearch.index_alias

    query_body = {
        "aggs": {
            "missing_doi": {
                "missing": {"field": "doi"}
            },
            "missing_title": {
                "missing": {"field": "title.keyword"}
            },
            "missing_publication_year": {
                "missing": {"field": "publication_year"}
            }
        },
        "size": 0
    }

    try:
        response = os_client.search(index=index_alias, body=query_body)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
    total_docs = response["hits"]["total"]["value"]
    aggs = response.get("aggregations", {})

    return {
        "total_records": total_docs,
        "metrics": {
            "null_rates": {
                "doi_null_rate": (aggs.get("missing_doi", {}).get("doc_count", 0) / total_docs * 100) if total_docs > 0 else 0,
                "title_null_rate": (aggs.get("missing_title", {}).get("doc_count", 0) / total_docs * 100) if total_docs > 0 else 0,
                "publication_year_null_rate": (aggs.get("missing_publication_year", {}).get("doc_count", 0) / total_docs * 100) if total_docs > 0 else 0,
            }
        }
    }
