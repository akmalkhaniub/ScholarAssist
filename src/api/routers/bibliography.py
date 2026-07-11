"""
ScholarAssist — Bibliography API Router
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from opensearchpy import OpenSearch
from typing import List

from src.api.dependencies import get_opensearch_client
from src.config.settings import get_settings
from src.bibliography.formatter import format_citations

router = APIRouter()

@router.get("/generate")
async def generate_bibliography(
    record_ids: str = Query(..., description="Comma-separated list of record IDs"),
    style: str = Query("apa", description="Citation style (apa, mla, ieee, chicago)"),
    os_client: OpenSearch = Depends(get_opensearch_client)
):
    """
    Generate a formatted bibliography for a list of record IDs.
    """
    id_list = [rid.strip() for rid in record_ids.split(",") if rid.strip()]
    if not id_list:
        raise HTTPException(status_code=400, detail="No valid record IDs provided")
        
    settings = get_settings()
    index_alias = settings.opensearch.index_alias

    # Fetch all records in a single query
    query_body = {
        "query": {
            "terms": {
                "_id": id_list
            }
        },
        "size": len(id_list)
    }

    try:
        response = os_client.search(index=index_alias, body=query_body)
        hits = response["hits"]["hits"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
    if not hits:
        raise HTTPException(status_code=404, detail="None of the requested records were found")

    # Extract source documents
    records = [hit["_source"] for hit in hits]
    
    # Format
    formatted_citations = format_citations(records, style)
    
    return {
        "style": style,
        "citations_found": len(formatted_citations),
        "requested": len(id_list),
        "bibliography": formatted_citations
    }
