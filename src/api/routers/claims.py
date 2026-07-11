"""
ScholarAssist — Claims API Router
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from opensearchpy import OpenSearch
import logging

from src.api.dependencies import get_opensearch_client
from src.config.settings import get_settings
from src.documents.storage import get_document
from src.claims.extractor import extract_claims
from src.claims.verifier import verify_claim

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/{document_id}/verify")
async def verify_document_claims(
    document_id: str,
    os_client: OpenSearch = Depends(get_opensearch_client)
):
    """
    Extracts claims from a previously uploaded document and verifies them against the index.
    """
    try:
        doc = get_document(document_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Document not found")
        
    settings = get_settings()
    index_name = settings.opensearch.index_alias

    # 1. Extract claims
    claims = extract_claims(doc["raw_text"])
    
    # 2. Verify claims
    verification_results = []
    for claim in claims:
        result = verify_claim(claim, os_client, index_name)
        verification_results.append(result)
        
    return {
        "document_id": document_id,
        "total_claims_extracted": len(claims),
        "results": verification_results
    }
