from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from typing import List, Dict, Any

from src.documents.storage import get_document
from src.documents.exporter import generate_annotated_document

router = APIRouter()

class ExportRequest(BaseModel):
    claims: List[Dict[str, Any]]
    citation_style: str = "APA"

@router.post("/{document_id}/export", response_class=PlainTextResponse)
def export_document(document_id: str, request: ExportRequest):
    """
    Exports a document with inline citations and a bibliography based on verified claims.
    """
    try:
        doc = get_document(document_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Document not found")
        
    annotated_text = generate_annotated_document(
        raw_text=doc["raw_text"],
        verified_claims=request.claims,
        citation_style=request.citation_style
    )
    
    return annotated_text
