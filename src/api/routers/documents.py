"""
ScholarAssist — Documents API Router
"""

from fastapi import APIRouter, UploadFile, File, HTTPException
import logging

from src.documents.parser import extract_text
from src.documents.storage import save_document, get_document

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/upload")
async def upload_document(file: UploadFile = File(...)):
    """
    Upload a research paper draft (PDF, TXT, DOCX).
    The text is extracted and stored, returning a document ID for further processing.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename missing")
        
    try:
        contents = await file.read()
        extracted_text = extract_text(contents, file.filename)
        
        # Save to mock S3 storage
        document_id = save_document(file.filename, extracted_text)
        
        return {
            "document_id": document_id,
            "filename": file.filename,
            "message": "Document uploaded and parsed successfully.",
            "text_length": len(extracted_text)
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error processing upload: {e}")
        raise HTTPException(status_code=500, detail="Internal server error during upload")

@router.get("/{document_id}")
async def get_document_metadata(document_id: str):
    """
    Retrieve document metadata.
    """
    try:
        doc = get_document(document_id)
        return {
            "document_id": document_id,
            "filename": doc["filename"],
            "status": doc["status"]
        }
    except KeyError:
        raise HTTPException(status_code=404, detail="Document not found")
