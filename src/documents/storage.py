"""
ScholarAssist — Document Storage

Handles the secure persistence of uploaded user documents.
"""

import uuid
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

# In a real environment, this would be an S3 bucket interface.
# For this prototype, we'll use an in-memory dictionary.
_MOCK_STORAGE = {}

def save_document(filename: str, raw_text: str, user_id: str = "anonymous") -> str:
    """
    Saves the extracted document text to storage and returns a unique document ID.
    """
    document_id = f"doc_{uuid.uuid4().hex[:12]}"
    
    _MOCK_STORAGE[document_id] = {
        "filename": filename,
        "raw_text": raw_text,
        "user_id": user_id,
        "status": "uploaded"
    }
    
    logger.info(f"Saved document {document_id} ({filename}) to storage.")
    return document_id

def get_document(document_id: str) -> Dict[str, Any]:
    """
    Retrieves a document's metadata and text from storage.
    """
    if document_id not in _MOCK_STORAGE:
        raise KeyError(f"Document {document_id} not found.")
    return _MOCK_STORAGE[document_id]
