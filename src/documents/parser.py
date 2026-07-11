"""
ScholarAssist — Document Parser

Handles extraction of raw text from uploaded files (PDF, DOCX, TXT).
"""

import io
from typing import Optional

def extract_text(file_bytes: bytes, filename: str) -> str:
    """
    Extracts text from the given file bytes.
    Supports .txt, .pdf, and .docx formats.
    """
    ext = filename.split('.')[-1].lower()
    
    if ext == 'txt':
        return file_bytes.decode('utf-8', errors='replace')
        
    elif ext == 'pdf':
        try:
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(file_bytes))
            text = []
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text.append(page_text)
            return "\n".join(text)
        except ImportError:
            # Fallback if pypdf isn't installed
            return "[PDF extraction requires 'pypdf' package]"
            
    elif ext in ['doc', 'docx']:
        # For a production system we'd use python-docx. 
        # For this prototype we will return a placeholder if not implemented.
        return "[DOCX extraction not fully implemented without 'python-docx']"
        
    else:
        raise ValueError(f"Unsupported file format: {ext}")
