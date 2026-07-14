"""
ScholarAssist - Document Exporter

Takes a document's raw text and the verified claims, and generates
an annotated version of the document with inline citations and a bibliography.
"""

import logging
from typing import List, Dict, Any
from src.bibliography.formatter import format_bibliography

logger = logging.getLogger(__name__)

def generate_annotated_document(raw_text: str, verified_claims: List[Dict[str, Any]], citation_style: str = "APA") -> str:
    """
    Scans the raw text for the context of each claim. If a claim is supported,
    it inserts an inline citation and appends the bibliography to the end.
    """
    logger.info(f"Generating annotated document with {len(verified_claims)} claims.")
    
    annotated_text = raw_text
    golden_record_ids = []
    
    # Simple search and replace for context
    citation_counter = 1
    
    for claim in verified_claims:
        if claim["verification_status"] == "Supported" and claim["evidence"]:
            context = claim.get("claim_text", "")
            
            # We'll use the first piece of evidence for the inline citation
            primary_evidence = claim["evidence"][0]
            golden_record_ids.append(primary_evidence["paper_id"])
            
            # Create inline citation marker
            inline_citation = f" [{citation_counter}]"
            citation_counter += 1
            
            # Simple replacement: Find the claim text in the raw text and append the marker
            # In a real app, you'd want robust NLP matching because the LLM might have altered the text slightly
            if context and context in annotated_text:
                annotated_text = annotated_text.replace(context, context + inline_citation)
            else:
                # Fallback: just append it to the context if we can't find exact match
                pass
                
    if not golden_record_ids:
        return annotated_text
        
    # Generate Bibliography
    bibliography_entries = format_bibliography(golden_record_ids, style=citation_style)
    
    annotated_text += "\n\n" + "="*40 + "\n"
    annotated_text += "BIBLIOGRAPHY\n"
    annotated_text += "="*40 + "\n\n"
    
    for idx, entry in enumerate(bibliography_entries):
        annotated_text += f"[{idx + 1}] {entry}\n\n"
        
    return annotated_text
