"""
ScholarAssist — Bibliography Formatter

Takes a list of OpenSearch golden_record_ids and formats them into standard citation styles (APA, MLA, IEEE).
"""

from typing import List, Dict, Any

def format_citations(records: List[Dict[str, Any]], style: str = "apa") -> List[str]:
    """
    Formats a list of academic records into a standard citation style.
    Supports 'apa', 'mla', 'ieee', 'chicago'.
    
    In a full production environment, we would use citeproc-py with CSL styles.
    For this prototype, we'll implement a basic string formatter.
    """
    formatted = []
    
    for record in records:
        authors = record.get("authors", [])
        title = record.get("title", "Unknown Title")
        year = record.get("publication_year", "n.d.")
        venue = record.get("venue", {}).get("name", "Unknown Venue")
        doi = record.get("doi", "")
        
        # Author formatting
        author_str = "Unknown Author"
        if authors:
            if len(authors) == 1:
                author_str = authors[0].get("name", "")
            elif len(authors) == 2:
                author_str = f"{authors[0].get('name', '')} & {authors[1].get('name', '')}"
            else:
                author_str = f"{authors[0].get('name', '')} et al."
                
        # Style formatting
        if style.lower() == "apa":
            citation = f"{author_str} ({year}). {title}. {venue}."
            if doi:
                citation += f" https://doi.org/{doi}"
        elif style.lower() == "mla":
            citation = f"{author_str}. \"{title}.\" {venue}, {year}."
        elif style.lower() == "ieee":
            citation = f"{author_str}, \"{title},\" {venue}, {year}."
        else:
            # Default to APA
            citation = f"{author_str} ({year}). {title}. {venue}."
            
        formatted.append(citation)
        
    return formatted
