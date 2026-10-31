"""
Academic Citation & BibTeX Generator Engine for ScholarAssist
"""
import re
from typing import Dict, Any, List

class CitationExporterEngine:
    """Generates formatted academic citations and BibTeX records."""

    @staticmethod
    def generate_bibtex(paper: Dict[str, Any]) -> str:
        """Generate BibTeX record for a research paper."""
        paper_id = paper.get("id", "paper1").replace("-", "_").lower()
        authors = " and ".join(paper.get("authors", ["Unknown Author"]))
        title = paper.get("title", "Untitled Paper")
        journal = paper.get("journal", "arXiv preprint")
        year = paper.get("year", 2026)

        return (
            f"@article{{{paper_id},\n"
            f"  author = {{{authors}}},\n"
            f"  title = {{{{{title}}}}},\n"
            f"  journal = {{{journal}}},\n"
            f"  year = {{{year}}}\n"
            f"}}"
        )

    @staticmethod
    def generate_apa_citation(paper: Dict[str, Any]) -> str:
        """Generate APA (7th Edition) formatted citation string."""
        authors = paper.get("authors", ["Unknown"])
        if len(authors) == 1:
            author_str = authors[0]
        elif len(authors) == 2:
            author_str = f"{authors[0]}, & {authors[1]}"
        else:
            author_str = f"{authors[0]} et al."

        title = paper.get("title", "Untitled Paper")
        journal = paper.get("journal", "arXiv")
        year = paper.get("year", 2026)

        return f"{author_str} ({year}). {title}. {journal}."
