"""
ScholarAssist — Normalization Cleaners

Provides PySpark DataFrame column expressions and UDFs for cleaning and standardizing
raw data fields (DOIs, titles, author names) before entity resolution.
"""

from __future__ import annotations

import re
from typing import Any, Callable

try:
    import pyspark.sql.functions as F
    from pyspark.sql.column import Column
except ImportError:  # Allow lightweight unit-test imports without PySpark installed
    F = None  # type: ignore[assignment]
    Column = Any  # type: ignore[misc, assignment]


# ---------------------------------------------------------------------------
# Pure-Python Reference Cleaners (Spark-parity & unit-testable)
# ---------------------------------------------------------------------------

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_DOI_PREFIX_RE = re.compile(r"^(https?://)?(dx\.)?doi\.org/")
_AUTHOR_HONORIFIC_RE = re.compile(r"^(dr|prof|mr|mrs|ms)\.?\s+", re.IGNORECASE)


def clean_doi(raw_doi: str | None) -> str | None:
    """Normalize a DOI string to lowercase canonical form, or return None if invalid."""
    if not raw_doi or not isinstance(raw_doi, str):
        return None
    cleaned = raw_doi.strip().lower()
    cleaned = _DOI_PREFIX_RE.sub("", cleaned)
    cleaned = re.sub(r"^doi:\s*", "", cleaned)
    cleaned = cleaned.strip()
    return cleaned if cleaned.startswith("10.") and "/" in cleaned else None


def strip_html(text: str | None) -> str | None:
    """Strip HTML tags from text while preserving None."""
    if text is None:
        return None
    return _HTML_TAG_RE.sub("", text)


def clean_title(raw_title: str | None) -> str:
    """Normalize a paper title for fuzzy matching / LSH blocking."""
    if not raw_title:
        return ""
    cleaned = strip_html(raw_title.lower()) or ""
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def normalize_author_name(raw_name: str | None) -> str:
    """Cleanse an author name by normalizing 'Last, First' order, titles, and whitespace."""
    if not raw_name or not isinstance(raw_name, str):
        return ""
    name = _HTML_TAG_RE.sub("", raw_name).strip()
    name = _AUTHOR_HONORIFIC_RE.sub("", name).strip()
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if parts[0] and parts[1]:
            name = f"{parts[1]} {parts[0]}"
    return re.sub(r"\s+", " ", name).strip()


def extract_first_author_lastname(authors: list[dict[str, Any]] | None) -> str | None:
    """Extract normalized lowercase alphabetic last name of the first author."""
    if not authors:
        return None
    first = authors[0]
    raw_name = first.get("name") if isinstance(first, dict) else str(first)
    normalized = normalize_author_name(raw_name)
    if not normalized:
        return None
    last_part = normalized.split(" ")[-1].lower()
    alpha_only = re.sub(r"[^a-z]", "", last_part)
    return alpha_only or None


# ---------------------------------------------------------------------------
# DOI Normalization
# ---------------------------------------------------------------------------

def normalize_doi(doi_col: Column) -> Column:
    """
    Standardizes DOIs by lowercasing, stripping whitespace, and removing
    common URI prefixes (e.g., https://doi.org/).
    
    Examples:
        - "HTTPS://DOI.ORG/10.123/ABC" -> "10.123/abc"
        - " doi:10.123/abc " -> "10.123/abc"
    """
    cleaned = F.lower(F.trim(doi_col))
    cleaned = F.regexp_replace(cleaned, r"^(https?://)?(dx\.)?doi\.org/", "")
    cleaned = F.regexp_replace(cleaned, r"^doi:", "")
    # Set to null if the string doesn't look like a valid DOI (starts with 10.)
    return F.when(cleaned.startswith("10."), cleaned).otherwise(F.lit(None).cast("string"))


# ---------------------------------------------------------------------------
# Title Normalization (For LSH / Fuzzy Matching)
# ---------------------------------------------------------------------------

def normalize_title(title_col: Column) -> Column:
    """
    Cleans a title for fuzzy matching (LSH):
    1. Lowercase
    2. Strip HTML tags
    3. Remove punctuation and non-alphanumeric characters
    4. Remove excessive whitespace
    
    This field is specifically used for blocking and MinHash, NOT for display.
    """
    cleaned = F.lower(title_col)
    # Strip basic HTML tags (e.g. <i>, </b>, <p>)
    cleaned = F.regexp_replace(cleaned, r"<[^>]+>", "")
    # Replace non-alphanumeric with a space
    cleaned = F.regexp_replace(cleaned, r"[^a-z0-9\s]", " ")
    # Replace multiple spaces with a single space
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    return F.trim(cleaned)


# ---------------------------------------------------------------------------
# General Text Cleaning
# ---------------------------------------------------------------------------

def clean_html(text_col: Column) -> Column:
    """Strips HTML tags from abstracts or display fields."""
    return F.when(text_col.isNotNull(), F.regexp_replace(text_col, r"<[^>]+>", "")).otherwise(text_col)


# ---------------------------------------------------------------------------
# Author Processing
# ---------------------------------------------------------------------------

def get_first_author_lastname(authors_col: Column) -> Column:
    """
    Extracts the last name of the first author from the unified authors array.
    Used for creating deterministic blocking keys.
    
    Assumes the author schema is an array of structs containing a 'name' field.
    """
    # Get the name of the first author in the array
    first_author_name = F.element_at(authors_col, 1)["name"]
    # Split by spaces and take the last part. This is naive but works for blocking.
    name_parts = F.split(F.trim(first_author_name), " ")
    last_name = F.element_at(name_parts, -1)
    
    # Clean the last name to alpha characters only
    cleaned_last_name = F.regexp_replace(F.lower(last_name), r"[^a-z]", "")
    return F.when(F.length(cleaned_last_name) > 0, cleaned_last_name).otherwise(F.lit(None).cast("string"))


def extract_authors(
    array_col: Column,
    name_path: str,
    id_path: str = None,
    orcid_path: str = None,
    affiliations_path: str = None
) -> Column:
    """
    Helper to map raw source author arrays into the unified ArrayType(AUTHOR_SCHEMA).
    This function creates an expression to use inside a select().
    """
    # We will use Spark's transform function
    # Unfortunately, doing this dynamically requires expressing the paths cleanly.
    # It is usually easier to do this specific logic in the transforms.py
    # but providing a generic SQL expression wrapper here is useful.
    pass
