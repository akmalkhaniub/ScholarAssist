"""
ScholarAssist — Normalization Cleaners

Provides PySpark DataFrame column expressions and UDFs for cleaning and standardizing
raw data fields (DOIs, titles, author names) before entity resolution.
"""

from __future__ import annotations

import re
from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql.column import Column


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
