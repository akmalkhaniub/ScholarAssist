"""
ScholarAssist — Unified PySpark Schema

Defines the normalized academic record schema (Silver Layer).
All raw data sources (Bronze Layer) are transformed into this schema
before entity resolution and deduplication.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# Unified Academic Record Schema
# ------------------------------
# Missing fields must be represented as null.

# Author sub-structure
AUTHOR_SCHEMA = StructType([
    StructField("id", StringType(), True),           # Internal/Source ID
    StructField("name", StringType(), True),         # Full name
    StructField("orcid", StringType(), True),        # ORCID if available
    StructField("affiliations", ArrayType(StringType()), True), # List of affiliation names
])

# Venue sub-structure
VENUE_SCHEMA = StructType([
    StructField("name", StringType(), True),         # Venue/Journal/Conference name
    StructField("issn", StringType(), True),         # ISSN
    StructField("type", StringType(), True),         # e.g., journal, conference, repository
])

# Open Access sub-structure
OPEN_ACCESS_SCHEMA = StructType([
    StructField("is_oa", BooleanType(), True),       # True if open access
    StructField("oa_url", StringType(), True),       # URL to full text
])

# Main Golden Record / Silver Record Schema
UNIFIED_RECORD_SCHEMA = StructType([
    StructField("doi", StringType(), True),          # Normalized DOI (lowercase, no protocol)
    StructField("title", StringType(), True),        # Original title
    StructField("normalized_title", StringType(), True), # Lowercase, punctuation stripped for LSH
    StructField("authors", ArrayType(AUTHOR_SCHEMA), True),
    StructField("publication_year", IntegerType(), True),
    StructField("venue", VENUE_SCHEMA, True),
    StructField("abstract", StringType(), True),     # Full abstract text
    StructField("references", ArrayType(StringType()), True), # List of referenced DOIs or IDs
    StructField("citation_count", IntegerType(), True),
    StructField("open_access", OPEN_ACCESS_SCHEMA, True),
    StructField("source_provenance", MapType(StringType(), StringType()), True), # field -> source
    StructField("provider_id", StringType(), True),  # Original ID from the provider
    StructField("source", StringType(), True),       # Name of the provider (e.g. openalex)
])
