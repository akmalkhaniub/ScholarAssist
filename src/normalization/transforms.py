"""
ScholarAssist — Normalization Transforms

Contains functions to transform raw provider-specific PySpark DataFrames into
the unified Silver Layer schema defined in `schema.py`.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StringType

from src.normalization.cleaners import (
    clean_html,
    normalize_doi,
    normalize_title,
)
from src.connectors.base import SourceName


def _base_transform(df: DataFrame, source: SourceName) -> DataFrame:
    """Adds common metadata fields and casts to unified schema constraints."""
    return df.withColumn("source", F.lit(source.value))


def transform_openalex(df: DataFrame) -> DataFrame:
    """
    Transforms raw OpenAlex `works` data into the unified schema.
    """
    # OpenAlex stores authors in an array of structs: `authorships` -> `author` -> `display_name`, `id`, `orcid`
    # and affiliations inside `institutions` -> `display_name`
    
    author_expr = F.expr("""
        transform(authorships, x -> struct(
            x.author.id as id,
            x.author.display_name as name,
            x.author.orcid as orcid,
            transform(x.institutions, i -> i.display_name) as affiliations
        ))
    """)

    venue_expr = F.expr("""
        struct(
            host_venue.display_name as name,
            host_venue.issn_l as issn,
            host_venue.type as type
        )
    """)

    oa_expr = F.expr("""
        struct(
            open_access.is_oa as is_oa,
            open_access.oa_url as oa_url
        )
    """)

    transformed = df.select(
        normalize_doi(F.col("doi")).alias("doi"),
        F.col("title").alias("title"),
        normalize_title(F.col("title")).alias("normalized_title"),
        author_expr.alias("authors"),
        F.col("publication_year").cast("integer").alias("publication_year"),
        venue_expr.alias("venue"),
        clean_html(F.col("abstract_inverted_index")).alias("abstract"), # NOTE: Inverted index needs expansion in reality
        F.col("referenced_works").alias("references"),
        F.col("cited_by_count").cast("integer").alias("citation_count"),
        oa_expr.alias("open_access"),
        F.create_map(F.lit("title"), F.lit("openalex")).alias("source_provenance"),
        F.col("id").alias("provider_id")
    )
    
    return _base_transform(transformed, SourceName.OPENALEX)


def transform_crossref(df: DataFrame) -> DataFrame:
    """
    Transforms raw Crossref `works` data into the unified schema.
    """
    author_expr = F.expr("""
        transform(author, x -> struct(
            x.ORCID as id,
            concat_ws(' ', x.given, x.family) as name,
            x.ORCID as orcid,
            transform(x.affiliation, a -> a.name) as affiliations
        ))
    """)

    venue_expr = F.expr("""
        struct(
            element_at(`container-title`, 1) as name,
            element_at(ISSN, 1) as issn,
            type as type
        )
    """)

    # Crossref open access is tricky, usually based on license URLs.
    # We set default null and let Unpaywall fill it in during merge.
    oa_expr = F.expr("""
        struct(
            cast(null as boolean) as is_oa,
            cast(null as string) as oa_url
        )
    """)

    transformed = df.select(
        normalize_doi(F.col("DOI")).alias("doi"),
        F.element_at(F.col("title"), 1).alias("title"),
        normalize_title(F.element_at(F.col("title"), 1)).alias("normalized_title"),
        author_expr.alias("authors"),
        F.col("created.date-parts")[0][0].cast("integer").alias("publication_year"),
        venue_expr.alias("venue"),
        clean_html(F.col("abstract")).alias("abstract"),
        F.expr("transform(reference, r -> r.DOI)").alias("references"),
        F.col("is-referenced-by-count").cast("integer").alias("citation_count"),
        oa_expr.alias("open_access"),
        F.create_map(F.lit("title"), F.lit("crossref")).alias("source_provenance"),
        F.col("DOI").alias("provider_id")
    )
    
    return _base_transform(transformed, SourceName.CROSSREF)


def transform_semantic_scholar(df: DataFrame) -> DataFrame:
    """
    Transforms raw Semantic Scholar `papers` data into the unified schema.
    """
    author_expr = F.expr("""
        transform(authors, x -> struct(
            x.authorId as id,
            x.name as name,
            cast(null as string) as orcid,
            array() as affiliations
        ))
    """)

    venue_expr = F.expr("""
        struct(
            venue as name,
            cast(null as string) as issn,
            publicationTypes[0] as type
        )
    """)

    oa_expr = F.expr("""
        struct(
            isOpenAccess as is_oa,
            openAccessPdf.url as oa_url
        )
    """)

    transformed = df.select(
        normalize_doi(F.col("externalIds.DOI")).alias("doi"),
        F.col("title").alias("title"),
        normalize_title(F.col("title")).alias("normalized_title"),
        author_expr.alias("authors"),
        F.col("year").cast("integer").alias("publication_year"),
        venue_expr.alias("venue"),
        clean_html(F.col("abstract")).alias("abstract"),
        F.array().alias("references"), # Semantic Scholar puts references in a different dataset
        F.col("citationCount").cast("integer").alias("citation_count"),
        oa_expr.alias("open_access"),
        F.create_map(F.lit("title"), F.lit("semantic_scholar")).alias("source_provenance"),
        F.col("paperId").alias("provider_id")
    )
    
    return _base_transform(transformed, SourceName.SEMANTIC_SCHOLAR)

# Similarly, we would implement transform_core, transform_dblp, etc.
