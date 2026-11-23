from __future__ import annotations

from typing import Any

try:
    from pyspark.sql import DataFrame
    import pyspark.sql.functions as F
except ImportError:  # Allow lightweight unit-test imports without PySpark installed
    DataFrame = Any  # type: ignore[misc, assignment]
    F = None  # type: ignore[assignment]

_CONFIDENCE_BY_MATCH_TYPE: dict[str, float] = {
    "exact_doi": 1.0,
    "normalized_doi": 0.95,
    "lsh_match": 0.85,
}


def score_match_confidence(match_type: str | None) -> float:
    """Return deterministic 0.0-1.0 confidence score for an edge match type."""
    if not match_type:
        return 0.5
    return _CONFIDENCE_BY_MATCH_TYPE.get(match_type, 0.5)

def calculate_confidence(edges_df: DataFrame) -> DataFrame:
    """
    Calculates a 0.0 - 1.0 confidence score based on the match type.
    Example rules: exact_doi = 1.0, normalized_doi = 0.95, lsh_match = 0.85.
    """
    confidence_expr = (
        F.when(F.col("match_type") == "exact_doi", 1.0)
        .when(F.col("match_type") == "normalized_doi", 0.95)
        .when(F.col("match_type") == "lsh_match", 0.85)
        .otherwise(0.5)
    )
    
    return edges_df.withColumn("confidence_score", confidence_expr)
