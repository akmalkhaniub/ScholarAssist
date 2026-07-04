"""
ScholarAssist — Data Quality Auditor

Performs automated assertions and sanity checks on datasets before they move between Medallion layers.
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class QualityAssertionError(Exception):
    pass


def assert_no_duplicates(df: DataFrame, primary_key: str):
    """Fails if the dataframe has duplicate rows for the given primary key."""
    total_count = df.count()
    distinct_count = df.select(primary_key).distinct().count()
    
    if total_count != distinct_count:
        raise QualityAssertionError(
            f"Duplicate check failed on column '{primary_key}'. "
            f"Total rows: {total_count}, Distinct IDs: {distinct_count}."
        )


def assert_not_null(df: DataFrame, column: str, max_null_fraction: float = 0.0):
    """Fails if the fraction of nulls in a column exceeds the maximum allowed fraction."""
    null_count = df.filter(F.col(column).isNull()).count()
    total_count = df.count()
    
    if total_count == 0:
        return
        
    null_fraction = null_count / total_count
    
    if null_fraction > max_null_fraction:
        raise QualityAssertionError(
            f"Null check failed on column '{column}'. "
            f"Null fraction: {null_fraction:.4f}, Max allowed: {max_null_fraction:.4f}."
        )
