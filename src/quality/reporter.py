"""
ScholarAssist — Data Quality Reporter

Generates data quality metrics (e.g. null rates, coverage) on the unified Silver and Gold records.
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)

def generate_null_report(df: DataFrame) -> dict:
    """Calculates the percentage of null values for each column in the DataFrame."""
    total_count = df.count()
    if total_count == 0:
        return {}

    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]).collect()[0].asDict()

    report = {
        column: (count / total_count) * 100
        for column, count in null_counts.items()
    }
    
    logger.info(f"Data Quality Null Report Generated for {len(df.columns)} columns.")
    return report

def log_quality_metrics(report: dict, threshold_percent: float = 50.0):
    """Logs warnings if null percentages exceed a threshold."""
    for column, null_pct in report.items():
        if null_pct > threshold_percent:
            logger.warning(f"Data Quality Warning: Column '{column}' has {null_pct:.2f}% null values.")
