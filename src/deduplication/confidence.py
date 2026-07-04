from pyspark.sql import DataFrame
import pyspark.sql.functions as F

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
