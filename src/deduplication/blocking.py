from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def create_deterministic_blocks(df: DataFrame) -> DataFrame:
    """
    Creates blocks for deduplication based on exact DOI, normalized DOI,
    and a heuristic blocking key: soundex(first_author_lastname) + publication_year.
    """
    if "doi" in df.columns:
        df = df.withColumn("normalized_doi", F.lower(F.trim(F.col("doi"))))
        
    df = df.withColumn(
        "heuristic_block_key",
        F.concat_ws("_", 
                    F.soundex(F.col("first_author_lastname")), 
                    F.col("publication_year").cast("string"))
    )
    
    return df

def generate_deterministic_edges(df_blocked: DataFrame) -> DataFrame:
    """
    Generates deterministic edge pairs for records sharing an exact DOI or normalized DOI.
    """
    exact_doi = df_blocked.filter(F.col("doi").isNotNull() & (F.col("doi") != "")) \
        .alias("a").join(
            df_blocked.alias("b"),
            F.col("a.doi") == F.col("b.doi")
        ).filter(F.col("a.id") < F.col("b.id")) \
        .select(
            F.col("a.id").alias("id1"),
            F.col("b.id").alias("id2"),
            F.lit("exact_doi").alias("match_type")
        )
        
    norm_doi = df_blocked.filter(F.col("normalized_doi").isNotNull() & (F.col("normalized_doi") != "")) \
        .alias("a").join(
            df_blocked.alias("b"),
            (F.col("a.normalized_doi") == F.col("b.normalized_doi")) &
            ((F.col("a.doi") != F.col("b.doi")) | F.col("a.doi").isNull() | F.col("b.doi").isNull())
        ).filter(F.col("a.id") < F.col("b.id")) \
        .select(
            F.col("a.id").alias("id1"),
            F.col("b.id").alias("id2"),
            F.lit("normalized_doi").alias("match_type")
        )
        
    return exact_doi.unionByName(norm_doi).distinct()
