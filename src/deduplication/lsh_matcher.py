from pyspark.ml.feature import Tokenizer, NGram, CountVectorizer, MinHashLSH
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def run_minhash_lsh(df: DataFrame, threshold: float = 0.85) -> DataFrame:
    """
    Tokenizes `normalized_title` into n-gram shingles, computes MinHash signatures,
    and runs LSH band-based matching to find near-duplicate titles within blocks.
    Returns a DataFrame of edge pairs (id1, id2).
    """
    # Ensure title is normalized
    df = df.withColumn(
        "normalized_title", 
        F.lower(F.trim(F.coalesce(F.col("title"), F.lit(""))))
    )
    
    tokenizer = Tokenizer(inputCol="normalized_title", outputCol="words")
    words_df = tokenizer.transform(df)
    
    ngram = NGram(n=3, inputCol="words", outputCol="ngrams")
    ngram_df = ngram.transform(words_df)
    
    cv = CountVectorizer(inputCol="ngrams", outputCol="features", minDF=1.0)
    cv_model = cv.fit(ngram_df)
    vectorized_df = cv_model.transform(ngram_df)
    
    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    model = mh.fit(vectorized_df)
    
    distance_threshold = 1.0 - threshold
    
    edges = model.approxSimilarityJoin(
        vectorized_df, vectorized_df, distance_threshold, distCol="jaccard_distance"
    )
    
    # Filter for matches within the same block and avoid self-matches/duplicates
    edges = edges.filter(
        (F.col("datasetA.id") < F.col("datasetB.id")) &
        (F.col("datasetA.heuristic_block_key") == F.col("datasetB.heuristic_block_key"))
    )
    
    edges_df = edges.select(
        F.col("datasetA.id").alias("id1"),
        F.col("datasetB.id").alias("id2"),
        F.lit("lsh_match").alias("match_type")
    )
    
    return edges_df
