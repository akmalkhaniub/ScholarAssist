import argparse
from pyspark.sql import SparkSession
from blocking import create_deterministic_blocks, generate_deterministic_edges
from lsh_matcher import run_minhash_lsh
from graph_resolver import resolve_clusters
from golden_record import merge_golden_records
from confidence import calculate_confidence

def main():
    parser = argparse.ArgumentParser(description="ScholarAssist Deduplication Job")
    parser.add_argument("--input-path", required=True, help="Path to input data (Bronze/Silver)")
    parser.add_argument("--output-path", required=True, help="Path to output data (Golden Records)")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("ScholarAssist-Deduplication") \
        .getOrCreate()

    # Read input records
    df = spark.read.parquet(args.input_path)
    
    # 1. Blocking
    df_blocked = create_deterministic_blocks(df)
    deterministic_edges = generate_deterministic_edges(df_blocked)
    
    # 2. LSH Matching
    lsh_edges = run_minhash_lsh(df_blocked, threshold=0.85)
    
    # 3. Combine edges and calculate confidence
    all_edges = deterministic_edges.unionByName(lsh_edges).distinct()
    scored_edges = calculate_confidence(all_edges)
    
    # 4. Resolve clusters using GraphFrames
    cluster_mapping = resolve_clusters(scored_edges)
    
    # 5. Golden Record generation
    provider_priority = ["Crossref", "OpenAlex", "Semantic Scholar"]
    golden_records = merge_golden_records(df_blocked, cluster_mapping, provider_priority)
    
    # Write output
    golden_records.write.mode("overwrite").parquet(args.output_path)
    
    spark.stop()

if __name__ == "__main__":
    main()
