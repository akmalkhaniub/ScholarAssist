"""
ScholarAssist — OpenSearch Bulk Indexer (Spark Job)

Reads Golden Records from the Silver/Gold S3 bucket and bulk writes
them to a newly created OpenSearch index.

Usage:
    spark-submit src/indexing/bulk_indexer.py --index-name scholar_works_v20260704_120000
"""

import argparse
import sys
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from src.config.settings import get_settings

logger = logging.getLogger(__name__)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index-name", type=str, required=True, help="Target OpenSearch index name")
    args = parser.parse_args()

    settings = get_settings()

    # The opensearch-spark connector is required in the spark-submit environment.
    spark = SparkSession.builder \
        .appName("ScholarAssist-Bulk-Indexer") \
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3.endpoint_url) \
        .config("spark.hadoop.fs.s3a.access.key", settings.s3.access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3.secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("opensearch.nodes", settings.opensearch.url) \
        .config("opensearch.net.ssl", str(settings.opensearch.use_ssl).lower()) \
        .config("opensearch.net.ssl.cert.allow.self.signed", "true") \
        .config("opensearch.nodes.wan.only", "true") \
        .getOrCreate()

    golden_path = f"s3a://{settings.s3.golden_bucket}/"

    logger.info(f"Reading Golden Records from {golden_path}")
    
    try:
        # Read the Delta table
        df = spark.read.format("delta").load(golden_path)
    except Exception as e:
        logger.error(f"Failed to read Golden Records: {e}")
        sys.exit(1)

    # Convert complex maps/arrays to JSON strings if needed, 
    # but opensearch-spark handles structs natively.
    # We map golden_record_id to the document _id field
    df = df.withColumn("id", F.col("golden_record_id"))

    logger.info(f"Writing {df.count()} records to OpenSearch index {args.index_name}")

    # Write to OpenSearch
    df.write \
        .format("opensearch") \
        .option("opensearch.resource", args.index_name) \
        .option("opensearch.mapping.id", "id") \
        .option("opensearch.write.operation", "index") \
        .option("opensearch.batch.size.entries", "5000") \
        .option("opensearch.batch.size.bytes", "10mb") \
        .mode("append") \
        .save()

    logger.info("Bulk indexing job completed successfully.")
    spark.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
