"""
ScholarAssist — Normalization Spark Job

This script reads raw JSON data from the Bronze S3 bucket,
applies source-specific schema normalization, and writes the output
to the Silver S3 bucket in Parquet format.

Usage:
    spark-submit src/normalization/job.py --source openalex --date 2026-07-04
"""

import argparse
import sys
from pyspark.sql import SparkSession
import logging

from src.config.settings import get_settings
from src.connectors.base import SourceName
from src.normalization.transforms import (
    transform_openalex,
    transform_crossref,
    transform_semantic_scholar,
)

logger = logging.getLogger(__name__)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, required=True, help="Name of the source")
    parser.add_argument("--date", type=str, required=True, help="Date of ingestion (YYYY-MM-DD)")
    args = parser.parse_args()

    source_name_str = args.source
    try:
        source = SourceName(source_name_str)
    except ValueError:
        logger.error(f"Invalid source: {source_name_str}")
        sys.exit(1)

    settings = get_settings()

    spark = SparkSession.builder \
        .appName(f"ScholarAssist-Normalize-{source.value}") \
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3.endpoint_url) \
        .config("spark.hadoop.fs.s3a.access.key", settings.s3.access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3.secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    raw_path = f"s3a://{settings.s3.raw_bucket}/{source.value}/{args.date}/"
    silver_path = f"s3a://{settings.s3.normalized_bucket}/{source.value}/"

    logger.info(f"Reading raw data from {raw_path}")
    
    # Read raw JSON files
    raw_df = spark.read.json(raw_path)

    # Apply specific transformations
    if source == SourceName.OPENALEX:
        normalized_df = transform_openalex(raw_df)
    elif source == SourceName.CROSSREF:
        normalized_df = transform_crossref(raw_df)
    elif source == SourceName.SEMANTIC_SCHOLAR:
        normalized_df = transform_semantic_scholar(raw_df)
    else:
        logger.error(f"Transform for {source.value} not implemented yet.")
        sys.exit(1)

    logger.info(f"Writing normalized data to {silver_path}")
    
    # Write to Silver Layer (Parquet/Delta)
    normalized_df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("publication_year") \
        .save(silver_path)
    
    logger.info("Normalization job completed successfully.")
    spark.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
