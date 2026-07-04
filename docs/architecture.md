# ScholarAssist Architecture

## Overview
ScholarAssist is a data pipeline designed to ingest, normalize, deduplicate, and index massive volumes of academic metadata (e.g., from OpenAlex, Crossref, Semantic Scholar) to serve a real-time citation and claim verification API.

The pipeline utilizes a Medallion architecture (Bronze -> Silver -> Gold):
- **Bronze (Raw)**: Raw, provider-specific JSON snapshots and API responses stored in S3/MinIO.
- **Silver (Normalized)**: Cleaned, schema-unified records stored as Parquet/Delta tables.
- **Gold (Resolved)**: Deduplicated "Golden Records" (resolved via PySpark and GraphFrames) ready for indexing.

## Components

### Data Connectors (`src/connectors`)
A unified interface `BaseConnector` standardizes `download_bulk` and `fetch_incremental` methods across all 7 sources.
Connectors handle complex paginations (Cursor, Scroll, Token) and rate limits, persisting state via `IngestionManifest` tracking.

### Ingestion Pipeline (`src/ingestion`)
Orchestrated by Apache Airflow. `BulkIngester` pulls massive initial datasets, and `IncrementalIngester` runs daily to capture deltas. Data is streamed into the Bronze S3 bucket.

### Normalization Pipeline (`src/normalization`)
PySpark jobs that read raw JSON and transform it into `UNIFIED_RECORD_SCHEMA`.
Functions in `cleaners.py` sanitize DOIs and HTML, and generate LSH-friendly titles.

### Deduplication Pipeline (`src/deduplication`)
A PySpark/GraphFrames job:
1. **Blocking**: Generates deterministic blocks based on exact DOI, or heuristics (Soundex(Author) + Year).
2. **MinHash LSH**: Finds near-duplicate titles within blocks.
3. **Graph Resolution**: Uses Connected Components to merge pair-wise matches into unified clusters.
4. **Golden Record**: Selects the best fields per cluster based on provider priority and computes confidence scores.

### OpenSearch Indexing (`src/indexing`)
The Golden Records are indexed in OpenSearch using `opensearch-hadoop`.
The `IndexManager` ensures zero-downtime updates using atomic alias switching between timestamp-versioned indices.

### REST API (`src/api`)
FastAPI application that queries the OpenSearch cluster to return Golden Records. Built for high performance, supporting caching and rate limiting.
