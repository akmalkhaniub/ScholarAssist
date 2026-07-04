# ScholarAssist — Ingestion Pipeline
"""
Public interface for the ingestion sub-package.

Classes:
    ManifestStore          — Persist/load IngestionManifest to/from S3
    RetryConfig            — Retry behaviour configuration
    RetryManager           — Execute callables with back-off and dead-letter
    BulkIngester           — Full (backfill) dataset ingestion
    IncrementalIngester    — Delta / incremental ingestion
"""

from src.ingestion.bulk_ingester import BulkIngester
from src.ingestion.incremental_ingester import IncrementalIngester
from src.ingestion.manifest import ManifestStore
from src.ingestion.retry import RetryConfig, RetryManager

__all__ = [
    "BulkIngester",
    "IncrementalIngester",
    "ManifestStore",
    "RetryConfig",
    "RetryManager",
]
