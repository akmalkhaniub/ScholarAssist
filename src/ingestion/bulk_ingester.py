"""
ScholarAssist — Bulk Ingester

Orchestrates a full (backfill) ingestion run for a given data source:

1. Create a fresh :class:`IngestionManifest`.
2. Download all raw files via the connector's ``download_bulk()`` method.
3. Compute SHA-256 checksums for every downloaded file.
4. Upload each file to the S3 **raw** bucket under
   ``{source}/{date}/{filename}``.
5. Persist the completed manifest to S3 via :class:`ManifestStore`.
6. Clean up the local temp directory.

Usage::

    ingester = BulkIngester(settings, manifest_store)
    async with connector:
        manifest = await ingester.ingest(SourceName.OPENALEX, connector)
"""

from __future__ import annotations

import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import boto3
import structlog
from mypy_boto3_s3.client import S3Client

from src.config.settings import Settings
from src.connectors.base import (
    BaseConnector,
    IngestionManifest,
    IngestionMode,
    SourceName,
)
from src.ingestion.manifest import ManifestStore

logger = structlog.get_logger(__name__)


class BulkIngester:
    """Run a full bulk ingestion for an academic data source.

    Parameters
    ----------
    settings:
        Root application settings (provides S3 configuration).
    manifest_store:
        The :class:`ManifestStore` used to persist manifests.
    """

    def __init__(
        self,
        settings: Settings,
        manifest_store: ManifestStore,
    ) -> None:
        self._settings = settings
        self._s3 = settings.s3
        self._manifest_store = manifest_store
        self._client: Optional[S3Client] = None

    # ------------------------------------------------------------------
    # S3 client (lazy)
    # ------------------------------------------------------------------

    @property
    def client(self) -> S3Client:
        """Lazy-initialised boto3 S3 client for raw-bucket uploads."""
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self._s3.endpoint_url,
                aws_access_key_id=self._s3.access_key,
                aws_secret_access_key=self._s3.secret_key,
                region_name=self._s3.region,
                use_ssl=self._s3.use_ssl,
            )
        return self._client

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def ingest(
        self,
        source: SourceName,
        connector: BaseConnector,
    ) -> IngestionManifest:
        """Execute a full bulk ingestion run.

        Parameters
        ----------
        source:
            Canonical source identifier (e.g. ``SourceName.OPENALEX``).
        connector:
            An initialised connector instance for the target source.

        Returns
        -------
        IngestionManifest
            The completed manifest with file counts, checksums, and status.
        """
        manifest = IngestionManifest(
            source=source,
            mode=IngestionMode.BULK,
        )

        # Capture the source version (snapshot date, API version, etc.)
        try:
            manifest.source_version = await connector.get_source_version()
        except Exception:
            logger.warning("bulk.source_version_failed", source=source.value, exc_info=True)

        temp_dir = Path(tempfile.mkdtemp(prefix=f"scholarassist-bulk-{source.value}-"))
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        logger.info(
            "bulk.started",
            source=source.value,
            temp_dir=str(temp_dir),
        )

        try:
            # 1. Delegate download to the connector
            manifest = await connector.download_bulk(temp_dir, manifest)

            # 2. Upload every file in the temp dir to S3 raw bucket
            for file_path in sorted(temp_dir.rglob("*")):
                if not file_path.is_file():
                    continue

                relative = file_path.relative_to(temp_dir)
                s3_key = f"{source.value}/{today}/{relative.as_posix()}"

                checksum = BaseConnector.compute_checksum(file_path)
                size_bytes = file_path.stat().st_size

                self._upload_file(file_path, s3_key)
                manifest.add_file(s3_key, size_bytes, checksum)

                logger.debug(
                    "bulk.file_uploaded",
                    key=s3_key,
                    size=size_bytes,
                    checksum=checksum[:12],
                )

            manifest.mark_completed()

        except Exception as exc:
            manifest.mark_failed(str(exc))
            logger.error(
                "bulk.failed",
                source=source.value,
                error=str(exc),
                exc_info=True,
            )
            raise

        finally:
            # 3. Always persist the manifest (even on failure for auditability)
            try:
                self._manifest_store.save(manifest)
            except Exception:
                logger.error("bulk.manifest_save_failed", exc_info=True)

            # 4. Clean up temp directory
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.debug("bulk.temp_cleaned", temp_dir=str(temp_dir))

        logger.info(
            "bulk.completed",
            source=source.value,
            files=manifest.files_downloaded,
            total_bytes=manifest.total_bytes,
            status=manifest.status,
        )
        return manifest

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _upload_file(self, local_path: Path, s3_key: str) -> None:
        """Upload a single local file to the raw S3 bucket."""
        with open(local_path, "rb") as fh:
            self.client.upload_fileobj(
                Fileobj=fh,
                Bucket=self._s3.raw_bucket,
                Key=s3_key,
            )
