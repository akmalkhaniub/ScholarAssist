"""
ScholarAssist — Incremental Ingester

Fetches only the records that have changed since the last successful
ingestion:

1. Retrieve the most recent successful manifest from :class:`ManifestStore`
   to obtain the ``last_update_cursor`` (or ``since`` date).
2. Create a new :class:`IngestionManifest` with ``mode=INCREMENTAL``.
3. Delegate to the connector's ``fetch_incremental()`` method.
4. Upload downloaded delta files to the S3 **raw** bucket under
   ``{source}/{date}/{filename}``.
5. Persist the new manifest (including the updated cursor).

Usage::

    ingester = IncrementalIngester(settings, manifest_store)
    async with connector:
        manifest = await ingester.ingest(SourceName.CROSSREF, connector)
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


class IncrementalIngester:
    """Fetch and persist incremental (delta) data for an academic source.

    Parameters
    ----------
    settings:
        Root application settings (provides S3 configuration).
    manifest_store:
        The :class:`ManifestStore` used to persist and look up manifests.
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
        """Execute an incremental ingestion run.

        Parameters
        ----------
        source:
            Canonical source identifier (e.g. ``SourceName.CROSSREF``).
        connector:
            An initialised connector instance for the target source.

        Returns
        -------
        IngestionManifest
            The completed manifest with file counts, checksums, updated cursor,
            and status.
        """
        # 1. Determine the resume point
        last_manifest = self._manifest_store.get_last_successful(source)
        since: Optional[datetime] = None
        cursor: Optional[str] = None

        if last_manifest is not None:
            cursor = last_manifest.last_update_cursor
            # Use completed_at as the "since" timestamp if no explicit cursor
            if cursor is None and last_manifest.completed_at is not None:
                since = last_manifest.completed_at
            logger.info(
                "incremental.resume_point",
                source=source.value,
                cursor=cursor,
                since=since.isoformat() if since else None,
            )
        else:
            logger.info(
                "incremental.no_prior_manifest",
                source=source.value,
            )

        # 2. Create a new manifest
        manifest = IngestionManifest(
            source=source,
            mode=IngestionMode.INCREMENTAL,
        )

        try:
            manifest.source_version = await connector.get_source_version()
        except Exception:
            logger.warning(
                "incremental.source_version_failed",
                source=source.value,
                exc_info=True,
            )

        temp_dir = Path(tempfile.mkdtemp(prefix=f"scholarassist-incr-{source.value}-"))
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        logger.info(
            "incremental.started",
            source=source.value,
            temp_dir=str(temp_dir),
        )

        try:
            # 3. Fetch incremental data
            manifest = await connector.fetch_incremental(
                target_dir=temp_dir,
                manifest=manifest,
                since=since,
                cursor=cursor,
            )

            # 4. Upload each delta file to S3 raw bucket
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
                    "incremental.file_uploaded",
                    key=s3_key,
                    size=size_bytes,
                    checksum=checksum[:12],
                )

            manifest.mark_completed()

        except Exception as exc:
            manifest.mark_failed(str(exc))
            logger.error(
                "incremental.failed",
                source=source.value,
                error=str(exc),
                exc_info=True,
            )
            raise

        finally:
            # 5. Persist manifest (even on failure)
            try:
                self._manifest_store.save(manifest)
            except Exception:
                logger.error("incremental.manifest_save_failed", exc_info=True)

            # 6. Clean up
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.debug("incremental.temp_cleaned", temp_dir=str(temp_dir))

        logger.info(
            "incremental.completed",
            source=source.value,
            files=manifest.files_downloaded,
            total_bytes=manifest.total_bytes,
            cursor=manifest.last_update_cursor,
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
