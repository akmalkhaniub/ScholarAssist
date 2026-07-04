"""
ScholarAssist — Manifest Store

Persists and retrieves ``IngestionManifest`` objects as JSON documents in an
S3-compatible object store (MinIO in development, AWS S3 in production).

Key layout::

    s3://scholarassist-manifests/{source}/{ISO-timestamp}.json

Usage::

    store = ManifestStore(settings.s3)
    key   = store.save(manifest)
    loaded = store.load(key)
    last   = store.get_last_successful(SourceName.OPENALEX)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

import boto3
import structlog
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from src.config.settings import S3Settings
from src.connectors.base import (
    IngestionManifest,
    IngestionMode,
    SourceName,
)

logger = structlog.get_logger(__name__)


class ManifestStore:
    """Read/write ``IngestionManifest`` objects to an S3 manifests bucket.

    Parameters
    ----------
    s3_settings:
        An :class:`S3Settings` instance with bucket names and credentials.
    """

    def __init__(self, s3_settings: S3Settings) -> None:
        self._settings = s3_settings
        self._bucket = s3_settings.manifests_bucket
        self._client: Optional[S3Client] = None

    # ------------------------------------------------------------------
    # S3 client (lazy)
    # ------------------------------------------------------------------

    @property
    def client(self) -> S3Client:
        """Lazy-initialised boto3 S3 client."""
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self._settings.endpoint_url,
                aws_access_key_id=self._settings.access_key,
                aws_secret_access_key=self._settings.secret_key,
                region_name=self._settings.region,
                use_ssl=self._settings.use_ssl,
            )
        return self._client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save(self, manifest: IngestionManifest) -> str:
        """Serialise *manifest* to JSON and upload to the manifests bucket.

        Returns
        -------
        str
            The S3 object key under which the manifest was stored.
        """
        timestamp = manifest.started_at.strftime("%Y%m%dT%H%M%SZ")
        key = f"{manifest.source.value}/{timestamp}.json"

        body = json.dumps(manifest.to_dict(), indent=2, default=str)

        self.client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )

        logger.info(
            "manifest.saved",
            source=manifest.source.value,
            key=key,
            status=manifest.status,
            files=manifest.files_downloaded,
        )
        return key

    def load(self, manifest_key: str) -> IngestionManifest:
        """Load an ``IngestionManifest`` from an S3 object key.

        Parameters
        ----------
        manifest_key:
            Full object key inside the manifests bucket,
            e.g. ``openalex/20260701T120000Z.json``.

        Raises
        ------
        FileNotFoundError
            If the key does not exist.
        """
        try:
            response = self.client.get_object(
                Bucket=self._bucket,
                Key=manifest_key,
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(
                    f"Manifest not found: s3://{self._bucket}/{manifest_key}"
                ) from exc
            raise

        data: dict[str, Any] = json.loads(response["Body"].read().decode("utf-8"))
        return _dict_to_manifest(data)

    def list_manifests(
        self,
        source: SourceName,
        limit: int = 10,
    ) -> list[str]:
        """Return the most recent manifest keys for *source*.

        Keys are returned in reverse-chronological order (newest first).

        Parameters
        ----------
        source:
            The data source to list manifests for.
        limit:
            Maximum number of keys to return.
        """
        prefix = f"{source.value}/"

        paginator = self.client.get_paginator("list_objects_v2")
        keys: list[str] = []

        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        # Keys are naturally sorted by timestamp (ascending); reverse for newest-first.
        keys.sort(reverse=True)
        return keys[:limit]

    def get_last_successful(
        self,
        source: SourceName,
    ) -> Optional[IngestionManifest]:
        """Return the most recent **completed** manifest for *source*.

        A manifest is considered successful if its ``status`` is
        ``"completed"`` or ``"completed_with_errors"``.

        Returns ``None`` if no successful manifest exists.
        """
        for key in self.list_manifests(source, limit=50):
            try:
                manifest = self.load(key)
            except Exception:
                logger.warning("manifest.load_failed", key=key, exc_info=True)
                continue

            if manifest.status in ("completed", "completed_with_errors"):
                logger.debug(
                    "manifest.last_successful",
                    source=source.value,
                    key=key,
                    cursor=manifest.last_update_cursor,
                )
                return manifest

        return None

    def get_last_cursor(self, source: SourceName) -> Optional[str]:
        """Return the ``last_update_cursor`` from the most recent successful manifest.

        This is a convenience short-cut used by
        :class:`~src.ingestion.incremental_ingester.IncrementalIngester` to
        determine where to resume incremental fetches.
        """
        manifest = self.get_last_successful(source)
        if manifest is not None:
            return manifest.last_update_cursor
        return None


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 string to a timezone-aware datetime."""
    if value is None:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _dict_to_manifest(data: dict[str, Any]) -> IngestionManifest:
    """Reconstruct an ``IngestionManifest`` from a JSON-decoded dict."""
    return IngestionManifest(
        source=SourceName(data["source"]),
        mode=IngestionMode(data["mode"]),
        started_at=_parse_iso(data.get("started_at")) or datetime.now(timezone.utc),
        completed_at=_parse_iso(data.get("completed_at")),
        status=data.get("status", "unknown"),
        files_downloaded=data.get("files_downloaded", 0),
        files_failed=data.get("files_failed", 0),
        total_bytes=data.get("total_bytes", 0),
        file_checksums=data.get("file_checksums", {}),
        source_version=data.get("source_version"),
        source_api_version=data.get("source_api_version"),
        last_update_cursor=data.get("last_update_cursor"),
        errors=data.get("errors", []),
    )
