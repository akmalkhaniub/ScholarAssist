"""
ScholarAssist — Abstract Base Connector

All data source connectors (OpenAlex, Crossref, Semantic Scholar, etc.) inherit
from this class and implement its abstract methods. This ensures a consistent
interface for bulk ingestion, incremental updates, checksum verification, and
manifest tracking across all providers.
"""

from __future__ import annotations

import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, AsyncIterator, Optional

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

logger = logging.getLogger(__name__)


class SourceName(str, Enum):
    """Canonical names for all supported academic data sources."""
    OPENALEX = "openalex"
    CROSSREF = "crossref"
    SEMANTIC_SCHOLAR = "semantic_scholar"
    CORE = "core"
    UNPAYWALL = "unpaywall"
    DBLP = "dblp"
    OPENCITATIONS = "opencitations"


class IngestionMode(str, Enum):
    """Whether this ingestion run is a full backfill or an incremental update."""
    BULK = "bulk"
    INCREMENTAL = "incremental"


@dataclass
class IngestionManifest:
    """
    Tracks metadata for a single ingestion run.

    Every bulk download or incremental update produces a manifest that is
    persisted to S3 for full auditability and reprocessing capability.
    """
    source: SourceName
    mode: IngestionMode
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    status: str = "in_progress"

    # File tracking
    files_downloaded: int = 0
    files_failed: int = 0
    total_bytes: int = 0
    file_checksums: dict[str, str] = field(default_factory=dict)  # filename → SHA-256

    # Source version tracking
    source_version: Optional[str] = None  # e.g., "2026-06-01" for OpenAlex snapshot
    source_api_version: Optional[str] = None
    last_update_cursor: Optional[str] = None  # For incremental: last processed cursor/date

    # Error tracking
    errors: list[dict[str, Any]] = field(default_factory=list)

    def mark_completed(self) -> None:
        """Mark the ingestion run as completed."""
        self.completed_at = datetime.now(timezone.utc)
        self.status = "completed" if self.files_failed == 0 else "completed_with_errors"

    def mark_failed(self, error: str) -> None:
        """Mark the ingestion run as failed."""
        self.completed_at = datetime.now(timezone.utc)
        self.status = "failed"
        self.errors.append({"timestamp": datetime.now(timezone.utc).isoformat(), "error": error})

    def add_file(self, filename: str, size_bytes: int, checksum: str) -> None:
        """Register a successfully downloaded file."""
        self.files_downloaded += 1
        self.total_bytes += size_bytes
        self.file_checksums[filename] = checksum

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-safe dictionary for S3 persistence."""
        return {
            "source": self.source.value,
            "mode": self.mode.value,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "files_downloaded": self.files_downloaded,
            "files_failed": self.files_failed,
            "total_bytes": self.total_bytes,
            "total_bytes_human": _human_bytes(self.total_bytes),
            "file_checksums": self.file_checksums,
            "source_version": self.source_version,
            "source_api_version": self.source_api_version,
            "last_update_cursor": self.last_update_cursor,
            "errors": self.errors,
        }


class BaseConnector(ABC):
    """
    Abstract base class for all academic data source connectors.

    Each connector must implement:
      - download_bulk():        Full dataset download (S3 dump, torrent, etc.)
      - fetch_incremental():    Delta updates since a given date/cursor
      - get_source_version():   Current version/snapshot ID of the source
      - estimate_record_count(): Estimated total records available from this source

    The base class provides:
      - HTTP client with retry logic
      - SHA-256 checksum computation
      - Manifest tracking
      - Structured logging
    """

    def __init__(
        self,
        source_name: SourceName,
        *,
        api_key: Optional[str] = None,
        email: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: float = 60.0,
        max_retries: int = 5,
    ):
        self.source_name = source_name
        self.api_key = api_key
        self.email = email
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries

        self._client: Optional[httpx.AsyncClient] = None
        self.logger = logging.getLogger(f"connector.{source_name.value}")

    # --------------------------------------------------------------------------
    # HTTP Client
    # --------------------------------------------------------------------------

    @property
    def client(self) -> httpx.AsyncClient:
        """Lazy-initialized async HTTP client with default headers."""
        if self._client is None or self._client.is_closed:
            headers = self._build_headers()
            self._client = httpx.AsyncClient(
                base_url=self.base_url or "",
                headers=headers,
                timeout=httpx.Timeout(self.timeout, connect=30.0),
                follow_redirects=True,
                limits=httpx.Limits(
                    max_connections=20,
                    max_keepalive_connections=10,
                ),
            )
        return self._client

    def _build_headers(self) -> dict[str, str]:
        """Build default HTTP headers. Override in subclasses to add auth."""
        headers: dict[str, str] = {
            "User-Agent": f"ScholarAssist/1.0 (https://scholarassist.dev; mailto:{self.email or 'contact@scholarassist.dev'})"
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectTimeout, httpx.ReadTimeout)),
    )
    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Make an HTTP request with automatic retry and exponential backoff."""
        response = await self.client.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    async def _get(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self._request("GET", url, **kwargs)

    async def _post(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self._request("POST", url, **kwargs)

    # --------------------------------------------------------------------------
    # Checksum & Integrity
    # --------------------------------------------------------------------------

    @staticmethod
    def compute_checksum(file_path: Path, algorithm: str = "sha256") -> str:
        """Compute a SHA-256 checksum for a local file."""
        hasher = hashlib.new(algorithm)
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def verify_checksum(file_path: Path, expected: str, algorithm: str = "sha256") -> bool:
        """Verify a file's checksum matches the expected value."""
        actual = BaseConnector.compute_checksum(file_path, algorithm)
        return actual == expected

    # --------------------------------------------------------------------------
    # Abstract Methods — Must be implemented by each source connector
    # --------------------------------------------------------------------------

    @abstractmethod
    async def download_bulk(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
    ) -> IngestionManifest:
        """
        Download the full dataset from this source.

        For S3-hosted sources (OpenAlex, Semantic Scholar), this downloads the
        complete snapshot. For API-based sources, this paginates through all
        available records.

        Args:
            target_dir: Local directory to download files into.
            manifest: The ingestion manifest to track progress.

        Returns:
            Updated manifest with file checksums and download stats.
        """
        ...

    @abstractmethod
    async def fetch_incremental(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        since: Optional[datetime] = None,
        cursor: Optional[str] = None,
    ) -> IngestionManifest:
        """
        Fetch incremental updates since the last ingestion.

        Args:
            target_dir: Local directory to download files into.
            manifest: The ingestion manifest to track progress.
            since: Fetch records updated after this datetime.
            cursor: Resume from a specific cursor/offset (source-specific).

        Returns:
            Updated manifest with delta download stats and new cursor position.
        """
        ...

    @abstractmethod
    async def get_source_version(self) -> str:
        """
        Return the current version identifier for this source.

        Examples: "2026-06-01" for OpenAlex monthly snapshots,
                  "2026-W26" for Crossref weekly dumps.
        """
        ...

    @abstractmethod
    async def estimate_record_count(self) -> int:
        """
        Return the estimated total number of records available from this source.

        Used for progress tracking and capacity planning.
        """
        ...

    # --------------------------------------------------------------------------
    # Lifecycle
    # --------------------------------------------------------------------------

    async def close(self) -> None:
        """Close the HTTP client and release resources."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "BaseConnector":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _human_bytes(num_bytes: int) -> str:
    """Convert bytes to a human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0  # type: ignore[assignment]
    return f"{num_bytes:.1f} PB"
