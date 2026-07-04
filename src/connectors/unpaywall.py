"""
ScholarAssist — Unpaywall Connector

Provides access to `Unpaywall <https://unpaywall.org>`_ open-access link
data.

Bulk path:
    Unpaywall publishes periodic data-feed snapshots as a compressed JSON
    file.  The feed URL is obtained from the Unpaywall data-feed endpoint.
    The connector downloads the full snapshot and registers it in the
    manifest.

Incremental path:
    The Unpaywall REST API (``/v2/{doi}``) provides a single-DOI lookup.
    For incremental updates the connector accepts a list of DOIs (produced
    by an upstream change-detection pipeline, e.g. from Crossref) and
    enriches each DOI with Unpaywall OA metadata.  Results are batched
    into compressed JSONL files.

Authentication:
    All requests require an ``email`` query parameter.

Estimated catalog size: ~30 M DOI → OA-location mappings.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

from src.connectors.base import (
    BaseConnector,
    IngestionManifest,
    IngestionMode,
    SourceName,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_UNPAYWALL_API = "https://api.unpaywall.org"
_FEED_ENDPOINT = f"{_UNPAYWALL_API}/feed"
_DOI_ENDPOINT = f"{_UNPAYWALL_API}/v2"
_DOI_BATCH_SIZE = 500  # DOIs per output file
_CONCURRENT_DOI_LOOKUPS = 10


class UnpaywallConnector(BaseConnector):
    """
    Connector for `Unpaywall <https://unpaywall.org>`_ OA metadata.

    Parameters
    ----------
    email : str
        **Required.**  All Unpaywall API calls must include an email.
    api_key : str, optional
        Not required for the public API.
    doi_list : list[str], optional
        For incremental mode — the list of DOIs to look up.  If not
        supplied, ``fetch_incremental`` uses the Unpaywall changefile feed.
    """

    def __init__(
        self,
        *,
        email: str,
        api_key: Optional[str] = None,
        doi_list: Optional[list[str]] = None,
    ) -> None:
        if not email:
            raise ValueError("Unpaywall connector requires an email address.")
        super().__init__(
            SourceName.UNPAYWALL,
            api_key=api_key,
            email=email,
            base_url=_UNPAYWALL_API,
        )
        self.doi_list: list[str] = doi_list or []

    # ------------------------------------------------------------------
    # Bulk download — data-feed snapshot
    # ------------------------------------------------------------------

    async def download_bulk(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
    ) -> IngestionManifest:
        """
        Download the Unpaywall data-feed snapshot.

        Steps:
        1. ``GET /feed/snapshot`` to discover the latest snapshot URL.
        2. Stream-download the compressed JSON file.
        3. Record the file in the manifest with checksum.

        Parameters
        ----------
        target_dir : Path
            Destination directory.
        manifest : IngestionManifest
            Ingestion manifest.

        Returns
        -------
        IngestionManifest
        """
        target_dir.mkdir(parents=True, exist_ok=True)
        manifest.source_version = await self.get_source_version()

        # Discover snapshot URL from the feed endpoint
        self.logger.info("Discovering Unpaywall snapshot URL...")
        try:
            resp = await self._get(
                "/feed/snapshot",
                params={"email": self.email},
            )
            snapshot_data = resp.json()
            snapshot_url: str = snapshot_data.get("download_url", "")
            if not snapshot_url:
                # Fall-back: the public Unpaywall dataset
                snapshot_url = (
                    "https://unpaywall-data-snapshots.s3.us-west-2.amazonaws.com"
                    "/unpaywall_snapshot.jsonl.gz"
                )
        except Exception:
            self.logger.warning(
                "Could not discover snapshot URL — using default."
            )
            snapshot_url = (
                "https://unpaywall-data-snapshots.s3.us-west-2.amazonaws.com"
                "/unpaywall_snapshot.jsonl.gz"
            )

        filename = snapshot_url.rsplit("/", 1)[-1] or "unpaywall_snapshot.jsonl.gz"
        dest = target_dir / filename

        self.logger.info("Downloading snapshot: %s", snapshot_url)
        sha = hashlib.sha256()
        size = 0
        try:
            async with self.client.stream("GET", snapshot_url) as stream:
                stream.raise_for_status()
                with open(dest, "wb") as fh:
                    async for chunk in stream.aiter_bytes(65_536):
                        fh.write(chunk)
                        sha.update(chunk)
                        size += len(chunk)
            manifest.add_file(filename, size, sha.hexdigest())
            self.logger.info("Snapshot downloaded: %s (%d bytes)", filename, size)
        except Exception as exc:
            manifest.files_failed += 1
            manifest.errors.append(
                {
                    "file": snapshot_url,
                    "error": str(exc),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            self.logger.error("Snapshot download failed: %s", exc)

        manifest.mark_completed()
        return manifest

    # ------------------------------------------------------------------
    # Incremental — DOI-based enrichment + changefile feed
    # ------------------------------------------------------------------

    async def fetch_incremental(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        since: Optional[datetime] = None,
        cursor: Optional[str] = None,
    ) -> IngestionManifest:
        """
        Incrementally fetch Unpaywall data.

        Strategy depends on the data available:

        1. **Changefile feed** (preferred):  When *since* is provided the
           connector requests the changefile list from
           ``/feed/changefile?interval=week`` and downloads every changefile
           with a date >= *since*.

        2. **DOI lookup** (fallback): If ``self.doi_list`` is populated the
           connector looks up each DOI individually via ``/v2/{doi}`` and
           batches results into compressed JSONL files.

        Parameters
        ----------
        target_dir : Path
            Output directory.
        manifest : IngestionManifest
            Running manifest.
        since : datetime, optional
            Download changefiles dated on or after this date.
        cursor : str, optional
            Resume offset into ``self.doi_list`` for DOI-lookup mode.

        Returns
        -------
        IngestionManifest
        """
        target_dir.mkdir(parents=True, exist_ok=True)

        # --- Strategy 1: changefile feed ---
        if since is not None:
            await self._fetch_changefiles(target_dir, manifest, since)
        elif self.doi_list:
            # --- Strategy 2: per-DOI lookups ---
            start_offset = int(cursor) if cursor else 0
            await self._fetch_by_doi(target_dir, manifest, start_offset)
        else:
            self.logger.warning(
                "No `since` date or DOI list provided — nothing to do."
            )

        manifest.mark_completed()
        return manifest

    async def _fetch_changefiles(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        since: datetime,
    ) -> None:
        """Download Unpaywall changefiles newer than *since*."""
        self.logger.info("Fetching changefile list from Unpaywall feed...")
        try:
            resp = await self._get(
                "/feed/changefile",
                params={"interval": "week", "email": self.email},
            )
            changefiles: list[dict[str, Any]] = resp.json().get("list", [])
        except Exception as exc:
            self.logger.error("Could not fetch changefile list: %s", exc)
            manifest.errors.append(
                {
                    "error": str(exc),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            return

        since_str = since.strftime("%Y-%m-%d")
        for cf in changefiles:
            cf_date = cf.get("date", "")
            if cf_date < since_str:
                continue
            cf_url = cf.get("url", "")
            if not cf_url:
                continue

            filename = cf_url.rsplit("/", 1)[-1]
            dest = target_dir / filename
            self.logger.info("Downloading changefile %s", filename)

            sha = hashlib.sha256()
            size = 0
            try:
                async with self.client.stream("GET", cf_url) as stream:
                    stream.raise_for_status()
                    with open(dest, "wb") as fh:
                        async for chunk in stream.aiter_bytes(65_536):
                            fh.write(chunk)
                            sha.update(chunk)
                            size += len(chunk)
                manifest.add_file(filename, size, sha.hexdigest())
            except Exception as exc:
                manifest.files_failed += 1
                manifest.errors.append(
                    {
                        "file": cf_url,
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                self.logger.error("Changefile download failed: %s", exc)

    async def _fetch_by_doi(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        start_offset: int,
    ) -> None:
        """Look up each DOI individually and batch results."""
        semaphore = asyncio.Semaphore(_CONCURRENT_DOI_LOOKUPS)
        dois = self.doi_list[start_offset:]
        batch: list[dict[str, Any]] = []
        batch_idx = 0

        async def _lookup(doi: str) -> Optional[dict[str, Any]]:
            async with semaphore:
                try:
                    resp = await self._get(
                        f"/v2/{doi}",
                        params={"email": self.email},
                    )
                    return resp.json()
                except Exception as exc:
                    self.logger.debug("DOI lookup failed for %s: %s", doi, exc)
                    return None

        for i, doi in enumerate(dois):
            result = await _lookup(doi)
            if result:
                batch.append(result)

            if len(batch) >= _DOI_BATCH_SIZE or i == len(dois) - 1:
                if batch:
                    page_file = (
                        target_dir / f"unpaywall_doi_{batch_idx:06d}.jsonl.gz"
                    )
                    sha = hashlib.sha256()
                    with gzip.open(page_file, "wt", encoding="utf-8") as gz:
                        for rec in batch:
                            gz.write(json.dumps(rec) + "\n")
                    raw = page_file.read_bytes()
                    sha.update(raw)
                    manifest.add_file(page_file.name, len(raw), sha.hexdigest())
                    manifest.last_update_cursor = str(start_offset + i + 1)
                    batch_idx += 1
                    batch = []

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    async def get_source_version(self) -> str:
        """
        Return the date of the latest Unpaywall snapshot.

        Returns
        -------
        str
            Snapshot date, e.g. ``"2026-06-15"``.
        """
        try:
            resp = await self._get(
                "/feed/snapshot", params={"email": self.email}
            )
            data = resp.json()
            return data.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        except Exception:
            return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    async def estimate_record_count(self) -> int:
        """
        Estimate total DOI → OA-location mappings.

        Unpaywall does not expose a direct count endpoint, so we
        return the commonly-cited catalog size.

        Returns
        -------
        int
            ~30 M.
        """
        return 30_000_000
