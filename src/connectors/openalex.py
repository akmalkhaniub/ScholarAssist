"""
ScholarAssist — OpenAlex Connector

Provides bulk ingest from the OpenAlex S3 snapshot and incremental updates
via the public REST API at https://api.openalex.org.

Bulk path:
    The OpenAlex dataset is released as monthly snapshots and hosted on an
    Amazon S3 bucket (``s3://openalex``).  This connector uses the public
    manifest endpoint to enumerate every compressed JSONL file in the snapshot,
    downloads them over HTTPS (no AWS credentials required), and records each
    file in the :class:`IngestionManifest`.

Incremental path:
    The REST API supports ``filter=from_updated_date:<ISO-date>`` combined
    with cursor-based pagination (``cursor=*`` for the first page, then use
    the returned ``meta.next_cursor``).  Results are written as one JSONL
    file per page.

Entities supported: **works** (primary), **authors**, **venues**.
Estimated catalog size: ~250 M records.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

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

_OPENALEX_API = "https://api.openalex.org"
_SNAPSHOT_MANIFEST_URL = (
    "https://openalex.s3.amazonaws.com/data/works/manifest"
)
_ENTITY_TYPES = ("works", "authors", "venues")
_PAGE_SIZE = 200  # max per_page for cursor pagination
_BULK_DOWNLOAD_CONCURRENCY = 8


class OpenAlexConnector(BaseConnector):
    """
    Connector for the `OpenAlex <https://openalex.org>`_ scholarly dataset.

    Parameters
    ----------
    api_key : str, optional
        Not required for the public API; passing one is harmless.
    email : str, optional
        Including an email address puts you in the *polite pool*, which
        receives faster responses and higher rate-limits.
    entity_types : tuple[str, ...], optional
        Which entity types to ingest.  Defaults to ``("works",)``.
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        email: Optional[str] = None,
        entity_types: tuple[str, ...] = ("works",),
    ) -> None:
        super().__init__(
            SourceName.OPENALEX,
            api_key=api_key,
            email=email,
            base_url=_OPENALEX_API,
        )
        self.entity_types = entity_types

    # ------------------------------------------------------------------
    # Header customisation (polite pool)
    # ------------------------------------------------------------------

    def _build_headers(self) -> dict[str, str]:
        headers = super()._build_headers()
        if self.email:
            headers["User-Agent"] = (
                f"ScholarAssist/1.0 (mailto:{self.email})"
            )
        return headers

    # ------------------------------------------------------------------
    # Bulk download — S3 snapshot via HTTPS
    # ------------------------------------------------------------------

    async def download_bulk(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
    ) -> IngestionManifest:
        """
        Download the full OpenAlex snapshot from the S3 bucket via HTTPS.

        For each entity type the connector:
        1. Fetches the per-entity ``manifest`` file that lists every
           compressed JSONL part-file with its S3 key and expected record
           count.
        2. Downloads each part file, computes a SHA-256 checksum, and
           registers it in the :class:`IngestionManifest`.

        Parameters
        ----------
        target_dir : Path
            Local directory in which to store downloaded files.
        manifest : IngestionManifest
            Running manifest for this ingestion session.

        Returns
        -------
        IngestionManifest
            The updated manifest.
        """
        target_dir.mkdir(parents=True, exist_ok=True)
        manifest.source_version = await self.get_source_version()

        for entity in self.entity_types:
            self.logger.info("Fetching manifest for entity=%s", entity)
            manifest_url = (
                f"https://openalex.s3.amazonaws.com/data/{entity}/manifest"
            )
            try:
                resp = await self._get(manifest_url)
                snapshot_manifest = resp.json()
            except Exception as exc:
                self.logger.error(
                    "Failed to fetch snapshot manifest for %s: %s",
                    entity,
                    exc,
                )
                manifest.errors.append(
                    {
                        "entity": entity,
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                continue

            entries: list[dict[str, Any]] = snapshot_manifest.get("entries", [])
            self.logger.info(
                "Snapshot manifest for %s contains %d part files",
                entity,
                len(entries),
            )

            semaphore = asyncio.Semaphore(_BULK_DOWNLOAD_CONCURRENCY)

            async def _download_entry(entry: dict[str, Any]) -> None:
                url = entry["url"]
                relative_path = entry.get("url", "").split("/data/")[-1]
                dest = target_dir / relative_path.replace("/", "_")
                async with semaphore:
                    await self._download_file(url, dest, manifest)

            tasks = [_download_entry(e) for e in entries]
            await asyncio.gather(*tasks)

        manifest.mark_completed()
        return manifest

    async def _download_file(
        self,
        url: str,
        dest: Path,
        manifest: IngestionManifest,
    ) -> None:
        """Stream-download a file, compute its checksum, and register it."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        sha = hashlib.sha256()
        size = 0
        try:
            async with self.client.stream("GET", url) as stream:
                stream.raise_for_status()
                with open(dest, "wb") as fh:
                    async for chunk in stream.aiter_bytes(chunk_size=65_536):
                        fh.write(chunk)
                        sha.update(chunk)
                        size += len(chunk)
            manifest.add_file(dest.name, size, sha.hexdigest())
            self.logger.debug("Downloaded %s (%d bytes)", dest.name, size)
        except Exception as exc:
            manifest.files_failed += 1
            manifest.errors.append(
                {
                    "file": url,
                    "error": str(exc),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            self.logger.error("Failed to download %s: %s", url, exc)

    # ------------------------------------------------------------------
    # Incremental — REST API with cursor pagination
    # ------------------------------------------------------------------

    async def fetch_incremental(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        since: Optional[datetime] = None,
        cursor: Optional[str] = None,
    ) -> IngestionManifest:
        """
        Fetch works updated since *since* using OpenAlex cursor pagination.

        Each page of results is written as a compressed JSONL file into
        *target_dir*.  The cursor for the **next** un-fetched page is stored
        in ``manifest.last_update_cursor`` so that a subsequent run can
        resume from where we left off.

        Parameters
        ----------
        target_dir : Path
            Directory to store the downloaded JSONL files.
        manifest : IngestionManifest
            Running manifest for this ingestion session.
        since : datetime, optional
            Only return records updated on or after this date.
        cursor : str, optional
            Resume pagination from this cursor.  If ``None`` and *since* is
            provided, pagination starts from the beginning.

        Returns
        -------
        IngestionManifest
        """
        target_dir.mkdir(parents=True, exist_ok=True)

        if since is None and cursor is None:
            self.logger.warning(
                "Neither `since` nor `cursor` supplied — "
                "defaulting to full incremental sweep."
            )

        params: dict[str, Any] = {
            "per_page": _PAGE_SIZE,
            "cursor": cursor or "*",
        }
        if since is not None:
            date_str = since.strftime("%Y-%m-%d")
            params["filter"] = f"from_updated_date:{date_str}"

        if self.email:
            params["mailto"] = self.email

        page_number = 0
        while True:
            self.logger.info(
                "Fetching incremental page %d (cursor=%s)",
                page_number,
                params.get("cursor", "?"),
            )
            try:
                resp = await self._get("/works", params=params)
                data = resp.json()
            except Exception as exc:
                self.logger.error("Incremental fetch failed: %s", exc)
                manifest.errors.append(
                    {
                        "error": str(exc),
                        "page": page_number,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                break

            results: list[dict[str, Any]] = data.get("results", [])
            if not results:
                self.logger.info("No more results — incremental sync done.")
                break

            # Write page as gzipped JSONL
            page_file = target_dir / f"openalex_incr_{page_number:06d}.jsonl.gz"
            sha = hashlib.sha256()
            size = 0
            with gzip.open(page_file, "wt", encoding="utf-8") as gz:
                for record in results:
                    line = json.dumps(record) + "\n"
                    gz.write(line)
            raw = page_file.read_bytes()
            sha.update(raw)
            size = len(raw)
            manifest.add_file(page_file.name, size, sha.hexdigest())

            # Advance cursor
            next_cursor = data.get("meta", {}).get("next_cursor")
            manifest.last_update_cursor = next_cursor
            page_number += 1

            if next_cursor is None:
                self.logger.info("Cursor exhausted — incremental sync done.")
                break
            params["cursor"] = next_cursor

        manifest.mark_completed()
        return manifest

    # ------------------------------------------------------------------
    # Metadata helpers
    # ------------------------------------------------------------------

    async def get_source_version(self) -> str:
        """
        Return the latest OpenAlex snapshot date.

        The manifest file hosted on S3 includes a ``meta.last_updated``
        field with the snapshot creation date.

        Returns
        -------
        str
            Snapshot date in ISO-8601 format, e.g. ``"2026-06-01"``.
        """
        manifest_url = (
            "https://openalex.s3.amazonaws.com/data/works/manifest"
        )
        resp = await self._get(manifest_url)
        data = resp.json()
        # The manifest stores "meta": {"last_updated": "2026-06-01"}
        return data.get("meta", {}).get(
            "last_updated", datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )

    async def estimate_record_count(self) -> int:
        """
        Return an estimate of the total number of works in OpenAlex.

        The ``/works`` endpoint returns ``meta.count`` with the total
        number of matching works (unfiltered = entire catalog).

        Returns
        -------
        int
            Estimated total work count (~250 M as of mid-2026).
        """
        params: dict[str, Any] = {"per_page": 1}
        if self.email:
            params["mailto"] = self.email
        resp = await self._get("/works", params=params)
        data = resp.json()
        return int(data.get("meta", {}).get("count", 250_000_000))
