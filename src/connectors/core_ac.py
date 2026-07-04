"""
ScholarAssist — CORE Connector

Provides access to the `CORE <https://core.ac.uk>`_ aggregated open-access
research papers via their REST API (v3).

Bulk path:
    CORE publishes dataset dumps that require registration.  This connector
    downloads the dump archive from the CORE data-dump endpoint and
    registers each file in the manifest.

Incremental path:
    The ``/search/works`` endpoint supports offset-based pagination with
    ``offset`` and ``limit`` parameters.  Optionally filter by
    ``updatedAfter`` to restrict to recently changed records.  The API
    also supports a **scroll** mode (``scrollId``) for more efficient
    deep traversal.

Authentication:
    All requests require an API key passed via
    ``Authorization: Bearer <key>``.

Estimated catalog size: ~200 M full-text papers.
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

_CORE_API = "https://api.core.ac.uk/v3"
_PAGE_LIMIT = 100  # max per page
_SCROLL_LIMIT = 1000  # for scroll mode
_BULK_DOWNLOAD_URL = "https://api.core.ac.uk/v3/data-dump"


class COREConnector(BaseConnector):
    """
    Connector for `CORE <https://core.ac.uk>`_ open-access aggregation.

    Parameters
    ----------
    api_key : str
        Required.  CORE API key (sent as ``Authorization: Bearer <key>``).
    email : str, optional
        Contact email for the ``User-Agent`` header.
    use_scroll : bool
        If ``True``, use scroll-based pagination (more efficient for
        large traversals).  Default ``True``.
    """

    def __init__(
        self,
        *,
        api_key: str,
        email: Optional[str] = None,
        use_scroll: bool = True,
    ) -> None:
        if not api_key:
            raise ValueError("CORE connector requires an API key.")
        super().__init__(
            SourceName.CORE,
            api_key=api_key,
            email=email,
            base_url=_CORE_API,
        )
        self.use_scroll = use_scroll

    # ------------------------------------------------------------------
    # Headers — Bearer auth
    # ------------------------------------------------------------------

    def _build_headers(self) -> dict[str, str]:
        headers = super()._build_headers()
        # BaseConnector already adds Authorization: Bearer <key>
        return headers

    # ------------------------------------------------------------------
    # Bulk download — data dump
    # ------------------------------------------------------------------

    async def download_bulk(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
    ) -> IngestionManifest:
        """
        Download the CORE data-dump archive.

        The CORE API exposes a ``/data-dump`` endpoint (requires an API
        key) that returns metadata about available dump files.  This
        connector downloads every listed archive.

        Parameters
        ----------
        target_dir : Path
            Local directory to store downloaded archives.
        manifest : IngestionManifest
            Running manifest.

        Returns
        -------
        IngestionManifest
        """
        target_dir.mkdir(parents=True, exist_ok=True)
        manifest.source_version = await self.get_source_version()

        self.logger.info("Requesting CORE data-dump listing...")
        try:
            resp = await self._get("/data-dump")
            dump_meta = resp.json()
        except Exception as exc:
            self.logger.error("Failed to fetch data-dump listing: %s", exc)
            manifest.mark_failed(str(exc))
            return manifest

        # dump_meta may contain a list of dump files or a single URL
        dump_entries: list[dict[str, Any]] = dump_meta if isinstance(dump_meta, list) else [dump_meta]

        for entry in dump_entries:
            dump_url = entry.get("downloadUrl") or entry.get("url", "")
            filename = entry.get("filename", dump_url.split("/")[-1])
            if not dump_url:
                self.logger.warning("Skipping entry with no URL: %s", entry)
                continue

            dest = target_dir / filename
            self.logger.info("Downloading %s -> %s", dump_url, dest)
            sha = hashlib.sha256()
            size = 0
            try:
                async with self.client.stream("GET", dump_url) as stream:
                    stream.raise_for_status()
                    with open(dest, "wb") as fh:
                        async for chunk in stream.aiter_bytes(65_536):
                            fh.write(chunk)
                            sha.update(chunk)
                            size += len(chunk)
                manifest.add_file(filename, size, sha.hexdigest())
                self.logger.info("Downloaded %s (%d bytes)", filename, size)
            except Exception as exc:
                manifest.files_failed += 1
                manifest.errors.append(
                    {
                        "file": dump_url,
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                self.logger.error("Download failed for %s: %s", dump_url, exc)

        manifest.mark_completed()
        return manifest

    # ------------------------------------------------------------------
    # Incremental — scroll / offset pagination
    # ------------------------------------------------------------------

    async def fetch_incremental(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        since: Optional[datetime] = None,
        cursor: Optional[str] = None,
    ) -> IngestionManifest:
        """
        Fetch works updated since *since* using the CORE search API.

        When ``use_scroll=True`` (default) the connector uses CORE's scroll
        API which is more efficient for deep traversals.  Otherwise it uses
        standard offset pagination.

        Parameters
        ----------
        target_dir : Path
            Output directory.
        manifest : IngestionManifest
            Running manifest.
        since : datetime, optional
            Only fetch records updated after this timestamp.
        cursor : str, optional
            Scroll ID or offset string to resume from.

        Returns
        -------
        IngestionManifest
        """
        target_dir.mkdir(parents=True, exist_ok=True)

        if self.use_scroll:
            return await self._scroll_fetch(target_dir, manifest, since, cursor)
        return await self._offset_fetch(target_dir, manifest, since, cursor)

    async def _scroll_fetch(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        since: Optional[datetime],
        scroll_id: Optional[str],
    ) -> IngestionManifest:
        """Incremental using CORE's scroll endpoint."""
        page = 0
        body: dict[str, Any] = {"limit": _SCROLL_LIMIT}
        if since:
            body["q"] = f"updatedDate>={since.strftime('%Y-%m-%d')}"
        else:
            body["q"] = "*"

        if scroll_id:
            body["scrollId"] = scroll_id

        while True:
            self.logger.info("Scroll page %d", page)
            try:
                resp = await self._post("/search/works", json=body)
                data = resp.json()
            except Exception as exc:
                self.logger.error("Scroll failed at page %d: %s", page, exc)
                manifest.errors.append(
                    {
                        "page": page,
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                break

            results: list[dict[str, Any]] = data.get("results", [])
            if not results:
                self.logger.info("Scroll exhausted.")
                break

            # Write page
            page_file = target_dir / f"core_scroll_{page:06d}.jsonl.gz"
            sha = hashlib.sha256()
            with gzip.open(page_file, "wt", encoding="utf-8") as gz:
                for rec in results:
                    gz.write(json.dumps(rec) + "\n")
            raw = page_file.read_bytes()
            sha.update(raw)
            manifest.add_file(page_file.name, len(raw), sha.hexdigest())

            new_scroll = data.get("scrollId")
            manifest.last_update_cursor = new_scroll
            page += 1

            if not new_scroll:
                break
            body["scrollId"] = new_scroll

        manifest.mark_completed()
        return manifest

    async def _offset_fetch(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        since: Optional[datetime],
        cursor: Optional[str],
    ) -> IngestionManifest:
        """Incremental using offset pagination."""
        offset = int(cursor) if cursor else 0
        page = 0

        query = f"updatedDate>={since.strftime('%Y-%m-%d')}" if since else "*"

        while True:
            params: dict[str, Any] = {
                "q": query,
                "offset": offset,
                "limit": _PAGE_LIMIT,
            }
            self.logger.info("Offset page %d (offset=%d)", page, offset)
            try:
                resp = await self._get("/search/works", params=params)
                data = resp.json()
            except Exception as exc:
                self.logger.error("Offset fetch page %d failed: %s", page, exc)
                manifest.errors.append(
                    {
                        "page": page,
                        "offset": offset,
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                break

            results: list[dict[str, Any]] = data.get("results", [])
            if not results:
                self.logger.info("No more results at offset %d.", offset)
                break

            page_file = target_dir / f"core_offset_{page:06d}.jsonl.gz"
            sha = hashlib.sha256()
            with gzip.open(page_file, "wt", encoding="utf-8") as gz:
                for rec in results:
                    gz.write(json.dumps(rec) + "\n")
            raw = page_file.read_bytes()
            sha.update(raw)
            manifest.add_file(page_file.name, len(raw), sha.hexdigest())

            offset += len(results)
            manifest.last_update_cursor = str(offset)
            page += 1

            total_hits = data.get("totalHits", 0)
            if offset >= total_hits:
                self.logger.info("Reached totalHits=%d.", total_hits)
                break

        manifest.mark_completed()
        return manifest

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    async def get_source_version(self) -> str:
        """
        Return the CORE data-dump version or current date.

        Returns
        -------
        str
            A version identifier, e.g. ``"2026-06-28"``.
        """
        try:
            resp = await self._get("/data-dump")
            data = resp.json()
            if isinstance(data, list) and data:
                return data[0].get("createdDate", "")[:10]
            if isinstance(data, dict):
                return data.get("createdDate", "")[:10]
        except Exception:
            self.logger.warning("Could not determine CORE version.")
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    async def estimate_record_count(self) -> int:
        """
        Estimate total works by issuing a count query.

        Returns
        -------
        int
            Estimated total (~200 M).
        """
        try:
            resp = await self._get(
                "/search/works",
                params={"q": "*", "limit": 0},
            )
            data = resp.json()
            return int(data.get("totalHits", 200_000_000))
        except Exception:
            self.logger.warning(
                "Could not estimate record count — returning default."
            )
            return 200_000_000
