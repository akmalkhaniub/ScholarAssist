"""
ScholarAssist — DBLP Connector

Provides bulk and incremental access to the `DBLP <https://dblp.org>`_
computer science bibliography.

Bulk path:
    DBLP publishes a complete XML dump at
    ``https://dblp.org/xml/dblp.xml.gz`` along with a DTD.  This connector
    downloads both files, verifies checksums, and records them in the
    manifest.  An MD5 checksum file is provided by DBLP.

Incremental path:
    The DBLP search API at ``https://dblp.org/search/publ/api`` supports
    JSON output (``?format=json``), hit-offset pagination via the ``f``
    (first) and ``h`` (hits-per-page) parameters, and full-text query.
    This connector paginates through results matching a time-windowed
    query and persists each page as compressed JSONL.

Estimated catalog size: ~7 M records.
"""

from __future__ import annotations

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

_DBLP_XML_URL = "https://dblp.org/xml/dblp.xml.gz"
_DBLP_DTD_URL = "https://dblp.org/xml/dblp.dtd"
_DBLP_MD5_URL = "https://dblp.org/xml/dblp.xml.gz.md5"
_DBLP_SEARCH_API = "https://dblp.org/search/publ/api"
_HITS_PER_PAGE = 1000  # maximum hits per request
_MAX_TOTAL_HITS = 10_000_000  # safety cap for pagination


class DBLPConnector(BaseConnector):
    """
    Connector for `DBLP <https://dblp.org>`_ computer science bibliography.

    Parameters
    ----------
    email : str, optional
        Contact email for the ``User-Agent`` header.
    """

    def __init__(
        self,
        *,
        email: Optional[str] = None,
    ) -> None:
        super().__init__(
            SourceName.DBLP,
            email=email,
            base_url="https://dblp.org",
        )

    # ------------------------------------------------------------------
    # Bulk download — XML dump
    # ------------------------------------------------------------------

    async def download_bulk(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
    ) -> IngestionManifest:
        """
        Download the full DBLP XML dump and its DTD.

        Files downloaded:
        * ``dblp.xml.gz`` — the complete bibliography (~3 GB compressed)
        * ``dblp.dtd`` — the XML schema definition
        * ``dblp.xml.gz.md5`` — checksum for integrity verification

        After downloading, the MD5 checksum is verified against the
        expected value published by DBLP.

        Parameters
        ----------
        target_dir : Path
            Destination directory.
        manifest : IngestionManifest
            Running manifest.

        Returns
        -------
        IngestionManifest
        """
        target_dir.mkdir(parents=True, exist_ok=True)
        manifest.source_version = await self.get_source_version()

        files_to_download = [
            (_DBLP_XML_URL, "dblp.xml.gz"),
            (_DBLP_DTD_URL, "dblp.dtd"),
            (_DBLP_MD5_URL, "dblp.xml.gz.md5"),
        ]

        for url, filename in files_to_download:
            dest = target_dir / filename
            self.logger.info("Downloading %s", url)
            sha = hashlib.sha256()
            size = 0
            try:
                async with self.client.stream("GET", url) as stream:
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
                        "file": url,
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                self.logger.error("Failed to download %s: %s", url, exc)

        # Verify MD5 if both files were downloaded
        md5_file = target_dir / "dblp.xml.gz.md5"
        xml_file = target_dir / "dblp.xml.gz"
        if md5_file.exists() and xml_file.exists():
            expected_md5 = md5_file.read_text().strip().split()[0]
            actual_md5 = self.compute_checksum(xml_file, algorithm="md5")
            if actual_md5 == expected_md5:
                self.logger.info("MD5 checksum verified for dblp.xml.gz")
            else:
                msg = (
                    f"MD5 mismatch for dblp.xml.gz: "
                    f"expected={expected_md5}, actual={actual_md5}"
                )
                self.logger.warning(msg)
                manifest.errors.append(
                    {
                        "file": "dblp.xml.gz",
                        "error": msg,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )

        manifest.mark_completed()
        return manifest

    # ------------------------------------------------------------------
    # Incremental — Search API with offset pagination
    # ------------------------------------------------------------------

    async def fetch_incremental(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        since: Optional[datetime] = None,
        cursor: Optional[str] = None,
    ) -> IngestionManifest:
        """
        Fetch publications via the DBLP search API.

        DBLP does not support a native ``updatedSince`` filter; instead
        the connector queries by year range (``year:{YYYY}:``) and
        paginates through all hits using offset-based pagination.

        Parameters
        ----------
        target_dir : Path
            Output directory.
        manifest : IngestionManifest
            Running manifest.
        since : datetime, optional
            Fetch publications from this year onward.
        cursor : str, optional
            Numeric offset to resume from.

        Returns
        -------
        IngestionManifest
        """
        target_dir.mkdir(parents=True, exist_ok=True)

        # Build a year-bounded query if `since` is provided
        if since is not None:
            query = f"year:{since.year}:"
        else:
            query = "*"

        offset = int(cursor) if cursor else 0
        page_number = 0

        while offset < _MAX_TOTAL_HITS:
            params: dict[str, Any] = {
                "q": query,
                "format": "json",
                "h": _HITS_PER_PAGE,
                "f": offset,
            }
            self.logger.info(
                "DBLP search page %d (offset=%d)", page_number, offset
            )
            try:
                resp = await self._get(_DBLP_SEARCH_API, params=params)
                data = resp.json()
            except Exception as exc:
                self.logger.error("Search page %d failed: %s", page_number, exc)
                manifest.errors.append(
                    {
                        "page": page_number,
                        "offset": offset,
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                break

            result = data.get("result", {})
            hits_container = result.get("hits", {})
            total = int(hits_container.get("@total", 0))
            hit_list: list[dict[str, Any]] = hits_container.get("hit", [])

            if not hit_list:
                self.logger.info("No more hits at offset %d.", offset)
                break

            # Persist page
            page_file = target_dir / f"dblp_incr_{page_number:06d}.jsonl.gz"
            sha = hashlib.sha256()
            with gzip.open(page_file, "wt", encoding="utf-8") as gz:
                for hit in hit_list:
                    info = hit.get("info", hit)
                    gz.write(json.dumps(info) + "\n")
            raw = page_file.read_bytes()
            sha.update(raw)
            manifest.add_file(page_file.name, len(raw), sha.hexdigest())

            offset += len(hit_list)
            manifest.last_update_cursor = str(offset)
            page_number += 1

            if offset >= total:
                self.logger.info(
                    "Reached total hits (%d). Done.", total
                )
                break

        manifest.mark_completed()
        return manifest

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    async def get_source_version(self) -> str:
        """
        Return the DBLP dump version by checking the ``Last-Modified``
        header of the XML dump URL.

        Returns
        -------
        str
            Version date, e.g. ``"2026-06-30"``.
        """
        try:
            resp = await self._request("HEAD", _DBLP_XML_URL)
            last_modified = resp.headers.get("Last-Modified", "")
            if last_modified:
                # Parse RFC-2822 date
                from email.utils import parsedate_to_datetime

                dt = parsedate_to_datetime(last_modified)
                return dt.strftime("%Y-%m-%d")
        except Exception:
            self.logger.warning("Could not determine DBLP version.")
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    async def estimate_record_count(self) -> int:
        """
        Estimate total publications by issuing a wildcard search.

        Returns
        -------
        int
            Total publication count (~7 M).
        """
        try:
            resp = await self._get(
                _DBLP_SEARCH_API,
                params={"q": "*", "format": "json", "h": 0},
            )
            data = resp.json()
            total = data.get("result", {}).get("hits", {}).get("@total", 0)
            return int(total) if int(total) > 0 else 7_000_000
        except Exception:
            return 7_000_000
