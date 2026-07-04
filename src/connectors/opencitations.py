"""
ScholarAssist — OpenCitations Connector

Provides access to the `OpenCitations <https://opencitations.net>`_ COCI
(Crossref Open Citation Index) dataset.

Bulk path:
    OpenCitations publishes CSV data dumps at
    ``https://opencitations.net/download``.  The connector fetches the
    download page, discovers dump file URLs, and downloads each archive.

Incremental path:
    The REST API at
    ``https://opencitations.net/index/coci/api/v1/citations/{doi}``
    returns all citations for a given DOI.  For incremental ingestion the
    connector accepts a list of DOIs (e.g. newly published or updated works
    from an upstream source) and fetches citation data for each.

Estimated catalog size: ~1.5 B citation links.
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

_OC_API = "https://opencitations.net"
_COCI_API = f"{_OC_API}/index/coci/api/v1"
_DOWNLOAD_PAGE = f"{_OC_API}/download"
_DOI_BATCH_SIZE = 500
_CONCURRENT_DOI_LOOKUPS = 5


class OpenCitationsConnector(BaseConnector):
    """
    Connector for `OpenCitations <https://opencitations.net>`_ COCI data.

    Parameters
    ----------
    email : str, optional
        Contact email for the ``User-Agent`` header.
    doi_list : list[str], optional
        For incremental mode — the DOIs whose citation data should be
        fetched.  Typically produced by an upstream connector that has
        detected newly added or updated works.
    """

    def __init__(
        self,
        *,
        email: Optional[str] = None,
        doi_list: Optional[list[str]] = None,
    ) -> None:
        super().__init__(
            SourceName.OPENCITATIONS,
            email=email,
            base_url=_OC_API,
        )
        self.doi_list: list[str] = doi_list or []

    # ------------------------------------------------------------------
    # Bulk download — CSV data dumps
    # ------------------------------------------------------------------

    async def download_bulk(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
    ) -> IngestionManifest:
        """
        Download the OpenCitations COCI CSV data dumps.

        The connector fetches the HTML download page, extracts URLs
        ending in ``.zip`` or ``.csv.gz``, and downloads each one.

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

        # Discover dump file URLs from the download page
        self.logger.info("Fetching OpenCitations download page...")
        try:
            resp = await self._get("/download")
            page_text = resp.text
        except Exception as exc:
            self.logger.error("Could not fetch download page: %s", exc)
            manifest.mark_failed(str(exc))
            return manifest

        # Simple link extraction (avoids an lxml/bs4 dependency)
        dump_urls = self._extract_dump_urls(page_text)
        if not dump_urls:
            # Fallback: well-known COCI dump URLs
            dump_urls = [
                "https://opencitations.net/download/coci.csv.zip",
            ]
            self.logger.warning(
                "Could not discover dump URLs — using fallback list."
            )

        self.logger.info("Found %d dump file(s) to download.", len(dump_urls))

        for url in dump_urls:
            filename = url.rsplit("/", 1)[-1]
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
                self.logger.error("Download failed for %s: %s", url, exc)

        manifest.mark_completed()
        return manifest

    @staticmethod
    def _extract_dump_urls(html: str) -> list[str]:
        """
        Extract data-dump file URLs from the OpenCitations download page.

        Performs a lightweight scan for ``href="..."`` patterns pointing
        to ``.zip``, ``.csv.gz``, or ``.tar.gz`` files.

        Parameters
        ----------
        html : str
            Raw HTML of the download page.

        Returns
        -------
        list[str]
            Discovered dump-file URLs.
        """
        import re

        pattern = re.compile(
            r'href=["\']'
            r'(https?://[^"\']+\.(?:zip|csv\.gz|tar\.gz))'
            r'["\']',
            re.IGNORECASE,
        )
        return list(dict.fromkeys(pattern.findall(html)))  # deduplicate

    # ------------------------------------------------------------------
    # Incremental — per-DOI citation lookups
    # ------------------------------------------------------------------

    async def fetch_incremental(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        since: Optional[datetime] = None,
        cursor: Optional[str] = None,
    ) -> IngestionManifest:
        """
        Fetch citation data for a list of DOIs via the COCI API.

        Each DOI is looked up at
        ``/index/coci/api/v1/citations/{doi}`` and the results are
        batched into compressed JSONL files.

        Parameters
        ----------
        target_dir : Path
            Output directory.
        manifest : IngestionManifest
            Running manifest.
        since : datetime, optional
            Not used directly — the DOI list is assumed to have been
            pre-filtered by the caller.
        cursor : str, optional
            Numeric offset into ``self.doi_list`` to resume from.

        Returns
        -------
        IngestionManifest
        """
        target_dir.mkdir(parents=True, exist_ok=True)

        if not self.doi_list:
            self.logger.warning(
                "No DOI list supplied for incremental citation fetch."
            )
            manifest.mark_completed()
            return manifest

        start = int(cursor) if cursor else 0
        dois = self.doi_list[start:]
        semaphore = asyncio.Semaphore(_CONCURRENT_DOI_LOOKUPS)
        batch: list[dict[str, Any]] = []
        batch_idx = 0

        async def _lookup(doi: str) -> list[dict[str, Any]]:
            async with semaphore:
                try:
                    resp = await self._get(
                        f"/index/coci/api/v1/citations/{doi}"
                    )
                    data = resp.json()
                    return data if isinstance(data, list) else [data]
                except Exception as exc:
                    self.logger.debug(
                        "Citation lookup failed for %s: %s", doi, exc
                    )
                    return []

        for i, doi in enumerate(dois):
            citations = await _lookup(doi)
            for cit in citations:
                cit["_queried_doi"] = doi
                batch.append(cit)

            # Flush batch
            if len(batch) >= _DOI_BATCH_SIZE or i == len(dois) - 1:
                if batch:
                    page_file = (
                        target_dir
                        / f"opencit_incr_{batch_idx:06d}.jsonl.gz"
                    )
                    sha = hashlib.sha256()
                    with gzip.open(page_file, "wt", encoding="utf-8") as gz:
                        for rec in batch:
                            gz.write(json.dumps(rec) + "\n")
                    raw = page_file.read_bytes()
                    sha.update(raw)
                    manifest.add_file(page_file.name, len(raw), sha.hexdigest())

                    manifest.last_update_cursor = str(start + i + 1)
                    batch_idx += 1
                    batch = []

        manifest.mark_completed()
        return manifest

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    async def get_source_version(self) -> str:
        """
        Return a version identifier for the OpenCitations COCI dataset.

        The download page occasionally includes a date stamp; otherwise
        the connector falls back to today's date.

        Returns
        -------
        str
            Version string, e.g. ``"2026-06-28"``.
        """
        try:
            resp = await self._get("/download")
            text = resp.text
            # Look for a date pattern on the page (YYYY-MM-DD)
            import re

            match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
            if match:
                return match.group(1)
        except Exception:
            pass
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    async def estimate_record_count(self) -> int:
        """
        Estimate the number of citation links in COCI.

        Returns
        -------
        int
            ~1.5 B citation links.
        """
        return 1_500_000_000
