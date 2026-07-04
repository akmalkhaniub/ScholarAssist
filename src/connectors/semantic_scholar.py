"""
ScholarAssist — Semantic Scholar Connector

Provides bulk access via the public S3 requester-pays datasets and
incremental updates through the Semantic Scholar Academic Graph API.

Bulk path:
    Semantic Scholar publishes weekly dataset releases at
    ``s3://ai2-s2-research-public/open-corpus/``.  The release manifest
    lists compressed JSONL files for papers, abstracts, and citations.
    This connector downloads those files over HTTPS from the public
    endpoint exposed by Semantic Scholar (``https://api.semanticscholar.org
    /datasets/v1/release``).

Incremental path:
    The Graph API ``/paper/search/bulk`` endpoint supports token-based
    pagination.  Results are filtered by ``publicationDateOrYear`` and
    stored as compressed JSONL.

Rate limits:
    * Without an API key: 1 req/sec
    * With an API key (``x-api-key`` header): 10 req/sec

Estimated catalog size: ~200 M papers.
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

_S2_API = "https://api.semanticscholar.org"
_DATASETS_API = f"{_S2_API}/datasets/v1/release"
_GRAPH_API = f"{_S2_API}/graph/v1"
_BULK_SEARCH = f"{_GRAPH_API}/paper/search/bulk"
_BATCH_SIZE = 1000
_RATE_LIMIT_DELAY_NO_KEY = 1.1  # seconds — stay under 1 req/s
_RATE_LIMIT_DELAY_KEY = 0.12  # seconds — stay under 10 req/s
_BULK_DOWNLOAD_CONCURRENCY = 6
_DATASET_NAMES = ("papers", "abstracts", "citations")


class SemanticScholarConnector(BaseConnector):
    """
    Connector for `Semantic Scholar <https://www.semanticscholar.org>`_.

    Parameters
    ----------
    api_key : str, optional
        S2 API key (sent via ``x-api-key``).  Increases rate limit to
        10 req/sec.
    email : str, optional
        Contact email for the ``User-Agent`` header.
    datasets : tuple[str, ...], optional
        Which dataset splits to ingest in bulk mode.  Defaults to
        ``("papers", "abstracts", "citations")``.
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        email: Optional[str] = None,
        datasets: tuple[str, ...] = _DATASET_NAMES,
    ) -> None:
        super().__init__(
            SourceName.SEMANTIC_SCHOLAR,
            api_key=api_key,
            email=email,
            base_url=_S2_API,
        )
        self.datasets = datasets
        self._delay = (
            _RATE_LIMIT_DELAY_KEY if api_key else _RATE_LIMIT_DELAY_NO_KEY
        )

    # ------------------------------------------------------------------
    # Headers
    # ------------------------------------------------------------------

    def _build_headers(self) -> dict[str, str]:
        headers = super()._build_headers()
        if self.api_key:
            # S2 expects the key in a custom header, not Bearer auth
            headers["x-api-key"] = self.api_key
            headers.pop("Authorization", None)
        return headers

    # ------------------------------------------------------------------
    # Bulk download — dataset releases
    # ------------------------------------------------------------------

    async def download_bulk(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
    ) -> IngestionManifest:
        """
        Download the latest Semantic Scholar dataset release.

        Steps:
        1. ``GET /datasets/v1/release`` → list of releases (sorted by date)
        2. Pick the most recent release and fetch its metadata.
        3. For each requested dataset (papers, abstracts, citations)
           download every part file.

        Parameters
        ----------
        target_dir : Path
            Destination directory.
        manifest : IngestionManifest
            Ingestion manifest to track progress.

        Returns
        -------
        IngestionManifest
        """
        target_dir.mkdir(parents=True, exist_ok=True)

        # 1. Discover latest release
        self.logger.info("Discovering latest S2 dataset release...")
        resp = await self._get(_DATASETS_API)
        releases: list[str] = resp.json()  # list of date strings
        if not releases:
            manifest.mark_failed("No dataset releases found")
            return manifest

        latest_release = sorted(releases)[-1]
        manifest.source_version = latest_release
        self.logger.info("Latest release: %s", latest_release)

        # 2. Get release details
        release_resp = await self._get(f"{_DATASETS_API}/{latest_release}")
        release_meta = release_resp.json()

        # 3. Download each dataset
        semaphore = asyncio.Semaphore(_BULK_DOWNLOAD_CONCURRENCY)

        for ds_entry in release_meta.get("datasets", []):
            ds_name = ds_entry.get("name", "")
            if ds_name not in self.datasets:
                continue

            files: list[str] = ds_entry.get("files", [])
            self.logger.info(
                "Dataset %s: %d files to download", ds_name, len(files)
            )

            async def _dl(url: str, name: str) -> None:
                dest = target_dir / f"{name}_{url.split('/')[-1]}"
                async with semaphore:
                    await self._download_file(url, dest, manifest)

            tasks = [_dl(f, ds_name) for f in files]
            await asyncio.gather(*tasks)

        manifest.mark_completed()
        return manifest

    async def _download_file(
        self,
        url: str,
        dest: Path,
        manifest: IngestionManifest,
    ) -> None:
        """Stream-download a single file and register it."""
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
    # Incremental — Graph API bulk search
    # ------------------------------------------------------------------

    async def fetch_incremental(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        since: Optional[datetime] = None,
        cursor: Optional[str] = None,
    ) -> IngestionManifest:
        """
        Fetch papers updated since *since* using the bulk search endpoint.

        The ``/paper/search/bulk`` endpoint returns up to 1 000 results
        per page with a continuation ``token``.  This connector respects
        rate limits by sleeping between requests.

        Parameters
        ----------
        target_dir : Path
            Output directory.
        manifest : IngestionManifest
            Running manifest.
        since : datetime, optional
            Only retrieve papers published on or after this date.
        cursor : str, optional
            Continuation token from a previous run.

        Returns
        -------
        IngestionManifest
        """
        target_dir.mkdir(parents=True, exist_ok=True)

        params: dict[str, Any] = {
            "fields": "paperId,title,year,authors,abstract,citationCount,publicationDate",
        }
        if since is not None:
            params["publicationDateOrYear"] = (
                f"{since.strftime('%Y-%m-%d')}:"
            )
        if cursor is not None:
            params["token"] = cursor

        page_number = 0
        while True:
            self.logger.info(
                "S2 incremental page %d (token=%s)",
                page_number,
                params.get("token", "initial")[:30],
            )
            try:
                resp = await self._get(
                    "/graph/v1/paper/search/bulk", params=params
                )
                data = resp.json()
            except Exception as exc:
                self.logger.error("Incremental fetch failed: %s", exc)
                manifest.errors.append(
                    {
                        "page": page_number,
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                break

            papers: list[dict[str, Any]] = data.get("data", [])
            if not papers:
                self.logger.info("No more papers — incremental sync done.")
                break

            # Persist page as gzipped JSONL
            page_file = target_dir / f"s2_incr_{page_number:06d}.jsonl.gz"
            sha = hashlib.sha256()
            with gzip.open(page_file, "wt", encoding="utf-8") as gz:
                for paper in papers:
                    gz.write(json.dumps(paper) + "\n")
            raw = page_file.read_bytes()
            sha.update(raw)
            manifest.add_file(page_file.name, len(raw), sha.hexdigest())

            # Advance token
            next_token = data.get("token")
            manifest.last_update_cursor = next_token
            page_number += 1

            if next_token is None:
                self.logger.info("Token exhausted — done.")
                break
            params["token"] = next_token

            # Rate-limit
            await asyncio.sleep(self._delay)

        manifest.mark_completed()
        return manifest

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    async def get_source_version(self) -> str:
        """
        Return the date of the latest Semantic Scholar dataset release.

        Returns
        -------
        str
            Release date, e.g. ``"2026-06-28"``.
        """
        resp = await self._get(_DATASETS_API)
        releases: list[str] = resp.json()
        if releases:
            return sorted(releases)[-1]
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    async def estimate_record_count(self) -> int:
        """
        Estimate total papers by querying the bulk search endpoint.

        Returns
        -------
        int
            Total paper count (~200 M).
        """
        try:
            resp = await self._get(
                "/graph/v1/paper/search/bulk",
                params={"query": "", "fields": "paperId"},
            )
            data = resp.json()
            return int(data.get("total", 200_000_000))
        except Exception:
            self.logger.warning(
                "Could not estimate record count — returning default."
            )
            return 200_000_000
