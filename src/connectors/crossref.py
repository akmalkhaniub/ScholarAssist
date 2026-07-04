"""
ScholarAssist — Crossref Connector

Provides bulk and incremental access to the Crossref metadata registry
via the REST API at https://api.crossref.org.

Bulk path:
    The Crossref Public Data File is a multi-terabyte JSON dump, but it
    requires a Plus membership.  For the open/free path this connector uses
    **cursor-based deep paging** through the ``/works`` endpoint.  Each page
    is persisted as a compressed JSONL file.

Incremental path:
    Uses ``filter=from-update-date:<YYYY-MM-DD>`` combined with cursor
    pagination to retrieve only records that changed since the last sync.

Polite pool:
    Including an email address in the ``User-Agent`` header or the ``mailto``
    query parameter moves requests into Crossref's *polite pool* which has
    faster and more reliable service.

Estimated catalog size: ~150 M works.
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

_CROSSREF_API = "https://api.crossref.org"
_PAGE_SIZE = 1000  # Crossref allows up to 1 000 rows per page
_CURSOR_INIT = "*"


class CrossrefConnector(BaseConnector):
    """
    Connector for the `Crossref <https://www.crossref.org>`_ metadata API.

    Parameters
    ----------
    email : str, optional
        Used for the *polite pool*.  **Strongly recommended** — unauthenticated
        requests without a contact email may be aggressively rate-limited.
    api_key : str, optional
        Crossref Plus API token (``Crossref-Plus-API-Token``).
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        email: Optional[str] = None,
    ) -> None:
        super().__init__(
            SourceName.CROSSREF,
            api_key=api_key,
            email=email,
            base_url=_CROSSREF_API,
        )

    # ------------------------------------------------------------------
    # Header / auth helpers
    # ------------------------------------------------------------------

    def _build_headers(self) -> dict[str, str]:
        """Include polite-pool email and optional Plus token."""
        headers = super()._build_headers()
        if self.email:
            headers["User-Agent"] = (
                f"ScholarAssist/1.0 "
                f"(https://scholarassist.dev; mailto:{self.email})"
            )
        if self.api_key:
            headers["Crossref-Plus-API-Token"] = f"Bearer {self.api_key}"
        return headers

    # ------------------------------------------------------------------
    # Internal cursor-paged download loop
    # ------------------------------------------------------------------

    async def _paginate_works(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        *,
        filters: Optional[str] = None,
        cursor: Optional[str] = None,
        file_prefix: str = "crossref",
    ) -> IngestionManifest:
        """
        Walk through ``/works`` using deep cursor pagination.

        Each page of results is stored as a gzip-compressed JSONL file.
        The last cursor value is persisted in the manifest so that a
        subsequent run can resume.

        Parameters
        ----------
        target_dir : Path
            Output directory for JSONL files.
        manifest : IngestionManifest
            Running manifest.
        filters : str, optional
            Crossref filter expression (e.g.
            ``"from-update-date:2026-06-01"``).
        cursor : str, optional
            Starting cursor.  Defaults to ``*`` (first page).
        file_prefix : str
            Prefix for generated file names.

        Returns
        -------
        IngestionManifest
        """
        params: dict[str, Any] = {
            "rows": _PAGE_SIZE,
            "cursor": cursor or _CURSOR_INIT,
        }
        if filters:
            params["filter"] = filters
        if self.email:
            params["mailto"] = self.email

        page_number = 0
        while True:
            self.logger.info(
                "Fetching %s page %d (cursor=%s)",
                file_prefix,
                page_number,
                params["cursor"][:40],
            )
            try:
                resp = await self._get("/works", params=params)
                payload = resp.json()
            except Exception as exc:
                self.logger.error("Page %d failed: %s", page_number, exc)
                manifest.errors.append(
                    {
                        "page": page_number,
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                manifest.files_failed += 1
                break

            message = payload.get("message", {})
            items: list[dict[str, Any]] = message.get("items", [])
            if not items:
                self.logger.info("No more items — pagination done.")
                break

            # Persist page
            page_file = target_dir / f"{file_prefix}_{page_number:08d}.jsonl.gz"
            sha = hashlib.sha256()
            with gzip.open(page_file, "wt", encoding="utf-8") as gz:
                for item in items:
                    gz.write(json.dumps(item) + "\n")
            raw = page_file.read_bytes()
            sha.update(raw)
            manifest.add_file(page_file.name, len(raw), sha.hexdigest())

            # Advance cursor
            next_cursor = message.get("next-cursor")
            manifest.last_update_cursor = next_cursor
            page_number += 1

            if next_cursor is None:
                self.logger.info("Cursor exhausted.")
                break
            params["cursor"] = next_cursor

        return manifest

    # ------------------------------------------------------------------
    # Bulk download
    # ------------------------------------------------------------------

    async def download_bulk(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
    ) -> IngestionManifest:
        """
        Download the full Crossref catalog via cursor-based deep paging.

        All ~150 M work records are fetched in pages of 1 000 and stored
        as gzip-compressed JSONL files.

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
        manifest.source_version = await self.get_source_version()

        manifest = await self._paginate_works(
            target_dir,
            manifest,
            file_prefix="crossref_bulk",
        )
        manifest.mark_completed()
        return manifest

    # ------------------------------------------------------------------
    # Incremental fetch
    # ------------------------------------------------------------------

    async def fetch_incremental(
        self,
        target_dir: Path,
        manifest: IngestionManifest,
        since: Optional[datetime] = None,
        cursor: Optional[str] = None,
    ) -> IngestionManifest:
        """
        Fetch works updated since *since* via cursor pagination.

        Parameters
        ----------
        target_dir : Path
            Destination directory.
        manifest : IngestionManifest
            Running manifest.
        since : datetime, optional
            Only fetch records modified on or after this date.
        cursor : str, optional
            Resume from this cursor value.

        Returns
        -------
        IngestionManifest
        """
        target_dir.mkdir(parents=True, exist_ok=True)

        filters: Optional[str] = None
        if since is not None:
            filters = f"from-update-date:{since.strftime('%Y-%m-%d')}"

        manifest = await self._paginate_works(
            target_dir,
            manifest,
            filters=filters,
            cursor=cursor,
            file_prefix="crossref_incr",
        )
        manifest.mark_completed()
        return manifest

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    async def get_source_version(self) -> str:
        """
        Return the Crossref API message version string.

        Returns
        -------
        str
            Version such as ``"1.0.0"`` or date-stamped build identifier.
        """
        resp = await self._get("/works", params={"rows": 0})
        data = resp.json()
        return data.get("message-version", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    async def estimate_record_count(self) -> int:
        """
        Return the total number of works in Crossref.

        The ``/works`` endpoint returns ``message.total-results`` with the
        full catalog size.

        Returns
        -------
        int
            Total work count (~150 M as of mid-2026).
        """
        params: dict[str, Any] = {"rows": 0}
        if self.email:
            params["mailto"] = self.email
        resp = await self._get("/works", params=params)
        data = resp.json()
        return int(data.get("message", {}).get("total-results", 150_000_000))
