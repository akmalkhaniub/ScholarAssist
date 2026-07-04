#!/usr/bin/env python3
"""
ScholarAssist — Download Sample Records

CLI script that fetches ~1 000 sample records from the OpenAlex and Crossref
REST APIs and writes them to ``tests/fixtures/`` as JSON files.  Useful for
local integration testing without needing to run a full bulk ingestion.

Usage::

    python -m scripts.download_samples             # defaults
    python -m scripts.download_samples --count 500  # fewer records
    python -m scripts.download_samples --output-dir data/samples

Dependencies (already in pyproject.toml)::

    click, httpx, tqdm, orjson
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any

import click
import httpx
from tqdm import tqdm

# ── Defaults ──────────────────────────────────────────────────────────────

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
USER_AGENT = (
    "ScholarAssist/1.0 "
    "(https://scholarassist.dev; mailto:contact@scholarassist.dev)"
)
OPENALEX_API = "https://api.openalex.org"
CROSSREF_API = "https://api.crossref.org"


# ── OpenAlex downloader ──────────────────────────────────────────────────

def download_openalex(count: int, output_dir: Path) -> Path:
    """Fetch *count* works from the OpenAlex API and save as JSON.

    Uses cursor-based pagination to walk through highly-cited works.
    """
    records: list[dict[str, Any]] = []
    per_page = min(count, 200)  # OpenAlex max per_page is 200
    cursor = "*"

    click.echo(f"\n📚 Downloading {count} OpenAlex works …")

    with httpx.Client(
        base_url=OPENALEX_API,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        pbar = tqdm(total=count, unit="rec", desc="OpenAlex")

        while len(records) < count:
            params: dict[str, Any] = {
                "filter": "is_paratext:false,type:journal-article",
                "sort": "cited_by_count:desc",
                "per_page": per_page,
                "cursor": cursor,
            }

            try:
                resp = client.get("/works", params=params)
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                click.secho(f"⚠ OpenAlex HTTP {exc.response.status_code}", fg="yellow")
                break

            data = resp.json()
            results = data.get("results", [])
            if not results:
                break

            records.extend(results)
            pbar.update(len(results))

            meta = data.get("meta", {})
            cursor = meta.get("next_cursor")
            if cursor is None:
                break

            # Be polite — OpenAlex docs ask for ≤10 req/s for the polite pool
            time.sleep(0.12)

        pbar.close()

    records = records[:count]
    out_path = output_dir / "openalex_works_sample.json"
    out_path.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    click.echo(f"✅  Saved {len(records)} OpenAlex records → {out_path}")
    return out_path


# ── Crossref downloader ──────────────────────────────────────────────────

def download_crossref(count: int, output_dir: Path) -> Path:
    """Fetch *count* works from the Crossref API and save as JSON.

    Uses offset-based pagination with a filter for journal articles.
    """
    records: list[dict[str, Any]] = []
    per_page = min(count, 100)  # Crossref default max rows is 1 000 but 100 is polite
    offset = 0

    click.echo(f"\n📖 Downloading {count} Crossref works …")

    with httpx.Client(
        base_url=CROSSREF_API,
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
        follow_redirects=True,
    ) as client:
        pbar = tqdm(total=count, unit="rec", desc="Crossref")

        while len(records) < count:
            params: dict[str, Any] = {
                "filter": "type:journal-article,has-abstract:true",
                "sort": "is-referenced-by-count",
                "order": "desc",
                "rows": per_page,
                "offset": offset,
            }

            try:
                resp = client.get("/works", params=params)
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                click.secho(f"⚠ Crossref HTTP {exc.response.status_code}", fg="yellow")
                break

            data = resp.json()
            items = data.get("message", {}).get("items", [])
            if not items:
                break

            records.extend(items)
            pbar.update(len(items))
            offset += len(items)

            # Crossref asks for ≤50 req/s in their etiquette guide
            time.sleep(0.15)

        pbar.close()

    records = records[:count]
    out_path = output_dir / "crossref_works_sample.json"
    out_path.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    click.echo(f"✅  Saved {len(records)} Crossref records → {out_path}")
    return out_path


# ── CLI ───────────────────────────────────────────────────────────────────

@click.command()
@click.option(
    "--count",
    "-n",
    default=1000,
    show_default=True,
    type=int,
    help="Number of sample records to download per source.",
)
@click.option(
    "--output-dir",
    "-o",
    default=str(DEFAULT_OUTPUT_DIR),
    show_default=True,
    type=click.Path(file_okay=False),
    help="Directory to write output JSON files.",
)
@click.option(
    "--sources",
    "-s",
    default="openalex,crossref",
    show_default=True,
    help="Comma-separated list of sources to download (openalex, crossref).",
)
def main(count: int, output_dir: str, sources: str) -> None:
    """Download sample academic records for local testing."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    source_list = [s.strip().lower() for s in sources.split(",")]
    created: list[Path] = []

    if "openalex" in source_list:
        created.append(download_openalex(count, out))

    if "crossref" in source_list:
        created.append(download_crossref(count, out))

    click.echo(f"\n🎉 Done — {len(created)} file(s) written to {out}")


if __name__ == "__main__":
    main()
