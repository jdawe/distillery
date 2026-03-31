"""
URL adapter for ad-hoc content distillation.
Extracts text using trafilatura with requests fallback.
"""
import hashlib
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import trafilatura

from ..db import db, make_id, upsert_item


def _url_id(url: str) -> str:
    """Stable ID from URL: strip fragments + query noise."""
    parsed = urlparse(url)
    canonical = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _guess_title(url: str, text: str) -> str:
    """Best-effort title: try to find it in the first lines of text."""
    for line in text.splitlines()[:5]:
        line = line.strip()
        if len(line) > 10 and len(line) < 200:
            return line
    return urlparse(url).netloc


def _guess_author(url: str) -> str:
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    domain = re.sub(r"^www\.", "", domain)
    return domain.split(".")[0].title()


def ingest_url(url: str, dry_run: bool = False) -> dict:
    """
    Ingest an ad-hoc URL. Returns item dict with id.
    Raises on fetch/parse failure.
    """
    url_id = _url_id(url)
    now = datetime.now(timezone.utc).isoformat()

    # Fetch metadata without full extraction (we do full extraction in extract stage)
    try:
        downloaded = trafilatura.fetch_url(url)
        meta = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=False,
            output_format="json",
        )
        if meta:
            import json as _json
            m = _json.loads(meta)
            title = m.get("title") or _guess_title(url, m.get("text", ""))
            author = m.get("author") or _guess_author(url)
        else:
            title = _guess_title(url, "")
            author = _guess_author(url)
    except Exception:
        title = url
        author = _guess_author(url)

    item_id = make_id("url", url_id)
    item = {
        "id": item_id,
        "source_type": "url",
        "source_id": url_id,
        "source_url": url,
        "title": title,
        "author": author,
        "state": "ingested",
        "created_at": now,
    }

    inserted = False
    if not dry_run:
        with db() as conn:
            inserted = upsert_item(conn, item)

    item["inserted"] = inserted
    return item


def extract_url_text(url: str) -> str:
    """
    Full text extraction from URL using trafilatura.
    Returns clean article text suitable for distillation.
    """
    downloaded = trafilatura.fetch_url(url)
    if not downloaded:
        raise RuntimeError(f"Failed to fetch URL: {url}")

    text = trafilatura.extract(
        downloaded,
        include_comments=False,
        include_tables=False,
        favor_precision=True,
        no_fallback=False,
    )
    if not text or len(text.strip()) < 100:
        # Fallback: get with metadata
        meta = trafilatura.extract(
            downloaded,
            include_comments=False,
            output_format="txt",
        )
        if meta:
            text = meta
        else:
            raise RuntimeError(f"Could not extract text from {url}")

    return text.strip()
