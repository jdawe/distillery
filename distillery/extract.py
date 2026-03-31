"""
Extract stage: pull raw text from source and write to
data/distillery/transcripts/{source_type}/{id}.txt

For YouTube: shells out to `summarize` CLI (which handles yt-dlp transcript extraction)
For newsletter: uses gog CLI to fetch message body
For URL: uses trafilatura
"""
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .db import db, get_pending, update_item, set_error

BASE = Path.home() / ".openclaw/workspace/data/distillery"
TRANSCRIPTS = BASE / "transcripts"


def transcript_path(source_type: str, item_id: str) -> Path:
    return TRANSCRIPTS / source_type / f"{item_id}.txt"


def run_extract(limit: Optional[int] = None, source_type: Optional[str] = None) -> dict:
    """Process all 'ingested' items through extraction. Returns {ok, failed}."""
    ok = 0
    failed = 0

    with db() as conn:
        items = get_pending(conn, "ingested", limit=limit, source_type=source_type)
        item_list = [dict(row) for row in items]

    for item in item_list:
        item_id = item["id"]
        src_type = item["source_type"]
        src_id = item["source_id"]
        src_url = item.get("source_url", "")

        try:
            text = _extract_text(src_type, src_id, src_url)
            if not text or len(text.strip()) < 50:
                raise RuntimeError("Extracted text too short or empty")

            out_path = transcript_path(src_type, item_id)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(text, encoding="utf-8")

            now = datetime.now(timezone.utc).isoformat()
            with db() as conn:
                update_item(
                    conn,
                    item_id,
                    state="extracted",
                    transcript_path=str(out_path),
                    extracted_at=now,
                    error=None,
                    error_at=None,
                )
            ok += 1

        except Exception as e:
            with db() as conn:
                set_error(conn, item_id, str(e))
                update_item(conn, item_id, state="failed:extract")
            failed += 1
            print(f"  ✗ extract [{src_type}/{src_id}]: {e}")

    return {"ok": ok, "failed": failed}


def _extract_text(source_type: str, source_id: str, source_url: str) -> str:
    if source_type == "youtube":
        return _extract_youtube(source_id)
    elif source_type == "newsletter":
        return _extract_newsletter(source_id)
    elif source_type == "url":
        return _extract_url(source_url)
    else:
        raise ValueError(f"Unknown source type: {source_type}")


def _extract_youtube(video_id: str) -> str:
    """
    Try cached transcript first, then summarize CLI.
    The summarize CLI can extract YouTube transcripts via yt-dlp.
    """
    # Check for existing transcript in old watchlater cache
    legacy_path = Path.home() / f".config/watchlater/transcripts/{video_id}.txt"
    if legacy_path.exists():
        text = legacy_path.read_text(encoding="utf-8").strip()
        if len(text) > 100:
            return text

    # Fall back to summarize CLI (extracts via yt-dlp)
    url = f"https://www.youtube.com/watch?v={video_id}"
    result = subprocess.run(
        ["summarize", "--extract", url],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(f"summarize --extract failed: {result.stderr.strip()[:300]}")
    text = result.stdout.strip()
    if not text:
        raise RuntimeError("summarize --extract returned empty output")
    return text


def _extract_newsletter(thread_id: str) -> str:
    """Extract text from Gmail thread using gog CLI."""
    from .adapters.newsletter import extract_newsletter_text
    return extract_newsletter_text(thread_id)


def _extract_url(url: str) -> str:
    """Extract text from URL using trafilatura."""
    from .adapters.url import extract_url_text
    return extract_url_text(url)
