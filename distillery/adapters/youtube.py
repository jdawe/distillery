"""
YouTube Watch Later adapter.
Ingests from a watchlater-export JSON file produced by watchlater-scrape.sh.
Supports the ~/.config/watchlater/manifest.json format used by the old pipeline.
"""
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

from ..db import db, make_id, upsert_item

DEFAULT_EXPORT = Path.home() / ".config/watchlater/watchlater-export.json"
LEGACY_MANIFEST = Path.home() / ".config/watchlater/manifest.json"

_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")


def _yt_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def parse_video_id(url: str) -> Optional[str]:
    """
    Extract an 11-char YouTube video ID from a watch/youtu.be/shorts/embed URL.
    Returns None if the URL isn't a recognizable YouTube video link.
    """
    parsed = urlparse(url)
    host = parsed.netloc.lower().removeprefix("www.").removeprefix("m.")
    if host not in ("youtube.com", "youtu.be", "music.youtube.com"):
        return None

    if host == "youtu.be":
        video_id = parsed.path.lstrip("/").split("/")[0]
    elif parsed.path.startswith("/shorts/") or parsed.path.startswith("/embed/"):
        video_id = parsed.path.split("/")[2] if len(parsed.path.split("/")) > 2 else ""
    else:
        video_id = parse_qs(parsed.query).get("v", [""])[0]

    return video_id if _VIDEO_ID_RE.match(video_id) else None


def ingest_youtube_url(url: str, dry_run: bool = False) -> dict:
    """
    Ingest a single ad-hoc YouTube video URL (not a Watch Later export).
    Fetches title/uploader via yt-dlp. Raises ValueError if the URL isn't
    a recognizable YouTube video link, RuntimeError if yt-dlp lookup fails.
    """
    video_id = parse_video_id(url)
    if not video_id:
        raise ValueError(f"Not a recognizable YouTube video URL: {url}")

    canonical_url = _yt_url(video_id)
    result = subprocess.run(
        ["yt-dlp", "--print", "%(title)s|||%(uploader)s", "--skip-download", canonical_url],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp metadata lookup failed: {result.stderr.strip()[:300]}")

    line = next((l for l in result.stdout.strip().splitlines() if "|||" in l), "")
    title, _, author = line.partition("|||")
    title = title.strip() or video_id
    author = author.strip()

    now = datetime.now(timezone.utc).isoformat()
    item = {
        "id": make_id("youtube", video_id),
        "source_type": "youtube",
        "source_id": video_id,
        "source_url": canonical_url,
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


def ingest_youtube(
    export_path: Optional[Path] = None,
    legacy_manifest: Optional[Path] = None,
    dry_run: bool = False,
) -> dict:
    """
    Import YouTube Watch Later items into the DB.
    Returns counts: {added, skipped, total}.
    """
    # Prefer explicit export, fall back to legacy manifest
    if export_path and export_path.exists():
        videos = _load_export(export_path)
    elif legacy_manifest and legacy_manifest.exists():
        videos = _load_legacy(legacy_manifest)
    elif DEFAULT_EXPORT.exists():
        videos = _load_export(DEFAULT_EXPORT)
    elif LEGACY_MANIFEST.exists():
        videos = _load_legacy(LEGACY_MANIFEST)
    else:
        raise FileNotFoundError(
            f"No YouTube export found. Expected: {DEFAULT_EXPORT} or {LEGACY_MANIFEST}"
        )

    added = 0
    skipped = 0
    now = datetime.now(timezone.utc).isoformat()

    with db() as conn:
        for video_id, meta in videos.items():
            item_id = make_id("youtube", video_id)
            item = {
                "id": item_id,
                "source_type": "youtube",
                "source_id": video_id,
                "source_url": _yt_url(video_id),
                "title": meta.get("title", ""),
                "author": meta.get("channel", ""),
                "state": "ingested",
                "created_at": now,
            }
            if not dry_run:
                inserted = upsert_item(conn, item)
            else:
                inserted = True  # dry run: assume new
            if inserted:
                added += 1
            else:
                skipped += 1

    return {"added": added, "skipped": skipped, "total": added + skipped}


def _load_export(path: Path) -> dict:
    """Load watchlater-export.json format: {video_id: {title, channel, ...}}"""
    data = json.loads(path.read_text())
    if isinstance(data, list):
        # list of {id, title, channel}
        return {v["id"]: v for v in data}
    elif isinstance(data, dict) and "videos" in data:
        return data["videos"]
    elif isinstance(data, dict):
        return data
    else:
        raise ValueError(f"Unexpected export format in {path}")


def _load_legacy(path: Path) -> dict:
    """Load legacy manifest.json: {updated, videos: {video_id: {...}}}"""
    data = json.loads(path.read_text())
    return data.get("videos", data)
