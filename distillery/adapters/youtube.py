"""
YouTube Watch Later adapter.
Ingests from a watchlater-export JSON file produced by watchlater-scrape.sh.
Supports the ~/.config/watchlater/manifest.json format used by the old pipeline.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ..db import db, make_id, upsert_item

DEFAULT_EXPORT = Path.home() / ".config/watchlater/watchlater-export.json"
LEGACY_MANIFEST = Path.home() / ".config/watchlater/manifest.json"


def _yt_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


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
