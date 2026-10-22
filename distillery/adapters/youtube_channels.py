"""
YouTube channel subscription monitor.

Fetches the latest videos from configured channels via yt-dlp (no API key
required) and ingests new videos as distillable items. Items enter at
'ingested' state — the extract stage handles transcript fetching via
youtube-transcript-api.

Why yt-dlp instead of RSS: YouTube's RSS feed endpoint started returning
HTTP 500 for certain channels (e.g. UC0C-17n9iuUQPylguM1d-lQ as of 2026-06),
silently breaking the pipeline. yt-dlp is community-maintained against
YouTube's frontend changes and works for every channel.

Channel config: $DISTILLERY_YOUTUBE_CHANNELS (default: $DISTILLERY_HOME/youtube-channels.json)
Format:
  [{"channel_id": "UCxxxxxxx", "name": "Channel Name"}, ...]
"""
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from ..db import db, make_id, upsert_item

from ..config import CHANNELS_PATH
CHANNEL_URL = "https://www.youtube.com/channel/{channel_id}/videos"
# How many of the most recent videos to consider per channel each run.
# DB dedup gates re-ingest; we only need a window large enough to cover
# anything posted since the last run.
DEFAULT_FETCH_LIMIT = 20


def _yt_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def _fetch_channel_videos(channel_id: str, name: str, fetch_limit: int) -> list[dict]:
    """Fetch the latest N videos from a channel via yt-dlp. Returns list of dicts."""
    url = CHANNEL_URL.format(channel_id=channel_id)
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--dump-json",
        "--no-warnings",
        "--playlist-end", str(fetch_limit),
        url,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        print(f"  ✗ channel {name} [{channel_id}]: yt-dlp timed out")
        return []
    except FileNotFoundError:
        print(f"  ✗ yt-dlp not installed (brew install yt-dlp)")
        return []

    if proc.returncode != 0:
        print(f"  ✗ channel {name} [{channel_id}]: yt-dlp exit {proc.returncode} — {proc.stderr.strip()[:200]}")
        return []

    videos = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        video_id = entry.get("id")
        if not video_id:
            continue
        videos.append({
            "video_id": video_id,
            "title": entry.get("title") or "",
            "channel": name,
        })
    return videos


def ingest_youtube_channels(
    channels: list[dict] | None = None,
    lookback_days: int = 3,  # retained for CLI compatibility; ignored — DB dedup gates re-ingest
    dry_run: bool = False,
    fetch_limit: int = DEFAULT_FETCH_LIMIT,
) -> dict:
    """
    Fetch recent videos from configured YouTube channels and ingest unseen ones.
    Returns counts: {added, skipped, total}.
    """
    if channels is None:
        if CHANNELS_PATH.exists():
            data = json.loads(CHANNELS_PATH.read_text(encoding="utf-8"))
            channels = data if isinstance(data, list) else data.get("channels", [])
        else:
            channels = []

    if not channels:
        return {"added": 0, "skipped": 0, "total": 0}

    now = datetime.now(timezone.utc).isoformat()
    added = 0
    skipped = 0

    for ch in channels:
        channel_id = ch.get("channel_id", "")
        name = ch.get("name", channel_id)
        if not channel_id:
            continue

        videos = _fetch_channel_videos(channel_id, name, fetch_limit)

        for v in videos:
            video_id = v["video_id"]
            item_id = make_id("youtube", video_id)

            with db() as conn:
                existing = conn.execute(
                    "SELECT id FROM items WHERE id = ?", [item_id]
                ).fetchone()
                if existing:
                    skipped += 1
                    continue

                if dry_run:
                    added += 1
                    continue

                item = {
                    "id": item_id,
                    "source_type": "youtube",
                    "source_id": video_id,
                    "source_url": _yt_url(video_id),
                    "title": v["title"],
                    "author": name,
                    "state": "ingested",
                    "created_at": now,
                }
                inserted = upsert_item(conn, item)
                if inserted:
                    added += 1
                else:
                    skipped += 1

    return {"added": added, "skipped": skipped, "total": added + skipped}
