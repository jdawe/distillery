"""
YouTube channel subscription monitor.

Fetches RSS feeds for configured channels and ingests new videos as distillable
items. Items enter at 'ingested' state — the extract stage handles transcript
fetching via youtube-transcript-api.

Channel config: ~/.openclaw/workspace/data/distillery/youtube-channels.json
Format:
  [{"channel_id": "UCxxxxxxx", "name": "Channel Name"}, ...]

RSS feed URL: https://www.youtube.com/feeds/videos.xml?channel_id=<id>
No API key required.
"""
from __future__ import annotations

import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json

from ..db import db, make_id, upsert_item

CHANNELS_PATH = Path.home() / ".openclaw/workspace/data/distillery/youtube-channels.json"
RSS_BASE = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
YT_NS = "http://www.youtube.com/xml/schemas/2015"
MEDIA_NS = "http://search.yahoo.com/mrss/"


def _yt_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def _fetch_channel_videos(channel_id: str, name: str, lookback_days: int) -> list[dict]:
    """Fetch recent videos from a channel's RSS feed. Returns list of video dicts."""
    url = RSS_BASE.format(channel_id=channel_id)
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            xml_bytes = resp.read()
    except Exception as e:
        print(f"  ✗ channel {name} [{channel_id}]: fetch failed — {e}")
        return []

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        print(f"  ✗ channel {name} [{channel_id}]: XML parse error — {e}")
        return []

    atom_ns = "http://www.w3.org/2005/Atom"
    videos = []
    for entry in root.findall(f"{{{atom_ns}}}entry"):
        video_id_el = entry.find(f"{{{YT_NS}}}videoId")
        if video_id_el is None:
            continue
        video_id = video_id_el.text.strip()

        title_el = entry.find(f"{{{atom_ns}}}title")
        title = title_el.text.strip() if title_el is not None and title_el.text else ""

        published_el = entry.find(f"{{{atom_ns}}}published")
        if published_el is not None and published_el.text:
            try:
                pub_dt = datetime.fromisoformat(published_el.text.replace("Z", "+00:00"))
            except ValueError:
                pub_dt = datetime.now(timezone.utc)
        else:
            pub_dt = datetime.now(timezone.utc)

        if pub_dt < cutoff:
            continue

        videos.append({
            "video_id": video_id,
            "title": title,
            "channel": name,
            "published_at": pub_dt.isoformat(),
        })

    return videos


def ingest_youtube_channels(
    channels: list[dict] | None = None,
    lookback_days: int = 3,
    dry_run: bool = False,
) -> dict:
    """
    Fetch recent videos from configured YouTube channels via RSS and ingest them.
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

        videos = _fetch_channel_videos(channel_id, name, lookback_days)

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
