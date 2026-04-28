"""
Deliver stage: Send distillation to Telegram.

For each rendered item:
1. Send text message: grade emoji + title + author + summary + key insights
2. Send voice note: the rendered audio (asVoice: true)
"""
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .db import db, get_pending, update_item, set_error

import os as _os
TELEGRAM_TARGET = _os.environ.get("DISTILLERY_TELEGRAM_CHAT") or "8040682185"
YT_PUBLISH = _os.environ.get(
    "DISTILLERY_YT_PUBLISH",
    str(Path.home() / ".openclaw/workspace/scripts/yt-publish.py"),
)
READ_LATER_PLAYLIST = "Read Later"

GRADE_EMOJI = {
    "skim": "⚡",
    "signal": "📡",
    "fire": "🔥",
}


def run_deliver(limit: Optional[int] = None, source_type: Optional[str] = None) -> dict:
    """Process all 'rendered' items through delivery. Returns {ok, failed}."""
    ok = 0
    failed = 0

    with db() as conn:
        items = get_pending(conn, "rendered", limit=limit, source_type=source_type)
        item_list = [dict(row) for row in items]

    for item in item_list:
        item_id = item["id"]
        try:
            msg_id = _deliver_item(item)
            if item.get("source_type") == "newsletter":
                _mark_newsletter_read(item["source_id"])
                if item.get("grade") == "fire":
                    _publish_to_read_later(item)
            now = datetime.now(timezone.utc).isoformat()
            with db() as conn:
                update_item(
                    conn,
                    item_id,
                    state="delivered",
                    delivered_at=now,
                    telegram_message_id=str(msg_id) if msg_id else None,
                    error=None,
                    error_at=None,
                )
            ok += 1
            print(f"  ✓ delivered: {item.get('title', item_id)[:50]}")
        except Exception as e:
            with db() as conn:
                set_error(conn, item_id, str(e))
                update_item(conn, item_id, state="failed:deliver")
            failed += 1
            print(f"  ✗ deliver [{item_id}]: {e}")

    return {"ok": ok, "failed": failed}


def _format_text_message(item: dict) -> str:
    grade = item.get("grade", "signal")
    emoji = GRADE_EMOJI.get(grade, "📡")
    title = item.get("title", "Untitled")
    author = item.get("author", "")
    summary = item.get("distill_summary", "")
    insights_raw = item.get("insights_json", "[]")
    source_url = item.get("source_url", "")

    try:
        insights = json.loads(insights_raw) if insights_raw else []
    except Exception:
        insights = []

    parts = [f"{emoji} *{title}*"]
    if author:
        parts.append(f"_{author}_")
    parts.append("")

    if summary:
        parts.append(summary)

    TG_LIMIT = 3900  # Telegram max is 4096; leave headroom for URL + safety

    # Build footer separately so it's always included
    footer = []
    if source_url and not source_url.startswith("gmail://"):
        footer = ["", source_url]

    if insights:
        parts.append("")
        included = 0
        for ins in insights:
            insight_text = ins.get("insight", "")
            why_matters = ins.get("why_matters", "")
            candidate = list(parts)
            if insight_text:
                candidate.append(f"• {insight_text}")
            if why_matters:
                candidate.append(f"  ↳ {why_matters}")
            preview = "\n".join(candidate + footer)
            if len(preview) > TG_LIMIT:
                remaining = len(insights) - included
                if remaining:
                    parts.append(f"  … {remaining} more insight(s) not shown")
                break
            parts = candidate
            included += 1

    parts.extend(footer)
    return "\n".join(parts)


def _deliver_item(item: dict) -> Optional[str]:
    """Send text + voice to Telegram. Returns message ID if available."""
    text_msg = _format_text_message(item)
    audio_path = item.get("render_audio_path")

    # Send text
    _send_telegram_text(text_msg)

    # Send voice note
    if audio_path and Path(audio_path).exists():
        _send_telegram_voice(audio_path)
    else:
        print(f"  ⚠ no audio file for {item['id']}, skipping voice")

    return None  # openclaw CLI doesn't return message IDs easily


def _send_telegram_text(text: str):
    cmd = [
        "openclaw", "message", "send",
        "--channel", "telegram",
        "--target", TELEGRAM_TARGET,
        "--message", text,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"Telegram text send failed: {result.stderr.strip()[:300]}")


def _send_telegram_voice(audio_path: str):
    cmd = [
        "openclaw", "message", "send",
        "--channel", "telegram",
        "--target", TELEGRAM_TARGET,
        "--media", audio_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise RuntimeError(f"Telegram voice send failed: {result.stderr.strip()[:300]}")


def _mark_newsletter_read(thread_id: str):
    cmd = ["gog", "gmail", "thread", "modify", thread_id, "--remove", "UNREAD"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        print(f"  ⚠ failed to mark thread {thread_id} read: {result.stderr.strip()[:200]}")


def _publish_to_read_later(item: dict):
    """🔥 newsletters → upload existing mp4 to YouTube Read Later for on-the-go listening."""
    video_path = item.get("render_path")
    if not video_path or not Path(video_path).exists():
        print(f"  ⚠ no mp4 for fire newsletter {item['id']}, skipping YT publish")
        return

    title = item.get("title", "Distilled")
    author = item.get("author", "")
    summary = item.get("distill_summary", "")
    description = "\n\n".join(p for p in [summary, f"Source: {author}" if author else "",
                                          "Distilled by JanJon."] if p)

    cmd = [
        YT_PUBLISH,
        "--video", video_path,
        "--title", title,
        "--description", description,
        "--playlist", READ_LATER_PLAYLIST,
        "--privacy", "unlisted",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        print(f"  ⚠ YT publish failed for {item['id']}: {result.stderr.strip()[:300]}")
    else:
        for line in result.stdout.strip().splitlines():
            print(f"    {line}")
