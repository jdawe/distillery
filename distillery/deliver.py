"""
Deliver stage: Send distillation to Telegram.

For each distilled item:
1. Send text message: grade emoji + title + author + summary + key insights

(Audio/video render and YouTube publish were dropped 2026-06-04 — JD reads
distillations directly in Telegram, so the render step was producing unused
media files and burning compute on every item. Pipeline is now
extract → distill → deliver, text-only.)
"""
import json
import subprocess
from datetime import datetime, timezone
from typing import Optional

from .db import db, get_pending, update_item, set_error

import os as _os
DISCORD_TARGET = _os.environ.get("DISTILLERY_DISCORD_CHANNEL") or "1553568538604871791"

GRADE_EMOJI = {
    "skim": "",
    "signal": "📡",
    "fire": "🔥",
}


def run_deliver(limit: Optional[int] = None, source_type: Optional[str] = None) -> dict:
    """Process all 'distilled' items through delivery. Returns {ok, failed}."""
    ok = 0
    failed = 0

    with db() as conn:
        items = get_pending(conn, "distilled", limit=limit, source_type=source_type)
        item_list = [dict(row) for row in items]

    for item in item_list:
        item_id = item["id"]
        try:
            msg_id = _deliver_item(item)
            if item.get("source_type") == "newsletter":
                _mark_newsletter_read(item["source_id"])
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


def _format_text_messages(item: dict) -> list:
    """Format a distillery item into one or more Telegram messages.

    Previously this built a single message and silently dropped any insights
    that didn't fit under Telegram's 4096-char limit, appending "N more
    insight(s) not shown" — JD asked (2026-07-30) to always see the full
    thing instead, split across follow-up messages rather than truncated.
    """
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

    DISCORD_LIMIT = 1900  # Discord max is 2000; leave headroom for safety

    title_line = f"{emoji} **{title}**" if emoji else f"**{title}**"
    header = [title_line]
    if author:
        header.append(f"_{author}_")
    header.append("")
    if summary:
        header.append(summary)

    footer = []
    if source_url and not source_url.startswith("gmail://"):
        footer = ["", f"<{source_url}>"]

    current = list(header)
    if insights:
        current.append("")

    messages = []
    for ins in insights:
        insight_text = ins.get("insight", "")
        why_matters = ins.get("why_matters", "")
        take = ins.get("take", "")
        block = []
        if insight_text:
            block.append(f"• {insight_text}")
        if why_matters:
            block.append(f"  ↳ {why_matters}")
        if take:
            block.append(f"  ⚖ {take}")

        candidate = current + block
        if len("\n".join(candidate)) > DISCORD_LIMIT and current != []:
            # This message is full — close it out, start the next with the
            # insight that didn't fit.
            messages.append("\n".join(current))
            current = [f"{title_line} (cont.)", ""] + block
        else:
            current = candidate

    messages.append("\n".join(current + footer))
    return messages


def _deliver_item(item: dict) -> Optional[str]:
    """Send text to Discord, across multiple messages if needed. Returns None
    (message IDs aren't captured — the CLI doesn't return them easily)."""
    for msg in _format_text_messages(item):
        _send_discord_text(msg)
    return None


def _send_discord_text(text: str):
    cmd = [
        "openclaw", "message", "send",
        "--channel", "discord",
        "--target", DISCORD_TARGET,
        "--message", text,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"Discord text send failed: {result.stderr.strip()[:300]}")


def _mark_newsletter_read(thread_id: str):
    account = _os.environ.get("GOG_ACCOUNT") or "jonathan.dawe@gmail.com"
    cmd = [
        "gog", "gmail", "thread", "modify", thread_id,
        "--account", account,
        "--remove=UNREAD",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        print(f"  ⚠ failed to mark thread {thread_id} read: {result.stderr.strip()[:200]}")
