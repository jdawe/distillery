"""
Newsletter adapter.
Pulls unread newsletter emails from Gmail via the `gog` CLI.
Extracts text content for distillation.
"""
import json
import subprocess
from datetime import datetime, timezone
from typing import Optional

from ..db import db, make_id, upsert_item

# Gmail query to find newsletters
NEWSLETTER_QUERY = "label:newsletters is:unread"


def ingest_newsletter(
    query: str = NEWSLETTER_QUERY,
    account: Optional[str] = None,
    limit: int = 20,
    dry_run: bool = False,
) -> dict:
    """
    Pull unread newsletters from Gmail and insert into DB as 'ingested'.
    Returns counts: {added, skipped, total}.
    """
    threads = _fetch_threads(query, account=account, limit=limit)
    added = 0
    skipped = 0
    now = datetime.now(timezone.utc).isoformat()

    with db() as conn:
        for thread in threads:
            thread_id = thread.get("id", "")
            if not thread_id:
                continue
            subject = thread.get("subject", "") or thread.get("snippet", "")[:80]
            sender = thread.get("from", "") or thread.get("sender", "")
            source_url = f"gmail://thread/{thread_id}"

            item_id = make_id("newsletter", thread_id)
            item = {
                "id": item_id,
                "source_type": "newsletter",
                "source_id": thread_id,
                "source_url": source_url,
                "title": subject,
                "author": sender,
                "state": "ingested",
                "created_at": now,
            }
            if not dry_run:
                inserted = upsert_item(conn, item)
            else:
                inserted = True
            if inserted:
                added += 1
            else:
                skipped += 1

    return {"added": added, "skipped": skipped, "total": added + skipped}


def _fetch_threads(query: str, account: Optional[str] = None, limit: int = 20) -> list:
    cmd = ["gog", "gmail", "search", query, "--json", f"--limit={limit}"]
    if account:
        cmd.extend(["--account", account])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f"gog gmail search failed: {result.stderr.strip()}")
        data = json.loads(result.stdout)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "threads" in data:
            return data["threads"]
        elif isinstance(data, dict) and "messages" in data:
            return data["messages"]
        return []
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse gog output: {e}")


def extract_newsletter_text(thread_id: str, account: Optional[str] = None) -> str:
    """
    Extract full text from a Gmail thread for distillation.
    Returns concatenated message bodies.
    """
    cmd = ["gog", "gmail", "messages", "get", thread_id, "--json"]
    if account:
        cmd.extend(["--account", account])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"gog gmail messages get failed: {result.stderr.strip()}")
    data = json.loads(result.stdout)

    # Extract text parts
    parts = []
    if isinstance(data, list):
        for msg in data:
            parts.append(_extract_body(msg))
    elif isinstance(data, dict):
        parts.append(_extract_body(data))

    return "\n\n---\n\n".join(p for p in parts if p.strip())


def _extract_body(msg: dict) -> str:
    """Recursively extract text body from a Gmail message object."""
    # Direct body field
    if "body" in msg and msg["body"]:
        return msg["body"]
    if "snippet" in msg and msg["snippet"]:
        return msg["snippet"]
    # Nested parts
    for part in msg.get("parts", []):
        body = _extract_body(part)
        if body:
            return body
    return ""
