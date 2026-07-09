"""
Newsletter adapter.
Pulls unread newsletter emails from Gmail via the `gog` CLI.
Extracts text content for distillation.
"""
import base64
import json
import subprocess
from datetime import datetime, timezone
from typing import Optional

from ..db import db, make_id, upsert_item

NEWSLETTER_LABEL_PREFIX = "Newsletters"


def ingest_newsletter(
    query: str = "",
    account: Optional[str] = None,
    limit: int = 20,
    dry_run: bool = False,
) -> dict:
    """
    Pull unread newsletters from Gmail and insert into DB as 'ingested'.
    Returns counts: {added, skipped, total}.
    """
    threads = _fetch_newsletter_threads(account=account, limit=limit)
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


def _fetch_newsletter_threads(account: Optional[str] = None, limit: int = 20) -> list:
    """Enumerate all Newsletters/* sub-labels and collect unread threads from each."""
    sub_labels = _get_newsletter_sub_labels(account)
    seen: set = set()
    all_threads: list = []
    for label_name in sub_labels:
        # Gmail search needs spaces replaced with hyphens: "Newsletters/The Diff" → "label:Newsletters/The-Diff"
        label_slug = label_name.replace(" ", "-")
        query = f"label:{label_slug} is:unread"
        threads = _run_search(query, account=account, limit=limit)
        for t in threads:
            tid = t.get("id", "")
            if tid and tid not in seen:
                seen.add(tid)
                all_threads.append(t)
    return all_threads


def _get_newsletter_sub_labels(account: Optional[str] = None) -> list:
    cmd = ["gog", "gmail", "labels", "list", "--json"]
    if account:
        cmd.extend(["--account", account])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"gog gmail labels list failed: {result.stderr.strip()}")
    data = json.loads(result.stdout)
    labels = data.get("labels", []) if isinstance(data, dict) else data
    prefix = NEWSLETTER_LABEL_PREFIX + "/"
    return [l["name"] for l in labels if l.get("name", "").startswith(prefix)]


def _run_search(query: str, account: Optional[str] = None, limit: int = 20) -> list:
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
            return data["threads"] or []
        elif isinstance(data, dict) and "messages" in data:
            return data["messages"]
        return []
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse gog output: {e}")


def extract_newsletter_text(thread_id: str, account: Optional[str] = None) -> str:
    """
    Extract full text from a Gmail thread for distillation.
    Uses gog gmail thread get and decodes base64 text/plain parts.
    """
    cmd = ["gog", "gmail", "thread", "get", thread_id, "--json"]
    if account:
        cmd.extend(["--account", account])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"gog gmail thread get failed: {result.stderr.strip()}")
    data = json.loads(result.stdout)
    messages = data.get("thread", {}).get("messages", [])

    parts = []
    for msg in messages:
        text = _extract_payload_text(msg.get("payload", {}))
        if text:
            parts.append(text)

    return "\n\n---\n\n".join(parts)


def _extract_payload_text(payload: dict) -> str:
    """Walk a Gmail message payload tree. Prefer text/plain; fall back to text/html
    (tag-stripped) for HTML-only newsletters like The Market Ear."""
    plain = _walk_for_mime(payload, "text/plain")
    if plain and len(plain.strip()) >= 50:
        return plain.strip()
    html = _walk_for_mime(payload, "text/html")
    if html:
        return _html_to_text(html).strip()
    return plain.strip() if plain else ""


def _walk_for_mime(payload: dict, target_mime: str) -> str:
    mime = payload.get("mimeType", "")
    body = payload.get("body", {})
    if mime == target_mime and isinstance(body, dict) and body.get("data"):
        return base64.urlsafe_b64decode(body["data"] + "==").decode("utf-8", errors="replace")
    for part in payload.get("parts", []):
        text = _walk_for_mime(part, target_mime)
        if text:
            return text
    return ""


def _html_to_text(html: str) -> str:
    """Strip HTML tags and collapse whitespace. Uses stdlib html.parser to avoid
    a BeautifulSoup dep. Preserves basic paragraph breaks."""
    from html.parser import HTMLParser
    import re

    class _Stripper(HTMLParser):
        BLOCK = {"p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6"}
        SKIP = {"style", "script", "head", "title"}

        def __init__(self):
            super().__init__()
            self.parts: list = []
            self.skip_depth = 0

        def handle_starttag(self, tag, attrs):
            if tag in self.SKIP:
                self.skip_depth += 1
            elif tag in self.BLOCK:
                self.parts.append("\n")

        def handle_endtag(self, tag):
            if tag in self.SKIP:
                self.skip_depth = max(0, self.skip_depth - 1)
            elif tag in self.BLOCK:
                self.parts.append("\n")

        def handle_data(self, data):
            if self.skip_depth == 0:
                self.parts.append(data)

    s = _Stripper()
    s.feed(html)
    text = "".join(s.parts)
    # Decode HTML entities that HTMLParser leaves alone in some paths
    import html as _html_mod
    text = _html_mod.unescape(text)
    # Collapse whitespace: preserve paragraph boundaries, drop runs
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text
