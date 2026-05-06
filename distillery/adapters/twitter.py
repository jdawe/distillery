"""
Twitter/X adapter: runs configured Grok search queries nightly and ingests
results as distillable items.

Items enter the pipeline at 'extracted' state — the transcript is written at
ingest time, so the extract stage is skipped. distill → render → deliver then
run as normal.

Query config: ~/.openclaw/workspace/data/distillery/twitter-queries.json
Format: a JSON array of query strings, e.g.
  ["$NVDA options sentiment", "0DTE SPY calls puts", "Fed FOMC market"]
"""
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from ..db import db, make_id, upsert_item

QUERIES_PATH = Path.home() / ".openclaw/workspace/data/distillery/twitter-queries.json"
TRANSCRIPTS = Path.home() / ".openclaw/workspace/data/distillery/transcripts/twitter"
GROK_SCRIPT = Path.home() / ".openclaw/workspace/scripts/grok-search.sh"


def _query_source_id(query: str, date: str) -> str:
    raw = f"{query}:{date}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def ingest_twitter(
    queries: list[str] | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Run X search queries via grok-search.sh and ingest results.
    Each query produces one item per day. Items start as 'extracted'.
    """
    if queries is None:
        if QUERIES_PATH.exists():
            queries = json.loads(QUERIES_PATH.read_text(encoding="utf-8"))
        else:
            queries = []

    if not queries:
        return {"added": 0, "skipped": 0, "total": 0}

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc).isoformat()
    added = 0
    skipped = 0

    for query in queries:
        source_id = _query_source_id(query, today)
        item_id = make_id("twitter", source_id)

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

        text = _run_grok(query)
        if not text:
            continue

        out_path = TRANSCRIPTS / f"{item_id}.txt"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            f"X/Twitter search: {query}\nDate: {today}\n\n{text}",
            encoding="utf-8",
        )

        item = {
            "id": item_id,
            "source_type": "twitter",
            "source_id": source_id,
            "title": f"X: {query} ({today})",
            "author": "X/Twitter via Grok",
            "state": "extracted",
            "transcript_path": str(out_path),
            "extracted_at": now,
            "created_at": now,
        }

        with db() as conn:
            inserted = upsert_item(conn, item)
            if inserted:
                added += 1
            else:
                skipped += 1

    return {"added": added, "skipped": skipped, "total": added + skipped}


def _run_grok(query: str) -> str | None:
    """Run grok-search.sh and return cleaned text, or None on failure."""
    try:
        result = subprocess.run(
            ["bash", str(GROK_SCRIPT), query],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            print(f"  ✗ grok-search [{query!r}]: {result.stderr.strip()[:200]}")
            return None

        lines = result.stdout.strip().splitlines()
        # Strip trailing cost line added by grok-search.sh
        if lines and lines[-1].startswith("--- Cost:"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

        if len(text) < 50:
            print(f"  ✗ grok-search [{query!r}]: response too short")
            return None

        return text

    except subprocess.TimeoutExpired:
        print(f"  ✗ grok-search [{query!r}]: timeout after 60s")
        return None
    except Exception as e:
        print(f"  ✗ grok-search [{query!r}]: {e}")
        return None
