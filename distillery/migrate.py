"""
Migration: Import existing watchlater manifest (149 items) into the new Distillery DB.

Marks all existing items as state='delivered' (they've already been processed).
Preserves existing transcript files by linking them into the new path structure.
"""
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from .db import db, init_db, make_id, upsert_item, update_item

LEGACY_MANIFEST = Path.home() / ".config/watchlater/manifest.json"
LEGACY_TRANSCRIPTS = Path.home() / ".config/watchlater/transcripts"
LEGACY_SUMMARIES = Path.home() / ".config/watchlater/summaries"

BASE = Path.home() / ".openclaw/workspace/data/distillery"
TRANSCRIPTS = BASE / "transcripts" / "youtube"


def migrate_watchlater(dry_run: bool = False, verbose: bool = False) -> dict:
    """
    Import legacy watchlater manifest into distillery DB.
    Returns {added, skipped, errors}.
    """
    if not LEGACY_MANIFEST.exists():
        raise FileNotFoundError(f"Legacy manifest not found: {LEGACY_MANIFEST}")

    data = json.loads(LEGACY_MANIFEST.read_text())
    videos = data.get("videos", data)

    TRANSCRIPTS.mkdir(parents=True, exist_ok=True)
    init_db()

    added = 0
    skipped = 0
    errors = 0
    now = datetime.now(timezone.utc).isoformat()

    for video_id, meta in videos.items():
        try:
            item_id = make_id("youtube", video_id)
            state = meta.get("state", "done")

            # Map legacy states to new state machine
            # Legacy 'failed:playlist' means it was delivered but playlist add failed — treat as delivered
            completed = meta.get("completed_steps", [])
            if state in ("done", "delivered") or "deliver" in completed:
                new_state = "delivered"
            elif state.startswith("failed"):
                new_state = "failed:migrate"
            else:
                new_state = "delivered"  # treat all legacy as done

            # Map timestamps
            delivered_at = meta.get("deliver_at") or meta.get("done_at") or now
            extracted_at = meta.get("extract_at")
            distilled_at = meta.get("summarize_at")
            created_at = meta.get("imported_at") or now

            # Find existing transcript
            legacy_tx_path = LEGACY_TRANSCRIPTS / f"{video_id}.txt"
            new_tx_path = TRANSCRIPTS / f"{item_id}.txt"
            transcript_path_str = None

            if legacy_tx_path.exists() and not new_tx_path.exists():
                if not dry_run:
                    shutil.copy2(legacy_tx_path, new_tx_path)
                transcript_path_str = str(new_tx_path)
                if verbose:
                    print(f"  → copied transcript: {video_id}")
            elif new_tx_path.exists():
                transcript_path_str = str(new_tx_path)
            elif legacy_tx_path.exists():
                transcript_path_str = str(legacy_tx_path)  # point to legacy

            item = {
                "id": item_id,
                "source_type": "youtube",
                "source_id": video_id,
                "source_url": f"https://www.youtube.com/watch?v={video_id}",
                "title": meta.get("title", ""),
                "author": meta.get("channel", ""),
                "state": new_state,
                "created_at": created_at,
            }

            if not dry_run:
                with db() as conn:
                    inserted = upsert_item(conn, item)
                    if inserted:
                        # Set all stage timestamps and paths
                        updates = {
                            "delivered_at": delivered_at,
                        }
                        if extracted_at:
                            updates["extracted_at"] = extracted_at
                        if distilled_at:
                            updates["distilled_at"] = distilled_at
                        if transcript_path_str:
                            updates["transcript_path"] = transcript_path_str
                        update_item(conn, item_id, **updates)
                    added += (1 if inserted else 0)
                    skipped += (0 if inserted else 1)
            else:
                added += 1  # dry run

            if verbose and added % 25 == 0:
                print(f"  {added + skipped}/{len(videos)} processed...")

        except Exception as e:
            errors += 1
            print(f"  ✗ migrate error [{video_id}]: {e}")

    return {"added": added, "skipped": skipped, "errors": errors, "total": len(videos)}
