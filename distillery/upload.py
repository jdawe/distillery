"""
Upload stage: Upload 🔥 items to YouTube "Distilled" playlist.

Only runs on items with grade='fire' that are in state='delivered'.
Uses YouTube OAuth credentials from $DISTILLERY_YT_TOKEN and $DISTILLERY_YT_CLIENT_SECRET.
"""
import json
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .db import db, get_pending, update_item, set_error

import os as _os
OAUTH_TOKEN = Path(_os.environ.get("DISTILLERY_YT_TOKEN", str(Path.home() / ".config/distillery/youtube-token.json")))
CLIENT_SECRET = Path(_os.environ.get("DISTILLERY_YT_CLIENT_SECRET", str(Path.home() / ".config/distillery/client-secret.json")))
DISTILLED_PLAYLIST_ID = _os.environ.get("DISTILLERY_YT_PLAYLIST", "")

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]


def run_upload(limit: Optional[int] = None) -> dict:
    """Upload fire-grade items that have been delivered. Returns {ok, failed, skipped}."""
    ok = 0
    failed = 0
    skipped = 0

    with db() as conn:
        # Get delivered items with grade=fire that haven't been uploaded
        rows = conn.execute(
            "SELECT * FROM items WHERE state = 'delivered' AND grade = 'fire' "
            "AND youtube_id IS NULL ORDER BY created_at ASC"
            + (f" LIMIT {limit}" if limit else "")
        ).fetchall()
        item_list = [dict(row) for row in rows]

    if not item_list:
        return {"ok": 0, "failed": 0, "skipped": 0}

    yt = _get_youtube_service()

    for item in item_list:
        item_id = item["id"]
        video_path = item.get("render_path")

        if not video_path or not Path(video_path).exists():
            skipped += 1
            print(f"  ⚠ no video file for fire item {item_id}, skipping upload")
            continue

        try:
            yt_id = _upload_video(
                yt,
                video_path=video_path,
                title=item.get("title", "Distilled"),
                author=item.get("author", ""),
                summary=item.get("distill_summary", ""),
                source_url=item.get("source_url", ""),
            )
            _add_to_playlist(yt, yt_id, DISTILLED_PLAYLIST_ID)

            now = datetime.now(timezone.utc).isoformat()
            with db() as conn:
                update_item(
                    conn,
                    item_id,
                    state="uploaded",
                    youtube_id=yt_id,
                    uploaded_at=now,
                    error=None,
                    error_at=None,
                )
            ok += 1
            print(f"  🔥 uploaded: {item.get('title', item_id)[:50]} → {yt_id}")

        except Exception as e:
            with db() as conn:
                set_error(conn, item_id, str(e))
                update_item(conn, item_id, state="failed:upload")
            failed += 1
            print(f"  ✗ upload [{item_id}]: {e}")

    return {"ok": ok, "failed": failed, "skipped": skipped}


def _get_youtube_service():
    """Build authenticated YouTube API service from stored OAuth token."""
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google.auth.transport.requests import Request

    if not OAUTH_TOKEN.exists():
        raise FileNotFoundError(f"YouTube OAuth token not found at {OAUTH_TOKEN}")

    token_data = json.loads(OAUTH_TOKEN.read_text())

    creds = Credentials(
        token=token_data.get("access_token") or token_data.get("token"),
        refresh_token=token_data.get("refresh_token"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=token_data.get("client_id"),
        client_secret=token_data.get("client_secret"),
        scopes=token_data.get("scopes", SCOPES),
    )

    # If client_id/secret not in token file, try loading from client_secret
    if not creds.client_id and CLIENT_SECRET.exists():
        cs_data = json.loads(CLIENT_SECRET.read_text())
        installed = cs_data.get("installed") or cs_data.get("web") or {}
        creds = Credentials(
            token=token_data.get("access_token") or token_data.get("token"),
            refresh_token=token_data.get("refresh_token"),
            token_uri=installed.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=installed.get("client_id"),
            client_secret=installed.get("client_secret"),
            scopes=token_data.get("scopes", SCOPES),
        )

    # Refresh if expired
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return build("youtube", "v3", credentials=creds)


def _upload_video(yt, video_path: str, title: str, author: str, summary: str, source_url: str) -> str:
    """Upload video to YouTube. Returns video ID."""
    from googleapiclient.http import MediaFileUpload

    description_parts = []
    if summary:
        description_parts.append(summary)
    if author:
        description_parts.append(f"\nSource: {author}")
    if source_url and not source_url.startswith("gmail://"):
        description_parts.append(source_url)
    description_parts.append("\n\nDistilled by JanJon Distillery 🔥")

    description = "\n".join(description_parts)

    body = {
        "snippet": {
            "title": f"🔥 {title}"[:100],
            "description": description[:5000],
            "tags": ["distilled", "janjon"],
            "categoryId": "22",  # People & Blogs
        },
        "status": {
            "privacyStatus": "private",
        },
    }

    media = MediaFileUpload(
        video_path,
        mimetype="video/mp4",
        resumable=True,
    )

    request = yt.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
    )

    response = None
    while response is None:
        status, response = request.next_chunk()

    return response["id"]


def _add_to_playlist(yt, video_id: str, playlist_id: str):
    """Add video to playlist."""
    yt.playlistItems().insert(
        part="snippet",
        body={
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {
                    "kind": "youtube#video",
                    "videoId": video_id,
                },
            }
        },
    ).execute()
