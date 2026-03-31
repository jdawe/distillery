"""
Render stage: Aria TTS + title card video.

For each distilled item:
1. Build TTS script from grade + title + author + summary + insights
2. Run edge-tts → audio/{id}.mp3
3. Run ffmpeg-full: lavfi color bg + audio + drawtext → video/{id}.mp4
4. Write paths to DB
"""
import asyncio
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .db import db, get_pending, update_item, set_error

BASE = Path.home() / ".openclaw/workspace/data/distillery"
AUDIO_DIR = BASE / "audio"
VIDEO_DIR = BASE / "video"

import os as _os
FFMPEG = _os.environ.get("DISTILLERY_FFMPEG", "/opt/homebrew/opt/ffmpeg-full/bin/ffmpeg")

GRADE_EMOJI = {
    "skim": "⚡",
    "signal": "📡",
    "fire": "🔥",
}


def _safe_filename(title: str, item_id: str, ext: str) -> str:
    """Build a filesystem-safe filename from title, falling back to item_id."""
    if not title or title.strip() == "Untitled":
        return f"{item_id}.{ext}"
    # Normalize: lowercase, replace non-alnum with hyphens, collapse, trim
    import re as _re
    name = _re.sub(r'[^a-z0-9]+', '-', title.lower().strip())
    name = name.strip('-')[:80]
    # Append short hash for uniqueness
    return f"{name}-{item_id[:6]}.{ext}"


def audio_path(item_id: str, title: str = "") -> Path:
    return AUDIO_DIR / _safe_filename(title, item_id, "mp3")


def video_path(item_id: str, title: str = "") -> Path:
    return VIDEO_DIR / _safe_filename(title, item_id, "mp4")


def _build_tts_script(item: dict) -> str:
    """Build spoken text for TTS from distillation data."""
    import json as _json

    grade = item.get("grade", "signal")
    title = item.get("title", "Untitled")
    author = item.get("author", "")
    summary = item.get("distill_summary", "")
    insights_raw = item.get("insights_json", "[]")

    try:
        insights = _json.loads(insights_raw) if insights_raw else []
    except Exception:
        insights = []

    lines = []

    # Grade label
    grade_label = {"skim": "Skim", "signal": "Signal", "fire": "Fire — deep cut"}
    lines.append(f"{grade_label.get(grade, grade)}.")
    lines.append("")

    # Title and author
    lines.append(f"{title}.")
    if author:
        lines.append(f"By {author}.")
    lines.append("")

    # Summary — only if it adds info beyond the title/author
    if summary:
        # Skip if summary is just "Author — Title" or "Title" restated
        summary_lower = summary.lower().strip()
        title_lower = title.lower().strip()
        author_lower = (author or "").lower().strip()
        is_redundant = (
            summary_lower == title_lower
            or summary_lower == f"{author_lower} — {title_lower}"
            or summary_lower == f"{author_lower} - {title_lower}"
            or summary_lower.replace("—", "-") == f"{author_lower} - {title_lower}".replace("—", "-")
        )
        if not is_redundant:
            lines.append(summary)
            lines.append("")

    # Insights
    if insights:
        lines.append(f"{'Key insight' if len(insights) == 1 else f'{len(insights)} insights'}:")
        lines.append("")
        for i, ins in enumerate(insights, 1):
            insight_text = ins.get("insight", "")
            why_new = ins.get("why_new", "")
            why_matters = ins.get("why_matters", "")
            if insight_text:
                lines.append(f"{insight_text}")
            if why_new:
                lines.append(f"Why it's new: {why_new}")
            if why_matters:
                lines.append(f"Why it matters: {why_matters}")
            if i < len(insights):
                lines.append("")

    # Strip markdown formatting — TTS shouldn't read asterisks
    text = "\n".join(lines)
    text = re.sub(r'\*{1,2}([^*]+)\*{1,2}', r'\1', text)
    text = re.sub(r'_([^_]+)_', r'\1', text)
    return text


def _sanitize_drawtext(s: str) -> str:
    """Escape text for ffmpeg drawtext filter.
    Preserves \\n sequences (ffmpeg newlines) inserted by _wrap_text."""
    # Temporarily replace our newline markers
    s = s.replace("\\n", "\x00NEWLINE\x00")
    s = s.replace("\\", "\\\\")
    s = s.replace("'", "'\\''")
    s = s.replace(":", "\\:")
    s = s.replace("%", "%%")
    s = s.replace(",", "\\,")
    s = s.replace(";", "\\;")
    s = s.replace("[", "\\[")
    s = s.replace("]", "\\]")
    # Restore newlines
    s = s.replace("\x00NEWLINE\x00", "\\n")
    return s


def _wrap_text(text: str, max_len: int = 35, max_lines: int = 5) -> list[str]:
    """Word-wrap text into a list of lines.
    Caps at max_lines to prevent overflow off the viewport."""
    words = text.split()
    lines = []
    current = []
    length = 0
    for word in words:
        if length + len(word) + 1 > max_len and current:
            lines.append(" ".join(current))
            if len(lines) >= max_lines:
                # Truncate with ellipsis
                lines[-1] = lines[-1][:max_len - 3] + "..."
                return lines
            current = [word]
            length = len(word)
        else:
            current.append(word)
            length += len(word) + 1
    if current:
        lines.append(" ".join(current))
    return lines


def run_render(limit: Optional[int] = None, source_type: Optional[str] = None) -> dict:
    """Process all 'distilled' items through render stage. Returns {ok, failed}."""
    ok = 0
    failed = 0

    with db() as conn:
        items = get_pending(conn, "distilled", limit=limit, source_type=source_type)
        item_list = [dict(row) for row in items]

    for item in item_list:
        item_id = item["id"]

        try:
            _render_item(item)
            ok += 1
        except Exception as e:
            with db() as conn:
                set_error(conn, item_id, str(e))
                update_item(conn, item_id, state="failed:render")
            failed += 1
            print(f"  ✗ render [{item_id}]: {e}")

    return {"ok": ok, "failed": failed}


def _render_item(item: dict):
    item_id = item["id"]
    title = item.get("title", "Untitled")
    author = item.get("author", "")
    grade = item.get("grade", "signal")

    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    VIDEO_DIR.mkdir(parents=True, exist_ok=True)

    # 1. TTS
    tts_text = _build_tts_script(item)
    a_path = audio_path(item_id, title)
    _run_tts(tts_text, a_path)

    # 2. Title card video
    v_path = video_path(item_id, title)
    grade_emoji = GRADE_EMOJI.get(grade, "")
    _run_ffmpeg_titlecard(
        audio=a_path,
        output=v_path,
        title=title,
        author=author,
        grade_label=f"{grade_emoji} {grade.upper()}",
    )

    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        update_item(
            conn,
            item_id,
            state="rendered",
            render_audio_path=str(a_path),
            render_path=str(v_path),
            rendered_at=now,
            error=None,
            error_at=None,
        )
    print(f"  ✓ rendered: {title[:50]}")


def _run_tts(text: str, output: Path):
    """Run edge-tts with Aria voice."""
    # edge-tts doesn't handle very long text well; write to temp file
    import tempfile, os
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
        f.write(text)
        tmp = f.name
    try:
        result = subprocess.run(
            [
                "edge-tts",
                "--voice", "en-US-AriaNeural",
                "--file", tmp,
                "--write-media", str(output),
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError(f"edge-tts failed: {result.stderr.strip()[:300]}")
        if not output.exists() or output.stat().st_size < 1000:
            raise RuntimeError(f"edge-tts produced no/empty output at {output}")
    finally:
        os.unlink(tmp)


def _run_ffmpeg_titlecard(
    audio: Path,
    output: Path,
    title: str,
    author: str,
    grade_label: str,
):
    """Generate title card mp4 with dark background + drawtext."""
    title_lines = _wrap_text(title, max_len=35, max_lines=4)
    author_safe = _sanitize_drawtext(author[:60] if author else "")
    grade_safe = _sanitize_drawtext(grade_label)

    # Calculate vertical centering for title block
    line_height = 50  # fontsize 36 + spacing
    total_title_height = len(title_lines) * line_height
    title_y_start = (720 - total_title_height) // 2 - 20

    vf_parts = [
        # Grade badge at top
        f"drawtext=text='{grade_safe}':fontsize=22:fontcolor=#aaaacc"
        f":x=(w-text_w)/2:y=80",
    ]

    # One drawtext per title line — no newline glyphs possible
    for i, line in enumerate(title_lines):
        line_safe = _sanitize_drawtext(line)
        y = title_y_start + i * line_height
        vf_parts.append(
            f"drawtext=text='{line_safe}':fontsize=36:fontcolor=white"
            f":x=(w-text_w)/2:y={y}"
        )

    if author_safe:
        vf_parts.append(
            f"drawtext=text='{author_safe}':fontsize=24:fontcolor=#8888aa"
            f":x=(w-text_w)/2:y=h-120"
        )

    vf = ",".join(vf_parts)

    # Get audio duration to avoid trailing silence
    probe = subprocess.run(
        [FFMPEG, "-i", str(audio), "-f", "null", "-"],
        capture_output=True, text=True, timeout=30,
    )
    # Parse duration from ffmpeg stderr
    duration = None
    for line in probe.stderr.splitlines():
        if "Duration:" in line:
            import re as _re
            m = _re.search(r"Duration:\s*(\d+):(\d+):(\d+\.\d+)", line)
            if m:
                h, mn, s = float(m.group(1)), float(m.group(2)), float(m.group(3))
                duration = h * 3600 + mn * 60 + s
                break

    cmd = [
        FFMPEG, "-y",
        "-f", "lavfi",
        "-i", "color=c=#1a1a2e:s=1280x720:r=1",
        "-i", str(audio),
        "-vf", vf,
        "-c:v", "libx264",
        "-tune", "stillimage",
        "-preset", "fast",
        "-c:a", "aac",
        "-b:a", "128k",
    ]
    # Use explicit duration if detected, otherwise fall back to -shortest
    if duration:
        cmd.extend(["-t", f"{duration:.2f}"])
    else:
        cmd.append("-shortest")
    cmd.append(str(output))

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg title card failed: {result.stderr[-500:]}")
