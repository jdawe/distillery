"""
Distill stage: Claude grades and extracts insights from transcript text.

Workflow:
1. Read transcript from disk
2. Build prompt from summarize.md + structured JSON output wrapper
3. Run claude --print (or anthropic Python SDK as fallback)
4. Parse JSON response → grade, summary, insights
5. Write full output to distillations/{id}.json
6. Update DB
"""
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .db import db, get_pending, update_item, set_error

BASE = Path.home() / ".openclaw/workspace/data/distillery"
DISTILLATIONS_DIR = BASE / "distillations"

import os as _os
SUMMARIZE_PROMPT = Path(_os.environ.get("DISTILLERY_PROMPT", str(Path(__file__).parent.parent / "prompts" / "summarize.md")))

GRADE_EMOJI = {
    "skim": "⚡",
    "signal": "📡",
    "fire": "🔥",
}

OUTPUT_WRAPPER = """
---

## Output Format

Respond with ONLY valid JSON, no markdown fences, no preamble:

{
  "grade": "skim|signal|fire",
  "summary": "One pithy sentence. Optional second if needed.",
  "insights": [
    {
      "insight": "...",
      "why_new": "...",
      "why_matters": "..."
    }
  ]
}

Grade rubric:
- skim: No material new learnings. Primary restatement of existing ideas.
- signal: 1-2 genuinely new or non-obvious insights.
- fire: Exceptional. Worth consuming in full. Multiple high-signal insights.

If no new insights, use grade "skim" with summary explaining why, and empty insights array.
"""

# Approximate token budget per distillation (truncate transcripts over this)
MAX_TRANSCRIPT_CHARS = 80_000


def distillation_path(item_id: str) -> Path:
    return DISTILLATIONS_DIR / f"{item_id}.json"


def run_distill(limit: Optional[int] = None, source_type: Optional[str] = None) -> dict:
    """Process all 'extracted' items through distillation. Returns {ok, failed}."""
    ok = 0
    failed = 0

    with db() as conn:
        items = get_pending(conn, "extracted", limit=limit, source_type=source_type)
        item_list = [dict(row) for row in items]

    for item in item_list:
        item_id = item["id"]
        transcript_path = item.get("transcript_path")

        if not transcript_path or not Path(transcript_path).exists():
            with db() as conn:
                set_error(conn, item_id, "transcript file missing")
                update_item(conn, item_id, state="failed:distill")
            failed += 1
            continue

        try:
            text = Path(transcript_path).read_text(encoding="utf-8")
            result = _distill(text, title=item.get("title", ""), author=item.get("author", ""))

            # Write full distillation to disk
            out_path = distillation_path(item_id)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

            now = datetime.now(timezone.utc).isoformat()
            with db() as conn:
                update_item(
                    conn,
                    item_id,
                    state="distilled",
                    grade=result["grade"],
                    distill_summary=result["summary"],
                    insights_json=json.dumps(result["insights"]),
                    distillation_path=str(out_path),
                    distilled_at=now,
                    error=None,
                    error_at=None,
                )
            ok += 1
            grade_emoji = GRADE_EMOJI.get(result["grade"], "?")
            print(f"  {grade_emoji} [{result['grade']}] {item.get('title', item_id)[:60]}")

        except Exception as e:
            with db() as conn:
                set_error(conn, item_id, str(e))
                update_item(conn, item_id, state="failed:distill")
            failed += 1
            print(f"  ✗ distill [{item_id}]: {e}")

    return {"ok": ok, "failed": failed}


def _distill(text: str, title: str = "", author: str = "") -> dict:
    """Run distillation via Claude. Returns parsed result dict."""
    prompt = _build_prompt(text, title=title, author=author)

    # Try claude CLI first
    result = _run_claude_cli(prompt)
    if result:
        return result

    # Fallback: anthropic Python SDK
    result = _run_anthropic_sdk(prompt)
    if result:
        return result

    raise RuntimeError("All distillation backends failed")


def _build_prompt(text: str, title: str = "", author: str = "") -> str:
    base_prompt = SUMMARIZE_PROMPT.read_text(encoding="utf-8") if SUMMARIZE_PROMPT.exists() else ""

    # Truncate if needed
    if len(text) > MAX_TRANSCRIPT_CHARS:
        text = text[:MAX_TRANSCRIPT_CHARS] + "\n\n[... truncated ...]"

    header = ""
    if title:
        header += f"Title: {title}\n"
    if author:
        header += f"Author/Source: {author}\n"
    if header:
        header = header.strip() + "\n\n"

    return f"{base_prompt}\n\n{OUTPUT_WRAPPER}\n\n---\n\n## Content\n\n{header}{text}"


def _run_claude_cli(prompt: str) -> Optional[dict]:
    """Run distillation via `claude --print --permission-mode bypassPermissions`."""
    try:
        result = subprocess.run(
            ["claude", "--print", "--permission-mode", "bypassPermissions"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            return None
        return _parse_json_response(result.stdout)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None


def _run_anthropic_sdk(prompt: str) -> Optional[dict]:
    """Fallback: run distillation via anthropic Python SDK."""
    try:
        import anthropic
        client = anthropic.Anthropic()
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text
        return _parse_json_response(text)
    except Exception as e:
        raise RuntimeError(f"Anthropic SDK distillation failed: {e}")


def _parse_json_response(text: str) -> dict:
    """Parse Claude's JSON response. Handles markdown fences and leading text."""
    text = text.strip()

    # Strip markdown code fences
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"```\s*$", "", text, flags=re.MULTILINE)

    # Find JSON object
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"No JSON found in response: {text[:200]}")

    data = json.loads(match.group())

    # Validate required fields
    if "grade" not in data:
        raise ValueError(f"Missing 'grade' in response: {data}")
    if data["grade"] not in ("skim", "signal", "fire"):
        # Normalize common variations
        grade_map = {"low": "skim", "medium": "signal", "high": "fire", "🔥": "fire"}
        data["grade"] = grade_map.get(data["grade"].lower(), "signal")

    if "summary" not in data:
        data["summary"] = ""
    if "insights" not in data:
        data["insights"] = []

    # Store raw response for inspection
    data["_raw"] = text

    return data
