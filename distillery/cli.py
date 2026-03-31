"""
JanJon Distillery CLI
Raw content in → concentrated signal out.

Usage:
  distill ingest youtube
  distill ingest newsletter
  distill ingest <url>
  distill run [--limit N] [--stage extract|distill|render|deliver|upload]
  distill status [--json]
  distill list [--grade fire|signal|skim] [--state STATE] [--limit N]
  distill history [--days 7]
  distill migrate
  distill cleanup [--days 14]
"""
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import click

from .db import init_db, db, status_counts

GRADE_EMOJI = {
    "skim": "⚡",
    "signal": "📡",
    "fire": "🔥",
    None: "  ",
}

STATE_ORDER = [
    "ingested",
    "extracted",
    "distilled",
    "rendered",
    "delivered",
    "uploaded",
]


@click.group()
@click.version_option("1.0.0", prog_name="distill")
def cli():
    """JanJon Distillery — raw content in, concentrated signal out."""
    init_db()


# ──────────────────────────────────────────────
# distill ingest
# ──────────────────────────────────────────────

@cli.group()
def ingest():
    """Ingest content from a source."""


@ingest.command("youtube")
@click.option("--export", type=click.Path(), default=None, help="Path to watchlater-export.json")
@click.option("--dry-run", is_flag=True)
def ingest_youtube(export, dry_run):
    """Pull from Watch Later (watchlater-export.json or legacy manifest)."""
    from .adapters.youtube import ingest_youtube as _ingest
    export_path = Path(export) if export else None
    result = _ingest(export_path=export_path, dry_run=dry_run)
    click.echo(f"YouTube: +{result['added']} new, {result['skipped']} already known "
               f"({result['total']} total)")


@ingest.command("newsletter")
@click.option("--query", default="label:newsletters is:unread", help="Gmail query")
@click.option("--account", default=None, help="Gmail account email")
@click.option("--limit", default=20, type=int)
@click.option("--dry-run", is_flag=True)
def ingest_newsletter(query, account, limit, dry_run):
    """Pull unread newsletters from Gmail."""
    from .adapters.newsletter import ingest_newsletter as _ingest
    result = _ingest(query=query, account=account, limit=limit, dry_run=dry_run)
    click.echo(f"Newsletter: +{result['added']} new, {result['skipped']} already known "
               f"({result['total']} total)")


@ingest.command("url")
@click.argument("url")
@click.option("--dry-run", is_flag=True)
def ingest_url_cmd(url, dry_run):
    """Ingest an ad-hoc URL."""
    from .adapters.url import ingest_url
    item = ingest_url(url, dry_run=dry_run)
    status = "new" if item.get("inserted") else "already known"
    click.echo(f"URL [{status}]: {item.get('title', url)}")
    click.echo(f"  id: {item['id']}")


# Handle: distill ingest <url> as top-level shorthand
@cli.command("ingest")
@click.argument("target")
@click.option("--dry-run", is_flag=True)
@click.pass_context
def ingest_dispatch(ctx, target, dry_run):
    """
    Ingest from any source.

    TARGET can be 'youtube', 'newsletter', or a URL.
    """
    if target == "youtube":
        ctx.invoke(ingest_youtube, export=None, dry_run=dry_run)
    elif target == "newsletter":
        ctx.invoke(ingest_newsletter, query="label:newsletters is:unread",
                   account=None, limit=20, dry_run=dry_run)
    elif target.startswith("http://") or target.startswith("https://"):
        ctx.invoke(ingest_url_cmd, url=target, dry_run=dry_run)
    else:
        click.echo(f"Unknown target: {target}. Use 'youtube', 'newsletter', or a URL.", err=True)
        sys.exit(1)


# ──────────────────────────────────────────────
# distill run
# ──────────────────────────────────────────────

STAGES = ("extract", "distill", "render", "deliver", "upload")


@cli.command("run")
@click.option("--limit", default=None, type=int, help="Max items to process per stage")
@click.option("--stage", default=None, type=click.Choice(STAGES),
              help="Run only this stage")
@click.option("--source", default=None, type=click.Choice(["youtube", "newsletter", "url"]),
              help="Limit to this source type")
def run_pipeline(limit, stage, source):
    """
    Process pending items through the pipeline.

    By default runs all stages in order: extract → distill → render → deliver → upload.
    Use --stage to run a single stage only.
    """
    from .extract import run_extract
    from .distill import run_distill
    from .render import run_render
    from .deliver import run_deliver
    from .upload import run_upload

    stages_to_run = [stage] if stage else list(STAGES)

    totals = {"ok": 0, "failed": 0}

    for s in stages_to_run:
        click.echo(f"\n▶ {s.upper()}")
        if s == "extract":
            r = run_extract(limit=limit, source_type=source)
        elif s == "distill":
            r = run_distill(limit=limit, source_type=source)
        elif s == "render":
            r = run_render(limit=limit, source_type=source)
        elif s == "deliver":
            r = run_deliver(limit=limit, source_type=source)
        elif s == "upload":
            r = run_upload(limit=limit)
        else:
            continue
        click.echo(f"  ✓ {r.get('ok',0)} ok  ✗ {r.get('failed',0)} failed")
        totals["ok"] += r.get("ok", 0)
        totals["failed"] += r.get("failed", 0)

    click.echo(f"\nDone. {totals['ok']} ok, {totals['failed']} failed.")


# ──────────────────────────────────────────────
# distill status
# ──────────────────────────────────────────────

@cli.command("status")
@click.option("--json", "as_json", is_flag=True)
@click.option("--today", is_flag=True, help="Show today's items only")
def status(as_json, today):
    """Show pipeline state summary."""
    with db() as conn:
        counts = status_counts(conn)

        if today:
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            rows = conn.execute(
                "SELECT * FROM items WHERE DATE(created_at) = ? ORDER BY created_at DESC",
                [today_str]
            ).fetchall()
        else:
            rows = None

    if as_json:
        output = {"counts": counts}
        if today and rows is not None:
            output["today"] = [dict(r) for r in rows]
        click.echo(json.dumps(output, indent=2))
        return

    click.echo("\n🏭 JanJon Distillery — Pipeline Status\n")

    if not counts:
        click.echo("  No items in DB yet.")
        return

    total_items = sum(v["total"] for v in counts.values())
    click.echo(f"  Total items: {total_items}\n")

    for state in STATE_ORDER + [k for k in counts if k not in STATE_ORDER]:
        if state not in counts:
            continue
        info = counts[state]
        click.echo(f"  {state:<12} {info['total']:>4}")
        for grade, n in sorted(info["by_grade"].items()):
            emoji = GRADE_EMOJI.get(grade, "  ")
            click.echo(f"    {emoji} {grade:<8} {n:>3}")

    # Failed states
    with db() as conn:
        failed = conn.execute(
            "SELECT state, COUNT(*) as n FROM items WHERE state LIKE 'failed%' GROUP BY state"
        ).fetchall()
    if failed:
        click.echo(f"\n  {'─'*20}")
        for row in failed:
            click.echo(f"  ✗ {row['state']:<20} {row['n']:>3}")

    if today and rows:
        click.echo(f"\n  Today ({today_str}):")
        for row in rows:
            r = dict(row)
            emoji = GRADE_EMOJI.get(r.get("grade"), "  ")
            click.echo(f"  {emoji} {r.get('state','?'):<12} {r.get('title','?')[:50]}")


# ──────────────────────────────────────────────
# distill list
# ──────────────────────────────────────────────

@cli.command("list")
@click.option("--grade", default=None, type=click.Choice(["fire", "signal", "skim"]))
@click.option("--state", default=None)
@click.option("--source", default=None, type=click.Choice(["youtube", "newsletter", "url"]))
@click.option("--limit", default=20, type=int)
@click.option("--json", "as_json", is_flag=True)
def list_items(grade, state, source, limit, as_json):
    """List items with optional filters."""
    with db() as conn:
        sql = "SELECT * FROM items WHERE 1=1"
        params = []
        if grade:
            sql += " AND grade = ?"
            params.append(grade)
        if state:
            sql += " AND state = ?"
            params.append(state)
        if source:
            sql += " AND source_type = ?"
            params.append(source)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()

    if as_json:
        click.echo(json.dumps([dict(r) for r in rows], indent=2))
        return

    if not rows:
        click.echo("No items found.")
        return

    for row in rows:
        r = dict(row)
        emoji = GRADE_EMOJI.get(r.get("grade"), "  ")
        date_str = (r.get("created_at") or "")[:10]
        click.echo(
            f"{emoji} {date_str}  {r.get('state','?'):<12}  "
            f"[{r.get('source_type','?'):<10}]  {r.get('title','?')[:55]}"
        )


# ──────────────────────────────────────────────
# distill history
# ──────────────────────────────────────────────

@cli.command("history")
@click.option("--days", default=7, type=int)
@click.option("--grade", default=None, type=click.Choice(["fire", "signal", "skim"]))
@click.option("--json", "as_json", is_flag=True)
def history(days, grade, as_json):
    """Show recent distillations."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with db() as conn:
        sql = ("SELECT * FROM items WHERE state IN ('delivered','uploaded') "
               "AND created_at >= ?")
        params = [cutoff]
        if grade:
            sql += " AND grade = ?"
            params.append(grade)
        sql += " ORDER BY created_at DESC"
        rows = conn.execute(sql, params).fetchall()

    if as_json:
        click.echo(json.dumps([dict(r) for r in rows], indent=2))
        return

    click.echo(f"\n📅 Last {days} days\n")
    if not rows:
        click.echo("  Nothing distilled yet.")
        return

    for row in rows:
        r = dict(row)
        emoji = GRADE_EMOJI.get(r.get("grade"), "  ")
        date_str = (r.get("distilled_at") or r.get("created_at") or "")[:10]
        yt_tag = f" [yt:{r['youtube_id']}]" if r.get("youtube_id") else ""
        click.echo(f"  {emoji} {date_str}  {r.get('title','?')[:55]}{yt_tag}")
        if r.get("distill_summary"):
            click.echo(f"     {r['distill_summary'][:80]}")


# ──────────────────────────────────────────────
# distill deliver (re-deliver)
# ──────────────────────────────────────────────

@cli.command("deliver")
@click.option("--date", default=None, help="Re-deliver items distilled on DATE (YYYY-MM-DD)")
@click.option("--id", "item_id", default=None, help="Re-deliver specific item by ID")
@click.option("--limit", default=None, type=int)
def deliver_cmd(date, item_id, limit):
    """Re-deliver distilled items to Telegram."""
    from .deliver import run_deliver

    if date or item_id:
        # Re-queue specific items for delivery
        with db() as conn:
            if item_id:
                conn.execute(
                    "UPDATE items SET state='rendered' WHERE id=? AND state='delivered'",
                    [item_id]
                )
            elif date:
                conn.execute(
                    "UPDATE items SET state='rendered' "
                    "WHERE DATE(distilled_at)=? AND state='delivered'",
                    [date]
                )

    result = run_deliver(limit=limit)
    click.echo(f"Delivered: {result['ok']} ok, {result['failed']} failed")


# ──────────────────────────────────────────────
# distill migrate
# ──────────────────────────────────────────────

@cli.command("migrate")
@click.option("--dry-run", is_flag=True)
@click.option("--verbose", "-v", is_flag=True)
def migrate(dry_run, verbose):
    """Import legacy watchlater manifest into Distillery DB."""
    from .migrate import migrate_watchlater
    click.echo("Migrating watchlater manifest...")
    result = migrate_watchlater(dry_run=dry_run, verbose=verbose)
    click.echo(
        f"Migration: +{result['added']} added, {result['skipped']} already existed, "
        f"{result['errors']} errors ({result['total']} total)"
    )
    if dry_run:
        click.echo("(dry run — no changes written)")


# ──────────────────────────────────────────────
# distill cleanup
# ──────────────────────────────────────────────

@cli.command("cleanup")
@click.option("--days", default=14, type=int, help="Delete renders older than N days")
@click.option("--dry-run", is_flag=True)
def cleanup(days, dry_run):
    """
    Delete local audio/video renders older than N days.
    🔥 items are exempt (they're on YouTube).
    DB records are preserved; only files are deleted.
    """
    from .db import db
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    with db() as conn:
        rows = conn.execute(
            "SELECT id, render_path, render_audio_path, grade FROM items "
            "WHERE created_at < ? AND state IN ('delivered','uploaded') "
            "AND grade != 'fire'",
            [cutoff]
        ).fetchall()

    deleted = 0
    freed_bytes = 0

    for row in rows:
        for path_field in (row["render_path"], row["render_audio_path"]):
            if path_field and Path(path_field).exists():
                size = Path(path_field).stat().st_size
                if not dry_run:
                    Path(path_field).unlink()
                deleted += 1
                freed_bytes += size

    freed_mb = freed_bytes / (1024 * 1024)
    action = "Would delete" if dry_run else "Deleted"
    click.echo(f"{action} {deleted} files, freed {freed_mb:.1f} MB ({len(rows)} items)")


def main():
    cli()


if __name__ == "__main__":
    main()
