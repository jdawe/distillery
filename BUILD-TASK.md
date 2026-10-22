# Build Task: JanJon Distillery CLI

Build the `distill` CLI as a Python package per SPEC.md in this directory.

## What to build

1. **`distillery/` Python package** with:
   - `db.py` — SQLite schema, migrations, CRUD operations
   - `adapters/youtube.py` — ingest from watchlater-export.json 
   - `adapters/newsletter.py` — ingest from Gmail via `gog gmail` CLI
   - `adapters/url.py` — ingest ad-hoc URL
   - `distill.py` — Claude distillation stage (calls `summarize` CLI for transcript extraction, then uses the distillation prompt for grading)
   - `render.py` — Aria TTS via `edge-tts` + title card via ffmpeg-full drawtext
   - `deliver.py` — Telegram delivery (text + voice note via `openclaw` message tool or direct API)
   - `upload.py` — YouTube upload for 🔥 items (uses `$DISTILLERY_YT_TOKEN` and `$DISTILLERY_YT_CLIENT_SECRET`)
   - `cli.py` — Click-based CLI entry point

2. **`distill` CLI entry point** — install as `distill` command
   - `distill ingest youtube` — import from watchlater-export.json
   - `distill ingest newsletter` — pull unread newsletters
   - `distill ingest <url>` — ad-hoc URL
   - `distill run [--limit N] [--stage extract|distill|render|deliver|upload]` — process pipeline
   - `distill status` — show counts by state/grade
   - `distill list [--grade fire|signal|skim] [--state STATE]`
   - `distill history [--days 7]`

3. **Migration script** — import existing watchlater manifest (149 items from ~/.config/watchlater/manifest.json) into new DB

4. **`setup.py` / `pyproject.toml`** — proper Python package with entry_points

## Key constraints

- **TTS:** Use `edge-tts --voice en-US-AriaNeural` (NOT ElevenLabs)
- **ffmpeg:** Use `/opt/homebrew/opt/ffmpeg-full/bin/ffmpeg` for drawtext (regular ffmpeg lacks freetype)
- **Distillation prompt:** Read from `$DISTILLERY_PROMPT` (default: `./prompts/summarize.md`) and wrap output in JSON: `{grade, summary, insights[]}`
- **Claude for distillation:** Use `claude --print --permission-mode bypassPermissions` with the text piped in, or the `summarize` CLI
- **YouTube OAuth:** Reuse credentials at `$DISTILLERY_YT_TOKEN` and `$DISTILLERY_YT_CLIENT_SECRET`
- **DB location:** `$DISTILLERY_DB` (default: `$DISTILLERY_HOME/distillery.db`, `DISTILLERY_HOME` defaults to `~/.distillery`)
- **File output:** `$DISTILLERY_HOME/{transcripts,audio,video}/`
- **Telegram:** Use `openclaw message send --channel telegram --to $DISTILLERY_TELEGRAM_CHAT` for text, with `--file` + `--as-voice` for audio

## What NOT to build
- Don't build the Watch Later scraper (watchlater-scrape.sh already works)
- Don't build Gmail OAuth (gog CLI handles that)
- Don't build a web UI

## Testing
- Test with `distill ingest https://www.thediff.co` (URL adapter)
- Test `distill status` shows the item
- Test `distill run --limit 1` processes it through extract → distill → render

## GitHub
After building, initialize as a proper git repo ready for `janjon/distillery` on GitHub.
Include a README.md with usage examples.
