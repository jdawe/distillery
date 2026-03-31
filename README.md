# JanJon Distillery 🏭

> Raw content in. Concentrated signal out.

A personal content distillation pipeline. Ingests YouTube Watch Later, newsletters, and ad-hoc URLs — grades them with Claude, renders audio with Aria TTS, and delivers to Telegram as text + voice notes. 🔥 items get uploaded to a private YouTube "Distilled" playlist.

---

## Architecture

```
SOURCE ADAPTERS                    PIPELINE STAGES
┌──────────────────┐
│ youtube           │──┐
│ (Watch Later)     │  │    ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│ newsletter        │──┼──→ │ EXTRACT  │─→ │ DISTILL  │─→ │ RENDER   │─→ │ DELIVER  │
│ (Gmail)           │  │    │ text out │   │ Claude   │   │ Aria TTS │   │ Telegram │
│ url               │──┘    │          │   │ + grade  │   │ + title  │   │ + YT 🔥  │
│ (ad-hoc)          │       └──────────┘   │          │   │ card mp4 │   └──────────┘
└──────────────────┘                       └──────────┘   └──────────┘
```

## Grades

| Grade | Emoji | Meaning |
|-------|-------|---------|
| `skim` | ⚡ | Nothing new. Delivered but low-signal. |
| `signal` | 📡 | 1-2 non-obvious insights. Worth your time. |
| `fire` | 🔥 | Exceptional. Also uploaded to YouTube Distilled. |

All grades get: Telegram text message + Aria TTS voice note + local mp4 render.  
Only 🔥 gets: uploaded to YouTube "Distilled" playlist (private).

## Data Layout

```
~/.openclaw/workspace/data/distillery/
  distillery.db                   # SQLite — pipeline state index
  transcripts/
    youtube/{item_id}.txt         # Raw extracted transcript
    newsletter/{item_id}.txt
    url/{item_id}.txt
  distillations/{item_id}.json    # Full Claude output (grade, summary, insights, raw)
  audio/{item_id}.mp3             # Aria TTS render
  video/{item_id}.mp4             # Title card video
```

The DB is the index. The filesystem is the data store. Every transformation is inspectable and replayable.

---

## Installation

```bash
cd projects/distillery
pip install -e .

# Configure your environment
cp .env.example .env
# Edit .env with your Telegram chat ID, YouTube playlist ID, etc.
```

Requires:
- `edge-tts` (`pip install edge-tts` or `brew install edge-tts`)
- `ffmpeg` with freetype support (e.g. `brew install ffmpeg-full` on macOS)
- `summarize` CLI (for YouTube transcript extraction)
- `gog` CLI (for Gmail newsletter extraction)
- `openclaw` CLI (for Telegram delivery)

---

## Usage

### Ingest

```bash
# Pull from Watch Later (reads watchlater-export.json or legacy manifest)
distill ingest youtube

# Pull unread newsletters from Gmail
distill ingest newsletter

# Ad-hoc URL
distill ingest https://www.thediff.co/archive/something

# Shorthand (auto-detects youtube/newsletter/url)
distill ingest youtube
distill ingest https://example.com/article
```

### Run pipeline

```bash
# Process all pending items through all stages
distill run

# Limit to 5 items
distill run --limit 5

# Run only a specific stage
distill run --stage extract
distill run --stage distill
distill run --stage render
distill run --stage deliver
distill run --stage upload

# Process only YouTube items
distill run --source youtube
```

### Status & monitoring

```bash
# Pipeline state counts
distill status

# Today's items
distill status --today

# JSON output (for morning briefing integration)
distill status --json
distill status --today --json

# List items with filters
distill list --grade fire
distill list --state delivered --limit 10
distill list --source newsletter

# Recent distillations
distill history
distill history --days 14 --grade fire
```

### Maintenance

```bash
# Re-deliver today's items to Telegram
distill deliver --date 2026-03-31

# Re-deliver specific item
distill deliver --id abc123def456

# Clean up old renders (keeps DB, deletes files; 🔥 exempt)
distill cleanup --days 14
distill cleanup --dry-run  # preview only

# Import legacy watchlater manifest
distill migrate
distill migrate --dry-run --verbose
```

---

## Morning Briefing Integration

The morning briefing cron uses:
```bash
distill status --today --json
```

And the nightly cron replaces `watchlater-nightly`:
```bash
distill ingest youtube && distill run
```

---

## Configuration

All settings are configurable via environment variables or a `.env` file in the project root.

| Setting | Env Variable | Default |
|---------|-------------|---------|
| DB path | `DISTILLERY_DB` | `~/.openclaw/workspace/data/distillery/distillery.db` |
| TTS Voice | `DISTILLERY_TTS_VOICE` | `en-US-AriaNeural` |
| ffmpeg path | `DISTILLERY_FFMPEG` | `ffmpeg` (or `ffmpeg-full` if installed) |
| YouTube OAuth token | `DISTILLERY_YT_TOKEN` | *(required for 🔥 uploads)* |
| YouTube client secret | `DISTILLERY_YT_CLIENT_SECRET` | *(required for 🔥 uploads)* |
| Distillation prompt | `DISTILLERY_PROMPT` | `./prompts/summarize.md` |
| Telegram chat ID | `DISTILLERY_TELEGRAM_CHAT` | *(required)* |
| YouTube playlist ID | `DISTILLERY_YT_PLAYLIST` | *(required for 🔥 uploads)* |

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

---

## Pipeline State Machine

```
ingested
  → extracted     (text pulled from source)
  → distilled     (Claude graded + insights extracted)
  → rendered      (Aria TTS audio + title card mp4)
  → delivered     (sent to Telegram)
  → uploaded      (🔥 only: on YouTube Distilled)

Any stage can → failed:{stage} with error stored in DB.
```

---

*JanJon Distillery — because you don't have time to drink from the firehose.*
