# JanJon Distillery — Spec

## Overview
Unified content distillation pipeline. Raw media in → concentrated signal out.
Handles any content source through pluggable adapters. Single SQLite DB tracks all state.

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

## CLI: `distill`

```
distill ingest <source> [args]    # Add content to pipeline
  distill ingest youtube           # Pull from Watch Later (runs scraper)
  distill ingest newsletter        # Pull unread newsletters from Gmail
  distill ingest <url>             # Ad-hoc URL

distill run [--limit N]           # Process pending items through all stages
distill status                    # Show pipeline state
distill list [--grade fire]       # List items by grade/state/source
distill deliver [--date today]    # Re-deliver today's distillations
distill history [--days 7]        # What was distilled recently
```

## Database Schema

```sql
CREATE TABLE items (
  id TEXT PRIMARY KEY,             -- deterministic hash(source_type + source_id)
  source_type TEXT NOT NULL,       -- 'youtube' | 'newsletter' | 'url'
  source_id TEXT NOT NULL,         -- video ID, gmail thread ID, URL
  source_url TEXT,                 -- canonical URL
  title TEXT,
  author TEXT,
  
  -- State machine
  state TEXT NOT NULL DEFAULT 'ingested',  
    -- ingested → extracted → distilled → rendered → delivered
  
  -- Extract stage
  transcript_path TEXT,            -- path to extracted text file
  extracted_at DATETIME,
  
  -- Distill stage (Claude)
  grade TEXT,                      -- 'skim' | 'signal' | 'fire'
  insights_json TEXT,              -- JSON: [{insight, why_new, why_matters}]
  distill_summary TEXT,            -- 1-2 sentence pithy summary for briefing
  distilled_at DATETIME,
  
  -- Render stage (Aria TTS + title card)
  render_path TEXT,                -- local mp4 path
  render_audio_path TEXT,          -- local mp3/ogg for Telegram voice
  rendered_at DATETIME,
  
  -- Deliver stage (Telegram)
  delivered_at DATETIME,
  telegram_message_id TEXT,
  
  -- Metadata
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  error TEXT,
  error_at DATETIME,
  
  UNIQUE(source_type, source_id)
);

CREATE INDEX idx_state ON items(state);
CREATE INDEX idx_grade ON items(grade);
CREATE INDEX idx_source ON items(source_type);
CREATE INDEX idx_created ON items(created_at);
```

## Grading

Claude distillation returns a structured grade alongside insights:
- **skim** — No material new learnings. Gets rendered + delivered but marked as low-signal.
- **signal** — 1-2 genuinely new/non-obvious insights. Default grade for most content.
- **fire** (🔥) — Worth consuming in full. Local render preserved across GC.

ALL grades get: text summary to Telegram + Aria TTS voice note to Telegram + local mp4 render.
🔥 renders are preserved indefinitely; other renders age out via `distill cleanup --days N`.

## Distillation Prompt

Uses existing `prompts/summarize.md` with structured output wrapper:
```json
{
  "grade": "signal",
  "summary": "One pithy sentence. Optional second.",
  "insights": [
    {
      "insight": "...",
      "why_new": "...",
      "why_matters": "..."
    }
  ]
}
```

## TTS

- Voice: Edge TTS `en-US-AriaNeural` (free, no API key)
- Speed: default (user listens at app-level speed control)
- Command: `edge-tts -t "<text>" --voice en-US-AriaNeural --write-media <output.mp3>`

## Video Render

Title card: ffmpeg-full drawtext (title + author on #1a1a2e background)
```
ffmpeg-full -f lavfi -i "color=c=#1a1a2e:s=1280x720:r=1" \
  -i audio.mp3 \
  -vf "drawtext=text='Title':fontsize=52:fontcolor=white:x=(w-text_w)/2:y=(h-text_h)/2-40, \
       drawtext=text='Author':fontsize=32:fontcolor=#8888aa:x=(w-text_w)/2:y=(h/2)+60" \
  -c:v libx264 -tune stillimage -c:a aac -shortest output.mp4
```

## Delivery

Telegram (target: `$DISTILLERY_TELEGRAM_CHAT`):
- Text message: grade emoji + title + author + pithy summary
- Voice note: the rendered audio sent as voice message (asVoice: true)

## File Layout

```
data/distillery/
  distillery.db              # SQLite database
  transcripts/               # Extracted text files
    youtube/<video_id>.txt
    newsletter/<thread_id>.txt
    url/<hash>.txt
  audio/                     # Rendered audio (mp3)
  video/                     # Rendered video (mp4) 
```

## Migration

Import existing watchlater manifest (149 items) into the new DB.
Mark all existing items as state='delivered' (they've already been through the old pipeline).

## Integration

- Morning briefing cron consumes `distill status --today --json` instead of doing its own summarization
- watchlater-nightly cron replaced by `distill ingest youtube && distill run`
- Ad-hoc: user says "distill <url>" → Jan runs `distill ingest <url> && distill run`

## Disk Cleanup

Weekly cleanup sweeps renders older than 14 days (keeps DB records, deletes files).
🔥 items are exempt (they're on YouTube permanently).

## Dependencies

- Python 3.14+
- edge-tts (Aria voice)
- ffmpeg-full (drawtext/freetype)
- summarize CLI (transcript extraction)
- trafilatura (URL text extraction)
- gog CLI (Gmail newsletter extraction)
