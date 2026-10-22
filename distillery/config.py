"""Paths resolved from the environment. Nothing here is specific to one user."""
import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("DISTILLERY_HOME") or "~/.distillery").expanduser()
DB_PATH = Path(os.environ.get("DISTILLERY_DB") or DATA_DIR / "distillery.db").expanduser()
CHANNELS_PATH = Path(
    os.environ.get("DISTILLERY_YOUTUBE_CHANNELS") or DATA_DIR / "youtube-channels.json"
).expanduser()
