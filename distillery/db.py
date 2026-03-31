"""
Database layer for JanJon Distillery.
SQLite + deterministic item IDs. DB is the index; filesystem is the data store.
"""
import hashlib
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

DB_PATH = Path.home() / ".openclaw/workspace/data/distillery/distillery.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
  id TEXT PRIMARY KEY,
  source_type TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_url TEXT,
  title TEXT,
  author TEXT,

  state TEXT NOT NULL DEFAULT 'ingested',

  -- Extract
  transcript_path TEXT,
  extracted_at TEXT,

  -- Distill
  grade TEXT,
  insights_json TEXT,
  distill_summary TEXT,
  distillation_path TEXT,
  distilled_at TEXT,

  -- Render
  render_path TEXT,
  render_audio_path TEXT,
  rendered_at TEXT,

  -- Deliver
  delivered_at TEXT,
  telegram_message_id TEXT,

  -- Upload (fire only)
  youtube_id TEXT,
  uploaded_at TEXT,

  -- Meta
  created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
  error TEXT,
  error_at TEXT,

  UNIQUE(source_type, source_id)
);

CREATE INDEX IF NOT EXISTS idx_state    ON items(state);
CREATE INDEX IF NOT EXISTS idx_grade    ON items(grade);
CREATE INDEX IF NOT EXISTS idx_source   ON items(source_type);
CREATE INDEX IF NOT EXISTS idx_created  ON items(created_at);
"""


def make_id(source_type: str, source_id: str) -> str:
    """Deterministic SHA-256 hash as item ID."""
    raw = f"{source_type}:{source_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db(db_path: Path = DB_PATH):
    conn = get_conn(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path = DB_PATH):
    with db(db_path) as conn:
        conn.executescript(SCHEMA)


def upsert_item(conn: sqlite3.Connection, item: dict) -> bool:
    """Insert or ignore. Returns True if new row was inserted."""
    cols = list(item.keys())
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    sql = f"INSERT OR IGNORE INTO items ({col_names}) VALUES ({placeholders})"
    cur = conn.execute(sql, [item[c] for c in cols])
    return cur.rowcount > 0


def update_item(conn: sqlite3.Connection, item_id: str, **kwargs):
    if not kwargs:
        return
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [item_id]
    conn.execute(f"UPDATE items SET {sets} WHERE id = ?", vals)


def get_item(conn: sqlite3.Connection, item_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM items WHERE id = ?", [item_id]).fetchone()


def get_item_by_source(
    conn: sqlite3.Connection, source_type: str, source_id: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM items WHERE source_type = ? AND source_id = ?",
        [source_type, source_id],
    ).fetchone()


def get_pending(
    conn: sqlite3.Connection,
    state: str,
    limit: Optional[int] = None,
    source_type: Optional[str] = None,
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM items WHERE state = ?"
    params: list[Any] = [state]
    if source_type:
        sql += " AND source_type = ?"
        params.append(source_type)
    sql += " ORDER BY created_at ASC"
    if limit:
        sql += f" LIMIT {limit}"
    return conn.execute(sql, params).fetchall()


def status_counts(conn: sqlite3.Connection) -> dict:
    rows = conn.execute(
        "SELECT state, grade, COUNT(*) as n FROM items GROUP BY state, grade ORDER BY state"
    ).fetchall()
    result: dict[str, Any] = {}
    for row in rows:
        s = row["state"]
        if s not in result:
            result[s] = {"total": 0, "by_grade": {}}
        result[s]["total"] += row["n"]
        g = row["grade"] or "ungraded"
        result[s]["by_grade"][g] = row["n"]
    return result


def set_error(conn: sqlite3.Connection, item_id: str, error: str):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE items SET error = ?, error_at = ? WHERE id = ?",
        [error, now, item_id],
    )
