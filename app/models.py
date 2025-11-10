from __future__ import annotations

import sqlite3
from typing import Mapping, Sequence

MEDIA_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS media_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tmdb_id INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    title TEXT NOT NULL,
    overview TEXT DEFAULT '',
    poster_path TEXT,
    backdrop_path TEXT,
    vote_average REAL DEFAULT 0.0,
    vote_count INTEGER DEFAULT 0,
    popularity REAL DEFAULT 0.0,
    release_date TEXT,
    genres TEXT,
    original_language TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tmdb_id, media_type)
);
"""

USERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    password_plain TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

INDEX_SQL: Sequence[str] = (
    "CREATE INDEX IF NOT EXISTS ix_media_type_popularity ON media_items (media_type, popularity);",
    "CREATE INDEX IF NOT EXISTS ix_media_type_vote ON media_items (media_type, vote_average);",
)


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables and indexes if they do not yet exist."""
    conn.execute(MEDIA_TABLE_SQL)
    conn.execute(USERS_TABLE_SQL)
    for stmt in INDEX_SQL:
        conn.execute(stmt)
    conn.commit()


def ensure_password_plain_column(conn: sqlite3.Connection) -> None:
    """Backfill the demo plaintext column when running against an older schema."""
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(users)")}
    if "password_plain" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN password_plain TEXT")
        conn.commit()


def ensure_admin_user(conn: sqlite3.Connection, hash_password) -> None:
    """Seed or normalise the demo Admin account."""
    admin_email = "Admin@Test.com"
    plaintext = "Admin"
    hashed = hash_password(plaintext)

    row = conn.execute(
        "SELECT id FROM users WHERE lower(email) = lower(?) LIMIT 1",
        (admin_email,),
    ).fetchone()

    if row:
        conn.execute(
            "UPDATE users SET password_hash = ?, password_plain = ? WHERE id = ?",
            (hashed, plaintext, row["id"]),
        )
    else:
        legacy = conn.execute(
            "SELECT id FROM users WHERE email = ? LIMIT 1",
            ("Admin",),
        ).fetchone()
        if legacy:
            conn.execute(
                "UPDATE users SET email = ?, password_hash = ?, password_plain = ? WHERE id = ?",
                (admin_email, hashed, plaintext, legacy["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO users (email, password_hash, password_plain) VALUES (?, ?, ?)",
                (admin_email, hashed, plaintext),
            )
    conn.execute(
        "UPDATE users SET password_plain = ? WHERE password_plain IS NULL",
        (plaintext,),
    )
    conn.commit()


def media_row_to_dict(row: Mapping[str, object]) -> dict:
    """Convert a sqlite3.Row from media_items into an API-friendly dict."""
    data = dict(row)
    genres_value = data.get("genres")
    if isinstance(genres_value, str) and genres_value:
        genres = [g.strip() for g in genres_value.split(",") if g.strip()]
    else:
        genres = []
    return {
        "id": data.get("id"),
        "tmdb_id": data.get("tmdb_id"),
        "media_type": data.get("media_type"),
        "title": data.get("title"),
        "overview": data.get("overview") or "",
        "poster_path": data.get("poster_path"),
        "backdrop_path": data.get("backdrop_path"),
        "vote_average": float(data.get("vote_average") or 0.0),
        "vote_count": int(data.get("vote_count") or 0),
        "popularity": float(data.get("popularity") or 0.0),
        "release_date": data.get("release_date"),
        "genres": genres,
        "original_language": data.get("original_language"),
    }
