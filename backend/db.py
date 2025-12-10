import os
import sqlite3
from typing import Sequence

from flask import g

DEFAULT_DB_PATH = os.getenv("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "..", "movie_tracker.db"))


def get_db() -> sqlite3.Connection:
    """Return a SQLite connection stored on Flask's `g` context."""
    if "sqlite_conn" not in g:
        path = os.getenv("DATABASE_PATH", DEFAULT_DB_PATH)
        conn = sqlite3.connect(path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")  # 30 second timeout for locks
        g.sqlite_conn = conn
    return g.sqlite_conn


def close_db(_: Exception | None = None) -> None:
    """Close the connection at the end of the request/app context."""
    conn = g.pop("sqlite_conn", None)
    if conn is not None:
        conn.close()


def query(sql: str, params: Sequence | dict = ()) -> list[sqlite3.Row]:
    """Execute a SELECT statement and return all rows."""
    conn = get_db()
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def execute(sql: str, params: Sequence | dict = ()) -> int:
    """Execute an INSERT/UPDATE/DELETE statement and return affected rows."""
    conn = get_db()
    cur = conn.execute(sql, params)
    conn.commit()
    rowcount = cur.rowcount
    cur.close()
    return rowcount

