#!/usr/bin/env python3
"""
diagnose_images.py
------------------
Utility script to figure out why certain movies/shows in the local SQLite
database are missing artwork or returning 404s when the frontend loads them.

Usage:
    python scripts/diagnose_images.py [--limit 50]

It prints a summary and lists each record whose poster/backdrop URL could not be
resolved or returned a non-200 status code.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional

import requests
from dotenv import load_dotenv

IMAGE_BASE = "https://image.tmdb.org/t/p"


@dataclass
class Record:
    media_type: str
    db_id: int
    tmdb_id: int
    title: str
    poster_path: Optional[str]
    backdrop_path: Optional[str]


def resolve_path(path: Optional[str], size: str) -> Optional[str]:
    if not path:
        return None
    if path.startswith("http://") or path.startswith("https://"):
        return path
    normalized = path if path.startswith("/") else f"/{path}"
    return f"{IMAGE_BASE}/{size}{normalized}"


def fetch_records(conn: sqlite3.Connection, limit: Optional[int]) -> Iterable[Record]:
    sql = """
        SELECT 'movie' AS media_type,
               movie_id AS db_id,
               tmdb_id,
               title,
               poster_path,
               NULL AS backdrop_path
        FROM movies
        UNION ALL
        SELECT 'show',
               show_id,
               tmdb_id,
               title,
               poster_path,
               NULL
        FROM shows
        ORDER BY title COLLATE NOCASE
    """
    if limit is not None:
        sql += " LIMIT ?"
        rows = conn.execute(sql, (limit,))
    else:
        rows = conn.execute(sql)
    for row in rows:
        yield Record(
            media_type=row[0],
            db_id=row[1],
            tmdb_id=row[2],
            title=row[3],
            poster_path=row[4],
            backdrop_path=row[5],
        )


def check_url(url: str) -> tuple[bool, int]:
    try:
        resp = requests.head(url, allow_redirects=True, timeout=8)
        status = resp.status_code
        if status == 405:  # some CDN endpoints disallow HEAD; fall back to GET
            resp = requests.get(url, stream=True, timeout=8)
            status = resp.status_code
        return 200 <= status < 400, status
    except requests.RequestException:
        return False, 0


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Diagnose missing artwork for movies/shows.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only check the first N records (useful while testing).",
    )
    args = parser.parse_args()

    db_path = os.getenv("DATABASE_PATH") or os.path.join(os.getcwd(), "movie_tracker.db")
    if not os.path.exists(db_path):
        raise SystemExit(f"Database not found at {db_path!r}. Run scripts/reset_db.sh first.")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    missing_path: list[str] = []
    unreachable: list[str] = []
    checked = 0

    for record in fetch_records(conn, args.limit):
        checked += 1
        poster_url = resolve_path(record.poster_path, "w342")
        if not poster_url:
            missing_path.append(f"{record.media_type}:{record.db_id} ({record.title}) → poster_path missing")
            continue

        ok, status = check_url(poster_url)
        if not ok:
            unreachable.append(
                f"{record.media_type}:{record.db_id} ({record.title}) → {poster_url} [status={status}]"
            )

    conn.close()

    print(f"Checked {checked} records in {db_path!r}")
    print()
    print(f"Records with no poster/backdrop path: {len(missing_path)}")
    if missing_path:
        print("-" * 80)
        print("\n".join(missing_path))
        print()

    print(f"Records with unreachable URLs: {len(unreachable)}")
    if unreachable:
        print("-" * 80)
        print("\n".join(unreachable))
        print()

    if not missing_path and not unreachable:
        print("All tested records returned a valid image URL.")


if __name__ == "__main__":
    main()

