from __future__ import annotations

import sqlite3
from collections import Counter
from datetime import datetime
from typing import Dict, List, Tuple

from .models import media_row_to_dict
from .tmdb import TMDbClient


def _prepare_media_payload(data: dict) -> dict:
    """Normalise incoming media data for storage."""
    genres = data.get("genres") or ""
    if isinstance(genres, list):
        genres = ",".join([g for g in genres if g])
    return {
        "tmdb_id": data.get("tmdb_id"),
        "media_type": data.get("media_type"),
        "title": data.get("title") or "Untitled",
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


def upsert_media(conn: sqlite3.Connection, items: List[dict]) -> Tuple[int, int]:
    """Insert or update media items. Returns (inserted, updated)."""
    if not items:
        return 0, 0

    inserted = updated = 0
    cur = conn.cursor()
    now = datetime.utcnow().isoformat(timespec="seconds")

    for raw in items:
        payload = _prepare_media_payload(raw)
        key = (payload["tmdb_id"], payload["media_type"])
        cur.execute(
            "SELECT id FROM media_items WHERE tmdb_id = ? AND media_type = ?",
            key,
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE media_items
                SET title = ?, overview = ?, poster_path = ?, backdrop_path = ?,
                    vote_average = ?, vote_count = ?, popularity = ?, release_date = ?,
                    genres = ?, original_language = ?, updated_at = ?
                WHERE tmdb_id = ? AND media_type = ?
                """,
                (
                    payload["title"],
                    payload["overview"],
                    payload["poster_path"],
                    payload["backdrop_path"],
                    payload["vote_average"],
                    payload["vote_count"],
                    payload["popularity"],
                    payload["release_date"],
                    payload["genres"],
                    payload["original_language"],
                    now,
                    payload["tmdb_id"],
                    payload["media_type"],
                ),
            )
            updated += int(cur.rowcount > 0)
        else:
            cur.execute(
                """
                INSERT INTO media_items (
                    tmdb_id, media_type, title, overview, poster_path, backdrop_path,
                    vote_average, vote_count, popularity, release_date, genres,
                    original_language, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["tmdb_id"],
                    payload["media_type"],
                    payload["title"],
                    payload["overview"],
                    payload["poster_path"],
                    payload["backdrop_path"],
                    payload["vote_average"],
                    payload["vote_count"],
                    payload["popularity"],
                    payload["release_date"],
                    payload["genres"],
                    payload["original_language"],
                    now,
                    now,
                ),
            )
            inserted += 1
    return inserted, updated


def ingest_trending_and_top(conn: sqlite3.Connection, pages: int = 1) -> Dict[str, int]:
    client = TMDbClient()

    collected: List[dict] = []
    for p in range(1, pages + 1):
        tr = client.trending_all("day", p)
        collected.extend([client.normalize(r) for r in tr.get("results", [])])

    for p in range(1, pages + 1):
        mv = client.top_rated_movies(p)
        collected.extend([client.normalize(r | {"media_type": "movie"}) for r in mv.get("results", [])])

    for p in range(1, pages + 1):
        tv = client.top_rated_tv(p)
        collected.extend([client.normalize(r | {"media_type": "tv"}) for r in tv.get("results", [])])

    ins, upd = upsert_media(conn, collected)
    conn.commit()
    return {"inserted": ins, "updated": upd, "total": ins + upd}


def list_items(conn: sqlite3.Connection, media_type: str, sort: str = "popularity", page: int = 1, limit: int = 20) -> Dict[str, object]:
    sort_column = "popularity" if sort == "popularity" else "vote_average"
    offset = (page - 1) * limit

    total = conn.execute(
        "SELECT COUNT(*) AS cnt FROM media_items WHERE media_type = ?",
        (media_type,),
    ).fetchone()["cnt"]

    rows = conn.execute(
        f"""
        SELECT *
        FROM media_items
        WHERE media_type = ?
        ORDER BY {sort_column} DESC
        LIMIT ? OFFSET ?
        """,
        (media_type, limit, offset),
    ).fetchall()

    return {
        "total": total,
        "page": page,
        "results": [media_row_to_dict(row) for row in rows],
    }


def compute_summary(conn: sqlite3.Connection) -> Dict[str, object]:
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM media_items").fetchone()[0] or 0
    movies = cur.execute(
        "SELECT COUNT(*) FROM media_items WHERE media_type = 'movie'"
    ).fetchone()[0] or 0
    tv = cur.execute(
        "SELECT COUNT(*) FROM media_items WHERE media_type = 'tv'"
    ).fetchone()[0] or 0
    avg_rating = cur.execute(
        "SELECT AVG(vote_average) FROM media_items"
    ).fetchone()[0] or 0.0

    genres_counter: Counter[str] = Counter()
    for (genre_str,) in cur.execute("SELECT genres FROM media_items").fetchall():
        if not genre_str:
            continue
        for genre in genre_str.split(","):
            genre = genre.strip()
            if genre:
                genres_counter[genre] += 1

    top_genres = [
        {"genre": name, "count": count}
        for name, count in genres_counter.most_common(10)
    ]

    lang_rows = cur.execute(
        """
        SELECT original_language, COUNT(*) AS cnt
        FROM media_items
        GROUP BY original_language
        ORDER BY cnt DESC
        LIMIT 10
        """
    ).fetchall()
    languages = [
        {"language": row["original_language"] or "unknown", "count": row["cnt"]}
        for row in lang_rows
    ]

    return {
        "total_items": total,
        "movies": movies,
        "tv": tv,
        "avg_rating": float(avg_rating or 0.0),
        "top_genres": top_genres,
        "languages": languages,
    }


def search_live(query: str, page: int = 1) -> Dict[str, object]:
    client = TMDbClient()
    data = client.search_multi(query, page)
    results = [client.normalize(r) for r in data.get("results", [])]
    return {"page": data.get("page", 1), "results": results, "total_results": data.get("total_results", 0)}
