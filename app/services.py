from __future__ import annotations

from collections import Counter
from typing import Dict, List, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models import MediaItem
from .tmdb import TMDbClient


def upsert_media(session: Session, items: List[dict]) -> Tuple[int, int]:
    """Insert or update media items. Returns (inserted, updated)."""
    inserted = updated = 0
    existing = {}
    if not items:
        return inserted, updated
    # Preload existing rows for efficiency (avoid tuple_ to keep it simple/cross-db)
    movie_ids = [i["tmdb_id"] for i in items if i["media_type"] == "movie"]
    tv_ids = [i["tmdb_id"] for i in items if i["media_type"] == "tv"]
    if movie_ids:
        for r in (
            session.query(MediaItem)
            .filter(MediaItem.media_type == "movie", MediaItem.tmdb_id.in_(movie_ids))
            .all()
        ):
            existing[(r.tmdb_id, r.media_type)] = r
    if tv_ids:
        for r in (
            session.query(MediaItem)
            .filter(MediaItem.media_type == "tv", MediaItem.tmdb_id.in_(tv_ids))
            .all()
        ):
            existing[(r.tmdb_id, r.media_type)] = r

    for data in items:
        key = (data["tmdb_id"], data["media_type"])
        row = existing.get(key)
        if row:
            # update a few fields
            row.title = data["title"]
            row.overview = data.get("overview")
            row.poster_path = data.get("poster_path")
            row.backdrop_path = data.get("backdrop_path")
            row.vote_average = data.get("vote_average") or 0.0
            row.vote_count = data.get("vote_count") or 0
            row.popularity = data.get("popularity") or 0.0
            row.release_date = data.get("release_date")
            row.genres = data.get("genres") or ""
            row.original_language = data.get("original_language")
            updated += 1
        else:
            session.add(MediaItem(**data))
            inserted += 1
    return inserted, updated


def ingest_trending_and_top(session: Session, pages: int = 1) -> Dict[str, int]:
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

    ins, upd = upsert_media(session, collected)
    session.commit()
    return {"inserted": ins, "updated": upd, "total": ins + upd}


def list_items(session: Session, media_type: str, sort: str = "popularity", page: int = 1, limit: int = 20) -> Dict[str, object]:
    sort_col = MediaItem.popularity if sort == "popularity" else MediaItem.vote_average
    q = (
        session.query(MediaItem)
        .filter(MediaItem.media_type == media_type)
        .order_by(sort_col.desc())
    )
    total = q.count()
    rows = q.offset((page - 1) * limit).limit(limit).all()
    return {"total": total, "page": page, "results": [r.to_dict() for r in rows]}


def compute_summary(session: Session) -> Dict[str, object]:
    total = session.query(func.count(MediaItem.id)).scalar() or 0
    movies = session.query(func.count(MediaItem.id)).filter(MediaItem.media_type == "movie").scalar() or 0
    tv = session.query(func.count(MediaItem.id)).filter(MediaItem.media_type == "tv").scalar() or 0
    avg_rating = session.query(func.avg(MediaItem.vote_average)).scalar() or 0.0

    # naive genre aggregation from stored comma-separated strings
    genres_counter: Counter[str] = Counter()
    for (gstr,) in session.query(MediaItem.genres).all():
        if not gstr:
            continue
        for g in gstr.split(","):
            genres_counter[g.strip()] += 1

    top_genres = [
        {"genre": name, "count": count}
        for name, count in genres_counter.most_common(10)
    ]

    # languages
    lang_counts = (
        session.query(MediaItem.original_language, func.count(MediaItem.id))
        .group_by(MediaItem.original_language)
        .order_by(func.count(MediaItem.id).desc())
        .limit(10)
        .all()
    )
    languages = [
        {"language": lang or "unknown", "count": cnt}
        for lang, cnt in lang_counts
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
