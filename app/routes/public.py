# /movie-tv-analytics/app/routes/public.py
from flask import Blueprint, jsonify, request, current_app

from ..services import ingest_trending_and_top, compute_summary, list_items, search_live

bp = Blueprint("public", __name__, url_prefix="/api")

@bp.get("/health")
def health():
    # Minimal proof that the server is up and responding
    return jsonify({"status": "healthy"})


@bp.post("/refresh")
def refresh():
    """Ingest one page of trending + top-rated movies/TV from TMDb into the local DB."""
    session = current_app.session()
    result = ingest_trending_and_top(session=session, pages=int(request.args.get("pages", 1)))
    return jsonify({"ok": True, **result})


@bp.get("/summary")
def summary():
    session = current_app.session()
    data = compute_summary(session)
    return jsonify(data)


@bp.get("/movies")
def movies():
    session = current_app.session()
    sort = request.args.get("sort", "popularity")
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 20))
    data = list_items(session, media_type="movie", sort=sort, page=page, limit=limit)
    return jsonify(data)


@bp.get("/tv")
def tv():
    session = current_app.session()
    sort = request.args.get("sort", "popularity")
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 20))
    data = list_items(session, media_type="tv", sort=sort, page=page, limit=limit)
    return jsonify(data)


@bp.get("/search")
def search():
    q = request.args.get("q", "")
    page = int(request.args.get("page", 1))
    data = search_live(q, page)
    return jsonify(data)
