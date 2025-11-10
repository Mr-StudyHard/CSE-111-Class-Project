from __future__ import annotations

import sqlite3
from typing import Any

from flask import Flask, jsonify, request
from werkzeug.security import check_password_hash, generate_password_hash

from .db import close_db, execute, get_db, query

app = Flask(__name__)
app.teardown_appcontext(close_db)


def _dicts(rows):
    return [dict(row) for row in rows]

IMAGE_BASE_URL = "https://image.tmdb.org/t/p"
PERIOD_DEFAULT_LIMITS = {"weekly": 10, "monthly": 20, "all": 40}
MAX_TRENDING_LIMIT = 50
MAX_PAGE_SIZE = 50
DEFAULT_PAGE_SIZE = 20

def _build_trending_sql(period: str) -> tuple[str, list]:
    """Build trending SQL ordered by popularity and rating."""
    # For now, we don't have created_at/updated_at columns for date filtering
    # But we can still differentiate periods by varying the scoring weight
    
    # Weekly: prioritize popularity heavily
    # Monthly: balance popularity and rating
    # All: prioritize rating
    
    if period == "weekly":
        order_clause = "ORDER BY popularity DESC, score DESC, title"
    elif period == "monthly":
        order_clause = "ORDER BY (COALESCE(popularity, 0) * 0.5 + COALESCE(score, 0) * 10) DESC, title"
    else:  # all
        order_clause = "ORDER BY score DESC, popularity DESC, title"
    
    sql = f"""
    SELECT *
    FROM (
        SELECT 'movie' AS media_type,
               m.movie_id AS item_id,
               m.tmdb_id,
               m.title,
               COALESCE(m.overview, '') AS overview,
               m.poster_path,
               NULL AS backdrop_path,
               m.tmdb_vote_avg AS score,
               m.popularity,
               CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END AS release_date,
               GROUP_CONCAT(DISTINCT g.name) AS genres
        FROM movies m
        LEFT JOIN movie_genres mg ON mg.movie_id = m.movie_id
        LEFT JOIN genres g ON g.genre_id = mg.genre_id
        GROUP BY m.movie_id
        UNION ALL
        SELECT 'show' AS media_type,
               s.show_id AS item_id,
               s.tmdb_id,
               s.title,
               COALESCE(s.overview, '') AS overview,
               s.poster_path,
               NULL AS backdrop_path,
               s.tmdb_vote_avg AS score,
               s.popularity,
               s.first_air_date AS release_date,
               GROUP_CONCAT(DISTINCT g.name) AS genres
        FROM shows s
        LEFT JOIN show_genres sg ON sg.show_id = s.show_id
        LEFT JOIN genres g ON g.genre_id = sg.genre_id
        GROUP BY s.show_id
    )
    {order_clause}
    LIMIT ?
    """
    return sql, []


def _tmdb_image(path: str | None, size: str) -> str | None:
    if not path:
        return None
    if path.startswith("http"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{IMAGE_BASE_URL}/{size}{path}"


def _get_int(param: str | None, default: int, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        value = int(param) if param is not None else default
    except (TypeError, ValueError):
        value = default
    value = max(minimum, value)
    if maximum is not None:
        value = min(value, maximum)
    return value


def _summary_payload() -> dict[str, Any]:
    movies_count = query("SELECT COUNT(*) AS cnt FROM movies")[0]["cnt"]
    shows_count = query("SELECT COUNT(*) AS cnt FROM shows")[0]["cnt"]
    total_items = movies_count + shows_count
    avg_row = query("SELECT AVG(rating) AS avg FROM reviews")
    avg_rating = float(avg_row[0]["avg"] or 0.0) if avg_row else 0.0
    top_genres_rows = query(
        """
        SELECT g.name AS genre, COUNT(*) AS cnt
        FROM movie_genres mg
        JOIN genres g ON g.genre_id = mg.genre_id
        GROUP BY g.genre_id
        ORDER BY cnt DESC, g.name
        LIMIT 10
        """
    )
    top_genres = [{"genre": row["genre"], "count": row["cnt"]} for row in top_genres_rows]
    return {
        "total_items": total_items,
        "movies": movies_count,
        "tv": shows_count,
        "avg_rating": avg_rating,
        "top_genres": top_genres,
        "languages": [],
    }


def _ensure_auth_bootstrap() -> None:
    """
    Make sure the users table has password columns and seed demo credentials.

    We add `password_hash` and `password_plain` columns on the fly for older
    databases, then guarantee the default Admin account exists. Any rows that
    still lack credentials receive a fallback placeholder so the login flow
    behaves deterministically.
    """
    conn = get_db()
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(users)")}
    altered = False
    if "password_hash" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN password_hash TEXT")
        altered = True
    if "password_plain" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN password_plain TEXT")
        altered = True
    if altered:
        conn.commit()

    admin_email = "Admin@Test.com"
    admin_password = "Admin"
    admin_hash = generate_password_hash(admin_password)
    existing_admin = conn.execute(
        "SELECT user_id FROM users WHERE lower(email) = lower(?) LIMIT 1",
        (admin_email,),
    ).fetchone()
    if existing_admin:
        conn.execute(
            """
            UPDATE users
            SET password_hash = ?, password_plain = ?
            WHERE user_id = ?
            """,
            (admin_hash, admin_password, existing_admin["user_id"]),
        )
    else:
        conn.execute(
            """
            INSERT INTO users (email, password_hash, password_plain)
            VALUES (?, ?, ?)
            """,
            (admin_email, admin_hash, admin_password),
        )

    fallback_plain = "changeme"
    fallback_hash = generate_password_hash(fallback_plain)
    conn.execute(
        """
        UPDATE users
        SET password_plain = COALESCE(password_plain, ?),
            password_hash = CASE
                WHEN password_hash IS NULL THEN ?
                ELSE password_hash
            END
        WHERE password_plain IS NULL OR password_hash IS NULL
        """,
        (fallback_plain, fallback_hash),
    )
    conn.commit()


@app.get("/api/health")
def health():
    """
    Lightweight readiness check used by the frontend to decide whether
    auxiliary views (like stored accounts) can be shown.

    We run a trivial query to ensure the SQLite connection works; any
    failure returns a 503 so the UI can surface the issue clearly.
    """
    try:
        _ensure_auth_bootstrap()
        query("SELECT 1")
    except Exception as exc:  # pragma: no cover - defensive logging path
        return jsonify({"status": "unhealthy", "error": str(exc)}), 503
    return jsonify({"status": "healthy"})


@app.get("/api/users")
def list_users():
    """
    Return a lightweight view of demo accounts for the admin UI.

    The legacy frontend expects `user`, `email`, and `password` fields.
    The new schema only stores email addresses, so we derive a friendly
    display name from the prefix and return a placeholder password.
    """
    _ensure_auth_bootstrap()
    rows = query(
        """
        SELECT user_id, email, password_plain, password_hash, created_at
        FROM users
        ORDER BY user_id
        """
    )
    results: list[dict[str, Any]] = []
    for row in rows:
        row_dict = dict(row)
        email = row_dict.get("email")
        if email and "@" in email:
            username = email.split("@", 1)[0]
        else:
            username = f"user-{row_dict.get('user_id')}"
        password_value = row_dict.get("password_plain") or row_dict.get("password_hash") or "******"
        results.append(
            {
                "user": username,
                "email": email,
                "password": password_value,
                "created_at": row_dict.get("created_at"),
            }
        )
    return jsonify(results)


@app.post("/api/signup")
def signup():
    _ensure_auth_bootstrap()
    payload = request.get_json(silent=True) or {}
    email = (payload.get("email") or "").strip()
    password = (payload.get("password") or "").strip()
    username = (payload.get("username") or "").strip()

    if not email or not password:
        return jsonify({"ok": False, "error": "Missing email or password"}), 400

    conn = get_db()
    exists = conn.execute(
        "SELECT 1 FROM users WHERE lower(email) = lower(?) LIMIT 1",
        (email,),
    ).fetchone()
    if exists:
        return jsonify({"ok": False, "error": "Email already exists"}), 409

    hashed = generate_password_hash(password)
    try:
        conn.execute(
            """
            INSERT INTO users (email, password_hash, password_plain)
            VALUES (?, ?, ?)
            """,
            (email, hashed, password),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.rollback()
        return jsonify({"ok": False, "error": "Email already exists"}), 409
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": f"server-error: {exc}"}), 500

    display_name = username or (email.split("@", 1)[0] if "@" in email else email)
    return jsonify({"ok": True, "user": display_name, "email": email})


@app.post("/api/login")
def login_route():
    _ensure_auth_bootstrap()
    payload = request.get_json(silent=True) or {}
    email = (payload.get("email") or "").strip()
    password = (payload.get("password") or "").strip()

    if not email or not password:
        return jsonify({"ok": False, "error": "Missing email or password"}), 400

    rows = query(
        """
        SELECT email, password_hash, password_plain
        FROM users
        WHERE lower(email) = lower(?)
        LIMIT 1
        """,
        (email,),
    )
    if not rows:
        return jsonify({"ok": False, "error": "Invalid credentials"}), 401

    record = dict(rows[0])
    stored_hash = record.get("password_hash")
    stored_plain = record.get("password_plain")

    verified = False
    if stored_hash:
        try:
            verified = check_password_hash(stored_hash, password)
        except ValueError:
            verified = False
    if not verified and stored_plain is not None:
        verified = stored_plain == password

    if not verified:
        return jsonify({"ok": False, "error": "Invalid credentials"}), 401

    display_name = email.split("@", 1)[0] if "@" in email else email
    return jsonify({"ok": True, "user": display_name, "email": record["email"]})


def _list_media(media_type: str, sort: str, page: int, limit: int) -> dict[str, Any]:
    table = "movies" if media_type == "movie" else "shows"
    id_col = "movie_id" if media_type == "movie" else "show_id"
    release_col = "release_year" if media_type == "movie" else "first_air_date"
    order_col = "tmdb_vote_avg"
    offset = (page - 1) * limit

    total = query(f"SELECT COUNT(*) AS cnt FROM {table}")[0]["cnt"]

    rows = query(
        f"""
        SELECT {id_col} AS record_id,
               tmdb_id,
               title,
               COALESCE(overview, '') AS overview,
               poster_path,
               tmdb_vote_avg,
               {release_col} AS release_value
        FROM {table}
        ORDER BY ({order_col} IS NULL), {order_col} DESC, title
        LIMIT ? OFFSET ?
        """,
        (limit, offset),
    )

    results = []
    for row in rows:
        data = dict(row)
        release_value = data.get("release_value")
        if media_type == "movie" and release_value is not None:
            release_value = str(release_value)
        result = {
            "media_type": media_type,
            "id": data["record_id"],
            "tmdb_id": data["tmdb_id"],
            "title": data["title"],
            "overview": data.get("overview") or "",
            "poster_path": data.get("poster_path"),
            "backdrop_path": None,
            "vote_average": data.get("tmdb_vote_avg"),
            "release_date": release_value,
            "genres": [],
        }
        results.append(result)

    return {"total": total, "page": page, "results": results}


@app.get("/api/summary")
def summary():
    return jsonify(_summary_payload())


@app.get("/api/movies")
def movies_list():
    limit = _get_int(request.args.get("limit"), DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE)
    page = _get_int(request.args.get("page"), 1)
    sort = request.args.get("sort", "popularity")
    payload = _list_media("movie", sort, page, limit)
    return jsonify(payload)


@app.get("/api/tv")
def shows_list():
    limit = _get_int(request.args.get("limit"), DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE)
    page = _get_int(request.args.get("page"), 1)
    sort = request.args.get("sort", "popularity")
    payload = _list_media("show", sort, page, limit)
    return jsonify(payload)


@app.get("/api/trending")
def trending():
    period = (request.args.get("period") or "weekly").lower()
    limit_param = request.args.get("limit")
    base_limit = PERIOD_DEFAULT_LIMITS.get(period, PERIOD_DEFAULT_LIMITS["weekly"])
    try:
        limit = int(limit_param) if limit_param else base_limit
    except (TypeError, ValueError):
        limit = base_limit
    limit = max(1, min(limit, MAX_TRENDING_LIMIT))

    sql, params = _build_trending_sql(period)
    rows = query(sql, (*params, limit * 2))
    results = []
    for row in rows:
        data = dict(row)
        genres = [g.strip() for g in (data.get("genres") or "").split(",") if g.strip()]
        poster_url = _tmdb_image(data.get("poster_path"), "w342")
        backdrop_url = _tmdb_image(data.get("backdrop_path"), "w780") or poster_url
        if not poster_url and not backdrop_url:
            continue
        results.append(
            {
                "media_type": data["media_type"],
                "item_id": data["item_id"],
                "tmdb_id": data["tmdb_id"],
                "title": data["title"],
                "overview": data.get("overview") or "",
                "poster_url": poster_url,
                "backdrop_url": backdrop_url,
                "tmdb_vote_avg": data.get("score"),
                "release_date": data.get("release_date"),
                "genres": genres,
            }
        )
    if period == "all":
        movies: list[dict[str, Any]] = [item for item in results if item["media_type"] == "movie"]
        shows: list[dict[str, Any]] = [item for item in results if item["media_type"] != "movie"]
        blended: list[dict[str, Any]] = []
        while len(blended) < limit and (movies or shows):
            if movies:
                blended.append(movies.pop(0))
            if len(blended) >= limit:
                break
            if shows:
                blended.append(shows.pop(0))
        # If one list ran out and we still need slots, append remaining items.
        if len(blended) < limit:
            blended.extend(movies[: limit - len(blended)])
        if len(blended) < limit:
            blended.extend(shows[: limit - len(blended)])
        results = blended
    return jsonify({"period": period, "results": results[:limit]})


@app.get("/api/new-releases")
def new_releases():
    limit = _get_int(request.args.get("limit"), 12, 1, MAX_PAGE_SIZE)
    media_filter = (request.args.get("type") or "all").lower()

    if media_filter == "movie":
        sql = """
            SELECT 'movie' AS media_type,
                   m.movie_id AS item_id,
                   m.tmdb_id,
                   m.title,
                   COALESCE(m.overview, '') AS overview,
                   m.poster_path,
                   m.tmdb_vote_avg AS score,
                   m.popularity,
                   m.release_year AS release_sort,
                   CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END AS release_date,
                   (
                       SELECT GROUP_CONCAT(DISTINCT g.name)
                       FROM movie_genres mg
                       JOIN genres g ON g.genre_id = mg.genre_id
                       WHERE mg.movie_id = m.movie_id
                   ) AS genres
            FROM movies m
            WHERE m.release_year IS NOT NULL
            ORDER BY (release_sort IS NULL), release_sort DESC, (score IS NULL), score DESC, popularity DESC, title
            LIMIT ?
        """
        rows = query(sql, (limit,))
    elif media_filter == "tv":
        sql = """
            SELECT 'tv' AS media_type,
                   s.show_id AS item_id,
                   s.tmdb_id,
                   s.title,
                   COALESCE(s.overview, '') AS overview,
                   s.poster_path,
                   s.tmdb_vote_avg AS score,
                   s.popularity,
                   CASE
                       WHEN s.first_air_date IS NOT NULL THEN CAST(substr(s.first_air_date, 1, 4) AS INTEGER)
                       ELSE NULL
                   END AS release_sort,
                   s.first_air_date AS release_date,
                   (
                       SELECT GROUP_CONCAT(DISTINCT g.name)
                       FROM show_genres sg
                       JOIN genres g ON g.genre_id = sg.genre_id
                       WHERE sg.show_id = s.show_id
                   ) AS genres
            FROM shows s
            WHERE s.first_air_date IS NOT NULL
            ORDER BY (release_sort IS NULL), release_sort DESC, (score IS NULL), score DESC, popularity DESC, title
            LIMIT ?
        """
        rows = query(sql, (limit,))
    else:
        rows = query(
            """
            SELECT *
            FROM (
                SELECT 'movie' AS media_type,
                       m.movie_id AS item_id,
                       m.tmdb_id,
                       m.title,
                       COALESCE(m.overview, '') AS overview,
                       m.poster_path,
                       m.tmdb_vote_avg AS score,
                       m.popularity,
                       m.release_year AS release_sort,
                       CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END AS release_date,
                       (
                           SELECT GROUP_CONCAT(DISTINCT g.name)
                           FROM movie_genres mg
                           JOIN genres g ON g.genre_id = mg.genre_id
                           WHERE mg.movie_id = m.movie_id
                       ) AS genres
                FROM movies m
                WHERE m.release_year IS NOT NULL
                UNION ALL
                SELECT 'tv' AS media_type,
                       s.show_id AS item_id,
                       s.tmdb_id,
                       s.title,
                       COALESCE(s.overview, '') AS overview,
                       s.poster_path,
                       s.tmdb_vote_avg AS score,
                       s.popularity,
                       CASE
                           WHEN s.first_air_date IS NOT NULL THEN CAST(substr(s.first_air_date, 1, 4) AS INTEGER)
                           ELSE NULL
                       END AS release_sort,
                       s.first_air_date AS release_date,
                       (
                           SELECT GROUP_CONCAT(DISTINCT g.name)
                           FROM show_genres sg
                           JOIN genres g ON g.genre_id = sg.genre_id
                           WHERE sg.show_id = s.show_id
                       ) AS genres
                FROM shows s
                WHERE s.first_air_date IS NOT NULL
            )
            ORDER BY (release_sort IS NULL), release_sort DESC, (score IS NULL), score DESC, popularity DESC, title
            LIMIT ?
            """,
            (limit,),
        )

    results = []
    for row in rows:
        data = dict(row)
        genres = [g.strip() for g in (data.get("genres") or "").split(",") if g.strip()]
        results.append(
            {
                "media_type": data["media_type"],
                "item_id": data["item_id"],
                "tmdb_id": data["tmdb_id"],
                "title": data["title"],
                "overview": data.get("overview") or "",
                "poster_path": data.get("poster_path"),
                "vote_average": data.get("score"),
                "popularity": data.get("popularity"),
                "release_date": data.get("release_date"),
                "genres": genres,
            }
        )
    return jsonify({"results": results})


@app.get("/api/search")
def search_catalog():
    term = (request.args.get("q") or "").strip()
    page = _get_int(request.args.get("page"), 1)
    if not term:
        return jsonify({"page": page, "results": [], "total_results": 0})
    
    like = f"%{term.lower()}%"
    
    # Search movies
    movie_rows = query(
        """
        SELECT 'movie' AS media_type,
               m.movie_id AS item_id,
               m.tmdb_id,
               m.title,
               COALESCE(m.overview, '') AS overview,
               m.poster_path,
               m.tmdb_vote_avg AS vote_average,
               m.popularity,
               CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END AS release_date,
               GROUP_CONCAT(DISTINCT g.name) AS genres
        FROM movies m
        LEFT JOIN movie_genres mg ON mg.movie_id = m.movie_id
        LEFT JOIN genres g ON g.genre_id = mg.genre_id
        WHERE lower(m.title) LIKE ?
        GROUP BY m.movie_id
        """,
        (like,),
    )
    
    # Search shows
    show_rows = query(
        """
        SELECT 'tv' AS media_type,
               s.show_id AS item_id,
               s.tmdb_id,
               s.title,
               COALESCE(s.overview, '') AS overview,
               s.poster_path,
               s.tmdb_vote_avg AS vote_average,
               s.popularity,
               s.first_air_date AS release_date,
               GROUP_CONCAT(DISTINCT g.name) AS genres
        FROM shows s
        LEFT JOIN show_genres sg ON sg.show_id = s.show_id
        LEFT JOIN genres g ON g.genre_id = sg.genre_id
        WHERE lower(s.title) LIKE ?
        GROUP BY s.show_id
        """,
        (like,),
    )
    
    # Combine and sort results
    all_rows = list(movie_rows) + list(show_rows)
    all_rows.sort(key=lambda r: (
        r["vote_average"] is None,
        -(r["vote_average"] or 0),
        r["title"]
    ))
    
    results = []
    for row in all_rows[:50]:  # Limit to 50 results
        data = dict(row)
        genres = [g.strip() for g in (data.get("genres") or "").split(",") if g.strip()]
        results.append(
            {
                "media_type": data["media_type"],
                "id": data["item_id"],
                "tmdb_id": data["tmdb_id"],
                "title": data["title"],
                "overview": data.get("overview") or "",
                "poster_path": data.get("poster_path"),
                "backdrop_path": None,
                "vote_average": data.get("vote_average"),
                "popularity": data.get("popularity"),
                "release_date": data.get("release_date"),
                "genres": genres,
                "original_language": None,
            }
        )
    
    return jsonify({"page": page, "results": results, "total_results": len(results)})


@app.get("/api/movie/<int:movie_id>")
def movie_detail(movie_id: int):
    row = query(
        """
        SELECT m.*,
               (
                   SELECT AVG(rating) FROM reviews WHERE movie_id = m.movie_id
               ) AS user_vote_avg,
               (
                   SELECT COUNT(*) FROM reviews WHERE movie_id = m.movie_id
               ) AS review_count
        FROM movies m
        WHERE m.movie_id = ?
        """,
        (movie_id,),
    )
    if not row:
        return jsonify({"error": "movie not found"}), 404

    movie = dict(row[0])
    genres = query(
        """
        SELECT g.name
        FROM movie_genres mg
        JOIN genres g ON g.genre_id = mg.genre_id
        WHERE mg.movie_id = ?
        ORDER BY g.name
        """,
        (movie_id,),
    )
    cast = query(
        """
        SELECT p.name, mc.character, mc.cast_order
        FROM movie_cast mc
        JOIN people p ON p.person_id = mc.person_id
        WHERE mc.movie_id = ?
        ORDER BY mc.cast_order ASC
        LIMIT 10
        """,
        (movie_id,),
    )
    movie["genres"] = [g["name"] for g in genres]
    movie["top_cast"] = _dicts(cast)
    return jsonify(movie)


@app.get("/api/show/<int:show_id>")
def show_detail(show_id: int):
    row = query(
        """
        SELECT s.*,
               (
                   SELECT AVG(rating) FROM reviews WHERE show_id = s.show_id
               ) AS user_vote_avg,
               (
                   SELECT COUNT(*) FROM reviews WHERE show_id = s.show_id
               ) AS review_count,
               (
                   SELECT COUNT(*) FROM seasons WHERE show_id = s.show_id
               ) AS season_count
        FROM shows s
        WHERE s.show_id = ?
        """,
        (show_id,),
    )
    if not row:
        return jsonify({"error": "show not found"}), 404

    show = dict(row[0])
    genres = query(
        """
        SELECT g.name
        FROM show_genres sg
        JOIN genres g ON g.genre_id = sg.genre_id
        WHERE sg.show_id = ?
        ORDER BY g.name
        """,
        (show_id,),
    )
    cast = query(
        """
        SELECT p.name, sc.character, sc.cast_order
        FROM show_cast sc
        JOIN people p ON p.person_id = sc.person_id
        WHERE sc.show_id = ?
        ORDER BY sc.cast_order ASC
        LIMIT 10
        """,
        (show_id,),
    )
    show["genres"] = [g["name"] for g in genres]
    show["top_cast"] = _dicts(cast)
    return jsonify(show)


@app.get("/api/show/<int:show_id>/seasons")
def show_seasons(show_id: int):
    rows = query(
        """
        SELECT
            se.season_id,
            se.season_number,
            se.title AS season_title,
            se.air_date AS season_air_date,
            ep.episode_id,
            ep.episode_number,
            ep.title AS episode_title,
            ep.air_date AS episode_air_date,
            ep.runtime_min
        FROM seasons se
        LEFT JOIN episodes ep ON ep.season_id = se.season_id
        WHERE se.show_id = ?
        ORDER BY se.season_number ASC, ep.episode_number ASC
        """,
        (show_id,),
    )
    if not rows:
        return jsonify({"error": "show not found"}), 404

    seasons: dict[int, Any] = {}
    for row in rows:
        sid = row["season_id"]
        season = seasons.setdefault(
            sid,
            {
                "season_id": sid,
                "season_number": row["season_number"],
                "title": row["season_title"],
                "air_date": row["season_air_date"],
                "episodes": [],
            },
        )
        if row["episode_id"] is not None:
            season["episodes"].append(
                {
                    "episode_id": row["episode_id"],
                    "episode_number": row["episode_number"],
                    "title": row["episode_title"],
                    "air_date": row["episode_air_date"],
                    "runtime_min": row["runtime_min"],
                }
            )
    ordered = sorted(seasons.values(), key=lambda item: item["season_number"])
    return jsonify(ordered)


@app.post("/api/reviews")
def create_review():
    payload = request.get_json(force=True, silent=True) or {}
    user_id = payload.get("user_id")
    target_type = payload.get("target_type")
    target_id = payload.get("target_id")
    rating = payload.get("rating")
    content = payload.get("content", "").strip()

    if not isinstance(user_id, int) or not isinstance(target_id, int):
        return jsonify({"error": "user_id and target_id must be integers"}), 400
    if target_type not in {"movie", "show"}:
        return jsonify({"error": "target_type must be 'movie' or 'show'"}), 400
    try:
        rating_value = float(rating)
    except (TypeError, ValueError):
        return jsonify({"error": "rating must be numeric"}), 400
    if not (0 <= rating_value <= 10):
        return jsonify({"error": "rating must be between 0 and 10"}), 400

    conn = get_db()
    if target_type == "movie":
        sql = """
            INSERT INTO reviews (user_id, movie_id, rating, content)
            VALUES (?, ?, ?, ?)
        """
        params = (user_id, target_id, rating_value, content or None)
    else:
        sql = """
            INSERT INTO reviews (user_id, show_id, rating, content)
            VALUES (?, ?, ?, ?)
        """
        params = (user_id, target_id, rating_value, content or None)

    try:
        cur = conn.execute(sql, params)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 400
    return jsonify({"ok": True, "review_id": cur.lastrowid})


@app.post("/api/watchlist")
def add_watchlist():
    payload = request.get_json(force=True, silent=True) or {}
    user_id = payload.get("user_id")
    target_type = payload.get("target_type")
    target_id = payload.get("target_id")
    if not isinstance(user_id, int) or not isinstance(target_id, int):
        return jsonify({"error": "user_id and target_id must be integers"}), 400
    if target_type not in {"movie", "show"}:
        return jsonify({"error": "target_type must be 'movie' or 'show'"}), 400

    conn = get_db()
    if target_type == "movie":
        sql = "INSERT INTO watchlists (user_id, movie_id, show_id) VALUES (?, ?, NULL)"
    else:
        sql = "INSERT INTO watchlists (user_id, movie_id, show_id) VALUES (?, NULL, ?)"

    try:
        with conn:
            conn.execute(sql, (user_id, target_id))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"ok": True})


@app.delete("/api/watchlist")
def remove_watchlist():
    payload = request.get_json(force=True, silent=True) or {}
    user_id = payload.get("user_id")
    target_type = payload.get("target_type")
    target_id = payload.get("target_id")
    if not isinstance(user_id, int) or not isinstance(target_id, int):
        return jsonify({"error": "user_id and target_id must be integers"}), 400
    if target_type not in {"movie", "show"}:
        return jsonify({"error": "target_type must be 'movie' or 'show'"}), 400

    if target_type == "movie":
        sql = "DELETE FROM watchlists WHERE user_id = ? AND movie_id = ?"
    else:
        sql = "DELETE FROM watchlists WHERE user_id = ? AND show_id = ?"

    deleted = execute(sql, (user_id, target_id))
    return jsonify({"ok": True, "deleted": deleted})


if __name__ == "__main__":
    app.run(debug=True)

