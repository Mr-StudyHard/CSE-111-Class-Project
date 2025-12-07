from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any
from uuid import uuid4

from flask import Flask, jsonify, request, send_from_directory
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

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
               m.overview,
               m.poster_path,
               NULL AS backdrop_path,
               m.tmdb_vote_avg AS score,
               m.popularity,
               CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END AS release_date,
               GROUP_CONCAT(DISTINCT g.name) AS genres
        FROM movies m
        INNER JOIN movie_genres mg ON mg.movie_id = m.movie_id
        INNER JOIN genres g ON g.genre_id = mg.genre_id
        WHERE m.overview IS NOT NULL AND m.overview != ''
        GROUP BY m.movie_id
        UNION ALL
        SELECT 'show' AS media_type,
               s.show_id AS item_id,
               s.tmdb_id,
               s.title,
               s.overview,
               s.poster_path,
               NULL AS backdrop_path,
               s.tmdb_vote_avg AS score,
               s.popularity,
               s.first_air_date AS release_date,
               GROUP_CONCAT(DISTINCT g.name) AS genres
        FROM shows s
        INNER JOIN show_genres sg ON sg.show_id = s.show_id
        INNER JOIN genres g ON g.genre_id = sg.genre_id
        WHERE s.overview IS NOT NULL AND s.overview != ''
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
    language_rows = query(
        """
        SELECT language, COUNT(*) AS cnt
        FROM (
            SELECT lower(trim(original_language)) AS language
            FROM movies
            WHERE original_language IS NOT NULL AND trim(original_language) != ''
            UNION ALL
            SELECT lower(trim(original_language)) AS language
            FROM shows
            WHERE original_language IS NOT NULL AND trim(original_language) != ''
        )
        WHERE language IS NOT NULL AND language != ''
        GROUP BY language
        ORDER BY cnt DESC, language
        LIMIT 20
        """
    )
    languages = [{"language": row["language"], "count": row["cnt"]} for row in language_rows]
    return {
        "total_items": total_items,
        "movies": movies_count,
        "tv": shows_count,
        "avg_rating": avg_rating,
        "top_genres": top_genres,
        "languages": languages,
    }


def _next_manual_tmdb_id(table: str) -> int:
    """
    Generate a synthetic TMDb ID for manually added records.

    Real TMDb IDs are positive integers. To avoid collisions we allocate
    strictly negative IDs and walk "downwards" from the most-negative one
    currently stored in the table.
    """
    conn = get_db()
    col = "tmdb_id"
    row = conn.execute(f"SELECT MIN({col}) AS min_id FROM {table}").fetchone()
    current_min = row["min_id"] if row and row["min_id"] is not None else 0
    if current_min is None or current_min >= 0:
        return -1
    return int(current_min) - 1


def _get_or_create_genre_id(name: str) -> int:
    """
    Look up a genre by case-insensitive name, inserting it if needed.
    """
    clean = name.strip()
    if not clean:
        raise ValueError("genre name must be non-empty")
    conn = get_db()
    row = conn.execute(
        "SELECT genre_id FROM genres WHERE lower(name) = lower(?)",
        (clean,),
    ).fetchone()
    if row:
        return int(row["genre_id"])
    cur = conn.execute(
        "INSERT INTO genres (tmdb_genre_id, name) VALUES (NULL, ?)",
        (clean,),
    )
    conn.commit()
    return int(cur.lastrowid)


def _ensure_auth_bootstrap() -> None:
    """
    Make sure the users table has password columns, display_name column, and seed demo credentials.

    We add `password_hash`, `password_plain`, and `display_name` columns on the fly for older
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
    if "display_name" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN display_name TEXT")
        altered = True
        # Backfill display_name from email for existing users
        all_users = conn.execute("SELECT user_id, email FROM users").fetchall()
        for user_row in all_users:
            # Handle both dict-like (Row) and tuple-like access
            try:
                email = user_row["email"]
                user_id = user_row["user_id"]
            except (TypeError, KeyError):
                # Fallback for tuple access
                user_id, email = user_row[0], user_row[1]
            
            if not email:
                continue
                
            if "@" in email:
                display_name = email.split("@", 1)[0]
            else:
                display_name = email
                
            conn.execute(
                "UPDATE users SET display_name = ? WHERE user_id = ?",
                (display_name, user_id)
            )
    if altered:
        conn.commit()

    admin_email = "Admin@Test.com"
    admin_password = "Admin"
    admin_display_name = "Admin"
    admin_hash = generate_password_hash(admin_password)
    existing_admin = conn.execute(
        "SELECT user_id FROM users WHERE lower(email) = lower(?) LIMIT 1",
        (admin_email,),
    ).fetchone()
    if existing_admin:
        conn.execute(
            """
            UPDATE users
            SET password_hash = ?, password_plain = ?, display_name = COALESCE(display_name, ?)
            WHERE user_id = ?
            """,
            (admin_hash, admin_password, admin_display_name, existing_admin["user_id"]),
        )
    else:
        conn.execute(
            """
            INSERT INTO users (email, password_hash, password_plain, display_name)
            VALUES (?, ?, ?, ?)
            """,
            (admin_email, admin_hash, admin_password, admin_display_name),
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
                "user_id": row_dict.get("user_id"),
                "password": password_value,
                "created_at": row_dict.get("created_at"),
            }
        )
    return jsonify(results)


@app.get("/api/user/by-email")
def get_user_by_email():
    """
    Get user information by email address.
    Query parameter: email
    """
    email = request.args.get("email")
    if not email:
        return jsonify({"ok": False, "error": "email parameter is required"}), 400
    
    rows = query(
        """
        SELECT user_id, email
        FROM users
        WHERE lower(email) = lower(?)
        LIMIT 1
        """,
        (email,),
    )
    
    if not rows:
        return jsonify({"ok": False, "error": "User not found"}), 404
    
    row = dict(rows[0])
    return jsonify({"ok": True, "user_id": row["user_id"], "email": row["email"]})


def _get_current_user() -> dict | None:
    """
    Extract and validate the current user from the request.
    Expects Authorization header with format: "Bearer user_id:email"
    Returns user dict, or None if not authenticated.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    
    try:
        token = auth_header[7:]  # Remove "Bearer " prefix
        user_id_str, email = token.split(":", 1)
        user_id = int(user_id_str)
        
        rows = query(
            "SELECT user_id, email FROM users WHERE user_id = ? AND lower(email) = lower(?)",
            (user_id, email),
        )
        if rows:
            row = dict(rows[0])
            return {
                "user_id": row["user_id"],
                "email": row["email"],
            }
    except (ValueError, IndexError):
        pass
    return None


@app.get("/api/user/settings")
def get_user_settings():
    """
    Get current user's settings.
    Requires authentication via Authorization header.
    """
    try:
        _ensure_auth_bootstrap()  # Ensure display_name column exists
        user = _get_current_user()
        if not user:
            return jsonify({"ok": False, "error": "Authentication required"}), 401
        
        rows = query(
            """
            SELECT user_id, email, display_name, created_at
            FROM users
            WHERE user_id = ?
            LIMIT 1
            """,
            (user["user_id"],),
        )
        
        if not rows:
            return jsonify({"ok": False, "error": "User not found"}), 404
        
        row = dict(rows[0])
        # If display_name is not set, derive from email
        display_name = row.get("display_name")
        if not display_name:
            email = row.get("email", "")
            display_name = email.split("@", 1)[0] if "@" in email else email
        
        return jsonify({
            "ok": True,
            "user_id": row["user_id"],
            "email": row["email"],
            "display_name": display_name,
            "created_at": row.get("created_at"),
            "is_admin": False  # Admin system was removed, always False
        })
    except Exception as exc:
        import traceback
        print(f"Error in get_user_settings: {traceback.format_exc()}")
        return jsonify({"ok": False, "error": f"Server error: {str(exc)}"}), 500


@app.put("/api/user/settings")
def update_user_settings():
    """
    Update current user's settings (display_name, email and/or password).
    Requires authentication via Authorization header.
    
    Expected JSON:
      {
        "current_password": "current",  # required for verification
        "display_name": "John Doe",    # optional
        "new_email": "new@email.com",   # optional
        "new_password": "newpassword"   # optional
      }
    """
    _ensure_auth_bootstrap()  # Ensure display_name column exists
    user = _get_current_user()
    if not user:
        return jsonify({"ok": False, "error": "Authentication required"}), 401
    
    payload = request.get_json(silent=True) or {}
    current_password = (payload.get("current_password") or "").strip()
    display_name = (payload.get("display_name") or "").strip()
    new_email = (payload.get("new_email") or "").strip()
    new_password = (payload.get("new_password") or "").strip()
    
    if not current_password:
        return jsonify({"ok": False, "error": "Current password is required"}), 400
    
    if not display_name and not new_email and not new_password:
        return jsonify({"ok": False, "error": "No changes specified"}), 400
    
    conn = get_db()
    
    # Verify current password
    rows = query(
        "SELECT user_id, password_hash, password_plain FROM users WHERE user_id = ?",
        (user["user_id"],),
    )
    
    if not rows:
        return jsonify({"ok": False, "error": "User not found"}), 404
    
    user_row = dict(rows[0])
    stored_hash = user_row.get("password_hash")
    stored_plain = user_row.get("password_plain")
    
    # Verify current password
    verified = False
    if stored_hash:
        try:
            verified = check_password_hash(stored_hash, current_password)
        except ValueError:
            verified = False
    if not verified and stored_plain is not None:
        verified = stored_plain == current_password
    
    if not verified:
        return jsonify({"ok": False, "error": "Current password is incorrect"}), 401
    
    try:
        updates = []
        params = []
        
        # Update display_name if provided
        if display_name:
            if len(display_name) > 50:
                return jsonify({"ok": False, "error": "Display name must be 50 characters or less"}), 400
            updates.append("display_name = ?")
            params.append(display_name)
        
        # Update email if provided
        if new_email:
            # Check if email already exists for another user
            existing = conn.execute(
                "SELECT user_id FROM users WHERE lower(email) = lower(?) AND user_id != ?",
                (new_email, user["user_id"]),
            ).fetchone()
            
            if existing:
                return jsonify({"ok": False, "error": "Email already in use"}), 409
            
            updates.append("email = ?")
            params.append(new_email)
        
        # Update password if provided
        if new_password:
            new_hash = generate_password_hash(new_password)
            updates.append("password_hash = ?")
            params.append(new_hash)
            updates.append("password_plain = ?")
            params.append(new_password)
        
        if updates:
            params.append(user["user_id"])
            sql = f"UPDATE users SET {', '.join(updates)} WHERE user_id = ?"
            conn.execute(sql, tuple(params))
            conn.commit()
        
        return jsonify({"ok": True, "message": "Settings updated successfully"})
    
    except sqlite3.IntegrityError:
        conn.rollback()
        return jsonify({"ok": False, "error": "Email already in use"}), 409
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": f"Failed to update settings: {str(exc)}"}), 500


@app.post("/api/signup")
def signup():
    _ensure_auth_bootstrap()
    payload = request.get_json(silent=True) or {}
    email = (payload.get("email") or "").strip()
    password = (payload.get("password") or "").strip()
    username = (payload.get("username") or "").strip()
    display_name = (payload.get("display_name") or "").strip()

    if not email or not password:
        return jsonify({"ok": False, "error": "Missing email or password"}), 400

    conn = get_db()
    exists = conn.execute(
        "SELECT 1 FROM users WHERE lower(email) = lower(?) LIMIT 1",
        (email,),
    ).fetchone()
    if exists:
        return jsonify({"ok": False, "error": "Email already exists"}), 409

    # Use display_name if provided, otherwise use username, otherwise derive from email
    final_display_name = display_name or username
    if not final_display_name:
        final_display_name = email.split("@", 1)[0] if "@" in email else email

    hashed = generate_password_hash(password)
    try:
        conn.execute(
            """
            INSERT INTO users (email, password_hash, password_plain, display_name)
            VALUES (?, ?, ?, ?)
            """,
            (email, hashed, password, final_display_name),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.rollback()
        return jsonify({"ok": False, "error": "Email already exists"}), 409
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": f"server-error: {exc}"}), 500

    return jsonify({"ok": True, "user": final_display_name, "email": email, "display_name": final_display_name})


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
        SELECT user_id, email, password_hash, password_plain, display_name
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

    # Use display_name if available, otherwise derive from email
    display_name = record.get("display_name")
    if not display_name:
        display_name = email.split("@", 1)[0] if "@" in email else email
    
    return jsonify({
        "ok": True,
        "user": display_name,
        "email": record["email"],
        "user_id": record["user_id"],
        "display_name": display_name
    })


def _list_media(media_type: str, sort: str, page: int, limit: int, genre: str | None = None, language: str | None = None) -> dict[str, Any]:
    table = "movies" if media_type == "movie" else "shows"
    id_col = "movie_id" if media_type == "movie" else "show_id"
    release_col = "release_year" if media_type == "movie" else "first_air_date"
    offset = (page - 1) * limit
    genre_table = "movie_genres" if media_type == "movie" else "show_genres"
    
    # Build WHERE clause
    where_conditions = ["t.overview IS NOT NULL", "t.overview != ''"]
    params_count = []
    params_rows = []
    
    # Add genre filter
    if genre and genre.lower() != "all":
        where_conditions.append("g.name = ?")
        params_count.append(genre)
        params_rows.append(genre)
    
    # Add language filter
    if language and language.lower() != "all":
        where_conditions.append("t.original_language = ?")
        params_count.append(language)
        params_rows.append(language)
    
    where_clause = " AND ".join(where_conditions)
    
    # Determine order clause
    if sort == "rating":
        order_clause = f"(t.tmdb_vote_avg IS NULL), t.tmdb_vote_avg DESC, t.title"
    elif sort == "title":
        order_clause = "t.title ASC"
    elif sort == "release_date":
        order_clause = f"(t.{release_col} IS NULL), t.{release_col} DESC, t.title"
    else:  # popularity (default)
        order_clause = f"(t.popularity IS NULL), t.popularity DESC, t.title"

    # Count only items with overview AND at least one genre
    total_sql = f"""
        SELECT COUNT(DISTINCT t.{id_col}) AS cnt 
        FROM {table} t
        INNER JOIN {genre_table} gt ON t.{id_col} = gt.{id_col}
        INNER JOIN genres g ON g.genre_id = gt.genre_id
        WHERE {where_clause}
    """
    total = query(total_sql, tuple(params_count))[0]["cnt"]

    rows = query(
        f"""
        SELECT DISTINCT t.{id_col} AS record_id,
               t.tmdb_id,
               t.title,
               t.overview,
               t.poster_path,
               t.tmdb_vote_avg,
               t.popularity,
               t.{release_col} AS release_value,
               t.original_language
        FROM {table} t
        INNER JOIN {genre_table} gt ON t.{id_col} = gt.{id_col}
        INNER JOIN genres g ON g.genre_id = gt.genre_id
        WHERE {where_clause}
        ORDER BY {order_clause}
        LIMIT ? OFFSET ?
        """,
        tuple(params_rows) + (limit, offset),
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
            "popularity": data.get("popularity"),
            "release_date": release_value,
            "original_language": data.get("original_language"),
            "genres": [],
        }
        results.append(result)

    return {"total": total, "page": page, "results": results}


@app.get("/api/summary")
def summary():
    return jsonify(_summary_payload())


@app.get("/api/genres")
def get_genres():
    """Get all available genres"""
    rows = query("SELECT name FROM genres ORDER BY name")
    genres = [row["name"] for row in rows]
    return jsonify({"genres": genres})


@app.get("/api/languages")
def get_languages():
    """Get all available languages from movies and shows"""
    movie_langs = query("SELECT DISTINCT original_language FROM movies WHERE original_language IS NOT NULL ORDER BY original_language")
    show_langs = query("SELECT DISTINCT original_language FROM shows WHERE original_language IS NOT NULL ORDER BY original_language")
    all_langs = set()
    for row in movie_langs:
        if row["original_language"]:
            all_langs.add(row["original_language"])
    for row in show_langs:
        if row["original_language"]:
            all_langs.add(row["original_language"])
    languages = sorted(list(all_langs))
    return jsonify({"languages": languages})


@app.get("/api/movies")
def movies_list():
    limit = _get_int(request.args.get("limit"), DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE)
    page = _get_int(request.args.get("page"), 1)
    sort = request.args.get("sort", "popularity")
    genre = request.args.get("genre")
    language = request.args.get("language")
    payload = _list_media("movie", sort, page, limit, genre, language)
    return jsonify(payload)


@app.post("/api/movies")
def create_movie():
    """
    Create a new movie record plus at least one genre association.

    This is used by the admin "Add Movie/TV" UI and expects JSON in the form:
      {
        "title": "Example",
        "overview": "...",           # optional
        "language": "en",           # optional ISO code
        "release_year": 2024,       # optional int
        "tmdb_score": 7.3,          # optional float -> tmdb_vote_avg
        "popularity": 10.0,         # optional float
        "poster_path": "/path.jpg", # optional string (relative or URL)
        "genre": "Drama"            # required, creates if missing
      }
    """
    payload = request.get_json(silent=True) or {}
    title = (payload.get("title") or "").strip()
    if not title:
        return jsonify({"ok": False, "error": "Title is required"}), 400

    overview = (payload.get("overview") or "").strip() or None
    language = (payload.get("language") or "").strip() or None
    poster_path = (payload.get("poster_path") or "").strip() or None
    try:
        release_year_raw = payload.get("release_year")
        release_year = int(release_year_raw) if release_year_raw not in (None, "") else None
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "release_year must be a whole number"}), 400

    def _coerce_float(key: str) -> float | None:
        value = payload.get(key)
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{key} must be numeric")

    try:
        tmdb_score = _coerce_float("tmdb_score")
        popularity = _coerce_float("popularity")
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    genre_name = (payload.get("genre") or "").strip()
    if not genre_name:
        return jsonify({"ok": False, "error": "Genre is required"}), 400

    conn = get_db()
    try:
        tmdb_id = _next_manual_tmdb_id("movies")
        cur = conn.execute(
            """
            INSERT INTO movies (
                tmdb_id, title, release_year, runtime_min,
                overview, poster_path, backdrop_path,
                original_language, tmdb_vote_avg, popularity
            )
            VALUES (?, ?, ?, NULL, ?, ?, NULL, ?, ?, ?)
            """,
            (tmdb_id, title, release_year, overview, poster_path, language, tmdb_score, popularity),
        )
        movie_id = int(cur.lastrowid)
        genre_id = _get_or_create_genre_id(genre_name)
        conn.execute(
            "INSERT OR IGNORE INTO movie_genres (movie_id, genre_id) VALUES (?, ?)",
            (movie_id, genre_id),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 400

    return jsonify(
        {
            "ok": True,
            "id": movie_id,
            "tmdb_id": tmdb_id,
            "title": title,
        }
    ), 201


@app.get("/api/tv")
def shows_list():
    limit = _get_int(request.args.get("limit"), DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE)
    page = _get_int(request.args.get("page"), 1)
    sort = request.args.get("sort", "popularity")
    genre = request.args.get("genre")
    language = request.args.get("language")
    payload = _list_media("show", sort, page, limit, genre, language)
    return jsonify(payload)


@app.post("/api/tv")
def create_show():
    """
    Create a new TV show record plus at least one genre association.

    Expected JSON is analogous to create_movie, but the date field is:
      {
        "title": "Example Series",
        "overview": "...",
        "language": "en",
        "first_air_year": 2024,     # optional
        "tmdb_score": 7.3,
        "popularity": 10.0,
        "poster_path": "/path.jpg",
        "genre": "Drama"
      }
    """
    payload = request.get_json(silent=True) or {}
    title = (payload.get("title") or "").strip()
    if not title:
        return jsonify({"ok": False, "error": "Title is required"}), 400

    overview = (payload.get("overview") or "").strip() or None
    language = (payload.get("language") or "").strip() or None
    poster_path = (payload.get("poster_path") or "").strip() or None

    try:
        first_air_raw = payload.get("first_air_year")
        first_air_year = int(first_air_raw) if first_air_raw not in (None, "") else None
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "first_air_year must be a whole number"}), 400

    def _coerce_float(key: str) -> float | None:
        value = payload.get(key)
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{key} must be numeric")

    try:
        tmdb_score = _coerce_float("tmdb_score")
        popularity = _coerce_float("popularity")
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    genre_name = (payload.get("genre") or "").strip()
    if not genre_name:
        return jsonify({"ok": False, "error": "Genre is required"}), 400

    conn = get_db()
    try:
        tmdb_id = _next_manual_tmdb_id("shows")
        # Store first_air_year as a YYYY-01-01 date string if provided.
        first_air_date = f"{first_air_year}-01-01" if first_air_year is not None else None
        cur = conn.execute(
            """
            INSERT INTO shows (
                tmdb_id, title, first_air_date, last_air_date,
                overview, poster_path, backdrop_path,
                original_language, tmdb_vote_avg, popularity
            )
            VALUES (?, ?, ?, NULL, ?, ?, NULL, ?, ?, ?)
            """,
            (tmdb_id, title, first_air_date, overview, poster_path, language, tmdb_score, popularity),
        )
        show_id = int(cur.lastrowid)
        genre_id = _get_or_create_genre_id(genre_name)
        conn.execute(
            "INSERT OR IGNORE INTO show_genres (show_id, genre_id) VALUES (?, ?)",
            (show_id, genre_id),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 400

    return jsonify(
        {
            "ok": True,
            "id": show_id,
            "tmdb_id": tmdb_id,
            "title": title,
        }
    ), 201


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
                   m.overview,
                   m.poster_path,
                   m.tmdb_vote_avg AS score,
                   m.popularity,
                   m.release_year AS release_sort,
                   CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END AS release_date,
                   GROUP_CONCAT(DISTINCT g.name) AS genres
            FROM movies m
            INNER JOIN movie_genres mg ON mg.movie_id = m.movie_id
            INNER JOIN genres g ON g.genre_id = mg.genre_id
            WHERE m.release_year IS NOT NULL AND m.overview IS NOT NULL AND m.overview != ''
            GROUP BY m.movie_id
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
                   s.overview,
                   s.poster_path,
                   s.tmdb_vote_avg AS score,
                   s.popularity,
                   CASE
                       WHEN s.first_air_date IS NOT NULL THEN CAST(substr(s.first_air_date, 1, 4) AS INTEGER)
                       ELSE NULL
                   END AS release_sort,
                   s.first_air_date AS release_date,
                   GROUP_CONCAT(DISTINCT g.name) AS genres
            FROM shows s
            INNER JOIN show_genres sg ON sg.show_id = s.show_id
            INNER JOIN genres g ON g.genre_id = sg.genre_id
            WHERE s.first_air_date IS NOT NULL AND s.overview IS NOT NULL AND s.overview != ''
            GROUP BY s.show_id
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
                       m.overview,
                       m.poster_path,
                       m.tmdb_vote_avg AS score,
                       m.popularity,
                       m.release_year AS release_sort,
                       CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END AS release_date,
                       GROUP_CONCAT(DISTINCT g.name) AS genres
                FROM movies m
                INNER JOIN movie_genres mg ON mg.movie_id = m.movie_id
                INNER JOIN genres g ON g.genre_id = mg.genre_id
                WHERE m.release_year IS NOT NULL AND m.overview IS NOT NULL AND m.overview != ''
                GROUP BY m.movie_id
                UNION ALL
                SELECT 'tv' AS media_type,
                       s.show_id AS item_id,
                       s.tmdb_id,
                       s.title,
                       s.overview,
                       s.poster_path,
                       s.tmdb_vote_avg AS score,
                       s.popularity,
                       CASE
                           WHEN s.first_air_date IS NOT NULL THEN CAST(substr(s.first_air_date, 1, 4) AS INTEGER)
                           ELSE NULL
                       END AS release_sort,
                       s.first_air_date AS release_date,
                       GROUP_CONCAT(DISTINCT g.name) AS genres
                FROM shows s
                INNER JOIN show_genres sg ON sg.show_id = s.show_id
                INNER JOIN genres g ON g.genre_id = sg.genre_id
                WHERE s.first_air_date IS NOT NULL AND s.overview IS NOT NULL AND s.overview != ''
                GROUP BY s.show_id
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
               m.overview,
               m.poster_path,
               m.tmdb_vote_avg AS vote_average,
               m.popularity,
               CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END AS release_date,
               GROUP_CONCAT(DISTINCT g.name) AS genres
        FROM movies m
        INNER JOIN movie_genres mg ON mg.movie_id = m.movie_id
        INNER JOIN genres g ON g.genre_id = mg.genre_id
        WHERE lower(m.title) LIKE ? AND m.overview IS NOT NULL AND m.overview != ''
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
               s.overview,
               s.poster_path,
               s.tmdb_vote_avg AS vote_average,
               s.popularity,
               s.first_air_date AS release_date,
               GROUP_CONCAT(DISTINCT g.name) AS genres
        FROM shows s
        INNER JOIN show_genres sg ON sg.show_id = s.show_id
        INNER JOIN genres g ON g.genre_id = sg.genre_id
        WHERE lower(s.title) LIKE ? AND s.overview IS NOT NULL AND s.overview != ''
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
    movie["vote_average"] = movie.get("tmdb_vote_avg")
    movie["runtime_minutes"] = movie.get("runtime_min")
    if movie.get("user_vote_avg") is not None:
        movie["user_avg_rating"] = float(movie["user_vote_avg"])
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
    show["vote_average"] = show.get("tmdb_vote_avg")
    if show.get("user_vote_avg") is not None:
        show["user_avg_rating"] = float(show["user_vote_avg"])
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


@app.delete("/api/movies/<int:movie_id>")
def delete_movie(movie_id: int):
    """
    Delete a movie and its associated data (genres, reviews, etc.).
    Also deletes the associated image file if it exists.
    """
    conn = get_db()
    try:
        # First check if movie exists
        check_row = conn.execute(
            "SELECT movie_id, poster_path FROM movies WHERE movie_id = ?",
            (movie_id,),
        ).fetchone()
        
        if not check_row:
            return jsonify({"ok": False, "error": f"Movie with ID {movie_id} not found"}), 404
        
        poster_path = check_row["poster_path"] if check_row else None
        
        # Delete the movie (cascade will handle related records)
        # Note: Foreign keys are already enabled in get_db()
        deleted = conn.execute(
            "DELETE FROM movies WHERE movie_id = ?",
            (movie_id,),
        ).rowcount
        
        if deleted == 0:
            return jsonify({"ok": False, "error": f"Failed to delete movie {movie_id}"}), 500
        
        conn.commit()
        
        # Delete associated image file if it's a local upload
        if poster_path and poster_path.startswith("imageofmovie/"):
            try:
                image_filename = poster_path.replace("imageofmovie/", "")
                image_path = IMAGE_UPLOAD_FOLDER / image_filename
                if image_path.exists():
                    image_path.unlink()
            except Exception:
                pass  # Don't fail if image deletion fails
        
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as exc:
        conn.rollback()
        error_msg = str(exc)
        # Log full traceback for debugging
        import traceback
        print(f"Error deleting movie {movie_id}: {traceback.format_exc()}")
        return jsonify({"ok": False, "error": error_msg}), 500


@app.put("/api/movies/<int:movie_id>")
def update_movie(movie_id: int):
    """
    Update an existing movie record.
    
    Expected JSON payload (all fields optional except title if provided):
      {
        "title": "Updated Title",
        "overview": "...",
        "language": "en",
        "release_year": 2024,
        "tmdb_score": 7.3,
        "popularity": 10.0,
        "poster_path": "/path.jpg",
        "genre": "Drama"
      }
    """
    conn = get_db()
    try:
        # Check if movie exists
        check_row = conn.execute(
            "SELECT movie_id FROM movies WHERE movie_id = ?",
            (movie_id,),
        ).fetchone()
        
        if not check_row:
            return jsonify({"ok": False, "error": f"Movie with ID {movie_id} not found"}), 404
        
        payload = request.get_json(silent=True) or {}
        
        # Build update fields
        updates = []
        params = []
        
        if "title" in payload:
            title = (payload.get("title") or "").strip()
            if not title:
                return jsonify({"ok": False, "error": "Title cannot be empty"}), 400
            updates.append("title = ?")
            params.append(title)
        
        if "overview" in payload:
            overview = (payload.get("overview") or "").strip() or None
            updates.append("overview = ?")
            params.append(overview)
        
        if "language" in payload:
            language = (payload.get("language") or "").strip() or None
            updates.append("original_language = ?")
            params.append(language)
        
        if "release_year" in payload:
            try:
                release_year_raw = payload.get("release_year")
                release_year = int(release_year_raw) if release_year_raw not in (None, "") else None
                updates.append("release_year = ?")
                params.append(release_year)
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "release_year must be a whole number"}), 400
        
        if "poster_path" in payload:
            poster_path = (payload.get("poster_path") or "").strip() or None
            updates.append("poster_path = ?")
            params.append(poster_path)
        
        def _coerce_float(key: str) -> float | None:
            value = payload.get(key)
            if value in (None, ""):
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                raise ValueError(f"{key} must be numeric")
        
        if "tmdb_score" in payload:
            try:
                tmdb_score = _coerce_float("tmdb_score")
                updates.append("tmdb_vote_avg = ?")
                params.append(tmdb_score)
            except ValueError as exc:
                return jsonify({"ok": False, "error": str(exc)}), 400
        
        if "popularity" in payload:
            try:
                popularity = _coerce_float("popularity")
                updates.append("popularity = ?")
                params.append(popularity)
            except ValueError as exc:
                return jsonify({"ok": False, "error": str(exc)}), 400
        
        if not updates:
            return jsonify({"ok": False, "error": "No fields to update"}), 400
        
        # Update the movie
        params.append(movie_id)
        update_sql = f"UPDATE movies SET {', '.join(updates)} WHERE movie_id = ?"
        conn.execute(update_sql, tuple(params))
        
        # Update genres if provided (comma-separated)
        if "genre" in payload:
            genre_input = (payload.get("genre") or "").strip()
            if genre_input:
                # Split by comma and process each genre
                genre_names = [g.strip() for g in genre_input.split(",") if g.strip()]
                if genre_names:
                    # Remove existing genres
                    conn.execute("DELETE FROM movie_genres WHERE movie_id = ?", (movie_id,))
                    # Add all new genres
                    for genre_name in genre_names:
                        genre_id = _get_or_create_genre_id(genre_name)
                        conn.execute(
                            "INSERT OR IGNORE INTO movie_genres (movie_id, genre_id) VALUES (?, ?)",
                            (movie_id, genre_id),
                        )
        
        conn.commit()
        return jsonify({"ok": True, "id": movie_id})
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.put("/api/tv/<int:show_id>")
def update_show(show_id: int):
    """
    Update an existing TV show record.
    
    Expected JSON payload (all fields optional except title if provided):
      {
        "title": "Updated Title",
        "overview": "...",
        "language": "en",
        "first_air_year": 2024,
        "tmdb_score": 7.3,
        "popularity": 10.0,
        "poster_path": "/path.jpg",
        "genre": "Drama"
      }
    """
    conn = get_db()
    try:
        # Check if show exists
        check_row = conn.execute(
            "SELECT show_id FROM shows WHERE show_id = ?",
            (show_id,),
        ).fetchone()
        
        if not check_row:
            return jsonify({"ok": False, "error": f"TV show with ID {show_id} not found"}), 404
        
        payload = request.get_json(silent=True) or {}
        
        # Build update fields
        updates = []
        params = []
        
        if "title" in payload:
            title = (payload.get("title") or "").strip()
            if not title:
                return jsonify({"ok": False, "error": "Title cannot be empty"}), 400
            updates.append("title = ?")
            params.append(title)
        
        if "overview" in payload:
            overview = (payload.get("overview") or "").strip() or None
            updates.append("overview = ?")
            params.append(overview)
        
        if "language" in payload:
            language = (payload.get("language") or "").strip() or None
            updates.append("original_language = ?")
            params.append(language)
        
        if "first_air_year" in payload:
            try:
                first_air_raw = payload.get("first_air_year")
                first_air_year = int(first_air_raw) if first_air_raw not in (None, "") else None
                first_air_date = f"{first_air_year}-01-01" if first_air_year is not None else None
                updates.append("first_air_date = ?")
                params.append(first_air_date)
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "first_air_year must be a whole number"}), 400
        
        if "poster_path" in payload:
            poster_path = (payload.get("poster_path") or "").strip() or None
            updates.append("poster_path = ?")
            params.append(poster_path)
        
        def _coerce_float(key: str) -> float | None:
            value = payload.get(key)
            if value in (None, ""):
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                raise ValueError(f"{key} must be numeric")
        
        if "tmdb_score" in payload:
            try:
                tmdb_score = _coerce_float("tmdb_score")
                updates.append("tmdb_vote_avg = ?")
                params.append(tmdb_score)
            except ValueError as exc:
                return jsonify({"ok": False, "error": str(exc)}), 400
        
        if "popularity" in payload:
            try:
                popularity = _coerce_float("popularity")
                updates.append("popularity = ?")
                params.append(popularity)
            except ValueError as exc:
                return jsonify({"ok": False, "error": str(exc)}), 400
        
        if not updates:
            return jsonify({"ok": False, "error": "No fields to update"}), 400
        
        # Update the show
        params.append(show_id)
        update_sql = f"UPDATE shows SET {', '.join(updates)} WHERE show_id = ?"
        conn.execute(update_sql, tuple(params))
        
        # Update genres if provided (comma-separated)
        if "genre" in payload:
            genre_input = (payload.get("genre") or "").strip()
            if genre_input:
                # Split by comma and process each genre
                genre_names = [g.strip() for g in genre_input.split(",") if g.strip()]
                if genre_names:
                    # Remove existing genres
                    conn.execute("DELETE FROM show_genres WHERE show_id = ?", (show_id,))
                    # Add all new genres
                    for genre_name in genre_names:
                        genre_id = _get_or_create_genre_id(genre_name)
                        conn.execute(
                            "INSERT OR IGNORE INTO show_genres (show_id, genre_id) VALUES (?, ?)",
                            (show_id, genre_id),
                        )
        
        conn.commit()
        return jsonify({"ok": True, "id": show_id})
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.delete("/api/tv/<int:show_id>")
def delete_show(show_id: int):
    """
    Delete a TV show and its associated data (genres, reviews, seasons, episodes, etc.).
    Also deletes the associated image file if it exists.
    """
    conn = get_db()
    try:
        # First check if show exists
        check_row = conn.execute(
            "SELECT show_id, poster_path FROM shows WHERE show_id = ?",
            (show_id,),
        ).fetchone()
        
        if not check_row:
            return jsonify({"ok": False, "error": f"TV show with ID {show_id} not found"}), 404
        
        poster_path = check_row["poster_path"] if check_row else None
        
        # Delete the show (cascade will handle related records)
        # Note: Foreign keys are already enabled in get_db()
        deleted = conn.execute(
            "DELETE FROM shows WHERE show_id = ?",
            (show_id,),
        ).rowcount
        
        if deleted == 0:
            return jsonify({"ok": False, "error": f"Failed to delete TV show {show_id}"}), 500
        
        conn.commit()
        
        # Delete associated image file if it's a local upload
        if poster_path and poster_path.startswith("imageofmovie/"):
            try:
                image_filename = poster_path.replace("imageofmovie/", "")
                image_path = IMAGE_UPLOAD_FOLDER / image_filename
                if image_path.exists():
                    image_path.unlink()
            except Exception:
                pass  # Don't fail if image deletion fails
        
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as exc:
        conn.rollback()
        error_msg = str(exc)
        # Log full traceback for debugging
        import traceback
        print(f"Error deleting movie {movie_id}: {traceback.format_exc()}")
        return jsonify({"ok": False, "error": error_msg}), 500


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


@app.get("/api/reviews")
def get_reviews():
    """
    Get reviews for a movie or TV show.
    Query parameters:
    - target_type: 'movie' or 'show'
    - target_id: the movie_id or show_id
    """
    target_type = request.args.get("target_type")
    target_id = request.args.get("target_id")
    
    if not target_type or not target_id:
        return jsonify({"error": "target_type and target_id are required"}), 400
    
    if target_type not in {"movie", "show"}:
        return jsonify({"error": "target_type must be 'movie' or 'show'"}), 400
    
    try:
        target_id_int = int(target_id)
    except (TypeError, ValueError):
        return jsonify({"error": "target_id must be an integer"}), 400
    
    conn = get_db()
    if target_type == "movie":
        sql = """
            SELECT r.review_id, r.user_id, r.content, r.rating, r.created_at,
                   u.email AS user_email
            FROM reviews r
            LEFT JOIN users u ON u.user_id = r.user_id
            WHERE r.movie_id = ?
            ORDER BY r.created_at DESC
        """
    else:
        sql = """
            SELECT r.review_id, r.user_id, r.content, r.rating, r.created_at,
                   u.email AS user_email
            FROM reviews r
            LEFT JOIN users u ON u.user_id = r.user_id
            WHERE r.show_id = ?
            ORDER BY r.created_at DESC
        """
    
    rows = query(sql, (target_id_int,))
    reviews = []
    for row in rows:
        reviews.append({
            "review_id": row["review_id"],
            "user_id": row["user_id"],
            "user_email": row["user_email"],
            "content": row["content"],
            "rating": row["rating"],
            "created_at": row["created_at"],
        })
    
    return jsonify({"ok": True, "reviews": reviews, "count": len(reviews)})


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
    
    # Rating is optional - default to None if not provided
    rating_value = None
    if rating is not None:
        try:
            rating_value = float(rating)
            if not (0 <= rating_value <= 10):
                return jsonify({"error": "rating must be between 0 and 10"}), 400
        except (TypeError, ValueError):
            return jsonify({"error": "rating must be numeric"}), 400
    
    if not content:
        return jsonify({"error": "Review content is required"}), 400

    conn = get_db()
    if target_type == "movie":
        sql = """
            INSERT INTO reviews (user_id, movie_id, rating, content)
            VALUES (?, ?, ?, ?)
        """
        params = (user_id, target_id, rating_value, content)
    else:
        sql = """
            INSERT INTO reviews (user_id, show_id, rating, content)
            VALUES (?, ?, ?, ?)
        """
        params = (user_id, target_id, rating_value, content)

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


@app.get("/api/movie/<int:movie_id>")
def get_movie_detail(movie_id: int):
    """Get detailed information for a specific movie."""
    rows = query(
        """
        SELECT m.movie_id,
               m.tmdb_id,
               m.title,
               m.overview,
               m.poster_path,
               m.backdrop_path,
               m.release_year,
               m.runtime_minutes,
               m.tmdb_vote_avg,
               m.tmdb_vote_count,
               m.popularity,
               m.original_language,
               m.budget,
               m.revenue,
               GROUP_CONCAT(DISTINCT g.name) AS genres,
               AVG(r.rating) AS user_avg_rating,
               COUNT(DISTINCT r.review_id) AS review_count
        FROM movies m
        LEFT JOIN movie_genres mg ON mg.movie_id = m.movie_id
        LEFT JOIN genres g ON g.genre_id = mg.genre_id
        LEFT JOIN reviews r ON r.movie_id = m.movie_id
        WHERE m.movie_id = ?
        GROUP BY m.movie_id
        """,
        (movie_id,),
    )
    
    if not rows:
        return jsonify({"error": "Movie not found"}), 404
    
    row = dict(rows[0])
    genres = [g.strip() for g in (row.get("genres") or "").split(",") if g.strip()]
    
    result = {
        "movie_id": row["movie_id"],
        "tmdb_id": row["tmdb_id"],
        "title": row["title"],
        "overview": row.get("overview") or "",
        "poster_path": row.get("poster_path"),
        "backdrop_path": row.get("backdrop_path"),
        "release_year": row.get("release_year"),
        "runtime_minutes": row.get("runtime_minutes"),
        "vote_average": row.get("tmdb_vote_avg"),
        "vote_count": row.get("tmdb_vote_count"),
        "popularity": row.get("popularity"),
        "original_language": row.get("original_language"),
        "budget": row.get("budget"),
        "revenue": row.get("revenue"),
        "genres": genres,
        "user_avg_rating": row.get("user_avg_rating"),
        "review_count": row.get("review_count") or 0,
        "media_type": "movie"
    }
    
    return jsonify(result)


@app.get("/api/show/<int:show_id>")
def get_show_detail(show_id: int):
    """Get detailed information for a specific TV show."""
    rows = query(
        """
        SELECT s.show_id,
               s.tmdb_id,
               s.title,
               s.overview,
               s.poster_path,
               s.backdrop_path,
               s.first_air_date,
               s.last_air_date,
               s.tmdb_vote_avg,
               s.tmdb_vote_count,
               s.popularity,
               s.original_language,
               COUNT(DISTINCT se.season_id) AS season_count,
               GROUP_CONCAT(DISTINCT g.name) AS genres,
               AVG(r.rating) AS user_avg_rating,
               COUNT(DISTINCT r.review_id) AS review_count
        FROM shows s
        LEFT JOIN seasons se ON se.show_id = s.show_id
        LEFT JOIN show_genres sg ON sg.show_id = s.show_id
        LEFT JOIN genres g ON g.genre_id = sg.genre_id
        LEFT JOIN reviews r ON r.show_id = s.show_id
        WHERE s.show_id = ?
        GROUP BY s.show_id
        """,
        (show_id,),
    )
    
    if not rows:
        return jsonify({"error": "TV show not found"}), 404
    
    row = dict(rows[0])
    # Split genres by comma - GROUP_CONCAT returns comma-separated string
    genres_str = row.get("genres") or ""
    genres = [g.strip() for g in genres_str.split(",") if g.strip()] if genres_str else []
    
    result = {
        "show_id": row["show_id"],
        "tmdb_id": row["tmdb_id"],
        "title": row["title"],
        "overview": row.get("overview") or "",
        "poster_path": row.get("poster_path"),
        "backdrop_path": row.get("backdrop_path"),
        "first_air_date": row.get("first_air_date"),
        "last_air_date": row.get("last_air_date"),
        "vote_average": row.get("tmdb_vote_avg"),
        "vote_count": row.get("tmdb_vote_count"),
        "popularity": row.get("popularity"),
        "original_language": row.get("original_language"),
        "season_count": row.get("season_count") or 0,
        "genres": genres,
        "user_avg_rating": row.get("user_avg_rating"),
        "review_count": row.get("review_count") or 0,
        "media_type": "tv"
    }
    
    return jsonify(result)


# Image upload configuration
IMAGE_UPLOAD_FOLDER = Path(__file__).parent.parent / "imageofmovie"
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}

# Ensure upload folder exists
IMAGE_UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)


def _allowed_file(filename: str) -> bool:
    """Check if file extension is allowed."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.post("/api/upload-image")
def upload_image():
    """
    Upload an image file and save it to the imageofmovie folder.
    Returns the relative path that can be stored in the database.
    """
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file provided"}), 400
    
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"ok": False, "error": "No file selected"}), 400
    
    if not _allowed_file(file.filename):
        return jsonify({"ok": False, "error": f"File type not allowed. Allowed: {', '.join(ALLOWED_EXTENSIONS)}"}), 400
    
    try:
        # Generate a unique filename to avoid collisions
        original_ext = file.filename.rsplit(".", 1)[1].lower()
        unique_filename = f"{uuid4().hex}.{original_ext}"
        filepath = IMAGE_UPLOAD_FOLDER / unique_filename
        
        # Save the file
        file.save(str(filepath))
        
        # Return relative path from project root (e.g., "imageofmovie/abc123.jpg")
        relative_path = f"imageofmovie/{unique_filename}"
        return jsonify({"ok": True, "path": relative_path})
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Failed to save image: {str(exc)}"}), 500


@app.get("/api/images/<path:filename>")
def serve_image(filename: str):
    """
    Serve uploaded images from the imageofmovie folder.
    """
    try:
        # Security: ensure filename doesn't contain path traversal
        # Remove any "imageofmovie/" prefix if present
        clean_filename = filename.replace("imageofmovie/", "").replace("imageofmovie\\", "")
        safe_filename = secure_filename(os.path.basename(clean_filename))
        
        if not safe_filename:
            return jsonify({"error": "Invalid filename"}), 400
        
        # Check if file exists
        filepath = IMAGE_UPLOAD_FOLDER / safe_filename
        if not filepath.exists() or not filepath.is_file():
            return jsonify({"error": f"Image not found: {safe_filename}"}), 404
        
        return send_from_directory(str(IMAGE_UPLOAD_FOLDER), safe_filename)
    except Exception as exc:
        import traceback
        return jsonify({"error": f"Error serving image: {str(exc)}", "trace": traceback.format_exc()}), 500


if __name__ == "__main__":
    app.run(debug=True)

