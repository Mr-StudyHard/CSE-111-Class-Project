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
               GROUP_CONCAT(DISTINCT g.name) AS genres,
               (
                   SELECT AVG(rating) FROM reviews WHERE movie_id = m.movie_id
               ) AS user_avg_rating,
               (
                   SELECT COUNT(*) FROM reviews WHERE movie_id = m.movie_id
               ) AS review_count
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
               GROUP_CONCAT(DISTINCT g.name) AS genres,
               (
                   SELECT AVG(rating) FROM reviews WHERE show_id = s.show_id
               ) AS user_avg_rating,
               (
                   SELECT COUNT(*) FROM reviews WHERE show_id = s.show_id
               ) AS review_count
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


def calculate_consolidated_rating(
    tmdb_rating: float | None,
    user_rating: float | None,
    user_count: int,
    confidence: float = 5.0
) -> float | None:
    """
    Calculate consolidated rating using Bayesian average.
    
    Combines TMDb rating and user ratings into a single rating that:
    - Uses TMDb rating for titles with no user reviews
    - Smoothly transitions to user-weighted rating as review count increases
    - Prevents titles with few reviews from dominating rankings
    - Treats 0.0 TMDb rating as invalid (unreleased/unrated) and uses user rating directly
    
    Args:
        tmdb_rating: TMDb vote average (0-10 scale)
        user_rating: Average user rating from reviews (0-10 scale)
        user_count: Number of user reviews
        confidence: Confidence constant - how many reviews worth of weight to give TMDb (default 5)
    
    Returns:
        Consolidated rating (0-10 scale) or None if both ratings unavailable
    """
    # Treat 0.0 TMDb rating as invalid/unavailable (unreleased movies/shows)
    if tmdb_rating is None or tmdb_rating == 0.0:
        return user_rating if user_count > 0 else None
    
    if user_count == 0:
        return tmdb_rating
    
    if user_rating is None:
        return tmdb_rating
    
    # Bayesian average: (C * tmdb_rating + user_rating * user_count) / (C + user_count)
    numerator = (confidence * tmdb_rating) + (user_rating * user_count)
    denominator = confidence + user_count
    return numerator / denominator


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


def _ensure_review_reactions_table() -> None:
    """
    Ensure the review_reactions table exists.
    Creates it if it doesn't exist (for databases created before this feature).
    """
    conn = get_db()
    try:
        # Check if table exists
        conn.execute("SELECT 1 FROM review_reactions LIMIT 1")
    except Exception:
        # Table doesn't exist, create it
        conn.execute("""
            CREATE TABLE IF NOT EXISTS review_reactions (
                reaction_id     INTEGER PRIMARY KEY,
                review_id       INTEGER NOT NULL REFERENCES reviews(review_id) ON DELETE CASCADE,
                user_id         INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                emote_type      TEXT NOT NULL CHECK (emote_type IN ('👍', '❤️', '😂', '😮', '😢', '🔥')),
                created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (review_id, user_id, emote_type)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_review_reactions_review ON review_reactions(review_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_review_reactions_user ON review_reactions(user_id)")
        conn.commit()


def _ensure_auth_bootstrap() -> None:
    """
    Make sure the users table has password columns, is_admin column, display_name column, and seed demo credentials.

    We add `password_hash`, `password_plain`, `is_admin`, and `display_name` columns on the fly for older
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
    if "is_admin" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0")
        altered = True
    if "display_name" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN display_name TEXT")
        altered = True
    if altered:
        conn.commit()

    admin_email = "Admin@Test.com"
    admin_password = "Admin"
    admin_hash = generate_password_hash(admin_password)
    existing_admin = conn.execute(
        "SELECT user_id, password_hash, password_plain FROM users WHERE lower(email) = lower(?) LIMIT 1",
        (admin_email,),
    ).fetchone()
    print(f"[DEBUG BOOTSTRAP] existing_admin: {dict(existing_admin) if existing_admin else None}")
    if existing_admin:
        print(f"[DEBUG BOOTSTRAP] password_hash is None: {existing_admin['password_hash'] is None}")
        print(f"[DEBUG BOOTSTRAP] password_plain is None: {existing_admin['password_plain'] is None}")
        print(f"[DEBUG BOOTSTRAP] password_plain value: '{existing_admin['password_plain']}'")
        # Preserve user-changed passwords; only set defaults if none exist
        if existing_admin["password_hash"] is None and existing_admin["password_plain"] is None:
            print(f"[DEBUG BOOTSTRAP] Resetting admin password to default")
            conn.execute(
                """
                UPDATE users
                SET password_hash = ?, password_plain = ?, is_admin = 1
                WHERE user_id = ?
                """,
                (admin_hash, admin_password, existing_admin["user_id"]),
            )
        else:
            print(f"[DEBUG BOOTSTRAP] NOT resetting admin password - already has credentials")
            conn.execute(
                "UPDATE users SET is_admin = 1 WHERE user_id = ?",
                (existing_admin["user_id"],),
            )
    else:
        conn.execute(
            """
            INSERT INTO users (email, password_hash, password_plain, is_admin)
            VALUES (?, ?, ?, 1)
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
    Requires admin privileges.

    The legacy frontend expects `user`, `email`, and `password` fields.
    The new schema only stores email addresses, so we derive a friendly
    display name from the prefix and return a placeholder password.
    """
    _ensure_auth_bootstrap()
    _require_admin()  # Admin only
    
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
    Returns user dict with user_id, email, and is_admin, or None if not authenticated.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    
    try:
        token = auth_header[7:]  # Remove "Bearer " prefix
        user_id_str, email = token.split(":", 1)
        user_id = int(user_id_str)
        
        rows = query(
            "SELECT user_id, email, is_admin FROM users WHERE user_id = ? AND lower(email) = lower(?)",
            (user_id, email),
        )
        if rows:
            row = dict(rows[0])
            # Handle is_admin: SQLite stores it as INTEGER (0 or 1), convert to boolean
            is_admin_value = row.get("is_admin")
            if is_admin_value is None:
                is_admin_bool = False
            else:
                # Convert to boolean: any non-zero value is True
                is_admin_bool = bool(int(is_admin_value) if is_admin_value else 0)
            return {
                "user_id": row["user_id"],
                "email": row["email"],
                "is_admin": is_admin_bool,
            }
    except (ValueError, IndexError):
        pass
    return None


def _require_admin() -> dict:
    """
    Require that the current user is authenticated and is an admin.
    Returns the user dict if admin, otherwise raises an abort with 403.
    """
    user = _get_current_user()
    if not user:
        from flask import abort
        abort(401, description="Authentication required")
    if not user.get("is_admin"):
        from flask import abort
        abort(403, description="Admin privileges required")
    return user


@app.get("/api/user/settings")
def get_user_settings():
    """
    Get current user's settings.
    Requires authentication via Authorization header.
    """
    try:
        # Debug logging
        auth_header = request.headers.get("Authorization", "")
        print(f"[DEBUG] GET /api/user/settings - Auth header: {auth_header[:50] if len(auth_header) > 50 else auth_header}")
        
        _ensure_auth_bootstrap()  # Ensure is_admin column exists
        user = _get_current_user()
        if not user:
            print(f"[DEBUG] GET /api/user/settings - No user found, returning 401")
            return jsonify({"ok": False, "error": "Authentication required"}), 401
        
        rows = query(
            """
            SELECT user_id, email, created_at, is_admin, display_name
            FROM users
            WHERE user_id = ?
            LIMIT 1
            """,
            (user["user_id"],),
        )
        
        if not rows:
            return jsonify({"ok": False, "error": "User not found"}), 404
        
        row = dict(rows[0])
        # Use display_name if set, otherwise derive from email
        display_name = row.get("display_name")
        if not display_name:
            display_name = row.get("email", "").split("@", 1)[0] if "@" in row.get("email", "") else row.get("email", "")
        
        return jsonify({
            "ok": True,
            "user_id": row["user_id"],
            "email": row["email"],
            "display_name": display_name,
            "created_at": row.get("created_at"),
            "is_admin": bool(row.get("is_admin", 0))
        })
    except Exception as exc:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Error in get_user_settings: {error_trace}")
        # Return 500 with detailed error for debugging
        return jsonify({"ok": False, "error": f"Server error: {str(exc)}", "trace": error_trace}), 500


@app.put("/api/user/settings")
def update_user_settings():
    """
    Update current user's settings (email and/or password).
    Requires authentication via Authorization header.
    
    Expected JSON:
      {
        "current_password": "current",  # required for verification
        "display_name": "John Doe",    # optional (ignored - column doesn't exist)
        "new_email": "new@email.com",   # optional
        "new_password": "newpassword"    # optional
      }
    """
    _ensure_auth_bootstrap()  # Ensure is_admin column exists
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
    
    # Check if any changes are being made
    has_display_name_change = bool(display_name)
    has_email_change = bool(new_email)
    has_password_change = bool(new_password)
    
    if not has_display_name_change and not has_email_change and not has_password_change:
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
        if has_display_name_change:
            if len(display_name) > 50:
                return jsonify({"ok": False, "error": "Display name must be 50 characters or less"}), 400
            updates.append("display_name = ?")
            params.append(display_name)
        
        # Update email if provided
        if has_email_change:
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
        if has_password_change:
            if len(new_password) < 1:
                return jsonify({"ok": False, "error": "Password cannot be empty"}), 400
            new_hash = generate_password_hash(new_password)
            print(f"[DEBUG] Updating password for user_id={user['user_id']}")
            print(f"[DEBUG] New password (plain): {new_password}")
            print(f"[DEBUG] New password hash: {new_hash[:50]}...")
            updates.append("password_hash = ?")
            params.append(new_hash)
            updates.append("password_plain = ?")
            params.append(new_password)
        
        if updates:
            params.append(user["user_id"])
            sql = f"UPDATE users SET {', '.join(updates)} WHERE user_id = ?"
            print(f"[DEBUG] Executing SQL: {sql}")
            print(f"[DEBUG] Params count: {len(params)}, user_id: {user['user_id']}")
            print(f"[DEBUG] Params values (masked): {[p if not isinstance(p, str) or len(p) < 10 else p[:3] + '...' for p in params[:-1]]}")
            try:
                result = conn.execute(sql, tuple(params))
                affected_rows = result.rowcount
                print(f"[DEBUG] UPDATE affected {affected_rows} row(s)")
                conn.commit()
                print(f"[DEBUG] Commit successful")
                
                # Force close connection to ensure changes are flushed to disk
                # This is needed because OneDrive can cause sync issues
                from flask import g
                if "sqlite_conn" in g:
                    g.sqlite_conn.close()
                    del g.sqlite_conn
                
                # Reopen connection and verify
                conn2 = get_db()
                try:
                    db_info = conn2.execute("PRAGMA database_list").fetchone()
                    if db_info:
                        print(f"[DEBUG] Database file: {db_info[2] if len(db_info) > 2 else 'unknown'}")
                except:
                    pass
                
                # Verify with fresh connection
                verify_cursor = conn2.execute(
                    "SELECT password_hash, password_plain FROM users WHERE user_id = ?",
                    (user["user_id"],)
                )
                verify_rows = verify_cursor.fetchall()
                if verify_rows:
                    verify_data = dict(verify_rows[0])
                    print(f"[DEBUG] After update (fresh connection) - password_plain: '{verify_data.get('password_plain')}'")
                    if has_password_change:
                        hash_matches = check_password_hash(verify_data.get('password_hash') or '', new_password)
                        plain_matches = verify_data.get('password_plain') == new_password
                        print(f"[DEBUG] After update - password_hash matches: {hash_matches}")
                        print(f"[DEBUG] After update - password_plain matches: {plain_matches}")
                        if not hash_matches and not plain_matches:
                            print(f"[ERROR] Password update verification FAILED!")
                            print(f"[ERROR] Expected password: '{new_password}'")
                            print(f"[ERROR] Stored password_plain: '{verify_data.get('password_plain')}'")
                else:
                    print(f"[ERROR] Could not verify update - user not found")
            except Exception as update_exc:
                print(f"[ERROR] Exception during UPDATE: {update_exc}")
                import traceback
                print(f"[ERROR] Traceback: {traceback.format_exc()}")
                raise
        
        return jsonify({"ok": True, "message": "Settings updated successfully"})
    
    except sqlite3.IntegrityError:
        conn.rollback()
        return jsonify({"ok": False, "error": "Email already in use"}), 409
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": f"Failed to update settings: {str(exc)}"}), 500


@app.get("/api/user/profile")
def get_user_profile():
    """
    Get current user's profile summary, stats, favorites, and watchlist.
    Requires authentication via Authorization header.
    """
    try:
        _ensure_auth_bootstrap()
        user = _get_current_user()
        if not user:
            return jsonify({"ok": False, "error": "Authentication required"}), 401

        # Basic user info
        rows = query(
            """
            SELECT user_id, email, created_at, display_name
            FROM users
            WHERE user_id = ?
            LIMIT 1
            """,
            (user["user_id"],),
        )
        if not rows:
            return jsonify({"ok": False, "error": "User not found"}), 404
        info = dict(rows[0])
        # Use display_name if set, otherwise derive from email
        display_name = info.get("display_name")
        if not display_name:
            display_name = info.get("email", "").split("@", 1)[0] if "@" in info.get("email", "") else info.get("email", "")

        # Stats: separate movie and TV statistics
        # Movie stats
        movie_stats_rows = query(
            """
            SELECT COUNT(*) AS review_count, AVG(rating) AS avg_rating
            FROM reviews
            WHERE user_id = ? AND movie_id IS NOT NULL
            """,
            (user["user_id"],),
        )
        movie_review_count = int(movie_stats_rows[0]["review_count"] or 0) if movie_stats_rows else 0
        movie_avg_rating = float(movie_stats_rows[0]["avg_rating"] or 0) if movie_stats_rows else 0.0
        # Assume ~2 hours per movie
        movie_hours = movie_review_count * 2
        
        # Movie discussion count
        movie_discussion_rows = query(
            """
            SELECT COUNT(*) AS discussion_count
            FROM discussions
            WHERE user_id = ? AND movie_id IS NOT NULL
            """,
            (user["user_id"],),
        )
        movie_discussion_count = int(movie_discussion_rows[0]["discussion_count"] or 0) if movie_discussion_rows else 0

        # TV stats
        tv_stats_rows = query(
            """
            SELECT COUNT(*) AS review_count, AVG(rating) AS avg_rating
            FROM reviews
            WHERE user_id = ? AND show_id IS NOT NULL
            """,
            (user["user_id"],),
        )
        tv_review_count = int(tv_stats_rows[0]["review_count"] or 0) if tv_stats_rows else 0
        tv_avg_rating = float(tv_stats_rows[0]["avg_rating"] or 0) if tv_stats_rows else 0.0
        # Assume ~2 hours per TV show episode (or per review)
        tv_hours = tv_review_count * 2
        
        # TV discussion count
        tv_discussion_rows = query(
            """
            SELECT COUNT(*) AS discussion_count
            FROM discussions
            WHERE user_id = ? AND show_id IS NOT NULL
            """,
            (user["user_id"],),
        )
        tv_discussion_count = int(tv_discussion_rows[0]["discussion_count"] or 0) if tv_discussion_rows else 0

        # Favorites: items from favorites table with poster images and metadata
        _ensure_favorites_table()  # Ensure table exists
        favorite_rows = query(
            """
            SELECT 
                f.movie_id,
                f.show_id,
                f.added_at,
                m.title AS movie_title,
                m.poster_path AS movie_poster,
                m.tmdb_vote_avg AS movie_vote_average,
                m.original_language AS movie_original_language,
                m.release_year AS movie_release_year,
                s.title AS show_title,
                s.poster_path AS show_poster,
                s.tmdb_vote_avg AS show_vote_average,
                s.original_language AS show_original_language,
                s.first_air_date AS show_first_air_date
            FROM favorites f
            LEFT JOIN movies m ON f.movie_id = m.movie_id
            LEFT JOIN shows s ON f.show_id = s.show_id
            WHERE f.user_id = ?
            ORDER BY f.added_at DESC
            """,
            (user["user_id"],),
        )
        favorites: list[dict[str, object]] = []
        for row in favorite_rows:
            data = dict(row)
            is_movie = data.get("movie_id") is not None
            title = data.get("movie_title") or data.get("show_title") or "Untitled"
            poster_path = data.get("movie_poster") or data.get("show_poster")
            media_type = "movie" if is_movie else "tv"
            
            # Get vote_average
            vote_average = data.get("movie_vote_average") if is_movie else data.get("show_vote_average")
            
            # Get original_language
            original_language = data.get("movie_original_language") if is_movie else data.get("show_original_language")
            
            # Get release_date
            release_date = None
            if is_movie:
                release_year = data.get("movie_release_year")
                if release_year:
                    release_date = str(release_year)
            else:
                first_air_date = data.get("show_first_air_date")
                if first_air_date:
                    # Extract year from first_air_date (format: YYYY-MM-DD or just year)
                    if isinstance(first_air_date, str) and len(first_air_date) >= 4:
                        release_date = first_air_date[:4]
                    elif isinstance(first_air_date, (int, float)):
                        release_date = str(int(first_air_date))
            
            favorites.append(
                {
                    "title": title,
                    "media_type": media_type,
                    "id": data.get("movie_id") if is_movie else data.get("show_id"),
                    "poster_path": poster_path,
                    "vote_average": float(vote_average) if vote_average is not None else None,
                    "original_language": original_language,
                    "release_date": release_date,
                }
            )

        # Watchlist items with poster images and metadata
        watchlist_rows = query(
            """
            SELECT
                w.user_id,
                w.movie_id,
                w.show_id,
                w.added_at,
                m.title AS movie_title,
                m.poster_path AS movie_poster,
                m.tmdb_vote_avg AS movie_vote_average,
                m.original_language AS movie_original_language,
                m.release_year AS movie_release_year,
                s.title AS show_title,
                s.poster_path AS show_poster,
                s.tmdb_vote_avg AS show_vote_average,
                s.original_language AS show_original_language,
                s.first_air_date AS show_first_air_date
            FROM watchlists w
            LEFT JOIN movies m ON w.movie_id = m.movie_id
            LEFT JOIN shows s ON w.show_id = s.show_id
            WHERE w.user_id = ?
            ORDER BY w.added_at DESC
            """,
            (user["user_id"],),
        )
        watchlist: list[dict[str, object]] = []
        for row in watchlist_rows:
            data = dict(row)
            is_movie = data.get("movie_id") is not None
            title = data.get("movie_title") or data.get("show_title") or "Untitled"
            poster_path = data.get("movie_poster") or data.get("show_poster")
            media_type = "movie" if is_movie else "tv"
            
            # Get vote_average
            vote_average = data.get("movie_vote_average") if is_movie else data.get("show_vote_average")
            
            # Get original_language
            original_language = data.get("movie_original_language") if is_movie else data.get("show_original_language")
            
            # Get release_date
            release_date = None
            if is_movie:
                release_year = data.get("movie_release_year")
                if release_year:
                    release_date = str(release_year)
            else:
                first_air_date = data.get("show_first_air_date")
                if first_air_date:
                    # Extract year from first_air_date (format: YYYY-MM-DD or just year)
                    if isinstance(first_air_date, str) and len(first_air_date) >= 4:
                        release_date = first_air_date[:4]
                    elif isinstance(first_air_date, (int, float)):
                        release_date = str(int(first_air_date))
            
            watchlist.append(
                {
                    "title": title,
                    "media_type": media_type,
                    "id": data.get("movie_id") if is_movie else data.get("show_id"),
                    "added_at": data.get("added_at"),
                    "poster_path": poster_path,
                    "vote_average": float(vote_average) if vote_average is not None else None,
                    "original_language": original_language,
                    "release_date": release_date,
                }
            )

        # Separate favorites and watchlist by type
        movie_favorites = [f for f in favorites if f["media_type"] == "movie"]
        tv_favorites = [f for f in favorites if f["media_type"] == "tv"]
        movie_watchlist = [w for w in watchlist if w["media_type"] == "movie"]
        tv_watchlist = [w for w in watchlist if w["media_type"] == "tv"]

        # Recent reviews (last 10)
        recent_review_rows = query(
            """
            SELECT 
                r.review_id,
                r.rating,
                r.content,
                r.created_at,
                r.movie_id,
                r.show_id,
                m.title AS movie_title,
                m.poster_path AS movie_poster,
                s.title AS show_title,
                s.poster_path AS show_poster
            FROM reviews r
            LEFT JOIN movies m ON r.movie_id = m.movie_id
            LEFT JOIN shows s ON r.show_id = s.show_id
            WHERE r.user_id = ?
            ORDER BY r.created_at DESC
            LIMIT 10
            """,
            (user["user_id"],),
        )
        recent_reviews = []
        for row in recent_review_rows:
            data = dict(row)
            title = data.get("movie_title") or data.get("show_title") or "Untitled"
            poster_path = data.get("movie_poster") or data.get("show_poster")
            media_type = "movie" if data.get("movie_id") is not None else "tv"
            recent_reviews.append({
                "review_id": data["review_id"],
                "title": title,
                "media_type": media_type,
                "id": data.get("movie_id") if media_type == "movie" else data.get("show_id"),
                "rating": float(data["rating"]) if data.get("rating") is not None else None,
                "content": data.get("content"),
                "created_at": data.get("created_at"),
                "poster_path": poster_path,
            })

        return jsonify(
            {
                "ok": True,
                "user": {
                    "user_id": info.get("user_id"),
                    "email": info.get("email"),
                    "display_name": display_name,
                    "created_at": info.get("created_at"),
                },
                "stats": {
                    "movies": {
                        "review_count": movie_review_count,
                        "avg_rating": movie_avg_rating,
                        "estimated_hours": movie_hours,
                        "discussion_count": movie_discussion_count,
                    },
                    "tv": {
                        "review_count": tv_review_count,
                        "avg_rating": tv_avg_rating,
                        "estimated_hours": tv_hours,
                        "discussion_count": tv_discussion_count,
                    },
                },
                "favorites": {
                    "movies": movie_favorites,
                    "tv": tv_favorites,
                },
                "watchlist": {
                    "movies": movie_watchlist,
                    "tv": tv_watchlist,
                },
                "recent_reviews": recent_reviews,
            }
        )
    except Exception as exc:
        import traceback
        print(f"Error in get_user_profile: {traceback.format_exc()}")
        return jsonify({"ok": False, "error": f"Server error: {str(exc)}"}), 500


@app.get("/api/users/<int:user_id>/public-profile")
def get_public_user_profile(user_id: int):
    """
    Get a user's public profile (viewable by anyone).
    Shows: display name, join date, stats, favorites, recent reviews, and watchlist.
    Does NOT show: email or other private data.
    """
    try:
        # Get basic user info
        rows = query(
            """
            SELECT user_id, email, display_name, created_at
            FROM users
            WHERE user_id = ?
            LIMIT 1
            """,
            (user_id,),
        )
        if not rows:
            return jsonify({"ok": False, "error": "User not found"}), 404
        
        info = dict(rows[0])
        # Use display_name if set, otherwise derive from email
        if info.get("display_name"):
            display_name = info["display_name"]
        elif info.get("email") and "@" in info["email"]:
            display_name = info["email"].split("@", 1)[0]
        else:
            display_name = f"User {user_id}"

        # Stats: review counts and averages
        movie_stats = query(
            """
            SELECT COUNT(*) AS review_count, AVG(rating) AS avg_rating
            FROM reviews
            WHERE user_id = ? AND movie_id IS NOT NULL
            """,
            (user_id,),
        )
        movie_review_count = int(movie_stats[0]["review_count"] or 0) if movie_stats else 0
        movie_avg_rating = round(float(movie_stats[0]["avg_rating"] or 0), 1) if movie_stats and movie_stats[0]["avg_rating"] else None

        tv_stats = query(
            """
            SELECT COUNT(*) AS review_count, AVG(rating) AS avg_rating
            FROM reviews
            WHERE user_id = ? AND show_id IS NOT NULL
            """,
            (user_id,),
        )
        tv_review_count = int(tv_stats[0]["review_count"] or 0) if tv_stats else 0
        tv_avg_rating = round(float(tv_stats[0]["avg_rating"] or 0), 1) if tv_stats and tv_stats[0]["avg_rating"] else None

        total_reviews = movie_review_count + tv_review_count
        
        # Calculate overall average rating
        overall_avg = None
        if total_reviews > 0:
            all_ratings = query(
                """
                SELECT AVG(rating) AS avg_rating
                FROM reviews
                WHERE user_id = ? AND rating IS NOT NULL
                """,
                (user_id,),
            )
            if all_ratings and all_ratings[0]["avg_rating"]:
                overall_avg = round(float(all_ratings[0]["avg_rating"]), 1)

        # Favorites: items from favorites table with poster images and metadata
        _ensure_favorites_table()  # Ensure table exists
        favorite_rows = query(
            """
            SELECT 
                f.movie_id,
                f.show_id,
                f.added_at,
                m.title AS movie_title,
                m.poster_path AS movie_poster,
                m.tmdb_vote_avg AS movie_vote_average,
                m.original_language AS movie_original_language,
                m.release_year AS movie_release_year,
                s.title AS show_title,
                s.poster_path AS show_poster,
                s.tmdb_vote_avg AS show_vote_average,
                s.original_language AS show_original_language,
                s.first_air_date AS show_first_air_date
            FROM favorites f
            LEFT JOIN movies m ON f.movie_id = m.movie_id
            LEFT JOIN shows s ON f.show_id = s.show_id
            WHERE f.user_id = ?
            ORDER BY f.added_at DESC
            """,
            (user_id,),
        )
        favorites = []
        for row in favorite_rows:
            data = dict(row)
            is_movie = data.get("movie_id") is not None
            title = data.get("movie_title") or data.get("show_title") or "Untitled"
            poster_path = data.get("movie_poster") or data.get("show_poster")
            media_type = "movie" if is_movie else "tv"
            
            # Get vote_average
            vote_average = data.get("movie_vote_average") if is_movie else data.get("show_vote_average")
            
            # Get original_language
            original_language = data.get("movie_original_language") if is_movie else data.get("show_original_language")
            
            # Get release_date
            release_date = None
            if is_movie:
                release_year = data.get("movie_release_year")
                if release_year:
                    release_date = str(release_year)
            else:
                first_air_date = data.get("show_first_air_date")
                if first_air_date:
                    # Extract year from first_air_date (format: YYYY-MM-DD or just year)
                    if isinstance(first_air_date, str) and len(first_air_date) >= 4:
                        release_date = first_air_date[:4]
                    elif isinstance(first_air_date, (int, float)):
                        release_date = str(int(first_air_date))
            
            favorites.append({
                "title": title,
                "media_type": media_type,
                "id": data.get("movie_id") if is_movie else data.get("show_id"),
                "poster_path": poster_path,
                "vote_average": float(vote_average) if vote_average is not None else None,
                "original_language": original_language,
                "release_date": release_date,
            })

        # Recent reviews (last 10)
        recent_reviews = query(
            """
            SELECT 
                r.review_id,
                r.rating,
                r.content,
                r.created_at,
                r.movie_id,
                r.show_id,
                m.title AS movie_title,
                m.poster_path AS movie_poster,
                s.title AS show_title,
                s.poster_path AS show_poster
            FROM reviews r
            LEFT JOIN movies m ON r.movie_id = m.movie_id
            LEFT JOIN shows s ON r.show_id = s.show_id
            WHERE r.user_id = ?
            ORDER BY r.created_at DESC
            LIMIT 10
            """,
            (user_id,),
        )
        reviews = []
        for row in recent_reviews:
            data = dict(row)
            title = data.get("movie_title") or data.get("show_title") or "Untitled"
            poster_path = data.get("movie_poster") or data.get("show_poster")
            media_type = "movie" if data.get("movie_id") is not None else "tv"
            reviews.append({
                "review_id": data["review_id"],
                "title": title,
                "media_type": media_type,
                "id": data.get("movie_id") if media_type == "movie" else data.get("show_id"),
                "rating": float(data["rating"]) if data.get("rating") is not None else None,
                "content": data.get("content"),
                "created_at": data.get("created_at"),
                "poster_path": poster_path,
            })

        # Watchlist items with poster images and metadata
        watchlist_rows = query(
            """
            SELECT
                w.user_id,
                w.movie_id,
                w.show_id,
                w.added_at,
                m.title AS movie_title,
                m.poster_path AS movie_poster,
                m.tmdb_vote_avg AS movie_vote_average,
                m.original_language AS movie_original_language,
                m.release_year AS movie_release_year,
                s.title AS show_title,
                s.poster_path AS show_poster,
                s.tmdb_vote_avg AS show_vote_average,
                s.original_language AS show_original_language,
                s.first_air_date AS show_first_air_date
            FROM watchlists w
            LEFT JOIN movies m ON w.movie_id = m.movie_id
            LEFT JOIN shows s ON w.show_id = s.show_id
            WHERE w.user_id = ?
            ORDER BY w.added_at DESC
            """,
            (user_id,),
        )
        watchlist = []
        for row in watchlist_rows:
            data = dict(row)
            is_movie = data.get("movie_id") is not None
            title = data.get("movie_title") or data.get("show_title") or "Untitled"
            poster_path = data.get("movie_poster") or data.get("show_poster")
            media_type = "movie" if is_movie else "tv"
            
            # Get vote_average
            vote_average = data.get("movie_vote_average") if is_movie else data.get("show_vote_average")
            
            # Get original_language
            original_language = data.get("movie_original_language") if is_movie else data.get("show_original_language")
            
            # Get release_date
            release_date = None
            if is_movie:
                release_year = data.get("movie_release_year")
                if release_year:
                    release_date = str(release_year)
            else:
                first_air_date = data.get("show_first_air_date")
                if first_air_date:
                    # Extract year from first_air_date (format: YYYY-MM-DD or just year)
                    if isinstance(first_air_date, str) and len(first_air_date) >= 4:
                        release_date = first_air_date[:4]
                    elif isinstance(first_air_date, (int, float)):
                        release_date = str(int(first_air_date))
            
            watchlist.append({
                "title": title,
                "media_type": media_type,
                "id": data.get("movie_id") if is_movie else data.get("show_id"),
                "added_at": data.get("added_at"),
                "poster_path": poster_path,
                "vote_average": float(vote_average) if vote_average is not None else None,
                "original_language": original_language,
                "release_date": release_date,
            })

        # Separate watchlist by type
        movie_watchlist = [w for w in watchlist if w["media_type"] == "movie"]
        tv_watchlist = [w for w in watchlist if w["media_type"] == "tv"]

        return jsonify({
            "ok": True,
            "user": {
                "user_id": user_id,
                "display_name": display_name,
                "created_at": info.get("created_at"),
            },
            "stats": {
                "total_reviews": total_reviews,
                "movie_reviews": movie_review_count,
                "tv_reviews": tv_review_count,
                "avg_rating": overall_avg,
                "movie_avg_rating": movie_avg_rating,
                "tv_avg_rating": tv_avg_rating,
            },
            "favorites": favorites,
            "recent_reviews": reviews,
            "watchlist": {
                "movies": movie_watchlist,
                "tv": tv_watchlist,
            },
        })
    except Exception as exc:
        import traceback
        print(f"Error in get_public_user_profile: {traceback.format_exc()}")
        return jsonify({"ok": False, "error": f"Server error: {str(exc)}"}), 500


@app.delete("/api/user/account")
def delete_user_account():
    """
    Delete the current user's account and all associated data.
    Requires authentication via Authorization header and password confirmation.
    
    Expected JSON:
      {
        "password": "user_password"  # required for confirmation
      }
    """
    try:
        _ensure_auth_bootstrap()
        user = _get_current_user()
        if not user:
            return jsonify({"ok": False, "error": "Authentication required"}), 401
        
        payload = request.get_json(silent=True) or {}
        password = (payload.get("password") or "").strip()
        
        if not password:
            return jsonify({"ok": False, "error": "Password is required for account deletion"}), 400
        
        conn = get_db()
        
        # Verify password
        rows = query(
            "SELECT user_id, password_hash, password_plain FROM users WHERE user_id = ?",
            (user["user_id"],),
        )
        
        if not rows:
            return jsonify({"ok": False, "error": "User not found"}), 404
        
        user_row = dict(rows[0])
        stored_hash = user_row.get("password_hash")
        stored_plain = user_row.get("password_plain")
        
        # Verify password
        verified = False
        if stored_hash:
            try:
                verified = check_password_hash(stored_hash, password)
            except ValueError:
                verified = False
        if not verified and stored_plain is not None:
            verified = stored_plain == password
        
        if not verified:
            return jsonify({"ok": False, "error": "Password is incorrect"}), 401
        
        # Delete user account (CASCADE will handle related data: reviews, watchlists, discussions, comments)
        # Foreign key constraints with ON DELETE CASCADE will automatically delete:
        # - reviews (user_id)
        # - watchlists (user_id)
        # - discussions (user_id) -> which will cascade delete comments
        conn.execute("DELETE FROM users WHERE user_id = ?", (user["user_id"],))
        conn.commit()
        
        return jsonify({"ok": True, "message": "Account deleted successfully"})
    except Exception as exc:
        import traceback
        print(f"Error in delete_user_account: {traceback.format_exc()}")
        conn.rollback()
        return jsonify({"ok": False, "error": f"Server error: {str(exc)}"}), 500


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
        # Use username as display_name if provided, otherwise derive from email
        display_name = username if username else None
        conn.execute(
            """
            INSERT INTO users (email, password_hash, password_plain, display_name)
            VALUES (?, ?, ?, ?)
            """,
            (email, hashed, password, display_name),
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
    # Force fresh connection BEFORE bootstrap to ensure we're reading latest data
    from flask import g as flask_g
    if "sqlite_conn" in flask_g:
        try:
            flask_g.sqlite_conn.close()
        except:
            pass
        del flask_g.sqlite_conn
    
    _ensure_auth_bootstrap()
    payload = request.get_json(silent=True) or {}
    email = (payload.get("email") or "").strip()
    password = (payload.get("password") or "").strip()

    if not email or not password:
        return jsonify({"ok": False, "error": "Missing email or password"}), 400

    # Use the same connection that _ensure_auth_bootstrap() created
    conn = get_db()
    # Force checkpoint if in WAL mode to ensure we see latest commits
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except:
        pass
    
    login_cursor = conn.execute(
        """
        SELECT user_id, email, password_hash, password_plain, is_admin
        FROM users
        WHERE lower(email) = lower(?)
        LIMIT 1
        """,
        (email,),
    )
    rows = login_cursor.fetchall()
    if not rows:
        return jsonify({"ok": False, "error": "Invalid credentials"}), 401

    record = dict(rows[0])
    stored_hash = record.get("password_hash")
    stored_plain = record.get("password_plain")

    print(f"[DEBUG LOGIN] Attempting login for email: {email}")
    print(f"[DEBUG LOGIN] User ID: {record.get('user_id')}")
    print(f"[DEBUG LOGIN] Has stored_hash: {bool(stored_hash)}")
    print(f"[DEBUG LOGIN] Has stored_plain: {bool(stored_plain)}")
    print(f"[DEBUG LOGIN] Password provided length: {len(password)}")
    print(f"[DEBUG LOGIN] Stored password_plain value: '{stored_plain}'")

    verified = False
    if stored_hash:
        try:
            verified = check_password_hash(stored_hash, password)
            print(f"[DEBUG LOGIN] Hash verification result: {verified}")
        except ValueError as e:
            print(f"[DEBUG LOGIN] Hash verification exception: {e}")
            verified = False
    if not verified and stored_plain is not None:
        plain_match = stored_plain == password
        print(f"[DEBUG LOGIN] Plain password check: stored='{stored_plain}', provided='{password}', match={plain_match}")
        verified = plain_match

    print(f"[DEBUG LOGIN] Final verification result: {verified}")
    if not verified:
        return jsonify({"ok": False, "error": "Invalid credentials"}), 401

    display_name = email.split("@", 1)[0] if "@" in email else email
    return jsonify({
        "ok": True,
        "user": display_name,
        "email": record["email"],
        "user_id": record["user_id"],
        "is_admin": bool(record.get("is_admin", 0))
    })


def _list_media(media_type: str, sort: str, page: int, limit: int, genre: str | None = None, language: str | None = None) -> dict[str, Any]:
    table = "movies" if media_type == "movie" else "shows"
    id_col = "movie_id" if media_type == "movie" else "show_id"
    # For movies, prefer release_date (full date) over release_year, fallback to release_year
    # For shows, use first_air_date (already full date)
    if media_type == "movie":
        release_col = "COALESCE(t.release_date, CAST(t.release_year AS TEXT))"
    else:
        release_col = "t.first_air_date"
    offset = (page - 1) * limit
    genre_table = "movie_genres" if media_type == "movie" else "show_genres"
    
    # Build WHERE clause
    where_conditions = ["t.overview IS NOT NULL", "t.overview != ''"]
    params_count = []
    params_rows = []
    
    # Add genre filter (case-insensitive)
    if genre and genre.strip() and genre.strip().lower() != "all":
        genre_value = genre.strip()
        where_conditions.append("LOWER(g.name) = LOWER(?)")
        params_count.append(genre_value)
        params_rows.append(genre_value)
    
    # Add language filter (case-insensitive)
    if language and language.strip() and language.strip().lower() != "all":
        language_value = language.strip()
        # Ensure original_language is not NULL/empty and matches (case-insensitive)
        where_conditions.append("t.original_language IS NOT NULL AND t.original_language != '' AND LOWER(TRIM(t.original_language)) = LOWER(TRIM(?))")
        params_count.append(language_value)
        params_rows.append(language_value)
    
    where_clause = " AND ".join(where_conditions)
    
    # Determine order clause
    if sort == "rating":
        order_clause = f"(t.tmdb_vote_avg IS NULL), t.tmdb_vote_avg DESC, t.title"
    elif sort == "title":
        order_clause = "t.title ASC"
    elif sort == "release_date":
        # Use full date for proper chronological sorting (newest first)
        order_clause = f"({release_col} IS NULL), {release_col} DESC, t.title"
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

    # Determine review table join based on media type
    review_table = "reviews"
    review_id_col = "movie_id" if media_type == "movie" else "show_id"
    
    # For SELECT, we need the actual column names, not the COALESCE expression
    if media_type == "movie":
        release_select = "COALESCE(t.release_date, CAST(t.release_year AS TEXT)) AS release_value"
    else:
        release_select = "t.first_air_date AS release_value"
    
    rows = query(
        f"""
        SELECT DISTINCT t.{id_col} AS record_id,
               t.tmdb_id,
               t.title,
               t.overview,
               t.poster_path,
               t.tmdb_vote_avg,
               t.popularity,
               {release_select},
               t.original_language,
               (
                   SELECT AVG(rating) FROM {review_table} WHERE {review_id_col} = t.{id_col}
               ) AS user_avg_rating,
               (
                   SELECT COUNT(*) FROM {review_table} WHERE {review_id_col} = t.{id_col}
               ) AS review_count
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
        
        # Calculate consolidated rating
        tmdb_rating = data.get("tmdb_vote_avg")
        user_rating = float(data["user_avg_rating"]) if data.get("user_avg_rating") is not None else None
        review_count = data.get("review_count") or 0
        consolidated = calculate_consolidated_rating(
            tmdb_rating=tmdb_rating,
            user_rating=user_rating,
            user_count=review_count,
            confidence=5.0
        )
        
        result = {
            "media_type": media_type,
            "id": data["record_id"],
            "tmdb_id": data["tmdb_id"],
            "title": data["title"],
            "overview": data.get("overview") or "",
            "poster_path": data.get("poster_path"),
            "backdrop_path": None,
            "vote_average": tmdb_rating,
            "consolidated_rating": round(consolidated, 2) if consolidated is not None else None,
            "popularity": data.get("popularity"),
            "release_date": release_value,
            "original_language": data.get("original_language"),
            "genres": [],
        }
        if user_rating is not None:
            result["user_avg_rating"] = round(user_rating, 2)
        if review_count > 0:
            result["review_count"] = review_count
        results.append(result)

    return {"total": total, "page": page, "results": results}


@app.get("/api/summary")
def summary():
    return jsonify(_summary_payload())


@app.get("/api/kpis")
def get_precomputed_kpis():
    """
    Get precomputed KPIs for analytics dashboards.
    Optional query param: category (user_activity, review_trends, title_stats, genre_stats, platform_stats)
    """
    category = request.args.get("category")
    
    try:
        if category:
            rows = query(
                """
                SELECT category, kpi_name, kpi_value, computed_at
                FROM precomputed_kpis
                WHERE category = ?
                ORDER BY kpi_name
                """,
                (category,)
            )
        else:
            rows = query(
                """
                SELECT category, kpi_name, kpi_value, computed_at
                FROM precomputed_kpis
                ORDER BY category, kpi_name
                """
            )
        
        result = {}
        for row in rows:
            cat = row["category"]
            if cat not in result:
                result[cat] = {}
            
            # Parse JSON values
            import json
            try:
                value = json.loads(row["kpi_value"])
            except (json.JSONDecodeError, TypeError):
                value = row["kpi_value"]
            
            result[cat][row["kpi_name"]] = {
                "value": value,
                "computed_at": row["computed_at"]
            }
        
        return jsonify({
            "ok": True,
            "kpis": result
        })
    except Exception as e:
        # Table might not exist yet
        return jsonify({
            "ok": True,
            "kpis": {},
            "message": "No precomputed KPIs available yet. Run the ETL scheduler to generate them."
        })


@app.post("/api/kpis/refresh")
def refresh_kpis():
    """
    Manually trigger KPI recomputation.
    Requires admin privileges.
    """
    _ensure_auth_bootstrap()
    _require_admin()
    
    try:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent))
        
        from etl.kpi_service import KPIService
        import yaml
        
        # Load config
        config_path = Path(__file__).parent.parent / "etl_config.yaml"
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
        else:
            config = {}
        
        # Run KPI computation
        kpi_service = KPIService(config)
        stats = kpi_service.run_kpi_computation()
        
        return jsonify({
            "ok": True,
            "message": "KPI refresh completed",
            "stats": stats
        })
    except Exception as e:
        import traceback
        print(f"KPI refresh error: {traceback.format_exc()}")
        return jsonify({
            "ok": False,
            "error": f"KPI refresh failed: {str(e)}"
        }), 500


# ============================================================================
# Analytics Endpoints
# ============================================================================

@app.get("/api/analytics/top-movies")
def analytics_top_movies():
    """
    Get top rated movies by consolidated rating (Q5).
    Uses consolidated rating (combines TMDb + user ratings).
    Query param: limit (default 5, max 50)
    """
    limit = _get_int(request.args.get("limit"), 5, 1, 50)
    
    rows = query(
        """
        SELECT 
            m.movie_id,
            m.tmdb_id,
            m.title,
            m.overview,
            m.poster_path,
            m.tmdb_vote_avg,
            m.popularity,
            m.release_year,
            m.original_language,
            COALESCE(AVG(r.rating), 0) AS user_avg_rating,
            COUNT(r.review_id) AS review_count,
            CASE 
                WHEN COUNT(r.review_id) = 0 THEN m.tmdb_vote_avg
                ELSE (5.0 * COALESCE(m.tmdb_vote_avg, 5.0) + 
                      AVG(r.rating) * COUNT(r.review_id)) / 
                     (5.0 + COUNT(r.review_id))
            END AS consolidated_rating
        FROM movies m
        LEFT JOIN reviews r ON r.movie_id = m.movie_id
        WHERE m.tmdb_vote_avg IS NOT NULL AND m.tmdb_vote_avg > 0
        GROUP BY m.movie_id
        HAVING consolidated_rating IS NOT NULL AND consolidated_rating > 0
        ORDER BY consolidated_rating DESC, m.popularity DESC
        LIMIT ?
        """,
        (limit,),
    )
    
    results = []
    for row in rows:
        data = dict(row)
        tmdb_rating = data.get("tmdb_vote_avg")
        user_rating = float(data["user_avg_rating"]) if data.get("user_avg_rating") is not None and data["user_avg_rating"] > 0 else None
        review_count = data.get("review_count") or 0
        consolidated = calculate_consolidated_rating(
            tmdb_rating=tmdb_rating,
            user_rating=user_rating,
            user_count=review_count,
            confidence=5.0
        )
        
        # Get genres
        genre_rows = query(
            """
            SELECT g.name
            FROM movie_genres mg
            JOIN genres g ON g.genre_id = mg.genre_id
            WHERE mg.movie_id = ?
            ORDER BY g.name
            """,
            (data["movie_id"],),
        )
        
        result = {
            "media_type": "movie",
            "id": data["movie_id"],
            "tmdb_id": data["tmdb_id"],
            "title": data["title"],
            "overview": data.get("overview") or "",
            "poster_path": data.get("poster_path"),
            "backdrop_path": None,
            "vote_average": tmdb_rating,
            "consolidated_rating": round(consolidated, 2) if consolidated is not None else None,
            "popularity": data.get("popularity"),
            "release_date": str(data["release_year"]) if data.get("release_year") else None,
            "original_language": data.get("original_language"),
            "genres": [g["name"] for g in genre_rows],
        }
        if user_rating is not None:
            result["user_avg_rating"] = round(user_rating, 2)
        if review_count > 0:
            result["review_count"] = review_count
        results.append(result)
    
    return jsonify({"results": results})


@app.get("/api/analytics/top-shows")
def analytics_top_shows():
    """
    Get top rated shows by consolidated rating (Q6).
    Uses consolidated rating (combines TMDb + user ratings).
    Query param: limit (default 5, max 50)
    """
    limit = _get_int(request.args.get("limit"), 5, 1, 50)
    
    rows = query(
        """
        SELECT 
            s.show_id,
            s.tmdb_id,
            s.title,
            s.overview,
            s.poster_path,
            s.tmdb_vote_avg,
            s.popularity,
            s.first_air_date,
            s.original_language,
            COALESCE(AVG(r.rating), 0) AS user_avg_rating,
            COUNT(r.review_id) AS review_count,
            CASE 
                WHEN COUNT(r.review_id) = 0 THEN s.tmdb_vote_avg
                ELSE (5.0 * COALESCE(s.tmdb_vote_avg, 5.0) + 
                      AVG(r.rating) * COUNT(r.review_id)) / 
                     (5.0 + COUNT(r.review_id))
            END AS consolidated_rating
        FROM shows s
        LEFT JOIN reviews r ON r.show_id = s.show_id
        WHERE s.tmdb_vote_avg IS NOT NULL AND s.tmdb_vote_avg > 0
        GROUP BY s.show_id
        HAVING consolidated_rating IS NOT NULL AND consolidated_rating > 0
        ORDER BY consolidated_rating DESC, s.popularity DESC
        LIMIT ?
        """,
        (limit,),
    )
    
    results = []
    for row in rows:
        data = dict(row)
        tmdb_rating = data.get("tmdb_vote_avg")
        user_rating = float(data["user_avg_rating"]) if data.get("user_avg_rating") is not None and data["user_avg_rating"] > 0 else None
        review_count = data.get("review_count") or 0
        consolidated = calculate_consolidated_rating(
            tmdb_rating=tmdb_rating,
            user_rating=user_rating,
            user_count=review_count,
            confidence=5.0
        )
        
        # Get genres
        genre_rows = query(
            """
            SELECT g.name
            FROM show_genres sg
            JOIN genres g ON g.genre_id = sg.genre_id
            WHERE sg.show_id = ?
            ORDER BY g.name
            """,
            (data["show_id"],),
        )
        
        result = {
            "media_type": "tv",
            "id": data["show_id"],
            "tmdb_id": data["tmdb_id"],
            "title": data["title"],
            "overview": data.get("overview") or "",
            "poster_path": data.get("poster_path"),
            "backdrop_path": None,
            "vote_average": tmdb_rating,
            "consolidated_rating": round(consolidated, 2) if consolidated is not None else None,
            "popularity": data.get("popularity"),
            "release_date": data.get("first_air_date"),
            "original_language": data.get("original_language"),
            "genres": [g["name"] for g in genre_rows],
        }
        if user_rating is not None:
            result["user_avg_rating"] = round(user_rating, 2)
        if review_count > 0:
            result["review_count"] = review_count
        results.append(result)
    
    return jsonify({"results": results})


@app.get("/api/analytics/genre-distribution")
def analytics_genre_distribution():
    """
    Get genre distribution for movies or shows (Q7, Q8).
    Query param: type ('movie' or 'show', default 'movie')
    """
    media_type = (request.args.get("type") or "movie").lower()
    if media_type not in {"movie", "show"}:
        return jsonify({"error": "type must be 'movie' or 'show'"}), 400
    
    if media_type == "movie":
        rows = query(
            """
            SELECT g.name,
                   COUNT(mg.movie_id) AS count
            FROM genres g
            LEFT JOIN movie_genres mg ON mg.genre_id = g.genre_id
            GROUP BY g.genre_id
            ORDER BY count DESC, g.name
            """
        )
    else:
        rows = query(
            """
            SELECT g.name,
                   COUNT(sg.show_id) AS count
            FROM genres g
            LEFT JOIN show_genres sg ON sg.genre_id = g.genre_id
            GROUP BY g.genre_id
            ORDER BY count DESC, g.name
            """
        )
    
    results = [
        {"name": row["name"], "count": row["count"]}
        for row in rows
    ]
    
    return jsonify({"results": results})


@app.get("/api/analytics/popular-watchlists")
def analytics_popular_watchlists():
    """
    Get most watchlisted titles (Q29).
    Query param: limit (default 10, max 50)
    """
    limit = _get_int(request.args.get("limit"), 10, 1, 50)
    
    rows = query(
        """
        SELECT target_type, target_id, target_title, poster_path, watchlist_count
        FROM (
            SELECT 'movie' AS target_type,
                   m.movie_id AS target_id,
                   m.title AS target_title,
                   m.poster_path,
                   COUNT(*) AS watchlist_count
            FROM watchlists w
            JOIN movies m ON m.movie_id = w.movie_id
            WHERE w.movie_id IS NOT NULL
            GROUP BY m.movie_id
            UNION ALL
            SELECT 'show' AS target_type,
                   s.show_id AS target_id,
                   s.title AS target_title,
                   s.poster_path,
                   COUNT(*) AS watchlist_count
            FROM watchlists w
            JOIN shows s ON s.show_id = w.show_id
            WHERE w.show_id IS NOT NULL
            GROUP BY s.show_id
        ) AS listing
        ORDER BY watchlist_count DESC
        LIMIT ?
        """,
        (limit,),
    )
    
    results = [
        {
            "media_type": row["target_type"],
            "id": row["target_id"],
            "title": row["target_title"],
            "poster_path": row["poster_path"],
            "watchlist_count": row["watchlist_count"]
        }
        for row in rows
    ]
    
    return jsonify({"results": results})


@app.get("/api/analytics/unreviewed")
def analytics_unreviewed():
    """
    Get titles with no user reviews yet (Q9).
    Query param: type ('all', 'movie', or 'show', default 'all')
    Query param: limit (default 20, max 100)
    """
    media_type = (request.args.get("type") or "all").lower()
    limit = _get_int(request.args.get("limit"), 20, 1, 100)
    
    if media_type not in {"all", "movie", "show"}:
        return jsonify({"error": "type must be 'all', 'movie', or 'show'"}), 400
    
    results = []
    
    if media_type in {"all", "movie"}:
        movie_rows = query(
            """
            SELECT m.movie_id, m.title, m.poster_path, m.tmdb_vote_avg, m.release_year
            FROM movies m
            LEFT JOIN reviews r ON r.movie_id = m.movie_id
            WHERE r.review_id IS NULL
            ORDER BY m.popularity DESC
            LIMIT ?
            """,
            (limit if media_type == "movie" else limit // 2,),
        )
        for row in movie_rows:
            results.append({
                "media_type": "movie",
                "id": row["movie_id"],
                "title": row["title"],
                "poster_path": row["poster_path"],
                "vote_average": row["tmdb_vote_avg"],
                "release_date": str(row["release_year"]) if row["release_year"] else None
            })
    
    if media_type in {"all", "show"}:
        show_rows = query(
            """
            SELECT s.show_id, s.title, s.poster_path, s.tmdb_vote_avg, s.first_air_date
            FROM shows s
            LEFT JOIN reviews r ON r.show_id = s.show_id
            WHERE r.review_id IS NULL
            ORDER BY s.popularity DESC
            LIMIT ?
            """,
            (limit if media_type == "show" else limit // 2,),
        )
        for row in show_rows:
            results.append({
                "media_type": "tv",
                "id": row["show_id"],
                "title": row["title"],
                "poster_path": row["poster_path"],
                "vote_average": row["tmdb_vote_avg"],
                "release_date": row["first_air_date"]
            })
    
    return jsonify({"results": results})


@app.get("/api/analytics/active-reviewers")
def analytics_active_reviewers():
    """
    Get users who reviewed the most titles (Q28).
    Query param: min_reviews (default 3, min 1)
    Query param: limit (default 10, max 50)
    """
    min_reviews = _get_int(request.args.get("min_reviews"), 3, 1, 100)
    limit = _get_int(request.args.get("limit"), 10, 1, 50)
    
    rows = query(
        """
        SELECT u.user_id, u.email, u.display_name,
               COUNT(r.review_id) AS review_count,
               AVG(r.rating) AS avg_rating
        FROM users u
        JOIN reviews r ON r.user_id = u.user_id
        GROUP BY u.user_id
        HAVING COUNT(r.review_id) >= ?
        ORDER BY review_count DESC, avg_rating DESC
        LIMIT ?
        """,
        (min_reviews, limit),
    )
    
    results = [
        {
            "user_id": row["user_id"],
            "display_name": row["display_name"] or row["email"].split("@")[0],
            "review_count": row["review_count"],
            "avg_rating": round(row["avg_rating"], 1) if row["avg_rating"] else None
        }
        for row in rows
    ]
    
    return jsonify({"results": results})


@app.get("/api/analytics/genre-ratings")
def analytics_genre_ratings():
    """
    Get average user rating per genre (Q27).
    Query param: type ('movie' or 'show', default 'movie')
    Admin only endpoint.
    """
    _ensure_auth_bootstrap()
    _require_admin()
    
    media_type = (request.args.get("type") or "movie").lower()
    if media_type not in {"movie", "show"}:
        return jsonify({"error": "type must be 'movie' or 'show'"}), 400
    
    if media_type == "movie":
        rows = query(
            """
            SELECT g.name,
                   AVG(r.rating) AS avg_rating,
                   COUNT(r.review_id) AS review_count
            FROM genres g
            JOIN movie_genres mg ON mg.genre_id = g.genre_id
            JOIN reviews r ON r.movie_id = mg.movie_id
            WHERE r.rating IS NOT NULL
            GROUP BY g.genre_id
            HAVING COUNT(r.review_id) > 0
            ORDER BY avg_rating DESC
            """
        )
    else:
        rows = query(
            """
            SELECT g.name,
                   AVG(r.rating) AS avg_rating,
                   COUNT(r.review_id) AS review_count
            FROM genres g
            JOIN show_genres sg ON sg.genre_id = g.genre_id
            JOIN reviews r ON r.show_id = sg.show_id
            WHERE r.rating IS NOT NULL
            GROUP BY g.genre_id
            HAVING COUNT(r.review_id) > 0
            ORDER BY avg_rating DESC
            """
        )
    
    results = [
        {
            "name": row["name"],
            "avg_rating": round(row["avg_rating"], 2) if row["avg_rating"] else None,
            "review_count": row["review_count"]
        }
        for row in rows
    ]
    
    return jsonify({"results": results})


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
    Requires admin privileges.

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
    _ensure_auth_bootstrap()
    _require_admin()  # Admin only
    
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
        
        # Calculate consolidated rating
        tmdb_rating = data.get("score")
        user_rating = float(data["user_avg_rating"]) if data.get("user_avg_rating") is not None else None
        review_count = data.get("review_count") or 0
        consolidated = calculate_consolidated_rating(
            tmdb_rating=tmdb_rating,
            user_rating=user_rating,
            user_count=review_count,
            confidence=5.0
        )
        
        result = {
            "media_type": data["media_type"],
            "item_id": data["item_id"],
            "tmdb_id": data["tmdb_id"],
            "title": data["title"],
            "overview": data.get("overview") or "",
            "poster_url": poster_url,
            "backdrop_url": backdrop_url,
            "tmdb_vote_avg": tmdb_rating,
            "consolidated_rating": round(consolidated, 2) if consolidated is not None else None,
            "release_date": data.get("release_date"),
            "genres": genres,
        }
        if user_rating is not None:
            result["user_avg_rating"] = round(user_rating, 2)
        if review_count > 0:
            result["review_count"] = review_count
        results.append(result)
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
                   COALESCE(m.release_date, CAST(m.release_year AS TEXT)) AS release_sort,
                   COALESCE(m.release_date, CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END) AS release_date,
                   m.original_language,
                   GROUP_CONCAT(DISTINCT g.name) AS genres,
                   (
                       SELECT AVG(rating) FROM reviews WHERE movie_id = m.movie_id
                   ) AS user_avg_rating,
                   (
                       SELECT COUNT(*) FROM reviews WHERE movie_id = m.movie_id
                   ) AS review_count
            FROM movies m
            INNER JOIN movie_genres mg ON mg.movie_id = m.movie_id
            INNER JOIN genres g ON g.genre_id = mg.genre_id
            WHERE m.release_year IS NOT NULL AND m.overview IS NOT NULL AND m.overview != ''
            GROUP BY m.movie_id
            ORDER BY 
                -- Prioritize movies with actual release dates over year-only
                (m.release_date IS NULL),
                -- Sort by release date (newest first)
                CASE 
                    WHEN m.release_date IS NOT NULL THEN m.release_date
                    ELSE CAST(m.release_year AS TEXT) || '-12-31'
                END DESC,
                -- Then by score and popularity
                (score IS NULL), score DESC, popularity DESC, title
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
                   s.original_language,
                   GROUP_CONCAT(DISTINCT g.name) AS genres,
                   (
                       SELECT AVG(rating) FROM reviews WHERE show_id = s.show_id
                   ) AS user_avg_rating,
                   (
                       SELECT COUNT(*) FROM reviews WHERE show_id = s.show_id
                   ) AS review_count
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
                       COALESCE(m.release_date, CAST(m.release_year AS TEXT)) AS release_sort,
                       COALESCE(m.release_date, CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END) AS release_date,
                       m.original_language,
                       GROUP_CONCAT(DISTINCT g.name) AS genres,
                       (
                           SELECT AVG(rating) FROM reviews WHERE movie_id = m.movie_id
                       ) AS user_avg_rating,
                       (
                           SELECT COUNT(*) FROM reviews WHERE movie_id = m.movie_id
                       ) AS review_count
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
                       s.original_language,
                       GROUP_CONCAT(DISTINCT g.name) AS genres,
                       (
                           SELECT AVG(rating) FROM reviews WHERE show_id = s.show_id
                       ) AS user_avg_rating,
                       (
                           SELECT COUNT(*) FROM reviews WHERE show_id = s.show_id
                       ) AS review_count
                FROM shows s
                INNER JOIN show_genres sg ON sg.show_id = s.show_id
                INNER JOIN genres g ON g.genre_id = sg.genre_id
                WHERE s.first_air_date IS NOT NULL AND s.overview IS NOT NULL AND s.overview != ''
                GROUP BY s.show_id
            )
            ORDER BY 
                -- Prioritize movies/shows with actual release dates over year-only
                (release_date IS NULL OR LENGTH(release_date) < 10),
                -- Sort by release date (newest first)
                CASE 
                    WHEN release_date IS NOT NULL AND LENGTH(release_date) >= 10 THEN release_date
                    WHEN release_sort IS NOT NULL THEN CAST(release_sort AS TEXT) || '-12-31'
                    ELSE '0000-01-01'
                END DESC,
                -- Then by score and popularity
                (score IS NULL), score DESC, popularity DESC, title
            LIMIT ?
            """,
            (limit,),
        )

    results = []
    for row in rows:
        data = dict(row)
        genres = [g.strip() for g in (data.get("genres") or "").split(",") if g.strip()]
        
        # Calculate consolidated rating
        tmdb_rating = data.get("score")
        user_rating = float(data["user_avg_rating"]) if data.get("user_avg_rating") is not None else None
        review_count = data.get("review_count") or 0
        consolidated = calculate_consolidated_rating(
            tmdb_rating=tmdb_rating,
            user_rating=user_rating,
            user_count=review_count,
            confidence=5.0
        )
        
        result = {
            "media_type": data["media_type"],
            "id": data["item_id"],
            "tmdb_id": data["tmdb_id"],
            "title": data["title"],
            "overview": data.get("overview") or "",
            "poster_path": data.get("poster_path"),
            "backdrop_path": None,
            "vote_average": tmdb_rating,
            "consolidated_rating": round(consolidated, 2) if consolidated is not None else None,
            "popularity": data.get("popularity"),
            "release_date": data.get("release_date"),
            "genres": genres,
            "original_language": data.get("original_language"),
        }
        if user_rating is not None:
            result["user_avg_rating"] = round(user_rating, 2)
        if review_count > 0:
            result["review_count"] = review_count
        results.append(result)
    return jsonify({"results": results})


@app.get("/api/future-releases")
def future_releases():
    limit = _get_int(request.args.get("limit"), 12, 1, MAX_PAGE_SIZE)
    media_filter = (request.args.get("type") or "all").lower()
    
    # Get current date for filtering (use timestamp threshold)
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    current_year = datetime.now().year

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
                   COALESCE(m.release_date, CAST(m.release_year AS TEXT)) AS release_sort,
                   COALESCE(m.release_date, CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END) AS release_date,
                   m.original_language,
                   GROUP_CONCAT(DISTINCT g.name) AS genres,
                   (
                       SELECT AVG(rating) FROM reviews WHERE movie_id = m.movie_id
                   ) AS user_avg_rating,
                   (
                       SELECT COUNT(*) FROM reviews WHERE movie_id = m.movie_id
                   ) AS review_count
            FROM movies m
            INNER JOIN movie_genres mg ON mg.movie_id = m.movie_id
            INNER JOIN genres g ON g.genre_id = mg.genre_id
            WHERE m.release_year IS NOT NULL 
              AND m.overview IS NOT NULL 
              AND m.overview != ''
              AND (
                  -- Has full release date and it's in the future
                  (m.release_date IS NOT NULL AND m.release_date > ?)
                  OR
                  -- Year-only entry in future year (exclude current year without specific date)
                  (m.release_date IS NULL AND m.release_year > ?)
              )
            GROUP BY m.movie_id
            ORDER BY 
                -- Prioritize movies with actual release dates over year-only
                (m.release_date IS NULL),
                -- Sort by release date (earliest future first)
                CASE 
                    WHEN m.release_date IS NOT NULL THEN m.release_date
                    ELSE CAST(m.release_year AS TEXT) || '-01-01'
                END ASC,
                -- Then by score and popularity
                (score IS NULL), score DESC, popularity DESC, title
            LIMIT ?
        """
        rows = query(sql, (today, current_year, limit))
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
                   s.original_language,
                   GROUP_CONCAT(DISTINCT g.name) AS genres,
                   (
                       SELECT AVG(rating) FROM reviews WHERE show_id = s.show_id
                   ) AS user_avg_rating,
                   (
                       SELECT COUNT(*) FROM reviews WHERE show_id = s.show_id
                   ) AS review_count
            FROM shows s
            INNER JOIN show_genres sg ON sg.show_id = s.show_id
            INNER JOIN genres g ON g.genre_id = sg.genre_id
            WHERE s.first_air_date IS NOT NULL 
              AND s.overview IS NOT NULL 
              AND s.overview != ''
              AND (
                  -- Has full air date and it's in the future
                  (s.first_air_date > ?)
                  OR
                  -- Year-only entry in future year (exclude current year without specific date)
                  (CAST(substr(s.first_air_date, 1, 4) AS INTEGER) > ?)
              )
            GROUP BY s.show_id
            ORDER BY 
                -- Prioritize shows with actual air dates over year-only
                (s.first_air_date IS NULL),
                -- Sort by air date (earliest future first)
                CASE 
                    WHEN s.first_air_date IS NOT NULL THEN s.first_air_date
                    WHEN release_sort IS NOT NULL THEN CAST(release_sort AS TEXT) || '-01-01'
                    ELSE '9999-12-31'
                END ASC,
                -- Then by score and popularity
                (score IS NULL), score DESC, popularity DESC, title
            LIMIT ?
        """
        rows = query(sql, (today, current_year, limit))
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
                       COALESCE(m.release_date, CAST(m.release_year AS TEXT)) AS release_sort,
                       COALESCE(m.release_date, CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END) AS release_date,
                       m.original_language,
                       GROUP_CONCAT(DISTINCT g.name) AS genres,
                       (
                           SELECT AVG(rating) FROM reviews WHERE movie_id = m.movie_id
                       ) AS user_avg_rating,
                       (
                           SELECT COUNT(*) FROM reviews WHERE movie_id = m.movie_id
                       ) AS review_count
                FROM movies m
                INNER JOIN movie_genres mg ON mg.movie_id = m.movie_id
                INNER JOIN genres g ON g.genre_id = mg.genre_id
                WHERE m.release_year IS NOT NULL 
                  AND m.overview IS NOT NULL 
                  AND m.overview != ''
                  AND (
                      (m.release_date IS NOT NULL AND m.release_date > ?)
                      OR
                      (m.release_date IS NULL AND m.release_year > ?)
                  )
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
                       s.original_language,
                       GROUP_CONCAT(DISTINCT g.name) AS genres,
                       (
                           SELECT AVG(rating) FROM reviews WHERE show_id = s.show_id
                       ) AS user_avg_rating,
                       (
                           SELECT COUNT(*) FROM reviews WHERE show_id = s.show_id
                       ) AS review_count
                FROM shows s
                INNER JOIN show_genres sg ON sg.show_id = s.show_id
                INNER JOIN genres g ON g.genre_id = sg.genre_id
                WHERE s.first_air_date IS NOT NULL 
                  AND s.overview IS NOT NULL 
                  AND s.overview != ''
                  AND (
                      (s.first_air_date > ?)
                      OR
                      (CAST(substr(s.first_air_date, 1, 4) AS INTEGER) > ?)
                  )
                GROUP BY s.show_id
            )
            ORDER BY 
                -- Prioritize items with actual release dates over year-only
                (release_date IS NULL OR LENGTH(release_date) < 10),
                -- Sort by release date (earliest future first)
                CASE 
                    WHEN release_date IS NOT NULL AND LENGTH(release_date) >= 10 THEN release_date
                    WHEN release_sort IS NOT NULL THEN CAST(release_sort AS TEXT) || '-01-01'
                    ELSE '9999-12-31'
                END ASC,
                -- Then by score and popularity
                (score IS NULL), score DESC, popularity DESC, title
            LIMIT ?
            """,
            (today, current_year, today, current_year, limit),
        )

    results = []
    for row in rows:
        data = dict(row)
        genres = [g.strip() for g in (data.get("genres") or "").split(",") if g.strip()]
        
        # Calculate consolidated rating
        tmdb_rating = data.get("score")
        user_rating = float(data["user_avg_rating"]) if data.get("user_avg_rating") is not None else None
        review_count = data.get("review_count") or 0
        consolidated = calculate_consolidated_rating(
            tmdb_rating=tmdb_rating,
            user_rating=user_rating,
            user_count=review_count,
            confidence=5.0
        )
        
        result = {
            "media_type": data["media_type"],
            "id": data["item_id"],
            "tmdb_id": data["tmdb_id"],
            "title": data["title"],
            "overview": data.get("overview") or "",
            "poster_path": data.get("poster_path"),
            "backdrop_path": None,
            "vote_average": tmdb_rating,
            "consolidated_rating": round(consolidated, 2) if consolidated is not None else None,
            "popularity": data.get("popularity"),
            "release_date": data.get("release_date"),
            "genres": genres,
            "original_language": data.get("original_language"),
        }
        if user_rating is not None:
            result["user_avg_rating"] = round(user_rating, 2)
        if review_count > 0:
            result["review_count"] = review_count
        results.append(result)
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
               GROUP_CONCAT(DISTINCT g.name) AS genres,
               (
                   SELECT AVG(rating) FROM reviews WHERE movie_id = m.movie_id
               ) AS user_avg_rating,
               (
                   SELECT COUNT(*) FROM reviews WHERE movie_id = m.movie_id
               ) AS review_count
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
               GROUP_CONCAT(DISTINCT g.name) AS genres,
               (
                   SELECT AVG(rating) FROM reviews WHERE show_id = s.show_id
               ) AS user_avg_rating,
               (
                   SELECT COUNT(*) FROM reviews WHERE show_id = s.show_id
               ) AS review_count
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
        
        # Calculate consolidated rating
        tmdb_rating = data.get("vote_average")
        user_rating = float(data["user_avg_rating"]) if data.get("user_avg_rating") is not None else None
        review_count = data.get("review_count") or 0
        consolidated = calculate_consolidated_rating(
            tmdb_rating=tmdb_rating,
            user_rating=user_rating,
            user_count=review_count,
            confidence=5.0
        )
        
        result = {
            "media_type": data["media_type"],
            "id": data["item_id"],
            "tmdb_id": data["tmdb_id"],
            "title": data["title"],
            "overview": data.get("overview") or "",
            "poster_path": data.get("poster_path"),
            "backdrop_path": None,
            "vote_average": tmdb_rating,
            "consolidated_rating": round(consolidated, 2) if consolidated is not None else None,
            "popularity": data.get("popularity"),
            "release_date": data.get("release_date"),
            "genres": genres,
            "original_language": None,
        }
        if user_rating is not None:
            result["user_avg_rating"] = round(user_rating, 2)
        if review_count > 0:
            result["review_count"] = review_count
        results.append(result)
    
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
    user_avg_rating = None
    review_count = movie.get("review_count") or 0
    if movie.get("user_vote_avg") is not None:
        user_avg_rating = float(movie["user_vote_avg"])
        movie["user_avg_rating"] = user_avg_rating
    
    # Calculate consolidated rating
    consolidated = calculate_consolidated_rating(
        tmdb_rating=movie.get("tmdb_vote_avg"),
        user_rating=user_avg_rating,
        user_count=review_count,
        confidence=5.0
    )
    if consolidated is not None:
        movie["consolidated_rating"] = round(consolidated, 2)
    
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
        SELECT p.person_id, p.name, p.profile_path, mc.character, mc.cast_order
        FROM movie_cast mc
        JOIN people p ON p.person_id = mc.person_id
        WHERE mc.movie_id = ?
        ORDER BY mc.cast_order ASC
        LIMIT 10
        """,
        (movie_id,),
    )
    movie["genres"] = [g["name"] for g in genres]
    movie["top_cast"] = [
        {
            **dict(c),
            "profile_url": _tmdb_image(c["profile_path"], "w185")
        }
        for c in cast
    ]
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
    user_avg_rating = None
    review_count = show.get("review_count") or 0
    if show.get("user_vote_avg") is not None:
        user_avg_rating = float(show["user_vote_avg"])
        show["user_avg_rating"] = user_avg_rating
    
    # Calculate consolidated rating
    consolidated = calculate_consolidated_rating(
        tmdb_rating=show.get("tmdb_vote_avg"),
        user_rating=user_avg_rating,
        user_count=review_count,
        confidence=5.0
    )
    if consolidated is not None:
        show["consolidated_rating"] = round(consolidated, 2)
    
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
        SELECT p.person_id, p.name, p.profile_path, sc.character, sc.cast_order
        FROM show_cast sc
        JOIN people p ON p.person_id = sc.person_id
        WHERE sc.show_id = ?
        ORDER BY sc.cast_order ASC
        LIMIT 10
        """,
        (show_id,),
    )
    show["genres"] = [g["name"] for g in genres]
    show["top_cast"] = [
        {
            **dict(c),
            "profile_url": _tmdb_image(c["profile_path"], "w185")
        }
        for c in cast
    ]
    return jsonify(show)


@app.get("/api/people/<int:person_id>")
def person_detail(person_id: int):
    """
    Get detailed information about a person (actor/actress).
    Returns person details including biography, birthday, social media links, etc.
    """
    row = query(
        """
        SELECT p.*
        FROM people p
        WHERE p.person_id = ?
        """,
        (person_id,),
    )
    
    if not row:
        return jsonify({"error": "Person not found"}), 404
    
    person = dict(row[0])
    
    # Add profile image URL
    if person.get("profile_path"):
        person["profile_image_url"] = _tmdb_image(person["profile_path"], "w185")
        person["profile_image_large_url"] = _tmdb_image(person["profile_path"], "h632")
    else:
        person["profile_image_url"] = None
        person["profile_image_large_url"] = None
    
    # Add social media URLs
    person["social_links"] = {}
    if person.get("imdb_id"):
        person["social_links"]["imdb"] = f"https://www.imdb.com/name/{person['imdb_id']}"
    if person.get("instagram_id"):
        person["social_links"]["instagram"] = f"https://www.instagram.com/{person['instagram_id']}"
    if person.get("twitter_id"):
        person["social_links"]["twitter"] = f"https://twitter.com/{person['twitter_id']}"
    if person.get("facebook_id"):
        person["social_links"]["facebook"] = f"https://www.facebook.com/{person['facebook_id']}"
    
    # Get movies this person has appeared in
    movies = query(
        """
        SELECT m.movie_id, m.title, m.release_year, m.poster_path, mc.character, mc.cast_order
        FROM movie_cast mc
        JOIN movies m ON m.movie_id = mc.movie_id
        WHERE mc.person_id = ?
        ORDER BY m.release_year DESC, mc.cast_order ASC
        LIMIT 20
        """,
        (person_id,),
    )
    person["movies"] = [
        {
            **dict(movie),
            "poster_url": _tmdb_image(movie["poster_path"], "w185")
        }
        for movie in movies
    ]
    
    # Get TV shows this person has appeared in
    shows = query(
        """
        SELECT s.show_id, s.title, s.first_air_date, s.poster_path, sc.character, sc.cast_order
        FROM show_cast sc
        JOIN shows s ON s.show_id = sc.show_id
        WHERE sc.person_id = ?
        ORDER BY s.first_air_date DESC, sc.cast_order ASC
        LIMIT 20
        """,
        (person_id,),
    )
    person["shows"] = [
        {
            **dict(show),
            "poster_url": _tmdb_image(show["poster_path"], "w185")
        }
        for show in shows
    ]
    
    return jsonify(person)


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
    Requires admin privileges.
    
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
    _ensure_auth_bootstrap()
    _require_admin()  # Admin only
    
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
    Requires admin privileges.
    
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
    _ensure_auth_bootstrap()
    _require_admin()  # Admin only
    
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
    Requires admin privileges.
    """
    _ensure_auth_bootstrap()
    _require_admin()  # Admin only
    
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
    _ensure_review_reactions_table()  # Ensure table exists
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
        review_id = row["review_id"]
        # Get reaction counts for this review
        reaction_rows = query(
            """
            SELECT emote_type, COUNT(*) AS count
            FROM review_reactions
            WHERE review_id = ?
            GROUP BY emote_type
            """,
            (review_id,)
        )
        reactions = {}
        for r_row in reaction_rows:
            reactions[r_row["emote_type"]] = r_row["count"]
        
        reviews.append({
            "review_id": review_id,
            "user_id": row["user_id"],
            "user_email": row["user_email"],
            "content": row["content"],
            "rating": row["rating"],
            "created_at": row["created_at"],
            "reactions": reactions,
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
    
    # Check if user already has a review for this movie/show
    if target_type == "movie":
        existing = conn.execute(
            "SELECT review_id FROM reviews WHERE user_id = ? AND movie_id = ?",
            (user_id, target_id)
        ).fetchone()
    else:
        existing = conn.execute(
            "SELECT review_id FROM reviews WHERE user_id = ? AND show_id = ?",
            (user_id, target_id)
        ).fetchone()
    
    if existing:
        return jsonify({"error": "You can only review each title once. Please edit your existing review instead."}), 400
    
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


@app.put("/api/reviews/<int:review_id>")
def update_review(review_id: int):
    """
    Update an existing review (Q15).
    Only the review owner can update their review, or admins can update any review.
    Auth: Required (review owner or admin)
    Body: { rating?: number, content?: string }
    """
    _ensure_auth_bootstrap()
    user = _get_current_user()
    if not user:
        return jsonify({"ok": False, "error": "Authentication required"}), 401
    
    payload = request.get_json(force=True, silent=True) or {}
    rating = payload.get("rating")
    content = payload.get("content")
    
    # At least one field must be provided
    if rating is None and content is None:
        return jsonify({"ok": False, "error": "At least one of 'rating' or 'content' must be provided"}), 400
    
    # Validate rating if provided
    rating_value = None
    if rating is not None:
        try:
            rating_value = float(rating)
            if not (0 <= rating_value <= 10):
                return jsonify({"ok": False, "error": "rating must be between 0 and 10"}), 400
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "rating must be numeric"}), 400
    
    # Validate content if provided
    if content is not None:
        content = content.strip()
        if not content:
            return jsonify({"ok": False, "error": "content cannot be empty"}), 400
    
    conn = get_db()
    try:
        # First, verify the review exists and get its owner
        check_row = conn.execute(
            "SELECT user_id, movie_id, show_id FROM reviews WHERE review_id = ?",
            (review_id,),
        ).fetchone()
        
        if not check_row:
            return jsonify({"ok": False, "error": "Review not found"}), 404
        
        review_owner_id = check_row["user_id"]
        is_owner = review_owner_id == user["user_id"]
        is_admin = user.get("is_admin", False)
        
        # Allow admins to update any review, regular users can only update their own
        if not is_owner and not is_admin:
            return jsonify({"ok": False, "error": "You can only update your own reviews"}), 403
        
        # Build UPDATE query dynamically based on what's provided
        updates = []
        params = []
        
        if rating_value is not None:
            updates.append("rating = ?")
            params.append(rating_value)
        
        if content is not None:
            updates.append("content = ?")
            params.append(content)
        
        params.append(review_id)
        update_sql = f"UPDATE reviews SET {', '.join(updates)} WHERE review_id = ?"
        
        conn.execute(update_sql, tuple(params))
        conn.commit()
        
        return jsonify({"ok": True, "message": "Review updated successfully"})
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.delete("/api/reviews/<int:review_id>")
def delete_review(review_id: int):
    """
    Delete an existing review (Q16).
    Only the review owner can delete their review, or admins can delete any review.
    Auth: Required (review owner or admin)
    """
    _ensure_auth_bootstrap()
    user = _get_current_user()
    if not user:
        return jsonify({"ok": False, "error": "Authentication required"}), 401
    
    conn = get_db()
    try:
        # First, verify the review exists and get its owner
        check_row = conn.execute(
            "SELECT user_id FROM reviews WHERE review_id = ?",
            (review_id,),
        ).fetchone()
        
        if not check_row:
            return jsonify({"ok": False, "error": "Review not found"}), 404
        
        review_owner_id = check_row["user_id"]
        is_owner = review_owner_id == user["user_id"]
        is_admin = user.get("is_admin", False)
        
        print(f"[DEBUG DELETE REVIEW] user_id={user['user_id']}, email={user.get('email')}, review_owner_id={review_owner_id}, is_owner={is_owner}, is_admin={is_admin}")
        
        # Allow admins to delete any review, regular users can only delete their own
        if not is_owner and not is_admin:
            print(f"[DEBUG DELETE REVIEW] Access denied: not owner ({is_owner}) and not admin ({is_admin})")
            return jsonify({"ok": False, "error": "You can only delete your own reviews"}), 403
        
        print(f"[DEBUG DELETE REVIEW] Access granted: proceeding with delete")
        
        # Delete the review
        deleted = conn.execute(
            "DELETE FROM reviews WHERE review_id = ?",
            (review_id,),
        ).rowcount
        
        conn.commit()
        
        if deleted == 0:
            return jsonify({"ok": False, "error": "Failed to delete review"}), 500
        
        return jsonify({"ok": True, "deleted": deleted, "message": "Review deleted successfully"})
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/api/reviews/<int:review_id>/reactions")
def add_review_reaction(review_id: int):
    """
    Add or toggle a reaction to a review.
    Auth: Required
    Body: { emote_type: '👍' | '❤️' | '😂' | '😮' | '😢' | '🔥' }
    """
    _ensure_auth_bootstrap()
    user = _get_current_user()
    if not user:
        return jsonify({"ok": False, "error": "Authentication required"}), 401
    
    payload = request.get_json(force=True, silent=True) or {}
    emote_type = payload.get("emote_type")
    
    valid_emotes = ['👍', '❤️', '😂', '😮', '😢', '🔥']
    if emote_type not in valid_emotes:
        return jsonify({"ok": False, "error": f"emote_type must be one of: {', '.join(valid_emotes)}"}), 400
    
    conn = get_db()
    try:
        # Verify review exists
        review_row = conn.execute(
            "SELECT review_id FROM reviews WHERE review_id = ?",
            (review_id,)
        ).fetchone()
        if not review_row:
            return jsonify({"ok": False, "error": "Review not found"}), 404
        
        # Check if reaction already exists
        existing = conn.execute(
            "SELECT reaction_id FROM review_reactions WHERE review_id = ? AND user_id = ? AND emote_type = ?",
            (review_id, user["user_id"], emote_type)
        ).fetchone()
        
        if existing:
            # Remove reaction (toggle off)
            conn.execute(
                "DELETE FROM review_reactions WHERE review_id = ? AND user_id = ? AND emote_type = ?",
                (review_id, user["user_id"], emote_type)
            )
            conn.commit()
            return jsonify({"ok": True, "action": "removed", "message": "Reaction removed"})
        else:
            # Add reaction
            conn.execute(
                "INSERT INTO review_reactions (review_id, user_id, emote_type) VALUES (?, ?, ?)",
                (review_id, user["user_id"], emote_type)
            )
            conn.commit()
            return jsonify({"ok": True, "action": "added", "message": "Reaction added"})
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/api/discussion/comments")
def get_title_comments():
    """
    Get all comments (with nested replies) for a movie or TV show.
    Query parameters:
    - title_type: 'movie' or 'show'
    - title_id: the movie_id or show_id
    Returns a tree structure with nested replies.
    """
    title_type = request.args.get("title_type")
    title_id = request.args.get("title_id")
    
    if not title_type or not title_id:
        return jsonify({"ok": False, "error": "title_type and title_id are required"}), 400
    
    if title_type not in {"movie", "show"}:
        return jsonify({"ok": False, "error": "title_type must be 'movie' or 'show'"}), 400
    
    try:
        title_id_int = int(title_id)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "title_id must be an integer"}), 400
    
    # Verify title exists
    conn = get_db()
    if title_type == "movie":
        title_check = conn.execute(
            "SELECT movie_id FROM movies WHERE movie_id = ?",
            (title_id_int,)
        ).fetchone()
    else:
        title_check = conn.execute(
            "SELECT show_id FROM shows WHERE show_id = ?",
            (title_id_int,)
        ).fetchone()
    
    if not title_check:
        return jsonify({"ok": False, "error": "Title not found"}), 404
    
    # Fetch all comments for this title
    rows = query(
        """
        SELECT c.comment_id, c.user_id, c.parent_comment_id, c.body, 
               c.created_at, c.updated_at, c.is_deleted,
               u.email AS user_email, u.display_name
        FROM title_comments c
        LEFT JOIN users u ON u.user_id = c.user_id
        WHERE c.title_type = ? AND c.title_id = ?
        ORDER BY c.created_at ASC
        """,
        (title_type, title_id_int)
    )
    
    # Build a flat list first
    comments_map = {}
    root_comments = []
    
    for row in rows:
        comment = {
            "comment_id": row["comment_id"],
            "user_id": row["user_id"],
            "user_email": row["user_email"] or "",
            "display_name": row["display_name"] or (row["user_email"] or "").split("@")[0],
            "parent_comment_id": row["parent_comment_id"],
            "body": "[deleted]" if row["is_deleted"] else row["body"],
            "is_deleted": bool(row["is_deleted"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "replies": []
        }
        comments_map[comment["comment_id"]] = comment
        
        if comment["parent_comment_id"] is None:
            root_comments.append(comment)
        else:
            # Add to parent's replies
            parent = comments_map.get(comment["parent_comment_id"])
            if parent:
                parent["replies"].append(comment)
    
    return jsonify({"ok": True, "comments": root_comments, "count": len(rows)})


@app.post("/api/discussion/comments")
def create_title_comment():
    """
    Create a new comment or reply for a movie or TV show.
    Auth: Required
    Body: {
        title_type: 'movie' | 'show',
        title_id: int,
        body: string,
        parent_comment_id?: int (optional, for replies)
    }
    """
    _ensure_auth_bootstrap()
    user = _get_current_user()
    if not user:
        return jsonify({"ok": False, "error": "Authentication required"}), 401
    
    payload = request.get_json(force=True, silent=True) or {}
    title_type = payload.get("title_type")
    title_id = payload.get("title_id")
    body = payload.get("body", "").strip()
    parent_comment_id = payload.get("parent_comment_id")
    
    if not title_type or title_id is None:
        return jsonify({"ok": False, "error": "title_type and title_id are required"}), 400
    
    if title_type not in {"movie", "show"}:
        return jsonify({"ok": False, "error": "title_type must be 'movie' or 'show'"}), 400
    
    try:
        title_id_int = int(title_id)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "title_id must be an integer"}), 400
    
    if not body:
        return jsonify({"ok": False, "error": "Comment body is required"}), 400
    
    conn = get_db()
    
    # Verify title exists
    if title_type == "movie":
        title_check = conn.execute(
            "SELECT movie_id FROM movies WHERE movie_id = ?",
            (title_id_int,)
        ).fetchone()
    else:
        title_check = conn.execute(
            "SELECT show_id FROM shows WHERE show_id = ?",
            (title_id_int,)
        ).fetchone()
    
    if not title_check:
        return jsonify({"ok": False, "error": "Title not found"}), 404
    
    # If parent_comment_id is provided, verify it exists and belongs to the same title
    if parent_comment_id is not None:
        try:
            parent_id_int = int(parent_comment_id)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "parent_comment_id must be an integer"}), 400
        
        parent_check = conn.execute(
            "SELECT comment_id FROM title_comments WHERE comment_id = ? AND title_type = ? AND title_id = ?",
            (parent_id_int, title_type, title_id_int)
        ).fetchone()
        
        if not parent_check:
            return jsonify({"ok": False, "error": "Parent comment not found"}), 404
    
    # Insert the comment
    try:
        cur = conn.execute(
            """
            INSERT INTO title_comments (title_type, title_id, user_id, parent_comment_id, body, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (title_type, title_id_int, user["user_id"], parent_comment_id, body)
        )
        conn.commit()
        
        comment_id = cur.lastrowid
        if not comment_id:
            conn.rollback()
            return jsonify({"ok": False, "error": "Failed to create comment: no row ID returned"}), 500
        
        # Fetch the created comment with user info
        new_comment_rows = query(
            """
            SELECT c.comment_id, c.user_id, c.parent_comment_id, c.body, 
                   c.created_at, c.updated_at, c.is_deleted,
                   u.email AS user_email, u.display_name
            FROM title_comments c
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.comment_id = ?
            """,
            (comment_id,)
        )
        
        if not new_comment_rows:
            conn.rollback()
            return jsonify({"ok": False, "error": "Failed to retrieve created comment"}), 500
        
        new_comment_row = new_comment_rows[0]
        
        comment = {
            "comment_id": new_comment_row["comment_id"],
            "user_id": new_comment_row["user_id"],
            "user_email": new_comment_row["user_email"] or "",
            "display_name": new_comment_row["display_name"] or (new_comment_row["user_email"] or "").split("@")[0],
            "parent_comment_id": new_comment_row["parent_comment_id"],
            "body": new_comment_row["body"],
            "is_deleted": bool(new_comment_row["is_deleted"]),
            "created_at": new_comment_row["created_at"],
            "updated_at": new_comment_row["updated_at"],
            "replies": []
        }
        
        return jsonify({"ok": True, "comment": comment})
    except sqlite3.OperationalError as exc:
        conn.rollback()
        error_msg = str(exc)
        if "no such table" in error_msg.lower() or "title_comments" in error_msg.lower():
            return jsonify({"ok": False, "error": "Database table 'title_comments' does not exist. Please run the migration script."}), 500
        return jsonify({"ok": False, "error": f"Database error: {error_msg}"}), 500
    except Exception as exc:
        conn.rollback()
        import traceback
        error_msg = str(exc)
        traceback.print_exc()
        return jsonify({"ok": False, "error": f"Server error: {error_msg}"}), 500


@app.put("/api/discussion/comments/<int:comment_id>")
def update_title_comment(comment_id: int):
    """
    Update an existing comment.
    Only the comment owner can update their comment, or admins can update any comment.
    Auth: Required (comment owner or admin)
    Body: { body: string }
    """
    _ensure_auth_bootstrap()
    user = _get_current_user()
    if not user:
        return jsonify({"ok": False, "error": "Authentication required"}), 401
    
    payload = request.get_json(force=True, silent=True) or {}
    body = payload.get("body", "").strip()
    
    if not body:
        return jsonify({"ok": False, "error": "Comment body is required"}), 400
    
    conn = get_db()
    try:
        # Verify comment exists and get its owner
        check_row = conn.execute(
            "SELECT user_id, is_deleted FROM title_comments WHERE comment_id = ?",
            (comment_id,)
        ).fetchone()
        
        if not check_row:
            return jsonify({"ok": False, "error": "Comment not found"}), 404
        
        if check_row["is_deleted"]:
            return jsonify({"ok": False, "error": "Cannot edit deleted comment"}), 400
        
        comment_owner_id = check_row["user_id"]
        is_owner = comment_owner_id == user["user_id"]
        is_admin = user.get("is_admin", False)
        
        if not is_owner and not is_admin:
            return jsonify({"ok": False, "error": "You can only edit your own comments"}), 403
        
        # Update the comment
        conn.execute(
            """
            UPDATE title_comments 
            SET body = ?, updated_at = CURRENT_TIMESTAMP
            WHERE comment_id = ?
            """,
            (body, comment_id)
        )
        conn.commit()
        
        # Fetch updated comment
        updated_row = query(
            """
            SELECT c.comment_id, c.user_id, c.parent_comment_id, c.body, 
                   c.created_at, c.updated_at, c.is_deleted,
                   u.email AS user_email, u.display_name
            FROM title_comments c
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.comment_id = ?
            """,
            (comment_id,)
        )[0]
        
        comment = {
            "comment_id": updated_row["comment_id"],
            "user_id": updated_row["user_id"],
            "user_email": updated_row["user_email"] or "",
            "display_name": updated_row["display_name"] or (updated_row["user_email"] or "").split("@")[0],
            "parent_comment_id": updated_row["parent_comment_id"],
            "body": updated_row["body"],
            "is_deleted": bool(updated_row["is_deleted"]),
            "created_at": updated_row["created_at"],
            "updated_at": updated_row["updated_at"],
            "replies": []
        }
        
        return jsonify({"ok": True, "comment": comment})
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.delete("/api/discussion/comments/<int:comment_id>")
def delete_title_comment(comment_id: int):
    """
    Delete an existing comment (soft delete - marks as deleted but keeps in DB).
    Only the comment owner can delete their comment, or admins can delete any comment.
    Auth: Required (comment owner or admin)
    """
    _ensure_auth_bootstrap()
    user = _get_current_user()
    if not user:
        return jsonify({"ok": False, "error": "Authentication required"}), 401
    
    conn = get_db()
    try:
        # Verify comment exists and get its owner
        check_row = conn.execute(
            "SELECT user_id FROM title_comments WHERE comment_id = ?",
            (comment_id,)
        ).fetchone()
        
        if not check_row:
            return jsonify({"ok": False, "error": "Comment not found"}), 404
        
        comment_owner_id = check_row["user_id"]
        is_owner = comment_owner_id == user["user_id"]
        is_admin = user.get("is_admin", False)
        
        if not is_owner and not is_admin:
            return jsonify({"ok": False, "error": "You can only delete your own comments"}), 403
        
        # Soft delete: mark as deleted
        conn.execute(
            """
            UPDATE title_comments 
            SET is_deleted = 1, body = '[deleted]', updated_at = CURRENT_TIMESTAMP
            WHERE comment_id = ?
            """,
            (comment_id,)
        )
        conn.commit()
        
        return jsonify({"ok": True, "message": "Comment deleted successfully"})
    except Exception as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/api/reviews/<int:review_id>/reactions")
def get_review_reactions(review_id: int):
    """
    Get all reactions for a review, including whether current user has reacted.
    Auth: Optional (if authenticated, shows user's reactions)
    """
    _ensure_review_reactions_table()  # Ensure table exists
    _ensure_auth_bootstrap()
    user = _get_current_user()
    
    conn = get_db()
    try:
        # Verify review exists
        review_row = conn.execute(
            "SELECT review_id FROM reviews WHERE review_id = ?",
            (review_id,)
        ).fetchone()
        if not review_row:
            return jsonify({"ok": False, "error": "Review not found"}), 404
        
        # Get all reaction counts
        reaction_rows = query(
            """
            SELECT emote_type, COUNT(*) AS count
            FROM review_reactions
            WHERE review_id = ?
            GROUP BY emote_type
            """,
            (review_id,)
        )
        reactions = {}
        for row in reaction_rows:
            reactions[row["emote_type"]] = row["count"]
        
        # Get current user's reactions if authenticated
        user_reactions = []
        if user:
            user_reaction_rows = query(
                """
                SELECT emote_type
                FROM review_reactions
                WHERE review_id = ? AND user_id = ?
                """,
                (review_id, user["user_id"])
            )
            user_reactions = [row["emote_type"] for row in user_reaction_rows]
        
        return jsonify({
            "ok": True,
            "reactions": reactions,
            "user_reactions": user_reactions
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


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


def _ensure_favorites_table() -> None:
    """Ensure the favorites table exists (backward compatibility)."""
    conn = get_db()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS favorites (
                user_id     INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                movie_id    INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
                show_id     INTEGER REFERENCES shows(show_id) ON DELETE CASCADE,
                added_at    TEXT DEFAULT CURRENT_TIMESTAMP,
                CHECK ( (movie_id IS NOT NULL) <> (show_id IS NOT NULL) ),
                PRIMARY KEY (user_id, movie_id, show_id)
            )
        """)
        conn.commit()
    except Exception:
        pass  # Table already exists or other error


@app.post("/api/favorites")
def add_favorite():
    """Add a movie or show to user's favorites."""
    _ensure_favorites_table()  # Ensure table exists
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
        sql = "INSERT INTO favorites (user_id, movie_id, show_id) VALUES (?, ?, NULL)"
    else:
        sql = "INSERT INTO favorites (user_id, movie_id, show_id) VALUES (?, NULL, ?)"

    try:
        with conn:
            conn.execute(sql, (user_id, target_id))
    except sqlite3.IntegrityError:
        # Already in favorites
        return jsonify({"ok": True, "already_favorited": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"ok": True})


@app.delete("/api/favorites")
def remove_favorite():
    """Remove a movie or show from user's favorites."""
    _ensure_favorites_table()  # Ensure table exists
    payload = request.get_json(force=True, silent=True) or {}
    user_id = payload.get("user_id")
    target_type = payload.get("target_type")
    target_id = payload.get("target_id")
    if not isinstance(user_id, int) or not isinstance(target_id, int):
        return jsonify({"error": "user_id and target_id must be integers"}), 400
    if target_type not in {"movie", "show"}:
        return jsonify({"error": "target_type must be 'movie' or 'show'"}), 400

    if target_type == "movie":
        sql = "DELETE FROM favorites WHERE user_id = ? AND movie_id = ?"
    else:
        sql = "DELETE FROM favorites WHERE user_id = ? AND show_id = ?"

    deleted = execute(sql, (user_id, target_id))
    return jsonify({"ok": True, "deleted": deleted})


@app.get("/api/favorites/check")
def check_favorite():
    """Check if a movie or show is in user's favorites."""
    _ensure_favorites_table()  # Ensure table exists
    user_id = request.args.get("user_id", type=int)
    target_type = request.args.get("target_type")
    target_id = request.args.get("target_id", type=int)
    
    if not user_id or not target_id:
        return jsonify({"error": "user_id and target_id are required"}), 400
    if target_type not in {"movie", "show"}:
        return jsonify({"error": "target_type must be 'movie' or 'show'"}), 400

    if target_type == "movie":
        sql = "SELECT 1 FROM favorites WHERE user_id = ? AND movie_id = ? LIMIT 1"
    else:
        sql = "SELECT 1 FROM favorites WHERE user_id = ? AND show_id = ? LIMIT 1"

    rows = query(sql, (user_id, target_id))
    is_favorited = len(rows) > 0
    return jsonify({"ok": True, "is_favorited": is_favorited})


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


# ============================================================================
# Discussion Board System (MAL-style per movie/show discussions)
# Uses the `discussions` and `comments` tables from schema.sql
# ============================================================================


def _get_current_user_id() -> int | None:
    """
    Get the current logged-in user's ID from the Authorization header.
    Returns None if not authenticated.
    """
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        if ":" in token:
            try:
                user_id_str, email = token.split(":", 1)
                user_id = int(user_id_str)
                # Verify user exists
                conn = get_db()
                row = conn.execute(
                    "SELECT user_id FROM users WHERE user_id = ? AND email = ?",
                    (user_id, email)
                ).fetchone()
                if row:
                    return user_id
            except (ValueError, TypeError):
                pass
    return None


def _require_auth() -> tuple[int, None] | tuple[None, tuple]:
    """
    Require authentication. Returns (user_id, None) if authenticated,
    or (None, error_response) if not.
    """
    user_id = _get_current_user_id()
    if not user_id:
        return None, (jsonify({"ok": False, "error": "Authentication required"}), 401)
    return user_id, None


# --- List discussions for a movie ---
@app.get("/api/movies/<int:movie_id>/discussions")
def list_movie_discussions(movie_id: int):
    """
    List all discussions for a given movie.
    Returns JSON array with discussion details and comment counts.
    """
    conn = get_db()
    
    # Verify movie exists
    movie = conn.execute(
        "SELECT movie_id FROM movies WHERE movie_id = ?",
        (movie_id,)
    ).fetchone()
    if not movie:
        return jsonify({"ok": False, "error": "Movie not found"}), 404
    
    # Fetch discussions with user info and comment count
    rows = conn.execute(
        """
        SELECT 
            d.discussion_id,
            d.title,
            d.user_id,
            COALESCE(u.display_name, u.email) AS user_display_name,
            d.created_at,
            (SELECT COUNT(*) FROM comments c WHERE c.discussion_id = d.discussion_id) AS comment_count
        FROM discussions d
        LEFT JOIN users u ON u.user_id = d.user_id
        WHERE d.movie_id = ?
        ORDER BY d.created_at DESC
        """,
        (movie_id,)
    ).fetchall()
    
    discussions = [
        {
            "discussion_id": row["discussion_id"],
            "title": row["title"],
            "user_id": row["user_id"],
            "user_display_name": row["user_display_name"] or "Unknown",
            "created_at": row["created_at"],
            "comment_count": row["comment_count"]
        }
        for row in rows
    ]
    
    return jsonify({"ok": True, "discussions": discussions})


# --- List discussions for a show ---
@app.get("/api/shows/<int:show_id>/discussions")
def list_show_discussions(show_id: int):
    """
    List all discussions for a given TV show.
    Returns JSON array with discussion details and comment counts.
    """
    conn = get_db()
    
    # Verify show exists
    show = conn.execute(
        "SELECT show_id FROM shows WHERE show_id = ?",
        (show_id,)
    ).fetchone()
    if not show:
        return jsonify({"ok": False, "error": "Show not found"}), 404
    
    # Fetch discussions with user info and comment count
    rows = conn.execute(
        """
        SELECT 
            d.discussion_id,
            d.title,
            d.user_id,
            COALESCE(u.display_name, u.email) AS user_display_name,
            d.created_at,
            (SELECT COUNT(*) FROM comments c WHERE c.discussion_id = d.discussion_id) AS comment_count
        FROM discussions d
        LEFT JOIN users u ON u.user_id = d.user_id
        WHERE d.show_id = ?
        ORDER BY d.created_at DESC
        """,
        (show_id,)
    ).fetchall()
    
    discussions = [
        {
            "discussion_id": row["discussion_id"],
            "title": row["title"],
            "user_id": row["user_id"],
            "user_display_name": row["user_display_name"] or "Unknown",
            "created_at": row["created_at"],
            "comment_count": row["comment_count"]
        }
        for row in rows
    ]
    
    return jsonify({"ok": True, "discussions": discussions})


# --- Create discussion for a movie ---
@app.post("/api/movies/<int:movie_id>/discussions")
def create_movie_discussion(movie_id: int):
    """
    Create a new discussion for a movie.
    Auth: Required
    Body: { "title": "string" }
    
    The CHECK constraint in schema.sql enforces: movie_id IS NOT NULL XOR show_id IS NOT NULL
    For movie discussions, we set movie_id and show_id = NULL.
    """
    user_id, error = _require_auth()
    if error:
        return error
    
    conn = get_db()
    
    # Verify movie exists
    movie = conn.execute(
        "SELECT movie_id FROM movies WHERE movie_id = ?",
        (movie_id,)
    ).fetchone()
    if not movie:
        return jsonify({"ok": False, "error": "Movie not found"}), 404
    
    payload = request.get_json(force=True, silent=True) or {}
    title = (payload.get("title") or "").strip()
    
    if not title:
        return jsonify({"ok": False, "error": "Discussion title is required"}), 400
    
    try:
        cur = conn.execute(
            """
            INSERT INTO discussions (user_id, movie_id, show_id, title, created_at)
            VALUES (?, ?, NULL, ?, CURRENT_TIMESTAMP)
            """,
            (user_id, movie_id, title)
        )
        conn.commit()
        
        discussion_id = cur.lastrowid
        
        # Fetch the created discussion
        row = conn.execute(
            """
            SELECT 
                d.discussion_id,
                d.title,
                d.user_id,
                COALESCE(u.display_name, u.email) AS user_display_name,
                d.created_at,
                d.movie_id,
                d.show_id
            FROM discussions d
            LEFT JOIN users u ON u.user_id = d.user_id
            WHERE d.discussion_id = ?
            """,
            (discussion_id,)
        ).fetchone()
        
        return jsonify({
            "ok": True,
            "discussion": {
                "discussion_id": row["discussion_id"],
                "title": row["title"],
                "user_id": row["user_id"],
                "user_display_name": row["user_display_name"] or "Unknown",
                "created_at": row["created_at"],
                "movie_id": row["movie_id"],
                "show_id": row["show_id"],
                "comment_count": 0
            }
        })
    except sqlite3.Error as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": f"Database error: {str(exc)}"}), 500


# --- Create discussion for a show ---
@app.post("/api/shows/<int:show_id>/discussions")
def create_show_discussion(show_id: int):
    """
    Create a new discussion for a TV show.
    Auth: Required
    Body: { "title": "string" }
    
    The CHECK constraint in schema.sql enforces: movie_id IS NOT NULL XOR show_id IS NOT NULL
    For show discussions, we set show_id and movie_id = NULL.
    """
    user_id, error = _require_auth()
    if error:
        return error
    
    conn = get_db()
    
    # Verify show exists
    show = conn.execute(
        "SELECT show_id FROM shows WHERE show_id = ?",
        (show_id,)
    ).fetchone()
    if not show:
        return jsonify({"ok": False, "error": "Show not found"}), 404
    
    payload = request.get_json(force=True, silent=True) or {}
    title = (payload.get("title") or "").strip()
    
    if not title:
        return jsonify({"ok": False, "error": "Discussion title is required"}), 400
    
    try:
        cur = conn.execute(
            """
            INSERT INTO discussions (user_id, movie_id, show_id, title, created_at)
            VALUES (?, NULL, ?, ?, CURRENT_TIMESTAMP)
            """,
            (user_id, show_id, title)
        )
        conn.commit()
        
        discussion_id = cur.lastrowid
        
        # Fetch the created discussion
        row = conn.execute(
            """
            SELECT 
                d.discussion_id,
                d.title,
                d.user_id,
                COALESCE(u.display_name, u.email) AS user_display_name,
                d.created_at,
                d.movie_id,
                d.show_id
            FROM discussions d
            LEFT JOIN users u ON u.user_id = d.user_id
            WHERE d.discussion_id = ?
            """,
            (discussion_id,)
        ).fetchone()
        
        return jsonify({
            "ok": True,
            "discussion": {
                "discussion_id": row["discussion_id"],
                "title": row["title"],
                "user_id": row["user_id"],
                "user_display_name": row["user_display_name"] or "Unknown",
                "created_at": row["created_at"],
                "movie_id": row["movie_id"],
                "show_id": row["show_id"],
                "comment_count": 0
            }
        })
    except sqlite3.Error as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": f"Database error: {str(exc)}"}), 500


# --- Get a single discussion with its comments ---
@app.get("/api/discussions/<int:discussion_id>")
def get_discussion(discussion_id: int):
    """
    Get a single discussion with all its comments.
    Returns discussion details and array of comments.
    """
    conn = get_db()
    
    # Fetch discussion with user info
    discussion_row = conn.execute(
        """
        SELECT 
            d.discussion_id,
            d.title,
            d.user_id,
            COALESCE(u.display_name, u.email) AS user_display_name,
            d.created_at,
            d.movie_id,
            d.show_id
        FROM discussions d
        LEFT JOIN users u ON u.user_id = d.user_id
        WHERE d.discussion_id = ?
        """,
        (discussion_id,)
    ).fetchone()
    
    if not discussion_row:
        return jsonify({"ok": False, "error": "Discussion not found"}), 404
    
    # Fetch comments for this discussion
    comment_rows = conn.execute(
        """
        SELECT 
            c.comment_id,
            c.user_id,
            COALESCE(u.display_name, u.email) AS user_display_name,
            c.content,
            c.created_at
        FROM comments c
        LEFT JOIN users u ON u.user_id = c.user_id
        WHERE c.discussion_id = ?
        ORDER BY c.created_at ASC
        """,
        (discussion_id,)
    ).fetchall()
    
    discussion = {
        "discussion_id": discussion_row["discussion_id"],
        "title": discussion_row["title"],
        "user_id": discussion_row["user_id"],
        "user_display_name": discussion_row["user_display_name"] or "Unknown",
        "created_at": discussion_row["created_at"],
        "movie_id": discussion_row["movie_id"],
        "show_id": discussion_row["show_id"]
    }
    
    comments = [
        {
            "comment_id": row["comment_id"],
            "user_id": row["user_id"],
            "user_display_name": row["user_display_name"] or "Unknown",
            "content": row["content"],
            "created_at": row["created_at"]
        }
        for row in comment_rows
    ]
    
    return jsonify({
        "ok": True,
        "discussion": discussion,
        "comments": comments
    })


# --- Add a comment to a discussion ---
@app.post("/api/discussions/<int:discussion_id>/comments")
def add_discussion_comment(discussion_id: int):
    """
    Add a comment to a discussion.
    Auth: Required
    Body: { "content": "string" }
    """
    user_id, error = _require_auth()
    if error:
        return error
    
    conn = get_db()
    
    # Verify discussion exists
    discussion = conn.execute(
        "SELECT discussion_id FROM discussions WHERE discussion_id = ?",
        (discussion_id,)
    ).fetchone()
    if not discussion:
        return jsonify({"ok": False, "error": "Discussion not found"}), 404
    
    payload = request.get_json(force=True, silent=True) or {}
    content = (payload.get("content") or "").strip()
    
    if not content:
        return jsonify({"ok": False, "error": "Comment content is required"}), 400
    
    try:
        cur = conn.execute(
            """
            INSERT INTO comments (discussion_id, user_id, content, created_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (discussion_id, user_id, content)
        )
        conn.commit()
        
        comment_id = cur.lastrowid
        
        # Fetch the created comment with user info
        row = conn.execute(
            """
            SELECT 
                c.comment_id,
                c.user_id,
                COALESCE(u.display_name, u.email) AS user_display_name,
                c.content,
                c.created_at
            FROM comments c
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.comment_id = ?
            """,
            (comment_id,)
        ).fetchone()
        
        return jsonify({
            "ok": True,
            "comment": {
                "comment_id": row["comment_id"],
                "user_id": row["user_id"],
                "user_display_name": row["user_display_name"] or "Unknown",
                "content": row["content"],
                "created_at": row["created_at"]
            }
        })
    except sqlite3.Error as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": f"Database error: {str(exc)}"}), 500


# --- Delete a discussion (owner or admin only) ---
@app.delete("/api/discussions/<int:discussion_id>")
def delete_discussion(discussion_id: int):
    """
    Delete a discussion. Only the owner can delete their discussion.
    Auth: Required (owner only)
    """
    user_id, error = _require_auth()
    if error:
        return error
    
    conn = get_db()
    
    # Check if discussion exists and user owns it
    discussion = conn.execute(
        "SELECT discussion_id, user_id FROM discussions WHERE discussion_id = ?",
        (discussion_id,)
    ).fetchone()
    
    if not discussion:
        return jsonify({"ok": False, "error": "Discussion not found"}), 404
    
    if discussion["user_id"] != user_id:
        # Check if user is admin
        user = conn.execute(
            "SELECT email FROM users WHERE user_id = ?",
            (user_id,)
        ).fetchone()
        is_admin = user and user["email"] in ("admin@example.com", "admin@plotsignal.com")
        
        if not is_admin:
            return jsonify({"ok": False, "error": "You can only delete your own discussions"}), 403
    
    try:
        # Delete the discussion (CASCADE will delete comments)
        conn.execute(
            "DELETE FROM discussions WHERE discussion_id = ?",
            (discussion_id,)
        )
        conn.commit()
        
        return jsonify({"ok": True, "message": "Discussion deleted"})
    except sqlite3.Error as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": f"Database error: {str(exc)}"}), 500


# --- Delete a comment (owner or admin only) ---
@app.delete("/api/discussions/<int:discussion_id>/comments/<int:comment_id>")
def delete_discussion_comment(discussion_id: int, comment_id: int):
    """
    Delete a comment from a discussion. Only the owner can delete their comment.
    Auth: Required (owner only)
    """
    user_id, error = _require_auth()
    if error:
        return error
    
    conn = get_db()
    
    # Check if comment exists and belongs to this discussion
    comment = conn.execute(
        "SELECT comment_id, user_id FROM comments WHERE comment_id = ? AND discussion_id = ?",
        (comment_id, discussion_id)
    ).fetchone()
    
    if not comment:
        return jsonify({"ok": False, "error": "Comment not found"}), 404
    
    if comment["user_id"] != user_id:
        # Check if user is admin
        user = conn.execute(
            "SELECT email FROM users WHERE user_id = ?",
            (user_id,)
        ).fetchone()
        is_admin = user and user["email"] in ("admin@example.com", "admin@plotsignal.com")
        
        if not is_admin:
            return jsonify({"ok": False, "error": "You can only delete your own comments"}), 403
    
    try:
        conn.execute(
            "DELETE FROM comments WHERE comment_id = ?",
            (comment_id,)
        )
        conn.commit()
        
        return jsonify({"ok": True, "message": "Comment deleted"})
    except sqlite3.Error as exc:
        conn.rollback()
        return jsonify({"ok": False, "error": f"Database error: {str(exc)}"}), 500


if __name__ == "__main__":
    app.run(debug=True)

