# /movie-tv-analytics/app/routes/public.py
import sqlite3

from flask import Blueprint, jsonify, request, current_app
from werkzeug.security import generate_password_hash

from ..services import ingest_trending_and_top, compute_summary, list_items, search_live

bp = Blueprint("public", __name__, url_prefix="/api")


@bp.get("/health")
def health():
    # Minimal proof that the server is up and responding
    return jsonify({"status": "healthy"})


@bp.post("/refresh")
def refresh():
    """Ingest one page of trending + top-rated movies/TV from TMDb into the local DB."""
    conn = current_app.session()
    result = ingest_trending_and_top(conn=conn, pages=int(request.args.get("pages", 1)))
    return jsonify({"ok": True, **result})


@bp.get("/summary")
def summary():
    conn = current_app.session()
    data = compute_summary(conn)
    return jsonify(data)


@bp.get("/movies")
def movies():
    conn = current_app.session()
    sort = request.args.get("sort", "popularity")
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 20))
    data = list_items(conn, media_type="movie", sort=sort, page=page, limit=limit)
    return jsonify(data)


@bp.get("/tv")
def tv():
    conn = current_app.session()
    sort = request.args.get("sort", "popularity")
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 20))
    data = list_items(conn, media_type="tv", sort=sort, page=page, limit=limit)
    return jsonify(data)


@bp.get("/search")
def search():
    q = request.args.get("q", "")
    page = int(request.args.get("page", 1))
    data = search_live(q, page)
    return jsonify(data)


@bp.get("/users")
def users():
    """Return stored user accounts for display (demo: show plaintext).

    WARNING: In a real application you would NEVER return plaintext passwords.
    This is done solely to satisfy the current demo requirement.
    """
    conn = current_app.session()
    rows = conn.execute(
        "SELECT email, password_plain FROM users ORDER BY id ASC"
    ).fetchall()

    def username(email: str) -> str:
        try:
            return email.split("@", 1)[0] if "@" in email else email
        except Exception:
            return email

    resp = jsonify([
        {
            "user": username(row["email"]),
            "email": row["email"],
            "password": row["password_plain"] or "",
        }
        for row in rows
    ])
    # Prevent browsers from caching demo credentials
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.post("/login")
def login():
    """Very simple demo login: match email AND plaintext password.

    WARNING: Real authentication should verify a hashed password and issue a
    session or token. This is a simplified version for the current demo.
    """
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip()
    password = (data.get("password") or "").strip()
    if not email or not password:
        return jsonify({"ok": False, "error": "Missing email or password"}), 400

    try:
        conn = current_app.session()
        row = conn.execute(
            "SELECT email, password_plain FROM users WHERE lower(email) = lower(?) LIMIT 1",
            (email,),
        ).fetchone()
        if not row or (row["password_plain"] or "") != password:
            return jsonify({"ok": False, "error": "Invalid credentials"}), 401

        uname = email.split("@", 1)[0] if "@" in email else email
        return jsonify({"ok": True, "user": uname, "email": email})
    except Exception as e:
        current_app.logger.exception("/api/login failed")
        return jsonify({"ok": False, "error": f"server-error: {str(e)}"}), 500


@bp.post("/signup")
def signup():
    """Create a new demo user with plaintext password."""
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip()
    password = (data.get("password") or "").strip()
    if not email or not password:
        return jsonify({"ok": False, "error": "Missing email or password"}), 400

    conn = current_app.session()
    try:
        existing = conn.execute(
            "SELECT 1 FROM users WHERE lower(email) = lower(?) LIMIT 1",
            (email,),
        ).fetchone()
        if existing:
            return jsonify({"ok": False, "error": "Email already exists"}), 409

        hashed = generate_password_hash(password)
        conn.execute(
            "INSERT INTO users (email, password_hash, password_plain) VALUES (?, ?, ?)",
            (email, hashed, password),
        )
        conn.commit()

        uname = email.split("@", 1)[0] if "@" in email else email
        return jsonify({"ok": True, "user": uname, "email": email})
    except sqlite3.IntegrityError:
        conn.rollback()
        return jsonify({"ok": False, "error": "Email already exists"}), 409
    except Exception as e:
        conn.rollback()
        current_app.logger.exception("/api/signup failed")
        return jsonify({"ok": False, "error": f"server-error: {str(e)}"}), 500


@bp.get("/__routes")
def list_routes():
    """Debug helper: list all registered URL rules."""
    rules = [r.rule for r in current_app.url_map.iter_rules()]
    return jsonify(sorted(rules))
