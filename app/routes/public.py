# /movie-tv-analytics/app/routes/public.py
from flask import Blueprint, jsonify, request, current_app
from sqlalchemy import func

from ..services import ingest_trending_and_top, compute_summary, list_items, search_live
from ..models import User
from werkzeug.security import generate_password_hash

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


@bp.get("/users")
def users():
    """Return stored user accounts for display (demo: show plaintext).

    WARNING: In a real application you would NEVER return plaintext passwords.
    This is done solely to satisfy the current demo requirement.
    """
    session = current_app.session()
    rows = session.query(User).order_by(User.id.asc()).all()

    def username(email: str) -> str:
        try:
            return email.split("@", 1)[0] if "@" in email else email
        except Exception:
            return email

    resp = jsonify([
        {
            "user": username(u.email),
            "email": u.email,
            "password": u.password_plain or "",
        }
        for u in rows
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
        session = current_app.session()
        # Case-insensitive email match for convenience
        user = session.query(User).filter(func.lower(User.email) == email.lower()).first()
        if not user:
            return jsonify({"ok": False, "error": "Invalid credentials"}), 401
        # Compare plaintext demo password
        if (user.password_plain or "") != password:
            return jsonify({"ok": False, "error": "Invalid credentials"}), 401

        # Success: return minimal profile
        uname = email.split("@",1)[0] if "@" in email else email
        return jsonify({"ok": True, "user": uname, "email": email})
    except Exception as e:
        current_app.logger.exception("/api/login failed")
        return jsonify({"ok": False, "error": f"server-error: {str(e)}"}), 500


@bp.post("/signup")
def signup():
    """Create a new demo user with plaintext password.

    Body: {"email": str, "password": str, "username": str?}
    Returns: {ok: true, user, email} or {ok:false, error}

    NOTE: This is a simplified demo.
    """
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip()
    password = (data.get("password") or "").strip()
    if not email or not password:
        return jsonify({"ok": False, "error": "Missing email or password"}), 400

    try:
        session = current_app.session()
        # Enforce unique email (case-insensitive)
        existing = session.query(User).filter(func.lower(User.email) == email.lower()).first()
        if existing:
            return jsonify({"ok": False, "error": "Email already exists"}), 409

        # Populate both hashed and plaintext (demo) columns to satisfy NOT NULL constraint
        hashed = generate_password_hash(password)
        u = User(email=email, password_hash=hashed, password_plain=password)
        session.add(u)
        session.commit()

        uname = email.split("@",1)[0] if "@" in email else email
        return jsonify({"ok": True, "user": uname, "email": email})
    except Exception as e:
        session.rollback()
        current_app.logger.exception("/api/signup failed")
        return jsonify({"ok": False, "error": f"server-error: {str(e)}"}), 500


@bp.get("/__routes")
def list_routes():
    """Debug helper: list all registered URL rules."""
    rules = [r.rule for r in current_app.url_map.iter_rules()]
    return jsonify(sorted(rules))
