# /movie-tv-analytics/app/init.py
import os
import sqlite3

from flask import Flask, g
from flask_cors import CORS
from werkzeug.security import generate_password_hash

from .models import ensure_admin_user, ensure_password_plain_column, init_db


def create_app():
    app = Flask(__name__)

    # 1) Configure DB (pin to an absolute file so it's consistent regardless of CWD)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    default_sqlite_path = os.path.abspath(os.path.join(project_root, 'movie_tracker.db'))
    db_path = os.getenv("DATABASE_PATH", default_sqlite_path)
    app.config["DATABASE_PATH"] = db_path

    def get_db() -> sqlite3.Connection:
        if "db_connection" not in g:
            conn = sqlite3.connect(app.config["DATABASE_PATH"])
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            g.db_connection = conn
        return g.db_connection

    app.session = get_db  # maintain backwards compatibility with previous code

    with app.app_context():
        conn = get_db()
        init_db(conn)
        ensure_password_plain_column(conn)
        ensure_admin_user(conn, generate_password_hash)

    # 2) Register routes (blueprints keep code organized)
    from .routes.public import bp as public_bp

    app.register_blueprint(public_bp)

    # 2.5) CORS for when not using Vite proxy (e.g., prod/preview)
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    @app.teardown_appcontext
    def close_db(exc):
        conn = g.pop("db_connection", None)
        if conn is not None:
            conn.close()

    # 4) Optional: ping route to confirm DB connectivity
    @app.get("/api/ping")
    def ping():
        try:
            conn = get_db()
            conn.execute("SELECT 1")
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}, 500

    return app
