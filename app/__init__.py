# /movie-tv-analytics/app/init.py
from flask import Flask
from flask_cors import CORS
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
import os

# Lazy import models so this file stays small
from .models import Base, User  # ensures models are registered for metadata.create_all()
from werkzeug.security import generate_password_hash

def create_app():
    app = Flask(__name__)

    # 1) Configure DB (reads .env via your shell; no write yet)
    db_url = os.getenv("DATABASE_URL", "sqlite:///app.db")
    engine = create_engine(db_url, future=True)
    Session = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))
    app.session = Session  # attach session to app for easy access in routes

    # Create tables on first run (SQLite-friendly). In production, prefer Alembic.
    Base.metadata.create_all(bind=engine)

    # Seed an Admin user (email="Admin", password="Admin") if not present
    try:
        with engine.begin() as conn:
            from sqlalchemy import select
            res = conn.execute(select(User).where(User.email == "Admin")).first()
            if not res:
                pwd = generate_password_hash("Admin")
                conn.execute(
                    User.__table__.insert().values(email="Admin", password_hash=pwd)
                )
    except Exception:
        # Non-fatal; app should still run even if seeding fails
        pass

    # 2) Register routes (blueprints keep code organized)
    from .routes.public import bp as public_bp
    app.register_blueprint(public_bp)

    # 2.5) CORS for when not using Vite proxy (e.g., prod/preview)
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    # 3) Cleanup session per request
    @app.teardown_appcontext
    def remove_session(exc):
        Session.remove()

    # 4) Optional: ping route to confirm DB connectivity
    @app.get("/api/ping")
    def ping():
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}, 500

    return app
