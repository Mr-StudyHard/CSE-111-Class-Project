# /movie-tv-analytics/app/init.py
from flask import Flask
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
import os

def create_app():
    app = Flask(__name__)

    # 1) Configure DB (reads .env via your shell; no write yet)
    db_url = os.getenv("DATABASE_URL", "sqlite:///app.db")
    engine = create_engine(db_url, future=True)
    Session = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))
    app.session = Session  # attach session to app for easy access in routes

    # 2) Register routes (blueprints keep code organized)
    from .routes.public import bp as public_bp
    app.register_blueprint(public_bp)

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
