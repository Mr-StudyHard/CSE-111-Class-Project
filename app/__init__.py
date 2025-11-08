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

    # 1) Configure DB (pin to an absolute file so it's consistent regardless of CWD)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    default_sqlite_path = os.path.abspath(os.path.join(project_root, 'app.db'))
    db_url = os.getenv("DATABASE_URL", f"sqlite:///{default_sqlite_path}")
    engine = create_engine(db_url, future=True)
    Session = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))
    app.session = Session  # attach session to app for easy access in routes

    # Create tables on first run (SQLite-friendly). In production, prefer Alembic.
    Base.metadata.create_all(bind=engine)
    # Ensure demo column exists (password_plain) for users table
    try:
        with engine.connect() as conn:
            cols = conn.execute(text("PRAGMA table_info(users)")).fetchall()
            col_names = {c[1] for c in cols}  # (cid, name, type, notnull, dflt_value, pk)
            if "password_plain" not in col_names:
                conn.execute(text("ALTER TABLE users ADD COLUMN password_plain VARCHAR(255)"))
    except Exception:
        pass

    # Seed or normalize the Admin user: Email "Admin@Test.com", Password "Admin" (also store plaintext for demo)
    # If an older seed with email "Admin" exists, update it to the new email.
    try:
        from sqlalchemy import select
        admin_email = "Admin@Test.com"
        with engine.begin() as conn:
            # Fetch all admin candidates
            old_admin = conn.execute(select(User).where(User.email == "Admin")).first()
            new_admin = conn.execute(select(User).where(User.email == admin_email)).first()
            if new_admin:
                # Already seeded; ensure plaintext is set for demo if missing
                user = new_admin[0]
                conn.execute(User.__table__.update().where(User.id == user.id).values(password_plain="Admin"))
            elif old_admin:
                # Perform direct SQL UPDATE to change email
                user_id = old_admin[0].id
                conn.execute(User.__table__.update().where(User.id == user_id).values(email=admin_email, password_plain="Admin"))
            else:
                pwd = generate_password_hash("Admin")
                conn.execute(User.__table__.insert().values(email=admin_email, password_hash=pwd, password_plain="Admin"))

            # Safety: backfill plaintext for any existing users missing it (demo requirement)
            conn.execute(User.__table__.update().where(User.password_plain == None).values(password_plain="Admin"))
    except Exception:
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
