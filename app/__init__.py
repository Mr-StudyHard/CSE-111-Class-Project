# This module remains to support legacy imports (`app:create_app`), but the new
# Project 2 backend lives in `backend/api_catalog.py`. We simply re-export that
# Flask application so existing entry points (e.g., `python run_server.py`) keep
# working without the old ORM stack.
from backend.api_catalog import app as backend_app  # type: ignore[attr-defined]


def create_app():
    return backend_app
