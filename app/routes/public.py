# /movie-tv-analytics/app/routes/public.py
from flask import Blueprint, jsonify

bp = Blueprint("public", __name__, url_prefix="/api")

@bp.get("/health")
def health():
    # Minimal proof that the server is up and responding
    return jsonify({"status": "healthy"})
