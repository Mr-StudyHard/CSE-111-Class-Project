from __future__ import annotations

from typing import Any

from flask import Flask, jsonify, request

from .db import close_db, get_db, query, execute

app = Flask(__name__)
app.teardown_appcontext(close_db)


def _dicts(rows):
    return [dict(row) for row in rows]


@app.get("/api/search")
def search_catalog():
    term = (request.args.get("q") or "").strip()
    if not term:
        return jsonify([])
    like = f"%{term.lower()}%"
    rows = query(
        """
        SELECT 'movie' AS target_type,
               movie_id AS target_id,
               title,
               tmdb_vote_avg,
               release_year AS year_or_date
        FROM movies
        WHERE lower(title) LIKE ?
        UNION ALL
        SELECT 'show' AS target_type,
               show_id AS target_id,
               title,
               tmdb_vote_avg,
               first_air_date AS year_or_date
        FROM shows
        WHERE lower(title) LIKE ?
        ORDER BY tmdb_vote_avg DESC NULLS LAST, title
        LIMIT 50
        """,
        (like, like),
    )
    return jsonify(_dicts(rows))


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
    try:
        rating_value = float(rating)
    except (TypeError, ValueError):
        return jsonify({"error": "rating must be numeric"}), 400
    if not (0 <= rating_value <= 10):
        return jsonify({"error": "rating must be between 0 and 10"}), 400

    conn = get_db()
    if target_type == "movie":
        sql = """
            INSERT INTO reviews (user_id, movie_id, rating, content)
            VALUES (?, ?, ?, ?)
        """
        params = (user_id, target_id, rating_value, content or None)
    else:
        sql = """
            INSERT INTO reviews (user_id, show_id, rating, content)
            VALUES (?, ?, ?, ?)
        """
        params = (user_id, target_id, rating_value, content or None)

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


if __name__ == "__main__":
    app.run(debug=True)

