# Project 2 Quickstart Guide

This document explains how to work with the raw-SQL Movie & Show Tracker stack without assuming any prior context.

---

## 1. Repository Layout

```
movie-tv-analytics/
├─ backend/
│  ├─ __init__.py
│  ├─ api_catalog.py        # Flask API (search, details, reviews, watchlists)
│  └─ db.py                 # sqlite3 helpers (get_db, query, execute)
├─ db/
│  ├─ schema.sql            # Database schema (tables, PK/FK, indexes)
│  └─ seed.sql              # Base dataset covering every table
├─ queries/
│  └─ use_cases.sql         # 30 runnable SQL statements (analytics + CRUD)
├─ scripts/
│  ├─ reset_db.sh           # Reset → seed → sample queries (optional ETL)
│  └─ tmdb_etl.py           # TMDb ingest loader (movies/shows/seasons/cast)
├─ env.example              # Copy to .env and set TMDB_API_KEY
├─ movie_tracker.db         # SQLite database (created by reset_db.sh)
├─ app/                     # Legacy entry point → re-exports backend API
├─ run_server.py            # Launches the Flask API (uses backend/api_catalog)
└─ web/                     # Vite/React frontend (unchanged from Project 1)
```

The new work happens inside `backend/`, `db/`, `scripts/`, and `queries/`. The legacy `app/` package now simply returns the new backend so existing commands (`python run_server.py`) continue to work.

---

## 2. TL;DR Commands

All commands run from the repository root (`movie-tv-analytics/`).

| Task | Command |
|------|---------|
| Install Python deps | `pip install -r requirements.txt` |
| Reset + seed the database | `bash scripts/reset_db.sh` |
| Copy env template | `cp env.example .env` |
| Fill TMDb key | `echo "TMDB_API_KEY=YOUR_KEY" >> .env` |
| Run TMDb ETL (optional) | `python scripts/tmdb_etl.py --movies 20 --shows 10 --episodes-per-season 5` |
| Re-run sample queries | `sqlite3 movie_tracker.db ".read queries/use_cases.sql"` |
| Launch Flask API | `FLASK_APP=backend/api_catalog.py flask run` **or** `python run_server.py` |
| Launch Vite frontend | `cd web && npm install && npm run dev` |

When `RUN_TMDB_ETL=1` is set, `scripts/reset_db.sh` will automatically run the loader after seeding.

---

## 3. Database Lifecycle

1. **Create** – `db/schema.sql` builds all tables exactly as required (users, movies, shows, seasons, episodes, genres, people, cast, reviews, discussions, comments, watchlists). Indices and constraints mirror the ERD.
2. **Seed** – `db/seed.sql` inserts:
   - 4 demo users & 10 genres
   - 6 movies, 3 shows, 2 seasons per show, 3 episodes per season
   - Basic cast, reviews, discussions, comments, watchlists
3. **ETL** – `scripts/tmdb_etl.py` uses the TMDb API to upsert genres, movies, shows, seasons, episodes, and cast via prepared sqlite3 statements (`INSERT ... ON CONFLICT`). Safe to re-run without wiping data.
4. **Queries** – `queries/use_cases.sql` contains 30 labelled statements demonstrating search, analytics, CRUD, watchlist ops, and cast management. They’re meant to run directly in sqlite3 (`sqlite3 movie_tracker.db ".read queries/use_cases.sql"`).

All runtime code uses `PRAGMA foreign_keys = ON` and raw SQL exclusively—no ORM.

---

## 4. Backend API (Flask)

Running `FLASK_APP=backend/api_catalog.py flask run` (or `python run_server.py`) gives you:

| Endpoint | Description |
|----------|-------------|
| `GET /api/search?q=title` | Union search across movies + shows (case-insensitive) |
| `GET /api/movie/<id>` | Movie info + genres + average user rating + TMDb rating + top cast |
| `GET /api/show/<id>` | Show info + season count + average user rating + top cast |
| `GET /api/show/<id>/seasons` | Nested seasons with ordered episodes + runtimes |
| `POST /api/reviews` | Add review for movie or show (expects JSON payload) |
| `POST /api/watchlist` | Add movie/show to a user’s watchlist |
| `DELETE /api/watchlist` | Remove from watchlist |

Every endpoint builds SQL using prepared statements through `backend/db.py`. The legacy `app/__init__.py` now simply re-exports this Flask app for compatibility.

---

## 5. Frontend Notes

The Vite/React UI lives under `web/` and still proxies `/api` → `127.0.0.1:5000` in development. After starting the Flask server, run `npm run dev` inside `web/` and browse to `http://localhost:5173`.

---

## 6. Troubleshooting

- **“No such column password_hash”** – Ensure you’re using the new schema (`scripts/reset_db.sh`) and not the old `app.db`. The default DB is `movie_tracker.db`.
- **`sqlite3` errors about `NULLS LAST`** – Fixed in the current query set; rerun `git pull` if you still see them.
- **ETL fails with HTTP 401/403** – Check that `.env` contains a valid `TMDB_API_KEY` and that your network allows outbound HTTPS to `api.themoviedb.org`.
- **Running from Windows (OneDrive path)** – All scripts quote file paths, but if you run into issues, try running from a non-synced directory.

---

## 7. Extensibility Tips

- To extend the API, add new SQL helpers in `backend/db.py`, then create routes in `backend/api_catalog.py`.
- When adjusting schema, edit `db/schema.sql` first, then regenerate the DB via `scripts/reset_db.sh`.
- Add more analytics queries by appending `-- Q31`, `-- Q32`, etc. to `queries/use_cases.sql`.

Feel free to reach out to the team if anything remains unclear—this guide should give newcomers everything they need to bootstrap the project.*** End Patch

