# Movie & Show Stats Tracker

This project now includes the Project 2 deliverables: a **raw-SQL** SQLite database, schema + seed files, a TMDb ETL loader, 30+ analytics queries, and a Flask API layer that never touches an ORM. Everything runs against `movie_tracker.db`.

## Stack
- **Backend:** Python 3.10+, Flask, `sqlite3` (standard library)
- **Frontend:** Vite, React, TypeScript, Axios
- **Database:** SQLite (`movie_tracker.db`, override with `DATABASE_PATH`)
- **ETL:** TMDb REST API via `scripts/tmdb_etl.py`
- **Env:** `python-dotenv`

```
movie-tv-analytics/
├─ backend/
│  ├─ api_catalog.py   # Read/write API endpoints (raw SQL)
│  └─ db.py            # sqlite helpers (get_db/query/execute)
├─ db/
│  ├─ schema.sql       # Tables, PK/FK, indexes
│  └─ seed.sql         # Base dataset covering every table
├─ queries/
│  └─ use_cases.sql    # ≥30 runnable SQL statements
├─ scripts/
│  ├─ reset_db.sh      # Drop + recreate + seed + run queries
│  └─ tmdb_etl.py      # TMDb ETL with UPSERT logic
├─ env.example         # Copy to .env and set TMDB_API_KEY
├─ movie_tracker.db    # Generated SQLite database
├─ web/                # React/Vite frontend (unchanged)
└─ README.md
```

## Prerequisites
- Python 3.10+
- Node 18+ (frontend)
- `pip install -r requirements.txt`

## Quick Start (one command per step)

| Step | Command |
|------|---------|
| Install Python deps | `pip install -r requirements.txt` |
| Reset + seed DB | `bash scripts/reset_db.sh` |
| Copy env template | `cp env.example .env` |
| Run TMDb ETL* | `python scripts/tmdb_etl.py --movies 20 --shows 10 --episodes-per-season 5` |
| Re-run sample queries | `sqlite3 movie_tracker.db ".read queries/use_cases.sql"` |
| Start API (Flask) | `FLASK_APP=backend/api_catalog.py flask run` |
| Start frontend (Vite) | `cd web && npm install && npm run dev` |

\*Requires `TMDB_API_KEY` in `.env`.

### Environment Variables
`env.example` contains the required keys:
```
TMDB_API_KEY=your_tmdb_api_key_here
# Optional:
# DATABASE_PATH=/absolute/path/to/movie_tracker.db
# FLASK_ENV=development
```
Copy it to `.env` and fill in the values before running the ETL or API.

For a step-by-step walkthrough (with screenshots and troubleshooting notes) see [`docs/PROJECT2_QUICKSTART.md`](docs/PROJECT2_QUICKSTART.md).

## Database Deliverables
1. **Schema (`db/schema.sql`)** – Implements the provided ERD with raw SQL, PK/FK constraints, cascade rules, and useful indexes.
2. **Seed (`db/seed.sql`)** – Populates users, movies, shows, seasons, episodes, genres, junction tables, people, cast, reviews, discussions, comments, and watchlists.
3. **ETL (`scripts/tmdb_etl.py`)** – Uses requests + sqlite3 prepared statements to upsert genres, movies, shows, seasons, episodes, and cast from TMDb. Idempotent by design.
4. **Use-case SQL (`queries/use_cases.sql`)** – 30 statements covering search, analytics, DML operations, cast management, and KPIs. Each query is labeled `-- Q#`.
5. **Reset script (`scripts/reset_db.sh`)** – Recreates `movie_tracker.db`, seeds it, optionally runs the ETL (`RUN_TMDB_ETL=1 bash scripts/reset_db.sh`), and executes the use-case statements for validation.
6. **Backend helpers (`backend/*.py`)** – `db.py` exposes raw-SQL helpers; `api_catalog.py` provides REST endpoints for search, detail pages, reviews, and watchlists (all raw SQL).

## API Catalog (Flask)
Endpoints exposed by `backend/api_catalog.py`:
- `GET /api/search?q=...` – combined movie/show search
- `GET /api/movie/<id>` – movie details + genres + avg ratings + top cast
- `GET /api/show/<id>` – show details + season count + top cast
- `GET /api/show/<id>/seasons` – seasons with nested episodes
- `POST /api/reviews` – insert review for movie or show
- `POST /api/watchlist` – add movie/show to watchlist
- `DELETE /api/watchlist` – remove movie/show from watchlist

Run with `FLASK_APP=backend/api_catalog.py flask run`, then hit `http://127.0.0.1:5000/api/...`.

### Query Coverage Checklist

The 30 reference queries in `queries/use_cases.sql` break down as follows:

- [x] **Q1** – Search movies and shows by title (powering `GET /api/search`)
- [x] **Q2** – Movie details with averages & genres (used in `GET /api/movie/<id>`)
- [x] **Q3** – Show details with season count (used in `GET /api/show/<id>`)
- [x] **Q4** – Episode listing by season (used in `GET /api/show/<id>/seasons`)
- [ ] **Q5** – Top movies by average user rating
- [ ] **Q6** – Top shows by average user rating
- [x] **Q7** – Movie genre distribution (summarised in `/api/summary`)
- [ ] **Q8** – Show genre distribution
- [ ] **Q9** – Titles with no user reviews
- [ ] **Q10** – Most discussed titles (comment totals)
- [x] **Q11** – Insert movie into watchlist (via `POST /api/watchlist`)
- [x] **Q12** – Insert show into watchlist (via `POST /api/watchlist`)
- [x] **Q13** – Delete watchlist entry (via `DELETE /api/watchlist`)
- [x] **Q14** – Insert review (via `POST /api/reviews`)
- [ ] **Q15** – Update review for movie
- [ ] **Q16** – Delete review for movie
- [ ] **Q17** – Create discussion for show
- [ ] **Q18** – Insert comment into discussion
- [ ] **Q19** – Delete empty discussions
- [ ] **Q20** – Upsert genre (admin operation)
- [ ] **Q21** – Upsert person by TMDb id
- [ ] **Q22** – Attach cast to movie
- [ ] **Q23** – Attach cast to show
- [x] **Q24** – Top cast for movie (returned from `GET /api/movie/<id>`)
- [x] **Q25** – Top cast for show (returned from `GET /api/show/<id>`)
- [ ] **Q26** – Compare user vs TMDb averages per movie
- [ ] **Q27** – Average user rating per genre
- [ ] **Q28** – Users with ≥3 reviews
- [ ] **Q29** – Watchlist counts per title
- [ ] **Q30** – Recently added titles

Checked entries are already exposed through the backend/UI; unchecked items remain available as future enhancements.

## Frontend
The Vite/React app remains the same as earlier coursework. Start it with `npm run dev` inside `web/`. The dev server proxies `/api` to the Flask backend on `127.0.0.1:5000`.

## Notes
- All persistence uses `sqlite3` with prepared statements—no SQLAlchemy or other ORMs.
- The ETL enables `PRAGMA foreign_keys = ON` and uses `INSERT ... ON CONFLICT` so it can be rerun safely.
- Change the database location by exporting `DATABASE_PATH` before running any scripts.
- `queries/use_cases.sql` can be executed wholesale (as in `reset_db.sh`) or cherry-picked for analytics dashboards.

Happy exploring!  
