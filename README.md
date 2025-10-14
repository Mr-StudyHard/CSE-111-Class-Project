# CSE-111 Class Project — Movie & TV Analytics (Flask + Vite)

A small full-stack project that ingests data from TMDb and exposes it via a Flask API, with a Vite (React + TypeScript) frontend.


**Current status:** Backend and frontend scaffolds are wired.  
- Flask serves `/api/*` (e.g., `/api/health`)  
- Vite dev server proxies `/api/*` → Flask during development

## Stack
- **Backend:** Python 3.10+, Flask, SQLAlchemy
- **Frontend:** Vite, React, TypeScript, Axios
- **DB:** SQLite (dev). Swappable via `DATABASE_URL`
- **Env:** `.env` loaded via `python-dotenv`
- **ETL (planned):** TMDb API

## Environment Variables

Create a `.env` file in the repo root:

```env
# Flask
FLASK_ENV=development
DATABASE_URL=sqlite:///app.db

# TMDb (for upcoming ETL)
TMDB_API_KEY=your_key_here

```
.env is git-ignored.
