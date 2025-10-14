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

## How everything is routed at the moment:

```
CSE-111-Class-Project/
├─ app/
│ ├─ init.py # Flask app factory; DB engine/session; registers blueprints and /api/ping
│ └─ routes/
│ └─ public.py # Blueprint mounted at /api (e.g., /api/health)
├─ web/ # Vite React TypeScript frontend
│ ├─ src/
│ │ ├─ App.tsx # Calls /api/health and renders the response
│ │ └─ api.ts # Axios instance (baseURL=/api)
│ └─ vite.config.ts # Dev proxy: /api -> http://127.0.0.1:5000

├─ .env # Local environment values (ignored by git)
├─ .gitignore # Ignores venv, node_modules, .env, app.db, etc.
└─ README.md
```
## Environment Variables

Create a `.env` file in the repo root:

```env
# Flask
FLASK_ENV=development
DATABASE_URL=sqlite:///app.db

# TMDb (for upcoming ETL)
TMDB_API_KEY=your_key_here

```
`.env` is git-ignored.


