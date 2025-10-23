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

## Getting Started (Development) 

0) Prerequisites
   - Python 3.10+
   - Node 18+ and npm
1) Python Environment
   ```
   python -m venv .venv
   # Windows: .venv\Scripts\activate
   source .venv/bin/activate
    
   # install backend deps (or use requirements.txt if present)
   pip install flask sqlalchemy python-dotenv requests
   ```
2) Run the Flask API (port 5000)
   ```
   # from repo root
   # mac/linux:
   export FLASK_APP="app:create_app"
   flask run
   # windows powershell:
    # $env:FLASK_APP="app:create_app"; flask run
   ```
   Check if the following outputs are in the routes:
     http://127.0.0.1:5000/api/health → `{"status":"healthy"}`
  
     http://127.0.0.1:5000/api/ping → tests DB connectivity (`SELECT 1`)
3) Run the Vite frontend (port 5173)
   ```
   cd web
   npm install
   npm run dev
   ```
   Open: http://localhost:5173
   The page calls `/api/health via` Vite’s proxy → Flask.


## UML Diagram 
<img width="1920" height="1080" alt="CSE 111 UML (1)" src="https://github.com/user-attachments/assets/b002f0ea-05ed-4377-9628-3becb561a3e0" />

## ER Diagram
<img width="2091" height="1991" alt="ER drawio (5)" src="https://github.com/user-attachments/assets/e8b15eac-4422-4f1b-8164-27811b7dddae" />
 



