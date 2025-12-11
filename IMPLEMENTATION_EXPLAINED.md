# Implementation Details - Simple Explanation

## The Big Picture
Your app has **3 main parts** that talk to each other:
1. **Frontend** (what users see) → React + TypeScript
2. **Backend** (the server) → Flask REST API  
3. **Database** (where data lives) → SQLite
4. **ETL** (data fetcher) → APScheduler + TMDb API

---

## 1. Flask REST API (Backend Server)

**What it is:** A Python web server that handles requests and talks to the database.

**What it does:**
- Receives requests from the frontend (like "get me movie #123")
- Queries the SQLite database
- Returns JSON data back to the frontend

**Example from your code:**
```python
# backend/api_catalog.py
@app.get("/api/movie/<id>")
def get_movie(id):
    # Query database for movie
    # Return JSON response
```

**Why "REST API"?** It follows REST principles:
- `GET /api/movies` = get list of movies
- `GET /api/movie/123` = get specific movie
- `POST /api/reviews` = create a review
- Standard way for frontend ↔ backend communication

---

## 2. TMDb API Integration

**What it is:** External service (The Movie Database) that has movie/show data.

**What it does:**
- Your ETL service calls TMDb API to get movie/show information
- Downloads metadata (title, cast, genres, ratings, etc.)
- Stores it in your SQLite database

**Example flow:**
1. ETL runs → calls `https://api.themoviedb.org/3/movie/123`
2. Gets JSON response with movie data
3. Inserts/updates your database

**Why needed?** You don't manually enter all movie data - you fetch it from TMDb!

---

## 3. React + TypeScript Frontend

**What it is:** The user interface that runs in the browser.

**React:** JavaScript library for building interactive UIs
- Components (reusable pieces like MovieCard, ReviewForm)
- State management (tracks what user is viewing/doing)

**TypeScript:** JavaScript with types
- Catches errors before runtime
- Better code completion and documentation

**Vite:** Build tool
- Fast development server
- Bundles your code for production
- Proxies API calls to Flask backend

**Example from your code:**
```typescript
// web/src/api.ts
const api = axios.create({ baseURL: '/api' })

// Frontend calls backend
const movies = await api.get('/movies')
```

**How it works:**
1. User clicks "View Movies" in browser
2. React component calls `api.get('/api/movies')`
3. Vite proxy forwards to Flask server (port 5000)
4. Flask queries database and returns JSON
5. React displays the data

---

## 4. Features Implemented

These are the **user-facing features** your app supports:

- **Reviews:** Users can rate and write reviews for movies/shows
- **Comments:** Users can comment on discussions
- **Discussions:** Users can start discussion threads about movies/shows
- **Watchlists:** Users can save movies/shows to watch later
- **Favorites:** Users can mark movies/shows as favorites
- **Reactions:** Users can react to reviews (👍, ❤️, etc.)

**Each feature = database tables + API endpoints + frontend UI**

---

## 5. Database Schema (17 Tables)

**What it is:** The structure of your SQLite database.

**17 Tables:**
1. `users` - user accounts
2. `movies` - movie data
3. `shows` - TV show data
4. `seasons` - TV seasons
5. `episodes` - TV episodes
6. `genres` - genre categories
7. `people` - actors/directors
8. `movie_genres` - links movies to genres (M:N)
9. `show_genres` - links shows to genres (M:N)
10. `movie_cast` - links movies to actors (M:N)
11. `show_cast` - links shows to actors (M:N)
12. `reviews` - user reviews
13. `discussions` - discussion threads
14. `comments` - comments on discussions
15. `watchlists` - user watchlists
16. `favorites` - user favorites
17. `review_reactions` - reactions to reviews

**Composite Keys:** Some tables use multiple columns as primary key
- Example: `movie_genres` has PK `(movie_id, genre_id)`
- Prevents duplicates: can't add same genre to same movie twice
- More efficient than separate unique constraint

**M:N Junction Tables:** Tables that connect two other tables
- `movie_genres` connects `movies` ↔ `genres`
- `movie_cast` connects `movies` ↔ `people`
- Needed because: one movie has many genres, one genre has many movies

---

## 6. Identifying Relationships

**What it means:** A child entity **cannot exist** without its parent.

**Examples in your schema:**

**Seasons → Shows:**
- A season MUST belong to a show
- If you delete a show, all its seasons are deleted (CASCADE)
- `seasons.show_id` is required (NOT NULL)

**Episodes → Seasons:**
- An episode MUST belong to a season
- If you delete a season, all its episodes are deleted (CASCADE)
- `episodes.season_id` is required (NOT NULL)

**Why important?** Ensures data integrity - you can't have orphaned seasons or episodes.

---

## 7. APScheduler ETL

**What it is:** Automated job scheduler that runs ETL tasks on a schedule.

**ETL = Extract, Transform, Load:**
- **Extract:** Get data from TMDb API
- **Transform:** Clean and format the data
- **Load:** Insert into your database

**APScheduler:** Python library that runs jobs automatically
- Can run every X hours
- Can run at specific times (cron)
- Runs in background

**Example from your code:**
```python
# etl/scheduler.py
scheduler.add_job(
    self.run_etl_job,
    trigger=IntervalTrigger(hours=24),  # Run every 24 hours
    id='tmdb_etl_job'
)
```

**Why needed?** Keeps your database updated with latest movies/shows without manual work.

---

## How Everything Works Together

```
┌─────────────┐
│   Browser   │  User interacts with React frontend
│  (React UI) │
└──────┬──────┘
       │ HTTP requests (GET/POST)
       ▼
┌─────────────┐
│ Flask API   │  Receives requests, queries database
│  (Backend)  │  Returns JSON responses
└──────┬──────┘
       │ SQL queries
       ▼
┌─────────────┐
│  SQLite DB  │  Stores all data (17 tables)
└──────┬──────┘
       ▲
       │ ETL inserts/updates
┌──────┴──────┐
│ APScheduler │  Runs on schedule
│  + TMDb API │  Fetches new data
└─────────────┘
```

**Example User Flow:**
1. User opens app → React frontend loads
2. User searches for "Inception" → React calls `GET /api/search?q=Inception`
3. Flask queries database → Returns movie data as JSON
4. React displays movie card with details
5. User clicks "Add to Watchlist" → React calls `POST /api/watchlist`
6. Flask inserts into `watchlists` table
7. User sees confirmation

**ETL Flow (Background):**
1. APScheduler triggers (e.g., every 24 hours)
2. ETL service calls TMDb API for trending movies
3. Transforms and validates data
4. Inserts/updates database
5. Database now has latest movies/shows

---

## Key Terms Simplified

- **REST API:** Standard way for frontend to request data from backend
- **JSON:** Data format (like a dictionary) that's easy to send over network
- **Composite Key:** Primary key made of multiple columns (prevents duplicates)
- **M:N Relationship:** Many-to-many (e.g., movies ↔ genres)
- **Junction Table:** Table that connects two other tables in M:N relationship
- **Identifying Relationship:** Child can't exist without parent
- **ETL:** Process of getting data from external source into your database
- **APScheduler:** Tool that runs tasks automatically on a schedule

---

## For Your Presentation

**You can say:**

"Our implementation uses a **3-tier architecture**:
- **Frontend:** React + TypeScript with Vite for the user interface
- **Backend:** Flask REST API that handles all business logic and database operations
- **Database:** SQLite with 17 tables including M:N junction tables with composite keys

We integrate with **TMDb API** for movie/show metadata, and use **APScheduler** to automatically fetch and update data on a schedule.

The schema includes **identifying relationships** (seasons→shows, episodes→seasons) to ensure data integrity, and **composite primary keys** in junction tables to prevent duplicate relationships."

