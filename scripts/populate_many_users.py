#!/usr/bin/env python3
"""
Populate movie_tracker.db with demo users that each have watchlists, favorites,
and genre-aware reviews that actually make sense.

Usage:
    python populate_demo_users.py [num_users]

Assumes:
    - This file lives in a scripts/ folder.
    - movie_tracker.db is in the project root (one level above this file).
"""

import sqlite3
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------
# Paths / constants
# ---------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
DB_PATH = str(SCRIPT_DIR.parent / "movie_tracker.db")

FIRST_NAMES = [
    "Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Avery", "Quinn",
    "Sam", "Cameron", "Dakota", "Drew", "Sage", "River", "Phoenix", "Skylar",
    "Blake", "Hayden", "Reese", "Parker", "Robin", "Finley", "Rowan", "Sawyer",
    "Sidney", "Jamie", "Dylan", "Peyton", "Kendall", "Emery", "Harper", "Charlie",
    "Olivia", "Emma", "Sophia", "Isabella", "Ava", "Mia", "Charlotte", "Amelia",
    "Noah", "Liam", "William", "James", "Benjamin", "Lucas", "Henry", "Alexander",
    "Michael", "Daniel", "Matthew", "David", "Joseph", "Jackson", "John", "Owen",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas", "Taylor",
    "Moore", "Jackson", "Martin", "Lee", "Thompson", "White", "Harris", "Sanchez",
    "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams",
    "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts",
    "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards",
]

REACTION_TYPES = ["👍", "❤️", "😂", "😮", "😢", "🔥"]

# Maps TMDb genre names to a nicer phrase to drop into review text
GENRE_NICKNAMES = {
    "Action": "action",
    "Adventure": "adventure",
    "Animation": "animation",
    "Comedy": "comedy",
    "Drama": "character drama",
    "Fantasy": "fantasy",
    "Horror": "horror",
    "Science Fiction": "sci-fi",
    "Sci-Fi & Fantasy": "sci-fi / fantasy",
    "Thriller": "thriller",
    "Crime": "crime drama",
    "Mystery": "mystery",
    "Family": "family story",
    "Romance": "romance",
    "Music": "music",
    "War": "war drama",
    "Western": "western",
}

# Review templates by sentiment
POSITIVE_TEMPLATES = [
    "Loved {title}! Great {genre} and pacing.",
    "{title} instantly went on my favorites list.",
    "Really enjoyed {title} — the {genre} elements were on point.",
    "{title} is exactly my kind of {genre}.",
    "I could happily rewatch {title}.",
]

NEUTRAL_TEMPLATES = [
    "{title} was pretty solid overall.",
    "{title} had some good {genre} moments, even if it dragged a bit.",
    "Decent watch — not a new favorite, but I liked parts of it.",
    "{title} was fine background watch; a few scenes really landed.",
    "Some pacing issues, but {title} still worked for me.",
]

NEGATIVE_TEMPLATES = [
    "{title} really wasn't for me.",
    "I love {genre}, but {title} didn't quite click.",
    "{title} had a cool idea, but the execution fell flat.",
    "Struggled to stay engaged with {title}.",
    "Not terrible, just not my thing.",
]

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_tables(conn: sqlite3.Connection) -> None:
    """Ensure favorites and review_reactions tables exist (for safety)."""

    # favorites
    try:
        conn.execute("SELECT 1 FROM favorites LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS favorites (
                user_id     INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                movie_id    INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
                show_id     INTEGER REFERENCES shows(show_id) ON DELETE CASCADE,
                added_at    TEXT DEFAULT CURRENT_TIMESTAMP,
                CHECK ( (movie_id IS NOT NULL) <> (show_id IS NOT NULL) ),
                PRIMARY KEY (user_id, movie_id, show_id)
            )
            """
        )
        conn.commit()
        print("[info] Created favorites table")

    # review_reactions
    try:
        conn.execute("SELECT 1 FROM review_reactions LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS review_reactions (
                reaction_id     INTEGER PRIMARY KEY,
                review_id       INTEGER NOT NULL REFERENCES reviews(review_id) ON DELETE CASCADE,
                user_id         INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                emote_type      TEXT NOT NULL CHECK (emote_type IN ('👍', '❤️', '😂', '😮', '😢', '🔥')),
                created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (review_id, user_id, emote_type)
            )
            """
        )
        conn.commit()
        print("[info] Created review_reactions table")


def generate_email(first_name: str, last_name: str, domain: str = "example.com") -> str:
    base = f"{first_name.lower()}.{last_name.lower()}"
    if random.random() < 0.3:
        base += str(random.randint(1, 99))
    return f"{base}@{domain}"


def create_user(conn: sqlite3.Connection, first_name: str, last_name: str) -> int:
    """Create a new user if the email doesn't already exist, return user_id."""
    email = generate_email(first_name, last_name)

    existing = conn.execute(
        "SELECT user_id FROM users WHERE lower(email) = lower(?) LIMIT 1",
        (email,),
    ).fetchone()
    if existing:
        return existing["user_id"]

    display_name = f"{first_name} {last_name}"
    days_ago = random.randint(0, 365)
    created_at = (datetime.now() - timedelta(days=days_ago)).isoformat()

    cur = conn.execute(
        "INSERT INTO users (email, display_name, created_at) VALUES (?, ?, ?)",
        (email, display_name, created_at),
    )
    return cur.lastrowid


def load_media(conn: sqlite3.Connection):
    """Load a pool of popular movies and shows with their genres."""
    movies = conn.execute(
        """
        SELECT m.movie_id,
               m.title,
               GROUP_CONCAT(g.name, ', ') AS genres
        FROM movies m
        LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
        LEFT JOIN genres g ON mg.genre_id = g.genre_id
        GROUP BY m.movie_id
        ORDER BY m.popularity DESC
        LIMIT 300
        """
    ).fetchall()

    shows = conn.execute(
        """
        SELECT s.show_id,
               s.title,
               GROUP_CONCAT(g.name, ', ') AS genres
        FROM shows s
        LEFT JOIN show_genres sg ON s.show_id = sg.show_id
        LEFT JOIN genres g ON sg.genre_id = g.genre_id
        GROUP BY s.show_id
        ORDER BY s.popularity DESC
        LIMIT 200
        """
    ).fetchall()

    return movies, shows


def load_all_genres(conn: sqlite3.Connection):
    rows = conn.execute("SELECT name FROM genres").fetchall()
    return [r["name"] for r in rows]


def create_user_profile(all_genres):
    """Assign each user some favorite + disliked genres."""
    genres = list(all_genres)
    random.shuffle(genres)
    num_fav = random.randint(2, 4)
    num_dis = random.randint(0, 2)

    favorite = set(genres[:num_fav])
    disliked = set(g for g in genres[num_fav : num_fav + num_dis] if g not in favorite)

    return {
        "favorite_genres": favorite,
        "disliked_genres": disliked,
    }


def parse_genres(row) -> list[str]:
    if row["genres"]:
        return [g.strip() for g in row["genres"].split(",") if g.strip()]
    return []


def genre_to_word(genres: list[str]) -> str:
    for g in genres:
        if g in GENRE_NICKNAMES:
            return GENRE_NICKNAMES[g]
    return genres[0].lower() if genres else "story"


def choose_sentiment(profile, item_genres: list[str]) -> str:
    """Choose 'positive', 'neutral', or 'negative' based on user taste vs genres."""
    item_genres_set = set(item_genres)
    likes = profile["favorite_genres"].intersection(item_genres_set)
    dislikes = profile["disliked_genres"].intersection(item_genres_set)

    if likes and not dislikes:
        weights = (0.75, 0.2, 0.05)
    elif dislikes and not likes:
        weights = (0.2, 0.4, 0.4)
    elif likes and dislikes:
        weights = (0.4, 0.35, 0.25)
    else:
        weights = (0.45, 0.4, 0.15)

    return random.choices(["positive", "neutral", "negative"], weights=weights, k=1)[0]


def pick_rating(sentiment: str) -> float:
    """Turn sentiment into a numeric rating."""
    if sentiment == "positive":
        low, high = 8.3, 10.0
    elif sentiment == "neutral":
        low, high = 6.0, 8.0
    else:  # negative / lukewarm
        low, high = 3.0, 6.5
    return round(random.uniform(low, high), 1)


def build_review_text(title: str, item_genres: list[str], sentiment: str) -> str:
    genre_word = genre_to_word(item_genres)
    if sentiment == "positive":
        template = random.choice(POSITIVE_TEMPLATES)
    elif sentiment == "neutral":
        template = random.choice(NEUTRAL_TEMPLATES)
    else:
        template = random.choice(NEGATIVE_TEMPLATES)
    return template.format(title=title, genre=genre_word)


def choose_n_for_seq(length: int, typical_min: int, typical_max: int) -> int:
    """Choose how many items to sample from a list, with safe bounds."""
    if length <= 0:
        return 0
    if length <= typical_min:
        return length
    return random.randint(typical_min, min(typical_max, length))


# ---------------------------------------------------------------------
# Main per-user population
# ---------------------------------------------------------------------


def populate_user_data(
    conn: sqlite3.Connection,
    user_id: int,
    movies,
    shows,
    all_reviews,
    all_user_ids,
    profile,
):
    stats = {"reviews": 0, "favorites": 0, "watchlist": 0, "reactions": 0}

    # Activity level controls how much stuff they have
    activity_level = random.choices(
        ["low", "medium", "high", "very_high"],
        weights=[0.3, 0.4, 0.25, 0.05],
    )[0]

    if activity_level == "low":
        num_movie_reviews = random.randint(2, 5)
        num_show_reviews = random.randint(1, 3)
    elif activity_level == "medium":
        num_movie_reviews = random.randint(5, 12)
        num_show_reviews = random.randint(3, 8)
    elif activity_level == "high":
        num_movie_reviews = random.randint(12, 25)
        num_show_reviews = random.randint(8, 18)
    else:  # very_high
        num_movie_reviews = random.randint(25, 40)
        num_show_reviews = random.randint(18, 30)

    # Which titles has this user already reviewed?
    reviewed_movie_ids = {
        r[0]
        for r in conn.execute(
            "SELECT movie_id FROM reviews WHERE user_id = ? AND movie_id IS NOT NULL",
            (user_id,),
        ).fetchall()
    }
    reviewed_show_ids = {
        r[0]
        for r in conn.execute(
            "SELECT show_id FROM reviews WHERE user_id = ? AND show_id IS NOT NULL",
            (user_id,),
        ).fetchall()
    }

    available_movies = [m for m in movies if m["movie_id"] not in reviewed_movie_ids]
    available_shows = [s for s in shows if s["show_id"] not in reviewed_show_ids]

    chosen_movies = random.sample(
        available_movies, min(num_movie_reviews, len(available_movies))
    )
    chosen_shows = random.sample(
        available_shows, min(num_show_reviews, len(available_shows))
    )

    # --- Reviews (movies) ---
    for movie in chosen_movies:
        genres = parse_genres(movie)
        sentiment = choose_sentiment(profile, genres)
        rating = pick_rating(sentiment)
        content = build_review_text(movie["title"], genres, sentiment)

        days_ago = random.randint(0, 180)
        created_at = (datetime.now() - timedelta(days=days_ago)).isoformat()

        conn.execute(
            "INSERT INTO reviews (user_id, movie_id, rating, content, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, movie["movie_id"], rating, content, created_at),
        )
        stats["reviews"] += 1

    # --- Reviews (shows) ---
    for show in chosen_shows:
        genres = parse_genres(show)
        sentiment = choose_sentiment(profile, genres)
        rating = pick_rating(sentiment)
        content = build_review_text(show["title"], genres, sentiment)

        days_ago = random.randint(0, 180)
        created_at = (datetime.now() - timedelta(days=days_ago)).isoformat()

        conn.execute(
            "INSERT INTO reviews (user_id, show_id, rating, content, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (user_id, show["show_id"], rating, content, created_at),
        )
        stats["reviews"] += 1

    # --- Favorites (from this user's own reviews) ---
    user_reviews = conn.execute(
        "SELECT movie_id, show_id, rating FROM reviews WHERE user_id = ?",
        (user_id,),
    ).fetchall()

    high_rated = [r for r in user_reviews if r["rating"] >= 8.0]
    base_pool = high_rated if high_rated else user_reviews

    n_fav = choose_n_for_seq(len(base_pool), typical_min=3, typical_max=15)
    favorite_rows = (
        random.sample(base_pool, n_fav) if n_fav > 0 else []
    )

    for r in favorite_rows:
        movie_id = r["movie_id"]
        show_id = r["show_id"]
        days_ago = random.randint(0, 90)
        added_at = (datetime.now() - timedelta(days=days_ago)).isoformat()

        try:
            if movie_id is not None:
                conn.execute(
                    "INSERT OR IGNORE INTO favorites "
                    "(user_id, movie_id, show_id, added_at) "
                    "VALUES (?, ?, NULL, ?)",
                    (user_id, movie_id, added_at),
                )
            elif show_id is not None:
                conn.execute(
                    "INSERT OR IGNORE INTO favorites "
                    "(user_id, movie_id, show_id, added_at) "
                    "VALUES (?, NULL, ?, ?)",
                    (user_id, show_id, added_at),
                )
            stats["favorites"] += 1
        except sqlite3.IntegrityError:
            # unique constraint hit; ignore
            pass

    # --- Watchlists (things not yet reviewed or favorited) ---
    favorited_movie_ids = {
        r[0]
        for r in conn.execute(
            "SELECT movie_id FROM favorites "
            "WHERE user_id = ? AND movie_id IS NOT NULL",
            (user_id,),
        ).fetchall()
    }
    favorited_show_ids = {
        r[0]
        for r in conn.execute(
            "SELECT show_id FROM favorites "
            "WHERE user_id = ? AND show_id IS NOT NULL",
            (user_id,),
        ).fetchall()
    }

    watchlist_movie_rows = [
        m
        for m in movies
        if m["movie_id"] not in reviewed_movie_ids
        and m["movie_id"] not in favorited_movie_ids
    ]
    watchlist_show_rows = [
        s
        for s in shows
        if s["show_id"] not in reviewed_show_ids
        and s["show_id"] not in favorited_show_ids
    ]

    n_watch_movies = choose_n_for_seq(len(watchlist_movie_rows), 3, 15)
    n_watch_shows = choose_n_for_seq(len(watchlist_show_rows), 2, 12)

    watchlist_movies = (
        random.sample(watchlist_movie_rows, n_watch_movies)
        if n_watch_movies > 0
        else []
    )
    watchlist_shows = (
        random.sample(watchlist_show_rows, n_watch_shows)
        if n_watch_shows > 0
        else []
    )

    for movie in watchlist_movies:
        days_ago = random.randint(0, 90)
        added_at = (datetime.now() - timedelta(days=days_ago)).isoformat()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO watchlists "
                "(user_id, movie_id, show_id, added_at) "
                "VALUES (?, ?, NULL, ?)",
                (user_id, movie["movie_id"], added_at),
            )
            stats["watchlist"] += 1
        except sqlite3.IntegrityError:
            pass

    for show in watchlist_shows:
        days_ago = random.randint(0, 90)
        added_at = (datetime.now() - timedelta(days=days_ago)).isoformat()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO watchlists "
                "(user_id, movie_id, show_id, added_at) "
                "VALUES (?, NULL, ?, ?)",
                (user_id, show["show_id"], added_at),
            )
            stats["watchlist"] += 1
        except sqlite3.IntegrityError:
            pass

    # --- Review reactions (react to other users' reviews) ---
    if all_reviews:
        other_reviews = [r for r in all_reviews if r["user_id"] != user_id]
        if other_reviews:
            max_react = min(30, len(other_reviews))
            min_react = min(5, max_react)
            num_reactions = random.randint(min_react, max_react)

            target_reviews = random.sample(other_reviews, num_reactions)
            for review in target_reviews:
                # 1–3 emotes per review
                num_emotes = random.choices(
                    [1, 2, 3], weights=[0.6, 0.3, 0.1]
                )[0]
                emotes = random.sample(
                    REACTION_TYPES, min(num_emotes, len(REACTION_TYPES))
                )
                for emote in emotes:
                    try:
                        days_ago = random.randint(0, 60)
                        created_at = (
                            datetime.now() - timedelta(days=days_ago)
                        ).isoformat()
                        conn.execute(
                            "INSERT INTO review_reactions "
                            "(review_id, user_id, emote_type, created_at) "
                            "VALUES (?, ?, ?, ?)",
                            (review["review_id"], user_id, emote, created_at),
                        )
                        stats["reactions"] += 1
                    except sqlite3.IntegrityError:
                        # already reacted with this emote
                        pass

    return stats


# ---------------------------------------------------------------------
# Top-level driver
# ---------------------------------------------------------------------


def populate_demo_users(num_users: int = 20) -> None:
    conn = get_connection()
    ensure_tables(conn)

    movies, shows = load_media(conn)
    if not movies and not shows:
        print("[error] No media found in the database. Run your TMDb ETL first.")
        return

    all_genres = load_all_genres(conn)
    print(f"[info] Using {len(movies)} movies, {len(shows)} shows, {len(all_genres)} genres")

    # Create users first
    created_users: list[tuple[int, str]] = []
    for i in range(num_users):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        user_id = create_user(conn, first, last)
        created_users.append((user_id, f"{first} {last}"))

    conn.commit()
    print(f"[info] Created {len(created_users)} users")

    # Preload existing reviews for reactions
    all_reviews_rows = conn.execute(
        "SELECT review_id, user_id FROM reviews"
    ).fetchall()
    all_reviews = [dict(r) for r in all_reviews_rows]

    all_user_ids = [
        r[0] for r in conn.execute("SELECT user_id FROM users").fetchall()
    ]

    totals = {"reviews": 0, "favorites": 0, "watchlist": 0, "reactions": 0}

    print("[info] Populating per-user data...")
    for idx, (user_id, name) in enumerate(created_users, start=1):
        profile = create_user_profile(all_genres)
        stats = populate_user_data(
            conn, user_id, movies, shows, all_reviews, all_user_ids, profile
        )

        for k in totals:
            totals[k] += stats[k]

        new_reviews = conn.execute(
            "SELECT review_id, user_id FROM reviews WHERE user_id = ?",
            (user_id,),
        ).fetchall()
        all_reviews.extend(dict(r) for r in new_reviews)

        if idx % 5 == 0 or idx == len(created_users):
            print(
                f"  - {idx}/{len(created_users)} users done "
                f"(reviews={totals['reviews']}, "
                f"favorites={totals['favorites']}, "
                f"watchlist={totals['watchlist']}, "
                f"reactions={totals['reactions']})"
            )

    conn.commit()
    conn.close()

    print("\n[done] Demo population complete")
    print(
        f"  -> {len(created_users)} users\n"
        f"  -> {totals['reviews']} reviews\n"
        f"  -> {totals['favorites']} favorites\n"
        f"  -> {totals['watchlist']} watchlist items\n"
        f"  -> {totals['reactions']} review reactions"
    )


if __name__ == "__main__":
    num = 20
    if len(sys.argv) > 1:
        try:
            num = int(sys.argv[1])
        except ValueError:
            print(f"[warn] Invalid num_users '{sys.argv[1]}', using default 20")
    populate_demo_users(num)
