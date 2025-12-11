PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER PRIMARY KEY,
    email       TEXT UNIQUE NOT NULL,
    display_name TEXT,
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS movies (
    movie_id        INTEGER PRIMARY KEY,
    tmdb_id         INTEGER UNIQUE NOT NULL,
    title           TEXT NOT NULL,
    release_year    INTEGER,
    release_date    TEXT,
    runtime_min     INTEGER,
    overview        TEXT,
    poster_path     TEXT,
    backdrop_path   TEXT,
    original_language TEXT,
    tmdb_vote_avg   REAL,
    popularity      REAL,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_movies_title ON movies(title);

CREATE TABLE IF NOT EXISTS shows (
    show_id         INTEGER PRIMARY KEY,
    tmdb_id         INTEGER UNIQUE NOT NULL,
    title           TEXT NOT NULL,
    first_air_date  TEXT,
    last_air_date   TEXT,
    overview        TEXT,
    poster_path     TEXT,
    backdrop_path   TEXT,
    original_language TEXT,
    tmdb_vote_avg   REAL,
    popularity      REAL,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_shows_title ON shows(title);

CREATE TABLE IF NOT EXISTS seasons (
    season_id      INTEGER PRIMARY KEY,
    show_id        INTEGER NOT NULL REFERENCES shows(show_id) ON DELETE CASCADE,
    season_number  INTEGER NOT NULL,
    title          TEXT,
    air_date       TEXT,
    UNIQUE (show_id, season_number)
);

CREATE TABLE IF NOT EXISTS episodes (
    episode_id      INTEGER PRIMARY KEY,
    season_id       INTEGER NOT NULL REFERENCES seasons(season_id) ON DELETE CASCADE,
    episode_number  INTEGER NOT NULL,
    title           TEXT,
    air_date        TEXT,
    runtime_min     INTEGER,
    UNIQUE (season_id, episode_number)
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id        INTEGER PRIMARY KEY,
    tmdb_genre_id   INTEGER UNIQUE,
    name            TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id    INTEGER NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
    genre_id    INTEGER NOT NULL REFERENCES genres(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE IF NOT EXISTS show_genres (
    show_id     INTEGER NOT NULL REFERENCES shows(show_id) ON DELETE CASCADE,
    genre_id    INTEGER NOT NULL REFERENCES genres(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (show_id, genre_id)
);

CREATE TABLE IF NOT EXISTS people (
    person_id       INTEGER PRIMARY KEY,
    tmdb_person_id  INTEGER UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    profile_path    TEXT
);

CREATE TABLE IF NOT EXISTS movie_cast (
    movie_id    INTEGER NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
    person_id   INTEGER NOT NULL REFERENCES people(person_id) ON DELETE CASCADE,
    character   TEXT,
    cast_order  INTEGER,
    PRIMARY KEY (movie_id, person_id)
);

CREATE TABLE IF NOT EXISTS show_cast (
    show_id     INTEGER NOT NULL REFERENCES shows(show_id) ON DELETE CASCADE,
    person_id   INTEGER NOT NULL REFERENCES people(person_id) ON DELETE CASCADE,
    character   TEXT,
    cast_order  INTEGER,
    PRIMARY KEY (show_id, person_id)
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id   INTEGER PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    movie_id    INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    show_id     INTEGER REFERENCES shows(show_id) ON DELETE CASCADE,
    rating      REAL CHECK (rating BETWEEN 0 AND 10),
    content     TEXT,
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
    CHECK ( (movie_id IS NOT NULL) <> (show_id IS NOT NULL) )
);
CREATE INDEX IF NOT EXISTS idx_reviews_movie ON reviews(movie_id);
CREATE INDEX IF NOT EXISTS idx_reviews_show ON reviews(show_id);
-- Note: Unique constraint for one review per user per movie/show is enforced in backend
-- SQLite doesn't support partial unique indexes, so we check in application code

CREATE TABLE IF NOT EXISTS discussions (
    discussion_id   INTEGER PRIMARY KEY,
    user_id         INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    movie_id        INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    show_id         INTEGER REFERENCES shows(show_id) ON DELETE CASCADE,
    title           TEXT NOT NULL,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
    CHECK ( (movie_id IS NOT NULL) <> (show_id IS NOT NULL) )
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id      INTEGER PRIMARY KEY,
    discussion_id   INTEGER NOT NULL REFERENCES discussions(discussion_id) ON DELETE CASCADE,
    user_id         INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    content         TEXT NOT NULL,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS watchlists (
    user_id     INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    movie_id    INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    show_id     INTEGER REFERENCES shows(show_id) ON DELETE CASCADE,
    added_at    TEXT DEFAULT CURRENT_TIMESTAMP,
    CHECK ( (movie_id IS NOT NULL) <> (show_id IS NOT NULL) ),
    PRIMARY KEY (user_id, movie_id, show_id)
);

CREATE TABLE IF NOT EXISTS favorites (
    user_id     INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    movie_id    INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    show_id     INTEGER REFERENCES shows(show_id) ON DELETE CASCADE,
    added_at    TEXT DEFAULT CURRENT_TIMESTAMP,
    CHECK ( (movie_id IS NOT NULL) <> (show_id IS NOT NULL) ),
    PRIMARY KEY (user_id, movie_id, show_id)
);

CREATE TABLE IF NOT EXISTS review_reactions (
    reaction_id     INTEGER PRIMARY KEY,
    review_id       INTEGER NOT NULL REFERENCES reviews(review_id) ON DELETE CASCADE,
    user_id         INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    emote_type      TEXT NOT NULL CHECK (emote_type IN ('👍', '❤️', '😂', '😮', '😢', '🔥')),
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (review_id, user_id, emote_type)
);
CREATE INDEX IF NOT EXISTS idx_review_reactions_review ON review_reactions(review_id);
CREATE INDEX IF NOT EXISTS idx_review_reactions_user ON review_reactions(user_id);

CREATE TABLE IF NOT EXISTS title_comments (
    comment_id      INTEGER PRIMARY KEY,
    title_type      TEXT NOT NULL CHECK (title_type IN ('movie', 'show')),
    title_id        INTEGER NOT NULL,
    user_id         INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    parent_comment_id INTEGER REFERENCES title_comments(comment_id) ON DELETE CASCADE,
    body            TEXT NOT NULL,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at      TEXT DEFAULT CURRENT_TIMESTAMP,
    is_deleted      INTEGER DEFAULT 0 CHECK (is_deleted IN (0, 1))
);
CREATE INDEX IF NOT EXISTS idx_title_comments_title ON title_comments(title_type, title_id);
CREATE INDEX IF NOT EXISTS idx_title_comments_user ON title_comments(user_id);
CREATE INDEX IF NOT EXISTS idx_title_comments_parent ON title_comments(parent_comment_id);

COMMIT;




