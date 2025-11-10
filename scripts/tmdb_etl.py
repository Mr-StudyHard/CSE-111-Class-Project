#!/usr/bin/env python3
"""
Fetch data from TMDb and upsert into movie_tracker.db using sqlite3 + raw SQL.

Example:
    python scripts/tmdb_etl.py --movies 20 --shows 10 --episodes-per-season 5
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from typing import Iterable

import requests
from dotenv import load_dotenv

API_BASE = "https://api.themoviedb.org/3"
DB_PATH = os.getenv("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "..", "movie_tracker.db"))


def ensure_api_key() -> str:
    load_dotenv()
    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        print("Missing TMDB_API_KEY. Set it in your environment or .env file.", file=sys.stderr)
        sys.exit(1)
    return api_key


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


class TMDbClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()

    def get(self, path: str, **params):
        params["api_key"] = self.api_key
        url = f"{API_BASE}{path}"
        resp = self.session.get(url, params=params, timeout=25)
        resp.raise_for_status()
        return resp.json()


def upsert_genres(conn: sqlite3.Connection, payload: Iterable[dict]):
    with conn:
        for item in payload:
            conn.execute(
                """
                INSERT INTO genres (tmdb_genre_id, name)
                VALUES (?, ?)
                ON CONFLICT(tmdb_genre_id) DO UPDATE SET name = excluded.name
                """,
                (item.get("id"), item.get("name")),
            )


def upsert_movie(conn: sqlite3.Connection, data: dict):
    release_year = None
    release_date = data.get("release_date")
    if release_date and len(release_date) >= 4:
        try:
            release_year = int(release_date[:4])
        except ValueError:
            release_year = None
    params = (
        data.get("id"),
        data.get("title"),
        release_year,
        data.get("runtime"),
        data.get("overview"),
        data.get("poster_path"),
        data.get("vote_average"),
        data.get("popularity"),
    )
    conn.execute(
        """
        INSERT INTO movies (
            tmdb_id, title, release_year, runtime_min, overview, poster_path, tmdb_vote_avg, popularity
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tmdb_id) DO UPDATE SET
            title = excluded.title,
            release_year = excluded.release_year,
            runtime_min = excluded.runtime_min,
            overview = excluded.overview,
            poster_path = excluded.poster_path,
            tmdb_vote_avg = excluded.tmdb_vote_avg,
            popularity = excluded.popularity
        """,
        params,
    )


def upsert_show(conn: sqlite3.Connection, data: dict):
    params = (
        data.get("id"),
        data.get("name"),
        data.get("first_air_date"),
        data.get("last_air_date"),
        data.get("overview"),
        data.get("poster_path"),
        data.get("vote_average"),
        data.get("popularity"),
    )
    conn.execute(
        """
        INSERT INTO shows (
            tmdb_id, title, first_air_date, last_air_date, overview, poster_path, tmdb_vote_avg, popularity
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tmdb_id) DO UPDATE SET
            title = excluded.title,
            first_air_date = excluded.first_air_date,
            last_air_date = excluded.last_air_date,
            overview = excluded.overview,
            poster_path = excluded.poster_path,
            tmdb_vote_avg = excluded.tmdb_vote_avg,
            popularity = excluded.popularity
        """,
        params,
    )


def upsert_person(conn: sqlite3.Connection, cast: dict):
    conn.execute(
        """
        INSERT INTO people (tmdb_person_id, name, profile_path)
        VALUES (?, ?, ?)
        ON CONFLICT(tmdb_person_id) DO UPDATE SET
            name = excluded.name,
            profile_path = excluded.profile_path
        """,
        (cast.get("id"), cast.get("name"), cast.get("profile_path")),
    )


def attach_movie_cast(conn: sqlite3.Connection, movie_tmdb_id: int, cast: dict):
    conn.execute(
        """
        INSERT INTO movie_cast (movie_id, person_id, character, cast_order)
        VALUES (
            (SELECT movie_id FROM movies WHERE tmdb_id = ?),
            (SELECT person_id FROM people WHERE tmdb_person_id = ?),
            ?, ?
        )
        ON CONFLICT(movie_id, person_id) DO UPDATE SET
            character = excluded.character,
            cast_order = excluded.cast_order
        """,
        (
            movie_tmdb_id,
            cast.get("id"),
            cast.get("character"),
            cast.get("order"),
        ),
    )


def attach_show_cast(conn: sqlite3.Connection, show_tmdb_id: int, cast: dict):
    conn.execute(
        """
        INSERT INTO show_cast (show_id, person_id, character, cast_order)
        VALUES (
            (SELECT show_id FROM shows WHERE tmdb_id = ?),
            (SELECT person_id FROM people WHERE tmdb_person_id = ?),
            ?, ?
        )
        ON CONFLICT(show_id, person_id) DO UPDATE SET
            character = excluded.character,
            cast_order = excluded.cast_order
        """,
        (
            show_tmdb_id,
            cast.get("id"),
            cast.get("character") or (cast.get("roles") or [{}])[0].get("character"),
            cast.get("order") if cast.get("order") is not None else cast.get("total_episode_count"),
        ),
    )


def link_movie_genres(conn: sqlite3.Connection, movie_tmdb_id: int, genres: Iterable[dict]):
    for genre in genres or []:
        conn.execute(
            """
            INSERT OR IGNORE INTO movie_genres (movie_id, genre_id)
            SELECT m.movie_id, g.genre_id
            FROM movies m, genres g
            WHERE m.tmdb_id = ? AND g.tmdb_genre_id = ?
            """,
            (movie_tmdb_id, genre.get("id")),
        )


def link_show_genres(conn: sqlite3.Connection, show_tmdb_id: int, genres: Iterable[dict]):
    for genre in genres or []:
        conn.execute(
            """
            INSERT OR IGNORE INTO show_genres (show_id, genre_id)
            SELECT s.show_id, g.genre_id
            FROM shows s, genres g
            WHERE s.tmdb_id = ? AND g.tmdb_genre_id = ?
            """,
            (show_tmdb_id, genre.get("id")),
        )


def upsert_season(conn: sqlite3.Connection, show_tmdb_id: int, season: dict):
    conn.execute(
        """
        INSERT INTO seasons (show_id, season_number, title, air_date)
        VALUES (
            (SELECT show_id FROM shows WHERE tmdb_id = ?),
            ?, ?, ?
        )
        ON CONFLICT(show_id, season_number) DO UPDATE SET
            title = excluded.title,
            air_date = excluded.air_date
        """,
        (
            show_tmdb_id,
            season.get("season_number"),
            season.get("name"),
            season.get("air_date"),
        ),
    )


def upsert_episode(conn: sqlite3.Connection, show_tmdb_id: int, season_number: int, episode: dict):
    conn.execute(
        """
        INSERT INTO episodes (season_id, episode_number, title, air_date, runtime_min)
        VALUES (
            (
                SELECT season_id
                FROM seasons
                WHERE show_id = (SELECT show_id FROM shows WHERE tmdb_id = ?)
                  AND season_number = ?
            ),
            ?, ?, ?, ?
        )
        ON CONFLICT(season_id, episode_number) DO UPDATE SET
            title = excluded.title,
            air_date = excluded.air_date,
            runtime_min = excluded.runtime_min
        """,
        (
            show_tmdb_id,
            season_number,
            episode.get("episode_number"),
            episode.get("name"),
            episode.get("air_date"),
            episode.get("runtime"),
        ),
    )


def iter_popular(client: TMDbClient, path: str, total: int):
    collected = 0
    page = 1
    while collected < total:
        data = client.get(path, page=page)
        results = data.get("results") or []
        if not results:
            break
        for item in results:
            yield item
            collected += 1
            if collected >= total:
                break
        page += 1


def process_movies(conn: sqlite3.Connection, client: TMDbClient, limit: int):
    for summary in iter_popular(client, "/movie/popular", limit):
        movie_id = summary.get("id")
        if not movie_id:
            continue
        detail = client.get(f"/movie/{movie_id}", append_to_response="credits")
        with conn:
            upsert_movie(conn, detail)
            link_movie_genres(conn, detail.get("id"), detail.get("genres"))
            credits = detail.get("credits", {}).get("cast", []) or []
            for cast in credits[:25]:
                upsert_person(conn, cast)
                attach_movie_cast(conn, detail["id"], cast)


def process_shows(conn: sqlite3.Connection, client: TMDbClient, limit: int, episodes_per_season: int):
    for summary in iter_popular(client, "/tv/popular", limit):
        show_id = summary.get("id")
        if not show_id:
            continue
        detail = client.get(
            f"/tv/{show_id}",
            append_to_response="aggregate_credits,seasons",
        )
        with conn:
            upsert_show(conn, detail)
            link_show_genres(conn, detail.get("id"), detail.get("genres"))
            credits = detail.get("aggregate_credits", {}).get("cast", []) or []
            for cast in credits[:25]:
                upsert_person(conn, cast)
                attach_show_cast(conn, detail["id"], cast)

            seasons = detail.get("seasons") or []
            for season in seasons:
                season_number = season.get("season_number")
                if season_number in (None, 0):
                    continue  # Skip specials
                upsert_season(conn, detail["id"], season)
                season_detail = client.get(f"/tv/{show_id}/season/{season_number}")
                for episode in (season_detail.get("episodes") or [])[:episodes_per_season]:
                    upsert_episode(conn, detail["id"], season_number, episode)


def main():
    parser = argparse.ArgumentParser(description="TMDb ETL loader for Movie Tracker.")
    parser.add_argument("--movies", type=int, default=20, help="Number of popular movies to ingest.")
    parser.add_argument("--shows", type=int, default=10, help="Number of popular shows to ingest.")
    parser.add_argument("--episodes-per-season", type=int, default=5, help="Episode limit per season.")
    args = parser.parse_args()

    api_key = ensure_api_key()
    conn = connect_db()
    client = TMDbClient(api_key)

    print("Fetching genre lists...")
    upsert_genres(conn, client.get("/genre/movie/list").get("genres", []))
    upsert_genres(conn, client.get("/genre/tv/list").get("genres", []))

    print(f"Ingesting {args.movies} movies...")
    process_movies(conn, client, args.movies)

    print(f"Ingesting {args.shows} shows...")
    process_shows(conn, client, args.shows, args.episodes_per_season)

    conn.close()
    print("ETL complete.")


if __name__ == "__main__":
    main()

