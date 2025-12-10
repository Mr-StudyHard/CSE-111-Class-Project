#!/usr/bin/env python3
"""
Enhanced TMDb ETL Service with Data Cleaning and Transformation
Extracts data from TMDb API, applies quality filters, and loads into database
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from dotenv import load_dotenv


API_BASE = "https://api.themoviedb.org/3"


class TMDbETLService:
    """
    Enhanced ETL service with data cleaning, transformation, and quality checks
    """
    
    def __init__(self, config: dict):
        """Initialize the ETL service with configuration"""
        load_dotenv()
        
        self.config = config
        self.logger = logging.getLogger('TMDbETLService')
        
        # Get API key
        self.api_key = os.getenv("TMDB_API_KEY")
        if not self.api_key:
            raise RuntimeError("TMDB_API_KEY not found in environment")
        
        # Database setup
        db_config = config.get('database', {})
        db_path = os.getenv("DATABASE_PATH") or db_config.get('path', 'movie_tracker.db')
        
        # Make path absolute if relative
        if not os.path.isabs(db_path):
            project_root = Path(__file__).parent.parent
            db_path = str(project_root / db_path)
        
        self.db_path = db_path
        self.logger.info(f"Database path: {self.db_path}")
        
        # Statistics tracking
        self.stats = {
            'movies_processed': 0,
            'movies_inserted': 0,
            'movies_updated': 0,
            'movies_skipped': 0,
            'shows_processed': 0,
            'shows_inserted': 0,
            'shows_updated': 0,
            'shows_skipped': 0,
            'genres_synced': 0,
            'people_synced': 0,
            'api_calls': 0,
            'errors': 0,
        }
        
        # HTTP session for connection pooling
        self.session = requests.Session()
    
        # Cache for person details to avoid redundant API calls
        self._person_cache = {}
    
    def _get_db_connection(self) -> sqlite3.Connection:
        """Get a database connection"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Set busy timeout to handle concurrent access (30 seconds)
        conn.execute("PRAGMA busy_timeout = 30000")
        
        # Enable WAL mode if configured
        if self.config.get('database', {}).get('enable_wal', True):
            conn.execute("PRAGMA journal_mode=WAL")
        
        return conn
    
    def _api_get(self, path: str, **params) -> dict:
        """Make API request with retry logic and rate limiting"""
        params['api_key'] = self.api_key
        url = f"{API_BASE}{path}"
        
        api_config = self.config.get('api', {})
        timeout = api_config.get('timeout', 30)
        max_retries = api_config.get('max_retries', 3)
        request_delay = api_config.get('request_delay', 0.25)
        
        for attempt in range(max_retries):
            try:
                # Rate limiting
                if request_delay > 0:
                    time.sleep(request_delay)
                
                resp = self.session.get(url, params=params, timeout=timeout)
                resp.raise_for_status()
                
                self.stats['api_calls'] += 1
                return resp.json()
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"API request failed (attempt {attempt + 1}/{max_retries}): {e}")
                
                if attempt == max_retries - 1:
                    self.stats['errors'] += 1
                    raise
                
                # Exponential backoff
                time.sleep(2 ** attempt)
        
        return {}
    
    def _ensure_schema_columns(self, conn: sqlite3.Connection):
        """Ensure all required columns exist in the database"""
        def has_column(table: str, column: str) -> bool:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            return any(row['name'] == column for row in rows)
        
        with conn:
            # Movies table columns
            if not has_column('movies', 'backdrop_path'):
                conn.execute("ALTER TABLE movies ADD COLUMN backdrop_path TEXT")
            if not has_column('movies', 'original_language'):
                conn.execute("ALTER TABLE movies ADD COLUMN original_language TEXT")
            if not has_column('movies', 'release_date'):
                conn.execute("ALTER TABLE movies ADD COLUMN release_date TEXT")
            
            # Shows table columns
            if not has_column('shows', 'backdrop_path'):
                conn.execute("ALTER TABLE shows ADD COLUMN backdrop_path TEXT")
            if not has_column('shows', 'original_language'):
                conn.execute("ALTER TABLE shows ADD COLUMN original_language TEXT")
            
            # People table extended columns
            if not has_column('people', 'birthday'):
                conn.execute("ALTER TABLE people ADD COLUMN birthday TEXT")
            if not has_column('people', 'deathday'):
                conn.execute("ALTER TABLE people ADD COLUMN deathday TEXT")
            if not has_column('people', 'place_of_birth'):
                conn.execute("ALTER TABLE people ADD COLUMN place_of_birth TEXT")
            if not has_column('people', 'biography'):
                conn.execute("ALTER TABLE people ADD COLUMN biography TEXT")
            if not has_column('people', 'imdb_id'):
                conn.execute("ALTER TABLE people ADD COLUMN imdb_id TEXT")
            if not has_column('people', 'instagram_id'):
                conn.execute("ALTER TABLE people ADD COLUMN instagram_id TEXT")
            if not has_column('people', 'twitter_id'):
                conn.execute("ALTER TABLE people ADD COLUMN twitter_id TEXT")
            if not has_column('people', 'facebook_id'):
                conn.execute("ALTER TABLE people ADD COLUMN facebook_id TEXT")
    
    def _fetch_person_details(self, tmdb_person_id: int) -> dict:
        """Fetch full person details including biography and external IDs"""
        if tmdb_person_id in self._person_cache:
            return self._person_cache[tmdb_person_id]
        
        try:
            person_data = self._api_get(
                f'/person/{tmdb_person_id}',
                append_to_response='external_ids'
            )
            self._person_cache[tmdb_person_id] = person_data
            return person_data
        except Exception as e:
            self.logger.warning(f"Could not fetch details for person {tmdb_person_id}: {e}")
            return {}
    
    def _clean_text(self, text: Optional[str]) -> Optional[str]:
        """Clean and normalize text data"""
        if not text:
            return None
        
        # Strip whitespace
        text = text.strip()
        
        # Remove null bytes that can cause SQLite issues
        text = text.replace('\x00', '')
        
        return text if text else None
    
    def _validate_movie_data(self, data: dict) -> bool:
        """Validate movie data meets quality standards"""
        quality_config = self.config.get('data_quality', {})
        
        # Check vote count
        min_vote_count = quality_config.get('min_vote_count', 0)
        if data.get('vote_count', 0) < min_vote_count:
            return False
        
        # Check popularity
        min_popularity = quality_config.get('min_popularity', 0)
        if data.get('popularity', 0) < min_popularity:
            return False
        
        # Check required fields
        if quality_config.get('require_poster', False) and not data.get('poster_path'):
            return False
        
        if quality_config.get('require_overview', False) and not data.get('overview'):
            return False
        
        return True
    
    def _validate_show_data(self, data: dict) -> bool:
        """Validate TV show data meets quality standards"""
        quality_config = self.config.get('data_quality', {})
        
        # Check vote count
        min_vote_count = quality_config.get('min_vote_count', 0)
        if data.get('vote_count', 0) < min_vote_count:
            return False
        
        # Check popularity
        min_popularity = quality_config.get('min_popularity', 0)
        if data.get('popularity', 0) < min_popularity:
            return False
        
        # Check required fields
        if quality_config.get('require_poster', False) and not data.get('poster_path'):
            return False
        
        if quality_config.get('require_overview', False) and not data.get('overview'):
            return False
        
        return True
    
    def _transform_movie_data(self, data: dict) -> dict:
        """Transform and clean movie data"""
        # Extract year and full date from release date
        release_year = None
        release_date_full = data.get('release_date')
        if release_date_full and len(release_date_full) >= 4:
            try:
                release_year = int(release_date_full[:4])
            except (ValueError, TypeError):
                release_year = None
        # Store full date if available (YYYY-MM-DD format, at least 10 chars)
        release_date = release_date_full if release_date_full and len(release_date_full) >= 10 else None
        
        return {
            'tmdb_id': data.get('id'),
            'title': self._clean_text(data.get('title')),
            'release_year': release_year,
            'release_date': release_date,
            'runtime_min': data.get('runtime'),
            'overview': self._clean_text(data.get('overview')),
            'poster_path': data.get('poster_path'),
            'backdrop_path': data.get('backdrop_path'),
            'original_language': data.get('original_language'),
            'tmdb_vote_avg': float(data.get('vote_average', 0)),
            'popularity': float(data.get('popularity', 0)),
        }
    
    def _transform_show_data(self, data: dict) -> dict:
        """Transform and clean TV show data"""
        return {
            'tmdb_id': data.get('id'),
            'title': self._clean_text(data.get('name')),
            'first_air_date': data.get('first_air_date'),
            'last_air_date': data.get('last_air_date'),
            'overview': self._clean_text(data.get('overview')),
            'poster_path': data.get('poster_path'),
            'backdrop_path': data.get('backdrop_path'),
            'original_language': data.get('original_language'),
            'tmdb_vote_avg': float(data.get('vote_average', 0)),
            'popularity': float(data.get('popularity', 0)),
        }
    
    def sync_genres(self, conn: sqlite3.Connection):
        """Sync genre data from TMDb"""
        self.logger.info("Syncing genres from TMDb...")
        
        try:
            movie_genres = self._api_get('/genre/movie/list').get('genres', [])
            tv_genres = self._api_get('/genre/tv/list').get('genres', [])
            
            all_genres = {g['id']: g for g in movie_genres + tv_genres}.values()
            
            with conn:
                for genre in all_genres:
                    conn.execute(
                        """
                        INSERT INTO genres (tmdb_genre_id, name)
                        VALUES (?, ?)
                        ON CONFLICT(tmdb_genre_id) DO UPDATE SET name = excluded.name
                        """,
                        (genre['id'], self._clean_text(genre['name']))
                    )
            
            self.stats['genres_synced'] = len(all_genres)
            self.logger.info(f"Synced {len(all_genres)} genres")
            
        except Exception as e:
            self.logger.error(f"Failed to sync genres: {e}")
            self.stats['errors'] += 1
    
    def _upsert_movie(self, conn: sqlite3.Connection, movie_data: dict):
        """Insert or update a movie"""
        params = (
            movie_data['tmdb_id'],
            movie_data['title'],
            movie_data['release_year'],
            movie_data.get('release_date'),
            movie_data['runtime_min'],
            movie_data['overview'],
            movie_data['poster_path'],
            movie_data['backdrop_path'],
            movie_data['original_language'],
            movie_data['tmdb_vote_avg'],
            movie_data['popularity'],
        )
        
        cursor = conn.execute(
            """
            INSERT INTO movies (
                tmdb_id, title, release_year, release_date, runtime_min, overview, poster_path,
                backdrop_path, original_language, tmdb_vote_avg, popularity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tmdb_id) DO UPDATE SET
                title = excluded.title,
                release_year = excluded.release_year,
                release_date = excluded.release_date,
                runtime_min = excluded.runtime_min,
                overview = excluded.overview,
                poster_path = excluded.poster_path,
                backdrop_path = excluded.backdrop_path,
                original_language = excluded.original_language,
                tmdb_vote_avg = excluded.tmdb_vote_avg,
                popularity = excluded.popularity
            """,
            params
        )
        
        # Track if this was an insert or update
        if cursor.rowcount > 0:
            # Check if it was an update by seeing if the movie already existed
            existing = conn.execute(
                "SELECT movie_id FROM movies WHERE tmdb_id = ? AND movie_id != ?",
                (movie_data['tmdb_id'], cursor.lastrowid)
            ).fetchone()
            
            if existing:
                self.stats['movies_updated'] += 1
            else:
                self.stats['movies_inserted'] += 1
    
    def _upsert_show(self, conn: sqlite3.Connection, show_data: dict):
        """Insert or update a TV show"""
        params = (
            show_data['tmdb_id'],
            show_data['title'],
            show_data['first_air_date'],
            show_data['last_air_date'],
            show_data['overview'],
            show_data['poster_path'],
            show_data['backdrop_path'],
            show_data['original_language'],
            show_data['tmdb_vote_avg'],
            show_data['popularity'],
        )
        
        cursor = conn.execute(
            """
            INSERT INTO shows (
                tmdb_id, title, first_air_date, last_air_date, overview, poster_path,
                backdrop_path, original_language, tmdb_vote_avg, popularity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tmdb_id) DO UPDATE SET
                title = excluded.title,
                first_air_date = excluded.first_air_date,
                last_air_date = excluded.last_air_date,
                overview = excluded.overview,
                poster_path = excluded.poster_path,
                backdrop_path = excluded.backdrop_path,
                original_language = excluded.original_language,
                tmdb_vote_avg = excluded.tmdb_vote_avg,
                popularity = excluded.popularity
            """,
            params
        )
        
        if cursor.rowcount > 0:
            existing = conn.execute(
                "SELECT show_id FROM shows WHERE tmdb_id = ? AND show_id != ?",
                (show_data['tmdb_id'], cursor.lastrowid)
            ).fetchone()
            
            if existing:
                self.stats['shows_updated'] += 1
            else:
                self.stats['shows_inserted'] += 1
    
    def _upsert_person(self, conn: sqlite3.Connection, person_data: dict):
        """Insert or update a person with extended details"""
        # Extract external IDs if present
        external_ids = person_data.get('external_ids', {})
        
        conn.execute(
            """
            INSERT INTO people (
                tmdb_person_id, name, profile_path, birthday, deathday,
                place_of_birth, biography, imdb_id, instagram_id, twitter_id, facebook_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tmdb_person_id) DO UPDATE SET
                name = excluded.name,
                profile_path = excluded.profile_path,
                birthday = COALESCE(excluded.birthday, birthday),
                deathday = COALESCE(excluded.deathday, deathday),
                place_of_birth = COALESCE(excluded.place_of_birth, place_of_birth),
                biography = COALESCE(excluded.biography, biography),
                imdb_id = COALESCE(excluded.imdb_id, imdb_id),
                instagram_id = COALESCE(excluded.instagram_id, instagram_id),
                twitter_id = COALESCE(excluded.twitter_id, twitter_id),
                facebook_id = COALESCE(excluded.facebook_id, facebook_id)
            """,
            (
                person_data.get('id'),
                self._clean_text(person_data.get('name')),
                person_data.get('profile_path'),
                person_data.get('birthday'),
                person_data.get('deathday'),
                self._clean_text(person_data.get('place_of_birth')),
                self._clean_text(person_data.get('biography')),
                external_ids.get('imdb_id'),
                external_ids.get('instagram_id'),
                external_ids.get('twitter_id'),
                external_ids.get('facebook_id')
            )
        )
        self.stats['people_synced'] += 1
    
    def _link_movie_genres(self, conn: sqlite3.Connection, movie_tmdb_id: int, genres: List[dict]):
        """Link genres to a movie"""
        for genre in genres or []:
            conn.execute(
                """
                INSERT OR IGNORE INTO movie_genres (movie_id, genre_id)
                SELECT m.movie_id, g.genre_id
                FROM movies m, genres g
                WHERE m.tmdb_id = ? AND g.tmdb_genre_id = ?
                """,
                (movie_tmdb_id, genre.get('id'))
            )
    
    def _link_show_genres(self, conn: sqlite3.Connection, show_tmdb_id: int, genres: List[dict]):
        """Link genres to a show"""
        for genre in genres or []:
            conn.execute(
                """
                INSERT OR IGNORE INTO show_genres (show_id, genre_id)
                SELECT s.show_id, g.genre_id
                FROM shows s, genres g
                WHERE s.tmdb_id = ? AND g.tmdb_genre_id = ?
                """,
                (show_tmdb_id, genre.get('id'))
            )
    
    def _attach_movie_cast(self, conn: sqlite3.Connection, movie_tmdb_id: int, cast: dict):
        """Attach cast member to a movie"""
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
                cast.get('id'),
                self._clean_text(cast.get('character')),
                cast.get('order')
            )
        )
    
    def _attach_show_cast(self, conn: sqlite3.Connection, show_tmdb_id: int, cast: dict):
        """Attach cast member to a show"""
        # Handle different cast data structures
        character = cast.get('character')
        if not character and cast.get('roles'):
            character = cast['roles'][0].get('character')
        
        cast_order = cast.get('order')
        if cast_order is None:
            cast_order = cast.get('total_episode_count')
        
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
                cast.get('id'),
                self._clean_text(character),
                cast_order
            )
        )
    
    def _upsert_season(self, conn: sqlite3.Connection, show_tmdb_id: int, season: dict):
        """Insert or update a season"""
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
                season.get('season_number'),
                self._clean_text(season.get('name')),
                season.get('air_date')
            )
        )
    
    def _upsert_episode(self, conn: sqlite3.Connection, show_tmdb_id: int, 
                        season_number: int, episode: dict):
        """Insert or update an episode"""
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
                episode.get('episode_number'),
                self._clean_text(episode.get('name')),
                episode.get('air_date'),
                episode.get('runtime')
            )
        )
    
    def _iter_popular(self, path: str, total: int):
        """Iterate through popular items from TMDb"""
        collected = 0
        page = 1
        
        while collected < total:
            try:
                data = self._api_get(path, page=page)
                results = data.get('results', [])
                
                if not results:
                    break
                
                for item in results:
                    yield item
                    collected += 1
                    if collected >= total:
                        break
                
                page += 1
                
            except Exception as e:
                self.logger.error(f"Error fetching page {page} from {path}: {e}")
                break
    
    def process_movies(self, conn: sqlite3.Connection, limit: int):
        """Process movies from TMDb"""
        self.logger.info(f"Processing {limit} movies...")
        
        max_cast = self.config.get('data_limits', {}).get('max_cast', 25)
        
        for summary in self._iter_popular('/movie/popular', limit):
            movie_id = summary.get('id')
            if not movie_id:
                continue
            
            try:
                self.stats['movies_processed'] += 1
                
                # Fetch detailed movie data
                detail = self._api_get(
                    f'/movie/{movie_id}',
                    append_to_response='credits'
                )
                
                # Validate data quality
                if not self._validate_movie_data(detail):
                    self.logger.debug(f"Movie {movie_id} skipped due to quality filters")
                    self.stats['movies_skipped'] += 1
                    continue
                
                # Transform data
                movie_data = self._transform_movie_data(detail)
                
                # Fetch all person details BEFORE entering transaction (to avoid long-running locks)
                credits = detail.get('credits', {}).get('cast', [])
                person_details_map = {}
                for cast in credits[:max_cast]:
                    # Fetch person details OUTSIDE transaction
                    person_details = self._fetch_person_details(cast.get('id'))
                    # Merge cast info with person details
                    person_details.update({
                        'id': cast.get('id'),
                        'name': cast.get('name'),
                        'profile_path': cast.get('profile_path'),
                    })
                    person_details_map[cast.get('id')] = person_details
                
                # Now do all database operations in a quick transaction
                with conn:
                    # Upsert movie
                    self._upsert_movie(conn, movie_data)
                    
                    # Link genres
                    self._link_movie_genres(conn, movie_id, detail.get('genres', []))
                    
                    # Process cast (using pre-fetched person details)
                    for cast in credits[:max_cast]:
                        person_details = person_details_map.get(cast.get('id'))
                        if person_details:
                            self._upsert_person(conn, person_details)
                            self._attach_movie_cast(conn, movie_id, cast)
                
                if self.stats['movies_processed'] % 10 == 0:
                    self.logger.info(f"Processed {self.stats['movies_processed']} movies...")
                
            except Exception as e:
                self.logger.error(f"Error processing movie {movie_id}: {e}")
                self.stats['errors'] += 1
        
        self.logger.info(
            f"Movies complete: {self.stats['movies_inserted']} inserted, "
            f"{self.stats['movies_updated']} updated, "
            f"{self.stats['movies_skipped']} skipped"
        )
    
    def process_shows(self, conn: sqlite3.Connection, limit: int, episodes_per_season: int):
        """Process TV shows from TMDb"""
        self.logger.info(f"Processing {limit} TV shows...")
        
        max_cast = self.config.get('data_limits', {}).get('max_cast', 25)
        
        for summary in self._iter_popular('/tv/popular', limit):
            show_id = summary.get('id')
            if not show_id:
                continue
            
            try:
                self.stats['shows_processed'] += 1
                
                # Fetch detailed show data
                detail = self._api_get(
                    f'/tv/{show_id}',
                    append_to_response='aggregate_credits,seasons'
                )
                
                # Validate data quality
                if not self._validate_show_data(detail):
                    self.logger.debug(f"Show {show_id} skipped due to quality filters")
                    self.stats['shows_skipped'] += 1
                    continue
                
                # Transform data
                show_data = self._transform_show_data(detail)
                
                # Fetch all person details BEFORE entering transaction
                credits = detail.get('aggregate_credits', {}).get('cast', [])
                person_details_map = {}
                for cast in credits[:max_cast]:
                    # Fetch person details OUTSIDE transaction
                    person_details = self._fetch_person_details(cast.get('id'))
                    # Merge cast info with person details
                    person_details.update({
                        'id': cast.get('id'),
                        'name': cast.get('name'),
                        'profile_path': cast.get('profile_path'),
                    })
                    person_details_map[cast.get('id')] = person_details
                
                # Fetch all season details BEFORE entering transaction
                seasons = detail.get('seasons', [])
                season_details_map = {}
                for season in seasons:
                    season_number = season.get('season_number')
                    if season_number in (None, 0):
                        continue  # Skip specials
                    try:
                        # Fetch season details OUTSIDE transaction
                        season_detail = self._api_get(f'/tv/{show_id}/season/{season_number}')
                        season_details_map[season_number] = season_detail
                    except Exception as e:
                        self.logger.warning(
                            f"Error fetching season {season_number} of show {show_id}: {e}"
                        )
                
                # Now do all database operations in a quick transaction
                with conn:
                    # Upsert show
                    self._upsert_show(conn, show_data)
                    
                    # Link genres
                    self._link_show_genres(conn, show_id, detail.get('genres', []))
                    
                    # Process cast (using pre-fetched person details)
                    for cast in credits[:max_cast]:
                        person_details = person_details_map.get(cast.get('id'))
                        if person_details:
                            self._upsert_person(conn, person_details)
                            self._attach_show_cast(conn, show_id, cast)
                    
                    # Process seasons and episodes (using pre-fetched season details)
                    for season in seasons:
                        season_number = season.get('season_number')
                        if season_number in (None, 0):
                            continue  # Skip specials
                        
                        self._upsert_season(conn, show_id, season)
                        
                        # Use pre-fetched season details
                        season_detail = season_details_map.get(season_number)
                        if season_detail:
                            episodes = season_detail.get('episodes', [])[:episodes_per_season]
                            for episode in episodes:
                                self._upsert_episode(conn, show_id, season_number, episode)
                
                if self.stats['shows_processed'] % 10 == 0:
                    self.logger.info(f"Processed {self.stats['shows_processed']} shows...")
                
            except Exception as e:
                self.logger.error(f"Error processing show {show_id}: {e}")
                self.stats['errors'] += 1
        
        self.logger.info(
            f"Shows complete: {self.stats['shows_inserted']} inserted, "
            f"{self.stats['shows_updated']} updated, "
            f"{self.stats['shows_skipped']} skipped"
        )
    
    def cleanup_stale_data(self, conn: sqlite3.Connection):
        """Remove old stale data that hasn't been updated"""
        cleanup_days = self.config.get('data_quality', {}).get('cleanup_stale_days', 0)
        
        if cleanup_days <= 0:
            return
        
        self.logger.info(f"Cleaning up data older than {cleanup_days} days...")
        
        cutoff_date = (datetime.now() - timedelta(days=cleanup_days)).isoformat()
        
        with conn:
            # Note: This assumes your schema has created_at columns
            # Modify if your schema is different
            cursor = conn.execute(
                "DELETE FROM movies WHERE created_at < ?",
                (cutoff_date,)
            )
            movies_deleted = cursor.rowcount
            
            cursor = conn.execute(
                "DELETE FROM shows WHERE created_at < ?",
                (cutoff_date,)
            )
            shows_deleted = cursor.rowcount
        
        self.logger.info(
            f"Cleanup complete: {movies_deleted} movies, {shows_deleted} shows removed"
        )
    
    def vacuum_database(self):
        """Optimize database with VACUUM"""
        self.logger.info("Running VACUUM to optimize database...")
        
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.execute("VACUUM")
            conn.close()
            self.logger.info("Database optimization complete")
        except Exception as e:
            self.logger.error(f"VACUUM failed: {e}")
    
    def run_full_etl(self) -> dict:
        """Run the complete ETL pipeline"""
        start_time = time.time()
        
        self.logger.info("Starting full ETL pipeline...")
        
        # Reset statistics
        self.stats = {k: 0 for k in self.stats.keys()}
        
        try:
            # Get database connection
            conn = self._get_db_connection()
            
            # Ensure schema is up to date
            self._ensure_schema_columns(conn)
            
            # Sync genres first
            self.sync_genres(conn)
            
            # Get data limits from config
            limits = self.config.get('data_limits', {})
            movie_limit = limits.get('movies', 100)
            show_limit = limits.get('shows', 50)
            episodes_per_season = limits.get('episodes_per_season', 10)
            
            # Process movies
            self.process_movies(conn, movie_limit)
            
            # Process TV shows
            self.process_shows(conn, show_limit, episodes_per_season)
            
            # Optional: Clean up stale data
            self.cleanup_stale_data(conn)
            
            # Close connection
            conn.close()
            
            execution_time = time.time() - start_time
            self.stats['execution_time'] = f"{execution_time:.2f}s"
            
            self.logger.info("ETL pipeline completed successfully")
            return self.stats
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}", exc_info=True)
            self.stats['errors'] += 1
            raise

