#!/usr/bin/env python3
"""
KPI Precomputation Service
Calculates and stores aggregated statistics for user behavior and title metrics
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv


class KPIService:
    """
    Precomputes and stores KPIs for user behavior and aggregated title statistics
    """
    
    def __init__(self, config: dict):
        """Initialize the KPI service with configuration"""
        load_dotenv()
        
        self.config = config
        self.logger = logging.getLogger('KPIService')
        
        # Database setup
        db_config = config.get('database', {})
        db_path = os.getenv("DATABASE_PATH") or db_config.get('path', 'movie_tracker.db')
        
        # Make path absolute if relative
        if not os.path.isabs(db_path):
            project_root = Path(__file__).parent.parent
            db_path = str(project_root / db_path)
        
        self.db_path = db_path
        self.logger.info(f"KPI Service using database: {self.db_path}")
        
        # Statistics tracking
        self.stats = {
            'kpis_computed': 0,
            'execution_time': 0,
            'errors': 0,
        }
    
    def _get_db_connection(self) -> sqlite3.Connection:
        """Get a database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    
    def _ensure_kpi_table(self, conn: sqlite3.Connection):
        """Ensure the KPI table exists"""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS precomputed_kpis (
                kpi_id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                kpi_name TEXT NOT NULL,
                kpi_value TEXT NOT NULL,
                computed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(category, kpi_name)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_kpis_category ON precomputed_kpis(category)
        """)
        conn.commit()
    
    def _store_kpi(self, conn: sqlite3.Connection, category: str, name: str, value: Any):
        """Store a KPI value (as JSON for complex types)"""
        json_value = json.dumps(value) if not isinstance(value, str) else value
        conn.execute("""
            INSERT INTO precomputed_kpis (category, kpi_name, kpi_value, computed_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(category, kpi_name) DO UPDATE SET
                kpi_value = excluded.kpi_value,
                computed_at = excluded.computed_at
        """, (category, name, json_value, datetime.now().isoformat()))
        self.stats['kpis_computed'] += 1
    
    # =========================================================================
    # USER BEHAVIOR KPIs
    # =========================================================================
    
    def compute_user_activity_kpis(self, conn: sqlite3.Connection):
        """Compute user activity related KPIs"""
        self.logger.info("Computing user activity KPIs...")
        
        # Top 10 most active reviewers
        top_reviewers = conn.execute("""
            SELECT 
                u.user_id,
                u.email,
                COUNT(r.review_id) as review_count,
                AVG(r.rating) as avg_rating
            FROM users u
            JOIN reviews r ON r.user_id = u.user_id
            GROUP BY u.user_id
            ORDER BY review_count DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'user_activity', 'top_reviewers', [
            {
                'user_id': row['user_id'],
                'email': row['email'],
                'review_count': row['review_count'],
                'avg_rating': round(row['avg_rating'], 2) if row['avg_rating'] else 0
            }
            for row in top_reviewers
        ])
        
        # Top 10 most active discussion posters
        top_discussers = conn.execute("""
            SELECT 
                u.user_id,
                u.email,
                COUNT(d.discussion_id) as discussion_count
            FROM users u
            JOIN discussions d ON d.user_id = u.user_id
            GROUP BY u.user_id
            ORDER BY discussion_count DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'user_activity', 'top_discussers', [
            {
                'user_id': row['user_id'],
                'email': row['email'],
                'discussion_count': row['discussion_count']
            }
            for row in top_discussers
        ])
        
        # Top 10 most active commenters
        top_commenters = conn.execute("""
            SELECT 
                u.user_id,
                u.email,
                COUNT(c.comment_id) as comment_count
            FROM users u
            JOIN comments c ON c.user_id = u.user_id
            GROUP BY u.user_id
            ORDER BY comment_count DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'user_activity', 'top_commenters', [
            {
                'user_id': row['user_id'],
                'email': row['email'],
                'comment_count': row['comment_count']
            }
            for row in top_commenters
        ])
        
        # Users with largest watchlists
        top_watchlisters = conn.execute("""
            SELECT 
                u.user_id,
                u.email,
                COUNT(w.user_id) as watchlist_size
            FROM users u
            JOIN watchlists w ON w.user_id = u.user_id
            GROUP BY u.user_id
            ORDER BY watchlist_size DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'user_activity', 'top_watchlisters', [
            {
                'user_id': row['user_id'],
                'email': row['email'],
                'watchlist_size': row['watchlist_size']
            }
            for row in top_watchlisters
        ])
        
        # Total user counts
        user_counts = conn.execute("""
            SELECT
                (SELECT COUNT(*) FROM users) as total_users,
                (SELECT COUNT(DISTINCT user_id) FROM reviews) as users_with_reviews,
                (SELECT COUNT(DISTINCT user_id) FROM discussions) as users_with_discussions,
                (SELECT COUNT(DISTINCT user_id) FROM watchlists) as users_with_watchlists
        """).fetchone()
        
        self._store_kpi(conn, 'user_activity', 'user_counts', {
            'total_users': user_counts['total_users'],
            'users_with_reviews': user_counts['users_with_reviews'],
            'users_with_discussions': user_counts['users_with_discussions'],
            'users_with_watchlists': user_counts['users_with_watchlists']
        })
        
        self.logger.info("User activity KPIs computed")
    
    def compute_review_trends(self, conn: sqlite3.Connection):
        """Compute review trend KPIs"""
        self.logger.info("Computing review trend KPIs...")
        
        # Reviews by day (last 30 days)
        reviews_by_day = conn.execute("""
            SELECT 
                DATE(created_at) as review_date,
                COUNT(*) as review_count,
                AVG(rating) as avg_rating
            FROM reviews
            WHERE created_at >= DATE('now', '-30 days')
            GROUP BY DATE(created_at)
            ORDER BY review_date DESC
        """).fetchall()
        
        self._store_kpi(conn, 'review_trends', 'daily_reviews_30d', [
            {
                'date': row['review_date'],
                'count': row['review_count'],
                'avg_rating': round(row['avg_rating'], 2) if row['avg_rating'] else 0
            }
            for row in reviews_by_day
        ])
        
        # Rating distribution
        rating_distribution = conn.execute("""
            SELECT 
                CAST(rating AS INTEGER) as rating_bucket,
                COUNT(*) as count
            FROM reviews
            WHERE rating IS NOT NULL
            GROUP BY CAST(rating AS INTEGER)
            ORDER BY rating_bucket
        """).fetchall()
        
        self._store_kpi(conn, 'review_trends', 'rating_distribution', [
            {'rating': row['rating_bucket'], 'count': row['count']}
            for row in rating_distribution
        ])
        
        # Movies vs TV review split
        media_split = conn.execute("""
            SELECT
                CASE 
                    WHEN movie_id IS NOT NULL THEN 'movie'
                    ELSE 'tv'
                END as media_type,
                COUNT(*) as count
            FROM reviews
            GROUP BY media_type
        """).fetchall()
        
        self._store_kpi(conn, 'review_trends', 'media_type_split', [
            {'type': row['media_type'], 'count': row['count']}
            for row in media_split
        ])
        
        self.logger.info("Review trend KPIs computed")
    
    # =========================================================================
    # TITLE STATISTICS KPIs
    # =========================================================================
    
    def compute_title_stats(self, conn: sqlite3.Connection):
        """Compute aggregated title statistics"""
        self.logger.info("Computing title statistics KPIs...")
        
        # Most reviewed movies
        most_reviewed_movies = conn.execute("""
            SELECT 
                m.movie_id,
                m.title,
                m.poster_path,
                COUNT(r.review_id) as review_count,
                AVG(r.rating) as avg_user_rating,
                m.tmdb_vote_avg
            FROM movies m
            JOIN reviews r ON r.movie_id = m.movie_id
            GROUP BY m.movie_id
            ORDER BY review_count DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'title_stats', 'most_reviewed_movies', [
            {
                'id': row['movie_id'],
                'title': row['title'],
                'poster_path': row['poster_path'],
                'review_count': row['review_count'],
                'avg_user_rating': round(row['avg_user_rating'], 2) if row['avg_user_rating'] else 0,
                'tmdb_rating': row['tmdb_vote_avg']
            }
            for row in most_reviewed_movies
        ])
        
        # Most reviewed TV shows
        most_reviewed_shows = conn.execute("""
            SELECT 
                s.show_id,
                s.title,
                s.poster_path,
                COUNT(r.review_id) as review_count,
                AVG(r.rating) as avg_user_rating,
                s.tmdb_vote_avg
            FROM shows s
            JOIN reviews r ON r.show_id = s.show_id
            GROUP BY s.show_id
            ORDER BY review_count DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'title_stats', 'most_reviewed_shows', [
            {
                'id': row['show_id'],
                'title': row['title'],
                'poster_path': row['poster_path'],
                'review_count': row['review_count'],
                'avg_user_rating': round(row['avg_user_rating'], 2) if row['avg_user_rating'] else 0,
                'tmdb_rating': row['tmdb_vote_avg']
            }
            for row in most_reviewed_shows
        ])
        
        # Highest user-rated movies (with min 2 reviews)
        highest_rated_movies = conn.execute("""
            SELECT 
                m.movie_id,
                m.title,
                m.poster_path,
                COUNT(r.review_id) as review_count,
                AVG(r.rating) as avg_user_rating
            FROM movies m
            JOIN reviews r ON r.movie_id = m.movie_id
            GROUP BY m.movie_id
            HAVING COUNT(r.review_id) >= 2
            ORDER BY avg_user_rating DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'title_stats', 'highest_rated_movies', [
            {
                'id': row['movie_id'],
                'title': row['title'],
                'poster_path': row['poster_path'],
                'review_count': row['review_count'],
                'avg_user_rating': round(row['avg_user_rating'], 2)
            }
            for row in highest_rated_movies
        ])
        
        # Highest user-rated TV shows (with min 2 reviews)
        highest_rated_shows = conn.execute("""
            SELECT 
                s.show_id,
                s.title,
                s.poster_path,
                COUNT(r.review_id) as review_count,
                AVG(r.rating) as avg_user_rating
            FROM shows s
            JOIN reviews r ON r.show_id = s.show_id
            GROUP BY s.show_id
            HAVING COUNT(r.review_id) >= 2
            ORDER BY avg_user_rating DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'title_stats', 'highest_rated_shows', [
            {
                'id': row['show_id'],
                'title': row['title'],
                'poster_path': row['poster_path'],
                'review_count': row['review_count'],
                'avg_user_rating': round(row['avg_user_rating'], 2)
            }
            for row in highest_rated_shows
        ])
        
        # Most discussed movies
        most_discussed_movies = conn.execute("""
            SELECT 
                m.movie_id,
                m.title,
                m.poster_path,
                COUNT(DISTINCT d.discussion_id) as discussion_count,
                COUNT(c.comment_id) as total_comments
            FROM movies m
            JOIN discussions d ON d.movie_id = m.movie_id
            LEFT JOIN comments c ON c.discussion_id = d.discussion_id
            GROUP BY m.movie_id
            ORDER BY discussion_count DESC, total_comments DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'title_stats', 'most_discussed_movies', [
            {
                'id': row['movie_id'],
                'title': row['title'],
                'poster_path': row['poster_path'],
                'discussion_count': row['discussion_count'],
                'total_comments': row['total_comments']
            }
            for row in most_discussed_movies
        ])
        
        # Most discussed TV shows
        most_discussed_shows = conn.execute("""
            SELECT 
                s.show_id,
                s.title,
                s.poster_path,
                COUNT(DISTINCT d.discussion_id) as discussion_count,
                COUNT(c.comment_id) as total_comments
            FROM shows s
            JOIN discussions d ON d.show_id = s.show_id
            LEFT JOIN comments c ON c.discussion_id = d.discussion_id
            GROUP BY s.show_id
            ORDER BY discussion_count DESC, total_comments DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'title_stats', 'most_discussed_shows', [
            {
                'id': row['show_id'],
                'title': row['title'],
                'poster_path': row['poster_path'],
                'discussion_count': row['discussion_count'],
                'total_comments': row['total_comments']
            }
            for row in most_discussed_shows
        ])
        
        # Most watchlisted movies
        most_watchlisted_movies = conn.execute("""
            SELECT 
                m.movie_id,
                m.title,
                m.poster_path,
                COUNT(w.user_id) as watchlist_count
            FROM movies m
            JOIN watchlists w ON w.movie_id = m.movie_id
            GROUP BY m.movie_id
            ORDER BY watchlist_count DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'title_stats', 'most_watchlisted_movies', [
            {
                'id': row['movie_id'],
                'title': row['title'],
                'poster_path': row['poster_path'],
                'watchlist_count': row['watchlist_count']
            }
            for row in most_watchlisted_movies
        ])
        
        # Most watchlisted TV shows
        most_watchlisted_shows = conn.execute("""
            SELECT 
                s.show_id,
                s.title,
                s.poster_path,
                COUNT(w.user_id) as watchlist_count
            FROM shows s
            JOIN watchlists w ON w.show_id = s.show_id
            GROUP BY s.show_id
            ORDER BY watchlist_count DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'title_stats', 'most_watchlisted_shows', [
            {
                'id': row['show_id'],
                'title': row['title'],
                'poster_path': row['poster_path'],
                'watchlist_count': row['watchlist_count']
            }
            for row in most_watchlisted_shows
        ])
        
        self.logger.info("Title statistics KPIs computed")
    
    def compute_genre_stats(self, conn: sqlite3.Connection):
        """Compute genre-related statistics"""
        self.logger.info("Computing genre statistics KPIs...")
        
        # Most popular genres by movie count
        movie_genres = conn.execute("""
            SELECT 
                g.name as genre,
                COUNT(mg.movie_id) as movie_count
            FROM genres g
            LEFT JOIN movie_genres mg ON mg.genre_id = g.genre_id
            GROUP BY g.genre_id
            ORDER BY movie_count DESC
            LIMIT 15
        """).fetchall()
        
        self._store_kpi(conn, 'genre_stats', 'movie_genre_distribution', [
            {'genre': row['genre'], 'count': row['movie_count']}
            for row in movie_genres
        ])
        
        # Most popular genres by show count
        show_genres = conn.execute("""
            SELECT 
                g.name as genre,
                COUNT(sg.show_id) as show_count
            FROM genres g
            LEFT JOIN show_genres sg ON sg.genre_id = g.genre_id
            GROUP BY g.genre_id
            ORDER BY show_count DESC
            LIMIT 15
        """).fetchall()
        
        self._store_kpi(conn, 'genre_stats', 'show_genre_distribution', [
            {'genre': row['genre'], 'count': row['show_count']}
            for row in show_genres
        ])
        
        # Highest rated genres (by user reviews)
        genre_ratings = conn.execute("""
            SELECT 
                g.name as genre,
                AVG(r.rating) as avg_rating,
                COUNT(r.review_id) as review_count
            FROM genres g
            JOIN movie_genres mg ON mg.genre_id = g.genre_id
            JOIN reviews r ON r.movie_id = mg.movie_id
            WHERE r.rating IS NOT NULL
            GROUP BY g.genre_id
            HAVING COUNT(r.review_id) >= 5
            ORDER BY avg_rating DESC
            LIMIT 10
        """).fetchall()
        
        self._store_kpi(conn, 'genre_stats', 'highest_rated_genres', [
            {
                'genre': row['genre'],
                'avg_rating': round(row['avg_rating'], 2),
                'review_count': row['review_count']
            }
            for row in genre_ratings
        ])
        
        self.logger.info("Genre statistics KPIs computed")
    
    def compute_platform_stats(self, conn: sqlite3.Connection):
        """Compute overall platform statistics"""
        self.logger.info("Computing platform statistics KPIs...")
        
        # Overall counts
        overall_counts = conn.execute("""
            SELECT
                (SELECT COUNT(*) FROM movies) as total_movies,
                (SELECT COUNT(*) FROM shows) as total_shows,
                (SELECT COUNT(*) FROM reviews) as total_reviews,
                (SELECT COUNT(*) FROM discussions) as total_discussions,
                (SELECT COUNT(*) FROM comments) as total_comments,
                (SELECT COUNT(*) FROM watchlists) as total_watchlist_items,
                (SELECT COUNT(*) FROM users) as total_users,
                (SELECT COUNT(*) FROM genres) as total_genres
        """).fetchone()
        
        self._store_kpi(conn, 'platform_stats', 'overall_counts', {
            'movies': overall_counts['total_movies'],
            'shows': overall_counts['total_shows'],
            'reviews': overall_counts['total_reviews'],
            'discussions': overall_counts['total_discussions'],
            'comments': overall_counts['total_comments'],
            'watchlist_items': overall_counts['total_watchlist_items'],
            'users': overall_counts['total_users'],
            'genres': overall_counts['total_genres']
        })
        
        # Average ratings
        avg_ratings = conn.execute("""
            SELECT
                (SELECT AVG(rating) FROM reviews WHERE movie_id IS NOT NULL) as avg_movie_rating,
                (SELECT AVG(rating) FROM reviews WHERE show_id IS NOT NULL) as avg_show_rating,
                (SELECT AVG(rating) FROM reviews) as overall_avg_rating
        """).fetchone()
        
        self._store_kpi(conn, 'platform_stats', 'average_ratings', {
            'movies': round(avg_ratings['avg_movie_rating'], 2) if avg_ratings['avg_movie_rating'] else 0,
            'shows': round(avg_ratings['avg_show_rating'], 2) if avg_ratings['avg_show_rating'] else 0,
            'overall': round(avg_ratings['overall_avg_rating'], 2) if avg_ratings['overall_avg_rating'] else 0
        })
        
        # Activity in last 7 days
        recent_activity = conn.execute("""
            SELECT
                (SELECT COUNT(*) FROM reviews WHERE created_at >= DATE('now', '-7 days')) as reviews_7d,
                (SELECT COUNT(*) FROM discussions WHERE created_at >= DATE('now', '-7 days')) as discussions_7d,
                (SELECT COUNT(*) FROM comments WHERE created_at >= DATE('now', '-7 days')) as comments_7d,
                (SELECT COUNT(*) FROM watchlists WHERE added_at >= DATE('now', '-7 days')) as watchlist_adds_7d
        """).fetchone()
        
        self._store_kpi(conn, 'platform_stats', 'activity_7d', {
            'reviews': recent_activity['reviews_7d'],
            'discussions': recent_activity['discussions_7d'],
            'comments': recent_activity['comments_7d'],
            'watchlist_adds': recent_activity['watchlist_adds_7d']
        })
        
        self.logger.info("Platform statistics KPIs computed")
    
    # =========================================================================
    # MAIN RUN METHOD
    # =========================================================================
    
    def run_kpi_computation(self) -> dict:
        """Run all KPI computations"""
        start_time = time.time()
        
        self.logger.info("=" * 60)
        self.logger.info("Starting KPI precomputation...")
        self.logger.info("=" * 60)
        
        # Reset statistics
        self.stats = {k: 0 for k in self.stats.keys()}
        
        try:
            conn = self._get_db_connection()
            
            # Ensure KPI table exists
            self._ensure_kpi_table(conn)
            
            # Compute all KPIs
            self.compute_user_activity_kpis(conn)
            self.compute_review_trends(conn)
            self.compute_title_stats(conn)
            self.compute_genre_stats(conn)
            self.compute_platform_stats(conn)
            
            # Commit all changes
            conn.commit()
            conn.close()
            
            execution_time = time.time() - start_time
            self.stats['execution_time'] = f"{execution_time:.2f}s"
            
            self.logger.info("=" * 60)
            self.logger.info(f"KPI precomputation completed successfully")
            self.logger.info(f"Total KPIs computed: {self.stats['kpis_computed']}")
            self.logger.info(f"Execution time: {execution_time:.2f}s")
            self.logger.info("=" * 60)
            
            return self.stats
            
        except Exception as e:
            self.logger.error(f"KPI computation failed: {e}", exc_info=True)
            self.stats['errors'] += 1
            raise

