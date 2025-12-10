#!/usr/bin/env python3
"""
Script to populate movies/shows that are future releases and new releases
relative to a specific date (2025-12-10).

Uses TMDb Discover API with date filters to fetch:
- Future releases: release_date > 2025-12-10
- New releases: release_date between (2025-12-10 - 90 days) and 2025-12-10
"""

import logging
import os
import sys
import yaml
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path to import ETL service
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from etl.tmdb_etl_service import TMDbETLService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('populate_releases.log', encoding='utf-8')
    ]
)

logger = logging.getLogger('PopulateReleases')

# Reference date: 2025-12-10
REFERENCE_DATE = datetime(2025, 12, 10)
FUTURE_START_DATE = REFERENCE_DATE.strftime('%Y-%m-%d')
NEW_RELEASES_START_DATE = (REFERENCE_DATE - timedelta(days=90)).strftime('%Y-%m-%d')
NEW_RELEASES_END_DATE = REFERENCE_DATE.strftime('%Y-%m-%d')

# Limits for each category
FUTURE_MOVIES_LIMIT = 50
FUTURE_SHOWS_LIMIT = 50
NEW_MOVIES_LIMIT = 50
NEW_SHOWS_LIMIT = 50


def load_config():
    """Load ETL configuration"""
    config_path = Path(__file__).parent.parent / 'etl_config.yaml'
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def discover_movies_by_date(etl_service: TMDbETLService, start_date: str, end_date: str = None, limit: int = 50):
    """
    Discover movies using TMDb Discover API with date filters
    
    Args:
        etl_service: TMDbETLService instance
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (optional)
        limit: Maximum number of movies to fetch
    """
    logger.info(f"Discovering movies from {start_date} to {end_date or 'future'}...")
    
    movies_fetched = []
    page = 1
    max_pages = 10  # TMDb typically returns 20 per page, so 10 pages = 200 max
    
    while len(movies_fetched) < limit and page <= max_pages:
        try:
            params = {
                'primary_release_date.gte': start_date,
                'sort_by': 'popularity.desc',
                'page': page
            }
            
            if end_date:
                params['primary_release_date.lte'] = end_date
            
            response = etl_service._api_get('/discover/movie', **params)
            results = response.get('results', [])
            
            if not results:
                break
            
            movies_fetched.extend(results)
            logger.info(f"Fetched page {page}: {len(results)} movies (total: {len(movies_fetched)})")
            
            # Check if there are more pages
            if page >= response.get('total_pages', 1):
                break
            
            page += 1
            
        except Exception as e:
            logger.error(f"Error fetching movies page {page}: {e}")
            break
    
    logger.info(f"Total movies discovered: {len(movies_fetched)}")
    return movies_fetched[:limit]


def discover_shows_by_date(etl_service: TMDbETLService, start_date: str, end_date: str = None, limit: int = 50):
    """
    Discover TV shows using TMDb Discover API with date filters
    
    Args:
        etl_service: TMDbETLService instance
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (optional)
        limit: Maximum number of shows to fetch
    """
    logger.info(f"Discovering TV shows from {start_date} to {end_date or 'future'}...")
    
    shows_fetched = []
    page = 1
    max_pages = 10
    
    while len(shows_fetched) < limit and page <= max_pages:
        try:
            params = {
                'first_air_date.gte': start_date,
                'sort_by': 'popularity.desc',
                'page': page
            }
            
            if end_date:
                params['first_air_date.lte'] = end_date
            
            response = etl_service._api_get('/discover/tv', **params)
            results = response.get('results', [])
            
            if not results:
                break
            
            shows_fetched.extend(results)
            logger.info(f"Fetched page {page}: {len(results)} shows (total: {len(shows_fetched)})")
            
            # Check if there are more pages
            if page >= response.get('total_pages', 1):
                break
            
            page += 1
            
        except Exception as e:
            logger.error(f"Error fetching shows page {page}: {e}")
            break
    
    logger.info(f"Total shows discovered: {len(shows_fetched)}")
    return shows_fetched[:limit]


def process_discovered_movies(etl_service: TMDbETLService, conn, movie_summaries: list):
    """Process discovered movies using the ETL service methods"""
    logger.info(f"Processing {len(movie_summaries)} discovered movies...")
    
    max_cast = etl_service.config.get('data_limits', {}).get('max_cast', 25)
    
    for summary in movie_summaries:
        movie_id = summary.get('id')
        if not movie_id:
            continue
        
        try:
            etl_service.stats['movies_processed'] += 1
            
            # Fetch detailed movie data
            detail = etl_service._api_get(
                f'/movie/{movie_id}',
                append_to_response='credits'
            )
            
            # Validate data quality
            if not etl_service._validate_movie_data(detail):
                logger.debug(f"Movie {movie_id} skipped due to quality filters")
                etl_service.stats['movies_skipped'] += 1
                continue
            
            # Transform data
            movie_data = etl_service._transform_movie_data(detail)
            
            # Fetch all person details BEFORE entering transaction
            credits = detail.get('credits', {}).get('cast', [])
            person_details_map = {}
            for cast in credits[:max_cast]:
                person_details = etl_service._fetch_person_details(cast.get('id'))
                person_details.update({
                    'id': cast.get('id'),
                    'name': cast.get('name'),
                    'profile_path': cast.get('profile_path'),
                })
                person_details_map[cast.get('id')] = person_details
            
            # Now do all database operations in a quick transaction
            with conn:
                # Upsert movie
                etl_service._upsert_movie(conn, movie_data)
                
                # Link genres
                etl_service._link_movie_genres(conn, movie_id, detail.get('genres', []))
                
                # Process cast
                for cast in credits[:max_cast]:
                    person_details = person_details_map.get(cast.get('id'))
                    if person_details:
                        etl_service._upsert_person(conn, person_details)
                        etl_service._attach_movie_cast(conn, movie_id, cast)
            
            if etl_service.stats['movies_processed'] % 10 == 0:
                logger.info(f"Processed {etl_service.stats['movies_processed']} movies...")
                
        except Exception as e:
            logger.error(f"Error processing movie {movie_id}: {e}")
            etl_service.stats['errors'] += 1


def process_discovered_shows(etl_service: TMDbETLService, conn, show_summaries: list, episodes_per_season: int = 10):
    """Process discovered TV shows using the ETL service methods"""
    logger.info(f"Processing {len(show_summaries)} discovered TV shows...")
    
    max_cast = etl_service.config.get('data_limits', {}).get('max_cast', 25)
    
    for summary in show_summaries:
        show_id = summary.get('id')
        if not show_id:
            continue
        
        try:
            etl_service.stats['shows_processed'] += 1
            
            # Fetch detailed show data
            detail = etl_service._api_get(
                f'/tv/{show_id}',
                append_to_response='aggregate_credits,seasons'
            )
            
            # Validate data quality
            if not etl_service._validate_show_data(detail):
                logger.debug(f"Show {show_id} skipped due to quality filters")
                etl_service.stats['shows_skipped'] += 1
                continue
            
            # Transform data
            show_data = etl_service._transform_show_data(detail)
            
            # Fetch all person details BEFORE entering transaction
            credits = detail.get('aggregate_credits', {}).get('cast', [])
            person_details_map = {}
            for cast in credits[:max_cast]:
                person_details = etl_service._fetch_person_details(cast.get('id'))
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
                    continue
                try:
                    season_detail = etl_service._api_get(f'/tv/{show_id}/season/{season_number}')
                    season_details_map[season_number] = season_detail
                except Exception as e:
                    logger.warning(f"Error fetching season {season_number} of show {show_id}: {e}")
            
            # Now do all database operations in a quick transaction
            with conn:
                # Upsert show
                etl_service._upsert_show(conn, show_data)
                
                # Link genres
                etl_service._link_show_genres(conn, show_id, detail.get('genres', []))
                
                # Process cast
                for cast in credits[:max_cast]:
                    person_details = person_details_map.get(cast.get('id'))
                    if person_details:
                        etl_service._upsert_person(conn, person_details)
                        etl_service._attach_show_cast(conn, show_id, cast)
                
                # Process seasons and episodes
                for season in seasons:
                    season_number = season.get('season_number')
                    if season_number in (None, 0):
                        continue
                    
                    etl_service._upsert_season(conn, show_id, season)
                    
                    season_detail = season_details_map.get(season_number)
                    if season_detail:
                        episodes = season_detail.get('episodes', [])[:episodes_per_season]
                        for episode in episodes:
                            etl_service._upsert_episode(conn, show_id, season_number, episode)
            
            if etl_service.stats['shows_processed'] % 10 == 0:
                logger.info(f"Processed {etl_service.stats['shows_processed']} shows...")
                
        except Exception as e:
            logger.error(f"Error processing show {show_id}: {e}")
            etl_service.stats['errors'] += 1


def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("Populate Releases Script")
    logger.info(f"Reference Date: {REFERENCE_DATE.strftime('%Y-%m-%d')}")
    logger.info("=" * 80)
    
    # Load configuration
    config = load_config()
    
    # Initialize ETL service
    try:
        etl_service = TMDbETLService(config)
    except Exception as e:
        logger.error(f"Failed to initialize ETL service: {e}")
        sys.exit(1)
    
    # Get database connection
    conn = etl_service._get_db_connection()
    
    try:
        # Sync genres first
        logger.info("Syncing genres...")
        etl_service.sync_genres(conn)
        
        # ============================================================
        # FUTURE RELEASES
        # ============================================================
        logger.info("\n" + "=" * 80)
        logger.info("FETCHING FUTURE RELEASES")
        logger.info("=" * 80)
        
        # Future movies
        logger.info(f"\nFetching future movies (release_date >= {FUTURE_START_DATE})...")
        future_movies = discover_movies_by_date(
            etl_service,
            start_date=FUTURE_START_DATE,
            end_date=None,
            limit=FUTURE_MOVIES_LIMIT
        )
        process_discovered_movies(etl_service, conn, future_movies)
        
        # Future TV shows
        logger.info(f"\nFetching future TV shows (first_air_date >= {FUTURE_START_DATE})...")
        future_shows = discover_shows_by_date(
            etl_service,
            start_date=FUTURE_START_DATE,
            end_date=None,
            limit=FUTURE_SHOWS_LIMIT
        )
        episodes_per_season = config.get('data_limits', {}).get('episodes_per_season', 10)
        process_discovered_shows(etl_service, conn, future_shows, episodes_per_season)
        
        # ============================================================
        # NEW RELEASES (last 90 days)
        # ============================================================
        logger.info("\n" + "=" * 80)
        logger.info("FETCHING NEW RELEASES (Last 90 Days)")
        logger.info("=" * 80)
        
        # New movies
        logger.info(f"\nFetching new movies ({NEW_RELEASES_START_DATE} to {NEW_RELEASES_END_DATE})...")
        new_movies = discover_movies_by_date(
            etl_service,
            start_date=NEW_RELEASES_START_DATE,
            end_date=NEW_RELEASES_END_DATE,
            limit=NEW_MOVIES_LIMIT
        )
        process_discovered_movies(etl_service, conn, new_movies)
        
        # New TV shows
        logger.info(f"\nFetching new TV shows ({NEW_RELEASES_START_DATE} to {NEW_RELEASES_END_DATE})...")
        new_shows = discover_shows_by_date(
            etl_service,
            start_date=NEW_RELEASES_START_DATE,
            end_date=NEW_RELEASES_END_DATE,
            limit=NEW_SHOWS_LIMIT
        )
        process_discovered_shows(etl_service, conn, new_shows, episodes_per_season)
        
        # ============================================================
        # SUMMARY
        # ============================================================
        logger.info("\n" + "=" * 80)
        logger.info("SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Movies processed: {etl_service.stats['movies_processed']}")
        logger.info(f"  - Inserted: {etl_service.stats['movies_inserted']}")
        logger.info(f"  - Updated: {etl_service.stats['movies_updated']}")
        logger.info(f"  - Skipped: {etl_service.stats['movies_skipped']}")
        logger.info(f"Shows processed: {etl_service.stats['shows_processed']}")
        logger.info(f"  - Inserted: {etl_service.stats['shows_inserted']}")
        logger.info(f"  - Updated: {etl_service.stats['shows_updated']}")
        logger.info(f"  - Skipped: {etl_service.stats['shows_skipped']}")
        logger.info(f"Genres synced: {etl_service.stats['genres_synced']}")
        logger.info(f"People synced: {etl_service.stats['people_synced']}")
        logger.info(f"API calls made: {etl_service.stats['api_calls']}")
        logger.info(f"Errors: {etl_service.stats['errors']}")
        logger.info("=" * 80)
        
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == '__main__':
    main()


