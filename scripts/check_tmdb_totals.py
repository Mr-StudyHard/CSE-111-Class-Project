#!/usr/bin/env python3
"""
Check how many movies and TV shows are available in TMDb API.
"""
import os
import sys
import requests
from dotenv import load_dotenv

API_BASE = "https://api.themoviedb.org/3"

def get_api_key():
    load_dotenv()
    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        print("Missing TMDB_API_KEY. Set it in your environment or .env file.", file=sys.stderr)
        sys.exit(1)
    return api_key

def get_total_count(api_key: str, endpoint: str):
    """Get total count from TMDb discover endpoint."""
    url = f"{API_BASE}{endpoint}"
    params = {"api_key": api_key, "page": 1}
    try:
        resp = requests.get(url, params=params, timeout=25)
        resp.raise_for_status()
        data = resp.json()
        total_results = data.get("total_results", 0)
        total_pages = data.get("total_pages", 0)
        return total_results, total_pages
    except Exception as e:
        print(f"Error fetching {endpoint}: {e}", file=sys.stderr)
        return None, None

def main():
    api_key = get_api_key()
    
    print("Querying TMDb API for total counts...")
    print("-" * 50)
    
    # Movies
    movies_total, movies_pages = get_total_count(api_key, "/discover/movie")
    if movies_total is not None:
        print(f"Movies: {movies_total:,} total results")
        print(f"        {movies_pages:,} pages (20 per page)")
    else:
        print("Movies: Error fetching count")
    
    # TV Shows
    tv_total, tv_pages = get_total_count(api_key, "/discover/tv")
    if tv_total is not None:
        print(f"TV Shows: {tv_total:,} total results")
        print(f"         {tv_pages:,} pages (20 per page)")
    else:
        print("TV Shows: Error fetching count")
    
    print("-" * 50)
    if movies_total and tv_total:
        print(f"Combined: {movies_total + tv_total:,} total titles")
    
    # Also check popular endpoints for comparison
    print("\nPopular endpoints (for reference):")
    popular_movies = get_total_count(api_key, "/movie/popular")
    popular_tv = get_total_count(api_key, "/tv/popular")
    if popular_movies[0]:
        print(f"Popular Movies: {popular_movies[0]:,} results")
    if popular_tv[0]:
        print(f"Popular TV Shows: {popular_tv[0]:,} results")

if __name__ == "__main__":
    main()





