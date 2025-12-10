#!/usr/bin/env python3
"""
Test release date filter functionality
"""
import sqlite3
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

DB_PATH = project_root / "movie_tracker.db"

def test_release_date_filter():
    """Test if release date filter/sort works correctly"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    print("=" * 80)
    print("Release Date Filter Diagnostic")
    print("=" * 80)
    
    # Find Five Nights at Freddy's 2
    print("\n1. Finding 'Five Nights at Freddy's 2':")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT movie_id, title, release_year, overview
        FROM movies 
        WHERE title LIKE '%Five Nights%Freddy%2%'
    """)
    fnaf2 = cursor.fetchone()
    if fnaf2:
        print(f"   Found: ID={fnaf2['movie_id']}, Title='{fnaf2['title']}', Release Year={fnaf2['release_year']}")
        fnaf2_year = fnaf2['release_year']
    else:
        print("   ERROR: 'Five Nights at Freddy's 2' not found!")
        return
    
    # Check if it has overview (required for filtering)
    if not fnaf2['overview']:
        print("   WARNING: Movie has no overview - will be filtered out!")
    
    # Find all movies with same release year
    print(f"\n2. Movies with release_year = {fnaf2_year}:")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT movie_id, title, release_year, 
               CASE WHEN overview IS NOT NULL AND overview != '' THEN 'Yes' ELSE 'No' END as has_overview
        FROM movies 
        WHERE release_year = ?
        ORDER BY title
        LIMIT 20
    """, (fnaf2_year,))
    same_year_movies = cursor.fetchall()
    print(f"   Found {len(same_year_movies)} movies with release_year={fnaf2_year}")
    for movie in same_year_movies[:10]:
        print(f"   - {movie['title']} (ID: {movie['movie_id']}, Has Overview: {movie['has_overview']})")
    
    # Test the actual query used by the API (with genre join requirement)
    print(f"\n3. Testing API query (with genre requirement):")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT DISTINCT m.movie_id, m.title, m.release_year
        FROM movies m
        INNER JOIN movie_genres mg ON m.movie_id = mg.movie_id
        INNER JOIN genres g ON g.genre_id = mg.genre_id
        WHERE m.overview IS NOT NULL 
          AND m.overview != ''
          AND m.release_year = ?
        ORDER BY m.release_year DESC, m.title
        LIMIT 20
    """, (fnaf2_year,))
    api_results = cursor.fetchall()
    print(f"   Found {len(api_results)} movies that would appear in API (with overview + genre)")
    
    fnaf2_in_results = False
    for movie in api_results:
        marker = " <-- FNAF2" if movie['movie_id'] == fnaf2['movie_id'] else ""
        if movie['movie_id'] == fnaf2['movie_id']:
            fnaf2_in_results = True
        print(f"   - {movie['title']} (Year: {movie['release_year']}){marker}")
    
    if not fnaf2_in_results:
        print("\n   [WARNING] 'Five Nights at Freddy's 2' is NOT in the API results!")
        print("   Checking why...")
        
        # Check if it has genres
        cursor = conn.execute("""
            SELECT g.name
            FROM movie_genres mg
            INNER JOIN genres g ON g.genre_id = mg.genre_id
            WHERE mg.movie_id = ?
        """, (fnaf2['movie_id'],))
        genres = cursor.fetchall()
        if genres:
            print(f"   [OK] Has genres: {', '.join([g['name'] for g in genres])}")
        else:
            print("   [ERROR] Has NO genres - this is why it's filtered out!")
        
        # Check overview
        if not fnaf2['overview']:
            print("   [ERROR] Has NO overview - this is why it's filtered out!")
        else:
            print(f"   [OK] Has overview: {fnaf2['overview'][:50]}...")
    else:
        print("\n   [OK] 'Five Nights at Freddy's 2' IS in the API results!")
    
    # Test sorting by release_date
    print(f"\n4. Testing sort by release_date (DESC - newest first):")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT DISTINCT m.movie_id, m.title, m.release_year
        FROM movies m
        INNER JOIN movie_genres mg ON m.movie_id = mg.movie_id
        INNER JOIN genres g ON g.genre_id = mg.genre_id
        WHERE m.overview IS NOT NULL 
          AND m.overview != ''
        ORDER BY (m.release_year IS NULL), m.release_year DESC, m.title
        LIMIT 10
    """)
    sorted_results = cursor.fetchall()
    print("   Top 10 movies when sorted by release_date DESC:")
    for i, movie in enumerate(sorted_results, 1):
        marker = " <-- FNAF2" if movie['movie_id'] == fnaf2['movie_id'] else ""
        print(f"   {i}. {movie['title']} ({movie['release_year']}){marker}")
    
    conn.close()
    print("\n" + "=" * 80)
    print("Diagnostic complete!")
    print("=" * 80)

if __name__ == "__main__":
    test_release_date_filter()

