#!/usr/bin/env python3
"""
Test release date sorting functionality
"""
import sqlite3
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
DB_PATH = project_root / "movie_tracker.db"

def test_release_date_sorting():
    """Test if release date sorting works correctly"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    print("=" * 80)
    print("Release Date Sorting Diagnostic")
    print("=" * 80)
    
    # Find Five Nights at Freddy's 2
    print("\n1. Finding 'Five Nights at Freddy's 2':")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT movie_id, title, release_year, release_date
        FROM movies 
        WHERE title LIKE '%Five Nights%Freddy%2%'
    """)
    fnaf2 = cursor.fetchone()
    if fnaf2:
        print(f"   Found: ID={fnaf2['movie_id']}, Title='{fnaf2['title']}'")
        print(f"   Release Year: {fnaf2['release_year']}")
        print(f"   Release Date: {fnaf2['release_date'] or 'NULL (not populated)'}")
        fnaf2_id = fnaf2['movie_id']
        fnaf2_year = fnaf2['release_year']
    else:
        print("   ERROR: 'Five Nights at Freddy's 2' not found!")
        return
    
    # Check how many movies have release_date populated
    print(f"\n2. Database Status:")
    print("-" * 80)
    cursor = conn.execute("SELECT COUNT(*) as cnt FROM movies WHERE release_date IS NOT NULL")
    with_date = cursor.fetchone()['cnt']
    cursor = conn.execute("SELECT COUNT(*) as cnt FROM movies WHERE release_year = 2025")
    total_2025 = cursor.fetchone()['cnt']
    print(f"   Movies with release_date populated: {with_date}")
    print(f"   Movies with release_year = 2025: {total_2025}")
    
    if with_date == 0:
        print("\n   [WARNING] No movies have release_date populated yet!")
        print("   Run ETL scheduler to populate release_date for existing movies.")
        print("   For now, testing with release_year sorting...")
    
    # Test sorting by release_date (new API logic)
    print(f"\n3. Testing API sorting logic (release_date DESC):")
    print("-" * 80)
    
    # Simulate the API query with COALESCE logic
    cursor = conn.execute("""
        SELECT DISTINCT m.movie_id, m.title, m.release_year, m.release_date,
               COALESCE(m.release_date, CAST(m.release_year AS TEXT)) AS release_sort
        FROM movies m
        INNER JOIN movie_genres mg ON m.movie_id = mg.movie_id
        INNER JOIN genres g ON g.genre_id = mg.genre_id
        WHERE m.overview IS NOT NULL 
          AND m.overview != ''
          AND m.release_year = 2025
        ORDER BY (COALESCE(m.release_date, CAST(m.release_year AS TEXT)) IS NULL), 
                 COALESCE(m.release_date, CAST(m.release_year AS TEXT)) DESC, 
                 m.title
        LIMIT 30
    """)
    sorted_results = cursor.fetchall()
    
    print(f"   Top 30 movies when sorted by release_date (2025 only):")
    fnaf2_position = None
    for i, movie in enumerate(sorted_results, 1):
        marker = ""
        if movie['movie_id'] == fnaf2_id:
            fnaf2_position = i
            marker = " <-- FNAF2"
        release_info = movie['release_date'] if movie['release_date'] else f"Year: {movie['release_year']}"
        print(f"   {i:2d}. {movie['title'][:50]:50s} ({release_info}){marker}")
    
    if fnaf2_position:
        print(f"\n   [OK] 'Five Nights at Freddy's 2' found at position {fnaf2_position}")
    else:
        print(f"\n   [WARNING] 'Five Nights at Freddy's 2' not in top 30 results")
        print("   Checking if it's in the full list...")
        cursor = conn.execute("""
            SELECT COUNT(*) as pos FROM (
                SELECT DISTINCT m.movie_id
                FROM movies m
                INNER JOIN movie_genres mg ON m.movie_id = mg.movie_id
                INNER JOIN genres g ON g.genre_id = mg.genre_id
                WHERE m.overview IS NOT NULL 
                  AND m.overview != ''
                  AND m.release_year = 2025
                  AND (COALESCE(m.release_date, CAST(m.release_year AS TEXT)) IS NULL OR
                       COALESCE(m.release_date, CAST(m.release_year AS TEXT)) > 
                       COALESCE((SELECT release_date FROM movies WHERE movie_id = ?), 
                                CAST((SELECT release_year FROM movies WHERE movie_id = ?) AS TEXT)))
            )
        """, (fnaf2_id, fnaf2_id))
        position = cursor.fetchone()['pos'] + 1
        print(f"   Estimated position: {position} out of {total_2025}")
    
    # Test with actual release dates if available
    if with_date > 0:
        print(f"\n4. Testing with actual release dates:")
        print("-" * 80)
        cursor = conn.execute("""
            SELECT DISTINCT m.movie_id, m.title, m.release_date
            FROM movies m
            INNER JOIN movie_genres mg ON m.movie_id = mg.movie_id
            INNER JOIN genres g ON g.genre_id = mg.genre_id
            WHERE m.overview IS NOT NULL 
              AND m.overview != ''
              AND m.release_date IS NOT NULL
              AND m.release_year = 2025
            ORDER BY m.release_date DESC, m.title
            LIMIT 20
        """)
        date_sorted = cursor.fetchall()
        print(f"   Top 20 movies with actual release dates (2025):")
        for i, movie in enumerate(date_sorted, 1):
            marker = " <-- FNAF2" if movie['movie_id'] == fnaf2_id else ""
            print(f"   {i:2d}. {movie['title'][:45]:45s} ({movie['release_date']}){marker}")
    
    # Compare old vs new sorting
    print(f"\n5. Comparison: Old (year only) vs New (full date) sorting:")
    print("-" * 80)
    print("   Old method: ORDER BY release_year DESC, title")
    print("   New method: ORDER BY release_date DESC, title (with year fallback)")
    print("\n   If release_date is NULL, both methods behave the same.")
    print("   If release_date is populated, new method sorts chronologically.")
    
    conn.close()
    print("\n" + "=" * 80)
    print("Diagnostic complete!")
    print("=" * 80)

if __name__ == "__main__":
    test_release_date_sorting()


