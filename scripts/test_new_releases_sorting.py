#!/usr/bin/env python3
"""
Test new releases carousel sorting
"""
import sqlite3
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
DB_PATH = project_root / "movie_tracker.db"

def test_new_releases_sorting():
    """Test if new releases carousel sorts correctly by release_date"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    print("=" * 80)
    print("New Releases Carousel Sorting Diagnostic")
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
        print(f"   Release Date: {fnaf2['release_date'] or 'NULL'}")
        fnaf2_id = fnaf2['movie_id']
    else:
        print("   ERROR: 'Five Nights at Freddy's 2' not found!")
        return
    
    # Test the new-releases query (movies only) - using updated sorting logic
    print(f"\n2. Testing New Releases API query (movies, limit=12):")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT 'movie' AS media_type,
               m.movie_id AS item_id,
               m.tmdb_id,
               m.title,
               m.tmdb_vote_avg AS score,
               m.popularity,
               COALESCE(m.release_date, CAST(m.release_year AS TEXT)) AS release_sort,
               COALESCE(m.release_date, CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END) AS release_date
        FROM movies m
        INNER JOIN movie_genres mg ON mg.movie_id = m.movie_id
        INNER JOIN genres g ON g.genre_id = mg.genre_id
        WHERE m.release_year IS NOT NULL AND m.overview IS NOT NULL AND m.overview != ''
        GROUP BY m.movie_id
        ORDER BY 
            (COALESCE(m.release_date, CAST(m.release_year AS TEXT)) IS NULL),
            CASE 
                WHEN m.release_date IS NOT NULL THEN m.release_date
                ELSE CAST(m.release_year AS TEXT) || '-12-31'
            END DESC,
            (score IS NULL), score DESC, popularity DESC, title
        LIMIT 12
    """)
    new_releases = cursor.fetchall()
    
    print(f"   Top 12 new releases (movies):")
    fnaf2_position = None
    for i, movie in enumerate(new_releases, 1):
        marker = ""
        if movie['item_id'] == fnaf2_id:
            fnaf2_position = i
            marker = " <-- FNAF2"
        release_info = movie['release_date'] if movie['release_date'] else f"Year: {movie['release_sort']}"
        print(f"   {i:2d}. {movie['title'][:50]:50s} ({release_info}){marker}")
    
    if fnaf2_position:
        print(f"\n   [OK] 'Five Nights at Freddy's 2' found at position {fnaf2_position}")
        if fnaf2_position <= 6:
            print(f"   [OK] Will appear in first carousel page (6 items per page)")
        else:
            print(f"   [INFO] Will appear in carousel page {((fnaf2_position - 1) // 6) + 1}")
    else:
        print(f"\n   [WARNING] 'Five Nights at Freddy's 2' not in top 12 new releases")
    
    # Test all media (movies + shows)
    print(f"\n3. Testing New Releases API query (all media, limit=12):")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT *
        FROM (
            SELECT 'movie' AS media_type,
                   m.movie_id AS item_id,
                   m.tmdb_id,
                   m.title,
                   m.tmdb_vote_avg AS score,
                   m.popularity,
                   COALESCE(m.release_date, CAST(m.release_year AS TEXT)) AS release_sort,
                   COALESCE(m.release_date, CASE WHEN m.release_year IS NOT NULL THEN CAST(m.release_year AS TEXT) ELSE NULL END) AS release_date
            FROM movies m
            INNER JOIN movie_genres mg ON mg.movie_id = m.movie_id
            INNER JOIN genres g ON g.genre_id = mg.genre_id
            WHERE m.release_year IS NOT NULL AND m.overview IS NOT NULL AND m.overview != ''
            GROUP BY m.movie_id
            UNION ALL
            SELECT 'tv' AS media_type,
                   s.show_id AS item_id,
                   s.tmdb_id,
                   s.title,
                   s.tmdb_vote_avg AS score,
                   s.popularity,
                   CASE
                       WHEN s.first_air_date IS NOT NULL THEN CAST(substr(s.first_air_date, 1, 4) AS INTEGER)
                       ELSE NULL
                   END AS release_sort,
                   s.first_air_date AS release_date
            FROM shows s
            INNER JOIN show_genres sg ON sg.show_id = s.show_id
            INNER JOIN genres g ON g.genre_id = sg.genre_id
            WHERE s.first_air_date IS NOT NULL AND s.overview IS NOT NULL AND s.overview != ''
            GROUP BY s.show_id
        )
        ORDER BY (release_sort IS NULL), release_sort DESC, (score IS NULL), score DESC, popularity DESC, title
        LIMIT 12
    """)
    all_new_releases = cursor.fetchall()
    
    print(f"   Top 12 new releases (all media):")
    fnaf2_position_all = None
    for i, item in enumerate(all_new_releases, 1):
        marker = ""
        if item['media_type'] == 'movie' and item['item_id'] == fnaf2_id:
            fnaf2_position_all = i
            marker = " <-- FNAF2"
        release_info = item['release_date'] if item['release_date'] else f"Year: {item['release_sort']}"
        media_type = item['media_type'].upper()
        print(f"   {i:2d}. [{media_type}] {item['title'][:45]:45s} ({release_info}){marker}")
    
    if fnaf2_position_all:
        print(f"\n   [OK] 'Five Nights at Freddy's 2' found at position {fnaf2_position_all}")
    else:
        print(f"\n   [INFO] 'Five Nights at Freddy's 2' not in top 12 (may be filtered by score/popularity)")
    
    # Check if sorting is chronological
    print(f"\n4. Verifying chronological sorting:")
    print("-" * 80)
    dates = [r['release_date'] for r in new_releases if r['release_date']]
    if len(dates) > 1:
        is_sorted = all(dates[i] >= dates[i+1] for i in range(len(dates)-1))
        if is_sorted:
            print("   [OK] Release dates are sorted chronologically (newest first)")
        else:
            print("   [ERROR] Release dates are NOT sorted chronologically!")
            print("   Dates:", dates[:5])
    else:
        print("   [INFO] Not enough movies with release_date to verify sorting")
    
    conn.close()
    print("\n" + "=" * 80)
    print("Diagnostic complete!")
    print("=" * 80)

if __name__ == "__main__":
    test_new_releases_sorting()

