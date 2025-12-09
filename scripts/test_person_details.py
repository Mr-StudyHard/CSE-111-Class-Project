#!/usr/bin/env python3
"""
Test script to verify the extended person details functionality.
This script will:
1. Check that the people table has all the new columns
2. Fetch a small sample of movies/shows with cast
3. Verify that person details are populated
4. Test the API endpoint
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "..", "movie_tracker.db"))


def check_schema():
    """Check if the people table has all the new columns"""
    print("=" * 60)
    print("CHECKING DATABASE SCHEMA")
    print("=" * 60)
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    columns = conn.execute("PRAGMA table_info(people)").fetchall()
    column_names = [col["name"] for col in columns]
    
    print(f"\nPeople table columns ({len(column_names)}):")
    for col in columns:
        print(f"  - {col['name']}: {col['type']}")
    
    required_columns = [
        "person_id", "tmdb_person_id", "name", "profile_path",
        "birthday", "deathday", "place_of_birth", "biography",
        "imdb_id", "instagram_id", "twitter_id", "facebook_id"
    ]
    
    missing = [col for col in required_columns if col not in column_names]
    if missing:
        print(f"\n[FAIL] MISSING COLUMNS: {', '.join(missing)}")
        return False
    else:
        print(f"\n[PASS] All required columns present!")
        return True


def check_sample_data():
    """Check if we have any people with extended details"""
    print("\n" + "=" * 60)
    print("CHECKING SAMPLE DATA")
    print("=" * 60)
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # Count total people
    total = conn.execute("SELECT COUNT(*) as count FROM people").fetchone()["count"]
    print(f"\nTotal people in database: {total}")
    
    # Count people with extended data
    with_birthday = conn.execute(
        "SELECT COUNT(*) as count FROM people WHERE birthday IS NOT NULL"
    ).fetchone()["count"]
    
    with_biography = conn.execute(
        "SELECT COUNT(*) as count FROM people WHERE biography IS NOT NULL"
    ).fetchone()["count"]
    
    with_imdb = conn.execute(
        "SELECT COUNT(*) as count FROM people WHERE imdb_id IS NOT NULL"
    ).fetchone()["count"]
    
    print(f"People with birthday: {with_birthday} ({with_birthday/total*100 if total > 0 else 0:.1f}%)")
    print(f"People with biography: {with_biography} ({with_biography/total*100 if total > 0 else 0:.1f}%)")
    print(f"People with IMDB ID: {with_imdb} ({with_imdb/total*100 if total > 0 else 0:.1f}%)")
    
    # Show a sample person with details
    sample = conn.execute(
        """
        SELECT name, birthday, place_of_birth, imdb_id, 
               LENGTH(biography) as bio_length
        FROM people
        WHERE birthday IS NOT NULL
        LIMIT 3
        """
    ).fetchall()
    
    if sample:
        print("\nSample people with extended details:")
        for person in sample:
            print(f"\n  Name: {person['name']}")
            print(f"  Birthday: {person['birthday']}")
            print(f"  Birthplace: {person['place_of_birth']}")
            print(f"  IMDB ID: {person['imdb_id']}")
            print(f"  Biography length: {person['bio_length']} chars")
    else:
        print("\n[WARN] No people with extended details found.")
        print("   Run the ETL script to populate data:")
        print("   python scripts/tmdb_etl.py --movies 5 --shows 2")
    
    conn.close()


def test_api_endpoint():
    """Test the /api/people/<id> endpoint"""
    print("\n" + "=" * 60)
    print("TESTING API ENDPOINT")
    print("=" * 60)
    
    try:
        import requests
        
        # Try to reach the backend
        response = requests.get("http://localhost:5000/api/people/1", timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            print("\n[PASS] API endpoint working!")
            print(f"   Person: {data.get('name')}")
            print(f"   Birthday: {data.get('birthday')}")
            print(f"   Biography: {data.get('biography', '')[:100]}...")
            print(f"   Social links: {len(data.get('social_links', {}))}")
        elif response.status_code == 404:
            print("\n[WARN] Person not found (ID=1). Database might be empty.")
        else:
            print(f"\n[FAIL] API returned status code: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("\n[WARN] Backend server not running.")
        print("   Start it with: python run_server.py")
    except ImportError:
        print("\n[WARN] 'requests' library not installed.")
        print("   Install with: pip install requests")
    except Exception as e:
        print(f"\n[FAIL] Error testing API: {e}")


def main():
    print("\n" + "=" * 60)
    print("PERSON DETAILS EXTENSION TEST")
    print("=" * 60)
    
    if not os.path.exists(DB_PATH):
        print(f"\n[FAIL] Database not found at: {DB_PATH}")
        print("   Create it by running: python scripts/tmdb_etl.py")
        sys.exit(1)
    
    print(f"\nUsing database: {DB_PATH}\n")
    
    # Run checks
    schema_ok = check_schema()
    check_sample_data()
    test_api_endpoint()
    
    print("\n" + "=" * 60)
    if schema_ok:
        print("[PASS] SCHEMA CHECKS PASSED")
    else:
        print("[FAIL] SCHEMA CHECKS FAILED")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()

