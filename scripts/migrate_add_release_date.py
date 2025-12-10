#!/usr/bin/env python3
"""
Migration script to add release_date column to movies table
This allows proper chronological sorting by full date instead of just year
"""
import os
import sqlite3
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "..", "movie_tracker.db"))

def has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check if a column exists in a table"""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in rows)

def migrate():
    """Add release_date column if it doesn't exist"""
    if not os.path.exists(DB_PATH):
        print(f"Error: Database file not found at {DB_PATH}")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Check if column already exists
        if has_column(conn, "movies", "release_date"):
            print("[OK] Column 'release_date' already exists in movies table")
            return True
        
        print("Adding 'release_date' column to movies table...")
        conn.execute("ALTER TABLE movies ADD COLUMN release_date TEXT")
        conn.commit()
        
        print("[OK] Successfully added 'release_date' column")
        
        # Count movies
        count = conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
        print(f"  Database has {count} movies")
        print("  Note: Existing movies will get release_date populated on next ETL run")
        
        return True
        
    except sqlite3.Error as e:
        print(f"✗ Error during migration: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    print("=" * 80)
    print("Release Date Column Migration")
    print("=" * 80)
    print(f"\nDatabase: {DB_PATH}")
    print()
    
    success = migrate()
    
    print()
    print("=" * 80)
    if success:
        print("Migration completed successfully!")
    else:
        print("Migration failed!")
        sys.exit(1)

