#!/usr/bin/env python3
"""
Migration script to add title_comments table for discussion board feature
This table supports per-title comments with nested replies
"""
import os
import sqlite3
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "..", "movie_tracker.db"))

def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """Check if a table exists"""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    return cursor.fetchone() is not None

def migrate():
    """Add title_comments table if it doesn't exist"""
    if not os.path.exists(DB_PATH):
        print(f"Error: Database file not found at {DB_PATH}")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Check if table already exists
        if table_exists(conn, "title_comments"):
            print("[OK] Table 'title_comments' already exists")
            return True
        
        print("Creating 'title_comments' table...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS title_comments (
                comment_id      INTEGER PRIMARY KEY,
                title_type      TEXT NOT NULL CHECK (title_type IN ('movie', 'show')),
                title_id        INTEGER NOT NULL,
                user_id         INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                parent_comment_id INTEGER REFERENCES title_comments(comment_id) ON DELETE CASCADE,
                body            TEXT NOT NULL,
                created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at      TEXT DEFAULT CURRENT_TIMESTAMP,
                is_deleted      INTEGER DEFAULT 0 CHECK (is_deleted IN (0, 1))
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_title_comments_title ON title_comments(title_type, title_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_title_comments_user ON title_comments(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_title_comments_parent ON title_comments(parent_comment_id)")
        conn.commit()
        
        print("[OK] Successfully created 'title_comments' table with indexes")
        return True
        
    except sqlite3.Error as e:
        print(f"✗ Error during migration: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    print("=" * 80)
    print("Migration: Add title_comments table")
    print("=" * 80)
    success = migrate()
    sys.exit(0 if success else 1)

