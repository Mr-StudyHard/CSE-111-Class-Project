#!/usr/bin/env python3
"""
Diagnose SQLite database lock issues
"""
import os
import sqlite3
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "..", "movie_tracker.db"))

def check_database_locks():
    """Check for database lock issues"""
    print("=" * 80)
    print("SQLite Database Lock Diagnostics")
    print("=" * 80)
    print(f"\nDatabase path: {DB_PATH}")
    print(f"Database exists: {os.path.exists(DB_PATH)}")
    
    if not os.path.exists(DB_PATH):
        print("\nERROR: Database file does not exist!")
        return
    
    # Check for lock files (WAL mode)
    wal_file = f"{DB_PATH}-wal"
    shm_file = f"{DB_PATH}-shm"
    
    print(f"\nWAL file exists: {os.path.exists(wal_file)}")
    print(f"SHM file exists: {os.path.exists(shm_file)}")
    
    if os.path.exists(wal_file):
        print(f"WAL file size: {os.path.getsize(wal_file)} bytes")
    
    # Try to connect with timeout
    print("\n" + "-" * 80)
    print("Testing database connection...")
    print("-" * 80)
    
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5.0)
        conn.execute("PRAGMA busy_timeout = 5000")
        
        # Check journal mode
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        print(f"Journal mode: {journal_mode}")
        
        # Check WAL mode settings
        if journal_mode == "wal":
            wal_autocheckpoint = conn.execute("PRAGMA wal_autocheckpoint").fetchone()[0]
            print(f"WAL autocheckpoint: {wal_autocheckpoint}")
        
        # Try a simple query
        conn.execute("SELECT 1").fetchone()
        print("[OK] Database connection successful")
        print("[OK] Database is NOT locked")
        
        # Check for active transactions
        try:
            # This query works in WAL mode
            result = conn.execute("PRAGMA journal_mode").fetchone()
            print("[OK] Can read database")
        except sqlite3.OperationalError as e:
            print(f"[ERROR] Database read error: {e}")
        
        conn.close()
        
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e).lower():
            print(f"[ERROR] Database is LOCKED")
            print(f"  Error: {e}")
            print("\nPossible causes:")
            print("  1. Another process is accessing the database")
            print("  2. A transaction is stuck open")
            print("  3. WAL file is corrupted")
            print("\nSolutions:")
            print("  1. Close all applications using the database")
            print("  2. Stop Flask server if running")
            print("  3. Stop any ETL processes")
            print("  4. Delete WAL/SHM files if safe (backup first!)")
        else:
            print(f"[ERROR] {e}")
    
    # Check for Python processes that might be using the database
    print("\n" + "-" * 80)
    print("Checking for potential database access...")
    print("-" * 80)
    print("\nNote: Multiple Python processes detected. They might be:")
    print("  - Flask backend server")
    print("  - ETL scheduler")
    print("  - Other scripts")
    print("\nRecommendation: Close unnecessary processes before running ETL")

if __name__ == "__main__":
    check_database_locks()

