#!/usr/bin/env python3
"""Script to list all user accounts from the database."""
import sqlite3
import os
from pathlib import Path

# Find the database file
script_dir = Path(__file__).parent
db_path = script_dir / "movie_tracker.db"

if not db_path.exists():
    # Try alternative location
    db_path = script_dir / "backend" / ".." / "movie_tracker.db"
    db_path = db_path.resolve()

if not db_path.exists():
    print(f"Error: Database file not found at {db_path}")
    exit(1)

# Connect to database
conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row

# Check if is_admin column exists
cursor = conn.execute("PRAGMA table_info(users)")
columns = [row[1] for row in cursor.fetchall()]
has_is_admin = 'is_admin' in columns

# Query all users
if has_is_admin:
    rows = conn.execute("""
        SELECT user_id, email, password_plain, is_admin, created_at 
        FROM users 
        ORDER BY user_id
    """).fetchall()
else:
    rows = conn.execute("""
        SELECT user_id, email, password_plain, created_at 
        FROM users 
        ORDER BY user_id
    """).fetchall()

# Display results
print("\n" + "=" * 90)
print("ALL USER ACCOUNTS")
print("=" * 90)

if not rows:
    print("No accounts found in the database.")
else:
    print(f"{'ID':<6} | {'Email':<35} | {'Password':<20} | {'Admin':<8} | {'Created At'}")
    print("-" * 90)
    
    for row in rows:
        user_id = row['user_id']
        email = row['email'] if 'email' in row.keys() else 'N/A'
        password = row['password_plain'] if 'password_plain' in row.keys() and row['password_plain'] else 'N/A'
        is_admin = row['is_admin'] if has_is_admin and 'is_admin' in row.keys() else (0 if has_is_admin else None)
        admin_status = 'Yes' if (is_admin and has_is_admin and is_admin != 0) else ('No' if has_is_admin else 'N/A')
        created_at = row['created_at'] if 'created_at' in row.keys() and row['created_at'] else 'N/A'
        
        print(f"{user_id:<6} | {email:<35} | {password:<20} | {admin_status:<8} | {created_at}")

print("=" * 90)
print(f"Total accounts: {len(rows)}")
print("=" * 90)

conn.close()
