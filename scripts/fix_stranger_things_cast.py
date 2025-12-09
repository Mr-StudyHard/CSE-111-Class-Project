#!/usr/bin/env python3
"""
Fix incorrect cast data for Stranger Things.
Corrects Marjorie Reynolds (incorrect) -> Millie Bobby Brown (correct) for Eleven character.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "..", "movie_tracker.db"))


def fix_stranger_things_cast():
    """Fix the incorrect cast association for Stranger Things."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    
    try:
        # Find Stranger Things show_id
        show_row = conn.execute(
            "SELECT show_id, tmdb_id FROM shows WHERE title = 'Stranger Things' LIMIT 1"
        ).fetchone()
        
        if not show_row:
            print("ERROR: Stranger Things not found in database")
            return False
        
        show_id = show_row["show_id"]
        show_tmdb_id = show_row["tmdb_id"]
        print(f"Found Stranger Things: show_id={show_id}, tmdb_id={show_tmdb_id}")
        
        # Find the incorrect cast entry (Marjorie Reynolds as Eleven)
        incorrect_cast = conn.execute(
            """
            SELECT sc.show_id, sc.person_id, sc.character, p.name, p.tmdb_person_id
            FROM show_cast sc
            JOIN people p ON p.person_id = sc.person_id
            WHERE sc.show_id = ? AND sc.character = 'Eleven' AND p.name LIKE '%Marjorie%'
            """,
            (show_id,)
        ).fetchone()
        
        if incorrect_cast:
            print(f"\nFound incorrect cast entry:")
            print(f"  Person: {incorrect_cast['name']} (tmdb_id: {incorrect_cast['tmdb_person_id']})")
            print(f"  Character: {incorrect_cast['character']}")
            
            # Millie Bobby Brown's TMDb ID is 87545
            # Check if the person record with tmdb_person_id 87545 is incorrectly named
            person_87545 = conn.execute(
                "SELECT person_id, name, tmdb_person_id FROM people WHERE tmdb_person_id = 87545 LIMIT 1"
            ).fetchone()
            
            if not person_87545:
                print("\nERROR: Person with tmdb_person_id 87545 not found")
                print("You may need to run the ETL script to populate cast data.")
                return False
            
            # Check if there's a separate Millie Bobby Brown record
            millie_record = conn.execute(
                "SELECT person_id, name, tmdb_person_id FROM people WHERE name LIKE '%Millie Bobby Brown%' LIMIT 1"
            ).fetchone()
            
            with conn:
                # If the person record with tmdb_id 87545 has wrong name, fix it
                if person_87545['name'] != 'Millie Bobby Brown':
                    conn.execute(
                        "UPDATE people SET name = 'Millie Bobby Brown' WHERE tmdb_person_id = 87545"
                    )
                    print(f"\nFixed person record: Updated name from '{person_87545['name']}' to 'Millie Bobby Brown'")
                    millie_person_id = person_87545['person_id']
                else:
                    millie_person_id = person_87545['person_id']
                
                # Delete ALL Eleven cast entries first (we'll add the correct one)
                conn.execute(
                    "DELETE FROM show_cast WHERE show_id = ? AND character = 'Eleven'",
                    (show_id,)
                )
                print(f"\nDeleted all Eleven cast entries (including duplicates)")
                
                # If there's a duplicate Millie record, delete it after moving any other cast entries
                if millie_record and millie_record['person_id'] != millie_person_id:
                    print(f"\nFound duplicate Millie Bobby Brown record (person_id: {millie_record['person_id']})")
                    # Check if duplicate has any other cast entries besides Eleven (which we just deleted)
                    other_cast = conn.execute(
                        "SELECT COUNT(*) as cnt FROM show_cast WHERE person_id = ?",
                        (millie_record['person_id'],)
                    ).fetchone()
                    if other_cast['cnt'] == 0:
                        conn.execute(
                            "DELETE FROM people WHERE person_id = ?",
                            (millie_record['person_id'],)
                        )
                        print(f"Deleted duplicate person record (no other cast entries)")
                
                # Insert the correct cast entry for Millie Bobby Brown as Eleven
                # Use INSERT OR IGNORE in case there's still a conflict
                conn.execute(
                    """
                    INSERT OR IGNORE INTO show_cast (show_id, person_id, character, cast_order) 
                    VALUES (?, ?, 'Eleven', 1)
                    """,
                    (show_id, millie_person_id)
                )
                # Update if it already exists
                conn.execute(
                    """
                    UPDATE show_cast 
                    SET character = 'Eleven', cast_order = 1 
                    WHERE show_id = ? AND person_id = ?
                    """,
                    (show_id, millie_person_id)
                )
                print(f"Ensured correct cast entry: Millie Bobby Brown -> Eleven")
                
                conn.commit()
                print("\n[SUCCESS] Successfully fixed Stranger Things cast!")
                return True
        else:
            # Check if the correct entry already exists
            correct_cast = conn.execute(
                """
                SELECT sc.show_id, sc.person_id, sc.character, p.name
                FROM show_cast sc
                JOIN people p ON p.person_id = sc.person_id
                WHERE sc.show_id = ? AND sc.character = 'Eleven'
                """,
                (show_id,)
            ).fetchone()
            
            if correct_cast:
                if 'Millie' in correct_cast['name'] or 'Millie Bobby Brown' == correct_cast['name']:
                    print(f"\n[SUCCESS] Cast entry is already correct!")
                    print(f"  {correct_cast['name']} is correctly listed as Eleven")
                    return True
                else:
                    print(f"\nWARNING: Found cast entry for Eleven but it's not Millie Bobby Brown:")
                    print(f"  Person: {correct_cast['name']}")
                    print(f"  Character: {correct_cast['character']}")
                    return False
            else:
                print("\nNo cast entry found for Eleven character")
                print("You may need to run the ETL script to populate cast data.")
                return False
                
    except Exception as e:
        print(f"\nERROR: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def verify_cast():
    """Verify the cast for Stranger Things."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    try:
        show_row = conn.execute(
            "SELECT show_id FROM shows WHERE title = 'Stranger Things' LIMIT 1"
        ).fetchone()
        
        if not show_row:
            print("Stranger Things not found")
            return
        
        show_id = show_row["show_id"]
        cast = conn.execute(
            """
            SELECT p.name, sc.character, sc.cast_order
            FROM show_cast sc
            JOIN people p ON p.person_id = sc.person_id
            WHERE sc.show_id = ?
            ORDER BY sc.cast_order ASC
            LIMIT 10
            """,
            (show_id,)
        ).fetchall()
        
        print("\nCurrent Stranger Things cast:")
        print("-" * 60)
        for member in cast:
            print(f"  {member['name']} as {member['character']} (order: {member['cast_order']})")
            
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("STRANGER THINGS CAST FIX")
    print("=" * 60)
    print(f"\nDatabase: {DB_PATH}\n")
    
    print("Current cast:")
    verify_cast()
    
    print("\n" + "=" * 60)
    print("FIXING CAST DATA")
    print("=" * 60)
    
    success = fix_stranger_things_cast()
    
    if success:
        print("\n" + "=" * 60)
        print("VERIFICATION")
        print("=" * 60)
        verify_cast()
    
    sys.exit(0 if success else 1)

