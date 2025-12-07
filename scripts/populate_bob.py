#!/usr/bin/env python3
"""
Populate Bob's profile with favorites (reviews), comments, and watchlist items.
"""
import sqlite3
import random
import os
from datetime import datetime, timedelta
from pathlib import Path

# Get the database path relative to the script location
SCRIPT_DIR = Path(__file__).parent
DB_PATH = str(SCRIPT_DIR.parent / "movie_tracker.db")

def get_bob_user_id(conn):
    """Get or create Bob user."""
    row = conn.execute(
        "SELECT user_id FROM users WHERE lower(email) = 'bob@example.com' LIMIT 1"
    ).fetchone()
    if row:
        return row[0]
    # Create Bob if doesn't exist
    conn.execute(
        "INSERT INTO users (email) VALUES ('bob@example.com')"
    )
    conn.commit()
    return conn.lastrowid

def populate_bob_data():
    """Populate Bob's favorites, comments, and watchlist."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    
    bob_id = get_bob_user_id(conn)
    print(f"Bob's user_id: {bob_id}")
    
    # Get available movies and shows
    movies = conn.execute(
        "SELECT movie_id, title FROM movies WHERE overview IS NOT NULL AND overview != '' ORDER BY popularity DESC LIMIT 50"
    ).fetchall()
    
    shows = conn.execute(
        "SELECT show_id, title FROM shows WHERE overview IS NOT NULL AND overview != '' ORDER BY popularity DESC LIMIT 50"
    ).fetchall()
    
    print(f"Found {len(movies)} movies and {len(shows)} shows")
    
    # Clear Bob's existing data (optional - comment out if you want to keep existing)
    conn.execute("DELETE FROM reviews WHERE user_id = ?", (bob_id,))
    conn.execute("DELETE FROM watchlists WHERE user_id = ?", (bob_id,))
    conn.execute("DELETE FROM comments WHERE user_id = ?", (bob_id,))
    # Delete discussions created by Bob
    discussion_ids = [r[0] for r in conn.execute("SELECT discussion_id FROM discussions WHERE user_id = ?", (bob_id,)).fetchall()]
    if discussion_ids:
        conn.execute("DELETE FROM comments WHERE discussion_id IN ({})".format(','.join('?'*len(discussion_ids))), discussion_ids)
        conn.execute("DELETE FROM discussions WHERE user_id = ?", (bob_id,))
    
    # 1. Add FAVORITES (reviews with ratings) - 20 movies, 15 shows
    print("\n=== Adding Favorites (Reviews) ===")
    favorite_movies = random.sample([m['movie_id'] for m in movies], min(20, len(movies)))
    favorite_shows = random.sample([s['show_id'] for s in shows], min(15, len(shows)))
    
    review_contents = [
        "Absolutely amazing! One of my all-time favorites.",
        "Incredible storytelling and cinematography. Highly recommend!",
        "This blew my mind. The acting was phenomenal.",
        "A masterpiece. Every scene was perfectly crafted.",
        "Loved every minute of it. Can't wait to watch again!",
        "Brilliant direction and writing. Top tier entertainment.",
        "One of the best I've seen this year. Stunning visuals.",
        "Exceptional quality. The plot twists were incredible.",
        "A true gem. The character development was outstanding.",
        "Perfect blend of action and emotion. Absolutely loved it!",
        "This is why I love cinema. Pure excellence.",
        "Outstanding performances all around. A must-watch!",
        "The soundtrack alone is worth watching for.",
        "Incredible attention to detail. A work of art.",
        "This will stay with me for a long time. Beautiful.",
    ]
    
    # Add movie reviews
    for movie_id in favorite_movies:
        rating = round(random.uniform(7.5, 10.0), 1)
        content = random.choice(review_contents)
        # Random date within last 6 months
        days_ago = random.randint(0, 180)
        created_at = (datetime.now() - timedelta(days=days_ago)).isoformat()
        
        conn.execute(
            "INSERT INTO reviews (user_id, movie_id, rating, content, created_at) VALUES (?, ?, ?, ?, ?)",
            (bob_id, movie_id, rating, content, created_at)
        )
        movie_title = next((m['title'] for m in movies if m['movie_id'] == movie_id), 'Unknown')
        print(f"  [+] Movie review: {movie_title[:50]} - Rating: {rating}/10")
    
    # Add show reviews
    for show_id in favorite_shows:
        rating = round(random.uniform(7.5, 10.0), 1)
        content = random.choice(review_contents)
        days_ago = random.randint(0, 180)
        created_at = (datetime.now() - timedelta(days=days_ago)).isoformat()
        
        conn.execute(
            "INSERT INTO reviews (user_id, show_id, rating, content, created_at) VALUES (?, ?, ?, ?, ?)",
            (bob_id, show_id, rating, content, created_at)
        )
        show_title = next((s['title'] for s in shows if s['show_id'] == show_id), 'Unknown')
        print(f"  [+] Show review: {show_title[:50]} - Rating: {rating}/10")
    
    # 2. Add WATCHLIST items - 15 movies, 12 shows
    print("\n=== Adding Watchlist Items ===")
    watchlist_movies = random.sample([m['movie_id'] for m in movies if m['movie_id'] not in favorite_movies], min(15, len(movies) - len(favorite_movies)))
    watchlist_shows = random.sample([s['show_id'] for s in shows if s['show_id'] not in favorite_shows], min(12, len(shows) - len(favorite_shows)))
    
    # Add movie watchlist items
    for movie_id in watchlist_movies:
        days_ago = random.randint(0, 90)
        added_at = (datetime.now() - timedelta(days=days_ago)).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO watchlists (user_id, movie_id, show_id, added_at) VALUES (?, ?, NULL, ?)",
            (bob_id, movie_id, added_at)
        )
        movie_title = next((m['title'] for m in movies if m['movie_id'] == movie_id), 'Unknown')
        print(f"  [+] Watchlist movie: {movie_title[:50]}")
    
    # Add show watchlist items
    for show_id in watchlist_shows:
        days_ago = random.randint(0, 90)
        added_at = (datetime.now() - timedelta(days=days_ago)).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO watchlists (user_id, movie_id, show_id, added_at) VALUES (?, NULL, ?, ?)",
            (bob_id, show_id, added_at)
        )
        show_title = next((s['title'] for s in shows if s['show_id'] == show_id), 'Unknown')
        print(f"  [+] Watchlist show: {show_title[:50]}")
    
    # 3. Add DISCUSSIONS and COMMENTS
    print("\n=== Adding Discussions and Comments ===")
    
    # Create discussions for some of Bob's favorite movies/shows
    discussion_titles = [
        "What did you think of the ending?",
        "Best scene in the entire series/movie?",
        "Who was your favorite character?",
        "The cinematography was incredible!",
        "Anyone else catch that easter egg?",
        "The soundtrack is amazing!",
        "What's your theory about...?",
        "This deserves more recognition!",
    ]
    
    discussion_contents = [
        "I've been thinking about this for days. What are your thoughts?",
        "This really stood out to me. Anyone else feel the same?",
        "I noticed something interesting on my rewatch. Did you catch it?",
        "The attention to detail here is incredible!",
        "This is one of those moments that makes the whole thing worth it.",
        "I can't stop thinking about this scene. So powerful!",
        "What did everyone think about this part?",
        "This is why I love this show/movie so much!",
    ]
    
    # Create discussions for 8 movies
    discussion_movies = random.sample(favorite_movies, min(8, len(favorite_movies)))
    for movie_id in discussion_movies:
        title = random.choice(discussion_titles)
        days_ago = random.randint(0, 60)
        created_at = (datetime.now() - timedelta(days=days_ago)).isoformat()
        
        cursor = conn.execute(
            "INSERT INTO discussions (user_id, movie_id, show_id, title, created_at) VALUES (?, ?, NULL, ?, ?)",
            (bob_id, movie_id, title, created_at)
        )
        discussion_id = cursor.lastrowid
        movie_title = next((m['title'] for m in movies if m['movie_id'] == movie_id), 'Unknown')
        print(f"  [+] Discussion: {title} (Movie: {movie_title[:40]})")
        
        # Add 2-4 comments to each discussion
        num_comments = random.randint(2, 4)
        for i in range(num_comments):
            comment_days_ago = random.randint(0, days_ago)
            comment_created_at = (datetime.now() - timedelta(days=comment_days_ago)).isoformat()
            content = random.choice(discussion_contents)
            conn.execute(
                "INSERT INTO comments (discussion_id, user_id, content, created_at) VALUES (?, ?, ?, ?)",
                (discussion_id, bob_id, content, comment_created_at)
            )
        print(f"    -> Added {num_comments} comments")
    
    # Create discussions for 6 shows
    discussion_shows = random.sample(favorite_shows, min(6, len(favorite_shows)))
    for show_id in discussion_shows:
        title = random.choice(discussion_titles)
        days_ago = random.randint(0, 60)
        created_at = (datetime.now() - timedelta(days=days_ago)).isoformat()
        
        cursor = conn.execute(
            "INSERT INTO discussions (user_id, movie_id, show_id, title, created_at) VALUES (?, NULL, ?, ?, ?)",
            (bob_id, show_id, title, created_at)
        )
        discussion_id = cursor.lastrowid
        show_title = next((s['title'] for s in shows if s['show_id'] == show_id), 'Unknown')
        print(f"  [+] Discussion: {title} (Show: {show_title[:40]})")
        
        # Add 2-4 comments to each discussion
        num_comments = random.randint(2, 4)
        for i in range(num_comments):
            comment_days_ago = random.randint(0, days_ago)
            comment_created_at = (datetime.now() - timedelta(days=comment_days_ago)).isoformat()
            content = random.choice(discussion_contents)
            conn.execute(
                "INSERT INTO comments (discussion_id, user_id, content, created_at) VALUES (?, ?, ?, ?)",
                (discussion_id, bob_id, content, comment_created_at)
            )
        print(f"    -> Added {num_comments} comments")
    
    conn.commit()
    conn.close()
    
    print("\n=== Summary ===")
    print(f"[+] Added {len(favorite_movies)} movie favorites (reviews)")
    print(f"[+] Added {len(favorite_shows)} show favorites (reviews)")
    print(f"[+] Added {len(watchlist_movies)} movies to watchlist")
    print(f"[+] Added {len(watchlist_shows)} shows to watchlist")
    print(f"[+] Created {len(discussion_movies) + len(discussion_shows)} discussions with comments")
    print("\nBob's profile has been populated!")

if __name__ == "__main__":
    populate_bob_data()

