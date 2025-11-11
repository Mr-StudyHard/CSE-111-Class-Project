#!/usr/bin/env bash
set -e

# reset_and_populate.sh
# Resets the database and populates it with movies and TV shows from TMDb.
# Usage: ./scripts/reset_and_populate.sh [--movies N] [--shows M]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DB_PATH="$PROJECT_ROOT/app.db"

# Default values
MOVIES=3000
SHOWS=3000
EPISODES_PER_SEASON=100

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --movies)
      MOVIES="$2"
      shift 2
      ;;
    --shows)
      SHOWS="$2"
      shift 2
      ;;
    --episodes-per-season)
      EPISODES_PER_SEASON="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--movies N] [--shows M] [--episodes-per-season E]"
      exit 1
      ;;
  esac
done

echo "========================================"
echo "  Movie & TV Analytics - DB Reset"
echo "========================================"
echo "Database: $DB_PATH"
echo "Movies:   $MOVIES"
echo "Shows:    $SHOWS"
echo "Episodes: $EPISODES_PER_SEASON per season"
echo "========================================"
echo ""

# 1. Remove existing database
if [ -f "$DB_PATH" ]; then
  echo "[1/3] Removing existing database..."
  rm "$DB_PATH"
  echo "      ✓ Database removed"
else
  echo "[1/3] No existing database found, creating fresh..."
fi

# 2. Initialize the schema
echo "[2/3] Initializing database schema..."
cd "$PROJECT_ROOT"
python -c "
import sys
import sqlite3
import os
sys.path.insert(0, '.')
from app.models import init_db

db_path = os.path.abspath('app.db')
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
init_db(conn)
conn.close()
print('      ✓ Schema initialized')
"

# 3. Run TMDb ETL with modular arguments
echo "[3/3] Fetching and populating data from TMDb..."
echo "      This may take several minutes depending on the counts..."
python "$SCRIPT_DIR/tmdb_etl.py" \
  --movies "$MOVIES" \
  --shows "$SHOWS" \
  --episodes-per-season "$EPISODES_PER_SEASON"

echo ""
echo "========================================"
echo "  ✓ Database reset and populated!"
echo "========================================"
echo "Total ingested: ~$MOVIES movies + ~$SHOWS shows"
echo "Ready to start the backend and frontend."
echo ""

