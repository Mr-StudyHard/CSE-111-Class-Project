#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${ROOT_DIR}/movie_tracker.db"

echo "Resetting database at ${DB_PATH}"
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" ".read ${ROOT_DIR}/db/schema.sql"
sqlite3 "${DB_PATH}" ".read ${ROOT_DIR}/db/seed.sql"

if [[ "${RUN_TMDB_ETL:-0}" == "1" ]]; then
  echo "Running TMDb ETL loader..."
  python "${ROOT_DIR}/scripts/tmdb_etl.py" --movies 10 --shows 5 --episodes-per-season 3
fi

echo "Executing sample queries..."
sqlite3 "${DB_PATH}" ".read ${ROOT_DIR}/queries/use_cases.sql" >/dev/null || true

echo "Database reset complete."

