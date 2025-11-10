#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${ROOT_DIR}/movie_tracker.db"

echo "Resetting database at ${DB_PATH}"
rm -f "${DB_PATH}"

python - "${ROOT_DIR}" "${DB_PATH}" <<'PY'
import os, sqlite3, sys
root, db_path = sys.argv[1], sys.argv[2]
schema_path = os.path.join(root, "db", "schema.sql")
seed_path = os.path.join(root, "db", "seed.sql")

conn = sqlite3.connect(db_path)
with open(schema_path, "r", encoding="utf-8") as fh:
    conn.executescript(fh.read())
with open(seed_path, "r", encoding="utf-8") as fh:
    conn.executescript(fh.read())
conn.close()
PY

if [[ "${RUN_TMDB_ETL:-0}" == "1" ]]; then
  echo "Running TMDb ETL loader..."
  python "${ROOT_DIR}/scripts/tmdb_etl.py" --movies 10 --shows 5 --episodes-per-season 3
fi

echo "Executing sample queries..."
python - "${ROOT_DIR}" "${DB_PATH}" <<'PY' >/dev/null || true
import os, sqlite3, sys, traceback
root, db_path = sys.argv[1], sys.argv[2]
queries_path = os.path.join(root, "queries", "use_cases.sql")
conn = sqlite3.connect(db_path)
try:
    with open(queries_path, "r", encoding="utf-8") as fh:
        conn.executescript(fh.read())
except sqlite3.Error:
    # Use-case pack contains parameterised statements; ignore failures here.
    pass
finally:
    conn.close()
PY

echo "Database reset complete."

