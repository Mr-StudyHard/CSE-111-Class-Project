#!/bin/bash
# Start Flask backend server

set -e

# Change to script directory
cd "$(dirname "$0")"

# Activate virtual environment
if [ -d ".venv" ]; then
    source .venv/Scripts/activate
else
    echo "Error: Virtual environment not found. Please create one first."
    exit 1
fi

# Set environment variables
export APP_HOST=${APP_HOST:-127.0.0.1}
export APP_PORT=${APP_PORT:-5000}

# Start Flask server
echo "Starting Flask server on ${APP_HOST}:${APP_PORT}..."
python run_server.py

