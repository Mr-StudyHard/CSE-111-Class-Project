#!/bin/bash
# Start the ETL Scheduler as a background service

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR" || exit 1

echo "Starting TMDb ETL Scheduler..."
echo "Project directory: $PROJECT_DIR"

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Warning: .env file not found"
    echo "Please create a .env file with TMDB_API_KEY"
fi

# Check if config file exists
if [ ! -f "etl_config.yaml" ]; then
    echo "Error: etl_config.yaml not found"
    exit 1
fi

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found"
    exit 1
fi

# Check if virtual environment exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Install/check dependencies
echo "Checking dependencies..."
python3 -m pip install -q -r requirements.txt

# Run the scheduler
echo "Starting scheduler..."
python3 run_etl_scheduler.py "$@"

