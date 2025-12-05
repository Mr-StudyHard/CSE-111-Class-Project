# PowerShell script to start the ETL Scheduler on Windows
# Usage: .\scripts\start_etl_scheduler.ps1

$ErrorActionPreference = "Stop"

# Get script directory and project root
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectDir = Split-Path -Parent $ScriptDir

# Change to project directory
Set-Location $ProjectDir

Write-Host "Starting TMDb ETL Scheduler..." -ForegroundColor Green
Write-Host "Project directory: $ProjectDir"

# Check if .env file exists
if (-not (Test-Path ".env")) {
    Write-Host "Warning: .env file not found" -ForegroundColor Yellow
    Write-Host "Please create a .env file with TMDB_API_KEY"
}

# Check if config file exists
if (-not (Test-Path "etl_config.yaml")) {
    Write-Host "Error: etl_config.yaml not found" -ForegroundColor Red
    exit 1
}

# Check if Python is available
try {
    $pythonVersion = python --version 2>&1
    Write-Host "Python version: $pythonVersion"
} catch {
    Write-Host "Error: Python not found" -ForegroundColor Red
    exit 1
}

# Check if virtual environment exists
if (Test-Path "venv\Scripts\Activate.ps1") {
    Write-Host "Activating virtual environment..."
    & "venv\Scripts\Activate.ps1"
}

# Install/check dependencies
Write-Host "Checking dependencies..."
python -m pip install -q -r requirements.txt

# Run the scheduler
Write-Host "Starting scheduler..." -ForegroundColor Green
python run_etl_scheduler.py $args

