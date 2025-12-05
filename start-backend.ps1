$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

function Get-Python {
	$venv = Join-Path $PSScriptRoot '.venv/Scripts/python.exe'
	if (Test-Path $venv) { return @{ FilePath=$venv; Args=@() } }
	$py = Get-Command py -ErrorAction SilentlyContinue
	if ($py) { return @{ FilePath=$py.Source; Args=@('-3') } }
	$python = Get-Command python -ErrorAction SilentlyContinue
	if ($python) { return @{ FilePath=$python.Source; Args=@() } }
	throw 'Python not found. Install Python 3.x or create a .venv first.'
}

# Start Flask using the runner script
$env:APP_HOST = '127.0.0.1'
$env:APP_PORT = '5000'
$py = Get-Python
& $py.FilePath @($py.Args + @('.\run_server.py'))