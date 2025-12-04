$ErrorActionPreference = 'Stop'

# Ensure we run from project root
Set-Location $PSScriptRoot

# Backend health URL
$backendHealth = 'http://127.0.0.1:5000/api/health'
function Test-Api {
	param([string]$Url)
	try { return (Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 2).Content -match 'healthy' } catch { return $false }
}

function Get-Python {
	# Prefer project venv, then py launcher, then python on PATH
	$venv = Join-Path $PSScriptRoot '.venv/Scripts/python.exe'
	if (Test-Path $venv) { return @{ FilePath=$venv; Args=@() } }

	$py = Get-Command py -ErrorAction SilentlyContinue
	if ($py) { return @{ FilePath=$py.Source; Args=@('-3') } }

	$python = Get-Command python -ErrorAction SilentlyContinue
	if ($python) { return @{ FilePath=$python.Source; Args=@() } }

	throw 'Python not found. Install Python 3.x or create a .venv first.'
}

if (-not (Test-Api -Url $backendHealth)) {
	Write-Host 'Backend not running. Starting backend…' -ForegroundColor Yellow
	$env:APP_HOST = '127.0.0.1'
	$env:APP_PORT = '5000'

	$pyInfo = Get-Python
	$log = Join-Path $PSScriptRoot 'backend.out.log'
	$elog = Join-Path $PSScriptRoot 'backend.err.log'
	if (Test-Path $log) { Remove-Item $log -Force -ErrorAction SilentlyContinue }
	if (Test-Path $elog) { Remove-Item $elog -Force -ErrorAction SilentlyContinue }

	$args = @()
	$args += $pyInfo.Args
	$args += @("$PSScriptRoot\run_server.py")

	try {
	$proc = Start-Process -FilePath $pyInfo.FilePath -ArgumentList $args -PassThru -WindowStyle Hidden -RedirectStandardOutput $log -RedirectStandardError $elog
	} catch {
		Write-Host "Failed to start backend: $($_.Exception.Message)" -ForegroundColor Red
		exit 1
	}

	$started = $false
	for ($i=0; $i -lt 20; $i++) {
		Start-Sleep -Milliseconds 750
		if (Test-Api -Url $backendHealth) { $started = $true; break }
		if ($proc.HasExited) { break }
	}

	if ($started) {
		Write-Host 'Backend is online.' -ForegroundColor Green
	} else {
	Write-Host 'Backend failed to become healthy. Recent backend logs:' -ForegroundColor Red
	if (Test-Path $log) { Write-Host '--- stdout (tail) ---' -ForegroundColor DarkGray; Get-Content $log -Tail 40 | Write-Host }
	if (Test-Path $elog) { Write-Host '--- stderr (tail) ---' -ForegroundColor DarkGray; Get-Content $elog -Tail 40 | Write-Host }
	if ((-not (Test-Path $log)) -and (-not (Test-Path $elog))) { Write-Host '(no log output captured)' }
		Write-Host "Tip: Ensure dependencies are installed (pip install -r requirements.txt) and that Python 3 is available." -ForegroundColor Yellow
		# Do not exit hard; allow Vite to start so UI can still run, but login will show offline.
	}
}
else {
	# Backend is up; ensure it includes latest routes (e.g., /api/signup). If not, restart it.
	try {
		$routes = (Invoke-WebRequest -UseBasicParsing -Uri ($backendHealth -replace '/health','/__routes') -TimeoutSec 3).Content | ConvertFrom-Json
		if ($routes -notcontains '/api/signup') {
			Write-Host 'Backend running without latest routes. Restarting backend to pick up changes…' -ForegroundColor Yellow
			# Try to stop any run_server.py processes
			Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'run_server.py' } | ForEach-Object { try { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue } catch {} }
			Start-Sleep -Milliseconds 500
			# Start again
			$pyInfo = Get-Python
			$args = @(); $args += $pyInfo.Args; $args += @("$PSScriptRoot\run_server.py")
			Start-Process -FilePath $pyInfo.FilePath -ArgumentList $args -WindowStyle Hidden | Out-Null
			# Wait briefly for health
			for ($i=0; $i -lt 12; $i++) { Start-Sleep -Milliseconds 500; if (Test-Api -Url $backendHealth) { break } }
		}

		# If signup route exists, probe it to ensure hashed password logic is active (avoid legacy 500 NOT NULL errors)
		elseif ($routes -contains '/api/signup') {
			$probeEmail = '__probe_signup__@example.com'
			$probeBody = @{ email = $probeEmail; password = 'probe123' } | ConvertTo-Json
			$needRestart = $false
			try {
				$resp = Invoke-WebRequest -UseBasicParsing -Uri ('http://127.0.0.1:5000/api/signup') -Method POST -ContentType 'application/json' -Body $probeBody -TimeoutSec 5
				if ($resp.StatusCode -ge 500) { $needRestart = $true }
			} catch {
				$needRestart = $true
			}
			if ($needRestart) {
				Write-Host 'Signup probe failed (legacy code still running). Restarting backend…' -ForegroundColor Yellow
				Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'run_server.py' } | ForEach-Object { try { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue } catch {} }
				Start-Sleep -Milliseconds 400
				$pyInfo = Get-Python
				$args = @(); $args += $pyInfo.Args; $args += @("$PSScriptRoot\run_server.py")
				Start-Process -FilePath $pyInfo.FilePath -ArgumentList $args -WindowStyle Hidden | Out-Null
				for ($i=0; $i -lt 16; $i++) { Start-Sleep -Milliseconds 500; if (Test-Api -Url $backendHealth) { break } }
			}
		}
	} catch {}
}

# Start Vite dev server
Set-Location $PSScriptRoot\web
npm run dev