$ErrorActionPreference = 'SilentlyContinue'

# Evita múltiples instancias
$existing = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'uvicorn app:app --host 127.0.0.1 --port 8080' }
if ($existing) {
  Write-Output "Dashboard already running"
  exit 0
}

Set-Location "C:\Users\Fernando\.openclaw\workspace\agent-ops-dashboard"

# Arranca en background
Start-Process -FilePath "py" -ArgumentList "-3 -m uvicorn app:app --host 127.0.0.1 --port 8080" -WindowStyle Hidden
Write-Output "Dashboard started"
