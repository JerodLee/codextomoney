param(
    [int]$Top = 3
)

$scriptPath = Join-Path $PSScriptRoot "crypto_momentum_scanner.py"

if (-not (Test-Path $scriptPath)) {
    Write-Error "Scanner not found: $scriptPath"
    exit 1
}

Write-Host "Starting 5-minute momentum scan (Ctrl+C to stop)..."
python $scriptPath --watch --interval-sec 300 --top $Top
