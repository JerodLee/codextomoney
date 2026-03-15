if (-not $env:TELEGRAM_BOT_TOKEN) {
    Write-Error "TELEGRAM_BOT_TOKEN is not set."
    exit 1
}

if (-not $env:TELEGRAM_CHAT_ID) {
    Write-Error "TELEGRAM_CHAT_ID is not set."
    exit 1
}

$scriptPath = Join-Path $PSScriptRoot "momentum_telegram_agent.py"
if (-not (Test-Path $scriptPath)) {
    Write-Error "Script not found: $scriptPath"
    exit 1
}

Write-Host "Starting Telegram momentum bot (5m interval, Ctrl+C to stop)..."
python $scriptPath --watch --interval-sec 300 --horizon-min 15 --message-style compact --state-file (Join-Path $PSScriptRoot "state\bot_state.json") --history-file (Join-Path $PSScriptRoot "state\eval_history.jsonl")
