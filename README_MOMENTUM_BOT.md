# Momentum Telegram Bot (Bithumb Spot + Bitget USDT-M)

This bot automates:

1. Momentum picks (Top 3)
2. Post-pick validation after N minutes (default: 15)
3. Auto-calibration from rolling performance
4. Telegram delivery

## Local run

Dry run:

```powershell
python .\momentum_telegram_agent.py --dry-run
```

Real send:

```powershell
$env:TELEGRAM_BOT_TOKEN="YOUR_BOT_TOKEN"
$env:TELEGRAM_CHAT_ID="YOUR_CHAT_ID"
python .\momentum_telegram_agent.py
```

Run every 5 minutes:

```powershell
python .\momentum_telegram_agent.py --watch --interval-sec 300
```

PowerShell launcher:

```powershell
.\start_telegram_bot_5m.ps1
```

## Telegram setup

1. Create a bot with BotFather
2. Use the token as `TELEGRAM_BOT_TOKEN`
3. Open a chat with the bot (or add it to a group)
4. Set `TELEGRAM_CHAT_ID`

To find chat id:

1. Send one message to the bot
2. Call `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`
3. Read `message.chat.id`

## Free server deployment (GitHub Actions)

Included workflow:

- `.github/workflows/momentum-telegram-bot.yml`
- Dashboard UI: `dashboard/index.html`

What it does:

1. Runs every 5 minutes
2. Builds picks, validates old picks, applies auto-calibration
3. Saves state files and commits them back

Reliability safeguards (applied):

1. Recommendation/watchdog steps retry up to 3 times on transient failures
2. State push step retries (`pull --rebase` + `push`) up to 3 times
3. State integrity check blocks conflicted/invalid JSON commits
4. Bot state auto-recovery:
   - if `state/bot_state.json` is broken, restore from `state/bot_state.backup.json`
   - if backup is also broken, rebuild from safe defaults
   - save corrupted snapshot as `state/bot_state.corrupt-<timestamp>.json`

Set repository secrets:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## Validation and calibration

Validation:

1. Save entry prices at recommendation time
2. After horizon, load exit prices
3. Compute net returns for Bithumb/Bitget/blended with round-trip fee + slippage assumptions
4. Mark as win when `return_blended > 0`

Calibration:

1. Track rolling metrics (default window: 120)
2. If win rate degrades:
   - tighten overheat/funding filters
   - raise liquidity floors
3. If win rate is strong:
   - loosen filters slightly
4. If candidate count is repeatedly zero:
   - loosen floors to recover coverage

Profitability controls:

1. Build `setup_quality` (0~1) from multi-timeframe market alignment, orderblock room/cushion, funding crowding, OI chase risk, and liquidity.
2. Apply setup-aware entry/target/stop adjustments (`trend` / `balanced` / `contrarian` modes).
3. Compute `expected_edge_pct` from model historical win rate (reliability-weighted) and current target/stop plan.
4. Execution profiles now gate by target%, RR, setup quality, and expected edge.
5. Position sizing fields are generated per pick: `position_size_pct` and `risk_per_trade_pct`.

Risk management + A/B:

1. RiskGuard pauses new recommendations when 24h cumulative net return breaches a loss limit or consecutive losses exceed a threshold.
2. During pause, alert/watchdog and validation continue, but new picks are temporarily blocked.
3. Weekly-window A/B review (7-day lookback) compares execution profiles and model performance.
4. Clear winners can be auto-promoted (profile switch + model enable/disable guardrails).

## State files

- `state/bot_state.json`: dynamic config + pending + results
- `state/bot_state.backup.json`: latest valid backup for auto-recovery
- `state/eval_history.jsonl`: evaluation history log

## Main options

```bash
python momentum_telegram_agent.py \
  --top 3 \
  --horizon-min 15 \
  --message-style compact \
  --metric-window 120 \
  --fee-bps-bithumb 4 \
  --fee-bps-bitget 6 \
  --slippage-bps-bithumb 4 \
  --slippage-bps-bitget 5 \
  --risk-max-daily-loss-pct 3.0 \
  --risk-max-consecutive-losses 5 \
  --risk-cooldown-min 120 \
  --watch \
  --interval-sec 300
```

Loss-only watchdog mode (near real-time alert check, no new picks):

```bash
python momentum_telegram_agent.py \
  --alerts-only \
  --watch \
  --interval-sec 15 \
  --cycles 14
```

## Notes

- This is a data-driven scanner, not investment advice.
- API delays or missing fields can delay recommendations/evaluations.

## Dashboard

Open:

- `dashboard/index.html`

Recommended local preview:

```powershell
cd .
python -m http.server 8080
```

Then visit:

- `http://localhost:8080/dashboard/index.html`

The page shows:

1. latest recommendation symbols
2. rolling win rate and return trend
3. calibration events and pre/post win-rate uplift
4. model-based performance (`momentum_long_v1`, `momentum_short_v1`)

Operations page:

- `dashboard/ops.html`
