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

Set repository secrets:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## Validation and calibration

Validation:

1. Save entry prices at recommendation time
2. After horizon, load exit prices
3. Compute returns for Bithumb, Bitget, and blended average
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

## State files

- `state/bot_state.json`: dynamic config + pending + results
- `state/eval_history.jsonl`: evaluation history log

## Main options

```bash
python momentum_telegram_agent.py \
  --top 3 \
  --horizon-min 15 \
  --message-style compact \
  --metric-window 120 \
  --watch \
  --interval-sec 300
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
