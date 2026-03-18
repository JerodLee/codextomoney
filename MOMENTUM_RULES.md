# Momentum System Rules (Bithumb Spot + Bitget USDT-M)

This document summarizes the current production logic used by:

- `crypto_momentum_scanner.py`
- `momentum_telegram_agent.py`
- `.github/workflows/momentum-telegram-bot.yml`

## 1) Recommendation Conditions

### 1.1 Universe

1. Load all KRW spot tickers from Bithumb.
2. Load all USDT futures tickers from Bitget (`USDT-FUTURES`).
3. Keep only symbol intersection (`BTC`, `ETH`, `TAO`, etc. where both sides exist).

### 1.2 Base Filters (current defaults)

- `min_bithumb_rate >= 1.0%`
- `min_bitget_rate >= 1.0%`
- `min_bithumb_value >= 2,000,000,000 KRW`
- `min_bitget_volume >= 10,000,000 USDT`

### 1.3 Additional Safety Filters

1. Overheat filter:
   - Remove if either side 24h change is `>= 40.0%`.
2. Conservative filter:
   - Remove if either side 24h change is `> 20.0%`.
   - Remove if `abs(funding_rate) > 0.0015`.
3. Orderability filter (Bithumb):
   - Keep only symbols with active orderbook (`bids` and `asks` both non-empty).

### 1.4 Ranking Score

Candidate score is computed as:

`score = 0.42*b_rate_score + 0.33*g_rate_score + 0.15*b_liq_score + 0.10*g_liq_score - funding_penalty`

Where:

- `b_rate_score`: Bithumb 24h rate normalized to 0~1 with cap at 50%.
- `g_rate_score`: Bitget 24h rate normalized to 0~1 with cap at 50%.
- `b_liq_score`, `g_liq_score`: logarithmic liquidity score relative to filter floors.
- `funding_penalty`: applied when `funding_rate > 0.001`.

Final output:

- Top N picks (default `top=3`)
- Telegram message style default: `compact`

### 1.5 Candidate Model Set (current)

- `momentum_long_v1`: base long score
- `momentum_short_v1`: base short score
- `momentum_long_v2`: long v1 + market context adjustment
- `momentum_short_v2`: short v1 + market context adjustment

v2 adjustment uses:

- market/BTC/ETH multi-timeframe trend (`24h, 12h, 6h, 1h, 15m, 5m, 1m`)
- funding-rate crowding penalty/bonus by side
- open-interest participation signal
- concentration regime (`btc`, `eth`, `single-alt`, `alt-broad`, `balanced`)
- Bithumb orderbook orderblock signal (bid/ask wall imbalance + near-wall distance)

Only active models in `state.model_registry` are used for recommendation scoring.

## 2) Performance Validation Logic

### 2.1 Entry Tracking

At recommendation time, store per pick:

- symbol
- timestamp
- entry Bithumb price
- entry Bitget price
- score, rates, liquidity, funding
- evaluation horizons minutes (default `5,15,30,60`)

### 2.2 Evaluation Timing

- A pick is evaluated on each due horizon (`5m`, `15m`, `30m`, `60m` by default).
- Each due horizon produces one evaluation row (`pick_id@{horizon}m`).
- If both market legs are temporarily unavailable, it waits up to `max_horizon * 2`.

### 2.3 Return Calculation

- `return_bithumb = (exit_bithumb - entry_bithumb) / entry_bithumb`
- `return_bitget = (exit_bitget - entry_bitget) / entry_bitget`
- `return_blended = average(available legs)`
- `win = (return_blended > 0)`

### 2.4 Rolling Metrics

On recent window (default `120` evaluations):

- win rate
- average blended return
- median blended return

### 2.5 Model-Level Metrics

On recent model window (default `max(120, metric_window*2)`), each model tracks:

- count
- win rate
- average blended return
- median blended return

### 2.6 Missed-Move Audit (false-negative scan)

To analyze missed pumps/dumps, the runner now tracks non-picked symbols too:

- scope:
  - liquid symbols only (`b_value >= min_bithumb_value` and `g_volume >= min_bitget_volume`)
  - both sides (`LONG`, `SHORT`) except already-picked rows
- reject-reason tags:
  - `long_b_rate`, `long_g_rate`, `short_g_rate`, `short_b_rate`, `short_funding`
  - `loss_cooldown`, `overheat`, `conservative_rate`, `conservative_funding`
  - `orderable_or_check_cap`, `rank_cut`, `unknown`
- evaluation:
  - same multi-horizon cadence (`5,15,30,60`)
  - missed threshold by horizon:
    - `5m: 1.5%`, `15m: 2.5%`, `30m: 3.5%`, `60m: 5.0%`
  - if side-adjusted blended return `>= threshold`, it is counted as a missed move
- outputs:
  - per-run summary (`missed_audit`) is saved in `run_history`
  - compact Telegram line includes `MissedAudit: eval/missed/top reasons`

## 3) Auto-Calibration Rules

### 3.1 Trigger Conditions

Calibration runs when either:

1. `no_candidate_streak >= 3` (fast recovery path), or
2. performance path is eligible:
   - newly evaluated picks `>= 3`
   - metric sample count `>= 20`
   - and at least 6 hours since last calibration

### 3.2 Recovery Calibration (low candidates)

If `no_candidate_streak >= 3`:

- `min_bithumb_value *= 0.90` (floor: `1,000,000,000`)
- `min_bitget_volume *= 0.90` (floor: `5,000,000`)
- `min_bithumb_rate -= 0.2` (floor: `0.5`)
- `min_bitget_rate -= 0.2` (floor: `0.5`)

### 3.3 Underperformance Calibration

If metrics count `>= 20` and `win_rate < 0.45`:

- `max_overheat_rate -= 2.0` (floor: `20.0`)
- `conservative_max_rate -= 1.0` (floor: `10.0`)
- `min_bithumb_value *= 1.10` (cap: `20,000,000,000`)
- `min_bitget_volume *= 1.10` (cap: `60,000,000`)
- `conservative_max_abs_funding *= 0.90` (floor: `0.0005`)

### 3.4 Strong Performance Calibration

If metrics count `>= 20`, `win_rate > 0.60`, and `avg_return > 0.002`:

- `max_overheat_rate += 1.0` (cap: `50.0`)
- `conservative_max_rate += 1.0` (cap: `25.0`)
- `min_bithumb_value *= 0.95` (floor: `1,000,000,000`)
- `min_bitget_volume *= 0.95` (floor: `5,000,000`)
- `conservative_max_abs_funding *= 1.05` (cap: `0.0025`)

### 3.5 Normalization

After calibration:

- rate-like thresholds are rounded to 0.01 step
- liquidity thresholds are kept as integer-like float values

### 3.6 Model Governance (auto model expansion)

Before each cycle, model governance checks model-level metrics.

- expansion trigger:
  - current model evaluated count `>= 24`
  - and model win rate `< 45%`
  - and next model in chain is disabled
- chain:
  - LONG: `momentum_long_v1 -> momentum_long_v2`
  - SHORT: `momentum_short_v1 -> momentum_short_v2`
- cooldown:
  - after expansion, governance waits 6 hours before next expansion check action

Each expansion is recorded in:

- `state.model_governance_events`
- latest `run_history[*].model_governance_notes`

### 3.7 ModelLab (underperformance diagnostics)

When a model underperforms with enough sample size:

- gate:
  - model count `>= 24`
  - and (`win_rate < 48%` or `avg_return < -0.10%`)
- analyze weak buckets on:
  - market alignment
  - funding crowding/contrarian zone
  - momentum bucket
  - open-interest bucket
  - market regime bucket
- output:
  - weakness summary (`dimension`, `bucket`, `n`, `win_rate`, `avg_return`)
  - concrete rule suggestions
  - next model id proposal (`vN+1`, e.g. `momentum_long_v2 -> momentum_long_v3`)

Important:

- ModelLab does **not** auto-create v3/v4 models.
- It generates design guidance to build/enable the next model deliberately.

## 4) State and Persistence

- `state/bot_state.json`
  - dynamic config
  - pending picks
  - evaluated results
  - metadata (`no_candidate_streak`, calibration/run timestamps)
- `state/eval_history.jsonl`
  - append-only evaluation log records

## 5) Runtime Defaults (Current)

- Recommendation scan: every 5 minutes (GitHub Actions cron)
- Loss watchdog: every 15 seconds for ~3.5 minutes after each scan (`--alerts-only --watch --interval-sec 15 --cycles 14`)
- Top picks per scan: 3
- Validation horizons: `5,15,30,60` minutes
- Message style: compact
- Social buzz (optional): `X` + `Threads` mention aggregation per scan

## 6) X/Threads Social Buzz Aggregation

### 6.1 Data Sources

- X recent search API (`X_BEARER_TOKEN`)
- Threads keyword search API (`THREADS_ACCESS_TOKEN`, optional URL template/base override)

### 6.2 Symbol Universe

- Merge symbols from:
  - current market rows
  - current candidate/pick rows
  - default major symbol set (`BTC`, `ETH`, `SOL`, `XRP`, ...)
- Cap by runtime option (`--social-max-symbols`, default 16)

### 6.3 Mention Matching and Scoring

- Count symbol mentions by:
  - cashtag (`$BTC`)
  - hashtag (`#BTC`)
  - bare symbol text (major symbols only, word-boundary safe)
- Apply engagement weight per post to build integrated score:
  - base mention count + weighted social reactions (likes/reposts/replies/quotes)
- Merge X + Threads into per-symbol totals:
  - `mentions_total`, `x_mentions`, `threads_mentions`
  - `score`

### 6.4 Persistence and Output

- Save snapshots in:
  - `state.social_buzz_history`
  - `state.meta.last_social_buzz`
  - `state.run_history[*].social_buzz`
- Telegram compact output:
  - append `SocialBuzz: SYMBOL(total, Xn/Tm) ...` when at least one source is enabled
- Dashboard output:
  - market tab card shows rank, counts, and rank delta (NEW/UP/DOWN/SAME)

## 7) Notes

- This system is a momentum scanner and automation pipeline, not investment advice.
- Sudden volatility, API gaps, or market microstructure can degrade short-term outcomes.
