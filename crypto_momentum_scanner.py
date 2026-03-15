#!/usr/bin/env python3
"""
Bithumb(spot KRW) + Bitget(USDT-M futures) momentum scanner.

Default behavior is tuned for conservative short-term picks:
- excludes overheated moves (>= 40%)
- keeps only symbols with active Bithumb KRW orderbook
- prints top 3 candidates

Examples
--------
python crypto_momentum_scanner.py
python crypto_momentum_scanner.py --watch --interval-sec 300
python crypto_momentum_scanner.py --normal --top 10
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Tuple


BITHUMB_TICKER_URL = "https://api.bithumb.com/public/ticker/ALL_KRW"
BITHUMB_ORDERBOOK_URL_TMPL = "https://api.bithumb.com/public/orderbook/{symbol}_KRW"
BITGET_TICKERS_URL = (
    "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
)


@dataclass
class BithumbTicker:
    symbol: str
    rate24h: float
    krw_value24h: float
    close_krw: float


@dataclass
class BitgetTicker:
    base_symbol: str
    symbol: str
    change24h_pct: float
    usdt_volume: float
    funding_rate: float
    last_price: float


@dataclass
class Candidate:
    symbol: str
    score: float
    b_rate24h: float
    b_krw_value24h: float
    b_close_krw: float
    g_change24h_pct: float
    g_usdt_volume: float
    g_funding_rate: float
    g_symbol: str
    g_last_price: float


def fetch_json(url: str, timeout_sec: int = 15) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (MomentumScanner)",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))


def to_float(raw: object, default: float = 0.0) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def parse_bithumb(payload: dict) -> Dict[str, BithumbTicker]:
    if payload.get("status") != "0000":
        raise ValueError(f"Bithumb API error: {payload.get('status')}")
    data = payload.get("data", {})
    out: Dict[str, BithumbTicker] = {}
    for symbol, row in data.items():
        if symbol == "date":
            continue
        if not isinstance(row, dict):
            continue
        out[symbol] = BithumbTicker(
            symbol=symbol,
            rate24h=to_float(row.get("fluctate_rate_24H")),
            krw_value24h=to_float(row.get("acc_trade_value_24H")),
            close_krw=to_float(row.get("closing_price")),
        )
    return out


def parse_bitget(payload: dict) -> Dict[str, BitgetTicker]:
    if payload.get("code") != "00000":
        raise ValueError(f"Bitget API error: {payload.get('code')}")

    out: Dict[str, BitgetTicker] = {}
    for row in payload.get("data", []):
        symbol = str(row.get("symbol", ""))
        if not symbol.endswith("USDT"):
            continue
        base = symbol[:-4]
        out[base] = BitgetTicker(
            base_symbol=base,
            symbol=symbol,
            change24h_pct=to_float(row.get("change24h")) * 100.0,
            usdt_volume=to_float(row.get("usdtVolume")),
            funding_rate=to_float(row.get("fundingRate")),
            last_price=to_float(row.get("lastPr")),
        )
    return out


def score_candidate(
    b: BithumbTicker,
    g: BitgetTicker,
    min_b_krw: float,
    min_g_usdt: float,
) -> float:
    # Rate component saturates to prevent one extreme pump from dominating score.
    b_rate = max(0.0, min(b.rate24h, 50.0))
    g_rate = max(0.0, min(g.change24h_pct, 50.0))
    b_rate_score = b_rate / 50.0
    g_rate_score = g_rate / 50.0

    # Log-liquidity keeps large caps from crushing small but tradable setups.
    b_liq_score = math.log10(1.0 + (b.krw_value24h / max(min_b_krw, 1.0))) / math.log10(
        11.0
    )
    g_liq_score = math.log10(1.0 + (g.usdt_volume / max(min_g_usdt, 1.0))) / math.log10(
        11.0
    )

    # Too-positive funding can indicate crowded longs.
    funding_penalty = 0.0
    if g.funding_rate > 0.001:
        funding_penalty = min(0.2, (g.funding_rate - 0.001) * 40.0)

    score = (
        0.42 * b_rate_score
        + 0.33 * g_rate_score
        + 0.15 * max(0.0, min(1.0, b_liq_score))
        + 0.10 * max(0.0, min(1.0, g_liq_score))
        - funding_penalty
    )
    return round(score, 4)


def build_candidates(
    bithumb: Dict[str, BithumbTicker],
    bitget: Dict[str, BitgetTicker],
    min_bithumb_rate: float,
    min_bitget_rate: float,
    min_bithumb_krw: float,
    min_bitget_usdt: float,
) -> List[Candidate]:
    candidates: List[Candidate] = []
    for symbol, b in bithumb.items():
        g = bitget.get(symbol)
        if not g:
            continue
        if b.rate24h < min_bithumb_rate:
            continue
        if g.change24h_pct < min_bitget_rate:
            continue
        if b.krw_value24h < min_bithumb_krw:
            continue
        if g.usdt_volume < min_bitget_usdt:
            continue

        candidates.append(
            Candidate(
                symbol=symbol,
                score=score_candidate(b, g, min_bithumb_krw, min_bitget_usdt),
                b_rate24h=b.rate24h,
                b_krw_value24h=b.krw_value24h,
                b_close_krw=b.close_krw,
                g_change24h_pct=g.change24h_pct,
                g_usdt_volume=g.usdt_volume,
                g_funding_rate=g.funding_rate,
                g_symbol=g.symbol,
                g_last_price=g.last_price,
            )
        )

    return sorted(
        candidates,
        key=lambda x: (x.score, x.b_rate24h, x.g_change24h_pct),
        reverse=True,
    )


def apply_overheat_filter(
    candidates: List[Candidate], max_overheat_rate: float
) -> Tuple[List[Candidate], int]:
    if max_overheat_rate <= 0:
        return candidates, 0
    filtered: List[Candidate] = []
    removed = 0
    for c in candidates:
        if c.b_rate24h >= max_overheat_rate or c.g_change24h_pct >= max_overheat_rate:
            removed += 1
            continue
        filtered.append(c)
    return filtered, removed


def apply_conservative_filter(
    candidates: List[Candidate],
    max_rate: float,
    max_abs_funding: float,
) -> Tuple[List[Candidate], int]:
    filtered: List[Candidate] = []
    removed = 0
    for c in candidates:
        if c.b_rate24h > max_rate or c.g_change24h_pct > max_rate:
            removed += 1
            continue
        if abs(c.g_funding_rate) > max_abs_funding:
            removed += 1
            continue
        filtered.append(c)
    return filtered, removed


def is_bithumb_orderable(symbol: str, timeout_sec: int = 8) -> bool:
    url = BITHUMB_ORDERBOOK_URL_TMPL.format(symbol=symbol)
    try:
        payload = fetch_json(url, timeout_sec=timeout_sec)
    except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
        return False
    if payload.get("status") != "0000":
        return False
    data = payload.get("data", {})
    bids = data.get("bids") or []
    asks = data.get("asks") or []
    return len(bids) > 0 and len(asks) > 0


def apply_bithumb_orderable_filter(
    candidates: List[Candidate],
    timeout_sec: int,
    max_checks: int,
) -> Tuple[List[Candidate], int, int]:
    filtered: List[Candidate] = []
    checked = 0
    removed = 0

    for c in candidates:
        if max_checks > 0 and checked >= max_checks:
            removed += 1
            continue
        checked += 1
        if is_bithumb_orderable(c.symbol, timeout_sec=timeout_sec):
            filtered.append(c)
        else:
            removed += 1

    return filtered, checked, removed


def human_k_rw(value: float) -> str:
    billion = 1_000_000_000
    if value >= billion:
        return f"{value / billion:.2f}B KRW"
    return f"{value:,.0f} KRW"


def human_usdt(value: float) -> str:
    million = 1_000_000
    if value >= million:
        return f"{value / million:.2f}M USDT"
    return f"{value:,.0f} USDT"


def print_table(rows: Iterable[Candidate], top: int) -> None:
    selected = list(rows)[:top]
    if not selected:
        print("No candidates matched current filters.")
        return

    print(
        "rank symbol score  b_rate24h  b_24h_value     g_rate24h  g_24h_vol      funding"
    )
    for i, c in enumerate(selected, start=1):
        print(
            f"{i:>4} {c.symbol:<6} {c.score:>5.3f} "
            f"{c.b_rate24h:>8.2f}%  {human_k_rw(c.b_krw_value24h):>12}  "
            f"{c.g_change24h_pct:>8.2f}%  {human_usdt(c.g_usdt_volume):>11}  "
            f"{c.g_funding_rate:>8.5f}"
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bithumb spot + Bitget USDT-M momentum scanner"
    )
    p.add_argument("--top", type=int, default=3, help="Top N symbols to print")
    p.add_argument(
        "--min-bithumb-rate",
        type=float,
        default=1.0,
        help="Min Bithumb 24h change percent",
    )
    p.add_argument(
        "--min-bitget-rate",
        type=float,
        default=1.0,
        help="Min Bitget 24h change percent",
    )
    p.add_argument(
        "--min-bithumb-value",
        type=float,
        default=1_000_000_000,
        help="Min Bithumb 24h KRW traded value",
    )
    p.add_argument(
        "--min-bitget-volume",
        type=float,
        default=5_000_000,
        help="Min Bitget 24h USDT volume",
    )
    p.add_argument(
        "--max-overheat-rate",
        type=float,
        default=40.0,
        help="Exclude if either exchange 24h change is >= this percent (0 to disable)",
    )

    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--conservative",
        dest="conservative",
        action="store_true",
        default=True,
        help="Use stricter final filtering (default)",
    )
    mode.add_argument(
        "--normal",
        dest="conservative",
        action="store_false",
        help="Disable conservative final filtering",
    )
    p.add_argument(
        "--conservative-min-bithumb-value",
        type=float,
        default=2_000_000_000,
        help="Min Bithumb KRW value in conservative mode",
    )
    p.add_argument(
        "--conservative-min-bitget-volume",
        type=float,
        default=10_000_000,
        help="Min Bitget USDT volume in conservative mode",
    )
    p.add_argument(
        "--conservative-max-rate",
        type=float,
        default=20.0,
        help="Max 24h change in conservative mode",
    )
    p.add_argument(
        "--conservative-max-abs-funding",
        type=float,
        default=0.0015,
        help="Max absolute funding rate in conservative mode",
    )

    orderable = p.add_mutually_exclusive_group()
    orderable.add_argument(
        "--require-bithumb-orderable",
        dest="require_orderable",
        action="store_true",
        default=True,
        help="Keep only symbols with active Bithumb orderbook (default)",
    )
    orderable.add_argument(
        "--skip-bithumb-orderable-check",
        dest="require_orderable",
        action="store_false",
        help="Skip Bithumb orderbook verification",
    )
    p.add_argument(
        "--orderbook-timeout-sec",
        type=int,
        default=8,
        help="Timeout for each Bithumb orderbook check",
    )
    p.add_argument(
        "--max-orderbook-checks",
        type=int,
        default=0,
        help="Max symbols to check for orderbook status (0 means no limit)",
    )

    p.add_argument(
        "--watch",
        action="store_true",
        help="Repeat scans on interval",
    )
    p.add_argument(
        "--interval-sec",
        type=int,
        default=300,
        help="Watch interval in seconds (default: 300 = 5 minutes)",
    )
    p.add_argument(
        "--cycles",
        type=int,
        default=0,
        help="Stop after N watch cycles (0 means infinite)",
    )

    return p.parse_args()


def run_once(args: argparse.Namespace, cycle: int) -> int:
    try:
        bithumb_json = fetch_json(BITHUMB_TICKER_URL)
        bitget_json = fetch_json(BITGET_TICKERS_URL)
        bithumb = parse_bithumb(bithumb_json)
        bitget = parse_bitget(bitget_json)
    except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
        print(f"Failed to fetch markets: {exc}", file=sys.stderr)
        return 1

    min_bithumb_value = args.min_bithumb_value
    min_bitget_volume = args.min_bitget_volume
    if args.conservative:
        min_bithumb_value = max(min_bithumb_value, args.conservative_min_bithumb_value)
        min_bitget_volume = max(min_bitget_volume, args.conservative_min_bitget_volume)

    candidates = build_candidates(
        bithumb=bithumb,
        bitget=bitget,
        min_bithumb_rate=args.min_bithumb_rate,
        min_bitget_rate=args.min_bitget_rate,
        min_bithumb_krw=min_bithumb_value,
        min_bitget_usdt=min_bitget_volume,
    )
    initial_count = len(candidates)

    candidates, removed_overheat = apply_overheat_filter(
        candidates, max_overheat_rate=args.max_overheat_rate
    )

    removed_conservative = 0
    if args.conservative:
        candidates, removed_conservative = apply_conservative_filter(
            candidates,
            max_rate=args.conservative_max_rate,
            max_abs_funding=args.conservative_max_abs_funding,
        )

    removed_orderable = 0
    orderbook_checked = 0
    if args.require_orderable:
        candidates, orderbook_checked, removed_orderable = apply_bithumb_orderable_filter(
            candidates,
            timeout_sec=args.orderbook_timeout_sec,
            max_checks=args.max_orderbook_checks,
        )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] scan #{cycle} | Bithumb spot + Bitget USDT-M")
    print(
        f"Mode: {'conservative' if args.conservative else 'normal'} | "
        f"Orderable check: {'on' if args.require_orderable else 'off'} | "
        f"Overheat cut: {args.max_overheat_rate:.2f}%"
    )
    print(
        f"Base filters: b_rate>={args.min_bithumb_rate}%, g_rate>={args.min_bitget_rate}%, "
        f"b_value>={min_bithumb_value:,.0f} KRW, g_vol>={min_bitget_volume:,.0f} USDT"
    )
    print(
        f"Universe={initial_count}, removed(overheat={removed_overheat}, "
        f"conservative={removed_conservative}, orderable={removed_orderable}), "
        f"orderbook_checked={orderbook_checked}"
    )
    print_table(candidates, args.top)
    return 0


def main() -> int:
    args = parse_args()
    if args.watch and args.interval_sec < 1:
        print("--interval-sec must be >= 1", file=sys.stderr)
        return 2

    cycle = 0
    last_rc = 0
    try:
        while True:
            cycle += 1
            if cycle > 1:
                print("")
            rc = run_once(args, cycle=cycle)
            if rc != 0:
                last_rc = rc

            if not args.watch:
                break
            if args.cycles > 0 and cycle >= args.cycles:
                break

            print(f"Next scan in {args.interval_sec} seconds...")
            time.sleep(args.interval_sec)
    except KeyboardInterrupt:
        print("\nStopped by user.")
        return 130

    return last_rc


if __name__ == "__main__":
    raise SystemExit(main())
