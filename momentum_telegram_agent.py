#!/usr/bin/env python3
"""
Telegram delivery + auto-validation + auto-calibration runner.

This script:
1) scans Bithumb spot + Bitget USDT-M momentum candidates
2) sends top picks to Telegram
3) validates past picks after a horizon (default 15m)
4) auto-tunes filters based on rolling outcomes
5) persists state for next runs
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from crypto_momentum_scanner import (
    BITHUMB_TICKER_URL,
    BITGET_TICKERS_URL,
    apply_bithumb_orderable_filter,
    apply_conservative_filter,
    apply_overheat_filter,
    build_candidates,
    parse_bithumb,
    parse_bitget,
    fetch_json,
)


KST = timezone(timedelta(hours=9))
DASHBOARD_OWNER = os.getenv("DASHBOARD_OWNER", "JerodLee")
DASHBOARD_REPO = os.getenv("DASHBOARD_REPO", "codextomoney")
DASHBOARD_REF = os.getenv("DASHBOARD_REF") or os.getenv("GITHUB_SHA") or "main"
DASHBOARD_URL = os.getenv(
    "DASHBOARD_URL",
    f"https://raw.githack.com/{DASHBOARD_OWNER}/{DASHBOARD_REPO}/{DASHBOARD_REF}/docs/index.html",
)


DEFAULT_DYNAMIC_CONFIG: Dict[str, float] = {
    "min_bithumb_rate": 1.0,
    "min_bitget_rate": 1.0,
    "min_bitget_short_rate": 1.0,
    "short_max_bithumb_rate": 3.0,
    "short_min_funding_rate": -0.0005,
    "min_bithumb_value": 2_000_000_000,
    "min_bitget_volume": 10_000_000,
    "max_overheat_rate": 40.0,
    "conservative_max_rate": 20.0,
    "conservative_max_abs_funding": 0.0015,
}

MARKET_TREND_UP = 0.6
MARKET_TREND_DOWN = -0.6
TIMEFRAME_MINUTES: List[Tuple[str, int]] = [
    ("24h", 24 * 60),
    ("12h", 12 * 60),
    ("6h", 6 * 60),
    ("1h", 60),
    ("15m", 15),
    ("5m", 5),
    ("1m", 1),
]
MODEL_LONG_ID = "momentum_long_v1"
MODEL_SHORT_ID = "momentum_short_v1"
MODEL_NAMES = {
    MODEL_LONG_ID: "롱 모멘텀 v1",
    MODEL_SHORT_ID: "숏 모멘텀 v1",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def round_step(v: float, step: float = 0.01) -> float:
    return round(round(v / step) * step, 8)


def model_id_from_side(side: str) -> str:
    return MODEL_SHORT_ID if side == "SHORT" else MODEL_LONG_ID


def model_name_from_id(model_id: str) -> str:
    return MODEL_NAMES.get(model_id, model_id)


def sanitize_dynamic_config(cfg: Dict[str, float]) -> Dict[str, float]:
    out = dict(DEFAULT_DYNAMIC_CONFIG)
    out.update(cfg or {})
    for k in (
        "min_bithumb_rate",
        "min_bitget_rate",
        "min_bitget_short_rate",
        "short_max_bithumb_rate",
        "max_overheat_rate",
        "conservative_max_rate",
    ):
        out[k] = round_step(float(out[k]), 0.01)
    out["short_min_funding_rate"] = round_step(float(out["short_min_funding_rate"]), 0.0001)
    out["conservative_max_abs_funding"] = round_step(
        min(0.01, max(0.0005, float(out["conservative_max_abs_funding"]))),
        0.0001,
    )
    out["min_bithumb_value"] = float(int(out["min_bithumb_value"]))
    out["min_bitget_volume"] = float(int(out["min_bitget_volume"]))
    return out


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "version": 2,
            "dynamic_config": dict(DEFAULT_DYNAMIC_CONFIG),
            "pending": [],
            "results": [],
            "recommendation_history": [],
            "run_history": [],
            "market_series": [],
            "calibration_events": [],
            "meta": {
                "no_candidate_streak": 0,
                "last_calibrated_at": None,
                "last_run_at": None,
            },
        }
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    data.setdefault("dynamic_config", dict(DEFAULT_DYNAMIC_CONFIG))
    for k, v in DEFAULT_DYNAMIC_CONFIG.items():
        data["dynamic_config"].setdefault(k, v)
    # Legacy states can contain an invalid zero cap due to old rounding.
    # Keep this in a sane range so recommendations are not fully blocked.
    try:
        cap = float(data["dynamic_config"].get("conservative_max_abs_funding", 0.0))
    except Exception:  # noqa: BLE001
        cap = 0.0
    if cap <= 0.0:
        data["dynamic_config"]["conservative_max_abs_funding"] = float(
            DEFAULT_DYNAMIC_CONFIG["conservative_max_abs_funding"]
        )
    data.setdefault("pending", [])
    data.setdefault("results", [])
    data.setdefault("recommendation_history", [])
    data.setdefault("run_history", [])
    data.setdefault("market_series", [])
    data.setdefault("calibration_events", [])
    data.setdefault("meta", {})
    data["meta"].setdefault("no_candidate_streak", 0)
    data["meta"].setdefault("last_calibrated_at", None)
    data["meta"].setdefault("last_run_at", None)
    data["meta"].setdefault("calibration_cooldown_until", None)
    data["meta"].setdefault("last_rollback_event_id", None)
    data.setdefault("version", 2)
    return data


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def append_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def fetch_market_snapshot() -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    bithumb_json = fetch_json(BITHUMB_TICKER_URL)
    bitget_json = fetch_json(BITGET_TICKERS_URL)
    bithumb = parse_bithumb(bithumb_json)
    bitget = parse_bitget(bitget_json)
    return bithumb, bitget, {
        "bithumb_count": len(bithumb),
        "bitget_count": len(bitget),
    }


def trend_sign(change24h: float | None) -> int:
    if change24h is None:
        return 0
    if change24h >= MARKET_TREND_UP:
        return 1
    if change24h <= MARKET_TREND_DOWN:
        return -1
    return 0


def trend_label(sign: int) -> str:
    if sign > 0:
        return "up"
    if sign < 0:
        return "down"
    return "neutral"


def _weighted_avg_price(
    bitget: Dict[str, Any],
    include_fn: Any = None,
) -> float | None:
    total_w = 0.0
    total_v = 0.0
    for sym, t in bitget.items():
        if include_fn and not include_fn(sym):
            continue
        vol = float(getattr(t, "usdt_volume", 0.0) or 0.0)
        px = float(getattr(t, "last_price", 0.0) or 0.0)
        if vol <= 0 or px <= 0:
            continue
        total_w += px * vol
        total_v += vol
    if total_v <= 0:
        return None
    return total_w / total_v


def compute_market_snapshot(
    bitget: Dict[str, Any],
    now: datetime,
) -> Dict[str, Any]:
    btc = bitget.get("BTC")
    eth = bitget.get("ETH")
    btc_px = float(getattr(btc, "last_price", 0.0) or 0.0) if btc else None
    eth_px = float(getattr(eth, "last_price", 0.0) or 0.0) if eth else None

    market_px = _weighted_avg_price(bitget)
    alt_px = _weighted_avg_price(bitget, include_fn=lambda s: s not in {"BTC", "ETH"})

    total_vol = 0.0
    btc_vol = 0.0
    eth_vol = 0.0
    top_alt_symbol = None
    top_alt_vol = 0.0
    for sym, t in bitget.items():
        vol = float(getattr(t, "usdt_volume", 0.0) or 0.0)
        total_vol += max(0.0, vol)
        if sym == "BTC":
            btc_vol = max(0.0, vol)
        elif sym == "ETH":
            eth_vol = max(0.0, vol)
        else:
            if vol > top_alt_vol:
                top_alt_vol = vol
                top_alt_symbol = sym

    btc_share = (btc_vol / total_vol) if total_vol > 0 else 0.0
    eth_share = (eth_vol / total_vol) if total_vol > 0 else 0.0
    alt_share = max(0.0, 1.0 - btc_share - eth_share)
    top_alt_share = (top_alt_vol / total_vol) if total_vol > 0 else 0.0

    regime = "balanced"
    if btc_share >= 0.45 and btc_share > eth_share + 0.08:
        regime = "btc"
    elif eth_share >= 0.30 and eth_share > btc_share:
        regime = "eth"
    elif top_alt_share >= 0.18 and top_alt_symbol:
        regime = "single-alt"
    elif alt_share >= 0.60:
        regime = "alt-broad"

    return {
        "at": iso_z(now),
        "prices": {
            "market": market_px,
            "btc": btc_px,
            "eth": eth_px,
            "alt": alt_px,
        },
        "concentration": {
            "regime": regime,
            "btc_share": btc_share,
            "eth_share": eth_share,
            "alt_share": alt_share,
            "top_alt_symbol": top_alt_symbol,
            "top_alt_share": top_alt_share,
        },
    }


def append_market_series(state: Dict[str, Any], snapshot: Dict[str, Any], keep: int = 5000) -> None:
    series = list(state.get("market_series", []))
    series.append(snapshot)
    if len(series) > keep:
        series = series[-keep:]
    state["market_series"] = series


def _series_points(series: List[Dict[str, Any]], key: str) -> List[Tuple[float, float]]:
    points: List[Tuple[float, float]] = []
    for row in series:
        try:
            ts = parse_iso(str(row.get("at"))).timestamp()
            val = row.get("prices", {}).get(key)
            if val is None:
                continue
            v = float(val)
            if v <= 0:
                continue
            points.append((ts, v))
        except Exception:  # noqa: BLE001
            continue
    points.sort(key=lambda x: x[0])
    return points


def _series_value_at(
    series: List[Dict[str, Any]],
    key: str,
    target_ts: float,
) -> float | None:
    if not series:
        return None

    points = _series_points(series, key)
    if not points:
        return None

    if target_ts <= points[0][0]:
        return points[0][1]
    if target_ts >= points[-1][0]:
        return points[-1][1]

    for i in range(1, len(points)):
        t1, v1 = points[i]
        t0, v0 = points[i - 1]
        if t0 <= target_ts <= t1:
            if abs(t1 - t0) < 1e-9:
                return v1
            ratio = (target_ts - t0) / (t1 - t0)
            return v0 + (v1 - v0) * ratio
    return None


def compute_timeframe_changes(
    series: List[Dict[str, Any]],
    now: datetime,
    key: str,
    fallback_24h: float | None = None,
) -> Dict[str, float | None]:
    now_ts = now.timestamp()
    points = _series_points(series, key)
    min_ts = points[0][0] if points else None
    now_val = _series_value_at(series, key, now_ts)
    out: Dict[str, float | None] = {}
    for label, mins in TIMEFRAME_MINUTES:
        target_ts = now_ts - (mins * 60.0)
        if len(points) < 2 or min_ts is None or target_ts < min_ts:
            if label == "24h" and fallback_24h is not None:
                out[label] = fallback_24h
            else:
                out[label] = None
            continue
        past_val = _series_value_at(series, key, target_ts)
        if now_val and past_val and past_val > 0:
            out[label] = ((now_val / past_val) - 1.0) * 100.0
        elif label == "24h" and fallback_24h is not None:
            out[label] = fallback_24h
        else:
            out[label] = None
    return out


def compute_market_indicators(
    bitget: Dict[str, Any],
    series: List[Dict[str, Any]],
    now: datetime,
    concentration: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    total_w = 0.0
    total_v = 0.0
    for t in bitget.values():
        vol = float(getattr(t, "usdt_volume", 0.0) or 0.0)
        chg = float(getattr(t, "change24h_pct", 0.0) or 0.0)
        if vol <= 0:
            continue
        total_w += chg * vol
        total_v += vol
    market_change24h = (total_w / total_v) if total_v > 0 else 0.0

    btc = bitget.get("BTC")
    eth = bitget.get("ETH")
    btc_change24h = float(getattr(btc, "change24h_pct", 0.0)) if btc else None
    eth_change24h = float(getattr(eth, "change24h_pct", 0.0)) if eth else None

    market_changes = compute_timeframe_changes(
        series=series,
        now=now,
        key="market",
        fallback_24h=market_change24h,
    )
    btc_changes = compute_timeframe_changes(
        series=series,
        now=now,
        key="btc",
        fallback_24h=btc_change24h,
    )
    eth_changes = compute_timeframe_changes(
        series=series,
        now=now,
        key="eth",
        fallback_24h=eth_change24h,
    )

    def pack(changes: Dict[str, float | None]) -> Dict[str, Any]:
        base = changes.get("1h")
        if base is None:
            base = changes.get("24h")
        s = trend_sign(base)
        return {
            "change24h": changes.get("24h"),
            "changes": changes,
            "sign": s,
            "trend": trend_label(s),
        }

    return {
        "market": pack(market_changes),
        "btc": pack(btc_changes),
        "eth": pack(eth_changes),
        "concentration": concentration,
    }


def compute_candidates(
    bithumb: Dict[str, Any],
    bitget: Dict[str, Any],
    cfg: Dict[str, float],
    orderbook_timeout_sec: int,
    max_orderbook_checks: int,
) -> Tuple[List[Any], Dict[str, int]]:
    candidates = build_candidates(
        bithumb=bithumb,
        bitget=bitget,
        min_bithumb_rate=cfg["min_bithumb_rate"],
        min_bitget_rate=cfg["min_bitget_rate"],
        min_bitget_short_rate=cfg["min_bitget_short_rate"],
        short_max_bithumb_rate=cfg["short_max_bithumb_rate"],
        short_min_funding_rate=cfg["short_min_funding_rate"],
        min_bithumb_krw=cfg["min_bithumb_value"],
        min_bitget_usdt=cfg["min_bitget_volume"],
        include_short=True,
    )
    base_universe = len(candidates)
    candidates, removed_overheat = apply_overheat_filter(
        candidates, cfg["max_overheat_rate"]
    )
    candidates, removed_conservative = apply_conservative_filter(
        candidates,
        max_rate=cfg["conservative_max_rate"],
        max_abs_funding=cfg["conservative_max_abs_funding"],
    )
    candidates, orderbook_checked, removed_orderable = apply_bithumb_orderable_filter(
        candidates,
        timeout_sec=orderbook_timeout_sec,
        max_checks=max_orderbook_checks,
    )
    return candidates, {
        "base_universe": base_universe,
        "removed_overheat": removed_overheat,
        "removed_conservative": removed_conservative,
        "removed_orderable": removed_orderable,
        "orderbook_checked": orderbook_checked,
    }


def make_recommendations(
    candidates: List[Any],
    top_n: int,
    min_short_picks: int,
    market_indicators: Dict[str, Dict[str, Any]],
    run_ts: datetime,
    horizon_min: int,
) -> List[Dict[str, Any]]:
    selected: List[Any] = []
    top_n = max(0, int(top_n))
    min_short_picks = max(0, int(min_short_picks))
    if top_n > 0:
        shorts = [
            c for c in candidates if str(getattr(c, "side", "LONG")).upper() == "SHORT"
        ]
        forced = shorts[: min(min_short_picks, top_n)]
        selected.extend(forced)
        remain = top_n - len(selected)
        if remain > 0:
            rest = [c for c in candidates if c not in selected]
            selected.extend(rest[:remain])
        selected = sorted(
            selected,
            key=lambda x: (x.score, abs(x.g_change24h_pct), abs(x.b_rate24h)),
            reverse=True,
        )

    out: List[Dict[str, Any]] = []
    market_changes = market_indicators.get("market", {}).get("changes", {}) or {}
    btc_changes = market_indicators.get("btc", {}).get("changes", {}) or {}
    eth_changes = market_indicators.get("eth", {}).get("changes", {}) or {}
    for c in selected:
        mid = model_id_from_side(c.side)
        out.append(
            {
                "id": f"{c.symbol}-{c.side}-{int(run_ts.timestamp())}",
                "symbol": c.symbol,
                "side": c.side,
                "model_id": mid,
                "model_name": model_name_from_id(mid),
                "created_at": iso_z(run_ts),
                "horizon_min": horizon_min,
                "entry_bithumb_price": c.b_close_krw,
                "entry_bitget_price": c.g_last_price,
                "score": c.score,
                "b_rate24h": c.b_rate24h,
                "g_rate24h": c.g_change24h_pct,
                "b_value24h": c.b_krw_value24h,
                "g_volume24h": c.g_usdt_volume,
                "g_funding_rate": c.g_funding_rate,
                "g_open_interest": c.g_open_interest,
                "g_symbol": c.g_symbol,
                "market_sign_market": int(market_indicators.get("market", {}).get("sign", 0)),
                "market_sign_btc": int(market_indicators.get("btc", {}).get("sign", 0)),
                "market_sign_eth": int(market_indicators.get("eth", {}).get("sign", 0)),
                "market_change_market_1h": market_changes.get("1h"),
                "market_change_market_24h": market_changes.get("24h"),
                "market_change_btc_1h": btc_changes.get("1h"),
                "market_change_btc_24h": btc_changes.get("24h"),
                "market_change_eth_1h": eth_changes.get("1h"),
                "market_change_eth_24h": eth_changes.get("24h"),
            }
        )
    return out


def pick_side(p: Dict[str, Any]) -> str:
    side = str(p.get("side", "LONG")).upper()
    return "SHORT" if side == "SHORT" else "LONG"


def pick_model_id(p: Dict[str, Any]) -> str:
    raw = str(p.get("model_id", "")).strip()
    if raw:
        return raw
    return model_id_from_side(pick_side(p))


def side_sign(side: str) -> int:
    return -1 if side == "SHORT" else 1


def trade_return_from_market_return(
    market_return: float | None,
    side: str,
) -> float | None:
    if market_return is None:
        return None
    return -market_return if side == "SHORT" else market_return


def relation_from_value(v: float | None, neutral_band: float = 0.2) -> str:
    if v is None:
        return "insufficient"
    if v > neutral_band:
        return "proportional"
    if v < -neutral_band:
        return "inverse"
    return "mixed"


def pearson_corr(xs: List[float], ys: List[float]) -> float | None:
    if len(xs) < 2 or len(ys) < 2 or len(xs) != len(ys):
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    cov = 0.0
    vx = 0.0
    vy = 0.0
    for x, y in zip(xs, ys):
        dx = x - mx
        dy = y - my
        cov += dx * dy
        vx += dx * dx
        vy += dy * dy
    if vx <= 1e-12 or vy <= 1e-12:
        return None
    return cov / math.sqrt(vx * vy)


def compute_alignment_now(
    picks: List[Dict[str, Any]],
    market_indicators: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for key in ("market", "btc", "eth"):
        m_sign = int(market_indicators.get(key, {}).get("sign", 0))
        if m_sign == 0:
            out[key] = {
                "value": None,
                "relation": "neutral-market",
                "sample": 0,
            }
            continue
        vals: List[float] = []
        for p in picks:
            s = side_sign(pick_side(p))
            vals.append(float(s * m_sign))
        v = (sum(vals) / len(vals)) if vals else None
        out[key] = {
            "value": v,
            "relation": relation_from_value(v),
            "sample": len(vals),
        }
    return out


def compute_alignment_history(
    recommendation_history: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    field_map = {
        "market": "market_sign_market",
        "btc": "market_sign_btc",
        "eth": "market_sign_eth",
    }
    for key, field in field_map.items():
        xs_sign: List[float] = []
        ys_sign: List[float] = []
        xs_change: List[float] = []
        ys_change: List[float] = []
        change_1h = f"market_change_{key}_1h"
        change_24h = f"market_change_{key}_24h"
        for p in recommendation_history:
            s = float(side_sign(pick_side(p)))
            try:
                m_sign = int(float(p.get(field, 0) or 0))
            except Exception:  # noqa: BLE001
                m_sign = 0
            if m_sign == 0:
                pass
            else:
                xs_sign.append(s)
                ys_sign.append(float(1 if m_sign > 0 else -1))

            ch_val = p.get(change_1h)
            if ch_val is None:
                ch_val = p.get(change_24h)
            try:
                ch = float(ch_val)
            except Exception:  # noqa: BLE001
                ch = 0.0
            if abs(ch) > 1e-9:
                xs_change.append(s)
                ys_change.append(ch)

        corr = pearson_corr(xs_sign, ys_sign)
        sample = len(xs_sign)
        if corr is None:
            corr = pearson_corr(xs_change, ys_change)
            if corr is not None:
                sample = len(xs_change)
        if corr is None and xs_sign:
            corr = sum((x * y) for x, y in zip(xs_sign, ys_sign)) / len(xs_sign)
            sample = len(xs_sign)
        out[key] = {
            "correlation": corr,
            "relation": relation_from_value(corr),
            "sample": sample,
        }
    return out


def _build_run_sign_series(run_history: List[Dict[str, Any]]) -> Dict[str, List[Tuple[float, int]]]:
    out: Dict[str, List[Tuple[float, int]]] = {"market": [], "btc": [], "eth": []}
    for r in run_history:
        try:
            ts = parse_iso(str(r.get("run_at"))).timestamp()
        except Exception:  # noqa: BLE001
            continue
        mi = r.get("market_indicators", {}) or {}
        for key in ("market", "btc", "eth"):
            s = mi.get(key, {}).get("sign")
            if s is None:
                continue
            try:
                out[key].append((ts, int(s)))
            except Exception:  # noqa: BLE001
                continue
    for key in out:
        out[key].sort(key=lambda x: x[0])
    return out


def _lookup_sign_at(series: List[Tuple[float, int]], ts: float) -> int:
    if not series:
        return 0
    last = 0
    for t, s in series:
        if t <= ts:
            last = s
            continue
        break
    if last != 0:
        return last
    return int(series[0][1])


def _build_run_change_series(
    run_history: List[Dict[str, Any]],
) -> Dict[str, List[Tuple[float, float | None, float | None]]]:
    out: Dict[str, List[Tuple[float, float | None, float | None]]] = {
        "market": [],
        "btc": [],
        "eth": [],
    }
    for r in run_history:
        try:
            ts = parse_iso(str(r.get("run_at"))).timestamp()
        except Exception:  # noqa: BLE001
            continue
        mi = r.get("market_indicators", {}) or {}
        for key in ("market", "btc", "eth"):
            row = mi.get(key, {}) or {}
            changes = row.get("changes", {}) or {}
            c1 = changes.get("1h")
            c24 = changes.get("24h", row.get("change24h"))
            c1f = None
            c24f = None
            try:
                if c1 is not None:
                    c1f = float(c1)
            except Exception:  # noqa: BLE001
                c1f = None
            try:
                if c24 is not None:
                    c24f = float(c24)
            except Exception:  # noqa: BLE001
                c24f = None
            out[key].append((ts, c1f, c24f))
    for key in out:
        out[key].sort(key=lambda x: x[0])
    return out


def _lookup_change_at(
    series: List[Tuple[float, float | None, float | None]],
    ts: float,
) -> Tuple[float | None, float | None]:
    if not series:
        return None, None
    last: Tuple[float, float | None, float | None] | None = None
    for row in series:
        if row[0] <= ts:
            last = row
            continue
        break
    if last is None:
        last = series[0]
    return last[1], last[2]


def enrich_recommendations_with_market_signs(
    recommendation_history: List[Dict[str, Any]],
    run_history: List[Dict[str, Any]],
) -> None:
    sign_series = _build_run_sign_series(run_history)
    change_series = _build_run_change_series(run_history)
    if not any(sign_series.values()) and not any(change_series.values()):
        return
    field_map = {
        "market": "market_sign_market",
        "btc": "market_sign_btc",
        "eth": "market_sign_eth",
    }
    for p in recommendation_history:
        try:
            ts = parse_iso(str(p.get("created_at"))).timestamp()
        except Exception:  # noqa: BLE001
            continue
        for key, field in field_map.items():
            cur = p.get(field)
            if cur not in (None, "", 0):
                pass
            else:
                s = _lookup_sign_at(sign_series[key], ts)
                if s != 0:
                    p[field] = int(s)
            c1, c24 = _lookup_change_at(change_series[key], ts)
            change_1h = f"market_change_{key}_1h"
            change_24h = f"market_change_{key}_24h"
            if p.get(change_1h) is None and c1 is not None:
                p[change_1h] = c1
            if p.get(change_24h) is None and c24 is not None:
                p[change_24h] = c24


def blend_returns(
    side: str,
    b_ret: float | None,
    g_ret: float | None,
) -> Tuple[float, int]:
    # Short recommendations are Bitget-first by design.
    if side == "SHORT":
        if g_ret is not None:
            return g_ret, 1
        if b_ret is not None:
            return b_ret, 1
        return 0.0, 0

    vals = [x for x in (b_ret, g_ret) if x is not None]
    if not vals:
        return 0.0, 0
    return sum(vals) / len(vals), len(vals)


def evaluate_pending(
    pending: List[Dict[str, Any]],
    bithumb: Dict[str, Any],
    bitget: Dict[str, Any],
    now: datetime,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    still_pending: List[Dict[str, Any]] = []
    finalized: List[Dict[str, Any]] = []

    for p in pending:
        created = parse_iso(p["created_at"])
        horizon = int(p.get("horizon_min", 30))
        side = pick_side(p)
        if now < created + timedelta(minutes=horizon):
            still_pending.append(p)
            continue

        symbol = p["symbol"]
        b_now = bithumb.get(symbol)
        g_now = bitget.get(symbol)

        b_market_ret = None
        g_market_ret = None
        if b_now and p.get("entry_bithumb_price", 0) > 0:
            b_market_ret = (b_now.close_krw - p["entry_bithumb_price"]) / p["entry_bithumb_price"]
        if g_now and p.get("entry_bitget_price", 0) > 0:
            g_market_ret = (g_now.last_price - p["entry_bitget_price"]) / p["entry_bitget_price"]

        b_ret = trade_return_from_market_return(b_market_ret, side)
        g_ret = trade_return_from_market_return(g_market_ret, side)

        if b_ret is None and g_ret is None:
            # Keep one extra horizon for temporary API mismatch.
            if now < created + timedelta(minutes=horizon * 2):
                still_pending.append(p)
                continue
        blended, available = blend_returns(side, b_ret, g_ret)

        finalized.append(
            {
                "id": p["id"],
                "symbol": symbol,
                "side": side,
                "model_id": pick_model_id(p),
                "model_name": model_name_from_id(pick_model_id(p)),
                "created_at": p["created_at"],
                "evaluated_at": iso_z(now),
                "horizon_min": horizon,
                "entry_bithumb_price": p.get("entry_bithumb_price"),
                "entry_bitget_price": p.get("entry_bitget_price"),
                "exit_bithumb_price": b_now.close_krw if b_now else None,
                "exit_bitget_price": g_now.last_price if g_now else None,
                "return_bithumb": b_ret,
                "return_bitget": g_ret,
                "return_blended": blended,
                "win": blended > 0,
                "available_legs": available,
                "score": p.get("score"),
                "g_funding_rate": p.get("g_funding_rate"),
                "g_open_interest": p.get("g_open_interest"),
            }
        )

    return still_pending, finalized


def compute_metrics(results: List[Dict[str, Any]], window: int = 120) -> Dict[str, float]:
    recent = results[-window:]
    if not recent:
        return {"count": 0, "win_rate": 0.0, "avg_return": 0.0, "median_return": 0.0}
    returns = [float(r.get("return_blended", 0.0)) for r in recent]
    wins = sum(1 for r in recent if r.get("win"))
    return {
        "count": len(recent),
        "win_rate": wins / len(recent),
        "avg_return": sum(returns) / len(returns),
        "median_return": statistics.median(returns),
    }


def compute_model_metrics(
    results: List[Dict[str, Any]],
    window: int = 240,
) -> Dict[str, Dict[str, float | int]]:
    recent = results[-window:]
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for r in recent:
        mid = pick_model_id(r)
        buckets.setdefault(mid, []).append(r)
    out: Dict[str, Dict[str, float | int]] = {}
    for mid, rows in buckets.items():
        vals = [float(x.get("return_blended", 0.0)) for x in rows]
        wins = sum(1 for x in rows if x.get("win"))
        out[mid] = {
            "count": len(rows),
            "win_rate": (wins / len(rows)) if rows else 0.0,
            "avg_return": (sum(vals) / len(vals)) if vals else 0.0,
            "median_return": statistics.median(vals) if vals else 0.0,
            "label": model_name_from_id(mid),
        }
    return out


def _assess_latest_calibration_uplift(
    results: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
    window: int = 30,
    min_after: int = 10,
) -> Dict[str, Any] | None:
    if not results or not events:
        return None
    ev = events[-1]
    if not ev.get("id") or not ev.get("at"):
        return None
    try:
        at_ts = parse_iso(str(ev["at"])).timestamp()
    except Exception:  # noqa: BLE001
        return None
    ordered = sorted(
        [r for r in results if r.get("evaluated_at")],
        key=lambda x: parse_iso(str(x["evaluated_at"])).timestamp(),
    )
    before = [r for r in ordered if parse_iso(str(r["evaluated_at"])).timestamp() < at_ts][-window:]
    after = [r for r in ordered if parse_iso(str(r["evaluated_at"])).timestamp() >= at_ts][:window]
    if len(before) < min_after or len(after) < min_after:
        return None
    wr_before = sum(1 for r in before if r.get("win")) / len(before)
    wr_after = sum(1 for r in after if r.get("win")) / len(after)
    return {
        "event_id": str(ev["id"]),
        "event_at": str(ev["at"]),
        "delta": wr_after - wr_before,
        "before_win_rate": wr_before,
        "after_win_rate": wr_after,
        "before_n": len(before),
        "after_n": len(after),
        "pre_config": ev.get("pre_config"),
    }


def should_calibrate(
    metrics: Dict[str, float],
    new_results_count: int,
    last_calibrated_at: str | None,
    now: datetime,
    no_candidate_streak: int,
) -> bool:
    # Liquidity starvation is handled fast, even before enough eval samples.
    if no_candidate_streak >= 3:
        return True

    # Performance-based calibration needs sample size.
    if new_results_count < 3 or metrics["count"] < 20:
        return False
    if not last_calibrated_at:
        return True
    prev = parse_iso(last_calibrated_at)
    return (now - prev) >= timedelta(hours=6)


def auto_calibrate(
    cfg: Dict[str, float],
    metrics: Dict[str, float],
    no_candidate_streak: int,
) -> Tuple[Dict[str, float], List[str]]:
    new_cfg = dict(cfg)
    notes: List[str] = []

    if no_candidate_streak >= 3:
        old_b = new_cfg["min_bithumb_value"]
        old_g = new_cfg["min_bitget_volume"]
        new_cfg["min_bithumb_value"] = max(1_000_000_000, old_b * 0.90)
        new_cfg["min_bitget_volume"] = max(5_000_000, old_g * 0.90)
        new_cfg["min_bithumb_rate"] = max(0.5, new_cfg["min_bithumb_rate"] - 0.2)
        new_cfg["min_bitget_rate"] = max(0.5, new_cfg["min_bitget_rate"] - 0.2)
        new_cfg["min_bitget_short_rate"] = max(0.5, new_cfg["min_bitget_short_rate"] - 0.2)
        new_cfg["short_max_bithumb_rate"] = min(8.0, new_cfg["short_max_bithumb_rate"] + 0.5)
        new_cfg["short_min_funding_rate"] = max(
            -0.0015, new_cfg["short_min_funding_rate"] - 0.0001
        )
        notes.append(
            "Low-candidate adjustment: loosened liquidity and momentum floors slightly."
        )

    if metrics["count"] >= 20 and metrics["win_rate"] < 0.45:
        new_cfg["max_overheat_rate"] = max(20.0, new_cfg["max_overheat_rate"] - 2.0)
        new_cfg["conservative_max_rate"] = max(
            10.0, new_cfg["conservative_max_rate"] - 1.0
        )
        new_cfg["min_bithumb_value"] = min(20_000_000_000, new_cfg["min_bithumb_value"] * 1.10)
        new_cfg["min_bitget_volume"] = min(60_000_000, new_cfg["min_bitget_volume"] * 1.10)
        new_cfg["conservative_max_abs_funding"] = max(
            0.0005, new_cfg["conservative_max_abs_funding"] * 0.90
        )
        notes.append(
            "Underperformance adjustment: tightened overheat/funding caps and raised liquidity floors."
        )
    elif metrics["count"] >= 20 and metrics["win_rate"] > 0.60 and metrics["avg_return"] > 0.002:
        new_cfg["max_overheat_rate"] = min(50.0, new_cfg["max_overheat_rate"] + 1.0)
        new_cfg["conservative_max_rate"] = min(
            25.0, new_cfg["conservative_max_rate"] + 1.0
        )
        new_cfg["min_bithumb_value"] = max(1_000_000_000, new_cfg["min_bithumb_value"] * 0.95)
        new_cfg["min_bitget_volume"] = max(5_000_000, new_cfg["min_bitget_volume"] * 0.95)
        new_cfg["conservative_max_abs_funding"] = min(
            0.0025, new_cfg["conservative_max_abs_funding"] * 1.05
        )
        notes.append(
            "Strong-performance adjustment: relaxed filters slightly to widen search."
        )

    return sanitize_dynamic_config(new_cfg), notes


def format_pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def format_money_k(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.2f}B KRW"
    return f"{v:,.0f} KRW"


def format_money_u(v: float) -> str:
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M USDT"
    return f"{v:,.0f} USDT"


def format_oi(v: float | None) -> str:
    if v is None:
        return "-"
    x = float(v)
    if abs(x) >= 1_000_000:
        return f"{x / 1_000_000:.2f}M"
    if abs(x) >= 1_000:
        return f"{x / 1_000:.2f}K"
    return f"{x:.0f}"


def format_pick_line(index: int, p: Dict[str, Any]) -> str:
    side = pick_side(p)
    fr = float(p.get("g_funding_rate", 0.0) or 0.0)
    oi = format_oi(p.get("g_open_interest"))
    return f"{index}) {p['symbol']} | {side} | score {p['score']:.3f} | fr {fr:.4f} | oi {oi}"


def evaluate_live_return(
    p: Dict[str, Any],
    bithumb: Dict[str, Any],
    bitget: Dict[str, Any],
) -> Tuple[float | None, float | None, float | None]:
    side = pick_side(p)
    symbol = p["symbol"]
    b_now = bithumb.get(symbol)
    g_now = bitget.get(symbol)

    b_market_ret = None
    g_market_ret = None
    if b_now and p.get("entry_bithumb_price", 0) > 0:
        b_market_ret = (b_now.close_krw - p["entry_bithumb_price"]) / p["entry_bithumb_price"]
    if g_now and p.get("entry_bitget_price", 0) > 0:
        g_market_ret = (g_now.last_price - p["entry_bitget_price"]) / p["entry_bitget_price"]

    b_ret = trade_return_from_market_return(b_market_ret, side)
    g_ret = trade_return_from_market_return(g_market_ret, side)
    blended, _ = blend_returns(side, b_ret, g_ret)
    if b_ret is None and g_ret is None:
        return b_ret, g_ret, None
    return b_ret, g_ret, blended


def detect_loss_alerts(
    pending: List[Dict[str, Any]],
    bithumb: Dict[str, Any],
    bitget: Dict[str, Any],
    now: datetime,
    threshold: float,
) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    now_iso = iso_z(now)
    for p in pending:
        b_ret, g_ret, blended = evaluate_live_return(p, bithumb, bitget)
        p["last_live_checked_at"] = now_iso
        p["last_live_return_blended"] = blended
        if blended is None:
            continue
        if blended < threshold and not p.get("loss_alert_sent_at"):
            p["loss_alert_sent_at"] = now_iso
            alerts.append(
                {
                    "id": p["id"],
                    "symbol": p["symbol"],
                    "side": pick_side(p),
                    "created_at": p["created_at"],
                    "horizon_min": int(p.get("horizon_min", 15)),
                    "live_return_blended": blended,
                    "live_return_bithumb": b_ret,
                    "live_return_bitget": g_ret,
                }
            )
    return alerts


def make_loss_alert_message(run_ts: datetime, alerts: List[Dict[str, Any]]) -> str:
    ts_kst = run_ts.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    lines = [f"손실 경고 | {ts_kst}"]
    lines.append("추천 종목이 음수 수익으로 전환되었습니다.")
    for i, a in enumerate(alerts, start=1):
        lines.append(
            f"{i}) {a['symbol']} | {a.get('side', 'LONG')} | 수익률 {format_pct(a['live_return_blended'])}"
        )
    lines.append(f"대시보드: {DASHBOARD_URL}")
    return "\n".join(lines)


def make_message(
    run_ts: datetime,
    picks: List[Dict[str, Any]],
    metrics: Dict[str, float],
    filter_stats: Dict[str, int],
    cfg: Dict[str, float],
    new_results_count: int,
    calibrate_notes: List[str],
    message_style: str,
) -> str:
    ts_kst = run_ts.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    lines: List[str] = []
    if message_style == "compact":
        lines.append(f"Momentum Scan | {ts_kst}")
        lines.append(
            f"Rules: overheat<{cfg['max_overheat_rate']:.0f}% | gLong>={cfg['min_bitget_rate']:.1f}% | gShort<=-{cfg['min_bitget_short_rate']:.1f}% | bShort<={cfg['short_max_bithumb_rate']:.1f}% | fShort>={cfg['short_min_funding_rate']:.4f} | bVal>={format_money_k(cfg['min_bithumb_value'])} | gVol>={format_money_u(cfg['min_bitget_volume'])}"
        )
        if picks:
            lines.append("Picks:")
            for i, p in enumerate(picks, start=1):
                lines.append(format_pick_line(i, p))
        else:
            lines.append("Picks: no symbols matched current rules")
        lines.append(
            f"Validation({int(metrics['count'])}): win {format_pct(metrics['win_rate'])} | avg {format_pct(metrics['avg_return'])} | med {format_pct(metrics['median_return'])} | new {new_results_count}"
        )
        if calibrate_notes:
            lines.append("Tune: " + "; ".join(calibrate_notes))
        lines.append(f"Dashboard: {DASHBOARD_URL}")
        return "\n".join(lines)

    lines.append(f"Momentum Scan | {ts_kst}")
    lines.append("Market: Bithumb Spot + Bitget USDT-M")
    lines.append(
        f"Filters: overheat<{cfg['max_overheat_rate']:.2f}%, gLong>={cfg['min_bitget_rate']:.2f}%, gShort<=-{cfg['min_bitget_short_rate']:.2f}%, bShort<={cfg['short_max_bithumb_rate']:.2f}%, fShort>={cfg['short_min_funding_rate']:.4f}, bValue>={format_money_k(cfg['min_bithumb_value'])}, gVol>={format_money_u(cfg['min_bitget_volume'])}"
    )
    lines.append(
        f"Candidates: base={filter_stats['base_universe']}, removed(overheat={filter_stats['removed_overheat']}, conservative={filter_stats['removed_conservative']}, orderable={filter_stats['removed_orderable']})"
    )
    if picks:
        lines.append("Top picks:")
        for i, p in enumerate(picks, start=1):
            lines.append(format_pick_line(i, p))
    else:
        lines.append("Top picks: no symbols matched current rules")
    lines.append(
        f"Validation(last {int(metrics['count'])}): winRate {format_pct(metrics['win_rate'])}, avg {format_pct(metrics['avg_return'])}, median {format_pct(metrics['median_return'])}, newly evaluated {new_results_count}"
    )
    if calibrate_notes:
        lines.append("Auto-calibration:")
        lines.extend(f"- {n}" for n in calibrate_notes)
    lines.append(f"Dashboard: {DASHBOARD_URL}")
    return "\n".join(lines)


def telegram_api_post(token: str, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8")
        except Exception:  # noqa: BLE001
            detail = ""
        raise RuntimeError(
            f"Telegram API HTTP {exc.code} on {method}. body={detail or '(empty)'}"
        ) from exc
    result = json.loads(body)
    if not result.get("ok"):
        raise RuntimeError(f"Telegram API error on {method}: {result}")
    return result


def send_telegram(token: str, chat_id: str, text: str) -> None:
    # Preflight makes invalid token failures explicit in CI logs.
    telegram_api_post(token=token, method="getMe", payload={})
    telegram_api_post(
        token=token,
        method="sendMessage",
        payload={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
    )


def run_cycle(args: argparse.Namespace, state: Dict[str, Any]) -> int:
    run_ts = utc_now()
    cfg = dict(state["dynamic_config"])
    pre_cfg = dict(state["dynamic_config"])

    try:
        bithumb, bitget, _ = fetch_market_snapshot()
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] market fetch failed: {exc}")
        return 1

    market_snapshot = compute_market_snapshot(bitget=bitget, now=run_ts)
    append_market_series(state, market_snapshot)
    market_indicators = compute_market_indicators(
        bitget=bitget,
        series=list(state.get("market_series", [])),
        now=run_ts,
        concentration=market_snapshot.get("concentration", {}),
    )

    candidates, filter_stats = compute_candidates(
        bithumb=bithumb,
        bitget=bitget,
        cfg=cfg,
        orderbook_timeout_sec=args.orderbook_timeout_sec,
        max_orderbook_checks=args.max_orderbook_checks,
    )

    picks = make_recommendations(
        candidates=candidates,
        top_n=args.top,
        min_short_picks=args.min_short_picks,
        market_indicators=market_indicators,
        run_ts=run_ts,
        horizon_min=args.horizon_min,
    )
    if picks:
        state["recommendation_history"].extend(picks)
        state["recommendation_history"] = state["recommendation_history"][-5000:]

    enrich_recommendations_with_market_signs(
        recommendation_history=state["recommendation_history"],
        run_history=state["run_history"],
    )

    pending = state["pending"] + picks
    pending, finalized = evaluate_pending(
        pending=pending,
        bithumb=bithumb,
        bitget=bitget,
        now=run_ts,
    )
    loss_alerts = detect_loss_alerts(
        pending=pending,
        bithumb=bithumb,
        bitget=bitget,
        now=run_ts,
        threshold=args.loss_alert_threshold,
    )
    state["pending"] = pending
    if finalized:
        state["results"].extend(finalized)
        # Keep state bounded.
        state["results"] = state["results"][-3000:]

    if picks:
        state["meta"]["no_candidate_streak"] = 0
    else:
        state["meta"]["no_candidate_streak"] = int(state["meta"]["no_candidate_streak"]) + 1

    metrics = compute_metrics(state["results"], window=args.metric_window)
    model_metrics = compute_model_metrics(state["results"], window=max(120, args.metric_window * 2))
    alignment_now = compute_alignment_now(picks, market_indicators)
    alignment_history = compute_alignment_history(state["recommendation_history"])

    calibrate_notes: List[str] = []
    calibrated = False
    cooldown_until_raw = state["meta"].get("calibration_cooldown_until")
    in_cooldown = False
    if cooldown_until_raw:
        try:
            in_cooldown = run_ts < parse_iso(str(cooldown_until_raw))
        except Exception:  # noqa: BLE001
            in_cooldown = False

    rollback_eval = _assess_latest_calibration_uplift(
        results=state["results"],
        events=state["calibration_events"],
        window=30,
        min_after=10,
    )
    if rollback_eval and rollback_eval["delta"] <= -0.05:
        last_rb = str(state["meta"].get("last_rollback_event_id") or "")
        if rollback_eval["event_id"] != last_rb and isinstance(rollback_eval.get("pre_config"), dict):
            cfg = sanitize_dynamic_config(rollback_eval["pre_config"])
            state["dynamic_config"] = cfg
            state["meta"]["last_calibrated_at"] = iso_z(run_ts)
            state["meta"]["calibration_cooldown_until"] = iso_z(run_ts + timedelta(hours=6))
            state["meta"]["last_rollback_event_id"] = rollback_eval["event_id"]
            note = (
                "Rollback applied: latest calibration reduced win rate by "
                f"{rollback_eval['delta'] * 100:.2f}pp (before {rollback_eval['before_win_rate'] * 100:.2f}% "
                f"-> after {rollback_eval['after_win_rate'] * 100:.2f}%)."
            )
            calibrate_notes.append(note)
            calibrated = True
            state["calibration_events"].append(
                {
                    "id": f"rollback-{int(run_ts.timestamp())}",
                    "at": iso_z(run_ts),
                    "type": "rollback",
                    "source_event_id": rollback_eval["event_id"],
                    "notes": [note],
                    "pre_config": pre_cfg,
                    "post_config": dict(cfg),
                    "metrics": dict(metrics),
                    "new_results_count": len(finalized),
                    "no_candidate_streak": state["meta"]["no_candidate_streak"],
                    "uplist_delta": rollback_eval["delta"],
                }
            )
            state["calibration_events"] = state["calibration_events"][-500:]
            in_cooldown = True

    if in_cooldown:
        calibrate_notes.append("Calibration cooldown active: tuning is paused for 6 hours after rollback.")
    elif should_calibrate(
        metrics=metrics,
        new_results_count=len(finalized),
        last_calibrated_at=state["meta"].get("last_calibrated_at"),
        now=run_ts,
        no_candidate_streak=state["meta"]["no_candidate_streak"],
    ):
        cfg, calibrate_notes = auto_calibrate(
            cfg=cfg,
            metrics=metrics,
            no_candidate_streak=state["meta"]["no_candidate_streak"],
        )
        state["dynamic_config"] = cfg
        state["meta"]["last_calibrated_at"] = iso_z(run_ts)
        calibrated = True
        state["calibration_events"].append(
            {
                "id": f"cal-{int(run_ts.timestamp())}",
                "at": iso_z(run_ts),
                "notes": calibrate_notes,
                "pre_config": pre_cfg,
                "post_config": dict(cfg),
                "metrics": dict(metrics),
                "new_results_count": len(finalized),
                "no_candidate_streak": state["meta"]["no_candidate_streak"],
            }
        )
        state["calibration_events"] = state["calibration_events"][-500:]

    msg = make_message(
        run_ts=run_ts,
        picks=picks,
        metrics=metrics,
        filter_stats=filter_stats,
        cfg=state["dynamic_config"],
        new_results_count=len(finalized),
        calibrate_notes=calibrate_notes,
        message_style=args.message_style,
    )

    print(msg)
    if loss_alerts:
        print(make_loss_alert_message(run_ts, loss_alerts))

    if not args.dry_run:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if not token or not chat_id:
            print("[ERROR] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID is not set")
            return 2
        try:
            send_telegram(token=token, chat_id=chat_id, text=msg)
            print("[INFO] telegram sent")
            if loss_alerts:
                send_telegram(
                    token=token,
                    chat_id=chat_id,
                    text=make_loss_alert_message(run_ts, loss_alerts),
                )
                print(f"[INFO] loss alert sent ({len(loss_alerts)})")
        except Exception as exc:  # noqa: BLE001
            print(f"[ERROR] telegram failed: {exc}")
            return 3

    state["run_history"].append(
        {
            "run_at": iso_z(run_ts),
            "picks_count": len(picks),
            "pending_count": len(state["pending"]),
            "results_count": len(state["results"]),
            "new_results_count": len(finalized),
            "metrics": dict(metrics),
            "filter_stats": dict(filter_stats),
            "config": dict(state["dynamic_config"]),
            "calibrated": calibrated,
            "calibration_notes": calibrate_notes,
            "loss_alert_count": len(loss_alerts),
            "market_indicators": market_indicators,
            "market_alignment_now": alignment_now,
            "market_alignment_history": alignment_history,
            "model_metrics": model_metrics,
        }
    )
    state["run_history"] = state["run_history"][-5000:]
    state["meta"]["last_run_at"] = iso_z(run_ts)
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Momentum Telegram automation runner"
    )
    p.add_argument("--state-file", default="state/bot_state.json")
    p.add_argument("--history-file", default="state/eval_history.jsonl")
    p.add_argument("--top", type=int, default=3)
    p.add_argument("--horizon-min", type=int, default=15)
    p.add_argument("--metric-window", type=int, default=120)
    p.add_argument(
        "--message-style",
        choices=("compact", "detailed"),
        default="compact",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--watch", action="store_true")
    p.add_argument("--interval-sec", type=int, default=300)
    p.add_argument("--cycles", type=int, default=0)
    p.add_argument("--orderbook-timeout-sec", type=int, default=8)
    p.add_argument("--max-orderbook-checks", type=int, default=0)
    p.add_argument("--loss-alert-threshold", type=float, default=0.0)
    p.add_argument("--min-short-picks", type=int, default=1)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    state_path = Path(args.state_file)
    history_path = Path(args.history_file)
    state = load_state(state_path)

    last_rc = 0
    cycle = 0
    while True:
        cycle += 1
        print(f"\n=== cycle {cycle} ===")
        pre_result_len = len(state["results"])
        rc = run_cycle(args, state)
        if rc != 0:
            last_rc = rc
        else:
            new_rows = state["results"][pre_result_len:]
            append_jsonl(history_path, new_rows)
        save_state(state_path, state)

        if not args.watch:
            break
        if args.cycles > 0 and cycle >= args.cycles:
            break
        print(f"[INFO] next cycle in {args.interval_sec}s")
        time.sleep(max(1, args.interval_sec))

    return last_rc


if __name__ == "__main__":
    raise SystemExit(main())
