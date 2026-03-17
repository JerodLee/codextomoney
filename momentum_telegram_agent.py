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
    BITHUMB_ORDERBOOK_URL_TMPL,
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
    f"https://rawcdn.githack.com/{DASHBOARD_OWNER}/{DASHBOARD_REPO}/{DASHBOARD_REF}/docs/index.html",
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
MODEL_LONG_V2_ID = "momentum_long_v2"
MODEL_SHORT_V2_ID = "momentum_short_v2"
MODEL_NAMES = {
    MODEL_LONG_ID: "롱 모멘텀 v1",
    MODEL_SHORT_ID: "숏 모멘텀 v1",
    MODEL_LONG_V2_ID: "롱 모멘텀 v2(시장보강)",
    MODEL_SHORT_V2_ID: "숏 모멘텀 v2(시장보강)",
}
DEFAULT_MODEL_REGISTRY: Dict[str, Dict[str, Any]] = {
    MODEL_LONG_ID: {"enabled": True, "side": "LONG"},
    MODEL_SHORT_ID: {"enabled": True, "side": "SHORT"},
    MODEL_LONG_V2_ID: {"enabled": False, "side": "LONG"},
    MODEL_SHORT_V2_ID: {"enabled": False, "side": "SHORT"},
}
MODEL_EVOLUTION_PATH: Dict[str, List[str]] = {
    "LONG": [MODEL_LONG_ID, MODEL_LONG_V2_ID],
    "SHORT": [MODEL_SHORT_ID, MODEL_SHORT_V2_ID],
}
MODEL_EXPANSION_MIN_COUNT = 24
MODEL_EXPANSION_WIN_RATE_FLOOR = 0.45
MODEL_EXPANSION_COOLDOWN_HOURS = 6
MODEL_RECOMMEND_MIN_COUNT = 24
MODEL_RECOMMEND_WIN_RATE_FLOOR = 0.48
MODEL_RECOMMEND_AVG_RETURN_FLOOR = -0.001
MODEL_DIAG_MIN_COUNT = 24
MODEL_DIAG_MIN_BUCKET = 8
DEFAULT_EVAL_HORIZONS = [5, 15, 30, 60]
MISSED_MOVE_THRESHOLDS = {
    5: 0.015,
    15: 0.025,
    30: 0.035,
    60: 0.050,
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


def next_model_id(model_id: str) -> str:
    mid = str(model_id or "").strip()
    if not mid:
        return "momentum_model_v2"
    if "_v" in mid:
        head, tail = mid.rsplit("_v", 1)
        try:
            ver = int(tail)
            return f"{head}_v{ver + 1}"
        except Exception:  # noqa: BLE001
            pass
    return f"{mid}_v2"


def sanitize_model_registry(raw: Dict[str, Any] | None) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    base = raw or {}
    for mid, spec in DEFAULT_MODEL_REGISTRY.items():
        row = dict(spec)
        src = base.get(mid, {})
        if isinstance(src, dict):
            row["enabled"] = bool(src.get("enabled", row["enabled"]))
            row["side"] = str(src.get("side", row["side"])).upper()
        out[mid] = row
    for mid, src in base.items():
        if mid in out or not isinstance(src, dict):
            continue
        side = str(src.get("side", "LONG")).upper()
        if side not in {"LONG", "SHORT"}:
            side = "LONG"
        out[mid] = {"enabled": bool(src.get("enabled", False)), "side": side}
    return out


def active_model_ids(registry: Dict[str, Dict[str, Any]], side: str) -> List[str]:
    s = str(side).upper()
    out: List[str] = []
    for mid, spec in registry.items():
        if not bool(spec.get("enabled", False)):
            continue
        if str(spec.get("side", "")).upper() != s:
            continue
        out.append(mid)
    return out


def active_models(registry: Dict[str, Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    for mid, spec in registry.items():
        if bool(spec.get("enabled", False)):
            out.append(mid)
    return out


def model_version(model_id: str) -> int:
    mid = str(model_id or "").strip()
    if "_v" in mid:
        _, tail = mid.rsplit("_v", 1)
        try:
            return max(1, int(tail))
        except Exception:  # noqa: BLE001
            pass
    return 1


def side_model_chain(registry: Dict[str, Dict[str, Any]], side: str) -> List[str]:
    s = str(side).upper()
    base_mid = model_id_from_side(s)
    family = base_mid.rsplit("_v", 1)[0] if "_v" in base_mid else base_mid
    seeded = set(MODEL_EVOLUTION_PATH.get(s, []))
    for mid, spec in sanitize_model_registry(registry).items():
        if str(spec.get("side", "")).upper() != s:
            continue
        if str(mid).startswith(family):
            seeded.add(str(mid))
    if not seeded:
        seeded.add(base_mid)
    return sorted(seeded, key=lambda mid: (model_version(mid), str(mid)))


def model_side_from_id(model_id: str) -> str:
    mid = str(model_id or "").strip()
    spec = DEFAULT_MODEL_REGISTRY.get(mid, {})
    side = str(spec.get("side", "")).upper()
    if side in {"LONG", "SHORT"}:
        return side
    if "SHORT" in mid.upper():
        return "SHORT"
    return "LONG"


def safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:  # noqa: BLE001
        return None


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def parse_eval_horizons(raw: str | None, fallback_horizon: int) -> List[int]:
    vals: List[int] = []
    for token in str(raw or "").split(","):
        tok = token.strip()
        if not tok:
            continue
        try:
            n = int(tok)
        except Exception:  # noqa: BLE001
            continue
        if 1 <= n <= 24 * 60:
            vals.append(n)
    if not vals:
        vals = [max(1, int(fallback_horizon))]
    out = sorted(set(vals))
    if fallback_horizon > 0 and int(fallback_horizon) not in out:
        out.append(int(fallback_horizon))
        out.sort()
    return out


def parse_pick_eval_horizons(p: Dict[str, Any]) -> List[int]:
    raw = p.get("eval_horizons_min")
    out: List[int] = []
    if isinstance(raw, list):
        for x in raw:
            try:
                n = int(x)
            except Exception:  # noqa: BLE001
                continue
            if 1 <= n <= 24 * 60:
                out.append(n)
    if not out:
        try:
            n = int(p.get("horizon_min", 15) or 15)
        except Exception:  # noqa: BLE001
            n = 15
        out = [max(1, n)]
    return sorted(set(out))


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
            "version": 3,
            "dynamic_config": dict(DEFAULT_DYNAMIC_CONFIG),
            "model_registry": sanitize_model_registry(DEFAULT_MODEL_REGISTRY),
            "pending": [],
            "results": [],
            "missed_queue": [],
            "missed_results": [],
            "recommendation_history": [],
            "run_history": [],
            "market_series": [],
            "calibration_events": [],
            "model_governance_events": [],
            "meta": {
                "no_candidate_streak": 0,
                "last_calibrated_at": None,
                "last_run_at": None,
                "last_model_governance_at": None,
                "model_governance_cooldown_until": None,
                "last_model_recommendation": None,
                "last_model_diagnostics": None,
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
    data.setdefault("missed_queue", [])
    data.setdefault("missed_results", [])
    data.setdefault("recommendation_history", [])
    data.setdefault("run_history", [])
    data.setdefault("market_series", [])
    data.setdefault("calibration_events", [])
    data.setdefault("model_governance_events", [])
    data.setdefault("model_registry", sanitize_model_registry(DEFAULT_MODEL_REGISTRY))
    data["model_registry"] = sanitize_model_registry(data.get("model_registry"))
    data.setdefault("meta", {})
    data["meta"].setdefault("no_candidate_streak", 0)
    data["meta"].setdefault("last_calibrated_at", None)
    data["meta"].setdefault("last_run_at", None)
    data["meta"].setdefault("calibration_cooldown_until", None)
    data["meta"].setdefault("last_rollback_event_id", None)
    data["meta"].setdefault("loss_cooldowns", {})
    data["meta"].setdefault("last_model_governance_at", None)
    data["meta"].setdefault("model_governance_cooldown_until", None)
    data["meta"].setdefault("last_model_recommendation", None)
    data["meta"].setdefault("last_model_diagnostics", None)
    data.setdefault("version", 3)
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


def _parse_orderbook_levels(
    payload: Dict[str, Any],
    top_n: int = 20,
) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    if payload.get("status") != "0000":
        return [], []
    data = payload.get("data", {}) or {}
    bids_raw = data.get("bids") or []
    asks_raw = data.get("asks") or []

    def norm(rows: List[Any]) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        for row in rows[: max(1, int(top_n))]:
            if not isinstance(row, dict):
                continue
            px = safe_float(row.get("price"))
            qty = safe_float(row.get("quantity"))
            if px is None or qty is None or px <= 0 or qty <= 0:
                continue
            out.append((float(px), float(qty)))
        return out

    bids = sorted(norm(bids_raw), key=lambda x: x[0], reverse=True)
    asks = sorted(norm(asks_raw), key=lambda x: x[0])
    return bids, asks


def compute_orderblock_features(
    symbol: str,
    ref_price: float,
    timeout_sec: int = 6,
) -> Dict[str, float] | None:
    sym = str(symbol or "").upper().strip()
    if not sym or ref_price <= 0:
        return None
    try:
        payload = fetch_json(
            BITHUMB_ORDERBOOK_URL_TMPL.format(symbol=sym),
            timeout_sec=timeout_sec,
        )
    except Exception:  # noqa: BLE001
        return None
    bids, asks = _parse_orderbook_levels(payload, top_n=20)
    if not bids or not asks:
        return None

    band_pct = 0.60
    lo = ref_price * (1.0 - (band_pct / 100.0))
    hi = ref_price * (1.0 + (band_pct / 100.0))
    bid_notional = sum((px * qty) for px, qty in bids if lo <= px <= ref_price)
    ask_notional = sum((px * qty) for px, qty in asks if ref_price <= px <= hi)
    if bid_notional <= 0 and ask_notional <= 0:
        bid_notional = sum((px * qty) for px, qty in bids[:5])
        ask_notional = sum((px * qty) for px, qty in asks[:5])

    ratio = (bid_notional + 1.0) / (ask_notional + 1.0)
    signal = clamp(math.log(ratio), -1.0, 1.0)
    best_bid = max(bids, key=lambda x: x[0] * x[1], default=None)
    best_ask = max(asks, key=lambda x: x[0] * x[1], default=None)
    support_dist_pct = None
    resist_dist_pct = None
    if best_bid is not None:
        support_dist_pct = max(0.0, ((ref_price - best_bid[0]) / ref_price) * 100.0)
    if best_ask is not None:
        resist_dist_pct = max(0.0, ((best_ask[0] - ref_price) / ref_price) * 100.0)

    return {
        "signal": float(signal),
        "bid_ask_ratio": float(ratio),
        "support_dist_pct": float(support_dist_pct or 0.0),
        "resist_dist_pct": float(resist_dist_pct or 0.0),
    }


def enrich_candidates_with_orderblock(
    candidates: List[Any],
    timeout_sec: int,
    max_checks: int,
) -> Dict[str, int]:
    checked = 0
    assigned = 0
    cache: Dict[str, Dict[str, float] | None] = {}
    if not candidates:
        return {"orderblock_checked": 0, "orderblock_assigned": 0}

    to_scan = candidates if max_checks <= 0 else candidates[: max(1, int(max_checks))]
    for c in to_scan:
        sym = str(getattr(c, "symbol", "")).upper().strip()
        ref_price = safe_float(getattr(c, "b_close_krw", None))
        if not sym or ref_price is None or ref_price <= 0:
            continue
        if sym in cache:
            continue
        checked += 1
        cache[sym] = compute_orderblock_features(
            symbol=sym,
            ref_price=float(ref_price),
            timeout_sec=timeout_sec,
        )

    for c in candidates:
        sym = str(getattr(c, "symbol", "")).upper().strip()
        ob = cache.get(sym)
        if not ob:
            continue
        setattr(c, "b_ob_signal", ob.get("signal"))
        setattr(c, "b_ob_bid_ask_ratio", ob.get("bid_ask_ratio"))
        setattr(c, "b_ob_support_dist_pct", ob.get("support_dist_pct"))
        setattr(c, "b_ob_resist_dist_pct", ob.get("resist_dist_pct"))
        assigned += 1
    return {"orderblock_checked": checked, "orderblock_assigned": assigned}


def compute_candidates(
    bithumb: Dict[str, Any],
    bitget: Dict[str, Any],
    cfg: Dict[str, float],
    orderbook_timeout_sec: int,
    max_orderbook_checks: int,
    blocked_symbols: set[str] | None = None,
    orderblock_timeout_sec: int = 6,
    max_orderblock_checks: int = 20,
) -> Tuple[List[Any], Dict[str, int], Dict[str, set[Tuple[str, str]]]]:
    base_candidates = build_candidates(
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

    def key_of(x: Any) -> Tuple[str, str]:
        return (
            str(getattr(x, "symbol", "")).upper(),
            str(getattr(x, "side", "LONG")).upper(),
        )

    candidates = list(base_candidates)
    blocked = {str(x).upper() for x in (blocked_symbols or set()) if str(x).strip()}
    removed_loss_cooldown = 0
    if blocked:
        kept = []
        for c in candidates:
            if str(getattr(c, "symbol", "")).upper() in blocked:
                removed_loss_cooldown += 1
                continue
            kept.append(c)
        candidates = kept
    base_universe = len(candidates)
    stage_after_blocked = set(key_of(c) for c in candidates)

    candidates, removed_overheat = apply_overheat_filter(
        candidates, cfg["max_overheat_rate"]
    )
    stage_after_overheat = set(key_of(c) for c in candidates)

    candidates, removed_conservative = apply_conservative_filter(
        candidates,
        max_rate=cfg["conservative_max_rate"],
        max_abs_funding=cfg["conservative_max_abs_funding"],
    )
    stage_after_conservative = set(key_of(c) for c in candidates)

    candidates, orderbook_checked, removed_orderable = apply_bithumb_orderable_filter(
        candidates,
        timeout_sec=orderbook_timeout_sec,
        max_checks=max_orderbook_checks,
    )
    stage_final = set(key_of(c) for c in candidates)

    ob_stats = enrich_candidates_with_orderblock(
        candidates=candidates,
        timeout_sec=orderblock_timeout_sec,
        max_checks=max_orderblock_checks,
    )
    return (
        candidates,
        {
            "base_universe": base_universe,
            "removed_loss_cooldown": removed_loss_cooldown,
            "removed_overheat": removed_overheat,
            "removed_conservative": removed_conservative,
            "removed_orderable": removed_orderable,
            "orderbook_checked": orderbook_checked,
            "orderblock_checked": int(ob_stats.get("orderblock_checked", 0)),
            "orderblock_assigned": int(ob_stats.get("orderblock_assigned", 0)),
        },
        {
            "base": set(key_of(c) for c in base_candidates),
            "after_blocked": stage_after_blocked,
            "after_overheat": stage_after_overheat,
            "after_conservative": stage_after_conservative,
            "final": stage_final,
        },
    )


def _weighted_change(
    market_indicators: Dict[str, Dict[str, Any]],
    key: str,
    weights: List[Tuple[str, float]],
) -> float | None:
    row = market_indicators.get(key, {}) or {}
    changes = row.get("changes", {}) or {}
    total = 0.0
    total_w = 0.0
    for tf, w in weights:
        val = safe_float(changes.get(tf))
        if val is None:
            continue
        total += val * float(w)
        total_w += float(w)
    if total_w <= 0:
        return None
    return total / total_w


def score_candidate_for_model(
    c: Any,
    model_id: str,
    market_indicators: Dict[str, Dict[str, Any]],
) -> float:
    base = float(getattr(c, "score", 0.0) or 0.0)
    if model_id in {MODEL_LONG_ID, MODEL_SHORT_ID}:
        return round(base, 4)

    side = model_side_from_id(model_id)
    direction = -1.0 if side == "SHORT" else 1.0

    fast_weights = [("1h", 0.35), ("15m", 0.25), ("5m", 0.20), ("1m", 0.20)]
    swing_weights = [("24h", 0.35), ("12h", 0.25), ("6h", 0.20), ("1h", 0.20)]
    market_fast = _weighted_change(market_indicators, "market", fast_weights)
    market_swing = _weighted_change(market_indicators, "market", swing_weights)
    btc_fast = _weighted_change(market_indicators, "btc", fast_weights)
    eth_fast = _weighted_change(market_indicators, "eth", fast_weights)

    signal = 0.0
    parts = [
        (market_fast, 0.45, 2.5),
        (market_swing, 0.30, 4.0),
        (btc_fast, 0.15, 2.0),
        (eth_fast, 0.10, 2.0),
    ]
    for raw, weight, scale in parts:
        if raw is None:
            continue
        signal += float(weight) * clamp(raw / scale, -1.0, 1.0)
    signal *= direction
    adjust = 0.10 * signal

    funding = safe_float(getattr(c, "g_funding_rate", None)) or 0.0
    if side == "LONG":
        if funding < 0:
            adjust += min(0.04, (-funding) * 30.0)
        elif funding > 0.0008:
            adjust -= min(0.06, (funding - 0.0008) * 45.0)
    else:
        if funding > 0:
            adjust += min(0.04, funding * 30.0)
        elif funding < -0.0008:
            adjust -= min(0.06, ((-funding) - 0.0008) * 45.0)

    oi = safe_float(getattr(c, "g_open_interest", None))
    change24h = safe_float(getattr(c, "g_change24h_pct", None)) or 0.0
    if oi is not None and oi > 0:
        oi_score = clamp(math.log10(1.0 + oi) / 7.0, 0.0, 1.0)
        dir_momo = direction * clamp(change24h / 15.0, -1.0, 1.0)
        adjust += 0.03 * oi_score * dir_momo

    concentration = market_indicators.get("concentration", {}) or {}
    regime = str(concentration.get("regime", "balanced"))
    symbol = str(getattr(c, "symbol", "")).upper()
    top_alt = str(concentration.get("top_alt_symbol", "")).upper()
    if regime == "btc":
        if symbol == "BTC":
            adjust += 0.02
        elif symbol not in {"ETH"}:
            adjust -= 0.01
    elif regime == "eth":
        if symbol == "ETH":
            adjust += 0.02
    elif regime == "single-alt" and top_alt:
        if symbol == top_alt:
            adjust += 0.03
        elif symbol not in {"BTC", "ETH"}:
            adjust -= 0.005

    ob_signal = safe_float(getattr(c, "b_ob_signal", None))
    ob_support = safe_float(getattr(c, "b_ob_support_dist_pct", None))
    ob_resist = safe_float(getattr(c, "b_ob_resist_dist_pct", None))
    if ob_signal is not None:
        # Orderblock pressure: bid-heavy helps LONG, ask-heavy helps SHORT.
        adjust += 0.05 * direction * clamp(float(ob_signal), -1.0, 1.0)
    near_band = 0.35
    if side == "LONG":
        if ob_resist is not None and ob_resist < near_band:
            adjust -= 0.03 * (1.0 - (ob_resist / near_band))
        if ob_support is not None and ob_support < near_band:
            adjust += 0.02 * (1.0 - (ob_support / near_band))
    else:
        if ob_support is not None and ob_support < near_band:
            adjust -= 0.03 * (1.0 - (ob_support / near_band))
        if ob_resist is not None and ob_resist < near_band:
            adjust += 0.02 * (1.0 - (ob_resist / near_band))

    return round(base + clamp(adjust, -0.20, 0.20), 4)


def compute_entry_plan_fields(
    c: Any,
    side: str,
    score: float,
) -> Dict[str, Any]:
    side_u = str(side).upper()
    direction = -1.0 if side_u == "SHORT" else 1.0
    b_px = safe_float(getattr(c, "b_close_krw", None))
    g_px = safe_float(getattr(c, "g_last_price", None))
    b_rate = abs(safe_float(getattr(c, "b_rate24h", None)) or 0.0)
    g_rate = abs(safe_float(getattr(c, "g_change24h_pct", None)) or 0.0)
    vol24 = clamp((b_rate + g_rate) / 2.0, 0.5, 25.0)

    stop_pct = clamp(0.45 + (vol24 * 0.08), 0.45, 2.80)
    rr_base = clamp(1.10 + (float(score) * 1.50), 1.10, 2.60)
    target_rr_pct = stop_pct * rr_base

    # Method 1) volatility+score RR target.
    # Method 2) orderblock distance target (closest wall distance).
    # Method 3) funding/OI crowding-adjusted target.
    ob_support = safe_float(getattr(c, "b_ob_support_dist_pct", None))
    ob_resist = safe_float(getattr(c, "b_ob_resist_dist_pct", None))
    ob_dist = ob_support if side_u == "SHORT" else ob_resist
    target_ob_pct: float | None = None
    if ob_dist is not None and ob_dist > 0:
        target_ob_pct = clamp(float(ob_dist) * 0.85, 0.35, 6.00)

    funding = safe_float(getattr(c, "g_funding_rate", None)) or 0.0
    oi = safe_float(getattr(c, "g_open_interest", None)) or 0.0
    oi_norm = clamp(math.log10(1.0 + max(0.0, oi)) / 7.0, 0.0, 1.0)
    funding_mult = clamp(1.0 + (-direction * funding * 80.0), 0.80, 1.20)
    oi_mult = clamp(1.02 - (oi_norm * 0.10), 0.90, 1.05)
    target_flow_pct = clamp(target_rr_pct * funding_mult * oi_mult, 0.35, 6.00)

    weighted_terms: List[Tuple[float, float]] = [(target_rr_pct, 0.50), (target_flow_pct, 0.20)]
    if target_ob_pct is not None:
        weighted_terms.append((target_ob_pct, 0.30))
    w_sum = sum(w for _, w in weighted_terms)
    target_pct = (
        sum(v * w for v, w in weighted_terms) / max(w_sum, 1e-9)
        if weighted_terms
        else target_rr_pct
    )
    target_pct = clamp(float(target_pct), 0.35, 6.00)
    target_basis = "rr+orderblock+flow" if target_ob_pct is not None else "rr+flow"

    # Method 1) volatility baseline entry pullback.
    # Method 2) orderblock distance guided entry pullback.
    # Method 3) funding/OI crowding-adjusted entry pullback.
    entry_base_offset_pct = clamp(0.10 + (vol24 * 0.02), 0.10, 0.80)
    entry_ob_dist = ob_resist if side_u == "SHORT" else ob_support
    entry_ob_offset_pct: float | None = None
    if entry_ob_dist is not None and entry_ob_dist > 0:
        entry_ob_offset_pct = clamp(float(entry_ob_dist) * 0.75, 0.08, 1.20)
    entry_funding_mult = clamp(1.0 + (direction * funding * 60.0), 0.80, 1.25)
    entry_oi_mult = clamp(0.95 + (oi_norm * 0.20), 0.90, 1.15)
    entry_flow_offset_pct = clamp(
        entry_base_offset_pct * entry_funding_mult * entry_oi_mult,
        0.08,
        1.20,
    )
    entry_terms: List[Tuple[float, float]] = [
        (entry_base_offset_pct, 0.55),
        (entry_flow_offset_pct, 0.20),
    ]
    if entry_ob_offset_pct is not None:
        entry_terms.append((entry_ob_offset_pct, 0.25))
    entry_w_sum = sum(w for _, w in entry_terms)
    entry_offset_pct = (
        sum(v * w for v, w in entry_terms) / max(entry_w_sum, 1e-9)
        if entry_terms
        else entry_base_offset_pct
    )
    entry_offset_pct = clamp(float(entry_offset_pct), 0.08, 1.20)
    entry_basis = "vol+orderblock+flow" if entry_ob_offset_pct is not None else "vol+flow"

    def rec_entry(px: float | None) -> float | None:
        if px is None or px <= 0:
            return None
        return px * (1.0 - direction * entry_offset_pct / 100.0)

    b_reco = rec_entry(b_px)
    g_reco = rec_entry(g_px)

    def target_price(px: float | None) -> float | None:
        if px is None or px <= 0:
            return None
        return px * (1.0 + direction * target_pct / 100.0)

    target_now_b = target_price(b_px)
    target_now_g = target_price(g_px)
    target_entry_b = target_price(b_reco if b_reco and b_reco > 0 else b_px)
    target_entry_g = target_price(g_reco if g_reco and g_reco > 0 else g_px)

    ref_px = g_px if g_px and g_px > 0 else b_px
    rr_now: float | None = None
    rr_entry: float | None = None
    if ref_px and ref_px > 0:
        target_px = ref_px * (1.0 + direction * target_pct / 100.0)
        stop_px = ref_px * (1.0 - direction * stop_pct / 100.0)
        entry_px = ref_px * (1.0 - direction * entry_offset_pct / 100.0)

        reward_now = abs(target_px - ref_px) / ref_px
        risk_now = abs(ref_px - stop_px) / ref_px
        if risk_now > 1e-9:
            rr_now = reward_now / risk_now

        reward_entry = abs(target_px - entry_px) / max(entry_px, 1e-9)
        risk_entry = abs(entry_px - stop_px) / max(entry_px, 1e-9)
        if risk_entry > 1e-9:
            rr_entry = reward_entry / risk_entry

    return {
        "entry_reco_bithumb_price": None if b_reco is None else round(float(b_reco), 8),
        "entry_reco_bitget_price": None if g_reco is None else round(float(g_reco), 8),
        "entry_reco_offset_pct": round(float(entry_offset_pct), 4),
        "entry_reco_basis": entry_basis,
        "entry_reco_base_offset_pct": round(float(entry_base_offset_pct), 4),
        "entry_reco_ob_offset_pct": None
        if entry_ob_offset_pct is None
        else round(float(entry_ob_offset_pct), 4),
        "entry_reco_flow_offset_pct": round(float(entry_flow_offset_pct), 4),
        "plan_stop_pct": round(float(stop_pct), 4),
        "plan_target_pct": round(float(target_pct), 4),
        "plan_target_basis": target_basis,
        "plan_target_rr_pct": round(float(target_rr_pct), 4),
        "plan_target_ob_pct": None if target_ob_pct is None else round(float(target_ob_pct), 4),
        "plan_target_flow_pct": round(float(target_flow_pct), 4),
        "target_now_bithumb_price": None if target_now_b is None else round(float(target_now_b), 8),
        "target_now_bitget_price": None if target_now_g is None else round(float(target_now_g), 8),
        "target_entry_bithumb_price": None if target_entry_b is None else round(float(target_entry_b), 8),
        "target_entry_bitget_price": None if target_entry_g is None else round(float(target_entry_g), 8),
        "rr_now": None if rr_now is None else round(float(rr_now), 6),
        "rr_entry": None if rr_entry is None else round(float(rr_entry), 6),
    }


def maybe_expand_models(
    state: Dict[str, Any],
    model_metrics: Dict[str, Dict[str, float | int]],
    now: datetime,
    min_count: int = MODEL_EXPANSION_MIN_COUNT,
    win_rate_floor: float = MODEL_EXPANSION_WIN_RATE_FLOOR,
    cooldown_hours: int = MODEL_EXPANSION_COOLDOWN_HOURS,
) -> List[str]:
    notes: List[str] = []
    state.setdefault("model_governance_events", [])
    state["model_registry"] = sanitize_model_registry(state.get("model_registry"))
    registry = state["model_registry"]

    meta = state.setdefault("meta", {})
    cooldown_raw = meta.get("model_governance_cooldown_until")
    if cooldown_raw:
        try:
            if now < parse_iso(str(cooldown_raw)):
                return notes
        except Exception:  # noqa: BLE001
            pass

    changed = False
    for side in ("LONG", "SHORT"):
        chain = side_model_chain(registry, side)
        if not chain:
            continue
        for current_mid in chain:
            if not bool(registry.get(current_mid, {}).get("enabled", False)):
                continue
            m = model_metrics.get(current_mid, {}) or {}
            count = int(m.get("count", 0) or 0)
            win_rate = float(m.get("win_rate", 0.0) or 0.0)
            if count < max(1, int(min_count)) or win_rate >= float(win_rate_floor):
                continue

            next_mid = next_model_id(current_mid)
            if bool(registry.get(next_mid, {}).get("enabled", False)):
                continue

            registry.setdefault(next_mid, {"enabled": False, "side": side})
            registry[next_mid]["enabled"] = True
            registry[next_mid]["side"] = side
            note = (
                f"Model expansion: {model_name_from_id(current_mid)} win {win_rate * 100:.2f}% "
                f"({count} eval) < {win_rate_floor * 100:.1f}% -> {model_name_from_id(next_mid)} enabled."
            )
            notes.append(note)
            state["model_governance_events"].append(
                {
                    "id": f"model-expand-{current_mid}-{next_mid}-{int(now.timestamp())}",
                    "at": iso_z(now),
                    "side": side,
                    "from_model": current_mid,
                    "to_model": next_mid,
                    "from_count": count,
                    "from_win_rate": win_rate,
                    "floor": float(win_rate_floor),
                    "note": note,
                }
            )
            changed = True
            break

    if changed:
        meta["last_model_governance_at"] = iso_z(now)
        meta["model_governance_cooldown_until"] = iso_z(
            now + timedelta(hours=max(1, int(cooldown_hours)))
        )
    state["model_governance_events"] = state["model_governance_events"][-500:]
    state["model_registry"] = registry
    return notes


def _market_context_snapshot(market_indicators: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    concentration = market_indicators.get("concentration", {}) or {}
    return {
        "market_trend": str(market_indicators.get("market", {}).get("trend", "neutral")),
        "btc_trend": str(market_indicators.get("btc", {}).get("trend", "neutral")),
        "eth_trend": str(market_indicators.get("eth", {}).get("trend", "neutral")),
        "regime": str(concentration.get("regime", "balanced")),
        "top_alt_symbol": str(concentration.get("top_alt_symbol", "")).upper() or None,
    }


def _side_model_fit(
    candidates: List[Any],
    side: str,
    model_id: str,
    market_indicators: Dict[str, Dict[str, Any]],
    top_k: int = 5,
) -> Dict[str, Any]:
    side_u = str(side).upper()
    vals: List[float] = []
    for c in candidates:
        if str(getattr(c, "side", "LONG")).upper() != side_u:
            continue
        vals.append(float(score_candidate_for_model(c, model_id, market_indicators)))
    if not vals:
        return {"candidate_count": 0, "top_avg": None, "top_best": None}
    vals.sort(reverse=True)
    top = vals[: max(1, int(top_k))]
    return {
        "candidate_count": len(vals),
        "top_avg": sum(top) / len(top),
        "top_best": top[0],
    }


def recommend_models_for_underperformance(
    metrics: Dict[str, float],
    model_metrics: Dict[str, Dict[str, float | int]],
    market_indicators: Dict[str, Dict[str, Any]],
    candidates: List[Any],
    model_registry: Dict[str, Dict[str, Any]],
    min_count: int = MODEL_RECOMMEND_MIN_COUNT,
    win_rate_floor: float = MODEL_RECOMMEND_WIN_RATE_FLOOR,
    avg_return_floor: float = MODEL_RECOMMEND_AVG_RETURN_FLOOR,
) -> Dict[str, Any]:
    count = int(metrics.get("count", 0) or 0)
    win_rate = float(metrics.get("win_rate", 0.0) or 0.0)
    avg_return = float(metrics.get("avg_return", 0.0) or 0.0)
    underperform = count >= max(1, int(min_count)) and (
        win_rate < float(win_rate_floor) or avg_return < float(avg_return_floor)
    )
    out: Dict[str, Any] = {
        "triggered": False,
        "underperformance": underperform,
        "gate": {
            "count": count,
            "win_rate": win_rate,
            "avg_return": avg_return,
            "min_count": int(min_count),
            "win_rate_floor": float(win_rate_floor),
            "avg_return_floor": float(avg_return_floor),
        },
        "market_context": _market_context_snapshot(market_indicators),
        "recommendations": [],
        "summary": "No recommendation: performance gate not met.",
    }
    if not underperform:
        return out

    registry = sanitize_model_registry(model_registry)
    for side in ("LONG", "SHORT"):
        mids = side_model_chain(registry, side)
        side_candidates = [
            c for c in candidates if str(getattr(c, "side", "LONG")).upper() == side
        ]
        if not side_candidates:
            continue

        base_mid = model_id_from_side(side)
        rows: List[Dict[str, Any]] = []
        fit_by_model: Dict[str, Dict[str, Any]] = {}
        for mid in mids:
            fit_by_model[mid] = _side_model_fit(
                candidates=side_candidates,
                side=side,
                model_id=mid,
                market_indicators=market_indicators,
            )
        base_fit = fit_by_model.get(base_mid, {}).get("top_avg")
        for mid in mids:
            fit_row = fit_by_model.get(mid, {})
            fit_avg = fit_row.get("top_avg")
            hist = model_metrics.get(mid, {}) or {}
            hist_count = int(hist.get("count", 0) or 0)
            hist_win = float(hist.get("win_rate", 0.0) or 0.0)
            hist_avg = float(hist.get("avg_return", 0.0) or 0.0)

            reliability = 0.0
            if hist_count > 0:
                reliability = (
                    0.60 * clamp((hist_win - 0.50) / 0.20, -1.0, 1.0)
                    + 0.40 * clamp(hist_avg / 0.01, -1.0, 1.0)
                ) * min(1.0, hist_count / 60.0)

            fit_component = -9.0 if fit_avg is None else float(fit_avg)
            fit_edge_vs_base = 0.0
            if fit_avg is not None and base_fit is not None:
                fit_edge_vs_base = float(fit_avg) - float(base_fit)
            composite = fit_component + (0.06 * reliability)
            rows.append(
                {
                    "model_id": mid,
                    "fit_avg": fit_avg,
                    "fit_component": fit_component,
                    "fit_edge_vs_base": fit_edge_vs_base,
                    "composite": composite,
                    "candidate_count": int(fit_row.get("candidate_count", 0) or 0),
                    "hist_count": hist_count,
                    "hist_win_rate": hist_win,
                    "hist_avg_return": hist_avg,
                }
            )

        if not rows:
            continue
        rows.sort(
            key=lambda x: (
                float(x["composite"]),
                float(x["fit_component"]),
            ),
            reverse=True,
        )
        best = rows[0]
        base_row = next((x for x in rows if str(x.get("model_id", "")) == base_mid), None)
        # Guardrail: if a non-active model looks worse on immediate market fit, keep baseline.
        if (
            base_row is not None
            and str(best.get("model_id", "")) != base_mid
            and float(best.get("fit_edge_vs_base", 0.0) or 0.0) < -0.005
        ):
            best = base_row
        second = rows[1] if len(rows) > 1 else None
        fit_edge_vs_next = 0.0
        if (
            second is not None
            and best.get("fit_avg") is not None
            and second.get("fit_avg") is not None
        ):
            fit_edge_vs_next = float(best["fit_avg"]) - float(second["fit_avg"])
        active_side = active_model_ids(registry, side)
        action = "keep" if best["model_id"] in active_side else "enable"
        out["recommendations"].append(
            {
                "side": side,
                "suggested_model": best["model_id"],
                "suggested_label": model_name_from_id(str(best["model_id"])),
                "action": action,
                "active_models": active_side,
                "candidate_count": int(best["candidate_count"]),
                "fit_top_avg": None
                if best.get("fit_avg") is None
                else round(float(best["fit_avg"]), 4),
                "fit_edge_vs_base": round(float(best["fit_edge_vs_base"]), 4),
                "fit_edge_vs_next": round(float(fit_edge_vs_next), 4),
                "hist_count": int(best["hist_count"]),
                "hist_win_rate": round(float(best["hist_win_rate"]), 6),
                "hist_avg_return": round(float(best["hist_avg_return"]), 6),
            }
        )

    out["triggered"] = bool(out["recommendations"])
    if out["triggered"]:
        chunks: List[str] = []
        for r in out["recommendations"]:
            chunks.append(
                f"{r['side']}->{r['suggested_label']}({r['action']}, "
                f"fitΔbase {float(r['fit_edge_vs_base']):+.3f}, "
                f"hist {float(r['hist_win_rate']) * 100:.1f}%/"
                f"{float(r['hist_avg_return']) * 100:.2f}%, n={int(r['hist_count'])})"
            )
        out["summary"] = " | ".join(chunks)
    else:
        out["summary"] = "Underperformance detected but side candidates were empty."
    return out


def make_recommendations(
    candidates: List[Any],
    top_n: int,
    min_short_picks: int,
    model_registry: Dict[str, Dict[str, Any]],
    market_indicators: Dict[str, Dict[str, Any]],
    run_ts: datetime,
    horizon_min: int,
    eval_horizons_min: List[int] | None = None,
) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    top_n = max(0, int(top_n))
    min_short_picks = max(0, int(min_short_picks))
    if top_n > 0 and candidates:
        registry = sanitize_model_registry(model_registry)
        all_rows: List[Dict[str, Any]] = []
        per_model: Dict[str, List[Dict[str, Any]]] = {}
        for c in candidates:
            side = str(getattr(c, "side", "LONG")).upper()
            mids = active_model_ids(registry, side)
            if not mids:
                mids = [model_id_from_side(side)]
            for mid in mids:
                scored = score_candidate_for_model(
                    c=c,
                    model_id=mid,
                    market_indicators=market_indicators,
                )
                row = {"candidate": c, "model_id": mid, "score": scored}
                all_rows.append(row)
                per_model.setdefault(mid, []).append(row)

        for rows in per_model.values():
            rows.sort(
                key=lambda x: (
                    float(x["score"]),
                    abs(float(getattr(x["candidate"], "g_change24h_pct", 0.0) or 0.0)),
                    abs(float(getattr(x["candidate"], "b_rate24h", 0.0) or 0.0)),
                ),
                reverse=True,
            )
        all_rows.sort(
            key=lambda x: (
                float(x["score"]),
                abs(float(getattr(x["candidate"], "g_change24h_pct", 0.0) or 0.0)),
                abs(float(getattr(x["candidate"], "b_rate24h", 0.0) or 0.0)),
            ),
            reverse=True,
        )

        used: set[Tuple[str, str]] = set()

        def add_row(row: Dict[str, Any]) -> bool:
            c = row["candidate"]
            key = (
                str(getattr(c, "symbol", "")).upper(),
                str(getattr(c, "side", "LONG")).upper(),
            )
            if key in used:
                return False
            used.add(key)
            selected.append(row)
            return True

        ordered_models: List[str] = []
        for side in ("LONG", "SHORT"):
            active = set(active_model_ids(registry, side))
            path = side_model_chain(registry, side)
            ordered_models.extend([mid for mid in path if mid in active])
            for mid in sorted(active):
                if mid not in ordered_models:
                    ordered_models.append(mid)
        seed_rows: List[Dict[str, Any]] = []
        for mid in ordered_models:
            ranked = per_model.get(mid, [])
            if ranked:
                seed_rows.append(ranked[0])
        seed_rows.sort(
            key=lambda x: (
                float(x["score"]),
                abs(float(getattr(x["candidate"], "g_change24h_pct", 0.0) or 0.0)),
                abs(float(getattr(x["candidate"], "b_rate24h", 0.0) or 0.0)),
            ),
            reverse=True,
        )
        for row in seed_rows:
            if len(selected) >= top_n:
                break
            add_row(row)

        short_count = sum(
            1
            for row in selected
            if str(getattr(row["candidate"], "side", "LONG")).upper() == "SHORT"
        )
        if short_count < min_short_picks:
            for row in all_rows:
                if len(selected) >= top_n:
                    break
                if str(getattr(row["candidate"], "side", "LONG")).upper() != "SHORT":
                    continue
                if add_row(row):
                    short_count += 1
                if short_count >= min_short_picks:
                    break

        for row in all_rows:
            if len(selected) >= top_n:
                break
            add_row(row)

        selected.sort(
            key=lambda x: (
                float(x["score"]),
                abs(float(getattr(x["candidate"], "g_change24h_pct", 0.0) or 0.0)),
                abs(float(getattr(x["candidate"], "b_rate24h", 0.0) or 0.0)),
            ),
            reverse=True,
        )

    out: List[Dict[str, Any]] = []
    eval_horizons = sorted(set(int(x) for x in (eval_horizons_min or [horizon_min]) if int(x) > 0))
    if not eval_horizons:
        eval_horizons = [max(1, int(horizon_min))]
    primary_horizon = int(eval_horizons[0])
    market_changes = market_indicators.get("market", {}).get("changes", {}) or {}
    btc_changes = market_indicators.get("btc", {}).get("changes", {}) or {}
    eth_changes = market_indicators.get("eth", {}).get("changes", {}) or {}
    concentration = market_indicators.get("concentration", {}) or {}
    market_trend = str(market_indicators.get("market", {}).get("trend", "neutral"))
    btc_trend = str(market_indicators.get("btc", {}).get("trend", "neutral"))
    eth_trend = str(market_indicators.get("eth", {}).get("trend", "neutral"))
    for row in selected:
        c = row["candidate"]
        mid = str(row["model_id"])
        score = float(row["score"])
        base_score = float(getattr(c, "score", 0.0) or 0.0)
        plan = compute_entry_plan_fields(
            c=c,
            side=str(c.side),
            score=score,
        )
        out.append(
            {
                "id": f"{c.symbol}-{c.side}-{mid}-{int(run_ts.timestamp())}",
                "symbol": c.symbol,
                "side": c.side,
                "model_id": mid,
                "model_name": model_name_from_id(mid),
                "created_at": iso_z(run_ts),
                "horizon_min": primary_horizon,
                "eval_horizons_min": list(eval_horizons),
                "evaluated_horizons": [],
                "entry_bithumb_price": c.b_close_krw,
                "entry_bitget_price": c.g_last_price,
                "score": score,
                "base_score": base_score,
                "model_score_delta": score - base_score,
                "b_rate24h": c.b_rate24h,
                "g_rate24h": c.g_change24h_pct,
                "b_value24h": c.b_krw_value24h,
                "g_volume24h": c.g_usdt_volume,
                "g_funding_rate": c.g_funding_rate,
                "g_open_interest": c.g_open_interest,
                "g_symbol": c.g_symbol,
                "ob_signal": safe_float(getattr(c, "b_ob_signal", None)),
                "ob_bid_ask_ratio": safe_float(getattr(c, "b_ob_bid_ask_ratio", None)),
                "ob_support_dist_pct": safe_float(getattr(c, "b_ob_support_dist_pct", None)),
                "ob_resist_dist_pct": safe_float(getattr(c, "b_ob_resist_dist_pct", None)),
                "market_sign_market": int(market_indicators.get("market", {}).get("sign", 0)),
                "market_sign_btc": int(market_indicators.get("btc", {}).get("sign", 0)),
                "market_sign_eth": int(market_indicators.get("eth", {}).get("sign", 0)),
                "market_change_market_1h": market_changes.get("1h"),
                "market_change_market_24h": market_changes.get("24h"),
                "market_change_btc_1h": btc_changes.get("1h"),
                "market_change_btc_24h": btc_changes.get("24h"),
                "market_change_eth_1h": eth_changes.get("1h"),
                "market_change_eth_24h": eth_changes.get("24h"),
                "market_regime": str(concentration.get("regime", "balanced")),
                "market_top_alt_symbol": str(concentration.get("top_alt_symbol", "")).upper(),
                "market_trend_market": market_trend,
                "market_trend_btc": btc_trend,
                "market_trend_eth": eth_trend,
                **plan,
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
        try:
            created = parse_iso(str(p["created_at"]))
        except Exception:  # noqa: BLE001
            continue
        side = pick_side(p)
        horizons = parse_pick_eval_horizons(p)
        if not horizons:
            continue
        max_horizon = max(horizons)
        done_raw = p.get("evaluated_horizons")
        done_set: set[int] = set()
        if isinstance(done_raw, list):
            for x in done_raw:
                try:
                    done_set.add(int(x))
                except Exception:  # noqa: BLE001
                    continue

        pending_horizons = [h for h in horizons if h not in done_set]
        if not pending_horizons:
            continue
        due_horizons = [
            h
            for h in pending_horizons
            if now >= created + timedelta(minutes=int(h))
        ]
        if not due_horizons:
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
            if now < created + timedelta(minutes=max_horizon * 2):
                still_pending.append(p)
                continue
        blended, available = blend_returns(side, b_ret, g_ret)

        model_id = pick_model_id(p)
        model_name = model_name_from_id(model_id)
        now_iso = iso_z(now)
        for horizon in sorted(set(int(h) for h in due_horizons)):
            finalized.append(
                {
                    "id": f"{p['id']}@{horizon}m",
                    "pick_id": p["id"],
                    "symbol": symbol,
                    "side": side,
                    "model_id": model_id,
                    "model_name": model_name,
                    "created_at": p["created_at"],
                    "evaluated_at": now_iso,
                    "horizon_min": int(horizon),
                    "horizon_label": f"{int(horizon)}m",
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
                    "base_score": p.get("base_score"),
                    "model_score_delta": p.get("model_score_delta"),
                    "b_rate24h": p.get("b_rate24h"),
                    "g_rate24h": p.get("g_rate24h"),
                    "g_funding_rate": p.get("g_funding_rate"),
                    "g_open_interest": p.get("g_open_interest"),
                    "ob_signal": p.get("ob_signal"),
                    "ob_bid_ask_ratio": p.get("ob_bid_ask_ratio"),
                    "ob_support_dist_pct": p.get("ob_support_dist_pct"),
                    "ob_resist_dist_pct": p.get("ob_resist_dist_pct"),
                    "market_sign_market": p.get("market_sign_market"),
                    "market_sign_btc": p.get("market_sign_btc"),
                    "market_sign_eth": p.get("market_sign_eth"),
                    "market_change_market_1h": p.get("market_change_market_1h"),
                    "market_change_market_24h": p.get("market_change_market_24h"),
                    "market_change_btc_1h": p.get("market_change_btc_1h"),
                    "market_change_btc_24h": p.get("market_change_btc_24h"),
                    "market_change_eth_1h": p.get("market_change_eth_1h"),
                    "market_change_eth_24h": p.get("market_change_eth_24h"),
                    "market_regime": p.get("market_regime"),
                }
            )
            done_set.add(int(horizon))

        p["evaluated_horizons"] = sorted(done_set)
        if len(done_set) < len(horizons):
            still_pending.append(p)

    return still_pending, finalized


def missed_threshold_for_horizon(horizon_min: int) -> float:
    h = max(1, int(horizon_min))
    if h in MISSED_MOVE_THRESHOLDS:
        return float(MISSED_MOVE_THRESHOLDS[h])
    # Fallback: use nearest configured horizon.
    keys = sorted(MISSED_MOVE_THRESHOLDS.keys())
    nearest = min(keys, key=lambda x: abs(int(x) - h))
    return float(MISSED_MOVE_THRESHOLDS[nearest])


def build_missed_watch_rows(
    bithumb: Dict[str, Any],
    bitget: Dict[str, Any],
    cfg: Dict[str, float],
    blocked_symbols: set[str],
    stage_diag: Dict[str, set[Tuple[str, str]]],
    picks: List[Dict[str, Any]],
    run_ts: datetime,
    eval_horizons_min: List[int],
) -> List[Dict[str, Any]]:
    picked = {
        (str(p.get("symbol", "")).upper(), str(p.get("side", "LONG")).upper())
        for p in picks
        if str(p.get("symbol", "")).strip()
    }
    blocked = {str(x).upper() for x in blocked_symbols if str(x).strip()}
    after_blocked = set(stage_diag.get("after_blocked", set()))
    after_overheat = set(stage_diag.get("after_overheat", set()))
    after_conservative = set(stage_diag.get("after_conservative", set()))
    final_keys = set(stage_diag.get("final", set()))

    horizons = sorted(set(int(x) for x in eval_horizons_min if int(x) > 0))
    if not horizons:
        horizons = list(DEFAULT_EVAL_HORIZONS)
    created_iso = iso_z(run_ts)
    created_tag = int(run_ts.timestamp())

    rows: List[Dict[str, Any]] = []
    symbols = sorted(set(bithumb.keys()) & set(bitget.keys()))
    for sym in symbols:
        b = bithumb.get(sym)
        g = bitget.get(sym)
        if b is None or g is None:
            continue
        b_val = safe_float(getattr(b, "krw_value24h", None)) or 0.0
        g_vol = safe_float(getattr(g, "usdt_volume", None)) or 0.0
        # Keep audit focused on tradable-liquidity symbols.
        if b_val < float(cfg["min_bithumb_value"]) or g_vol < float(cfg["min_bitget_volume"]):
            continue

        b_rate = safe_float(getattr(b, "rate24h", None)) or 0.0
        b_px = safe_float(getattr(b, "close_krw", None))
        g_rate = safe_float(getattr(g, "change24h_pct", None)) or 0.0
        g_px = safe_float(getattr(g, "last_price", None))
        g_funding = safe_float(getattr(g, "funding_rate", None)) or 0.0
        g_oi = safe_float(getattr(g, "holding_amount", None))

        for side in ("LONG", "SHORT"):
            key = (str(sym).upper(), side)
            if key in picked:
                continue

            reasons: List[str] = []
            if side == "LONG":
                if b_rate < float(cfg["min_bithumb_rate"]):
                    reasons.append("long_b_rate")
                if g_rate < float(cfg["min_bitget_rate"]):
                    reasons.append("long_g_rate")
            else:
                if g_rate > -abs(float(cfg["min_bitget_short_rate"])):
                    reasons.append("short_g_rate")
                if b_rate > float(cfg["short_max_bithumb_rate"]):
                    reasons.append("short_b_rate")
                if g_funding < float(cfg["short_min_funding_rate"]):
                    reasons.append("short_funding")

            if not reasons:
                if sym in blocked:
                    reasons.append("loss_cooldown")
                elif key in after_blocked and key not in after_overheat:
                    reasons.append("overheat")
                elif key in after_overheat and key not in after_conservative:
                    if (
                        abs(b_rate) > float(cfg["conservative_max_rate"])
                        or abs(g_rate) > float(cfg["conservative_max_rate"])
                    ):
                        reasons.append("conservative_rate")
                    elif abs(g_funding) > float(cfg["conservative_max_abs_funding"]):
                        reasons.append("conservative_funding")
                    else:
                        reasons.append("conservative")
                elif key in after_conservative and key not in final_keys:
                    reasons.append("orderable_or_check_cap")
                elif key in final_keys:
                    reasons.append("rank_cut")
                else:
                    reasons.append("unknown")

            rows.append(
                {
                    "id": f"miss-{sym}-{side}-{created_tag}",
                    "symbol": str(sym).upper(),
                    "side": side,
                    "created_at": created_iso,
                    "eval_horizons_min": list(horizons),
                    "evaluated_horizons": [],
                    "entry_bithumb_price": b_px,
                    "entry_bitget_price": g_px,
                    "b_rate24h": b_rate,
                    "g_rate24h": g_rate,
                    "g_funding_rate": g_funding,
                    "g_open_interest": g_oi,
                    "reject_reasons": reasons,
                    "primary_reason": reasons[0] if reasons else "unknown",
                }
            )
    return rows


def evaluate_missed_queue(
    queue: List[Dict[str, Any]],
    bithumb: Dict[str, Any],
    bitget: Dict[str, Any],
    now: datetime,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    still_pending: List[Dict[str, Any]] = []
    finalized: List[Dict[str, Any]] = []

    for p in queue:
        try:
            created = parse_iso(str(p["created_at"]))
        except Exception:  # noqa: BLE001
            continue
        side = pick_side(p)
        horizons = parse_pick_eval_horizons(p)
        if not horizons:
            continue
        max_horizon = max(horizons)
        done_raw = p.get("evaluated_horizons")
        done_set: set[int] = set()
        if isinstance(done_raw, list):
            for x in done_raw:
                try:
                    done_set.add(int(x))
                except Exception:  # noqa: BLE001
                    continue

        pending_horizons = [h for h in horizons if h not in done_set]
        if not pending_horizons:
            continue
        due_horizons = [
            h
            for h in pending_horizons
            if now >= created + timedelta(minutes=int(h))
        ]
        if not due_horizons:
            still_pending.append(p)
            continue

        symbol = str(p.get("symbol", "")).upper()
        b_now = bithumb.get(symbol)
        g_now = bitget.get(symbol)

        b_market_ret = None
        g_market_ret = None
        b_entry = safe_float(p.get("entry_bithumb_price"))
        g_entry = safe_float(p.get("entry_bitget_price"))
        if b_now and b_entry is not None and b_entry > 0:
            b_market_ret = (b_now.close_krw - b_entry) / b_entry
        if g_now and g_entry is not None and g_entry > 0:
            g_market_ret = (g_now.last_price - g_entry) / g_entry

        b_ret = trade_return_from_market_return(b_market_ret, side)
        g_ret = trade_return_from_market_return(g_market_ret, side)
        if b_ret is None and g_ret is None:
            if now < created + timedelta(minutes=max_horizon * 2):
                still_pending.append(p)
                continue
        blended, available = blend_returns(side, b_ret, g_ret)
        now_iso = iso_z(now)
        for horizon in sorted(set(int(h) for h in due_horizons)):
            threshold = missed_threshold_for_horizon(horizon)
            finalized.append(
                {
                    "id": f"{p.get('id', 'miss')}@{horizon}m",
                    "watch_id": p.get("id"),
                    "symbol": symbol,
                    "side": side,
                    "created_at": p.get("created_at"),
                    "evaluated_at": now_iso,
                    "horizon_min": int(horizon),
                    "entry_bithumb_price": p.get("entry_bithumb_price"),
                    "entry_bitget_price": p.get("entry_bitget_price"),
                    "exit_bithumb_price": b_now.close_krw if b_now else None,
                    "exit_bitget_price": g_now.last_price if g_now else None,
                    "return_bithumb": b_ret,
                    "return_bitget": g_ret,
                    "return_blended": blended,
                    "available_legs": available,
                    "missed_threshold": threshold,
                    "missed": blended >= threshold,
                    "primary_reason": p.get("primary_reason"),
                    "reject_reasons": p.get("reject_reasons", []),
                    "b_rate24h": p.get("b_rate24h"),
                    "g_rate24h": p.get("g_rate24h"),
                    "g_funding_rate": p.get("g_funding_rate"),
                    "g_open_interest": p.get("g_open_interest"),
                }
            )
            done_set.add(int(horizon))

        p["evaluated_horizons"] = sorted(done_set)
        if len(done_set) < len(horizons):
            still_pending.append(p)

    return still_pending, finalized


def summarize_missed_evaluations(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "evaluated": len(rows),
        "missed": 0,
        "top_reasons": [],
        "samples": [],
    }
    flagged = [r for r in rows if bool(r.get("missed"))]
    out["missed"] = len(flagged)
    if not flagged:
        return out
    bucket: Dict[str, int] = {}
    for r in flagged:
        reason = str(r.get("primary_reason", "unknown"))
        bucket[reason] = bucket.get(reason, 0) + 1
    top = sorted(bucket.items(), key=lambda x: x[1], reverse=True)
    out["top_reasons"] = [{"reason": k, "count": v} for k, v in top[:4]]

    samples: List[str] = []
    for r in flagged[:3]:
        sym = str(r.get("symbol", "-"))
        side = str(r.get("side", "-")).upper()
        h = int(safe_float(r.get("horizon_min")) or 0)
        ret = float(safe_float(r.get("return_blended")) or 0.0)
        reason = str(r.get("primary_reason", "unknown"))
        samples.append(f"{sym}:{side}@{h}m {ret * 100:.2f}% ({reason})")
    out["samples"] = samples
    return out


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


def _bucket_stats(
    rows: List[Dict[str, Any]],
    bucket_fn: Any,
) -> Dict[str, Dict[str, float | int]]:
    agg: Dict[str, Dict[str, float | int]] = {}
    for r in rows:
        bucket = str(bucket_fn(r) or "").strip()
        if not bucket:
            continue
        cur = agg.setdefault(bucket, {"count": 0, "wins": 0, "sum_return": 0.0})
        cur["count"] = int(cur["count"]) + 1
        if bool(r.get("win")):
            cur["wins"] = int(cur["wins"]) + 1
        ret = safe_float(r.get("return_blended"))
        cur["sum_return"] = float(cur["sum_return"]) + (0.0 if ret is None else float(ret))
    out: Dict[str, Dict[str, float | int]] = {}
    for bucket, cur in agg.items():
        cnt = int(cur.get("count", 0) or 0)
        wins = int(cur.get("wins", 0) or 0)
        total = float(cur.get("sum_return", 0.0) or 0.0)
        out[bucket] = {
            "count": cnt,
            "win_rate": (wins / cnt) if cnt > 0 else 0.0,
            "avg_return": (total / cnt) if cnt > 0 else 0.0,
        }
    return out


def _pick_weak_bucket(
    stats: Dict[str, Dict[str, float | int]],
    min_bucket_count: int,
) -> Tuple[str, Dict[str, float | int]] | None:
    rows = []
    for bucket, cur in stats.items():
        cnt = int(cur.get("count", 0) or 0)
        if cnt < max(1, int(min_bucket_count)):
            continue
        rows.append((bucket, cur))
    if not rows:
        return None
    rows.sort(
        key=lambda x: (
            float(x[1].get("win_rate", 0.0) or 0.0),
            float(x[1].get("avg_return", 0.0) or 0.0),
            -int(x[1].get("count", 0) or 0),
        )
    )
    return rows[0]


def _proposal_from_issue(side: str, dim: str, bucket: str) -> str:
    side_u = str(side).upper()
    if dim == "alignment":
        if bucket == "inverse":
            return "시장 역행 구간 진입 패널티를 강화합니다."
        if bucket == "aligned":
            return "정방향 구간에서 점수 가중치를 높여 정합성 종목을 우선합니다."
        return "중립장 구간 전용 게이트를 분리합니다."
    if dim == "funding":
        if "crowded" in bucket:
            return "혼잡(funding crowding) 구간 회피 조건을 강화합니다."
        if "contrarian" in bucket:
            return "역발상 funding 보너스/패널티를 사이드별로 재조정합니다."
        return "funding 중립 구간의 노이즈 필터를 강화합니다."
    if dim == "momentum":
        if bucket == "high-momentum":
            return "과열 구간 상한(overheat/conservative)을 더 보수적으로 조정합니다."
        if bucket == "low-momentum":
            return "최소 모멘텀 바닥값을 상향해 약한 추세를 제외합니다."
        return "중간 모멘텀 구간에서 변동성 대비 손익비를 재조정합니다."
    if dim == "open_interest":
        if bucket == "low-oi":
            return "OI 하한을 높여 체결 신뢰도가 낮은 종목을 제외합니다."
        if bucket == "high-oi":
            return "고 OI 과밀 구간에서 추격 진입 패널티를 강화합니다."
        return "OI 구간별 스코어 기여도를 재조정합니다."
    if dim == "regime":
        if bucket == "btc":
            return "BTC 주도장 전용 가중치를 별도 모델에 분리합니다."
        if bucket == "eth":
            return "ETH 주도장 전용 가중치를 별도 모델에 분리합니다."
        if bucket == "single-alt":
            return "단일 알트 쏠림장 전용 가중치를 별도 모델에 분리합니다."
        if bucket == "alt-broad":
            return "광범위 알트장 전용 가중치를 별도 모델에 분리합니다."
        return "균형장 전용 기준선을 별도로 튜닝합니다."
    if side_u == "SHORT":
        return "SHORT 전용 리스크 컷(손절폭/진입 오프셋/펀딩 필터)을 강화합니다."
    return "LONG 전용 리스크 컷(손절폭/진입 오프셋/모멘텀 필터)을 강화합니다."


def diagnose_underperforming_models(
    results: List[Dict[str, Any]],
    model_metrics: Dict[str, Dict[str, float | int]],
    min_count: int = MODEL_DIAG_MIN_COUNT,
    win_rate_floor: float = MODEL_RECOMMEND_WIN_RATE_FLOOR,
    avg_return_floor: float = MODEL_RECOMMEND_AVG_RETURN_FLOOR,
    min_bucket_count: int = MODEL_DIAG_MIN_BUCKET,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "triggered": False,
        "items": [],
        "gate": {
            "min_count": int(min_count),
            "win_rate_floor": float(win_rate_floor),
            "avg_return_floor": float(avg_return_floor),
            "min_bucket_count": int(min_bucket_count),
        },
    }
    if not results or not model_metrics:
        return out

    for mid, mm in model_metrics.items():
        count = int(mm.get("count", 0) or 0)
        win_rate = float(mm.get("win_rate", 0.0) or 0.0)
        avg_return = float(mm.get("avg_return", 0.0) or 0.0)
        if count < max(1, int(min_count)):
            continue
        if win_rate >= float(win_rate_floor) and avg_return >= float(avg_return_floor):
            continue

        side = model_side_from_id(mid)
        rows = [r for r in results if pick_model_id(r) == mid]
        rows = rows[-max(120, count):]
        if not rows:
            continue

        def alignment_bucket(r: Dict[str, Any]) -> str:
            m_sign = int(safe_float(r.get("market_sign_market")) or 0)
            if m_sign == 0:
                return "neutral-market"
            side_v = side_sign(pick_side(r))
            rel = side_v * m_sign
            if rel > 0:
                return "aligned"
            if rel < 0:
                return "inverse"
            return "neutral-market"

        def funding_bucket(r: Dict[str, Any]) -> str:
            fr = safe_float(r.get("g_funding_rate"))
            if fr is None:
                return "unknown"
            if side == "LONG":
                if fr >= 0.0010:
                    return "crowded-long"
                if fr <= -0.0003:
                    return "contrarian-long"
            else:
                if fr <= -0.0010:
                    return "crowded-short"
                if fr >= 0.0003:
                    return "contrarian-short"
            return "neutral-funding"

        def momentum_bucket(r: Dict[str, Any]) -> str:
            g_rate = abs(safe_float(r.get("g_rate24h")) or 0.0)
            if g_rate >= 6.0:
                return "high-momentum"
            if g_rate <= 1.5:
                return "low-momentum"
            return "mid-momentum"

        def oi_bucket(r: Dict[str, Any]) -> str:
            oi = safe_float(r.get("g_open_interest"))
            if oi is None:
                return "unknown"
            if oi < 100_000:
                return "low-oi"
            if oi >= 1_000_000:
                return "high-oi"
            return "mid-oi"

        def regime_bucket(r: Dict[str, Any]) -> str:
            raw = str(r.get("market_regime", "")).strip()
            return raw or "unknown"

        dims = [
            ("alignment", _bucket_stats(rows, alignment_bucket)),
            ("funding", _bucket_stats(rows, funding_bucket)),
            ("momentum", _bucket_stats(rows, momentum_bucket)),
            ("open_interest", _bucket_stats(rows, oi_bucket)),
            ("regime", _bucket_stats(rows, regime_bucket)),
        ]

        issues: List[Dict[str, Any]] = []
        proposals: List[str] = []
        for dim, stats in dims:
            weak = _pick_weak_bucket(stats, min_bucket_count=min_bucket_count)
            if not weak:
                continue
            bucket, cur = weak
            issue = {
                "dimension": dim,
                "bucket": bucket,
                "count": int(cur.get("count", 0) or 0),
                "win_rate": float(cur.get("win_rate", 0.0) or 0.0),
                "avg_return": float(cur.get("avg_return", 0.0) or 0.0),
            }
            issues.append(issue)
            proposal = _proposal_from_issue(side=side, dim=dim, bucket=bucket)
            if proposal not in proposals:
                proposals.append(proposal)

        if not issues:
            proposals = ["저성과 원인 표본이 부족해 추가 데이터 확보 후 재진단이 필요합니다."]

        nxt = next_model_id(mid)
        summary_parts: List[str] = []
        for issue in issues[:2]:
            summary_parts.append(
                f"{issue['dimension']}:{issue['bucket']}(n={issue['count']}, win={issue['win_rate'] * 100:.1f}%)"
            )
        summary = " | ".join(summary_parts) if summary_parts else "no-bucket-signal"

        out["items"].append(
            {
                "model_id": str(mid),
                "model_label": model_name_from_id(str(mid)),
                "side": side,
                "count": count,
                "win_rate": win_rate,
                "avg_return": avg_return,
                "next_model_id": nxt,
                "next_model_label": model_name_from_id(nxt),
                "issues": issues[:4],
                "proposals": proposals[:4],
                "summary": summary,
            }
        )

    out["triggered"] = bool(out["items"])
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


def prune_loss_cooldowns(
    raw: Dict[str, Any],
    now: datetime,
) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (raw or {}).items():
        sym = str(k).upper().strip()
        if not sym:
            continue
        try:
            until = parse_iso(str(v))
        except Exception:  # noqa: BLE001
            continue
        if now < until:
            out[sym] = iso_z(until)
    return out


def merge_loss_cooldowns(
    meta: Dict[str, Any],
    alerts: List[Dict[str, Any]],
    now: datetime,
    cooldown_min: int,
) -> Dict[str, str]:
    cur = prune_loss_cooldowns(meta.get("loss_cooldowns", {}), now)
    mins = max(1, int(cooldown_min))
    for a in alerts:
        sym = str(a.get("symbol", "")).upper().strip()
        if not sym:
            continue
        new_until = now + timedelta(minutes=mins)
        old = cur.get(sym)
        if old:
            try:
                old_dt = parse_iso(old)
                if old_dt > new_until:
                    new_until = old_dt
            except Exception:  # noqa: BLE001
                pass
        cur[sym] = iso_z(new_until)
    meta["loss_cooldowns"] = cur
    return cur


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


def format_model_hint_line(model_recommendation: Dict[str, Any] | None) -> str | None:
    if not model_recommendation or not bool(model_recommendation.get("triggered")):
        return None
    recs = model_recommendation.get("recommendations", []) or []
    parts: List[str] = []
    for r in recs:
        mid = str(r.get("suggested_model", "")).strip()
        if not mid:
            continue
        side = str(r.get("side", "?")).upper()
        action = str(r.get("action", "keep")).lower()
        fit_edge = r.get("fit_edge_vs_base")
        hist_win = r.get("hist_win_rate")
        hist_avg = r.get("hist_avg_return")
        hist_count = int(r.get("hist_count", 0) or 0)
        fit_txt = "n/a" if fit_edge is None else f"{float(fit_edge):+.3f}"
        wr_txt = "n/a" if hist_win is None else f"{float(hist_win) * 100:.1f}%"
        ar_txt = "n/a" if hist_avg is None else f"{float(hist_avg) * 100:.2f}%"
        parts.append(
            f"{side}:{model_name_from_id(mid)}({action}, fitΔ {fit_txt}, hist {wr_txt}/{ar_txt}, n={hist_count})"
        )
    if not parts:
        return None
    regime = str((model_recommendation.get("market_context", {}) or {}).get("regime", "balanced"))
    return "ModelHint: underperform -> " + " | ".join(parts) + f" | regime={regime}"


def format_model_lab_line(model_diagnostics: Dict[str, Any] | None) -> str | None:
    if not model_diagnostics or not bool(model_diagnostics.get("triggered")):
        return None
    rows = model_diagnostics.get("items", []) or []
    parts: List[str] = []
    for item in rows[:2]:
        side = str(item.get("side", "?")).upper()
        mid = str(item.get("model_id", "")).strip()
        nxt = str(item.get("next_model_id", "")).strip()
        summary = str(item.get("summary", "")).strip()
        if not side or not mid:
            continue
        bit = f"{side}:{model_name_from_id(mid)}"
        if summary:
            bit += f" weak[{summary}]"
        if nxt:
            bit += f" -> {nxt}"
        parts.append(bit)
    if not parts:
        return None
    return "ModelLab: " + " | ".join(parts)


def make_message(
    run_ts: datetime,
    picks: List[Dict[str, Any]],
    metrics: Dict[str, float],
    filter_stats: Dict[str, int],
    cfg: Dict[str, float],
    new_results_count: int,
    calibrate_notes: List[str],
    model_governance_notes: List[str],
    model_recommendation: Dict[str, Any] | None,
    model_diagnostics: Dict[str, Any] | None,
    missed_summary: Dict[str, Any] | None,
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
        if missed_summary and int(missed_summary.get("evaluated", 0) or 0) > 0:
            top_reasons = missed_summary.get("top_reasons", []) or []
            top_txt = ", ".join(
                f"{str(x.get('reason', 'unknown'))}={int(x.get('count', 0) or 0)}"
                for x in top_reasons[:2]
            )
            if not top_txt:
                top_txt = "none"
            lines.append(
                f"MissedAudit: eval {int(missed_summary.get('evaluated', 0) or 0)} | missed {int(missed_summary.get('missed', 0) or 0)} | top {top_txt}"
            )
        if int(filter_stats.get("removed_loss_cooldown", 0)) > 0:
            lines.append(f"Risk block: cooldown filtered {int(filter_stats['removed_loss_cooldown'])} symbols")
        ob_checked = int(filter_stats.get("orderblock_checked", 0) or 0)
        ob_assigned = int(filter_stats.get("orderblock_assigned", 0) or 0)
        if ob_checked > 0:
            lines.append(f"Orderblock: assigned {ob_assigned}/{ob_checked}")
        if calibrate_notes:
            lines.append("Tune: " + "; ".join(calibrate_notes))
        if model_governance_notes:
            lines.append("ModelOps: " + "; ".join(model_governance_notes))
        hint_line = format_model_hint_line(model_recommendation)
        if hint_line:
            lines.append(hint_line)
        lab_line = format_model_lab_line(model_diagnostics)
        if lab_line:
            lines.append(lab_line)
        lines.append(f"Dashboard: {DASHBOARD_URL}")
        return "\n".join(lines)

    lines.append(f"Momentum Scan | {ts_kst}")
    lines.append("Market: Bithumb Spot + Bitget USDT-M")
    lines.append(
        f"Filters: overheat<{cfg['max_overheat_rate']:.2f}%, gLong>={cfg['min_bitget_rate']:.2f}%, gShort<=-{cfg['min_bitget_short_rate']:.2f}%, bShort<={cfg['short_max_bithumb_rate']:.2f}%, fShort>={cfg['short_min_funding_rate']:.4f}, bValue>={format_money_k(cfg['min_bithumb_value'])}, gVol>={format_money_u(cfg['min_bitget_volume'])}"
    )
    lines.append(
        f"Candidates: base={filter_stats['base_universe']}, removed(cooldown={filter_stats.get('removed_loss_cooldown', 0)}, overheat={filter_stats['removed_overheat']}, conservative={filter_stats['removed_conservative']}, orderable={filter_stats['removed_orderable']})"
    )
    if int(filter_stats.get("orderblock_checked", 0) or 0) > 0:
        lines.append(
            f"Orderblock: assigned={int(filter_stats.get('orderblock_assigned', 0) or 0)}/{int(filter_stats.get('orderblock_checked', 0) or 0)}"
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
    if missed_summary and int(missed_summary.get("evaluated", 0) or 0) > 0:
        lines.append(
            f"Missed audit: evaluated {int(missed_summary.get('evaluated', 0) or 0)}, missed {int(missed_summary.get('missed', 0) or 0)}"
        )
        top_reasons = missed_summary.get("top_reasons", []) or []
        if top_reasons:
            lines.append(
                "Missed top reasons: "
                + ", ".join(
                    f"{str(x.get('reason', 'unknown'))}={int(x.get('count', 0) or 0)}"
                    for x in top_reasons[:4]
                )
            )
    if calibrate_notes:
        lines.append("Auto-calibration:")
        lines.extend(f"- {n}" for n in calibrate_notes)
    if model_governance_notes:
        lines.append("Model governance:")
        lines.extend(f"- {n}" for n in model_governance_notes)
    hint_line = format_model_hint_line(model_recommendation)
    if hint_line:
        lines.append(hint_line)
    lab_line = format_model_lab_line(model_diagnostics)
    if lab_line:
        lines.append(lab_line)
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
    state["model_registry"] = sanitize_model_registry(state.get("model_registry"))
    model_governance_notes = maybe_expand_models(
        state=state,
        model_metrics=compute_model_metrics(
            state.get("results", []),
            window=max(120, args.metric_window * 2),
        ),
        now=run_ts,
    )

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

    # 1) Detect loss alerts on existing pending first, then block those symbols for new picks.
    state["meta"]["loss_cooldowns"] = prune_loss_cooldowns(
        state["meta"].get("loss_cooldowns", {}),
        run_ts,
    )
    existing_pending = list(state["pending"])
    pre_loss_alerts = detect_loss_alerts(
        pending=existing_pending,
        bithumb=bithumb,
        bitget=bitget,
        now=run_ts,
        threshold=args.loss_alert_threshold,
    )
    if pre_loss_alerts:
        merge_loss_cooldowns(
            meta=state["meta"],
            alerts=pre_loss_alerts,
            now=run_ts,
            cooldown_min=args.loss_cooldown_min,
        )
    blocked_symbols = set(state["meta"].get("loss_cooldowns", {}).keys())

    candidates, filter_stats, candidate_stage_diag = compute_candidates(
        bithumb=bithumb,
        bitget=bitget,
        cfg=cfg,
        orderbook_timeout_sec=args.orderbook_timeout_sec,
        max_orderbook_checks=args.max_orderbook_checks,
        blocked_symbols=blocked_symbols,
        orderblock_timeout_sec=args.orderblock_timeout_sec,
        max_orderblock_checks=args.max_orderblock_checks,
    )

    picks = make_recommendations(
        candidates=candidates,
        top_n=args.top,
        min_short_picks=args.min_short_picks,
        model_registry=state["model_registry"],
        market_indicators=market_indicators,
        run_ts=run_ts,
        horizon_min=args.horizon_min,
        eval_horizons_min=args.eval_horizons_min,
    )
    if picks:
        state["recommendation_history"].extend(picks)
        state["recommendation_history"] = state["recommendation_history"][-5000:]

    enrich_recommendations_with_market_signs(
        recommendation_history=state["recommendation_history"],
        run_history=state["run_history"],
    )

    pending = existing_pending + picks
    pending, finalized = evaluate_pending(
        pending=pending,
        bithumb=bithumb,
        bitget=bitget,
        now=run_ts,
    )
    post_loss_alerts = detect_loss_alerts(
        pending=pending,
        bithumb=bithumb,
        bitget=bitget,
        now=run_ts,
        threshold=args.loss_alert_threshold,
    )
    if post_loss_alerts:
        merge_loss_cooldowns(
            meta=state["meta"],
            alerts=post_loss_alerts,
            now=run_ts,
            cooldown_min=args.loss_cooldown_min,
        )
    loss_alerts = pre_loss_alerts + post_loss_alerts
    state["pending"] = pending
    if finalized:
        state["results"].extend(finalized)
        # Keep state bounded.
        state["results"] = state["results"][-3000:]

    # Missed-opportunity audit: queue non-picked liquid symbols, then evaluate due rows.
    state.setdefault("missed_queue", [])
    state.setdefault("missed_results", [])
    new_watch_rows = build_missed_watch_rows(
        bithumb=bithumb,
        bitget=bitget,
        cfg=cfg,
        blocked_symbols=blocked_symbols,
        stage_diag=candidate_stage_diag,
        picks=picks,
        run_ts=run_ts,
        eval_horizons_min=list(args.eval_horizons_min),
    )
    if new_watch_rows:
        state["missed_queue"].extend(new_watch_rows)
        state["missed_queue"] = state["missed_queue"][-8000:]
    missed_queue, missed_evals = evaluate_missed_queue(
        queue=state.get("missed_queue", []),
        bithumb=bithumb,
        bitget=bitget,
        now=run_ts,
    )
    state["missed_queue"] = missed_queue
    missed_flagged = [r for r in missed_evals if bool(r.get("missed"))]
    if missed_flagged:
        state["missed_results"].extend(missed_flagged)
        state["missed_results"] = state["missed_results"][-3000:]
    missed_summary = summarize_missed_evaluations(missed_evals)

    if picks:
        state["meta"]["no_candidate_streak"] = 0
    else:
        state["meta"]["no_candidate_streak"] = int(state["meta"]["no_candidate_streak"]) + 1

    metrics = compute_metrics(state["results"], window=args.metric_window)
    model_metrics = compute_model_metrics(state["results"], window=max(120, args.metric_window * 2))
    model_recommendation = recommend_models_for_underperformance(
        metrics=metrics,
        model_metrics=model_metrics,
        market_indicators=market_indicators,
        candidates=candidates,
        model_registry=state.get("model_registry", {}),
    )
    model_diagnostics = diagnose_underperforming_models(
        results=state["results"],
        model_metrics=model_metrics,
    )
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
        model_governance_notes=model_governance_notes,
        model_recommendation=model_recommendation,
        model_diagnostics=model_diagnostics,
        missed_summary=missed_summary,
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
            "model_registry": dict(state.get("model_registry", {})),
            "active_models": active_models(state.get("model_registry", {})),
            "calibrated": calibrated,
            "calibration_notes": calibrate_notes,
            "model_governance_notes": model_governance_notes,
            "loss_alert_count": len(loss_alerts),
            "loss_cooldown_symbols": len(state["meta"].get("loss_cooldowns", {})),
            "market_indicators": market_indicators,
            "market_alignment_now": alignment_now,
            "market_alignment_history": alignment_history,
            "model_metrics": model_metrics,
            "model_recommendation": model_recommendation,
            "model_diagnostics": model_diagnostics,
            "missed_audit": missed_summary,
        }
    )
    state["run_history"] = state["run_history"][-5000:]
    state.setdefault("meta", {})["last_model_recommendation"] = model_recommendation
    state["meta"]["last_model_diagnostics"] = model_diagnostics
    state["meta"]["last_run_at"] = iso_z(run_ts)
    return 0


def run_alerts_only_cycle(args: argparse.Namespace, state: Dict[str, Any]) -> int:
    run_ts = utc_now()
    try:
        bithumb, bitget, _ = fetch_market_snapshot()
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] market fetch failed (alerts-only): {exc}")
        return 1

    state["meta"]["loss_cooldowns"] = prune_loss_cooldowns(
        state["meta"].get("loss_cooldowns", {}),
        run_ts,
    )
    existing_pending = list(state["pending"])
    pre_loss_alerts = detect_loss_alerts(
        pending=existing_pending,
        bithumb=bithumb,
        bitget=bitget,
        now=run_ts,
        threshold=args.loss_alert_threshold,
    )
    if pre_loss_alerts:
        merge_loss_cooldowns(
            meta=state["meta"],
            alerts=pre_loss_alerts,
            now=run_ts,
            cooldown_min=args.loss_cooldown_min,
        )

    pending, finalized = evaluate_pending(
        pending=existing_pending,
        bithumb=bithumb,
        bitget=bitget,
        now=run_ts,
    )
    post_loss_alerts = detect_loss_alerts(
        pending=pending,
        bithumb=bithumb,
        bitget=bitget,
        now=run_ts,
        threshold=args.loss_alert_threshold,
    )
    if post_loss_alerts:
        merge_loss_cooldowns(
            meta=state["meta"],
            alerts=post_loss_alerts,
            now=run_ts,
            cooldown_min=args.loss_cooldown_min,
        )

    loss_alerts = pre_loss_alerts + post_loss_alerts
    state["pending"] = pending
    if finalized:
        state["results"].extend(finalized)
        state["results"] = state["results"][-3000:]

    if loss_alerts:
        alert_msg = make_loss_alert_message(run_ts, loss_alerts)
        print(alert_msg)
    else:
        print(f"[INFO] alerts-only: no loss alert | pending={len(state['pending'])}")

    if not args.dry_run and loss_alerts:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if not token or not chat_id:
            print("[ERROR] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID is not set")
            return 2
        try:
            send_telegram(
                token=token,
                chat_id=chat_id,
                text=make_loss_alert_message(run_ts, loss_alerts),
            )
            print(f"[INFO] loss alert sent ({len(loss_alerts)})")
        except Exception as exc:  # noqa: BLE001
            print(f"[ERROR] telegram failed: {exc}")
            return 3

    state["meta"]["last_alert_watch_at"] = iso_z(run_ts)
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
    p.add_argument(
        "--eval-horizons-min",
        default="5,15,30,60",
        help="Comma-separated evaluation horizons in minutes (e.g. 5,15,30,60)",
    )
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
    p.add_argument("--orderblock-timeout-sec", type=int, default=6)
    p.add_argument("--max-orderblock-checks", type=int, default=20)
    p.add_argument("--loss-alert-threshold", type=float, default=0.0)
    p.add_argument("--loss-cooldown-min", type=int, default=60)
    p.add_argument("--min-short-picks", type=int, default=1)
    p.add_argument("--alerts-only", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    args.eval_horizons_min = parse_eval_horizons(
        raw=getattr(args, "eval_horizons_min", ""),
        fallback_horizon=int(args.horizon_min),
    )
    args.horizon_min = int(args.eval_horizons_min[0]) if args.eval_horizons_min else int(args.horizon_min)
    state_path = Path(args.state_file)
    history_path = Path(args.history_file)
    state = load_state(state_path)

    last_rc = 0
    cycle = 0
    while True:
        cycle += 1
        print(f"\n=== cycle {cycle} ===")
        pre_result_len = len(state["results"])
        if args.alerts_only:
            rc = run_alerts_only_cycle(args, state)
        else:
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
