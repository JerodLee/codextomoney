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
import re
import shutil
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
MODEL_LONG_V3_ID = "momentum_long_v3"
MODEL_SHORT_V3_ID = "momentum_short_v3"
MODEL_NAMES = {
    MODEL_LONG_ID: "롱 모멘텀 v1",
    MODEL_SHORT_ID: "숏 모멘텀 v1",
    MODEL_LONG_V2_ID: "롱 모멘텀 v2(시장보강)",
    MODEL_SHORT_V2_ID: "숏 모멘텀 v2(시장보강)",
    MODEL_LONG_V3_ID: "롱 스윙 v3(수익확장/보유형)",
    MODEL_SHORT_V3_ID: "숏 스윙 v3(수익확장/보유형)",
}
DEFAULT_MODEL_REGISTRY: Dict[str, Dict[str, Any]] = {
    MODEL_LONG_ID: {"enabled": True, "side": "LONG"},
    MODEL_SHORT_ID: {"enabled": True, "side": "SHORT"},
    MODEL_LONG_V2_ID: {"enabled": False, "side": "LONG"},
    MODEL_SHORT_V2_ID: {"enabled": False, "side": "SHORT"},
    MODEL_LONG_V3_ID: {"enabled": True, "side": "LONG"},
    MODEL_SHORT_V3_ID: {"enabled": False, "side": "SHORT"},
}
MODEL_EVOLUTION_PATH: Dict[str, List[str]] = {
    "LONG": [MODEL_LONG_ID, MODEL_LONG_V2_ID, MODEL_LONG_V3_ID],
    "SHORT": [MODEL_SHORT_ID, MODEL_SHORT_V2_ID, MODEL_SHORT_V3_ID],
}
MODEL_VERSIONS_BY_SIDE: Dict[str, Tuple[str, str, str]] = {
    "LONG": (MODEL_LONG_ID, MODEL_LONG_V2_ID, MODEL_LONG_V3_ID),
    "SHORT": (MODEL_SHORT_ID, MODEL_SHORT_V2_ID, MODEL_SHORT_V3_ID),
}
MODEL_EXPANSION_MIN_COUNT = 24
MODEL_EXPANSION_WIN_RATE_FLOOR = 0.45
MODEL_EXPANSION_COOLDOWN_HOURS = 6
MODEL_RECOMMEND_MIN_COUNT = 24
MODEL_RECOMMEND_WIN_RATE_FLOOR = 0.48
MODEL_RECOMMEND_AVG_RETURN_FLOOR = -0.001
MODEL_DIAG_MIN_COUNT = 24
MODEL_DIAG_MIN_BUCKET = 8
MODEL_V2_MIGRATION_MIN_COUNT = 36
MODEL_V2_MIGRATION_WIN_GAP = 0.05
MODEL_V2_MIGRATION_AVG_GAP = 0.0010
DEFAULT_EXECUTION_PROFILE = 1
EXECUTION_PROFILE_RULES: Dict[int, Dict[str, Any]] = {
    1: {
        "name": "conservative",
        "min_target_pct": 0.90,
        "min_rr_entry": 1.35,
        "min_setup_quality": 0.58,
        "min_edge_pct": 0.12,
    },
    2: {
        "name": "balanced",
        "min_target_pct": 0.70,
        "min_rr_entry": 1.25,
        "min_setup_quality": 0.52,
        "min_edge_pct": 0.05,
    },
    3: {
        "name": "aggressive",
        "min_target_pct": 0.50,
        "min_rr_entry": 1.15,
        "min_setup_quality": 0.46,
        "min_edge_pct": -0.03,
    },
}
DAILY_REVIEW_INTERVAL_HOURS = 24
DAILY_REVIEW_LOOKBACK_HOURS = 24
DAILY_REVIEW_MIN_RESULTS = 10
DAILY_REVIEW_MIN_MODEL_RESULTS = 8
DEFAULT_EVAL_HORIZONS = [5, 15, 30, 60]
MISSED_MOVE_THRESHOLDS = {
    5: 0.015,
    15: 0.025,
    30: 0.035,
    60: 0.050,
}
DEFAULT_BITHUMB_FEE_BPS = 4.0
DEFAULT_BITGET_FEE_BPS = 6.0
DEFAULT_BITHUMB_SLIPPAGE_BPS = 4.0
DEFAULT_BITGET_SLIPPAGE_BPS = 5.0
DEFAULT_RISK_MAX_DAILY_LOSS_PCT = 3.0
DEFAULT_RISK_MAX_CONSECUTIVE_LOSSES = 5
DEFAULT_RISK_COOLDOWN_MIN = 120
RISK_LOOKBACK_HOURS = 24
WEEKLY_AB_LOOKBACK_HOURS = 24 * 7
WEEKLY_AB_INTERVAL_HOURS = 24
WEEKLY_AB_MIN_RESULTS = 30
WEEKLY_AB_MIN_PROFILE_RESULTS = 18
WEEKLY_AB_MIN_MODEL_RESULTS = 14
SOCIAL_BUZZ_HISTORY_KEEP = 2000
SOCIAL_BUZZ_TOP_KEEP = 12
SOCIAL_DEFAULT_SYMBOLS: Tuple[str, ...] = (
    "BTC",
    "ETH",
    "SOL",
    "XRP",
    "BNB",
    "DOGE",
    "ADA",
    "TRX",
    "LINK",
    "TAO",
)
MAJOR_BARE_SYMBOLS = {
    "BTC",
    "ETH",
    "SOL",
    "XRP",
    "BNB",
    "DOGE",
    "ADA",
    "TRX",
    "LINK",
    "TAO",
}
X_RECENT_SEARCH_URL = os.getenv(
    "X_RECENT_SEARCH_URL",
    "https://api.x.com/2/tweets/search/recent",
)
THREADS_GRAPH_BASE_URL = os.getenv(
    "THREADS_GRAPH_BASE_URL",
    "https://graph.threads.net/v1.0",
)


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


def norm01(v: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return clamp((float(v) - float(lo)) / (float(hi) - float(lo)), 0.0, 1.0)


def bps_to_return(bps: float) -> float:
    return float(bps) / 10_000.0


def apply_roundtrip_cost(
    gross_return: float | None,
    fee_bps: float,
    slippage_bps: float,
) -> float | None:
    if gross_return is None:
        return None
    rt_cost = 2.0 * (bps_to_return(fee_bps) + bps_to_return(slippage_bps))
    return float(gross_return) - float(rt_cost)


def _recent_rows_by_ts(
    rows: List[Dict[str, Any]],
    key: str,
    now: datetime,
    lookback_hours: int,
) -> List[Dict[str, Any]]:
    cutoff = now - timedelta(hours=max(1, int(lookback_hours)))
    out: List[Dict[str, Any]] = []
    for r in rows:
        raw = r.get(key)
        if not raw:
            continue
        try:
            ts = parse_iso(str(raw))
        except Exception:  # noqa: BLE001
            continue
        if cutoff <= ts <= now:
            out.append(r)
    out.sort(key=lambda x: parse_iso(str(x.get(key))))
    return out


def assess_risk_guard(
    results: List[Dict[str, Any]],
    now: datetime,
    cooldown_until_raw: str | None,
    max_daily_loss_pct: float,
    max_consecutive_losses: int,
    cooldown_min: int,
    lookback_hours: int = RISK_LOOKBACK_HOURS,
) -> Dict[str, Any]:
    rows = _recent_rows_by_ts(
        rows=results,
        key="evaluated_at",
        now=now,
        lookback_hours=lookback_hours,
    )
    daily_return = sum(float(safe_float(r.get("return_blended")) or 0.0) for r in rows)
    streak = 0
    for r in reversed(rows):
        if bool(r.get("win")):
            break
        streak += 1

    reasons: List[str] = []
    max_daily_loss = -abs(float(max_daily_loss_pct) / 100.0)
    max_streak = max(1, int(max_consecutive_losses))
    if daily_return <= max_daily_loss:
        reasons.append(
            f"24h net return {daily_return * 100:.2f}% <= -{abs(max_daily_loss_pct):.2f}%"
        )
    if streak >= max_streak:
        reasons.append(f"consecutive losses {streak} >= {max_streak}")

    in_cooldown = False
    cooldown_until: datetime | None = None
    if cooldown_until_raw:
        try:
            cooldown_until = parse_iso(str(cooldown_until_raw))
            in_cooldown = now < cooldown_until
        except Exception:  # noqa: BLE001
            cooldown_until = None

    triggered_new = bool(reasons) and not in_cooldown
    if triggered_new:
        cooldown_until = now + timedelta(minutes=max(1, int(cooldown_min)))
        in_cooldown = True

    status = {
        "lookback_hours": int(lookback_hours),
        "sample": len(rows),
        "daily_return": float(daily_return),
        "consecutive_losses": int(streak),
        "max_daily_loss_pct": float(max_daily_loss_pct),
        "max_consecutive_losses": int(max_streak),
        "cooldown_min": int(max(1, int(cooldown_min))),
        "triggered_new": bool(triggered_new),
        "reasons": reasons,
        "in_cooldown": bool(in_cooldown),
        "allow_new_picks": not bool(in_cooldown),
        "cooldown_until": iso_z(cooldown_until) if cooldown_until else None,
    }
    return status


def _weekly_ab_due(last_weekly_ab_at: str | None, now: datetime, interval_hours: int) -> bool:
    if not last_weekly_ab_at:
        return True
    try:
        prev = parse_iso(str(last_weekly_ab_at))
    except Exception:  # noqa: BLE001
        return True
    return (now - prev) >= timedelta(hours=max(1, int(interval_hours)))


def run_weekly_ab_review(
    state: Dict[str, Any],
    now: datetime,
    lookback_hours: int = WEEKLY_AB_LOOKBACK_HOURS,
    interval_hours: int = WEEKLY_AB_INTERVAL_HOURS,
    min_results: int = WEEKLY_AB_MIN_RESULTS,
    min_profile_results: int = WEEKLY_AB_MIN_PROFILE_RESULTS,
    min_model_results: int = WEEKLY_AB_MIN_MODEL_RESULTS,
    allow_apply: bool = True,
) -> Dict[str, Any]:
    meta = state.setdefault("meta", {})
    last_weekly_ab_at = meta.get("last_weekly_ab_at")
    if not _weekly_ab_due(
        last_weekly_ab_at=str(last_weekly_ab_at or ""),
        now=now,
        interval_hours=interval_hours,
    ):
        return {
            "due": False,
            "ran": False,
            "applied": False,
            "notes": [],
            "summary": "",
            "event": None,
            "execution_profile": sanitize_execution_profile(meta.get("execution_profile")),
            "model_registry": sanitize_model_registry(state.get("model_registry", {})),
        }

    rows = _recent_rows_by_ts(
        rows=list(state.get("results", [])),
        key="evaluated_at",
        now=now,
        lookback_hours=max(24, int(lookback_hours)),
    )
    notes: List[str] = []
    event_id = f"weekly-ab-{int(now.timestamp())}"
    reg = sanitize_model_registry(state.get("model_registry", {}))
    current_profile = sanitize_execution_profile(meta.get("execution_profile", DEFAULT_EXECUTION_PROFILE))
    next_profile = current_profile
    applied = False

    profile_stats: Dict[int, Dict[str, float]] = {}
    for p in (1, 2, 3):
        prof_rows: List[Dict[str, Any]] = []
        for r in rows:
            raw_prof = safe_float(r.get("execution_profile"))
            if raw_prof is None:
                row_prof = DEFAULT_EXECUTION_PROFILE
            else:
                row_prof = sanitize_execution_profile(raw_prof, default=DEFAULT_EXECUTION_PROFILE)
            if row_prof == p:
                prof_rows.append(r)
        if len(prof_rows) < max(1, int(min_profile_results)):
            continue
        vals = [float(safe_float(r.get("return_blended")) or 0.0) for r in prof_rows]
        wr = sum(1 for r in prof_rows if bool(r.get("win"))) / len(prof_rows)
        avg = sum(vals) / len(vals) if vals else 0.0
        score = avg + (0.0035 * (wr - 0.50))
        profile_stats[p] = {
            "count": float(len(prof_rows)),
            "win_rate": float(wr),
            "avg_return": float(avg),
            "score": float(score),
        }

    if len(rows) >= max(1, int(min_results)) and profile_stats:
        best_profile = max(profile_stats.keys(), key=lambda p: float(profile_stats[p]["score"]))
        cur_stat = profile_stats.get(current_profile)
        best_stat = profile_stats.get(best_profile)
        if cur_stat and best_stat:
            win_gap = float(best_stat["win_rate"]) - float(cur_stat["win_rate"])
            avg_gap = float(best_stat["avg_return"]) - float(cur_stat["avg_return"])
            if (
                best_profile != current_profile
                and (avg_gap >= 0.0008 or win_gap >= 0.03)
                and allow_apply
            ):
                next_profile = int(best_profile)
                applied = True
                notes.append(
                    "Weekly A/B profile promotion: "
                    f"P{current_profile} -> P{next_profile} "
                    f"(win {best_stat['win_rate'] * 100:.1f}% vs {cur_stat['win_rate'] * 100:.1f}%, "
                    f"avg {best_stat['avg_return'] * 100:.2f}% vs {cur_stat['avg_return'] * 100:.2f}%)."
                )

    model_stats: Dict[str, Dict[str, float]] = {}
    for mid in sorted({pick_model_id(r) for r in rows}):
        m_rows = [r for r in rows if pick_model_id(r) == mid]
        if len(m_rows) < max(1, int(min_model_results)):
            continue
        vals = [float(safe_float(r.get("return_blended")) or 0.0) for r in m_rows]
        wr = sum(1 for r in m_rows if bool(r.get("win"))) / len(m_rows)
        avg = sum(vals) / len(vals) if vals else 0.0
        model_stats[mid] = {
            "count": float(len(m_rows)),
            "win_rate": float(wr),
            "avg_return": float(avg),
            "score": float(avg + (0.0030 * (wr - 0.50))),
        }

    for side in ("LONG", "SHORT"):
        mids = [m for m in model_stats if model_side_from_id(m) == side]
        if len(mids) < 2:
            continue
        mids.sort(key=lambda m: float(model_stats[m]["score"]), reverse=True)
        best_mid = mids[0]
        best_row = model_stats[best_mid]
        active_side = active_model_ids(reg, side)
        if best_mid in active_side:
            continue
        base_mid = model_id_from_side(side)
        baseline = model_stats.get(base_mid)
        if not baseline:
            continue
        win_gap = float(best_row["win_rate"]) - float(baseline["win_rate"])
        avg_gap = float(best_row["avg_return"]) - float(baseline["avg_return"])
        if (win_gap >= 0.05 or avg_gap >= 0.0012) and allow_apply:
            reg.setdefault(best_mid, {"enabled": False, "side": side})
            reg[best_mid]["enabled"] = True
            reg[best_mid]["side"] = side
            applied = True
            notes.append(
                "Weekly A/B model promotion: "
                f"{model_name_from_id(base_mid)} -> {model_name_from_id(best_mid)} "
                f"({side}, win +{win_gap * 100:.1f}%p, avg +{avg_gap * 100:.2f}%p)."
            )
            disable_mid: str | None = None
            loser_rows = [
                m for m in active_side
                if m != best_mid and m != base_mid and model_stats.get(m)
            ]
            if loser_rows:
                loser_rows.sort(key=lambda m: float(model_stats[m]["score"]))
                worst_mid = loser_rows[0]
                worst = model_stats[worst_mid]
                worst_win_gap = float(best_row["win_rate"]) - float(worst["win_rate"])
                worst_avg_gap = float(best_row["avg_return"]) - float(worst["avg_return"])
                if worst_win_gap >= 0.06 and worst_avg_gap >= 0.0015:
                    reg[worst_mid]["enabled"] = False
                    disable_mid = worst_mid
                    notes.append(
                        f"Weekly A/B prune: disabled {model_name_from_id(worst_mid)} "
                        f"(winner edge win +{worst_win_gap * 100:.1f}%p, avg +{worst_avg_gap * 100:.2f}%p)."
                    )
            state.setdefault("model_governance_events", []).append(
                {
                    "id": f"{event_id}-{side.lower()}",
                    "at": iso_z(now),
                    "type": "weekly_ab_promotion",
                    "side": side,
                    "from_model": base_mid,
                    "to_model": best_mid,
                    "disabled_model": disable_mid,
                    "baseline": baseline,
                    "winner": best_row,
                }
            )

    summary = (
        "; ".join(notes)
        if notes
        else "Weekly A/B review: no promotion (insufficient edge or sample)."
    )
    event = {
        "id": event_id,
        "at": iso_z(now),
        "lookback_hours": int(lookback_hours),
        "interval_hours": int(interval_hours),
        "results_count": len(rows),
        "profile_from": int(current_profile),
        "profile_to": int(next_profile),
        "profile_stats": profile_stats,
        "model_stats_sample": {k: v for k, v in list(model_stats.items())[:12]},
        "applied": bool(applied and allow_apply),
        "notes": list(notes),
        "summary": summary,
    }
    state.setdefault("weekly_ab_events", []).append(event)
    state["weekly_ab_events"] = state["weekly_ab_events"][-400:]
    meta["last_weekly_ab_at"] = iso_z(now)
    meta["last_weekly_ab_event_id"] = event_id

    if applied and allow_apply:
        meta["execution_profile"] = int(next_profile)
        meta["execution_profile_updated_at"] = iso_z(now)
        state["model_registry"] = sanitize_model_registry(reg)

    return {
        "due": True,
        "ran": True,
        "applied": bool(applied and allow_apply),
        "notes": notes,
        "summary": summary,
        "event": event,
        "execution_profile": int(meta.get("execution_profile", current_profile)),
        "model_registry": sanitize_model_registry(state.get("model_registry", reg)),
    }


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


def sanitize_execution_profile(v: Any, default: int = DEFAULT_EXECUTION_PROFILE) -> int:
    try:
        n = int(v)
    except Exception:  # noqa: BLE001
        n = int(default)
    if n not in EXECUTION_PROFILE_RULES:
        n = int(default)
    return n


def execution_profile_rule(profile: int) -> Dict[str, Any]:
    p = sanitize_execution_profile(profile)
    return dict(EXECUTION_PROFILE_RULES.get(p, EXECUTION_PROFILE_RULES[DEFAULT_EXECUTION_PROFILE]))


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


def eval_horizons_for_model(
    model_id: str,
    default_horizons: List[int],
    fallback_horizon: int,
) -> List[int]:
    base = sorted(set(int(x) for x in (default_horizons or [fallback_horizon]) if int(x) > 0))
    if not base:
        base = [max(1, int(fallback_horizon))]
    mid = str(model_id or "").strip()
    if mid in {MODEL_LONG_V3_ID, MODEL_SHORT_V3_ID}:
        out = [h for h in base if h >= 15]
        for h in (30, 60, 120):
            if h not in out:
                out.append(h)
        out = sorted(set(out))
        if not out:
            out = [30, 60, 120]
        return out
    return base


def default_v3_tuning() -> Dict[str, Dict[str, float]]:
    return {
        MODEL_LONG_V3_ID: {
            "inverse_guard": 1.0,
            "crowding_guard": 1.0,
            "low_momo_penalty": 1.0,
            "obstacle_guard": 1.0,
        },
        MODEL_SHORT_V3_ID: {
            "inverse_guard": 1.0,
            "crowding_guard": 1.0,
            "low_momo_penalty": 1.0,
            "obstacle_guard": 1.0,
        },
    }


def sanitize_v3_tuning(raw: Dict[str, Any] | None) -> Dict[str, Dict[str, float]]:
    out = default_v3_tuning()
    src = raw or {}
    for mid in (MODEL_LONG_V3_ID, MODEL_SHORT_V3_ID):
        row_src = src.get(mid, {})
        if not isinstance(row_src, dict):
            continue
        row = out[mid]
        for k in ("inverse_guard", "crowding_guard", "low_momo_penalty", "obstacle_guard"):
            val = safe_float(row_src.get(k))
            if val is None:
                continue
            row[k] = round(clamp(float(val), 0.70, 2.00), 4)
    return out


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


def _default_state() -> Dict[str, Any]:
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
        "social_buzz_history": [],
        "calibration_events": [],
        "model_governance_events": [],
        "model_transition_events": [],
        "daily_review_events": [],
        "weekly_ab_events": [],
        "state_recovery_events": [],
        "meta": {
            "no_candidate_streak": 0,
            "last_calibrated_at": None,
            "last_run_at": None,
            "last_model_governance_at": None,
            "model_governance_cooldown_until": None,
            "last_model_recommendation": None,
            "last_model_diagnostics": None,
            "execution_profile": DEFAULT_EXECUTION_PROFILE,
            "execution_profile_updated_at": None,
            "last_daily_review_at": None,
            "last_daily_review_event_id": None,
            "last_weekly_ab_at": None,
            "last_weekly_ab_event_id": None,
            "risk_guard_cooldown_until": None,
            "last_risk_guard_at": None,
            "last_risk_guard_status": None,
            "last_state_recovery_at": None,
            "last_social_buzz_at": None,
            "last_social_buzz": None,
            "v3_tuning": default_v3_tuning(),
        },
    }


def _state_backup_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}.backup{path.suffix}")


def _load_state_json(path: Path) -> Dict[str, Any]:
    txt = path.read_text(encoding="utf-8")
    for marker in ("<<<<<<<", "=======", ">>>>>>>"):
        if marker in txt:
            raise ValueError(f"conflict marker detected in state file: {marker}")
    obj = json.loads(txt)
    if not isinstance(obj, dict):
        raise ValueError("state root must be a JSON object")
    return obj


def _stamp_state_recovery(
    data: Dict[str, Any],
    *,
    reason: str,
    source: str,
    corrupted_copy: str | None,
    now: datetime,
) -> None:
    data.setdefault("state_recovery_events", [])
    event = {
        "at": iso_z(now),
        "reason": str(reason)[:240],
        "source": str(source),
        "corrupted_copy": corrupted_copy,
    }
    data["state_recovery_events"].append(event)
    data["state_recovery_events"] = [
        x for x in data["state_recovery_events"] if isinstance(x, dict)
    ][-200:]
    data.setdefault("meta", {})
    data["meta"]["last_state_recovery_at"] = iso_z(now)


def _recover_state(path: Path, exc: Exception) -> Dict[str, Any]:
    now = utc_now()
    backup = _state_backup_path(path)
    corrupted_copy: str | None = None

    if path.exists():
        ts = now.strftime("%Y%m%dT%H%M%SZ")
        corrupted = path.with_name(f"{path.stem}.corrupt-{ts}{path.suffix}")
        try:
            shutil.copy2(path, corrupted)
            corrupted_copy = str(corrupted.name)
        except Exception as cp_exc:  # noqa: BLE001
            print(f"[WARN] failed to copy corrupted state snapshot: {cp_exc}")

    recovered: Dict[str, Any]
    source = "default"
    if backup.exists():
        try:
            recovered = _load_state_json(backup)
            source = "backup"
            print(f"[WARN] state recovery: loaded backup {backup.name}")
        except Exception as backup_exc:  # noqa: BLE001
            print(f"[WARN] backup state is also invalid: {backup_exc}")
            recovered = _default_state()
    else:
        recovered = _default_state()

    _stamp_state_recovery(
        recovered,
        reason=f"{type(exc).__name__}: {exc}",
        source=source,
        corrupted_copy=corrupted_copy,
        now=now,
    )
    return recovered


def _normalize_state(data: Dict[str, Any]) -> Dict[str, Any]:
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
    data.setdefault("social_buzz_history", [])
    data.setdefault("calibration_events", [])
    data.setdefault("model_governance_events", [])
    data.setdefault("model_transition_events", [])
    data.setdefault("daily_review_events", [])
    data.setdefault("weekly_ab_events", [])
    data.setdefault("state_recovery_events", [])
    if isinstance(data.get("model_transition_events"), list):
        data["model_transition_events"] = [
            x for x in data["model_transition_events"] if isinstance(x, dict)
        ][-500:]
    else:
        data["model_transition_events"] = []
    if isinstance(data.get("state_recovery_events"), list):
        data["state_recovery_events"] = [
            x for x in data["state_recovery_events"] if isinstance(x, dict)
        ][-200:]
    else:
        data["state_recovery_events"] = []
    if isinstance(data.get("weekly_ab_events"), list):
        data["weekly_ab_events"] = [
            x for x in data["weekly_ab_events"] if isinstance(x, dict)
        ][-400:]
    else:
        data["weekly_ab_events"] = []
    if isinstance(data.get("social_buzz_history"), list):
        data["social_buzz_history"] = [
            x for x in data["social_buzz_history"] if isinstance(x, dict)
        ][-SOCIAL_BUZZ_HISTORY_KEEP:]
    else:
        data["social_buzz_history"] = []
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
    data["meta"].setdefault("execution_profile_updated_at", None)
    data["meta"].setdefault("last_daily_review_at", None)
    data["meta"].setdefault("last_daily_review_event_id", None)
    data["meta"].setdefault("last_weekly_ab_at", None)
    data["meta"].setdefault("last_weekly_ab_event_id", None)
    data["meta"].setdefault("risk_guard_cooldown_until", None)
    data["meta"].setdefault("last_risk_guard_at", None)
    data["meta"].setdefault("last_risk_guard_status", None)
    data["meta"].setdefault("last_state_recovery_at", None)
    data["meta"].setdefault("last_social_buzz_at", None)
    data["meta"].setdefault("last_social_buzz", None)
    data["meta"]["v3_tuning"] = sanitize_v3_tuning(data["meta"].get("v3_tuning"))
    data["meta"]["execution_profile"] = sanitize_execution_profile(
        data["meta"].get("execution_profile", DEFAULT_EXECUTION_PROFILE),
        default=DEFAULT_EXECUTION_PROFILE,
    )
    data.setdefault("version", 3)
    return data


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return _normalize_state(_default_state())
    try:
        data = _load_state_json(path)
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] state load failed: {exc}")
        data = _recover_state(path, exc)
    return _normalize_state(data)


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state = _normalize_state(state)
    payload = json.dumps(state, ensure_ascii=False, indent=2)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(payload)
    tmp.replace(path)

    backup = _state_backup_path(path)
    backup_tmp = backup.with_suffix(".tmp")
    try:
        with backup_tmp.open("w", encoding="utf-8") as f:
            f.write(payload)
        backup_tmp.replace(backup)
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] failed to write state backup: {exc}")


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


def fetch_market_snapshot_with_retry(
    attempts: int = 3,
    base_sleep_sec: float = 1.5,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    tries = max(1, int(attempts))
    last_exc: Exception | None = None
    for i in range(tries):
        try:
            return fetch_market_snapshot()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if i + 1 >= tries:
                break
            wait_s = max(0.5, float(base_sleep_sec) * float(i + 1))
            print(
                f"[WARN] market fetch retry {i + 1}/{tries - 1} failed: {exc}; "
                f"sleep {wait_s:.1f}s"
            )
            time.sleep(wait_s)
    if last_exc is None:
        raise RuntimeError("market fetch failed without exception")
    raise RuntimeError(f"market fetch failed after {tries} attempts: {last_exc}")


def fetch_json_with_headers(
    url: str,
    *,
    headers: Dict[str, str] | None = None,
    timeout_sec: int = 15,
) -> Dict[str, Any]:
    req_headers = {
        "User-Agent": "Mozilla/5.0 (MomentumScanner SocialBuzz)",
        "Accept": "application/json",
    }
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))


def _clean_symbol(sym: Any) -> str:
    raw = str(sym or "").upper().strip()
    out = re.sub(r"[^A-Z0-9]", "", raw)
    if out.endswith("USDT"):
        out = out[:-4]
    return out


def build_social_symbol_universe(
    *,
    picks: List[Dict[str, Any]],
    candidates: List[Any],
    bithumb: Dict[str, Any],
    bitget: Dict[str, Any],
    max_symbols: int,
) -> List[str]:
    max_n = max(3, int(max_symbols))
    out: List[str] = []
    seen: set[str] = set()

    def push(sym: Any) -> None:
        s = _clean_symbol(sym)
        if not s:
            return
        if len(s) < 2 or len(s) > 12:
            return
        if s in seen:
            return
        seen.add(s)
        out.append(s)

    for s in SOCIAL_DEFAULT_SYMBOLS:
        push(s)
    for p in picks:
        push(p.get("symbol"))
    for c in sorted(candidates, key=lambda x: float(getattr(x, "score", 0.0) or 0.0), reverse=True):
        push(getattr(c, "symbol", ""))
        if len(out) >= max_n:
            break
    for sym, t in sorted(
        bitget.items(),
        key=lambda kv: float(getattr(kv[1], "usdt_volume", 0.0) or 0.0),
        reverse=True,
    ):
        push(sym)
        if len(out) >= max_n:
            break
    if len(out) < max_n:
        for sym in bithumb.keys():
            push(sym)
            if len(out) >= max_n:
                break
    return out[:max_n]


def _compile_symbol_patterns(symbols: List[str]) -> Dict[str, re.Pattern[str]]:
    pats: Dict[str, re.Pattern[str]] = {}
    for sym in symbols:
        esc = re.escape(sym)
        if sym in MAJOR_BARE_SYMBOLS:
            # Major symbols allow bare-word mention matching.
            patt = rf"(?<![A-Z0-9])(?:[$#])?{esc}(?![A-Z0-9])"
        else:
            # Non-major symbols require cashtag/hashtag to reduce false positives.
            patt = rf"(?<![A-Z0-9])(?:[$#]){esc}(?![A-Z0-9])"
        pats[sym] = re.compile(patt, re.IGNORECASE)
    return pats


def _x_post_weight(post: Dict[str, Any]) -> float:
    pm = post.get("public_metrics", {}) or {}
    likes = int(pm.get("like_count", 0) or 0)
    reposts = int(pm.get("retweet_count", pm.get("repost_count", 0)) or 0)
    replies = int(pm.get("reply_count", 0) or 0)
    quotes = int(pm.get("quote_count", 0) or 0)
    eng = max(0, likes + reposts + replies + quotes)
    return 1.0 + min(4.0, eng / 25.0)


def collect_x_social_mentions(
    symbols: List[str],
    *,
    max_results: int,
    timeout_sec: int = 15,
) -> Dict[str, Any]:
    token = os.getenv("X_BEARER_TOKEN", "").strip()
    if not token:
        return {
            "enabled": False,
            "ok": False,
            "provider": "x",
            "error": "token_missing",
            "mentions": {},
            "sample_posts": 0,
            "query": None,
        }

    syms = [s for s in symbols if s]
    query_terms: List[str] = []
    for s in syms:
        query_terms.extend([f"${s}", f"#{s}"])
        if s in MAJOR_BARE_SYMBOLS:
            query_terms.append(s)
    if not query_terms:
        return {
            "enabled": True,
            "ok": False,
            "provider": "x",
            "error": "no_query_terms",
            "mentions": {},
            "sample_posts": 0,
            "query": None,
        }
    query = f"({' OR '.join(query_terms)}) -is:retweet"
    params = {
        "query": query,
        "max_results": max(10, min(100, int(max_results))),
        "tweet.fields": "created_at,lang,public_metrics",
    }
    url = f"{X_RECENT_SEARCH_URL}?{urllib.parse.urlencode(params)}"
    try:
        payload = fetch_json_with_headers(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout_sec=timeout_sec,
        )
        posts = payload.get("data", []) or []
        pats = _compile_symbol_patterns(syms)
        counts: Dict[str, Dict[str, float]] = {
            s: {"mentions": 0.0, "score": 0.0} for s in syms
        }
        for post in posts:
            text = str(post.get("text", "") or "")
            if not text:
                continue
            weight = _x_post_weight(post)
            for sym, pat in pats.items():
                if pat.search(text):
                    counts[sym]["mentions"] += 1.0
                    counts[sym]["score"] += weight
        return {
            "enabled": True,
            "ok": True,
            "provider": "x",
            "error": None,
            "mentions": counts,
            "sample_posts": len(posts),
            "query": query,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "enabled": True,
            "ok": False,
            "provider": "x",
            "error": str(exc),
            "mentions": {},
            "sample_posts": 0,
            "query": query,
        }


def _build_threads_keyword_url(query: str, token: str, limit: int) -> str:
    template = os.getenv("THREADS_KEYWORD_SEARCH_URL_TEMPLATE", "").strip()
    q_enc = urllib.parse.quote_plus(query)
    base_url = THREADS_GRAPH_BASE_URL.rstrip("/")
    if template:
        return template.format(
            query=query,
            query_urlencoded=q_enc,
            token=token,
            limit=int(limit),
            base_url=base_url,
        )
    params = {
        "q": query,
        "search_type": os.getenv("THREADS_SEARCH_TYPE", "TOP"),
        "limit": max(5, min(50, int(limit))),
        "fields": "id,text,username,timestamp",
        "access_token": token,
    }
    return f"{base_url}/keyword_search?{urllib.parse.urlencode(params)}"


def collect_threads_social_mentions(
    symbols: List[str],
    *,
    max_symbols: int,
    per_symbol_limit: int = 20,
    timeout_sec: int = 15,
) -> Dict[str, Any]:
    token = os.getenv("THREADS_ACCESS_TOKEN", "").strip()
    if not token:
        return {
            "enabled": False,
            "ok": False,
            "provider": "threads",
            "error": "token_missing",
            "mentions": {},
            "sample_posts": 0,
            "queries": 0,
        }
    syms = [s for s in symbols if s][: max(1, int(max_symbols))]
    counts: Dict[str, Dict[str, float]] = {s: {"mentions": 0.0, "score": 0.0} for s in syms}
    ok_queries = 0
    total_posts = 0
    errs: List[str] = []
    for s in syms:
        url = _build_threads_keyword_url(s, token=token, limit=per_symbol_limit)
        try:
            payload = fetch_json_with_headers(url, timeout_sec=timeout_sec)
            rows = payload.get("data", []) or []
            total_posts += len(rows)
            counts[s]["mentions"] += float(len(rows))
            counts[s]["score"] += float(len(rows))
            ok_queries += 1
        except Exception as exc:  # noqa: BLE001
            errs.append(f"{s}:{exc}")
        time.sleep(0.12)
    return {
        "enabled": True,
        "ok": ok_queries > 0,
        "provider": "threads",
        "error": "; ".join(errs[:4]) if errs else None,
        "mentions": counts,
        "sample_posts": total_posts,
        "queries": len(syms),
        "ok_queries": ok_queries,
    }


def collect_social_buzz_snapshot(
    *,
    now: datetime,
    symbols: List[str],
    x_max_results: int,
    threads_max_symbols: int,
) -> Dict[str, Any]:
    syms = [s for s in symbols if s]
    x_res = collect_x_social_mentions(
        syms,
        max_results=x_max_results,
    )
    th_res = collect_threads_social_mentions(
        syms,
        max_symbols=threads_max_symbols,
    )
    rows: List[Dict[str, Any]] = []
    for s in syms:
        x_row = (x_res.get("mentions", {}) or {}).get(s, {}) or {}
        t_row = (th_res.get("mentions", {}) or {}).get(s, {}) or {}
        x_mentions = int(round(float(x_row.get("mentions", 0.0) or 0.0)))
        t_mentions = int(round(float(t_row.get("mentions", 0.0) or 0.0)))
        x_score = float(x_row.get("score", 0.0) or 0.0)
        t_score = float(t_row.get("score", 0.0) or 0.0)
        score = x_score + t_score
        mentions = x_mentions + t_mentions
        if mentions <= 0 and score <= 0:
            continue
        rows.append(
            {
                "symbol": s,
                "score": round(score, 3),
                "mentions_total": int(mentions),
                "x_mentions": int(x_mentions),
                "threads_mentions": int(t_mentions),
                "x_score": round(x_score, 3),
                "threads_score": round(t_score, 3),
            }
        )
    rows.sort(key=lambda r: (float(r.get("score", 0.0)), int(r.get("mentions_total", 0))), reverse=True)
    rows = rows[:SOCIAL_BUZZ_TOP_KEEP]
    return {
        "at": iso_z(now),
        "symbols_considered": len(syms),
        "top_symbols": rows,
        "providers": {
            "x": {
                "enabled": bool(x_res.get("enabled")),
                "ok": bool(x_res.get("ok")),
                "sample_posts": int(x_res.get("sample_posts", 0) or 0),
                "error": x_res.get("error"),
            },
            "threads": {
                "enabled": bool(th_res.get("enabled")),
                "ok": bool(th_res.get("ok")),
                "sample_posts": int(th_res.get("sample_posts", 0) or 0),
                "queries": int(th_res.get("queries", 0) or 0),
                "ok_queries": int(th_res.get("ok_queries", 0) or 0),
                "error": th_res.get("error"),
            },
        },
    }


def append_social_buzz_history(
    state: Dict[str, Any],
    snapshot: Dict[str, Any],
    keep: int = SOCIAL_BUZZ_HISTORY_KEEP,
) -> None:
    if not isinstance(snapshot, dict):
        return
    hist = list(state.get("social_buzz_history", []))
    hist.append(snapshot)
    state["social_buzz_history"] = hist[-max(50, int(keep)) :]


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


def compute_setup_quality(
    c: Any,
    side: str,
    model_id: str,
    market_indicators: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    side_u = "SHORT" if str(side).upper() == "SHORT" else "LONG"
    mid = str(model_id or "").strip()
    is_swing = mid in {MODEL_LONG_V3_ID, MODEL_SHORT_V3_ID}
    direction = -1.0 if side_u == "SHORT" else 1.0

    fast_weights = (
        [("6h", 0.35), ("1h", 0.30), ("15m", 0.20), ("5m", 0.15)]
        if is_swing
        else [("1h", 0.35), ("15m", 0.25), ("5m", 0.20), ("1m", 0.20)]
    )
    swing_weights = (
        [("24h", 0.40), ("12h", 0.30), ("6h", 0.20), ("1h", 0.10)]
        if is_swing
        else [("24h", 0.35), ("12h", 0.25), ("6h", 0.20), ("1h", 0.20)]
    )
    market_fast = _weighted_change(market_indicators, "market", fast_weights)
    market_swing = _weighted_change(market_indicators, "market", swing_weights)
    btc_fast = _weighted_change(market_indicators, "btc", fast_weights)
    eth_fast = _weighted_change(market_indicators, "eth", fast_weights)

    align_core = 0.0
    for raw, weight, scale in (
        (
            [
                (market_fast, 0.50, 3.0),
                (market_swing, 0.35, 4.6),
                (btc_fast, 0.10, 2.8),
                (eth_fast, 0.05, 2.8),
            ]
            if is_swing
            else [
                (market_fast, 0.55, 2.5),
                (market_swing, 0.25, 4.0),
                (btc_fast, 0.12, 2.2),
                (eth_fast, 0.08, 2.2),
            ]
        )
    ):
        if raw is None:
            continue
        align_core += float(weight) * clamp(float(raw) / float(scale), -1.0, 1.0)
    alignment = direction * clamp(align_core, -1.0, 1.0)
    alignment_score = clamp(0.5 + (alignment * 0.5), 0.0, 1.0)

    change24h = safe_float(getattr(c, "g_change24h_pct", None)) or 0.0
    momentum_signed = direction * clamp(change24h / (12.0 if is_swing else 10.0), -1.0, 1.0)
    momentum_score = clamp(0.5 + (momentum_signed * 0.5), 0.0, 1.0)

    funding = safe_float(getattr(c, "g_funding_rate", None)) or 0.0
    crowded = direction * funding
    crowded_cutoff = 0.00035 if is_swing else 0.00045
    crowd_penalty = clamp(
        (crowded - crowded_cutoff) * (900.0 if is_swing else 850.0),
        0.0,
        0.45,
    )
    crowd_score = clamp(1.0 - crowd_penalty, 0.25, 1.0)

    oi = safe_float(getattr(c, "g_open_interest", None)) or 0.0
    oi_norm = clamp(math.log10(1.0 + max(0.0, oi)) / 7.0, 0.0, 1.0)
    late_chase = oi_norm * norm01(
        abs(change24h),
        2.0 if is_swing else 1.5,
        18.0 if is_swing else 14.0,
    )
    chase_score = clamp(
        1.0 - ((0.32 if is_swing else 0.28) * late_chase),
        0.35,
        1.0,
    )

    ob_support = safe_float(getattr(c, "b_ob_support_dist_pct", None))
    ob_resist = safe_float(getattr(c, "b_ob_resist_dist_pct", None))
    room = ob_resist if side_u == "LONG" else ob_support
    cushion = ob_support if side_u == "LONG" else ob_resist

    if room is not None and room > 0:
        room_score = norm01(
            room,
            0.25 if is_swing else 0.18,
            2.20 if is_swing else 1.40,
        )
    else:
        room_score = 0.52

    if cushion is not None and cushion > 0:
        ideal = 0.38 if is_swing else 0.28
        span = 0.62 if is_swing else 0.45
        cushion_score = 1.0 - clamp(abs(cushion - ideal) / span, 0.0, 1.0)
    else:
        cushion_score = 0.50

    g_vol = safe_float(getattr(c, "g_usdt_volume", None)) or 0.0
    b_val = safe_float(getattr(c, "b_krw_value24h", None)) or 0.0
    liq_g = clamp(math.log10(1.0 + (g_vol / 10_000_000.0)) / math.log10(11.0), 0.0, 1.0)
    liq_b = clamp(math.log10(1.0 + (b_val / 2_000_000_000.0)) / math.log10(11.0), 0.0, 1.0)
    liquidity_score = (0.55 * liq_g) + (0.45 * liq_b)

    if is_swing:
        quality = (
            (0.30 * alignment_score)
            + (0.17 * room_score)
            + (0.12 * cushion_score)
            + (0.14 * crowd_score)
            + (0.11 * chase_score)
            + (0.09 * liquidity_score)
            + (0.07 * momentum_score)
        )
    else:
        quality = (
            (0.28 * alignment_score)
            + (0.18 * room_score)
            + (0.12 * cushion_score)
            + (0.14 * crowd_score)
            + (0.10 * chase_score)
            + (0.10 * liquidity_score)
            + (0.08 * momentum_score)
        )
    quality = clamp(quality, 0.0, 1.0)

    if alignment >= 0.24 and room_score >= 0.50:
        entry_mode = "trend"
    elif alignment <= -0.24:
        entry_mode = "contrarian"
    else:
        entry_mode = "balanced"

    if quality >= 0.72:
        quality_label = "A"
    elif quality >= 0.58:
        quality_label = "B"
    else:
        quality_label = "C"

    return {
        "quality": round(float(quality), 6),
        "quality_label": quality_label,
        "alignment": round(float(alignment), 6),
        "entry_mode": entry_mode,
        "room_score": round(float(room_score), 6),
        "crowding_score": round(float(crowd_score), 6),
        "liquidity_score": round(float(liquidity_score), 6),
    }


def score_candidate_for_model(
    c: Any,
    model_id: str,
    market_indicators: Dict[str, Dict[str, Any]],
    model_tuning: Dict[str, Dict[str, float]] | None = None,
) -> float:
    base = float(getattr(c, "score", 0.0) or 0.0)
    if model_id in {MODEL_LONG_ID, MODEL_SHORT_ID}:
        return round(base, 4)

    is_swing = model_id in {MODEL_LONG_V3_ID, MODEL_SHORT_V3_ID}
    tune_row = (model_tuning or {}).get(model_id, {}) if model_tuning else {}
    inverse_guard = clamp(float(safe_float(tune_row.get("inverse_guard")) or 1.0), 0.70, 2.00)
    crowding_guard = clamp(float(safe_float(tune_row.get("crowding_guard")) or 1.0), 0.70, 2.00)
    low_momo_penalty = clamp(float(safe_float(tune_row.get("low_momo_penalty")) or 1.0), 0.70, 2.00)
    obstacle_guard = clamp(float(safe_float(tune_row.get("obstacle_guard")) or 1.0), 0.70, 2.00)
    side = model_side_from_id(model_id)
    direction = -1.0 if side == "SHORT" else 1.0

    fast_weights = (
        [("6h", 0.35), ("1h", 0.30), ("15m", 0.20), ("5m", 0.15)]
        if is_swing
        else [("1h", 0.35), ("15m", 0.25), ("5m", 0.20), ("1m", 0.20)]
    )
    swing_weights = (
        [("24h", 0.40), ("12h", 0.30), ("6h", 0.20), ("1h", 0.10)]
        if is_swing
        else [("24h", 0.35), ("12h", 0.25), ("6h", 0.20), ("1h", 0.20)]
    )
    market_fast = _weighted_change(market_indicators, "market", fast_weights)
    market_swing = _weighted_change(market_indicators, "market", swing_weights)
    btc_fast = _weighted_change(market_indicators, "btc", fast_weights)
    eth_fast = _weighted_change(market_indicators, "eth", fast_weights)

    signal = 0.0
    parts = (
        [
            (market_fast, 0.25, 3.2),
            (market_swing, 0.50, 4.5),
            (btc_fast, 0.15, 2.5),
            (eth_fast, 0.10, 2.5),
        ]
        if is_swing
        else [
            (market_fast, 0.45, 2.5),
            (market_swing, 0.30, 4.0),
            (btc_fast, 0.15, 2.0),
            (eth_fast, 0.10, 2.0),
        ]
    )
    for raw, weight, scale in parts:
        if raw is None:
            continue
        signal += float(weight) * clamp(raw / scale, -1.0, 1.0)
    signal *= direction
    adjust = (0.12 if is_swing else 0.10) * signal

    funding = safe_float(getattr(c, "g_funding_rate", None)) or 0.0
    if side == "LONG":
        if funding < 0:
            k = 36.0 if is_swing else 30.0
            limit = 0.05 if is_swing else 0.04
            adjust += min(limit, (-funding) * k)
        elif funding > (0.0006 if is_swing else 0.0008):
            limit = 0.08 if is_swing else 0.06
            cutoff = 0.0006 if is_swing else 0.0008
            adjust -= min(limit * crowding_guard, (funding - cutoff) * 45.0 * crowding_guard)
    else:
        if funding > 0:
            k = 36.0 if is_swing else 30.0
            limit = 0.05 if is_swing else 0.04
            adjust += min(limit, funding * k)
        elif funding < -(0.0006 if is_swing else 0.0008):
            limit = 0.08 if is_swing else 0.06
            cutoff = 0.0006 if is_swing else 0.0008
            adjust -= min(limit * crowding_guard, ((-funding) - cutoff) * 45.0 * crowding_guard)

    oi = safe_float(getattr(c, "g_open_interest", None))
    change24h = safe_float(getattr(c, "g_change24h_pct", None)) or 0.0
    if oi is not None and oi > 0:
        oi_score = clamp(math.log10(1.0 + oi) / 7.0, 0.0, 1.0)
        dir_momo = direction * clamp(change24h / 15.0, -1.0, 1.0)
        adjust += (0.04 if is_swing else 0.03) * oi_score * dir_momo
    if is_swing:
        dir_trend24 = direction * clamp(change24h / 12.0, -1.0, 1.0)
        adjust += 0.04 * dir_trend24
        if abs(change24h) < 1.0:
            adjust -= 0.02 * low_momo_penalty

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
    if is_swing:
        if side == "LONG" and market_swing is not None:
            if market_swing < -0.25:
                adjust -= 0.06 * inverse_guard
            elif market_swing > 0.35:
                adjust += 0.04
        if side == "SHORT" and market_swing is not None:
            if market_swing > 0.25:
                adjust -= 0.06 * inverse_guard
            elif market_swing < -0.35:
                adjust += 0.04

    ob_signal = safe_float(getattr(c, "b_ob_signal", None))
    ob_support = safe_float(getattr(c, "b_ob_support_dist_pct", None))
    ob_resist = safe_float(getattr(c, "b_ob_resist_dist_pct", None))
    if ob_signal is not None:
        # Orderblock pressure: bid-heavy helps LONG, ask-heavy helps SHORT.
        ob_w = 0.06 if is_swing else 0.05
        adjust += ob_w * direction * clamp(float(ob_signal), -1.0, 1.0)
    near_band = 0.45 if is_swing else 0.35
    if side == "LONG":
        if ob_resist is not None and ob_resist < near_band:
            penalty = 0.05 if is_swing else 0.03
            adjust -= penalty * obstacle_guard * (1.0 - (ob_resist / near_band))
        if ob_support is not None and ob_support < near_band:
            bonus = 0.03 if is_swing else 0.02
            adjust += bonus * (1.0 - (ob_support / near_band))
    else:
        if ob_support is not None and ob_support < near_band:
            penalty = 0.05 if is_swing else 0.03
            adjust -= penalty * obstacle_guard * (1.0 - (ob_support / near_band))
        if ob_resist is not None and ob_resist < near_band:
            bonus = 0.03 if is_swing else 0.02
            adjust += bonus * (1.0 - (ob_resist / near_band))

    lo, hi = (-0.24, 0.26) if is_swing else (-0.20, 0.20)
    return round(base + clamp(adjust, lo, hi), 4)


def compute_entry_plan_fields(
    c: Any,
    side: str,
    score: float,
    model_id: str | None = None,
    setup: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    side_u = str(side).upper()
    mid = str(model_id or "").strip()
    is_swing = mid in {MODEL_LONG_V3_ID, MODEL_SHORT_V3_ID}
    direction = -1.0 if side_u == "SHORT" else 1.0
    setup_row = dict(setup or {})
    setup_quality = clamp(float(safe_float(setup_row.get("quality")) or 0.5), 0.0, 1.0)
    setup_alignment = clamp(float(safe_float(setup_row.get("alignment")) or 0.0), -1.0, 1.0)
    setup_label = str(setup_row.get("quality_label", "B")).strip().upper() or "B"
    setup_entry_mode = str(setup_row.get("entry_mode", "balanced")).strip().lower()
    if setup_entry_mode not in {"trend", "balanced", "contrarian"}:
        setup_entry_mode = "balanced"

    b_px = safe_float(getattr(c, "b_close_krw", None))
    g_px = safe_float(getattr(c, "g_last_price", None))
    b_rate = abs(safe_float(getattr(c, "b_rate24h", None)) or 0.0)
    g_rate = abs(safe_float(getattr(c, "g_change24h_pct", None)) or 0.0)
    vol24 = clamp((b_rate + g_rate) / 2.0, 0.5, 25.0)

    if is_swing:
        stop_pct = clamp(0.55 + (vol24 * 0.085), 0.60, 3.40)
        rr_base = clamp(1.35 + (float(score) * 1.80), 1.35, 3.20)
    else:
        stop_pct = clamp(0.45 + (vol24 * 0.08), 0.45, 2.80)
        rr_base = clamp(1.10 + (float(score) * 1.50), 1.10, 2.60)
    if setup_entry_mode == "trend":
        mode_stop_mult = 0.96
    elif setup_entry_mode == "contrarian":
        mode_stop_mult = 1.06
    else:
        mode_stop_mult = 1.00
    stop_setup_mult = (
        clamp(1.08 - (setup_quality * 0.20), 0.86, 1.08)
        * clamp(1.0 - (setup_alignment * 0.06), 0.92, 1.08)
        * float(mode_stop_mult)
    )
    stop_pct = clamp(
        float(stop_pct) * float(stop_setup_mult),
        0.55 if is_swing else 0.40,
        3.60 if is_swing else 2.90,
    )
    target_rr_pct = stop_pct * rr_base

    # Method 1) volatility+score RR target.
    # Method 2) orderblock distance target (closest wall distance).
    # Method 3) funding/OI crowding-adjusted target.
    ob_support = safe_float(getattr(c, "b_ob_support_dist_pct", None))
    ob_resist = safe_float(getattr(c, "b_ob_resist_dist_pct", None))
    ob_dist = ob_support if side_u == "SHORT" else ob_resist
    target_ob_pct: float | None = None
    if ob_dist is not None and ob_dist > 0:
        if is_swing:
            target_ob_pct = clamp(float(ob_dist) * 0.90, 0.60, 8.00)
        else:
            target_ob_pct = clamp(float(ob_dist) * 0.85, 0.35, 6.00)

    funding = safe_float(getattr(c, "g_funding_rate", None)) or 0.0
    oi = safe_float(getattr(c, "g_open_interest", None)) or 0.0
    oi_norm = clamp(math.log10(1.0 + max(0.0, oi)) / 7.0, 0.0, 1.0)
    if is_swing:
        funding_mult = clamp(1.0 + (-direction * funding * 90.0), 0.78, 1.22)
        oi_mult = clamp(1.03 - (oi_norm * 0.12), 0.88, 1.06)
    else:
        funding_mult = clamp(1.0 + (-direction * funding * 80.0), 0.80, 1.20)
        oi_mult = clamp(1.02 - (oi_norm * 0.10), 0.90, 1.05)
    target_flow_pct = clamp(
        target_rr_pct * funding_mult * oi_mult,
        0.60 if is_swing else 0.35,
        8.00 if is_swing else 6.00,
    )

    weighted_terms: List[Tuple[float, float]] = (
        [(target_rr_pct, 0.45), (target_flow_pct, 0.20)]
        if is_swing
        else [(target_rr_pct, 0.50), (target_flow_pct, 0.20)]
    )
    if target_ob_pct is not None:
        weighted_terms.append((target_ob_pct, 0.35 if is_swing else 0.30))
    w_sum = sum(w for _, w in weighted_terms)
    target_pct = (
        sum(v * w for v, w in weighted_terms) / max(w_sum, 1e-9)
        if weighted_terms
        else target_rr_pct
    )
    if setup_entry_mode == "trend":
        mode_target_mult = 1.08 if is_swing else 1.05
    elif setup_entry_mode == "contrarian":
        mode_target_mult = 0.92
    else:
        mode_target_mult = 1.00
    target_setup_mult = (
        clamp(0.88 + (setup_quality * 0.34), 0.84, 1.22)
        * clamp(1.0 + (setup_alignment * 0.10), 0.88, 1.14)
        * float(mode_target_mult)
    )
    target_pct *= float(target_setup_mult)
    target_pct = clamp(float(target_pct), 0.60 if is_swing else 0.35, 8.00 if is_swing else 6.00)
    target_basis = "rr+orderblock+flow" if target_ob_pct is not None else "rr+flow"

    # Method 1) volatility baseline entry pullback.
    # Method 2) orderblock distance guided entry pullback.
    # Method 3) funding/OI crowding-adjusted entry pullback.
    if is_swing:
        entry_base_offset_pct = clamp(0.16 + (vol24 * 0.03), 0.16, 1.20)
    else:
        entry_base_offset_pct = clamp(0.10 + (vol24 * 0.02), 0.10, 0.80)
    entry_ob_dist = ob_resist if side_u == "SHORT" else ob_support
    entry_ob_offset_pct: float | None = None
    if entry_ob_dist is not None and entry_ob_dist > 0:
        if is_swing:
            entry_ob_offset_pct = clamp(float(entry_ob_dist) * 0.85, 0.12, 1.60)
        else:
            entry_ob_offset_pct = clamp(float(entry_ob_dist) * 0.75, 0.08, 1.20)

    if is_swing:
        entry_funding_mult = clamp(1.0 + (direction * funding * 45.0), 0.88, 1.16)
        entry_oi_mult = clamp(1.0 + (oi_norm * 0.10), 1.00, 1.16)
    else:
        entry_funding_mult = clamp(1.0 + (direction * funding * 60.0), 0.80, 1.25)
        entry_oi_mult = clamp(0.95 + (oi_norm * 0.20), 0.90, 1.15)
    entry_flow_offset_pct = clamp(
        entry_base_offset_pct * entry_funding_mult * entry_oi_mult,
        0.12 if is_swing else 0.08,
        1.80 if is_swing else 1.20,
    )
    entry_terms: List[Tuple[float, float]] = (
        [(entry_base_offset_pct, 0.45), (entry_flow_offset_pct, 0.20)]
        if is_swing
        else [(entry_base_offset_pct, 0.55), (entry_flow_offset_pct, 0.20)]
    )
    if entry_ob_offset_pct is not None:
        entry_terms.append((entry_ob_offset_pct, 0.35 if is_swing else 0.25))
    entry_w_sum = sum(w for _, w in entry_terms)
    entry_offset_pct = (
        sum(v * w for v, w in entry_terms) / max(entry_w_sum, 1e-9)
        if entry_terms
        else entry_base_offset_pct
    )
    if setup_entry_mode == "trend":
        mode_entry_mult = 0.90 if is_swing else 0.92
    elif setup_entry_mode == "contrarian":
        mode_entry_mult = 1.10
    else:
        mode_entry_mult = 1.00
    entry_setup_mult = (
        clamp(1.12 - (setup_quality * 0.24), 0.86, 1.12)
        * clamp(1.0 - (setup_alignment * 0.08), 0.88, 1.10)
        * float(mode_entry_mult)
    )
    entry_offset_pct *= float(entry_setup_mult)
    entry_offset_pct = clamp(float(entry_offset_pct), 0.12 if is_swing else 0.08, 1.80 if is_swing else 1.20)
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
        "plan_stop_setup_mult": round(float(stop_setup_mult), 4),
        "plan_target_setup_mult": round(float(target_setup_mult), 4),
        "plan_entry_setup_mult": round(float(entry_setup_mult), 4),
        "plan_holding_style": "swing" if is_swing else "intraday",
        "setup_quality": round(float(setup_quality), 6),
        "setup_quality_label": setup_label,
        "setup_alignment": round(float(setup_alignment), 6),
        "setup_entry_mode": setup_entry_mode,
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


def _diagnostic_item_by_model(
    diagnostics: Dict[str, Any],
    model_id: str,
) -> Dict[str, Any] | None:
    items = diagnostics.get("items", []) if isinstance(diagnostics, dict) else []
    if not isinstance(items, list):
        return None
    for it in items:
        if not isinstance(it, dict):
            continue
        if str(it.get("model_id", "")) == str(model_id):
            return it
    return None


def migrate_v2_to_v3_by_gap(
    state: Dict[str, Any],
    model_metrics: Dict[str, Dict[str, float | int]],
    model_diagnostics: Dict[str, Any],
    now: datetime,
    min_count: int = MODEL_V2_MIGRATION_MIN_COUNT,
    min_win_gap: float = MODEL_V2_MIGRATION_WIN_GAP,
    min_avg_gap: float = MODEL_V2_MIGRATION_AVG_GAP,
) -> Tuple[Dict[str, float], Dict[str, Dict[str, float]], List[str]]:
    registry = sanitize_model_registry(state.get("model_registry", {}))
    meta = state.setdefault("meta", {})
    current_tuning = sanitize_v3_tuning(meta.get("v3_tuning"))
    score_bias: Dict[str, float] = {}
    tuning_updates: Dict[str, Dict[str, float]] = {}
    notes: List[str] = []

    for side in ("LONG", "SHORT"):
        mids = MODEL_VERSIONS_BY_SIDE.get(side)
        if not mids:
            continue
        v1_mid, v2_mid, v3_mid = mids
        m1 = model_metrics.get(v1_mid, {}) or {}
        m2 = model_metrics.get(v2_mid, {}) or {}
        n1 = int(m1.get("count", 0) or 0)
        n2 = int(m2.get("count", 0) or 0)
        if n1 < max(1, int(min_count)) or n2 < max(1, int(min_count)):
            continue

        w1 = float(m1.get("win_rate", 0.0) or 0.0)
        w2 = float(m2.get("win_rate", 0.0) or 0.0)
        a1 = float(m1.get("avg_return", 0.0) or 0.0)
        a2 = float(m2.get("avg_return", 0.0) or 0.0)
        win_gap = w1 - w2
        avg_gap = a1 - a2
        if win_gap < float(min_win_gap) and avg_gap < float(min_avg_gap):
            continue

        # Keep v1 online, but shift traffic from v2 to v3 via score-bias migration.
        penalty = clamp(
            0.04 + (max(0.0, win_gap - float(min_win_gap)) * 0.90)
            + (max(0.0, avg_gap - float(min_avg_gap)) * 10.0),
            0.04,
            0.16,
        )
        bonus = clamp(0.03 + (penalty * 0.70), 0.03, 0.12)
        score_bias[v2_mid] = -round(float(penalty), 4)
        score_bias[v3_mid] = round(float(bonus), 4)

        registry.setdefault(v3_mid, {"enabled": False, "side": side})
        registry[v3_mid]["enabled"] = True
        registry[v3_mid]["side"] = side

        diag_item = _diagnostic_item_by_model(model_diagnostics, v2_mid)
        issues = (diag_item or {}).get("issues", []) if isinstance(diag_item, dict) else []
        issue_bits: List[str] = []
        tune = dict(current_tuning.get(v3_mid, default_v3_tuning().get(v3_mid, {})))
        for it in (issues or [])[:3]:
            if not isinstance(it, dict):
                continue
            dim = str(it.get("dimension", ""))
            bucket = str(it.get("bucket", ""))
            issue_bits.append(f"{dim}:{bucket}")
            if dim == "alignment" and bucket == "inverse":
                tune["inverse_guard"] = clamp(float(tune.get("inverse_guard", 1.0)) + 0.25, 0.70, 2.00)
            if dim == "funding" and "crowded" in bucket:
                tune["crowding_guard"] = clamp(float(tune.get("crowding_guard", 1.0)) + 0.25, 0.70, 2.00)
            if dim == "momentum" and bucket == "low-momentum":
                tune["low_momo_penalty"] = clamp(float(tune.get("low_momo_penalty", 1.0)) + 0.20, 0.70, 2.00)
            if dim == "momentum" and bucket == "high-momentum":
                tune["obstacle_guard"] = clamp(float(tune.get("obstacle_guard", 1.0)) + 0.20, 0.70, 2.00)
            if dim == "open_interest" and bucket == "low-oi":
                tune["low_momo_penalty"] = clamp(float(tune.get("low_momo_penalty", 1.0)) + 0.10, 0.70, 2.00)

        tuning_updates[v3_mid] = {
            "inverse_guard": round(float(tune.get("inverse_guard", 1.0)), 4),
            "crowding_guard": round(float(tune.get("crowding_guard", 1.0)), 4),
            "low_momo_penalty": round(float(tune.get("low_momo_penalty", 1.0)), 4),
            "obstacle_guard": round(float(tune.get("obstacle_guard", 1.0)), 4),
        }
        issue_txt = ", ".join(issue_bits) if issue_bits else "no-dominant-bucket"
        notes.append(
            f"{side} v2->v3 migration: v1({n1},{w1*100:.1f}%/{a1*100:.2f}%) "
            f"vs v2({n2},{w2*100:.1f}%/{a2*100:.2f}%), bias(v2 {score_bias[v2_mid]:+.3f}, "
            f"v3 {score_bias[v3_mid]:+.3f}), issues={issue_txt}"
        )

        state.setdefault("model_transition_events", []).append(
            {
                "id": f"migrate-{side.lower()}-{int(now.timestamp())}",
                "at": iso_z(now),
                "side": side,
                "v1_model": v1_mid,
                "v2_model": v2_mid,
                "v3_model": v3_mid,
                "v1_count": n1,
                "v2_count": n2,
                "v1_win_rate": w1,
                "v2_win_rate": w2,
                "v1_avg_return": a1,
                "v2_avg_return": a2,
                "score_bias_v2": score_bias[v2_mid],
                "score_bias_v3": score_bias[v3_mid],
                "tuning": dict(tuning_updates[v3_mid]),
                "issues": issue_bits,
            }
        )

    if tuning_updates:
        merged = dict(current_tuning)
        for mid, row in tuning_updates.items():
            cur = dict(merged.get(mid, default_v3_tuning().get(mid, {})))
            for k, v in row.items():
                old = float(safe_float(cur.get(k)) or 1.0)
                cur[k] = round(clamp((old * 0.60) + (float(v) * 0.40), 0.70, 2.00), 4)
            merged[mid] = cur
        meta["v3_tuning"] = sanitize_v3_tuning(merged)
    else:
        meta["v3_tuning"] = sanitize_v3_tuning(current_tuning)

    state["model_registry"] = registry
    state["model_transition_events"] = state.get("model_transition_events", [])[-500:]
    return score_bias, dict(meta.get("v3_tuning", {})), notes


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
    execution_profile: int = DEFAULT_EXECUTION_PROFILE,
    model_score_bias: Dict[str, float] | None = None,
    model_tuning: Dict[str, Dict[str, float]] | None = None,
    historical_model_metrics: Dict[str, Dict[str, float | int]] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    top_n = max(0, int(top_n))
    min_short_picks = max(0, int(min_short_picks))
    model_score_bias = dict(model_score_bias or {})
    model_tuning = dict(model_tuning or {})
    historical_model_metrics = dict(historical_model_metrics or {})
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
                    model_tuning=model_tuning,
                )
                setup = compute_setup_quality(
                    c=c,
                    side=side,
                    model_id=mid,
                    market_indicators=market_indicators,
                )
                setup_quality = clamp(float(safe_float(setup.get("quality")) or 0.5), 0.0, 1.0)
                quality_boost = (setup_quality - 0.5) * (
                    0.20 if mid in {MODEL_LONG_V3_ID, MODEL_SHORT_V3_ID} else 0.14
                )
                bias = clamp(float(safe_float(model_score_bias.get(mid)) or 0.0), -0.30, 0.30)
                scored = round(float(scored) + float(bias) + float(quality_boost), 4)
                row = {
                    "candidate": c,
                    "model_id": mid,
                    "score": scored,
                    "score_bias": bias,
                    "setup": setup,
                    "quality_boost": quality_boost,
                }
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
    removed_execution_profile = 0
    execution_profile = sanitize_execution_profile(
        execution_profile,
        default=DEFAULT_EXECUTION_PROFILE,
    )
    execution_rule = execution_profile_rule(execution_profile)
    min_target_pct = float(execution_rule.get("min_target_pct", 0.0) or 0.0)
    min_rr_entry = float(execution_rule.get("min_rr_entry", 0.0) or 0.0)
    min_setup_quality = float(execution_rule.get("min_setup_quality", 0.0) or 0.0)
    min_edge_pct = float(execution_rule.get("min_edge_pct", -9_999.0) or -9_999.0)
    default_eval_horizons = sorted(
        set(int(x) for x in (eval_horizons_min or [horizon_min]) if int(x) > 0)
    )
    if not default_eval_horizons:
        default_eval_horizons = [max(1, int(horizon_min))]
    market_changes = market_indicators.get("market", {}).get("changes", {}) or {}
    btc_changes = market_indicators.get("btc", {}).get("changes", {}) or {}
    eth_changes = market_indicators.get("eth", {}).get("changes", {}) or {}
    concentration = market_indicators.get("concentration", {}) or {}
    market_trend = str(market_indicators.get("market", {}).get("trend", "neutral"))
    btc_trend = str(market_indicators.get("btc", {}).get("trend", "neutral"))
    eth_trend = str(market_indicators.get("eth", {}).get("trend", "neutral"))
    removed_setup_quality = 0
    removed_expected_edge = 0
    for row in selected:
        c = row["candidate"]
        mid = str(row["model_id"])
        score = float(row["score"])
        base_score = float(getattr(c, "score", 0.0) or 0.0)
        bias_applied = float(safe_float(row.get("score_bias")) or 0.0)
        quality_boost = float(safe_float(row.get("quality_boost")) or 0.0)
        setup = dict(row.get("setup") or {})
        row_eval_horizons = eval_horizons_for_model(
            model_id=mid,
            default_horizons=default_eval_horizons,
            fallback_horizon=horizon_min,
        )
        primary_horizon = int(row_eval_horizons[0])
        plan = compute_entry_plan_fields(
            c=c,
            side=str(c.side),
            score=score,
            model_id=mid,
            setup=setup,
        )
        plan_target_pct = safe_float(plan.get("plan_target_pct"))
        plan_rr_entry = safe_float(plan.get("rr_entry"))
        plan_stop_pct = safe_float(plan.get("plan_stop_pct"))
        plan_setup_quality = clamp(float(safe_float(plan.get("setup_quality")) or 0.5), 0.0, 1.0)

        hist = historical_model_metrics.get(mid, {}) if isinstance(historical_model_metrics, dict) else {}
        hist_count = int(safe_float((hist or {}).get("count")) or 0)
        hist_win_rate_raw = clamp(
            float(safe_float((hist or {}).get("win_rate")) or 0.5),
            0.0,
            1.0,
        )
        reliability = clamp(hist_count / 36.0, 0.0, 1.0)
        hist_win_rate = (hist_win_rate_raw * reliability) + (0.5 * (1.0 - reliability))
        expected_edge_pct: float | None = None
        if plan_target_pct is not None and plan_stop_pct is not None:
            expected_edge_pct = (
                (float(hist_win_rate) * float(plan_target_pct))
                - ((1.0 - float(hist_win_rate)) * float(plan_stop_pct))
            )
            if hist_count < 8:
                expected_edge_pct -= 0.04

        if (
            plan_target_pct is None
            or plan_rr_entry is None
            or float(plan_target_pct) < min_target_pct
            or float(plan_rr_entry) < min_rr_entry
        ):
            removed_execution_profile += 1
            continue
        if plan_setup_quality < min_setup_quality:
            removed_setup_quality += 1
            continue
        if expected_edge_pct is None or float(expected_edge_pct) < min_edge_pct:
            removed_expected_edge += 1
            continue
        edge_for_size = float(expected_edge_pct)
        stop_for_size = float(plan_stop_pct or 0.0)
        profile_base_size = {1: 0.70, 2: 1.00, 3: 1.30}.get(
            int(execution_profile),
            1.0,
        )
        size_mult_quality = clamp(0.75 + (plan_setup_quality * 0.80), 0.60, 1.40)
        size_mult_edge = clamp(0.85 + (edge_for_size * 1.40), 0.55, 1.50)
        size_mult_vol = clamp(1.10 - (stop_for_size / 3.50), 0.55, 1.05)
        position_size_pct = clamp(
            profile_base_size * size_mult_quality * size_mult_edge * size_mult_vol,
            0.25,
            2.50,
        )
        risk_per_trade_pct = max(0.0, position_size_pct * (stop_for_size / 100.0))
        out.append(
            {
                "id": f"{c.symbol}-{c.side}-{mid}-{int(run_ts.timestamp())}",
                "symbol": c.symbol,
                "side": c.side,
                "model_id": mid,
                "model_name": model_name_from_id(mid),
                "created_at": iso_z(run_ts),
                "horizon_min": primary_horizon,
                "eval_horizons_min": list(row_eval_horizons),
                "evaluated_horizons": [],
                "entry_bithumb_price": c.b_close_krw,
                "entry_bitget_price": c.g_last_price,
                "score": score,
                "base_score": base_score,
                "model_score_delta": score - base_score,
                "model_score_bias": bias_applied,
                "model_quality_boost": quality_boost,
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
                "execution_profile": execution_profile,
                "execution_profile_name": str(execution_rule.get("name", "")),
                "execution_min_target_pct": min_target_pct,
                "execution_min_rr_entry": min_rr_entry,
                "execution_min_setup_quality": min_setup_quality,
                "execution_min_edge_pct": min_edge_pct,
                "expected_edge_pct": None
                if expected_edge_pct is None
                else round(float(expected_edge_pct), 6),
                "model_hist_win_rate": round(float(hist_win_rate), 6),
                "model_hist_count": hist_count,
                "position_size_pct": round(float(position_size_pct), 4),
                "risk_per_trade_pct": round(float(risk_per_trade_pct), 6),
                **plan,
            }
        )
    return out, {
        "execution_profile": execution_profile,
        "execution_rule": execution_rule,
        "removed_execution_profile": int(removed_execution_profile),
        "removed_setup_quality": int(removed_setup_quality),
        "removed_expected_edge": int(removed_expected_edge),
    }


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

        b_ret_gross = trade_return_from_market_return(b_market_ret, side)
        g_ret_gross = trade_return_from_market_return(g_market_ret, side)
        b_fee_bps = float(
            safe_float(p.get("assumed_fee_bps_bithumb")) or DEFAULT_BITHUMB_FEE_BPS
        )
        g_fee_bps = float(
            safe_float(p.get("assumed_fee_bps_bitget")) or DEFAULT_BITGET_FEE_BPS
        )
        b_slip_bps = float(
            safe_float(p.get("assumed_slippage_bps_bithumb")) or DEFAULT_BITHUMB_SLIPPAGE_BPS
        )
        g_slip_bps = float(
            safe_float(p.get("assumed_slippage_bps_bitget")) or DEFAULT_BITGET_SLIPPAGE_BPS
        )
        b_ret = apply_roundtrip_cost(
            gross_return=b_ret_gross,
            fee_bps=b_fee_bps,
            slippage_bps=b_slip_bps,
        )
        g_ret = apply_roundtrip_cost(
            gross_return=g_ret_gross,
            fee_bps=g_fee_bps,
            slippage_bps=g_slip_bps,
        )

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
                    "return_bithumb_gross": b_ret_gross,
                    "return_bitget_gross": g_ret_gross,
                    "return_bithumb": b_ret,
                    "return_bitget": g_ret,
                    "return_blended": blended,
                    "win": blended > 0,
                    "available_legs": available,
                    "assumed_fee_bps_bithumb": b_fee_bps,
                    "assumed_fee_bps_bitget": g_fee_bps,
                    "assumed_slippage_bps_bithumb": b_slip_bps,
                    "assumed_slippage_bps_bitget": g_slip_bps,
                    "score": p.get("score"),
                    "base_score": p.get("base_score"),
                    "model_score_delta": p.get("model_score_delta"),
                    "model_quality_boost": p.get("model_quality_boost"),
                    "b_rate24h": p.get("b_rate24h"),
                    "g_rate24h": p.get("g_rate24h"),
                    "g_funding_rate": p.get("g_funding_rate"),
                    "g_open_interest": p.get("g_open_interest"),
                    "setup_quality": p.get("setup_quality"),
                    "setup_quality_label": p.get("setup_quality_label"),
                    "setup_entry_mode": p.get("setup_entry_mode"),
                    "expected_edge_pct": p.get("expected_edge_pct"),
                    "position_size_pct": p.get("position_size_pct"),
                    "risk_per_trade_pct": p.get("risk_per_trade_pct"),
                    "execution_profile": p.get("execution_profile"),
                    "execution_profile_name": p.get("execution_profile_name"),
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


def _rows_within_hours(
    rows: List[Dict[str, Any]],
    ts_key: str,
    now: datetime,
    lookback_hours: int,
) -> List[Dict[str, Any]]:
    cutoff = now - timedelta(hours=max(1, int(lookback_hours)))
    out: List[Dict[str, Any]] = []
    for r in rows:
        raw = r.get(ts_key)
        if not raw:
            continue
        try:
            ts = parse_iso(str(raw))
        except Exception:  # noqa: BLE001
            continue
        if cutoff <= ts <= now:
            out.append(r)
    return out


def _config_change_rows(before: Dict[str, float], after: Dict[str, float]) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    keys = sorted(set(before.keys()) | set(after.keys()))
    for k in keys:
        b = safe_float(before.get(k))
        a = safe_float(after.get(k))
        if b is None or a is None:
            continue
        if abs(float(a) - float(b)) < 1e-12:
            continue
        out.append({"key": str(k), "before": float(b), "after": float(a)})
    return out


def _daily_review_due(last_daily_review_at: str | None, now: datetime, interval_hours: int) -> bool:
    if not last_daily_review_at:
        return True
    try:
        prev = parse_iso(str(last_daily_review_at))
    except Exception:  # noqa: BLE001
        return True
    return (now - prev) >= timedelta(hours=max(1, int(interval_hours)))


def run_daily_batch_review(
    state: Dict[str, Any],
    now: datetime,
    cfg: Dict[str, float],
    execution_profile: int,
    lookback_hours: int = DAILY_REVIEW_LOOKBACK_HOURS,
    interval_hours: int = DAILY_REVIEW_INTERVAL_HOURS,
    min_results: int = DAILY_REVIEW_MIN_RESULTS,
    min_model_results: int = DAILY_REVIEW_MIN_MODEL_RESULTS,
    allow_apply: bool = True,
) -> Dict[str, Any]:
    meta = state.setdefault("meta", {})
    last_daily_review_at = meta.get("last_daily_review_at")
    if not _daily_review_due(
        last_daily_review_at=str(last_daily_review_at or ""),
        now=now,
        interval_hours=interval_hours,
    ):
        return {
            "due": False,
            "ran": False,
            "applied": False,
            "summary": "",
            "notes": [],
            "config": dict(cfg),
            "execution_profile": sanitize_execution_profile(execution_profile),
            "event": None,
        }

    lookback_h = max(1, int(lookback_hours))
    min_eval_n = max(1, int(min_results))
    min_model_n = max(1, int(min_model_results))
    current_profile = sanitize_execution_profile(execution_profile, default=DEFAULT_EXECUTION_PROFILE)
    next_profile = current_profile

    day_results = _rows_within_hours(
        rows=state.get("results", []),
        ts_key="evaluated_at",
        now=now,
        lookback_hours=lookback_h,
    )
    day_recommendations = _rows_within_hours(
        rows=state.get("recommendation_history", []),
        ts_key="created_at",
        now=now,
        lookback_hours=lookback_h,
    )

    eval_n = len(day_results)
    rec_n = len(day_recommendations)
    day_metrics = compute_metrics(day_results, window=max(1, eval_n))
    day_model_metrics = compute_model_metrics(day_results, window=max(1, eval_n))
    day_diagnostics = diagnose_underperforming_models(
        results=day_results,
        model_metrics=day_model_metrics,
        min_count=max(6, min_eval_n // 2),
        min_bucket_count=4,
    )

    new_cfg = dict(cfg)
    notes: List[str] = []
    model_expansions: List[Dict[str, Any]] = []
    registry = sanitize_model_registry(state.get("model_registry"))

    win_rate = float(day_metrics.get("win_rate", 0.0) or 0.0)
    avg_return = float(day_metrics.get("avg_return", 0.0) or 0.0)
    if not bool(allow_apply):
        notes.append(
            f"Daily review executed in cooldown mode: rec={rec_n}, eval={eval_n}, "
            f"win={win_rate * 100:.2f}%, avg={avg_return * 100:.2f}% (no auto-change)."
        )
    elif eval_n < max(4, min_eval_n // 2):
        notes.append(
            f"Daily review sample is still small: eval={eval_n}, rec={rec_n}. "
            "Kept major risk thresholds unchanged."
        )
        if rec_n < 4:
            new_cfg["min_bithumb_value"] = max(1_000_000_000, new_cfg["min_bithumb_value"] * 0.92)
            new_cfg["min_bitget_volume"] = max(5_000_000, new_cfg["min_bitget_volume"] * 0.92)
            new_cfg["min_bithumb_rate"] = max(0.5, new_cfg["min_bithumb_rate"] - 0.10)
            new_cfg["min_bitget_rate"] = max(0.5, new_cfg["min_bitget_rate"] - 0.10)
            new_cfg["min_bitget_short_rate"] = max(0.5, new_cfg["min_bitget_short_rate"] - 0.10)
            notes.append("Daily low-flow adjustment: relaxed liquidity/momentum floors slightly.")
    elif win_rate < 0.45 or avg_return < -0.0010:
        new_cfg["max_overheat_rate"] = max(20.0, new_cfg["max_overheat_rate"] - 2.0)
        new_cfg["conservative_max_rate"] = max(10.0, new_cfg["conservative_max_rate"] - 1.0)
        new_cfg["min_bithumb_value"] = min(20_000_000_000, new_cfg["min_bithumb_value"] * 1.08)
        new_cfg["min_bitget_volume"] = min(60_000_000, new_cfg["min_bitget_volume"] * 1.08)
        new_cfg["conservative_max_abs_funding"] = max(
            0.0005, new_cfg["conservative_max_abs_funding"] * 0.92
        )
        notes.append(
            f"Daily underperformance adjustment: win={win_rate * 100:.2f}% "
            f"avg={avg_return * 100:.2f}% -> tightened risk/liquidity filters."
        )
        if current_profile > 1:
            next_profile = current_profile - 1
            notes.append(f"Execution profile auto-step: P{current_profile} -> P{next_profile}.")
    elif win_rate > 0.58 and avg_return > 0.0015 and rec_n >= min_eval_n:
        new_cfg["max_overheat_rate"] = min(50.0, new_cfg["max_overheat_rate"] + 1.5)
        new_cfg["conservative_max_rate"] = min(25.0, new_cfg["conservative_max_rate"] + 1.0)
        new_cfg["min_bithumb_value"] = max(1_000_000_000, new_cfg["min_bithumb_value"] * 0.95)
        new_cfg["min_bitget_volume"] = max(5_000_000, new_cfg["min_bitget_volume"] * 0.95)
        new_cfg["conservative_max_abs_funding"] = min(
            0.0025, new_cfg["conservative_max_abs_funding"] * 1.05
        )
        notes.append(
            f"Daily strong-performance adjustment: win={win_rate * 100:.2f}% "
            f"avg={avg_return * 100:.2f}% -> widened search slightly."
        )
        if current_profile < 3:
            next_profile = current_profile + 1
            notes.append(f"Execution profile auto-step: P{current_profile} -> P{next_profile}.")
    else:
        notes.append(
            f"Daily review stable: win={win_rate * 100:.2f}%, avg={avg_return * 100:.2f}%, "
            "no major threshold change."
        )

    if bool(allow_apply):
        for side in ("LONG", "SHORT"):
            active = active_model_ids(registry, side)
            weak_mid = ""
            weak_win = 1.0
            weak_cnt = 0
            for mid in active:
                mm = day_model_metrics.get(mid, {}) or {}
                cnt = int(mm.get("count", 0) or 0)
                if cnt < min_model_n:
                    continue
                wr = float(mm.get("win_rate", 0.0) or 0.0)
                if wr < weak_win:
                    weak_mid = str(mid)
                    weak_win = wr
                    weak_cnt = cnt
            if weak_mid and weak_win < 0.40:
                nxt = next_model_id(weak_mid)
                if not bool(registry.get(nxt, {}).get("enabled", False)):
                    registry.setdefault(nxt, {"enabled": False, "side": side})
                    registry[nxt]["enabled"] = True
                    registry[nxt]["side"] = side
                    model_expansions.append(
                        {
                            "side": side,
                            "from_model": weak_mid,
                            "to_model": nxt,
                            "count": weak_cnt,
                            "win_rate": weak_win,
                        }
                    )
                    notes.append(
                        f"Daily model expansion: {model_name_from_id(weak_mid)} "
                        f"({weak_cnt} eval, win {weak_win * 100:.2f}%) -> {model_name_from_id(nxt)} enabled."
                    )

    if day_diagnostics.get("triggered"):
        items = day_diagnostics.get("items", []) or []
        chunks = [
            f"{str(x.get('model_label', '-'))}:{str(x.get('summary', 'no-signal'))}"
            for x in items[:2]
        ]
        if chunks:
            notes.append("Daily model diagnosis: " + " | ".join(chunks))

    new_cfg = sanitize_dynamic_config(new_cfg)
    cfg_changes = _config_change_rows(cfg, new_cfg)
    applied = bool(cfg_changes or model_expansions or (next_profile != current_profile))

    state["model_registry"] = registry
    event_id = f"daily-review-{int(now.timestamp())}"
    event = {
        "id": event_id,
        "at": iso_z(now),
        "lookback_hours": lookback_h,
        "recommendations": rec_n,
        "evaluations": eval_n,
        "metrics": dict(day_metrics),
        "model_metrics": day_model_metrics,
        "config_changes": cfg_changes,
        "execution_profile_from": current_profile,
        "execution_profile_to": next_profile,
        "model_expansions": model_expansions,
        "notes": notes,
        "applied": applied,
    }
    state.setdefault("daily_review_events", []).append(event)
    state["daily_review_events"] = state["daily_review_events"][-500:]
    meta["last_daily_review_at"] = iso_z(now)
    meta["last_daily_review_event_id"] = event_id

    summary = (
        f"Daily review({lookback_h}h): rec {rec_n}, eval {eval_n}, "
        f"win {format_pct(win_rate)}, avg {format_pct(avg_return)}"
    )
    if applied:
        summary += (
            f" | cfgΔ {len(cfg_changes)} | modelΔ {len(model_expansions)} "
            f"| profile P{current_profile}->P{next_profile}"
        )
    else:
        summary += " | no-change"

    return {
        "due": True,
        "ran": True,
        "applied": applied,
        "summary": summary,
        "notes": notes,
        "config": dict(new_cfg),
        "execution_profile": next_profile,
        "event": event,
    }


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
    q = clamp(float(safe_float(p.get("setup_quality")) or 0.0), 0.0, 1.0)
    q_lbl = str(p.get("setup_quality_label", "")).strip().upper() or "-"
    edge = safe_float(p.get("expected_edge_pct"))
    size = safe_float(p.get("position_size_pct"))
    prof = sanitize_execution_profile(
        p.get("execution_profile", DEFAULT_EXECUTION_PROFILE),
        default=DEFAULT_EXECUTION_PROFILE,
    )
    edge_txt = "-" if edge is None else f"{edge:+.2f}%"
    size_txt = "-" if size is None else f"{size:.2f}%"
    return (
        f"{index}) {p['symbol']} | {side} | P{prof} | score {p['score']:.3f} | "
        f"q {q_lbl}/{q*100:.0f} | edge {edge_txt} | size {size_txt} | fr {fr:.4f} | oi {oi}"
    )


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

    b_ret_gross = trade_return_from_market_return(b_market_ret, side)
    g_ret_gross = trade_return_from_market_return(g_market_ret, side)
    b_fee_bps = float(
        safe_float(p.get("assumed_fee_bps_bithumb")) or DEFAULT_BITHUMB_FEE_BPS
    )
    g_fee_bps = float(
        safe_float(p.get("assumed_fee_bps_bitget")) or DEFAULT_BITGET_FEE_BPS
    )
    b_slip_bps = float(
        safe_float(p.get("assumed_slippage_bps_bithumb")) or DEFAULT_BITHUMB_SLIPPAGE_BPS
    )
    g_slip_bps = float(
        safe_float(p.get("assumed_slippage_bps_bitget")) or DEFAULT_BITGET_SLIPPAGE_BPS
    )
    b_ret = apply_roundtrip_cost(
        gross_return=b_ret_gross,
        fee_bps=b_fee_bps,
        slippage_bps=b_slip_bps,
    )
    g_ret = apply_roundtrip_cost(
        gross_return=g_ret_gross,
        fee_bps=g_fee_bps,
        slippage_bps=g_slip_bps,
    )
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


def format_social_buzz_line(social_buzz: Dict[str, Any] | None, top_n: int = 3) -> str | None:
    row = dict(social_buzz or {})
    top = row.get("top_symbols", []) or []
    providers = row.get("providers", {}) or {}
    x_enabled = bool((providers.get("x", {}) or {}).get("enabled", False))
    t_enabled = bool((providers.get("threads", {}) or {}).get("enabled", False))
    x_ok = bool((providers.get("x", {}) or {}).get("ok", False))
    t_ok = bool((providers.get("threads", {}) or {}).get("ok", False))
    if not top:
        if not x_enabled and not t_enabled:
            return None
        if not x_ok and not t_ok:
            return "SocialBuzz: unavailable (X/Threads credentials or API check needed)"
        return "SocialBuzz: no dominant symbol mentions in current window"
    parts: List[str] = []
    for r in top[: max(1, int(top_n))]:
        sym = str(r.get("symbol", "-"))
        total = int(r.get("mentions_total", 0) or 0)
        x_n = int(r.get("x_mentions", 0) or 0)
        t_n = int(r.get("threads_mentions", 0) or 0)
        parts.append(f"{sym}({total}, X{x_n}/T{t_n})")
    return "SocialBuzz: " + " | ".join(parts)


def make_message(
    run_ts: datetime,
    picks: List[Dict[str, Any]],
    metrics: Dict[str, float],
    filter_stats: Dict[str, Any],
    cfg: Dict[str, float],
    execution_profile: int,
    execution_rule: Dict[str, Any],
    new_results_count: int,
    calibrate_notes: List[str],
    model_governance_notes: List[str],
    model_recommendation: Dict[str, Any] | None,
    model_diagnostics: Dict[str, Any] | None,
    social_buzz: Dict[str, Any] | None,
    missed_summary: Dict[str, Any] | None,
    risk_guard_status: Dict[str, Any] | None,
    message_style: str,
) -> str:
    ts_kst = run_ts.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    prof = sanitize_execution_profile(execution_profile, default=DEFAULT_EXECUTION_PROFILE)
    prof_rule = execution_rule or execution_profile_rule(prof)
    min_target = float(prof_rule.get("min_target_pct", 0.0) or 0.0)
    min_rr = float(prof_rule.get("min_rr_entry", 0.0) or 0.0)
    min_setup_q = float(prof_rule.get("min_setup_quality", 0.0) or 0.0)
    min_edge = float(prof_rule.get("min_edge_pct", 0.0) or 0.0)
    removed_prof = int(filter_stats.get("removed_execution_profile", 0) or 0)
    removed_quality = int(filter_stats.get("removed_setup_quality", 0) or 0)
    removed_edge = int(filter_stats.get("removed_expected_edge", 0) or 0)
    removed_risk_guard = int(filter_stats.get("removed_risk_guard", 0) or 0)
    risk_guard_status = dict(risk_guard_status or {})
    lines: List[str] = []
    if message_style == "compact":
        lines.append(f"Momentum Scan | {ts_kst}")
        lines.append(
            f"Rules: overheat<{cfg['max_overheat_rate']:.0f}% | gLong>={cfg['min_bitget_rate']:.1f}% | gShort<=-{cfg['min_bitget_short_rate']:.1f}% | bShort<={cfg['short_max_bithumb_rate']:.1f}% | fShort>={cfg['short_min_funding_rate']:.4f} | bVal>={format_money_k(cfg['min_bithumb_value'])} | gVol>={format_money_u(cfg['min_bitget_volume'])} | P{prof}(tp>={min_target:.2f}%, rr>={min_rr:.2f}, q>={min_setup_q:.2f}, edge>={min_edge:.2f}%)"
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
        if removed_risk_guard > 0:
            lines.append(
                f"RiskGuard: paused new picks ({removed_risk_guard} blocked, "
                f"24h {float(risk_guard_status.get('daily_return', 0.0))*100:.2f}%, "
                f"streak {int(risk_guard_status.get('consecutive_losses', 0))})"
            )
        if removed_prof > 0:
            lines.append(f"Execution profile P{prof}: filtered {removed_prof} symbols (tp/rr)")
        if removed_quality > 0:
            lines.append(
                f"Execution profile P{prof}: filtered {removed_quality} symbols (timing q<{min_setup_q:.2f})"
            )
        if removed_edge > 0:
            lines.append(
                f"Execution profile P{prof}: filtered {removed_edge} symbols (edge<{min_edge:.2f}%)"
            )
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
        buzz_line = format_social_buzz_line(social_buzz, top_n=3)
        if buzz_line:
            lines.append(buzz_line)
        lines.append(f"Dashboard: {DASHBOARD_URL}")
        return "\n".join(lines)

    lines.append(f"Momentum Scan | {ts_kst}")
    lines.append("Market: Bithumb Spot + Bitget USDT-M")
    lines.append(
        f"Filters: overheat<{cfg['max_overheat_rate']:.2f}%, gLong>={cfg['min_bitget_rate']:.2f}%, gShort<=-{cfg['min_bitget_short_rate']:.2f}%, bShort<={cfg['short_max_bithumb_rate']:.2f}%, fShort>={cfg['short_min_funding_rate']:.4f}, bValue>={format_money_k(cfg['min_bithumb_value'])}, gVol>={format_money_u(cfg['min_bitget_volume'])}"
    )
    lines.append(
        f"Execution profile: P{prof} (tp>={min_target:.2f}%, rr_entry>={min_rr:.2f}, q>={min_setup_q:.2f}, edge>={min_edge:.2f}%)"
    )
    lines.append(
        f"Candidates: base={filter_stats['base_universe']}, removed(cooldown={filter_stats.get('removed_loss_cooldown', 0)}, risk_guard={filter_stats.get('removed_risk_guard', 0)}, overheat={filter_stats['removed_overheat']}, conservative={filter_stats['removed_conservative']}, orderable={filter_stats['removed_orderable']})"
    )
    if removed_prof > 0:
        lines.append(f"Execution profile filter: removed {removed_prof}")
    if removed_quality > 0:
        lines.append(f"Execution profile filter: removed {removed_quality} (timing quality)")
    if removed_edge > 0:
        lines.append(f"Execution profile filter: removed {removed_edge} (expected edge)")
    if removed_risk_guard > 0:
        lines.append(
            "RiskGuard: new recommendations paused "
            f"(blocked {removed_risk_guard}, 24h {float(risk_guard_status.get('daily_return', 0.0))*100:.2f}%, "
            f"streak {int(risk_guard_status.get('consecutive_losses', 0))})."
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
    buzz_line = format_social_buzz_line(social_buzz, top_n=5)
    if buzz_line:
        lines.append(buzz_line)
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


def send_telegram(
    token: str,
    chat_id: str,
    text: str,
    retries: int = 3,
    preflight: bool = True,
) -> None:
    attempts = max(1, int(retries))
    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            # Preflight once so auth errors are explicit in CI logs.
            if preflight and i == 0:
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
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if i + 1 >= attempts:
                break
            backoff = min(10, 2 + (i * 2))
            print(
                f"[WARN] telegram send retry {i + 1}/{attempts - 1} failed: {exc}; "
                f"sleep {backoff}s"
            )
            time.sleep(backoff)
    raise RuntimeError(f"telegram send failed after {attempts} attempts: {last_exc}")


def run_cycle(args: argparse.Namespace, state: Dict[str, Any]) -> int:
    run_ts = utc_now()
    cfg = dict(state["dynamic_config"])
    pre_cfg = dict(state["dynamic_config"])
    saved_profile = sanitize_execution_profile(
        state.get("meta", {}).get("execution_profile", DEFAULT_EXECUTION_PROFILE),
        default=DEFAULT_EXECUTION_PROFILE,
    )
    requested_profile = getattr(args, "execution_profile", None)
    execution_profile = sanitize_execution_profile(
        requested_profile if requested_profile is not None else saved_profile,
        default=saved_profile,
    )
    execution_rule = execution_profile_rule(execution_profile)
    state.setdefault("meta", {})["execution_profile"] = execution_profile
    if requested_profile is not None and execution_profile != saved_profile:
        state["meta"]["execution_profile_updated_at"] = iso_z(run_ts)
    state["model_registry"] = sanitize_model_registry(state.get("model_registry"))
    pre_model_metrics = compute_model_metrics(
        state.get("results", []),
        window=max(120, args.metric_window * 2),
    )
    pre_model_diagnostics = diagnose_underperforming_models(
        results=state.get("results", []),
        model_metrics=pre_model_metrics,
    )
    model_governance_notes = maybe_expand_models(
        state=state,
        model_metrics=pre_model_metrics,
        now=run_ts,
    )
    model_score_bias, v3_tuning, transition_notes = migrate_v2_to_v3_by_gap(
        state=state,
        model_metrics=pre_model_metrics,
        model_diagnostics=pre_model_diagnostics,
        now=run_ts,
    )
    if transition_notes:
        model_governance_notes.extend(transition_notes)
    risk_guard_status = assess_risk_guard(
        results=state.get("results", []),
        now=run_ts,
        cooldown_until_raw=state.get("meta", {}).get("risk_guard_cooldown_until"),
        max_daily_loss_pct=float(args.risk_max_daily_loss_pct),
        max_consecutive_losses=int(args.risk_max_consecutive_losses),
        cooldown_min=int(args.risk_cooldown_min),
        lookback_hours=RISK_LOOKBACK_HOURS,
    )
    state.setdefault("meta", {})["last_risk_guard_at"] = iso_z(run_ts)
    state["meta"]["last_risk_guard_status"] = {
        "sample": int(risk_guard_status.get("sample", 0)),
        "daily_return": float(risk_guard_status.get("daily_return", 0.0)),
        "consecutive_losses": int(risk_guard_status.get("consecutive_losses", 0)),
        "in_cooldown": bool(risk_guard_status.get("in_cooldown", False)),
        "allow_new_picks": bool(risk_guard_status.get("allow_new_picks", True)),
        "cooldown_until": risk_guard_status.get("cooldown_until"),
        "reasons": list(risk_guard_status.get("reasons", []) or []),
    }
    if risk_guard_status.get("triggered_new") and risk_guard_status.get("cooldown_until"):
        state["meta"]["risk_guard_cooldown_until"] = str(risk_guard_status["cooldown_until"])
    if not bool(risk_guard_status.get("allow_new_picks", True)):
        reasons_txt = ", ".join(risk_guard_status.get("reasons", []) or ["cooldown"])
        model_governance_notes.append(f"RiskGuard active: {reasons_txt}")

    try:
        bithumb, bitget, _ = fetch_market_snapshot_with_retry(attempts=3, base_sleep_sec=1.5)
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
    filter_stats["removed_risk_guard"] = 0
    candidates_for_reco = list(candidates)
    if not bool(risk_guard_status.get("allow_new_picks", True)):
        filter_stats["removed_risk_guard"] = len(candidates_for_reco)
        candidates_for_reco = []

    picks, pick_filter_stats = make_recommendations(
        candidates=candidates_for_reco,
        top_n=args.top,
        min_short_picks=args.min_short_picks,
        model_registry=state["model_registry"],
        market_indicators=market_indicators,
        run_ts=run_ts,
        horizon_min=args.horizon_min,
        eval_horizons_min=args.eval_horizons_min,
        execution_profile=execution_profile,
        model_score_bias=model_score_bias,
        model_tuning=v3_tuning,
        historical_model_metrics=pre_model_metrics,
    )
    filter_stats["removed_execution_profile"] = int(
        pick_filter_stats.get("removed_execution_profile", 0) or 0
    )
    filter_stats["removed_setup_quality"] = int(
        pick_filter_stats.get("removed_setup_quality", 0) or 0
    )
    filter_stats["removed_expected_edge"] = int(
        pick_filter_stats.get("removed_expected_edge", 0) or 0
    )
    if picks:
        for p in picks:
            p["assumed_fee_bps_bithumb"] = float(args.fee_bps_bithumb)
            p["assumed_fee_bps_bitget"] = float(args.fee_bps_bitget)
            p["assumed_slippage_bps_bithumb"] = float(args.slippage_bps_bithumb)
            p["assumed_slippage_bps_bitget"] = float(args.slippage_bps_bitget)
    if picks:
        state["recommendation_history"].extend(picks)
        state["recommendation_history"] = state["recommendation_history"][-5000:]

    social_buzz: Dict[str, Any] = {
        "at": iso_z(run_ts),
        "symbols_considered": 0,
        "top_symbols": [],
        "providers": {
            "x": {"enabled": False, "ok": False, "error": "disabled"},
            "threads": {"enabled": False, "ok": False, "error": "disabled"},
        },
    }
    if not bool(getattr(args, "disable_social_buzz", False)):
        social_symbols = build_social_symbol_universe(
            picks=picks,
            candidates=candidates,
            bithumb=bithumb,
            bitget=bitget,
            max_symbols=int(getattr(args, "social_max_symbols", 16)),
        )
        social_buzz = collect_social_buzz_snapshot(
            now=run_ts,
            symbols=social_symbols,
            x_max_results=int(getattr(args, "social_x_max_results", 80)),
            threads_max_symbols=int(getattr(args, "social_threads_max_symbols", 8)),
        )
        append_social_buzz_history(state, social_buzz)
        state.setdefault("meta", {})["last_social_buzz_at"] = iso_z(run_ts)
        state["meta"]["last_social_buzz"] = social_buzz

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

    daily_pre_cfg = dict(cfg)
    daily_review = run_daily_batch_review(
        state=state,
        now=run_ts,
        cfg=cfg,
        execution_profile=execution_profile,
        lookback_hours=int(args.daily_review_lookback_hours),
        interval_hours=int(args.daily_review_hours),
        min_results=int(args.daily_review_min_results),
        min_model_results=int(args.daily_review_min_model_results),
        allow_apply=not in_cooldown,
    )
    if daily_review.get("ran"):
        if daily_review.get("summary"):
            calibrate_notes.append(str(daily_review.get("summary")))
        for n in (daily_review.get("notes") or [])[:3]:
            calibrate_notes.append(f"Daily: {str(n)}")
        if bool(daily_review.get("applied")):
            cfg = sanitize_dynamic_config(daily_review.get("config", cfg))
            state["dynamic_config"] = cfg
            state["meta"]["last_calibrated_at"] = iso_z(run_ts)
            calibrated = True
            next_profile = sanitize_execution_profile(
                daily_review.get("execution_profile", execution_profile),
                default=execution_profile,
            )
            if next_profile != execution_profile:
                execution_profile = next_profile
                execution_rule = execution_profile_rule(execution_profile)
                state["meta"]["execution_profile"] = execution_profile
                state["meta"]["execution_profile_updated_at"] = iso_z(run_ts)
            state["calibration_events"].append(
                {
                    "id": f"daily-cal-{int(run_ts.timestamp())}",
                    "at": iso_z(run_ts),
                    "type": "daily_review",
                    "notes": list(daily_review.get("notes") or []),
                    "summary": str(daily_review.get("summary") or ""),
                    "pre_config": daily_pre_cfg,
                    "post_config": dict(cfg),
                    "metrics": dict(metrics),
                    "new_results_count": len(finalized),
                    "no_candidate_streak": state["meta"]["no_candidate_streak"],
                    "daily_review_event_id": str(
                        (daily_review.get("event") or {}).get("id") or ""
                    ),
                }
            )
            state["calibration_events"] = state["calibration_events"][-500:]

    weekly_ab = run_weekly_ab_review(
        state=state,
        now=run_ts,
        lookback_hours=int(args.weekly_ab_lookback_hours),
        interval_hours=int(args.weekly_ab_interval_hours),
        min_results=int(args.weekly_ab_min_results),
        min_profile_results=int(args.weekly_ab_min_profile_results),
        min_model_results=int(args.weekly_ab_min_model_results),
        allow_apply=not in_cooldown,
    )
    if weekly_ab.get("ran"):
        if weekly_ab.get("summary"):
            calibrate_notes.append(str(weekly_ab.get("summary")))
        for n in (weekly_ab.get("notes") or [])[:3]:
            model_governance_notes.append(f"WeeklyAB: {str(n)}")
        next_profile = sanitize_execution_profile(
            weekly_ab.get("execution_profile", execution_profile),
            default=execution_profile,
        )
        if next_profile != execution_profile:
            execution_profile = next_profile
            execution_rule = execution_profile_rule(execution_profile)
            state["meta"]["execution_profile"] = execution_profile
            state["meta"]["execution_profile_updated_at"] = iso_z(run_ts)
        state["model_registry"] = sanitize_model_registry(
            weekly_ab.get("model_registry", state.get("model_registry", {}))
        )

    if in_cooldown:
        calibrate_notes.append("Calibration cooldown active: tuning is paused for 6 hours after rollback.")
    elif not bool(daily_review.get("applied")) and should_calibrate(
        metrics=metrics,
        new_results_count=len(finalized),
        last_calibrated_at=state["meta"].get("last_calibrated_at"),
        now=run_ts,
        no_candidate_streak=state["meta"]["no_candidate_streak"],
    ):
        cfg, auto_notes = auto_calibrate(
            cfg=cfg,
            metrics=metrics,
            no_candidate_streak=state["meta"]["no_candidate_streak"],
        )
        calibrate_notes.extend(auto_notes)
        state["dynamic_config"] = cfg
        state["meta"]["last_calibrated_at"] = iso_z(run_ts)
        calibrated = True
        state["calibration_events"].append(
            {
                "id": f"cal-{int(run_ts.timestamp())}",
                "at": iso_z(run_ts),
                "notes": auto_notes,
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
        execution_profile=execution_profile,
        execution_rule=execution_rule,
        new_results_count=len(finalized),
        calibrate_notes=calibrate_notes,
        model_governance_notes=model_governance_notes,
        model_recommendation=model_recommendation,
        model_diagnostics=model_diagnostics,
        social_buzz=social_buzz,
        missed_summary=missed_summary,
        risk_guard_status=risk_guard_status,
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
                    preflight=False,
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
            "model_score_bias": dict(model_score_bias),
            "v3_tuning": dict(v3_tuning),
            "loss_alert_count": len(loss_alerts),
            "loss_cooldown_symbols": len(state["meta"].get("loss_cooldowns", {})),
            "execution_profile": execution_profile,
            "execution_rule": dict(execution_rule),
            "market_indicators": market_indicators,
            "market_alignment_now": alignment_now,
            "market_alignment_history": alignment_history,
            "model_metrics": model_metrics,
            "model_recommendation": model_recommendation,
            "model_diagnostics": model_diagnostics,
            "social_buzz": social_buzz,
            "missed_audit": missed_summary,
            "risk_guard": dict(risk_guard_status),
            "assumed_costs": {
                "fee_bps_bithumb": float(args.fee_bps_bithumb),
                "fee_bps_bitget": float(args.fee_bps_bitget),
                "slippage_bps_bithumb": float(args.slippage_bps_bithumb),
                "slippage_bps_bitget": float(args.slippage_bps_bitget),
            },
            "daily_review": {
                "ran": bool(daily_review.get("ran")),
                "applied": bool(daily_review.get("applied")),
                "summary": str(daily_review.get("summary") or ""),
                "event_id": str((daily_review.get("event") or {}).get("id") or "") or None,
            },
            "weekly_ab": {
                "ran": bool(weekly_ab.get("ran")),
                "applied": bool(weekly_ab.get("applied")),
                "summary": str(weekly_ab.get("summary") or ""),
                "event_id": str((weekly_ab.get("event") or {}).get("id") or "") or None,
            },
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
        bithumb, bitget, _ = fetch_market_snapshot_with_retry(attempts=3, base_sleep_sec=1.5)
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
                preflight=False,
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
    p.add_argument("--execution-profile", type=int, choices=(1, 2, 3), default=None)
    p.add_argument("--daily-review-hours", type=int, default=24)
    p.add_argument("--daily-review-lookback-hours", type=int, default=24)
    p.add_argument("--daily-review-min-results", type=int, default=10)
    p.add_argument("--daily-review-min-model-results", type=int, default=8)
    p.add_argument("--fee-bps-bithumb", type=float, default=DEFAULT_BITHUMB_FEE_BPS)
    p.add_argument("--fee-bps-bitget", type=float, default=DEFAULT_BITGET_FEE_BPS)
    p.add_argument("--slippage-bps-bithumb", type=float, default=DEFAULT_BITHUMB_SLIPPAGE_BPS)
    p.add_argument("--slippage-bps-bitget", type=float, default=DEFAULT_BITGET_SLIPPAGE_BPS)
    p.add_argument("--risk-max-daily-loss-pct", type=float, default=DEFAULT_RISK_MAX_DAILY_LOSS_PCT)
    p.add_argument("--risk-max-consecutive-losses", type=int, default=DEFAULT_RISK_MAX_CONSECUTIVE_LOSSES)
    p.add_argument("--risk-cooldown-min", type=int, default=DEFAULT_RISK_COOLDOWN_MIN)
    p.add_argument("--weekly-ab-lookback-hours", type=int, default=WEEKLY_AB_LOOKBACK_HOURS)
    p.add_argument("--weekly-ab-interval-hours", type=int, default=WEEKLY_AB_INTERVAL_HOURS)
    p.add_argument("--weekly-ab-min-results", type=int, default=WEEKLY_AB_MIN_RESULTS)
    p.add_argument("--weekly-ab-min-profile-results", type=int, default=WEEKLY_AB_MIN_PROFILE_RESULTS)
    p.add_argument("--weekly-ab-min-model-results", type=int, default=WEEKLY_AB_MIN_MODEL_RESULTS)
    p.add_argument("--disable-social-buzz", action="store_true")
    p.add_argument("--social-max-symbols", type=int, default=16)
    p.add_argument("--social-x-max-results", type=int, default=80)
    p.add_argument("--social-threads-max-symbols", type=int, default=8)
    p.add_argument("--alerts-only", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    args.eval_horizons_min = parse_eval_horizons(
        raw=getattr(args, "eval_horizons_min", ""),
        fallback_horizon=int(args.horizon_min),
    )
    args.horizon_min = int(args.eval_horizons_min[0]) if args.eval_horizons_min else int(args.horizon_min)
    args.fee_bps_bithumb = max(0.0, float(getattr(args, "fee_bps_bithumb", DEFAULT_BITHUMB_FEE_BPS)))
    args.fee_bps_bitget = max(0.0, float(getattr(args, "fee_bps_bitget", DEFAULT_BITGET_FEE_BPS)))
    args.slippage_bps_bithumb = max(
        0.0,
        float(getattr(args, "slippage_bps_bithumb", DEFAULT_BITHUMB_SLIPPAGE_BPS)),
    )
    args.slippage_bps_bitget = max(
        0.0,
        float(getattr(args, "slippage_bps_bitget", DEFAULT_BITGET_SLIPPAGE_BPS)),
    )
    args.risk_max_daily_loss_pct = max(
        0.5,
        float(getattr(args, "risk_max_daily_loss_pct", DEFAULT_RISK_MAX_DAILY_LOSS_PCT)),
    )
    args.risk_max_consecutive_losses = max(
        2,
        int(getattr(args, "risk_max_consecutive_losses", DEFAULT_RISK_MAX_CONSECUTIVE_LOSSES)),
    )
    args.risk_cooldown_min = max(
        15,
        int(getattr(args, "risk_cooldown_min", DEFAULT_RISK_COOLDOWN_MIN)),
    )
    args.weekly_ab_interval_hours = max(
        6,
        int(getattr(args, "weekly_ab_interval_hours", WEEKLY_AB_INTERVAL_HOURS)),
    )
    args.weekly_ab_lookback_hours = max(
        24,
        int(getattr(args, "weekly_ab_lookback_hours", WEEKLY_AB_LOOKBACK_HOURS)),
    )
    args.social_max_symbols = max(3, min(50, int(getattr(args, "social_max_symbols", 16))))
    args.social_x_max_results = max(10, min(100, int(getattr(args, "social_x_max_results", 80))))
    args.social_threads_max_symbols = max(
        1,
        min(args.social_max_symbols, int(getattr(args, "social_threads_max_symbols", 8))),
    )
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
