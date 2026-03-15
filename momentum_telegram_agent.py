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
    "min_bithumb_value": 2_000_000_000,
    "min_bitget_volume": 10_000_000,
    "max_overheat_rate": 40.0,
    "conservative_max_rate": 20.0,
    "conservative_max_abs_funding": 0.0015,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def round_step(v: float, step: float = 0.01) -> float:
    return round(round(v / step) * step, 8)


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "version": 2,
            "dynamic_config": dict(DEFAULT_DYNAMIC_CONFIG),
            "pending": [],
            "results": [],
            "recommendation_history": [],
            "run_history": [],
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
    data.setdefault("pending", [])
    data.setdefault("results", [])
    data.setdefault("recommendation_history", [])
    data.setdefault("run_history", [])
    data.setdefault("calibration_events", [])
    data.setdefault("meta", {})
    data["meta"].setdefault("no_candidate_streak", 0)
    data["meta"].setdefault("last_calibrated_at", None)
    data["meta"].setdefault("last_run_at", None)
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
        min_bithumb_krw=cfg["min_bithumb_value"],
        min_bitget_usdt=cfg["min_bitget_volume"],
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
    run_ts: datetime,
    horizon_min: int,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for c in candidates[:top_n]:
        out.append(
            {
                "id": f"{c.symbol}-{int(run_ts.timestamp())}",
                "symbol": c.symbol,
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
                "g_symbol": c.g_symbol,
            }
        )
    return out


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
        if now < created + timedelta(minutes=horizon):
            still_pending.append(p)
            continue

        symbol = p["symbol"]
        b_now = bithumb.get(symbol)
        g_now = bitget.get(symbol)

        b_ret = None
        g_ret = None
        if b_now and p.get("entry_bithumb_price", 0) > 0:
            b_ret = (b_now.close_krw - p["entry_bithumb_price"]) / p["entry_bithumb_price"]
        if g_now and p.get("entry_bitget_price", 0) > 0:
            g_ret = (g_now.last_price - p["entry_bitget_price"]) / p["entry_bitget_price"]

        if b_ret is None and g_ret is None:
            # Keep one extra horizon for temporary API mismatch.
            if now < created + timedelta(minutes=horizon * 2):
                still_pending.append(p)
                continue
            blended = 0.0
            available = 0
        else:
            vals = [x for x in (b_ret, g_ret) if x is not None]
            blended = sum(vals) / len(vals)
            available = len(vals)

        finalized.append(
            {
                "id": p["id"],
                "symbol": symbol,
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

    for k in (
        "min_bithumb_rate",
        "min_bitget_rate",
        "max_overheat_rate",
        "conservative_max_rate",
        "conservative_max_abs_funding",
    ):
        new_cfg[k] = round_step(new_cfg[k], 0.01)
    for k in ("min_bithumb_value", "min_bitget_volume"):
        new_cfg[k] = float(int(new_cfg[k]))

    return new_cfg, notes


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


def format_pick_line(index: int, p: Dict[str, Any]) -> str:
    return (
        f"{index}) {p['symbol']} | score {p['score']:.3f} | "
        f"b24h {p['b_rate24h']:.2f}% | g24h {p['g_rate24h']:.2f}% | "
        f"bVal {format_money_k(p['b_value24h'])} | gVol {format_money_u(p['g_volume24h'])}"
    )


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
            f"Rules: overheat<{cfg['max_overheat_rate']:.0f}% | bVal>={format_money_k(cfg['min_bithumb_value'])} | gVol>={format_money_u(cfg['min_bitget_volume'])}"
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
        f"Filters: overheat<{cfg['max_overheat_rate']:.2f}%, bValue>={format_money_k(cfg['min_bithumb_value'])}, gVol>={format_money_u(cfg['min_bitget_volume'])}"
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
        run_ts=run_ts,
        horizon_min=args.horizon_min,
    )
    if picks:
        state["recommendation_history"].extend(picks)
        state["recommendation_history"] = state["recommendation_history"][-5000:]

    pending = state["pending"] + picks
    pending, finalized = evaluate_pending(
        pending=pending,
        bithumb=bithumb,
        bitget=bitget,
        now=run_ts,
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

    calibrate_notes: List[str] = []
    calibrated = False
    if should_calibrate(
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

    if not args.dry_run:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if not token or not chat_id:
            print("[ERROR] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID is not set")
            return 2
        try:
            send_telegram(token=token, chat_id=chat_id, text=msg)
            print("[INFO] telegram sent")
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
