"""변동 모니터링: 구역 단계/시세 스냅샷을 저장하고 변동을 감지한다.

GitHub Actions(또는 cron)에서 주기적으로 capture_snapshot()을 호출하면
이전 스냅샷과 비교해 단계 변경/가격 급변 이벤트를 누적하고,
텔레그램 설정이 있으면 알림을 보낸다.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import httpx

from config import (
    PRICE_ALERT_THRESHOLD_PCT,
    STATE_DIR,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
)
from sources.areas import list_areas, get_area, price_summary

SNAPSHOT_FILE = STATE_DIR / "snapshot.json"
CHANGES_FILE = STATE_DIR / "changes.json"
MAX_CHANGES = 500


def _load(path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return default
    return default


def _save(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_snapshot() -> dict:
    snap = {}
    for a in list_areas():
        summary = price_summary(a, months=6)
        snap[a["id"]] = {
            "name": a["name"],
            "type": a["type"],
            "district": a["district"],
            "stage": a["stage"],
            "avg_pyeong": summary.get("avg_pyeong"),
            "data_source": summary.get("data_source"),
        }
    return snap


def capture_snapshot() -> dict:
    """현재 상태 스냅샷을 만들고 직전 스냅샷과 비교해 변동을 기록한다."""
    prev = _load(SNAPSHOT_FILE, {})
    current = _build_snapshot()
    now_iso = datetime.now(timezone.utc).isoformat()

    new_changes = []
    for area_id, cur in current.items():
        old = prev.get(area_id)
        if not old:
            continue
        # 단계 변경
        if old.get("stage") != cur.get("stage"):
            new_changes.append({
                "ts": now_iso, "area_id": area_id, "name": cur["name"],
                "district": cur["district"], "type": cur["type"], "kind": "stage",
                "from": old.get("stage"), "to": cur.get("stage"),
            })
        # 가격 급변
        o, c = old.get("avg_pyeong"), cur.get("avg_pyeong")
        if o and c and o > 0:
            pct = round((c - o) / o * 100, 1)
            if abs(pct) >= PRICE_ALERT_THRESHOLD_PCT:
                new_changes.append({
                    "ts": now_iso, "area_id": area_id, "name": cur["name"],
                    "district": cur["district"], "type": cur["type"], "kind": "price",
                    "from": o, "to": c, "pct": pct,
                })

    if new_changes:
        log = _load(CHANGES_FILE, [])
        log = (new_changes + log)[:MAX_CHANGES]
        _save(CHANGES_FILE, log)
        _notify(new_changes)

    _save(SNAPSHOT_FILE, current)
    return {"snapshot_at": now_iso, "areas": len(current), "new_changes": len(new_changes)}


def recent_changes(limit: int = 50) -> list[dict]:
    return _load(CHANGES_FILE, [])[:limit]


def _format_change(c: dict) -> str:
    if c["kind"] == "stage":
        return f'🏗️ [{c["district"]}] {c["name"]} 단계 변경: {c["from"]} → {c["to"]}'
    arrow = "📈" if c.get("pct", 0) > 0 else "📉"
    return (f'{arrow} [{c["district"]}] {c["name"]} 평단가 {c["from"]}→{c["to"]}만원 '
            f'({c["pct"]:+.1f}%)')


def _notify(changes: list[dict]) -> None:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    lines = ["*서울 재개발 모니터링 알림*", ""]
    lines += [_format_change(c) for c in changes[:20]]
    text = "\n".join(lines)
    try:
        httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except httpx.HTTPError:
        pass
