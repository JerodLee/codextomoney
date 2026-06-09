"""구역(재개발/신통기획/모아타운) 데이터 로딩·필터링 및 시세 집계."""
from __future__ import annotations

import hashlib
import json
import time
from datetime import date

from config import CACHE_TTL_SECONDS, DATA_DIR, has_molit_key
from data.districts import district_code
from sources.molit import Trade, fetch_apt_trades, trade_to_dict

_areas_cache: tuple[float, dict] | None = None

# 자치구별 대략적 아파트 평균 평단가(만원/3.3㎡) — 시드 추정용 기준값.
# 실거래가 키가 없을 때만 사용하며, 응답에 data_source="seed_estimate"로 표기된다.
_DISTRICT_BASE_PYEONG = {
    "강남구": 8200, "서초구": 7600, "송파구": 5600, "용산구": 6200, "성동구": 5200,
    "마포구": 4700, "광진구": 4500, "양천구": 4200, "영등포구": 4300, "동작구": 4400,
    "강동구": 4100, "종로구": 4000, "중구": 4100, "서대문구": 3700, "동대문구": 3600,
    "성북구": 3500, "강서구": 3600, "관악구": 3300, "은평구": 3300, "노원구": 3200,
    "구로구": 3200, "중랑구": 3000, "강북구": 2900, "도봉구": 2900, "금천구": 3100,
}

_PYEONG = 3.305785  # 1평 = 3.305785㎡


def load_areas() -> dict:
    global _areas_cache
    now = time.time()
    if _areas_cache and now - _areas_cache[0] < CACHE_TTL_SECONDS:
        return _areas_cache[1]
    with open(DATA_DIR / "areas.json", encoding="utf-8") as f:
        data = json.load(f)
    _areas_cache = (now, data)
    return data


def list_areas(district: str | None = None, type_: str | None = None,
               stage: str | None = None, q: str | None = None) -> list[dict]:
    areas = load_areas()["areas"]
    out = []
    for a in areas:
        if district and a["district"] != district:
            continue
        if type_ and a["type"] != type_:
            continue
        if stage and a["stage"] != stage:
            continue
        if q:
            hay = f'{a["name"]} {a["district"]} {a["dong"]} {a.get("tag","")}'.lower()
            if q.lower() not in hay:
                continue
        out.append(a)
    return out


def get_area(area_id: str) -> dict | None:
    for a in load_areas()["areas"]:
        if a["id"] == area_id:
            return a
    return None


def recent_months(n: int) -> list[str]:
    """최근 n개월 YYYYMM 리스트 (최신순)."""
    today = date.today()
    y, m = today.year, today.month
    out = []
    for _ in range(n):
        out.append(f"{y}{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out


def _seed_trades(area: dict, months: int) -> list[Trade]:
    """실거래가 키가 없을 때 사용하는 결정론적 추정 거래(라벨 명시)."""
    base_pyeong = _DISTRICT_BASE_PYEONG.get(area["district"], 3300)
    # 구역 id로 결정론적 변동 부여
    seed = int(hashlib.md5(area["id"].encode()).hexdigest(), 16)
    rng = (seed % 1000) / 1000.0
    base_per_m2 = (base_pyeong / _PYEONG) * (0.9 + rng * 0.2)
    trades: list[Trade] = []
    for i, ym in enumerate(recent_months(months)):
        y, m = int(ym[:4]), int(ym[4:])
        # 완만한 상승 추세 + 월별 노이즈
        trend = 1.0 + (months - i) * 0.004
        noise = 0.97 + ((seed >> (i % 16)) & 7) / 100.0
        per_m2 = base_per_m2 * trend * noise
        for k in range(2):  # 월 2건 가량
            area_m2 = 59.9 if k == 0 else 84.9
            amount = int(per_m2 * area_m2)
            trades.append(Trade(
                apt=f'{area["dong"]} (추정)', dong=area["dong"], jibun="-",
                area_m2=area_m2, floor=5 + k * 4, build_year=2005,
                deal_date=f"{y}-{m:02d}-{10 + k*8:02d}", amount_manwon=amount,
            ))
    return trades


def area_trades(area: dict, months: int = 12) -> tuple[list[Trade], str]:
    """구역 인근(법정동 일치) 실거래 + 데이터 출처 라벨 반환."""
    code = district_code(area["district"])
    if has_molit_key() and code:
        collected: list[Trade] = []
        for ym in recent_months(months):
            for t in fetch_apt_trades(code, ym):
                if t.dong == area["dong"]:
                    collected.append(t)
        if collected:
            collected.sort(key=lambda t: t.deal_date, reverse=True)
            return collected, "molit_live"
        # 키는 있으나 해당 동 거래가 없으면 추정으로 폴백
        return _seed_trades(area, months), "seed_estimate"
    return _seed_trades(area, months), "seed_estimate"


def price_summary(area: dict, months: int = 12) -> dict:
    trades, source = area_trades(area, months)
    if not trades:
        return {"data_source": source, "count": 0, "monthly": [], "recent": []}

    # 월별 평균 평단가(만원/평)
    buckets: dict[str, list[float]] = {}
    for t in trades:
        ym = t.deal_date[:7]  # YYYY-MM
        buckets.setdefault(ym, []).append(t.price_per_m2 * _PYEONG)
    monthly = [
        {"month": ym, "avg_pyeong": round(sum(v) / len(v)), "count": len(v)}
        for ym, v in sorted(buckets.items())
    ]

    all_pyeong = [t.price_per_m2 * _PYEONG for t in trades]
    avg = round(sum(all_pyeong) / len(all_pyeong))
    change_pct = None
    if len(monthly) >= 2 and monthly[0]["avg_pyeong"]:
        change_pct = round(
            (monthly[-1]["avg_pyeong"] - monthly[0]["avg_pyeong"])
            / monthly[0]["avg_pyeong"] * 100, 1
        )

    return {
        "data_source": source,
        "count": len(trades),
        "avg_pyeong": avg,
        "min_pyeong": round(min(all_pyeong)),
        "max_pyeong": round(max(all_pyeong)),
        "change_pct": change_pct,
        "monthly": monthly,
        "recent": [trade_to_dict(t) for t in trades[:10]],
    }
