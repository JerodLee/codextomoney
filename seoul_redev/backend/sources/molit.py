"""국토교통부 실거래가 공개시스템 API 클라이언트 (아파트 매매).

서비스키(MOLIT_SERVICE_KEY)가 있으면 실데이터를, 없으면 빈 결과를 반환한다.
호출 결과는 (LAWD_CD, DEAL_YMD) 단위로 메모리 캐시된다.
"""
from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict

import httpx

from config import (
    CACHE_TTL_SECONDS,
    HTTP_TIMEOUT,
    MOLIT_APT_TRADE_URL,
    MOLIT_SERVICE_KEY,
    has_molit_key,
)


@dataclass
class Trade:
    apt: str
    dong: str  # 법정동 (umdNm)
    jibun: str
    area_m2: float  # 전용면적
    floor: int
    build_year: int
    deal_date: str  # YYYY-MM-DD
    amount_manwon: int  # 거래금액(만원)

    @property
    def price_per_m2(self) -> float:
        return round(self.amount_manwon / self.area_m2, 1) if self.area_m2 else 0.0


# (lawd_cd, deal_ymd) -> (fetched_at, [Trade])
_cache: dict[tuple[str, str], tuple[float, list[Trade]]] = {}


def _text(node: ET.Element, tag: str, default: str = "") -> str:
    el = node.find(tag)
    return (el.text or default).strip() if el is not None and el.text else default


def _parse_items(xml_text: str) -> list[Trade]:
    trades: list[Trade] = []
    root = ET.fromstring(xml_text)
    for item in root.iter("item"):
        try:
            amount = int(_text(item, "dealAmount").replace(",", "").strip() or "0")
            year = _text(item, "dealYear")
            month = _text(item, "dealMonth").zfill(2)
            day = _text(item, "dealDay").zfill(2)
            area = float(_text(item, "excluUseAr") or "0")
            trades.append(
                Trade(
                    apt=_text(item, "aptNm"),
                    dong=_text(item, "umdNm"),
                    jibun=_text(item, "jibun"),
                    area_m2=area,
                    floor=int(_text(item, "floor") or "0"),
                    build_year=int(_text(item, "buildYear") or "0"),
                    deal_date=f"{year}-{month}-{day}" if year else "",
                    amount_manwon=amount,
                )
            )
        except (ValueError, AttributeError):
            continue
    return trades


def fetch_apt_trades(lawd_cd: str, deal_ymd: str) -> list[Trade]:
    """LAWD_CD(시군구 5자리) + DEAL_YMD(YYYYMM) 한 달치 아파트 매매 실거래."""
    if not has_molit_key():
        return []

    key = (lawd_cd, deal_ymd)
    now = time.time()
    cached = _cache.get(key)
    if cached and now - cached[0] < CACHE_TTL_SECONDS:
        return cached[1]

    params = {
        "serviceKey": MOLIT_SERVICE_KEY,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "numOfRows": "1000",
        "pageNo": "1",
    }
    try:
        resp = httpx.get(MOLIT_APT_TRADE_URL, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        trades = _parse_items(resp.text)
    except (httpx.HTTPError, ET.ParseError):
        # 네트워크/파싱 오류 시 빈 결과(대시보드는 시드 시세로 폴백).
        return _cache.get(key, (0, []))[1]

    _cache[key] = (now, trades)
    return trades


def trade_to_dict(t: Trade) -> dict:
    d = asdict(t)
    d["price_per_m2"] = t.price_per_m2
    return d
