"""Configuration for the Seoul redevelopment dashboard backend.

All settings are read from environment variables (optionally a local .env file).
The app runs fully on bundled seed data when no API key is supplied; supplying a
MOLIT (국토교통부 실거래가) service key enables live transaction-price enrichment.
"""
from __future__ import annotations

import os
from pathlib import Path

try:  # optional, only needed for local dev convenience
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover - dotenv is optional
    pass

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
STATE_DIR = BASE_DIR / "state"
STATE_DIR.mkdir(exist_ok=True)

FRONTEND_DIR = BASE_DIR.parent / "frontend"

# 국토교통부 실거래가 공개시스템 서비스키 (공공데이터포털 data.go.kr 발급).
# 일반 인증키(Decoding) 권장. 미설정 시 시드 시세로 동작.
MOLIT_SERVICE_KEY = os.getenv("MOLIT_SERVICE_KEY", "").strip()

# 아파트 매매 실거래가 상세 자료 엔드포인트.
MOLIT_APT_TRADE_URL = os.getenv(
    "MOLIT_APT_TRADE_URL",
    "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
)

# 실거래가 응답/구역 데이터 캐시 TTL(초).
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "1800"))

# 변동 모니터링 시 가격 급변으로 간주할 임계치(%).
PRICE_ALERT_THRESHOLD_PCT = float(os.getenv("PRICE_ALERT_THRESHOLD_PCT", "5"))

# 선택: 텔레그램 알림 (기존 모멘텀 봇 인프라와 동일 포맷).
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "15"))


def has_molit_key() -> bool:
    return bool(MOLIT_SERVICE_KEY)
