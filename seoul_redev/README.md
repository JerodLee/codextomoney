# 서울 재개발 모니터링 대시보드

서울시의 **재개발 정비구역 · 신속통합기획(신통기획) · 모아타운** 현황과
해당 지역의 **실거래가/시세 추이**를 지도·표·차트로 한눈에 보고, 단계 변경과
시세 급변을 **지속 모니터링**하는 대시보드입니다.

## 구성

```
seoul_redev/
├─ backend/                FastAPI 백엔드 (REST API + 정적 프론트 서빙)
│  ├─ app.py               엔드포인트
│  ├─ config.py            환경변수 설정
│  ├─ data/
│  │  ├─ areas.json        구역 시드 데이터셋(공개정보 기반)
│  │  └─ districts.py      서울 25개 자치구 법정동코드
│  ├─ sources/
│  │  ├─ molit.py          국토부 실거래가 API 클라이언트
│  │  └─ areas.py          구역 필터링 + 시세 집계
│  ├─ services/monitor.py  스냅샷/변동 감지/텔레그램 알림
│  ├─ state/               스냅샷·변동 내역 저장
│  └─ Dockerfile
├─ frontend/               지도(Leaflet) + 차트(Chart.js) 정적 SPA
└─ scripts/monitor_run.py  주기 모니터링 실행 스크립트
```

## 빠른 시작 (로컬)

```bash
cd seoul_redev/backend
pip install -r requirements.txt
# (선택) 실거래가 연동: cp .env.example .env  후 MOLIT_SERVICE_KEY 입력
uvicorn app:app --reload
```

브라우저에서 http://localhost:8000 접속.

### Docker

```bash
cd seoul_redev
docker build -f backend/Dockerfile -t seoul-redev .
docker run -p 8000:8000 -e MOLIT_SERVICE_KEY=발급키 seoul-redev
```

## 데이터 소스

| 항목 | 소스 | 비고 |
|------|------|------|
| 실거래가/시세 | 국토교통부 실거래가 공개시스템 API (data.go.kr) | `MOLIT_SERVICE_KEY` 필요 |
| 구역 현황 | 서울시 정비사업 정보몽땅(cleanup.seoul.go.kr), 서울시 보도자료 | 시드 데이터셋, 주기 갱신 |

- **키가 없으면** 자치구별 기준 평단가에 기반한 **시드 추정 시세**로 동작하며,
  API 응답·화면에 `seed_estimate`로 명확히 표기됩니다.
- **키가 있으면** 각 구역의 법정동에 해당하는 아파트 매매 실거래를 수집해
  월별 평단가/추이/최근 거래를 보여줍니다(`molit_live`).

### 실거래가 API 키 발급
1. [공공데이터포털](https://www.data.go.kr) 회원가입
2. "아파트 매매 실거래가 상세 자료" 활용신청
3. 발급된 **일반 인증키(Decoding)** 를 `MOLIT_SERVICE_KEY` 에 설정

## API

| Method | Path | 설명 |
|--------|------|------|
| GET | `/api/health` | 상태/데이터 버전 |
| GET | `/api/meta` | 유형·단계 통계, LIVE 여부 |
| GET | `/api/districts` | 자치구 목록 + 구역 수 |
| GET | `/api/areas?district=&type=&stage=&q=` | 구역 목록(필터·검색) |
| GET | `/api/areas/{id}?months=12` | 구역 상세 + 시세 요약 |
| GET | `/api/areas/{id}/trades?months=12` | 구역 인근 실거래 목록 |
| POST | `/api/monitor/snapshot` | 스냅샷 캡처 + 변동 감지 |
| GET | `/api/monitor/changes?limit=50` | 최근 변동 내역 |

## 변동 모니터링

`.github/workflows/seoul-redev-monitor.yml` 가 6시간마다
`scripts/monitor_run.py` 를 실행해 스냅샷을 만들고, 직전 대비
**단계 변경**·**평단가 급변(기본 ±5%)** 을 `backend/state/changes.json` 에
누적·커밋합니다. 다음 시크릿을 저장소에 등록하면 텔레그램 알림도 발송됩니다.

- `MOLIT_SERVICE_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

## 한계 / TODO

- 구역 좌표·단계는 시드 근사값입니다. 정확도를 높이려면 정비몽땅 고시 데이터로
  `data/areas.json` 을 정기 갱신하거나, 서울 열린데이터광장 API로 자동화하세요.
- 실거래가는 아파트 매매 기준입니다. 연립·다세대/단독은 별도 엔드포인트
  (`getRTMSDataSvcRHTrade` 등) 추가로 확장 가능합니다.
- 매물(호가) 현황은 공개 API가 없어 미포함(스크래핑은 ToS 이슈). 필요 시
  KB부동산/부동산원 시세 API 연동을 검토하세요.
