"""서울시 재개발/신통기획/모아타운 모니터링 대시보드 — FastAPI 백엔드."""
from __future__ import annotations

import sys
from pathlib import Path

# 패키지 형태가 아니어도 import가 되도록 백엔드 디렉터리를 경로에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from config import FRONTEND_DIR, has_molit_key
from data.districts import SEOUL_DISTRICTS
from sources.areas import get_area, list_areas, load_areas, price_summary, area_trades
from sources.molit import trade_to_dict
from services.monitor import capture_snapshot, recent_changes

app = FastAPI(title="서울 재개발 모니터링 대시보드", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health():
    meta = load_areas()["meta"]
    return {
        "status": "ok",
        "molit_live": has_molit_key(),
        "data_version": meta.get("version"),
        "area_count": len(load_areas()["areas"]),
    }


@app.get("/api/districts")
def districts():
    counts: dict[str, int] = {}
    for a in load_areas()["areas"]:
        counts[a["district"]] = counts.get(a["district"], 0) + 1
    return [
        {"name": name, "code": code, "area_count": counts.get(name, 0)}
        for name, code in SEOUL_DISTRICTS.items()
    ]


@app.get("/api/meta")
def meta():
    data = load_areas()
    types: dict[str, int] = {}
    stages: dict[str, int] = {}
    for a in data["areas"]:
        types[a["type"]] = types.get(a["type"], 0) + 1
        stages[a["stage"]] = stages.get(a["stage"], 0) + 1
    return {
        **data["meta"],
        "types": types,
        "stages": stages,
        "molit_live": has_molit_key(),
    }


@app.get("/api/areas")
def areas(
    district: str | None = Query(None),
    type: str | None = Query(None),
    stage: str | None = Query(None),
    q: str | None = Query(None),
):
    return list_areas(district=district, type_=type, stage=stage, q=q)


@app.get("/api/areas/{area_id}")
def area_detail(area_id: str, months: int = Query(12, ge=1, le=36)):
    area = get_area(area_id)
    if not area:
        raise HTTPException(status_code=404, detail="area not found")
    return {**area, "price": price_summary(area, months=months)}


@app.get("/api/areas/{area_id}/trades")
def area_trades_endpoint(area_id: str, months: int = Query(12, ge=1, le=36)):
    area = get_area(area_id)
    if not area:
        raise HTTPException(status_code=404, detail="area not found")
    trades, source = area_trades(area, months=months)
    return {"data_source": source, "count": len(trades),
            "trades": [trade_to_dict(t) for t in trades]}


@app.post("/api/monitor/snapshot")
def monitor_snapshot():
    return capture_snapshot()


@app.get("/api/monitor/changes")
def monitor_changes(limit: int = Query(50, ge=1, le=500)):
    return recent_changes(limit=limit)


# ---- 정적 프론트엔드 서빙 ----
if FRONTEND_DIR.exists():
    @app.get("/")
    def index():
        return FileResponse(FRONTEND_DIR / "index.html")

    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
