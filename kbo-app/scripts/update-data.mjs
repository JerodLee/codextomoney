#!/usr/bin/env node
// KBO 데이터 정기 갱신기 — cron/systemd 타이머로 주기 실행.
// 우선순위: DATA_SOURCE_URL(JSON 피드) > SEED_FILE(로컬 JSON) > (미설정 시 no-op).
// 실제 "라이브" 최신 데이터를 원하면 DATA_SOURCE_URL 을 본인 피드로 지정하세요
// (예: KBO 스탯을 모아둔 Google Sheet를 JSON으로 export 하는 URL, 자체 스크레이퍼 결과 등).
import fs from "fs";

const BASE = process.env.BASE_URL || "http://127.0.0.1:3000";
const TOKEN = process.env.ADMIN_TOKEN || "";
const SOURCE = process.env.DATA_SOURCE_URL || "";
const SEED = process.env.SEED_FILE || "";

if (!TOKEN) { console.error("ERROR: ADMIN_TOKEN 환경변수가 필요합니다."); process.exit(1); }

async function loadDataset() {
  if (SOURCE) {
    const r = await fetch(SOURCE);
    if (!r.ok) throw new Error("source fetch 실패: " + r.status);
    console.log("[update] source 로드:", SOURCE);
    return await r.json();
  }
  if (SEED && fs.existsSync(SEED)) {
    console.log("[update] seed 파일 로드:", SEED);
    return JSON.parse(fs.readFileSync(SEED, "utf8"));
  }
  return null;
}

async function main() {
  const dataset = await loadDataset();
  if (!dataset) { console.log("[update] DATA_SOURCE_URL/SEED_FILE 미설정 — 갱신 건너뜀(no-op)."); return; }
  dataset.updatedAt = new Date().toISOString();
  const res = await fetch(BASE + "/api/data", {
    method: "POST",
    headers: { "content-type": "application/json", "x-admin-token": TOKEN },
    body: JSON.stringify(dataset),
  });
  console.log("[update] POST /api/data ->", res.status, (await res.text()).slice(0, 200));
  if (!res.ok) process.exit(1);
}
main().catch((e) => { console.error("[update] 실패:", e.message); process.exit(1); });
