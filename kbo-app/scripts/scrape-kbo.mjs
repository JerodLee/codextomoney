#!/usr/bin/env node
// KBO 순위 스크레이퍼 (소스: Wikipedia · 사실 데이터 · CC BY-SA)
// - MediaWiki API로 "YYYY KBO League season" 문서의 위키텍스트를 받아 순위표를 파싱.
// - 결과를 앱 데이터셋 형태 {teams:{id:{w,l,t}}, updatedAt} 로 만들어
//   (기본) data/dataset.json 에 저장하거나, --post 로 서버 /api/data 에 반영.
//
// ⚠ 이 개발 샌드박스는 외부망이 막혀 있어 실행이 안 됩니다. EC2/로컬(개방망)에서 실행하세요.
// ⚠ 위키 표 레이아웃은 시즌마다 다를 수 있습니다. 아래 COLS(열 위치)만 맞추면 됩니다.
//
// 사용:
//   node scripts/scrape-kbo.mjs                          # 위키 수집 → data/dataset.json
//   node scripts/scrape-kbo.mjs --year 2025 --post       # 수집 후 /api/data 반영 (ADMIN_TOKEN 필요)
//   node scripts/scrape-kbo.mjs --source fixture --file sample.wikitext   # 파서 테스트(오프라인)
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const args = Object.fromEntries(process.argv.slice(2).reduce((a, x, i, arr) => {
  if (x.startsWith("--")) a.push([x.slice(2), arr[i + 1] && !arr[i + 1].startsWith("--") ? arr[i + 1] : true]);
  return a;
}, []));

const YEAR = args.year || "2025";
const SOURCE = args.source || "wikipedia";
const BASE_URL = process.env.BASE_URL || "http://127.0.0.1:3000";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "";
const OUT = args.out || path.join(__dirname, "..", "data", "dataset.json");

// 위키 표의 (0-index) 열 위치 — 표 레이아웃이 바뀌면 여기만 조정.
// 일반적 KBO 순위표: [순위, 팀, 경기, 승, 패, 무, 승률, 게임차]
const COLS = { team: 1, w: 3, l: 4, t: 5 };

// 팀 별칭 → 앱 팀 id (영문 위키 표기 기준)
const TEAM_ALIASES = {
  lg: ["LG Twins", "LG"], hh: ["Hanwha Eagles", "Hanwha"], ss: ["Samsung Lions", "Samsung"],
  kia: ["KIA Tigers", "Kia Tigers", "KIA", "Kia"], ssg: ["SSG Landers", "SSG"],
  lt: ["Lotte Giants", "Lotte"], kt: ["KT Wiz", "kt Wiz", "KT"], ds: ["Doosan Bears", "Doosan"],
  nc: ["NC Dinos", "NC Dinos", "NC"], kw: ["Kiwoom Heroes", "Kiwoom"],
};
function teamIdFromText(txt) {
  const t = txt.toLowerCase();
  for (const [id, aliases] of Object.entries(TEAM_ALIASES)) {
    if (aliases.some(a => t.includes(a.toLowerCase()))) return id;
  }
  return null;
}

async function getWikitext(year) {
  const title = `${year} KBO League season`;
  const url = `https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=revisions&rvprop=content&rvslots=main&titles=${encodeURIComponent(title)}`;
  const r = await fetch(url, { headers: { "User-Agent": "kbo-fan-app/1.0 (educational; contact via repo)" } });
  if (!r.ok) throw new Error("wiki fetch " + r.status);
  const j = await r.json();
  const page = j.query?.pages?.[0];
  const content = page?.revisions?.[0]?.slots?.main?.content;
  if (!content) throw new Error("문서 본문을 찾을 수 없음: " + title);
  return content;
}

// --- 위키텍스트 표 파서 (best-effort) ---
function cleanCell(c) {
  let s = c;
  if (s.includes("|")) s = s.slice(s.lastIndexOf("|") + 1); // style="..."| value → value
  s = s.replace(/\[\[[^\]|]*\|([^\]]*)\]\]/g, "$1").replace(/\[\[([^\]]*)\]\]/g, "$1"); // links
  s = s.replace(/\{\{[^}]*\}\}/g, " ").replace(/<ref[^>]*>.*?<\/ref>/gs, "").replace(/<[^>]+>/g, "");
  s = s.replace(/'''?/g, "").replace(/&nbsp;/g, " ").trim();
  return s;
}
function rowCells(rowText) {
  let s = rowText.replace(/^\s*[|!]/, "");
  s = s.replace(/\n\s*[|!]/g, "||");
  return s.split("||").map(cleanCell);
}
function parseStandings(wikitext) {
  const tables = wikitext.split(/\{\|/).slice(1).map(t => "{|" + t.split(/\n\|\}/)[0]);
  let best = null, bestScore = 0;
  for (const tbl of tables) {
    const rows = tbl.split(/\|-/).map(r => r.trim());
    const parsed = [];
    for (const row of rows) {
      const cells = rowCells(row);
      const teamCell = cells[COLS.team];
      if (!teamCell) continue;
      const id = teamIdFromText(teamCell);
      if (!id) continue;
      const w = parseInt(cells[COLS.w], 10), l = parseInt(cells[COLS.l], 10), t = parseInt(cells[COLS.t], 10);
      if (Number.isFinite(w) && Number.isFinite(l)) parsed.push({ id, w, l, t: Number.isFinite(t) ? t : 0 });
    }
    if (parsed.length > bestScore) { bestScore = parsed.length; best = parsed; }
  }
  return best || [];
}

function toDataset(rows) {
  const teams = {};
  for (const r of rows) teams[r.id] = { w: r.w, l: r.l, t: r.t };
  return { teams, updatedAt: new Date().toISOString(), source: `Wikipedia: ${YEAR} KBO League season` };
}

async function main() {
  let wikitext;
  if (SOURCE === "fixture") {
    if (!args.file) throw new Error("--file <path> 가 필요합니다");
    wikitext = fs.readFileSync(args.file, "utf8");
  } else {
    wikitext = await getWikitext(YEAR);
  }
  const rows = parseStandings(wikitext);
  if (!rows.length) { console.error("[scrape] 순위 행을 못 찾음 — COLS 열 위치를 조정하세요."); process.exit(2); }
  console.log("[scrape] 파싱된 구단:", rows.length, "→", rows.map(r => `${r.id}:${r.w}-${r.l}-${r.t}`).join(" "));
  const dataset = toDataset(rows);

  if (args.post) {
    if (!ADMIN_TOKEN) throw new Error("--post 에는 ADMIN_TOKEN 이 필요합니다");
    const res = await fetch(BASE_URL + "/api/data", {
      method: "POST", headers: { "content-type": "application/json", "x-admin-token": ADMIN_TOKEN },
      body: JSON.stringify(dataset),
    });
    console.log("[scrape] POST /api/data ->", res.status, (await res.text()).slice(0, 120));
    if (!res.ok) process.exit(1);
  } else {
    fs.mkdirSync(path.dirname(OUT), { recursive: true });
    fs.writeFileSync(OUT, JSON.stringify(dataset, null, 2));
    console.log("[scrape] 저장:", OUT);
  }
}
main().catch(e => { console.error("[scrape] 실패:", e.message); process.exit(1); });
