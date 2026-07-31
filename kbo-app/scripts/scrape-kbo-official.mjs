#!/usr/bin/env node
// KBO 순위 스크레이퍼 — 출처: KBO 공식 기록실 (koreabaseball.com)
//
// 기존 위키피디아 스크레이퍼(scrape-kbo.mjs)를 대체한다. 위키는 갱신이 늦고
// 표 레이아웃이 시즌마다 바뀌어, 팬이 공식 기록과 대조하면 어긋나는 순간이 온다.
//
// 이 스크립트의 두 가지 원칙:
//  1) 열 위치를 고정하지 않는다 — 헤더(경기/승/패/무)를 읽어 열 인덱스를 찾는다.
//     표에 열이 추가·삭제돼도 깨지지 않는다.
//  2) 검증을 통과하지 못한 데이터는 절대 반영하지 않는다 (--post 포함).
//
// 사용:
//   node scripts/scrape-kbo-official.mjs                    # 수집 → data/dataset.json
//   node scripts/scrape-kbo-official.mjs --post             # 수집 → 서버 /api/data (ADMIN_TOKEN 필요)
//   node scripts/scrape-kbo-official.mjs --file fixture.html  # 오프라인 파서 테스트
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { validateStandings, formatReport } from "./lib/validate-standings.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const args = Object.fromEntries(process.argv.slice(2).reduce((a, x, i, arr) => {
  if (x.startsWith("--")) a.push([x.slice(2), arr[i + 1] && !arr[i + 1].startsWith("--") ? arr[i + 1] : true]);
  return a;
}, []));

const SRC_URL = args.url || "https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx";
const BASE_URL = process.env.BASE_URL || "http://127.0.0.1:3000";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "";
const OUT = args.out || path.join(__dirname, "..", "data", "dataset.json");

// KBO 공식 표기(한글) → 앱 팀 id
const TEAM_ALIASES = {
  lg: ["LG"], hh: ["한화"], ss: ["삼성"], kia: ["KIA"], ssg: ["SSG"],
  lt: ["롯데"], kt: ["KT"], ds: ["두산"], nc: ["NC"], kw: ["키움"],
};
function teamIdFromText(txt) {
  const t = (txt || "").trim();
  for (const [id, aliases] of Object.entries(TEAM_ALIASES)) {
    if (aliases.some((a) => t.toUpperCase().includes(a.toUpperCase()))) return id;
  }
  return null;
}

// ---------- 아주 작은 HTML 표 파서 (의존성 없이) ----------
const stripTags = (s) => s.replace(/<[^>]*>/g, " ").replace(/&nbsp;/g, " ")
  .replace(/&amp;/g, "&").replace(/\s+/g, " ").trim();

function extractTables(html) {
  const out = [];
  const re = /<table[\s\S]*?<\/table>/gi;
  let m;
  while ((m = re.exec(html))) out.push(m[0]);
  return out;
}
function extractRows(tableHtml) {
  const rows = [];
  const re = /<tr[\s\S]*?<\/tr>/gi;
  let m;
  while ((m = re.exec(tableHtml))) {
    const cells = [];
    const cre = /<(t[hd])\b[^>]*>([\s\S]*?)<\/\1>/gi;
    let c;
    while ((c = cre.exec(m[0]))) cells.push(stripTags(c[2]));
    if (cells.length) rows.push(cells);
  }
  return rows;
}

// 헤더 행에서 필요한 열의 인덱스를 찾는다 (열 순서 변경에 견디도록)
const HEADER_KEYS = {
  team: ["팀명", "팀"],
  gp: ["경기수", "경기"],
  w: ["승"],
  l: ["패"],
  t: ["무"],
  pct: ["승률"],
};
function findHeaderMap(rows) {
  for (let i = 0; i < rows.length; i++) {
    const cells = rows[i];
    const map = {};
    for (const [key, names] of Object.entries(HEADER_KEYS)) {
      // 정확히 일치 우선 ("승"이 "승률"을 먹지 않도록)
      let idx = cells.findIndex((c) => names.includes(c));
      if (idx < 0) idx = cells.findIndex((c) => names.some((n) => c === n));
      if (idx >= 0) map[key] = idx;
    }
    if (map.team != null && map.w != null && map.l != null) return { headerRow: i, map };
  }
  return null;
}

export function parseStandings(html) {
  for (const tbl of extractTables(html)) {
    const rows = extractRows(tbl);
    const found = findHeaderMap(rows);
    if (!found) continue;
    const { headerRow, map } = found;
    const teams = {};
    for (let i = headerRow + 1; i < rows.length; i++) {
      const cells = rows[i];
      const id = teamIdFromText(cells[map.team]);
      if (!id) continue;
      const num = (idx) => {
        if (idx == null) return null;
        const v = parseInt(String(cells[idx]).replace(/[^\d-]/g, ""), 10);
        return Number.isFinite(v) ? v : null;
      };
      const w = num(map.w), l = num(map.l);
      const t = map.t != null ? (num(map.t) ?? 0) : 0;
      if (w == null || l == null) continue;
      const rec = { w, l, t };
      if (map.pct != null) {
        const p = parseFloat(String(cells[map.pct]).replace(/[^\d.]/g, ""));
        if (Number.isFinite(p)) rec.pct = p;
      }
      teams[id] = rec;
    }
    if (Object.keys(teams).length) return teams;
  }
  return {};
}

async function fetchHtml(url) {
  const r = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (compatible; kbo-fan-app/1.0; personal use)",
      "Accept-Language": "ko-KR,ko;q=0.9",
    },
  });
  if (!r.ok) throw new Error(`fetch ${r.status} ${url}`);
  return r.text();
}

async function main() {
  const html = args.file ? fs.readFileSync(args.file, "utf8") : await fetchHtml(SRC_URL);
  const teams = parseStandings(html);

  if (!Object.keys(teams).length) {
    console.error("[scrape] 순위 행을 찾지 못했습니다 — 표 구조가 바뀌었을 수 있습니다.");
    process.exit(2);
  }

  const dataset = {
    teams,
    updatedAt: new Date().toISOString(),
    source: "KBO 공식 기록실 (koreabaseball.com)",
    sourceUrl: SRC_URL,
  };

  // 검증 실패 시 여기서 멈춘다. 틀린 데이터를 올리느니 옛 데이터를 두는 편이 낫다.
  const v = validateStandings(dataset);
  console.log(formatReport(v));
  console.log("[scrape] " + Object.entries(teams).map(([id, r]) => `${id}:${r.w}-${r.l}-${r.t}`).join(" "));
  if (!v.ok) {
    console.error("[scrape] 검증 실패 — 반영하지 않고 종료합니다.");
    process.exit(3);
  }

  if (args.post) {
    if (!ADMIN_TOKEN) throw new Error("--post 에는 ADMIN_TOKEN 이 필요합니다");
    const res = await fetch(BASE_URL + "/api/data", {
      method: "POST",
      headers: { "content-type": "application/json", "x-admin-token": ADMIN_TOKEN },
      body: JSON.stringify(dataset),
    });
    const txt = await res.text();
    console.log("[scrape] POST /api/data ->", res.status, txt.slice(0, 200));
    if (!res.ok) process.exit(1);
  } else {
    fs.mkdirSync(path.dirname(OUT), { recursive: true });
    fs.writeFileSync(OUT, JSON.stringify(dataset, null, 2));
    console.log("[scrape] 저장:", OUT);
  }
}

// 파서만 import 할 때는 실행하지 않는다
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => { console.error("[scrape] 실패:", e.message); process.exit(1); });
}
