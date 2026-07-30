#!/usr/bin/env node
// 선수 이슈 스크레이퍼 — 경기에 영향을 주는 소식(부상·선발·라인업·복귀·이적)을 모은다.
//
// 소스: Google News RSS (기계 판독용 공개 피드). 제목·출처·링크만 저장하고 본문은 복사하지 않으며,
//       항상 원문 링크로 보낸다. 커뮤니티(MLBPARK·디시 등) 본문 스크레이핑은 하지 않는다
//       (약관·저작권 문제 + 미확인 루머로 실존 인물의 건강 정보를 사실처럼 유통할 위험).
//       커뮤니티는 앱에서 '키워드 딥링크'로 열도록 되어 있다.
//
// ⚠ 이 개발 샌드박스는 외부망이 막혀 있어 실행되지 않습니다. EC2/로컬(개방망)에서 실행하세요.
//
// 사용:
//   node scripts/scrape-issues.mjs                       # 수집 → data/issues.json
//   ADMIN_TOKEN=xxx node scripts/scrape-issues.mjs --post # 수집 후 /api/issues 반영
//   node scripts/scrape-issues.mjs --source fixture --file sample.rss   # 파서 테스트(오프라인)
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const args = Object.fromEntries(process.argv.slice(2).reduce((a, x, i, arr) => {
  if (x.startsWith("--")) a.push([x.slice(2), arr[i + 1] && !arr[i + 1].startsWith("--") ? arr[i + 1] : true]);
  return a;
}, []));

const BASE_URL = process.env.BASE_URL || "http://127.0.0.1:3000";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "";
const OUT = args.out || path.join(__dirname, "..", "data", "issues.json");
const DAYS = Number(args.days || 7);
const DELAY_MS = Number(args.delay || 1200); // 소스 존중: 호출 간 간격

const TEAM_QUERY = {
  lg: "LG 트윈스", hh: "한화 이글스", ss: "삼성 라이온즈", kia: "KIA 타이거즈", ssg: "SSG 랜더스",
  lt: "롯데 자이언츠", kt: "KT 위즈", ds: "두산 베어스", nc: "NC 다이노스", kw: "키움 히어로즈",
};
// 주요 선수(팀별) — 제목에서 이름을 찾아 태깅한다.
const STARS = {
  lg: ["오스틴", "홍창기", "문보경", "박해민", "신민재", "임찬규", "켈리", "손주영", "유영찬", "박동원", "김현수", "오지환"],
  hh: ["노시환", "채은성", "문현빈", "안치홍", "폰세", "류현진", "와이스", "주현상", "문동주", "김서현", "엄상백"],
  ss: ["구자욱", "디아즈", "김영웅", "이재현", "박병호", "원태인", "후라도", "김재윤", "강민호", "김지찬"],
  kia: ["김도영", "최형우", "나성범", "소크라테스", "김선빈", "네일", "양현종", "윤영철", "정해영", "이의리", "박찬호"],
  ssg: ["에레디아", "최정", "한유섬", "박성한", "최지훈", "김광현", "앤더슨", "조병현", "노경은", "문승원"],
  lt: ["레이예스", "윤동희", "전준우", "나승엽", "고승민", "박세웅", "반즈", "김원중", "황성빈", "손호영", "윌커슨"],
  kt: ["로하스", "강백호", "배정대", "황재균", "고영표", "벤자민", "소형준", "박영현", "장성우", "쿠에바스"],
  ds: ["양의지", "김재환", "정수빈", "강승호", "양석환", "곽빈", "브랜든", "김택연", "허경민", "이영하"],
  nc: ["데이비슨", "박민우", "손아섭", "김주원", "권희동", "카일 하트", "하트", "이용찬", "류진욱", "김형준"],
  kw: ["송성문", "최주환", "이주형", "도슨", "김재현", "하영민", "헤이수스", "주승우", "김동헌", "고영우"],
};
// 카테고리 규칙 (앞에서부터 우선 적용)
const RULES = [
  { cat: "부상·이탈", sev: "critical", kw: ["부상", "통증", "이탈", "골절", "염좌", "수술", "정밀 검진", "정밀검진", "엔트리 제외", "말소", "结장", "결장", "병원", "손상", "재검"] },
  { cat: "복귀·콜업", sev: "good", kw: ["복귀", "콜업", "1군 등록", "1군 합류", "재활 등판", "엔트리 등록", "복귀전"] },
  { cat: "선발 예고", sev: "info", kw: ["선발 예고", "선발 등판", "선발 맞대결", "선발 로테이션", "등판 예고", "선발 출격"] },
  { cat: "라인업·출장", sev: "info", kw: ["라인업", "선발 라인업", "스타팅", "출장", "선발 출장", "휴식", "제외"] },
  { cat: "이적·계약", sev: "warning", kw: ["트레이드", "방출", "웨이버", "FA 계약", "영입", "재계약", "임의해지"] },
];
function classify(title) {
  for (const r of RULES) for (const k of r.kw) if (title.includes(k)) return { cat: r.cat, sev: r.sev, hit: k };
  return null;
}
function findPlayer(title, teamId) {
  const names = STARS[teamId] || [];
  return names.find((n) => title.includes(n)) || "";
}

// --- RSS 파서 (의존성 없음) ---
function decode(s) {
  return String(s || "")
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
    .replace(/<[^>]+>/g, "")
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'").replace(/&apos;/g, "'").replace(/&amp;/g, "&")
    .replace(/\s+/g, " ").trim();
}
function parseRSS(xml) {
  const items = [];
  const blocks = String(xml).split(/<item[\s>]/).slice(1);
  for (const b of blocks) {
    const seg = b.split(/<\/item>/)[0];
    const pick = (tag) => { const m = seg.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, "i")); return m ? decode(m[1]) : ""; };
    const title = pick("title");
    const link = pick("link") || (seg.match(/<link[^>]*>([\s\S]*?)<\/link>/i) ? "" : "");
    const pub = pick("pubDate");
    const source = pick("source");
    if (!title) continue;
    const ts = pub ? Date.parse(pub) : NaN;
    items.push({ title, url: link, source, ts: Number.isFinite(ts) ? ts : Date.now() });
  }
  return items;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function fetchRSS(query) {
  const url = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=ko&gl=KR&ceid=KR:ko`;
  const r = await fetch(url, { headers: { "User-Agent": "kbo-fan-app/1.0 (personal dashboard)" } });
  if (!r.ok) throw new Error("rss " + r.status);
  return parseRSS(await r.text());
}

function buildIssues(rawByTeam) {
  const cutoff = Date.now() - DAYS * 864e5;
  const seen = new Set(), out = [];
  for (const [teamId, list] of Object.entries(rawByTeam)) {
    for (const it of list) {
      if (it.ts < cutoff) continue;
      const c = classify(it.title);
      if (!c) continue;                                  // 경기 영향 키워드가 없으면 버림
      const key = it.title.slice(0, 60);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
        id: Buffer.from(key).toString("base64url").slice(0, 12),
        team: teamId, cat: c.cat, sev: c.sev,
        player: findPlayer(it.title, teamId),
        title: it.title, url: it.url, source: it.source || "news",
        ts: it.ts, rumor: false,
      });
    }
  }
  return out.sort((a, b) => b.ts - a.ts);
}

async function main() {
  const rawByTeam = {};
  if (args.source === "fixture") {
    if (!args.file) throw new Error("--file <path> 가 필요합니다");
    const items = parseRSS(fs.readFileSync(args.file, "utf8"));
    // 픽스처는 제목에 팀명이 들어 있다고 보고 팀을 매칭
    for (const [id, q] of Object.entries(TEAM_QUERY)) {
      const short = q.split(" ")[0];
      rawByTeam[id] = items.filter((x) => x.title.includes(short));
    }
  } else {
    for (const [id, q] of Object.entries(TEAM_QUERY)) {
      try {
        rawByTeam[id] = await fetchRSS(`${q} (부상 OR 선발 OR 라인업 OR 복귀 OR 트레이드)`);
        console.log(`[issues] ${q}: ${rawByTeam[id].length}건 수집`);
      } catch (e) { console.error(`[issues] ${q} 실패: ${e.message}`); rawByTeam[id] = []; }
      await sleep(DELAY_MS);
    }
  }
  const items = buildIssues(rawByTeam);
  console.log(`[issues] 경기 영향 이슈 ${items.length}건`,
    Object.entries(items.reduce((a, x) => (a[x.cat] = (a[x.cat] || 0) + 1, a), {})).map(([k, v]) => `${k}:${v}`).join(" "));

  if (args.post) {
    if (!ADMIN_TOKEN) throw new Error("--post 에는 ADMIN_TOKEN 이 필요합니다");
    const res = await fetch(BASE_URL + "/api/issues", {
      method: "POST", headers: { "content-type": "application/json", "x-admin-token": ADMIN_TOKEN },
      body: JSON.stringify({ items }),
    });
    console.log("[issues] POST /api/issues ->", res.status, (await res.text()).slice(0, 120));
    if (!res.ok) process.exit(1);
  } else {
    fs.mkdirSync(path.dirname(OUT), { recursive: true });
    fs.writeFileSync(OUT, JSON.stringify({ items, updatedAt: Date.now() }, null, 2));
    console.log("[issues] 저장:", OUT);
  }
}
main().catch((e) => { console.error("[issues] 실패:", e.message); process.exit(1); });
