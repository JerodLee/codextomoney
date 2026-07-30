// KBO 팬 앱 백엔드 — 정적 프론트 서빙 + 팬 게시판 API + 데이터 동기화 API
// 순수 JS 의존성(express)만 사용, 저장은 JSON 파일(원자적 쓰기). EC2에서 `node server.js`로 실행.
import express from "express";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const PUBLIC_DIR = path.join(__dirname, "public");
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || ""; // 데이터 동기화(POST /api/data) 보호용

fs.mkdirSync(DATA_DIR, { recursive: true });
const BOARD_FILE = path.join(DATA_DIR, "board.json");
const DATASET_FILE = path.join(DATA_DIR, "dataset.json");
const ISSUES_FILE = path.join(DATA_DIR, "issues.json");

// ---------- 작은 JSON 스토어 (직렬화된 원자적 쓰기) ----------
function readJSON(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return fallback; }
}
let writeChain = Promise.resolve();
function writeJSON(file, obj) {
  writeChain = writeChain.then(() => new Promise((resolve) => {
    const tmp = file + ".tmp";
    fs.writeFile(tmp, JSON.stringify(obj), (err) => {
      if (err) { console.error("write error", err); return resolve(); }
      fs.rename(tmp, file, () => resolve());
    });
  }));
  return writeChain;
}

let board = readJSON(BOARD_FILE, { posts: [] });

const TEAMS = new Set(["lg", "hh", "ss", "kia", "ssg", "lt", "kt", "ds", "nc", "kw", "etc"]);
const clip = (s, n) => String(s == null ? "" : s).slice(0, n).trim();

// ---------- 모더레이션 ----------
const AUTO_HIDE = Number(process.env.AUTO_HIDE || 5); // 신고 누적 시 자동 숨김 임계치
const BANNED = (process.env.BANNED_WORDS || "").split(",").map((s) => s.trim()).filter(Boolean);
function maskBanned(s) {
  let out = s;
  for (const w of BANNED) {
    if (!w) continue;
    out = out.split(w).join("*".repeat(Math.max(2, w.length)));
  }
  return out;
}
function isAdmin(req) {
  return !!ADMIN_TOKEN && req.headers["x-admin-token"] === ADMIN_TOKEN;
}

// ---------- 간단 레이트 리밋 (IP 기준) ----------
const hits = new Map();
function rateLimit(ip, max = 8, windowMs = 60000) {
  const now = Date.now();
  const arr = (hits.get(ip) || []).filter((t) => now - t < windowMs);
  if (arr.length >= max) { hits.set(ip, arr); return false; }
  arr.push(now); hits.set(ip, arr); return true;
}
function ipKey(req) {
  const raw = (req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "") + "";
  return crypto.createHash("sha256").update(raw).digest("hex").slice(0, 16);
}

const app = express();
app.use(express.json({ limit: "64kb" }));
app.use(express.static(PUBLIC_DIR, { extensions: ["html"] }));

app.get("/api/health", (_req, res) => res.json({ ok: true, mode: "server", time: Date.now() }));

// ---------- 게시판 ----------
app.get("/api/board", (req, res) => {
  const team = req.query.team || "all";
  const sort = req.query.sort || "new";
  const admin = isAdmin(req);
  let posts = board.posts
    .filter((p) => admin || !p.hidden)
    .map((p) => ({
      id: p.id, team: p.team, nick: p.nick, title: p.title, body: p.body,
      ts: p.ts, score: p.score || 0, comments: (p.comments || []).filter((c) => admin || !c.hidden).length,
      reports: p.reports || 0, hidden: !!p.hidden,
    }));
  if (team !== "all") posts = posts.filter((p) => p.team === team);
  posts.sort((a, b) => (sort === "hot" ? (b.score - a.score) || (b.ts - a.ts) : b.ts - a.ts));
  res.json({ posts: posts.slice(0, 200), admin });
});

app.get("/api/board/:id", (req, res) => {
  const p = board.posts.find((x) => x.id === req.params.id);
  if (!p || (p.hidden && !isAdmin(req))) return res.status(404).json({ error: "not_found" });
  const admin = isAdmin(req);
  const comments = (p.comments || []).map((c, i) => ({ ...c, idx: i }))
    .filter((c) => admin || !c.hidden);
  res.json({ post: { ...p, voters: undefined, comments } });
});

app.post("/api/board", async (req, res) => {
  if (!rateLimit(ipKey(req))) return res.status(429).json({ error: "rate_limited" });
  const team = TEAMS.has(req.body.team) ? req.body.team : "etc";
  const nick = clip(req.body.nick, 20) || "익명팬";
  const title = maskBanned(clip(req.body.title, 80));
  const body = maskBanned(clip(req.body.body, 2000));
  if (!title || !body) return res.status(400).json({ error: "bad_request" });
  const post = { id: crypto.randomUUID().slice(0, 8), team, nick, title, body, ts: Date.now(), score: 0, voters: {}, comments: [], reports: 0, hidden: false };
  board.posts.unshift(post);
  if (board.posts.length > 2000) board.posts.length = 2000;
  await writeJSON(BOARD_FILE, board);
  res.json({ post: { ...post, voters: undefined } });
});

app.post("/api/board/:id/comment", async (req, res) => {
  if (!rateLimit(ipKey(req))) return res.status(429).json({ error: "rate_limited" });
  const p = board.posts.find((x) => x.id === req.params.id);
  if (!p) return res.status(404).json({ error: "not_found" });
  const nick = clip(req.body.nick, 20) || "익명팬";
  const body = maskBanned(clip(req.body.body, 600));
  if (!body) return res.status(400).json({ error: "bad_request" });
  const c = { nick, body, ts: Date.now(), reports: 0, hidden: false };
  (p.comments = p.comments || []).push(c);
  await writeJSON(BOARD_FILE, board);
  res.json({ comment: c, comments: p.comments.length });
});

app.post("/api/board/:id/vote", async (req, res) => {
  const p = board.posts.find((x) => x.id === req.params.id);
  if (!p) return res.status(404).json({ error: "not_found" });
  const dir = req.body.dir > 0 ? 1 : -1;
  const k = ipKey(req);
  p.voters = p.voters || {};
  if (p.voters[k] === dir) delete p.voters[k]; else p.voters[k] = dir;
  p.score = Object.values(p.voters).reduce((s, v) => s + v, 0);
  await writeJSON(BOARD_FILE, board);
  res.json({ score: p.score });
});

// ---------- 신고 (누적 시 자동 숨김) ----------
app.post("/api/board/:id/report", async (req, res) => {
  if (!rateLimit(ipKey(req), 20)) return res.status(429).json({ error: "rate_limited" });
  const p = board.posts.find((x) => x.id === req.params.id);
  if (!p) return res.status(404).json({ error: "not_found" });
  p.reports = (p.reports || 0) + 1;
  if (p.reports >= AUTO_HIDE) p.hidden = true;
  await writeJSON(BOARD_FILE, board);
  res.json({ reports: p.reports, hidden: !!p.hidden });
});
app.post("/api/board/:id/comment/:idx/report", async (req, res) => {
  if (!rateLimit(ipKey(req), 20)) return res.status(429).json({ error: "rate_limited" });
  const p = board.posts.find((x) => x.id === req.params.id);
  const c = p && (p.comments || [])[+req.params.idx];
  if (!c) return res.status(404).json({ error: "not_found" });
  c.reports = (c.reports || 0) + 1;
  if (c.reports >= AUTO_HIDE) c.hidden = true;
  await writeJSON(BOARD_FILE, board);
  res.json({ reports: c.reports, hidden: !!c.hidden });
});

// ---------- 관리자 삭제 (x-admin-token) ----------
app.delete("/api/board/:id", async (req, res) => {
  if (!isAdmin(req)) return res.status(401).json({ error: "unauthorized" });
  const i = board.posts.findIndex((x) => x.id === req.params.id);
  if (i < 0) return res.status(404).json({ error: "not_found" });
  board.posts.splice(i, 1);
  await writeJSON(BOARD_FILE, board);
  res.json({ ok: true });
});
app.delete("/api/board/:id/comment/:idx", async (req, res) => {
  if (!isAdmin(req)) return res.status(401).json({ error: "unauthorized" });
  const p = board.posts.find((x) => x.id === req.params.id);
  if (!p || !(p.comments || [])[+req.params.idx]) return res.status(404).json({ error: "not_found" });
  p.comments.splice(+req.params.idx, 1);
  await writeJSON(BOARD_FILE, board);
  res.json({ ok: true });
});
// 관리자: 숨김 해제
app.post("/api/board/:id/unhide", async (req, res) => {
  if (!isAdmin(req)) return res.status(401).json({ error: "unauthorized" });
  const p = board.posts.find((x) => x.id === req.params.id);
  if (!p) return res.status(404).json({ error: "not_found" });
  p.hidden = false; p.reports = 0;
  await writeJSON(BOARD_FILE, board);
  res.json({ ok: true });
});

// ---------- 선수 이슈(부상·선발·라인업) ----------
// 스크레이퍼가 POST 로 올리고, 프론트가 GET 으로 읽는다. 출처 링크를 항상 보존한다.
app.get("/api/issues", (req, res) => {
  const store = readJSON(ISSUES_FILE, null) || { items: [], updatedAt: null };
  const team = req.query.team || "all";
  const cat = req.query.cat || "all";
  let items = Array.isArray(store.items) ? store.items : [];
  if (team !== "all") items = items.filter((x) => x.team === team);
  if (cat !== "all") items = items.filter((x) => x.cat === cat);
  items = items.slice().sort((a, b) => (b.ts || 0) - (a.ts || 0)).slice(0, 300);
  res.json({ items, updatedAt: store.updatedAt || null });
});
app.post("/api/issues", async (req, res) => {
  if (!isAdmin(req)) return res.status(401).json({ error: "unauthorized" });
  const src = Array.isArray(req.body?.items) ? req.body.items : [];
  const items = src.slice(0, 1000).map((x) => ({
    id: clip(x.id, 40) || crypto.randomUUID().slice(0, 8),
    team: TEAMS.has(x.team) ? x.team : "etc",
    cat: clip(x.cat, 20) || "기타",
    sev: ["critical", "warning", "info", "good"].includes(x.sev) ? x.sev : "info",
    player: clip(x.player, 30),
    title: clip(x.title, 200),
    url: /^https?:\/\//.test(x.url || "") ? clip(x.url, 500) : "",
    source: clip(x.source, 60),
    ts: Number.isFinite(+x.ts) ? +x.ts : Date.now(),
    rumor: !!x.rumor,
  })).filter((x) => x.title && x.url);
  await writeJSON(ISSUES_FILE, { items, updatedAt: Date.now() });
  res.json({ ok: true, count: items.length });
});

// ---------- 데이터셋 동기화 (관리자 토큰) ----------
app.get("/api/data", (_req, res) => res.json(readJSON(DATASET_FILE, null) || {}));
app.post("/api/data", async (req, res) => {
  if (!ADMIN_TOKEN || req.headers["x-admin-token"] !== ADMIN_TOKEN)
    return res.status(401).json({ error: "unauthorized" });
  await writeJSON(DATASET_FILE, req.body || {});
  res.json({ ok: true });
});

app.listen(PORT, () => console.log(`⚾ KBO 팬 앱: http://localhost:${PORT}  (data: ${DATA_DIR})`));
