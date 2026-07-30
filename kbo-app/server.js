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
  let posts = board.posts.map((p) => ({
    id: p.id, team: p.team, nick: p.nick, title: p.title, body: p.body,
    ts: p.ts, score: p.score || 0, comments: (p.comments || []).length,
  }));
  if (team !== "all") posts = posts.filter((p) => p.team === team);
  posts.sort((a, b) => (sort === "hot" ? (b.score - a.score) || (b.ts - a.ts) : b.ts - a.ts));
  res.json({ posts: posts.slice(0, 200) });
});

app.get("/api/board/:id", (req, res) => {
  const p = board.posts.find((x) => x.id === req.params.id);
  if (!p) return res.status(404).json({ error: "not_found" });
  res.json({ post: { ...p, voters: undefined } });
});

app.post("/api/board", async (req, res) => {
  if (!rateLimit(ipKey(req))) return res.status(429).json({ error: "rate_limited" });
  const team = TEAMS.has(req.body.team) ? req.body.team : "etc";
  const nick = clip(req.body.nick, 20) || "익명팬";
  const title = clip(req.body.title, 80);
  const body = clip(req.body.body, 2000);
  if (!title || !body) return res.status(400).json({ error: "bad_request" });
  const post = { id: crypto.randomUUID().slice(0, 8), team, nick, title, body, ts: Date.now(), score: 0, voters: {}, comments: [] };
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
  const body = clip(req.body.body, 600);
  if (!body) return res.status(400).json({ error: "bad_request" });
  const c = { nick, body, ts: Date.now() };
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

// ---------- 데이터셋 동기화 (관리자 토큰) ----------
app.get("/api/data", (_req, res) => res.json(readJSON(DATASET_FILE, null) || {}));
app.post("/api/data", async (req, res) => {
  if (!ADMIN_TOKEN || req.headers["x-admin-token"] !== ADMIN_TOKEN)
    return res.status(401).json({ error: "unauthorized" });
  await writeJSON(DATASET_FILE, req.body || {});
  res.json({ ok: true });
});

app.listen(PORT, () => console.log(`⚾ KBO 팬 앱: http://localhost:${PORT}  (data: ${DATA_DIR})`));
