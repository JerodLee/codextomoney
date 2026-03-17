const $ = (id) => document.getElementById(id);

function detectRepo() {
  const host = window.location.hostname || "";
  const pathParts = window.location.pathname.split("/").filter(Boolean);
  if (host.endsWith("github.io")) {
    const owner = host.split(".")[0];
    const repo = pathParts[0] || "codextomoney";
    return { owner, repo, branch: "main" };
  }
  return { owner: "JerodLee", repo: "codextomoney", branch: "main" };
}

const source = detectRepo();
$("ownerInput").value = source.owner;
$("repoInput").value = source.repo;
$("branchInput").value = source.branch;
const MARKET_CHANGE_KEYS = ["24h", "12h", "6h", "1h", "15m", "5m", "1m"];
const RECENT_ROWS = 12;
const CORR_ROWS = 12;
const CAL_ROWS = 12;
const SYMBOL_NAME_MAP = {
  BTC: "Bitcoin",
  ETH: "Ethereum",
  XRP: "Ripple",
  SOL: "Solana",
  ADA: "Cardano",
  DOGE: "Dogecoin",
  BNB: "BNB",
  TAO: "Bittensor",
  THE: "THE",
  TRUMP: "Official Trump",
  RESOLV: "Resolv",
};

function fmtPct(v) {
  return `${(v * 100).toFixed(2)}%`;
}

function fmtNum(v, d = 2) {
  return Number(v).toFixed(d);
}

function fmtOi(v) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "-";
  if (Math.abs(x) >= 1_000_000) return `${(x / 1_000_000).toFixed(2)}M`;
  if (Math.abs(x) >= 1_000) return `${(x / 1_000).toFixed(2)}K`;
  return `${x.toFixed(0)}`;
}

function fmtPctValue(v, d = 2) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "-";
  return `${x.toFixed(d)}%`;
}

function fmtFunding(v) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "-";
  return x.toFixed(4);
}

function fmtKrw(v) {
  const x = Number(v);
  if (!Number.isFinite(x) || x <= 0) return "-";
  return x.toLocaleString("ko-KR", { maximumFractionDigits: 0 });
}

function fmtUsdt(v) {
  const x = Number(v);
  if (!Number.isFinite(x) || x <= 0) return "-";
  if (Math.abs(x) >= 1000) return x.toFixed(2);
  if (Math.abs(x) >= 1) return x.toFixed(4);
  return x.toFixed(6);
}

function fmtRr(v) {
  const x = Number(v);
  if (!Number.isFinite(x) || x <= 0) return "-";
  return `${x.toFixed(2)} : 1`;
}

function concentrationText(c) {
  if (!c || !c.regime) return "쏠림: 데이터없음";
  const btc = Number(c.btc_share || 0);
  const eth = Number(c.eth_share || 0);
  const alt = Number(c.alt_share || 0);
  const topAlt = Number(c.top_alt_share || 0);
  const topAltSymbol = c.top_alt_symbol || "-";
  const base = `BTC ${fmtPct(btc)} | ETH ${fmtPct(eth)} | ALT ${fmtPct(alt)}`;
  if (c.regime === "btc") return `쏠림: BTC 주도 (${base})`;
  if (c.regime === "eth") return `쏠림: ETH 주도 (${base})`;
  if (c.regime === "alt-broad") return `쏠림: 알트 전반 강세 (${base})`;
  if (c.regime === "single-alt") {
    return `쏠림: 특정 알트(${topAltSymbol}) 집중 ${fmtPct(topAlt)} (${base})`;
  }
  return `쏠림: 균형 (${base})`;
}

function fmtTime(ts) {
  if (!ts) return "-";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "-";
  return d.toLocaleString("ko-KR", { hour12: false });
}

function sideOf(row) {
  return String(row?.side || "LONG").toUpperCase() === "SHORT" ? "SHORT" : "LONG";
}

function modelOf(row) {
  const raw = String(row?.model_id || "").trim();
  if (raw) return raw;
  return sideOf(row) === "SHORT" ? "momentum_short_v1" : "momentum_long_v1";
}

function modelLabel(modelId) {
  const map = {
    momentum_long_v1: "롱 모멘텀 v1",
    momentum_short_v1: "숏 모멘텀 v1",
    momentum_long_v2: "롱 모멘텀 v2(시장보강)",
    momentum_short_v2: "숏 모멘텀 v2(시장보강)",
  };
  return map[modelId] || modelId || "-";
}

function modelShortLabel(modelId) {
  const map = {
    momentum_long_v1: "롱v1",
    momentum_short_v1: "숏v1",
    momentum_long_v2: "롱v2",
    momentum_short_v2: "숏v2",
  };
  return map[modelId] || modelId || "-";
}

function winRate(rows) {
  if (!rows.length) return null;
  const wins = rows.filter((r) => Boolean(r.win)).length;
  return wins / rows.length;
}

function avgReturn(rows) {
  if (!rows.length) return null;
  const sum = rows.reduce((acc, r) => acc + Number(r.return_blended || 0), 0);
  return sum / rows.length;
}

function activeModelNames(state) {
  const latestRun = (state.run_history || []).slice(-1)[0] || {};
  const fromRun = Array.isArray(latestRun.active_models) ? latestRun.active_models : [];
  if (fromRun.length) {
    return fromRun.map((id) => modelLabel(String(id)));
  }
  const reg = state.model_registry || {};
  return Object.entries(reg)
    .filter(([, spec]) => Boolean(spec?.enabled))
    .map(([id]) => modelLabel(String(id)));
}

function symbolName(symbol) {
  const code = String(symbol || "").toUpperCase();
  return SYMBOL_NAME_MAP[code] || code;
}

function countBySymbol(rows) {
  const out = new Map();
  for (const r of rows || []) {
    const sym = String(r?.symbol || "").toUpperCase().trim();
    if (!sym) continue;
    out.set(sym, (out.get(sym) || 0) + 1);
  }
  return out;
}

function countWinsBySymbol(rows) {
  const out = new Map();
  for (const r of rows || []) {
    const sym = String(r?.symbol || "").toUpperCase().trim();
    if (!sym) continue;
    if (!Boolean(r?.win)) continue;
    out.set(sym, (out.get(sym) || 0) + 1);
  }
  return out;
}

function countByPickId(rows) {
  const out = new Map();
  for (const r of rows || []) {
    const raw = String(r?.pick_id || r?.id || "").trim();
    if (!raw) continue;
    const pickId = raw.includes("@") ? raw.split("@")[0] : raw;
    if (!pickId) continue;
    out.set(pickId, (out.get(pickId) || 0) + 1);
  }
  return out;
}

function evalPlanCount(pick) {
  const hs = Array.isArray(pick?.eval_horizons_min)
    ? pick.eval_horizons_min.map((x) => Number(x)).filter((x) => Number.isFinite(x) && x > 0)
    : [];
  if (hs.length) return hs.length;
  const h = Number(pick?.horizon_min);
  return Number.isFinite(h) && h > 0 ? 1 : 1;
}

function evalHorizonText(row) {
  const h = Number(row?.horizon_min);
  return Number.isFinite(h) && h > 0 ? `${Math.round(h)}m` : "-";
}

function symbolCellHtml(symbol, recCount, evalCount, winCount) {
  const code = String(symbol || "-").toUpperCase();
  return `
    <div class="sym-cell">
      <div class="sym-code mono">${code}</div>
      <div class="sym-name">${symbolName(code)}</div>
      <div class="sym-meta">\uCD94\uCC9C ${recCount}\uD68C \u00B7 \uAC80\uC99D ${evalCount}\uD68C \u00B7 \uC2B9 ${winCount}\uD68C</div>
    </div>
  `;
}

function sideBadgeHtml(side) {
  const s = String(side || "LONG").toUpperCase() === "SHORT" ? "SHORT" : "LONG";
  return `<span class="side-pill ${s === "SHORT" ? "side-short" : "side-long"}">${s}</span>`;
}

function statusLabel(done) {
  return done ? "\uAC80\uC99D\uC644\uB8CC" : "\uB300\uAE30";
}

function executionProfileOf(row) {
  const p = Number(row?.execution_profile);
  if (Number.isFinite(p) && p >= 1 && p <= 3) return Math.trunc(p);
  return 1;
}

function resultLabel(win) {
  return win ? "\uC2B9" : "\uD328";
}

function clampNum(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

function computePlanFields(row) {
  const side = sideOf(row);
  const dir = side === "SHORT" ? -1 : 1;
  const score = Number.isFinite(Number(row?.score)) ? Number(row.score) : 0.25;
  const bNow = Number(row?.entry_bithumb_price);
  const gNow = Number(row?.entry_bitget_price);
  const bRate = Math.abs(Number(row?.b_rate24h || 0));
  const gRate = Math.abs(Number(row?.g_rate24h || 0));
  const vol24 = clampNum((bRate + gRate) / 2, 0.5, 25);
  const stopPct = clampNum(0.45 + (vol24 * 0.08), 0.45, 2.8);
  const rrBase = clampNum(1.10 + (score * 1.50), 1.10, 2.60);
  const targetPctCalc = stopPct * rrBase;
  const entryOffsetPct = clampNum(0.10 + (vol24 * 0.02), 0.10, 0.80);

  const recoCalc = (px) => (Number.isFinite(px) && px > 0
    ? px * (1 - (dir * entryOffsetPct / 100))
    : null);
  const targetCalc = (px, pct) => (Number.isFinite(px) && px > 0 && Number.isFinite(pct) && pct > 0
    ? px * (1 + (dir * pct / 100))
    : null);

  const bRecoStored = Number(row?.entry_reco_bithumb_price);
  const gRecoStored = Number(row?.entry_reco_bitget_price);
  const rrNowStored = Number(row?.rr_now);
  const rrEntryStored = Number(row?.rr_entry);
  const targetPctStored = Number(row?.plan_target_pct);
  const targetRrPctStored = Number(row?.plan_target_rr_pct);
  const targetObPctStored = Number(row?.plan_target_ob_pct);
  const targetFlowPctStored = Number(row?.plan_target_flow_pct);
  const targetBasis = String(row?.plan_target_basis || "");
  const bTargetNowStored = Number(row?.target_now_bithumb_price);
  const gTargetNowStored = Number(row?.target_now_bitget_price);
  const bTargetEntryStored = Number(row?.target_entry_bithumb_price);
  const gTargetEntryStored = Number(row?.target_entry_bitget_price);

  const bReco = Number.isFinite(bRecoStored) && bRecoStored > 0 ? bRecoStored : recoCalc(bNow);
  const gReco = Number.isFinite(gRecoStored) && gRecoStored > 0 ? gRecoStored : recoCalc(gNow);
  const targetPct = Number.isFinite(targetPctStored) && targetPctStored > 0 ? targetPctStored : targetPctCalc;
  const targetRrPct = Number.isFinite(targetRrPctStored) && targetRrPctStored > 0
    ? targetRrPctStored
    : targetPctCalc;
  const targetObPct = Number.isFinite(targetObPctStored) && targetObPctStored > 0 ? targetObPctStored : null;
  const targetFlowPct = Number.isFinite(targetFlowPctStored) && targetFlowPctStored > 0
    ? targetFlowPctStored
    : targetPctCalc;
  const bTargetNow = Number.isFinite(bTargetNowStored) && bTargetNowStored > 0
    ? bTargetNowStored
    : targetCalc(bNow, targetPct);
  const gTargetNow = Number.isFinite(gTargetNowStored) && gTargetNowStored > 0
    ? gTargetNowStored
    : targetCalc(gNow, targetPct);
  const bTargetEntry = Number.isFinite(bTargetEntryStored) && bTargetEntryStored > 0
    ? bTargetEntryStored
    : targetCalc(Number.isFinite(Number(bReco)) ? Number(bReco) : bNow, targetPct);
  const gTargetEntry = Number.isFinite(gTargetEntryStored) && gTargetEntryStored > 0
    ? gTargetEntryStored
    : targetCalc(Number.isFinite(Number(gReco)) ? Number(gReco) : gNow, targetPct);

  let rrNow = Number.isFinite(rrNowStored) && rrNowStored > 0 ? rrNowStored : null;
  let rrEntry = Number.isFinite(rrEntryStored) && rrEntryStored > 0 ? rrEntryStored : null;
  if (rrNow == null || rrEntry == null) {
    const refPx = Number.isFinite(gNow) && gNow > 0
      ? gNow
      : (Number.isFinite(bNow) && bNow > 0 ? bNow : null);
    if (refPx != null) {
      const targetPx = refPx * (1 + (dir * targetPct / 100));
      const stopPx = refPx * (1 - (dir * stopPct / 100));
      const entryPx = refPx * (1 - (dir * entryOffsetPct / 100));

      const rewardNow = Math.abs(targetPx - refPx) / Math.abs(refPx);
      const riskNow = Math.abs(refPx - stopPx) / Math.abs(refPx);
      if (rrNow == null && riskNow > 1e-9) rrNow = rewardNow / riskNow;

      const rewardEntry = Math.abs(targetPx - entryPx) / Math.abs(entryPx);
      const riskEntry = Math.abs(entryPx - stopPx) / Math.abs(entryPx);
      if (rrEntry == null && riskEntry > 1e-9) rrEntry = rewardEntry / riskEntry;
    }
  }

  return {
    bNow: Number.isFinite(bNow) && bNow > 0 ? bNow : null,
    gNow: Number.isFinite(gNow) && gNow > 0 ? gNow : null,
    bReco: Number.isFinite(Number(bReco)) && Number(bReco) > 0 ? Number(bReco) : null,
    gReco: Number.isFinite(Number(gReco)) && Number(gReco) > 0 ? Number(gReco) : null,
    bTargetNow: Number.isFinite(Number(bTargetNow)) && Number(bTargetNow) > 0 ? Number(bTargetNow) : null,
    gTargetNow: Number.isFinite(Number(gTargetNow)) && Number(gTargetNow) > 0 ? Number(gTargetNow) : null,
    bTargetEntry: Number.isFinite(Number(bTargetEntry)) && Number(bTargetEntry) > 0 ? Number(bTargetEntry) : null,
    gTargetEntry: Number.isFinite(Number(gTargetEntry)) && Number(gTargetEntry) > 0 ? Number(gTargetEntry) : null,
    targetPct: Number.isFinite(targetPct) && targetPct > 0 ? targetPct : null,
    targetRrPct: Number.isFinite(targetRrPct) && targetRrPct > 0 ? targetRrPct : null,
    targetObPct,
    targetFlowPct: Number.isFinite(targetFlowPct) && targetFlowPct > 0 ? targetFlowPct : null,
    targetBasis,
    rrNow,
    rrEntry,
  };
}

function pricePairCellHtml(krw, usdt) {
  return `
    <div class="plan-cell">
      <div class="mono plan-line">KRW ${fmtKrw(krw)}</div>
      <div class="mono plan-line">USDT ${fmtUsdt(usdt)}</div>
    </div>
  `;
}

function rrCellHtml(rr) {
  return `<span class="rr-badge">${fmtRr(rr)}</span>`;
}

function targetBasisCellHtml(plan) {
  const basis = String(plan?.targetBasis || "");
  const rr = Number(plan?.targetRrPct);
  const ob = Number(plan?.targetObPct);
  const flow = Number(plan?.targetFlowPct);
  const parts = [];
  if (Number.isFinite(rr) && rr > 0) parts.push(`rr ${fmtPctValue(rr, 2)}`);
  if (Number.isFinite(ob) && ob > 0) parts.push(`ob ${fmtPctValue(ob, 2)}`);
  if (Number.isFinite(flow) && flow > 0) parts.push(`flow ${fmtPctValue(flow, 2)}`);
  return `
    <div class="plan-cell">
      <div class="mono plan-line">TP ${fmtPctValue(plan?.targetPct, 2)}</div>
      <div class="plan-line">${basis || "-"}</div>
      <div class="plan-line">${parts.length ? parts.join(" | ") : "-"}</div>
    </div>
  `;
}

function relationText(v) {
  const map = {
    proportional: "비례",
    inverse: "반비례",
    mixed: "혼합",
    "neutral-market": "시장중립",
    insufficient: "데이터부족",
  };
  return map[v] || "데이터부족";
}

function trendText(v) {
  const map = { up: "상승", down: "하락", neutral: "중립" };
  return map[v] || "중립";
}

function sideSign(row) {
  return sideOf(row) === "SHORT" ? -1 : 1;
}

function relationFromNumeric(v, neutralBand = 0.2) {
  if (v == null || !Number.isFinite(Number(v))) return "insufficient";
  if (v > neutralBand) return "proportional";
  if (v < -neutralBand) return "inverse";
  return "mixed";
}

function pearsonCorr(xs, ys) {
  if (!xs.length || xs.length !== ys.length || xs.length < 2) return null;
  const mx = xs.reduce((a, b) => a + b, 0) / xs.length;
  const my = ys.reduce((a, b) => a + b, 0) / ys.length;
  let cov = 0;
  let vx = 0;
  let vy = 0;
  for (let i = 0; i < xs.length; i += 1) {
    const dx = xs[i] - mx;
    const dy = ys[i] - my;
    cov += dx * dy;
    vx += dx * dx;
    vy += dy * dy;
  }
  if (vx <= 1e-12 || vy <= 1e-12) return null;
  return cov / Math.sqrt(vx * vy);
}

function directionalAgreement(xs, ys) {
  if (!xs.length || xs.length !== ys.length) return null;
  let signed = 0;
  let totalW = 0;
  for (let i = 0; i < xs.length; i += 1) {
    const x = Number(xs[i]);
    const y = Number(ys[i]);
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
    const w = Math.max(Math.abs(y), 1e-9);
    const ySign = y >= 0 ? 1 : -1;
    signed += x * ySign * w;
    totalW += w;
  }
  if (totalW <= 0) return null;
  return signed / totalW;
}

async function fetchJsonFirst(urls) {
  let lastErr = "unknown";
  for (const u of urls) {
    try {
      const res = await fetch(u, { cache: "no-store" });
      if (!res.ok) {
        lastErr = `HTTP ${res.status} ${u}`;
        continue;
      }
      return { data: await res.json(), url: u };
    } catch (e) {
      lastErr = `${u}: ${String(e)}`;
    }
  }
  throw new Error(`Failed to load JSON: ${lastErr}`);
}

function stateRecencyScore(state) {
  if (!state || typeof state !== "object") return 0;
  const fromMeta = Date.parse(String(state?.meta?.last_run_at || ""));
  if (Number.isFinite(fromMeta) && fromMeta > 0) return fromMeta;

  const runs = Array.isArray(state.run_history) ? state.run_history : [];
  const lastRun = runs.length ? runs[runs.length - 1] : null;
  const fromRun = Date.parse(String(lastRun?.run_at || ""));
  if (Number.isFinite(fromRun) && fromRun > 0) return fromRun;

  const recs = Array.isArray(state.recommendation_history) ? state.recommendation_history : [];
  const lastRec = recs.length ? recs[recs.length - 1] : null;
  const fromRec = Date.parse(String(lastRec?.created_at || ""));
  if (Number.isFinite(fromRec) && fromRec > 0) return fromRec;
  return 0;
}

async function fetchJsonNewest(urls) {
  const hits = [];
  let lastErr = "unknown";
  for (const u of urls) {
    try {
      const res = await fetch(u, { cache: "no-store" });
      if (!res.ok) {
        lastErr = `HTTP ${res.status} ${u}`;
        continue;
      }
      const data = await res.json();
      hits.push({ data, url: u, score: stateRecencyScore(data) });
    } catch (e) {
      lastErr = `${u}: ${String(e)}`;
    }
  }
  if (!hits.length) {
    throw new Error(`Failed to load JSON: ${lastErr}`);
  }
  hits.sort((a, b) => b.score - a.score);
  return { data: hits[0].data, url: hits[0].url };
}

function candidateStateUrls(owner, repo, branch) {
  const ts = Date.now();
  const raw = `https://raw.githubusercontent.com/${owner}/${repo}/${branch}/state/bot_state.json?t=${ts}`;
  const jsdelivr = `https://cdn.jsdelivr.net/gh/${owner}/${repo}@${branch}/state/bot_state.json?t=${ts}`;
  const host = String(window.location.hostname || "").toLowerCase();
  const isLocal = host === "localhost" || host === "127.0.0.1" || host === "";
  // On hosted pages, avoid same-origin fallback because commit-pinned hosts can serve stale state.
  if (!isLocal) return [raw, jsdelivr];
  return [raw, jsdelivr, "../state/bot_state.json", "/state/bot_state.json", "./state/bot_state.json"];
}

function buildSvgLine(svgEl, points, stroke = "#0e8a7b", dot = "#ff9e57") {
  if (!svgEl) return;
  const w = 900;
  const h = 240;
  if (!points.length) {
    svgEl.innerHTML = `<text x="16" y="28" fill="#5c6a72" font-size="14">추이 데이터가 아직 없습니다.</text>`;
    return;
  }
  const min = Math.min(...points.map((p) => p.y), 0);
  const max = Math.max(...points.map((p) => p.y), 1);
  const span = Math.max(max - min, 0.0001);
  const xStep = points.length > 1 ? w / (points.length - 1) : w;
  const xy = points.map((p, i) => {
    const x = i * xStep;
    const y = h - ((p.y - min) / span) * (h - 24) - 12;
    return { x, y };
  });
  const path = xy.map((p, i) => `${i ? "L" : "M"}${p.x.toFixed(2)} ${p.y.toFixed(2)}`).join(" ");
  svgEl.innerHTML = `
    <rect x="0" y="0" width="${w}" height="${h}" fill="none"></rect>
    <line x1="0" y1="${h - 12}" x2="${w}" y2="${h - 12}" stroke="#d7e0e4" />
    <path d="${path}" fill="none" stroke="${stroke}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></path>
    <circle cx="${xy[xy.length - 1].x.toFixed(2)}" cy="${xy[xy.length - 1].y.toFixed(2)}" r="4.8" fill="${dot}"></circle>
  `;
}

function renderRules(cfg) {
  const body = $("rulesBody");
  body.innerHTML = "";
  const rows = Object.entries(cfg || {});
  for (const [k, v] of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td class="mono">${k}</td><td class="mono">${v}</td>`;
    body.appendChild(tr);
  }
}

function renderPicks(state) {
  const body = $("pickBody");
  body.innerHTML = "";
  const metaEl = $("pickMeta");
  const allPicks = state.recommendation_history || [];
  const allResults = state.results || [];
  const totalRec = allPicks.length;
  const totalEval = allResults.length;
  const totalWin = allResults.filter((r) => Boolean(r?.win)).length;
  const picks = [...allPicks]
    .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
    .slice(0, RECENT_ROWS);
  const evalByPick = countByPickId(allResults);
  const recCounts = countBySymbol(allPicks);
  const evalCounts = countBySymbol(allResults);
  const winCounts = countWinsBySymbol(allResults);

  if (metaEl) {
    metaEl.textContent = `최신 ${RECENT_ROWS}건 · 누적 추천 ${totalRec}회 · 검증 ${totalEval}회 · 승 ${totalWin}회`;
  }

  if (!picks.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="17" class="muted">추천 이력이 아직 없습니다.</td>`;
    body.appendChild(tr);
    return;
  }

  for (const p of picks) {
    const side = sideOf(p);
    const symbol = String(p.symbol || "").toUpperCase();
    const doneN = evalByPick.get(String(p.id || "")) || 0;
    const planN = evalPlanCount(p);
    let status = "\uB300\uAE30";
    if (doneN > 0 && doneN < planN) {
      status = `\uAC80\uC99D\uC911 ${doneN}/${planN}`;
    } else if (doneN >= planN) {
      status = statusLabel(true);
    }
    const recN = recCounts.get(symbol) || 0;
    const evalN = evalCounts.get(symbol) || 0;
    const winN = winCounts.get(symbol) || 0;
    const plan = computePlanFields(p);
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${fmtTime(p.created_at)}</td>
      <td>${symbolCellHtml(symbol, recN, evalN, winN)}</td>
      <td>${sideBadgeHtml(side)}</td>
      <td class="mono">${modelLabel(modelOf(p))}</td>
      <td class="mono">P${executionProfileOf(p)}</td>
      <td>${Number.isFinite(Number(p.score)) ? fmtNum(p.score, 3) : "-"}</td>
      <td>${pricePairCellHtml(plan.bNow, plan.gNow)}</td>
      <td>${rrCellHtml(plan.rrNow)}</td>
      <td>${pricePairCellHtml(plan.bReco, plan.gReco)}</td>
      <td>${rrCellHtml(plan.rrEntry)}</td>
      <td>${pricePairCellHtml(plan.bTargetEntry, plan.gTargetEntry)}</td>
      <td>${targetBasisCellHtml(plan)}</td>
      <td>${fmtFunding(p.g_funding_rate)}</td>
      <td>${fmtOi(p.g_open_interest)}</td>
      <td>${fmtPctValue(p.b_rate24h, 2)}</td>
      <td>${fmtPctValue(p.g_rate24h, 2)}</td>
      <td>${status}</td>
    `;
    body.appendChild(tr);
  }
}

function renderEvaluations(state, results) {
  const body = $("evalBody");
  body.innerHTML = "";
  const metaEl = $("evalMeta");
  const allPicks = state.recommendation_history || [];
  const totalRec = allPicks.length;
  const totalEval = (results || []).length;
  const totalWin = (results || []).filter((r) => Boolean(r?.win)).length;
  const recCounts = countBySymbol(allPicks);
  const evalCounts = countBySymbol(results);
  const winCounts = countWinsBySymbol(results);
  const rows = [...results]
    .sort((a, b) => new Date(b.evaluated_at) - new Date(a.evaluated_at))
    .slice(0, RECENT_ROWS);

  if (metaEl) {
    metaEl.textContent = `최신 ${RECENT_ROWS}건 · 누적 추천 ${totalRec}회 · 검증 ${totalEval}회 · 승 ${totalWin}회`;
  }

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="7" class="muted">검증 데이터가 아직 없습니다.</td>`;
    body.appendChild(tr);
    return;
  }

  for (const r of rows) {
    const side = sideOf(r);
    const symbol = String(r.symbol || "").toUpperCase();
    const recN = recCounts.get(symbol) || 0;
    const evalN = evalCounts.get(symbol) || 0;
    const winN = winCounts.get(symbol) || 0;
    const ret = Number(r.return_blended || 0);
    const klass = ret >= 0 ? "good" : "bad";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${fmtTime(r.evaluated_at)}</td>
      <td class="mono">${evalHorizonText(r)}</td>
      <td>${symbolCellHtml(symbol, recN, evalN, winN)}</td>
      <td>${sideBadgeHtml(side)}</td>
      <td class="mono">${modelLabel(modelOf(r))}</td>
      <td class="${klass}">${fmtPct(ret)}</td>
      <td><span class="result-pill ${r.win ? "result-win" : "result-loss"}">${resultLabel(Boolean(r.win))}</span></td>
    `;
    body.appendChild(tr);
  }
}

function computeModelRows(results, window = 240) {
  const recent = [...results]
    .filter((r) => r && r.evaluated_at)
    .sort((a, b) => new Date(a.evaluated_at) - new Date(b.evaluated_at))
    .slice(-window);
  const by = new Map();
  for (const r of recent) {
    const id = modelOf(r);
    if (!by.has(id)) by.set(id, []);
    by.get(id).push(r);
  }
  const rows = [];
  for (const [id, arr] of by.entries()) {
    const wr = winRate(arr);
    const avg = avgReturn(arr);
    const vals = arr.map((x) => Number(x.return_blended || 0)).sort((a, b) => a - b);
    const med = vals.length
      ? (vals.length % 2 ? vals[(vals.length - 1) / 2] : (vals[vals.length / 2 - 1] + vals[vals.length / 2]) / 2)
      : null;
    rows.push({
      id,
      label: modelLabel(id),
      count: arr.length,
      win_rate: wr,
      avg_return: avg,
      median_return: med,
    });
  }
  rows.sort((a, b) => b.count - a.count);
  return rows;
}

function renderModelMetrics(state, results) {
  const body = $("modelBody");
  if (!body) return;
  body.innerHTML = "";

  const latestRun = (state.run_history || []).slice(-1)[0] || {};
  const latestModelMetrics = latestRun.model_metrics || {};
  let rows = [];
  if (latestModelMetrics && Object.keys(latestModelMetrics).length) {
    rows = Object.entries(latestModelMetrics).map(([id, m]) => ({
      id,
      label: m.label || modelLabel(id),
      count: Number(m.count || 0),
      win_rate: Number(m.win_rate || 0),
      avg_return: Number(m.avg_return || 0),
      median_return: Number(m.median_return || 0),
    })).sort((a, b) => b.count - a.count);
  } else {
    rows = computeModelRows(results, 240);
  }

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="5" class="muted">모델 성과 데이터가 아직 없습니다.</td>`;
    body.appendChild(tr);
    return;
  }

  for (const r of rows) {
    const wrCls = r.win_rate >= 0.5 ? "good" : "bad";
    const avgCls = r.avg_return >= 0 ? "good" : "bad";
    const medCls = r.median_return >= 0 ? "good" : "bad";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono">${r.label}</td>
      <td>${r.count}</td>
      <td class="${wrCls}">${fmtPct(r.win_rate)}</td>
      <td class="${avgCls}">${fmtPct(r.avg_return)}</td>
      <td class="${medCls}">${fmtPct(r.median_return)}</td>
    `;
    body.appendChild(tr);
  }
}

function modelIssueLabel(dim) {
  const key = String(dim || "");
  const map = {
    alignment: "시장정합",
    funding: "펀딩",
    momentum: "모멘텀",
    open_interest: "OI",
    regime: "장세",
  };
  return map[key] || key || "-";
}

function renderModelLab(state) {
  const body = $("modelLabBody");
  if (!body) return;
  body.innerHTML = "";

  const latestRun = (state.run_history || []).slice(-1)[0] || {};
  const diag = latestRun.model_diagnostics || state.meta?.last_model_diagnostics || null;
  const rows = Array.isArray(diag?.items) ? diag.items : [];
  const rec = latestRun.model_recommendation || state.meta?.last_model_recommendation || null;
  const recRows = Array.isArray(rec?.recommendations) ? rec.recommendations : [];
  const latestModelMetrics = latestRun.model_metrics || {};

  if (!rows.length && recRows.length) {
    for (const r of recRows.slice(0, 6)) {
      const active = Array.isArray(r?.active_models) ? r.active_models : [];
      const activeMid = String(active[0] || "");
      const mm = activeMid ? (latestModelMetrics[activeMid] || null) : null;
      const wr = Number(mm?.win_rate);
      const ar = Number(mm?.avg_return);
      const n = Number(mm?.count || 0);
      const perfTxt = mm
        ? `n=${n} | win=${Number.isFinite(wr) ? fmtPct(wr) : "-"} | avg=${Number.isFinite(ar) ? fmtPct(ar) : "-"}`
        : `fit_edge=${fmtNum(Number(r?.fit_edge_vs_base || 0), 3)}`;
      const fit = Number(r?.fit_edge_vs_base);
      const issueTxt = Number.isFinite(fit)
        ? `시장적합 우위: fit_edge_vs_base ${fit >= 0 ? "+" : ""}${fmtNum(fit, 3)}`
        : "시장적합 기반 제안";
      const proposalTxt = `진단 데이터 누적 후 상세 원인 분석 갱신. 우선 ${modelLabel(String(r?.suggested_model || "-"))} 병행 검토`;
      const nextMid = String(r?.suggested_model || "-");
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="mono">${modelLabel(activeMid || nextMid)}</td>
        <td>${perfTxt}</td>
        <td>${issueTxt}</td>
        <td>${proposalTxt}</td>
        <td class="mono">${nextMid}</td>
      `;
      body.appendChild(tr);
    }
    return;
  }

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="5" class="muted">저성과 원인 진단 데이터가 아직 없습니다. (다음 실행에서 누적 후 표시)</td>`;
    body.appendChild(tr);
    return;
  }

  for (const r of rows.slice(0, 6)) {
    const wr = Number(r?.win_rate);
    const ar = Number(r?.avg_return);
    const wrTxt = Number.isFinite(wr) ? fmtPct(wr) : "-";
    const arTxt = Number.isFinite(ar) ? fmtPct(ar) : "-";
    const perfTxt = `n=${Number(r?.count || 0)} | win=${wrTxt} | avg=${arTxt}`;

    const issues = Array.isArray(r?.issues) ? r.issues : [];
    const issueTxt = issues.length
      ? issues.slice(0, 2).map((x) => {
        const n = Number(x?.count || 0);
        const w = Number(x?.win_rate);
        const wTxt = Number.isFinite(w) ? fmtPct(w) : "-";
        return `${modelIssueLabel(x?.dimension)}:${String(x?.bucket || "-")} (n=${n}, win=${wTxt})`;
      }).join(" | ")
      : "-";

    const proposals = Array.isArray(r?.proposals) ? r.proposals : [];
    const proposalTxt = proposals.length ? proposals.slice(0, 2).join(" ; ") : "-";

    const nextMid = String(r?.next_model_id || "-");
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono">${modelLabel(String(r?.model_id || "-"))}</td>
      <td>${perfTxt}</td>
      <td>${issueTxt}</td>
      <td>${proposalTxt}</td>
      <td class="mono">${nextMid}</td>
    `;
    body.appendChild(tr);
  }
}

function computeCalibrationRows(events, results, window = 30) {
  const out = [];
  const sortedResults = [...results].sort((a, b) => new Date(a.evaluated_at) - new Date(b.evaluated_at));
  for (const ev of events) {
    const at = new Date(ev.at).getTime();
    const before = sortedResults.filter((r) => new Date(r.evaluated_at).getTime() < at).slice(-window);
    const after = sortedResults.filter((r) => new Date(r.evaluated_at).getTime() >= at).slice(0, window);
    const wBefore = winRate(before);
    const wAfter = winRate(after);
    out.push({
      at: ev.at,
      notes: (ev.notes || []).join("; "),
      before: wBefore,
      after: wAfter,
      delta: wBefore == null || wAfter == null ? null : (wAfter - wBefore),
    });
  }
  return out;
}

function renderCalibrations(events, results) {
  const body = $("calBody");
  body.innerHTML = "";
  const rows = computeCalibrationRows(events, results).reverse().slice(0, CAL_ROWS);
  const calCountEl = $("kCalCount");
  const upliftEl = $("kUplift");
  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="5" class="muted">보정 이벤트가 아직 없습니다.</td>`;
    body.appendChild(tr);
    if (calCountEl) calCountEl.textContent = "0";
    if (upliftEl) upliftEl.textContent = "-";
    return;
  }
  if (calCountEl) calCountEl.textContent = String(rows.length);

  const latestDelta = rows.find((r) => r.delta != null)?.delta ?? null;
  if (upliftEl) {
    upliftEl.textContent = latestDelta == null ? "데이터없음" : fmtPct(latestDelta);
    upliftEl.className = `v ${latestDelta == null ? "" : latestDelta >= 0 ? "good" : "bad"}`;
  }

  for (const r of rows) {
    const dCls = r.delta == null ? "" : r.delta >= 0 ? "good" : "bad";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${fmtTime(r.at)}</td>
      <td>${r.before == null ? "데이터없음" : fmtPct(r.before)}</td>
      <td>${r.after == null ? "데이터없음" : fmtPct(r.after)}</td>
      <td class="${dCls}">${r.delta == null ? "데이터없음" : fmtPct(r.delta)}</td>
      <td>${r.notes || "-"}</td>
    `;
    body.appendChild(tr);
  }
}

function renderTrend(state) {
  const runHistory = state.run_history || [];
  let points = runHistory
    .filter((r) => r.metrics && Number.isFinite(Number(r.metrics.win_rate)))
    .slice(-90)
    .map((r) => ({ x: r.run_at, y: Number(r.metrics.win_rate) }));

  let source = "실행 이력";
  if (points.length < 2) {
    // Backward compatibility: build a synthetic trend from legacy results.
    const results = [...(state.results || [])]
      .filter((r) => r && r.evaluated_at)
      .sort((a, b) => new Date(a.evaluated_at) - new Date(b.evaluated_at));
    let wins = 0;
    points = results.map((r, idx) => {
      if (r.win) wins += 1;
      return { x: r.evaluated_at, y: wins / (idx + 1) };
    }).slice(-90);
    source = "검증 결과(누적)";
  }

  buildSvgLine($("trendChart"), points);
  $("trendMeta").textContent = points.length
    ? `최근 ${points.length}개 포인트 (${source})`
    : "추이 데이터가 아직 없습니다.";
}

function buildCumulativeSidePoints(results, side) {
  const filtered = [...results]
    .filter((r) => r && r.evaluated_at && sideOf(r) === side)
    .sort((a, b) => new Date(a.evaluated_at) - new Date(b.evaluated_at));
  let wins = 0;
  return filtered.map((r, idx) => {
    if (r.win) wins += 1;
    return { x: r.evaluated_at, y: wins / (idx + 1) };
  }).slice(-90);
}

function renderSideTrends(results) {
  const longEl = $("trendLongChart");
  const shortEl = $("trendShortChart");
  if (!longEl || !shortEl) return;

  const longPoints = buildCumulativeSidePoints(results, "LONG");
  const shortPoints = buildCumulativeSidePoints(results, "SHORT");

  buildSvgLine(longEl, longPoints, "#0e8a7b", "#ff9e57");
  buildSvgLine(shortEl, shortPoints, "#2f6bff", "#7f9bff");

  const longMeta = $("trendLongMeta");
  const shortMeta = $("trendShortMeta");
  const longLatest = longPoints.length ? Number(longPoints[longPoints.length - 1].y) : null;
  const shortLatest = shortPoints.length ? Number(shortPoints[shortPoints.length - 1].y) : null;
  if (longMeta) {
    longMeta.textContent = longPoints.length
      ? `LONG 승률 ${fmtPct(longLatest)} · 검증 ${longPoints.length}건`
      : "LONG 검증 데이터가 아직 없습니다.";
  }
  if (shortMeta) {
    shortMeta.textContent = shortPoints.length
      ? `SHORT 승률 ${fmtPct(shortLatest)} · 검증 ${shortPoints.length}건`
      : "SHORT 검증 데이터가 아직 없습니다.";
  }
}

function renderKpis(state, results) {
  const runHistory = state.run_history || [];
  const latestRun = runHistory[runHistory.length - 1];
  const rollingMetrics = latestRun?.metrics || null;
  const activeModelsEl = $("kActiveModels");
  const modelHintEl = $("kModelHint");
  const longWinEl = $("kLongWinRate");
  const shortWinEl = $("kShortWinRate");
  const lastPicksEl = $("kLastPicks");

  $("kLastRun").textContent = fmtTime(state.meta?.last_run_at);
  if (lastPicksEl) {
    const n = Number(latestRun?.picks_count);
    lastPicksEl.textContent = Number.isFinite(n) ? `${n}건` : "-";
  }
  $("kPending").textContent = String((state.pending || []).length);
  $("kWinRate").textContent = rollingMetrics ? fmtPct(Number(rollingMetrics.win_rate || 0)) : "-";
  $("kAvgReturn").textContent = rollingMetrics ? fmtPct(Number(rollingMetrics.avg_return || 0)) : "-";
  if (activeModelsEl) {
    const names = activeModelNames(state);
    activeModelsEl.textContent = names.length ? names.join(", ") : "-";
  }
  if (!rollingMetrics && results.length) {
    const recent = results.slice(-120);
    const wr = winRate(recent);
    const ar = avgReturn(recent);
    $("kWinRate").textContent = wr == null ? "-" : fmtPct(wr);
    $("kAvgReturn").textContent = ar == null ? "-" : fmtPct(ar);
  }

  if (modelHintEl) {
    const rec = latestRun?.model_recommendation || state.meta?.last_model_recommendation || null;
    const rows = Array.isArray(rec?.recommendations) ? rec.recommendations : [];
    if (rec?.triggered && rows.length) {
      const parts = rows
        .map((r) => {
          const side = String(r?.side || "").toUpperCase();
          const mid = String(r?.suggested_model || "");
          if (!side || !mid) return null;
          return `${side}:${modelShortLabel(mid)}`;
        })
        .filter(Boolean);
      if (parts.length) {
        modelHintEl.textContent = parts.join(" | ");
        modelHintEl.className = "v bad";
      } else {
        modelHintEl.textContent = "보류";
        modelHintEl.className = "v";
      }
    } else {
      modelHintEl.textContent = "정상";
      modelHintEl.className = "v good";
    }
  }

  if (longWinEl || shortWinEl) {
    const longRows = (results || []).filter((r) => sideOf(r) === "LONG");
    const shortRows = (results || []).filter((r) => sideOf(r) === "SHORT");
    const longWr = winRate(longRows);
    const shortWr = winRate(shortRows);
    if (longWinEl) {
      longWinEl.textContent = longWr == null ? "-" : fmtPct(longWr);
      longWinEl.className = `v ${longWr == null ? "" : longWr >= 0.5 ? "good" : "bad"}`;
    }
    if (shortWinEl) {
      shortWinEl.textContent = shortWr == null ? "-" : fmtPct(shortWr);
      shortWinEl.className = `v ${shortWr == null ? "" : shortWr >= 0.5 ? "good" : "bad"}`;
    }
  }
}

function renderMarketIndicators(state) {
  const body = $("marketBody");
  if (!body) return;
  body.innerHTML = "";
  const concentrationEl = $("concentrationText");

  const runHistory = state.run_history || [];
  const latestRun = runHistory[runHistory.length - 1] || {};
  const indicators = latestRun.market_indicators || null;
  const nowAlign = latestRun.market_alignment_now || {};
  const histAlign = latestRun.market_alignment_history || {};

  if (!indicators) {
    if (concentrationEl) concentrationEl.textContent = "쏠림: 데이터없음";
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="12" class="muted">시장 인디케이터 데이터가 아직 없습니다.</td>`;
    body.appendChild(tr);
    return;
  }
  if (concentrationEl) {
    concentrationEl.textContent = concentrationText(indicators.concentration || {});
  }

  const rows = [
    { key: "market", label: "시장 전체" },
    { key: "btc", label: "BTC" },
    { key: "eth", label: "ETH" },
  ];

  for (const row of rows) {
    const i = indicators[row.key] || {};
    const n = nowAlign[row.key] || {};
    const h = histAlign[row.key] || {};

    const changes = { ...(i.changes || {}) };
    if (
      (changes["24h"] == null || !Number.isFinite(Number(changes["24h"])))
      && Number.isFinite(Number(i.change24h))
    ) {
      changes["24h"] = Number(i.change24h);
    }
    const corrRaw = h.correlation;
    const hasCorr = corrRaw != null && Number.isFinite(Number(corrRaw));
    const corr = hasCorr ? Number(corrRaw) : null;
    const corrCls = hasCorr ? (corr >= 0 ? "good" : "bad") : "";
    const changeCells = MARKET_CHANGE_KEYS.map((k) => {
      const v = Number(changes[k]);
      const hasV = Number.isFinite(v);
      const cls = hasV ? (v >= 0 ? "good" : "bad") : "";
      const text = hasV ? fmtPct(v / 100) : "데이터없음";
      return `<td class="${cls}">${text}</td>`;
    }).join("");

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono">${row.label}</td>
      ${changeCells}
      <td>${trendText(i.trend)}</td>
      <td>${relationText(n.relation)}</td>
      <td class="${corrCls}">${hasCorr ? fmtNum(corr, 3) : "데이터없음"}</td>
      <td>${Number(h.sample || 0)}</td>
    `;
    body.appendChild(tr);
  }
}

function renderSymbolCorrelations(state) {
  const body = $("symbolCorrBody");
  if (!body) return;
  body.innerHTML = "";

  const recs = state.recommendation_history || [];
  const runHistory = state.run_history || [];
  const latestRun = runHistory[runHistory.length - 1] || {};
  const indicators = latestRun.market_indicators || {};
  const runSignSeries = { market: [], btc: [], eth: [] };
  const runValueSeries = { market: [], btc: [], eth: [] };
  for (const run of runHistory) {
    const ts = Date.parse(run?.run_at || "");
    if (!Number.isFinite(ts)) continue;
    for (const key of Object.keys(runSignSeries)) {
      const sign = Number(run?.market_indicators?.[key]?.sign || 0);
      if (Number.isFinite(sign) && sign !== 0) {
        runSignSeries[key].push({ ts, sign: sign > 0 ? 1 : -1 });
      }

      let change = Number(run?.market_indicators?.[key]?.changes?.["1h"]);
      if (!Number.isFinite(change)) {
        change = Number(run?.market_indicators?.[key]?.change24h);
      }
      if (Number.isFinite(change) && Math.abs(change) > 1e-9) {
        runValueSeries[key].push({ ts, value: change });
      }
    }
  }
  for (const key of Object.keys(runSignSeries)) {
    runSignSeries[key].sort((a, b) => a.ts - b.ts);
    runValueSeries[key].sort((a, b) => a.ts - b.ts);
  }
  function latestNonZeroSign(key) {
    const series = runSignSeries[key] || [];
    if (!series.length) return 0;
    return Number(series[series.length - 1]?.sign || 0);
  }

  function lookupSignAt(series, ts) {
    if (!Array.isArray(series) || !series.length) return 0;
    if (!Number.isFinite(ts)) return Number(series[series.length - 1]?.sign || 0);
    let last = 0;
    for (const p of series) {
      if (p.ts <= ts) {
        last = Number(p.sign || 0);
      } else {
        break;
      }
    }
    if (last !== 0) return last;
    return Number(series[0]?.sign || 0);
  }
  function lookupValueAt(series, ts) {
    if (!Array.isArray(series) || !series.length) return null;
    if (!Number.isFinite(ts)) return Number(series[series.length - 1]?.value ?? null);
    let last = null;
    for (const p of series) {
      if (p.ts <= ts) {
        last = Number(p.value ?? null);
      } else {
        break;
      }
    }
    if (last != null && Number.isFinite(last)) return last;
    const first = Number(series[0]?.value ?? null);
    return Number.isFinite(first) ? first : null;
  }

  const keyToField = {
    market: "market_sign_market",
    btc: "market_sign_btc",
    eth: "market_sign_eth",
  };

  const bySymbol = new Map();
  for (const r of recs) {
    if (!r || !r.symbol) continue;
    const sym = String(r.symbol);
    if (!bySymbol.has(sym)) {
      bySymbol.set(sym, {
        latest: r,
        market: { xs: [], ys: [] },
        btc: { xs: [], ys: [] },
        eth: { xs: [], ys: [] },
      });
    }
    const row = bySymbol.get(sym);
    if (new Date(r.created_at) > new Date(row.latest.created_at)) {
      row.latest = r;
    }
    const s = sideSign(r);
    const createdTs = Date.parse(r.created_at || "");
    for (const [k, f] of Object.entries(keyToField)) {
      let y = Number(r[`market_change_${k}_1h`]);
      if (!Number.isFinite(y)) {
        y = Number(r[`market_change_${k}_24h`]);
      }
      if (!Number.isFinite(y) || Math.abs(y) <= 1e-9) {
        y = Number(lookupValueAt(runValueSeries[k], createdTs));
      }
      if (!Number.isFinite(y) || Math.abs(y) <= 1e-9) {
        let m = Number(r[f]);
        if (!Number.isFinite(m) || m === 0) {
          m = lookupSignAt(runSignSeries[k], createdTs);
        }
        if (!Number.isFinite(m) || m === 0) continue;
        y = m > 0 ? 1 : -1;
      }
      row[k].xs.push(s);
      row[k].ys.push(y);
    }
  }

  const rows = [];
  for (const [sym, row] of bySymbol.entries()) {
    const cMarketPearson = pearsonCorr(row.market.xs, row.market.ys);
    const cBtcPearson = pearsonCorr(row.btc.xs, row.btc.ys);
    const cEthPearson = pearsonCorr(row.eth.xs, row.eth.ys);
    const cMarket = cMarketPearson == null
      ? directionalAgreement(row.market.xs, row.market.ys)
      : cMarketPearson;
    const cBtc = cBtcPearson == null
      ? directionalAgreement(row.btc.xs, row.btc.ys)
      : cBtcPearson;
    const cEth = cEthPearson == null
      ? directionalAgreement(row.eth.xs, row.eth.ys)
      : cEthPearson;

    const sNow = sideSign(row.latest);
    let mNow = Number(indicators?.market?.sign || 0);
    let bNow = Number(indicators?.btc?.sign || 0);
    let eNow = Number(indicators?.eth?.sign || 0);
    if (!Number.isFinite(mNow) || mNow === 0) mNow = latestNonZeroSign("market");
    if (!Number.isFinite(bNow) || bNow === 0) bNow = latestNonZeroSign("btc");
    if (!Number.isFinite(eNow) || eNow === 0) eNow = latestNonZeroSign("eth");
    if (!Number.isFinite(mNow) || mNow === 0) {
      const v = Number(lookupValueAt(runValueSeries.market, Date.now()));
      if (Number.isFinite(v) && Math.abs(v) > 1e-9) mNow = v > 0 ? 1 : -1;
    }
    if (!Number.isFinite(bNow) || bNow === 0) {
      const v = Number(lookupValueAt(runValueSeries.btc, Date.now()));
      if (Number.isFinite(v) && Math.abs(v) > 1e-9) bNow = v > 0 ? 1 : -1;
    }
    if (!Number.isFinite(eNow) || eNow === 0) {
      const v = Number(lookupValueAt(runValueSeries.eth, Date.now()));
      if (Number.isFinite(v) && Math.abs(v) > 1e-9) eNow = v > 0 ? 1 : -1;
    }
    const relNowMarket = mNow === 0 ? "neutral-market" : relationFromNumeric(sNow * mNow, 0.01);
    const relNowBtc = bNow === 0 ? "neutral-market" : relationFromNumeric(sNow * bNow, 0.01);
    const relNowEth = eNow === 0 ? "neutral-market" : relationFromNumeric(sNow * eNow, 0.01);

    rows.push({
      sym,
      relNowText: `${relationText(relNowMarket)}/${relationText(relNowBtc)}/${relationText(relNowEth)}`,
      cMarket,
      cBtc,
      cEth,
      sMarket: row.market.xs.length,
      sBtc: row.btc.xs.length,
      sEth: row.eth.xs.length,
    });
  }

  const sorted = rows
    .filter((r) => (r.sMarket + r.sBtc + r.sEth) > 0)
    .sort((a, b) => (b.sMarket + b.sBtc + b.sEth) - (a.sMarket + a.sBtc + a.sEth))
    .slice(0, CORR_ROWS);

  if (!sorted.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="6" class="muted">종목별 상관도 데이터가 아직 없습니다.</td>`;
    body.appendChild(tr);
    return;
  }

  for (const r of sorted) {
    const hasM = r.cMarket != null && Number.isFinite(Number(r.cMarket));
    const hasB = r.cBtc != null && Number.isFinite(Number(r.cBtc));
    const hasE = r.cEth != null && Number.isFinite(Number(r.cEth));
    const mVal = hasM ? Number(r.cMarket) : null;
    const bVal = hasB ? Number(r.cBtc) : null;
    const eVal = hasE ? Number(r.cEth) : null;
    const clsM = hasM ? (mVal >= 0 ? "good" : "bad") : "";
    const clsB = hasB ? (bVal >= 0 ? "good" : "bad") : "";
    const clsE = hasE ? (eVal >= 0 ? "good" : "bad") : "";

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono">${r.sym}</td>
      <td>${r.relNowText}</td>
      <td class="${clsM}">${hasM ? fmtNum(mVal, 3) : "데이터없음"}</td>
      <td class="${clsB}">${hasB ? fmtNum(bVal, 3) : "데이터없음"}</td>
      <td class="${clsE}">${hasE ? fmtNum(eVal, 3) : "데이터없음"}</td>
      <td>${r.sMarket}/${r.sBtc}/${r.sEth}</td>
    `;
    body.appendChild(tr);
  }
}

function setupTabs() {
  const buttons = [...document.querySelectorAll(".tab-btn[data-tab]")];
  const panels = [...document.querySelectorAll(".panel[data-panel]")];
  if (!buttons.length || !panels.length) return;

  const activate = (tab) => {
    buttons.forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === tab);
    });
    panels.forEach((panel) => {
      panel.classList.toggle("hidden", panel.dataset.panel !== tab);
    });
    try {
      localStorage.setItem("momentum_dashboard_tab", tab);
    } catch (_) {
      // no-op
    }
  };

  buttons.forEach((btn) => {
    btn.addEventListener("click", () => activate(String(btn.dataset.tab || "summary")));
  });

  let initial = "summary";
  try {
    initial = localStorage.getItem("momentum_dashboard_tab") || "summary";
  } catch (_) {
    initial = "summary";
  }
  if (!buttons.some((btn) => btn.dataset.tab === initial)) initial = "summary";
  activate(initial);
}

async function loadAndRender() {
  const owner = $("ownerInput").value.trim();
  const repo = $("repoInput").value.trim();
  const branch = $("branchInput").value.trim() || "main";
  const urls = candidateStateUrls(owner, repo, branch);

  $("sourceText").textContent = `소스: owner=${owner} repo=${repo} branch=${branch}`;
  $("loadStatus").textContent = "상태 데이터를 불러오는 중...";
  $("loadStatus").className = "status muted";

  try {
    const { data: state, url } = await fetchJsonNewest(urls);
    const results = state.results || [];
    renderKpis(state, results);
    renderMarketIndicators(state);
    renderSymbolCorrelations(state);
    renderTrend(state);
    renderSideTrends(results);
    renderModelMetrics(state, results);
    renderModelLab(state);
    renderRules(state.dynamic_config || {});
    renderPicks(state);
    renderEvaluations(state, results);
    renderCalibrations(state.calibration_events || [], results);

    $("loadStatus").textContent = `불러오기 성공: ${url}`;
    $("loadStatus").className = "status good";
  } catch (e) {
    $("loadStatus").textContent = `불러오기 실패: ${String(e)}`;
    $("loadStatus").className = "status bad";
  }
}

$("refreshBtn").addEventListener("click", loadAndRender);
$("applySourceBtn").addEventListener("click", loadAndRender);
setupTabs();

loadAndRender();
setInterval(loadAndRender, 60000);

