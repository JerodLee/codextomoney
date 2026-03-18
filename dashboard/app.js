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
let latestLoadedState = null;
let selectedAnalyzeSymbol = "BTC";

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

function fmtSignedPctValue(v, d = 2) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "-";
  return `${x >= 0 ? "+" : ""}${x.toFixed(d)}%`;
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
    momentum_long_v3: "롱 스윙 v3(수익확장/보유형)",
    momentum_short_v3: "숏 스윙 v3(수익확장/보유형)",
  };
  return map[modelId] || modelId || "-";
}

function modelShortLabel(modelId) {
  const map = {
    momentum_long_v1: "롱v1",
    momentum_short_v1: "숏v1",
    momentum_long_v2: "롱v2",
    momentum_short_v2: "숏v2",
    momentum_long_v3: "롱v3",
    momentum_short_v3: "숏v3",
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
  const setupQualityStored = Number(row?.setup_quality);
  const setupQualityLabel = String(row?.setup_quality_label || "").toUpperCase();
  const setupEntryMode = String(row?.setup_entry_mode || "").toLowerCase();
  const expectedEdgePctStored = Number(row?.expected_edge_pct);
  const sizePctStored = Number(row?.position_size_pct);
  const riskPctStored = Number(row?.risk_per_trade_pct);
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
    setupQuality: Number.isFinite(setupQualityStored) ? setupQualityStored : null,
    setupQualityLabel: setupQualityLabel || null,
    setupEntryMode: setupEntryMode || null,
    expectedEdgePct: Number.isFinite(expectedEdgePctStored) ? expectedEdgePctStored : null,
    sizePct: Number.isFinite(sizePctStored) ? sizePctStored : null,
    riskPct: Number.isFinite(riskPctStored) ? riskPctStored : null,
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
  const q = Number(plan?.setupQuality);
  const qLabel = String(plan?.setupQualityLabel || "");
  const edge = Number(plan?.expectedEdgePct);
  const sizePct = Number(plan?.sizePct);
  const riskPct = Number(plan?.riskPct);
  const mode = String(plan?.setupEntryMode || "");
  const modeMap = { trend: "trend", balanced: "balanced", contrarian: "contrarian" };
  const modeText = modeMap[mode] || "-";
  const edgeText = Number.isFinite(edge) ? `${edge >= 0 ? "+" : ""}${edge.toFixed(2)}%` : "-";
  const qText = Number.isFinite(q) ? `${(q * 100).toFixed(0)}` : "-";
  const sizeText = Number.isFinite(sizePct) ? `${sizePct.toFixed(2)}%` : "-";
  const riskText = Number.isFinite(riskPct) ? `${riskPct.toFixed(2)}%` : "-";
  const parts = [];
  if (Number.isFinite(rr) && rr > 0) parts.push(`rr ${fmtPctValue(rr, 2)}`);
  if (Number.isFinite(ob) && ob > 0) parts.push(`ob ${fmtPctValue(ob, 2)}`);
  if (Number.isFinite(flow) && flow > 0) parts.push(`flow ${fmtPctValue(flow, 2)}`);
  return `
    <div class="plan-cell">
      <div class="mono plan-line">TP ${fmtPctValue(plan?.targetPct, 2)}</div>
      <div class="plan-line">${basis || "-"}</div>
      <div class="plan-line">${parts.length ? parts.join(" | ") : "-"}</div>
      <div class="plan-line">Q ${qLabel || "-"} (${qText}) | edge ${edgeText}</div>
      <div class="plan-line">mode ${modeText}</div>
      <div class="plan-line">size ${sizeText} | risk ${riskText}</div>
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

function renderMarketScene(state) {
  const sceneEl = $("marketScene");
  const legendEl = $("marketSceneLegend");
  const metaEl = $("marketSceneMeta");
  if (!sceneEl) return;

  if (legendEl) legendEl.innerHTML = "";
  if (metaEl) metaEl.textContent = "시장 입체 그래프를 계산 중입니다...";

  const runHistory = Array.isArray(state?.run_history) ? state.run_history : [];
  const latestRun = runHistory[runHistory.length - 1] || {};
  const indicators = latestRun?.market_indicators || null;
  const recs = Array.isArray(state?.recommendation_history) ? state.recommendation_history : [];

  if (!indicators) {
    sceneEl.innerHTML = `<text x="18" y="32" fill="#5c6a72" font-size="14">시장 데이터가 아직 없습니다.</text>`;
    if (metaEl) metaEl.textContent = "최신 run에 market_indicators가 없어 그래프를 만들 수 없습니다.";
    return;
  }

  const WEIGHTS = {
    "24h": 0.22,
    "12h": 0.18,
    "6h": 0.16,
    "1h": 0.14,
    "15m": 0.12,
    "5m": 0.10,
    "1m": 0.08,
  };
  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
  const finiteOrNull = (v) => {
    const x = Number(v);
    return Number.isFinite(x) ? x : null;
  };
  const avg = (arr) => {
    const vals = (arr || []).map((x) => Number(x)).filter((x) => Number.isFinite(x));
    if (!vals.length) return null;
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  };
  const normalize = (value, min, max, fallback = 0.5) => {
    if (!Number.isFinite(value)) return fallback;
    if (!Number.isFinite(min) || !Number.isFinite(max) || Math.abs(max - min) < 1e-9) {
      return fallback;
    }
    return clamp((value - min) / (max - min), 0, 1);
  };
  const weightedMomentum = (entry) => {
    if (!entry || typeof entry !== "object") return null;
    const changes = { ...(entry.changes || {}) };
    const c24 = finiteOrNull(entry.change24h);
    if (!Number.isFinite(Number(changes["24h"])) && c24 != null) {
      changes["24h"] = c24;
    }
    let num = 0;
    let den = 0;
    for (const [k, w] of Object.entries(WEIGHTS)) {
      const v = finiteOrNull(changes[k]);
      if (v == null) continue;
      num += v * w;
      den += w;
    }
    if (den <= 0) return null;
    return num / den;
  };
  const turbulence = (entry, momentumFallback) => {
    if (!entry || typeof entry !== "object") return momentumFallback == null ? 0 : Math.abs(momentumFallback) * 0.35;
    const changes = { ...(entry.changes || {}) };
    const c24 = finiteOrNull(entry.change24h);
    if (!Number.isFinite(Number(changes["24h"])) && c24 != null) {
      changes["24h"] = c24;
    }
    const vals = MARKET_CHANGE_KEYS
      .map((k) => finiteOrNull(changes[k]))
      .filter((x) => x != null);
    if (!vals.length) return momentumFallback == null ? 0 : Math.abs(momentumFallback) * 0.35;
    const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
    const mad = vals.reduce((a, b) => a + Math.abs(b - mean), 0) / vals.length;
    return mad + (momentumFallback == null ? 0 : Math.abs(momentumFallback) * 0.2);
  };

  const concentration = indicators.concentration || {};
  const marketMom = weightedMomentum(indicators.market || {});
  const btcMom = weightedMomentum(indicators.btc || {});
  const ethMom = weightedMomentum(indicators.eth || {});
  const marketRisk = turbulence(indicators.market || {}, marketMom);
  const btcRisk = turbulence(indicators.btc || {}, btcMom);
  const ethRisk = turbulence(indicators.eth || {}, ethMom);

  const baseNodes = [
    {
      id: "market",
      label: "시장",
      type: "core",
      color: "#0e8a7b",
      momentum: marketMom,
      risk: marketRisk,
      liquidity: 1,
    },
    {
      id: "btc",
      label: "BTC",
      type: "core",
      color: "#f59d28",
      momentum: btcMom,
      risk: btcRisk,
      liquidity: finiteOrNull(concentration.btc_share) ?? 0.45,
    },
    {
      id: "eth",
      label: "ETH",
      type: "core",
      color: "#3668ff",
      momentum: ethMom,
      risk: ethRisk,
      liquidity: finiteOrNull(concentration.eth_share) ?? 0.25,
    },
  ];
  const altShare = finiteOrNull(concentration.alt_share);
  if (altShare != null && altShare > 0.01) {
    const coreMean = avg([marketMom, btcMom, ethMom]);
    const altMom = marketMom != null && coreMean != null ? marketMom - coreMean * 0.45 : marketMom;
    const altRisk = avg([marketRisk, btcRisk, ethRisk]) ?? 0;
    baseNodes.push({
      id: "alt",
      label: "ALT",
      type: "core",
      color: "#7f8f2f",
      momentum: altMom,
      risk: altRisk,
      liquidity: altShare,
    });
  }

  const sortedPicks = [...recs].sort((a, b) => new Date(b?.created_at || 0) - new Date(a?.created_at || 0));
  const seenSymbols = new Set();
  const pickNodes = [];
  for (const p of sortedPicks) {
    const symbol = String(p?.symbol || "").toUpperCase().trim();
    if (!symbol || seenSymbols.has(symbol)) continue;
    seenSymbols.add(symbol);

    const bRate = finiteOrNull(p?.b_rate24h);
    const gRate = finiteOrNull(p?.g_rate24h);
    const momentum = avg([bRate, gRate]);
    if (momentum == null) continue;

    const funding = Math.abs(finiteOrNull(p?.g_funding_rate) ?? 0) * 10000;
    const oi = finiteOrNull(p?.g_open_interest);
    const oiScore = oi != null && oi > 0 ? clamp(Math.log10(oi + 1) / 7, 0, 1) : 0;
    const score = finiteOrNull(p?.score) ?? 0;
    const confidence = clamp(score, 0, 1);
    const risk = funding + Math.abs(momentum) * 0.08 + oiScore + confidence * 0.8;

    const bValue = finiteOrNull(p?.b_value24h);
    const gVol = finiteOrNull(p?.g_volume24h);
    const liqB = bValue != null && bValue > 0 ? Math.log10(bValue + 1) : 0;
    const liqG = gVol != null && gVol > 0 ? Math.log10(gVol + 1) : 0;
    const liquidity = liqB * 0.55 + liqG * 0.45;

    const side = sideOf(p);
    pickNodes.push({
      id: `pick-${symbol}`,
      label: symbol,
      type: "pick",
      side,
      color: side === "SHORT" ? "#d55252" : "#159a5b",
      momentum,
      risk,
      liquidity,
      score: confidence,
    });
    if (pickNodes.length >= 10) break;
  }

  const nodes = [...baseNodes, ...pickNodes].filter((n) => n.momentum != null);
  if (!nodes.length) {
    sceneEl.innerHTML = `<text x="18" y="32" fill="#5c6a72" font-size="14">시각화할 포인트가 아직 없습니다.</text>`;
    if (metaEl) metaEl.textContent = "추천 또는 시장 인디케이터 축 데이터가 부족합니다.";
    return;
  }

  const maxAbsMomentum = Math.max(1, ...nodes.map((n) => Math.abs(Number(n.momentum) || 0)));
  const riskVals = nodes.map((n) => Number(n.risk) || 0);
  const liqVals = nodes.map((n) => Number(n.liquidity) || 0);
  const riskMin = Math.min(...riskVals);
  const riskMax = Math.max(...riskVals);
  const liqMin = Math.min(...liqVals);
  const liqMax = Math.max(...liqVals);

  for (const n of nodes) {
    n.xn = clamp((Number(n.momentum) || 0) / maxAbsMomentum, -1, 1);
    n.yn = normalize(Number(n.risk) || 0, riskMin, riskMax, 0.42);
    n.zn = normalize(Number(n.liquidity) || 0, liqMin, liqMax, n.type === "core" ? 0.68 : 0.45);
  }

  const project = (x, y, z) => {
    const px = 480 + x * 248 + (z - 0.5) * 190;
    const py = 318 - y * 186 - (z - 0.5) * 108;
    return { x: px, y: py };
  };
  const pointPath = (arr) => arr.map((p, idx) => `${idx ? "L" : "M"}${p.x.toFixed(2)} ${p.y.toFixed(2)}`).join(" ");

  const floor = [
    project(-1, 0, 0),
    project(1, 0, 0),
    project(1, 0, 1),
    project(-1, 0, 1),
  ];
  const xAxisA = project(-1, 0, 0);
  const xAxisB = project(1, 0, 0);
  const yAxisA = project(-1, 0, 0);
  const yAxisB = project(-1, 1, 0);
  const zAxisA = project(1, 0, 0);
  const zAxisB = project(1, 0, 1);

  const gridParts = [];
  for (const y of [0.25, 0.5, 0.75]) {
    const p = [project(-1, y, 0), project(1, y, 0), project(1, y, 1), project(-1, y, 1), project(-1, y, 0)];
    gridParts.push(`<path d="${pointPath(p)}" fill="none" stroke="#cfe0df" stroke-width="1" />`);
  }
  for (const x of [-0.5, 0, 0.5]) {
    const p = [project(x, 0, 0), project(x, 1, 0), project(x, 1, 1), project(x, 0, 1)];
    gridParts.push(`<path d="${pointPath(p)}" fill="none" stroke="#d9e7e7" stroke-width="1" />`);
  }
  for (const z of [0.25, 0.5, 0.75]) {
    const p = [project(-1, 0, z), project(-1, 1, z), project(1, 1, z), project(1, 0, z)];
    gridParts.push(`<path d="${pointPath(p)}" fill="none" stroke="#e0ecec" stroke-width="1" />`);
  }

  const nodesSorted = [...nodes].sort((a, b) => a.zn - b.zn);
  const nodeParts = [];
  for (const n of nodesSorted) {
    const p = project(n.xn, n.yn, n.zn);
    const shadow = project(n.xn, 0, n.zn);
    const radius = (n.type === "core" ? 7 : 4.5) + n.zn * 4.8;
    const label = n.type === "pick" ? `${n.label} ${n.side === "SHORT" ? "S" : "L"}` : n.label;
    const tx = p.x + (n.xn >= 0 ? 10 : -10);
    const ty = p.y - (n.type === "core" ? 12 : 8);
    const anchor = n.xn >= 0 ? "start" : "end";
    nodeParts.push(`<line x1="${p.x.toFixed(2)}" y1="${p.y.toFixed(2)}" x2="${shadow.x.toFixed(2)}" y2="${shadow.y.toFixed(2)}" stroke="#d2dddd" stroke-width="1" stroke-dasharray="3 3" />`);
    nodeParts.push(`<circle cx="${p.x.toFixed(2)}" cy="${p.y.toFixed(2)}" r="${radius.toFixed(2)}" fill="${n.color}" fill-opacity="${n.type === "core" ? "0.92" : "0.82"}" stroke="#ffffff" stroke-width="1.6" />`);
    nodeParts.push(`<text x="${tx.toFixed(2)}" y="${ty.toFixed(2)}" text-anchor="${anchor}" font-size="${n.type === "core" ? "13" : "11"}" font-weight="${n.type === "core" ? "700" : "600"}" fill="#243238">${label}</text>`);
  }

  sceneEl.innerHTML = `
    <defs>
      <linearGradient id="sceneFloorGrad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="#f4fbfa" />
        <stop offset="100%" stop-color="#e9f2f1" />
      </linearGradient>
    </defs>
    <rect x="0" y="0" width="960" height="400" fill="transparent"></rect>
    <polygon points="${floor.map((p) => `${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(" ")}" fill="url(#sceneFloorGrad)" stroke="#d3e1df" stroke-width="1.2"></polygon>
    ${gridParts.join("")}
    <line x1="${xAxisA.x.toFixed(2)}" y1="${xAxisA.y.toFixed(2)}" x2="${xAxisB.x.toFixed(2)}" y2="${xAxisB.y.toFixed(2)}" stroke="#97aeb3" stroke-width="2"></line>
    <line x1="${yAxisA.x.toFixed(2)}" y1="${yAxisA.y.toFixed(2)}" x2="${yAxisB.x.toFixed(2)}" y2="${yAxisB.y.toFixed(2)}" stroke="#97aeb3" stroke-width="2"></line>
    <line x1="${zAxisA.x.toFixed(2)}" y1="${zAxisA.y.toFixed(2)}" x2="${zAxisB.x.toFixed(2)}" y2="${zAxisB.y.toFixed(2)}" stroke="#97aeb3" stroke-width="2"></line>
    <text x="${(xAxisB.x + 10).toFixed(2)}" y="${(xAxisB.y + 6).toFixed(2)}" font-size="12" fill="#526069">모멘텀</text>
    <text x="${(yAxisB.x - 2).toFixed(2)}" y="${(yAxisB.y - 10).toFixed(2)}" font-size="12" fill="#526069">리스크</text>
    <text x="${(zAxisB.x + 8).toFixed(2)}" y="${(zAxisB.y - 6).toFixed(2)}" font-size="12" fill="#526069">유동성</text>
    ${nodeParts.join("")}
  `;

  if (legendEl) {
    const hasAlt = nodes.some((n) => n.id === "alt");
    const chips = [
      { label: "시장/BTC/ETH", color: "#0e8a7b" },
      ...(hasAlt ? [{ label: "ALT 클러스터", color: "#7f8f2f" }] : []),
      { label: "추천 LONG", color: "#159a5b" },
      { label: "추천 SHORT", color: "#d55252" },
    ];
    legendEl.innerHTML = chips
      .map((c) => `<span class="scene-chip"><span class="scene-dot" style="background:${c.color}"></span>${c.label}</span>`)
      .join("");
  }

  if (metaEl) {
    const marketNode = nodes.find((n) => n.id === "market");
    const riskiest = [...pickNodes].sort((a, b) => (Number(b.risk) || 0) - (Number(a.risk) || 0))[0];
    const deepest = [...pickNodes].sort((a, b) => (Number(b.liquidity) || 0) - (Number(a.liquidity) || 0))[0];
    const parts = [`시장 모멘텀 ${fmtSignedPctValue(marketNode?.momentum, 2)}`];
    if (riskiest) parts.push(`리스크 상단 ${riskiest.label}(${riskiest.side})`);
    if (deepest) parts.push(`유동성 상단 ${deepest.label}(${deepest.side})`);
    metaEl.textContent = `입체 맵 해석: ${parts.join(" | ")}.`;
  }
}

function renderSocialBuzz(state) {
  const body = $("socialBuzzBody");
  const metaEl = $("socialBuzzMeta");
  const sourceEl = $("socialBuzzSource");
  if (!body) return;
  body.innerHTML = "";

  const runHistory = Array.isArray(state?.run_history) ? state.run_history : [];
  const latestRun = runHistory[runHistory.length - 1] || {};
  const buzzHistory = Array.isArray(state?.social_buzz_history) ? state.social_buzz_history : [];
  const latestBuzz = latestRun?.social_buzz || state?.meta?.last_social_buzz || buzzHistory[buzzHistory.length - 1] || null;

  if (!latestBuzz) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="7" class="muted">회자 종목 데이터가 아직 없습니다.</td>`;
    body.appendChild(tr);
    if (metaEl) metaEl.textContent = "최신 소셜 집계 없음";
    if (sourceEl) sourceEl.textContent = "소스 상태: X/Threads 미수집";
    return;
  }

  const rows = Array.isArray(latestBuzz?.top_symbols) ? latestBuzz.top_symbols : [];
  const p = latestBuzz?.providers || {};
  const x = p.x || {};
  const t = p.threads || {};
  const xEnabled = Boolean(x.enabled);
  const tEnabled = Boolean(t.enabled);
  const xOk = Boolean(x.ok);
  const tOk = Boolean(t.ok);

  if (!rows.length) {
    const tr = document.createElement("tr");
    let reason = "현재 구간에서 유의미한 회자 종목이 없습니다.";
    if (!xEnabled && !tEnabled) {
      reason = "X/Threads 토큰이 설정되지 않아 소셜 집계를 수집하지 못했습니다.";
    } else if ((xEnabled && !xOk) && (tEnabled && !tOk)) {
      reason = "X/Threads 소스 오류로 집계가 비어 있습니다. 소스 상태를 확인해 주세요.";
    } else if ((xEnabled && !xOk) || (tEnabled && !tOk)) {
      reason = "일부 소스 오류로 집계가 제한되었습니다. 소스 상태를 확인해 주세요.";
    }
    tr.innerHTML = `<td colspan="7" class="muted">${reason}</td>`;
    body.appendChild(tr);
  }

  let prevBuzz = null;
  const nowAt = String(latestBuzz?.at || "");
  for (let i = buzzHistory.length - 1; i >= 0; i -= 1) {
    const b = buzzHistory[i];
    if (!b || !Array.isArray(b.top_symbols)) continue;
    if (nowAt && String(b.at || "") === nowAt) continue;
    prevBuzz = b;
    break;
  }
  if (!prevBuzz && runHistory.length >= 2) {
    for (let i = runHistory.length - 2; i >= 0; i -= 1) {
      const b = runHistory[i]?.social_buzz;
      if (b && Array.isArray(b.top_symbols)) {
        prevBuzz = b;
        break;
      }
    }
  }
  const prevRank = new Map();
  if (prevBuzz && Array.isArray(prevBuzz.top_symbols)) {
    prevBuzz.top_symbols.forEach((r, idx) => {
      const sym = String(r?.symbol || "").toUpperCase();
      if (!sym) return;
      prevRank.set(sym, idx + 1);
    });
  }

  for (let i = 0; i < rows.length; i += 1) {
    const r = rows[i] || {};
    const rankNow = i + 1;
    const sym = String(r.symbol || "-").toUpperCase();
    const score = Number(r.score);
    const mention = Number(r.mentions_total);
    const xMention = Number(r.x_mentions);
    const tMention = Number(r.threads_mentions);
    const oldRank = prevRank.has(sym) ? Number(prevRank.get(sym)) : null;

    let deltaHtml = `<span class="delta-chip delta-flat">SAME</span>`;
    if (!Number.isFinite(oldRank)) {
      deltaHtml = `<span class="delta-chip delta-up">NEW</span>`;
    } else if (oldRank > rankNow) {
      deltaHtml = `<span class="delta-chip delta-up">UP ${oldRank - rankNow}</span>`;
    } else if (oldRank < rankNow) {
      deltaHtml = `<span class="delta-chip delta-down">DOWN ${rankNow - oldRank}</span>`;
    }

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono">${rankNow}</td>
      <td class="mono">${sym} / ${symbolName(sym)}</td>
      <td class="${Number.isFinite(score) && score > 0 ? "good" : ""}">${Number.isFinite(score) ? fmtNum(score, 3) : "-"}</td>
      <td>${Number.isFinite(mention) ? Math.trunc(mention) : "-"}</td>
      <td>${Number.isFinite(xMention) ? Math.trunc(xMention) : "-"}</td>
      <td>${Number.isFinite(tMention) ? Math.trunc(tMention) : "-"}</td>
      <td>${deltaHtml}</td>
    `;
    body.appendChild(tr);
  }

  if (metaEl) {
    const tsTxt = fmtTime(latestBuzz?.at);
    const n = Number(latestBuzz?.symbols_considered || 0);
    metaEl.textContent = `집계시각 ${tsTxt} · 집계대상 ${Number.isFinite(n) ? n : 0}종목`;
  }
  if (sourceEl) {
    const xTxt = !x.enabled
      ? "X off"
      : (x.ok ? `X ok(posts ${Number(x.sample_posts || 0)})` : `X err(${String(x.error || "unknown")})`);
    const tTxt = !t.enabled
      ? "Threads off"
      : (t.ok
        ? `Threads ok(posts ${Number(t.sample_posts || 0)}, q ${Number(t.ok_queries || 0)}/${Number(t.queries || 0)})`
        : `Threads err(${String(t.error || "unknown")})`);
    sourceEl.textContent = `소스 상태: ${xTxt} | ${tTxt}`;
  }
}

function normalizeAnalyzeSymbol(raw) {
  const cleaned = String(raw || "")
    .toUpperCase()
    .trim()
    .replace(/[^A-Z0-9]/g, "");
  if (!cleaned) return "";
  const noSuffix = cleaned.replace(/USDT(P|M)?$/, "");
  return noSuffix || cleaned;
}

function buildBitgetChartEmbedUrl(symbolBase) {
  const base = normalizeAnalyzeSymbol(symbolBase) || "BTC";
  const tvSymbol = `BITGET:${base}USDT.P`;
  const params = new URLSearchParams({
    symbol: tvSymbol,
    interval: "15",
    theme: "light",
    style: "1",
    locale: "kr",
    timezone: "Asia/Seoul",
    withdateranges: "1",
    hide_top_toolbar: "0",
    hide_side_toolbar: "0",
    allow_symbol_change: "0",
    saveimage: "0",
    toolbarbg: "f1f3f6",
    hideideas: "1",
  });
  return `https://s.tradingview.com/widgetembed/?${params.toString()}`;
}

function analyzeSymbolFromState(state, symbolBase) {
  const sym = normalizeAnalyzeSymbol(symbolBase);
  const recsAll = Array.isArray(state?.recommendation_history) ? state.recommendation_history : [];
  const resultsAll = Array.isArray(state?.results) ? state.results : [];
  const recs = recsAll.filter((r) => String(r?.symbol || "").toUpperCase() === sym);
  const results = resultsAll.filter((r) => String(r?.symbol || "").toUpperCase() === sym);

  const sortedRecs = [...recs].sort((a, b) => new Date(b?.created_at || 0) - new Date(a?.created_at || 0));
  const sortedResults = [...results].sort((a, b) => new Date(b?.evaluated_at || 0) - new Date(a?.evaluated_at || 0));
  const latestRec = sortedRecs[0] || null;
  const latestEval = sortedResults[0] || null;

  const winCount = sortedResults.filter((r) => Boolean(r?.win)).length;
  const winRateVal = sortedResults.length ? (winCount / sortedResults.length) : null;
  const returns = sortedResults
    .map((r) => Number(r?.return_blended))
    .filter((x) => Number.isFinite(x));
  const avgRet = returns.length ? (returns.reduce((a, b) => a + b, 0) / returns.length) : null;
  const medianRet = (() => {
    if (!returns.length) return null;
    const arr = [...returns].sort((a, b) => a - b);
    const m = Math.floor(arr.length / 2);
    return arr.length % 2 === 1 ? arr[m] : (arr[m - 1] + arr[m]) / 2;
  })();

  const longCount = sortedRecs.filter((r) => sideOf(r) === "LONG").length;
  const shortCount = sortedRecs.filter((r) => sideOf(r) === "SHORT").length;
  let sideBias = "Balanced";
  if (longCount > shortCount) sideBias = `LONG bias (${longCount}:${shortCount})`;
  if (shortCount > longCount) sideBias = `SHORT bias (${longCount}:${shortCount})`;

  const modelCounter = new Map();
  for (const r of sortedRecs) {
    const m = modelOf(r);
    modelCounter.set(m, (modelCounter.get(m) || 0) + 1);
  }
  const topModels = [...modelCounter.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([m, n]) => `${modelShortLabel(m)}(${n})`);

  const latestMomentum24h = latestRec
    ? (() => {
      const b = Number(latestRec?.b_rate24h);
      const g = Number(latestRec?.g_rate24h);
      const vals = [b, g].filter((x) => Number.isFinite(x));
      if (!vals.length) return null;
      return vals.reduce((a, b2) => a + b2, 0) / vals.length;
    })()
    : null;

  const fundingAbs = latestRec ? Math.abs(Number(latestRec?.g_funding_rate || 0)) : null;
  const moveAbs = latestMomentum24h == null ? null : Math.abs(latestMomentum24h);
  let riskTag = "Neutral";
  if (fundingAbs != null && moveAbs != null) {
    if (fundingAbs >= 0.003 || moveAbs >= 20) riskTag = "Overheat / High";
    else if (fundingAbs >= 0.001 || moveAbs >= 10) riskTag = "Caution";
    else if (fundingAbs <= 0.0003 && moveAbs <= 4) riskTag = "Stable";
  }

  return {
    symbol: sym,
    recCount: sortedRecs.length,
    evalCount: sortedResults.length,
    winCount,
    winRate: winRateVal,
    avgReturn: avgRet,
    medianReturn: medianRet,
    longCount,
    shortCount,
    sideBias,
    topModels,
    latestRec,
    latestEval,
    latestMomentum24h,
    riskTag,
  };
}

function renderSymbolAnalyzer(state, rawSymbol) {
  const inputEl = $("symbolAnalyzeInput");
  const frameEl = $("bitgetChartFrame");
  const statusEl = $("symbolAnalyzeStatus");
  const bodyEl = $("symbolAnalysisBody");
  if (!inputEl || !frameEl || !statusEl || !bodyEl) return;

  const sym = normalizeAnalyzeSymbol(rawSymbol || inputEl.value || selectedAnalyzeSymbol || "BTC");
  if (!sym) {
    statusEl.textContent = "Enter a symbol code like BTC, ETH, TAO.";
    statusEl.className = "status bad";
    bodyEl.innerHTML = `<p class="muted">Type a symbol and click Analyze.</p>`;
    return;
  }

  selectedAnalyzeSymbol = sym;
  if (inputEl.value !== sym) inputEl.value = sym;
  try {
    localStorage.setItem("momentum_symbol_analyzer", sym);
  } catch (_) {
    // no-op
  }

  const chartUrl = buildBitgetChartEmbedUrl(sym);
  if (frameEl.src !== chartUrl) frameEl.src = chartUrl;

  if (!state) {
    statusEl.textContent = `${sym}USDT chart loaded. Waiting for dashboard state to complete analysis.`;
    statusEl.className = "status muted";
    bodyEl.innerHTML = `<p class="muted">State data is loading...</p>`;
    return;
  }

  const stats = analyzeSymbolFromState(state, sym);
  const wrTxt = stats.winRate == null ? "-" : fmtPct(stats.winRate);
  const wrCls = stats.winRate == null ? "" : (stats.winRate >= 0.5 ? "good" : "bad");
  const avgTxt = stats.avgReturn == null ? "-" : fmtPct(stats.avgReturn);
  const avgCls = stats.avgReturn == null ? "" : (stats.avgReturn >= 0 ? "good" : "bad");
  const medTxt = stats.medianReturn == null ? "-" : fmtPct(stats.medianReturn);
  const medCls = stats.medianReturn == null ? "" : (stats.medianReturn >= 0 ? "good" : "bad");
  const latestSide = stats.latestRec ? sideOf(stats.latestRec) : "-";
  const latestSideCls = latestSide === "LONG" ? "good" : (latestSide === "SHORT" ? "bad" : "");
  const latestScore = Number.isFinite(Number(stats.latestRec?.score)) ? fmtNum(Number(stats.latestRec.score), 3) : "-";
  const latestFunding = Number.isFinite(Number(stats.latestRec?.g_funding_rate))
    ? fmtFunding(stats.latestRec?.g_funding_rate)
    : "-";
  const latestOi = Number.isFinite(Number(stats.latestRec?.g_open_interest))
    ? fmtOi(stats.latestRec?.g_open_interest)
    : "-";
  const latestMom24h = stats.latestMomentum24h == null ? "-" : fmtSignedPctValue(stats.latestMomentum24h, 2);
  const latestEvalRet = Number.isFinite(Number(stats.latestEval?.return_blended))
    ? fmtPct(Number(stats.latestEval.return_blended))
    : "-";
  const latestEvalCls = Number.isFinite(Number(stats.latestEval?.return_blended))
    ? (Number(stats.latestEval.return_blended) >= 0 ? "good" : "bad")
    : "";
  const modelText = stats.topModels.length ? stats.topModels.join(", ") : "-";

  const lines = [];
  lines.push(`Recommendations ${stats.recCount}, evaluations ${stats.evalCount}, wins ${stats.winCount}`);
  lines.push(`Direction bias: ${stats.sideBias}`);
  lines.push(`Active model mix: ${modelText}`);
  if (stats.latestRec) {
    lines.push(
      `Latest signal: ${fmtTime(stats.latestRec.created_at)} | ${latestSide} | score ${latestScore} | 24h ${latestMom24h}`
    );
    lines.push(`Funding ${latestFunding} | OI ${latestOi} | Risk ${stats.riskTag}`);
  }
  if (stats.latestEval) {
    lines.push(`Latest evaluation: ${fmtTime(stats.latestEval.evaluated_at)} | return ${latestEvalRet}`);
  }

  statusEl.textContent = `${sym}USDT (Bitget Perpetual) chart + internal recommendation analytics`;
  statusEl.className = "status good";

  bodyEl.innerHTML = `
    <div class="analysis-kpis">
      <article class="analysis-kpi"><p class="k">Win Rate</p><p class="v ${wrCls}">${wrTxt}</p></article>
      <article class="analysis-kpi"><p class="k">Avg Return</p><p class="v ${avgCls}">${avgTxt}</p></article>
      <article class="analysis-kpi"><p class="k">Median Return</p><p class="v ${medCls}">${medTxt}</p></article>
      <article class="analysis-kpi"><p class="k">Latest Side</p><p class="v ${latestSideCls}">${latestSide}</p></article>
      <article class="analysis-kpi"><p class="k">Latest Score</p><p class="v">${latestScore}</p></article>
      <article class="analysis-kpi"><p class="k">Latest Eval Return</p><p class="v ${latestEvalCls}">${latestEvalRet}</p></article>
    </div>
    <p class="analysis-note"><strong>${sym}</strong> (${symbolName(sym)}) quick analysis</p>
    <ul class="analysis-list">
      ${lines.map((line) => `<li>${line}</li>`).join("")}
    </ul>
  `;
}

function setupSymbolAnalyzer() {
  const inputEl = $("symbolAnalyzeInput");
  const buttonEl = $("symbolAnalyzeBtn");
  if (!inputEl || !buttonEl) return;

  try {
    const saved = normalizeAnalyzeSymbol(localStorage.getItem("momentum_symbol_analyzer") || "");
    if (saved) selectedAnalyzeSymbol = saved;
  } catch (_) {
    // no-op
  }
  inputEl.value = selectedAnalyzeSymbol;

  const submit = () => {
    const sym = normalizeAnalyzeSymbol(inputEl.value || selectedAnalyzeSymbol);
    renderSymbolAnalyzer(latestLoadedState, sym);
  };

  buttonEl.addEventListener("click", submit);
  inputEl.addEventListener("keydown", (evt) => {
    if (evt.key !== "Enter") return;
    evt.preventDefault();
    submit();
  });

  renderSymbolAnalyzer(latestLoadedState, selectedAnalyzeSymbol);
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
    latestLoadedState = state;
    const results = state.results || [];
    renderKpis(state, results);
    renderMarketScene(state);
    renderSocialBuzz(state);
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
    renderSymbolAnalyzer(state, $("symbolAnalyzeInput")?.value || selectedAnalyzeSymbol);

    $("loadStatus").textContent = `불러오기 성공: ${url}`;
    $("loadStatus").className = "status good";
  } catch (e) {
    latestLoadedState = null;
    renderSymbolAnalyzer(null, $("symbolAnalyzeInput")?.value || selectedAnalyzeSymbol);
    $("loadStatus").textContent = `불러오기 실패: ${String(e)}`;
    $("loadStatus").className = "status bad";
  }
}

$("refreshBtn").addEventListener("click", loadAndRender);
$("applySourceBtn").addEventListener("click", loadAndRender);
setupTabs();
setupSymbolAnalyzer();

loadAndRender();
setInterval(loadAndRender, 60000);

