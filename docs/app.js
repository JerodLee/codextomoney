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
  let sum = 0;
  for (let i = 0; i < xs.length; i += 1) {
    sum += Number(xs[i]) * Number(ys[i]);
  }
  return sum / xs.length;
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

function candidateStateUrls(owner, repo, branch) {
  const ts = Date.now();
  const raw = `https://raw.githubusercontent.com/${owner}/${repo}/${branch}/state/bot_state.json?t=${ts}`;
  // raw.githack can cache aggressively, so prefer raw.githubusercontent first.
  return [raw, "../state/bot_state.json", "/state/bot_state.json", "./state/bot_state.json"];
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
  const picks = [...(state.recommendation_history || [])]
    .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
    .slice(0, 20);
  const resultIds = new Set((state.results || []).map((r) => r.id));

  if (!picks.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="9" class="muted">추천 이력이 아직 없습니다.</td>`;
    body.appendChild(tr);
    return;
  }

  for (const p of picks) {
    const side = sideOf(p);
    const status = resultIds.has(p.id) ? "평가완료" : "대기";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${fmtTime(p.created_at)}</td>
      <td class="mono">${p.symbol}</td>
      <td class="mono">${side}</td>
      <td>${Number.isFinite(Number(p.score)) ? fmtNum(p.score, 3) : "-"}</td>
      <td>${fmtFunding(p.g_funding_rate)}</td>
      <td>${fmtOi(p.g_open_interest)}</td>
      <td>${fmtPctValue(p.b_rate24h, 2)}</td>
      <td>${fmtPctValue(p.g_rate24h, 2)}</td>
      <td class="mono">${status}</td>
    `;
    body.appendChild(tr);
  }
}

function renderEvaluations(results) {
  const body = $("evalBody");
  body.innerHTML = "";
  const rows = [...results]
    .sort((a, b) => new Date(b.evaluated_at) - new Date(a.evaluated_at))
    .slice(0, 20);

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="5" class="muted">검증 데이터가 아직 없습니다.</td>`;
    body.appendChild(tr);
    return;
  }

  for (const r of rows) {
    const side = sideOf(r);
    const ret = Number(r.return_blended || 0);
    const klass = ret >= 0 ? "good" : "bad";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${fmtTime(r.evaluated_at)}</td>
      <td class="mono">${r.symbol}</td>
      <td class="mono">${side}</td>
      <td class="${klass}">${fmtPct(ret)}</td>
      <td class="${r.win ? "good" : "bad"}">${r.win ? "승" : "패"}</td>
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
  const rows = computeCalibrationRows(events, results).reverse().slice(0, 20);
  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="5" class="muted">보정 이벤트가 아직 없습니다.</td>`;
    body.appendChild(tr);
    $("kCalCount").textContent = "0";
    $("kUplift").textContent = "-";
    return;
  }
  $("kCalCount").textContent = String(rows.length);

  const latestDelta = rows.find((r) => r.delta != null)?.delta ?? null;
  $("kUplift").textContent = latestDelta == null ? "데이터없음" : fmtPct(latestDelta);
  $("kUplift").className = `v ${latestDelta == null ? "" : latestDelta >= 0 ? "good" : "bad"}`;

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
    .slice(-120)
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
    }).slice(-120);
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
  }).slice(-120);
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
  if (longMeta) {
    longMeta.textContent = longPoints.length
      ? `LONG 검증 ${longPoints.length}건(누적 승률)`
      : "LONG 검증 데이터가 아직 없습니다.";
  }
  if (shortMeta) {
    shortMeta.textContent = shortPoints.length
      ? `SHORT 검증 ${shortPoints.length}건(누적 승률)`
      : "SHORT 검증 데이터가 아직 없습니다.";
  }
}

function renderKpis(state, results) {
  const runHistory = state.run_history || [];
  const latestRun = runHistory[runHistory.length - 1];
  const rollingMetrics = latestRun?.metrics || null;

  $("kLastRun").textContent = fmtTime(state.meta?.last_run_at);
  $("kPending").textContent = String((state.pending || []).length);
  $("kWinRate").textContent = rollingMetrics ? fmtPct(Number(rollingMetrics.win_rate || 0)) : "-";
  $("kAvgReturn").textContent = rollingMetrics ? fmtPct(Number(rollingMetrics.avg_return || 0)) : "-";
  if (!rollingMetrics && results.length) {
    const recent = results.slice(-120);
    const wr = winRate(recent);
    const ar = avgReturn(recent);
    $("kWinRate").textContent = wr == null ? "-" : fmtPct(wr);
    $("kAvgReturn").textContent = ar == null ? "-" : fmtPct(ar);
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
  for (const run of runHistory) {
    const ts = Date.parse(run?.run_at || "");
    if (!Number.isFinite(ts)) continue;
    for (const key of Object.keys(runSignSeries)) {
      const sign = Number(run?.market_indicators?.[key]?.sign || 0);
      if (!Number.isFinite(sign) || sign === 0) continue;
      runSignSeries[key].push({ ts, sign: sign > 0 ? 1 : -1 });
    }
  }
  for (const key of Object.keys(runSignSeries)) {
    runSignSeries[key].sort((a, b) => a.ts - b.ts);
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
      let m = Number(r[f]);
      if (!Number.isFinite(m) || m === 0) {
        m = lookupSignAt(runSignSeries[k], createdTs);
      }
      if (!Number.isFinite(m) || m === 0) continue;
      m = m > 0 ? 1 : -1;
      row[k].xs.push(s);
      row[k].ys.push(m);
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
    .slice(0, 25);

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

async function loadAndRender() {
  const owner = $("ownerInput").value.trim();
  const repo = $("repoInput").value.trim();
  const branch = $("branchInput").value.trim() || "main";
  const urls = candidateStateUrls(owner, repo, branch);

  $("sourceText").textContent = `소스: owner=${owner} repo=${repo} branch=${branch}`;
  $("loadStatus").textContent = "상태 데이터를 불러오는 중...";
  $("loadStatus").className = "status muted";

  try {
    const { data: state, url } = await fetchJsonFirst(urls);
    const results = state.results || [];
    renderKpis(state, results);
    renderMarketIndicators(state);
    renderSymbolCorrelations(state);
    renderTrend(state);
    renderSideTrends(results);
    renderRules(state.dynamic_config || {});
    renderPicks(state);
    renderEvaluations(results);
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

loadAndRender();
setInterval(loadAndRender, 60000);
