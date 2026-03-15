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

function fmtPct(v) {
  return `${(v * 100).toFixed(2)}%`;
}

function fmtNum(v, d = 2) {
  return Number(v).toFixed(d);
}

function fmtTime(ts) {
  if (!ts) return "-";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "-";
  return d.toLocaleString("ko-KR", { hour12: false });
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

function buildSvgLine(svgEl, points) {
  const w = 900;
  const h = 240;
  if (!points.length) {
    svgEl.innerHTML = `<text x="16" y="28" fill="#5c6a72" font-size="14">No trend data yet.</text>`;
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
    <path d="${path}" fill="none" stroke="#0e8a7b" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></path>
    <circle cx="${xy[xy.length - 1].x.toFixed(2)}" cy="${xy[xy.length - 1].y.toFixed(2)}" r="4.8" fill="#ff9e57"></circle>
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
    tr.innerHTML = `<td colspan="6" class="muted">No recommendation history yet.</td>`;
    body.appendChild(tr);
    return;
  }

  for (const p of picks) {
    const status = resultIds.has(p.id) ? "evaluated" : "pending";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${fmtTime(p.created_at)}</td>
      <td class="mono">${p.symbol}</td>
      <td>${fmtNum(p.score, 3)}</td>
      <td>${fmtNum(p.b_rate24h, 2)}%</td>
      <td>${fmtNum(p.g_rate24h, 2)}%</td>
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
    tr.innerHTML = `<td colspan="4" class="muted">No evaluations yet.</td>`;
    body.appendChild(tr);
    return;
  }

  for (const r of rows) {
    const ret = Number(r.return_blended || 0);
    const klass = ret >= 0 ? "good" : "bad";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${fmtTime(r.evaluated_at)}</td>
      <td class="mono">${r.symbol}</td>
      <td class="${klass}">${fmtPct(ret)}</td>
      <td class="${r.win ? "good" : "bad"}">${r.win ? "WIN" : "LOSE"}</td>
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
    tr.innerHTML = `<td colspan="5" class="muted">No calibration events yet.</td>`;
    body.appendChild(tr);
    $("kCalCount").textContent = "0";
    $("kUplift").textContent = "-";
    return;
  }
  $("kCalCount").textContent = String(rows.length);

  const latestDelta = rows.find((r) => r.delta != null)?.delta ?? null;
  $("kUplift").textContent = latestDelta == null ? "n/a" : fmtPct(latestDelta);
  $("kUplift").className = `v ${latestDelta == null ? "" : latestDelta >= 0 ? "good" : "bad"}`;

  for (const r of rows) {
    const dCls = r.delta == null ? "" : r.delta >= 0 ? "good" : "bad";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${fmtTime(r.at)}</td>
      <td>${r.before == null ? "n/a" : fmtPct(r.before)}</td>
      <td>${r.after == null ? "n/a" : fmtPct(r.after)}</td>
      <td class="${dCls}">${r.delta == null ? "n/a" : fmtPct(r.delta)}</td>
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

  let source = "run_history";
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
    source = "results(cumulative)";
  }

  buildSvgLine($("trendChart"), points);
  $("trendMeta").textContent = points.length
    ? `showing ${points.length} points from ${source}`
    : "no trend data yet";
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

async function loadAndRender() {
  const owner = $("ownerInput").value.trim();
  const repo = $("repoInput").value.trim();
  const branch = $("branchInput").value.trim() || "main";
  const urls = candidateStateUrls(owner, repo, branch);

  $("sourceText").textContent = `owner=${owner} repo=${repo} branch=${branch}`;
  $("loadStatus").textContent = "Loading state...";
  $("loadStatus").className = "status muted";

  try {
    const { data: state, url } = await fetchJsonFirst(urls);
    const results = state.results || [];
    renderKpis(state, results);
    renderTrend(state);
    renderRules(state.dynamic_config || {});
    renderPicks(state);
    renderEvaluations(results);
    renderCalibrations(state.calibration_events || [], results);

    $("loadStatus").textContent = `Loaded from ${url}`;
    $("loadStatus").className = "status good";
  } catch (e) {
    $("loadStatus").textContent = `Load failed: ${String(e)}`;
    $("loadStatus").className = "status bad";
  }
}

$("refreshBtn").addEventListener("click", loadAndRender);
$("applySourceBtn").addEventListener("click", loadAndRender);

loadAndRender();
setInterval(loadAndRender, 60000);
