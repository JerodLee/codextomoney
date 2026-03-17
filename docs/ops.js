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
const PROFILE_RULES = {
  1: { name: "P1 보수형", minTargetPct: 0.9, minRrEntry: 1.35 },
  2: { name: "P2 균형형", minTargetPct: 0.7, minRrEntry: 1.25 },
  3: { name: "P3 공격형", minTargetPct: 0.5, minRrEntry: 1.15 },
};

function fmtPct(v) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "-";
  return `${(x * 100).toFixed(2)}%`;
}

function fmtNum(v) {
  const x = Number(v);
  if (!Number.isFinite(x)) return "-";
  return `${x.toLocaleString("ko-KR")}`;
}

function fmtBytes(n) {
  const x = Number(n);
  if (!Number.isFinite(x) || x < 0) return "-";
  if (x >= 1024 * 1024) return `${(x / (1024 * 1024)).toFixed(2)} MB`;
  if (x >= 1024) return `${(x / 1024).toFixed(2)} KB`;
  return `${Math.round(x)} B`;
}

function statusLabel(ratio) {
  if (!Number.isFinite(ratio)) return { txt: "정책형", cls: "" };
  if (ratio >= 0.95) return { txt: "상한 근접", cls: "bad" };
  if (ratio >= 0.8) return { txt: "주의", cls: "bad" };
  return { txt: "정상", cls: "good" };
}

async function fetchFirst(urls, asText = false) {
  let lastErr = "unknown";
  for (const u of urls) {
    try {
      const res = await fetch(u, { cache: "no-store" });
      if (!res.ok) {
        lastErr = `HTTP ${res.status} ${u}`;
        continue;
      }
      const data = asText ? await res.text() : await res.json();
      return { data, url: u };
    } catch (e) {
      lastErr = `${u}: ${String(e)}`;
    }
  }
  throw new Error(`Failed to load: ${lastErr}`);
}

function stateUrls(owner, repo, branch) {
  const ts = Date.now();
  return [
    `https://raw.githubusercontent.com/${owner}/${repo}/${branch}/state/bot_state.json?t=${ts}`,
    "../state/bot_state.json",
    "/state/bot_state.json",
  ];
}

function evalHistoryUrls(owner, repo, branch) {
  const ts = Date.now();
  return [
    `https://raw.githubusercontent.com/${owner}/${repo}/${branch}/state/eval_history.jsonl?t=${ts}`,
    "../state/eval_history.jsonl",
    "/state/eval_history.jsonl",
  ];
}

function renderStorage(state, evalText) {
  const body = $("storageBody");
  body.innerHTML = "";

  const recs = (state.recommendation_history || []).length;
  const results = (state.results || []).length;
  const runs = (state.run_history || []).length;
  const series = (state.market_series || []).length;
  const cals = (state.calibration_events || []).length;
  const pending = (state.pending || []).length;
  const lines = String(evalText || "")
    .split(/\r?\n/)
    .filter((x) => x.trim().length > 0).length;
  const bytes = new Blob([String(evalText || "")]).size;

  const rows = [
    { name: "추천 이력", now: recs, limit: 5000 },
    { name: "검증 결과", now: results, limit: 3000 },
    { name: "실행 이력", now: runs, limit: 5000 },
    { name: "시장 시계열", now: series, limit: 5000 },
    { name: "보정 이벤트", now: cals, limit: 500 },
    { name: "평가 대기", now: pending, limit: null },
    { name: "eval_history 라인", now: lines, limit: null },
    { name: "eval_history 크기", now: fmtBytes(bytes), limit: null, raw: true },
  ];

  for (const r of rows) {
    const ratio = r.limit ? Number(r.now) / Number(r.limit) : NaN;
    const st = statusLabel(ratio);
    const tr = document.createElement("tr");
    const nowTxt = r.raw ? String(r.now) : fmtNum(r.now);
    const limTxt = r.limit ? `${fmtNum(r.limit)} (상한)` : "누적/정책형";
    tr.innerHTML = `
      <td>${r.name}</td>
      <td class="mono">${nowTxt}</td>
      <td class="mono">${limTxt}</td>
      <td class="${st.cls}">${st.txt}</td>
    `;
    body.appendChild(tr);
  }
}

function modelLabel(id) {
  const map = {
    momentum_long_v1: "롱 모멘텀 v1",
    momentum_short_v1: "숏 모멘텀 v1",
    momentum_long_v2: "롱 모멘텀 v2(시장보강)",
    momentum_short_v2: "숏 모멘텀 v2(시장보강)",
    momentum_long_v3: "롱 스윙 v3(수익확장/보유형)",
    momentum_short_v3: "숏 스윙 v3(수익확장/보유형)",
  };
  return map[id] || id || "-";
}

function profileOfState(state) {
  const p = Number(state?.meta?.execution_profile);
  if (Number.isFinite(p) && p >= 1 && p <= 3) return Math.trunc(p);
  return 1;
}

function profileRuleText(profile) {
  const p = Number(profile);
  const r = PROFILE_RULES[p] || PROFILE_RULES[1];
  return `${r.name} | tp>=${r.minTargetPct.toFixed(2)}% | rr>=${r.minRrEntry.toFixed(2)}`;
}

function renderExecutionProfile(state) {
  const p = profileOfState(state);
  const updatedAt = String(state?.meta?.execution_profile_updated_at || "");
  $("profileSelect").value = String(p);
  const suffix = updatedAt ? ` | updated ${updatedAt}` : "";
  $("profileCurrent").textContent = `현재 프로필: P${p} (${profileRuleText(p)})${suffix}`;
}

async function dispatchExecutionProfile(owner, repo, branch, token, profile) {
  const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/momentum-telegram-bot.yml/dispatches`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${token}`,
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      ref: branch,
      inputs: { execution_profile: String(profile) },
    }),
  });
  if (res.status === 204) return;
  let detail = "";
  try {
    detail = await res.text();
  } catch (_) {
    detail = "";
  }
  throw new Error(`dispatch failed: HTTP ${res.status} ${detail}`);
}

function renderModelSnapshot(state) {
  const body = $("modelSnapBody");
  body.innerHTML = "";
  const latestRun = (state.run_history || []).slice(-1)[0] || {};
  const mm = latestRun.model_metrics || {};
  const rows = Object.entries(mm).map(([id, m]) => ({
    id,
    label: m.label || modelLabel(id),
    count: Number(m.count || 0),
    winRate: Number(m.win_rate || 0),
    avgReturn: Number(m.avg_return || 0),
  })).sort((a, b) => b.count - a.count);

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="4" class="muted">모델 스냅샷 데이터가 아직 없습니다.</td>`;
    body.appendChild(tr);
    return;
  }
  for (const r of rows) {
    const wrCls = r.winRate >= 0.5 ? "good" : "bad";
    const avgCls = r.avgReturn >= 0 ? "good" : "bad";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono">${r.label}</td>
      <td>${fmtNum(r.count)}</td>
      <td class="${wrCls}">${fmtPct(r.winRate)}</td>
      <td class="${avgCls}">${fmtPct(r.avgReturn)}</td>
    `;
    body.appendChild(tr);
  }
}

async function loadAndRender() {
  const owner = $("ownerInput").value.trim();
  const repo = $("repoInput").value.trim();
  const branch = $("branchInput").value.trim() || "main";
  const sUrls = stateUrls(owner, repo, branch);
  const eUrls = evalHistoryUrls(owner, repo, branch);

  $("sourceText").textContent = `소스: owner=${owner} repo=${repo} branch=${branch}`;
  $("loadStatus").textContent = "운영 데이터를 불러오는 중...";
  $("loadStatus").className = "status muted";
  try {
    const [{ data: state, url: stateUrl }, { data: evalText, url: evalUrl }] = await Promise.all([
      fetchFirst(sUrls, false),
      fetchFirst(eUrls, true),
    ]);
    renderStorage(state, evalText);
    renderModelSnapshot(state);
    renderExecutionProfile(state);
    $("loadStatus").textContent = `불러오기 성공: state=${stateUrl} | eval=${evalUrl}`;
    $("loadStatus").className = "status good";
  } catch (e) {
    $("loadStatus").textContent = `불러오기 실패: ${String(e)}`;
    $("loadStatus").className = "status bad";
  }
}

async function onApplyProfile() {
  const owner = $("ownerInput").value.trim();
  const repo = $("repoInput").value.trim();
  const branch = $("branchInput").value.trim() || "main";
  const token = String($("ghTokenInput").value || "").trim();
  const profile = Number($("profileSelect").value || 1);
  const st = $("profileApplyStatus");
  if (!token) {
    st.textContent = "PAT를 입력해 주세요. (repo + workflow 권한 필요)";
    st.className = "status bad";
    return;
  }
  if (![1, 2, 3].includes(profile)) {
    st.textContent = "프로필 값이 올바르지 않습니다.";
    st.className = "status bad";
    return;
  }
  st.textContent = `프로필 P${profile} 적용 요청 중...`;
  st.className = "status muted";
  try {
    await dispatchExecutionProfile(owner, repo, branch, token, profile);
    st.textContent = `프로필 P${profile} 적용 요청 완료. 1~2분 내 반영됩니다.`;
    st.className = "status good";
  } catch (e) {
    st.textContent = `프로필 적용 실패: ${String(e)}`;
    st.className = "status bad";
  }
}

$("refreshBtn").addEventListener("click", loadAndRender);
$("applySourceBtn").addEventListener("click", loadAndRender);
$("applyProfileBtn").addEventListener("click", onApplyProfile);

loadAndRender();
setInterval(loadAndRender, 60000);
