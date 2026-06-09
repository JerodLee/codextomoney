// 서울 재개발 모니터링 대시보드 프론트엔드
const API = (location.origin && location.origin.startsWith("http")) ? location.origin : "http://localhost:8000";
const $ = (s) => document.querySelector(s);

const TYPE_COLORS = { "재개발": "#f97316", "신통기획": "#3b82f6", "모아타운": "#a855f7" };

let map, markersLayer, allAreas = [], detailChart;

async function api(path) {
  const res = await fetch(API + path);
  if (!res.ok) throw new Error(path + " -> " + res.status);
  return res.json();
}

function initMap() {
  map = L.map("map", { zoomControl: true }).setView([37.5547, 126.9906], 11);
  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    attribution: "&copy; OpenStreetMap, &copy; CARTO", maxZoom: 19,
  }).addTo(map);
  markersLayer = L.layerGroup().addTo(map);
}

function renderMarkers(areas) {
  markersLayer.clearLayers();
  areas.forEach((a) => {
    if (a.lat == null || a.lng == null) return;
    const color = TYPE_COLORS[a.type] || "#999";
    const m = L.circleMarker([a.lat, a.lng], {
      radius: 8, color, weight: 2, fillColor: color, fillOpacity: 0.55,
    });
    m.bindTooltip(`<b>${a.name}</b><br>${a.type} · ${a.stage}`, { direction: "top" });
    m.on("click", () => openDetail(a.id));
    markersLayer.addLayer(m);
  });
}

async function loadMeta() {
  const meta = await api("/api/meta");
  const tag = $("#liveTag");
  if (meta.molit_live) { tag.textContent = "실거래가 LIVE"; tag.className = "tag live"; }
  else { tag.textContent = "시드 추정 시세"; tag.className = "tag seed"; }

  const stageSel = $("#stageFilter");
  Object.keys(meta.stages || {}).forEach((s) => {
    const o = document.createElement("option"); o.value = s; o.textContent = s; stageSel.appendChild(o);
  });
  renderStats(meta);

  const dist = await api("/api/districts");
  const dSel = $("#districtFilter");
  dist.forEach((d) => {
    if (!d.area_count) return;
    const o = document.createElement("option");
    o.value = d.name; o.textContent = `${d.name} (${d.area_count})`; dSel.appendChild(o);
  });
}

function renderStats(meta) {
  const t = meta.types || {};
  const total = Object.values(t).reduce((a, b) => a + b, 0);
  const cards = [
    { v: total, l: "전체 구역", c: "var(--accent)" },
    { v: t["재개발"] || 0, l: "재개발", c: TYPE_COLORS["재개발"] },
    { v: t["신통기획"] || 0, l: "신통기획", c: TYPE_COLORS["신통기획"] },
    { v: t["모아타운"] || 0, l: "모아타운", c: TYPE_COLORS["모아타운"] },
  ];
  $("#stats").innerHTML = cards.map((c) =>
    `<div class="stat"><div class="v">${c.v}</div><div class="l">${c.l}</div>
     <div class="bar" style="background:${c.c}"></div></div>`).join("");
}

async function loadAreas() {
  const params = new URLSearchParams();
  const d = $("#districtFilter").value, ty = $("#typeFilter").value,
        st = $("#stageFilter").value, q = $("#searchInput").value.trim();
  if (d) params.set("district", d);
  if (ty) params.set("type", ty);
  if (st) params.set("stage", st);
  if (q) params.set("q", q);
  allAreas = await api("/api/areas?" + params.toString());
  renderTable(allAreas);
  renderMarkers(allAreas);
  $("#listCount").textContent = `(${allAreas.length})`;
}

function renderTable(areas) {
  const rows = areas.map((a) => `
    <tr data-id="${a.id}">
      <td>${a.name}</td>
      <td><span class="pill ${a.type}">${a.type}</span></td>
      <td>${a.district}</td>
      <td>${a.stage}</td>
      <td class="num" data-price="${a.id}">…</td>
    </tr>`).join("");
  $("#areaTable tbody").innerHTML = rows || `<tr><td colspan="5" class="muted">결과 없음</td></tr>`;
  document.querySelectorAll("#areaTable tbody tr[data-id]").forEach((tr) =>
    tr.addEventListener("click", () => openDetail(tr.dataset.id)));
  areas.forEach((a) => loadRowPrice(a.id));
}

async function loadRowPrice(id) {
  try {
    const d = await api(`/api/areas/${encodeURIComponent(id)}?months=6`);
    const cell = document.querySelector(`td[data-price="${CSS.escape(id)}"]`);
    if (!cell) return;
    const p = d.price || {};
    if (!p.avg_pyeong) { cell.textContent = "-"; return; }
    const chg = p.change_pct;
    const chgHtml = chg == null ? "" :
      `<span class="${chg >= 0 ? "up" : "down"}"> ${chg >= 0 ? "▲" : "▼"}${Math.abs(chg)}%</span>`;
    cell.innerHTML = `${p.avg_pyeong.toLocaleString()}${chgHtml}`;
  } catch (e) { /* ignore */ }
}

async function openDetail(id) {
  const d = await api(`/api/areas/${encodeURIComponent(id)}?months=12`);
  const p = d.price || {};
  const srcLabel = p.data_source === "molit_live"
    ? '<span class="tag live">실거래 기반</span>'
    : '<span class="tag seed">추정 시세</span>';
  const recent = (p.recent || []).map((t) => `
    <tr><td>${t.deal_date}</td><td>${t.apt}</td>
    <td class="num">${t.area_m2}㎡</td><td class="num">${(t.amount_manwon/10000).toFixed(1)}억</td>
    <td class="num">${t.floor}층</td></tr>`).join("");

  $("#detailBody").innerHTML = `
    <h2>${d.name} ${srcLabel}</h2>
    <p class="muted">${d.type} · ${d.district} ${d.dong} · ${d.tag || ""}</p>
    <div class="kv">
      <div class="b"><div class="l">현재 단계</div><div class="v">${d.stage}</div></div>
      <div class="b"><div class="l">평균 평단가</div><div class="v">${p.avg_pyeong ? p.avg_pyeong.toLocaleString()+"만" : "-"}</div></div>
      <div class="b"><div class="l">12개월 변동</div><div class="v ${p.change_pct>=0?"up":"down"}">${p.change_pct==null?"-":(p.change_pct>=0?"+":"")+p.change_pct+"%"}</div></div>
      <div class="b"><div class="l">거래 건수</div><div class="v">${p.count||0}</div></div>
    </div>
    ${d.note ? `<p class="muted">${d.note}</p>` : ""}
    <div class="chart-box"><canvas id="trendChart" height="120"></canvas></div>
    <h3 style="margin:8px 0">최근 실거래</h3>
    <table><thead><tr><th>일자</th><th>단지</th><th class="num">면적</th><th class="num">금액</th><th class="num">층</th></tr></thead>
    <tbody>${recent || '<tr><td colspan="5" class="muted">데이터 없음</td></tr>'}</tbody></table>`;

  $("#detailOverlay").classList.remove("hidden");
  drawTrend(p.monthly || []);
}

function drawTrend(monthly) {
  const ctx = document.getElementById("trendChart");
  if (!ctx) return;
  if (detailChart) detailChart.destroy();
  detailChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: monthly.map((m) => m.month),
      datasets: [{
        label: "평균 평단가(만원)", data: monthly.map((m) => m.avg_pyeong),
        borderColor: "#2dd4bf", backgroundColor: "rgba(45,212,191,.15)",
        fill: true, tension: 0.3, pointRadius: 3,
      }],
    },
    options: {
      plugins: { legend: { labels: { color: "#8b98a5" } } },
      scales: {
        x: { ticks: { color: "#8b98a5" }, grid: { color: "#2c3744" } },
        y: { ticks: { color: "#8b98a5" }, grid: { color: "#2c3744" } },
      },
    },
  });
}

async function loadChanges() {
  try {
    const changes = await api("/api/monitor/changes?limit=30");
    if (!changes.length) {
      $("#changesList").innerHTML = '<div class="muted">감지된 변동이 없습니다. 스냅샷이 누적되면 단계 변경·시세 급변이 표시됩니다.</div>';
      return;
    }
    $("#changesList").innerHTML = changes.map((c) => {
      const when = (c.ts || "").slice(0, 10);
      let msg;
      if (c.kind === "stage") msg = `🏗️ <b>${c.name}</b> 단계 변경: ${c.from} → ${c.to}`;
      else msg = `${c.pct >= 0 ? "📈" : "📉"} <b>${c.name}</b> 평단가 ${c.from}→${c.to}만원 <span class="${c.pct>=0?"up":"down"}">(${c.pct>=0?"+":""}${c.pct}%)</span>`;
      return `<div class="change"><span>${msg}</span><span class="t">${when}</span></div>`;
    }).join("");
  } catch (e) {
    $("#changesList").innerHTML = '<div class="muted">변동 데이터를 불러오지 못했습니다.</div>';
  }
}

function debounce(fn, ms) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; }

async function init() {
  initMap();
  await loadMeta();
  await loadAreas();
  await loadChanges();

  $("#refreshBtn").addEventListener("click", () => { loadAreas(); loadChanges(); });
  $("#districtFilter").addEventListener("change", loadAreas);
  $("#typeFilter").addEventListener("change", loadAreas);
  $("#stageFilter").addEventListener("change", loadAreas);
  $("#searchInput").addEventListener("input", debounce(loadAreas, 300));
  $("#closeDetail").addEventListener("click", () => $("#detailOverlay").classList.add("hidden"));
  $("#detailOverlay").addEventListener("click", (e) => { if (e.target.id === "detailOverlay") $("#detailOverlay").classList.add("hidden"); });
}

init();
