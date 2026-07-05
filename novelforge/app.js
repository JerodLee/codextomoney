/* =========================================================================
 * NovelForge — 소설 · 게임북 집필 플랫폼
 * 순수 바닐라 JS. localStorage 저장, Claude API 직접 호출.
 * ========================================================================= */

(function () {
  "use strict";

  const LS_KEY = "novelforge_state_v1";
  const API_URL = "https://api.anthropic.com/v1/messages";
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const uid = () => Math.random().toString(36).slice(2, 10);
  const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));

  /* ---------- 상태 ---------- */
  let store = loadStore();
  let genController = null; // 진행 중인 생성 중단용
  let currentEpisodeId = null;

  function blankProject(name) {
    return {
      id: uid(),
      name: name || "새 작품",
      meta: { title: "", totalEpisodes: 0, charsPerEp: 5500, genre: "", logline: "", synopsis: "" },
      theme: { protagonist: "", world: "", conflicts: [] },
      canvas: { events: [], characters: [] },
      structure: { parts: [] },
      episodes: [],
      showLinks: true,
    };
  }

  function loadStore() {
    try {
      const raw = localStorage.getItem(LS_KEY);
      if (raw) {
        const s = JSON.parse(raw);
        if (s && s.projects && s.activeId) return s;
      }
    } catch (e) { /* ignore */ }
    const p = blankProject("나의 첫 작품");
    return {
      projects: { [p.id]: p },
      activeId: p.id,
      settings: { apiKey: "", model: "claude-opus-4-8" },
    };
  }

  function save() {
    try { localStorage.setItem(LS_KEY, JSON.stringify(store)); }
    catch (e) { toast("저장 실패: 저장 공간이 부족할 수 있습니다."); }
  }

  const project = () => store.projects[store.activeId];

  /* ---------- 유틸: 글자수 ---------- */
  const countChars = (s) => (s || "").length;
  const countNoSpace = (s) => (s || "").replace(/\s/g, "").length;

  /* ---------- 토스트 ---------- */
  let toastTimer = null;
  function toast(msg) {
    const el = $("#toast");
    el.textContent = msg;
    el.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => (el.hidden = true), 2600);
  }

  /* ===================================================================
   * 렌더링 진입점
   * =================================================================== */
  function renderAll() {
    renderProjectSelect();
    renderSetup();
    renderCanvas();
    renderTheme();
    renderStructure();
    renderEpisodeList();
    renderEpisodeEditor();
  }

  /* ---------- 프로젝트 선택 ---------- */
  function renderProjectSelect() {
    const sel = $("#projectSelect");
    sel.innerHTML = "";
    Object.values(store.projects).forEach((p) => {
      const o = document.createElement("option");
      o.value = p.id;
      o.textContent = p.name + (p.meta.title ? ` — ${p.meta.title}` : "");
      if (p.id === store.activeId) o.selected = true;
      sel.appendChild(o);
    });
  }

  /* ===================================================================
   * 1. 작품 설정
   * =================================================================== */
  function renderSetup() {
    const m = project().meta;
    $("#metaTitle").value = m.title || "";
    $("#metaTotalEpisodes").value = m.totalEpisodes || "";
    $("#metaCharsPerEp").value = m.charsPerEp || 5500;
    $("#metaGenre").value = m.genre || "";
    $("#metaLogline").value = m.logline || "";
    $("#metaSynopsis").value = m.synopsis || "";
    renderSetupStats();
  }

  function renderSetupStats() {
    const p = project();
    const total = p.meta.totalEpisodes || 0;
    const written = p.episodes.filter((e) => (e.body || "").trim().length > 0).length;
    const totalChars = p.episodes.reduce((s, e) => s + countChars(e.body), 0);
    $("#setupStats").innerHTML = `
      <div class="stat">총 회차 <b>${total || "—"}</b></div>
      <div class="stat">생성된 회차 <b>${written}</b></div>
      <div class="stat">작성된 회차 카드 <b>${p.episodes.length}</b></div>
      <div class="stat">누적 분량 <b>${totalChars.toLocaleString()}</b> 자</div>`;
  }

  function bindSetup() {
    const map = {
      "#metaTitle": "title", "#metaGenre": "genre",
      "#metaLogline": "logline", "#metaSynopsis": "synopsis",
    };
    Object.entries(map).forEach(([sel, key]) => {
      $(sel).addEventListener("input", (e) => {
        project().meta[key] = e.target.value;
        if (key === "title") renderProjectSelect();
        debouncedSave();
      });
    });
    $("#metaTotalEpisodes").addEventListener("input", (e) => {
      project().meta.totalEpisodes = parseInt(e.target.value) || 0;
      renderSetupStats(); debouncedSave();
    });
    $("#metaCharsPerEp").addEventListener("input", (e) => {
      project().meta.charsPerEp = parseInt(e.target.value) || 5500;
      debouncedSave();
    });
  }

  /* ===================================================================
   * 2. 사건 · 인물 캔버스 (드래그 앤 드롭)
   * =================================================================== */
  function renderCanvas() {
    const canvas = $("#canvas");
    // 카드 제거 (svg 레이어는 유지)
    $$(".card", canvas).forEach((c) => c.remove());
    const p = project();

    p.canvas.events.forEach((ev) => canvas.appendChild(makeEventCard(ev)));
    p.canvas.characters.forEach((ch) => canvas.appendChild(makeCharCard(ch)));
    $("#showLinks").checked = p.showLinks !== false;
    drawLinks();
  }

  function makeCardShell(cls, item) {
    const card = document.createElement("div");
    card.className = "card " + cls;
    card.style.left = (item.x || 40) + "px";
    card.style.top = (item.y || 40) + "px";
    card.dataset.id = item.id;
    return card;
  }

  function makeEventCard(ev) {
    const card = makeCardShell("event", ev);
    card.innerHTML = `
      <div class="card-head" data-drag>
        <span class="card-tag">사건</span>
        <button class="card-del" title="삭제">✕</button>
      </div>
      <div class="card-body">
        <input class="card-title" value="${escapeAttr(ev.title)}" placeholder="사건 이름" />
        <textarea class="card-note" placeholder="사건 설명">${escapeHtml(ev.note || "")}</textarea>
      </div>`;
    $(".card-title", card).addEventListener("input", (e) => { ev.title = e.target.value; debouncedSave(); drawLinks(); });
    $(".card-note", card).addEventListener("input", (e) => { ev.note = e.target.value; debouncedSave(); });
    $(".card-del", card).addEventListener("click", () => {
      project().canvas.events = project().canvas.events.filter((x) => x.id !== ev.id);
      // 연관 인물의 링크에서 제거
      project().canvas.characters.forEach((c) => { c.links = (c.links || []).filter((id) => id !== ev.id); });
      save(); renderCanvas();
    });
    enableDrag(card, ev);
    return card;
  }

  function makeCharCard(ch) {
    const card = makeCardShell("character", ch);
    const events = project().canvas.events;
    const opts = events.map((ev) => `
      <label><input type="checkbox" value="${ev.id}" ${(ch.links || []).includes(ev.id) ? "checked" : ""}/> ${escapeHtml(ev.title || "(제목없음)")}</label>`).join("");
    card.innerHTML = `
      <div class="card-head" data-drag>
        <span class="card-tag">인물</span>
        <button class="card-del" title="삭제">✕</button>
      </div>
      <div class="card-body">
        <input class="card-title" value="${escapeAttr(ch.name)}" placeholder="인물 이름" />
        <input class="card-role" value="${escapeAttr(ch.role || "")}" placeholder="역할 (주인공/적/조력자…)" style="font-size:.78rem;margin-top:.2rem;color:var(--ink-soft)"/>
        <textarea class="card-note" placeholder="인물 설명 · 목적">${escapeHtml(ch.desc || "")}</textarea>
        <details class="link-select">
          <summary>연관 사건 (${(ch.links || []).length})</summary>
          <div class="link-opts">${opts || '<span class="mini">먼저 사건을 추가하세요.</span>'}</div>
        </details>
      </div>`;
    $(".card-title", card).addEventListener("input", (e) => { ch.name = e.target.value; debouncedSave(); drawLinks(); });
    $(".card-role", card).addEventListener("input", (e) => { ch.role = e.target.value; debouncedSave(); });
    $(".card-note", card).addEventListener("input", (e) => { ch.desc = e.target.value; debouncedSave(); });
    $$(".link-opts input", card).forEach((chk) => {
      chk.addEventListener("change", () => {
        const set = new Set(ch.links || []);
        chk.checked ? set.add(chk.value) : set.delete(chk.value);
        ch.links = Array.from(set);
        $("summary", $(".link-select", card)).textContent = `연관 사건 (${ch.links.length})`;
        save(); drawLinks();
      });
    });
    $(".card-del", card).addEventListener("click", () => {
      project().canvas.characters = project().canvas.characters.filter((x) => x.id !== ch.id);
      save(); renderCanvas();
    });
    enableDrag(card, ch);
    return card;
  }

  function enableDrag(card, item) {
    const handle = $("[data-drag]", card);
    let startX, startY, origX, origY, dragging = false;
    handle.addEventListener("pointerdown", (e) => {
      if (e.target.classList.contains("card-del")) return;
      dragging = true;
      handle.setPointerCapture(e.pointerId);
      startX = e.clientX; startY = e.clientY;
      origX = item.x || 40; origY = item.y || 40;
      card.style.zIndex = 20;
    });
    handle.addEventListener("pointermove", (e) => {
      if (!dragging) return;
      item.x = clamp(origX + (e.clientX - startX), 0, 2400 - 210);
      item.y = clamp(origY + (e.clientY - startY), 0, 1600 - 80);
      card.style.left = item.x + "px";
      card.style.top = item.y + "px";
      drawLinks();
    });
    const end = (e) => {
      if (!dragging) return;
      dragging = false;
      card.style.zIndex = "";
      try { handle.releasePointerCapture(e.pointerId); } catch (_) {}
      save();
    };
    handle.addEventListener("pointerup", end);
    handle.addEventListener("pointercancel", end);
  }

  function drawLinks() {
    const svg = $("#linkLayer");
    svg.innerHTML = "";
    const p = project();
    if (p.showLinks === false) return;
    const centers = {};
    $$(".card").forEach((c) => {
      const x = parseFloat(c.style.left) + c.offsetWidth / 2;
      const y = parseFloat(c.style.top) + 24;
      centers[c.dataset.id] = { x, y };
    });
    p.canvas.characters.forEach((ch) => {
      (ch.links || []).forEach((evId) => {
        const a = centers[ch.id], b = centers[evId];
        if (!a || !b) return;
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1", a.x); line.setAttribute("y1", a.y);
        line.setAttribute("x2", b.x); line.setAttribute("y2", b.y);
        line.setAttribute("stroke", "#c9a689");
        line.setAttribute("stroke-width", "2");
        line.setAttribute("stroke-dasharray", "5 4");
        svg.appendChild(line);
      });
    });
  }

  function bindCanvas() {
    $("#addEventBtn").addEventListener("click", () => {
      const n = project().canvas.events.length;
      project().canvas.events.push({ id: uid(), title: "", note: "", x: 60 + (n % 4) * 40, y: 60 + (n % 6) * 40 });
      save(); renderCanvas();
    });
    $("#addCharBtn").addEventListener("click", () => {
      const n = project().canvas.characters.length;
      project().canvas.characters.push({ id: uid(), name: "", role: "", desc: "", links: [], x: 560 + (n % 4) * 40, y: 60 + (n % 6) * 40 });
      save(); renderCanvas();
    });
    $("#showLinks").addEventListener("change", (e) => {
      project().showLinks = e.target.checked; save(); drawLinks();
    });
  }

  /* ===================================================================
   * 3. 주제 · 갈등
   * =================================================================== */
  function renderTheme() {
    const t = project().theme;
    $("#themeProtagonist").value = t.protagonist || "";
    $("#themeWorld").value = t.world || "";
    renderConflicts();
  }

  function renderConflicts() {
    const box = $("#conflictList");
    box.innerHTML = "";
    const conflicts = project().theme.conflicts;
    if (!conflicts.length) box.innerHTML = '<p class="mini">아직 갈등이 없습니다. “＋ 갈등 추가”를 누르세요.</p>';
    conflicts.forEach((c) => {
      const row = document.createElement("div");
      row.className = "conflict-item";
      row.innerHTML = `
        <input class="c-a" placeholder="인물 A" value="${escapeAttr(c.a)}" />
        <input class="c-b" placeholder="인물 B" value="${escapeAttr(c.b)}" />
        <input class="c-desc" placeholder="목적의 충돌 내용" value="${escapeAttr(c.desc)}" />
        <button class="row-del" title="삭제">🗑</button>`;
      $(".c-a", row).addEventListener("input", (e) => { c.a = e.target.value; debouncedSave(); });
      $(".c-b", row).addEventListener("input", (e) => { c.b = e.target.value; debouncedSave(); });
      $(".c-desc", row).addEventListener("input", (e) => { c.desc = e.target.value; debouncedSave(); });
      $(".row-del", row).addEventListener("click", () => {
        project().theme.conflicts = conflicts.filter((x) => x !== c); save(); renderConflicts();
      });
      box.appendChild(row);
    });
  }

  function bindTheme() {
    $("#themeProtagonist").addEventListener("input", (e) => { project().theme.protagonist = e.target.value; debouncedSave(); });
    $("#themeWorld").addEventListener("input", (e) => { project().theme.world = e.target.value; debouncedSave(); });
    $("#addConflictBtn").addEventListener("click", () => {
      project().theme.conflicts.push({ a: "", b: "", desc: "" }); save(); renderConflicts();
    });
  }

  /* ===================================================================
   * 4. 부 · 장 구성
   * =================================================================== */
  function renderStructure() {
    const box = $("#structureList");
    box.innerHTML = "";
    const parts = project().structure.parts;
    if (!parts.length) box.innerHTML = '<p class="mini">아직 부가 없습니다. “＋ 부 추가”를 누르세요.</p>';
    parts.forEach((part, pi) => box.appendChild(makePartBlock(part, pi)));
  }

  function makePartBlock(part, pi) {
    const el = document.createElement("div");
    el.className = "part-block";
    el.innerHTML = `
      <div class="part-head">
        <input class="part-title" value="${escapeAttr(part.title)}" placeholder="제 ${pi + 1} 부 제목" />
        <button class="btn small add-chapter">＋ 장</button>
        <button class="row-del" title="부 삭제">🗑</button>
      </div>
      <div class="part-body">
        <label>이 부의 스토리 진행 요약
          <textarea class="part-summary" rows="2" placeholder="이 부에서 일어나는 큰 흐름">${escapeHtml(part.summary || "")}</textarea>
        </label>
        <div class="chapters"></div>
      </div>`;
    $(".part-title", el).addEventListener("input", (e) => { part.title = e.target.value; debouncedSave(); });
    $(".part-summary", el).addEventListener("input", (e) => { part.summary = e.target.value; debouncedSave(); });
    $(".add-chapter", el).addEventListener("click", () => {
      part.chapters.push({ id: uid(), title: "", summary: "", episodeIds: [] }); save(); renderStructure();
    });
    $(".row-del", el).addEventListener("click", () => {
      project().structure.parts = project().structure.parts.filter((x) => x !== part); save(); renderStructure();
    });
    const chBox = $(".chapters", el);
    part.chapters.forEach((ch, ci) => chBox.appendChild(makeChapterBlock(part, ch, ci)));
    return el;
  }

  function makeChapterBlock(part, ch, ci) {
    const el = document.createElement("div");
    el.className = "chapter-block";
    el.innerHTML = `
      <div class="chapter-head">
        <input class="ch-title" value="${escapeAttr(ch.title)}" placeholder="제 ${ci + 1} 장 제목" />
        <button class="row-del" title="장 삭제">🗑</button>
      </div>
      <textarea class="ch-summary" rows="2" placeholder="이 장의 스토리 진행 요약">${escapeHtml(ch.summary || "")}</textarea>
      <div class="ep-assign"></div>`;
    $(".ch-title", el).addEventListener("input", (e) => { ch.title = e.target.value; debouncedSave(); });
    $(".ch-summary", el).addEventListener("input", (e) => { ch.summary = e.target.value; debouncedSave(); });
    $(".row-del", el).addEventListener("click", () => {
      part.chapters = part.chapters.filter((x) => x !== ch); save(); renderStructure();
    });
    // 회차 배분 칩
    const assign = $(".ep-assign", el);
    project().episodes.forEach((ep) => {
      const chip = document.createElement("span");
      const assigned = (ch.episodeIds || []).includes(ep.id);
      chip.className = "ep-chip" + (assigned ? " assigned" : "");
      chip.textContent = `${ep.number}화`;
      chip.title = ep.title || "";
      chip.addEventListener("click", () => {
        // 한 회차는 한 장에만 속하도록: 다른 장에서 제거
        project().structure.parts.forEach((pp) => pp.chapters.forEach((cc) => {
          cc.episodeIds = (cc.episodeIds || []).filter((id) => id !== ep.id);
        }));
        if (!assigned) ch.episodeIds.push(ep.id);
        save(); renderStructure();
      });
      assign.appendChild(chip);
    });
    if (!project().episodes.length) assign.innerHTML = '<span class="mini">회차 탭에서 회차를 먼저 추가하면 여기에 배분할 수 있습니다.</span>';
    return el;
  }

  function bindStructure() {
    $("#addPartBtn").addEventListener("click", () => {
      project().structure.parts.push({ id: uid(), title: "", summary: "", chapters: [] }); save(); renderStructure();
    });
  }

  /* ===================================================================
   * 5. 회차 집필
   * =================================================================== */
  function nextEpisodeNumber() {
    const nums = project().episodes.map((e) => e.number || 0);
    return (nums.length ? Math.max(...nums) : 0) + 1;
  }

  function renderEpisodeList() {
    const box = $("#episodeList");
    box.innerHTML = "";
    const eps = project().episodes.slice().sort((a, b) => a.number - b.number);
    if (!eps.length) { box.innerHTML = '<p class="mini">회차가 없습니다.</p>'; return; }
    eps.forEach((ep) => {
      const item = document.createElement("div");
      item.className = "ep-item" + (ep.id === currentEpisodeId ? " active" : "");
      const chars = countChars(ep.body);
      const score = ep.rating && ep.rating.overall != null ? `<span class="score-pill">★${ep.rating.overall}</span>` : "";
      item.innerHTML = `
        <div class="ep-item-title"><span>제 ${ep.number} 화</span>${score}</div>
        <div class="ep-item-meta"><span>${escapeHtml((ep.title || "(제목없음)").slice(0, 16))}</span><span>${chars.toLocaleString()}자</span></div>`;
      item.addEventListener("click", () => { currentEpisodeId = ep.id; renderEpisodeList(); renderEpisodeEditor(); });
      box.appendChild(item);
    });
  }

  function currentEpisode() {
    return project().episodes.find((e) => e.id === currentEpisodeId) || null;
  }

  function renderEpisodeEditor() {
    const ep = currentEpisode();
    const empty = $("#episodeEmpty"), body = $("#editorBody");
    if (!ep) { empty.hidden = false; body.hidden = true; return; }
    empty.hidden = true; body.hidden = false;

    $("#epTitle").value = ep.title || "";
    $("#epNumBadge").textContent = `제 ${ep.number} 화`;
    $("#epSummary").value = ep.summary || "";
    $("#epDetail").value = ep.detail || "";
    $("#epMemo").value = ep.memo || "";
    $("#epBody").value = ep.body || "";
    $("#epTarget").value = ep.targetChars || project().meta.charsPerEp || 5500;
    updateBodyCounter();

    // 평가 카드
    if (ep.rating && ep.rating.overall != null) {
      renderRating(ep.rating);
      $("#ratingCard").hidden = false;
    } else {
      $("#ratingCard").hidden = true;
    }
    $("#userFeedback").value = (ep.rating && ep.rating.userNote) || "";
    $("#saveStatus").textContent = "";
  }

  function updateBodyCounter() {
    const ep = currentEpisode();
    if (!ep) return;
    const body = $("#epBody").value;
    const chars = countChars(body);
    const noSpace = countNoSpace(body);
    const target = parseInt($("#epTarget").value) || 5500;
    $("#bodyCounter").textContent = `${chars.toLocaleString()}자 (공백 제외 ${noSpace.toLocaleString()}자)`;
    $("#epCountBadge").textContent = `${target.toLocaleString()}자 / 현재 ${chars.toLocaleString()}자`;
    const bar = $("#bodyProgress");
    const pct = clamp((chars / target) * 100, 0, 100);
    bar.style.width = pct + "%";
    bar.classList.toggle("over", chars >= target);
  }

  function bindEpisodes() {
    $("#addEpisodeBtn").addEventListener("click", () => {
      const ep = {
        id: uid(), number: nextEpisodeNumber(), title: "",
        summary: "", detail: "", memo: "", body: "",
        targetChars: project().meta.charsPerEp || 5500, rating: null,
      };
      project().episodes.push(ep);
      currentEpisodeId = ep.id;
      save(); renderEpisodeList(); renderEpisodeEditor(); renderStructure();
    });

    const bindField = (sel, key, extra) => $(sel).addEventListener("input", (e) => {
      const ep = currentEpisode(); if (!ep) return;
      ep[key] = e.target.value; debouncedSave(); if (extra) extra();
    });
    bindField("#epTitle", "title", () => renderEpisodeList());
    bindField("#epSummary", "summary");
    bindField("#epDetail", "detail");
    bindField("#epMemo", "memo");
    $("#epBody").addEventListener("input", (e) => {
      const ep = currentEpisode(); if (!ep) return;
      ep.body = e.target.value; updateBodyCounter(); debouncedSave(); renderEpisodeList();
    });
    $("#epTarget").addEventListener("input", (e) => {
      const ep = currentEpisode(); if (!ep) return;
      ep.targetChars = parseInt(e.target.value) || 5500; updateBodyCounter(); debouncedSave();
    });
    $("#userFeedback").addEventListener("input", (e) => {
      const ep = currentEpisode(); if (!ep) return;
      ep.rating = ep.rating || {}; ep.rating.userNote = e.target.value; debouncedSave();
    });

    $("#saveEpisodeBtn").addEventListener("click", () => { save(); flashSave("저장되었습니다."); });
    $("#generateBtn").addEventListener("click", onGenerate);
    $("#stopGenBtn").addEventListener("click", () => { if (genController) genController.abort(); });
    $("#finalizeBtn").addEventListener("click", onFinalize);
  }

  function flashSave(msg) {
    const el = $("#saveStatus"); el.textContent = msg;
    setTimeout(() => (el.textContent = ""), 2000);
  }

  /* ---------- 평가 렌더 ---------- */
  const RATING_DIMS = [
    { key: "pace", label: "진행 속도" },
    { key: "tension", label: "박진감" },
    { key: "importance", label: "중요도" },
    { key: "humor", label: "개그 요소" },
    { key: "overall", label: "종합" },
  ];
  function renderRating(r) {
    const grid = $("#ratingGrid");
    grid.innerHTML = RATING_DIMS.map((d) => {
      const v = r[d.key];
      const pct = v != null ? (v / 10) * 100 : 0;
      return `<div class="rating-item">
        <div class="r-label">${d.label}</div>
        <div class="r-score">${v != null ? v : "—"}</div>
        <div class="r-bar"><div style="width:${pct}%"></div></div>
      </div>`;
    }).join("");
    $("#feedbackText").textContent = r.feedback || "";
    const ul = $("#improvementList");
    ul.innerHTML = (r.improvements || []).map((s) => `<li>${escapeHtml(s)}</li>`).join("") || "<li>—</li>";
  }

  /* ===================================================================
   * Claude API
   * =================================================================== */
  function apiHeaders() {
    return {
      "content-type": "application/json",
      "x-api-key": store.settings.apiKey,
      "anthropic-version": "2023-06-01",
      "anthropic-dangerous-direct-browser-access": "true",
    };
  }

  function ensureKey() {
    if (!store.settings.apiKey) {
      toast("먼저 ⚙️ 설정에서 API 키를 입력하세요.");
      openSettings();
      return false;
    }
    return true;
  }

  /** 컨텍스트 문자열 구성 */
  function buildContext(ep) {
    const p = project();
    const m = p.meta;
    const t = p.theme;
    const lines = [];
    if (m.title) lines.push(`제목: ${m.title}`);
    if (m.genre) lines.push(`장르/톤: ${m.genre}`);
    if (m.logline) lines.push(`로그라인: ${m.logline}`);
    if (m.synopsis) lines.push(`전체 줄거리:\n${m.synopsis}`);
    if (t.protagonist) lines.push(`주인공의 근본적 문제: ${t.protagonist}`);
    if (t.world) lines.push(`세계관의 화두: ${t.world}`);
    if (t.conflicts.length) lines.push("인물 간 갈등:\n" + t.conflicts.map((c) => `- ${c.a} vs ${c.b}: ${c.desc}`).join("\n"));

    // 주요 사건 · 인물
    if (p.canvas.events.length) lines.push("주요 사건:\n" + p.canvas.events.map((e) => `- ${e.title}: ${e.note || ""}`).join("\n"));
    if (p.canvas.characters.length) lines.push("주요 인물:\n" + p.canvas.characters.map((c) => `- ${c.name}(${c.role || "역할미정"}): ${c.desc || ""}`).join("\n"));

    // 이 회차가 속한 부/장
    p.structure.parts.forEach((part, pi) => part.chapters.forEach((ch, ci) => {
      if ((ch.episodeIds || []).includes(ep.id)) {
        lines.push(`위치: 제${pi + 1}부 "${part.title}" / 제${ci + 1}장 "${ch.title}"`);
        if (part.summary) lines.push(`부 요약: ${part.summary}`);
        if (ch.summary) lines.push(`장 요약: ${ch.summary}`);
      }
    }));

    // 직전 회차 요약 (연속성)
    const prev = p.episodes.filter((e) => e.number < ep.number).sort((a, b) => b.number - a.number).slice(0, 3).reverse();
    if (prev.length) {
      lines.push("직전 회차들의 요약:\n" + prev.map((e) => `- 제${e.number}화: ${e.summary || "(요약없음)"}`).join("\n"));
    }
    return lines.join("\n\n");
  }

  /* ---------- 생성하기 (스트리밍) ---------- */
  async function onGenerate() {
    if (!ensureKey()) return;
    const ep = currentEpisode(); if (!ep) return;
    if (!ep.summary && !ep.detail) { toast("최소한 회차 요약 또는 상세 정리를 입력하세요."); return; }

    const target = parseInt($("#epTarget").value) || 5500;
    const context = buildContext(ep);

    const system = `당신은 한국어 장편 소설/게임북을 집필하는 숙련된 작가입니다.
주어진 작품 설정과 회차 설계를 바탕으로 완성도 높은 1회분 소설 본문을 씁니다.

규칙:
- 순수한 소설 본문만 출력합니다. 해설, 머리말, "다음은..." 같은 메타 문장, 마크다운 제목을 쓰지 마세요.
- 목표 분량은 약 ${target}자입니다. ±15% 범위를 지키세요.
- 장면 묘사, 대사, 인물의 내면을 균형 있게 배치하고 몰입감 있게 씁니다.
- 앞 회차와의 연속성을 유지하고, 이번 회차 요약/상세정리의 사건을 반드시 다룹니다.
- 회차 마지막은 다음 화가 궁금해지도록 여운이나 훅으로 맺습니다.`;

    const userMsg = `# 작품 컨텍스트
${context}

# 이번 회차 (제 ${ep.number} 화) 설계
## 회차 요약
${ep.summary || "(없음)"}

## 회차 상세 정리
${ep.detail || "(없음)"}

## 보조 메모 (등장인물·복선·아이디어)
${ep.memo || "(없음)"}

위 설계에 따라 제 ${ep.number} 화의 소설 본문을 약 ${target}자로 집필하세요.`;

    const btn = $("#generateBtn"), stop = $("#stopGenBtn"), area = $("#epBody");
    btn.disabled = true; stop.hidden = false;
    const prevBody = area.value;
    area.value = "";
    ep.body = "";
    let acc = "";

    genController = new AbortController();
    try {
      const res = await fetch(API_URL, {
        method: "POST",
        headers: apiHeaders(),
        signal: genController.signal,
        body: JSON.stringify({
          model: store.settings.model || "claude-opus-4-8",
          max_tokens: 24000,
          stream: true,
          system,
          messages: [{ role: "user", content: userMsg }],
        }),
      });
      if (!res.ok) {
        const errText = await res.text();
        throw new Error(`API ${res.status}: ${errText.slice(0, 300)}`);
      }
      await readSSE(res, (delta) => {
        acc += delta;
        area.value = acc;
        area.scrollTop = area.scrollHeight;
        ep.body = acc;
        updateBodyCounter();
      });
      ep.body = acc || prevBody;
      if (!acc) { area.value = prevBody; ep.body = prevBody; }
      save(); updateBodyCounter(); renderEpisodeList();
      flashSave("생성 완료 — 확인 후 저장하세요.");
    } catch (err) {
      if (err.name === "AbortError") {
        // 중지: 지금까지 생성분 유지
        ep.body = acc; save(); flashSave("생성을 중지했습니다.");
      } else {
        console.error(err);
        area.value = prevBody; ep.body = prevBody;
        toast("생성 실패: " + err.message);
      }
    } finally {
      btn.disabled = false; stop.hidden = true; genController = null;
    }
  }

  /** SSE 스트림 파서 */
  async function readSSE(res, onDelta) {
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 1);
        if (!line.startsWith("data:")) continue;
        const data = line.slice(5).trim();
        if (data === "[DONE]") return;
        try {
          const ev = JSON.parse(data);
          if (ev.type === "content_block_delta" && ev.delta && ev.delta.type === "text_delta") {
            onDelta(ev.delta.text);
          } else if (ev.type === "error") {
            throw new Error(ev.error ? ev.error.message : "stream error");
          }
        } catch (e) { if (e.message && e.message !== "Unexpected end of JSON input") { /* ignore parse noise */ } }
      }
    }
  }

  /* ---------- 최종 저장 및 평가 ---------- */
  async function onFinalize() {
    const ep = currentEpisode(); if (!ep) return;
    save();
    if (!ensureKey()) { flashSave("본문은 저장되었습니다 (평가는 API 키 필요)."); return; }
    if (!(ep.body || "").trim()) { toast("평가할 본문이 없습니다."); return; }

    const btn = $("#finalizeBtn");
    btn.disabled = true; btn.textContent = "평가 중…";

    const schema = {
      type: "object",
      properties: {
        pace: { type: "integer" }, tension: { type: "integer" },
        importance: { type: "integer" }, humor: { type: "integer" },
        overall: { type: "integer" },
        feedback: { type: "string" },
        improvements: { type: "array", items: { type: "string" } },
      },
      required: ["pace", "tension", "importance", "humor", "overall", "feedback", "improvements"],
      additionalProperties: false,
    };

    const system = `당신은 냉정하면서도 건설적인 소설 편집자입니다. 주어진 회차 본문을 평가합니다.
각 항목은 0~10 정수로 채점합니다: pace(진행 속도), tension(박진감), importance(전체 서사에서의 중요도), humor(개그·유머 요소), overall(종합 평점).
feedback은 2~4문장의 총평, improvements는 구체적 개선점 3~5개를 한국어로 작성합니다.`;

    const user = `작품: ${project().meta.title || "무제"} / 제 ${ep.number} 화
회차 요약: ${ep.summary || "(없음)"}

[본문]
${(ep.body || "").slice(0, 12000)}

위 회차를 평가해 주세요.`;

    try {
      const res = await fetch(API_URL, {
        method: "POST",
        headers: apiHeaders(),
        body: JSON.stringify({
          model: store.settings.model || "claude-opus-4-8",
          max_tokens: 2000,
          system,
          messages: [{ role: "user", content: user }],
          output_config: { format: { type: "json_schema", schema } },
        }),
      });
      if (!res.ok) throw new Error(`API ${res.status}: ${(await res.text()).slice(0, 300)}`);
      const data = await res.json();
      const textBlock = (data.content || []).find((b) => b.type === "text");
      if (!textBlock) throw new Error("응답에 텍스트가 없습니다.");
      const rating = JSON.parse(textBlock.text);
      rating.userNote = (ep.rating && ep.rating.userNote) || "";
      ep.rating = rating;
      save();
      renderRating(rating);
      $("#ratingCard").hidden = false;
      renderEpisodeList();
      flashSave("최종 저장 및 평가 완료!");
    } catch (err) {
      console.error(err);
      toast("평가 실패: " + err.message);
    } finally {
      btn.disabled = false; btn.textContent = "✅ 최종 저장 및 평가";
    }
  }

  /* ===================================================================
   * 탭 / 프로젝트 / 설정 / 백업
   * =================================================================== */
  function bindTabs() {
    $$(".tab").forEach((tab) => {
      tab.addEventListener("click", () => {
        $$(".tab").forEach((t) => t.classList.remove("active"));
        $$(".view").forEach((v) => v.classList.remove("active"));
        tab.classList.add("active");
        $("#view-" + tab.dataset.view).classList.add("active");
        if (tab.dataset.view === "canvas") requestAnimationFrame(drawLinks);
      });
    });
  }

  function bindProjectControls() {
    $("#projectSelect").addEventListener("change", (e) => {
      store.activeId = e.target.value; currentEpisodeId = null; save(); renderAll();
    });
    $("#newProjectBtn").addEventListener("click", () => {
      const name = prompt("새 작품 이름:", "새 작품");
      if (name == null) return;
      const p = blankProject(name.trim() || "새 작품");
      store.projects[p.id] = p; store.activeId = p.id; currentEpisodeId = null;
      save(); renderAll();
    });
    $("#renameProjectBtn").addEventListener("click", () => {
      const name = prompt("작품 이름 변경:", project().name);
      if (name == null) return;
      project().name = name.trim() || project().name; save(); renderProjectSelect();
    });
    $("#deleteProjectBtn").addEventListener("click", () => {
      if (Object.keys(store.projects).length <= 1) { toast("마지막 작품은 삭제할 수 없습니다."); return; }
      if (!confirm(`"${project().name}" 작품을 삭제할까요? 되돌릴 수 없습니다.`)) return;
      delete store.projects[store.activeId];
      store.activeId = Object.keys(store.projects)[0];
      currentEpisodeId = null; save(); renderAll();
    });
    $("#exportBtn").addEventListener("click", () => {
      const blob = new Blob([JSON.stringify(project(), null, 2)], { type: "application/json" });
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = (project().meta.title || project().name || "novelforge") + ".json";
      a.click(); URL.revokeObjectURL(a.href);
    });
    $("#importBtn").addEventListener("click", () => $("#importFile").click());
    $("#importFile").addEventListener("change", (e) => {
      const file = e.target.files[0]; if (!file) return;
      const reader = new FileReader();
      reader.onload = () => {
        try {
          const obj = JSON.parse(reader.result);
          if (!obj.meta || !obj.episodes) throw new Error("형식이 올바르지 않습니다.");
          obj.id = uid();
          if (!obj.name) obj.name = obj.meta.title || "가져온 작품";
          store.projects[obj.id] = obj; store.activeId = obj.id; currentEpisodeId = null;
          save(); renderAll(); toast("작품을 불러왔습니다.");
        } catch (err) { toast("불러오기 실패: " + err.message); }
      };
      reader.readAsText(file);
      e.target.value = "";
    });
  }

  function openSettings() {
    $("#apiKeyInput").value = store.settings.apiKey || "";
    $("#modelSelect").value = store.settings.model || "claude-opus-4-8";
    $("#settingsModal").hidden = false;
  }
  function bindSettings() {
    $("#settingsBtn").addEventListener("click", openSettings);
    $("#settingsCancel").addEventListener("click", () => ($("#settingsModal").hidden = true));
    $("#settingsSave").addEventListener("click", () => {
      store.settings.apiKey = $("#apiKeyInput").value.trim();
      store.settings.model = $("#modelSelect").value;
      save(); $("#settingsModal").hidden = true; toast("설정을 저장했습니다.");
    });
  }

  /* ---------- 유틸 ---------- */
  let saveTimer = null;
  function debouncedSave() { clearTimeout(saveTimer); saveTimer = setTimeout(save, 400); }
  function escapeHtml(s) { return (s || "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c])); }
  function escapeAttr(s) { return (s || "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }

  /* ---------- 초기화 ---------- */
  function init() {
    bindTabs();
    bindProjectControls();
    bindSettings();
    bindSetup();
    bindCanvas();
    bindTheme();
    bindStructure();
    bindEpisodes();
    renderAll();
  }

  document.addEventListener("DOMContentLoaded", init);
})();
