// KBO 순위 데이터 검증 — "기록 틀림" 한 줄이면 신뢰가 끝나므로, 잘못된 값이
// 앱에 반영되는 경로 자체를 막는다. 스크레이퍼와 서버가 같은 규칙을 공유한다.
//
// 여기 규칙은 취향이 아니라 리그 구조상 반드시 참인 항등식이다:
//  · 모든 경기는 한 팀의 승이 곧 다른 팀의 패 → Σ승 == Σ패
//  · 무승부는 두 팀에 동시에 기록 → Σ무 는 짝수
// 파싱이 열을 밀려 읽거나 행을 흘리면 거의 항상 이 둘이 먼저 깨진다.

export const OFFICIAL_TEAMS = ["lg", "hh", "ss", "kia", "ssg", "lt", "kt", "ds", "nc", "kw"];
export const GAMES_PER_SEASON = 144;

// 팀 간 경기수 편차 상한. 우천 순연이 몰리면 시즌 중 10경기 안팎까지 벌어지므로
// 정상 범위는 넉넉히 두고, 그보다 크면 행 누락으로 본다.
const MAX_GP_SPREAD = 25;

/**
 * @param {{teams?:Record<string,{w:number,l:number,t:number,pct?:number}>}} dataset
 * @param {{partial?:boolean}} [opts] partial=true면 10개 구단 전체가 아니어도 통과(부분 갱신용)
 * @returns {{ok:boolean, errors:string[], warnings:string[], stats:object}}
 */
export function validateStandings(dataset, opts = {}) {
  const errors = [];
  const warnings = [];
  const teams = dataset && typeof dataset === "object" ? dataset.teams : null;

  if (!teams || typeof teams !== "object" || Array.isArray(teams)) {
    return { ok: false, errors: ["teams 객체가 없습니다"], warnings, stats: {} };
  }

  const ids = Object.keys(teams);
  if (ids.length === 0) {
    return { ok: false, errors: ["teams 가 비어 있습니다"], warnings, stats: {} };
  }

  // 1) 구단 id — 공식 10개 구단만. 오타/별칭 유입 차단.
  const unknown = ids.filter((id) => !OFFICIAL_TEAMS.includes(id));
  if (unknown.length) errors.push(`알 수 없는 구단 id: ${unknown.join(", ")}`);

  if (!opts.partial) {
    const missing = OFFICIAL_TEAMS.filter((id) => !ids.includes(id));
    if (missing.length) errors.push(`구단 누락(${missing.length}개): ${missing.join(", ")}`);
  }

  // 2) 승/패/무 값 자체의 정합성
  let sumW = 0, sumL = 0, sumT = 0;
  const gps = [];
  for (const id of ids) {
    const r = teams[id] || {};
    const w = r.w, l = r.l, t = r.t == null ? 0 : r.t;
    const bad = [["w", w], ["l", l], ["t", t]]
      .filter(([, v]) => !Number.isInteger(v) || v < 0)
      .map(([k]) => k);
    if (bad.length) { errors.push(`${id}: ${bad.join("/")} 값이 0 이상 정수가 아님`); continue; }

    const gp = w + l + t;
    if (gp > GAMES_PER_SEASON) errors.push(`${id}: 경기수 ${gp} > ${GAMES_PER_SEASON}`);
    gps.push(gp);
    sumW += w; sumL += l; sumT += t;

    // 3) 승률이 함께 왔다면 재계산과 대조 (무승부는 승률 계산에서 제외)
    if (r.pct != null) {
      const denom = w + l;
      const calc = denom ? w / denom : 0;
      if (Math.abs(calc - r.pct) > 0.0015) {
        errors.push(`${id}: 승률 불일치 (표기 ${r.pct}, 계산 ${calc.toFixed(3)})`);
      }
    }
  }

  // 4) 리그 항등식 — 전체 구단을 받은 경우에만 성립
  if (!opts.partial && !errors.some((e) => e.includes("누락"))) {
    if (sumW !== sumL) {
      errors.push(`리그 승/패 합 불일치: Σ승 ${sumW} ≠ Σ패 ${sumL} (파싱 오류 가능성 높음)`);
    }
    if (sumT % 2 !== 0) {
      errors.push(`리그 무승부 합이 홀수: Σ무 ${sumT} (무승부는 양 팀에 동시 기록)`);
    }
  }

  // 5) 경기수 편차 — 행을 흘렸을 때 잡히는 보조 신호
  if (gps.length > 1) {
    const spread = Math.max(...gps) - Math.min(...gps);
    if (spread > MAX_GP_SPREAD) {
      warnings.push(`구단별 경기수 편차 ${spread} (정상 범위 초과 — 행 누락 의심)`);
    }
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    stats: { teams: ids.length, sumW, sumL, sumT, maxGp: gps.length ? Math.max(...gps) : 0 },
  };
}

/** 로그용 한 줄 요약 */
export function formatReport(v) {
  const head = v.ok ? "✅ 검증 통과" : "❌ 검증 실패";
  const lines = [`${head} (구단 ${v.stats.teams}, Σ승 ${v.stats.sumW}, Σ패 ${v.stats.sumL}, Σ무 ${v.stats.sumT})`];
  for (const e of v.errors) lines.push(`   ✗ ${e}`);
  for (const w of v.warnings) lines.push(`   ⚠ ${w}`);
  return lines.join("\n");
}
