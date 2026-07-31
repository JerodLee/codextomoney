#!/usr/bin/env node
// 오프라인 테스트: 순위 파서 + 검증 규칙. 외부망 없이 돌아간다.
//   node scripts/test-data.mjs
import assert from "assert";
import { validateStandings, OFFICIAL_TEAMS } from "./lib/validate-standings.mjs";
import { parseStandings } from "./scrape-kbo-official.mjs";

let pass = 0, fail = 0;
function test(name, fn) {
  try { fn(); console.log(`  ✓ ${name}`); pass++; }
  catch (e) { console.error(`  ✗ ${name}\n      ${e.message}`); fail++; }
}

// ---- 정상 데이터 ----
// 10개 구단이 각 142경기(승+패=140, 무=2)를 치른 상태.
// 리그 항등식을 만족하려면 Σ승 == Σ패 == 700 이어야 한다.
function goodTeams() {
  return {
    lg:  { w: 80, l: 60, t: 2 }, hh:  { w: 78, l: 62, t: 2 },
    ss:  { w: 75, l: 65, t: 2 }, kia: { w: 74, l: 66, t: 2 },
    ssg: { w: 72, l: 68, t: 2 }, lt:  { w: 70, l: 70, t: 2 },
    kt:  { w: 68, l: 72, t: 2 }, ds:  { w: 66, l: 74, t: 2 },
    nc:  { w: 62, l: 78, t: 2 }, kw:  { w: 55, l: 85, t: 2 },
  };
}

console.log("\n[검증 규칙]");

test("정상 데이터는 통과한다", () => {
  const v = validateStandings({ teams: goodTeams() });
  assert.ok(v.ok, "errors: " + v.errors.join(" / "));
  assert.strictEqual(v.stats.sumW, v.stats.sumL);
});

test("리그 Σ승 ≠ Σ패 를 잡는다 (열 밀림의 주 증상)", () => {
  // 경기수는 그대로 두고 승/패 균형만 깨뜨려, 다른 규칙에 먼저 걸리지 않게 한다
  const t = goodTeams(); t.lg.w = 85; t.lg.l = 55;
  const v = validateStandings({ teams: t });
  assert.ok(!v.ok);
  assert.ok(v.errors.some((e) => e.includes("승/패 합 불일치")), v.errors.join("/"));
});

test("Σ무 가 홀수면 잡는다", () => {
  const t = goodTeams(); t.lg.t = 3; t.lg.l = 59; // 합은 맞추고 무만 홀수로
  const v = validateStandings({ teams: t });
  assert.ok(!v.ok);
  assert.ok(v.errors.some((e) => e.includes("홀수")), v.errors.join("/"));
});

test("구단 누락을 잡는다", () => {
  const t = goodTeams(); delete t.kw;
  const v = validateStandings({ teams: t });
  assert.ok(!v.ok);
  assert.ok(v.errors.some((e) => e.includes("누락")));
});

test("알 수 없는 구단 id 를 잡는다", () => {
  const t = goodTeams(); t.abc = { w: 1, l: 1, t: 0 };
  const v = validateStandings({ teams: t });
  assert.ok(!v.ok);
  assert.ok(v.errors.some((e) => e.includes("알 수 없는")));
});

test("144경기 초과를 잡는다", () => {
  const t = goodTeams(); t.lg = { w: 100, l: 60, t: 0 };
  const v = validateStandings({ teams: t });
  assert.ok(!v.ok);
  assert.ok(v.errors.some((e) => e.includes("경기수")));
});

test("음수·소수 값을 잡는다", () => {
  assert.ok(!validateStandings({ teams: { ...goodTeams(), lg: { w: -1, l: 60, t: 2 } } }).ok);
  assert.ok(!validateStandings({ teams: { ...goodTeams(), lg: { w: 80.5, l: 60, t: 2 } } }).ok);
});

test("표기 승률과 계산 승률의 불일치를 잡는다", () => {
  const t = goodTeams(); t.lg.pct = 0.700; // 실제 80/(80+60)=0.571
  const v = validateStandings({ teams: t });
  assert.ok(!v.ok);
  assert.ok(v.errors.some((e) => e.includes("승률 불일치")));
});

test("올바른 승률은 통과한다", () => {
  const t = goodTeams(); t.lg.pct = 80 / 140;
  assert.ok(validateStandings({ teams: t }).ok);
});

test("teams 없는 요청은 실패한다", () => {
  assert.ok(!validateStandings({}).ok);
  assert.ok(!validateStandings(null).ok);
  assert.ok(!validateStandings({ teams: [] }).ok);
});

// ---- 파서: KBO 공식 표 형태를 본뜬 픽스처 ----
console.log("\n[공식 순위표 파서]");

const FIXTURE = `<html><body>
<table class="tData tt">
<thead><tr><th>순위</th><th>팀명</th><th>경기수</th><th>승</th><th>패</th><th>무</th><th>승률</th><th>게임차</th><th>최근10경기</th></tr></thead>
<tbody>
  <tr><td>1</td><td>LG</td><td>142</td><td>80</td><td>60</td><td>2</td><td>0.571</td><td>0.0</td><td>6승0무4패</td></tr>
  <tr><td>2</td><td>한화</td><td>142</td><td>78</td><td>62</td><td>2</td><td>0.557</td><td>2.0</td><td>5승1무4패</td></tr>
  <tr><td>3</td><td>삼성</td><td>142</td><td>75</td><td>65</td><td>2</td><td>0.536</td><td>5.0</td><td>4승0무6패</td></tr>
  <tr><td>4</td><td>KIA</td><td>142</td><td>74</td><td>66</td><td>2</td><td>0.529</td><td>6.0</td><td>5승0무5패</td></tr>
  <tr><td>5</td><td>SSG</td><td>142</td><td>72</td><td>68</td><td>2</td><td>0.514</td><td>8.0</td><td>5승0무5패</td></tr>
  <tr><td>6</td><td>롯데</td><td>142</td><td>70</td><td>70</td><td>2</td><td>0.500</td><td>10.0</td><td>4승0무6패</td></tr>
  <tr><td>7</td><td>KT</td><td>142</td><td>68</td><td>72</td><td>2</td><td>0.486</td><td>12.0</td><td>6승0무4패</td></tr>
  <tr><td>8</td><td>두산</td><td>142</td><td>66</td><td>74</td><td>2</td><td>0.471</td><td>14.0</td><td>3승0무7패</td></tr>
  <tr><td>9</td><td>NC</td><td>142</td><td>62</td><td>78</td><td>2</td><td>0.443</td><td>18.0</td><td>4승0무6패</td></tr>
  <tr><td>10</td><td>키움</td><td>142</td><td>55</td><td>85</td><td>2</td><td>0.393</td><td>25.0</td><td>2승0무8패</td></tr>
</tbody></table></body></html>`;

test("공식 표에서 10개 구단을 모두 파싱한다", () => {
  const teams = parseStandings(FIXTURE);
  assert.strictEqual(Object.keys(teams).length, 10, JSON.stringify(teams));
  for (const id of OFFICIAL_TEAMS) assert.ok(teams[id], `${id} 누락`);
  assert.deepStrictEqual({ w: teams.lg.w, l: teams.lg.l, t: teams.lg.t }, { w: 80, l: 60, t: 2 });
  assert.deepStrictEqual({ w: teams.kw.w, l: teams.kw.l, t: teams.kw.t }, { w: 55, l: 85, t: 2 });
});

test("파싱 결과가 검증을 통과한다 (승률 대조 포함)", () => {
  const v = validateStandings({ teams: parseStandings(FIXTURE) });
  assert.ok(v.ok, v.errors.join(" / "));
});

test("'승'이 '승률' 열을 잘못 집지 않는다", () => {
  const teams = parseStandings(FIXTURE);
  assert.strictEqual(teams.lg.w, 80); // 0.571 을 집었다면 실패
});

test("열 순서가 바뀌어도 헤더를 보고 따라간다", () => {
  const reordered = FIXTURE
    .replace("<th>승</th><th>패</th><th>무</th>", "<th>무</th><th>패</th><th>승</th>")
    .replace(/<td>(\d+)<\/td><td>(\d+)<\/td><td>(2)<\/td>/g, "<td>$3</td><td>$2</td><td>$1</td>");
  const teams = parseStandings(reordered);
  assert.strictEqual(teams.lg.w, 80);
  assert.strictEqual(teams.lg.t, 2);
});

test("표가 없으면 빈 객체를 돌려준다 (조용히 잘못된 값을 만들지 않는다)", () => {
  assert.deepStrictEqual(parseStandings("<html><body><p>점검 중</p></body></html>"), {});
});

console.log(`\n${fail === 0 ? "✅" : "❌"} ${pass} passed, ${fail} failed\n`);
process.exit(fail === 0 ? 0 : 1);
