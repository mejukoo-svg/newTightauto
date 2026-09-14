// asc-copy Edge Function 로컬 검증 — Deno·fetch 를 스텁해 핸들러를 직접 호출한다.
// 실행: node --experimental-strip-types _verify.mjs   (실제 메타·Supabase 호출 없음)
//
// 시나리오
//   · 원본 광고 A(집착_일반 세트, 영상 V1) → 집착 ASC 캠페인 2개(세트 S1·S2) + 구미호 ASC(S3) + 집착 일반 캠페인
//     - S1 에는 이미 V1 을 쓰는 광고가 있음 → skip(같은 소재)
//     - S2 는 비어 있음 → copy
//     - S3 는 상품이 달라 대상 아님
//   · 광고 B: 마킹이 DB 에 없음 → 거절
//   · 광고 C: 상품(재물) ASC 없음 → 오류
//   · dryRun=false + select=[A|S2] → /copies 1회, 로그 1건

const ACC = "act_1270614404675034";
const TOK = "TOK-1";
const AD_A = "120250000000000001", AD_B = "120250000000000002", AD_C = "120250000000000003";
const SET_SRC = "120240000000000010";
const CAMP_ASC1 = "120230000000000001", CAMP_ASC2 = "120230000000000002", CAMP_GUM = "120230000000000003", CAMP_NORMAL = "120230000000000004";
const S1 = "120240000000000001", S2 = "120240000000000002", S3 = "120240000000000003", S4 = "120240000000000004";

let handler = null;
let calls = [];
globalThis.__ENV = { SUPABASE_URL: "https://sb.test", SB_SECRET_KEY: "svc", META_TOKEN_1: TOK };
globalThis.Deno = { env: { get: (k) => globalThis.__ENV[k] }, serve: (h) => { handler = h; } };

const HL = { [AD_A]: "asc", [AD_C]: "asc" }; // AD_B 는 마킹 없음
const CR = (id, vid) => ({ id, effective_object_story_id: `story_${id}`, object_story_spec: { video_data: { video_id: vid } } });
const ADS = {
  [AD_A]: { id: AD_A, name: "집착_소재A", status: "ACTIVE", effective_status: "ACTIVE", account_id: ACC.slice(4),
    adset: { id: SET_SRC, name: "집착_0801_일반" }, campaign: { id: CAMP_NORMAL, name: "🔥집착_0801_전환" }, creative: CR("cr_A", "V1") },
  [AD_C]: { id: AD_C, name: "재물_소재C", status: "ACTIVE", effective_status: "ACTIVE", account_id: ACC.slice(4),
    adset: { id: "120240000000000099", name: "재물_일반" }, campaign: { id: "120230000000000099", name: "💵재물_0601_전환" }, creative: CR("cr_C", "V9") },
};
const CAMPS = [
  { id: CAMP_ASC1, name: "집착_0623_ASC(2)_찐위닝", effective_status: "ACTIVE", smart_promotion_type: "AUTOMATED_SHOPPING_ADS" },
  { id: CAMP_ASC2, name: "🔥집착_0901_ASC_부계", effective_status: "PAUSED" },
  { id: CAMP_GUM, name: "구미호_0623_ASC(2)_찐위닝", effective_status: "ACTIVE", smart_promotion_type: "AUTOMATED_SHOPPING_ADS" },
  { id: CAMP_NORMAL, name: "🔥집착_0801_전환", effective_status: "ACTIVE" },
];
const ADSETS = {
  [CAMP_ASC1]: [{ id: S1, name: "집착 ASC 세트1", effective_status: "ACTIVE" }],
  [CAMP_ASC2]: [{ id: S2, name: "집착 ASC 부계 세트", effective_status: "PAUSED" }],
  [CAMP_GUM]: [{ id: S3, name: "구미호 ASC", effective_status: "ACTIVE" }],
  [CAMP_NORMAL]: [{ id: S4, name: "집착 일반", effective_status: "ACTIVE" }],
};
const SET_ADS = {
  [S1]: [{ id: "9001", name: "집착_소재A_복제", effective_status: "ACTIVE", creative: CR("cr_A2", "V1") }, { id: "9002", name: "지운것", effective_status: "DELETED", creative: CR("cr_A3", "V1") }],
  [S2]: [{ id: "9003", name: "다른소재", effective_status: "ACTIVE", creative: CR("cr_X", "V7") }],
  [S3]: [],
};

globalThis.fetch = async (url, opts = {}) => {
  const u = String(url);
  const method = (opts.method || "GET").toUpperCase();
  calls.push({ method, url: u, body: opts.body });
  const ok = (j) => ({ ok: true, status: 200, json: async () => j });
  if (u.startsWith("https://sb.test/auth/v1/user")) return ok({ id: "u1", email: "tester@x" });
  if (u.startsWith("https://sb.test/rest/v1/ad_creative_highlights")) return ok(Object.entries(HL).map(([ad_id, highlight]) => ({ ad_id, highlight })));
  if (u.startsWith("https://sb.test/rest/v1/asc_copy_log")) return method === "POST" ? ok({}) : ok([]);
  const m = u.match(/graph\.facebook\.com\/v[\d.]+\/([^?]+)(?:\?(.*))?$/);
  if (!m) throw new Error("unexpected fetch " + u);
  const path = m[1], q = new URLSearchParams(m[2] || "");
  // POST 는 토큰이 폼 본문에 실린다
  const tokIn = method === "POST" ? new URLSearchParams(opts.body || "").get("access_token") : q.get("access_token");
  if (tokIn !== TOK) return { ok: false, status: 400, json: async () => ({ error: { message: "bad token" } }) };
  if (method === "POST" && path.endsWith("/copies")) {
    const b = new URLSearchParams(opts.body);
    return ok({ copied_ad_id: "NEW_" + b.get("adset_id"), ad_object_ids: [{ ad_object_type: "ad", source_id: path.split("/")[0], copied_id: "NEW_" + b.get("adset_id") }] });
  }
  if (path === `${ACC}/campaigns`) return ok({ data: CAMPS });
  if (path.endsWith("/adsets")) return ok({ data: ADSETS[path.split("/")[0]] || [] });
  if (path.endsWith("/ads")) return ok({ data: SET_ADS[path.split("/")[0]] || [] });
  if (ADS[path]) return ok(ADS[path]);
  return { ok: false, status: 404, json: async () => ({ error: { message: "Unsupported get request: " + path } }) };
};

await import("./index.ts");
if (!handler) throw new Error("handler not registered");

function req(body) {
  return new Request("https://fn.test/asc-copy", { method: "POST", headers: { Authorization: "Bearer jwt" }, body: JSON.stringify(body) });
}
let fails = 0;
function check(cond, msg) { console.log((cond ? "  ✓ " : "  ✗ ") + msg); if (!cond) fails++; }

console.log("1) dry-run");
let r = await (await handler(req({ mode: "cr", dryRun: true, items: [
  { ad_id: AD_A, ad_account_id: ACC }, { ad_id: AD_B, ad_account_id: ACC }, { ad_id: AD_C, ad_account_id: ACC }] }))).json();
check(r.ok === true && r.dryRun === true, "ok/dryRun");
const pA = r.plan.find((p) => p.ad_id === AD_A), pB = r.plan.find((p) => p.ad_id === AD_B), pC = r.plan.find((p) => p.ad_id === AD_C);
check(pA && pA.product === "집착" && !pA.error, "A: 상품=집착 " + (pA?.error || ""));
check(pA.targets.length === 2, "A: 대상 2세트(집착 ASC 만, 구미호·일반 제외) → " + pA.targets.map((t) => t.adset_name).join(", "));
const tS1 = pA.targets.find((t) => t.adset_id === S1), tS2 = pA.targets.find((t) => t.adset_id === S2);
check(tS1 && tS1.action === "skip" && /이미 있음/.test(tS1.note), "A→S1: 같은 영상(V1) 이미 있음 → skip: " + tS1?.note);
check(tS2 && tS2.action === "copy" && /PAUSED/.test(tS2.note), "A→S2: copy (세트 PAUSED 안내): " + tS2?.note);
check(pB && /마킹 불일치/.test(pB.error), "B: 마킹 없음 → 거절: " + pB?.error);
check(pC && /ASC 캠페인이 이 계정에 없음/.test(pC.error) && pC.product === "재물", "C: 재물 ASC 없음 → 오류: " + pC?.error);
check(!calls.some((c) => c.method === "POST" && /copies/.test(c.url)), "dry-run 에서 /copies 호출 없음");

console.log("2) apply (select A|S2)");
calls = [];
r = await (await handler(req({ mode: "cr", dryRun: false, items: [{ ad_id: AD_A, ad_account_id: ACC }], select: [`${AD_A}|${S2}`] }))).json();
const copies = calls.filter((c) => c.method === "POST" && /copies/.test(c.url));
check(copies.length === 1 && copies[0].url.includes(`/${AD_A}/copies`), "/copies 1회 호출 (원본 A)");
const cb = new URLSearchParams(copies[0]?.body || "");
check(cb.get("adset_id") === S2 && cb.get("status_option") === "ACTIVE" && /NO_RENAME/.test(cb.get("rename_options") || ""), "adset_id=S2, ACTIVE, NO_RENAME");
const t2 = r.plan[0].targets.find((t) => t.adset_id === S2);
if (!t2?.applied) console.log("   dbg:", JSON.stringify(t2), "| planErr:", r.plan[0].error);
check(t2.applied === true && t2.copied_ad_id === "NEW_" + S2, "응답에 copied_ad_id: " + t2.copied_ad_id);
const logPost = calls.find((c) => c.method === "POST" && /asc_copy_log/.test(c.url));
const logRows = logPost ? JSON.parse(logPost.body) : [];
check(logRows.length === 1 && logRows[0].ok === true && logRows[0].target_adset_id === S2 && logRows[0].product === "집착" && logRows[0].actor === "tester@x", "asc_copy_log 1건 기록");
check(r.applied === 1 && r.failed === 0, "applied=1 failed=0");

console.log("3) 상품 추출 규칙");
// 핸들러 내부 함수를 직접 못 부르므로 캠페인명→상품 매핑은 위 시나리오(🔥집착_… → 집착, 💵재물_… → 재물)로 확인됨.
console.log(fails ? `\n✗ ${fails} 실패` : "\n✓ 전부 통과");
process.exit(fails ? 1 : 0);
