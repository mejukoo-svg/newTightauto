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
  if (method === "POST" && path === `${ACC}/ads`) {
    const b = new URLSearchParams(opts.body);
    return ok({ id: "NEW_" + b.get("adset_id") });
  }
  if (method === "POST" && path.endsWith("/copies")) throw new Error("/copies 는 더 이상 쓰지 않는다 (standard enhancements 거부)");
  if (path === `${ACC}/campaigns`) return ok({ data: CAMPS });
  // 일괄 엣지: act/adsets (campaign.id IN) · act/ads (adset.id IN) — filtering 값을 존중한다
  const flt = (() => { try { return JSON.parse(q.get("filtering") || "[]")[0] || {}; } catch { return {}; } })();
  if (path === `${ACC}/adsets`) {
    const ids = new Set((flt.value || []).map(String));
    const data = [];
    for (const [cid, sets] of Object.entries(ADSETS)) if (ids.has(cid)) for (const s of sets) data.push({ ...s, campaign_id: cid });
    return ok({ data });
  }
  if (path === `${ACC}/ads`) {
    const ids = new Set((flt.value || []).map(String));
    const data = [];
    for (const [sid, ads] of Object.entries(SET_ADS)) if (ids.has(sid)) for (const a of ads) data.push({ ...a, adset_id: sid });
    return ok({ data });
  }
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
check(tS2 && tS2.action === "copy" && /PAUSED/.test(tS2.note) && /켜지 않음/.test(tS2.note), "A→S2: copy (중단 ASC 에도 넣되 켜지 않음): " + tS2?.note);
check(pB && /마킹 불일치/.test(pB.error), "B: 마킹 없음 → 거절: " + pB?.error);
check(pC && /ASC 캠페인이 이 계정에 없음/.test(pC.error) && pC.product === "재물", "C: 재물 ASC 없음 → 오류: " + pC?.error);
check(!calls.some((c) => c.method === "POST" && /graph\.facebook/.test(c.url)), "dry-run 에서 메타 쓰기 호출 없음");

{
  const gets = calls.filter((c) => c.method === "GET" && /graph\.facebook/.test(c.url) && !/\/1202500000000000/.test(c.url));
  check(gets.length === 3, "계정당 목록 호출 3회(campaigns/adsets 일괄 + 대상 세트 ads 일괄) → " + gets.length);
  const adsCall = gets.find((c) => /\/ads\?/.test(c.url));
  const fv = JSON.parse(new URLSearchParams(adsCall.url.split("?")[1]).get("filtering"))[0].value.sort();
  check(fv.join(",") === [S1, S2].sort().join(",") && /limit=100/.test(adsCall.url), "ads 일괄 조회는 같은 상품 세트(S1,S2)만 · limit 100 → " + fv.join(","));
}
console.log("1b) 요청 한도 재시도");
{
  const orig = globalThis.fetch; let n = 0;
  globalThis.fetch = async (url, opts) => {
    if (/\/ads$/.test(String(url)) && (opts?.method || "GET") === "POST" && n++ === 0) return { ok: false, status: 400, json: async () => ({ error: { code: 17, message: "(#17) 이 광고 계정에서 너무 많은 요청이 있습니다." } }) };
    return orig(url, opts);
  };
  const t0 = Date.now();
  const rr = await (await handler(req({ mode: "cr", dryRun: false, items: [{ ad_id: AD_A, ad_account_id: ACC }], select: [`${AD_A}|${S2}`] }))).json();
  globalThis.fetch = orig;
  const tt = rr.plan[0].targets.find((t) => t.adset_id === S2);
  check(n === 2 && tt.applied === true && Date.now() - t0 >= 2900, "code 17 → 3s 후 재시도 성공 (" + (Date.now() - t0) + "ms)");
}
console.log("2) apply (select A|S2)");
calls = [];
r = await (await handler(req({ mode: "cr", dryRun: false, items: [{ ad_id: AD_A, ad_account_id: ACC }], select: [`${AD_A}|${S2}`] }))).json();
const copies = calls.filter((c) => c.method === "POST" && new RegExp(`/${ACC}/ads$`).test(c.url));
check(copies.length === 1, "act/ads POST 1회 (creative_id 참조 생성)");
const cb = new URLSearchParams(copies[0]?.body || "");
check(cb.get("adset_id") === S2 && cb.get("status") === "ACTIVE" && JSON.parse(cb.get("creative") || "{}").creative_id === "cr_A" && cb.get("name") === "집착_소재A" && !cb.has("tracking_specs"), "adset_id=S2, ACTIVE, creative_id=cr_A, 이름 유지, tracking_specs 미전달");
const t2 = r.plan[0].targets.find((t) => t.adset_id === S2);
if (!t2?.applied) console.log("   dbg:", JSON.stringify(t2), "| planErr:", r.plan[0].error);
check(t2.applied === true && t2.copied_ad_id === "NEW_" + S2, "응답에 copied_ad_id: " + t2.copied_ad_id);
const logPost = calls.find((c) => c.method === "POST" && /asc_copy_log/.test(c.url));
const logRows = logPost ? JSON.parse(logPost.body) : [];
check(logRows.length === 1 && logRows[0].ok === true && logRows[0].target_adset_id === S2 && logRows[0].product === "집착" && logRows[0].actor === "tester@x", "asc_copy_log 1건 기록");
check(r.applied === 1 && r.failed === 0, "applied=1 failed=0");

console.log("3) 글로벌 — 국가+상품 매칭");
const GACC = "act_2677707262628563", GTOK = "TOK-GL";
globalThis.__ENV.META_TOKEN_GlobalTT = GTOK;
const AD_G = "120260000000000001";
const GC = { twMudang1: "120270000000000001", twMudangWW: "120270000000000002", usMudang: "120270000000000003", twMunyeo: "120270000000000004", twMudangNormal: "120270000000000005" };
const GS = { twMudang1: "120280000000000001", twMudangWW: "120280000000000002", usMudang: "120280000000000003", twMunyeo: "120280000000000004", twMudangNormal: "120280000000000005" };
HL[AD_G] = "asc";
ADS[AD_G] = { id: AD_G, name: "대만_무당_소재G", status: "ACTIVE", effective_status: "ACTIVE", account_id: GACC.slice(4),
  adset: { id: GS.twMudangNormal, name: "대만_shaman_tw_광범위" }, campaign: { id: GC.twMudangNormal, name: "대만_shaman_tw_전환캠페인" }, creative: CR("cr_G", "VG") };
const GCAMPS = [
  { id: GC.twMudang1, name: "대만_무당_ASC", effective_status: "ACTIVE" },
  { id: GC.twMudangWW, name: "대만_무당_ASC_전세계중국어_tROAS", effective_status: "ACTIVE" },
  { id: GC.usMudang, name: "미국_무당_ASC_미국", effective_status: "ACTIVE" },
  { id: GC.twMunyeo, name: "대만_무녀_tw_ASC_tCPA", effective_status: "ACTIVE" },
  { id: GC.twMudangNormal, name: "대만_shaman_tw_전환캠페인", effective_status: "ACTIVE" },
];
for (const k of Object.keys(GC)) ADSETS[GC[k]] = [{ id: GS[k], name: k, effective_status: "ACTIVE" }];
const prevFetch = globalThis.fetch;
globalThis.fetch = async (url, opts = {}) => {
  const u = String(url), method = (opts.method || "GET").toUpperCase();
  const tokIn = method === "POST" ? new URLSearchParams(opts.body || "").get("access_token") : new URLSearchParams(u.split("?")[1] || "").get("access_token");
  if (u.includes("graph.facebook.com") && tokIn === GTOK) {
    calls.push({ method, url: u, body: opts.body });
    const ok = (j) => ({ ok: true, status: 200, json: async () => j });
    const path = u.match(/graph\.facebook\.com\/v[\d.]+\/([^?]+)/)[1];
    if (path === `${GACC}/campaigns`) return ok({ data: GCAMPS });
    if (path === `${GACC}/adsets`) {
      const ids = new Set((JSON.parse(new URLSearchParams(u.split("?")[1] || "").get("filtering") || "[{}]")[0].value || []).map(String));
      const data = [];
      for (const [cid, sets] of Object.entries(ADSETS)) if (ids.has(cid)) for (const s of sets) data.push({ ...s, campaign_id: cid });
      return ok({ data });
    }
    if (path === `${GACC}/ads`) return ok({ data: [] });
    if (ADS[path]) return ok(ADS[path]);
    return { ok: false, status: 404, json: async () => ({ error: { message: "nf " + path } }) };
  }
  return prevFetch(url, opts);
};
r = await (await handler(req({ mode: "cr", region: "gl", dryRun: true, items: [{ ad_id: AD_G, ad_account_id: GACC }] }))).json();
const pG = r.plan[0];
check(pG && !pG.error && pG.product === "TW shaman", "G: 국가+상품 = TW shaman (shaman↔무당 canon): " + (pG?.error || pG?.product));
const gNames = (pG?.targets || []).map((t) => t.adset_name).sort().join(",");
check(gNames === "twMudang1,twMudangWW", "G: 대만_무당 ASC 2개만 (미국 무당·대만 무녀·일반 제외) → " + gNames);
r = await (await handler(req({ mode: "cr", region: "xx", dryRun: true, items: [{ ad_id: AD_G, ad_account_id: GACC }] }))).json();
check(r.ok === false && /region/.test(r.error), "알 수 없는 region 거절: " + r.error);

console.log("4) 상품 추출 규칙");
// 핸들러 내부 함수를 직접 못 부르므로 캠페인명→상품 매핑은 위 시나리오(🔥집착_… → 집착, 💵재물_… → 재물)로 확인됨.
console.log(fails ? `\n✗ ${fails} 실패` : "\n✓ 전부 통과");
process.exit(fails ? 1 : 0);
