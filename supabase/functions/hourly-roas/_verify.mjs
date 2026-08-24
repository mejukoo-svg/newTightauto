// hourly-roas Edge Function 로컬 검증 — Deno·fetch 를 스텁해 핸들러를 직접 호출한다.
// 실행: node --experimental-strip-types _verify.mjs   (실제 메타·Mixpanel·Supabase 호출 없음)
//
// 여기서 지키는 것:
//   · KST 버킷 — export 의 properties.time(UTC epoch)이 어느 '시(hour)'로 떨어지는가.
//     UTC 16:00 = KST 다음날 01:00 이라 날짜 경계가 틀리면 여기서 잡힌다.
//   · 귀속 — utm_term=이 세트 + Meta 계열 utm_source 만. 다른 세트/오가닉/구글은 새지 않는다.
//   · dedup — 같은 order_id 는 1건, utm_term 보유 행이 이긴다($insert_id 중복도 제거).
//   · 통화 — 글로벌(USD 표시) 세트의 TWD 결제가 USD 로 환산되는가.
//   · 입력 검증 — 모드/세트ID/날짜 형식, 비로그인 401.

let handler = null;
globalThis.Deno = {
  env: { get: (k) => globalThis.__ENV[k] },
  serve: (h) => { handler = h; },
};

globalThis.__ENV = {
  SUPABASE_URL: "https://example.supabase.co",
  SUPABASE_SERVICE_ROLE_KEY: "svc",
  META_TOKEN_1: "TOK-1",
  META_TOKEN_VANCED: "TOK-VN",
  MIXPANEL_USERNAME: "mpuser",
  MIXPANEL_SECRET: "mpsecret",
  MIXPANEL_PROJECT_ID: "3390233",
};

const KR_ACC = "act_1270614404675034";   // KRW
const GL_ACC = "act_1054081590008088";   // USD
const SET_KR = "120214000000000001";
const SET_GL = "120214000000000002";
const DATE = "2026-08-23";

// ── Mixpanel export 픽스처 ─────────────────────────────────────
// time 은 UTC epoch(초). KST = +9h.
const T = (iso) => Math.floor(new Date(iso).getTime() / 1000);
const ev = (p) => JSON.stringify({ event: "결제완료", properties: p });

const MP_LINES = [
  // ① 이 세트 · KST 2026-08-23 01시 (UTC 08-22 16:00) — 날짜 경계 테스트
  ev({ time: T("2026-08-22T16:30:00Z"), utm_term: SET_KR, utm_source: "ig", amount: 30000, order_id: "A1", $insert_id: "i1", 통화: "KRW", distinct_id: "d1" }),
  // ② 같은 주문 중복행(utm 없음, 금액 더 큼) → ①이 이겨야 한다
  ev({ time: T("2026-08-22T16:31:00Z"), utm_term: "", utm_source: "", amount: 99000, order_id: "A1", $insert_id: "i2", 통화: "KRW", distinct_id: "d1" }),
  // ③ 이 세트 · KST 14시 · 주문번호 없고 $insert_id 중복 2행 → 1건만
  ev({ time: T("2026-08-23T05:00:00Z"), utm_term: SET_KR, utm_source: "fb", amount: 20000, $insert_id: "i3", 통화: "KRW", distinct_id: "d2" }),
  ev({ time: T("2026-08-23T05:00:00Z"), utm_term: SET_KR, utm_source: "fb", amount: 20000, $insert_id: "i3", 통화: "KRW", distinct_id: "d2" }),
  // ④ 다른 세트 → 새면 안 된다
  ev({ time: T("2026-08-23T06:00:00Z"), utm_term: "999999999999999", utm_source: "ig", amount: 50000, order_id: "B1", 통화: "KRW", distinct_id: "d3" }),
  // ⑤ utm_term 은 이 세트지만 Meta 계열 소스가 아님(stale utm) → 제외
  ev({ time: T("2026-08-23T07:00:00Z"), utm_term: SET_KR, utm_source: "google", amount: 70000, order_id: "C1", 통화: "KRW", distinct_id: "d4" }),
  // ⑥ 전날(KST 08-22 23시) → 이 날짜 아님
  ev({ time: T("2026-08-22T14:00:00Z"), utm_term: SET_KR, utm_source: "ig", amount: 11000, order_id: "D1", 통화: "KRW", distinct_id: "d5" }),
  // ⑦ 글로벌 세트 · KST 10시 · TWD 3200 → USD 100 (픽스처 환율 TWD 32/USD)
  ev({ time: T("2026-08-23T01:00:00Z"), utm_term: SET_GL, utm_source: "ig", amount: 3200, order_id: "E1", 통화: "TWD", distinct_id: "d6" }),
];

// ── Meta insights 픽스처 ───────────────────────────────────────
const META_ROWS = {
  [SET_KR]: [
    { date_start: DATE, hourly_stats_aggregated_by_advertiser_time_zone: "01:00:00 - 01:59:59", spend: "10000", impressions: "5000", inline_link_clicks: "50" },
    { date_start: DATE, hourly_stats_aggregated_by_advertiser_time_zone: "14:00:00 - 14:59:59", spend: "5000", impressions: "2000", inline_link_clicks: "20" },
  ],
  [SET_GL]: [
    { date_start: DATE, hourly_stats_aggregated_by_advertiser_time_zone: "10:00:00 - 10:59:59", spend: "40", impressions: "900", inline_link_clicks: "12" },
  ],
};

let mpCalls = 0;
let metaAuth = [];

globalThis.fetch = async (url, opts) => {
  const u = String(url);
  const res = (body, status = 200) => ({
    ok: status >= 200 && status < 300, status,
    json: async () => body,
    text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
  });

  if (u.includes("/auth/v1/user")) {
    const jwt = (opts?.headers?.Authorization || "").replace(/^Bearer\s+/, "");
    return jwt === "GOODJWT" ? res({ id: "u1", email: "dashboard@newtightauto.app" }) : res({}, 401);
  }
  if (u.includes("graph.facebook.com")) {
    const m = /graph\.facebook\.com\/[^/]+\/(\d+)\/insights/.exec(u);
    const setId = m ? m[1] : "";
    metaAuth.push(new URL(u).searchParams.get("access_token"));
    return res({ data: META_ROWS[setId] || [] });
  }
  if (u.includes("data.mixpanel.com")) {
    mpCalls++;
    const qs = new URL(u).searchParams;
    // export 날짜창: KST 하루를 덮으려면 전날부터여야 한다
    if (qs.get("from_date") !== "2026-08-22") throw new Error("from_date 가 전날이 아님: " + qs.get("from_date"));
    if (qs.get("to_date") !== DATE) throw new Error("to_date 불일치: " + qs.get("to_date"));
    if (u.includes("where=")) throw new Error("where 절을 쓰면 안 된다(날짜창 무시 사례)");
    return res(MP_LINES.join("\n"));
  }
  if (u.includes("open.er-api.com")) {
    return res({ rates: { KRW: 1450, TWD: 32, USD: 1, JPY: 155, HKD: 7.8, THB: 35.5, SGD: 1.28 } });
  }
  throw new Error("예상치 못한 fetch: " + u);
};

await import("./index.ts");

const call = (body, jwt = "GOODJWT") =>
  handler(new Request("https://f/hourly-roas", {
    method: "POST",
    headers: jwt ? { Authorization: "Bearer " + jwt, "Content-Type": "application/json" } : { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }));

let fail = 0;
const ok = (cond, label, extra) => {
  console.log((cond ? "  ✅ " : "  ❌ ") + label + (cond ? "" : "  ← " + JSON.stringify(extra)));
  if (!cond) fail++;
};
const near = (a, b) => Math.abs(a - b) < 0.5;

// ── 1) 국내 세트 ───────────────────────────────────────────────
console.log("\n[1] 국내(kr) — KST 버킷 · 귀속 · dedup");
{
  const j = await (await call({ mode: "kr", adset_id: SET_KR, ad_account_id: KR_ACC, date: DATE })).json();
  const h = j.hours || [];
  ok(j.ok === true && j.currency === "KRW", "ok + 통화 KRW", j);
  ok(near(h[1]?.spend, 10000) && near(h[14]?.spend, 5000), "지출이 01시/14시로", [h[1]?.spend, h[14]?.spend]);
  ok(near(h[1]?.revenue, 30000), "01시 매출 30,000 (UTC 16:30 → KST 01:30, 중복행 아닌 utm 보유행)", h[1]);
  ok(h[1]?.purchases === 1, "01시 구매 1건 (order_id 중복 제거)", h[1]);
  ok(near(h[14]?.revenue, 20000) && h[14]?.purchases === 1, "14시 매출 20,000 · 1건 ($insert_id 중복 제거)", h[14]);
  ok(near(j.totals.revenue, 50000), "합계 매출 50,000 — 다른 세트·비Meta·전날 결제는 제외", j.totals);
  ok(near(j.totals.spend, 15000), "합계 지출 15,000", j.totals);
  ok(j.totals.impressions === 7000 && j.totals.clicks === 70, "노출·클릭 합계", j.totals);
  ok(metaAuth.includes("TOK-1"), "계정→토큰 매핑(META_TOKEN_1)", metaAuth);
}

// ── 2) 글로벌 세트 (USD 환산) ──────────────────────────────────
console.log("\n[2] 글로벌(gl) — TWD 결제 → USD");
{
  const j = await (await call({ mode: "gl", adset_id: SET_GL, ad_account_id: GL_ACC, date: DATE })).json();
  const h = j.hours || [];
  ok(j.currency === "USD", "통화 USD", j.currency);
  ok(near(h[10]?.spend, 40), "10시 지출 $40 (계정통화 USD 그대로)", h[10]);
  ok(near(h[10]?.revenue, 100), "10시 매출 TWD 3,200 → $100", h[10]);
  ok(near(j.totals.revenue / j.totals.spend * 100, 250), "하루 ROAS 250%", j.totals);
}

// ── 3) 캐시: 같은 날짜 두 번째 조회는 export 재호출 없음 ────────
console.log("\n[3] Mixpanel export 캐시");
ok(mpCalls === 1, "export 호출 1회 (세트가 달라도 같은 날짜는 재사용)", mpCalls);

// ── 4) 입력 검증 ───────────────────────────────────────────────
console.log("\n[4] 입력 검증");
{
  const bad = async (body, label) => {
    const r = await call(body);
    ok(r.status === 400, label, r.status);
  };
  await bad({ mode: "cr", adset_id: SET_KR, date: DATE }, "소재(cr) 모드는 400");
  await bad({ mode: "kr", adset_id: "12", date: DATE }, "짧은 세트ID는 400");
  await bad({ mode: "kr", adset_id: SET_KR, date: "8/23" }, "잘못된 날짜는 400");
  const r401 = await call({ mode: "kr", adset_id: SET_KR, date: DATE }, "");
  ok(r401.status === 401, "비로그인 401", r401.status);
  const rjwt = await call({ mode: "kr", adset_id: SET_KR, date: DATE }, "BADJWT");
  ok(rjwt.status === 401, "무효 JWT 401", rjwt.status);
}

// ── 5) 미래 날짜 to_date 클램프 (KST 오늘 00~09시 400 방지) ─────
console.log("\n[5] to_date UTC 상한 클램프");
{
  const utcToday = new Date().toISOString().slice(0, 10);
  let seenTo = null;
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url, opts) => {
    const u = String(url);
    if (u.includes("data.mixpanel.com")) {
      seenTo = new URL(u).searchParams.get("to_date");
      return { ok: true, status: 200, json: async () => ({}), text: async () => "" };
    }
    return realFetch(url, opts);
  };
  // KST 로는 '오늘'이지만 UTC 로는 아직 어제일 수 있는 날짜
  const kstToday = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
  await call({ mode: "kr", adset_id: SET_KR, ad_account_id: KR_ACC, date: kstToday });
  globalThis.fetch = realFetch;
  ok(seenTo != null && seenTo <= utcToday, "to_date ≤ UTC 오늘", { seenTo, utcToday });
}

console.log(fail ? `\n❌ 실패 ${fail}건` : "\n✅ 전부 통과");
process.exit(fail ? 1 : 0);
