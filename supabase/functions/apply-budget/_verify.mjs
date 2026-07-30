// apply-budget Edge Function 로컬 검증 — Deno·fetch 를 스텁해 핸들러를 직접 호출한다.
// 실행: node --experimental-strip-types _verify.mjs   (실제 메타·Supabase 호출 없음)

const ACC_NEW = "act_1808141386564262"; // META_TOKEN_2_1 담당 (구 META_TOKEN_2)
const ACC_T1 = "act_1270614404675034"; // META_TOKEN_1 담당 — 회귀 확인용

const NEW_TOK = "TOK-2-1-new-with-ads_management";
const OLD_TOK = "TOK-2-old-ads_read-only";
const T1_TOK = "TOK-1";

let handler = null;
let calls = [];
let LOG_ROWS = []; // budget_apply_log 조회 응답 (기본: 오늘 적용 이력 없음)

globalThis.Deno = {
  env: { get: (k) => globalThis.__ENV[k] },
  serve: (h) => {
    handler = h;
  },
};

// 실제 ID 길이를 그대로 쓴다 — act_1808141386564262 은 13자리, 나머지 계정은 18자리,
// 구글 디멘드젠 ad_group_id 는 12·16자리(16자리가 옛 15자리 가드를 통과했다).
const SET_13 = "6902893311021"; // act_1808141386564262 실제 세트
const SET_18 = "120214000000000001"; // act_1270614404675034 (CBO)
const SET_OFF = "120214000000000002"; // act_1270614404675034 (OFF 대상)
const GOOGLE_16 = "1959521207820001"; // 구글 ad_group_id 형태 — 메타 세트가 아님

const HL = {
  [SET_13]: "up10",
  [SET_18]: "up20",
  [SET_OFF]: "off",
  [GOOGLE_16]: "up10",
};

// 세트 → 소속 계정 (metaGet 의 account_id 대조용)
const OWNER = {
  [SET_13]: "1808141386564262",
  [SET_18]: "1270614404675034",
  [SET_OFF]: "1270614404675034",
};

globalThis.fetch = async (url, init = {}) => {
  const u = String(url);
  const method = init.method || "GET";
  const tok = new URL(u).searchParams.get("access_token") ||
    new URLSearchParams(init.body || "").get("access_token") || "";
  calls.push({ method, u: u.split("?")[0], tok, full: u });

  const ok = (j) => new Response(JSON.stringify(j), { status: 200, headers: { "Content-Type": "application/json" } });

  if (u.includes("/auth/v1/user")) return ok({ id: "u1", email: "dashboard@newtightauto.app" });
  if (u.includes("/rest/v1/adset_highlights")) {
    return ok(Object.entries(HL).map(([adset_id, highlight]) => ({ adset_id, highlight })));
  }
  if (u.includes("/rest/v1/budget_apply_log")) {
    if (method === "POST") return new Response("", { status: 201 });
    return ok(LOG_ROWS); // 오늘 이미 적용된 세트 조회 (테스트마다 LOG_ROWS 를 바꾼다)
  }

  if (u.includes("graph.facebook.com")) {
    const path = u.split("graph.facebook.com/")[1].split("?")[0].split("/").pop();
    if (method === "POST") return ok({ success: true });
    if (path.startsWith("act_")) return ok({ currency: "KRW" });
    const acct = OWNER[path];
    if (path === SET_13) {
      return ok({ id: path, name: "속궁합_츄량", status: "ACTIVE", account_id: acct, daily_budget: "200000", campaign_id: "c1", campaign: { id: "c1", name: "캠페인1" } });
    }
    if (path === SET_18) {
      return ok({ id: path, name: "위닝ASC_CBO", status: "ACTIVE", account_id: acct, campaign_id: "c9", campaign: { id: "c9", name: "CBO캠페인", daily_budget: "300000" } });
    }
    if (path === SET_OFF) {
      return ok({ id: path, name: "중단대상", status: "ACTIVE", account_id: acct, daily_budget: "50000", campaign_id: "c2", campaign: { id: "c2" } });
    }
    // 구글 ad_group_id 등 메타에 없는 객체 — 실제 Graph API 와 같은 오류를 흉내낸다
    return new Response(JSON.stringify({ error: { message: "Unsupported get request. Object with ID does not exist", code: 100 } }), { status: 400 });
  }
  return new Response("{}", { status: 404 });
};

function baseEnv() {
  return {
    SUPABASE_URL: "https://stub.supabase.co",
    SUPABASE_SERVICE_ROLE_KEY: "svc",
    META_TOKEN_1: T1_TOK,
  };
}

async function run(env, body, logRows = []) {
  globalThis.__ENV = env;
  LOG_ROWS = logRows;
  calls = [];
  const res = await handler(new Request("https://stub/apply-budget", {
    method: "POST",
    headers: { Authorization: "Bearer jwt", "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }));
  return { status: res.status, json: await res.json(), calls };
}

globalThis.__ENV = baseEnv();
await import("./index.ts");
if (!handler) throw new Error("Deno.serve 핸들러를 못 잡음");

const items = [
  { adset_id: SET_13, ad_account_id: ACC_NEW, tag: "up10" },
];
let fail = 0;
const check = (name, cond, extra = "") => {
  console.log(`${cond ? "  OK  " : "  FAIL"} ${name}${extra ? "  — " + extra : ""}`);
  if (!cond) fail++;
};

console.log("\n[1] META_TOKEN_2_1 · META_TOKEN_2 둘 다 있을 때 → 새 토큰을 쓴다");
{
  const r = await run({ ...baseEnv(), META_TOKEN_2_1: NEW_TOK, META_TOKEN_2: OLD_TOK }, { mode: "kr", dryRun: true, items });
  const toks = new Set(r.calls.filter((c) => c.u.includes("graph")).map((c) => c.tok));
  check("메타 호출이 새 토큰만 사용", toks.size === 1 && toks.has(NEW_TOK), [...toks].join(","));
  check("계획 정상 (200000 → 220000)", r.json?.plan?.[0]?.before === "200000" && r.json?.plan?.[0]?.after === "220000",
    JSON.stringify(r.json?.plan?.[0]?.error || ""));
  check("dry-run 은 쓰기 0건", r.calls.filter((c) => c.method === "POST" && c.u.includes("graph")).length === 0);
}

console.log("\n[2] META_TOKEN_2_1 없고 구 META_TOKEN_2 만 있을 때 → 구 토큰으로 폴백");
{
  const r = await run({ ...baseEnv(), META_TOKEN_2: OLD_TOK }, { mode: "kr", dryRun: true, items });
  const toks = new Set(r.calls.filter((c) => c.u.includes("graph")).map((c) => c.tok));
  check("구 토큰 사용", toks.size === 1 && toks.has(OLD_TOK), [...toks].join(","));
}

console.log("\n[3] 둘 다 없을 때 → 후보 이름을 모두 보여주는 오류");
{
  const r = await run(baseEnv(), { mode: "kr", dryRun: true, items });
  check("오류 메시지에 두 이름", r.json?.plan?.[0]?.error === "토큰 미설정: META_TOKEN_2_1 / META_TOKEN_2", r.json?.plan?.[0]?.error);
  check("메타 호출 0건", r.calls.filter((c) => c.u.includes("graph")).length === 0);
}

console.log("\n[4] 실제 적용(dryRun:false) → 새 토큰으로 POST");
{
  const r = await run({ ...baseEnv(), META_TOKEN_2_1: NEW_TOK, META_TOKEN_2: OLD_TOK },
    { mode: "kr", dryRun: false, items });
  const posts = r.calls.filter((c) => c.method === "POST" && c.u.includes("graph"));
  check("쓰기 1건", posts.length === 1, JSON.stringify(posts));
  check("쓰기에 새 토큰 사용", posts[0]?.tok === NEW_TOK, posts[0]?.tok);
  check("applied=true", r.json?.plan?.[0]?.applied === true, JSON.stringify(r.json?.plan?.[0]?.error || ""));
}

console.log("\n[5] 회귀 — 다른 계정(META_TOKEN_1, 18자리 세트)은 그대로");
{
  const r = await run({ ...baseEnv(), META_TOKEN_2_1: NEW_TOK }, {
    mode: "kr", dryRun: true,
    items: [
      { adset_id: SET_18, ad_account_id: ACC_T1, tag: "up20" },
      { adset_id: SET_OFF, ad_account_id: ACC_T1, tag: "off" },
    ],
  });
  const toks = new Set(r.calls.filter((c) => c.u.includes("graph")).map((c) => c.tok));
  check("META_TOKEN_1 사용", toks.size === 1 && toks.has(T1_TOK), [...toks].join(","));
  const cbo = r.json?.plan?.find((p) => p.adset_id === SET_18);
  check("CBO 는 캠페인 예산 (300000 → 360000)", cbo?.scope === "campaign" && cbo?.target_id === "c9" && cbo?.after === "360000", JSON.stringify(cbo));
  const off = r.json?.plan?.find((p) => p.adset_id === SET_OFF);
  check("OFF 는 세트 status → PAUSED", off?.scope === "adset" && off?.field === "status" && off?.after === "PAUSED", JSON.stringify(off));
}

console.log("\n[6] 구글 ad_group_id(16자리)는 메타 계정으로 보내도 막힌다");
{
  const r = await run({ ...baseEnv(), META_TOKEN_2_1: NEW_TOK }, {
    mode: "kr", dryRun: false,
    items: [{ adset_id: GOOGLE_16, ad_account_id: ACC_T1, tag: "up10" }],
  });
  const p = r.json?.plan?.[0];
  check("오류로 처리", !!p?.error, JSON.stringify(p?.error));
  check("쓰기 0건", r.calls.filter((c) => c.method === "POST" && c.u.includes("graph")).length === 0);
}

console.log("\n[7] 계정 소유가 다른 세트를 보내면 막힌다 (account_id 대조)");
{
  const r = await run({ ...baseEnv(), META_TOKEN_2_1: NEW_TOK }, {
    mode: "kr", dryRun: false,
    // SET_13 은 act_1808141386564262 소속인데 act_1270614404675034 로 요청
    items: [{ adset_id: SET_13, ad_account_id: ACC_T1, tag: "up10" }],
  });
  const p = r.json?.plan?.[0];
  check("소유 계정 불일치 오류", (p?.error || "").includes("소속인데"), JSON.stringify(p?.error));
  check("쓰기 0건", r.calls.filter((c) => c.method === "POST" && c.u.includes("graph")).length === 0);
}

console.log("\n[8] 오늘 이미 적용된 세트는 redo 로 표시된다 (막지는 않는다)");
{
  const nowIso = new Date(Date.now() - 3600 * 1000).toISOString(); // 1시간 전
  const r = await run({ ...baseEnv(), META_TOKEN_2_1: NEW_TOK }, { mode: "kr", dryRun: true, items },
    [{ adset_id: SET_13, tag: "up10", before_value: "500000", after_value: "550000", applied_at: nowIso }]);
  const p = r.json?.plan?.[0];
  check("redo=true", p?.redo === true, JSON.stringify(p?.note));
  check("비고에 이력 문구", (p?.note || "").includes("이미 적용됨") && (p.note || "").includes("+10%"), p?.note);
  check("계획은 그대로 살아있다(차단 아님)", !p?.error && p?.after === "220000", JSON.stringify(p?.error));
  const q = r.calls.find((c) => c.u.includes("budget_apply_log") && c.method === "GET");
  check("로그 조회에 region·ok·KST일자 필터", !!q && q.full.includes("region=eq.kr") &&
    q.full.includes("ok=is.true") && q.full.includes("applied_at=gte."), q?.full?.slice(-120));
}

console.log("\n[9] 오늘 이력이 없으면 redo 없음 · 다른 세트 이력은 옮겨붙지 않는다");
{
  const r = await run({ ...baseEnv(), META_TOKEN_2_1: NEW_TOK }, { mode: "kr", dryRun: true, items },
    [{ adset_id: SET_18, tag: "up20", before_value: "1", after_value: "2", applied_at: new Date().toISOString() }]);
  const p = r.json?.plan?.[0];
  check("다른 세트 이력에 반응하지 않음", !p?.redo, JSON.stringify(p?.note));
}

console.log(fail ? `\n실패 ${fail}건` : "\n전부 통과");
process.exit(fail ? 1 : 0);


