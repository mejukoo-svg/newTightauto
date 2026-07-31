// apply-budget-google 로컬 검증 — Deno·fetch 를 스텁해 핸들러를 직접 호출한다.
// 실행: node --experimental-strip-types _verify.mjs   (실제 구글·Supabase 호출 없음)

const CUST = "5912047700";
const AG1 = "201847072270"; // 캠페인 A (예산 100,000원)
const AG2 = "198897301655"; // 캠페인 B (예산 100,000원)
const AG3 = "202590576670"; // 캠페인 B — AG2 와 같은 캠페인(충돌 시나리오용)
const AG_OFF = "196002475823"; // OFF 대상
const CAMP_A = "23985137658", CAMP_B = "24060513481", CAMP_OFF = "23924516839";

let handler = null;
let calls = [];
let LOG_ROWS = [];
let HL = {};

globalThis.Deno = {
  env: { get: (k) => globalThis.__ENV[k] },
  serve: (h) => { handler = h; },
};

const AG = {
  [AG1]: { camp: CAMP_A, name: "무당_올웨이즈온", status: "ENABLED" },
  [AG2]: { camp: CAMP_B, name: "무녀_피안싸다구", status: "ENABLED" },
  [AG3]: { camp: CAMP_B, name: "무녀_피안싸다구_tROAS", status: "ENABLED" },
  [AG_OFF]: { camp: CAMP_OFF, name: "구미호", status: "ENABLED" },
};
const BUD = {
  [CAMP_A]: { rn: `customers/${CUST}/campaignBudgets/1001`, micros: "100000000000" }, // 10만원
  [CAMP_B]: { rn: `customers/${CUST}/campaignBudgets/1002`, micros: "100000000000" },
  [CAMP_OFF]: { rn: `customers/${CUST}/campaignBudgets/1003`, micros: "50000000000" },
};

globalThis.fetch = async (url, init = {}) => {
  const u = String(url);
  const method = init.method || "GET";
  let body = null;
  try { body = init.body ? JSON.parse(init.body) : null; } catch { body = String(init.body || ""); }
  calls.push({ method, u: u.split("?")[0], full: u, body });
  const ok = (j) => new Response(JSON.stringify(j), { status: 200, headers: { "Content-Type": "application/json" } });

  if (u.includes("/auth/v1/user")) return ok({ id: "u1", email: "dashboard@newtightauto.app" });
  if (u.includes("/rest/v1/adset_highlights")) {
    return ok(Object.entries(HL).map(([adset_id, highlight]) => ({ adset_id, highlight })));
  }
  if (u.includes("/rest/v1/budget_apply_log")) {
    if (method === "POST") return new Response("", { status: 201 });
    return ok(LOG_ROWS);
  }
  if (u.includes("oauth2.googleapis.com/token")) return ok({ access_token: "AT-TEST", expires_in: 3599 });

  if (u.includes("googleAds:search")) {
    const q = body.query;
    const ids = (q.match(/IN \(([^)]*)\)/) || [, ""])[1].split(",").map((s) => s.trim()).filter(Boolean);
    if (q.includes("FROM ad_group")) {
      return ok({
        results: ids.filter((id) => AG[id]).map((id) => ({
          adGroup: { id, name: AG[id].name, status: AG[id].status, resourceName: `customers/${CUST}/adGroups/${id}` },
          campaign: { id: AG[id].camp, name: "캠페인" + AG[id].camp, status: "ENABLED" },
        })),
      });
    }
    return ok({
      results: ids.filter((c) => BUD[c]).map((c) => ({
        campaign: { id: c },
        campaignBudget: { resourceName: BUD[c].rn, amountMicros: BUD[c].micros, explicitlyShared: false, referenceCount: 1 },
      })),
    });
  }
  if (u.includes(":mutate")) return ok({ results: [{ resourceName: body.operations[0].update.resourceName }] });
  return new Response("{}", { status: 404 });
};

function baseEnv() {
  return {
    SUPABASE_URL: "https://stub.supabase.co", SUPABASE_SERVICE_ROLE_KEY: "svc",
    G_ADS_DEV_TOKEN: "dev", G_ADS_CLIENT_ID: "cid", G_ADS_CLIENT_SECRET: "sec",
    G_ADS_REFRESH_TOKEN: "ref", G_ADS_LOGIN_ID: "1577915960", G_ADS_CUSTOMER_ID: CUST,
  };
}

async function run(body, { env = baseEnv(), hl = null, log = [] } = {}) {
  globalThis.__ENV = env;
  HL = hl || Object.fromEntries((body.items || []).map((i) => [i.adset_id, i.tag]));
  LOG_ROWS = log;
  calls = [];
  const res = await handler(new Request("https://stub/apply-budget-google", {
    method: "POST",
    headers: { Authorization: "Bearer jwt", "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }));
  return { status: res.status, json: await res.json(), calls };
}

globalThis.__ENV = baseEnv();
await import("./index.ts");
if (!handler) throw new Error("Deno.serve 핸들러를 못 잡음");

let fail = 0;
const check = (name, cond, extra = "") => {
  console.log(`${cond ? "  OK  " : "  FAIL"} ${name}${extra ? "  — " + extra : ""}`);
  if (!cond) fail++;
};
const mutates = (r) => r.calls.filter((c) => c.u.includes(":mutate"));

console.log("\n[1] dry-run — 캠페인 예산 ±% 계획, 쓰기 0건");
{
  const r = await run({ dryRun: true, items: [{ adset_id: AG1, tag: "up10" }] });
  const p = r.json?.plan?.[0];
  check("scope=campaign", p?.scope === "campaign", JSON.stringify(p?.error));
  check("100,000 → 110,000 (원 단위)", p?.before === "100000" && p?.after === "110000", `${p?.before}→${p?.after}`);
  check("대상은 캠페인 예산 리소스", p?.target_id === BUD[CAMP_A].rn, p?.target_id);
  check("비고에 캠페인 예산 경고", (p?.note || "").includes("하위 광고그룹 전체에 영향"), p?.note);
  check("쓰기 0건", mutates(r).length === 0);
}

console.log("\n[2] 실제 적용 — micros 로 환산해 campaignBudgets:mutate 1건");
{
  const r = await run({ dryRun: false, items: [{ adset_id: AG1, tag: "down20" }] });
  const m = mutates(r);
  check("mutate 1건", m.length === 1, JSON.stringify(m.map((x) => x.u)));
  check("campaignBudgets 엔드포인트", (m[0]?.u || "").includes("campaignBudgets:mutate"), m[0]?.u);
  check("80,000원 = 80000000000 micros", m[0]?.body?.operations?.[0]?.update?.amountMicros === "80000000000",
    m[0]?.body?.operations?.[0]?.update?.amountMicros);
  check("updateMask=amount_micros", m[0]?.body?.operations?.[0]?.updateMask === "amount_micros");
  check("applied=true", r.json?.plan?.[0]?.applied === true, JSON.stringify(r.json?.plan?.[0]?.error));
}

console.log("\n[3] OFF — 광고그룹만 PAUSED, 캠페인 예산은 건드리지 않는다");
{
  const r = await run({ dryRun: false, items: [{ adset_id: AG_OFF, tag: "off" }] });
  const m = mutates(r);
  const p = r.json?.plan?.[0];
  check("adGroups:mutate 1건", m.length === 1 && m[0].u.includes("adGroups:mutate"), m[0]?.u);
  check("ACTIVE→PAUSED", p?.before === "ENABLED" && p?.after === "PAUSED", `${p?.before}→${p?.after}`);
  check("캠페인 예산 호출 없음", !m.some((x) => x.u.includes("campaignBudgets")));
}

console.log("\n[4] 복증(watch) — 아무것도 하지 않는다");
{
  const r = await run({ dryRun: false, items: [{ adset_id: AG1, tag: "watch" }] });
  check("쓰기 0건", mutates(r).length === 0);
  check("비고 = 복증", (r.json?.plan?.[0]?.note || "").includes("복증"), r.json?.plan?.[0]?.note);
}

console.log("\n[5] 같은 캠페인에 다른 증감률 → 양쪽 차단 / 같은 증감률 → 1회만 적용");
{
  const r = await run({ dryRun: false, items: [{ adset_id: AG2, tag: "up10" }, { adset_id: AG3, tag: "up20" }] });
  check("둘 다 conflict", r.json?.plan?.every((p) => p.conflict), JSON.stringify(r.json?.plan?.map((p) => p.error)));
  check("쓰기 0건", mutates(r).length === 0);

  const r2 = await run({ dryRun: false, items: [{ adset_id: AG2, tag: "up20" }, { adset_id: AG3, tag: "up20" }] });
  check("같은 비율은 mutate 1건", mutates(r2).length === 1, JSON.stringify(mutates(r2).map((x) => x.body?.operations?.[0]?.update)));
  check("두 행 모두 applied", r2.json?.plan?.every((p) => p.applied === true),
    JSON.stringify(r2.json?.plan?.map((p) => [p.adset_id, p.applied, p.error])));
}

console.log("\n[6] DB 마킹과 다르면 차단 (낡은 화면)");
{
  const r = await run({ dryRun: false, items: [{ adset_id: AG1, tag: "up20" }] }, { hl: { [AG1]: "up10" } });
  check("마킹 불일치 오류", (r.json?.plan?.[0]?.error || "").includes("마킹 불일치"), r.json?.plan?.[0]?.error);
  check("쓰기 0건", mutates(r).length === 0);
  check("구글 호출 자체가 없음", !r.calls.some((c) => c.u.includes("googleads")));
}

console.log("\n[7] 계정에 없는 광고그룹 / ID 형식 오류");
{
  const r = await run({ dryRun: true, items: [{ adset_id: "999999999999", tag: "up10" }, { adset_id: "abc", tag: "up10" }] },
    { hl: { "999999999999": "up10", abc: "up10" } });
  check("미존재 광고그룹 오류", (r.json?.plan?.[0]?.error || "").includes("찾지 못함"), r.json?.plan?.[0]?.error);
  check("숫자 아닌 ID 거부", (r.json?.plan?.[1]?.error || "").includes("형식"), r.json?.plan?.[1]?.error);
}

console.log("\n[8] 오늘 이미 적용된 광고그룹은 redo 표시 (차단은 아님)");
{
  const r = await run({ dryRun: true, items: [{ adset_id: AG1, tag: "up10" }] },
    { log: [{ adset_id: AG1, tag: "up10", before_value: "90000", after_value: "100000", applied_at: new Date(Date.now() - 3600e3).toISOString() }] });
  const p = r.json?.plan?.[0];
  check("redo=true", p?.redo === true, p?.note);
  check("비고에 이력", (p?.note || "").includes("이미 적용됨"), p?.note);
  const q = r.calls.find((c) => c.u.includes("budget_apply_log") && c.method === "GET");
  check("로그 조회 region=gd", !!q && q.full.includes("region=eq.gd") && q.full.includes("ok=is.true"), q?.full?.slice(-90));
}

console.log("\n[9] 미인증 / 자격증명 누락");
{
  const res = await handler(new Request("https://stub/x", { method: "POST", body: "{}" }));
  check("Authorization 없으면 401", res.status === 401);
  const r = await run({ dryRun: true, items: [{ adset_id: AG1, tag: "up10" }] },
    { env: { ...baseEnv(), G_ADS_REFRESH_TOKEN: "" } });
  check("자격증명 누락 500", r.status === 500 && (r.json?.error || "").includes("G_ADS_REFRESH_TOKEN"), r.json?.error);
}

console.log(fail ? `\n실패 ${fail}건` : "\n전부 통과");
process.exit(fail ? 1 : 0);
