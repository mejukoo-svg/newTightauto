// 추이차트·날짜탭 '예산' 컬럼 실시간 조회 — 지금 메타에 설정돼 있는 예산을 그대로 읽어 돌려준다.
//
// 왜 필요한가:
//   예산 컬럼은 파이프라인이 시간당 한 번 찍는 스냅샷이라, 증액/감액을 적용한 직후나
//   Ads Manager 에서 직접 바꾼 뒤에는 화면이 옛 값을 보여준다. 게다가 파이프라인은
//   activities(예산 변경이력)로 값을 복원하는데, 메타가 최근 이벤트를 조용히 누락하면
//   며칠째 옛 값에 고착된다(실측 2026-09-24: +20% 적용 뒤에도 적용 전 값 유지).
//   → 화면이 열릴 때 메타에서 '지금 값'을 직접 읽어 덮어쓴다.
//
// 왜 Edge Function 인가: index.html 은 공개 정적 파일이라 메타 토큰을 브라우저에 둘 수 없다
//   (apply-budget 과 같은 이유). 이 함수는 읽기 전용이라 아무것도 수정하지 않는다.
//
// 예산이 붙어 있는 곳을 찾는 순서는 apply-budget 의 planOne / 파이프라인의 budget_resolve 와 같다:
//   세트 daily_budget → 세트 lifetime_budget(기간으로 나눠 일예산 환산)
//   → 캠페인 daily_budget → 캠페인 lifetime_budget(환산)
//
// 요청: POST { mode:'kr'|'gl'|'vn', items:[{adset_id, ad_account_id}] }
// 응답: { ok, budgets:{ [adset_id]: {raw, value, source} }, missing:[id...] }
//        value  = 대시보드 예산 컬럼과 같은 단위(국내·밴스드 ₩ / 글로벌 raw÷100)
//        source = 'adset_daily' | 'adset_lifetime' | 'campaign_daily' | 'campaign_lifetime'
//
// 배포: supabase functions deploy current-budgets

const META_API_VERSION = "v21.0";
const GRAPH = `https://graph.facebook.com/${META_API_VERSION}`;

// apply-budget 의 ACC_TOKEN_ENV 와 같은 매핑을 유지할 것 (읽기만 하므로 읽기 토큰이면 충분).
const ACC_TOKEN_ENV: Record<string, string[]> = {
  // 국내
  "act_1270614404675034": ["META_TOKEN_1"],
  "act_707835224206178": ["META_TOKEN_1"],
  "act_1808141386564262": ["META_TOKEN_2_1", "META_TOKEN_2"],
  // 글로벌
  "act_1054081590008088": ["META_TOKEN_1"],
  "act_2677707262628563": ["META_TOKEN_GlobalTT", "META_TOKEN_4", "META_TOKEN_3"],
  "act_1335040608536838": ["META_TOKEN_GlobalTT", "META_TOKEN_4", "META_TOKEN_3"],
  "act_993712016404855": ["META_TOKEN_ACT_9937"],
  "act_1021437716898605": ["META_TOKEN_1"],
  // 밴스드
  "act_25183853061243175": ["META_TOKEN_VANCED"],
  "act_1560037899174007": ["META_TOKEN_VANCED"],
  "act_1286632473622244": ["META_TOKEN_VANCED"],
};

function tokenFor(acc: string): string {
  for (const n of ACC_TOKEN_ENV[acc] || []) {
    const v = Deno.env.get(n) || "";
    if (v) return v;
  }
  return "";
}

// 저장 컬럼과 같은 단위로 맞춘다 — 파이프라인과 동일한 규칙이어야 화면에서 두 값이 섞이지 않는다.
//   국내(ad_performance_daily.budget)             : 계정이 전부 KRW → raw 그대로
//   글로벌(global_ad_performance_daily.budget_usd) : raw ÷ 100
//   밴스드(vanced_ad_performance_daily.budget)     : KRW → raw 그대로
const MODE_DIV: Record<string, number> = { kr: 1, gl: 100, vn: 1 };

const BATCH = 50; // ids= 한 번에 보낼 오브젝트 수 (메타 상한)
const MAX_ITEMS = 1000; // 한 요청에서 볼 세트 수 상한

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY = Deno.env.get("SB_SECRET_KEY") ||
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS, "Content-Type": "application/json" },
  });
}

async function getUser(jwt: string) {
  const r = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
    headers: { Authorization: `Bearer ${jwt}`, apikey: SERVICE_KEY },
  });
  if (!r.ok) return null;
  return await r.json();
}

function num(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

// 메타 시각('2026-09-22T18:10:03+0900') → ms. Date 가 못 읽는 +0900 형태라 콜론을 끼워 준다.
function ts(s: unknown): number {
  if (!s) return NaN;
  const t = Date.parse(String(s).replace(/([+-]\d{2})(\d{2})$/, "$1:$2"));
  return Number.isFinite(t) ? t : NaN;
}

// 총예산 → 일예산 환산. 기간을 모르면 0(=모름) — 확실하지 않은 큰 숫자를 넣지 않는다.
//   budget_resolve.lifetime_to_daily 와 같은 규칙.
function lifetimeToDaily(life: unknown, start: unknown, end: unknown): number {
  const lt = num(life);
  if (!lt) return 0;
  const s = ts(start);
  if (!Number.isFinite(s)) return 0;
  const e = ts(end);
  const until = Number.isFinite(e) && e > s ? e : Date.now();
  const days = Math.max((until - s) / 86400000, 1);
  return Math.round(lt / days);
}

const FIELDS = "id,daily_budget,lifetime_budget,start_time,end_time,campaign_id," +
  "campaign{id,daily_budget,lifetime_budget,start_time,stop_time}";

async function fetchBatch(ids: string[], token: string): Promise<Record<string, any>> {
  const out: Record<string, any> = {};
  if (!ids.length) return out;
  const u = `${GRAPH}/?ids=${encodeURIComponent(ids.join(","))}` +
    `&fields=${encodeURIComponent(FIELDS)}&access_token=${encodeURIComponent(token)}`;
  const r = await fetch(u);
  if (!r.ok) {
    // 잘못된 id 하나에 배치 전체가 400 이 된다 → 반으로 쪼개 재시도, 1개짜리는 버린다.
    if (ids.length === 1) return out;
    const mid = ids.length >> 1;
    Object.assign(out, await fetchBatch(ids.slice(0, mid), token));
    Object.assign(out, await fetchBatch(ids.slice(mid), token));
    return out;
  }
  const j = await r.json().catch(() => ({}));
  for (const [k, v] of Object.entries(j || {})) {
    if (v && typeof v === "object" && (v as any).id) out[String(k)] = v;
  }
  return out;
}

function resolve(a: any): { raw: number; source: string } {
  let v = num(a.daily_budget);
  if (v) return { raw: v, source: "adset_daily" };
  v = lifetimeToDaily(a.lifetime_budget, a.start_time, a.end_time);
  if (v) return { raw: v, source: "adset_lifetime" };
  const c = a.campaign || {};
  v = num(c.daily_budget);
  if (v) return { raw: v, source: "campaign_daily" };
  // 캠페인 총예산은 세트 자신의 일정으로 먼저 나눈다 — 캠페인 stop_time 에는 '마지막으로
  // 손댄 시각'이 들어오는 일이 있어 그대로 나누면 일예산이 몇 배로 튄다(budget_resolve 와 동일).
  v = lifetimeToDaily(c.lifetime_budget, a.start_time, a.end_time) ||
    lifetimeToDaily(c.lifetime_budget, c.start_time, c.stop_time);
  if (v) return { raw: v, source: "campaign_lifetime" };
  return { raw: 0, source: "" };
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });
  if (req.method !== "POST") return json({ ok: false, error: "POST only" }, 405);

  const jwt = (req.headers.get("Authorization") || "").replace(/^Bearer\s+/i, "");
  const user = jwt ? await getUser(jwt) : null;
  if (!user?.id) return json({ ok: false, error: "로그인이 필요합니다." }, 401);

  const body = await req.json().catch(() => null);
  const mode = String(body?.mode || "");
  const div = MODE_DIV[mode];
  if (!div) return json({ ok: false, error: `지원하지 않는 모드: ${mode}` }, 400);

  const items = Array.isArray(body?.items) ? body.items.slice(0, MAX_ITEMS) : [];
  // 계정별로 묶는다 — 세트는 자기 계정 토큰으로만 읽힌다.
  const byAcc: Record<string, string[]> = {};
  for (const it of items) {
    const id = String(it?.adset_id ?? "").trim();
    const acc = String(it?.ad_account_id ?? "").trim();
    if (!/^\d{9,}$/.test(id)) continue;
    if (!ACC_TOKEN_ENV[acc]) continue;
    if (!byAcc[acc]) byAcc[acc] = [];
    byAcc[acc].push(id);
  }

  const budgets: Record<string, { raw: number; value: number; source: string }> = {};
  const noToken: string[] = [];
  await Promise.all(
    Object.entries(byAcc).map(async ([acc, ids]) => {
      const token = tokenFor(acc);
      if (!token) {
        noToken.push(acc);
        return;
      }
      for (let i = 0; i < ids.length; i += BATCH) {
        const got = await fetchBatch(ids.slice(i, i + BATCH), token);
        for (const [id, a] of Object.entries(got)) {
          const r = resolve(a);
          if (r.raw > 0) {
            budgets[id] = {
              raw: r.raw,
              value: div === 1 ? r.raw : Math.round((r.raw / div) * 100) / 100,
              source: r.source,
            };
          }
        }
      }
    }),
  );

  const missing = items
    .map((it: any) => String(it?.adset_id ?? ""))
    .filter((id: string) => id && !budgets[id]);

  const res: Record<string, unknown> = {
    ok: true,
    mode,
    at: new Date().toISOString(),
    budgets,
    missing,
  };
  if (noToken.length) res.noToken = noToken;
  return json(res);
});
