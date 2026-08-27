// 추이차트 셀 클릭 → '세트 × 그 날짜'의 시간별(1시간) ROAS 화면에 쓰는 데이터.
//
// 왜 Edge Function 인가:
//   시간별 그레인은 DB에 없다. kr_channel_revenue_4h 는 '채널 × 4시간' 이라 세트로 쪼갤 수 없고,
//   세트-시간 테이블을 새로 적재하면 DDL + cron + 백필이 붙는다(그리고 적재 안 된 날짜는 빈 화면).
//   반대로 원천(Meta insights hourly / Mixpanel export)은 아무 과거 날짜나 즉시 조회된다.
//   → 클릭할 때 그때그때 원천을 읽는다. 저장하지 않으므로 백필도, 창(window) 제한도 없다.
//   Meta 토큰·Mixpanel 시크릿을 브라우저에 둘 수 없어서 서버 한 겹이 필요하다(apply-budget 과 같은 이유).
//
// 요청: POST { mode:'kr'|'gl'|'vn', adset_id, ad_account_id?,
//              date:'YYYY-MM-DD'  또는  date_from/date_to (추이차트 주별·월별 셀) }
// 응답: { ok, currency, date_from, date_to, days, hours:[{h,...}], totals, notes[] }
//
// 구간(주/월) 요청이면 **그 구간의 모든 날을 시각(0~23시)으로 접어서** 합산한다.
// "이 주에는 몇 시가 잘 팔렸나" 를 보는 화면이지 날짜별 추이가 아니다(그건 추이차트 본체다).
// 지출은 Meta insights 를 time_range 로 한 번에 받고(응답이 날짜×시각 행이라 시각으로 누적하면 끝),
// 매출은 Mixpanel export 를 구간 전체로 한 번만 받는다 — 날짜마다 호출하면 N배 느리다.
//
// 그레인·기준(추이차트 셀과 같게 맞춘 것):
//   지출 = Meta insights, breakdowns=hourly_stats_aggregated_by_advertiser_time_zone.
//          광고주 타임존이 KST 라 이 시각이 곧 KST 시각 → 매출 시각과 정렬된다.
//   매출 = Mixpanel 결제완료/payment_complete 중 utm_term(=adset_id) 일치 + Meta 계열 utm_source.
//          파이프라인(국내/글로벌/밴스드 세트별)의 귀속 규칙과 동일. dedup 도 같은 순서로.
//   통화 = 국내·밴스드 KRW / 글로벌 USD (추이차트 norm() 의 표시 통화와 같다).
//
// 추이차트 일별 셀과 완전히 일치하지 않는 지점(응답 notes 로 UI에 그대로 노출한다):
//   · 크로스셀 UTM 백필(distinct_id 라스트터치 재귀속)은 일별 파이프라인에만 있다 → 여기 합계가 더 낮을 수 있다.
//   · 환율은 실시간(open.er-api.com)이고 파이프라인은 그날의 일별 환율을 쓴다.
//   · 글로벌 GL_META_DAYS(메타 보고값 치환 날짜)는 셀이 Meta 기준, 여기는 Mixpanel 기준이다.
//
// 배포: README.md

const META_API_VERSION = "v21.0";
const GRAPH = `https://graph.facebook.com/${META_API_VERSION}`;

// 광고계정 → (토큰 환경변수 후보, 계정 지출통화).
// apply-budget 의 ACC_TOKEN_ENV, 국내_시간대매출_supabase.py 의 META_SPEND_ACCOUNTS 와 같은 목록.
// 계정이 늘면 세 곳을 함께 고칠 것.
const ACC: Record<string, { env: string[]; ccy: string }> = {
  // 국내
  "act_1270614404675034": { env: ["META_TOKEN_1"], ccy: "KRW" },
  "act_707835224206178": { env: ["META_TOKEN_1"], ccy: "KRW" },
  "act_1808141386564262": { env: ["META_TOKEN_2_1", "META_TOKEN_2"], ccy: "KRW" },
  // 글로벌
  "act_1054081590008088": { env: ["META_TOKEN_1"], ccy: "USD" },
  "act_2677707262628563": { env: ["META_TOKEN_GlobalTT", "META_TOKEN_4", "META_TOKEN_3"], ccy: "USD" },
  "act_1335040608536838": { env: ["META_TOKEN_GlobalTT", "META_TOKEN_4", "META_TOKEN_3"], ccy: "USD" },
  "act_993712016404855": { env: ["META_TOKEN_ACT_9937"], ccy: "USD" },
  "act_1021437716898605": { env: ["META_TOKEN_1"], ccy: "USD" },
  // 밴스드 (대만 계정 포함 — 밴스드 파이프라인 관례상 지출 원장은 KRW)
  "act_25183853061243175": { env: ["META_TOKEN_VANCED"], ccy: "KRW" },
  "act_1560037899174007": { env: ["META_TOKEN_VANCED"], ccy: "KRW" },
  "act_1286632473622244": { env: ["META_TOKEN_VANCED"], ccy: "KRW" },
};

// ad_account_id 를 못 받았을 때(파이프라인 행에 계정이 비어 있는 경우) 순서대로 시도할 토큰.
const TOKEN_ENVS = [
  "META_TOKEN_1", "META_TOKEN_VANCED", "META_TOKEN_GlobalTT", "META_TOKEN_ACT_9937",
  "META_TOKEN_2_1", "META_TOKEN_2", "META_TOKEN_4", "META_TOKEN_3",
];

const MODE_CCY: Record<string, string> = { kr: "KRW", gl: "USD", vn: "KRW" };
// 한 번에 집계할 수 있는 최대 일수(추이차트 월별 버킷 = 최대 31일).
const MAX_SPAN_DAYS = 31;

const MP_PROJECT = Deno.env.get("MIXPANEL_PROJECT_ID") || "3390233";
const MP_USER = Deno.env.get("MIXPANEL_USERNAME") || "";
const MP_SECRET = Deno.env.get("MIXPANEL_SECRET") || "";
const MP_EVENTS = ["결제완료", "payment_complete"];

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY = Deno.env.get("SB_SECRET_KEY") || Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { ...CORS, "Content-Type": "application/json" } });
}

async function getUser(jwt: string) {
  const r = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
    headers: { Authorization: `Bearer ${jwt}`, apikey: SERVICE_KEY },
  });
  if (!r.ok) return null;
  return await r.json();
}

// ── 공통 헬퍼 (파이프라인에서 이식) ────────────────────────────
function cleanId(v: unknown): string {
  const s = String(v ?? "").trim();
  if (!s) return "";
  if (/^\d+$/.test(s)) return s;
  if (/^\d+\.\d+$/.test(s)) return s.split(".")[0];              // 시트 경유로 실수화된 ID
  if (/^[\d.]+[eE][+\-]?\d+$/.test(s)) {                          // 1.23e+17 형태
    const n = Number(s);
    if (isFinite(n)) return BigInt(Math.round(n)).toString();
  }
  const digits = s.replace(/[^0-9]/g, "");
  return digits || s;
}

const META_UTM = new Set(["ig", "fb", "an", "msg", "instagram", "facebook", "threads", "th"]);
function isMetaSource(src: unknown): boolean {
  const s = String(src ?? "").trim().toLowerCase();
  if (!s) return false;
  if (META_UTM.has(s)) return true;
  return s.startsWith("ig") || s.startsWith("fb") || s.includes("instagram") ||
    s.includes("facebook") || s.includes("site_source_name");
}

const KNOWN_NONKRW = new Set(["TWD", "HKD", "THB", "JPY", "USD"]);
const SUFFIX_CCY: Record<string, string> = { tw: "TWD", th: "THB", jp: "JPY", hk: "HKD" };
const CC_CCY: Record<string, string> = { TW: "TWD", HK: "HKD", TH: "THB", JP: "JPY" };
function eventCurrency(p: Record<string, unknown>): string {
  const c = String(p["통화"] ?? "").trim().toUpperCase();
  if (KNOWN_NONKRW.has(c)) return c;
  if (c === "KRW") return "KRW";
  const m = /-([a-z]{2,3})$/.exec(String(p["서비스"] ?? "").trim().toLowerCase());
  if (m && SUFFIX_CCY[m[1]]) return SUFFIX_CCY[m[1]];
  const cc = String(p["mp_country_code"] ?? "").trim().toUpperCase();
  return CC_CCY[cc] || "KRW";
}

// USD 1 단위당 각 통화 값. 실패하면 폴백(파이프라인 FALLBACK_RATES 와 같은 값).
const FALLBACK_PER_USD: Record<string, number> = {
  USD: 1, KRW: 1450, TWD: 32, JPY: 155, HKD: 7.8, THB: 35.5, SGD: 1.28,
};
let _rates: Record<string, number> | null = null;
async function perUsd(): Promise<Record<string, number>> {
  if (_rates) return _rates;
  const r = { ...FALLBACK_PER_USD };
  try {
    const res = await fetch("https://open.er-api.com/v6/latest/USD");
    if (res.ok) {
      const j = await res.json();
      for (const k of Object.keys(r)) if (j?.rates?.[k]) r[k] = Number(j.rates[k]);
    }
  } catch { /* 폴백 유지 */ }
  _rates = r;
  return r;
}
// 환율표를 한 번 받아두고 이벤트 수천 건을 동기로 환산한다(루프 안에서 await 하지 않게).
function makeConv(rt: Record<string, number>) {
  return (amount: number, from: string, to: string) => {
    if (!amount || from === to) return amount;
    return amount / (rt[from] ?? 1) * (rt[to] ?? 1);
  };
}

// ── Meta: 세트(들) × 하루 × 1시간 지출 ────────────────────────
type Hourly = { spend: number; impressions: number; clicks: number };
type SetRef = { id: string; acc: string };
// 종합 행은 그 모드의 모든 세트를 보낸다. 국내·글로벌이 각각 수백 개라 넉넉히 두되,
// 실수로 만 단위가 오면 거절한다(계정별 filtering IN 호출 수가 그만큼 늘어난다).
const MAX_SETS = 1200;

function tokensFor(acc: string): string[] {
  const names = ACC[acc]?.env ?? TOKEN_ENVS;
  const out: string[] = [];
  for (const n of names) {
    const v = Deno.env.get(n) || "";
    if (v && !out.includes(v)) out.push(v);
  }
  // 계정 매핑에 없는(신규) 계정이면 가진 토큰을 전부 훑는다 — 소유 계정 토큰만 200 을 준다.
  if (!ACC[acc]) {
    for (const n of TOKEN_ENVS) {
      const v = Deno.env.get(n) || "";
      if (v && !out.includes(v)) out.push(v);
    }
  }
  return out;
}

// insights 응답(시간대 브레이크다운) → 시각별 누계. mul = 계정통화→표시통화 배수.
function accumHourly(rows: any[], hours: Record<number, Hourly>, mul: number) {
  for (const row of rows) {
    const m = /^(\d{1,2})/.exec(String(row["hourly_stats_aggregated_by_advertiser_time_zone"] ?? ""));
    if (!m) continue;
    const h = Number(m[1]);
    if (!(h >= 0 && h <= 23)) continue;
    const cur = hours[h] ?? (hours[h] = { spend: 0, impressions: 0, clicks: 0 });
    cur.spend += Number(row.spend || 0) * mul;
    cur.impressions += Number(row.impressions || 0);
    cur.clicks += Number(row.inline_link_clicks || 0);
  }
}

// 토큰을 차례로 시도하며 페이지를 끝까지 훑는다. 200 을 준 토큰의 결과만 채택.
// 페이지 상한 — 넘으면 잘렸다고 알린다(조용한 과소집계 금지).
const MAX_PAGES = 60;
let truncated = false;

async function metaPaged(path: string, params: Record<string, string>, acc: string,
                         hours: Record<number, Hourly>, mul: number): Promise<string> {
  const tokens = tokensFor(acc);
  if (!tokens.length) return "Meta 토큰이 설정되지 않았습니다(Edge Secret 확인)";
  let lastErr = "";
  for (const token of tokens) {
    const qs = new URLSearchParams({ ...params, access_token: token });
    let url: string | null = `${GRAPH}/${path}?${qs}`;
    const tmp: Record<number, Hourly> = {};
    let ok = false, page = 0;
    while (url) {
      let res: Response;
      try { res = await fetch(url); } catch (e) { lastErr = String((e as Error).message || e); break; }
      if (!res.ok) { lastErr = (await res.text()).slice(0, 300); break; }
      const j = await res.json();
      ok = true;
      accumHourly(j.data ?? [], tmp, mul);
      url = j.paging?.next ?? null;
      if (++page > MAX_PAGES) { if (url) truncated = true; break; }
    }
    if (ok) {
      for (const [h, v] of Object.entries(tmp)) {
        const cur = hours[Number(h)] ?? (hours[Number(h)] = { spend: 0, impressions: 0, clicks: 0 });
        cur.spend += v.spend; cur.impressions += v.impressions; cur.clicks += v.clicks;
      }
      return "";
    }
  }
  return lastErr || "Meta 조회 실패";
}

// ⚠ 구간(주/월)에서는 time_increment 를 "all_days" 로 둔다.
//   "1" 이면 응답이 (세트 × 날짜 × 24시각) 행이라, 종합 행(세트 수백 개) × 한 달이면
//   7만 행을 넘겨 페이지 상한에 걸려 **조용히 잘린다**. 어차피 날짜는 접어서 시각별로만 쓰므로
//   Meta 쪽에서 미리 합쳐 받는 편이 정확하고 훨씬 빠르다(세트당 24행).
//   하루짜리는 기존 동작을 그대로 둔다("1" == all_days 이지만 검증된 경로를 건드리지 않는다).
const META_BASE_PARAMS = (from: string, to: string) => ({
  time_range: JSON.stringify({ since: from, until: to }),
  breakdowns: "hourly_stats_aggregated_by_advertiser_time_zone",
  time_increment: from === to ? "1" : "all_days",
  fields: "spend,impressions,inline_link_clicks",
  limit: "500",
});

// 세트 하나든(셀 클릭) 수백 개든(종합·소계) 같은 입구.
//   1개 → 세트 엔드포인트(계정 없이도 조회된다).
//   여러 개 → 계정별로 묶어 level=adset + filtering IN 한 번에. 세트 수만큼 호출하지 않는다.
//   계정을 모르는 세트는 세트 엔드포인트로 개별 조회하되 MAX_SOLO 개까지만(무한 팬아웃 방지).
const MAX_SOLO = 25;
const CHUNK = 100;   // filtering IN 한 번에 넣는 세트 수 (URL 길이·응답 크기 타협점)

async function metaHourlySets(sets: SetRef[], from: string, to: string, target: string,
                              conv: (n: number, f: string, t: string) => number) {
  const hours: Record<number, Hourly> = {};
  const errs: string[] = [];
  const notes: string[] = [];
  // time_increment=1 이라 응답은 날짜×시각 행이다. accumHourly 가 날짜를 무시하고
  // 시각으로만 누적하므로 구간을 그대로 넘기면 시각별 합계가 나온다.
  const base = META_BASE_PARAMS(from, to);

  if (sets.length === 1) {
    const s = sets[0];
    const mul = conv(1, ACC[s.acc]?.ccy || target, target);
    const e = await metaPaged(`${s.id}/insights`, base, s.acc, hours, mul);
    if (e) errs.push(e);
    return { hours, err: errs.join(" / "), notes };
  }

  const byAcc = new Map<string, string[]>();
  for (const s of sets) {
    const k = s.acc || "";
    if (!byAcc.has(k)) byAcc.set(k, []);
    byAcc.get(k)!.push(s.id);
  }
  for (const [acc, ids] of byAcc) {
    if (!acc) {
      const solo = ids.slice(0, MAX_SOLO);
      if (ids.length > solo.length) notes.push(`광고계정이 비어 있는 세트 ${ids.length}개 중 ${solo.length}개만 지출을 집계했습니다`);
      for (const id of solo) {
        const e = await metaPaged(`${id}/insights`, base, "", hours, 1);
        if (e) errs.push(e);
      }
      continue;
    }
    const mul = conv(1, ACC[acc]?.ccy || target, target);
    for (let i = 0; i < ids.length; i += CHUNK) {
      const chunk = ids.slice(i, i + CHUNK);
      const e = await metaPaged(`${acc}/insights`, {
        ...base,
        level: "adset",
        filtering: JSON.stringify([{ field: "adset.id", operator: "IN", value: chunk }]),
      }, acc, hours, mul);
      if (e) errs.push(e);
    }
  }
  return { hours, err: [...new Set(errs)].join(" / "), notes };
}

// ── Mixpanel: 세트 × 하루 × 1시간 매출 ────────────────────────
type Ev = {
  date: string; hour: number; utm_term: string; utm_source: string;
  revenue: number; ccy: string; order: string; insert_id: string;
  distinct_id: string; svc: string;
};

// 같은 날짜를 다른 세트로 여러 번 클릭하면 export 를 다시 받지 않는다(isolate 살아있는 동안).
// export 는 '그 날짜 전체 결제'라 세트와 무관하게 재사용된다.
const MP_CACHE = new Map<string, { at: number; evs: Ev[] }>();
const MP_TTL_MS = 10 * 60 * 1000;

function utcToday(): string {
  return new Date().toISOString().slice(0, 10);
}
function addDays(d: string, n: number): string {
  const t = new Date(d + "T00:00:00Z");
  t.setUTCDate(t.getUTCDate() + n);
  return t.toISOString().slice(0, 10);
}

// export 응답을 한 줄씩 흘려보내며 파싱한다. res.text() 로 통째로 받으면 한 달치(20만 줄 이상)에서
// 메모리가 터진다 — 구간 지원의 핵심이라 반드시 스트리밍을 유지할 것.
async function mpFetchLines(from: string, to: string, onEv: (ev: any) => void): Promise<string> {
  const qs = new URLSearchParams({
    from_date: from, to_date: to,
    event: JSON.stringify(MP_EVENTS),
    project_id: MP_PROJECT,
  });
  // where 절은 쓰지 않는다 — 조건이 붙으면 날짜창이 조용히 무시되는 사례가 있어(memory:
  // mixpanel-export-where-gotchas) 파이프라인과 같이 전량 받아 여기서 필터한다.
  const auth = "Basic " + btoa(`${MP_USER}:${MP_SECRET}`);
  for (let attempt = 0; attempt < 3; attempt++) {
    const res = await fetch(`https://data.mixpanel.com/api/2.0/export?${qs}`, { headers: { Authorization: auth } });
    if (res.status === 429) { await new Promise((r) => setTimeout(r, 3000 * (attempt + 1))); continue; }
    if (!res.ok) return `Mixpanel HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`;
    if (!res.body) return "Mixpanel 응답 본문이 비어 있습니다";
    const reader = res.body.getReader();
    const dec = new TextDecoder();
    let buf = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let i: number;
      while ((i = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, i);
        buf = buf.slice(i + 1);
        if (line.trim()) { try { onEv(JSON.parse(line)); } catch { /* 깨진 줄은 버린다 */ } }
      }
    }
    if (buf.trim()) { try { onEv(JSON.parse(buf)); } catch { /* 마지막 줄 */ } }
    return "";
  }
  return "Mixpanel 조회 실패(429 재시도 초과)";
}

async function mpEvents(from: string, to: string): Promise<{ evs: Ev[]; err: string }> {
  const ck = `${from}|${to}`;
  const hit = MP_CACHE.get(ck);
  if (hit && Date.now() - hit.at < MP_TTL_MS) return { evs: hit.evs, err: "" };
  if (!MP_USER || !MP_SECRET) {
    return { evs: [], err: "Mixpanel 시크릿(MIXPANEL_USERNAME/MIXPANEL_SECRET)이 설정되지 않았습니다" };
  }
  // export 의 날짜 필터는 UTC 기준. KST 하루(= UTC 전날 15:00 ~ 당일 15:00)를 덮으려면 전날까지 받는다.
  // to_date 가 UTC 오늘보다 크면 400 → UTC 오늘로 자른다(KST 오늘 00~09시에 발생).
  const qTo = to > utcToday() ? utcToday() : to;
  const qFrom = addDays(from, -1);

  const evs: Ev[] = [];
  const err = await mpFetchLines(qFrom, qTo, (ev) => {
    const p = ev?.properties || {};
    const ts = Number(p.time || 0);
    if (!ts) return;
    const kst = new Date((ts + 9 * 3600) * 1000);            // epoch(UTC초) + 9h → KST 벽시계
    let ut = "";
    for (const k of ["utm_term", "UTM_Term", "UTM Term"]) if (p[k]) { ut = cleanId(p[k]); break; }
    let us = "";
    for (const k of ["utm_source", "UTM_Source", "UTM Source"]) if (p[k]) { us = String(p[k]).trim(); break; }
    const amt = Number(p.amount ?? p["결제금액"] ?? 0) || 0;   // 해외는 amount=실청구액(memory)
    const val = Number(p.value ?? 0) || 0;
    evs.push({
      date: kst.toISOString().slice(0, 10),
      hour: kst.getUTCHours(),
      utm_term: ut, utm_source: us,
      revenue: amt > 0 ? amt : (val > 0 ? val : 0),
      ccy: eventCurrency(p),
      order: String(p.order_id ?? p.order_no ?? "").trim(),
      insert_id: String(p["$insert_id"] ?? p.insert_id ?? ""),
      distinct_id: String(p.distinct_id ?? ""),
      svc: String(p["서비스"] ?? ""),
    });
  });
  if (err) return { evs: [], err };
  const kept = dedup(evs);
  MP_CACHE.set(ck, { at: Date.now(), evs: kept });
  return { evs: kept, err: "" };
}

// order_id(utm_term 보유 우선 → revenue 큰 것) → $insert_id → (date,distinct_id,서비스).
// 파이프라인(국내_시간대매출/글로벌_세트별/밴스드_세트별)과 같은 순서. export 는 $insert_id
// 중복을 제거해 주지 않으므로(memory) 반드시 거쳐야 한다.
function dedup(evs: Ev[]): Ev[] {
  const byOrder = new Map<string, Ev>();
  const out: Ev[] = [];
  const seenIns = new Set<string>();
  const seenDds = new Set<string>();
  for (const e of evs) {
    if (e.order) {
      const cur = byOrder.get(e.order);
      if (!cur) { byOrder.set(e.order, e); continue; }
      const better = (e.utm_term ? 1 : 0) - (cur.utm_term ? 1 : 0) || (e.revenue - cur.revenue);
      if (better > 0) byOrder.set(e.order, e);
      continue;
    }
    if (e.insert_id) {
      if (seenIns.has(e.insert_id)) continue;
      seenIns.add(e.insert_id);
    } else {
      const k = `${e.date}|${e.distinct_id}|${e.svc}`;
      if (seenDds.has(k)) continue;
      seenDds.add(k);
    }
    out.push(e);
  }
  return out.concat([...byOrder.values()]);
}

// ── 핸들러 ────────────────────────────────────────────────────
Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });
  if (req.method !== "POST") return json({ ok: false, error: "POST only" }, 405);

  const jwt = (req.headers.get("Authorization") || "").replace(/^Bearer\s+/i, "");
  if (!jwt) return json({ ok: false, error: "인증 없음" }, 401);
  const user = await getUser(jwt);
  if (!user?.id) return json({ ok: false, error: "로그인이 필요합니다" }, 401);

  let body: any;
  try { body = await req.json(); } catch { return json({ ok: false, error: "JSON 파싱 실패" }, 400); }

  const mode = String(body?.mode || "");
  // 일별 셀은 date 하나, 주별·월별 셀은 date_from~date_to. 예전 클라이언트 호환으로 date 도 받는다.
  const dFrom = String(body?.date_from || body?.date || "");
  const dTo = String(body?.date_to || body?.date || dFrom);

  // 셀 하나(세트) = adset_id / 종합·소계 = sets:[{id,acc}]. 둘 다 같은 경로로 처리한다.
  const raw: any[] = Array.isArray(body?.sets) && body.sets.length
    ? body.sets
    : [{ id: body?.adset_id, acc: body?.ad_account_id }];
  const seen = new Set<string>();
  const sets: SetRef[] = [];
  for (const s of raw) {
    const id = cleanId(s?.id ?? s?.adset_id);
    if (!/^\d{6,}$/.test(id) || seen.has(id)) continue;
    seen.add(id);
    sets.push({ id, acc: String(s?.acc ?? s?.ad_account_id ?? "") });
  }

  if (!MODE_CCY[mode]) return json({ ok: false, error: "세트 단위 추이차트(국내·글로벌·밴스드)에서만 지원합니다" }, 400);
  if (!sets.length) return json({ ok: false, error: "세트 ID 형식 오류" }, 400);
  if (sets.length > MAX_SETS) return json({ ok: false, error: `한 번에 ${MAX_SETS}개 세트까지만 집계합니다(요청 ${sets.length}개)` }, 400);
  const DFMT = /^\d{4}-\d{2}-\d{2}$/;
  if (!DFMT.test(dFrom) || !DFMT.test(dTo)) return json({ ok: false, error: "날짜 형식 오류" }, 400);
  if (dTo < dFrom) return json({ ok: false, error: "날짜 구간이 거꾸로입니다" }, 400);
  const days = Math.round((Date.parse(dTo + "T00:00:00Z") - Date.parse(dFrom + "T00:00:00Z")) / 86400000) + 1;
  // 한 달(31일)까지. 더 길면 Mixpanel export 수신만으로 함수 실행시간을 넘긴다.
  if (days > MAX_SPAN_DAYS) {
    return json({ ok: false, error: `한 번에 ${MAX_SPAN_DAYS}일까지만 집계합니다(요청 ${days}일)` }, 400);
  }

  const target = MODE_CCY[mode];
  const idSet = new Set(sets.map((s) => s.id));
  truncated = false;   // isolate 가 재사용되므로 요청마다 초기화

  // 환율표를 먼저 받아 두고(캐시라 대개 즉시) 지출·매출을 병렬로 — MP export 가 훨씬 느리다.
  const rates = await perUsd();
  const conv = makeConv(rates);
  const [meta, mp] = await Promise.all([metaHourlySets(sets, dFrom, dTo, target, conv), mpEvents(dFrom, dTo)]);

  const hours = Array.from({ length: 24 }, (_, h) => ({
    h, spend: 0, revenue: 0, purchases: 0, impressions: 0, clicks: 0,
  }));

  for (const [hs, v] of Object.entries(meta.hours)) {
    const h = Number(hs);
    hours[h].spend = v.spend;              // metaHourlySets 가 이미 표시통화로 환산해 준다
    hours[h].impressions = v.impressions;
    hours[h].clicks = v.clicks;
  }
  for (const e of mp.evs) {
    if (e.date < dFrom || e.date > dTo) continue;
    if (!e.utm_term || !idSet.has(e.utm_term)) continue;
    if (!isMetaSource(e.utm_source)) continue;                 // stale utm 오귀속 차단(파이프라인과 동일)
    hours[e.hour].revenue += conv(e.revenue, e.ccy, target);
    hours[e.hour].purchases += 1;
  }

  const totals = hours.reduce((a, x) => ({
    spend: a.spend + x.spend, revenue: a.revenue + x.revenue, purchases: a.purchases + x.purchases,
    impressions: a.impressions + x.impressions, clicks: a.clicks + x.clicks,
  }), { spend: 0, revenue: 0, purchases: 0, impressions: 0, clicks: 0 });

  const notes: string[] = [];
  if (truncated) notes.push("Meta 응답이 페이지 상한에 걸려 지출 일부가 빠졌습니다 — 기간을 좁혀서 다시 봐 주세요.");
  if (meta.err) notes.push("지출(Meta) 조회 실패: " + meta.err);
  if (mp.err) notes.push("매출(Mixpanel) 조회 실패: " + mp.err);
  for (const n of meta.notes) notes.push(n);
  notes.push("매출=Mixpanel utm_term 귀속. 일별 파이프라인의 크로스셀 UTM 백필은 반영되지 않아 합계가 셀보다 낮을 수 있습니다.");
  if (days > 1) notes.push(`${days}일치를 시각(0~23시)으로 접어 합산한 값입니다 — 날짜별 추이가 아니라 '이 구간에 몇 시가 잘 나왔나' 를 보는 화면입니다.`);
  const ccys = [...new Set(sets.map((s) => ACC[s.acc]?.ccy || target))].filter((c) => c !== target);
  if (ccys.length) notes.push(`지출 ${ccys.join("·")}→${target} 실시간 환율 환산`);

  return json({
    ok: true, mode, date: dFrom, date_from: dFrom, date_to: dTo, days, sets: sets.length,
    adset_id: sets.length === 1 ? sets[0].id : null,
    ad_account_id: sets.length === 1 ? sets[0].acc : null,
    currency: target, hours, totals, notes,
  });
});
