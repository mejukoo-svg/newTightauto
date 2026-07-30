// 날짜탭 '메타에 예산 적용' — 대시보드 증감액 마킹을 실제 Meta 예산에 반영한다.
//
// 왜 Edge Function 인가:
//   index.html 은 정적 파일이라 소스가 그대로 공개된다(Supabase Auth 로 데이터는 잠갔지만 HTML 본문은 아님).
//   Meta 쓰기 토큰을 브라우저에 둘 수 없으므로 서버 한 겹을 둔다. 토큰은 Edge Secret 에만 존재.
//
// 설계상 중요한 두 가지:
//   1) 브라우저가 보낸 예산값을 쓰지 않는다. 날짜탭 '예산'은 파이프라인 스냅샷 + 통화환산을 거친
//      표시용 값이라 실제 메타값과 다를 수 있다. 여기서 매번 메타에서 현재값을 다시 읽어 ± 를 계산한다.
//   2) CBO(캠페인 예산) 캠페인은 세트에 daily_budget 이 없다. 파이프라인(fetch_adset_budgets)은
//      이때 캠페인 예산을 세트 예산으로 폴백 표시하므로, 그 값을 세트에 그대로 쓰면 엉뚱한 곳을 바꾼다.
//      → 예산이 실제로 붙어있는 객체(세트 vs 캠페인)를 판별해 그쪽을 수정하고, scope 를 응답에 실어
//        UI 가 "이 캠페인의 모든 세트에 영향" 이라고 경고할 수 있게 한다.
//
// 요청: POST { mode:'kr'|'gl'|'vn', dryRun:boolean, items:[{adset_id, ad_account_id, tag}] }
// 응답: { ok, dryRun, plan:[...] }   — dryRun 이면 계획만, 아니면 각 항목에 applied/error 포함
//
// 배포: README.md 참고 (supabase functions deploy apply-budget --no-verify-jwt=false)

const META_API_VERSION = "v21.0";
const GRAPH = `https://graph.facebook.com/${META_API_VERSION}`;

// 광고계정 → 토큰 환경변수명(후보를 앞에서부터 훑어 먼저 설정된 것을 쓴다). 파이프라인
// (국내_세트별_supabase.py / 글로벌_세트별_supabase.py / 밴스드_세트별_supabase.py)의
// META_TOKENS 매핑과 계정 목록을 동일하게 유지할 것.
const ACC_TOKEN_ENV: Record<string, string[]> = {
  // 국내
  "act_1270614404675034": ["META_TOKEN_1"],
  "act_707835224206178": ["META_TOKEN_1"],
  // 파이프라인이 쓰는 META_TOKEN_2 는 스코프가 ads_read 뿐이라 예산 수정이 불가하다.
  // ads_management 를 가진 META_TOKEN_2_1 을 우선 쓰고, 미설정일 때만 구 토큰으로 폴백한다
  // (폴백 시 읽기·계획은 되고 적용 단계에서 메타가 권한 오류를 돌려준다).
  "act_1808141386564262": ["META_TOKEN_2_1", "META_TOKEN_2"],
  // 글로벌
  "act_1054081590008088": ["META_TOKEN_1"],
  "act_2677707262628563": ["META_TOKEN_GlobalTT"],
  "act_1335040608536838": ["META_TOKEN_GlobalTT"],
  "act_993712016404855": ["META_TOKEN_ACT_9937"],
  "act_1021437716898605": ["META_TOKEN_1"],
  // 밴스드
  "act_25183853061243175": ["META_TOKEN_VANCED"],
  "act_1560037899174007": ["META_TOKEN_VANCED"],
  "act_1286632473622244": ["META_TOKEN_VANCED"],
};

// 계정에 쓸 토큰을 고른다. envName 은 오류 메시지에 쓰이므로 미설정일 때는 후보 전체를 보여준다.
function tokenFor(acc: string): { envName: string; token: string } | null {
  const names = ACC_TOKEN_ENV[acc];
  if (!names) return null;
  for (const n of names) {
    const v = Deno.env.get(n) || "";
    if (v) return { envName: n, token: v };
  }
  return { envName: names.join(" / "), token: "" };
}

// index.html 의 HL_CONFIG 와 같은 값. null = 예산 변경 없음.
const TAG_PCT: Record<string, number | null> = {
  up20: 20,
  up10: 10,
  down10: -10,
  down20: -20,
  off: null, // 세트를 PAUSED 로
  watch: null, // '복증' — 지켜보기, 아무것도 하지 않음
};

// index.html HL_CONFIG 의 label 과 같은 값 (재적용 안내 문구용)
const TAG_LABEL: Record<string, string> = {
  up20: "+20%", up10: "+10%", down10: "-10%", down20: "-20%", off: "OFF", watch: "복증",
};

// 마킹이 저장된 하이라이트 테이블 (index.html hlTbl() 과 동일)
const HL_TBL: Record<string, { tbl: string; col: string }> = {
  kr: { tbl: "adset_highlights", col: "adset_id" },
  gl: { tbl: "global_adset_highlights", col: "adset_id" },
  vn: { tbl: "vanced_adset_highlights", col: "adset_id" },
};

// 최소통화단위 → 표시금액 배수. 표시용일 뿐이고 ±% 계산은 단위와 무관하다.
const CCY_OFFSET: Record<string, number> = {
  KRW: 1, JPY: 1, VND: 1,
  USD: 100, TWD: 100, HKD: 100, EUR: 100, SGD: 100, THB: 100, MXN: 100, GBP: 100,
};

const MAX_ITEMS = 200;

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
// SUPABASE_SERVICE_ROLE_KEY 는 런타임이 자동 주입하는 레거시 JWT 키다.
// 프로젝트가 새 키 형식(sb_secret_…)으로 넘어가 레거시가 비활성화되면 이게 무효해지고,
// 증상이 '로그인이 필요합니다'(401) 로만 보여 원인을 찾기 어렵다 → SB_SECRET_KEY 로 덮어쓸 수 있게 둔다.
const SERVICE_KEY = Deno.env.get("SB_SECRET_KEY") || Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const DB_SCHEMA = "new-tightauto";

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

// ── Supabase 헬퍼 ──────────────────────────────────────────────
async function getUser(jwt: string) {
  const r = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
    headers: { Authorization: `Bearer ${jwt}`, apikey: SERVICE_KEY },
  });
  if (!r.ok) return null;
  return await r.json();
}

async function sbSelect(table: string, query: string) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${query}`, {
    headers: {
      apikey: SERVICE_KEY,
      Authorization: `Bearer ${SERVICE_KEY}`,
      "Accept-Profile": DB_SCHEMA,
    },
  });
  if (!r.ok) return [];
  return await r.json();
}

async function sbInsert(table: string, rows: unknown[]) {
  if (!rows.length) return;
  await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: "POST",
    headers: {
      apikey: SERVICE_KEY,
      Authorization: `Bearer ${SERVICE_KEY}`,
      "Content-Type": "application/json",
      "Content-Profile": DB_SCHEMA,
      Prefer: "return=minimal",
    },
    body: JSON.stringify(rows),
  }).catch(() => {});
}

// ── Meta 헬퍼 ─────────────────────────────────────────────────
function metaErr(j: any): string {
  const e = j?.error;
  if (!e) return "";
  return e.error_user_msg || e.message || JSON.stringify(e);
}

async function metaGet(path: string, fields: string, token: string) {
  const u = `${GRAPH}/${path}?fields=${encodeURIComponent(fields)}&access_token=${encodeURIComponent(token)}`;
  const r = await fetch(u);
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(metaErr(j) || `Meta GET ${r.status}`);
  return j;
}

async function metaPost(path: string, body: Record<string, string>, token: string) {
  const form = new URLSearchParams({ ...body, access_token: token });
  const r = await fetch(`${GRAPH}/${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: form.toString(),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(metaErr(j) || `Meta POST ${r.status}`);
  return j;
}

const ccyCache = new Map<string, string>();
async function accCurrency(acc: string, token: string): Promise<string> {
  if (ccyCache.has(acc)) return ccyCache.get(acc)!;
  let cur = "";
  try {
    const j = await metaGet(acc, "currency", token);
    cur = String(j.currency || "");
  } catch { /* 통화는 표시용 — 실패해도 진행 */ }
  ccyCache.set(acc, cur);
  return cur;
}

// ── 계획 수립 ─────────────────────────────────────────────────
type Plan = {
  adset_id: string;
  adset_name: string;
  ad_account_id: string;
  tag: string;
  pct: number | null;
  scope: "adset" | "campaign" | "";
  target_id: string;
  field: string;
  currency: string;
  offset: number;
  before: string;
  after: string;
  note: string;
  error: string;
  applied?: boolean;
  conflict?: boolean; // 같은 CBO 캠페인에 다른 증감률 — 선택을 하나로 줄이면 해소된다
  shared_with?: string[]; // 같은 CBO 캠페인을 공유하는 다른 세트
  redo?: boolean; // 오늘 이미 적용된 세트 — 또 적용하면 ±% 가 복리로 걸린다
};

// 오늘(KST) 이미 성공적으로 적용된 세트. 하이라이트는 남아 있고 서버는 메타의 '현재' 예산으로
// ±% 를 다시 계산하므로, 두 번 누르면 50만→55만→60.5만처럼 복리가 된다. 그래서 계획 단계에서
// 로그를 읽어 표시하고, 클라이언트가 기본 해제·전체선택 제외로 다룬다(체크하면 재적용 가능).
// 기준일을 KST 로 잡는 이유: 하이라이트를 지우는 cron 도 KST 자정이다.
async function fetchDoneToday(mode: string): Promise<Record<string, any>> {
  const kst = new Date(Date.now() + 9 * 3600 * 1000);
  const from = new Date(
    Date.UTC(kst.getUTCFullYear(), kst.getUTCMonth(), kst.getUTCDate()) - 9 * 3600 * 1000,
  ).toISOString();
  const rows: any[] = await sbSelect(
    "budget_apply_log",
    `select=adset_id,tag,before_value,after_value,applied_at&region=eq.${encodeURIComponent(mode)}` +
      `&ok=is.true&applied_at=gte.${encodeURIComponent(from)}&order=applied_at.desc`,
  );
  const m: Record<string, any> = {};
  for (const r of rows) {
    const k = String(r?.adset_id ?? "");
    if (k && !m[k]) m[k] = r; // 같은 세트가 여러 건이면 가장 최근 것
  }
  return m;
}

// 적용 이력을 사람이 읽는 한 줄로. 시각은 KST.
function redoNote(r: any): string {
  const t = new Date(r.applied_at);
  const hhmm = isNaN(t.getTime())
    ? ""
    : new Date(t.getTime() + 9 * 3600 * 1000).toISOString().slice(11, 16);
  const label = TAG_LABEL[String(r.tag)] || String(r.tag || "");
  const move = r.before_value && r.after_value ? ` ${r.before_value}→${r.after_value}` : "";
  return `오늘 ${hhmm} 이미 적용됨 (${label}${move})`;
}

function blank(item: any, err: string): Plan {
  return {
    adset_id: String(item?.adset_id ?? ""),
    adset_name: "",
    ad_account_id: String(item?.ad_account_id ?? ""),
    tag: String(item?.tag ?? ""),
    pct: null, scope: "", target_id: "", field: "",
    currency: "", offset: 1, before: "", after: "",
    note: "", error: err,
  };
}

async function planOne(
  item: any,
  hlMap: Record<string, string>,
  doneMap: Record<string, any>,
): Promise<Plan> {
  const id = String(item?.adset_id ?? "").trim();
  const acc = String(item?.ad_account_id ?? "").trim();
  const tag = String(item?.tag ?? "").trim();

  // 숫자 ID 여부만 본다. 길이로는 구글 디멘드젠 ad_group_id 를 걸러낼 수 없다 —
  // 실측(2026-07-30) 메타 세트 13·14·18자리 / 구글 ad_group 12·16자리로 구간이 섞인다.
  // 실제 방어는 아래 metaGet 의 account_id 대조(요청한 계정의 세트인지)가 한다.
  if (!/^\d{9,}$/.test(id)) return blank(item, "메타 광고세트 ID 형식이 아님");
  if (!(tag in TAG_PCT)) return blank(item, `알 수 없는 마킹: ${tag}`);
  const sel = tokenFor(acc);
  if (!sel) return blank(item, `등록되지 않은 광고계정: ${acc || "(없음)"}`);
  const token = sel.token;
  if (!token) return blank(item, `토큰 미설정: ${sel.envName}`);

  // 대시보드 표시와 DB 마킹이 어긋난 채로(새로고침 전 낡은 화면) 적용되는 것을 막는다.
  if ((hlMap[id] || "") !== tag) {
    return blank(item, `마킹 불일치 (DB=${hlMap[id] || "없음"}) — 새로고침 후 재시도`);
  }

  const p = blank(item, "");
  p.tag = tag;
  p.pct = TAG_PCT[tag];

  try {
    const a = await metaGet(
      id,
      "id,name,status,effective_status,account_id,daily_budget,lifetime_budget,campaign_id," +
        "campaign{id,name,status,daily_budget,lifetime_budget}",
      token,
    );
    // 브라우저가 보낸 계정과 실제 소유 계정이 다르면 중단한다. 엉뚱한 객체(구글 ad_group_id,
    // 다른 계정 세트)를 이 계정 토큰으로 수정하는 경로를 여기서 끊는다.
    const owner = a.account_id ? `act_${a.account_id}` : "";
    if (owner && owner !== acc) {
      return blank(item, `세트가 ${owner} 소속인데 ${acc} 로 요청됨 — 새로고침 후 재시도`);
    }
    p.adset_name = String(a.name || "");
    p.currency = await accCurrency(acc, token);
    p.offset = CCY_OFFSET[p.currency] ?? 1;

    // OFF → 세트만 PAUSED. 캠페인은 절대 건드리지 않는다(다른 세트 동반 중단 방지).
    if (tag === "off") {
      p.scope = "adset";
      p.target_id = id;
      p.field = "status";
      p.before = String(a.status || "");
      p.after = "PAUSED";
      if (String(a.status) === "PAUSED") {
        p.note = "이미 중단됨 — 변경 없음";
        p.after = p.before;
      }
      return p;
    }

    if (tag === "watch") {
      p.note = "복증 — 예산 변경 없음";
      return p;
    }

    // 예산이 실제로 붙어있는 객체를 찾는다. 세트 우선, 없으면 캠페인(CBO).
    const camp = a.campaign || {};
    const cand: Array<[("adset" | "campaign"), string, string, number]> = [
      ["adset", id, "daily_budget", Number(a.daily_budget || 0)],
      ["adset", id, "lifetime_budget", Number(a.lifetime_budget || 0)],
      ["campaign", String(camp.id || a.campaign_id || ""), "daily_budget", Number(camp.daily_budget || 0)],
      ["campaign", String(camp.id || a.campaign_id || ""), "lifetime_budget", Number(camp.lifetime_budget || 0)],
    ];
    const hit = cand.find(([, tid, , v]) => v > 0 && tid);
    if (!hit) {
      p.error = "예산이 설정된 세트·캠페인을 찾지 못함";
      return p;
    }
    const [scope, targetId, field, cur] = hit;
    p.scope = scope;
    p.target_id = targetId;
    p.field = field;
    p.before = String(cur);

    const next = Math.round(cur * (1 + (p.pct as number) / 100));
    if (next <= 0) {
      p.error = "변경 후 예산이 0 이하";
      return p;
    }
    p.after = String(next);
    if (next === cur) p.note = "반올림 결과 동일 — 변경 없음";
    if (scope === "campaign") p.note = (p.note ? p.note + " / " : "") + "CBO 캠페인 예산 — 하위 세트 전체에 영향";
  } catch (e) {
    p.error = String((e as Error).message || e).slice(0, 400);
  }

  // 오늘 이미 적용된 세트는 계획은 그대로 내주고 표시만 남긴다 — 막지 않고 사람이 고르게 한다.
  const done = doneMap[id];
  if (done && !p.error) {
    p.redo = true;
    p.note = (p.note ? p.note + " / " : "") + redoNote(done);
  }
  return p;
}

// 같은 CBO 캠페인을 두 세트가 서로 다른 비율로 가리키면 어느 쪽을 따를지 알 수 없다 → 둘 다 막는다.
// 같은 비율이면 한 번만 쓰면 되므로 중복은 접는다.
function resolveCampaignConflicts(plans: Plan[]) {
  const byCamp = new Map<string, Plan[]>();
  for (const p of plans) {
    if (p.error || p.scope !== "campaign" || !p.target_id) continue;
    const arr = byCamp.get(p.target_id) || [];
    arr.push(p);
    byCamp.set(p.target_id, arr);
  }
  for (const [cid, arr] of byCamp) {
    if (arr.length < 2) continue;
    const tags = new Set(arr.map((p) => p.tag));
    if (tags.size > 1) {
      // 선택에서 한쪽을 빼고 다시 요청하면 충돌이 사라진다 → conflict 로 표시해 UI 가 체크는 허용하게 한다
      for (const p of arr) {
        p.conflict = true;
        p.error = `같은 CBO 캠페인(${cid})에 서로 다른 증감률 (${[...tags].join(", ")}) — 한쪽만 선택하면 적용됩니다`;
      }
    } else {
      // 동일 비율: 첫 항목만 실제로 쓰고 나머지는 그 결과를 공유
      arr.slice(1).forEach((p) => {
        p.note = (p.note ? p.note + " / " : "") + "같은 CBO 캠페인 — 첫 세트와 함께 1회만 적용";
        p.field = "";
      });
      arr[0].shared_with = arr.slice(1).map((p) => p.adset_id);
    }
  }
}

// ── 엔트리 ────────────────────────────────────────────────────
Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });
  if (req.method !== "POST") return json({ ok: false, error: "POST only" }, 405);

  const jwt = (req.headers.get("Authorization") || "").replace(/^Bearer\s+/i, "");
  if (!jwt) return json({ ok: false, error: "인증 없음" }, 401);
  const user = await getUser(jwt);
  if (!user?.id) return json({ ok: false, error: "로그인이 필요합니다" }, 401);

  let body: any;
  try {
    body = await req.json();
  } catch {
    return json({ ok: false, error: "JSON 파싱 실패" }, 400);
  }

  const mode = String(body?.mode || "");
  const dryRun = body?.dryRun !== false; // 기본은 안전한 dry-run
  const items = Array.isArray(body?.items) ? body.items : [];

  if (!(mode in HL_TBL)) {
    return json({ ok: false, error: "예산 적용은 국내(kr)·글로벌(gl)·밴스드(vn) 세트에서만 가능합니다" }, 400);
  }
  if (!items.length) return json({ ok: false, error: "적용할 마킹이 없습니다" }, 400);
  if (items.length > MAX_ITEMS) return json({ ok: false, error: `한 번에 ${MAX_ITEMS}개까지만 적용할 수 있습니다` }, 400);

  // DB 마킹 스냅샷 — 브라우저가 보낸 tag 와 대조
  const { tbl, col } = HL_TBL[mode];
  const hlRows: any[] = await sbSelect(tbl, `select=${col},highlight`);
  const hlMap: Record<string, string> = {};
  for (const r of hlRows) if (r?.[col]) hlMap[String(r[col])] = String(r.highlight ?? "");

  // 중복 세트 제거
  const seen = new Set<string>();
  const uniq = items.filter((it: any) => {
    const k = String(it?.adset_id ?? "");
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });

  const doneMap = await fetchDoneToday(mode);

  const plans: Plan[] = [];
  for (const it of uniq) plans.push(await planOne(it, hlMap, doneMap));
  resolveCampaignConflicts(plans);

  if (dryRun) return json({ ok: true, dryRun: true, actor: user.email || "", plan: plans });

  // ── 실제 적용 ──
  const logs: any[] = [];
  for (const p of plans) {
    if (p.error || !p.field || p.after === p.before || !p.target_id) {
      p.applied = false; // 공유 세트(shared_with)의 결과는 루프가 끝난 뒤 덮어쓴다
      continue;
    }
    const token = tokenFor(p.ad_account_id)?.token || "";
    try {
      await metaPost(p.target_id, { [p.field]: p.after }, token);
      p.applied = true;
    } catch (e) {
      p.applied = false;
      p.error = String((e as Error).message || e).slice(0, 400);
    }
    logs.push({
      actor: user.email || user.id,
      region: mode,
      adset_id: p.adset_id,
      adset_name: p.adset_name,
      ad_account_id: p.ad_account_id,
      tag: p.tag,
      scope: p.scope,
      target_id: p.target_id,
      field: p.field,
      before_value: p.before,
      after_value: p.after,
      currency: p.currency,
      ok: !!p.applied,
      error: p.error || null,
    });
  }

  // 같은 CBO 캠페인을 공유한 세트들에 결과를 물려준다.
  // 루프 안에서 하면 아직 순회하지 않은 공유 세트가 스킵 분기에서 applied=false 로 되덮인다.
  for (const p of plans) {
    if (!p.shared_with?.length) continue;
    for (const sid of p.shared_with) {
      const q = plans.find((x) => x.adset_id === sid);
      if (!q) continue;
      q.applied = p.applied;
      if (!p.applied && p.error) q.error = p.error;
    }
  }
  await sbInsert("budget_apply_log", logs);

  const okN = plans.filter((p) => p.applied).length;
  const errN = plans.filter((p) => p.error).length;
  return json({ ok: true, dryRun: false, actor: user.email || "", applied: okN, failed: errN, plan: plans });
});
