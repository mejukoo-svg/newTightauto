// 구글 디멘드젠 탭의 증감액 마킹(+20/+10/-10/-20/OFF)을 Google Ads 에 실제로 반영한다.
//
// 메타용 apply-budget 과 같은 계약: POST { mode:'gd', dryRun, items:[{adset_id, tag}] }
//   → { ok, dryRun, actor, plan:[...] }  (dryRun 이면 계획만, 아니면 각 항목에 applied/error)
//
// 메타와 다른 점 (2026-07-31 실측):
//   · 구글에는 광고그룹 예산이 없다 → ±% 는 항상 '캠페인 예산'을 바꾼다. OFF 만 광고그룹 상태.
//   · 디멘드젠 예산은 전부 비공유(explicitly_shared=false) 이고 활성 캠페인당 활성 광고그룹이
//     1개라 충돌이 사실상 없지만, 같은 캠페인에 서로 다른 증감률이 걸리는 경우는 그대로 막는다.
//   · 금액 단위는 micros (1원 = 1,000,000). 계획/로그는 사람이 읽는 '원' 으로 두고
//     mutate 직전에만 micros 로 바꾼다.
//
// 배포: README.md 참고.

// v21 은 2026-08 현재 sunset 진행 중 — 요청의 절반쯤이 "Version v21 is deprecated" 로 막힌다.
// v24 는 파이프라인의 google-ads 파이썬 라이브러리 상한과 같은 버전이다(스택 전체가 한 버전을 본다).
const API = "v24";
const ADS = `https://googleads.googleapis.com/${API}`;

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_KEY = Deno.env.get("SB_SECRET_KEY") || Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
const DB_SCHEMA = "new-tightauto";
const REGION = "gd"; // budget_apply_log.region — 메타(kr/gl/vn)와 구분

const G_DEV = Deno.env.get("G_ADS_DEV_TOKEN") || "";
const G_CID = Deno.env.get("G_ADS_CLIENT_ID") || "";
const G_SECRET = Deno.env.get("G_ADS_CLIENT_SECRET") || "";
const G_REFRESH = Deno.env.get("G_ADS_REFRESH_TOKEN") || "";
const digits = (s: string) => String(s || "").replace(/\D/g, "");
const G_LOGIN = digits(Deno.env.get("G_ADS_LOGIN_ID") || ""); // MCC
const G_CUST = digits(Deno.env.get("G_ADS_CUSTOMER_ID") || "") || "5912047700";

const MAX_ITEMS = 200;

// index.html HL_CONFIG 와 같은 값. null = 예산 변경 없음.
const TAG_PCT: Record<string, number | null> = {
  up20: 20, up10: 10, down10: -10, down20: -20, down50: -50,
  off: null, // 광고그룹을 PAUSED 로
  watch: null, // '복증' — 아무것도 하지 않음
};
const TAG_LABEL: Record<string, string> = {
  up20: "+20%", up10: "+10%", down10: "-10%", down20: "-20%", down50: "-50%", off: "OFF", watch: "복증",
};

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

// ── Supabase ──────────────────────────────────────────────────
async function getUser(jwt: string) {
  const r = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
    headers: { Authorization: `Bearer ${jwt}`, apikey: SERVICE_KEY },
  });
  if (!r.ok) return null;
  return await r.json();
}

async function sbSelect(table: string, query: string) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${query}`, {
    headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}`, "Accept-Profile": DB_SCHEMA },
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

// ── Google Ads ────────────────────────────────────────────────
// access_token 은 1시간짜리라 호출 때마다 refresh_token 으로 새로 받는다.
async function accessToken(): Promise<string> {
  const r = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: G_CID, client_secret: G_SECRET,
      refresh_token: G_REFRESH, grant_type: "refresh_token",
    }).toString(),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j.access_token) {
    throw new Error(`구글 인증 실패: ${j.error_description || j.error || r.status}`);
  }
  return j.access_token as string;
}

function gErr(j: any): string {
  const e = j?.error;
  const d = e?.details?.[0]?.errors?.[0];
  return d?.message || e?.message || JSON.stringify(j).slice(0, 200);
}

async function gPost(path: string, body: unknown, token: string) {
  const r = await fetch(`${ADS}/customers/${G_CUST}/${path}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "developer-token": G_DEV,
      "login-customer-id": G_LOGIN,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(gErr(j));
  return j;
}

const search = (query: string, token: string) => gPost("googleAds:search", { query }, token);

// ── 계획 ──────────────────────────────────────────────────────
type Plan = {
  adset_id: string; // 광고그룹 id (대시보드 마킹 키)
  adset_name: string;
  ad_account_id: string;
  campaign_id: string;
  campaign_name: string;
  tag: string;
  pct: number | null;
  scope: "adgroup" | "campaign" | "";
  target_id: string; // 실제로 고칠 객체의 resourceName
  field: string; // daily_budget | status
  currency: string;
  offset: number;
  before: string; // 예산은 '원', 상태는 문자열
  after: string;
  note: string;
  error: string;
  applied?: boolean;
  conflict?: boolean;
  redo?: boolean;
  shared_with?: string[];
};

function blank(item: any, err: string): Plan {
  return {
    adset_id: String(item?.adset_id ?? ""), adset_name: "", ad_account_id: G_CUST,
    campaign_id: "", campaign_name: "", tag: String(item?.tag ?? ""), pct: null,
    scope: "", target_id: "", field: "", currency: "KRW", offset: 1,
    before: "", after: "", note: "", error: err,
  };
}

// 오늘(KST) 이미 적용된 광고그룹 — 또 적용하면 ±% 가 복리로 걸린다. 막지 않고 표시만 한다.
async function fetchDoneToday(): Promise<Record<string, any>> {
  const kst = new Date(Date.now() + 9 * 3600 * 1000);
  const from = new Date(
    Date.UTC(kst.getUTCFullYear(), kst.getUTCMonth(), kst.getUTCDate()) - 9 * 3600 * 1000,
  ).toISOString();
  const rows: any[] = await sbSelect(
    "budget_apply_log",
    `select=adset_id,tag,before_value,after_value,applied_at&region=eq.${REGION}` +
      `&ok=is.true&applied_at=gte.${encodeURIComponent(from)}&order=applied_at.desc`,
  );
  const m: Record<string, any> = {};
  for (const r of rows) {
    const k = String(r?.adset_id ?? "");
    if (k && !m[k]) m[k] = r;
  }
  return m;
}

function redoNote(r: any): string {
  const t = new Date(r.applied_at);
  const hhmm = isNaN(t.getTime()) ? ""
    : new Date(t.getTime() + 9 * 3600 * 1000).toISOString().slice(11, 16);
  const label = TAG_LABEL[String(r.tag)] || String(r.tag || "");
  const move = r.before_value && r.after_value ? ` ${r.before_value}→${r.after_value}` : "";
  return `오늘 ${hhmm} 이미 적용됨 (${label}${move})`;
}

// 같은 캠페인에 서로 다른 증감률이 걸리면 어느 쪽을 따를지 알 수 없다 → 둘 다 막는다.
// 같은 비율이면 캠페인 예산을 한 번만 쓰고 결과를 공유한다.
function resolveCampaignConflicts(plans: Plan[]) {
  const byCamp = new Map<string, Plan[]>();
  for (const p of plans) {
    if (p.error || p.scope !== "campaign" || !p.target_id) continue;
    const arr = byCamp.get(p.target_id) || [];
    arr.push(p);
    byCamp.set(p.target_id, arr);
  }
  for (const [rn, arr] of byCamp) {
    if (arr.length < 2) continue;
    const tags = new Set(arr.map((p) => p.tag));
    if (tags.size > 1) {
      for (const p of arr) {
        p.conflict = true;
        p.error = `같은 캠페인(${p.campaign_name || rn})에 서로 다른 증감률 (${[...tags].join(", ")}) — 한쪽만 선택하면 적용됩니다`;
      }
    } else {
      arr.slice(1).forEach((p) => {
        p.note = (p.note ? p.note + " / " : "") + "같은 캠페인 — 첫 광고그룹과 함께 1회만 적용";
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

  const dryRun = body?.dryRun !== false; // 기본은 안전한 dry-run
  const items = Array.isArray(body?.items) ? body.items : [];
  if (!items.length) return json({ ok: false, error: "적용할 마킹이 없습니다" }, 400);
  if (items.length > MAX_ITEMS) {
    return json({ ok: false, error: `한 번에 ${MAX_ITEMS}개까지만 적용할 수 있습니다` }, 400);
  }
  for (const k of ["G_ADS_DEV_TOKEN", "G_ADS_CLIENT_ID", "G_ADS_CLIENT_SECRET", "G_ADS_REFRESH_TOKEN", "G_ADS_LOGIN_ID"]) {
    if (!Deno.env.get(k)) return json({ ok: false, error: `구글 자격증명 미설정: ${k}` }, 500);
  }

  // 구글 광고그룹 마킹도 메타와 같은 adset_highlights 에 들어간다 (키 = ad_group_id)
  const hlRows: any[] = await sbSelect("adset_highlights", "select=adset_id,highlight");
  const hlMap: Record<string, string> = {};
  for (const r of hlRows) if (r?.adset_id) hlMap[String(r.adset_id)] = String(r.highlight ?? "");

  const seen = new Set<string>();
  const uniq = items.filter((it: any) => {
    const k = String(it?.adset_id ?? "");
    if (!k || seen.has(k)) return false;
    seen.add(k);
    return true;
  });

  // 화면에 보이던 순서(지출 내림차순) 그대로 돌려준다 → plans 는 uniq 와 인덱스가 일치한다.
  const plans: Plan[] = [];
  const ok: { idx: number; item: any; id: string }[] = [];
  for (const it of uniq) {
    const idx = plans.length;
    const id = String(it?.adset_id ?? "").trim();
    const tag = String(it?.tag ?? "").trim();
    if (!/^\d+$/.test(id)) { plans.push(blank(it, "광고그룹 ID 형식이 아님")); continue; }
    if (!(tag in TAG_PCT)) { plans.push(blank(it, `알 수 없는 마킹: ${tag}`)); continue; }
    // 낡은 화면에서의 오적용 차단 — 브라우저가 보낸 tag 를 DB 와 대조
    if ((hlMap[id] || "") !== tag) {
      plans.push(blank(it, `마킹 불일치 (DB=${hlMap[id] || "없음"}) — 새로고침 후 재시도`));
      continue;
    }
    plans.push(blank(it, "")); // 자리만 잡아두고 구글 조회 후 채운다
    ok.push({ idx, item: it, id });
  }

  if (ok.length) {
    try {
      const token = await accessToken();
      const ids = ok.map((o) => o.id).join(",");
      // 광고그룹 → 캠페인
      const ag = await search(
        `SELECT ad_group.id, ad_group.name, ad_group.status, ad_group.resource_name, ` +
        `campaign.id, campaign.name, campaign.status ` +
        `FROM ad_group WHERE ad_group.id IN (${ids})`, token);
      const agMap: Record<string, any> = {};
      for (const r of (ag.results || [])) agMap[String(r.adGroup.id)] = r;

      // 캠페인 → 예산 (campaign_budget 은 FROM campaign 에서만 뽑힌다)
      const campIds = [...new Set((ag.results || []).map((r: any) => String(r.campaign.id)))];
      const budMap: Record<string, any> = {};
      if (campIds.length) {
        const cb = await search(
          `SELECT campaign.id, campaign_budget.resource_name, campaign_budget.amount_micros, ` +
          `campaign_budget.explicitly_shared, campaign_budget.reference_count ` +
          `FROM campaign WHERE campaign.id IN (${campIds.join(",")})`, token);
        for (const r of (cb.results || [])) budMap[String(r.campaign.id)] = r.campaignBudget;
      }

      for (const { idx, item, id } of ok) {
        const p = blank(item, "");
        plans[idx] = p;
        p.tag = String(item.tag);
        p.pct = TAG_PCT[p.tag];
        const row = agMap[id];
        if (!row) {
          p.error = `구글 계정(${G_CUST})에서 광고그룹을 찾지 못함`;
          continue;
        }
        p.adset_name = String(row.adGroup?.name || "");
        p.campaign_id = String(row.campaign?.id || "");
        p.campaign_name = String(row.campaign?.name || "");

        // OFF → 광고그룹만 일시중지. 캠페인은 건드리지 않는다(다른 광고그룹 동반 중단 방지).
        if (p.tag === "off") {
          p.scope = "adgroup";
          p.target_id = String(row.adGroup?.resourceName || "");
          p.field = "status";
          p.before = String(row.adGroup?.status || "");
          p.after = "PAUSED";
          if (p.before === "PAUSED") { p.note = "이미 중단됨 — 변경 없음"; p.after = p.before; }
          continue;
        }
        if (p.tag === "watch") { p.note = "복증 — 예산 변경 없음"; continue; }

        const bud = budMap[p.campaign_id];
        const micros = Number(bud?.amountMicros || 0);
        if (!bud?.resourceName || !(micros > 0)) {
          p.error = "캠페인 예산을 찾지 못함";
          continue;
        }
        p.scope = "campaign";
        p.target_id = String(bud.resourceName);
        p.field = "daily_budget";
        const won = Math.round(micros / 1_000_000);
        const next = Math.round(won * (1 + (p.pct as number) / 100));
        p.before = String(won);
        p.after = String(next);
        if (next <= 0) { p.error = "변경 후 예산이 0 이하"; continue; }
        if (next === won) p.note = "반올림 결과 동일 — 변경 없음";
        // 구글은 광고그룹 예산이 없다 — ±% 는 반드시 캠페인 예산을 건드린다.
        p.note = (p.note ? p.note + " / " : "") + "캠페인 예산 — 하위 광고그룹 전체에 영향";
        if (bud.explicitlyShared) {
          p.note += " / ⚠ 공유 예산(다른 캠페인과 공유)";
        }
      }
    } catch (e) {
      // 조회 단계에서 통째로 실패 — 자리를 잡아둔 행에만 같은 오류를 채운다
      const msg = String((e as Error).message || e).slice(0, 400);
      for (const { idx, item } of ok) plans[idx] = blank(item, msg);
    }
  }

  resolveCampaignConflicts(plans);

  const doneMap = await fetchDoneToday();
  for (const p of plans) {
    const done = doneMap[p.adset_id];
    if (done && !p.error) {
      p.redo = true;
      p.note = (p.note ? p.note + " / " : "") + redoNote(done);
    }
  }

  if (dryRun) return json({ ok: true, dryRun: true, actor: user.email || "", plan: plans });

  // ── 실제 적용 ──
  const token = await accessToken();
  const logs: any[] = [];
  for (const p of plans) {
    if (p.error || !p.field || p.after === p.before || !p.target_id) {
      p.applied = false; // 공유 캠페인 결과는 루프가 끝난 뒤 물려준다
      continue;
    }
    try {
      if (p.field === "status") {
        await gPost("adGroups:mutate", {
          operations: [{ update: { resourceName: p.target_id, status: p.after }, updateMask: "status" }],
        }, token);
      } else {
        await gPost("campaignBudgets:mutate", {
          operations: [{
            update: { resourceName: p.target_id, amountMicros: String(Number(p.after) * 1_000_000) },
            updateMask: "amount_micros",
          }],
        }, token);
      }
      p.applied = true;
    } catch (e) {
      p.applied = false;
      p.error = String((e as Error).message || e).slice(0, 400);
    }
    logs.push({
      actor: user.email || user.id,
      region: REGION,
      adset_id: p.adset_id,
      adset_name: p.adset_name,
      ad_account_id: G_CUST,
      tag: p.tag,
      scope: p.scope,
      target_id: p.target_id,
      field: p.field,
      before_value: p.before,
      after_value: p.after,
      currency: "KRW",
      ok: !!p.applied,
      error: p.error || null,
    });
  }

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
