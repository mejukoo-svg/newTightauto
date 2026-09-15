// 소재별 탭 'ASC' 마킹 → 같은 상품의 모든 ASC 세트에 소재(광고) 복사
//
// 왜 Edge Function 인가: apply-budget 과 같은 이유. index.html 은 공개 소스라 Meta 쓰기 토큰을
// 브라우저에 둘 수 없다. 토큰은 Edge Secret 에만 있고 브라우저는 "어느 소재를" 만 보낸다.
//
// 동작
//   1) 로그인 JWT 검증 → ad_creative_highlights 의 현재 마킹이 'asc' 인지 대조(낡은 화면 방지)
//   2) 원본 광고를 메타에서 읽어 소속 계정 대조 + 캠페인명에서 상품명 추출
//      (파이프라인 국내_소재별_supabase.py 의 extract_product 와 같은 규칙)
//   3) 같은 계정의 ASC 캠페인(이름에 'ASC' — smart_promotion_type 은 우리 계정에서 구분이 안 됨) 중
//      상품명이 같은 것 → 그 하위 세트 전부가 대상
//   4) 대상 세트에 같은 소재가 이미 있으면(creative id / story id / video id / image hash 일치) 건너뜀
//      + asc_copy_log 에 성공 기록이 있으면 건너뜀
//   5) dryRun=false 면 POST /{ad_id}/copies {adset_id, status_option} → asc_copy_log 기록
//      ※ 이동이 아니라 복사다. 원본 광고·세트는 건드리지 않는다.
//      ※ 중단(PAUSED)된 ASC 세트에도 넣는다. 캠페인·세트의 status 는 읽기만 하고 절대 바꾸지 않는다 —
//        꺼진 ASC 는 꺼진 채로 두고, 나중에 사람이 켜면 들어가 있던 소재가 같이 돈다.
//
// 요청: POST { mode:'cr', region:'kr'|'gl', dryRun:boolean, items:[{ad_id, ad_account_id}], select?:["<ad_id>|<adset_id>",…] }
//   region: 상품 매칭 규칙 선택. kr = 캠페인명 첫 토큰(국내_소재별 extract_product), gl = 국가+상품(글로벌 canon, 아래 glKey)
// 응답: { ok, dryRun, plan:[{ad_id, ad_name, product, targets:[{adset_id, action, note, error, applied, copied_ad_id}]}] }
//
// 배포: Edge Function 은 git push 로 배포되지 않는다 — apply-budget/README.md 의 절차대로 따로 배포할 것.

const META_API_VERSION = "v21.0";
const GRAPH = `https://graph.facebook.com/${META_API_VERSION}`;

// 광고계정 → 토큰 환경변수명. apply-budget/index.ts 의 ACC_TOKEN_ENV 와 동일하게 유지할 것.
// 소재 복사(POST /ads, /copies)는 예산 수정과 같은 ads_management + 계정 ADVERTISE 권한을 쓴다.
const ACC_TOKEN_ENV: Record<string, string[]> = {
  // 국내
  "act_1270614404675034": ["META_TOKEN_1"],
  "act_707835224206178": ["META_TOKEN_1"],
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

function tokenFor(acc: string): { envName: string; token: string } | null {
  const names = ACC_TOKEN_ENV[acc];
  if (!names) return null;
  for (const n of names) {
    const v = Deno.env.get(n) || "";
    if (v) return { envName: n, token: v };
  }
  return { envName: names.join(" / "), token: "" };
}

// 마킹이 저장된 하이라이트 테이블. 국내·글로벌 소재 모두 ad_creative_highlights(ad_id 는 계정을 넘어 유일) 를 쓴다.
const HL_TBL: Record<string, { tbl: string; col: string }> = {
  cr: { tbl: "ad_creative_highlights", col: "ad_id" },
};
const REGIONS = new Set(["kr", "gl"]);
const HL_TAG = "asc";
// 복사된 광고의 초기 상태. ACTIVE = 바로 게재(죽은 소재를 ASC 에서 되살리는 용도라 원본이 꺼져 있어도 켠다).
const STATUS_OPTION = "ACTIVE";

const MAX_ITEMS = 50;
const MAX_TARGETS = 200;

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
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

// 요청 한도(rate limit) — "이 광고 계정에서 너무 많은 요청이 있습니다". 일시적이라 잠깐 쉬고 다시 시도한다.
//   2026-09-15 실사고: ASC 세트마다 세트·광고를 따로 조회하고 확인 시 그걸 또 반복해 계정당 수십 호출이
//   1분에 몰렸다 → 호출은 계정당 3번(캠페인·세트·광고 일괄)으로 줄이고, 그래도 걸리면 3s·6s·12s 재시도.
const RATE_CODES = new Set([4, 17, 32, 613, 80000, 80001, 80002, 80003, 80004]);
function isRateLimited(j: any): boolean {
  const e = j?.error || {};
  return RATE_CODES.has(Number(e.code)) || !!e.is_transient || /too many|너무 많은 요청/i.test(String(e.message || e.error_user_msg || ""));
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function metaFetch(url: string, init: RequestInit | undefined, label: string) {
  let last: any = {};
  for (let attempt = 0; attempt < 4; attempt++) {
    if (attempt) await sleep(3000 * 2 ** (attempt - 1));
    const r = await fetch(url, init);
    const j = await r.json().catch(() => ({}));
    if (r.ok) return j;
    last = j;
    if (!isRateLimited(j)) throw new Error(metaErr(j) || `${label} ${r.status}`);
  }
  throw new Error((metaErr(last) || `${label} 요청 한도`) + " (재시도 3회 후 실패 — 1~2분 뒤 다시 시도)");
}

async function metaGet(path: string, params: Record<string, string>, token: string) {
  const q = new URLSearchParams({ ...params, access_token: token });
  return await metaFetch(`${GRAPH}/${path}?${q.toString()}`, undefined, "Meta GET");
}

// 목록 엣지(campaigns/adsets/ads)를 paging.next 까지 전부 읽는다.
async function metaList(path: string, params: Record<string, string>, token: string): Promise<any[]> {
  const out: any[] = [];
  let j = await metaGet(path, { limit: "500", ...params }, token);
  out.push(...(j.data || []));
  let next: string = j.paging?.next || "";
  for (let i = 0; next && i < 20; i++) {
    j = await metaFetch(next, undefined, "Meta GET");
    out.push(...(j.data || []));
    next = j.paging?.next || "";
  }
  return out;
}

async function metaPost(path: string, body: Record<string, string>, token: string) {
  const form = new URLSearchParams({ ...body, access_token: token });
  return await metaFetch(`${GRAPH}/${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: form.toString(),
  }, "Meta POST");
}

// ── 상품명 추출 — 국내_소재별_supabase.py extract_product 와 같은 규칙 ──────────
// 캠페인명 → 세트명 순으로, 구분자로 쪼갠 첫 토큰(숫자만인 토큰은 건너뜀)에서 앞쪽 이모지를 벗긴 것.
//   '집착_0623_ASC(2)_찐위닝' → '집착',  '💔재회_0603_ASC_부계' → '재회'
function extractProduct(...sources: string[]): string {
  for (const src of sources) {
    if (!src) continue;
    for (let t of String(src).trim().split(/[_\s\-/|,()\[\]]+/)) {
      t = t.trim();
      if (!t || /^\d+$/.test(t)) continue;
      const cps = Array.from(t);
      let i = 0;
      for (; i < cps.length; i++) {
        const c = cps[i];
        if ((c >= "가" && c <= "힣") || (c >= "ㄱ" && c <= "ㅣ") ||
            /^[\p{L}\p{N}]$/u.test(c) || c === "." || c === "%") break;
      }
      t = cps.slice(i).join("").trim();
      if (t) return t;
    }
  }
  return "기타";
}

// ── 글로벌 상품 키 — app.js 의 GL_NON_PRODUCT_TOKENS / GL_PRODUCT_CANON 과 같은 값 ──────────
// 글로벌 캠페인명은 '{국가}_{상품}_…_ASC_…' 꼴이고 같은 상품 ASC 가 국가별로 따로 있다
// ('미국_무당_ASC_미국' / '대만_무당_ASC_전세계중국어' …). 언어가 다르므로 **국가 + 상품** 이 같아야 대상이다.
//   국가 = 첫 국가 토큰(전세계·간체 같은 광역 토큰은 실제 국가가 없을 때만 WW 로),  상품 = canonical 영문명(없으면 원 토큰)
const GL_COUNTRY: Record<string, string> = {
  "대만": "TW", "tw": "TW", "taiwan": "TW", "홍콩": "HK", "hk": "HK", "hongkong": "HK",
  "일본": "JP", "jp": "JP", "japan": "JP", "태국": "TH", "th": "TH", "thailand": "TH",
  "미국": "US", "us": "US", "usa": "US", "호주": "AU", "au": "AU", "australia": "AU",
  "싱가포르": "SG", "싱가폴": "SG", "sg": "SG", "singapore": "SG",
  "말레이시아": "MY", "my": "MY", "malaysia": "MY", "멕시코": "MX", "mx": "MX", "mexico": "MX",
  "한국": "KR", "kr": "KR", "korea": "KR", "중국": "CN", "cn": "CN", "china": "CN",
  "마카오": "MO", "mo": "MO", "macau": "MO", "베트남": "VN", "vn": "VN", "vietnam": "VN",
  "영국": "GB", "gb": "GB", "uk": "GB",
  "전세계": "WW", "worldwide": "WW", "global": "WW", "간체": "WW", "번체": "WW", "sc": "WW", "tc": "WW",
};
const GL_CANON: Record<string, string> = {
  "solo": "solo", "솔로": "solo",
  "shaman": "shaman", "무당": "shaman", "mudang": "shaman", "moodang": "shaman", "샤먼": "shaman", "범산": "shaman",
  "mzpian": "mzpian", "무녀": "mzpian",
  "possessive": "possessive", "집착": "possessive", "clinger": "possessive",
  "job": "job", "커리어": "job",
  "again": "again", "재회": "again",
  "againjami": "againjami", "재회자미두수": "againjami",
  "adult": "adult", "18금": "adult",
  "adult29": "adult29", "29금": "adult29",
  "starsun": "starsun", "별선": "starsun",
  "starsea": "starsea", "별해": "starsea",
  "money": "money", "재물운": "money",
  "gender": "gender", "home": "home", "marry": "marry", "1%": "1%",
  "desirezodiac": "desirezodiac", "shyshy": "shyshy",
  "구미호": "구미호", "속궁합": "속궁합",
};
// 상품이 될 수 없는 구조 토큰 (글로벌_소재별_supabase.py SKIP_WORDS 의 비국가 부분 + 우리 네이밍 마커)
const GL_SKIP = new Set(["asc", "cbo", "abo", "dpa", "advantage", "campaign", "adset", "ad", "ads", "set", "purchase",
  "conversion", "traffic", "v1", "v2", "v3", "v4", "v5", "test", "new", "old", "copy", "sajutight", "ttsaju", "saju", "tight",
  "asia", "broad", "interest", "lookalike", "retarget", "custom", "전환캠페인", "복제", "사본", "인플", "인플루언서",
  "troas", "tcpa", "x2", "x4", "2nd", "국내", "글로벌"]);

function stripEmoji(t: string): string {
  const cps = Array.from(t);
  let i = 0;
  for (; i < cps.length; i++) {
    const c = cps[i];
    if ((c >= "\uAC00" && c <= "\uD7A3") || (c >= "\u3131" && c <= "\u3163") || /^[\p{L}\p{N}]$/u.test(c) || c === "." || c === "%") break;
  }
  return cps.slice(i).join("").trim();
}

function glKey(...sources: string[]): { key: string; label: string } {
  for (const src of sources) {
    if (!src) continue;
    const toks = String(src).trim().split(/[_\s\-/|,()\[\]]+/).map((t) => stripEmoji(t)).filter(Boolean);
    let country = "", ww = "", product = "";
    for (const raw of toks) {
      const k = raw.toLowerCase();
      if (/^\d+$/.test(k)) continue;
      const cc = GL_COUNTRY[k];
      if (cc) {
        if (cc === "WW") { if (!ww) ww = "WW"; } else if (!country) country = cc;
        continue;
      }
      if (product) continue;
      if (GL_CANON[k]) { product = GL_CANON[k]; continue; }
      if (GL_SKIP.has(k) || k.length < 2) continue;
      product = k;
    }
    if (product) {
      const c = country || ww;
      return { key: `${c}|${product}`, label: `${c || "?"} ${product}` };
    }
  }
  return { key: "", label: "" };
}

// region 별 상품 키. kr 은 첫 토큰 규칙(extractProduct), gl 은 국가+상품.
function productKey(region: string, campaignName: string, adsetName: string): { key: string; label: string } {
  if (region === "gl") return glKey(campaignName, adsetName);
  const p = extractProduct(campaignName, adsetName);
  return p === "기타" ? { key: "", label: "" } : { key: p, label: p };
}

// 실측(2026-09-14, act_1270614404675034): 우리 ASC 캠페인 31개 전부 smart_promotion_type 이
// GUIDED_CREATION 으로 나온다(AUTOMATED_SHOPPING_ADS 아님) → 실제로는 캠페인명의 'ASC' 규칙이 판별한다.
function isAscCampaign(c: any): boolean {
  if (String(c?.smart_promotion_type || "") === "AUTOMATED_SHOPPING_ADS") return true;
  return /asc/i.test(String(c?.name || ""));
}

// 소재의 지문 — 같은 영상/이미지를 쓰는 광고를 다른 creative id 로 만들어도 잡아낸다.
const CREATIVE_FIELDS =
  "creative{id,effective_object_story_id,object_story_spec{video_data{video_id,image_hash},link_data{image_hash,child_attachments{video_id,image_hash}}},asset_feed_spec{videos{video_id},images{hash}}}";

function fingerprint(cr: any): Set<string> {
  const s = new Set<string>();
  if (!cr) return s;
  if (cr.id) s.add(`cr:${cr.id}`);
  if (cr.effective_object_story_id) s.add(`story:${cr.effective_object_story_id}`);
  const oss = cr.object_story_spec || {};
  const vd = oss.video_data || {};
  if (vd.video_id) s.add(`vid:${vd.video_id}`);
  const ld = oss.link_data || {};
  if (ld.image_hash) s.add(`img:${ld.image_hash}`);
  for (const ca of ld.child_attachments || []) {
    if (ca?.video_id) s.add(`vid:${ca.video_id}`);
    if (ca?.image_hash) s.add(`img:${ca.image_hash}`);
  }
  const afs = cr.asset_feed_spec || {};
  for (const v of afs.videos || []) if (v?.video_id) s.add(`vid:${v.video_id}`);
  for (const im of afs.images || []) if (im?.hash) s.add(`img:${im.hash}`);
  return s;
}

function overlaps(a: Set<string>, b: Set<string>): boolean {
  for (const k of a) if (b.has(k)) return true;
  return false;
}

// ── 계정별 ASC 캠페인·세트·광고 — 계정당 3호출로 일괄 조회 ──
//   ① act/campaigns  ② act/adsets (campaign.id IN ASC 캠페인들)  ③ act/ads (adset.id IN **대상 세트만**, 지문용)
//   세트·캠페인마다 따로 부르면 ASC 30개 계정에서 60+ 호출이 되고, 확인 단계의 재계획까지 합쳐 한도에 걸린다.
//   ③ 은 같은 상품의 세트만 묶어 부른다 — 계정의 ASC 세트 전부(실측 23세트·688광고)를 한 번에 달라고 하면
//   "Please reduce the amount of data" 로 거절되고, limit 100 페이징으로도 39초가 걸린다.
//   캐시는 짧은 TTL 로 모듈에 둔다 — dry-run 직후 확인을 누르면 같은 인스턴스에선 호출 0 으로 재계획한다.
//   (복사 직후 지문이 잠깐 낡을 수 있지만 asc_copy_log 대조가 같은 소재 재투입을 막는다.)
type AscAdset = {
  campaign_id: string; campaign_name: string; campaign_status: string; product: string;
  adset_id: string; adset_name: string; adset_status: string;
  fps: Set<string>[]; // 세트 안 광고들의 지문 (loadAdsFor 가 채운다)
  ad_names: string[];
  loaded?: boolean;
};
const CACHE_TTL_MS = 120_000;
const ascCache = new Map<string, { at: number; sets: AscAdset[] }>();

async function ascAdsetsOf(acc: string, token: string, region: string): Promise<AscAdset[]> {
  const ck = `${region}:${acc}`;
  const hit = ascCache.get(ck);
  if (hit && Date.now() - hit.at < CACHE_TTL_MS) return hit.sets;
  const camps = (await metaList(`${acc}/campaigns`, {
    fields: "id,name,effective_status,smart_promotion_type",
    effective_status: JSON.stringify(["ACTIVE", "PAUSED"]),
  }, token)).filter(isAscCampaign);
  const out: AscAdset[] = [];
  if (camps.length) {
    const byCamp = new Map<string, any>(camps.map((c: any) => [String(c.id), c]));
    const sets = await metaList(`${acc}/adsets`, {
      fields: "id,name,effective_status,campaign_id",
      effective_status: JSON.stringify(["ACTIVE", "PAUSED"]),
      filtering: JSON.stringify([{ field: "campaign.id", operator: "IN", value: [...byCamp.keys()] }]),
    }, token);
    for (const s of sets) {
      const c = byCamp.get(String(s.campaign_id));
      if (!c) continue;
      out.push({
        campaign_id: String(c.id), campaign_name: String(c.name || ""),
        campaign_status: String(c.effective_status || ""), product: productKey(region, String(c.name || ""), "").key,
        adset_id: String(s.id), adset_name: String(s.name || ""), adset_status: String(s.effective_status || ""),
        fps: [], ad_names: [],
      });
    }
  }
  ascCache.set(ck, { at: Date.now(), sets: out });
  return out;
}

// 대상 세트들의 광고를 한 번에 읽어 지문을 채운다(이미 읽은 세트는 건너뜀). creative 필드가 무거워 limit 100.
async function loadAdsFor(acc: string, targets: AscAdset[], token: string) {
  const need = targets.filter((t) => !t.loaded);
  if (!need.length) return;
  // 삭제·보관된 광고는 지문 비교에서 뺀다 — 예전에 지운 소재를 다시 넣는 건 정상 동작이다.
  const ads = (await metaList(`${acc}/ads`, {
    limit: "100",
    fields: `id,name,effective_status,adset_id,${CREATIVE_FIELDS}`,
    filtering: JSON.stringify([{ field: "adset.id", operator: "IN", value: need.map((t) => t.adset_id) }]),
  }, token)).filter((a: any) => !/^(DELETED|ARCHIVED)$/.test(String(a.effective_status || "")));
  const bySet = new Map<string, AscAdset>(need.map((t) => [t.adset_id, t]));
  for (const t of need) { t.fps = []; t.ad_names = []; t.loaded = true; }
  for (const a of ads) {
    const t = bySet.get(String(a.adset_id));
    if (!t) continue;
    t.fps.push(fingerprint(a.creative));
    t.ad_names.push(String(a.name || ""));
  }
}

// ── 계획 ──────────────────────────────────────────────────────
type Target = {
  key: string; // `${ad_id}|${adset_id}`
  campaign_id: string; campaign_name: string; campaign_status: string;
  adset_id: string; adset_name: string; adset_status: string;
  action: "copy" | "skip";
  note: string; error: string;
  applied?: boolean; copied_ad_id?: string;
};
type Plan = {
  ad_id: string; ad_name: string; ad_account_id: string;
  product: string; src_campaign_name: string; src_adset_id: string; src_adset_name: string; src_status: string;
  error: string;
  targets: Target[];
};

function blank(item: any, err: string): Plan {
  return {
    ad_id: String(item?.ad_id ?? ""), ad_name: "", ad_account_id: String(item?.ad_account_id ?? ""),
    product: "", src_campaign_name: "", src_adset_id: "", src_adset_name: "", src_status: "",
    error: err, targets: [],
  };
}

// 과거 성공 기록 — (ad_id, target_adset_id) 별 최근 1건
async function fetchDone(adIds: string[]): Promise<Record<string, any>> {
  if (!adIds.length) return {};
  const inList = adIds.map((s) => `"${s}"`).join(",");
  const rows: any[] = await sbSelect(
    "asc_copy_log",
    `select=ad_id,target_adset_id,copied_ad_id,applied_at&ok=is.true&ad_id=in.(${encodeURIComponent(inList)})&order=applied_at.desc`,
  );
  const m: Record<string, any> = {};
  for (const r of rows) {
    const k = `${r.ad_id}|${r.target_adset_id}`;
    if (!m[k]) m[k] = r;
  }
  return m;
}

function kstStamp(iso: string): string {
  const t = new Date(iso);
  if (isNaN(t.getTime())) return "";
  return new Date(t.getTime() + 9 * 3600 * 1000).toISOString().slice(5, 16).replace("T", " ");
}

async function planOne(item: any, hlMap: Record<string, string>, doneMap: Record<string, any>, region: string): Promise<Plan> {
  const id = String(item?.ad_id ?? "").trim();
  const acc = String(item?.ad_account_id ?? "").trim();
  if (!/^\d{9,}$/.test(id)) return blank(item, "메타 광고 ID 형식이 아님");
  const sel = tokenFor(acc);
  if (!sel) return blank(item, `등록되지 않은 광고계정: ${acc || "(없음)"}`);
  if (!sel.token) return blank(item, `토큰 미설정: ${sel.envName}`);
  const token = sel.token;

  // 대시보드 표시와 DB 마킹이 어긋난 채로(새로고침 전 낡은 화면) 실행되는 것을 막는다.
  if ((hlMap[id] || "") !== HL_TAG) {
    return blank(item, `마킹 불일치 (DB=${hlMap[id] || "없음"}) — 새로고침 후 재시도`);
  }

  const p = blank(item, "");
  try {
    const a = await metaGet(id, {
      fields: `id,name,status,effective_status,account_id,adset{id,name},campaign{id,name,smart_promotion_type},${CREATIVE_FIELDS}`,
    }, token);
    const owner = a.account_id ? `act_${a.account_id}` : "";
    if (owner && owner !== acc) return blank(item, `광고가 ${owner} 소속인데 ${acc} 로 요청됨 — 새로고침 후 재시도`);
    p.ad_name = String(a.name || "");
    p.src_status = String(a.effective_status || a.status || "");
    p.src_campaign_name = String(a.campaign?.name || "");
    p.src_adset_id = String(a.adset?.id || "");
    p.src_adset_name = String(a.adset?.name || "");
    if (/^(DELETED|ARCHIVED)$/.test(p.src_status)) {
      p.error = `원본 광고가 ${p.src_status} 상태 — 복사 불가`;
      return p;
    }
    const pk = productKey(region, p.src_campaign_name, p.src_adset_name);
    p.product = pk.label;
    if (!pk.key) {
      p.error = "캠페인명에서 상품명을 찾지 못함";
      return p;
    }
    const srcFp = fingerprint(a.creative);
    const srcAdsetId = p.src_adset_id;

    const cands = (await ascAdsetsOf(acc, token, region)).filter((t) => t.product === pk.key);
    if (cands.length) await loadAdsFor(acc, cands.filter((t) => t.adset_id !== srcAdsetId), token);
    if (!cands.length) {
      p.error = `'${p.product}' ${region === "gl" ? "국가·상품" : "상품"}의 ASC 캠페인이 이 계정에 없음`;
      return p;
    }
    for (const t of cands) {
      const tg: Target = {
        key: `${id}|${t.adset_id}`,
        campaign_id: t.campaign_id, campaign_name: t.campaign_name, campaign_status: t.campaign_status,
        adset_id: t.adset_id, adset_name: t.adset_name, adset_status: t.adset_status,
        action: "copy", note: "", error: "",
      };
      if (t.adset_id === srcAdsetId) {
        tg.action = "skip"; tg.note = "원본이 이미 이 세트에 있음";
        p.targets.push(tg); continue;
      }
      const dupIdx = t.fps.findIndex((fp) => overlaps(fp, srcFp));
      if (dupIdx >= 0) {
        tg.action = "skip";
        tg.note = "같은 소재가 이미 있음" + (t.ad_names?.[dupIdx] ? ` (${t.ad_names[dupIdx].slice(0, 30)})` : "");
        p.targets.push(tg); continue;
      }
      const done = doneMap[tg.key];
      if (done) {
        tg.action = "skip";
        tg.note = `${kstStamp(done.applied_at)} 이미 복사됨 (${done.copied_ad_id || ""})`;
        p.targets.push(tg); continue;
      }
      // 중단된 ASC 에도 광고를 넣는다(켜지는 않는다 — 여기서 캠페인·세트 status 는 절대 수정하지 않음).
      if (t.adset_status !== "ACTIVE" || t.campaign_status !== "ACTIVE") {
        tg.note = `${t.campaign_status !== "ACTIVE" ? "캠페인" : "세트"} ${t.campaign_status !== "ACTIVE" ? t.campaign_status : t.adset_status} — 광고만 추가, ASC 는 켜지 않음`;
      }
      p.targets.push(tg);
    }
  } catch (e) {
    p.error = String((e as Error).message || e).slice(0, 400);
  }
  return p;
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
  const region = String(body?.region || "kr");
  const dryRun = body?.dryRun !== false; // 기본은 안전한 dry-run
  const items = Array.isArray(body?.items) ? body.items : [];
  const select: Set<string> | null = Array.isArray(body?.select) ? new Set(body.select.map(String)) : null;

  if (!(mode in HL_TBL)) return json({ ok: false, error: "ASC 복사는 소재(cr) 마킹에서만 가능합니다" }, 400);
  if (!REGIONS.has(region)) return json({ ok: false, error: `알 수 없는 region: ${region}` }, 400);
  if (!items.length) return json({ ok: false, error: "복사할 소재가 없습니다" }, 400);
  if (items.length > MAX_ITEMS) return json({ ok: false, error: `한 번에 ${MAX_ITEMS}개까지만 복사할 수 있습니다` }, 400);

  const { tbl, col } = HL_TBL[mode];
  const hlRows: any[] = await sbSelect(tbl, `select=${col},highlight`);
  const hlMap: Record<string, string> = {};
  for (const r of hlRows) if (r?.[col]) hlMap[String(r[col])] = String(r.highlight ?? "");

  const seen = new Set<string>();
  const uniq = items.filter((it: any) => {
    const k = String(it?.ad_id ?? "");
    if (!k || seen.has(k)) return false;
    seen.add(k);
    return true;
  });

  const doneMap = await fetchDone(uniq.map((it: any) => String(it.ad_id)));

  const plans: Plan[] = [];
  for (const it of uniq) plans.push(await planOne(it, hlMap, doneMap, region));

  const nTargets = plans.reduce((n, p) => n + p.targets.filter((t) => t.action === "copy").length, 0);
  if (nTargets > MAX_TARGETS) return json({ ok: false, error: `복사 대상이 ${nTargets}건 — 한 번에 ${MAX_TARGETS}건까지만` }, 400);

  if (dryRun) return json({ ok: true, dryRun: true, actor: user.email || "", plan: plans });

  // ── 실제 복사 ──
  const logs: any[] = [];
  for (const p of plans) {
    if (p.error) continue;
    const token = tokenFor(p.ad_account_id)?.token || "";
    for (const t of p.targets) {
      if (t.action !== "copy" || t.error) { t.applied = false; continue; }
      if (select && !select.has(t.key)) { t.applied = false; t.note = (t.note ? t.note + " / " : "") + "선택 안 함"; continue; }
      try {
        const j = await metaPost(`${p.ad_id}/copies`, {
          adset_id: t.adset_id,
          status_option: STATUS_OPTION,
          rename_options: JSON.stringify({ rename_strategy: "NO_RENAME" }),
        }, token);
        const copied = String(j.copied_ad_id || j.ad_object_ids?.find?.((o: any) => o?.ad_object_type === "ad")?.copied_id || j.id || "");
        t.applied = true;
        t.copied_ad_id = copied;
      } catch (e) {
        t.applied = false;
        t.error = String((e as Error).message || e).slice(0, 400);
      }
      logs.push({
        actor: user.email || user.id,
        region: region,
        ad_id: p.ad_id,
        ad_name: p.ad_name,
        ad_account_id: p.ad_account_id,
        product: p.product,
        src_campaign_name: p.src_campaign_name,
        src_adset_id: p.src_adset_id,
        target_campaign_id: t.campaign_id,
        target_campaign_name: t.campaign_name,
        target_adset_id: t.adset_id,
        target_adset_name: t.adset_name,
        copied_ad_id: t.copied_ad_id || null,
        status_option: STATUS_OPTION,
        ok: !!t.applied,
        error: t.error || null,
      });
    }
  }
  await sbInsert("asc_copy_log", logs);

  const okN = logs.filter((l) => l.ok).length;
  const errN = logs.filter((l) => !l.ok).length;
  return json({ ok: true, dryRun: false, actor: user.email || "", applied: okN, failed: errN, plan: plans });
});
