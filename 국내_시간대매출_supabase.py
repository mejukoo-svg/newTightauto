# -*- coding: utf-8 -*-
"""
============================================================
국내 채널별 매출 — 4시간(intra-day) 버킷 → Supabase(kr_channel_revenue_4h)
============================================================
목적: index.html 국내 '📊 매출' 탭의 '시간별' 보기용. 하루를 4시간 6구간
      (00,04,08,12,16,20 KST)으로 나눠 채널별 Mixpanel 귀속 매출을 집계.

소스   : Mixpanel export  결제완료/payment_complete  (properties.time = UTC epoch)
버킷   : time + 9h(KST) → hour//4*4 ∈ {0,4,8,12,16,20}
채널   : ① utm_term(adset_id)∈국내메타세트 & utm_source=Meta → '국내 메타'
         ② utm_term(adset_id)∈밴스드(국내)세트 & utm_source=Meta → '밴스드'
         ③ is_naver_event → '네이버'
         ④ properties.ch=='google' → '구글디멘드젠'
         ⑤ 그 외 → 스킵
지출   : Meta insights(level=account, hourly breakdown, time_increment=1)로 국내메타·밴스드
         계정의 (date,4h,channel) 지출을 받아 매출과 합쳐 ROAS 산출. 네이버·구글=지출0(매출만).
dedup  : order_id(utm_term우선·max revenue) → $insert_id → (date,distinct_id,서비스)
통화   : 국내 KRW 그대로. 구글디멘드젠 해외결제만 KRW 환산(구글_디멘드젠_mp 로직 이식).
가드   : only-raise — 새 집계가 기존 저장값보다 낮으면(=transient MP fetch 실패) 기존값 보존.
         (memory: mixpanel-fetch-fail-zeroes-revenue)

환경변수: MIXPANEL_PROJECT_ID, MIXPANEL_USERNAME, MIXPANEL_SECRET,
          SUPABASE_URL, SUPABASE_SERVICE_KEY,
          REFRESH_DAYS(기본4), FULL_REFRESH(기본false)

[사용법]  python 국내_시간대매출_supabase.py
============================================================
"""
import os, sys, json, time, re, logging
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal
from collections import defaultdict
from urllib.parse import unquote

import requests as req_lib

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("kr4h")

# ---- .env 로컬 로드 (GitHub Actions 는 env 주입) ----
def _load_env():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(p):
        return
    for line in open(p, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())
_load_env()

MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME   = os.environ.get("MIXPANEL_USERNAME", "")
MIXPANEL_SECRET     = os.environ.get("MIXPANEL_SECRET", "")
SUPABASE_URL        = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY        = os.environ.get("SUPABASE_SERVICE_KEY", "")

MIXPANEL_EVENTS = ["결제완료", "payment_complete"]

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "4"))
MP_FETCH_BUFFER_DAYS = 2   # KST 새벽 경계분 확보용(export 는 UTC 날짜 필터)

START = date(2025, 12, 1) if FULL_REFRESH else TODAY - timedelta(days=REFRESH_DAYS - 1)
END = TODAY

VN_TW_ACC = "act_1286632473622244"   # 대만 밴스드 계정(국내 밴스드에서 제외)

# 채널 라벨 — index.html chrev 채널명과 정확히 일치시킬 것
CH_META   = "국내 메타"
CH_VANCED = "밴스드"
CH_NAVER  = "네이버"
CH_GGDG   = "구글디멘드젠"

# ── Meta 시간대 지출(국내메타·밴스드) — Mixpanel 매출과 합쳐 4시간 ROAS 산출 ──
#   계정 광고주 타임존=KST 이므로 hourly breakdown 시각이 KST와 일치(매출 버킷과 정렬됨).
#   네이버·구글디멘드젠은 시간대 지출 원천이 제한적이라 지출=0(매출만).
META_API_VERSION = "v21.0"
META_BASE = f"https://graph.facebook.com/{META_API_VERSION}"
META_TOKEN_1 = os.environ.get("META_TOKEN_1", "")
META_TOKEN_2 = os.environ.get("META_TOKEN_2", "")
META_TOKEN_VANCED = os.environ.get("META_TOKEN_VANCED", "")
# (계정, 토큰, 채널). 밴스드 대만(act_1286632473622244)은 국내 아님 → 제외.
META_SPEND_ACCOUNTS = [
    ("act_1270614404675034", META_TOKEN_1, CH_META),
    ("act_707835224206178",  META_TOKEN_1, CH_META),
    ("act_1808141386564262", META_TOKEN_2, CH_META),
    ("act_25183853061243175", META_TOKEN_VANCED, CH_VANCED),
    ("act_1560037899174007",  META_TOKEN_VANCED, CH_VANCED),
]

# =========================================================
# 헬퍼 (기존 파이프라인에서 이식)
# =========================================================
def clean_id(val):
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    if re.match(r"^\d+$", s):
        return s
    try:
        if ("E" in s or "e" in s) and re.match(r"^[\d.]+[eE][+\-]?\d+$", s):
            return str(int(Decimal(s)))
    except Exception:
        pass
    try:
        if re.match(r"^\d+\.\d+$", s):
            return str(int(Decimal(s)))
    except Exception:
        pass
    numeric_only = re.sub(r"[^0-9]", "", s)
    return numeric_only if numeric_only else s

META_UTM_SOURCES = {"ig", "fb", "an", "msg", "instagram", "facebook", "threads", "th"}
def is_meta_source(src):
    s = str(src).strip().lower() if src is not None else ""
    if not s:
        return False
    if s in META_UTM_SOURCES:
        return True
    if s.startswith("ig") or s.startswith("fb") or "instagram" in s or "facebook" in s or "site_source_name" in s:
        return True
    return False

NAVER_UTM_VALUES = {"naver", "네이버"}
def is_naver_event(props):
    for key in ["utm_source", "$initial_utm_source", "UTM_Source", "utm_Source", "UTM Source"]:
        v = props.get(key)
        if v and str(v).strip().lower() in NAVER_UTM_VALUES:
            return True
    for k in ("$initial_referring_domain", "referring_domain"):
        rd = props.get(k)
        if rd and "naver" in str(rd).lower():
            return True
    for k in ("$initial_referrer", "referrer", "$referrer"):
        rf = props.get(k)
        if rf and "naver.com" in str(rf).lower():
            return True
    return False

GOOGLE_CH_VALUES = {"google"}
EXCLUDE_CT_SUBSTR = ["moodang_260529"]
def is_google_event(props):
    v = props.get("ch")
    return v is not None and str(v).strip().lower() in GOOGLE_CH_VALUES
def is_excluded_ct(ct):
    if not ct:
        return False
    low = str(ct).lower()
    return any(s in low for s in EXCLUDE_CT_SUBSTR)

# ── 구글디멘드젠 해외결제 KRW 환산 (구글_디멘드젠_mp 이식) ──
KNOWN_NONKRW = {"TWD", "HKD", "THB", "JPY", "USD"}
SUFFIX_CURRENCY = {"tw": "TWD", "th": "THB", "jp": "JPY", "hk": "HKD"}
FALLBACK_KRW_PER = {"TWD": 48.0, "HKD": 197.0, "THB": 45.0, "JPY": 10.3, "USD": 1540.0, "KRW": 1.0}
def currency_from_suffix(svc):
    m = re.search(r'-([a-z]{2,3})$', str(svc or "").strip().lower())
    return SUFFIX_CURRENCY.get(m.group(1)) if m else None
def event_currency(props):
    c = str(props.get("통화") or "").strip().upper()
    if c in KNOWN_NONKRW:
        return c
    if c == "KRW":
        return "KRW"
    sc = currency_from_suffix(props.get("서비스"))
    if sc:
        return sc
    cc = str(props.get("mp_country_code") or "").strip().upper()
    return {"TW": "TWD", "HK": "HKD", "TH": "THB", "JP": "JPY"}.get(cc, "KRW")
_krw_rates = None
def get_krw_rates():
    global _krw_rates
    if _krw_rates is not None:
        return _krw_rates
    rates = dict(FALLBACK_KRW_PER)
    try:
        r = req_lib.get("https://open.er-api.com/v6/latest/USD", timeout=15)
        if r.status_code == 200:
            usd = r.json().get("rates", {})
            krw = usd.get("KRW")
            if krw:
                for cur in ("TWD", "HKD", "THB", "JPY"):
                    per = usd.get(cur)
                    if per:
                        rates[cur] = krw / per
                rates["USD"] = krw
                rates["KRW"] = 1.0
    except Exception as e:
        log.warning(f"  ⚠️ 환율 조회 실패 → 폴백 사용: {e}")
    _krw_rates = rates
    return rates

# =========================================================
# Supabase
# =========================================================
class SupabaseClient:
    def __init__(self, url, key):
        self.base = url.rstrip("/")
        self.key = key
        self.h = {"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    def select_rows(self, table, select, flt=""):
        """페이지네이션 GET (Range 헤더 1000/페이지)."""
        out = []
        step = 1000
        start = 0
        while True:
            headers = dict(self.h)
            headers["Range-Unit"] = "items"
            headers["Range"] = f"{start}-{start+step-1}"
            url = f"{self.base}/rest/v1/{table}?select={select}{flt}"
            r = req_lib.get(url, headers=headers, timeout=60)
            if r.status_code not in (200, 206):
                log.warning(f"  ⚠️ select {table} HTTP {r.status_code}: {r.text[:200]}")
                break
            rows = r.json()
            out.extend(rows)
            if len(rows) < step:
                break
            start += step
        return out

    def upsert(self, table, records, on_conflict):
        if not records:
            return
        url = f"{self.base}/rest/v1/{table}?on_conflict={on_conflict}"
        headers = dict(self.h)
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        for i in range(0, len(records), 500):
            chunk = records[i:i+500]
            r = req_lib.post(url, headers=headers, data=json.dumps(chunk), timeout=120)
            if r.status_code not in (200, 201, 204):
                log.error(f"  ❌ upsert {table} HTTP {r.status_code}: {r.text[:300]}")
                raise RuntimeError(f"upsert 실패: {r.status_code}")

# =========================================================
# 채널 세트(adset_id) 로드
# =========================================================
def load_channel_adsets(sb):
    """국내 메타 / 밴스드(국내) adset_id 집합을 Supabase 에서 로드."""
    cutoff = (TODAY - timedelta(days=120)).isoformat()
    kr = set()
    for row in sb.select_rows("ad_performance_daily", "adset_id", f"&date=gte.{cutoff}"):
        aid = clean_id(row.get("adset_id"))
        if aid:
            kr.add(aid)
    vn = set()
    for row in sb.select_rows("vanced_ad_performance_daily", "adset_id,ad_account_id", f"&date=gte.{cutoff}"):
        if str(row.get("ad_account_id") or "") == VN_TW_ACC:
            continue   # 대만 밴스드 제외 → 국내 밴스드만
        aid = clean_id(row.get("adset_id"))
        if aid:
            vn.add(aid)
    log.info(f"  📇 채널 세트: 국내메타 {len(kr)}개 / 밴스드(국내) {len(vn)}개 adset")
    return kr, vn

# =========================================================
# Mixpanel
# =========================================================
def fetch_mixpanel(from_date, to_date):
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "event": json.dumps(MIXPANEL_EVENTS),
        "project_id": MIXPANEL_PROJECT_ID,
    }
    log.info(f"  📡 Mixpanel export {params['from_date']} ~ {params['to_date']}")
    for attempt in range(4):
        try:
            resp = req_lib.get(url, params=params,
                               auth=(MIXPANEL_USERNAME, MIXPANEL_SECRET), timeout=300)
            if resp.status_code == 429:
                w = 30 + attempt * 30
                log.warning(f"  ⏳ 429 → {w}s"); time.sleep(w); continue
            if resp.status_code != 200:
                log.error(f"  ❌ HTTP {resp.status_code}: {resp.text[:300]}"); return []
            lines = [l for l in resp.text.splitlines() if l.strip()]
            log.info(f"  📊 total events: {len(lines)}")
            return lines
        except Exception as e:
            log.error(f"  ❌ Mixpanel 예외: {e}"); time.sleep(5)
    return []

def parse_events(lines):
    """JSONL → 결제 이벤트 dict 목록. 시각(KST date+hour), 매출, 채널판별용 필드 추출."""
    out = []
    for ln in lines:
        try:
            ev = json.loads(ln)
        except Exception:
            continue
        props = ev.get("properties", {}) or {}
        ts = props.get("time", 0)
        try:
            ts = int(ts)
        except Exception:
            continue
        if ts <= 0:
            continue
        dt_kst = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)
        ds = dt_kst.date().isoformat()
        hour = dt_kst.hour

        ut = ""
        for k in ["utm_term", "UTM_Term", "UTM Term"]:
            if props.get(k):
                ut = clean_id(str(props[k]).strip()); break
        us = ""
        for k in ["utm_source", "UTM_Source", "UTM Source"]:
            if props.get(k):
                us = str(props[k]).strip(); break

        raw_amount = props.get("amount") or props.get("결제금액")
        raw_value = props.get("value")
        amount_val = 0.0
        if raw_amount is not None:
            try: amount_val = float(raw_amount)
            except Exception: pass
        value_val = 0.0
        if raw_value is not None:
            try: value_val = float(raw_value)
            except Exception: pass
        revenue = amount_val if amount_val > 0 else (value_val if value_val > 0 else 0.0)

        out.append({
            "distinct_id": props.get("distinct_id"),
            "date": ds, "hour": hour, "bucket": (hour // 4) * 4,
            "utm_term": ut, "utm_source": us,
            "revenue": revenue,
            "서비스": props.get("서비스", ""),
            "insert_id": props.get("$insert_id") or props.get("insert_id") or "",
            "order_id": props.get("order_id") or "",
            "props": props,
        })
    log.info(f"  ✅ 파싱: {len(out)}건")
    return out

def dedup_events(events):
    """order_id(utm_term우선·max revenue) → insert_id → (date,distinct_id,서비스)."""
    with_oid = [e for e in events if str(e["order_id"]).strip()]
    no_oid = [e for e in events if not str(e["order_id"]).strip()]

    # order_id 그룹: utm_term 보유 우선 → revenue 큰 것 1건
    by_oid = {}
    for e in with_oid:
        oid = str(e["order_id"]).strip()
        key = (1 if e["utm_term"] else 0, e["revenue"])
        cur = by_oid.get(oid)
        if cur is None or key > cur[0]:
            by_oid[oid] = (key, e)
    kept = [v[1] for v in by_oid.values()]

    # order_id 없는 행: insert_id → (date,distinct_id,서비스)
    seen_ins = set(); seen_dds = set()
    for e in no_oid:
        ins = str(e["insert_id"]).strip()
        if ins:
            if ins in seen_ins:
                continue
            seen_ins.add(ins)
        else:
            k = (e["date"], e["distinct_id"], e["서비스"])
            if k in seen_dds:
                continue
            seen_dds.add(k)
        kept.append(e)
    log.info(f"  🧹 dedup: {len(events)} → {len(kept)}건 (order_id {len(kept)-len(no_oid)}? / 유지 {len(kept)})")
    return kept

def classify(e, kr_adsets, vn_adsets):
    """이벤트 → (channel, revenue_krw) 또는 None."""
    props = e["props"]
    ut = e["utm_term"]
    meta = is_meta_source(e["utm_source"])
    # ① / ② Meta 계열 + adset 세트 소속
    if meta and ut:
        if ut in kr_adsets:
            return CH_META, e["revenue"]
        if ut in vn_adsets:
            return CH_VANCED, e["revenue"]
    # ③ 네이버
    if is_naver_event(props):
        return CH_NAVER, e["revenue"]
    # ④ 구글 디멘드젠 (ch=google), 해외통화는 KRW 환산
    if is_google_event(props):
        if is_excluded_ct(props.get("ct")):
            return None
        cur = event_currency(props)
        rev = e["revenue"]
        if cur != "KRW":
            rev = rev * get_krw_rates().get(cur, 1.0)
        return CH_GGDG, rev
    return None

# =========================================================
# Meta 시간대 지출
# =========================================================
def _hour_of(bucket):
    m = re.match(r"^(\d{1,2})", str(bucket or ""))
    return int(m.group(1)) if m else -1

def fetch_meta_hourly_spend(start_d, end_d):
    """Meta insights(level=account, hourly breakdown, time_increment=1) → (date,4h버킷,channel) 지출(KRW)."""
    spend = defaultdict(float)
    for acc, token, channel in META_SPEND_ACCOUNTS:
        if not token:
            log.warning(f"  ⚠️ META 토큰 없음 → {acc} 지출 스킵")
            continue
        url = f"{META_BASE}/{acc}/insights"
        params = {
            "access_token": token, "level": "account",
            "time_range": json.dumps({"since": start_d.isoformat(), "until": end_d.isoformat()}),
            "breakdowns": "hourly_stats_aggregated_by_advertiser_time_zone",
            "time_increment": 1, "fields": "spend", "limit": 500,
        }
        page = 0; n = 0
        while url:
            try:
                r = req_lib.get(url, params=params if page == 0 else None, timeout=120)
            except Exception as e:
                log.warning(f"  ⚠️ {acc} 지출조회 예외: {e}"); break
            if r.status_code != 200:
                log.warning(f"  ⚠️ {acc} 지출 HTTP {r.status_code}: {r.text[:200]}"); break
            j = r.json()
            for row in j.get("data", []):
                ds = row.get("date_start")
                hr = _hour_of(row.get("hourly_stats_aggregated_by_advertiser_time_zone"))
                if not ds or hr < 0:
                    continue
                sp = 0.0
                try: sp = float(row.get("spend") or 0)
                except Exception: pass
                if sp <= 0:
                    continue
                spend[(ds, (hr // 4) * 4, channel)] += sp
                n += 1
            url = j.get("paging", {}).get("next"); params = None; page += 1
            if page > 300:
                break
        log.info(f"  💸 {acc} ({channel}): {n} (date,hour) 지출행")
    return spend


# =========================================================
# 메인
# =========================================================
def main():
    for name, val in [("MIXPANEL_USERNAME", MIXPANEL_USERNAME), ("MIXPANEL_SECRET", MIXPANEL_SECRET),
                      ("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_SERVICE_KEY", SUPABASE_KEY)]:
        if not val:
            log.error(f"❌ 환경변수 없음: {name}"); sys.exit(1)

    log.info("=" * 56)
    log.info(f"⏰ 국내 채널별 4시간 매출 → kr_channel_revenue_4h  ({START} ~ {END})")
    log.info("=" * 56)

    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    kr_adsets, vn_adsets = load_channel_adsets(sb)

    fetch_from = START - timedelta(days=MP_FETCH_BUFFER_DAYS)
    lines = fetch_mixpanel(fetch_from, END)
    if not lines:
        log.error("⚠️ Mixpanel 이벤트 없음 — 기존값 보존(업로드 스킵)")
        return
    events = parse_events(lines)
    events = dedup_events(events)

    # (date, bucket, channel) 집계 — 업로드 대상은 date ∈ [START, END] 만
    start_s, end_s = START.isoformat(), END.isoformat()
    agg = defaultdict(lambda: [0.0, 0])   # key -> [revenue, count]
    classified = 0
    for e in events:
        if not (start_s <= e["date"] <= end_s):
            continue
        res = classify(e, kr_adsets, vn_adsets)
        if not res:
            continue
        ch, rev = res
        k = (e["date"], e["bucket"], ch)
        agg[k][0] += rev
        agg[k][1] += 1
        classified += 1
    log.info(f"  🔎 채널 귀속: {classified}건 → {len(agg)} (date,bucket,channel) 셀")

    # Meta 시간대 지출(국내메타·밴스드) — 매출 버킷과 (date,4h,channel) 정렬
    meta_spend = fetch_meta_hourly_spend(START, END)

    # only-raise 가드: 기존 저장값과 비교해 큰 값 유지(transient fetch 실패로 인한 하향 방지)
    existing = {}
    for row in sb.select_rows("kr_channel_revenue_4h", "date,bucket,channel,revenue,spend,purchase_count",
                              f"&date=gte.{start_s}&date=lte.{end_s}"):
        existing[(row["date"], int(row["bucket"]), row["channel"])] = (
            float(row.get("revenue") or 0), float(row.get("spend") or 0), int(row.get("purchase_count") or 0))

    # 매출 셀 ∪ 지출 셀 (지출만 있는 초기 시간대도 포함)
    keys = set(agg.keys()) | set(meta_spend.keys())
    records = []
    lowered = 0
    for (ds, bk, ch) in keys:
        rev, cnt = agg.get((ds, bk, ch), (0.0, 0))
        rev_f, cnt_f = round(rev, 2), cnt
        sp_f = round(meta_spend.get((ds, bk, ch), 0.0), 2)
        old = existing.get((ds, bk, ch))
        if old:
            if old[0] > rev_f:                       # 매출 보존
                rev_f, cnt_f = old[0], max(cnt_f, old[2]); lowered += 1
            if old[1] > sp_f:                        # 지출 보존
                sp_f = old[1]
        records.append({"date": ds, "bucket": int(bk), "channel": ch,
                        "revenue": rev_f, "spend": sp_f, "purchase_count": cnt_f})
    if lowered:
        log.info(f"  🛡️ only-raise: {lowered}셀 매출 기존값 보존")

    # 채널별 합계 로그 (매출·지출·ROAS)
    per_r = defaultdict(float); per_s = defaultdict(float)
    for r in records:
        per_r[r["channel"]] += r["revenue"]; per_s[r["channel"]] += r["spend"]
    for ch in (CH_META, CH_VANCED, CH_NAVER, CH_GGDG):
        rv, sp = per_r.get(ch, 0), per_s.get(ch, 0)
        roas = (rv / sp * 100) if sp > 0 else 0
        log.info(f"    {ch}: 매출 ₩{rv:,.0f}  지출 ₩{sp:,.0f}  ROAS {roas:.0f}%")

    # 일자별 채널 매출 합계 (정합성 대조용 — 국내메타는 ad_performance_daily 와 근사해야 함)
    per_dc = defaultdict(float)
    for r in records:
        per_dc[(r["date"], r["channel"])] += r["revenue"]
    for ds in sorted({r["date"] for r in records}):
        parts = [f"{ch}=₩{per_dc.get((ds,ch),0):,.0f}" for ch in (CH_META, CH_VANCED, CH_NAVER, CH_GGDG)]
        log.info(f"    📅 {ds}: " + "  ".join(parts))

    if os.environ.get("DRY_RUN", "").lower() == "true":
        log.info(f"  🧪 DRY_RUN — 업로드 스킵 ({len(records)}행)")
        _dump = sorted(records, key=lambda r: (r["date"], r["bucket"], r["channel"]))
        for r in _dump[-24:]:
            roas = (r["revenue"]/r["spend"]*100) if r["spend"] > 0 else 0
            log.info(f"      {r['date']} {r['bucket']:02d}시 {r['channel']}: 매출₩{r['revenue']:,.0f} 지출₩{r['spend']:,.0f} ROAS{roas:.0f}% ({r['purchase_count']}건)")
        return

    sb.upsert("kr_channel_revenue_4h", records, on_conflict="date,bucket,channel")
    log.info(f"✅ 업로드 완료: {len(records)}행")


if __name__ == "__main__":
    main()
