# -*- coding: utf-8 -*-
"""
글로벌_소재별_supabase.py
========================
글로벌 Meta Ads (소재/ad 레벨) + Mixpanel → Supabase

글로벌_세트별과 차이점:
  - Meta level='ad' (소재 단위)
  - Mixpanel 매칭: utm_content (ad_id) 기준 (KR ad 와 동일 컨벤션)
  - 테이블: global_ad_creative_daily

환경변수:
  META_TOKEN_1, META_TOKEN_GlobalTT (or META_TOKEN_4 / META_TOKEN_3)
  MIXPANEL_PROJECT_ID, MIXPANEL_USERNAME, MIXPANEL_SECRET
  SUPABASE_URL, SUPABASE_SERVICE_KEY
  REFRESH_DAYS (기본 10), FULL_REFRESH (true/false)
"""

import os, json, time, re, math, logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from decimal import Decimal
import requests as req_lib

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logging.getLogger("stripe").setLevel(logging.WARNING)  # Stripe SDK 요청 로그(페이지당 2줄) 소음 억제
log = logging.getLogger(__name__)

# =========================================================
# 환경변수
# =========================================================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

META_TOKEN_1 = os.environ.get("META_TOKEN_1", "")
META_TOKEN_GLOBAL = os.environ.get("META_TOKEN_GlobalTT", "")
META_TOKEN_4 = os.environ.get("META_TOKEN_4", "")
META_TOKEN_ACT_2677 = META_TOKEN_GLOBAL or META_TOKEN_4 or os.environ.get("META_TOKEN_3", "")
META_TOKEN_ACT_9937 = os.environ.get("META_TOKEN_ACT_9937", "")  # Saju Taiwan (993712016404855, USD)

META_TOKENS = {
    "act_1054081590008088": META_TOKEN_1,
    "act_2677707262628563": META_TOKEN_ACT_2677,
    "act_1335040608536838": META_TOKEN_ACT_2677,
    "act_993712016404855": META_TOKEN_ACT_9937,
    "act_1021437716898605": META_TOKEN_1,  # 글로벌계정 (USD 빌링)
}
META_TOKEN_DEFAULT = META_TOKEN_1
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
ALL_AD_ACCOUNTS = list(META_TOKENS.keys())

# 계정별 기본 통화 — 글로벌(해외) 계정은 절대 KRW가 아니다.
# 세트/캠페인명에 시장 키워드가 없을 때의 기본값이자, 'kr/한국/국내'(예: '한국연예인' 소구)
# 로 인한 KRW 오판을 계정 단위에서 차단하는 안전장치. (대만 계정 = 항상 비원화)
ACCOUNT_CURRENCY = {
    "act_1054081590008088": "TWD",  # 대만 (타이트사주)
    "act_2677707262628563": "TWD",  # GlobalTT
    "act_1335040608536838": "TWD",  # GlobalTT
    "act_993712016404855":  "TWD",  # Saju Taiwan
    "act_1021437716898605": "TWD",  # 글로벌계정 (USD 빌링, 매출 TWD base)
}

MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME = os.environ.get("MIXPANEL_USERNAME", "")
MIXPANEL_SECRET = os.environ.get("MIXPANEL_SECRET", "")
MIXPANEL_EVENT_NAMES = ["결제완료", "payment_complete"]

# Meta 채널 판별 (utm_source 화이트리스트) — 세트 파이프라인과 동일.
# 타채널(google 등) 결제가 직전 Meta 방문의 stale utm_content(소재 id)을 달고 들어와
# Meta 소재 매출로 잘못 합산되는 문제 차단 → 마지막 터치가 Meta인 결제만 소재에 귀속.
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

# 글로벌 성과 측정 시 제외할 "한국" 국가값 (Mixpanel mp_country_code).
# raw export(/api/2.0/export)의 mp_country_code는 ISO alpha-2 코드("KR")로 저장됨
# (Mixpanel UI 표시값은 "South Korea"). 풀네임도 방어적으로 함께 매칭.
KOREA_CC = {"KR", "KOR", "SOUTH KOREA", "KOREA, REPUBLIC OF", "REPUBLIC OF KOREA", "한국", "대한민국"}

KST = timezone(timedelta(hours=9))
# ★ 스냅샷 기준시각 (2026-09-28): 전체 파이프라인(supabase.yml)이 세트·소재 job 에 같은 SNAPSHOT_TS(epoch초)를 넘긴다.
#   두 로더가 같은 시각을 "지금"으로 보고 그 이후 MP 결제는 이번 회차에서 제외 → 세트 매출 = 소재 소계 (오늘 칸 포함).
#   미설정(단독 워크플로·로컬 실행)이면 기존대로 실행 시각.
SNAPSHOT_TS = int(os.environ.get("SNAPSHOT_TS", "0") or 0)
TODAY = (datetime.fromtimestamp(SNAPSHOT_TS, KST) if SNAPSHOT_TS > 0 else datetime.now(KST)).replace(tzinfo=None)
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
FULL_REFRESH_START = datetime(2025, 12, 1)
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))

if FULL_REFRESH:
    REFRESH_DAYS = (TODAY - FULL_REFRESH_START).days + 1
    log.info(f"🔥 FULL_REFRESH: {FULL_REFRESH_START:%Y-%m-%d} ~ 오늘 ({REFRESH_DAYS}일)")

DATA_REFRESH_START = TODAY - timedelta(days=REFRESH_DAYS - 1)

# 환율 폴백
FALLBACK_RATES = {"TWD": 32.0, "JPY": 155.0, "HKD": 7.8, "KRW": 1450.0, "USD": 1.0, "THB": 35.5, "SGD": 1.28}
CURRENCY_TO_COUNTRY = {"TWD": "대만", "JPY": "일본", "HKD": "홍콩", "KRW": "한국", "USD": "글로벌", "THB": "태국", "SGD": "싱가포르"}
# 소재(ad_id) → 캠페인명 (2단계 meta_data 로 채움). MP 결제의 통화를 캠페인명으로 판별할 때 사용.
AD_CAMPAIGN_NAME = {}
# 세트(adset_id) → 캠페인명. 결제 통화를 세트 로더와 똑같이 '결제의 세트(utm_term)' 캠페인명으로 판별하기 위함.
ADSET_CAMPAIGN_NAME = {}
# '(소재 미상)' 행 ad_id 접두어 — 세트에는 귀속되지만 그날 지출 소재로 못 붙인 매출을 세트별로 모은다 (2026-09-28).
UNATTR_PREFIX = "unattr_"
UNATTR_AD_NAME = "(소재 미상)"

# country(mp_country_code) 행 단위 통화 보정 — 세트별(글로벌_세트별_supabase.py)과 동일.
#   HK/TH 고객은 -tw 스토어프론트에서 결제해도 Stripe가 현지통화(HKD/THB)로 청구.
#   JP/US 등은 -tw면 TWD 청구이므로 제외(엔화 5배 오환산 방지).
COUNTRY_CURRENCY_OVERRIDE = {"HK": "HKD", "TH": "THB"}


# =========================================================
# 유틸리티
# =========================================================
def clean_id(val):
    if val is None: return ""
    s = str(val).strip()
    if not s: return ""
    if re.match(r'^\d+$', s): return s
    try:
        if re.match(r'^[\d.]+[eE][+\-]?\d+$', s): return str(int(Decimal(s)))
    except: pass
    try:
        if re.match(r'^\d+\.\d+$', s): return str(int(Decimal(s)))
    except: pass
    return re.sub(r'[^0-9]', '', s) or s

def make_date_key(dt):
    return f"{dt.year % 100:02d}/{dt.month:02d}/{dt.day:02d}"

SKIP_WORDS = {"tw","kr","hk","my","sg","id","jp","th","vn","ph","asia","taiwan","japan","hongkong","korea",
    "singapore","malaysia","thailand","broad","interest","lookalike","retarget","custom","asc","cbo","abo",
    "dpa","advantage","campaign","adset","ad","ads","set","purchase","conversion","traffic",
    "v1","v2","v3","v4","v5","test","new","old","copy","sajutight","ttsaju","saju","tight",
    "대만","일본","홍콩","한국","국내","글로벌","태국","台灣","台湾","日本","香港"}

def extract_product(adset_name, campaign_name=None):
    for name in [campaign_name, adset_name]:
        if not name: continue
        parts = re.split(r'[-_\s]+', str(name).lower().strip())
        candidates = [p for p in parts if p and p not in SKIP_WORDS and len(p) > 1 and not re.match(r'^\d+$', p)]
        if candidates: return candidates[0]
    return "기타"

def detect_currency(adset_name, campaign_name=None, account_id=None):
    # 글로벌(해외) 파이프라인 전용 — KRW로 판별되는 일이 없어야 한다.
    #   · 모든 글로벌 계정은 해외 계정(국내 KRW 계정과 분리)이고, 한국 결제는
    #     mp_country_code=KR 필터로 이미 제외됨.
    #   · 세트명에 '한국연예인' 같은 소구 문구가 섞여도 KRW로 오판하지 않도록
    #     KRW 분기를 제거. 시장은 jp/hk/th/tw/sg 키워드로만 판별, 없으면 계정 기본통화.
    #   · ★ SG(싱가포르)는 tw 뒤에서 본다 — '대만_무당_ASC_싱가포르'처럼 뒤쪽 토큰이 타겟(오디언스)인
    #     캠페인은 대만 스토어(-tw, TWD 결제)라 SGD로 잡으면 매출이 ~25배 부풀려진다(세트별과 동일).
    for name in [adset_name, campaign_name]:
        if not name: continue
        n = str(name); nl = n.lower()
        parts = re.split(r'[-_\s]', nl)
        if "jp" in parts or "japan" in parts or "일본" in n: return "JPY"
        if "hk" in parts or "hongkong" in parts or "홍콩" in n: return "HKD"
        if "th" in parts or "thailand" in parts or "태국" in n: return "THB"
        if "tw" in parts or "taiwan" in parts or "대만" in n or "台灣" in n: return "TWD"
        if "sg" in parts or "singapore" in parts or "싱가포르" in n or "싱가폴" in n: return "SGD"
    return ACCOUNT_CURRENCY.get(account_id, "TWD")


# =========================================================
# 건별 실제 통화 판별 (세트별 로더와 동일 — 2026-06-29)
#   서비스 접미사(-tw=TWD 등) + Stripe (amount,시각) 매칭으로 결제건마다 진짜 통화를 확정.
#   country=HK 일괄 HKD(÷7.8) 강제가 -tw TWD 결제(~25%)를 ~4배 과대계상하던 문제 해결.
# =========================================================
STRIPE_API_KEY = os.environ.get("STRIPE_API_KEY", "")
STRIPE_DIVISOR = {"jpy": 1, "twd": 100, "hkd": 100, "usd": 100, "krw": 1, "thb": 100, "sgd": 100}
KNOWN_CURRENCIES = {"TWD", "HKD", "JPY", "THB", "SGD", "USD", "KRW"}
SUFFIX_CURRENCY = {"tw": "TWD", "th": "THB", "jp": "JPY", "hk": "HKD", "sg": "SGD"}

def market_suffix(svc):
    m = re.search(r'-([a-z]{2,3})$', str(svc or "").strip().lower())
    return m.group(1) if m else ""

def currency_from_suffix(svc):
    return SUFFIX_CURRENCY.get(market_suffix(svc))

# Stripe charge 통화 인덱스: {amount_major: [(created_ts, currency_upper), ...]}
stripe_cur_index = {}

def build_stripe_currency_index(start_date, end_date):
    """Stripe charge 를 페이징해 (amount_major→[(created,통화)]) 인덱스 구축.
    MP 결제와 (amount,시각±1h) 매칭으로 건별 실제 청구통화 복원에 사용."""
    global stripe_cur_index
    if not STRIPE_API_KEY:
        log.warning("  ⚠️ STRIPE_API_KEY 없음 — 통화 인덱스 생략(폴백 통화 사용)")
        return
    try:
        import stripe
    except ImportError:
        log.warning("  ⚠️ stripe 패키지 없음 — 통화 인덱스 생략")
        return
    stripe.api_key = STRIPE_API_KEY
    start_ts = int(start_date.timestamp()); end_ts = int(end_date.timestamp())
    has_more = True; starting_after = None; n = 0
    while has_more:
        params = {"limit": 100, "created": {"gte": start_ts, "lte": end_ts}, "status": "succeeded"}
        if starting_after: params["starting_after"] = starting_after
        resp = stripe.Charge.list(**params)
        for ch in resp.data:
            n += 1
            cur = (getattr(ch, 'currency', '') or '').lower()
            if cur not in STRIPE_DIVISOR: continue
            amt_major = round((getattr(ch, 'amount', 0) or 0) / STRIPE_DIVISOR.get(cur, 100))
            cur_up = cur.upper()
            if cur_up in KNOWN_CURRENCIES and amt_major > 0:
                stripe_cur_index.setdefault(amt_major, []).append((int(ch.created), cur_up))
        has_more = resp.has_more
        if resp.data: starting_after = resp.data[-1].id
    log.info(f"  💱 Stripe 통화 인덱스: {n}건 → {len(stripe_cur_index)}개 amount값")

def resolve_currency_by_stripe(amount_major, ts, window=3600):
    """amount(현지 major)·시각으로 Stripe charge 매칭 → 실제 통화. 실패 시 None."""
    if not ts or amount_major <= 0: return None
    cands = stripe_cur_index.get(amount_major)
    if not cands: return None
    best = None; best_dt = window + 1
    for created, cur in cands:
        dt = abs(created - ts)
        if dt < best_dt: best_dt = dt; best = cur
    return best if (best is not None and best_dt <= window) else None


# =========================================================
# 환율
# =========================================================
usd_rates = {}

def fetch_usd_rates(start_date, end_date, currency="TWD"):
    rates = {}
    fallback = FALLBACK_RATES.get(currency, 1.0)
    try:
        import yfinance as yf
        pair = f"USD{currency}=X"
        ticker = yf.Ticker(pair)
        hist = ticker.history(start=start_date.strftime('%Y-%m-%d'),
                              end=(end_date + timedelta(days=3)).strftime('%Y-%m-%d'))
        if not hist.empty:
            for idx, row in hist.iterrows():
                dt = idx.to_pydatetime().replace(tzinfo=None)
                dk = make_date_key(dt)
                rates[dk] = round(float(row['Close']), 4)
            log.info(f"  ✅ USD/{currency}: {len(rates)}일")
    except Exception as e:
        log.warning(f"  ⚠️ USD/{currency} yfinance 실패: {e}")
    if not rates:
        try:
            resp = req_lib.get("https://open.er-api.com/v6/latest/USD", timeout=10)
            if resp.status_code == 200:
                rate = resp.json().get('rates', {}).get(currency, fallback)
                d = start_date
                while d <= end_date:
                    rates[make_date_key(d)] = round(float(rate), 4)
                    d += timedelta(days=1)
        except: pass
    return rates

def get_rate(rates_dict, dk, fallback=1.0):
    if dk in rates_dict: return rates_dict[dk]
    if rates_dict:
        sorted_keys = sorted(rates_dict.keys())
        prev = [k for k in sorted_keys if k <= dk]
        if prev: return rates_dict[prev[-1]]
        return rates_dict[sorted_keys[0]]
    return fallback

def local_to_usd(amount, currency, dk):
    if currency == "USD": return amount
    rates = usd_rates.get(currency, {})
    rate = get_rate(rates, dk, FALLBACK_RATES.get(currency, 1.0))
    return amount / rate if rate > 0 else 0


# =========================================================
# Meta API (ad 레벨)
# =========================================================
def get_token(acc_id):
    return META_TOKENS.get(acc_id, META_TOKEN_DEFAULT)

# 요청한도(rate limit) 에러 — 403 으로 오지만 일시적이라 재시도해야 한다.
#   즉시 포기하면 그 (계정,날짜) Meta 데이터가 통째로 비고, 병합에서 매출만 있는
#   spend=0 레코드가 만들어져 이미 저장된 지출을 0 으로 덮는다(2026-07-26 대만 사고).
_META_RATE_LIMIT_CODES = {4, 17, 32, 613, 80000, 80001, 80002, 80003, 80004}


def _is_rate_limit(resp):
    try:
        e = resp.json().get("error", {}) or {}
        return bool(e.get("is_transient")) or int(e.get("code", 0)) in _META_RATE_LIMIT_CODES
    except Exception:
        return False


def meta_api_get(url, params=None, token=None):
    if params is None: params = {}
    params['access_token'] = token or META_TOKEN_DEFAULT
    for attempt in range(5):
        try:
            resp = req_lib.get(url, params=params, timeout=120)
            if resp.status_code == 200: return resp.json()
            if resp.status_code == 400:
                log.error(f"  ❌ Meta 400: {resp.json().get('error',{}).get('message','')[:200]}")
                return None
            if resp.status_code in [429,500,502,503] or (resp.status_code == 403 and _is_rate_limit(resp)):
                time.sleep(30 + attempt * 30); continue
            return None
        except Exception as e:
            if attempt < 4: time.sleep(15)
            else: return None
    return None

def _extract_action(al, types):
    if not al: return 0
    for a in al:
        if a.get('action_type','') in types:
            try: return float(a.get('value',0))
            except: return 0
    return 0

def fetch_meta_insights_ad_level(ad_account_id, single_date):
    """level='ad' — 소재 단위 수집"""
    url = f"{META_BASE_URL}/{ad_account_id}/insights"
    fields = "campaign_name,adset_name,adset_id,ad_name,ad_id,spend,cpm,reach,impressions,frequency,actions,cost_per_action_type,purchase_roas,unique_outbound_clicks,unique_outbound_clicks_ctr,cost_per_unique_outbound_click"
    # breakdowns=country: 세트 로더와 같이 한국(KR) 노출분 지출을 빼기 위해 국가별로 받는다 (2026-09-28).
    params = {'fields':fields,'level':'ad','breakdowns':'country','time_increment':1,
        'time_range':json.dumps({'since':single_date,'until':single_date}),
        'limit':500,'filtering':json.dumps([{'field':'spend','operator':'GREATER_THAN','value':'0'}])}
    all_results = []
    data = meta_api_get(url, params, token=get_token(ad_account_id))
    while data:
        all_results.extend(data.get('data', []))
        next_url = data.get('paging', {}).get('next')
        if next_url:
            time.sleep(1)
            try:
                resp = req_lib.get(next_url, timeout=120)
                data = resp.json() if resp.status_code == 200 else None
            except: data = None
        else: break
    return all_results

def fetch_adset_budgets(ad_account_id):
    url = f"{META_BASE_URL}/{ad_account_id}/adsets"
    params = {'fields':'id,daily_budget,campaign_id','limit':500,
        'filtering':json.dumps([{'field':'effective_status','operator':'IN','value':['ACTIVE']}])}
    results = {}
    data = meta_api_get(url, params, token=get_token(ad_account_id))
    while data:
        for row in data.get('data', []):
            asid = row.get('id', '')
            budget = row.get('daily_budget', '0')
            try: results[asid] = int(float(budget)) if budget else 0
            except: results[asid] = 0
        next_url = data.get('paging', {}).get('next')
        if next_url:
            time.sleep(1)
            try: resp = req_lib.get(next_url, timeout=120); data = resp.json() if resp.status_code == 200 else None
            except: data = None
        else: break
    return results


# =========================================================
# Mixpanel (utm_content = ad_id 매칭)
# =========================================================
def fetch_mixpanel_data(from_date, to_date):
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {'from_date':from_date,'to_date':to_date,'event':json.dumps(MIXPANEL_EVENT_NAMES),'project_id':MIXPANEL_PROJECT_ID}
    log.info(f"  📡 Mixpanel: {from_date} ~ {to_date}")
    # 반환 규약: 정상=list(빈 list 가능=실제 결제 없음) · 수집 실패=None (호출부가 기존 매출 보존하도록 구분)
    for attempt in range(4):
        try:
            resp = req_lib.get(url, params=params, auth=(MIXPANEL_USERNAME, MIXPANEL_SECRET), timeout=300)
            if resp.status_code == 429:
                time.sleep(30 + attempt * 30); continue
            if resp.status_code != 200: return None
            lines = [l for l in resp.text.split('\n') if l.strip()]
            log.info(f"  📊 이벤트: {len(lines)}건")
            data = []
            for line in lines:
                try:
                    ev = json.loads(line); props = ev.get('properties', {}); ts = props.get('time', 0)
                    if ts:
                        dt_kst = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)
                        ds = f"{dt_kst.year%100:02d}/{dt_kst.month:02d}/{dt_kst.day:02d}"
                    else: ds = None
                    # ★ utm_content = ad_id (KR ad-level 과 동일 컨벤션)
                    ut = None
                    for k in ['utm_content','UTM_Content','UTM Content']:
                        if k in props and props[k]: ut = clean_id(str(props[k]).strip()); break
                    # utm_term = adset_id — 결제의 세트 귀속(세트 로더와 동일 기준)
                    uterm = None
                    for k in ['utm_term','UTM_Term','UTM Term']:
                        if k in props and props[k]: uterm = clean_id(str(props[k]).strip()); break
                    # 채널 판별용 utm_source (Meta 결제만 귀속)
                    us = ''
                    for k in ['utm_source','UTM_Source','UTM Source']:
                        if k in props and props[k]: us = str(props[k]).strip(); break
                    raw_a = props.get('amount') or props.get('결제금액')
                    raw_v = props.get('value')
                    a_val = float(raw_a) if raw_a else 0.0
                    v_val = float(raw_v) if raw_v else 0.0
                    revenue = a_val if a_val > 0 else (v_val if v_val > 0 else 0.0)
                    # mp_country_code: 결제 국가 (글로벌 성과에서 한국 제외용)
                    country = props.get('mp_country_code') or ''
                    # 결제 이벤트 명시 통화(있으면 최우선, 커버리지 ~12%지만 정확)
                    cur_explicit = props.get('통화') or props.get('currency') or ''
                    cur_explicit = str(cur_explicit).strip().upper() if cur_explicit else ''
                    # 주문번호: 대만=merchant_uid/주문번호, 한국=order_id. 중복 결제 판단 1차 키.
                    order_no = props.get('merchant_uid') or props.get('주문번호') or props.get('order_id') or props.get('imp_uid') or ''
                    data.append({'distinct_id':props.get('distinct_id'),'date':ds,'ts':int(ts) if ts else 0,'pt':int(props.get('mp_processing_time_ms') or 0)//1000,'utm_content':ut or '','utm_term':uterm or '','utm_source':us or '','revenue':revenue,'서비스':props.get('서비스',''),'insert_id':props.get('$insert_id') or props.get('insert_id') or '','order_no':str(order_no).strip(),'country':str(country).strip(),'cur_explicit':cur_explicit})
                except: pass
            log.info(f"  ✅ 파싱: {len(data)}건")
            return data
        except Exception as e:
            log.error(f"  ❌ Mixpanel 오류: {e}"); return None
    return None  # 429 4회 소진 등 — 수집 실패로 간주


# =========================================================
# Supabase 클라이언트
# =========================================================
class SupabaseClient:
    def __init__(self, url, key):
        clean_url = re.sub(r'[^\x20-\x7E]', '', url).strip().rstrip("/")
        if not clean_url.startswith("http"): clean_url = f"https://{clean_url}"
        self.base_url = clean_url
        self.key = key.strip()
        self.headers = {"apikey": self.key, "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}
        # new-tightauto: SUPABASE_DB_SCHEMA 설정 시에만 스키마 프로파일 헤더 (미설정=기존 public)
        _sc = os.environ.get('SUPABASE_DB_SCHEMA', '').strip()
        if _sc:
            self.headers['Accept-Profile'] = _sc
            self.headers['Content-Profile'] = _sc

    def _sanitize(self, records):
        clean = []
        for rec in records:
            row = {}
            for k, v in rec.items():
                if hasattr(v, 'item'): v = v.item()
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): v = 0
                row[k] = v
            clean.append(row)
        return clean

    def select(self, table, query):
        """읽기 전용 GET. 실패 시 [] 반환."""
        url = f"{self.base_url}/rest/v1/{table}?{query}"
        try:
            resp = req_lib.get(url, headers=self.headers, timeout=60)
            if resp.status_code == 200: return resp.json()
            log.error(f"  ❌ select: HTTP {resp.status_code} | {resp.text[:200]}")
        except Exception as e:
            log.error(f"  ❌ select 예외: {e}")
        return []

    def delete(self, table, query):
        """DELETE ... where <query>. query 예: 'date=eq.2026-07-01&ad_id=in.(a,b)'"""
        url = f"{self.base_url}/rest/v1/{table}?{query}"
        try:
            resp = req_lib.delete(url, headers=self.headers, timeout=60)
            if resp.status_code in [200, 204]:
                return True
            log.error(f"  ❌ delete: HTTP {resp.status_code} | {resp.text[:200]}")
        except Exception as e:
            log.error(f"  ❌ delete 예외: {e}")
        return False

    def upsert(self, table, records, chunk_size=500):
        url = f"{self.base_url}/rest/v1/{table}"
        total = len(records); success = 0
        for i in range(0, total, chunk_size):
            chunk = self._sanitize(records[i:i+chunk_size])
            try:
                resp = req_lib.post(url, headers=self.headers, json=chunk, timeout=60)
                if resp.status_code in [200, 201]:
                    success += len(chunk)
                    log.info(f"  ✅ upsert {success}/{total}")
                else:
                    log.error(f"  ❌ upsert: HTTP {resp.status_code} | {resp.text[:300]}")
            except Exception as e:
                log.error(f"  ❌ upsert 예외: {e}")
            time.sleep(0.5)
        return success


# =========================================================
# 메인
# =========================================================
def main():
    log.info("=" * 60)
    log.info("🌏🎨 글로벌 소재별 Meta(ad) + Mixpanel → Supabase")
    log.info("=" * 60)
    log.info(f"📅 갱신: {DATA_REFRESH_START:%Y-%m-%d} ~ 오늘 ({REFRESH_DAYS}일)")

    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)

    # 1) 환율 조회
    log.info("\n1단계: 환율 조회")
    rate_start = DATA_REFRESH_START - timedelta(days=7)
    for curr in ["TWD", "JPY", "HKD", "KRW", "THB", "SGD"]:
        usd_rates[curr] = fetch_usd_rates(rate_start, TODAY, curr)

    # 2) Meta Insights (ad level)
    log.info(f"\n2단계: Meta Insights ad level ({REFRESH_DAYS}일 × {len(ALL_AD_ACCOUNTS)}계정)")
    meta_data = defaultdict(list)
    purchase_types = ['purchase','omni_purchase','offsite_conversion.fb_pixel_purchase']

    for day_offset in range(REFRESH_DAYS):
        td = TODAY - timedelta(days=day_offset)
        target_str = td.strftime('%Y-%m-%d')
        dk = make_date_key(td)
        day_rows = []
        for acc_id in ALL_AD_ACCOUNTS:
            rows = fetch_meta_insights_ad_level(acc_id, target_str)
            if rows:
                # country breakdown 행 → 소재 단위 합산. 지출/지표는 비한국(KR viewer 제외) 행만 — 세트 로더와 동일.
                #   KR 행만 있는 소재도 행은 남긴다(세트 로더도 세트 행은 남기고 지출만 0).
                by_ad = {}
                for row in rows:
                    if float(row.get('spend',0)) <= 0: continue
                    aid = row.get('ad_id','')
                    a = by_ad.get(aid)
                    if a is None:
                        a = by_ad[aid] = {
                            'campaign_name': row.get('campaign_name',''),
                            'adset_name': row.get('adset_name',''),
                            'adset_id': row.get('adset_id',''),
                            'ad_name': row.get('ad_name',''),
                            'ad_id': aid,
                            'ad_account_id': acc_id,
                            'spend': 0.0, 'reach': 0, 'impressions': 0, 'unique_clicks': 0.0,
                            'results_meta': 0.0, '_roas_w': 0.0,
                            'date_key': dk, 'date_obj': td,
                        }
                    if str(row.get('country','') or '').strip().upper() in KOREA_CC: continue
                    sp = float(row.get('spend',0))
                    a['spend'] += sp
                    a['reach'] += int(float(row.get('reach',0)))
                    a['impressions'] += int(float(row.get('impressions',0)))
                    a['unique_clicks'] += _extract_action(row.get('unique_outbound_clicks',[]), ['outbound_click'])
                    a['results_meta'] += _extract_action(row.get('actions',[]), purchase_types)
                    a['_roas_w'] += _extract_action(row.get('purchase_roas',[]), purchase_types) * sp
                for a in by_ad.values():
                    sp, imp, rch, uclk, res = a['spend'], a['impressions'], a['reach'], a['unique_clicks'], a['results_meta']
                    # 파생지표: 합산값으로 재계산 (세트 로더와 동일)
                    a['cpm'] = (sp / imp * 1000) if imp > 0 else 0
                    a['frequency'] = (imp / rch) if rch > 0 else 0
                    a['unique_ctr'] = (uclk / imp * 100) if imp > 0 else 0
                    a['cost_per_click'] = (sp / uclk) if uclk > 0 else 0
                    a['cost_per_result'] = (sp / res) if res > 0 else 0
                    a['meta_roas'] = (a.pop('_roas_w') / sp) if sp > 0 else 0
                    day_rows.append(a)
            time.sleep(1)
        if day_rows:
            meta_data[dk] = day_rows
            log.info(f"  📊 {dk}: {len(day_rows)}건")
    log.info(f"✅ Meta: {sum(len(v) for v in meta_data.values())}건")

    # 2.4) 소재→캠페인명 맵 — MP 결제의 통화를 캠페인명(hk/tw)으로 판별하기 위해 채운다.
    for _rows in meta_data.values():
        for _mr in _rows:
            _aid = _mr.get('ad_id')
            if _aid and _mr.get('campaign_name'):
                AD_CAMPAIGN_NAME[str(_aid)] = _mr['campaign_name']
    log.info(f"✅ 소재→캠페인명 맵: {len(AD_CAMPAIGN_NAME)}개")
    for _rows in meta_data.values():
        for _mr in _rows:
            if _mr.get('adset_id') and _mr.get('campaign_name'):
                ADSET_CAMPAIGN_NAME[str(_mr['adset_id'])] = _mr['campaign_name']

    # 2.5) 예산 (adset 단위)
    log.info("\n2.5단계: 예산 조회")
    budget_map = {}
    for acc_id in ALL_AD_ACCOUNTS:
        budget_map.update(fetch_adset_budgets(acc_id))
        time.sleep(1)
    log.info(f"✅ 예산: {len(budget_map)}개")

    time.sleep(30)

    # 3) Mixpanel (utm_content = ad_id)
    log.info(f"\n3단계: Mixpanel ({REFRESH_DAYS}일, utm_content 매칭)")
    YESTERDAY = TODAY - timedelta(days=1)
    mp_raw = []
    # 수집 실패한 날짜(iso) 집합 — 매출 0 덮어쓰기 방지 (2026-06-08). 실패와 '실제 결제 없음' 구분.
    uncovered = set()
    def _mark_uncovered(s, e):
        d = s
        while d <= e:
            uncovered.add(d.strftime('%Y-%m-%d')); d += timedelta(days=1)
    # ★ KST 경계 보정 버퍼 (2026-07-31): MP export 는 from_date 를 UTC 날짜로 필터하지만
    #   parse 는 KST(UTC+9)로 재버킷팅 → 윈도우 첫 KST 날짜의 00:00~09:00 이 통째로 누락(~28~35%).
    #   REFRESH_DAYS=10 상 각 날짜의 마지막 기록(D+9)이 항상 첫날이라 영구 고착됐다.
    #   상세: 글로벌_세트별_supabase.py 의 동일 주석 참조.
    MP_FETCH_BUFFER_DAYS = 2
    # 과거 구간: 7일 청크로 분할 (대용량 응답 timeout 위험 완화 · 실패한 청크만 보존모드)
    chunk_start = DATA_REFRESH_START
    while chunk_start <= YESTERDAY:
        chunk_end = min(chunk_start + timedelta(days=6), YESTERDAY)
        fetch_from = (chunk_start - timedelta(days=MP_FETCH_BUFFER_DAYS)).strftime('%Y-%m-%d')
        res = fetch_mixpanel_data(fetch_from, chunk_end.strftime('%Y-%m-%d'))
        if res is None:
            log.error(f"  ❌ Mixpanel 수집 실패: {chunk_start:%Y-%m-%d}~{chunk_end:%Y-%m-%d} → 해당 날짜 기존 매출 보존")
            _mark_uncovered(chunk_start, chunk_end)
        else:
            mp_raw.extend(res)
        chunk_start = chunk_end + timedelta(days=1)
    # 오늘(KST) 별도 호출 — MP export 의 from/to 는 UTC 날짜라, KST 00:00~09:00 엔 KST 오늘이 아직 UTC 미래 날짜여서
    #   export 가 실패하고 '수집 실패 → 기존 매출 보존'으로 빠져 오늘 매출이 아침마다 옛 값에 고정됐다(2026-09-28 발견).
    #   그 시간대의 KST 오늘 결제는 위 청크(to=어제 KST = 오늘 UTC)에 이미 들어 있으므로 호출을 건너뛴다.
    #   (국내_세트별_supabase.py 의 mp_today_str <= utc_today 처리와 동일)
    _utc_today = datetime.fromtimestamp(SNAPSHOT_TS if SNAPSHOT_TS > 0 else time.time(), timezone.utc).strftime('%Y-%m-%d')
    if TODAY.strftime('%Y-%m-%d') <= _utc_today:
        today_res = fetch_mixpanel_data(TODAY.strftime('%Y-%m-%d'), TODAY.strftime('%Y-%m-%d'))
        if today_res is None:
            log.error(f"  ❌ Mixpanel 수집 실패: 오늘({TODAY:%Y-%m-%d}) → 기존 매출 보존")
            _mark_uncovered(TODAY, TODAY)
        else:
            mp_raw.extend(today_res)
    else:
        log.info(f"  ── 오늘({TODAY:%Y-%m-%d} KST)은 아직 UTC {_utc_today} — 앞 청크에 포함, 별도 호출 생략 ──")
    # 스냅샷 컷오프 — 기준시각 이후 결제 제외 (세트·소재 동일 시점 정합)
    #   결제시각(ts)뿐 아니라 Mixpanel 처리시각(pt=mp_processing_time_ms)도 자른다(2026-09-28): 세트·소재 job 이
    #   export 를 서로 다른 시각에 호출하면, 결제시각은 기준 이전이지만 늦게 적재된 이벤트가 나중 job 에만 잡혀
    #   하루 1~3건씩 어긋났다. 처리시각까지 자르면 두 job 이 같은 이벤트 집합을 본다. (pt 없으면 결제시각만)
    if SNAPSHOT_TS > 0:
        _bn = len(mp_raw)
        mp_raw = [r for r in mp_raw if (not r.get('ts') or r['ts'] <= SNAPSHOT_TS) and (not r.get('pt') or r['pt'] <= SNAPSHOT_TS)]
        log.info(f"  ⏱️ 스냅샷 컷오프 {datetime.fromtimestamp(SNAPSHOT_TS, KST):%m-%d %H:%M} KST: {_bn} → {len(mp_raw)}건")
    log.info(f"✅ Mixpanel: {len(mp_raw)}건" + (f" · ⚠️ 수집실패 보존 {len(uncovered)}일" if uncovered else ""))

    # 3.5) Stripe 통화 인덱스 — MP 결제의 실제 청구통화를 (amount,시각)으로 건별 복원하기 위함
    log.info("\n3.5단계: Stripe 통화 인덱스")
    _sc_start = datetime(DATA_REFRESH_START.year, DATA_REFRESH_START.month, DATA_REFRESH_START.day, tzinfo=KST)
    build_stripe_currency_index(_sc_start, datetime.now(KST))

    # Mixpanel 집계 — 세트 귀속(utm_term) 우선 + 세트 안에서 소재(utm_content) 배분 (2026-09-28)
    #   결제 1건의 '세트'는 세트 로더(글로벌_세트별_supabase.py)와 똑같은 규칙으로 정한다:
    #   utm_term · 채널분류 · 주문 dedup · 라스트터치 24h 크로스셀 백필 · KR 결제 제외 · 세트 캠페인명 통화.
    #   '소재'는 그 결제의 utm_content 가 그 세트의 그날 지출 소재일 때만 붙이고, 못 붙인 매출
    #   (광고 id 없음·다른 세트 광고·그날 지출 없는 광고)은 세트별 '(소재 미상)' 행으로 모은다.
    #   → 소재 소계 = 세트 매출. (이전: utm_content 로만 매칭해 이런 결제가 소재 쪽에서 통째로 빠졌다.)
    import pandas as pd
    mp_value_map = {}; mp_count_map = {}   # (date, ad_key, country) → USD/건수 · ad_key = ad_id 또는 UNATTR_PREFIX+adset_id
    AD_DAY_ADSET = {(dk, str(mr['ad_id'])): str(mr['adset_id'])
                    for dk, rows in meta_data.items() for mr in rows if mr.get('ad_id')}
    if mp_raw:
        df = pd.DataFrame(mp_raw)

        def _norm(x):
            s = str(x).strip() if x is not None else ''
            return '' if s.lower() in ('', 'none', 'undefined', 'null') else s
        for _c in ('utm_term', 'utm_content'):
            if _c not in df.columns: df[_c] = ''
            df[_c] = df[_c].apply(_norm)

        # 0) 채널 분류 (meta / organic / other) — 세트와 동일.
        #   - other(google/tiktok 등): stale utm 오귀속 차단 → 통째로 제외.
        #   - organic(utm_source 빈값): 크로스셀로 utm 소실된 결제 → 자기 utm 불신, 2단계에서 직전 Meta 결제로부터만 상속.
        #   - meta: 자기 utm_term(=adset_id)·utm_content(=ad_id) 사용.
        if 'utm_source' not in df.columns: df['utm_source'] = ''
        def _src_class(us):
            if is_meta_source(us): return 'meta'
            return 'organic' if str(us).strip() == '' else 'other'
        df['_src'] = df['utm_source'].apply(_src_class)
        _bn = len(df); _no = int((df['_src'] == 'other').sum())
        df = df[df['_src'] != 'other'].copy()
        df.loc[df['_src'] == 'organic', ['utm_term', 'utm_content']] = ''
        log.info(f"  🔵 채널 분류: 전체 {_bn} → meta+organic {len(df)}건 (other 타채널 {_no}건 제외)")

        # 1) 중복 결제 dedup — 주문번호(order_no) 우선 · 세트와 동일(utm_term 보유 행 우선, 그다음 revenue 큰 행)
        if 'order_no' not in df.columns: df['order_no'] = ''
        df['order_no'] = df['order_no'].fillna('').astype(str).str.strip()
        _has_ord = df['order_no'].str.len() > 0
        df_ord = df[_has_ord].copy()
        df_no  = df[~_has_ord].copy()
        n_a, n_b = len(df_ord), len(df_no)
        if n_a:
            df_ord['_hasutm'] = (df_ord['utm_term'].astype(str).str.len() > 0).astype(int)
            df_ord = (df_ord.sort_values(['order_no','_hasutm','revenue'], ascending=[True, False, False])
                            .drop_duplicates(subset=['order_no'], keep='first')
                            .drop(columns=['_hasutm']))
        if n_b:
            if 'insert_id' in df_no.columns:
                _a = df_no[df_no['insert_id'].astype(str).str.len() > 0].drop_duplicates(subset=['insert_id'], keep='first')
                _b = df_no[df_no['insert_id'].astype(str).str.len() == 0]
                df_no = pd.concat([_a, _b], ignore_index=True)
            df_no = df_no.drop_duplicates(subset=['date','distinct_id','revenue','서비스','utm_term'], keep='first')
        df_d = pd.concat([df_ord, df_no], ignore_index=True)
        log.info(f"  주문번호 dedup: 주문있음 {n_a}->{len(df_ord)} · 주문없음 {n_b}->{len(df_no)} · 합계 {len(df_d)}")

        # 2) utm_term 백필 — 크로스셀 회수 (라스트터치 · 24h) · 세트와 동일 규칙.
        #   상속할 때 그 접점 결제의 utm_content(소재)도 함께 상속한다(세트·소재 귀속이 같은 접점에서 나오도록).
        BACKFILL_WINDOW_SEC = 86400
        if 'ts' not in df_d.columns: df_d['ts'] = 0
        df_d = df_d.reset_index(drop=True)
        df_d['_ismeta'] = (df_d['_src'] == 'meta') & (df_d['utm_term'].astype(str).str.len() > 0)
        _s = df_d.sort_values(['distinct_id', 'ts'], kind='mergesort').reset_index(drop=False)
        _T = _s['utm_term'].astype(str).tolist(); _C = _s['utm_content'].astype(str).tolist()
        _TS = _s['ts'].fillna(0).astype('int64').tolist()
        _D = _s['distinct_id'].astype(str).tolist(); _M = _s['_ismeta'].tolist(); _IX = _s['index'].tolist()
        _ld = None; _lt = None; _lc = None; _lts = None; _rec = {}
        for _i in range(len(_s)):
            _d = _D[_i]
            if _d != _ld: _ld = _d; _lt = None; _lc = None; _lts = None
            if _d in ('', 'None', 'nan', 'null'): continue
            if _T[_i]:
                if _M[_i] and _TS[_i] > 0: _lt = _T[_i]; _lc = _C[_i]; _lts = _TS[_i]  # 라스트터치 갱신
            else:
                if _lt and _TS[_i] > 0 and _lts and 0 <= _TS[_i] - _lts <= BACKFILL_WINDOW_SEC: _rec[_IX[_i]] = (_lt, _lc)
        _rec_rev = float(df_d.loc[list(_rec.keys()), 'revenue'].sum()) if _rec else 0.0
        for _oi, (_t, _c) in _rec.items():
            df_d.at[_oi, 'utm_term'] = _t
            if not df_d.at[_oi, 'utm_content']: df_d.at[_oi, 'utm_content'] = _c
        log.info(f"  🔗 크로스셀 백필(라스트터치·24h): {len(_rec)}건 회수 · 매출 {_rec_rev:,.0f} local")

        before_n = len(df_d)
        df_d = df_d[df_d['utm_term'].astype(str).str.len() > 0]
        log.info(f"  utm_term filter: {before_n} -> {len(df_d)} ({before_n - len(df_d)}건 미회수 organic 제외)")

        # 한국(South Korea) 결제 제외 (2026-05-27) — 세트와 동일. country 미상은 유지.
        if 'country' in df_d.columns:
            before_kr = len(df_d)
            _is_kr = df_d['country'].astype(str).str.strip().str.upper().isin(KOREA_CC)
            df_d = df_d[~_is_kr]
            log.info(f"  한국(KR) 제외: {before_kr} -> {len(df_d)} ({before_kr - len(df_d)}건 South Korea 결제 제외)")

        if 'country' not in df_d.columns: df_d['country'] = ''
        df_d['country'] = df_d['country'].fillna('').astype(str).str.strip().str.upper()

        # ★ 통화 확정 — 세트 로더와 동일: ① MP '통화' 프라퍼티 → ② 결제의 세트(utm_term) 캠페인명 키워드.
        _src_stat = {'explicit': 0, 'campaign': 0}
        def _resolve_cur(row):
            ce = str(row.get('cur_explicit') or '').strip().upper()
            if ce in KNOWN_CURRENCIES:
                _src_stat['explicit'] += 1; return ce
            cn = ADSET_CAMPAIGN_NAME.get(str(row.get('utm_term') or ''), '')
            _src_stat['campaign'] += 1
            return detect_currency('', cn, None)   # 캠페인명 hk/tw → HKD/TWD (없으면 TWD)
        df_d['cur_eff'] = df_d.apply(_resolve_cur, axis=1)
        df_d['rev_usd'] = df_d.apply(lambda r: local_to_usd(float(r.get('revenue') or 0), r['cur_eff'], r['date']), axis=1)
        log.info(f"  💱 건별통화 출처: {_src_stat}")

        # 3) 세트 안에서 소재 배분 — utm_content 가 그 세트의 그날 지출 소재면 그 소재, 아니면 '(소재 미상)'
        df_d['ad_key'] = [c if (c and AD_DAY_ADSET.get((d, c)) == t) else UNATTR_PREFIX + t
                          for d, t, c in zip(df_d['date'], df_d['utm_term'].astype(str), df_d['utm_content'].astype(str))]
        # 진단 로그 — 실제 행이 되는 범위(그날 Meta 행이 있는 세트)만 센다(창 밖 날짜·지출 없는 세트 결제는 세트·소재 모두 버림)
        _SETDAY = {(d, a) for (d, _ad), a in AD_DAY_ADSET.items()}
        _inrow = [(d, t) in _SETDAY for d, t in zip(df_d['date'], df_d['utm_term'].astype(str))]
        _un = df_d['ad_key'].str.startswith(UNATTR_PREFIX) & pd.Series(_inrow, index=df_d.index)
        _rv = df_d.loc[_un, 'rev_usd'].sum(); _tv = df_d.loc[pd.Series(_inrow, index=df_d.index), 'rev_usd'].sum()
        log.info(f"  🧩 소재 배분(행 대상 {sum(_inrow)}건): (소재 미상) {int(_un.sum())}건 · ${float(_rv):,.0f} ({(_rv / _tv * 100) if _tv else 0:.1f}%)")
        for (d, ak, cc), v in df_d.groupby(['date','ad_key','country'])['rev_usd'].sum().items():
            if d and ak: mp_value_map[(d, str(ak), str(cc))] = v   # ★ USD 단위
        for (d, ak, cc), c in df_d.groupby(['date','ad_key','country']).size().items():
            if d and ak: mp_count_map[(d, str(ak), str(cc))] = c

    # 4-0) Mixpanel 수집 실패 날짜의 기존 매출 보존맵 로드 (0 덮어쓰기 방지)
    prev_map = {}
    if uncovered:
        _dlist = ",".join(sorted(uncovered))
        log.warning(f"  🛡️ 보존모드: {sorted(uncovered)} — 기존 매출/건수 유지(0 덮어쓰기 방지)")
        _poff = 0
        while True:  # PostgREST 1000행 캡 → 페이지네이션 (장기 백필 시 uncovered 일수×소재수 초과 — 2026-07-13 세트별 사고와 동일 패턴)
            _pchunk = sb.select("global_ad_creative_daily",
                                f"select=date,ad_id,revenue_usd,results_mp&date=in.({_dlist})"
                                f"&order=date.asc,ad_id.asc&limit=1000&offset={_poff}")
            if not isinstance(_pchunk, list) or not _pchunk:
                break
            for e in _pchunk:
                prev_map[(str(e.get('date')), str(e.get('ad_id')))] = (
                    float(e.get('revenue_usd') or 0.0), int(e.get('results_mp') or 0))
            if len(_pchunk) < 1000:
                break
            _poff += 1000
        log.info(f"  🛡️ 보존맵: {len(prev_map)}행 로드")

    # 4) 병합
    log.info(f"\n4단계: 병합")
    # (date, ad_id) → 결제 발생 country 집합 — 매출을 country별로 쪼개 통화 보정 적용
    mp_idx = {}
    for (d2, a2, cc2) in mp_value_map:
        mp_idx.setdefault((d2, a2), set()).add(cc2)

    records = []
    for dk, rows in meta_data.items():
        parts = dk.split('/'); iso_date = f"20{parts[0]}-{parts[1]}-{parts[2]}"
        for mr in rows:
            ad_id = mr['ad_id']
            if not ad_id: continue
            spend = mr['spend']  # USD
            currency = detect_currency(mr['adset_name'], mr['campaign_name'], mr.get('ad_account_id'))
            country = CURRENCY_TO_COUNTRY.get(currency, '글로벌')
            # ★ mp_value_map 은 건별 실제통화로 이미 USD 환산됨 → country별 합산만 (재환산 없음).
            mpc = 0; revenue = 0.0
            for cc in mp_idx.get((dk, ad_id), set()):
                revenue += float(mp_value_map.get((dk, ad_id, cc), 0.0))
                mpc += mp_count_map.get((dk, ad_id, cc), 0)
            # 🛡️ 이 날짜 Mixpanel 수집 실패 → 기존 매출/건수 보존 (0 덮어쓰기 방지). 지출은 신규 반영.
            if iso_date in uncovered:
                _pv = prev_map.get((iso_date, str(ad_id)))
                if _pv: revenue, mpc = _pv[0], _pv[1]
            profit = revenue - spend
            roas = (revenue / spend * 100) if spend > 0 else 0
            cvr = (mpc / mr['unique_clicks'] * 100) if mr['unique_clicks'] > 0 and mpc > 0 else 0
            budget_raw = budget_map.get(mr['adset_id'], 0)
            budget_val = round(budget_raw / 100, 2) if budget_raw > 0 else 0
            product = extract_product(mr['adset_name'], mr['campaign_name'])

            records.append({
                'date': iso_date, 'ad_id': ad_id,
                'campaign_name': mr['campaign_name'], 'adset_name': mr['adset_name'],
                'adset_id': mr['adset_id'], 'ad_name': mr['ad_name'],
                'ad_account_id': mr['ad_account_id'], 'product': product,
                'country': country, 'currency': currency,
                'spend_usd': round(spend, 2), 'cost_per_result': round(mr['cost_per_result'], 2),
                'purchase_roas_meta': round(mr['meta_roas'], 4),
                'cpm': round(mr['cpm'], 2), 'reach': mr['reach'], 'impressions': mr['impressions'],
                'unique_clicks': int(mr['unique_clicks']), 'unique_ctr': round(mr['unique_ctr'], 4),
                'cost_per_click': round(mr['cost_per_click'], 2), 'frequency': round(mr['frequency'], 4),
                'results_meta': int(mr['results_meta']), 'results_mp': mpc,
                'revenue_usd': round(revenue, 2), 'profit_usd': round(profit, 2),
                'roas': round(roas, 2), 'cvr': round(cvr, 4), 'budget_usd': budget_val,
            })
    # 4-1) (소재 미상) 행 — 세트에는 귀속됐지만 그날 지출 소재로 못 붙인 매출. 지출 0, 매출/건수만.
    #   세트 로더는 그날 Meta 행이 있는 세트에만 매출을 싣는다 → 같은 조건(그날 소재 행이 있는 세트)에서만 만든다.
    n_unattr = 0
    for dk, rows in meta_data.items():
        parts = dk.split('/'); iso_date = f"20{parts[0]}-{parts[1]}-{parts[2]}"
        rep_by_adset = {}
        for mr in rows:
            if mr.get('adset_id'): rep_by_adset.setdefault(str(mr['adset_id']), mr)
        for asid, r0 in rep_by_adset.items():
            ak = UNATTR_PREFIX + asid
            mpc = 0; revenue = 0.0
            for cc in mp_idx.get((dk, ak), set()):
                revenue += float(mp_value_map.get((dk, ak, cc), 0.0))
                mpc += mp_count_map.get((dk, ak, cc), 0)
            if iso_date in uncovered:
                _pv = prev_map.get((iso_date, ak))
                if _pv: revenue, mpc = _pv[0], _pv[1]
            if revenue <= 0 and mpc <= 0: continue
            currency = detect_currency(r0['adset_name'], r0['campaign_name'], r0.get('ad_account_id'))
            budget_raw = budget_map.get(r0['adset_id'], 0)
            records.append({
                'date': iso_date, 'ad_id': ak,
                'campaign_name': r0['campaign_name'], 'adset_name': r0['adset_name'],
                'adset_id': r0['adset_id'], 'ad_name': UNATTR_AD_NAME,
                'ad_account_id': r0['ad_account_id'], 'product': extract_product(r0['adset_name'], r0['campaign_name']),
                'country': CURRENCY_TO_COUNTRY.get(currency, '글로벌'), 'currency': currency,
                'spend_usd': 0, 'cost_per_result': 0, 'purchase_roas_meta': 0,
                'cpm': 0, 'reach': 0, 'impressions': 0, 'unique_clicks': 0, 'unique_ctr': 0,
                'cost_per_click': 0, 'frequency': 0, 'results_meta': 0, 'results_mp': mpc,
                'revenue_usd': round(revenue, 2), 'profit_usd': round(revenue, 2),
                'roas': 0, 'cvr': 0, 'budget_usd': round(budget_raw / 100, 2) if budget_raw > 0 else 0,
            })
            n_unattr += 1
    log.info(f"✅ 레코드: {len(records)}개 (그중 (소재 미상) {n_unattr}행)")

    # 5) Supabase upsert
    log.info(f"\n5단계: Supabase upsert ({len(records)}행)")
    if records:
        sb.upsert("global_ad_creative_daily", records)

    # 5-1) 이번 회차에 안 생긴 옛 (소재 미상) 행 정리 — 매출이 소재로 옮겨갔거나 사라진 경우 stale 방지.
    #   수집 실패(uncovered) 날짜는 건드리지 않는다.
    _keep = {(r['date'], r['ad_id']) for r in records if str(r['ad_id']).startswith(UNATTR_PREFIX)}
    _ps = DATA_REFRESH_START.strftime('%Y-%m-%d'); _pe = TODAY.strftime('%Y-%m-%d')
    _old = []; _poff = 0
    while True:
        _chunk = sb.select("global_ad_creative_daily",
                           f"select=date,ad_id&ad_id=like.{UNATTR_PREFIX}*&date=gte.{_ps}&date=lte.{_pe}"
                           f"&order=date.asc,ad_id.asc&limit=1000&offset={_poff}")
        if not isinstance(_chunk, list) or not _chunk: break
        _old.extend(_chunk)
        if len(_chunk) < 1000: break
        _poff += 1000
    _stale = defaultdict(list)
    for e in _old:
        k = (str(e.get('date')), str(e.get('ad_id')))
        if k not in _keep and k[0] not in uncovered: _stale[k[0]].append(k[1])
    for _d, _ids in _stale.items():
        for i in range(0, len(_ids), 100):
            sb.delete("global_ad_creative_daily", f"date=eq.{_d}&ad_id=in.({','.join(_ids[i:i+100])})")
    if _stale:
        log.info(f"  🧹 stale (소재 미상) 행 삭제: {sum(len(v) for v in _stale.values())}행")

    log.info("\n" + "=" * 60)
    log.info("✅ 글로벌 소재별 파이프라인 완료!")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
