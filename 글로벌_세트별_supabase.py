# -*- coding: utf-8 -*-
"""
글로벌_세트별_supabase.py
========================
글로벌 Meta Ads + Mixpanel + Stripe → Supabase 직통 파이프라인

Meta: USD 기준 (act_1054, act_2677)
Mixpanel: 현지통화 → USD 환산
Stripe: 국가별(대만/홍콩/일본) 매출 → USD/KRW

환경변수:
  META_TOKEN_1, META_TOKEN_GlobalTT (or META_TOKEN_4, META_TOKEN_3)
  MIXPANEL_PROJECT_ID, MIXPANEL_USERNAME, MIXPANEL_SECRET
  STRIPE_API_KEY
  SUPABASE_URL, SUPABASE_SERVICE_KEY
  REFRESH_DAYS (기본 10), FULL_REFRESH (true/false)
"""

import os, sys, json, time, re, math, logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
import requests as req_lib

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
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

# Meta 채널 판별 (utm_source 화이트리스트) — 타채널(google 등) 결제가 직전 Meta 방문의
# stale utm_term(세트 id)을 달고 들어와 Meta 세트 매출로 잘못 합산되는 것을 차단.
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

STRIPE_API_KEY = os.environ.get("STRIPE_API_KEY", "")

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None)
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
FULL_REFRESH_START = datetime(2025, 12, 1)
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))

if FULL_REFRESH:
    REFRESH_DAYS = (TODAY - FULL_REFRESH_START).days + 1
    log.info(f"🔥 FULL_REFRESH: {FULL_REFRESH_START:%Y-%m-%d} ~ 오늘 ({REFRESH_DAYS}일)")

DATA_REFRESH_START = TODAY - timedelta(days=REFRESH_DAYS - 1)

# 환율 폴백
FALLBACK_RATES = {"TWD": 32.0, "JPY": 155.0, "HKD": 7.8, "KRW": 1450.0, "USD": 1.0, "THB": 35.5}
CURRENCY_TO_COUNTRY = {"TWD": "대만", "JPY": "일본", "HKD": "홍콩", "KRW": "한국", "USD": "글로벌", "THB": "태국"}
STRIPE_CURRENCY_MAP = {"twd": "TW", "hkd": "HK", "jpy": "JP", "usd": "GLOBAL", "krw": "KR", "thb": "TH"}
STRIPE_COUNTRY_NAMES = {"TW": "대만", "HK": "홍콩", "JP": "일본", "GLOBAL": "글로벌(USD)", "KR": "한국(KRW)", "TH": "태국"}
STRIPE_DIVISOR = {"jpy": 1, "twd": 100, "hkd": 100, "usd": 100, "krw": 1, "thb": 100}


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
    #     KRW 분기를 제거. 시장은 jp/hk/th/tw 키워드로만 판별, 없으면 계정 기본통화.
    for name in [adset_name, campaign_name]:
        if not name: continue
        n = str(name); nl = n.lower()
        parts = re.split(r'[-_\s]', nl)
        if "jp" in parts or "japan" in parts or "일본" in n: return "JPY"
        if "hk" in parts or "hongkong" in parts or "홍콩" in n: return "HKD"
        if "th" in parts or "thailand" in parts or "태국" in n: return "THB"
        if "tw" in parts or "taiwan" in parts or "대만" in n or "台灣" in n: return "TWD"
    return ACCOUNT_CURRENCY.get(account_id, "TWD")


# 서비스 접미사 → 통화 (스토어프론트 기준, 캠페인명 detect_currency 보다 신뢰도 높음).
#   접미사 -tw → TWD, -th → THB. (HK/JP/US 등 해외 고객도 -tw 스토어프론트는 TWD 결제)
SUFFIX_CURRENCY = {"tw": "TWD", "th": "THB", "jp": "JPY", "hk": "HKD"}

def market_suffix(svc):
    m = re.search(r'-([a-z]{2,3})$', str(svc or "").strip().lower())
    return m.group(1) if m else ""

def currency_from_suffix(svc):
    # 접미사 없으면 None → 호출부가 detect_currency/account 로 폴백
    return SUFFIX_CURRENCY.get(market_suffix(svc))


# =========================================================
# 환율 조회
# =========================================================
usd_rates = {}  # {currency: {date_key: rate}}

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
# Meta API
# =========================================================
def get_token(acc_id):
    return META_TOKENS.get(acc_id, META_TOKEN_DEFAULT)

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
            if resp.status_code in [429,500,502,503]:
                time.sleep(30 + attempt * 30); continue
            return None
        except Exception as e:
            if attempt < 4: time.sleep(15)
            else: return None
    return None

def fetch_meta_insights_daily(ad_account_id, single_date):
    url = f"{META_BASE_URL}/{ad_account_id}/insights"
    fields = "campaign_name,adset_name,adset_id,spend,cpm,reach,impressions,frequency,actions,cost_per_action_type,purchase_roas,unique_outbound_clicks,unique_outbound_clicks_ctr,cost_per_unique_outbound_click"
    params = {'fields':fields,'level':'adset','breakdowns':'country','time_increment':1,
        'time_range':json.dumps({'since':single_date,'until':single_date}),
        # 계정 기본 윈도우와 동일 → actions.value(=results_meta) 불변, 7d_click 키만 추가
        'action_attribution_windows':json.dumps(['7d_click','1d_view']),
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

def _extract_action(al, types):
    if not al: return 0
    for a in al:
        if a.get('action_type','') in types:
            try: return float(a.get('value',0))
            except: return 0
    return 0

def _extract_action_window(al, types, window_key):
    """특정 어트리뷰션 윈도우(예: '7d_click') 값만 — 뷰스루/클릭 분리용."""
    if not al: return 0
    for a in al:
        if a.get('action_type','') in types:
            try: return float(a.get(window_key,0) or 0)
            except: return 0
    return 0

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
# Mixpanel
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
                    ut = None
                    for k in ['utm_term','UTM_Term','UTM Term']:
                        if k in props and props[k]: ut = clean_id(str(props[k]).strip()); break
                    us = ''
                    for k in ['utm_source','UTM_Source','UTM Source']:
                        if k in props and props[k]: us = str(props[k]).strip(); break
                    raw_a = props.get('결제금액') or props.get('amount')
                    raw_v = props.get('value')
                    a_val = float(raw_a) if raw_a else 0.0
                    v_val = float(raw_v) if raw_v else 0.0
                    revenue = a_val if a_val > 0 else (v_val if v_val > 0 else 0.0)
                    svc = props.get('서비스','')
                    # mp_country_code: 결제 국가 (ISO). country 분할의 기준값.
                    country = str(props.get('mp_country_code') or '').strip().upper()
                    # 통화: 서비스 접미사 기준(-tw=TWD,-th=THB). 없으면 빈값 → 레코드 빌드에서 폴백.
                    cur = currency_from_suffix(svc) or ''
                    # 주문번호: 대만=merchant_uid/주문번호, 한국=order_id (없으면 imp_uid). 중복 결제 판단의 1차 키.
                    order_no = props.get('merchant_uid') or props.get('주문번호') or props.get('order_id') or props.get('imp_uid') or ''
                    data.append({'distinct_id':props.get('distinct_id'),'date':ds,'utm_term':ut or '','utm_source':us or '','revenue':revenue,'서비스':svc,'insert_id':props.get('$insert_id') or props.get('insert_id') or '','order_no':str(order_no).strip(),'country':country,'currency':cur})
                except: pass
            log.info(f"  ✅ 파싱: {len(data)}건")
            return data
        except Exception as e:
            # 타임아웃/연결오류 → 즉시 포기하지 말고 백오프 후 재시도 (대용량 export 간헐 타임아웃 대응)
            log.warning(f"  ⚠️ Mixpanel 오류(시도 {attempt+1}/4): {e}")
            if attempt < 3: time.sleep(30 + attempt * 30); continue
            return None
    return None  # 429/타임아웃 4회 소진 — 수집 실패로 간주


# =========================================================
# Stripe
# =========================================================
def fetch_stripe_revenue(start_date, end_date):
    if not STRIPE_API_KEY:
        log.warning("  ⚠️ STRIPE_API_KEY 없음 — Stripe 수집 건너뜀")
        return {}
    try:
        import stripe
    except ImportError:
        log.warning("  ⚠️ stripe 패키지 없음")
        return {}
    stripe.api_key = STRIPE_API_KEY
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())
    all_charges = []; has_more = True; starting_after = None
    log.info(f"  💳 Stripe: {start_date:%Y-%m-%d} ~ {end_date:%Y-%m-%d}")
    while has_more:
        params = {"limit": 100, "created": {"gte": start_ts, "lte": end_ts}, "status": "succeeded"}
        if starting_after: params["starting_after"] = starting_after
        response = stripe.Charge.list(**params)
        all_charges.extend(response.data)
        has_more = response.has_more
        if response.data: starting_after = response.data[-1].id
    log.info(f"  💳 총 {len(all_charges)}건")

    # 국가별 일별 집계
    revenue = defaultdict(lambda: defaultdict(float))  # country -> date -> KRW
    for ch in all_charges:
        currency = (getattr(ch, 'currency', '') or '').lower()
        if currency not in STRIPE_DIVISOR: continue
        charge_dt = datetime.fromtimestamp(ch.created, tz=KST)
        date_str = charge_dt.strftime('%Y-%m-%d')
        dk = make_date_key(charge_dt)
        # 통화 기준으로만 국가 분류 (billing address 국가 무시)
        # USD/기타 통화 결제는 모두 제외 — JPY/TWD/HKD 만 집계
        country_code = STRIPE_CURRENCY_MAP.get(currency)
        if country_code not in STRIPE_COUNTRY_NAMES: continue
        country_name = STRIPE_COUNTRY_NAMES[country_code]
        divisor = STRIPE_DIVISOR.get(currency, 100)
        amount_local = ch.amount / divisor
        # KRW 환산
        krw_rates = usd_rates.get('KRW', {})
        krw_rate = get_rate(krw_rates, dk, 1450)
        if currency == 'usd':
            amount_krw = round(amount_local * krw_rate)
        else:
            local_rates = usd_rates.get(currency.upper(), {})
            usd_to_local = get_rate(local_rates, dk, FALLBACK_RATES.get(currency.upper(), 1))
            amount_usd = amount_local / usd_to_local if usd_to_local > 0 else 0
            amount_krw = round(amount_usd * krw_rate)
        revenue[country_name][date_str] += amount_krw
    return revenue


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
        """읽기 전용 GET. 실패 시 [] 반환 (보존 로직은 빈 결과를 '보존 불가'로 자연 처리)."""
        url = f"{self.base_url}/rest/v1/{table}?{query}"
        try:
            resp = req_lib.get(url, headers=self.headers, timeout=60)
            if resp.status_code == 200: return resp.json()
            log.error(f"  ❌ select: HTTP {resp.status_code} | {resp.text[:200]}")
        except Exception as e:
            log.error(f"  ❌ select 예외: {e}")
        return []

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
    log.info("🌏 글로벌 Meta + Mixpanel + Stripe → Supabase")
    log.info("=" * 60)
    log.info(f"📅 갱신: {DATA_REFRESH_START:%Y-%m-%d} ~ 오늘 ({REFRESH_DAYS}일)")

    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)

    # 1) 환율 조회
    log.info("\n1단계: 환율 조회")
    rate_start = DATA_REFRESH_START - timedelta(days=7)
    for curr in ["TWD", "JPY", "HKD", "KRW", "THB"]:
        usd_rates[curr] = fetch_usd_rates(rate_start, TODAY, curr)

    # 2) Meta Insights
    log.info(f"\n2단계: Meta Insights ({REFRESH_DAYS}일 × {len(ALL_AD_ACCOUNTS)}계정)")
    meta_data = defaultdict(list)
    for day_offset in range(REFRESH_DAYS):
        td = TODAY - timedelta(days=day_offset)
        target_str = td.strftime('%Y-%m-%d')
        dk = make_date_key(td)
        day_rows = []
        for acc_id in ALL_AD_ACCOUNTS:
            rows = fetch_meta_insights_daily(acc_id, target_str)
            if rows:
                purchase_types = ['purchase','omni_purchase','offsite_conversion.fb_pixel_purchase']
                for row in rows:
                    if float(row.get('spend',0))<=0: continue  # breakdown=country 하위 0지출행 제외
                    day_rows.append({
                        'campaign_name': row.get('campaign_name',''),
                        'adset_name': row.get('adset_name',''),
                        'adset_id': row.get('adset_id',''),
                        'ad_account_id': acc_id,
                        'country': str(row.get('country','') or 'XX').strip().upper(),  # Meta country breakdown (ISO)
                        'spend': float(row.get('spend',0)),
                        'cpm': float(row.get('cpm',0)),
                        'reach': int(float(row.get('reach',0))),
                        'impressions': int(float(row.get('impressions',0))),
                        'frequency': float(row.get('frequency',0)),
                        'results_meta': _extract_action(row.get('actions',[]), purchase_types),
                        'results_meta_click': _extract_action_window(row.get('actions',[]), purchase_types, '7d_click'),
                        'cost_per_result': _extract_action(row.get('cost_per_action_type',[]), purchase_types),
                        'unique_clicks': _extract_action(row.get('unique_outbound_clicks',[]), ['outbound_click']),
                        'unique_ctr': _extract_action(row.get('unique_outbound_clicks_ctr',[]), ['outbound_click']),
                        'cost_per_click': _extract_action(row.get('cost_per_unique_outbound_click',[]), ['outbound_click']),
                        'meta_roas': _extract_action(row.get('purchase_roas',[]), purchase_types),
                        'date_key': dk, 'date_obj': td,
                    })
            time.sleep(1)
        if day_rows:
            meta_data[dk] = day_rows
            log.info(f"  📊 {dk}: {len(day_rows)}건")
    log.info(f"✅ Meta: {sum(len(v) for v in meta_data.values())}건")

    # 2.5) 예산
    log.info("\n2.5단계: 예산 조회")
    budget_map = {}
    for acc_id in ALL_AD_ACCOUNTS:
        budget_map.update(fetch_adset_budgets(acc_id))
        time.sleep(1)
    log.info(f"✅ 예산: {len(budget_map)}개")

    time.sleep(30)  # Meta rate limit cooldown

    # 3) Mixpanel
    log.info(f"\n3단계: Mixpanel ({REFRESH_DAYS}일)")
    YESTERDAY = TODAY - timedelta(days=1)
    mp_raw = []
    # 수집 실패한 날짜(iso) 집합 — 이 날짜는 매출을 0으로 덮어쓰지 않고 기존 값 보존 (2026-06-08)
    #   배경: 과거 구간 export 가 timeout/429/non-200 으로 실패하면 그대로 매출 0 업서트되어
    #         '지출은 있는데 매출 0' 구간이 생김(추이차트 0). 실패와 '실제 결제 없음'을 구분해 방지.
    uncovered = set()
    def _mark_uncovered(s, e):
        d = s
        while d <= e:
            uncovered.add(d.strftime('%Y-%m-%d')); d += timedelta(days=1)
    # 과거 구간: 7일 청크로 분할 (대용량 응답 timeout 위험 완화 · 실패한 청크만 보존모드)
    chunk_start = DATA_REFRESH_START
    while chunk_start <= YESTERDAY:
        chunk_end = min(chunk_start + timedelta(days=6), YESTERDAY)
        res = fetch_mixpanel_data(chunk_start.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d'))
        if res is None:
            log.error(f"  ❌ Mixpanel 수집 실패: {chunk_start:%Y-%m-%d}~{chunk_end:%Y-%m-%d} → 해당 날짜 기존 매출 보존")
            _mark_uncovered(chunk_start, chunk_end)
        else:
            mp_raw.extend(res)
        chunk_start = chunk_end + timedelta(days=1)
    today_res = fetch_mixpanel_data(TODAY.strftime('%Y-%m-%d'), TODAY.strftime('%Y-%m-%d'))
    if today_res is None:
        log.error(f"  ❌ Mixpanel 수집 실패: 오늘({TODAY:%Y-%m-%d}) → 기존 매출 보존")
        _mark_uncovered(TODAY, TODAY)
    else:
        mp_raw.extend(today_res)
    log.info(f"✅ Mixpanel: {len(mp_raw)}건" + (f" · ⚠️ 수집실패 보존 {len(uncovered)}일" if uncovered else ""))

    # Mixpanel 집계
    import pandas as pd
    mp_value_map = {}; mp_count_map = {}; utm_currency = {}
    if mp_raw:
        df = pd.DataFrame(mp_raw)

        # ▼ Meta 채널 결제만 귀속 (google 등 타채널 stale utm_term 오염 차단)
        _bn = len(df); df = df[df['utm_source'].apply(is_meta_source)]
        log.info(f"  🔵 Meta 소스 필터: {_bn} → {len(df)}건 (비-Meta {_bn-len(df)}건 제외)")

        def _norm(x):
            s = str(x).strip() if x is not None else ''
            return '' if s.lower() in ('', 'none', 'undefined', 'null') else s
        df['utm_term'] = df['utm_term'].apply(_norm)

        # 1) 중복 결제 dedup — 주문번호(order_no) 우선 (2026-06-08)
        #    같은 주문번호 = 같은 주문 = 1건. Mixpanel 은 같은 결제를 다른 $insert_id 로
        #    평균 1.7회 재기록해 $insert_id dedup 만으론 과대귀속됨 → 주문번호로 주문단위 dedup.
        #    order_no: 대만=merchant_uid/주문번호, 한국=order_id.
        if 'order_no' not in df.columns: df['order_no'] = ''
        df['order_no'] = df['order_no'].fillna('').astype(str).str.strip()
        _has_ord = df['order_no'].str.len() > 0
        df_ord = df[_has_ord].copy()
        df_no  = df[~_has_ord].copy()
        n_a, n_b = len(df_ord), len(df_no)
        # (A) 주문번호 있는 행: 주문단위 1건 — utm_term 보존 우선, 그다음 revenue 큰 행
        if n_a:
            df_ord['_hasutm'] = (df_ord['utm_term'].astype(str).str.len() > 0).astype(int)
            df_ord = (df_ord.sort_values(['order_no','_hasutm','revenue'], ascending=[True, False, False])
                            .drop_duplicates(subset=['order_no'], keep='first')
                            .drop(columns=['_hasutm']))
        # (B) 주문번호 없는 행(드묾): $insert_id → (date,distinct_id,revenue,서비스,utm_term) 복합키 fallback
        if n_b:
            if 'insert_id' in df_no.columns:
                _a = df_no[df_no['insert_id'].astype(str).str.len() > 0].drop_duplicates(subset=['insert_id'], keep='first')
                _b = df_no[df_no['insert_id'].astype(str).str.len() == 0]
                df_no = pd.concat([_a, _b], ignore_index=True)
            df_no = df_no.drop_duplicates(subset=['date','distinct_id','revenue','서비스','utm_term'], keep='first')
        df_d = pd.concat([df_ord, df_no], ignore_index=True)
        log.info(f"  주문번호 dedup: 주문있음 {n_a}->{len(df_ord)} · 주문없음 {n_b}->{len(df_no)} · 합계 {len(df_d)}")

        # === 진단: 서비스별 organic 비율 (글로벌 시장 식별용 · 결과 미변경) ===
        # 제품(서비스)별로 결제통화가 일정하므로 통화 오염 없이 organic 비율 비교 가능.
        try:
            _t = df_d.copy()
            _t['_hasutm'] = _t['utm_term'].astype(str).str.len() > 0
            g_all = _t.groupby('서비스')['revenue'].sum()
            g_utm = _t[_t['_hasutm']].groupby('서비스')['revenue'].sum()
            g_cnt = _t.groupby('서비스').size()
            log.info("  ── 서비스별 진단 (organic 비율, local통화) ──")
            for svc in g_all.sort_values(ascending=False).head(15).index:
                tot = float(g_all.get(svc, 0)); utm = float(g_utm.get(svc, 0)); cnt = int(g_cnt.get(svc, 0))
                pct = (utm / tot * 100) if tot else 0
                log.info(f"   [{svc!r}] 건수 {cnt} 매출 {tot:,.0f} · utm있음 {utm:,.0f} ({pct:.0f}%) · organic {tot-utm:,.0f}")
        except Exception as e:
            log.warning(f"  서비스 진단 skip: {e}")

        # 2) utm_term backfill 비활성화 (2026-05-06)
        # 이유: backfill이 같은 user의 organic 결제까지 광고에 귀속시켜
        #       추이차트 매출이 Stripe 실결제를 초과하는 over-attribution 유발.
        before_n = len(df_d); before_rev = float(df_d['revenue'].sum())
        df_d = df_d[df_d['utm_term'].astype(str).str.len() > 0]
        after_rev = float(df_d['revenue'].sum())
        log.info(f"  utm_term filter: {before_n} -> {len(df_d)} ({before_n - len(df_d)}건 organic 제외 · 매출 {before_rev:,.0f} -> {after_rev:,.0f} local, organic drop {before_rev - after_rev:,.0f})")

        # 3) (구) Logical payment dedup — 주문번호 dedup(1단계)으로 대체됨 (2026-06-08).
        #    주문번호 기반이 더 정확(같은 날 같은 금액 별개 주문을 잘못 합치지 않음).
        #    주문번호 없는 행만 1단계 (B) 에서 복합키 fallback 으로 처리.

        # 4) 한국(South Korea) 결제 제외 (2026-05-27)
        # 글로벌 성과는 비한국 결제만으로 측정. mp_country_code=KR(=South Korea)인 결제 제외.
        # country 미상(None/빈값)은 '한국으로 나온 것'이 아니므로 유지.
        if 'country' in df_d.columns:
            before_kr = len(df_d)
            _is_kr = df_d['country'].astype(str).str.strip().str.upper().isin(KOREA_CC)
            df_d = df_d[~_is_kr]
            log.info(f"  한국(KR) 제외: {before_kr} -> {len(df_d)} ({before_kr - len(df_d)}건 South Korea 결제 제외)")

        # country 정규화 (ISO 대문자, 미상=XX) — (date, utm_term, country) 그레인 집계
        if 'country' not in df_d.columns: df_d['country'] = ''
        df_d['country'] = df_d['country'].fillna('').astype(str).str.strip().str.upper().replace('', 'XX')
        for (d, ut, cc), v in df_d.groupby(['date','utm_term','country'])['revenue'].sum().items():
            if d and ut: mp_value_map[(d, str(ut), str(cc))] = v
        for (d, ut, cc), c in df_d.groupby(['date','utm_term','country']).size().items():
            if d and ut: mp_count_map[(d, str(ut), str(cc))] = c
        # adset(utm_term)별 결제통화 — 서비스 접미사 기준(스토어프론트, adset당 일관). USD 환산에 사용.
        if 'currency' in df_d.columns:
            _cc = df_d[df_d['currency'].astype(str).str.len() > 0]
            for ut, cur in _cc.groupby('utm_term')['currency'].agg(lambda s: s.value_counts().index[0]).items():
                if ut: utm_currency[str(ut)] = str(cur)

    # 4) Stripe
    log.info(f"\n4단계: Stripe 매출")
    stripe_start = datetime(DATA_REFRESH_START.year, DATA_REFRESH_START.month, DATA_REFRESH_START.day, tzinfo=KST)
    stripe_end = datetime.now(KST)
    stripe_revenue = fetch_stripe_revenue(stripe_start, stripe_end)
    for country, dates in stripe_revenue.items():
        total = sum(dates.values())
        log.info(f"  {country}: ₩{total:,.0f} KRW")

    # 5-0) Mixpanel 수집 실패 날짜의 기존 매출 보존맵 로드 (0 덮어쓰기 방지)
    prev_map = {}
    if uncovered:
        _dlist = ",".join(sorted(uncovered))
        log.warning(f"  🛡️ 보존모드: {sorted(uncovered)} — 기존 매출/건수 유지(0 덮어쓰기 방지)")
        for e in sb.select("global_ad_performance_daily",
                           f"select=date,adset_id,country,revenue_usd,results_mp&date=in.({_dlist})"):
            prev_map[(str(e.get('date')), str(e.get('adset_id')), str(e.get('country') or ''))] = (
                float(e.get('revenue_usd') or 0.0), int(e.get('results_mp') or 0))
        log.info(f"  🛡️ 보존맵: {len(prev_map)}행 로드")

    # 5) 병합 — (date, adset_id, country) 그레인.
    #    지출 country = Meta breakdown(ISO), 매출 country = mp_country_code(ISO). 둘을 full-outer 합집합.
    #    통화는 서비스 접미사(스토어프론트) 기준 → USD 환산. country 는 ISO 코드 그대로 저장. 한국(KR) 제외.
    log.info(f"\n5단계: 병합 (country 분할)")
    records = []
    matched_local = 0.0  # 진단용: adset_id 매칭에 성공해 귀속된 mp 매출(local) 합
    # mp (date, adset) → [country...] 인덱스
    mp_idx = defaultdict(set)
    for (d2, a2, cc2) in mp_value_map:
        mp_idx[(d2, a2)].add(cc2)
    for dk, rows in meta_data.items():
        parts = dk.split('/'); iso_date = f"20{parts[0]}-{parts[1]}-{parts[2]}"
        # adset_id → {country: meta_row}, 대표행
        by_adset = defaultdict(dict); rep = {}
        for mr in rows:
            asid = mr['adset_id']
            if not asid: continue
            by_adset[asid][mr['country']] = mr
            rep.setdefault(asid, mr)
        for asid, cc_rows in by_adset.items():
            r0 = rep[asid]
            # 통화: 서비스 접미사 우선, 없으면 캠페인명/계정 폴백
            currency = utm_currency.get(asid) or detect_currency(r0['adset_name'], r0['campaign_name'], r0.get('ad_account_id'))
            mp_currency = currency if currency in ("KRW", "THB", "USD", "JPY", "HKD", "TWD") else "TWD"
            # 지출(Meta breakdown) + 매출(mp) country 합집합
            countries = set(cc_rows.keys()) | set(mp_idx.get((dk, asid), set()))
            for cc in countries:
                if str(cc).upper() in KOREA_CC: continue  # 글로벌 = 비한국 성과
                # 통화는 country 행 단위: HK 고객은 -tw 스토어프론트여도 HKD (Stripe 현지통화 청구)
                row_currency = "HKD" if str(cc).upper() == "HK" else mp_currency
                mr = cc_rows.get(cc)
                spend = mr['spend'] if mr else 0.0  # Already USD
                mpc = mp_count_map.get((dk, asid, cc), 0)
                mpv_local = mp_value_map.get((dk, asid, cc), 0.0)
                matched_local += float(mpv_local)
                revenue = local_to_usd(float(mpv_local), row_currency, dk)
                # 🛡️ Mixpanel 수집 실패일 → 기존 매출/건수 보존 (0 덮어쓰기 방지)
                if iso_date in uncovered:
                    _pv = prev_map.get((iso_date, str(asid), str(cc)))
                    if _pv and _pv[0] > 0: revenue, mpc = _pv[0], _pv[1]
                profit = revenue - spend
                roas = (revenue / spend * 100) if spend > 0 else 0
                uclk = mr['unique_clicks'] if mr else 0
                cvr = (mpc / uclk * 100) if uclk > 0 and mpc > 0 else 0
                # 예산: adset 일예산을 country 지출 비중으로 비례배분
                tot_sp = sum(x['spend'] for x in cc_rows.values())
                budget_raw = budget_map.get(asid, 0)
                budget_val = round((budget_raw / 100) * (spend / tot_sp), 2) if (budget_raw > 0 and tot_sp > 0) else 0
                product = extract_product(r0['adset_name'], r0['campaign_name'])
                records.append({
                    'date': iso_date, 'adset_id': asid,
                    'campaign_name': r0['campaign_name'], 'adset_name': r0['adset_name'],
                    'ad_account_id': r0['ad_account_id'], 'product': product,
                    'country': cc, 'currency': row_currency,  # 실효 환산통화(스토어프론트+HK예외)
                    'spend_usd': round(spend, 2), 'cost_per_result': round(mr['cost_per_result'], 2) if mr else 0,
                    'purchase_roas_meta': round(mr['meta_roas'], 4) if mr else 0,
                    'cpm': round(mr['cpm'], 2) if mr else 0, 'reach': mr['reach'] if mr else 0, 'impressions': mr['impressions'] if mr else 0,
                    'unique_clicks': int(uclk), 'unique_ctr': round(mr['unique_ctr'], 4) if mr else 0,
                    'cost_per_click': round(mr['cost_per_click'], 2) if mr else 0, 'frequency': round(mr['frequency'], 4) if mr else 0,
                    'results_meta': int(mr['results_meta']) if mr else 0, 'results_meta_click': int(mr.get('results_meta_click', 0)) if mr else 0,
                    'results_mp': mpc,
                    'revenue_usd': round(revenue, 2), 'profit_usd': round(profit, 2),
                    'roas': round(roas, 2), 'cvr': round(cvr, 4), 'budget_usd': budget_val,
                })
    log.info(f"✅ 레코드: {len(records)}개 (country 분할)")

    # === 진단 로그 (귀속 손실 추적 — 결과/업로드 미변경) ===
    attributed_usd = sum(r['revenue_usd'] for r in records)
    total_utm_local = sum(mp_value_map.values())          # utm_term 있고 비KR·dedup 후 전체 매출(local)
    unmatched_local = total_utm_local - matched_local     # ②: utm_term은 있으나 adset_id 매칭 실패분
    unmatched_pct = (unmatched_local / total_utm_local * 100) if total_utm_local > 0 else 0
    log.info("  ── 귀속 진단 (local≈TWD 기준) ──")
    log.info(f"  utm_term 매출(비KR·dedup후) 합: {total_utm_local:,.0f} local")
    log.info(f"    └ adset 매칭 성공(귀속): {matched_local:,.0f}  /  매칭 실패②: {unmatched_local:,.0f} ({unmatched_pct:.1f}%)")
    log.info(f"  최종 귀속 매출(USD): ${attributed_usd:,.0f}")

    # 6) Supabase upsert — 광고 성과
    log.info(f"\n6단계: Supabase upsert ({len(records)}행)")
    if records:
        sb.upsert("global_ad_performance_daily", records)

    # 7) Stripe → Supabase
    log.info(f"\n7단계: Stripe 매출 → Supabase")
    stripe_records = []
    for country, dates in stripe_revenue.items():
        for date_str, krw_val in dates.items():
            dk = make_date_key(datetime.strptime(date_str, '%Y-%m-%d'))
            krw_rates = usd_rates.get('KRW', {})
            krw_rate = get_rate(krw_rates, dk, 1450)
            usd_val = round(krw_val / krw_rate, 2) if krw_rate > 0 else 0
            stripe_records.append({
                'date': date_str, 'country': country,
                'revenue_local': 0, 'revenue_usd': usd_val, 'revenue_krw': round(krw_val),
                'usd_krw_rate': round(krw_rate, 2),
            })
    if stripe_records:
        sb.upsert("global_stripe_daily", stripe_records)
        log.info(f"✅ Stripe: {len(stripe_records)}행")

    # === 진단 로그: 메타 귀속 매출 vs Stripe 실매출 (동일 기간 합, USD) ===
    stripe_total_usd = sum(s['revenue_usd'] for s in stripe_records)
    if stripe_total_usd > 0:
        log.info("  ── 매출 비교 (기간 합, USD) ──")
        log.info(f"  메타 귀속 ${attributed_usd:,.0f}  vs  Stripe 실매출 ${stripe_total_usd:,.0f}  → 귀속률 {attributed_usd / stripe_total_usd * 100:.1f}%")
        log.info(f"  (주의: 이 Stripe 합은 본 스크립트 수집분=TW/HK/JP/TH, USD통화·일본 별도수집분 제외될 수 있음)")

    log.info("\n" + "=" * 60)
    log.info("✅ 글로벌 파이프라인 완료!")
    log.info("=" * 60)

if __name__ == "__main__":
    main()

