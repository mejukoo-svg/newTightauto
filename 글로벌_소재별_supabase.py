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
    params = {'fields':fields,'level':'ad','time_increment':1,
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
                    # 주문번호: 대만=merchant_uid/주문번호, 한국=order_id. 중복 결제 판단 1차 키.
                    order_no = props.get('merchant_uid') or props.get('주문번호') or props.get('order_id') or props.get('imp_uid') or ''
                    data.append({'distinct_id':props.get('distinct_id'),'date':ds,'utm_content':ut or '','utm_source':us or '','revenue':revenue,'서비스':props.get('서비스',''),'insert_id':props.get('$insert_id') or props.get('insert_id') or '','order_no':str(order_no).strip(),'country':str(country).strip()})
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
    for curr in ["TWD", "JPY", "HKD", "KRW", "THB"]:
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
                for row in rows:
                    day_rows.append({
                        'campaign_name': row.get('campaign_name',''),
                        'adset_name': row.get('adset_name',''),
                        'adset_id': row.get('adset_id',''),
                        'ad_name': row.get('ad_name',''),
                        'ad_id': row.get('ad_id',''),
                        'ad_account_id': acc_id,
                        'spend': float(row.get('spend',0)),
                        'cpm': float(row.get('cpm',0)),
                        'reach': int(float(row.get('reach',0))),
                        'impressions': int(float(row.get('impressions',0))),
                        'frequency': float(row.get('frequency',0)),
                        'results_meta': _extract_action(row.get('actions',[]), purchase_types),
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

    # Mixpanel 집계 (utm_content = ad_id)
    import pandas as pd
    mp_value_map = {}; mp_count_map = {}
    if mp_raw:
        df = pd.DataFrame(mp_raw)

        def _norm(x):
            s = str(x).strip() if x is not None else ''
            return '' if s.lower() in ('', 'none', 'undefined', 'null') else s
        df['utm_content'] = df['utm_content'].apply(_norm)

        # 0) Meta 채널 결제만 귀속 (google 등 타채널의 stale utm_content 오염 차단) — 세트와 동일
        if 'utm_source' in df.columns:
            _bn = len(df)
            df = df[df['utm_source'].apply(is_meta_source)]
            log.info(f"  🔵 Meta 소스 필터: {_bn} → {len(df)}건 (비-Meta {_bn - len(df)}건 제외)")

        # 1) 중복 결제 dedup — 주문번호(order_no) 우선 (2026-06-08)
        #    같은 주문번호 = 같은 주문 = 1건. order_no: 대만=merchant_uid/주문번호, 한국=order_id.
        if 'order_no' not in df.columns: df['order_no'] = ''
        df['order_no'] = df['order_no'].fillna('').astype(str).str.strip()
        _has_ord = df['order_no'].str.len() > 0
        df_ord = df[_has_ord].copy()
        df_no  = df[~_has_ord].copy()
        n_a, n_b = len(df_ord), len(df_no)
        # (A) 주문번호 있는 행: 주문단위 1건 — utm_content 보존 우선, 그다음 revenue 큰 행
        if n_a:
            df_ord['_hasutm'] = (df_ord['utm_content'].astype(str).str.len() > 0).astype(int)
            df_ord = (df_ord.sort_values(['order_no','_hasutm','revenue'], ascending=[True, False, False])
                            .drop_duplicates(subset=['order_no'], keep='first')
                            .drop(columns=['_hasutm']))
        # (B) 주문번호 없는 행(드묾): $insert_id → 복합키 fallback
        if n_b:
            if 'insert_id' in df_no.columns:
                _a = df_no[df_no['insert_id'].astype(str).str.len() > 0].drop_duplicates(subset=['insert_id'], keep='first')
                _b = df_no[df_no['insert_id'].astype(str).str.len() == 0]
                df_no = pd.concat([_a, _b], ignore_index=True)
            df_no = df_no.drop_duplicates(subset=['date','distinct_id','revenue','서비스','utm_content'], keep='first')
        df_d = pd.concat([df_ord, df_no], ignore_index=True)
        log.info(f"  주문번호 dedup: 주문있음 {n_a}->{len(df_ord)} · 주문없음 {n_b}->{len(df_no)} · 합계 {len(df_d)}")

        # 2) utm_content 비어있는 organic 결제 제외 (글로벌 세트 스크래퍼와 동일 정책)
        before_n = len(df_d)
        df_d = df_d[df_d['utm_content'].astype(str).str.len() > 0]
        log.info(f"  utm_content filter: {before_n} -> {len(df_d)} ({before_n - len(df_d)}건 organic 제외)")

        # 3) (구) Logical payment dedup — 주문번호 dedup(1단계)으로 대체됨 (2026-06-08).
        #    주문번호 없는 행만 1단계 (B) 에서 복합키 fallback 처리.

        # 한국(South Korea) 결제 제외 (2026-05-27)
        # 글로벌 성과는 비한국 결제만으로 측정. mp_country_code=KR(=South Korea)인 결제 제외.
        # country 미상(None/빈값)은 '한국으로 나온 것'이 아니므로 유지.
        if 'country' in df_d.columns:
            before_kr = len(df_d)
            _is_kr = df_d['country'].astype(str).str.strip().str.upper().isin(KOREA_CC)
            df_d = df_d[~_is_kr]
            log.info(f"  한국(KR) 제외: {before_kr} -> {len(df_d)} ({before_kr - len(df_d)}건 South Korea 결제 제외)")

        # country(mp_country_code) 정규화 후 (date, utm_content, country) 그레인 집계
        #   → 통화 country 보정(HK→HKD/TH→THB)을 country별로 적용하기 위함 (세트별과 정합).
        if 'country' not in df_d.columns: df_d['country'] = ''
        df_d['country'] = df_d['country'].fillna('').astype(str).str.strip().str.upper()
        for (d, ut, cc), v in df_d.groupby(['date','utm_content','country'])['revenue'].sum().items():
            if d and ut: mp_value_map[(d, str(ut), str(cc))] = v
        for (d, ut, cc), c in df_d.groupby(['date','utm_content','country']).size().items():
            if d and ut: mp_count_map[(d, str(ut), str(cc))] = c

    # 4-0) Mixpanel 수집 실패 날짜의 기존 매출 보존맵 로드 (0 덮어쓰기 방지)
    prev_map = {}
    if uncovered:
        _dlist = ",".join(sorted(uncovered))
        log.warning(f"  🛡️ 보존모드: {sorted(uncovered)} — 기존 매출/건수 유지(0 덮어쓰기 방지)")
        for e in sb.select("global_ad_creative_daily",
                           f"select=date,ad_id,revenue_usd,results_mp&date=in.({_dlist})"):
            prev_map[(str(e.get('date')), str(e.get('ad_id')))] = (
                float(e.get('revenue_usd') or 0.0), int(e.get('results_mp') or 0))
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
            # 스토어프론트(=Stripe 청구통화) 베이스 — 세트 로더와 정합.
            mp_currency = currency if currency in ("KRW", "THB", "HKD", "JPY") else "TWD"
            # 매출은 country 행 단위로 통화 보정(HK→HKD/TH→THB) 후 USD 합산 — 세트별과 동일 기준.
            mpc = 0; revenue = 0.0
            for cc in mp_idx.get((dk, ad_id), set()):
                row_cur = COUNTRY_CURRENCY_OVERRIDE.get(str(cc).upper(), mp_currency)
                v_local = mp_value_map.get((dk, ad_id, cc), 0.0)
                revenue += local_to_usd(float(v_local), row_cur, dk)
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
    log.info(f"✅ 레코드: {len(records)}개")

    # 5) Supabase upsert
    log.info(f"\n5단계: Supabase upsert ({len(records)}행)")
    if records:
        sb.upsert("global_ad_creative_daily", records)

    log.info("\n" + "=" * 60)
    log.info("✅ 글로벌 소재별 파이프라인 완료!")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
