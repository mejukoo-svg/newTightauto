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

META_TOKENS = {
    "act_1054081590008088": META_TOKEN_1,
    "act_2677707262628563": META_TOKEN_ACT_2677,
}
META_TOKEN_DEFAULT = META_TOKEN_1
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
ALL_AD_ACCOUNTS = list(META_TOKENS.keys())

MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME = os.environ.get("MIXPANEL_USERNAME", "")
MIXPANEL_SECRET = os.environ.get("MIXPANEL_SECRET", "")
MIXPANEL_EVENT_NAMES = ["결제완료", "payment_complete"]

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
FALLBACK_RATES = {"TWD": 32.0, "JPY": 155.0, "HKD": 7.8, "KRW": 1450.0, "USD": 1.0}
CURRENCY_TO_COUNTRY = {"TWD": "대만", "JPY": "일본", "HKD": "홍콩", "KRW": "한국", "USD": "글로벌"}
STRIPE_CURRENCY_MAP = {"twd": "TW", "hkd": "HK", "jpy": "JP"}
STRIPE_COUNTRY_NAMES = {"TW": "대만", "HK": "홍콩", "JP": "일본"}
STRIPE_DIVISOR = {"jpy": 1, "twd": 100, "hkd": 100, "usd": 100}


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
    "singapore","malaysia","broad","interest","lookalike","retarget","custom","asc","cbo","abo",
    "dpa","advantage","campaign","adset","ad","ads","set","purchase","conversion","traffic",
    "v1","v2","v3","v4","v5","test","new","old","copy","sajutight","ttsaju","saju","tight",
    "대만","일본","홍콩","한국","국내","글로벌","台灣","台湾","日本","香港"}

def extract_product(adset_name, campaign_name=None):
    for name in [campaign_name, adset_name]:
        if not name: continue
        parts = re.split(r'[-_\s]+', str(name).lower().strip())
        candidates = [p for p in parts if p and p not in SKIP_WORDS and len(p) > 1 and not re.match(r'^\d+$', p)]
        if candidates: return candidates[0]
    return "기타"

def detect_currency(adset_name, campaign_name=None):
    for name in [adset_name, campaign_name]:
        if not name: continue
        n = str(name); nl = n.lower()
        parts = re.split(r'[-_\s]', nl)
        if "jp" in parts or "japan" in parts or "일본" in n: return "JPY"
        if "hk" in parts or "hongkong" in parts or "홍콩" in n: return "HKD"
        if "kr" in parts or "korea" in parts or "한국" in n or "국내" in n: return "KRW"
        if "tw" in parts or "taiwan" in parts or "대만" in n or "台灣" in n: return "TWD"
    return "TWD"


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
    params = {'fields':fields,'level':'adset','time_increment':1,
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

def _extract_action(al, types):
    if not al: return 0
    for a in al:
        if a.get('action_type','') in types:
            try: return float(a.get('value',0))
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
    for attempt in range(4):
        try:
            resp = req_lib.get(url, params=params, auth=(MIXPANEL_USERNAME, MIXPANEL_SECRET), timeout=300)
            if resp.status_code == 429:
                time.sleep(30 + attempt * 30); continue
            if resp.status_code != 200: return []
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
                    raw_a = props.get('결제금액') or props.get('amount')
                    raw_v = props.get('value')
                    a_val = float(raw_a) if raw_a else 0.0
                    v_val = float(raw_v) if raw_v else 0.0
                    revenue = a_val if a_val > 0 else (v_val if v_val > 0 else 0.0)
                    data.append({'distinct_id':props.get('distinct_id'),'date':ds,'utm_term':ut or '','revenue':revenue,'서비스':props.get('서비스',''),'insert_id':props.get('$insert_id') or props.get('insert_id') or ''})
                except: pass
            log.info(f"  ✅ 파싱: {len(data)}건")
            return data
        except Exception as e:
            log.error(f"  ❌ Mixpanel 오류: {e}"); return []
    return []


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
    for curr in ["TWD", "JPY", "HKD", "KRW"]:
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
                    day_rows.append({
                        'campaign_name': row.get('campaign_name',''),
                        'adset_name': row.get('adset_name',''),
                        'adset_id': row.get('adset_id',''),
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
    if REFRESH_DAYS > 14:
        chunk_start = DATA_REFRESH_START
        while chunk_start <= YESTERDAY:
            chunk_end = min(chunk_start + timedelta(days=6), YESTERDAY)
            mp_raw.extend(fetch_mixpanel_data(chunk_start.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
            chunk_start = chunk_end + timedelta(days=1)
    else:
        if DATA_REFRESH_START <= YESTERDAY:
            mp_raw.extend(fetch_mixpanel_data(DATA_REFRESH_START.strftime('%Y-%m-%d'), YESTERDAY.strftime('%Y-%m-%d')))
    today_data = fetch_mixpanel_data(TODAY.strftime('%Y-%m-%d'), TODAY.strftime('%Y-%m-%d'))
    if today_data: mp_raw.extend(today_data)
    log.info(f"✅ Mixpanel: {len(mp_raw)}건")

    # Mixpanel 집계
    import pandas as pd
    mp_value_map = {}; mp_count_map = {}
    if mp_raw:
        df = pd.DataFrame(mp_raw)

        def _norm(x):
            s = str(x).strip() if x is not None else ''
            return '' if s.lower() in ('', 'none', 'undefined', 'null') else s
        df['utm_term'] = df['utm_term'].apply(_norm)

        # 1) $insert_id 기준 dedup (Mixpanel canonical)
        if 'insert_id' in df.columns:
            df_iid = df[df['insert_id'].astype(str).str.len() > 0]
            df_no_iid = df[df['insert_id'].astype(str).str.len() == 0]
            df_iid = df_iid.drop_duplicates(subset=['insert_id'], keep='first')
            df_d = pd.concat([df_iid, df_no_iid], ignore_index=True)
        else:
            df_d = df.drop_duplicates(subset=['date','distinct_id','서비스'], keep='first')

        # 2) utm_term backfill: (date, distinct_id) 그룹 내 빈 값 채움 → 패키지 2번째 이벤트 attribution
        has_utm_mask = df_d['utm_term'].astype(str).str.len() > 0
        bf_map = df_d[has_utm_mask].groupby(['date','distinct_id'])['utm_term'].first().to_dict()
        def _fill(row):
            if row['utm_term']:
                return row['utm_term']
            return bf_map.get((row['date'], row['distinct_id']), '')
        df_d['utm_term'] = df_d.apply(_fill, axis=1)
        df_d = df_d[df_d['utm_term'].astype(str).str.len() > 0]

        for (d, ut), v in df_d.groupby(['date','utm_term'])['revenue'].sum().items():
            if d and ut: mp_value_map[(d, str(ut))] = v
        for (d, ut), c in df_d.groupby(['date','utm_term']).size().items():
            if d and ut: mp_count_map[(d, str(ut))] = c

    # 4) Stripe
    log.info(f"\n4단계: Stripe 매출")
    stripe_start = datetime(DATA_REFRESH_START.year, DATA_REFRESH_START.month, DATA_REFRESH_START.day, tzinfo=KST)
    stripe_end = datetime.now(KST)
    stripe_revenue = fetch_stripe_revenue(stripe_start, stripe_end)
    for country, dates in stripe_revenue.items():
        total = sum(dates.values())
        log.info(f"  {country}: ₩{total:,.0f} KRW")

    # 5) 병합
    log.info(f"\n5단계: 병합")
    records = []
    for dk, rows in meta_data.items():
        parts = dk.split('/'); iso_date = f"20{parts[0]}-{parts[1]}-{parts[2]}"
        for mr in rows:
            asid = mr['adset_id']
            if not asid: continue
            spend = mr['spend']  # Already USD
            currency = detect_currency(mr['adset_name'], mr['campaign_name'])
            country = CURRENCY_TO_COUNTRY.get(currency, '글로벌')
            # Mixpanel: 현지통화 → USD
            mpc = mp_count_map.get((dk, asid), 0)
            mpv_local = mp_value_map.get((dk, asid), 0.0)
            revenue = local_to_usd(float(mpv_local), currency, dk)
            profit = revenue - spend
            roas = (revenue / spend * 100) if spend > 0 else 0
            cvr = (mpc / mr['unique_clicks'] * 100) if mr['unique_clicks'] > 0 and mpc > 0 else 0
            budget_raw = budget_map.get(asid, 0)
            budget_val = round(budget_raw / 100, 2) if budget_raw > 0 else 0
            product = extract_product(mr['adset_name'], mr['campaign_name'])

            records.append({
                'date': iso_date, 'adset_id': asid,
                'campaign_name': mr['campaign_name'], 'adset_name': mr['adset_name'],
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

    log.info("\n" + "=" * 60)
    log.info("✅ 글로벌 파이프라인 완료!")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
