# -*- coding: utf-8 -*-
"""
국내_소재별_supabase.py
======================
국내 Meta Ads (소재/ad 레벨) + Mixpanel → Supabase

국내_세트별과 차이점:
  - Meta level='ad' (소재 단위)
  - Mixpanel 매칭: utm_content (ad_id) 기준
  - 테이블: ad_creative_daily

환경변수:
  META_TOKEN_1 / META_TOKEN_2
  MIXPANEL_PROJECT_ID / MIXPANEL_USERNAME / MIXPANEL_SECRET
  SUPABASE_URL / SUPABASE_SERVICE_KEY
  REFRESH_DAYS (기본 10), FULL_REFRESH
"""

import os, json, time, re, math, logging
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

META_TOKEN_A = os.environ.get("META_TOKEN_1", "")
META_TOKEN_B = os.environ.get("META_TOKEN_2", "")
META_TOKENS = {
    "act_1270614404675034": META_TOKEN_A,
    "act_707835224206178": META_TOKEN_A,
    "act_1808141386564262": META_TOKEN_B,
}
META_TOKEN_DEFAULT = META_TOKEN_A
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
ALL_AD_ACCOUNTS = list(META_TOKENS.keys())

MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME = os.environ.get("MIXPANEL_USERNAME", "")
MIXPANEL_SECRET = os.environ.get("MIXPANEL_SECRET", "")
MIXPANEL_EVENT_NAMES = ["결제완료", "payment_complete"]

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None)
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
FULL_REFRESH_START = datetime(2025, 1, 1)
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))

if FULL_REFRESH:
    REFRESH_DAYS = (TODAY - FULL_REFRESH_START).days + 1
    log.info(f"🔥 FULL_REFRESH: {FULL_REFRESH_START:%Y-%m-%d} ~ 오늘 ({REFRESH_DAYS}일)")

DATA_REFRESH_START = TODAY - timedelta(days=REFRESH_DAYS - 1)


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

def extract_product(adset_name, campaign_name=""):
    for source in [campaign_name, adset_name]:
        if not source: continue
        tokens = re.split(r'[_\s\-/|,()\[\]]+', str(source).strip())
        for token in tokens:
            token = token.strip()
            if not token: continue
            if re.match(r'^\d+$', token): continue
            # Strip leading emojis
            i = 0
            while i < len(token):
                c = token[i]
                if '\uAC00' <= c <= '\uD7A3' or '\u3131' <= c <= '\u3163' or c.isalnum() or c in '.%': break
                i += 1
            token = token[i:].strip()
            if token: return token
    return "기타"

def get_token(acc_id):
    return META_TOKENS.get(acc_id, META_TOKEN_DEFAULT)


# =========================================================
# Meta API (ad 레벨)
# =========================================================
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
    """★ level='ad' — 소재 단위 수집"""
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
    results = {}; needs_campaign = {}
    data = meta_api_get(url, params, token=get_token(ad_account_id))
    while data:
        for row in data.get('data', []):
            asid = row.get('id',''); budget = row.get('daily_budget','0'); cid = row.get('campaign_id','')
            try: b = int(float(budget)) if budget else 0
            except: b = 0
            results[asid] = b
            if b == 0 and cid: needs_campaign[asid] = cid
        next_url = data.get('paging',{}).get('next')
        if next_url:
            time.sleep(1)
            try: resp = req_lib.get(next_url, timeout=120); data = resp.json() if resp.status_code == 200 else None
            except: data = None
        else: break
    if needs_campaign:
        cids = set(needs_campaign.values()); cb = {}
        for cid in cids:
            try:
                cd = meta_api_get(f"{META_BASE_URL}/{cid}",{'fields':'id,daily_budget'},token=get_token(ad_account_id))
                if cd: cb[cid] = int(float(cd.get('daily_budget','0') or '0'))
                time.sleep(0.5)
            except: pass
        for asid, cid in needs_campaign.items():
            if cb.get(cid, 0) > 0: results[asid] = cb[cid]
    return results


# =========================================================
# Mixpanel (utm_content = ad_id 매칭)
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
                    ev = json.loads(line); props = ev.get('properties',{}); ts = props.get('time',0)
                    if ts:
                        dt_kst = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)
                        ds = f"{dt_kst.year%100:02d}/{dt_kst.month:02d}/{dt_kst.day:02d}"
                    else: ds = None
                    # ★ utm_content = ad_id
                    ut = None
                    for k in ['utm_content','UTM_Content','UTM Content']:
                        if k in props and props[k]: ut = clean_id(str(props[k]).strip()); break
                    raw_a = props.get('amount') or props.get('결제금액')
                    raw_v = props.get('value')
                    a_val = float(raw_a) if raw_a else 0.0
                    v_val = float(raw_v) if raw_v else 0.0
                    revenue = a_val if a_val > 0 else (v_val if v_val > 0 else 0.0)
                    data.append({'distinct_id':props.get('distinct_id'),'date':ds,'utm_content':ut or '','revenue':revenue,'서비스':props.get('서비스',''),'insert_id':props.get('$insert_id') or props.get('insert_id') or ''})
                except: pass
            log.info(f"  ✅ 파싱: {len(data)}건")
            return data
        except Exception as e:
            log.error(f"  ❌ Mixpanel 오류: {e}"); return []
    return []


# =========================================================
# Supabase
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
    log.info("🎨 국내 소재별 Meta(ad) + Mixpanel → Supabase")
    log.info("=" * 60)
    log.info(f"📅 갱신: {DATA_REFRESH_START:%Y-%m-%d} ~ 오늘 ({REFRESH_DAYS}일)")

    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)

    # 1) Meta Insights (ad level)
    log.info(f"\n1단계: Meta Insights ad level ({REFRESH_DAYS}일 × {len(ALL_AD_ACCOUNTS)}계정)")
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

    total_meta = sum(len(v) for v in meta_data.values())
    log.info(f"✅ Meta: {total_meta}건")

    # 2) 예산
    log.info("\n2단계: 예산 조회")
    time.sleep(30)
    budget_map = {}
    for acc_id in ALL_AD_ACCOUNTS:
        budget_map.update(fetch_adset_budgets(acc_id))
        time.sleep(1)
    log.info(f"✅ 예산: {len(budget_map)}개")

    # 3) Mixpanel (utm_content = ad_id)
    log.info(f"\n3단계: Mixpanel ({REFRESH_DAYS}일, utm_content 매칭)")
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

    # Mixpanel 집계 (utm_content = ad_id)
    import pandas as pd
    mp_value_map = {}; mp_count_map = {}
    if mp_raw:
        df = pd.DataFrame(mp_raw)

        def _norm(x):
            s = str(x).strip() if x is not None else ''
            return '' if s.lower() in ('', 'none', 'undefined', 'null') else s
        df['utm_content'] = df['utm_content'].apply(_norm)

        # 1) $insert_id 기준 dedup (Mixpanel canonical)
        if 'insert_id' in df.columns:
            df_iid = df[df['insert_id'].astype(str).str.len() > 0]
            df_no_iid = df[df['insert_id'].astype(str).str.len() == 0]
            df_iid = df_iid.drop_duplicates(subset=['insert_id'], keep='first')
            df_d = pd.concat([df_iid, df_no_iid], ignore_index=True)
        else:
            df_d = df.drop_duplicates(subset=['date','distinct_id','서비스'], keep='first')

        # 2) utm_content backfill: (date, distinct_id) 그룹 내 utm_content 채움
        has_uc_mask = df_d['utm_content'].astype(str).str.len() > 0
        bf_map = df_d[has_uc_mask].groupby(['date','distinct_id'])['utm_content'].first().to_dict()
        def _fill(row):
            if row['utm_content']:
                return row['utm_content']
            return bf_map.get((row['date'], row['distinct_id']), '')
        df_d['utm_content'] = df_d.apply(_fill, axis=1)

        # 3) utm_content 채워진 이벤트만 attribution
        df_d = df_d[df_d['utm_content'].astype(str).str.len() > 0]

        log.info(f"  📊 매출 합계 (utm_content backfill 적용): ₩{int(df_d['revenue'].sum()):,}")
        for (d, ut), v in df_d.groupby(['date','utm_content'])['revenue'].sum().items():
            if d and ut: mp_value_map[(d, str(ut))] = v
        for (d, ut), c in df_d.groupby(['date','utm_content']).size().items():
            if d and ut: mp_count_map[(d, str(ut))] = c

    # 4) 병합
    log.info(f"\n4단계: 병합")
    records = []
    for dk, rows in meta_data.items():
        parts = dk.split('/'); iso_date = f"20{parts[0]}-{parts[1]}-{parts[2]}"
        for mr in rows:
            ad_id = mr['ad_id']
            if not ad_id: continue
            spend = mr['spend']
            # ★ Mixpanel 매칭: (date_key, ad_id)
            mpc = mp_count_map.get((dk, ad_id), 0)
            mpv = mp_value_map.get((dk, ad_id), 0.0)
            revenue = float(mpv)
            profit = revenue - spend
            roas = (revenue / spend * 100) if spend > 0 else 0
            cvr = (mpc / mr['unique_clicks'] * 100) if mr['unique_clicks'] > 0 and mpc > 0 else 0
            budget_raw = budget_map.get(mr['adset_id'], 0)
            budget_val = budget_raw if budget_raw > 0 else 0
            product = extract_product(mr['adset_name'], mr['campaign_name'])

            records.append({
                'date': iso_date, 'ad_id': ad_id,
                'campaign_name': mr['campaign_name'], 'adset_name': mr['adset_name'],
                'adset_id': mr['adset_id'], 'ad_name': mr['ad_name'],
                'ad_account_id': mr['ad_account_id'], 'product': product,
                'spend': round(spend, 2), 'cost_per_result': round(mr['cost_per_result'], 2),
                'purchase_roas_meta': round(mr['meta_roas'], 4),
                'cpm': round(mr['cpm'], 2), 'reach': mr['reach'], 'impressions': mr['impressions'],
                'unique_clicks': int(mr['unique_clicks']), 'unique_ctr': round(mr['unique_ctr'], 4),
                'cost_per_click': round(mr['cost_per_click'], 2), 'frequency': round(mr['frequency'], 4),
                'results_meta': int(mr['results_meta']), 'results_mp': mpc,
                'revenue': round(revenue, 2), 'profit': round(profit, 2),
                'roas': round(roas, 2), 'cvr': round(cvr, 4), 'budget': budget_val,
            })
    log.info(f"✅ 레코드: {len(records)}개")

    # 5) Supabase upsert
    log.info(f"\n5단계: Supabase upsert ({len(records)}행)")
    if records:
        sb.upsert("ad_creative_daily", records)

    log.info("\n" + "=" * 60)
    log.info("✅ 소재별 파이프라인 완료!")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
