# -*- coding: utf-8 -*-
"""
밴스드_세트별_supabase.py
========================
Vanced Meta Ads (adset level) + Mixpanel → Supabase

계정: act_25183853061243175 (META_TOKEN_VANCED)
Mixpanel 매칭: utm_term (adset_id)
테이블: vanced_ad_performance_daily
"""

import os, json, time, re, math, logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from decimal import Decimal
import requests as req_lib

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
META_TOKEN = os.environ.get("META_TOKEN_VANCED", "")
META_AD_ACCOUNT = os.environ.get("META_AD_ACCOUNT_ID_VANCED", "") or "act_25183853061243175"
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME = os.environ.get("MIXPANEL_USERNAME", "")
MIXPANEL_SECRET = os.environ.get("MIXPANEL_SECRET", "")
MIXPANEL_EVENT_NAMES = ["결제완료", "payment_complete"]

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None)
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))
if FULL_REFRESH:
    REFRESH_DAYS = (TODAY - datetime(2025, 11, 9)).days + 1

DATA_REFRESH_START = TODAY - timedelta(days=REFRESH_DAYS - 1)

PRODUCT_PREFIXES = [
    ('29금궁합', '29금궁합'), ('29금', '29금'), ('Solo', 'Solo'), ('Kids', 'Kids'),
    ('Money', 'Money'), ('Reunion', 'Reunion'), ('1%', '1%'),
    ('career', 'career'), ('Year', 'Year'), ('Star', 'Star'),
]

def adset_to_product(name):
    for prefix, product in PRODUCT_PREFIXES:
        if name.startswith(prefix): return product
    return 'etc'

def clean_id(val):
    if val is None: return ""
    s = str(val).strip()
    if re.match(r'^\d+$', s): return s
    try:
        if re.match(r'^[\d.]+[eE][+\-]?\d+$', s): return str(int(Decimal(s)))
    except: pass
    return re.sub(r'[^0-9]', '', s) or s

def _extract_action(al, types):
    if not al: return 0
    for a in al:
        if a.get('action_type','') in types:
            try: return float(a.get('value',0))
            except: return 0
    return 0

def _extract_outbound(dl):
    if not dl: return 0
    for a in dl:
        if a.get('action_type') in ['outbound_click','link_click']:
            return float(a.get('value',0))
    return float(dl[0].get('value',0)) if len(dl)==1 else 0

PURCHASE_TYPES = ['offsite_conversion.fb_pixel_purchase','purchase','omni_purchase']

# Meta API
def meta_api_get(url, params=None):
    if params is None: params = {}
    params['access_token'] = META_TOKEN
    for attempt in range(5):
        try:
            resp = req_lib.get(url, params=params, timeout=120)
            if resp.status_code == 200: return resp.json()
            if resp.status_code in [429,500,502,503]:
                time.sleep(30 + attempt*30); continue
            log.error(f"  ❌ Meta {resp.status_code}: {resp.text[:200]}"); return None
        except: time.sleep(15)
    return None

def fetch_meta_insights(single_date):
    url = f"{META_BASE_URL}/{META_AD_ACCOUNT}/insights"
    params = {'fields':'campaign_name,adset_name,adset_id,spend,cpm,reach,impressions,frequency,actions,cost_per_action_type,purchase_roas,unique_outbound_clicks,unique_outbound_clicks_ctr,cost_per_unique_outbound_click',
        'level':'adset','time_increment':1,'time_range':json.dumps({'since':single_date,'until':single_date}),'limit':500,
        'filtering':json.dumps([{'field':'spend','operator':'GREATER_THAN','value':'0'}])}
    all_results = []
    data = meta_api_get(url, params)
    while data:
        all_results.extend(data.get('data',[]));
        nxt = data.get('paging',{}).get('next')
        if nxt:
            time.sleep(1)
            try: resp=req_lib.get(nxt,timeout=120); data=resp.json() if resp.status_code==200 else None
            except: data=None
        else: break
    return all_results

def fetch_budgets():
    url = f"{META_BASE_URL}/{META_AD_ACCOUNT}/adsets"
    params = {'fields':'id,daily_budget,campaign_id','limit':500,
        'filtering':json.dumps([{'field':'effective_status','operator':'IN','value':['ACTIVE']}])}
    results = {}
    data = meta_api_get(url, params)
    while data:
        for row in data.get('data',[]):
            try: results[row['id']] = int(float(row.get('daily_budget','0') or '0'))
            except: results[row['id']] = 0
        nxt = data.get('paging',{}).get('next')
        if nxt:
            time.sleep(1)
            try: resp=req_lib.get(nxt,timeout=120); data=resp.json() if resp.status_code==200 else None
            except: data=None
        else: break
    return results

# Mixpanel
def fetch_mixpanel(from_date, to_date):
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {'from_date':from_date,'to_date':to_date,'event':json.dumps(MIXPANEL_EVENT_NAMES),'project_id':MIXPANEL_PROJECT_ID}
    log.info(f"  📡 Mixpanel: {from_date} ~ {to_date}")
    for attempt in range(4):
        try:
            resp = req_lib.get(url, params=params, auth=(MIXPANEL_USERNAME,MIXPANEL_SECRET), timeout=300)
            if resp.status_code == 429: time.sleep(30+attempt*30); continue
            if resp.status_code != 200: return []
            lines = [l for l in resp.text.split('\n') if l.strip()]
            log.info(f"  📊 이벤트: {len(lines)}건")
            data = []
            for line in lines:
                try:
                    ev=json.loads(line); props=ev.get('properties',{}); ts=props.get('time',0)
                    if ts:
                        dt_kst=datetime.fromtimestamp(ts,tz=timezone.utc)+timedelta(hours=9)
                        ds=f"{dt_kst.year%100:02d}/{dt_kst.month:02d}/{dt_kst.day:02d}"
                    else: ds=None
                    ut=None
                    for k in ['utm_term','UTM_Term','UTM Term']:
                        if k in props and props[k]: ut=clean_id(str(props[k]).strip()); break
                    raw_a=props.get('amount') or props.get('결제금액'); raw_v=props.get('value')
                    a_val=float(raw_a) if raw_a else 0.0; v_val=float(raw_v) if raw_v else 0.0
                    revenue=a_val if a_val>0 else (v_val if v_val>0 else 0.0)
                    data.append({'distinct_id':props.get('distinct_id'),'date':ds,'utm_term':ut or '','revenue':revenue,'서비스':props.get('서비스','')})
                except: pass
            log.info(f"  ✅ 파싱: {len(data)}건")
            return data
        except Exception as e: log.error(f"  ❌ Mixpanel: {e}"); return []
    return []

# Supabase
class SB:
    def __init__(s):
        s.url=re.sub(r'[^\x20-\x7E]','',SUPABASE_URL).strip().rstrip("/")
        if not s.url.startswith("http"): s.url=f"https://{s.url}"
        s.h={"apikey":SUPABASE_KEY,"Authorization":f"Bearer {SUPABASE_KEY}","Content-Type":"application/json","Prefer":"resolution=merge-duplicates"}
    def upsert(s,t,recs,cs=500):
        total=len(recs);ok=0
        for i in range(0,total,cs):
            chunk=[]
            for r in recs[i:i+cs]:
                row={}
                for k,v in r.items():
                    if hasattr(v,'item'): v=v.item()
                    if isinstance(v,float) and (math.isnan(v) or math.isinf(v)): v=0
                    row[k]=v
                chunk.append(row)
            try:
                resp=req_lib.post(f"{s.url}/rest/v1/{t}",headers=s.h,json=chunk,timeout=60)
                if resp.status_code in [200,201]: ok+=len(chunk); log.info(f"  ✅ upsert {ok}/{total}")
                else: log.error(f"  ❌ {resp.status_code}: {resp.text[:300]}")
            except Exception as e: log.error(f"  ❌ {e}")
            time.sleep(0.5)
        return ok

def main():
    log.info("="*60)
    log.info("🎯 밴스드 세트별 Meta+Mixpanel → Supabase")
    log.info("="*60)
    log.info(f"📅 갱신: {DATA_REFRESH_START:%Y-%m-%d} ~ 오늘 ({REFRESH_DAYS}일)")
    log.info(f"🔑 계정: {META_AD_ACCOUNT}")

    sb = SB()

    # 1) Meta
    log.info(f"\n1단계: Meta Insights adset level")
    meta_data = {}
    for d in range(REFRESH_DAYS):
        td=TODAY-timedelta(days=d); ts=td.strftime('%Y-%m-%d')
        dk=f"{td.year%100:02d}/{td.month:02d}/{td.day:02d}"
        rows=fetch_meta_insights(ts)
        if rows:
            for r in rows:
                pr_list=r.get('purchase_roas',[])
                meta_data[(dk,r.get('adset_id',''))]={
                    'campaign_name':r.get('campaign_name',''),'adset_name':r.get('adset_name',''),'adset_id':r.get('adset_id',''),
                    'spend':float(r.get('spend',0)),'cpm':float(r.get('cpm',0)),'reach':int(float(r.get('reach',0))),
                    'impressions':int(float(r.get('impressions',0))),'frequency':float(r.get('frequency',0)),
                    'results_meta':_extract_action(r.get('actions',[]),PURCHASE_TYPES),
                    'cost_per_result':_extract_action(r.get('cost_per_action_type',[]),PURCHASE_TYPES),
                    'unique_clicks':_extract_outbound(r.get('unique_outbound_clicks')),
                    'unique_ctr':_extract_outbound(r.get('unique_outbound_clicks_ctr')),
                    'cost_per_click':_extract_outbound(r.get('cost_per_unique_outbound_click')),
                    'meta_roas':float(pr_list[0]['value'])*100 if pr_list else 0,
                }
            log.info(f"  📊 {dk}: {len(rows)}건")
        time.sleep(1)
    log.info(f"✅ Meta: {len(meta_data)}건")

    # 2) Budget
    log.info("\n2단계: 예산")
    time.sleep(30)
    budget_map = fetch_budgets()
    log.info(f"✅ 예산: {len(budget_map)}개")

    # 3) Mixpanel (utm_term)
    log.info(f"\n3단계: Mixpanel")
    import pandas as pd
    YESTERDAY=TODAY-timedelta(days=1); mp_raw=[]
    if REFRESH_DAYS>14:
        cs=DATA_REFRESH_START
        while cs<=YESTERDAY:
            ce=min(cs+timedelta(days=6),YESTERDAY)
            mp_raw.extend(fetch_mixpanel(cs.strftime('%Y-%m-%d'),ce.strftime('%Y-%m-%d')))
            cs=ce+timedelta(days=1)
    else:
        if DATA_REFRESH_START<=YESTERDAY:
            mp_raw.extend(fetch_mixpanel(DATA_REFRESH_START.strftime('%Y-%m-%d'),YESTERDAY.strftime('%Y-%m-%d')))
    td=fetch_mixpanel(TODAY.strftime('%Y-%m-%d'),TODAY.strftime('%Y-%m-%d'))
    if td: mp_raw.extend(td)
    log.info(f"✅ Mixpanel: {len(mp_raw)}건")

    mp_value_map,mp_count_map={},{}
    if mp_raw:
        df=pd.DataFrame(mp_raw)
        df=df[df['utm_term'].notna()&(df['utm_term']!='')&(df['utm_term']!='None')]
        df=df.sort_values('revenue',ascending=False)
        df_d=df.drop_duplicates(subset=['date','distinct_id','서비스'],keep='first')
        log.info(f"  📊 매출합: ₩{int(df_d['revenue'].sum()):,}")
        for (d,ut),v in df_d.groupby(['date','utm_term'])['revenue'].sum().items():
            if d and ut: mp_value_map[(d,str(ut))]=v
        for (d,ut),c in df_d.groupby(['date','utm_term']).size().items():
            if d and ut: mp_count_map[(d,str(ut))]=c

    # 4) Merge
    log.info(f"\n4단계: 병합")
    records=[]
    for (dk,asid),mr in meta_data.items():
        parts=dk.split('/'); iso=f"20{parts[0]}-{parts[1]}-{parts[2]}"
        spend=mr['spend']; mpc=mp_count_map.get((dk,asid),0); mpv=mp_value_map.get((dk,asid),0.0)
        revenue=float(mpv); profit=revenue-spend
        roas=(revenue/spend*100) if spend>0 else 0
        cvr=(mpc/mr['unique_clicks']*100) if mr['unique_clicks']>0 and mpc>0 else 0
        product=adset_to_product(mr['adset_name'])
        records.append({
            'date':iso,'adset_id':asid,'campaign_name':mr['campaign_name'],'adset_name':mr['adset_name'],
            'ad_account_id':META_AD_ACCOUNT,'product':product,
            'spend':round(spend,2),'cost_per_result':round(mr['cost_per_result'],2),
            'purchase_roas_meta':round(mr['meta_roas']/100,4) if mr['meta_roas'] else 0,
            'cpm':round(mr['cpm'],2),'reach':mr['reach'],'impressions':mr['impressions'],
            'unique_clicks':int(mr['unique_clicks']),'unique_ctr':round(mr['unique_ctr'],4),
            'cost_per_click':round(mr['cost_per_click'],2),'frequency':round(mr['frequency'],4),
            'results_meta':int(mr['results_meta']),'results_mp':mpc,
            'revenue':round(revenue,2),'profit':round(profit,2),'roas':round(roas,2),'cvr':round(cvr,4),
            'budget':budget_map.get(asid,0),
        })
    log.info(f"✅ 레코드: {len(records)}개")

    # 5) Upsert
    log.info(f"\n5단계: Supabase upsert")
    if records: sb.upsert("vanced_ad_performance_daily", records)

    log.info("\n"+"="*60)
    log.info("✅ 밴스드 세트별 완료!")
    log.info("="*60)

if __name__=="__main__": main()
