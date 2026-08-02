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
# 밴스드 광고 계정들 — 여러 계정 데이터를 합쳐 밴스드 탭(vanced_ad_performance_daily)에 표시.
#   act_25183853061243175 = 타이트사주 (밴스드)
#   act_1560037899174007  = 타이트사주 2 (밴스드)
#   act_1286632473622244  = 타이트사주(밴스드_대만)
# 콤마구분 env META_AD_ACCOUNT_IDS_VANCED 로 덮어쓸 수 있음.
_DEFAULT_VANCED_ACCOUNTS = "act_25183853061243175,act_1560037899174007,act_1286632473622244"
def _norm_acct(a):
    a = a.strip()
    return a if a.startswith("act_") else (f"act_{a}" if a else "")
META_AD_ACCOUNTS = [_norm_acct(a) for a in os.environ.get("META_AD_ACCOUNT_IDS_VANCED", _DEFAULT_VANCED_ACCOUNTS).split(",") if a.strip()]
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

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

# 통화·국가 처리 (country 분할)
# ─────────────────────────────────────────────────────────────────────────────
# 결제 통화(amount/결제금액의 단위)는 Stripe 원장으로 확정: TWD/HKD/THB/JPY (+국내 KRW).
#   · 서비스 접미사 -th → THB, -jp → JPY (JP 스토어프론트), -tw → TWD
#   · HK 고객(mp_country_code=HK)은 -tw 스토어프론트에서도 HKD 로 결제 (Stripe HK 현지통화 청구)
#   · 접미사 없음 → KRW (국내)
# 국가(country)는 Mixpanel mp_country_code(ISO alpha-2). 누락 시 국내(접미사 없음)=KR.
SUFFIX_CURRENCY = {"tw": "TWD", "th": "THB", "jp": "JPY", "hk": "HKD"}

def _get_krw_rate(cur, fallback):
    env = os.environ.get(f"{cur}_KRW_RATE")
    if env:
        try: return float(env)
        except: pass
    try:
        r = req_lib.get(f"https://open.er-api.com/v6/latest/{cur}", timeout=20)
        if r.status_code == 200:
            v = r.json().get("rates", {}).get("KRW")
            if v and float(v) > 0: return float(v)
    except: pass
    return fallback

TWD_KRW_RATE = _get_krw_rate("TWD", 47.85)
THB_KRW_RATE = _get_krw_rate("THB", 42.0)
HKD_KRW_RATE = _get_krw_rate("HKD", 178.0)
JPY_KRW_RATE = _get_krw_rate("JPY", 9.1)
CURRENCY_KRW = {"TWD": TWD_KRW_RATE, "THB": THB_KRW_RATE, "HKD": HKD_KRW_RATE, "JPY": JPY_KRW_RATE, "KRW": 1.0}

def market_suffix(svc):
    m = re.search(r'-([a-z]{2,3})$', str(svc or "").strip().lower())
    return m.group(1) if m else ""

def payment_currency(svc, cc=None):
    # amount 통화 = 스토어프론트(서비스 접미사) 통화. HK 고객이 -tw 스토어프론트에서 결제해도
    #   MP amount 는 TWD(표시가격, 예 결제금액=1490)로 기록되므로 TWD 로 환산해야 한다.
    #   (구버전은 cc=='HK' 를 HKD 로 강제 → TWD amount 를 HKD 환율(×178)로 곱해
    #    홍콩 매출·ROAS 를 TWD 대비 ~3.7배 부풀렸다. cc 오버라이드 제거로 수정.)
    sfx = market_suffix(svc)
    if sfx in ("th", "jp", "hk"): return SUFFIX_CURRENCY[sfx]   # 실제 -th/-jp/-hk 스토어프론트만
    if sfx == "tw": return "TWD"
    return "KRW"

def to_krw(revenue, svc, cc=None):
    # 결제 통화(서비스 접미사 + HK 예외) → KRW 환산 (KRW 는 그대로)
    return revenue * CURRENCY_KRW.get(payment_currency(svc, cc), 1.0)

def payment_country(props):
    cc = str(props.get("mp_country_code", "") or "").strip().upper()
    if cc:
        return cc
    sf = market_suffix(props.get("서비스", ""))
    return "KR" if not sf else sf.upper()

# 세트가 어느 시장을 산 것인가 = 캠페인/세트명의 시장 태그. 대시보드 국가필터의 기준값.
#   ★ 구매자 위치(mp_country_code)로 국가를 나누면 안 된다 — 홍콩 세트 고객의 31%가 -tw
#     스토어에서 결제해 MP 국가가 TW 로 잡히는데, 지출은 세트 단위(HK)로만 들어와서
#     홍콩 필터에는 지출 100% + 매출 64% 만 남아 ROAS 가 무너져 보였다(2026-08-02 실측).
#     글로벌 세트별이 2026-07-02 에 같은 이유로 캠페인명 기준으로 전환했고 그 규칙을 맞춘다.
_MARKET_PATTERNS = [
    ("HK", re.compile(r'(?:^|[_\-\s])hk(?:[_\-\s]|$)|홍콩')),
    ("TW", re.compile(r'(?:^|[_\-\s])tw(?:[_\-\s]|$)|대만')),
    ("JP", re.compile(r'(?:^|[_\-\s])jp(?:[_\-\s]|$)|일본')),
    ("TH", re.compile(r'(?:^|[_\-\s])th(?:[_\-\s]|$)|태국')),
]

def campaign_country(adset_name, campaign_name=None):
    s = f"{adset_name or ''} {campaign_name or ''}".lower()
    for iso, pat in _MARKET_PATTERNS:
        if pat.search(s):
            return iso
    return "KR"   # 태그 없으면 국내

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
# 요청한도(rate limit) 에러 코드 — 403 으로 오지만 일시적이라 재시도해야 한다.
#   code 4/17/32/613 = App/User/Page/Custom rate limit, 80000~80004 = Ads Insights 한도.
_META_RATE_LIMIT_CODES = {4, 17, 32, 613, 80000, 80001, 80002, 80003, 80004}


def _is_rate_limit(resp):
    try:
        e = resp.json().get("error", {}) or {}
        return bool(e.get("is_transient")) or int(e.get("code", 0)) in _META_RATE_LIMIT_CODES
    except Exception:
        return False


def meta_api_get(url, params=None):
    if params is None: params = {}
    params['access_token'] = META_TOKEN
    for attempt in range(5):
        try:
            resp = req_lib.get(url, params=params, timeout=120)
            if resp.status_code == 200: return resp.json()
            # ★ 403 "Application request limit reached"(code 4) 를 즉시 포기하면 그 (계정,날짜)
            #   Meta 데이터가 통째로 비고, 병합에서 Mixpanel 매출만으로 spend=0 레코드가 만들어져
            #   이미 저장된 지출이 0 으로 덮인다(2026-07-26 대만 사고). → 백오프 후 재시도.
            if resp.status_code in [429,500,502,503] or (resp.status_code == 403 and _is_rate_limit(resp)):
                log.warning(f"  ⏳ Meta {resp.status_code} 재시도 {attempt+1}/5")
                time.sleep(30 + attempt*30); continue
            log.error(f"  ❌ Meta {resp.status_code}: {resp.text[:200]}"); return None
        except: time.sleep(15)
    return None


def fetch_meta_insights_range(since, until, account):
    """계정 1개의 [since~until] 일별 인사이트를 '한 번의 호출'로 가져온다.

    이전에는 날짜마다 1콜(10일×3계정=30콜/실행, 매시)이라 요청한도에 자주 걸렸다.
    time_increment=1 이면 범위 호출도 날짜별 행(date_start)으로 쪼개져 온다 → 결과 동일, 호출 1/10.
    반환: 성공 시 행 리스트(빈 리스트 = 지출 0), 실패 시 None(← '데이터 없음'과 반드시 구분).
    """
    url = f"{META_BASE_URL}/{account}/insights"
    params = {'fields':'campaign_name,adset_name,adset_id,spend,cpm,reach,impressions,frequency,actions,cost_per_action_type,purchase_roas,unique_outbound_clicks,unique_outbound_clicks_ctr,cost_per_unique_outbound_click',
        'level':'adset','breakdowns':'country','time_increment':1,'time_range':json.dumps({'since':since,'until':until}),'limit':500,
        'filtering':json.dumps([{'field':'spend','operator':'GREATER_THAN','value':'0'}])}
    all_results = []
    data = meta_api_get(url, params)
    if data is None:
        return None
    while data:
        all_results.extend(data.get('data',[]));
        nxt = data.get('paging',{}).get('next')
        if nxt:
            time.sleep(1)
            try:
                resp = req_lib.get(nxt, timeout=120)
                data = resp.json() if resp.status_code == 200 else None
            except Exception:
                data = None
            if data is None:      # 페이지 도중 실패 = 부분 데이터 → 지출 과소계상 위험, 실패로 처리
                return None
        else: break
    return all_results

def fetch_budgets(account):
    # effective_status 는 ACTIVE 뿐 아니라 PAUSED 계열까지 포함(ARCHIVED/DELETED만 제외).
    #   ACTIVE만 조회하면 최근 지출은 있었으나 꺼진 세트가 budget_map 에서 빠져 날짜탭
    #   예산 컬럼이 0/과거값으로 고착됐다(국내와 동일 이슈). 폴백 없어 세트 증가분은 페이지네이션뿐.
    url = f"{META_BASE_URL}/{account}/adsets"
    params = {'fields':'id,daily_budget,campaign_id','limit':500,
        'filtering':json.dumps([{'field':'effective_status','operator':'IN','value':[
            'ACTIVE','PAUSED','CAMPAIGN_PAUSED','ADSET_PAUSED','IN_PROCESS',
            'WITH_ISSUES','PENDING_REVIEW','PENDING_BILLING_INFO','DISAPPROVED','PREAPPROVED']}])}
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
            if resp.status_code in (429,500,502,503):
                if attempt<3: time.sleep(30+attempt*30); continue
                return []
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
                    us=''
                    for k in ['utm_source','UTM_Source','UTM Source']:
                        if k in props and props[k]: us=str(props[k]).strip(); break
                    svc=props.get('서비스',''); _cc=payment_country(props)
                    raw_a=props.get('amount') or props.get('결제금액'); raw_v=props.get('value')
                    a_val=float(raw_a) if raw_a else 0.0; v_val=float(raw_v) if raw_v else 0.0
                    revenue=a_val if a_val>0 else (v_val if v_val>0 else 0.0)
                    if revenue>0:
                        # 통화 = MP '통화' 프로퍼티(실제 결제통화, ground truth) 우선.
                        #   해외결제는 통화가 항상 채워지고(TWD/HKD/JPY/THB), 국내(KRW)만 비어있음.
                        #   → HK 고객이 -tw 에서 TWD 결제하든 진짜 HKD 결제하든 통화값 그대로 정확 환산.
                        #   통화 누락/미지원 시에만 서비스접미사 추론(payment_currency) 폴백.
                        mp_cur = str(props.get('통화') or '').strip().upper()
                        cur = mp_cur if mp_cur in CURRENCY_KRW else payment_currency(svc, _cc)
                        revenue = revenue * CURRENCY_KRW.get(cur, 1.0)
                    data.append({'distinct_id':props.get('distinct_id'),'date':ds,'utm_term':ut or '','utm_source':us or '','revenue':revenue,'서비스':svc,'country':_cc,'insert_id':props.get('$insert_id') or props.get('insert_id') or ''})
                except: pass
            log.info(f"  ✅ 파싱: {len(data)}건")
            return data
        except Exception as e:
            # 타임아웃/연결오류 → 백오프 후 재시도 (대용량 export 간헐 타임아웃 대응)
            log.warning(f"  ⚠️ Mixpanel 오류(시도 {attempt+1}/4): {e}")
            if attempt<3: time.sleep(30+attempt*30); continue
            return []
    return []

# Supabase
class SB:
    def __init__(s):
        s.url=re.sub(r'[^\x20-\x7E]','',SUPABASE_URL).strip().rstrip("/")
        if not s.url.startswith("http"): s.url=f"https://{s.url}"
        s.h={"apikey":SUPABASE_KEY,"Authorization":f"Bearer {SUPABASE_KEY}","Content-Type":"application/json","Prefer":"resolution=merge-duplicates"}
        # new-tightauto: SUPABASE_DB_SCHEMA 설정 시 스키마 프로파일 헤더 (미설정=기존 public)
        _sc = os.environ.get('SUPABASE_DB_SCHEMA', '').strip()
        if _sc:
            s.h['Accept-Profile'] = _sc
            s.h['Content-Profile'] = _sc
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
    def delete(s,t,query):
        # 구 country 분할 행 정리용. query 예: "date=eq.2026-08-02&adset_id=in.(1,2,3)"
        try:
            resp=req_lib.delete(f"{s.url}/rest/v1/{t}?{query}",headers=s.h,timeout=60)
            if resp.status_code not in (200,204):
                log.error(f"  ❌ delete: HTTP {resp.status_code} | {resp.text[:200]}")
                return False
            return True
        except Exception as e:
            log.error(f"  ❌ delete 예외: {e}")
            return False

def main():
    log.info("="*60)
    log.info("🎯 밴스드 세트별 Meta+Mixpanel → Supabase")
    log.info("="*60)
    log.info(f"📅 갱신: {DATA_REFRESH_START:%Y-%m-%d} ~ 오늘 ({REFRESH_DAYS}일)")
    log.info(f"🔑 계정 {len(META_AD_ACCOUNTS)}개: {', '.join(META_AD_ACCOUNTS)}")
    log.info(f"💱 TWD→KRW 환율: {TWD_KRW_RATE} (대만 -tw 결제 환산)")

    sb = SB()

    # 1) Meta (계정별 — adset_id 는 계정 간 고유하므로 한 dict 에 합산)
    log.info(f"\n1단계: Meta Insights adset level")
    meta_data = {}
    # Meta 조회가 실패한 (계정, 날짜) — 이 조합은 '지출 0'이 아니라 '모름'이므로
    #   아래 병합에서 레코드를 만들지 않아 이미 저장된 지출을 보존한다.
    failed_days = set()
    _all_dks = [ (TODAY-timedelta(days=d)) for d in range(REFRESH_DAYS) ]
    _all_dks = [ f"{t.year%100:02d}/{t.month:02d}/{t.day:02d}" for t in _all_dks ]
    for account in META_AD_ACCOUNTS:
        log.info(f"  🔑 {account}")
        rows = fetch_meta_insights_range(DATA_REFRESH_START.strftime('%Y-%m-%d'),
                                         TODAY.strftime('%Y-%m-%d'), account)
        if rows is None:
            for dk in _all_dks: failed_days.add((account, dk))
            log.warning(f"    ⚠️ Meta 조회 실패 → {account} 의 {REFRESH_DAYS}일 지출 보존(레코드 생성 스킵)")
            time.sleep(5)
            continue
        by_day = defaultdict(int)
        for r in rows:
            ds = str(r.get('date_start') or '')
            if len(ds) != 10: continue                       # 날짜 없는 행은 그레인 불명 → 스킵
            dk = f"{ds[2:4]}/{ds[5:7]}/{ds[8:10]}"
            by_day[dk] += 1
            if float(r.get('spend',0))<=0: continue  # breakdown=country 하위행 0지출 제외
            cc=str(r.get('country','') or 'XX').strip().upper()  # Meta country breakdown (ISO)
            pr_list=r.get('purchase_roas',[])
            # (dk, adset_id, country) 그레인 — 같은 adset 도 국가별로 별도 행
            meta_data[(dk,r.get('adset_id',''),cc)]={
                'campaign_name':r.get('campaign_name',''),'adset_name':r.get('adset_name',''),'adset_id':r.get('adset_id',''),
                'country':cc,
                'spend':float(r.get('spend',0)),'cpm':float(r.get('cpm',0)),'reach':int(float(r.get('reach',0))),
                'impressions':int(float(r.get('impressions',0))),'frequency':float(r.get('frequency',0)),
                'results_meta':_extract_action(r.get('actions',[]),PURCHASE_TYPES),
                'cost_per_result':_extract_action(r.get('cost_per_action_type',[]),PURCHASE_TYPES),
                'unique_clicks':_extract_outbound(r.get('unique_outbound_clicks')),
                'unique_ctr':_extract_outbound(r.get('unique_outbound_clicks_ctr')),
                'cost_per_click':_extract_outbound(r.get('cost_per_unique_outbound_click')),
                'meta_roas':float(pr_list[0]['value'])*100 if pr_list else 0,
                'account':account,
            }
        log.info(f"    📊 {len(rows)}건 / {len(by_day)}일 " + ' '.join(f"{k}:{v}" for k,v in sorted(by_day.items(), reverse=True)))
        time.sleep(1)
    log.info(f"✅ Meta: {len(meta_data)}건 (조회실패 {len(failed_days)} 계정·일)")

    # 2) Budget (계정별 — adset_id 고유하므로 budget_map 합산)
    log.info("\n2단계: 예산")
    budget_map = {}
    for account in META_AD_ACCOUNTS:
        time.sleep(30)
        bm = fetch_budgets(account)
        budget_map.update(bm)
        log.info(f"  ✅ 예산 {account}: {len(bm)}개")
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
        _bn=len(df); df=df[df['utm_source'].apply(is_meta_source)]; log.info(f"  🔵 Meta 소스 필터: {_bn} → {len(df)}건 (비-Meta {_bn-len(df)}건 제외)")
        def _norm(x):
            s=str(x).strip() if x is not None else ''
            return '' if s.lower() in ('','none','undefined','null') else s
        df['utm_term']=df['utm_term'].apply(_norm)
        if 'insert_id' in df.columns:
            df_iid=df[df['insert_id'].astype(str).str.len()>0]
            df_no=df[df['insert_id'].astype(str).str.len()==0]
            df_iid=df_iid.drop_duplicates(subset=['insert_id'],keep='first')
            df_d=pd.concat([df_iid,df_no],ignore_index=True)
        else:
            df_d=df.drop_duplicates(subset=['date','distinct_id','서비스'],keep='first')
        # utm_term backfill (패키지 2번째 이벤트 attribution)
        has_mask=df_d['utm_term'].astype(str).str.len()>0
        bf_map=df_d[has_mask].groupby(['date','distinct_id'])['utm_term'].first().to_dict()
        def _fill(r):
            return r['utm_term'] if r['utm_term'] else bf_map.get((r['date'],r['distinct_id']),'')
        df_d['utm_term']=df_d.apply(_fill,axis=1)
        df_d=df_d[df_d['utm_term'].astype(str).str.len()>0]
        if 'country' not in df_d.columns: df_d['country']=''
        df_d['country']=df_d['country'].fillna('').astype(str).str.strip().str.upper().replace('','KR')
        log.info(f"  📊 매출합 (utm_term backfill 적용): ₩{int(df_d['revenue'].sum()):,}")
        # (date, utm_term, country) 그레인으로 집계 — country 는 mp_country_code(ISO), 없으면 KR
        for (d,ut,cc),v in df_d.groupby(['date','utm_term','country'])['revenue'].sum().items():
            if d and ut: mp_value_map[(d,str(ut),str(cc))]=v
        for (d,ut,cc),c in df_d.groupby(['date','utm_term','country']).size().items():
            if d and ut: mp_count_map[(d,str(ut),str(cc))]=c

    # 4) Merge — (dk, adset_id) 세트당 1행. country 는 캠페인명 시장 태그(campaign_country).
    #    ★ 2026-08-02 전환: 예전엔 (dk, adset_id, country) 로 쪼갰는데, 지출 country 는 Meta
    #      breakdown(=세트가 산 시장)이고 매출 country 는 mp_country_code(=구매자 위치)라
    #      기준이 서로 달랐다. 홍콩 세트 고객의 31%가 -tw 스토어에서 결제해 매출만 TW 행으로
    #      빠져나가는 바람에, 대시보드 국가필터=홍콩이 '지출 100% + 매출 64%' 가 되어
    #      ROAS 가 무너져 보였다(7/31~8/2 은 아예 매출 0). 국내(KR) 태그 세트는 매출 100%가
    #      KR 이라 이 전환의 영향이 없고, 대만 태그는 5% 회수된다.
    #    지출/지표는 breakdown 행 전체를 합산한다 — 밴스드는 국내가 주력이라 글로벌과 달리
    #    KR viewer 를 제외하지 않는다(제외하면 국내 지출이 통째로 사라진다).
    log.info(f"\n4단계: 병합 (세트당 1행, 캠페인명 country)")
    by_set=defaultdict(list)          # (dk, adset_id) → Meta country breakdown 행들
    for (dk,asid,cc),mr in meta_data.items():
        if asid: by_set[(dk,asid)].append(mr)
    adset_info={}                     # 매출만 있는 세트의 라벨 보충용
    for (dk,asid),rows in by_set.items():
        adset_info.setdefault(asid, rows[0])
    # 매출·건수는 mp country 를 무시하고 세트 단위로 합산
    mp_rev=defaultdict(float); mp_cnt=defaultdict(int)
    for (d2,a2,cc2),v in mp_value_map.items(): mp_rev[(d2,a2)]+=v
    for (d2,a2,cc2),c in mp_count_map.items(): mp_cnt[(d2,a2)]+=c

    all_keys=set(by_set.keys())|set(mp_rev.keys())
    records=[]
    _preserved=0
    for (dk,asid) in all_keys:
        parts=dk.split('/'); iso=f"20{parts[0]}-{parts[1]}-{parts[2]}"
        rows=by_set.get((dk,asid)) or []
        info=rows[0] if rows else adset_info.get(asid)
        if not info:  # 지출·메타정보 전무한 매출행(드묾) — 라벨 불가, 스킵
            continue
        # ★ Meta 조회가 실패한 (계정, 날짜) 는 지출이 '0'이 아니라 '모름'이다.
        #   여기서 레코드를 만들면 Mixpanel 매출만 있는 spend=0 행이 기존 지출을 덮어쓴다
        #   (2026-07-26 대만 계정 사고: 403 rate limit → 매 실행마다 지출 0 고착).
        if not rows and (info['account'], dk) in failed_days:
            _preserved+=1
            continue
        spend=sum(m['spend'] for m in rows)
        impressions=sum(m['impressions'] for m in rows)
        reach=sum(m['reach'] for m in rows)
        uclk=sum(m['unique_clicks'] for m in rows)
        results_meta=sum(m['results_meta'] for m in rows)
        # 파생지표는 합산값으로 재계산 — country 분할을 없앴으니 breakdown 행의 값을 그대로 못 쓴다
        cpm=(spend/impressions*1000) if impressions>0 else 0
        frequency=(impressions/reach) if reach>0 else 0
        unique_ctr=(uclk/impressions*100) if impressions>0 else 0
        cost_per_click=(spend/uclk) if uclk>0 else 0
        cost_per_result=(spend/results_meta) if results_meta>0 else 0
        meta_roas=(sum(m['meta_roas']*m['spend'] for m in rows)/spend) if spend>0 else 0  # 지출가중평균
        mpc=int(mp_cnt.get((dk,asid),0)); revenue=float(mp_rev.get((dk,asid),0.0))
        profit=revenue-spend
        roas=(revenue/spend*100) if spend>0 else 0
        cvr=(mpc/uclk*100) if uclk>0 and mpc>0 else 0
        product=adset_to_product(info['adset_name'])
        cc=campaign_country(info['adset_name'], info['campaign_name'])
        records.append({
            'date':iso,'adset_id':asid,'country':cc,
            'campaign_name':info['campaign_name'],'adset_name':info['adset_name'],
            'ad_account_id':info['account'],'product':product,
            'currency':'KRW',  # revenue 는 이미 KRW 환산됨(서비스 접미사 기준)
            'spend':round(spend,2),'cost_per_result':round(cost_per_result,2),
            'purchase_roas_meta':round(meta_roas/100,4) if meta_roas else 0,
            'cpm':round(cpm,2),'reach':int(reach),'impressions':int(impressions),
            'unique_clicks':int(uclk),'unique_ctr':round(unique_ctr,4),
            'cost_per_click':round(cost_per_click,2),'frequency':round(frequency,4),
            'results_meta':int(results_meta),'results_mp':mpc,
            'revenue':round(revenue,2),'profit':round(profit,2),'roas':round(roas,2),'cvr':round(cvr,4),
            # 예산: 세트 일예산 전액. country 분할이 없으니 비례배분하지 않는다.
            'budget':int(budget_map.get(asid,0)),
        })
    log.info(f"✅ 레코드: {len(records)}개 (세트당 1행, 캠페인명 country)" +
             (f" · Meta 실패로 보존(스킵) {_preserved}행" if _preserved else ""))

    # 5) Upsert — 구 country 분할 행이 stale 로 남아 이중계상되므로 (date, adset_id) 를 먼저 지운다.
    log.info(f"\n5단계: Supabase upsert ({len(records)}행)")
    if records:
        by_date_ids=defaultdict(set)
        for r in records: by_date_ids[r['date']].add(str(r['adset_id']))
        for _dt,_ids in by_date_ids.items():
            _idl=sorted(_ids)
            for _j in range(0,len(_idl),100):   # in.() URL 길이 보호
                sb.delete("vanced_ad_performance_daily",
                          f"date=eq.{_dt}&adset_id=in.({','.join(_idl[_j:_j+100])})")
        log.info(f"  🧹 구 country 분할 행 정리: {len(by_date_ids)}일 × 세트")
        sb.upsert("vanced_ad_performance_daily", records)

    log.info("\n"+"="*60)
    log.info("✅ 밴스드 세트별 완료!")
    log.info("="*60)

if __name__=="__main__": main()
