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

# Meta 채널 판별 (utm_source 화이트리스트) — 세트 파이프라인과 동일.
# 타 채널(google/tiktok 등) 결제가 직전 Meta 방문에서 남은 stale utm_content(소재 id)을
# 그대로 달고 들어와 Meta 소재 매출로 잘못 합산되는 문제를 차단한다.
# → 마지막 터치가 Meta(ig/fb/an 등)인 결제만 소재에 귀속한다.
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

KST = timezone(timedelta(hours=9))
# ★ 스냅샷 기준시각 (2026-09-28): 전체 파이프라인(supabase.yml)이 세트·소재 job 에 같은 SNAPSHOT_TS(epoch초)를 넘긴다.
#   두 로더가 같은 시각을 "지금"으로 보고 그 이후 MP 결제는 이번 회차에서 제외 → 세트 매출 = 소재 소계 (오늘 칸 포함).
#   미설정(단독 실행)이면 기존대로 실행 시각.
SNAPSHOT_TS = int(os.environ.get("SNAPSHOT_TS", "0") or 0)
TODAY = (datetime.fromtimestamp(SNAPSHOT_TS, KST) if SNAPSHOT_TS > 0 else datetime.now(KST)).replace(tzinfo=None)
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
# '(소재 미상)' 행 ad_id 접두어 — 세트에는 귀속되지만 그날 지출 소재로 못 붙인 매출을 세트별로 모은다 (2026-09-28).
UNATTR_PREFIX = "unattr_"
UNATTR_AD_NAME = "(소재 미상)"

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
                    data.append({'distinct_id':props.get('distinct_id'),'date':ds,'ts':int(ts) if ts else 0,'pt':int(props.get('mp_processing_time_ms') or 0)//1000,'utm_content':ut or '','utm_term':uterm or '','utm_source':us or '','revenue':revenue,'서비스':props.get('서비스',''),'insert_id':props.get('$insert_id') or props.get('insert_id') or '','order_id':props.get('order_id') or ''})
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
    # ★ KST 경계 보정 버퍼 (2026-07-31): MP export 는 from_date 를 UTC 날짜로 필터하지만
    #   parse 는 KST(UTC+9)로 재버킷팅 → 윈도우 첫 KST 날짜의 00:00~09:00 이 통째로 누락(~28~35%).
    #   REFRESH_DAYS=10 상 각 날짜의 마지막 기록(D+9)이 항상 첫날이라 영구 고착됐다(7/4~7/22 사고).
    #   records 는 meta_data 날짜에만 생성되고 겹침은 insert_id/order_id dedup 이 제거하므로 안전.
    #   국내_세트별_supabase.py 의 동일 수정(MP_FETCH_BUFFER_DAYS)을 이식한 것.
    MP_FETCH_BUFFER_DAYS = 2
    if REFRESH_DAYS > 14:
        chunk_start = DATA_REFRESH_START
        while chunk_start <= YESTERDAY:
            chunk_end = min(chunk_start + timedelta(days=6), YESTERDAY)
            fetch_from = (chunk_start - timedelta(days=MP_FETCH_BUFFER_DAYS)).strftime('%Y-%m-%d')
            mp_raw.extend(fetch_mixpanel_data(fetch_from, chunk_end.strftime('%Y-%m-%d')))
            chunk_start = chunk_end + timedelta(days=1)
    else:
        if DATA_REFRESH_START <= YESTERDAY:
            mp_from = (DATA_REFRESH_START - timedelta(days=MP_FETCH_BUFFER_DAYS)).strftime('%Y-%m-%d')
            mp_raw.extend(fetch_mixpanel_data(mp_from, YESTERDAY.strftime('%Y-%m-%d')))
    today_data = fetch_mixpanel_data(TODAY.strftime('%Y-%m-%d'), TODAY.strftime('%Y-%m-%d'))
    if today_data: mp_raw.extend(today_data)
    # 스냅샷 컷오프 — 기준시각 이후 결제 제외 (세트·소재 동일 시점 정합)
    #   결제시각(ts)뿐 아니라 Mixpanel 처리시각(pt=mp_processing_time_ms)도 자른다(2026-09-28): 세트·소재 job 이
    #   export 를 서로 다른 시각에 호출하면, 결제시각은 기준 이전이지만 늦게 적재된 이벤트가 나중 job 에만 잡혀
    #   하루 1~3건씩 어긋났다. 처리시각까지 자르면 두 job 이 같은 이벤트 집합을 본다. (pt 없으면 결제시각만)
    if SNAPSHOT_TS > 0:
        _bn = len(mp_raw)
        mp_raw = [r for r in mp_raw if (not r.get('ts') or r['ts'] <= SNAPSHOT_TS) and (not r.get('pt') or r['pt'] <= SNAPSHOT_TS)]
        log.info(f"  ⏱️ 스냅샷 컷오프 {datetime.fromtimestamp(SNAPSHOT_TS, KST):%m-%d %H:%M} KST: {_bn} → {len(mp_raw)}건")
    log.info(f"✅ Mixpanel: {len(mp_raw)}건")

    # Mixpanel 집계 — 세트 귀속(utm_term) 우선 + 세트 안에서 소재(utm_content) 배분 (2026-09-28)
    #   결제 1건의 '세트'는 세트 로더(국내_세트별_supabase.py)와 똑같은 규칙으로 정한다:
    #   utm_term · 채널분류 · insert_id/order_id dedup · 라스트터치 24h 크로스셀 백필.
    #   '소재'는 그 결제의 utm_content 가 그 세트의 그날 지출 소재일 때만 붙이고, 못 붙인 매출
    #   (광고 id 없음·다른 세트 광고·그날 지출 없는 광고)은 세트별 '(소재 미상)' 행으로 모은다 → 소재 소계 = 세트 매출.
    import pandas as pd
    mp_value_map = {}; mp_count_map = {}   # (date, ad_key) · ad_key = ad_id 또는 UNATTR_PREFIX+adset_id
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
        #   - other(google 등): stale utm 오귀속 차단 → 통째로 제외.
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

        # 1) $insert_id dedup — 세트와 동일(insert_id 없는 행은 필드 조합으로 추가 dedup)
        if 'insert_id' in df.columns:
            df_iid = df[df['insert_id'].astype(str).str.len() > 0]
            df_no_iid = df[df['insert_id'].astype(str).str.len() == 0]
            df_iid = df_iid.drop_duplicates(subset=['insert_id'], keep='first')
            df_no_iid = df_no_iid.drop_duplicates(subset=['date', 'distinct_id', '서비스', 'utm_term', 'revenue'], keep='first')
            df_d = pd.concat([df_iid, df_no_iid], ignore_index=True)
        else:
            df_d = df.drop_duplicates(subset=['date','distinct_id','서비스'], keep='first')

        # 1.5) order_id 주문 단위 dedup (결제완료/payment_complete 이중발화 방지) — 세트와 동일(utm_term 보유 행 우선)
        if 'order_id' in df_d.columns:
            df_d['_oid'] = df_d['order_id'].astype(str).str.strip()
            _has_oid = df_d['_oid'].str.len() > 0
            _with = df_d[_has_oid].copy()
            _without = df_d[~_has_oid]
            _with['_hasu'] = (_with['utm_term'].astype(str).str.len() > 0).astype(int)
            _with = (_with.sort_values(['_oid','_hasu','revenue'], ascending=[True, False, False])
                          .drop_duplicates(subset=['_oid'], keep='first')
                          .drop(columns=['_hasu']))
            df_d = pd.concat([_with, _without], ignore_index=True).drop(columns=['_oid'])
            log.info(f"  🧹 order_id 주문단위 dedup 후: {len(df_d)}건")

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
        log.info(f"  🔗 크로스셀 백필(라스트터치·24h): {len(_rec)}건 회수 · 매출 ₩{int(_rec_rev):,}")

        # 3) utm_term 채워진 결제만 귀속 (미회수 organic 제외)
        df_d = df_d[df_d['utm_term'].astype(str).str.len() > 0].copy()
        log.info(f"  📊 매출 합계 (크로스셀 백필 적용): ₩{int(df_d['revenue'].sum()):,}")

        # 4) 세트 안에서 소재 배분 — utm_content 가 그 세트의 그날 지출 소재면 그 소재, 아니면 '(소재 미상)'
        df_d['ad_key'] = [c if (c and AD_DAY_ADSET.get((d, c)) == t) else UNATTR_PREFIX + t
                          for d, t, c in zip(df_d['date'], df_d['utm_term'].astype(str), df_d['utm_content'].astype(str))]
        # 진단 로그 — 실제 행이 되는 범위(그날 Meta 행이 있는 세트)만 센다(창 밖 날짜·지출 없는 세트 결제는 세트·소재 모두 버림)
        _SETDAY = {(d, a) for (d, _ad), a in AD_DAY_ADSET.items()}
        _inrow = [(d, t) in _SETDAY for d, t in zip(df_d['date'], df_d['utm_term'].astype(str))]
        _un = df_d['ad_key'].str.startswith(UNATTR_PREFIX) & pd.Series(_inrow, index=df_d.index)
        _rv = df_d.loc[_un, 'revenue'].sum(); _tv = df_d.loc[pd.Series(_inrow, index=df_d.index), 'revenue'].sum()
        log.info(f"  🧩 소재 배분(행 대상 {sum(_inrow)}건): (소재 미상) {int(_un.sum())}건 · ₩{int(_rv):,} ({(_rv / _tv * 100) if _tv else 0:.1f}%)")
        for (d, ak), v in df_d.groupby(['date','ad_key'])['revenue'].sum().items():
            if d and ak: mp_value_map[(d, str(ak))] = v
        for (d, ak), c in df_d.groupby(['date','ad_key']).size().items():
            if d and ak: mp_count_map[(d, str(ak))] = c

    # ── only-raise 가드용: 현재 저장된 귀속(results_mp/revenue) 미리 읽기 ──
    #   국내_세트별_supabase.py 의 동일 가드를 이식(2026-08-04).
    #   부실/부분 실패한 Mixpanel fetch 가 이미 정상인 과거 귀속을 '낮추지' 못하게 한다.
    #   (spend 등 Meta-side 지표는 항상 최신값으로 갱신 — 가드 대상은 매출/구매수뿐)
    #   ※ 이 가드가 없어 7/4~7/21 구간이 KST 경계 누락값(~70%)으로 덮여 고착됐다.
    #   ★ (2026-09-28) 소재 단위 → 세트 단위 판정으로 변경. 매출이 세트 안에서 소재↔(소재 미상)로 옮겨가면
    #     소재 단위 가드는 옛 값을 붙잡아 이중계상한다. 그래서 판정은 세트 로더와 같은 조건 —
    #     '세트의 새 구매수 합 < 세트 테이블(ad_performance_daily) 기존 구매수' — 으로 하고, 발동하면 세트 로더가
    #     기존 세트 값을 유지하므로 소재 쪽은 새 배분을 그대로 쓰되 (기존 세트 값 − 새 합) 차이를 그 세트의
    #     '(소재 미상)' 행에 얹어 소재 소계 = 세트 값을 맞춘다.
    #     (세트·소재 job 은 동시에 시작해 둘 다 이번 회차 쓰기 전 상태를 읽는다)
    prev_attr = {}
    prev_set = {}   # (date, adset_id) → 세트 테이블 기존 (구매수, 매출)
    _ps = DATA_REFRESH_START.strftime("%Y-%m-%d")
    _pe = TODAY.strftime("%Y-%m-%d")
    _off = 0
    while True:
        _u = (f"{sb.base_url}/rest/v1/ad_creative_daily?select=date,ad_id,adset_id,results_mp,revenue"
              f"&date=gte.{_ps}&date=lte.{_pe}&order=date.asc,ad_id.asc&limit=1000&offset={_off}")
        try:
            _chunk = req_lib.get(_u, headers={**sb.headers, "Prefer": ""}, timeout=60).json()
        except Exception as _e:
            log.warning(f"  ⚠️ 기존 귀속 읽기 실패(가드 비활성화): {_e}")
            _chunk = []
        if not isinstance(_chunk, list) or not _chunk:
            break
        for _row in _chunk:
            prev_attr[(_row.get("date"), str(_row.get("ad_id")))] = (
                int(_row.get("results_mp") or 0), float(_row.get("revenue") or 0.0))
        if len(_chunk) < 1000:
            break
        _off += 1000
    _off = 0
    while True:
        _u = (f"{sb.base_url}/rest/v1/ad_performance_daily?select=date,adset_id,results_mp,revenue"
              f"&date=gte.{_ps}&date=lte.{_pe}&order=date.asc,adset_id.asc&limit=1000&offset={_off}")
        try:
            _chunk = req_lib.get(_u, headers={**sb.headers, "Prefer": ""}, timeout=60).json()
        except Exception as _e:
            log.warning(f"  ⚠️ 세트 기존 귀속 읽기 실패(가드 비활성화): {_e}")
            _chunk = []
        if not isinstance(_chunk, list) or not _chunk:
            break
        for _row in _chunk:
            prev_set[(_row.get("date"), str(_row.get("adset_id")))] = (
                int(_row.get("results_mp") or 0), float(_row.get("revenue") or 0.0))
        if len(_chunk) < 1000:
            break
        _off += 1000
    log.info(f"  🛡️ only-raise 가드: 기존 귀속 {len(prev_attr)}건 · 세트 {len(prev_set)}건 로드")

    # 4) 병합
    log.info(f"\n4단계: 병합")
    records = []
    _guarded = 0
    n_unattr = 0
    for dk, rows in meta_data.items():
        parts = dk.split('/'); iso_date = f"20{parts[0]}-{parts[1]}-{parts[2]}"
        # 세트 단위 only-raise 판정 — 세트 로더와 같은 조건: 새 구매수 합(소재 + (소재 미상)) < 세트 테이블 기존 구매수.
        #   발동 시 세트 로더는 기존 (구매수, 매출)을 유지 → 그 차이를 (소재 미상)에 얹을 보정값으로 기록.
        rep_by_adset = {}
        for mr in rows:
            if mr.get('adset_id'): rep_by_adset.setdefault(str(mr['adset_id']), mr)
        guard_adj = {}   # adset_id → (구매수 보정, 매출 보정)
        for asid in rep_by_adset:
            keys = [UNATTR_PREFIX + asid] + [str(mr['ad_id']) for mr in rows if str(mr.get('adset_id')) == asid and mr.get('ad_id')]
            new_cnt = sum(mp_count_map.get((dk, k), 0) for k in keys)
            new_rev = sum(float(mp_value_map.get((dk, k), 0.0)) for k in keys)
            p_cnt, p_rev = prev_set.get((iso_date, asid), (0, 0.0))
            if new_cnt < p_cnt:
                guard_adj[asid] = (p_cnt - new_cnt, p_rev - new_rev)
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
        # (소재 미상) 행 — 세트에는 귀속됐지만 그날 지출 소재로 못 붙인 매출. 지출 0, 매출/건수만.
        #   세트 로더는 그날 Meta 행이 있는 세트에만 매출을 싣는다 → 같은 조건(그날 소재 행이 있는 세트)에서만 만든다.
        for asid, r0 in rep_by_adset.items():
            ak = UNATTR_PREFIX + asid
            mpc = mp_count_map.get((dk, ak), 0)
            mpv = float(mp_value_map.get((dk, ak), 0.0))
            if asid in guard_adj:   # only-raise 가드 발동 세트 — 세트 로더가 유지한 기존 값과의 차이를 여기 얹는다
                mpc += guard_adj[asid][0]; mpv += guard_adj[asid][1]
                _guarded += 1
            if mpv == 0 and mpc == 0: continue
            budget_raw = budget_map.get(r0['adset_id'], 0)
            records.append({
                'date': iso_date, 'ad_id': ak,
                'campaign_name': r0['campaign_name'], 'adset_name': r0['adset_name'],
                'adset_id': r0['adset_id'], 'ad_name': UNATTR_AD_NAME,
                'ad_account_id': r0['ad_account_id'], 'product': extract_product(r0['adset_name'], r0['campaign_name']),
                'spend': 0, 'cost_per_result': 0, 'purchase_roas_meta': 0,
                'cpm': 0, 'reach': 0, 'impressions': 0, 'unique_clicks': 0, 'unique_ctr': 0,
                'cost_per_click': 0, 'frequency': 0, 'results_meta': 0, 'results_mp': mpc,
                'revenue': round(mpv, 2), 'profit': round(mpv, 2),
                'roas': 0, 'cvr': 0, 'budget': budget_raw if budget_raw > 0 else 0,
            })
            n_unattr += 1
    log.info(f"✅ 레코드: {len(records)}개 (그중 (소재 미상) {n_unattr}행)" + (f" · 🛡️ 가드 발동 세트 {_guarded}개(차이를 (소재 미상)에 반영)" if _guarded else ""))

    # 5) Supabase upsert
    log.info(f"\n5단계: Supabase upsert ({len(records)}행)")
    if records:
        sb.upsert("ad_creative_daily", records)

    # 5-1) 이번 회차에 안 생긴 옛 (소재 미상) 행 정리 — 매출이 소재로 옮겨갔거나 사라진 경우 stale 방지.
    _keep = {(r['date'], r['ad_id']) for r in records if str(r['ad_id']).startswith(UNATTR_PREFIX)}
    _stale = defaultdict(list)
    for (_d, _aid) in prev_attr:
        if str(_aid).startswith(UNATTR_PREFIX) and (_d, _aid) not in _keep:
            _stale[_d].append(_aid)
    for _d, _ids in _stale.items():
        for i in range(0, len(_ids), 100):
            sb.delete("ad_creative_daily", f"date=eq.{_d}&ad_id=in.({','.join(_ids[i:i+100])})")
    if _stale:
        log.info(f"  🧹 stale (소재 미상) 행 삭제: {sum(len(v) for v in _stale.values())}행")

    log.info("\n" + "=" * 60)
    log.info("✅ 소재별 파이프라인 완료!")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
