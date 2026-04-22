"""
네이버 검색광고 + 믹스패널 세션 어트리뷰션 → Supabase 자동화
─────────────────────────────────────────────────────────────
  ㆍ대상 테이블: public.naver_sa_daily  (+ public.google_ads_daily 선택)
  ㆍ기존 구글시트(마스터탭/주간추이/채널별_추이차트) 출력은 모두 제거
  ㆍ채널별/주간/일별 추이차트는 Supabase VIEW 에서 SELECT 로 제공

  requirements.txt:
    requests
    pandas
    numpy
    gspread                 # 구글 Ads 집계 fetch 용 (옵션)
    gspread-formatting      # (선택) 예전 의존, 남겨둠
    supabase                # ★ 신규

  필수 환경변수 (GitHub Secrets):
    NAVER_API_KEY, NAVER_SECRET_KEY, NAVER_CUSTOMER_ID
    MIXPANEL_PROJECT_ID, MIXPANEL_USERNAME, MIXPANEL_SECRET
    SUPABASE_URL                       # e.g. https://qkvqiorazdrhtuicnpec.supabase.co
    SUPABASE_SERVICE_ROLE_KEY          # service_role key (RLS 우회)

  선택 환경변수:
    SYNC_GOOGLE_ADS=true               # 네이버/구글 Daily 시트에서 구글 집계 동기화
    GCP_SERVICE_ACCOUNT_KEY            # SYNC_GOOGLE_ADS=true 인 경우 필요
    URL_NAV_GOO                        # 네이버/구글 Daily 시트 URL
    REFRESH_DAYS=730                   # 최근 N일만 갱신 (기본 2년)
    DRY_RUN=true                       # Supabase 쓰기 스킵, 로그만
"""
import os, sys, time, math, hashlib, hmac, base64, json, bisect, re, threading
import requests, pandas as pd, numpy as np
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ★ GitHub Actions 로그 즉시 출력
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ══════════════════════════════════════════════════════════════
# 환경변수
# ══════════════════════════════════════════════════════════════
def require_env(name):
    v = os.environ.get(name)
    if not v:
        print(f"❌ 환경변수 '{name}' 미설정")
        sys.exit(1)
    return v

# 네이버 SA API
NAVER_API_KEY     = require_env("NAVER_API_KEY")
NAVER_SECRET_KEY  = require_env("NAVER_SECRET_KEY")
NAVER_CUSTOMER_ID = require_env("NAVER_CUSTOMER_ID")
NAVER_BASE_URL    = "https://api.searchad.naver.com"

# 믹스패널
MIXPANEL_PROJECT_ID = require_env("MIXPANEL_PROJECT_ID")
MIXPANEL_USERNAME   = require_env("MIXPANEL_USERNAME")
MIXPANEL_SECRET     = require_env("MIXPANEL_SECRET")

# Supabase
SUPABASE_URL              = require_env("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = require_env("SUPABASE_SERVICE_ROLE_KEY")

# 옵션
SYNC_GOOGLE_ADS = os.environ.get("SYNC_GOOGLE_ADS", "false").lower() in ("true", "1", "yes")
DRY_RUN         = os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes")

# 기간
REFRESH_DAYS  = int(os.environ.get("REFRESH_DAYS", "730"))
END_DATE_STR   = datetime.now().strftime("%Y-%m-%d")
START_DATE_STR = (datetime.now() - timedelta(days=REFRESH_DAYS)).strftime("%Y-%m-%d")

# 병렬
WORKERS    = int(os.environ.get("WORKERS", "15"))
MP_WORKERS = int(os.environ.get("MP_WORKERS", "5"))

# 세션 어트리뷰션
SESSION_WINDOW_SEC = int(os.environ.get("SESSION_WINDOW_SEC", "1800"))
LANDING_EVENTS = ["$mp_web_page_view", "페이지뷰", "Page View"]
PAYMENT_EVENTS = ["결제완료", "payment_complete"]

print("=" * 60)
print("🚀 네이버SA + 믹스패널 → Supabase")
print(f"   기간:           {START_DATE_STR} ~ {END_DATE_STR} (최근 {REFRESH_DAYS}일)")
print(f"   Supabase URL:   {SUPABASE_URL}")
print(f"   세션 윈도우:    {SESSION_WINDOW_SEC//60}분")
print(f"   구글Ads 동기화: {SYNC_GOOGLE_ADS}")
print(f"   DRY_RUN:        {DRY_RUN}")
print("=" * 60)

# ══════════════════════════════════════════════════════════════
# Supabase 클라이언트 (service_role → RLS 우회)
# ══════════════════════════════════════════════════════════════
try:
    from supabase import create_client, Client
except ImportError:
    print("❌ supabase 패키지 미설치: pip install supabase")
    sys.exit(1)

sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
print("✅ Supabase 연결 준비 완료")

# ══════════════════════════════════════════════════════════════
# 공통 유틸
# ══════════════════════════════════════════════════════════════
def safe_float(v):
    try:
        f = float(v)
        return 0.0 if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return 0.0

def safe_int(v):
    return int(safe_float(v))

def _num(x):
    try:
        v = str(x).replace(",", "").replace("₩", "").replace("%", "").replace("\\", "").replace("W", "").replace("￦", "").replace("+", "").strip()
        return float(v) if v and v not in ["-", "#DIV/0!", "", "nan", "None", "NaN"] else 0.0
    except Exception:
        return 0.0

def _infer_year(month):
    today = date.today()
    return today.year if month <= today.month else today.year - 1

def parse_date(raw):
    raw = str(raw).strip()
    if not raw or raw in ['', 'nan', 'None']:
        return None
    raw = raw.split('\n')[0].strip()
    for pat in [r'^(\d{2})/(\d{1,2})/(\d{1,2})', r'^(\d{2})-(\d{1,2})-(\d{1,2})$']:
        m = re.match(pat, raw)
        if m:
            try:
                return date(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except Exception:
                pass
    m = re.match(r'^(\d{1,2})/(\d{1,2})(?:\s+\(|\s*$|\s*[\(\[])', raw)
    if m:
        mo, dy = int(m.group(1)), int(m.group(2))
        try:
            return date(_infer_year(mo), mo, dy)
        except Exception:
            return None
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"]:
        try:
            return datetime.strptime(raw[:19 if 'H' in fmt else 10], fmt).date()
        except Exception:
            pass
    return None

# ══════════════════════════════════════════════════════════════
# 네이버 API
# ══════════════════════════════════════════════════════════════
def get_naver_header(method, uri):
    ts = str(int(time.time() * 1000))
    sig = hmac.new(NAVER_SECRET_KEY.encode(), f"{ts}.{method}.{uri}".encode(), hashlib.sha256).digest()
    return {
        'Content-Type': 'application/json; charset=UTF-8',
        'X-Timestamp': ts, 'X-API-KEY': NAVER_API_KEY,
        'X-Customer': NAVER_CUSTOMER_ID,
        'X-Signature': base64.b64encode(sig).decode()
    }

def naver_get(uri, params=None):
    r = requests.get(NAVER_BASE_URL + uri, headers=get_naver_header('GET', uri), params=params)
    return r.json() if r.status_code == 200 else None

def get_stat(kw_id, start, end):
    CHUNK = 30
    s_dt = datetime.strptime(start, '%Y-%m-%d')
    e_dt = datetime.strptime(end, '%Y-%m-%d')
    def _call(since, until):
        return requests.get(NAVER_BASE_URL + '/stats',
            headers=get_naver_header('GET', '/stats'),
            params={'id': kw_id,
                    'fields': '["impCnt","clkCnt","salesAmt","cpc","ctr","ccnt"]',
                    'timeRange': json.dumps({"since": since, "until": until}),
                    'breakdown': 'date'})
    if (e_dt - s_dt).days <= CHUNK:
        r = _call(start, end)
        return r.json().get('data', []) if r.status_code == 200 else []
    out = []
    cs = s_dt
    while cs <= e_dt:
        ce = min(cs + timedelta(days=CHUNK - 1), e_dt)
        r = _call(cs.strftime('%Y-%m-%d'), ce.strftime('%Y-%m-%d'))
        if r.status_code == 200:
            out.extend(r.json().get('data', []))
        cs = ce + timedelta(days=1)
        time.sleep(0.05)
    return out

def collect_naver():
    print("\n🔍 네이버 데이터 수집...")
    camps = naver_get('/ncc/campaigns') or []
    cmp = {c['nccCampaignId']: c.get('name', '') for c in camps}
    print(f"  ✅ 캠페인 {len(camps)}개")

    ags = []
    for cid in cmp:
        a = naver_get('/ncc/adgroups', {'nccCampaignId': cid}) or []
        ags.extend(a)
        time.sleep(0.2)
    ag_map = {a['nccAdgroupId']: {'name': a.get('name', ''), 'cid': a.get('nccCampaignId', '')} for a in ags}
    print(f"  ✅ 광고그룹 {len(ags)}개")

    kws = []
    for aid, ai in ag_map.items():
        ks = naver_get('/ncc/keywords', {'nccAdgroupId': aid}) or []
        for k in ks:
            k['_ag'] = ai['name']
            k['_cmp'] = cmp.get(ai['cid'], '')
        kws.extend(ks)
        time.sleep(0.2)
    kw_map = {k.get('nccKeywordId', ''): {
        'kw': k.get('keyword', ''), 'cmp': k.get('_cmp', ''), 'ag': k.get('_ag', '')
    } for k in kws}
    print(f"  ✅ 키워드 {len(kws)}개")

    # 실제 데이터 범위 감지
    print(f"\n🔍 실제 데이터 범위 감지...")
    ids = list(kw_map.keys())
    earliest, latest, found = END_DATE_STR, START_DATE_STR, False
    for sid in ids[:5]:
        for s in get_stat(sid, START_DATE_STR, END_DATE_STR):
            if safe_float(s.get('salesAmt', 0)) > 0 or safe_int(s.get('impCnt', 0)) > 0:
                d = s.get('dateStart', '')
                if d:
                    found = True
                    earliest = min(earliest, d)
                    latest = max(latest, d)
        time.sleep(0.1)
    if found:
        a_start = (datetime.strptime(earliest, '%Y-%m-%d') - timedelta(days=3)).strftime('%Y-%m-%d')
        a_end   = END_DATE_STR
        print(f"  ✅ {earliest} ~ {latest} → 조회: {a_start} ~ {a_end}")
    else:
        a_start, a_end = START_DATE_STR, END_DATE_STR
        print(f"  ⚠️ 미발견, 전체 범위 사용")

    days = (datetime.strptime(a_end, '%Y-%m-%d') - datetime.strptime(a_start, '%Y-%m-%d')).days
    chunks = math.ceil(days / 30)
    print(f"\n📊 {len(ids)}개 × {chunks}청크 = ~{len(ids)*chunks:,}회 호출 (병렬 {WORKERS}스레드)")

    progress = {'done': 0, 'active': 0}
    lock = threading.Lock()

    def fetch_one(kid):
        info = kw_map.get(kid, {})
        result = []
        try:
            for s in get_stat(kid, a_start, a_end):
                cost = int(safe_float(s.get('salesAmt', 0)) * 1.1)
                clk = safe_int(s.get('clkCnt', 0))
                imp = safe_int(s.get('impCnt', 0))
                ctr = safe_float(s.get('ctr', 0))
                cpc = safe_float(s.get('cpc', 0))
                result.append({
                    'Date': s.get('dateStart', ''), '캠페인': info.get('cmp', ''),
                    '광고그룹': info.get('ag', ''), '키워드': info.get('kw', ''),
                    '키워드ID': kid, '노출수': imp, '클릭수': clk,
                    'CTR(%)': round(ctr * 100, 2) if ctr < 1 else round(ctr, 2),
                    '총비용(VAT포함,원)': cost, 'CPC(VAT포함)': int(cpc * 1.1) if cpc else 0,
                })
        except Exception:
            pass
        with lock:
            progress['done'] += 1
            progress['active'] += len(result)
            if progress['done'] % 100 == 0:
                print(f"    {progress['done']}/{len(ids)} ({progress['active']}건)")
        return result

    rows = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for future in as_completed({ex.submit(fetch_one, kid): kid for kid in ids}):
            rows.extend(future.result())
    print(f"  ✅ 전체 완료: {progress['done']}/{len(ids)} ({progress['active']}건)")

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[(df['총비용(VAT포함,원)'] > 0) | (df['노출수'] > 0) | (df['클릭수'] > 0)]
        df = df.sort_values(['Date', '총비용(VAT포함,원)'], ascending=[False, False])
    rng_s = df['Date'].min() if len(df) else ''
    rng_e = df['Date'].max() if len(df) else ''
    print(f"✅ {len(df)}건 ({rng_s} ~ {rng_e})")
    return df, (a_start, a_end)

# ══════════════════════════════════════════════════════════════
# 믹스패널 (랜딩/결제 수집 + 세션 매칭) — 원본 로직 그대로 유지
# ══════════════════════════════════════════════════════════════
MP_EXPORT_URL = "https://data.mixpanel.com/api/2.0/export"
KST = timezone(timedelta(hours=9))

def mp_auth_header():
    auth = base64.b64encode(f"{MIXPANEL_USERNAME}:{MIXPANEL_SECRET}".encode()).decode()
    return {'Authorization': f'Basic {auth}', 'Accept': 'application/json'}

def make_mp_chunks(from_d, to_d, chunk_days=30):
    s_dt = datetime.strptime(from_d, '%Y-%m-%d')
    e_dt = datetime.strptime(to_d, '%Y-%m-%d')
    chunks = []
    cs = s_dt
    while cs <= e_dt:
        ce = min(cs + timedelta(days=chunk_days - 1), e_dt)
        chunks.append((cs.strftime('%Y-%m-%d'), ce.strftime('%Y-%m-%d')))
        cs = ce + timedelta(days=1)
    return chunks

LANDING_CANDIDATES = [
    "$mp_web_page_view", "Page View", "페이지뷰", "page_view", "pageview", "Pageview",
    "$pageview", "view_page", "session_start", "Session Start", "세션시작",
    "view_landing", "랜딩", "landing", "screen_view", "Screen View",
    "mp_page_view", "web_page_view", "visit", "사이트방문", "view_item", "View Item",
]

def detect_landing_events(sample_date):
    print(f"\n🔎 랜딩 이벤트명 자동 탐지 ({sample_date})...")
    hdrs = mp_auth_header()
    s_dt = datetime.strptime(sample_date, '%Y-%m-%d')
    sample_from = (s_dt - timedelta(days=2)).strftime('%Y-%m-%d')
    found = {}

    def probe(event_name):
        try:
            r = requests.get(MP_EXPORT_URL, headers=hdrs, timeout=60, stream=True, params={
                'project_id': MIXPANEL_PROJECT_ID,
                'from_date': sample_from, 'to_date': sample_date,
                'event': json.dumps([event_name])
            })
            if r.status_code != 200:
                return (event_name, 0, 0)
            total = nsa = 0
            for line in r.iter_lines(decode_unicode=True):
                if not line or not line.strip(): continue
                total += 1
                if total > 5000: break
                try:
                    p = json.loads(line).get('properties', {})
                    ch = str(p.get('ch', '')).lower().strip()
                    ct = str(p.get('ct', '')).lower().strip()
                    src = str(p.get('utm_source', p.get('$source', ''))).lower()
                    med = str(p.get('utm_medium', p.get('$medium', ''))).lower()
                    if (ch == 'naver' and 'nsa' in ct) or \
                       ('naver' in src and ('nsa' in med or 'cpc' in med or 'paid' in med)):
                        nsa += 1
                except Exception:
                    continue
            r.close()
            return (event_name, total, nsa)
        except Exception:
            return (event_name, 0, 0)

    print(f"  후보 {len(LANDING_CANDIDATES)}개 (병렬 {MP_WORKERS})")
    with ThreadPoolExecutor(max_workers=MP_WORKERS) as ex:
        for f in as_completed({ex.submit(probe, n): n for n in LANDING_CANDIDATES}):
            name, total, nsa = f.result()
            if total > 0:
                print(f"    '{name}' → {total:,}건, NSA {nsa}건", "✅" if nsa > 0 else "")
            if nsa > 0:
                found[name] = nsa

    if found:
        srt = sorted(found.items(), key=lambda x: x[1], reverse=True)
        print(f"  ✅ 랜딩 이벤트:")
        for n, c in srt:
            print(f"    '{n}' ({c}건)")
        return [n for n, _ in srt]
    print(f"  ⚠️ NSA 랜딩 이벤트 없음 → 전체 귀속 방식")
    return []

def collect_sa_landings(from_d, to_d):
    print(f"\n🔍 네이버SA 랜딩 세션 수집 ({from_d} ~ {to_d})...")
    print(f"  이벤트: {LANDING_EVENTS}")
    chunks = make_mp_chunks(from_d, to_d)
    print(f"  {len(chunks)}청크, 병렬 {MP_WORKERS}스레드")

    lock = threading.Lock()
    stats = {'done': 0, 'matched': 0}

    def fetch_chunk(chunk):
        cs, ce = chunk
        local = []
        try:
            r = requests.get(MP_EXPORT_URL, headers=mp_auth_header(), timeout=300, params={
                'project_id': MIXPANEL_PROJECT_ID,
                'from_date': cs, 'to_date': ce,
                'event': json.dumps(LANDING_EVENTS)
            })
            if r.status_code != 200:
                return local
            for line in r.text.strip().split('\n'):
                if not line.strip(): continue
                try:
                    p = json.loads(line).get('properties', {})
                    ch = str(p.get('ch', '')).lower().strip()
                    ct = str(p.get('ct', '')).lower().strip()
                    if ch != 'naver' or 'nsa' not in ct: continue
                    did = str(p.get('distinct_id', ''))
                    ts = int(p.get('time', 0))
                    if did and ts > 0:
                        local.append((did, ts))
                except Exception:
                    continue
        except Exception:
            pass
        with lock:
            stats['done'] += 1
            stats['matched'] += len(local)
            if stats['done'] % 3 == 0 or stats['done'] == len(chunks):
                print(f"    {stats['done']}/{len(chunks)} (누적 {stats['matched']:,}건)")
        return local

    all_events = []
    with ThreadPoolExecutor(max_workers=MP_WORKERS) as ex:
        for f in as_completed([ex.submit(fetch_chunk, c) for c in chunks]):
            all_events.extend(f.result())

    sa_sessions = defaultdict(list)
    for did, ts in all_events:
        sa_sessions[did].append(ts)
    for did in sa_sessions:
        sa_sessions[did].sort()

    print(f"  ✅ SA 랜딩 {sum(len(v) for v in sa_sessions.values()):,}건, 유저 {len(sa_sessions):,}명")
    return dict(sa_sessions)

def get_mixpanel(from_d, to_d, sa_sessions=None):
    print(f"\n💰 믹스패널 결제 수집 ({from_d} ~ {to_d})...")
    INVALID_CR = {'vanced', '{keyword}', '{keword}', ''}
    chunks = make_mp_chunks(from_d, to_d)
    print(f"  {len(chunks)}청크, 병렬 {MP_WORKERS}스레드")

    lock = threading.Lock()
    stats = {'done': 0, 'nsa_matched': 0, 'total': 0, 'skipped_invalid': 0}

    def fetch_chunk(chunk):
        cs, ce = chunk
        local, lt, ls = [], 0, 0
        try:
            r = requests.get(MP_EXPORT_URL, headers=mp_auth_header(), timeout=300, params={
                'project_id': MIXPANEL_PROJECT_ID,
                'from_date': cs, 'to_date': ce,
                'event': json.dumps(PAYMENT_EVENTS)
            })
            if r.status_code != 200:
                return local, lt, ls
            for line in r.text.strip().split('\n'):
                if not line.strip(): continue
                try:
                    p = json.loads(line).get('properties', {})
                    lt += 1
                    ch = str(p.get('ch', '')).lower().strip()
                    ct = str(p.get('ct', '')).lower().strip()
                    cr = str(p.get('cr', '')).strip()
                    if ch != 'naver' or 'nsa' not in ct: continue
                    if cr in INVALID_CR or cr.startswith('{'):
                        ls += 1; continue
                    amt = safe_float(p.get('amount', 0))
                    val = safe_float(p.get('value', 0))
                    rev = amt if amt > 0 else (val if val > 0 else 0)
                    ts = int(p.get('time', 0))
                    dt = datetime.fromtimestamp(ts, tz=KST).strftime('%Y-%m-%d')
                    did = str(p.get('distinct_id', ''))
                    svc = str(p.get('서비스', ''))
                    local.append({'date': dt, 'cr': cr, 'revenue': rev,
                                  'did': did, 'svc': svc, 'ts': ts})
                except Exception:
                    continue
        except Exception:
            pass
        with lock:
            stats['done'] += 1
            stats['nsa_matched'] += len(local)
            stats['total'] += lt
            stats['skipped_invalid'] += ls
            if stats['done'] % 3 == 0 or stats['done'] == len(chunks):
                print(f"    {stats['done']}/{len(chunks)} (NSA {stats['nsa_matched']:,}건)")
        return local, lt, ls

    raw = []
    with ThreadPoolExecutor(max_workers=MP_WORKERS) as ex:
        for f in as_completed([ex.submit(fetch_chunk, c) for c in chunks]):
            events, _, _ = f.result()
            raw.extend(events)

    if stats['skipped_invalid'] > 0:
        print(f"  🚫 무효 cr 스킵: {stats['skipped_invalid']}건")
    if not raw:
        print(f"  ⚠️ 매칭 없음")
        return defaultdict(lambda: defaultdict(lambda: {'revenue': 0, 'count': 0}))

    df_r = pd.DataFrame(raw).sort_values('revenue', ascending=False)
    before = len(df_r)
    df_r = df_r.drop_duplicates(subset=['date', 'did', 'svc'], keep='first')
    print(f"  ✅ 중복 제거: {before} → {len(df_r)}건")

    has_sessions = sa_sessions and len(sa_sessions) > 0
    if has_sessions:
        print(f"  🎯 세션 매칭 (윈도우: {SESSION_WINDOW_SEC//60}분)...")
        def within(r):
            landings = sa_sessions.get(r['did'])
            if not landings: return False
            idx = bisect.bisect_right(landings, r['ts'])
            if idx > 0 and 0 <= (r['ts'] - landings[idx - 1]) <= SESSION_WINDOW_SEC:
                return True
            return False
        df_r['session_match'] = df_r.apply(within, axis=1)
        direct   = df_r[df_r['session_match']]
        indirect = df_r[~df_r['session_match']]
        print(f"    ✅ 직접전환: {len(direct)}건 / ₩{direct['revenue'].sum():,.0f}")
        print(f"    ⚠️ 간접전환: {len(indirect)}건 / ₩{indirect['revenue'].sum():,.0f}")
        df_use = direct.copy()
    else:
        print(f"  ⚠️ 세션 없음 → 전체 귀속")
        df_use = df_r.copy()

    revenue = defaultdict(lambda: defaultdict(lambda: {'revenue': 0, 'count': 0}))
    for _, r in df_use.iterrows():
        revenue[r['cr']][r['date']]['revenue'] += r['revenue']
        revenue[r['cr']][r['date']]['count'] += 1
    return revenue

def merge(df_naver, rev_data):
    print("\n🔄 병합...")
    nkw = set(df_naver['키워드'].unique()) if len(df_naver) else set()
    mcr = set(rev_data.keys())
    print(f"  네이버 {len(nkw)}개 / 믹스패널 {len(mcr)}개 / 매칭 {len(nkw & mcr)}개")

    rl, cl = [], []
    for _, r in df_naver.iterrows():
        d = rev_data.get(r['키워드'], {}).get(r['Date'], {})
        rl.append(d.get('revenue', 0))
        cl.append(d.get('count', 0))

    df = df_naver.copy()
    df['결과(믹스패널)'] = cl
    df['매출'] = rl
    df['이익'] = df['매출'] - df['총비용(VAT포함,원)']
    df['ROAS'] = df.apply(lambda r:
        round(r['매출'] / r['총비용(VAT포함,원)'] * 100, 1) if r['총비용(VAT포함,원)'] > 0
        else (99999 if r['매출'] > 0 else 0), axis=1)
    df['CVR(%)'] = df.apply(lambda r:
        round(r['결과(믹스패널)'] / r['클릭수'] * 100, 2) if r['클릭수'] > 0 else 0, axis=1)

    print(f"✅ {len(df)}건, 비용 ₩{df['총비용(VAT포함,원)'].sum():,.0f}, 매출 ₩{df['매출'].sum():,.0f}")
    return df

# ══════════════════════════════════════════════════════════════
# ★ Supabase 업서트
# ══════════════════════════════════════════════════════════════
def upsert_naver_sa_daily(df):
    print(f"\n📤 Supabase → naver_sa_daily 업서트")
    if df.empty:
        print("  ⚠️ 빈 DataFrame, 스킵")
        return

    records = []
    for _, r in df.iterrows():
        dt = r['Date']
        if not dt: continue
        cp = (r['캠페인']  or '').strip() or 'unknown'
        ag = (r['광고그룹'] or '').strip() or 'unknown'
        kw = (r['키워드']   or '').strip() or 'unknown'
        records.append({
            'date':        dt,
            'campaign':    cp,
            'adgroup':     ag,
            'keyword':     kw,
            'cost_vat':    safe_float(r['총비용(VAT포함,원)']),
            'clicks':      safe_int(r['클릭수']),
            'impressions': safe_int(r['노출수']),
            'ctr':         safe_float(r['CTR(%)']),
            'cpc_vat':     safe_float(r['CPC(VAT포함)']),
            'results_mp':  safe_int(r['결과(믹스패널)']),
            'revenue':     safe_float(r['매출']),
            'profit':      safe_float(r['이익']),
            'roas':        safe_float(r['ROAS']),
            'cvr':         safe_float(r['CVR(%)']),
        })

    print(f"  총 {len(records)}행 업서트 준비")
    if DRY_RUN:
        print(f"  [DRY_RUN] 실제 쓰기 스킵 — 샘플:")
        for rec in records[:3]:
            print(f"    {rec}")
        return

    BATCH = 500
    ok = 0
    for i in range(0, len(records), BATCH):
        batch = records[i:i+BATCH]
        try:
            sb.table('naver_sa_daily').upsert(
                batch,
                on_conflict='date,campaign,adgroup,keyword',
            ).execute()
            ok += len(batch)
            if (i // BATCH) % 5 == 0 or ok == len(records):
                print(f"    ✅ {ok}/{len(records)}")
        except Exception as e:
            print(f"    ⚠️ 배치 {i}~{i+len(batch)} 실패: {e}")
    print(f"  ✅ naver_sa_daily: {ok}/{len(records)}건 업서트")

def upsert_google_ads_daily_aggregate(google_daily):
    """URL_NAV_GOO 시트에서 읽은 구글 일별 집계를 google_ads_daily에 저장.
       키워드 단위 데이터가 없으므로 synthetic key (TOTAL/TOTAL/TOTAL) 사용."""
    print(f"\n📤 Supabase → google_ads_daily 업서트 (집계 only)")
    if not google_daily:
        print("  ⚠️ 빈 데이터, 스킵")
        return
    records = []
    for dt, d in google_daily.items():
        spend = safe_float(d.get('spend', 0))
        revenue = safe_float(d.get('revenue', 0))
        if spend == 0 and revenue == 0: continue
        dt_str = dt.strftime('%Y-%m-%d') if hasattr(dt, 'strftime') else str(dt)
        records.append({
            'date':     dt_str,
            'campaign': 'TOTAL',
            'adgroup':  'TOTAL',
            'keyword':  'TOTAL',
            'cost_vat': spend,
            'revenue':  revenue,
            'profit':   revenue - spend,
            'roas':     round(revenue / spend * 100, 1) if spend > 0 else 0,
        })

    print(f"  총 {len(records)}일 업서트")
    if DRY_RUN:
        print(f"  [DRY_RUN] 실제 쓰기 스킵")
        return
    try:
        sb.table('google_ads_daily').upsert(
            records,
            on_conflict='date,campaign,adgroup,keyword',
        ).execute()
        print(f"  ✅ google_ads_daily: {len(records)}건")
    except Exception as e:
        print(f"  ⚠️ 실패: {e}")

# ══════════════════════════════════════════════════════════════
# 구글 Ads 집계 fetch (옵션, gspread 사용)
# ══════════════════════════════════════════════════════════════
def fetch_google_aggregate_from_sheet():
    """SYNC_GOOGLE_ADS=true 인 경우만 호출. gspread로 네이버/구글 Daily 시트 읽기."""
    try:
        import gspread, tempfile
    except ImportError:
        print("  ⚠️ gspread 미설치 → 구글Ads 동기화 스킵")
        return {}

    URL_NAV_GOO = os.environ.get("URL_NAV_GOO", "")
    if not URL_NAV_GOO:
        print("  ⚠️ URL_NAV_GOO 미설정, 스킵")
        return {}
    gcp_json = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
    if not gcp_json:
        print("  ⚠️ GCP_SERVICE_ACCOUNT_KEY 미설정, 스킵")
        return {}

    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    tmp.write(gcp_json); tmp.close()
    gc = gspread.service_account(filename=tmp.name)

    print(f"\n📖 구글 Ads 집계 fetch (네이버/구글 Daily 시트)...")
    try:
        ss = gc.open_by_url(URL_NAV_GOO)
    except Exception as e:
        print(f"  ⚠️ 시트 열기 실패: {e}")
        return {}

    ws = None
    for s in ss.worksheets():
        if '네이버' in s.title and 'Daily' in s.title:
            ws = s; break
    if ws is None:
        print("  ⚠️ 'Daily' 탭 없음"); return {}

    vals = ws.get_all_values()
    if not vals or len(vals) < 3:
        return {}

    header_row_idx = None
    for ri in range(min(5, len(vals))):
        row_str = ' '.join(str(v).strip() for v in vals[ri])
        if '브랜드 지출' in row_str or '총 지출' in row_str:
            header_row_idx = ri; break
    if header_row_idx is None:
        return {}

    headers = [str(v).strip() for v in vals[header_row_idx]]
    d3_col = None
    for ci, h in enumerate(headers):
        if 'D3' in h.upper() and 'COST' in h.upper():
            d3_col = ci; break

    google_spend_cols, google_rev_col = [], None
    date_col = 0
    data_start_row = header_row_idx + 1

    if d3_col is not None:
        for ci in range(d3_col + 1, len(headers)):
            h = headers[ci]
            if h in ('브랜드 지출', '일반 지출'):
                google_spend_cols.append(ci)
            elif h in ('구매전환값', '총구매전환값', '총 구매전환값') and google_rev_col is None:
                google_rev_col = ci
        for ci, h in enumerate(headers):
            if h == '일':
                date_col = ci; break

    start_d = datetime.strptime(START_DATE_STR, '%Y-%m-%d').date()
    end_d   = datetime.strptime(END_DATE_STR, '%Y-%m-%d').date()
    out = defaultdict(lambda: {'spend': 0.0, 'revenue': 0.0})
    for ri in range(data_start_row, len(vals)):
        row = vals[ri]
        dt = parse_date(row[date_col]) if date_col < len(row) else None
        if dt is None or dt < start_d or dt > end_d: continue
        for sc in google_spend_cols:
            if sc < len(row): out[dt]['spend'] += _num(row[sc])
        if google_rev_col is not None and google_rev_col < len(row):
            out[dt]['revenue'] += _num(row[google_rev_col])

    print(f"  → {len(out)}일 수집")
    return dict(out)

# ══════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════
def main():
    # ─ 1) 네이버 SA API 수집 ─
    df_naver, date_range = collect_naver()
    if df_naver.empty:
        print("⚠️ 네이버 데이터 없음 → 종료")
        return

    # ─ 2) 랜딩 이벤트 탐지 + 세션 수집 ─
    detected = detect_landing_events(date_range[1])
    if detected:
        global LANDING_EVENTS
        LANDING_EVENTS = detected
    sa_sessions = collect_sa_landings(date_range[0], date_range[1])

    # ─ 3) 결제 + 세션 매칭 ─
    rev = get_mixpanel(date_range[0], date_range[1], sa_sessions=sa_sessions)

    # ─ 4) 병합 ─
    df = merge(df_naver, rev)
    print(f"\n📊 최종: {len(df)}행 ({df['Date'].min()} ~ {df['Date'].max()})")

    # ─ 5) Supabase 업서트 ─
    upsert_naver_sa_daily(df)

    # ─ 6) (옵션) 구글 Ads 집계 동기화 ─
    if SYNC_GOOGLE_ADS:
        google_daily = fetch_google_aggregate_from_sheet()
        if google_daily:
            upsert_google_ads_daily_aggregate(google_daily)

    # ─ 완료 ─
    tc = safe_float(df['총비용(VAT포함,원)'].sum())
    tr = safe_float(df['매출'].sum())
    roas = (tr / tc * 100) if tc > 0 else 0
    print("\n" + "=" * 60)
    print("🎉 완료!")
    print(f"   비용:  ₩{tc:,.0f}")
    print(f"   매출:  ₩{tr:,.0f} (직접전환)")
    print(f"   ROAS:  {roas:.1f}%")
    print(f"   → naver_sa_daily: {len(df)}건")
    print("=" * 60)
    print("\n💡 추이차트/주간/채널별 뷰 확인:")
    print("   SELECT * FROM v_naver_sa_trend_daily   ORDER BY date DESC LIMIT 20;")
    print("   SELECT * FROM v_naver_sa_trend_weekly  ORDER BY week_start DESC LIMIT 20;")
    print("   SELECT * FROM v_channel_trend_daily    WHERE date >= CURRENT_DATE - 30;")


if __name__ == "__main__":
    main()
