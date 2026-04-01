# -*- coding: utf-8 -*-
"""
[Vanced] 소재별 퍼포먼스 데이터 자동 업데이트 — Parallel Edition
Meta Ads API (ad-level: 지출/노출/클릭) + Mixpanel (매출/결과수 - utm_content 매칭)
→ Google Sheets 전체 탭 자동 갱신

★ 핵심
  - Meta: ad-level(소재별) 데이터 수집
  - Mixpanel: '결제완료' + 'payment_complete' OR 조회
  - amount 필드: 'amount' → '결제금액' → 'value' 순으로 OR 추출
  - utm_content → ad_id 매칭 (소재별)
  - 모든 I/O를 병렬(ThreadPoolExecutor) 처리
  - 탭: 마스터탭, 추이차트, 증감액, 추이차트(주간), 예산, 주간종합, 주간종합_2, 주간종합_3
"""

import os
import json
import time
import calendar
import threading
import gspread
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

print("=" * 60)
print("🚀 [Vanced] 소재별 자동화 (★ 병렬 최적화)")
print("   Meta ad-level + Mixpanel utm_content 매칭")
print("=" * 60)

# =============================================================================
# 환경변수에서 설정 로드
# =============================================================================
def load_env():
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    except NameError:
        print("ℹ️  Colab 환경 감지 → .env 파일 로드 스킵")
        return
    if os.path.exists(env_path):
        print(f"ℹ️  .env 파일 로드: {env_path}")
        with open(env_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, val = line.split('=', 1)
                    os.environ.setdefault(key.strip(), val.strip())

load_env()

# =============================================================================
# 환경변수 읽기
# =============================================================================
def get_config():
    cfg = {}
    cfg['META_ACCESS_TOKEN']   = os.environ.get('META_TOKEN_VANCED', '')
    cfg['META_AD_ACCOUNT_ID']  = os.environ.get('META_AD_ACCOUNT_ID_VANCED', 'act_25183853061243175')
    cfg['META_API_VERSION']    = os.environ.get('META_API_VERSION', 'v21.0')
    cfg['META_BASE_URL']       = f"https://graph.facebook.com/{cfg['META_API_VERSION']}"

    cfg['MIXPANEL_PROJECT_ID'] = os.environ.get('MIXPANEL_PROJECT_ID', '3390233')
    cfg['MIXPANEL_USERNAME']   = os.environ.get('MIXPANEL_USERNAME', '')
    cfg['MIXPANEL_SECRET']     = os.environ.get('MIXPANEL_SECRET', '')
    cfg['MIXPANEL_EVENT_NAMES'] = ['결제완료', 'payment_complete']

    cfg['SPREADSHEET_URL'] = os.environ.get(
        'SPREADSHEET_URL_VANCED_AD',
        'https://docs.google.com/spreadsheets/d/13Lfyp-buMOmNYHqxKeGWyQr4Vw4hboKkzjCQBkc4Wrg/edit?usp=sharing'
    )
    cfg['GCP_SA_KEY_JSON'] = os.environ.get('GCP_SERVICE_ACCOUNT_KEY', '')

    cfg['FROM_DATE'] = os.environ.get('FROM_DATE', '2025-11-09')
    cfg['TO_DATE']   = os.environ.get('TO_DATE', datetime.now().strftime('%Y-%m-%d'))

    missing = []
    if not cfg['META_ACCESS_TOKEN']:   missing.append('META_TOKEN_VANCED')
    if not cfg['GCP_SA_KEY_JSON']:     missing.append('GCP_SERVICE_ACCOUNT_KEY')
    if not cfg['MIXPANEL_USERNAME']:   missing.append('MIXPANEL_USERNAME')
    if not cfg['MIXPANEL_SECRET']:     missing.append('MIXPANEL_SECRET')

    if missing:
        print("\n❌ 필수 환경변수가 설정되지 않았습니다:")
        for m in missing:
            print(f"   - {m}")
        raise ValueError(f"필수 환경변수 누락: {', '.join(missing)}")

    return cfg

# =============================================================================
# Google Sheets 인증
# =============================================================================
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

def get_gc(cfg):
    sa_info = json.loads(cfg['GCP_SA_KEY_JSON'])
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)

# =============================================================================
# 병렬 제어
# =============================================================================
WRITE_SEMAPHORE = threading.Semaphore(2)
FORMAT_SEMAPHORE = threading.Semaphore(1)  # ★ 서식 적용은 1개씩 직렬 실행

def with_retry(fn, *args, max_retries=8, **kwargs):
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e):
                wait_time = 20 + (attempt * 15)
                print(f"  ⏳ Sheets Rate limit. {wait_time:.0f}초 대기 (시도 {attempt+1}/{max_retries})")
                time.sleep(wait_time)
            elif attempt == max_retries - 1:
                raise
            else:
                time.sleep(5)
    return None

# =============================================================================
# 상수
# =============================================================================
PRODUCTS = ['29금궁합', 'Solo', 'Money', '29금', '1%', 'Kids', 'Reunion', 'career', 'Year']

AD_PRODUCT_PREFIXES = [
    ('29금궁합', '29금궁합'), ('29금', '29금'), ('Solo', 'Solo'), ('Kids', 'Kids'),
    ('Money', 'Money'), ('Reunion', 'Reunion'), ('1%', '1%'),
    ('career', 'career'), ('Year', 'Year'), ('Star', 'Star'),
]

PURCHASE_TYPES = ['offsite_conversion.fb_pixel_purchase', 'purchase', 'omni_purchase']

ANALYSIS_TAB_NAMES = ["추이차트", "추이차트(주간)", "증감액", "예산", "주간종합", "주간종합_2", "주간종합_3", "마스터탭"]

# =============================================================================
# 유틸
# =============================================================================
def ad_to_product(adset_name):
    """adset_name 기준으로 제품 분류 (소재는 adset 하위이므로 adset명으로 분류)"""
    for prefix, product in AD_PRODUCT_PREFIXES:
        if adset_name.startswith(prefix):
            return product
    return 'etc'

def meta_date_to_key(ds):
    dt = datetime.strptime(ds, '%Y-%m-%d')
    return f"{dt.month}/{dt.day}"

def date_key_to_dt(dk):
    m, d = int(dk.split('/')[0]), int(dk.split('/')[1])
    return datetime(2025 if m >= 11 else 2026, m, d)

def date_key_to_weekday(dk):
    return ['월','화','수','목','금','토','일'][date_key_to_dt(dk).weekday()]

def extract_action(al, types):
    if not al:
        return 0
    for a in al:
        if a.get('action_type') in types:
            return float(a.get('value', 0))
    return 0

def extract_outbound(dl):
    if not dl:
        return 0
    for a in dl:
        if a.get('action_type') in ['outbound_click', 'link_click']:
            return float(a.get('value', 0))
    return float(dl[0].get('value', 0)) if len(dl) == 1 else 0

def fmt(v):
    return '0' if not v else f"{int(round(v)):,}"

def fmtp(v):
    return '0.00%' if not v else f"{v:.2f}%"

def trend_cell(spend, revenue, profit, roas):
    return f"{round(roas)}\n₩{fmt(profit)}\n+₩{fmt(revenue)}\n-₩{fmt(spend)}"

def change_cell(roas, chg, spend, cpm):
    cs = f"+{chg:.1f}%" if chg >= 0 else f"{chg:.1f}%"
    return f"{round(roas)}\n{cs}\n-₩{fmt(spend)}\n₩{fmt(cpm)}"

# =============================================================================
# 리치텍스트 포맷팅
# =============================================================================
BLACK        = {"red": 0, "green": 0, "blue": 0}
DARK_GREEN   = {"red": 0.22, "green": 0.46, "blue": 0.11}
BRIGHT_GREEN = {"red": 0, "green": 0.70, "blue": 0}
RED          = {"red": 0.85, "green": 0, "blue": 0}

def _fmt(color):
    return {"foregroundColor": color, "bold": True, "fontFamily": "Arial"}

def apply_rich_text(sh, ws, data_2d, start_row_idx, start_col_idx, cell_type='trend'):
    sheet_id = ws.id
    api_rows = []
    for row_data in data_2d:
        api_cells = []
        for cell_text in row_data:
            text = str(cell_text) if cell_text else ''
            lines = text.split('\n')
            if len(lines) != 4 or not text.strip():
                api_cells.append({})
                continue
            idx1 = len(lines[0]) + 1
            idx2 = idx1 + len(lines[1]) + 1
            idx3 = idx2 + len(lines[2]) + 1
            if cell_type == 'trend':
                runs = [
                    {"startIndex": 0, "format": _fmt(BLACK)},
                    {"startIndex": idx1, "format": _fmt(DARK_GREEN)},
                    {"startIndex": idx2, "format": _fmt(BLACK)},
                    {"startIndex": idx3, "format": _fmt(RED)},
                ]
            elif cell_type == 'change':
                chg_color = BRIGHT_GREEN if lines[1].startswith('+') else RED
                runs = [
                    {"startIndex": 0, "format": _fmt(BLACK)},
                    {"startIndex": idx1, "format": _fmt(chg_color)},
                    {"startIndex": idx2, "format": _fmt(RED)},
                    {"startIndex": idx3, "format": _fmt(BLACK)},
                ]
            else:
                api_cells.append({})
                continue
            api_cells.append({
                "userEnteredValue": {"stringValue": text},
                "userEnteredFormat": {"verticalAlignment": "TOP", "wrapStrategy": "WRAP"},
                "textFormatRuns": runs,
            })
        api_rows.append({"values": api_cells})
    if not api_rows:
        return
    max_cols = max((len(r['values']) for r in api_rows), default=0)
    CHUNK = 20  # ★ 50→20 축소: API 안정성 향상
    applied, failed = 0, 0
    for i in range(0, len(api_rows), CHUNK):
        chunk = api_rows[i:i+CHUNK]
        try:
            # ★ 세마포어로 병렬 서식 충돌 방지
            with FORMAT_SEMAPHORE:
                with_retry(sh.batch_update, body={"requests": [{"updateCells": {
                    "rows": chunk,
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row_idx + i,
                        "endRowIndex": start_row_idx + i + len(chunk),
                        "startColumnIndex": start_col_idx,
                        "endColumnIndex": start_col_idx + max_cols,
                    },
                    "fields": "userEnteredValue,userEnteredFormat,textFormatRuns"
                }}]})
            applied += len(chunk)
        except Exception as e:
            failed += len(chunk)
            print(f"      ⚠️ 리치텍스트 청크 실패 (행 {i+1}~{i+len(chunk)}): {e}")
        time.sleep(2)  # ★ 1→2초: API 쿨다운 여유
    total = applied + failed
    if failed:
        print(f"      → 리치텍스트 포맷: {applied}/{total}행 성공, {failed}행 실패")
    else:
        print(f"      → 리치텍스트 포맷 적용 완료 ({applied}행)")

# =============================================================================
# Part 1: Meta Ads API — ad-level fetch (★ 재시도 로직 포함)
# =============================================================================
META_MAX_RETRIES = 3          # 최대 재시도 횟수
META_RETRY_WAIT  = [60, 120, 180]  # 시도별 대기 시간(초)
META_RETRYABLE_CODES = {1, 2, 4, 17, 32, 613}  # 재시도 가능한 Meta error codes
META_RETRYABLE_SUBCODES = {99, 1504044, 2446079}  # 재시도 가능한 subcodes

def _is_meta_retryable(resp):
    """Meta API 응답이 재시도 가능한 일시적 오류인지 판별"""
    if resp.status_code in (500, 502, 503):
        return True  # ★ 서버 오류는 무조건 재시도
    if resp.status_code == 429:
        return True
    if resp.status_code == 400:
        try:
            err = resp.json().get('error', {})
            if err.get('is_transient', False):
                return True
            if err.get('code') in META_RETRYABLE_CODES:
                return True
            if err.get('error_subcode') in META_RETRYABLE_SUBCODES:
                return True
        except (json.JSONDecodeError, KeyError):
            pass
    return False

def _meta_request_with_retry(url, params=None, max_retries=META_MAX_RETRIES):
    """단일 Meta API 요청에 재시도 로직 적용"""
    for attempt in range(max_retries + 1):
        try:
            if params:
                resp = requests.get(url, params=params, timeout=120)
            else:
                resp = requests.get(url, timeout=120)

            # 성공
            if resp.status_code == 200:
                return resp

            # 재시도 가능한 오류
            if _is_meta_retryable(resp) and attempt < max_retries:
                wait = META_RETRY_WAIT[min(attempt, len(META_RETRY_WAIT) - 1)]
                err_msg = ''
                try:
                    err_msg = resp.json().get('error', {}).get('message', '')[:100]
                except Exception:
                    err_msg = resp.text[:100]
                print(f"   ⚠️ Meta API 일시 오류 (HTTP {resp.status_code}): {err_msg}")
                print(f"   🔄 {wait}초 대기 후 재시도 ({attempt + 1}/{max_retries})...")
                time.sleep(wait)
                continue

            # 재시도 불가능한 오류 → 그대로 반환
            return resp

        except requests.exceptions.Timeout:
            if attempt < max_retries:
                wait = META_RETRY_WAIT[min(attempt, len(META_RETRY_WAIT) - 1)]
                print(f"   ⚠️ Meta API 타임아웃")
                print(f"   🔄 {wait}초 대기 후 재시도 ({attempt + 1}/{max_retries})...")
                time.sleep(wait)
                continue
            raise
        except requests.exceptions.ConnectionError:
            if attempt < max_retries:
                wait = META_RETRY_WAIT[min(attempt, len(META_RETRY_WAIT) - 1)]
                print(f"   ⚠️ Meta API 연결 오류")
                print(f"   🔄 {wait}초 대기 후 재시도 ({attempt + 1}/{max_retries})...")
                time.sleep(wait)
                continue
            raise

    # 모든 재시도 소진 — 마지막 응답 반환
    return resp


def _generate_monthly_ranges(from_date, to_date):
    """날짜 범위를 월별 청크로 분할"""
    chunks = []
    start = datetime.strptime(from_date, '%Y-%m-%d')
    end = datetime.strptime(to_date, '%Y-%m-%d')
    while start <= end:
        # 해당 월의 마지막 날
        last_day = calendar.monthrange(start.year, start.month)[1]
        month_end = datetime(start.year, start.month, last_day)
        chunk_end = min(month_end, end)
        chunks.append((start.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
        # 다음 월 1일로 이동
        start = chunk_end + timedelta(days=1)
    return chunks


def fetch_meta_raw(cfg):
    """Meta API에서 ad-level raw 데이터 가져옴 (★ 월별 분할 + 재시도 로직)"""
    print(f"\n📡 [Meta] ad(소재) 일간 데이터 fetch ({cfg['FROM_DATE']} ~ {cfg['TO_DATE']})...")

    # ★ 날짜 범위를 월별로 분할하여 Meta 서버 부하 방지
    monthly_ranges = _generate_monthly_ranges(cfg['FROM_DATE'], cfg['TO_DATE'])
    print(f"   📅 월별 분할: {len(monthly_ranges)}개 청크")

    meta_raw = []
    for chunk_idx, (chunk_from, chunk_to) in enumerate(monthly_ranges, 1):
        print(f"\n   📦 [{chunk_idx}/{len(monthly_ranges)}] {chunk_from} ~ {chunk_to}")
        next_url, page = None, 0
        while True:
            page += 1
            if next_url:
                resp = _meta_request_with_retry(next_url)
            else:
                resp = _meta_request_with_retry(
                    f"{cfg['META_BASE_URL']}/{cfg['META_AD_ACCOUNT_ID']}/insights",
                    params={
                        'access_token': cfg['META_ACCESS_TOKEN'],
                        'fields': 'campaign_name,adset_name,adset_id,ad_name,ad_id,'
                                  'spend,cpm,reach,impressions,frequency,'
                                  'actions,action_values,cost_per_action_type,purchase_roas,'
                                  'unique_outbound_clicks,unique_outbound_clicks_ctr,cost_per_unique_outbound_click',
                        'level': 'ad',
                        'time_increment': 1,
                        'time_range': json.dumps({'since': chunk_from, 'until': chunk_to}),
                        'limit': 500,
                        'filtering': json.dumps([{'field': 'spend', 'operator': 'GREATER_THAN', 'value': '0'}]),
                    },
                )
            if resp.status_code != 200:
                print(f"   ❌ Meta API 오류 {resp.status_code} (재시도 소진): {resp.text[:300]}")
                break
            data = resp.json()
            rows = data.get('data', [])
            meta_raw.extend(rows)
            print(f"      페이지 {page}: +{len(rows)}건 (누적 {len(meta_raw)}건)")
            next_url = data.get('paging', {}).get('next')
            if not next_url or not rows:
                break
            time.sleep(0.5)
        time.sleep(1)  # 청크 간 쿨다운

    print(f"\n✅ Meta: {len(meta_raw)}건 (총 {len(monthly_ranges)}개 월별 청크)")
    return meta_raw

def parse_meta(meta_raw):
    """Meta raw → meta_data, all_ads 파싱 (소재별)"""
    meta_data, all_ads = {}, {}
    for row in meta_raw:
        dk = meta_date_to_key(row['date_start'])
        ad_id = row.get('ad_id', '')
        ad_name = row.get('ad_name', '')
        adset_name = row.get('adset_name', '')
        adset_id = row.get('adset_id', '')
        cn = row.get('campaign_name', '')
        sp = float(row.get('spend', 0))
        imp = float(row.get('impressions', 0))
        pr_list = row.get('purchase_roas', [])

        meta_data[(dk, ad_id)] = {
            'date': dk,
            'campaign_name': cn,
            'adset_name': adset_name,
            'adset_id': adset_id,
            'ad_name': ad_name,
            'ad_id': ad_id,
            'product': ad_to_product(adset_name),
            'spend': sp,
            'cpm': float(row.get('cpm', 0)),
            'reach': float(row.get('reach', 0)),
            'impressions': imp,
            'frequency': float(row.get('frequency', 0)),
            'unique_ob_clicks': extract_outbound(row.get('unique_outbound_clicks')),
            'unique_ob_ctr': extract_outbound(row.get('unique_outbound_clicks_ctr')),
            'cost_per_unique_ob': extract_outbound(row.get('cost_per_unique_outbound_click')),
            'purchases_meta': extract_action(row.get('actions'), PURCHASE_TYPES),
            'revenue_meta': extract_action(row.get('action_values'), PURCHASE_TYPES),
            'cost_per_purchase': extract_action(row.get('cost_per_action_type'), PURCHASE_TYPES),
            'purchase_roas': float(pr_list[0]['value']) * 100 if pr_list else 0,
        }
        if ad_id not in all_ads:
            all_ads[ad_id] = {
                'campaign_name': cn,
                'adset_name': adset_name,
                'adset_id': adset_id,
                'ad_name': ad_name,
                'product': ad_to_product(adset_name),
            }
    print(f"   소재(ad): {len(all_ads)}개")
    return meta_data, all_ads

# =============================================================================
# Part 2: Mixpanel — raw fetch + 매칭
# =============================================================================
def fetch_mixpanel_raw(cfg):
    """Mixpanel에서 raw 이벤트 가져옴"""
    print(f"\n📡 [Mixpanel] 결제 이벤트 조회 (이벤트: {cfg['MIXPANEL_EVENT_NAMES']})...")
    resp = requests.get(
        "https://data.mixpanel.com/api/2.0/export",
        params={
            'from_date': cfg['FROM_DATE'],
            'to_date': cfg['TO_DATE'],
            'event': json.dumps(cfg['MIXPANEL_EVENT_NAMES']),
            'project_id': cfg['MIXPANEL_PROJECT_ID'],
        },
        auth=(cfg['MIXPANEL_USERNAME'], cfg['MIXPANEL_SECRET']),
        timeout=300,
    )
    if resp.status_code != 200:
        print(f"❌ Mixpanel 오류: {resp.status_code} - {resp.text[:300]}")
        return []
    lines = [l for l in resp.text.split('\n') if l.strip()]
    print(f"✅ Mixpanel: {len(lines)}건 수신")
    return lines

def parse_and_match_mixpanel(lines, all_ads):
    """
    Mixpanel raw lines → 파싱 + 중복제거 + ad_id 매칭 (소재별)
    ★ utm_content 값을 ad_id로 직접 매칭 (소재별은 utm_content 사용)
    """
    mp_matched = defaultdict(lambda: {'count': 0, 'revenue': 0.0})
    total_events, matched_events, unmatched_events = 0, 0, 0

    if not lines:
        return mp_matched, 0, 0, 0

    seen, events_parsed = set(), []
    for line in lines:
        try:
            ev = json.loads(line)
            props = ev.get('properties', {})
            ts = props.get('time', 0)
            if not ts:
                continue
            dt_kst = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)
            dk = f"{dt_kst.month}/{dt_kst.day}"
            utm_content = str(props.get('utm_content', '') or '').strip()

            # amount 필드: 'amount' → '결제금액' → 'value' 순서
            amount = 0.0
            for field_name in ('amount', '결제금액', 'value'):
                raw = props.get(field_name)
                if raw is not None and str(raw).strip() not in ('', 'None', 'nan', 'null'):
                    try:
                        val = float(raw)
                        if val > 0:
                            amount = val
                            break
                    except (ValueError, TypeError):
                        pass

            events_parsed.append({
                'dk': dk,
                'ts': ts,
                'utm_content': utm_content,
                'amount': amount,
                'distinct_id': props.get('distinct_id', ''),
                'service': str(props.get('서비스', '')).strip(),
            })
        except (json.JSONDecodeError, KeyError):
            pass

    events_parsed.sort(key=lambda x: x['ts'])
    deduped = []
    for e in events_parsed:
        key = (e['dk'], e['distinct_id'], e['service'])
        if key not in seen:
            seen.add(key)
            deduped.append(e)
    total_events = len(deduped)
    print(f"   중복 제거 후: {total_events}건")

    for e in deduped:
        utm = e['utm_content']
        if not utm:
            unmatched_events += 1
            continue

        # ★ utm_content → ad_id 직접 매칭
        if utm in all_ads:
            mp_matched[(e['dk'], utm)]['count'] += 1
            mp_matched[(e['dk'], utm)]['revenue'] += e['amount']
            matched_events += 1
        else:
            unmatched_events += 1

    print(f"   ★ 매칭: {matched_events}건 ({matched_events / max(total_events, 1) * 100:.1f}%)")
    print(f"     - 미매칭: {unmatched_events}건")
    return mp_matched, total_events, matched_events, unmatched_events

# =============================================================================
# Part 3: 마스터 데이터 조합
# =============================================================================
def build_master(meta_data, mp_matched):
    print(f"\n🔧 [3] 마스터탭 데이터 조합...")
    master_rows = []
    for key, meta in sorted(meta_data.items(), key=lambda x: (date_key_to_dt(x[0][0]), x[0][1]), reverse=True):
        dk, ad_id = key
        mp = mp_matched.get((dk, ad_id), {'count': 0, 'revenue': 0.0})
        profit = mp['revenue'] - meta['spend']
        roas = mp['revenue'] / meta['spend'] if meta['spend'] > 0 else 0
        cvr = (mp['count'] / meta['unique_ob_clicks'] * 100) if meta['unique_ob_clicks'] > 0 else 0
        master_rows.append({
            **meta,
            'purchases_mp': mp['count'],
            'revenue_mp': mp['revenue'],
            'profit': profit,
            'roas_calc': roas,
            'cvr': cvr,
        })
    all_dates = sorted(set(r['date'] for r in master_rows), key=date_key_to_dt, reverse=True)
    print(f"   {len(master_rows)}행, {len(all_dates)}일")

    print(f"\n   📊 최근 5일 ROAS 검증:")
    for date in all_dates[:5]:
        rows = [r for r in master_rows if r['date'] == date]
        ts = sum(r['spend'] for r in rows)
        tr = sum(r['revenue_mp'] for r in rows)
        tp = sum(r['purchases_mp'] for r in rows)
        print(f"   {date}: 지출=₩{ts:,.0f} | 결과(MP)={tp:.0f}건 | MP매출=₩{tr:,.0f} | ROAS={tr / ts * 100:.0f}%" if ts > 0 else f"   {date}: 지출=₩0")
    return master_rows, all_dates

# =============================================================================
# Part 4: 파생 데이터
# =============================================================================
def calc_derived(master_rows, all_dates):
    print(f"\n📊 [4] 파생 데이터 계산...")
    daily_total = {}
    for date in all_dates:
        rows = [r for r in master_rows if r['date'] == date]
        sp = sum(r['spend'] for r in rows)
        rv = sum(r['revenue_mp'] for r in rows)
        imp = sum(r['impressions'] for r in rows)
        uc = sum(r['unique_ob_clicks'] for r in rows)
        daily_total[date] = {
            'spend': sp,
            'revenue': rv,
            'profit': rv - sp,
            'roas': rv / sp * 100 if sp > 0 else 0,
            'cvr': sum(r['purchases_mp'] for r in rows) / uc * 100 if uc > 0 else 0,
            'purchases_meta': sum(r['purchases_meta'] for r in rows),
            'purchases_mp': sum(r['purchases_mp'] for r in rows),
            'impressions': imp,
            'cpm': sp / imp * 1000 if imp > 0 else 0,
        }

    daily_product = {}
    for date in all_dates:
        for p in PRODUCTS:
            rows = [r for r in master_rows if r['date'] == date and r['product'] == p]
            if not rows:
                daily_product[(date, p)] = {'spend': 0, 'revenue': 0, 'profit': 0, 'roas': 0}
                continue
            sp = sum(r['spend'] for r in rows)
            rv = sum(r['revenue_mp'] for r in rows)
            daily_product[(date, p)] = {
                'spend': sp,
                'revenue': rv,
                'profit': rv - sp,
                'roas': rv / sp * 100 if sp > 0 else 0,
            }

    ad_daily = defaultdict(dict)
    for r in master_rows:
        ad_daily[r['ad_id']][r['date']] = r
    return daily_total, daily_product, ad_daily

# =============================================================================
# Part 5: Google Sheets 업데이트 — 개별 탭 함수
# =============================================================================

def _update_master_tab(sh, ws, master_rows):
    """[1/8] 마스터탭"""
    print(f"   ⏳ [1/8] 마스터탭 시작...")
    ms = []
    for r in master_rows:
        dt = date_key_to_dt(r['date'])
        ms.append([
            dt.strftime('%Y-%m-%d'), r['campaign_name'], r['adset_name'],
            r['ad_name'], r['ad_id'],
            round(r['spend']), round(r['cost_per_purchase'], 2), round(r['purchase_roas'], 2),
            round(r['cpm'], 2), round(r['reach']), round(r['impressions']),
            round(r['unique_ob_clicks']), round(r['unique_ob_ctr'], 2), round(r['cost_per_unique_ob'], 2),
            round(r['frequency'], 2), round(r['purchases_meta']), round(r['purchases_mp'], 1),
            round(r['revenue_meta']), round(r['revenue_mp']), round(r['profit']),
            round(r['roas_calc'], 4), round(r['cvr'], 2),
        ])
    header = [
        '날짜', '캠페인명', '광고세트명', '소재명', '소재 ID',
        '지출', '결과당비용', '구매ROAS(메타)',
        'CPM', '도달', '노출',
        '고유OB클릭', '고유OB CTR', '고유OB클릭당비용',
        '빈도', '구매(메타)', '구매(MP)',
        '매출(메타)', '매출(MP)', '순이익',
        'ROAS', 'CVR',
    ]
    ws.batch_clear([f'A1:V{ws.row_count}'])
    ws.update(range_name='A1', values=[header] + ms)

    # 서식
    try:
        with_retry(ws.format, 'A1:V1', {
            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
            'textFormat': {'bold': True}
        })
        with_retry(sh.batch_update, body={"requests": [
            {"updateSheetProperties": {
                "properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }}
        ]})
    except Exception as e:
        print(f"      ⚠️ 마스터탭 서식 오류: {e}")

    print(f"   ✅ [1/8] 마스터탭: {len(ms)}행")
    return len(ms)


def _update_budget_tab(ws, dc, daily_total, daily_product):
    """[2/8] 예산"""
    print(f"   ⏳ [2/8] 예산 시작...")
    bd = [[''] + dc]
    bd.append(['전체 쓴돈'] + [round(daily_total.get(d, {}).get('spend', 0)) for d in dc])
    bd.append(['전체 번돈'] + [round(daily_total.get(d, {}).get('revenue', 0)) for d in dc])
    bd.append(['전체 순이익'] + [round(daily_total.get(d, {}).get('profit', 0)) for d in dc])
    bd.append(['총예산'] + [round(daily_total.get(d, {}).get('spend', 0)) for d in dc])
    for p in PRODUCTS:
        bd.append([f'{p} 예산'] + [round(daily_product.get((d, p), {}).get('spend', 0)) for d in dc])
    ws.clear()
    ws.update(range_name='A1', values=bd)
    print(f"   ✅ [2/8] 예산: {len(bd)}행")


def _update_trend_tab(sh, ws, tao, dc, daily_total, ad_daily):
    """[3/8] 추이차트"""
    print(f"   ⏳ [3/8] 추이차트 시작...")
    td = [['캠페인명', '광고세트명', '소재명', '소재 ID', '7일 평균'] + dc]
    for cn, asn, adn, ad_id in tao:
        if cn == '종합':
            row = ['종합', '', '', '']
            r7 = dc[:7]
            ts = sum(daily_total.get(d, {}).get('spend', 0) for d in r7)
            tr = sum(daily_total.get(d, {}).get('revenue', 0) for d in r7)
            row.append(trend_cell(ts, tr, tr - ts, tr / ts * 100 if ts > 0 else 0))
            for date in dc:
                dt = daily_total.get(date, {})
                row.append(trend_cell(dt.get('spend', 0), dt.get('revenue', 0), dt.get('profit', 0), dt.get('roas', 0)))
        else:
            row = [cn or '', asn or '', adn or '', ad_id or '']
            r7 = dc[:7]
            ts = sum(ad_daily.get(ad_id, {}).get(d, {}).get('spend', 0) for d in r7)
            tr = sum(ad_daily.get(ad_id, {}).get(d, {}).get('revenue_mp', 0) for d in r7)
            row.append(trend_cell(ts, tr, tr - ts, tr / ts * 100 if ts > 0 else 0))
            for date in dc:
                ad = ad_daily.get(ad_id, {}).get(date)
                row.append(trend_cell(ad['spend'], ad['revenue_mp'], ad['profit'], ad['roas_calc'] * 100) if ad else '')
        td.append(row)
    ws.clear()
    ws.update(range_name='A1', values=td)
    print(f"   ✅ [3/8] 추이차트: {len(td) - 1}행 × {len(dc) + 5}열")
    time.sleep(2)
    apply_rich_text(sh, ws, [row[4:] for row in td[1:]], 1, 4, 'trend')


def _update_change_tab(sh, ws, tao, dc, daily_total, ad_daily):
    """[4/8] 증감액"""
    print(f"   ⏳ [4/8] 증감액 시작...")
    cd = [['캠페인명', '광고세트명', '소재명', '소재 ID', '7일 평균'] + dc]
    for cn, asn, adn, ad_id in tao:
        if cn == '종합':
            row = ['종합', '', '', '']
            r7 = dc[:7]
            ts = sum(daily_total.get(d, {}).get('spend', 0) for d in r7)
            tr = sum(daily_total.get(d, {}).get('revenue', 0) for d in r7)
            ti = sum(daily_total.get(d, {}).get('impressions', 0) for d in r7)
            row.append(change_cell(tr / ts * 100 if ts > 0 else 0, 0, ts, ts / ti * 1000 if ti > 0 else 0))
            for i, date in enumerate(dc):
                dt = daily_total.get(date, {})
                rt = dt.get('roas', 0)
                pr = daily_total.get(dc[i + 1], {}).get('roas', 0) if i + 1 < len(dc) else 0
                row.append(change_cell(rt, ((rt - pr) / pr * 100) if pr > 0 else 0, dt.get('spend', 0), dt.get('cpm', 0)))
        else:
            row = [cn or '', asn or '', adn or '', ad_id or '']
            r7 = dc[:7]
            ts = sum(ad_daily.get(ad_id, {}).get(d, {}).get('spend', 0) for d in r7)
            tr = sum(ad_daily.get(ad_id, {}).get(d, {}).get('revenue_mp', 0) for d in r7)
            ti = sum(ad_daily.get(ad_id, {}).get(d, {}).get('impressions', 0) for d in r7)
            row.append(change_cell(tr / ts * 100 if ts > 0 else 0, 0, ts, ts / ti * 1000 if ti > 0 else 0))
            for i, date in enumerate(dc):
                ad = ad_daily.get(ad_id, {}).get(date)
                if ad:
                    rt = ad['roas_calc'] * 100
                    pa = ad_daily.get(ad_id, {}).get(dc[i + 1]) if i + 1 < len(dc) else None
                    pr = pa['roas_calc'] * 100 if pa and pa['roas_calc'] > 0 else 0
                    row.append(change_cell(rt, ((rt - pr) / pr * 100) if pr > 0 else 0, ad['spend'], ad['cpm']))
                else:
                    row.append('')
        cd.append(row)
    ws.clear()
    ws.update(range_name='A1', values=cd)
    print(f"   ✅ [4/8] 증감액: {len(cd) - 1}행")
    time.sleep(2)
    apply_rich_text(sh, ws, [row[4:] for row in cd[1:]], 1, 4, 'change')


def _update_weekly_trend_tab(sh, ws, tao, all_dates, daily_total, master_rows):
    """[5/8] 추이차트(주간)"""
    print(f"   ⏳ [5/8] 추이차트(주간) 시작...")
    wp = OrderedDict()
    for date in all_dates:
        dt = date_key_to_dt(date)
        s = dt - timedelta(days=dt.weekday())
        e = s + timedelta(days=6)
        wp.setdefault(f"{s.month}/{s.day}~{e.month}/{e.day}", []).append(date)
    wks = list(wp.keys())

    wt = {}
    for wk, diw in wp.items():
        sp = sum(daily_total.get(d, {}).get('spend', 0) for d in diw)
        rv = sum(daily_total.get(d, {}).get('revenue', 0) for d in diw)
        wt[wk] = {'spend': sp, 'revenue': rv, 'profit': rv - sp, 'roas': rv / sp * 100 if sp > 0 else 0}

    wa = defaultdict(lambda: defaultdict(lambda: {'spend': 0, 'revenue': 0}))
    for r in master_rows:
        dt = date_key_to_dt(r['date'])
        s = dt - timedelta(days=dt.weekday())
        e = s + timedelta(days=6)
        wk = f"{s.month}/{s.day}~{e.month}/{e.day}"
        wa[r['ad_id']][wk]['spend'] += r['spend']
        wa[r['ad_id']][wk]['revenue'] += r['revenue_mp']

    wtd = [['캠페인명', '광고세트명', '소재명', '소재 ID', '전체 평균'] + wks]
    for cn, asn, adn, ad_id in tao:
        if cn == '종합':
            row = ['종합', '', '', '']
            ts = sum(w['spend'] for w in wt.values())
            tr = sum(w['revenue'] for w in wt.values())
            row.append(trend_cell(ts, tr, tr - ts, tr / ts * 100 if ts > 0 else 0))
            for wk in wks:
                w = wt.get(wk, {})
                row.append(trend_cell(w.get('spend', 0), w.get('revenue', 0), w.get('profit', 0), w.get('roas', 0)))
        else:
            row = [cn or '', asn or '', adn or '', ad_id or '']
            wad = wa.get(ad_id, {})
            ts = sum(w['spend'] for w in wad.values())
            tr = sum(w['revenue'] for w in wad.values())
            row.append(trend_cell(ts, tr, tr - ts, tr / ts * 100 if ts > 0 else 0))
            for wk in wks:
                w = wad.get(wk, {'spend': 0, 'revenue': 0})
                s_, r_ = w['spend'], w['revenue']
                row.append(trend_cell(s_, r_, r_ - s_, r_ / s_ * 100 if s_ > 0 else 0) if s_ > 0 or r_ > 0 else '')
        wtd.append(row)
    ws.clear()
    ws.update(range_name='A1', values=wtd)
    print(f"   ✅ [5/8] 추이차트(주간): {len(wtd) - 1}행")
    time.sleep(2)
    apply_rich_text(sh, ws, [row[4:] for row in wtd[1:]], 1, 4, 'trend')


def _update_weekly_summary_tabs(sh, ws1, ws2, ws3, all_dates, daily_total, master_rows):
    """[6-8/8] 주간종합 + _2 + _3"""
    print(f"   ⏳ [6-8/8] 주간종합 시작...")

    wp = OrderedDict()
    for date in all_dates:
        dt = date_key_to_dt(date)
        s = dt - timedelta(days=dt.weekday())
        e = s + timedelta(days=6)
        wp.setdefault(f"{s.month}/{s.day}~{e.month}/{e.day}", []).append(date)
    wks = list(wp.keys())

    wprod = defaultdict(lambda: defaultdict(lambda: {'spend': 0, 'revenue': 0, 'pm': 0}))
    mprod = defaultdict(lambda: defaultdict(lambda: {'spend': 0, 'revenue': 0, 'pm': 0}))
    for r in master_rows:
        dt = date_key_to_dt(r['date'])
        s = dt - timedelta(days=dt.weekday())
        e = s + timedelta(days=6)
        wk = f"{s.month}/{s.day}~{e.month}/{e.day}"
        ld = calendar.monthrange(dt.year, dt.month)[1]
        mk = f"{dt.month}/1~{dt.month}/{ld}"
        p = r['product']
        wprod[wk][p]['spend'] += r['spend']
        wprod[wk][p]['revenue'] += r['revenue_mp']
        wprod[wk][p]['pm'] += r['purchases_mp']
        mprod[mk][p]['spend'] += r['spend']
        mprod[mk][p]['revenue'] += r['revenue_mp']
        mprod[mk][p]['pm'] += r['purchases_mp']

    wtf = {}
    for wk, diw in wp.items():
        sp = sum(daily_total.get(d, {}).get('spend', 0) for d in diw)
        rv = sum(daily_total.get(d, {}).get('revenue', 0) for d in diw)
        pm = sum(daily_total.get(d, {}).get('purchases_meta', 0) for d in diw)
        pmp = sum(daily_total.get(d, {}).get('purchases_mp', 0) for d in diw)
        uc = sum(sum(r['unique_ob_clicks'] for r in master_rows if r['date'] == d) for d in diw)
        wtf[wk] = {
            'spend': sp, 'revenue': rv, 'profit': rv - sp,
            'roas': rv / sp * 100 if sp > 0 else 0,
            'purchases_meta': pm, 'purchases_mp': pmp,
            'cvr': pmp / uc * 100 if uc > 0 else 0,
        }

    mps = OrderedDict()
    for date in all_dates:
        dt = date_key_to_dt(date)
        ld = calendar.monthrange(dt.year, dt.month)[1]
        mps.setdefault(f"{dt.month}/1~{dt.month}/{ld}", []).append(date)
    mt = {}
    for mk, dim in mps.items():
        sp = sum(daily_total.get(d, {}).get('spend', 0) for d in dim)
        rv = sum(daily_total.get(d, {}).get('revenue', 0) for d in dim)
        pm = sum(daily_total.get(d, {}).get('purchases_meta', 0) for d in dim)
        pmp = sum(daily_total.get(d, {}).get('purchases_mp', 0) for d in dim)
        uc = sum(sum(r['unique_ob_clicks'] for r in master_rows if r['date'] == d) for d in dim)
        mt[mk] = {
            'spend': sp, 'revenue': rv, 'profit': rv - sp,
            'roas': rv / sp * 100 if sp > 0 else 0,
            'purchases_meta': pm, 'purchases_mp': pmp,
            'cvr': pmp / uc * 100 if uc > 0 else 0,
        }

    daily_product = {}
    for date in all_dates:
        for p in PRODUCTS:
            rows = [r for r in master_rows if r['date'] == date and r['product'] == p]
            if not rows:
                daily_product[(date, p)] = {'spend': 0, 'revenue': 0, 'profit': 0, 'roas': 0}
                continue
            sp = sum(r['spend'] for r in rows)
            rv = sum(r['revenue_mp'] for r in rows)
            daily_product[(date, p)] = {
                'spend': sp, 'revenue': rv, 'profit': rv - sp,
                'roas': rv / sp * 100 if sp > 0 else 0,
            }

    def bwb(pk, pd, prd):
        b = [
            [pk] + [''] * 10,
            ['지출 금액', '구매 (메타)', '구매 (믹스패널)', '매출', '이익', 'ROAS', 'CVR', '전체', '', '', ''],
            [fmt(pd.get('spend', 0)), fmt(pd.get('purchases_meta', 0)), fmt(pd.get('purchases_mp', 0)),
             fmt(pd.get('revenue', 0)), fmt(pd.get('profit', 0)), fmtp(pd.get('roas', 0)),
             fmtp(pd.get('cvr', 0)), '', '', '', ''],
            [''] + PRODUCTS + ['합'],
        ]
        for lbl, fn, tk in [
            ('제품별 ROAS', lambda d: fmtp(d['revenue'] / d['spend'] * 100 if d['spend'] > 0 else 0), 'roas'),
            ('제품별 순이익', lambda d: fmt(d['revenue'] - d['spend']), 'profit'),
            ('제품별 매출', lambda d: fmt(d['revenue']), 'revenue'),
            ('제품별 예산', lambda d: fmt(d['spend']), 'spend'),
        ]:
            row = [lbl]
            for p in PRODUCTS:
                row.append(fn(prd.get(p, {'spend': 0, 'revenue': 0})))
            row.append(fmtp(pd.get(tk, 0)) if tk == 'roas' else fmt(pd.get(tk, 0)))
            b.append(row)
        sr = ['제품별 예산 비중']
        tsp = pd.get('spend', 0)
        for p in PRODUCTS:
            sr.append(fmtp(prd.get(p, {'spend': 0})['spend'] / tsp * 100 if tsp > 0 else 0))
        sr.append('100.00%')
        b.append(sr)
        b.append([''] * 11)
        return b

    # ── 주간종합 ──
    wsd = []
    for mk in mps.keys():
        wsd.extend(bwb(mk, mt[mk], {p: mprod[mk].get(p, {'spend': 0, 'revenue': 0, 'pm': 0}) for p in PRODUCTS}))
        for wk in wks:
            if any(d in set(mps.get(mk, [])) for d in wp.get(wk, [])):
                wsd.extend(bwb(wk, wtf[wk], {p: wprod[wk].get(p, {'spend': 0, 'revenue': 0, 'pm': 0}) for p in PRODUCTS}))

    # ── 주간종합_2 ──
    w2 = [
        ['📊 기간별 전체 요약', '', '', '', '', '', ''],
        ['기간', '유형', '지출금액', '매출', '이익', 'ROAS', 'CVR'],
    ]
    for mk in mps.keys():
        m = mt[mk]
        w2.append([mk, '월별', fmt(m['spend']), fmt(m['revenue']), fmt(m['profit']), fmtp(m['roas']), fmtp(m['cvr'])])
        for wk in wks:
            if any(d in set(mps.get(mk, [])) for d in wp.get(wk, [])):
                w = wtf[wk]
                w2.append([wk, '주간', fmt(w['spend']), fmt(w['revenue']), fmt(w['profit']), fmtp(w['roas']), fmtp(w['cvr'])])
    w2.extend([[], [], ['📈 제품별 ROAS', '', '', '', '', '', ''], ['기간', '유형'] + PRODUCTS])
    for mk in mps.keys():
        row = [mk, '월별']
        for p in PRODUCTS:
            d = mprod[mk].get(p, {'spend': 0, 'revenue': 0})
            row.append(fmtp(d['revenue'] / d['spend'] * 100 if d['spend'] > 0 else 0))
        w2.append(row)
        for wk in wks:
            if any(d in set(mps.get(mk, [])) for d in wp.get(wk, [])):
                row = [wk, '주간']
                for p in PRODUCTS:
                    d = wprod[wk].get(p, {'spend': 0, 'revenue': 0})
                    row.append(fmtp(d['revenue'] / d['spend'] * 100 if d['spend'] > 0 else 0))
                w2.append(row)

    # ── 주간종합_3 ──
    w3 = [
        ['📊 일별 전체 요약', '', '', '', '', '', '', ''],
        ['날짜', '요일', '지출금액', '매출', '이익', 'ROAS', 'CVR', ''],
    ]
    for date in all_dates:
        dt = daily_total.get(date, {})
        w3.append([
            date, date_key_to_weekday(date),
            fmt(dt.get('spend', 0)), fmt(dt.get('revenue', 0)),
            fmt(dt.get('profit', 0)), fmtp(dt.get('roas', 0)),
            fmtp(dt.get('cvr', 0)), '',
        ])
    for lbl, vk in [('ROAS', 'roas'), ('순이익', 'profit'), ('매출', 'revenue'), ('예산', 'spend')]:
        w3.append([])
        w3.append(['날짜', '요일'] + PRODUCTS)
        for date in all_dates:
            row = [date, date_key_to_weekday(date)]
            for p in PRODUCTS:
                v = daily_product.get((date, p), {}).get(vk, 0)
                row.append(fmtp(v) if vk == 'roas' else fmt(v))
            w3.append(row)
    w3.append(['📊 일별 제품별 예산 비중', '', '', '', '', '', '', ''])
    w3.append(['날짜', '요일'] + PRODUCTS)
    for date in all_dates:
        row = [date, date_key_to_weekday(date)]
        tsp = daily_total.get(date, {}).get('spend', 0)
        for p in PRODUCTS:
            row.append(fmtp(daily_product.get((date, p), {}).get('spend', 0) / tsp * 100 if tsp > 0 else 0))
        w3.append(row)

    # ── 3개 탭 병렬 쓰기 ──
    def _write_ws1():
        ws1.clear()
        ws1.update(range_name='A1', values=wsd)
        print(f"   ✅ [6/8] 주간종합: {len(wsd)}행")

    def _write_ws2():
        ws2.clear()
        ws2.update(range_name='A1', values=w2)
        print(f"   ✅ [7/8] 주간종합_2: {len(w2)}행")

    def _write_ws3():
        ws3.clear()
        ws3.update(range_name='A1', values=w3)
        print(f"   ✅ [8/8] 주간종합_3: {len(w3)}행")

    with ThreadPoolExecutor(max_workers=3, thread_name_prefix='weekly') as ex:
        futs = [ex.submit(_write_ws1), ex.submit(_write_ws2), ex.submit(_write_ws3)]
        for f in as_completed(futs):
            f.result()


# =============================================================================
# Part 5: Google Sheets 업데이트 — 메인 오케스트레이터
# =============================================================================
def update_sheets(cfg, gc, master_rows, all_dates, all_ads, daily_total, daily_product, ad_daily):
    print(f"\n📝 [5] Google Sheets 업데이트 (병렬 모드)...")
    sh = gc.open_by_url(cfg['SPREADSHEET_URL'])
    dc = all_dates

    # ★ 워크시트 캐싱
    print(f"   워크시트 목록 캐싱...")
    all_ws = {ws.title: ws for ws in sh.worksheets()}
    print(f"   ✅ {len(all_ws)}개 탭 캐싱 완료: {list(all_ws.keys())}")

    # ★ 필요한 탭 생성 (없으면)
    required_tabs = {
        '마스터탭': (5000, 25),
        '추이차트': (2000, 200),
        '증감액': (2000, 200),
        '추이차트(주간)': (2000, 100),
        '예산': (100, 200),
        '주간종합': (2000, 20),
        '주간종합_2': (2000, 20),
        '주간종합_3': (3000, 20),
    }
    for tab_name, (rows, cols) in required_tabs.items():
        if tab_name not in all_ws:
            try:
                ws_new = with_retry(sh.add_worksheet, title=tab_name, rows=rows, cols=cols)
                all_ws[tab_name] = ws_new
                print(f"   + '{tab_name}' 탭 생성")
                time.sleep(1)
            except Exception as e:
                # 이미 존재하는 경우
                if "already exists" in str(e):
                    all_ws[tab_name] = sh.worksheet(tab_name)
                else:
                    print(f"   ❌ '{tab_name}' 생성 실패: {e}")
                    raise

    # ══════════════════════════════════════════════════════════════════════
    # ★ 소재 순서: 최신 날짜 매출 내림차순 다중 키 정렬
    # ══════════════════════════════════════════════════════════════════════
    tao = [('종합', None, None, None)]  # (campaign, adset, ad_name, ad_id)
    ad_sort_list = []
    for ad_id, info in all_ads.items():
        sort_key = tuple(
            -(ad_daily.get(ad_id, {}).get(d, {}).get('revenue_mp', 0)
              if isinstance(ad_daily.get(ad_id, {}).get(d), dict) else 0)
            for d in dc
        )
        ad_sort_list.append((sort_key, info['campaign_name'], info['adset_name'], info['ad_name'], ad_id))
    ad_sort_list.sort()
    for _, cn, asn, adn, ad_id in ad_sort_list:
        tao.append((cn, asn, adn, ad_id))
    print(f"   ★ 소재 정렬 완료: {len(tao) - 1}개 (최신 날짜 매출 내림차순)")

    # ══════════════════════════════════════════════════════════════════════
    # 병렬 업데이트
    # ══════════════════════════════════════════════════════════════════════
    with ThreadPoolExecutor(max_workers=6, thread_name_prefix='sheets') as executor:
        futures = {}

        futures['마스터탭'] = executor.submit(
            _update_master_tab, sh, all_ws['마스터탭'], master_rows
        )
        futures['예산'] = executor.submit(
            _update_budget_tab, all_ws['예산'], dc, daily_total, daily_product
        )
        futures['추이차트'] = executor.submit(
            _update_trend_tab, sh, all_ws['추이차트'], tao, dc, daily_total, ad_daily
        )
        futures['증감액'] = executor.submit(
            _update_change_tab, sh, all_ws['증감액'], tao, dc, daily_total, ad_daily
        )
        futures['추이차트(주간)'] = executor.submit(
            _update_weekly_trend_tab, sh, all_ws['추이차트(주간)'], tao, all_dates, daily_total, master_rows
        )
        futures['주간종합'] = executor.submit(
            _update_weekly_summary_tabs, sh,
            all_ws['주간종합'], all_ws['주간종합_2'], all_ws['주간종합_3'],
            all_dates, daily_total, master_rows,
        )

        for name, fut in futures.items():
            try:
                fut.result()
            except Exception as e:
                print(f"   ❌ {name} 실패: {e}")
                raise

    # ══════════════════════════════════════════════════════════════════════
    # 탭 순서 정리
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n   📝 탭 순서 정리...")
    try:
        all_worksheets = sh.worksheets()
        analysis_set = set(ANALYSIS_TAB_NAMES)
        analysis_tabs, other_tabs = [], []
        for ws in all_worksheets:
            if ws.title in analysis_set:
                analysis_tabs.append(ws)
            else:
                other_tabs.append(ws)
        analysis_order = {name: i for i, name in enumerate(ANALYSIS_TAB_NAMES)}
        analysis_tabs.sort(key=lambda ws: analysis_order.get(ws.title, 999))
        final_order = other_tabs + analysis_tabs
        reorder_requests = [
            {"updateSheetProperties": {
                "properties": {"sheetId": ws.id, "index": idx},
                "fields": "index"
            }} for idx, ws in enumerate(final_order)
        ]
        if reorder_requests:
            with_retry(sh.batch_update, body={"requests": reorder_requests})
            print(f"   ✅ 탭 순서 정리 완료")
    except Exception as e:
        print(f"   ⚠️ 탭 순서 오류: {e}")

    print(f"\n   ✅ 전체 시트 병렬 업데이트 완료")
    return len(master_rows), len(all_ads)

# =============================================================================
# Main
# =============================================================================
def main():
    cfg = get_config()
    print(f"\n✅ 환경변수 로드 완료")
    print(f"   Meta 계정: {cfg['META_AD_ACCOUNT_ID']}")
    print(f"   기간: {cfg['FROM_DATE']} ~ {cfg['TO_DATE']}")
    print(f"   Mixpanel 이벤트: {cfg['MIXPANEL_EVENT_NAMES']}")
    print(f"   스프레드시트: {cfg['SPREADSHEET_URL']}")

    gc = get_gc(cfg)
    print(f"   Google Sheets 인증 완료")

    # ══════════════════════════════════════════════════════════════════════
    # Phase 1: Meta + Mixpanel 병렬 fetch
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print(f"🚀 Phase 1: API 데이터 병렬 수집")
    print(f"{'=' * 80}")

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix='api') as executor:
        fut_meta = executor.submit(fetch_meta_raw, cfg)
        fut_mp   = executor.submit(fetch_mixpanel_raw, cfg)
        meta_raw = fut_meta.result()
        mp_lines = fut_mp.result()

    if not meta_raw:
        print("\n⚠️  Meta 데이터가 비어있어 시트 업데이트를 건너뜁니다.")
        return

    # ══════════════════════════════════════════════════════════════════════
    # Phase 2: 파싱 + 매칭 + 파생
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print(f"🔧 Phase 2: 데이터 파싱 & 조합")
    print(f"{'=' * 80}")

    meta_data, all_ads = parse_meta(meta_raw)
    mp_matched, total_events, matched_events, _ = parse_and_match_mixpanel(mp_lines, all_ads)
    master_rows, all_dates = build_master(meta_data, mp_matched)
    daily_total, daily_product, ad_daily = calc_derived(master_rows, all_dates)

    # ══════════════════════════════════════════════════════════════════════
    # Phase 3: Google Sheets 병렬 업데이트
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print(f"📝 Phase 3: Google Sheets 병렬 업데이트")
    print(f"{'=' * 80}")

    n_rows, n_ads = update_sheets(cfg, gc, master_rows, all_dates, all_ads, daily_total, daily_product, ad_daily)

    print(f"\n{'=' * 80}")
    print(f"✅ 전체 업데이트 완료!")
    print(f"   Meta: {cfg['META_AD_ACCOUNT_ID']} | 소재: {n_ads}개")
    print(f"   Mixpanel: {total_events}건 | 매칭 {matched_events}건 ({matched_events / max(total_events, 1) * 100:.1f}%)")
    print(f"   Mixpanel 이벤트: {cfg['MIXPANEL_EVENT_NAMES']}")
    print(f"   기간: {cfg['FROM_DATE']} ~ {cfg['TO_DATE']} ({len(all_dates)}일)")
    print(f"   스프레드시트: {cfg['SPREADSHEET_URL']}")
    print(f"{'=' * 80}")


if __name__ == '__main__':
    main()
