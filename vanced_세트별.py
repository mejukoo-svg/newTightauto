# -*- coding: utf-8 -*-
"""
[Vanced] 퍼포먼스 데이터 자동 업데이트 v4 — Parallel Edition + 상품별 추이차트
Meta Ads API (지출/노출/클릭) + Mixpanel (매출/결과수 - utm_term 매칭)
→ Google Sheets 전체 탭 자동 갱신

★ v4 변경사항
  - 추이차트_상품별 탭 추가 (상품별 그룹핑, 전날매출순 정렬, 세트는 전날지출순)
  - 상단: 상품별 종합 성과 요약 → 하단: 상품별 상세 세트 목록

★ v3 변경사항
  - 결제완료 이벤트: '결제완료' + 'payment_complete' 둘 다 OR 조회
  - amount 필드: 'amount' → '결제금액' → 'value' 순으로 OR 추출
  - 모든 I/O를 병렬(ThreadPoolExecutor) 처리: API fetch 병렬 + 시트 업데이트 병렬
  - 추이차트/증감액/추이차트(주간) 정렬: 최신 날짜 매출 내림차순 다중 키 정렬
"""

import os
import json
import time
import calendar
import gspread
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials

# =============================================================================
# 환경변수에서 설정 로드
# =============================================================================
def load_env():
    """로컬 실행 시 .env 파일에서 환경변수 로드 (Colab에서는 스킵)"""
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    except NameError:
        print("ℹ️  Colab 환경 감지 → .env 파일 로드 스킵 (os.environ 직접 설정 필요)")
        return

    if os.path.exists(env_path):
        print(f"ℹ️  .env 파일 로드: {env_path}")
        with open(env_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, val = line.split('=', 1)
                    os.environ.setdefault(key.strip(), val.strip())
    else:
        print(f"⚠️  .env 파일 없음: {env_path}")

load_env()

# =============================================================================
# 환경변수 읽기
# =============================================================================
def get_config():
    cfg = {}
    cfg['META_ACCESS_TOKEN']   = os.environ.get('META_TOKEN_VANCED', '')
    cfg['META_AD_ACCOUNT_ID']  = os.environ.get('META_AD_ACCOUNT_ID', 'act_25183853061243175')
    cfg['META_API_VERSION']    = os.environ.get('META_API_VERSION', 'v21.0')
    cfg['META_BASE_URL']       = f"https://graph.facebook.com/{cfg['META_API_VERSION']}"

    cfg['MIXPANEL_PROJECT_ID'] = os.environ.get('MIXPANEL_PROJECT_ID', '3390233')
    cfg['MIXPANEL_USERNAME']   = os.environ.get('MIXPANEL_USERNAME', '')
    cfg['MIXPANEL_SECRET']     = os.environ.get('MIXPANEL_SECRET', '')
    # ★ 이벤트명 2개 OR
    cfg['MIXPANEL_EVENT_NAMES'] = ['결제완료', 'payment_complete']

    cfg['SPREADSHEET_URL']     = os.environ.get('SPREADSHEET_URL', '')
    cfg['GCP_SA_KEY_JSON']     = os.environ.get('GCP_SERVICE_ACCOUNT_KEY', '')

    cfg['FROM_DATE'] = os.environ.get('FROM_DATE', '2025-11-09')
    cfg['TO_DATE']   = os.environ.get('TO_DATE', datetime.now().strftime('%Y-%m-%d'))

    missing = []
    if not cfg['META_ACCESS_TOKEN']:   missing.append('META_TOKEN_VANCED')
    if not cfg['GCP_SA_KEY_JSON']:     missing.append('GCP_SERVICE_ACCOUNT_KEY')
    if not cfg['SPREADSHEET_URL']:     missing.append('SPREADSHEET_URL')
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
# 상수
# =============================================================================
PRODUCTS = ['29금궁합', 'Solo', 'Money', '29금', '1%', 'Kids', 'Reunion', 'career', 'Year']

ADSET_PRODUCT_PREFIXES = [
    ('29금궁합', '29금궁합'), ('29금', '29금'), ('Solo', 'Solo'), ('Kids', 'Kids'),
    ('Money', 'Money'), ('Reunion', 'Reunion'), ('1%', '1%'),
    ('career', 'career'), ('Year', 'Year'), ('Star', 'Star'),
]

PURCHASE_TYPES = ['offsite_conversion.fb_pixel_purchase', 'purchase', 'omni_purchase']

# ★ v4: 상품별 추이차트 헤더 색상
PRODUCT_CHART_BG = [
    {"red": 0.22, "green": 0.42, "blue": 0.65},
    {"red": 0.33, "green": 0.57, "blue": 0.33},
    {"red": 0.53, "green": 0.30, "blue": 0.58},
    {"red": 0.68, "green": 0.42, "blue": 0.18},
    {"red": 0.58, "green": 0.22, "blue": 0.22},
    {"red": 0.20, "green": 0.52, "blue": 0.52},
    {"red": 0.48, "green": 0.48, "blue": 0.25},
    {"red": 0.38, "green": 0.25, "blue": 0.48},
    {"red": 0.30, "green": 0.55, "blue": 0.45},
    {"red": 0.60, "green": 0.35, "blue": 0.40},
    {"red": 0.35, "green": 0.35, "blue": 0.55},
]

# =============================================================================
# 유틸
# =============================================================================
def adset_to_product(name):
    for prefix, product in ADSET_PRODUCT_PREFIXES:
        if name.startswith(prefix):
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
WHITE        = {"red": 1, "green": 1, "blue": 1}
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
    CHUNK = 50
    for i in range(0, len(api_rows), CHUNK):
        chunk = api_rows[i:i+CHUNK]
        sh.batch_update({"requests": [{"updateCells": {
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
        time.sleep(1)
    print(f"      → 리치텍스트 포맷 적용 ({len(api_rows)}행)")

# =============================================================================
# Part 1: Meta Ads API — raw fetch (병렬 준비용)
# =============================================================================
def fetch_meta_raw(cfg):
    """Meta API에서 raw 데이터만 가져옴 (파싱은 별도)"""
    print(f"\n📡 [Meta] adset 일간 데이터 fetch ({cfg['FROM_DATE']} ~ {cfg['TO_DATE']})...")
    meta_raw, next_url, page = [], None, 0
    while True:
        page += 1
        if next_url:
            resp = requests.get(next_url, timeout=120)
        else:
            resp = requests.get(
                f"{cfg['META_BASE_URL']}/{cfg['META_AD_ACCOUNT_ID']}/insights",
                params={
                    'access_token': cfg['META_ACCESS_TOKEN'],
                    'fields': 'campaign_name,adset_name,adset_id,spend,cpm,reach,impressions,frequency,'
                              'actions,action_values,cost_per_action_type,purchase_roas,'
                              'unique_outbound_clicks,unique_outbound_clicks_ctr,cost_per_unique_outbound_click',
                    'level': 'adset',
                    'time_increment': 1,
                    'time_range': json.dumps({'since': cfg['FROM_DATE'], 'until': cfg['TO_DATE']}),
                    'limit': 500,
                },
                timeout=120,
            )
        if resp.status_code != 200:
            print(f"   ❌ Meta API 오류 {resp.status_code}: {resp.text[:300]}")
            break
        data = resp.json()
        rows = data.get('data', [])
        meta_raw.extend(rows)
        print(f"   [Meta] 페이지 {page}: +{len(rows)}건 (누적 {len(meta_raw)}건)")
        next_url = data.get('paging', {}).get('next')
        if not next_url or not rows:
            break
        time.sleep(0.5)
    print(f"✅ Meta: {len(meta_raw)}건")
    return meta_raw

def parse_meta(meta_raw):
    """Meta raw → meta_data, all_adsets 파싱"""
    meta_data, all_adsets = {}, {}
    for row in meta_raw:
        dk = meta_date_to_key(row['date_start'])
        aid = row['adset_id']
        an = row.get('adset_name', '')
        cn = row.get('campaign_name', '')
        sp = float(row.get('spend', 0))
        imp = float(row.get('impressions', 0))
        pr_list = row.get('purchase_roas', [])
        meta_data[(dk, aid)] = {
            'date': dk,
            'campaign_name': cn,
            'adset_name': an,
            'adset_id': aid,
            'product': adset_to_product(an),
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
        if aid not in all_adsets:
            all_adsets[aid] = {
                'campaign_name': cn,
                'adset_name': an,
                'product': adset_to_product(an),
            }
    print(f"   광고세트: {len(all_adsets)}개")
    return meta_data, all_adsets

# =============================================================================
# Part 2: Mixpanel — raw fetch (병렬 준비용)
# =============================================================================
def fetch_mixpanel_raw(cfg):
    """Mixpanel에서 raw 이벤트만 가져옴 (매칭은 별도)"""
    print(f"\n📡 [Mixpanel] 결제 이벤트 조회 (이벤트: {cfg['MIXPANEL_EVENT_NAMES']})...")
    resp = requests.get(
        "https://data.mixpanel.com/api/2.0/export",
        params={
            'from_date': cfg['FROM_DATE'],
            'to_date': cfg['TO_DATE'],
            # ★ 이벤트명 2개 OR 조회
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

def parse_and_match_mixpanel(lines, all_adsets):
    """Mixpanel raw lines → 파싱 + 중복제거 + adset 매칭"""
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
            utm_term = str(props.get('utm_term', '') or '').strip()

            # ★ amount 필드: 'amount' → '결제금액' → 'value' 순서로 OR 추출
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
                'utm_term': utm_term,
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
        utm = e['utm_term']
        if not utm:
            unmatched_events += 1
            continue
        if utm in all_adsets:
            mp_matched[(e['dk'], utm)]['count'] += 1
            mp_matched[(e['dk'], utm)]['revenue'] += e['amount']
            matched_events += 1
        else:
            unmatched_events += 1
    print(f"   ★ 매칭: {matched_events}건 ({matched_events / max(total_events, 1) * 100:.1f}%) | 미매칭: {unmatched_events}건")
    return mp_matched, total_events, matched_events, unmatched_events

# =============================================================================
# Part 3: 마스터 데이터 조합
# =============================================================================
def build_master(meta_data, mp_matched):
    print(f"\n🔧 [3] 마스터탭 데이터 조합...")
    master_rows = []
    for key, meta in sorted(meta_data.items(), key=lambda x: (date_key_to_dt(x[0][0]), x[0][1]), reverse=True):
        dk, aid = key
        mp = mp_matched.get((dk, aid), {'count': 0, 'revenue': 0.0})
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
        print(f"   {date}: 지출=₩{ts:,.0f} | 결과(MP)={tp}건 | MP매출=₩{tr:,.0f} | ROAS={tr / ts * 100:.0f}%" if ts > 0 else f"   {date}: 지출=₩0")
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

    adset_daily = defaultdict(dict)
    for r in master_rows:
        adset_daily[r['adset_id']][r['date']] = r
    return daily_total, daily_product, adset_daily

# =============================================================================
# Part 5: Google Sheets 업데이트 — 개별 탭 함수 (병렬 실행용)
# =============================================================================

def _update_master_tab(sh, ws, master_rows):
    """[1/8] 마스터탭"""
    print(f"   ⏳ [1/8] 마스터탭 시작...")
    ms = []
    for r in master_rows:
        dt = date_key_to_dt(r['date'])
        ms.append([
            dt.strftime('%Y-%m-%d'), r['campaign_name'], r['adset_name'], r['adset_id'],
            round(r['spend']), round(r['cost_per_purchase'], 2), round(r['purchase_roas'], 2),
            round(r['cpm'], 2), round(r['reach']), round(r['impressions']),
            round(r['unique_ob_clicks']), round(r['unique_ob_ctr'], 2), round(r['cost_per_unique_ob'], 2),
            round(r['frequency'], 2), round(r['purchases_meta']), round(r['purchases_mp']),
            round(r['revenue_meta']), round(r['revenue_mp']), round(r['profit']),
            round(r['roas_calc'], 4), round(r['cvr'], 2),
        ])
    ws.batch_clear([f'A2:U{ws.row_count}'])
    if ms:
        ws.update(range_name='A2', values=ms)
    print(f"   ✅ [1/8] 마스터탭: {len(ms)}행")
    return len(ms)


def _update_budget_tab(ws, dc, daily_total, daily_product):
    """[4/8] 예산"""
    print(f"   ⏳ [4/8] 예산 시작...")
    bd = [[''] + dc]
    bd.append(['전체 쓴돈'] + [round(daily_total.get(d, {}).get('spend', 0)) for d in dc])
    bd.append(['전체 번돈'] + [round(daily_total.get(d, {}).get('revenue', 0)) for d in dc])
    bd.append(['전체 순이익'] + [round(daily_total.get(d, {}).get('profit', 0)) for d in dc])
    bd.append(['총예산'] + [round(daily_total.get(d, {}).get('spend', 0)) for d in dc])
    for p in PRODUCTS:
        bd.append([f'{p} 예산'] + [round(daily_product.get((d, p), {}).get('spend', 0)) for d in dc])
    ws.clear()
    ws.update(range_name='A1', values=bd)
    print(f"   ✅ [4/8] 예산: {len(bd)}행")


def _update_trend_tab(sh, ws, tao, dc, daily_total, adset_daily):
    """[2/8] 추이차트"""
    print(f"   ⏳ [2/8] 추이차트 시작...")
    td = [['캠페인 이름', '광고 세트 이름', '광고 세트 ID', '7일 평균'] + dc]
    for cn, an, aid in tao:
        if cn == '종합':
            row = ['종합', '', '']
            r7 = dc[:7]
            ts = sum(daily_total.get(d, {}).get('spend', 0) for d in r7)
            tr = sum(daily_total.get(d, {}).get('revenue', 0) for d in r7)
            row.append(trend_cell(ts, tr, tr - ts, tr / ts * 100 if ts > 0 else 0))
            for date in dc:
                dt = daily_total.get(date, {})
                row.append(trend_cell(dt.get('spend', 0), dt.get('revenue', 0), dt.get('profit', 0), dt.get('roas', 0)))
        else:
            row = [cn or '', an or '', aid or '']
            r7 = dc[:7]
            ts = sum(adset_daily.get(aid, {}).get(d, {}).get('spend', 0) for d in r7)
            tr = sum(adset_daily.get(aid, {}).get(d, {}).get('revenue_mp', 0) for d in r7)
            row.append(trend_cell(ts, tr, tr - ts, tr / ts * 100 if ts > 0 else 0))
            for date in dc:
                ad = adset_daily.get(aid, {}).get(date)
                row.append(trend_cell(ad['spend'], ad['revenue_mp'], ad['profit'], ad['roas_calc'] * 100) if ad else '')
        td.append(row)
    ws.clear()
    ws.update(range_name='A1', values=td)
    print(f"   ✅ [2/8] 추이차트: {len(td) - 1}행 × {len(dc) + 4}열")
    time.sleep(2)
    apply_rich_text(sh, ws, [row[3:] for row in td[1:]], 1, 3, 'trend')


def _update_change_tab(sh, ws, tao, dc, daily_total, adset_daily):
    """[3/8] 증감액"""
    print(f"   ⏳ [3/8] 증감액 시작...")
    cd = [['캠페인 이름', '광고 세트 이름', '광고 세트 ID', '7일 평균'] + dc]
    for cn, an, aid in tao:
        if cn == '종합':
            row = ['종합', '', '']
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
            row = [cn or '', an or '', aid or '']
            r7 = dc[:7]
            ts = sum(adset_daily.get(aid, {}).get(d, {}).get('spend', 0) for d in r7)
            tr = sum(adset_daily.get(aid, {}).get(d, {}).get('revenue_mp', 0) for d in r7)
            ti = sum(adset_daily.get(aid, {}).get(d, {}).get('impressions', 0) for d in r7)
            row.append(change_cell(tr / ts * 100 if ts > 0 else 0, 0, ts, ts / ti * 1000 if ti > 0 else 0))
            for i, date in enumerate(dc):
                ad = adset_daily.get(aid, {}).get(date)
                if ad:
                    rt = ad['roas_calc'] * 100
                    pa = adset_daily.get(aid, {}).get(dc[i + 1]) if i + 1 < len(dc) else None
                    pr = pa['roas_calc'] * 100 if pa and pa['roas_calc'] > 0 else 0
                    row.append(change_cell(rt, ((rt - pr) / pr * 100) if pr > 0 else 0, ad['spend'], ad['cpm']))
                else:
                    row.append('')
        cd.append(row)
    ws.clear()
    ws.update(range_name='A1', values=cd)
    print(f"   ✅ [3/8] 증감액: {len(cd) - 1}행")
    time.sleep(2)
    apply_rich_text(sh, ws, [row[3:] for row in cd[1:]], 1, 3, 'change')


# =============================================================================
# ★ v4: 추이차트_상품별 탭 (국내 세트별 로직 이식)
# =============================================================================
def _update_product_trend_tab(sh, ws, dc, all_adsets, adset_daily, daily_total):
    """[8/8] 추이차트_상품별 — 상품별 그룹핑, 전날매출순 정렬"""
    print(f"   ⏳ [8/8] 추이차트_상품별 시작...")

    # 전날 = dc[1] (dc[0]이 오늘)
    yesterday = dc[1] if len(dc) >= 2 else (dc[0] if dc else None)
    r7 = dc[:7]  # 최근 7일

    # ── 광고세트를 상품별로 그룹핑 ──
    product_groups = defaultdict(list)  # product → [{'aid', 'cn', 'an', 'adset_daily'}, ...]
    for aid, info in all_adsets.items():
        product = info['product']
        product_groups[product].append({
            'aid': aid,
            'cn': info['campaign_name'],
            'an': info['adset_name'],
        })

    # ── 상품별 전날 매출 합산 → 정렬 ──
    product_yesterday_revenue = {}
    for product, items in product_groups.items():
        rev = 0
        if yesterday:
            for item in items:
                ad = adset_daily.get(item['aid'], {}).get(yesterday)
                if ad:
                    rev += ad.get('revenue_mp', 0)
        product_yesterday_revenue[product] = rev

    sorted_products = sorted(product_groups.keys(),
                             key=lambda p: product_yesterday_revenue.get(p, 0),
                             reverse=True)

    # ── 상품 내 세트: 전날 지출순 정렬 ──
    for product in sorted_products:
        product_groups[product].sort(
            key=lambda item: adset_daily.get(item['aid'], {}).get(yesterday, {}).get('spend', 0)
                             if yesterday else 0,
            reverse=True
        )

    # ── 헤더 생성 ──
    hdr = ['캠페인 이름', '광고 세트 이름', '광고 세트 ID', '7일 평균'] + dc

    # ── 헬퍼: 특정 세트 집합의 날짜별 합산 trend_cell 생성 ──
    def _make_aggregate_row(label_parts, items_list):
        """items_list = [{'aid': ...}, ...] 세트들의 합산 행 생성"""
        row = list(label_parts)  # [col0, col1, col2]

        # 7일 평균
        ts7, tr7 = 0, 0
        for d in r7:
            for item in items_list:
                ad = adset_daily.get(item['aid'], {}).get(d)
                if ad:
                    ts7 += ad.get('spend', 0)
                    tr7 += ad.get('revenue_mp', 0)
        row.append(trend_cell(ts7, tr7, tr7 - ts7, tr7 / ts7 * 100 if ts7 > 0 else 0))

        # 날짜별
        for d in dc:
            ds, dr = 0, 0
            for item in items_list:
                ad = adset_daily.get(item['aid'], {}).get(d)
                if ad:
                    ds += ad.get('spend', 0)
                    dr += ad.get('revenue_mp', 0)
            if ds > 0 or dr > 0:
                row.append(trend_cell(ds, dr, dr - ds, dr / ds * 100 if ds > 0 else 0))
            else:
                row.append('')
        return row

    def _make_adset_row(item):
        """개별 광고세트 행 생성"""
        aid = item['aid']
        row = [item['cn'] or '', item['an'] or '', aid or '']

        # 7일 평균
        ts7, tr7 = 0, 0
        for d in r7:
            ad = adset_daily.get(aid, {}).get(d)
            if ad:
                ts7 += ad.get('spend', 0)
                tr7 += ad.get('revenue_mp', 0)
        row.append(trend_cell(ts7, tr7, tr7 - ts7, tr7 / ts7 * 100 if ts7 > 0 else 0))

        # 날짜별
        for d in dc:
            ad = adset_daily.get(aid, {}).get(d)
            if ad:
                row.append(trend_cell(ad['spend'], ad['revenue_mp'], ad['profit'], ad['roas_calc'] * 100))
            else:
                row.append('')
        return row

    # ══════════════════════════════════════════════════════════════════════
    # 데이터 행 구성
    # ══════════════════════════════════════════════════════════════════════
    rows = []
    product_header_indices = []   # 상품 헤더의 rows 내 인덱스 (포맷용)
    summary_header_idx = len(rows)

    # ── 상단: 상품별 종합 성과 요약 ──
    rows.append(["📊 상품별 종합 성과"] + [""] * (len(hdr) - 1))
    summary_start = len(rows)
    for product in sorted_products:
        items = product_groups[product]
        row = _make_aggregate_row(
            [product, f"({len(items)}개 세트)", ""],
            items
        )
        rows.append(row)
    summary_end = len(rows)
    rows.append([""] * len(hdr))  # 구분 빈 행

    # ── 하단: 상품별 상세 세트 목록 ──
    for p_idx, product in enumerate(sorted_products):
        items = product_groups[product]
        p_yd_rev = product_yesterday_revenue.get(product, 0)

        # 상품 헤더 행
        product_header_indices.append(len(rows))
        rows.append(
            [f"📦 {product}  |  전날 매출 ₩{fmt(p_yd_rev)}  |  {len(items)}개 세트"]
            + [""] * (len(hdr) - 1)
        )

        # 상품 종합 행
        rows.append(_make_aggregate_row([f"{product} 종합", "", ""], items))

        # 개별 세트 행
        for item in items:
            rows.append(_make_adset_row(item))

        # 상품 구분 빈 행
        rows.append([""] * len(hdr))

    # ══════════════════════════════════════════════════════════════════════
    # 시트 쓰기
    # ══════════════════════════════════════════════════════════════════════
    ws.clear()
    all_data = [hdr] + rows
    ws.update(range_name='A1', values=all_data)
    print(f"   ✅ [8/8] 추이차트_상품별: {len(sorted_products)}개 상품, {len(rows)}행")
    time.sleep(2)

    # ── 리치텍스트 적용 (데이터 셀: col 3 이후) ──
    apply_rich_text(sh, ws, [row[3:] for row in rows], 1, 3, 'trend')
    time.sleep(1)

    # ── 상품 헤더 포맷 (배경색 + 머지 + 볼드) ──
    sheet_id = ws.id
    fmt_reqs = []

    # 종합 성과 헤더 (navy 배경)
    fmt_reqs.append({
        "repeatCell": {
            "range": {"sheetId": sheet_id,
                      "startRowIndex": summary_header_idx + 1,
                      "endRowIndex": summary_header_idx + 2,
                      "startColumnIndex": 0,
                      "endColumnIndex": min(len(hdr), 26)},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.15, "green": 0.2, "blue": 0.35},
                "textFormat": {"bold": True, "foregroundColor": WHITE},
                "horizontalAlignment": "LEFT",
            }},
            "fields": "userEnteredFormat",
        }
    })
    fmt_reqs.append({
        "mergeCells": {
            "range": {"sheetId": sheet_id,
                      "startRowIndex": summary_header_idx + 1,
                      "endRowIndex": summary_header_idx + 2,
                      "startColumnIndex": 0,
                      "endColumnIndex": min(len(hdr), 26)},
            "mergeType": "MERGE_ALL",
        }
    })

    # 종합 성과 행 볼드 (A열)
    fmt_reqs.append({
        "repeatCell": {
            "range": {"sheetId": sheet_id,
                      "startRowIndex": summary_start + 1,
                      "endRowIndex": summary_end + 1,
                      "startColumnIndex": 0,
                      "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {
                "textFormat": {"bold": True},
                "horizontalAlignment": "LEFT",
            }},
            "fields": "userEnteredFormat",
        }
    })

    # 상품별 헤더 (색상 배경 + 머지)
    for p_idx, row_idx in enumerate(product_header_indices):
        sheet_row = row_idx + 1  # +1 for header row offset
        color = PRODUCT_CHART_BG[p_idx % len(PRODUCT_CHART_BG)]

        fmt_reqs.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id,
                          "startRowIndex": sheet_row,
                          "endRowIndex": sheet_row + 1,
                          "startColumnIndex": 0,
                          "endColumnIndex": min(len(hdr), 26)},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": color,
                    "textFormat": {"bold": True, "foregroundColor": WHITE},
                    "horizontalAlignment": "LEFT",
                }},
                "fields": "userEnteredFormat",
            }
        })
        fmt_reqs.append({
            "mergeCells": {
                "range": {"sheetId": sheet_id,
                          "startRowIndex": sheet_row,
                          "endRowIndex": sheet_row + 1,
                          "startColumnIndex": 0,
                          "endColumnIndex": min(len(hdr), 26)},
                "mergeType": "MERGE_ALL",
            }
        })

    # 헤더 행 포맷 (고정 + 볼드)
    fmt_reqs.append({
        "repeatCell": {
            "range": {"sheetId": sheet_id,
                      "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": 0, "endColumnIndex": min(len(hdr), 26)},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                "textFormat": {"bold": True},
            }},
            "fields": "userEnteredFormat",
        }
    })
    fmt_reqs.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id,
                           "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount",
        }
    })

    # 열 너비 설정 (D열부터 = 날짜 컬럼)
    for cn in range(3, min(len(hdr), 26)):
        fmt_reqs.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                          "startIndex": cn, "endIndex": cn + 1},
                "properties": {"pixelSize": 95},
                "fields": "pixelSize",
            }
        })

    if fmt_reqs:
        # 배치로 나눠 적용 (rate limit 방지)
        BATCH = 50
        for i in range(0, len(fmt_reqs), BATCH):
            sh.batch_update({"requests": fmt_reqs[i:i + BATCH]})
            time.sleep(1)
        print(f"      → 상품별 포맷 적용 ({len(fmt_reqs)}개 요청)")


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
        wa[r['adset_id']][wk]['spend'] += r['spend']
        wa[r['adset_id']][wk]['revenue'] += r['revenue_mp']
    wtd = [['캠페인 이름', '광고 세트 이름', '광고 세트 ID', '전체 평균'] + wks]
    for cn, an, aid in tao:
        if cn == '종합':
            row = ['종합', '', '']
            ts = sum(w['spend'] for w in wt.values())
            tr = sum(w['revenue'] for w in wt.values())
            row.append(trend_cell(ts, tr, tr - ts, tr / ts * 100 if ts > 0 else 0))
            for wk in wks:
                w = wt.get(wk, {})
                row.append(trend_cell(w.get('spend', 0), w.get('revenue', 0), w.get('profit', 0), w.get('roas', 0)))
        else:
            row = [cn or '', an or '', aid or '']
            wad = wa.get(aid, {})
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
    apply_rich_text(sh, ws, [row[3:] for row in wtd[1:]], 1, 3, 'trend')


def _update_weekly_summary_tabs(sh, ws1, ws2, ws3, all_dates, daily_total, master_rows):
    """[6-7/8] 주간종합 + _2 + _3 (내부에서 병렬로 _2, _3 처리)"""
    print(f"   ⏳ [6-7/8] 주간종합 + _2 + _3 시작...")

    # ── 공통 파생 데이터 계산 ──
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

    # ── 주간종합 데이터 준비 ──
    wsd = []
    for mk in mps.keys():
        wsd.extend(bwb(mk, mt[mk], {p: mprod[mk].get(p, {'spend': 0, 'revenue': 0, 'pm': 0}) for p in PRODUCTS}))
        for wk in wks:
            if any(d in set(mps.get(mk, [])) for d in wp.get(wk, [])):
                wsd.extend(bwb(wk, wtf[wk], {p: wprod[wk].get(p, {'spend': 0, 'revenue': 0, 'pm': 0}) for p in PRODUCTS}))

    # ── 주간종합_2 데이터 준비 ──
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

    # ── 주간종합_3 데이터 준비 ──
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

    # ── 3개 탭 병렬 쓰기 (병렬의 병렬) ──
    def _write_ws1():
        ws1.clear()
        ws1.update(range_name='A1', values=wsd)
        print(f"   ✅ [6/8] 주간종합: {len(wsd)}행")

    def _write_ws2():
        ws2.clear()
        ws2.update(range_name='A1', values=w2)
        print(f"   ✅ [6/8] 주간종합_2: {len(w2)}행")

    def _write_ws3():
        ws3.clear()
        ws3.update(range_name='A1', values=w3)
        print(f"   ✅ [7/8] 주간종합_3: {len(w3)}행")

    with ThreadPoolExecutor(max_workers=3, thread_name_prefix='weekly') as ex:
        futs = [ex.submit(_write_ws1), ex.submit(_write_ws2), ex.submit(_write_ws3)]
        for f in as_completed(futs):
            f.result()  # 예외 전파


# =============================================================================
# Part 5: Google Sheets 업데이트 — 메인 오케스트레이터
# =============================================================================
def update_sheets(cfg, gc, master_rows, all_dates, all_adsets, daily_total, daily_product, adset_daily):
    print(f"\n📝 [5] Google Sheets 업데이트 (병렬 모드)...")
    sh = gc.open_by_url(cfg['SPREADSHEET_URL'])
    dc = all_dates

    # ★ 워크시트 캐싱
    print(f"   워크시트 목록 캐싱...")
    all_ws = {ws.title: ws for ws in sh.worksheets()}
    print(f"   ✅ {len(all_ws)}개 탭 캐싱 완료: {list(all_ws.keys())}")

    # ★ v4: 추이차트_상품별 탭이 없으면 생성
    if '추이차트_상품별' not in all_ws:
        print(f"   🆕 '추이차트_상품별' 탭 생성...")
        ws_new = sh.add_worksheet(title='추이차트_상품별', rows=2000, cols=len(dc) + 10)
        all_ws['추이차트_상품별'] = ws_new
        time.sleep(1)

    # ══════════════════════════════════════════════════════════════════════
    # ★ 광고세트 순서: 최신 날짜 매출 내림차순 → 차순 날짜 매출 내림차순 다중 키 정렬
    # ══════════════════════════════════════════════════════════════════════
    tao = [('종합', None, None)]
    adset_sort_list = []
    for aid, info in all_adsets.items():
        sort_key = tuple(
            -(adset_daily.get(aid, {}).get(d, {}).get('revenue_mp', 0)
              if isinstance(adset_daily.get(aid, {}).get(d), dict) else 0)
            for d in dc
        )
        adset_sort_list.append((sort_key, info['campaign_name'], info['adset_name'], aid))
    adset_sort_list.sort()
    for _, cn, an, aid in adset_sort_list:
        tao.append((cn, an, aid))
    print(f"   ★ 광고세트 정렬 완료: {len(tao) - 1}개 (최신 날짜 매출 내림차순)")

    # ══════════════════════════════════════════════════════════════════════
    # 병렬 그룹 A: 독립 탭 (마스터탭, 예산)
    # 병렬 그룹 B: tao 의존 탭 (추이차트, 증감액, 추이차트(주간))
    # 병렬 그룹 C: 주간종합 3종
    # 병렬 그룹 D: ★ v4 추이차트_상품별 (adset_daily 의존)
    # ══════════════════════════════════════════════════════════════════════
    with ThreadPoolExecutor(max_workers=7, thread_name_prefix='sheets') as executor:
        futures = {}

        # 그룹 A: 독립
        futures['마스터탭'] = executor.submit(
            _update_master_tab, sh, all_ws['마스터탭'], master_rows
        )
        futures['예산'] = executor.submit(
            _update_budget_tab, all_ws['예산'], dc, daily_total, daily_product
        )

        # 그룹 B: tao 의존
        futures['추이차트'] = executor.submit(
            _update_trend_tab, sh, all_ws['추이차트'], tao, dc, daily_total, adset_daily
        )
        futures['증감액'] = executor.submit(
            _update_change_tab, sh, all_ws['증감액'], tao, dc, daily_total, adset_daily
        )
        futures['추이차트(주간)'] = executor.submit(
            _update_weekly_trend_tab, sh, all_ws['추이차트(주간)'], tao, all_dates, daily_total, master_rows
        )

        # 그룹 C: 주간종합 3종
        futures['주간종합'] = executor.submit(
            _update_weekly_summary_tabs, sh,
            all_ws['주간종합'], all_ws['주간종합_2'], all_ws['주간종합_3'],
            all_dates, daily_total, master_rows,
        )

        # ★ v4 그룹 D: 추이차트_상품별
        futures['추이차트_상품별'] = executor.submit(
            _update_product_trend_tab, sh, all_ws['추이차트_상품별'],
            dc, all_adsets, adset_daily, daily_total,
        )

        # 모든 완료 대기 + 에러 리포트
        for name, fut in futures.items():
            try:
                fut.result()
            except Exception as e:
                print(f"   ❌ {name} 실패: {e}")
                raise

    print(f"\n   ✅ 전체 시트 병렬 업데이트 완료")
    return len(master_rows), len(all_adsets)

# =============================================================================
# Main
# =============================================================================
def main():
    cfg = get_config()
    print(f"\n✅ 환경변수 로드 완료")
    print(f"   Meta 계정: {cfg['META_AD_ACCOUNT_ID']}")
    print(f"   기간: {cfg['FROM_DATE']} ~ {cfg['TO_DATE']}")
    print(f"   Mixpanel 이벤트: {cfg['MIXPANEL_EVENT_NAMES']}")

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

    meta_data, all_adsets = parse_meta(meta_raw)
    mp_matched, total_events, matched_events, _ = parse_and_match_mixpanel(mp_lines, all_adsets)
    master_rows, all_dates = build_master(meta_data, mp_matched)
    daily_total, daily_product, adset_daily = calc_derived(master_rows, all_dates)

    # ══════════════════════════════════════════════════════════════════════
    # Phase 3: Google Sheets 병렬 업데이트
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 80}")
    print(f"📝 Phase 3: Google Sheets 병렬 업데이트")
    print(f"{'=' * 80}")

    n_rows, n_adsets = update_sheets(cfg, gc, master_rows, all_dates, all_adsets, daily_total, daily_product, adset_daily)

    print(f"\n{'=' * 80}")
    print(f"✅ 전체 업데이트 완료!")
    print(f"   Meta: {cfg['META_AD_ACCOUNT_ID']} | 광고세트: {n_adsets}개")
    print(f"   Mixpanel: {total_events}건 | 매칭 {matched_events}건 ({matched_events / max(total_events, 1) * 100:.1f}%)")
    print(f"   Mixpanel 이벤트: {cfg['MIXPANEL_EVENT_NAMES']}")
    print(f"   기간: {cfg['FROM_DATE']} ~ {cfg['TO_DATE']} ({len(all_dates)}일)")
    print(f"   ★ v4: 추이차트_상품별 탭 추가")
    print(f"{'=' * 80}")


if __name__ == '__main__':
    main()
