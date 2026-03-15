# -*- coding: utf-8 -*-
"""
[Vanced] 퍼포먼스 데이터 자동 업데이트 v3
Meta Ads API (지출/노출/클릭) + Mixpanel (매출/결과수 - utm_term 매칭)
→ Google Sheets 전체 탭 자동 갱신

★ Colab 호환 버전
  - Colab 셀에서 os.environ 세팅 후 main() 호출
  - 또는 Google Colab Secrets / userdata 활용
"""

import os
import json
import time
import calendar
import gspread
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict, OrderedDict
from google.oauth2.service_account import Credentials

# =============================================================================
# 환경변수에서 설정 로드
# =============================================================================
def load_env():
    """로컬 실행 시 .env 파일에서 환경변수 로드 (Colab에서는 스킵)"""
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    except NameError:
        # Colab 환경: __file__ 미정의 → .env 로드 스킵
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
# 환경변수 읽기 (함수로 감싸서 lazy 로드)
# =============================================================================
def get_config():
    """환경변수에서 설정값을 읽어 dict로 반환"""
    cfg = {}
    cfg['META_ACCESS_TOKEN']   = os.environ.get('META_TOKEN_VANCED', '')
    cfg['META_AD_ACCOUNT_ID']  = os.environ.get('META_AD_ACCOUNT_ID', 'act_25183853061243175')
    cfg['META_API_VERSION']    = os.environ.get('META_API_VERSION', 'v21.0')
    cfg['META_BASE_URL']       = f"https://graph.facebook.com/{cfg['META_API_VERSION']}"

    cfg['MIXPANEL_PROJECT_ID'] = os.environ.get('MIXPANEL_PROJECT_ID', '3390233')
    cfg['MIXPANEL_USERNAME']   = os.environ.get('MIXPANEL_USERNAME', '')
    cfg['MIXPANEL_SECRET']     = os.environ.get('MIXPANEL_SECRET', '')
    cfg['MIXPANEL_EVENT_NAME'] = os.environ.get('MIXPANEL_EVENT_NAME', '결제완료')

    cfg['SPREADSHEET_URL']     = os.environ.get('SPREADSHEET_URL', '')
    cfg['GCP_SA_KEY_JSON']     = os.environ.get('GCP_SERVICE_ACCOUNT_KEY', '')

    cfg['FROM_DATE'] = os.environ.get('FROM_DATE', '2025-11-09')
    cfg['TO_DATE']   = os.environ.get('TO_DATE', datetime.now().strftime('%Y-%m-%d'))

    # ── 필수값 검증 ──
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
        print("\n💡 Colab에서는 아래처럼 셀에서 먼저 설정하세요:")
        print("   import os")
        print("   os.environ['META_TOKEN_VANCED'] = '...'")
        print("   os.environ['GCP_SERVICE_ACCOUNT_KEY'] = '{...}'")
        print("   os.environ['SPREADSHEET_URL'] = 'https://docs.google.com/...'")
        print("   os.environ['MIXPANEL_USERNAME'] = '...'")
        print("   os.environ['MIXPANEL_SECRET'] = '...'")
        raise ValueError(f"필수 환경변수 누락: {', '.join(missing)}")

    return cfg

# =============================================================================
# Google Sheets 인증 (서비스 계정) — lazy 초기화
# =============================================================================
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

def get_gc(cfg):
    """Google Sheets 클라이언트 생성"""
    try:
        sa_info = json.loads(cfg['GCP_SA_KEY_JSON'])
    except json.JSONDecodeError as e:
        print(f"\n❌ GCP_SERVICE_ACCOUNT_KEY JSON 파싱 실패: {e}")
        print("   서비스 계정 키 JSON이 올바른지 확인하세요.")
        raise
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
DARK_GREEN   = {"red": 0.22, "green": 0.46, "blue": 0.11}
BRIGHT_GREEN = {"red": 0, "green": 0.70, "blue": 0}
RED          = {"red": 0.85, "green": 0, "blue": 0}

def _fmt(color):
    return {"foregroundColor": color, "bold": True, "fontFamily": "Arial"}

def apply_rich_text(sh, ws, data_2d, start_row_idx, start_col_idx, cell_type='trend'):
    """ws: 이미 캐싱된 워크시트 객체를 직접 받음 (API 호출 절약)"""
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
        time.sleep(1)  # ★ 청크 간 sleep 추가
    print(f"      → 리치텍스트 포맷 적용 ({len(api_rows)}행)")

# =============================================================================
# Part 1: Meta Ads API
# =============================================================================
def fetch_meta(cfg):
    print("=" * 80)
    print("🚀 [Vanced] 퍼포먼스 데이터 자동 업데이트 v3")
    print("=" * 80)
    print(f"\n📡 [1] Meta Ads API: adset 일간 데이터 ({cfg['FROM_DATE']} ~ {cfg['TO_DATE']})...")

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
        print(f"   페이지 {page}: +{len(rows)}건 (누적 {len(meta_raw)}건)")
        next_url = data.get('paging', {}).get('next')
        if not next_url or not rows:
            break
        time.sleep(0.5)
    print(f"✅ Meta: {len(meta_raw)}건")

    if not meta_raw:
        print("   ⚠️  Meta에서 받아온 데이터가 0건입니다!")
        print("   확인사항:")
        print(f"     - META_TOKEN_1 유효한지 (만료 여부)")
        print(f"     - META_AD_ACCOUNT_ID: {cfg['META_AD_ACCOUNT_ID']}")
        print(f"     - 기간: {cfg['FROM_DATE']} ~ {cfg['TO_DATE']}")

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
# Part 2: Mixpanel
# =============================================================================
def fetch_mixpanel(cfg, all_adsets):
    print(f"\n📡 [2] Mixpanel API: 결제완료 이벤트 조회...")
    resp = requests.get(
        "https://data.mixpanel.com/api/2.0/export",
        params={
            'from_date': cfg['FROM_DATE'],
            'to_date': cfg['TO_DATE'],
            'event': json.dumps([cfg['MIXPANEL_EVENT_NAME']]),
            'project_id': cfg['MIXPANEL_PROJECT_ID'],
        },
        auth=(cfg['MIXPANEL_USERNAME'], cfg['MIXPANEL_SECRET']),
        timeout=300,
    )

    mp_matched = defaultdict(lambda: {'count': 0, 'revenue': 0.0})
    total_events, matched_events, unmatched_events = 0, 0, 0

    if resp.status_code != 200:
        print(f"❌ Mixpanel 오류: {resp.status_code} - {resp.text[:300]}")
        return mp_matched, 0, 0, 0

    lines = [l for l in resp.text.split('\n') if l.strip()]
    print(f"✅ Mixpanel: {len(lines)}건 수신")

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
            raw_amount = props.get('amount')
            raw_value = props.get('value')
            amount = 0.0
            if raw_amount is not None and str(raw_amount).strip() not in ['', 'None', 'nan', 'null']:
                try:
                    amount = float(raw_amount)
                except (ValueError, TypeError):
                    pass
            if amount <= 0 and raw_value is not None and str(raw_value).strip() not in ['', 'None', 'nan', 'null']:
                try:
                    amount = float(raw_value)
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

    # 검증
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
# Part 5: Google Sheets 업데이트
# =============================================================================
def update_sheets(cfg, gc, master_rows, all_dates, all_adsets, daily_total, daily_product, adset_daily):
    print(f"\n📝 [5] Google Sheets 업데이트...")
    sh = gc.open_by_url(cfg['SPREADSHEET_URL'])
    dc = all_dates

    # ★ 워크시트 객체 한 번에 캐싱 (fetch_sheet_metadata 호출 최소화)
    print(f"   워크시트 목록 캐싱...")
    all_ws = {ws.title: ws for ws in sh.worksheets()}
    print(f"   ✅ {len(all_ws)}개 탭 캐싱 완료: {list(all_ws.keys())}")

    # --- 5a. 마스터탭 ---
    print(f"\n   [1/7] 마스터탭...")
    ws = all_ws['마스터탭']
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
    print(f"   ✅ 마스터탭: {len(ms)}행")
    time.sleep(3)

    # --- 광고세트 순서 ---
    ws_t = all_ws['추이차트']
    existing = ws_t.get_all_values()
    tao = []
    for row in existing[1:]:
        if row[0] == '종합':
            tao.append(('종합', None, None))
        elif len(row) > 2 and row[2]:
            tao.append((row[0], row[1], row[2]))
    eids = {t[2] for t in tao if t[2]}
    for aid, info in all_adsets.items():
        if aid not in eids:
            tao.append((info['campaign_name'], info['adset_name'], aid))
    if not any(t[0] == '종합' for t in tao):
        tao.insert(0, ('종합', None, None))
    time.sleep(3)

    # --- 5b. 추이차트 ---
    print(f"\n   [2/7] 추이차트...")
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
    ws_t.clear()
    ws_t.update(range_name='A1', values=td)
    print(f"   ✅ {len(td) - 1}행 × {len(dc) + 4}열")
    time.sleep(3)
    apply_rich_text(sh, ws_t, [row[3:] for row in td[1:]], 1, 3, 'trend')
    time.sleep(5)

    # --- 5c. 증감액 ---
    print(f"\n   [3/7] 증감액...")
    ws_c = all_ws['증감액']
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
    ws_c.clear()
    ws_c.update(range_name='A1', values=cd)
    print(f"   ✅ {len(cd) - 1}행")
    time.sleep(3)
    apply_rich_text(sh, ws_c, [row[3:] for row in cd[1:]], 1, 3, 'change')
    time.sleep(5)

    # --- 5d. 예산 ---
    print(f"\n   [4/7] 예산...")
    ws_b = all_ws['예산']
    bd = [[''] + dc]
    bd.append(['전체 쓴돈'] + [round(daily_total.get(d, {}).get('spend', 0)) for d in dc])
    bd.append(['전체 번돈'] + [round(daily_total.get(d, {}).get('revenue', 0)) for d in dc])
    bd.append(['전체 순이익'] + [round(daily_total.get(d, {}).get('profit', 0)) for d in dc])
    bd.append(['총예산'] + [round(daily_total.get(d, {}).get('spend', 0)) for d in dc])
    for p in PRODUCTS:
        bd.append([f'{p} 예산'] + [round(daily_product.get((d, p), {}).get('spend', 0)) for d in dc])
    ws_b.clear()
    ws_b.update(range_name='A1', values=bd)
    print(f"   ✅ {len(bd)}행")
    time.sleep(3)

    # --- 5e. 추이차트(주간) ---
    print(f"\n   [5/7] 추이차트(주간)...")
    ws_wt = all_ws['추이차트(주간)']
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
    ws_wt.clear()
    ws_wt.update(range_name='A1', values=wtd)
    print(f"   ✅ {len(wtd) - 1}행")
    time.sleep(3)
    apply_rich_text(sh, ws_wt, [row[3:] for row in wtd[1:]], 1, 3, 'trend')
    time.sleep(10)  # ★ 주간종합 진입 전 넉넉한 대기

    # --- 5f. 주간종합 + _2 + _3 ---
    print(f"\n   [6/7] 주간종합 + _2 + _3...")
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

    wsd = []
    for mk in mps.keys():
        wsd.extend(bwb(mk, mt[mk], {p: mprod[mk].get(p, {'spend': 0, 'revenue': 0, 'pm': 0}) for p in PRODUCTS}))
        for wk in wks:
            if any(d in set(mps.get(mk, [])) for d in wp.get(wk, [])):
                wsd.extend(bwb(wk, wtf[wk], {p: wprod[wk].get(p, {'spend': 0, 'revenue': 0, 'pm': 0}) for p in PRODUCTS}))

    ws_weekly1 = all_ws['주간종합']
    ws_weekly1.clear()
    ws_weekly1.update(range_name='A1', values=wsd)
    print(f"   ✅ 주간종합: {len(wsd)}행")
    time.sleep(5)

    ws2 = all_ws['주간종합_2']
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
    ws2.clear()
    ws2.update(range_name='A1', values=w2)
    print(f"   ✅ 주간종합_2: {len(w2)}행")
    time.sleep(5)

    # --- 5g. 주간종합_3 ---
    print(f"\n   [7/7] 주간종합_3...")
    ws3 = all_ws['주간종합_3']
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
    ws3.clear()
    ws3.update(range_name='A1', values=w3)
    print(f"   ✅ {len(w3)}행")

    return len(ms), len(all_adsets)

# =============================================================================
# Main
# =============================================================================
def main():
    # ── 설정 로드 & 검증 ──
    cfg = get_config()
    print(f"\n✅ 환경변수 로드 완료")
    print(f"   Meta 계정: {cfg['META_AD_ACCOUNT_ID']}")
    print(f"   기간: {cfg['FROM_DATE']} ~ {cfg['TO_DATE']}")

    # ── Google Sheets 인증 ──
    gc = get_gc(cfg)
    print(f"   Google Sheets 인증 완료")

    # ── 데이터 수집 & 처리 ──
    meta_data, all_adsets = fetch_meta(cfg)
    if not meta_data:
        print("\n⚠️  Meta 데이터가 비어있어 시트 업데이트를 건너뜁니다.")
        return

    mp_matched, total_events, matched_events, _ = fetch_mixpanel(cfg, all_adsets)
    master_rows, all_dates = build_master(meta_data, mp_matched)
    daily_total, daily_product, adset_daily = calc_derived(master_rows, all_dates)
    n_rows, n_adsets = update_sheets(cfg, gc, master_rows, all_dates, all_adsets, daily_total, daily_product, adset_daily)

    print(f"\n{'=' * 80}")
    print(f"✅ 전체 업데이트 완료!")
    print(f"   Meta: {cfg['META_AD_ACCOUNT_ID']} | 광고세트: {n_adsets}개")
    print(f"   Mixpanel: {total_events}건 | 매칭 {matched_events}건 ({matched_events / max(total_events, 1) * 100:.1f}%)")
    print(f"   기간: {cfg['FROM_DATE']} ~ {cfg['TO_DATE']} ({len(all_dates)}일)")
    print(f"{'=' * 80}")


if __name__ == '__main__':
    main()
