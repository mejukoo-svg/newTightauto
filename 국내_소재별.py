# -*- coding: utf-8 -*-
# v3-ad 통합 코드 — ★★★ 병렬의 병렬 최적화 버전 ★★★
# 변경 사항:
#   1) Mixpanel 이벤트: "결제완료" + "payment_complete" OR 처리
#   2) 매출 필드: "amount" + "결제금액" OR 처리
#   3) Meta API: 날짜×계정 전체를 ThreadPoolExecutor로 동시 호출
#   4) Mixpanel: Meta와 동시에 concurrent.futures로 병렬 실행
#   5) 기존 탭 읽기: 병렬 읽기 (ThreadPoolExecutor)
#   6) 날짜 탭 쓰기: ★병렬 쓰기★ (세마포어로 rate-limit 제어)
#   7) 분석 탭 데이터 생성: 병렬 준비 + ★병렬 쓰기★
#   8) 분석 탭 서식: ★병렬 서식 적용★

print("=" * 60)
print("🚀 v3-ad 통합 자동화 (★ 병렬의 병렬 최적화 버전)")
print("   Meta/Mixpanel 동시 수집 + 탭 병렬 읽기/쓰기")
print("   날짜탭 병렬 쓰기 + 분석탭 병렬 쓰기 + 서식 병렬 적용")
print("=" * 60)

# =========================================================
# 인증
# =========================================================
import os, json

if 'GCP_SERVICE_ACCOUNT_KEY' in os.environ:
    import gspread
    from google.oauth2.service_account import Credentials
    service_account_info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_KEY'])
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    gc = gspread.authorize(creds)
    print("✅ GitHub Actions 서비스 계정 인증 완료")
else:
    from google.colab import auth
    auth.authenticate_user()
    import gspread
    from google.auth import default
    creds, _ = default(scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    print("✅ Google Colab 인증 완료")

SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL",
    "https://docs.google.com/spreadsheets/d/10bDQsuXYGHCNOpZkaZrtLYskwjIwBR-Rxv2D9fQF7fQ/edit?usp=sharing")
sh = gc.open_by_url(SPREADSHEET_URL)
print(f"✅ 스프레드시트: {sh.title}\n")

import requests as req_lib
import pandas as pd
from datetime import datetime, timedelta, timezone
import time, random, math, re, calendar, threading
from collections import defaultdict
from gspread.exceptions import APIError
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================================================
# 병렬 제어용 세마포어 / 락
# =========================================================
SHEETS_SEMAPHORE = threading.Semaphore(2)   # Sheets API 동시 2개
META_SEMAPHORE = threading.Semaphore(6)     # Meta API 동시 6개
DATA_LOCK = threading.Lock()                # 공유 dict 쓰기 보호
WRITE_SEMAPHORE = threading.Semaphore(2)    # Sheets 쓰기 동시 2개
FORMAT_SEMAPHORE = threading.Semaphore(2)   # Sheets 서식 동시 2개

# =========================================================
# Meta Ads API 설정
# =========================================================
META_TOKEN_DEFAULT = os.environ.get("META_TOKEN_1", "")
META_TOKENS = {
    "act_707835224206178":  os.environ.get("META_TOKEN_1", ""),
    "act_1270614404675034": os.environ.get("META_TOKEN_1", ""),
    "act_1808141386564262": os.environ.get("META_TOKEN_2", ""),
}
def get_token(acc_id):
    return META_TOKENS.get(acc_id, META_TOKEN_DEFAULT)

META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

# =========================================================
# Mixpanel 설정 — ★ 이벤트명 OR 처리 ★
# =========================================================
MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME = os.environ.get("MIXPANEL_USERNAME", "")
MIXPANEL_SECRET = os.environ.get("MIXPANEL_SECRET", "")
MIXPANEL_EVENT_NAMES = ["결제완료", "payment_complete"]  # ★ 둘 다 수집

# =========================================================
# 기본 설정
# =========================================================
KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None)
CURRENT_YEAR = TODAY.year
CURRENT_MONTH = TODAY.month
DATA_START_DATE = datetime(2026, 1, 1)
REFRESH_DAYS = 7
WEEKLY_TREND_REFRESH_WEEKS = 2

DATE_TAB_HEADERS = [
    "캠페인 이름", "광고 세트 이름", "광고 이름", "광고 세트 ID", "광고 ID",
    "지출 금액 (KRW)", "결과당 비용", "구매 ROAS(광고 지출 대비 수익률)",
    "CPM(1,000회 노출당 비용)", "도달", "노출",
    "고유 아웃바운드 클릭", "고유 아웃바운드 CTR(클릭률)", "고유 아웃바운드 클릭당 비용",
    "빈도", "결과",
    "결과(믹스패널)", "매출", "이익", "ROAS", "CVR",
    "기존 예산", "증액률", "변동 예산", "메모",
]

print(f"📅 현재 날짜: {TODAY.strftime('%Y-%m-%d')} (KST)")
print(f"📅 Meta/Mixpanel 수집: 최근 {REFRESH_DAYS}일만")
print(f"📅 ★ 병렬의 병렬: Meta(날짜×계정 동시) + Mixpanel 동시 + 탭 읽기/쓰기 병렬")
print(f"📅 ★ Mixpanel 이벤트: {MIXPANEL_EVENT_NAMES}")
print(f"📅 ★ 매출 필드: amount OR 결제금액")
print()

PRODUCT_KEYWORDS = ["재물", "솔로", "재회", "29금궁합", "자녀", "29금", "1%", "별해"]
WEEKDAY_NAMES = ['월', '화', '수', '목', '금', '토', '일']

COLORS = {
    "dark_gray": {"red":0.4,"green":0.4,"blue":0.4}, "black": {"red":0.2,"green":0.2,"blue":0.2},
    "light_gray": {"red":0.9,"green":0.9,"blue":0.9}, "light_gray2": {"red":0.95,"green":0.95,"blue":0.95},
    "white": {"red":1,"green":1,"blue":1}, "green": {"red":0.56,"green":0.77,"blue":0.49},
    "light_yellow": {"red":1.0,"green":0.98,"blue":0.8}, "light_blue": {"red":0.85,"green":0.92,"blue":0.98},
    "light_red": {"red":1.0,"green":0.85,"blue":0.85}, "dark_blue": {"red":0.2,"green":0.3,"blue":0.5},
    "navy": {"red":0.15,"green":0.2,"blue":0.35}, "dark_green": {"red":0.2,"green":0.5,"blue":0.3},
    "orange": {"red":0.9,"green":0.5,"blue":0.2}, "purple": {"red":0.5,"green":0.3,"blue":0.6},
}

SUMMARY_PRODUCTS = ["집착", "결혼", "커리어", "1%", "별해", "솔로", "재물", "재회", "29금궁합", "자녀", "29금"]
ANALYSIS_TAB_NAMES = ["추이차트", "추이차트(주간)", "증감액", "예산", "주간종합", "주간종합_2", "주간종합_3", "마스터탭", "소재랭킹"]


# =========================================================
# 유틸리티 함수
# =========================================================
def with_retry(fn, *args, max_retries=8, **kwargs):
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e):
                wait_time = 20 + (attempt * 15) + random.random()
                print(f"  ⏳ Sheets Rate limit. {wait_time:.0f}초 대기 (시도 {attempt+1}/{max_retries})")
                time.sleep(wait_time)
            elif attempt == max_retries - 1:
                raise
            else:
                time.sleep(5 + random.random())
    return None

def with_retry_sem(fn, *args, max_retries=8, **kwargs):
    """Sheets API 호출을 세마포어로 감싸는 with_retry"""
    with SHEETS_SEMAPHORE:
        return with_retry(fn, *args, max_retries=max_retries, **kwargs)

def with_write_sem(fn, *args, max_retries=8, **kwargs):
    """Sheets 쓰기를 WRITE_SEMAPHORE로 감싸는 with_retry"""
    with WRITE_SEMAPHORE:
        return with_retry(fn, *args, max_retries=max_retries, **kwargs)

def with_format_sem(fn, *args, max_retries=8, **kwargs):
    """Sheets 서식을 FORMAT_SEMAPHORE로 감싸는 with_retry"""
    with FORMAT_SEMAPHORE:
        return with_retry(fn, *args, max_retries=max_retries, **kwargs)

def clean_id(val):
    if val is None: return ""
    s = str(val).strip()
    if not s: return ""
    try:
        if ("E" in s or "e" in s) and re.match(r'^[\d.]+[eE][+\-]?\d+$', s):
            return str(int(float(s)))
    except: pass
    try:
        if re.match(r'^\d+\.\d+$', s):
            return str(int(float(s)))
    except: pass
    if re.match(r'^\d+$', s): return s
    numeric_only = re.sub(r'[^0-9]', '', s)
    return numeric_only if numeric_only else s

def extract_product(cn):
    for p in PRODUCT_KEYWORDS:
        if p in cn: return p
    return "기타"

def normalize_date_key(ds):
    try:
        parts = ds.split('/')
        if len(parts) == 3:
            return f"{int(parts[0]):02d}/{int(parts[1]):02d}/{int(parts[2]):02d}"
        elif len(parts) == 2:
            m, d = int(parts[0]), int(parts[1])
            year = 2026 if m <= 3 else 2025
            return f"{year % 100:02d}/{m:02d}/{d:02d}"
        return ds
    except: return ds

def _to_num(x):
    try:
        v = str(x).replace(",","").replace("₩","").replace("%","").replace("\\","").replace("W","").replace("￦","").strip()
        return float(v) if v and v not in ["-","#DIV/0!"] else 0.0
    except: return 0.0

def money(n):
    try: return f"₩{int(round(float(n))):,}"
    except: return "₩0"

def cell_text(profit, revenue, spend, cpm=0, cvr=0):
    if spend == 0: return ""
    roas = (revenue / spend * 100) if spend > 0 else 0
    return f"{roas:.0f}\n{money(profit)}\n-{money(spend)}\n{'₩'+str(int(round(cpm)))+'' if cpm > 0 else '₩0'}\n{cvr:.1f}%"

def cell_text_change(roas, chg, spend, cpm, cvr=0):
    if spend == 0: return ""
    cl = f"+{chg:.1f}%" if chg > 0 else f"{chg:.1f}%" if chg < 0 else "0.0%"
    return f"{roas:.0f}\n{cl}\n-{money(spend)}\n{'₩'+str(int(round(cpm))) if cpm > 0 else '₩0'}\n{cvr:.1f}%"

def find_col(headers, kws):
    for kw in kws:
        kl = kw.lower()
        for i, h in enumerate(headers):
            if kl in str(h).lower(): return i
    return None

def get_week_range(d):
    wd = d.weekday(); m = d - timedelta(days=wd); s = m + timedelta(days=6)
    return f"'{m.month}/{m.day}({WEEKDAY_NAMES[0]})~{s.month}/{s.day}({WEEKDAY_NAMES[6]})"

def get_week_range_short(d):
    wd = d.weekday(); m = d - timedelta(days=wd); s = m + timedelta(days=6)
    return f"{m.month}/{m.day}~{s.month}/{s.day}"

def get_month_range_display(f, l):
    return f"'{f.month}.{f.day}~{l.month}.{l.day}"

def get_week_monday(d):
    return d - timedelta(days=d.weekday())

def parse_date_tab(tab_name):
    if '/' not in tab_name: return None
    if tab_name in ANALYSIS_TAB_NAMES: return None
    try:
        parts = tab_name.split('/')
        if len(parts) == 3:
            y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            return datetime(2000 + y, m, d)
        elif len(parts) == 2:
            m, d = int(parts[0]), int(parts[1])
            year = 2026 if m <= 3 else 2025
            return datetime(year, m, d)
        return None
    except: return None

def get_cell_format(bg=None, tc=None, bold=False, ha="CENTER"):
    f = {"horizontalAlignment": ha, "verticalAlignment": "MIDDLE"}
    if bg: f["backgroundColor"] = bg
    if tc or bold:
        f["textFormat"] = {}
        if tc: f["textFormat"]["foregroundColor"] = tc
        if bold: f["textFormat"]["bold"] = True
    return f

def create_format_request(sid, sr, er, sc, ec, fmt):
    return {"repeatCell": {"range": {"sheetId": sid, "startRowIndex": sr, "endRowIndex": er, "startColumnIndex": sc, "endColumnIndex": ec}, "cell": {"userEnteredFormat": fmt}, "fields": "userEnteredFormat"}}

def create_border_request(sid, sr, er, sc, ec):
    bs = {"style": "SOLID", "width": 1, "color": {"red": 0.7, "green": 0.7, "blue": 0.7}}
    return {"updateBorders": {"range": {"sheetId": sid, "startRowIndex": sr, "endRowIndex": er, "startColumnIndex": sc, "endColumnIndex": ec}, "top": bs, "bottom": bs, "left": bs, "right": bs, "innerHorizontal": bs, "innerVertical": bs}}

def create_number_format_request(sid, sr, er, sc, ec, ft="NUMBER", p="#,##0"):
    return {"repeatCell": {"range": {"sheetId": sid, "startRowIndex": sr, "endRowIndex": er, "startColumnIndex": sc, "endColumnIndex": ec}, "cell": {"userEnteredFormat": {"numberFormat": {"type": ft, "pattern": p}}}, "fields": "userEnteredFormat.numberFormat"}}

def match_summary_product(campaign_name, product_keyword):
    cn = campaign_name
    if product_keyword == "29금":
        return "29금" in cn and "29금궁합" not in cn
    return product_keyword in cn


# =========================================================
# 서식 함수들
# =========================================================
def apply_c2_label_formatting(sh, ws):
    sid = ws.id; cv = "ROAS\n순이익\n지출금액\nCPM\nCVR"
    lines = cv.split('\n'); indices = [0]; pos = 0
    for line in lines[:-1]: pos += len(line) + 1; indices.append(pos)
    bk = {"foregroundColor": {"red":0,"green":0,"blue":0}}
    dg = {"foregroundColor": {"red":0.22,"green":0.46,"blue":0.11}}
    rd = {"foregroundColor": {"red":0.85,"green":0.0,"blue":0.0}}
    bl = {"foregroundColor": {"red":0.0,"green":0.0,"blue":0.85}}
    req = {"updateCells": {"range": {"sheetId": sid, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 2, "endColumnIndex": 3},
        "rows": [{"values": [{"userEnteredValue": {"stringValue": cv}, "textFormatRuns": [
            {"startIndex": indices[0], "format": bk}, {"startIndex": indices[1], "format": dg},
            {"startIndex": indices[2], "format": rd}, {"startIndex": indices[3], "format": bl},
            {"startIndex": indices[4], "format": bk}]}]}], "fields": "userEnteredValue,textFormatRuns"}}
    with_retry(sh.batch_update, body={"requests": [req]}); time.sleep(1)

def apply_trend_chart_formatting(sh, ws, headers, rows_count, is_change_tab=False, sunday_col_indices=None, format_col_end=None):
    sid = ws.id
    try: with_retry(ws.format, 'A1:Z1', {'backgroundColor': {'red':0.9,'green':0.9,'blue':0.9}, 'textFormat': {'bold': True}}); time.sleep(1)
    except: pass
    if sunday_col_indices:
        try:
            sr = [create_format_request(sid, 0, 1, ci, ci+1, get_cell_format(COLORS["light_red"], bold=True)) for ci in sunday_col_indices]
            if sr: with_retry(sh.batch_update, body={"requests": sr}); time.sleep(1)
        except: pass
    try: with_retry(ws.format, 'A2:Z2', {'textFormat': {'bold': True}}); time.sleep(1)
    except: pass
    try: with_retry(sh.batch_update, body={"requests": [{"updateSheetProperties": {"properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": 1}}, "fields": "gridProperties.frozenRowCount"}}]}); time.sleep(2)
    except: pass
    try: with_retry(ws.format, 'D2:Z1000', {'wrapStrategy': 'WRAP', 'verticalAlignment': 'TOP'}); time.sleep(1)
    except: pass
    try:
        cwr = [{"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": cn, "endIndex": cn+1}, "properties": {"pixelSize": 95}, "fields": "pixelSize"}} for cn in range(3, min(len(headers), 26))]
        if cwr: with_retry(sh.batch_update, body={"requests": cwr})
    except: pass

    print("  🎨 ROAS 조건부 서식...")
    try:
        sc, ec = 3, len(headers)
        rules = [
            {'c': '=AND(NOT(ISBLANK(INDIRECT(ADDRESS(ROW(),COLUMN())))),LEN(TRIM(INDIRECT(ADDRESS(ROW(),COLUMN()))))>0,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))=0)', 'clr': {'red':1.0,'green':0.6,'blue':0.6}},
            {'c': '=AND(NOT(ISBLANK(INDIRECT(ADDRESS(ROW(),COLUMN())))),LEN(TRIM(INDIRECT(ADDRESS(ROW(),COLUMN()))))>0,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))>=300)', 'clr': {'red':0.6,'green':1.0,'blue':1.0}},
            {'c': '=AND(NOT(ISBLANK(INDIRECT(ADDRESS(ROW(),COLUMN())))),LEN(TRIM(INDIRECT(ADDRESS(ROW(),COLUMN()))))>0,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))>=200,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))<300)', 'clr': {'red':0.7,'green':1.0,'blue':0.7}},
            {'c': '=AND(NOT(ISBLANK(INDIRECT(ADDRESS(ROW(),COLUMN())))),LEN(TRIM(INDIRECT(ADDRESS(ROW(),COLUMN()))))>0,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))>=100,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))<200)', 'clr': {'red':1.0,'green':1.0,'blue':0.6}},
            {'c': '=AND(NOT(ISBLANK(INDIRECT(ADDRESS(ROW(),COLUMN())))),LEN(TRIM(INDIRECT(ADDRESS(ROW(),COLUMN()))))>0,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))<100,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))>0)', 'clr': {'red':1.0,'green':0.8,'blue':0.8}},
        ]
        fr = [{'addConditionalFormatRule': {'rule': {'ranges': [{'sheetId': sid, 'startRowIndex': 1, 'endRowIndex': rows_count+2, 'startColumnIndex': sc, 'endColumnIndex': ec}],
            'booleanRule': {'condition': {'type': 'CUSTOM_FORMULA', 'values': [{'userEnteredValue': r['c']}]}, 'format': {'backgroundColor': r['clr']}}}, 'index': 0}} for r in rules]
        with_retry(sh.batch_update, body={'requests': fr}); time.sleep(3)
    except Exception as e:
        print(f"  ⚠️ 조건부 서식 오류: {e}")

    tcs = 3
    tce = min(format_col_end or len(headers), len(headers), 20)
    print(f"  🎨 텍스트 색상 (col {tcs}~{tce-1})...")
    try:
        fr = []
        bk = {"foregroundColor":{"red":0,"green":0,"blue":0}}
        dg3 = {"foregroundColor":{"red":0.22,"green":0.46,"blue":0.11}}
        rd = {"foregroundColor":{"red":0.85,"green":0.0,"blue":0.0}}
        gn = {"foregroundColor":{"red":0.0,"green":0.7,"blue":0.0}}
        bl = {"foregroundColor":{"red":0.0,"green":0.0,"blue":0.85}}

        # ★ 컬럼별 병렬 읽기 후 배치 서식 ★
        col_values_cache = {}
        def _read_col(ci):
            try:
                vals = with_retry_sem(ws.col_values, ci+1)
                return (ci, vals)
            except:
                return (ci, None)

        with ThreadPoolExecutor(max_workers=4) as col_executor:
            col_futures = [col_executor.submit(_read_col, ci) for ci in range(tcs, tce)]
            for cf in as_completed(col_futures):
                ci, vals = cf.result()
                if vals: col_values_cache[ci] = vals

        for ci in range(tcs, tce):
            cv = col_values_cache.get(ci)
            if not cv or len(cv) < 2: continue
            for ri in range(2, min(len(cv)+1, rows_count+3)):
                val = cv[ri-1] if ri-1 < len(cv) else ""
                if not val or '\n' not in val: continue
                lines = val.split('\n')
                if len(lines) < 4: continue
                l1e = len(lines[0]); l2e = l1e+1+len(lines[1]); l3e = l2e+1+len(lines[2]); l4s = l3e+1
                if len(lines) >= 5: l4e = l4s+len(lines[3]); l5s = l4e+1
                if is_change_tab:
                    cc = gn if lines[1].startswith('+') else rd if lines[1].startswith('-') else bk
                    tr = [{"startIndex":0,"format":bk},{"startIndex":l1e+1,"format":cc},{"startIndex":l2e+1,"format":rd},{"startIndex":l4s,"format":bl}]
                else:
                    tr = [{"startIndex":0,"format":bk},{"startIndex":l1e+1,"format":dg3},{"startIndex":l2e+1,"format":rd},{"startIndex":l4s,"format":bl}]
                if len(lines) >= 5: tr.append({"startIndex":l5s,"format":bk})
                fr.append({"updateCells":{"range":{"sheetId":sid,"startRowIndex":ri-1,"endRowIndex":ri,"startColumnIndex":ci,"endColumnIndex":ci+1},"rows":[{"values":[{"userEnteredValue":{"stringValue":val},"textFormatRuns":tr}]}],"fields":"userEnteredValue,textFormatRuns"}})
                if len(fr) >= 300:
                    try: with_retry(sh.batch_update, body={"requests": fr}); fr = []; time.sleep(3)
                    except: fr = []
        if fr:
            try: with_retry(sh.batch_update, body={"requests": fr}); time.sleep(2)
            except: pass
        print("  ✅ 텍스트 색상 완료")
    except Exception as e:
        print(f"  ⚠️ 텍스트 색상 오류: {e}")


# =========================================================
# Meta Ads API 함수
# =========================================================
def meta_api_get(url, params=None, token=None):
    if params is None: params = {}
    params['access_token'] = token or META_TOKEN_DEFAULT
    for attempt in range(5):
        try:
            resp = req_lib.get(url, params=params, timeout=120)
            if resp.status_code == 200: return resp.json()
            elif resp.status_code == 400:
                err = resp.json().get('error', {})
                print(f"  ❌ Meta 400: {err.get('message', resp.text[:200])}"); return None
            elif resp.status_code in [429, 500, 502, 503]:
                wait = 30 + attempt * 30
                print(f"  ⏳ Meta {resp.status_code}, {wait}초 대기 (시도 {attempt+1}/5)")
                time.sleep(wait)
            else:
                print(f"  ❌ Meta {resp.status_code}: {resp.text[:200]}")
                if attempt < 4: time.sleep(15)
                else: return None
        except Exception as e:
            print(f"  ❌ Meta 요청 오류: {e}")
            if attempt < 4: time.sleep(15)
            else: return None
    return None

def extract_action_value(al, types):
    if not al: return 0
    for a in al:
        if a.get('action_type','') in types:
            try: return float(a.get('value',0))
            except: return 0
    return 0

def fetch_meta_insights_daily(ad_account_id, single_date, active_ids=None):
    url = f"{META_BASE_URL}/{ad_account_id}/insights"
    fields = 'campaign_name,adset_name,adset_id,ad_name,ad_id,spend,cpm,reach,impressions,frequency,actions,cost_per_action_type,purchase_roas,unique_outbound_clicks,unique_outbound_clicks_ctr,cost_per_unique_outbound_click'
    params = {
        'fields': fields, 'level': 'ad', 'time_increment': 1,
        'time_range': json.dumps({'since': single_date, 'until': single_date}),
        'limit': 500,
        'filtering': json.dumps([{'field': 'spend', 'operator': 'GREATER_THAN', 'value': '0'}])
    }
    all_results = []
    data = meta_api_get(url, params, token=get_token(ad_account_id))
    while data:
        rows = data.get('data', [])
        all_results.extend(rows)
        next_url = data.get('paging', {}).get('next')
        if next_url:
            time.sleep(1)
            try:
                resp = req_lib.get(next_url, timeout=120)
                data = resp.json() if resp.status_code == 200 else None
            except: data = None
        else:
            break
    if active_ids:
        all_results = [r for r in all_results if r.get('ad_id') in active_ids]
    return all_results

def parse_single_day_insights(rows, date_str, date_obj):
    purchase_types = ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase']
    outbound_types = ['outbound_click']
    parsed = []
    for row in rows:
        spend = float(row.get('spend', 0))
        cpm = float(row.get('cpm', 0))
        reach = int(float(row.get('reach', 0)))
        impressions = int(float(row.get('impressions', 0)))
        frequency = float(row.get('frequency', 0))
        actions = row.get('actions', [])
        results = extract_action_value(actions, purchase_types)
        cost_per_action = row.get('cost_per_action_type', [])
        cost_per_result = extract_action_value(cost_per_action, purchase_types)
        purchase_roas_list = row.get('purchase_roas', [])
        meta_roas = extract_action_value(purchase_roas_list, purchase_types)
        uo = row.get('unique_outbound_clicks', [])
        unique_clicks = extract_action_value(uo, outbound_types)
        uc = row.get('unique_outbound_clicks_ctr', [])
        unique_ctr = extract_action_value(uc, outbound_types)
        cpu = row.get('cost_per_unique_outbound_click', [])
        cost_per_click = extract_action_value(cpu, outbound_types)
        parsed.append({
            'campaign_name': row.get('campaign_name', ''), 'adset_name': row.get('adset_name', ''),
            'ad_name': row.get('ad_name', ''), 'adset_id': row.get('adset_id', ''),
            'ad_id': row.get('ad_id', ''), 'spend': spend, 'cost_per_result': cost_per_result,
            'meta_roas': meta_roas, 'cpm': cpm, 'reach': reach,
            'impressions': impressions, 'unique_clicks': unique_clicks,
            'unique_ctr': unique_ctr, 'cost_per_unique_click': cost_per_click,
            'frequency': frequency, 'results': results, 'date_obj': date_obj,
        })
    return parsed


# =========================================================
# Mixpanel 함수 — ★ 이벤트명 OR + 매출 필드 OR ★
# =========================================================
def fetch_mixpanel_data(from_date, to_date):
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {'from_date': from_date, 'to_date': to_date,
              'event': json.dumps(MIXPANEL_EVENT_NAMES),  # ★ 둘 다 수집
              'project_id': MIXPANEL_PROJECT_ID}
    print(f"  📡 Mixpanel: {from_date} ~ {to_date}")
    print(f"  📡 이벤트: {MIXPANEL_EVENT_NAMES}")
    try:
        resp = req_lib.get(url, params=params, auth=(MIXPANEL_USERNAME, MIXPANEL_SECRET), timeout=300)
        if resp.status_code != 200:
            print(f"  ❌ Mixpanel {resp.status_code}"); return []
        lines = [l for l in resp.text.split('\n') if l.strip()]
        print(f"  📊 이벤트: {len(lines)}건")
        data = []
        stat_amount_only = stat_value_only = stat_both = stat_neither = 0
        event_name_counts = defaultdict(int)
        for line in lines:
            try:
                ev = json.loads(line); props = ev.get('properties', {})
                event_name = ev.get('event', '')
                event_name_counts[event_name] += 1
                ts = props.get('time', 0)
                if ts:
                    dt_kst = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)
                    ds = f"{dt_kst.year % 100:02d}/{dt_kst.month:02d}/{dt_kst.day:02d}"
                else: ds = None
                ut = None
                for k in ['utm_content','UTM_Content','UTM Content']:
                    if k in props and props[k]: ut = str(props[k]).strip(); break
                if ut: ut = clean_id(ut)

                # ★★★ amount OR 결제금액 ★★★
                raw_amount = props.get('amount')
                if raw_amount is None:
                    raw_amount = props.get('결제금액')
                raw_value = props.get('value')

                amount_val = 0.0
                if raw_amount is not None:
                    try: amount_val = float(raw_amount)
                    except: amount_val = 0.0
                value_val = 0.0
                if raw_value is not None:
                    try: value_val = float(raw_value)
                    except: value_val = 0.0
                revenue = amount_val if amount_val > 0 else (value_val if value_val > 0 else 0.0)
                has_amount = amount_val > 0; has_value = value_val > 0
                if has_amount and has_value: stat_both += 1
                elif has_amount: stat_amount_only += 1
                elif has_value: stat_value_only += 1
                else: stat_neither += 1
                data.append({'distinct_id': props.get('distinct_id'), 'time': ts, 'date': ds,
                    'utm_content': ut or '', 'amount': amount_val, 'value_raw': value_val,
                    'revenue': revenue, '서비스': props.get('서비스', '')})
            except: pass
        print(f"  ✅ 파싱: {len(data)}건")
        print(f"  📊 이벤트별: {dict(event_name_counts)}")
        print(f"  📊 매출 출처: amount/결제금액={stat_amount_only}, value만={stat_value_only}, 둘다={stat_both}, 없음={stat_neither}")
        return data
    except Exception as e:
        print(f"  ❌ Mixpanel 오류: {e}"); return []


# =========================================================
# 날짜탭 하단 요약표
# =========================================================
def generate_date_tab_summary(rows):
    total_spend = total_meta_purchase = total_mp_purchase = total_revenue = total_profit = total_unique_clicks = 0.0
    spend_by_account = {"본계정": 0.0, "부계정": 0.0, "3rd계정": 0.0}
    prod_spend = {p: 0.0 for p in SUMMARY_PRODUCTS}
    prod_revenue = {p: 0.0 for p in SUMMARY_PRODUCTS}
    prod_profit = {p: 0.0 for p in SUMMARY_PRODUCTS}

    for row in rows:
        cn = str(row[0]) if len(row) > 0 else ""
        asid = str(row[3]) if len(row) > 3 else ""
        sp = _to_num(row[5]) if len(row) > 5 else 0
        meta_p = _to_num(row[15]) if len(row) > 15 else 0
        mp_p = _to_num(row[16]) if len(row) > 16 else 0
        rev = _to_num(row[17]) if len(row) > 17 else 0
        prof = _to_num(row[18]) if len(row) > 18 else 0
        uc = _to_num(row[11]) if len(row) > 11 else 0
        if not cn or cn in ["전체", "합계", "Total"]: continue
        total_spend += sp; total_meta_purchase += meta_p; total_mp_purchase += mp_p
        total_revenue += rev; total_profit += prof; total_unique_clicks += uc
        if asid.startswith("12023"): spend_by_account["본계정"] += sp
        elif asid.startswith("12024"): spend_by_account["부계정"] += sp
        elif asid.startswith("6"): spend_by_account["3rd계정"] += sp
        for p in SUMMARY_PRODUCTS:
            if match_summary_product(cn, p):
                prod_spend[p] += sp; prod_revenue[p] += rev; prod_profit[p] += prof; break

    total_roas = (total_revenue / total_spend * 100) if total_spend > 0 else 0
    total_cvr = (total_mp_purchase / total_unique_clicks * 100) if total_unique_clicks > 0 else 0

    summary_data = [[""] * 25, [""] * 25]
    r_title = [""] * 25; r_title[14] = "전체"; summary_data.append(r_title)
    r_hdr = [""] * 25
    r_hdr[11] = "본계정"; r_hdr[12] = "부계정"; r_hdr[13] = "3rd 계정"
    r_hdr[14] = "지출 금액 (KRW)"; r_hdr[15] = "구매 (메타)"; r_hdr[16] = "구매 (믹스패널)"
    r_hdr[17] = "매출"; r_hdr[18] = "이익"; r_hdr[19] = "ROAS"; r_hdr[20] = "CVR"
    summary_data.append(r_hdr)
    r_val = [""] * 25
    r_val[11] = round(spend_by_account["본계정"]); r_val[12] = round(spend_by_account["부계정"])
    r_val[13] = round(spend_by_account["3rd계정"]); r_val[14] = round(total_spend)
    r_val[15] = round(total_meta_purchase); r_val[16] = round(total_mp_purchase)
    r_val[17] = round(total_revenue); r_val[18] = round(total_profit)
    r_val[19] = round(total_roas, 1); r_val[20] = total_cvr / 100 if total_cvr else 0
    summary_data.append(r_val)

    sp_list = SUMMARY_PRODUCTS
    total_prod_spend = sum(prod_spend[p] for p in sp_list)
    total_prod_revenue = sum(prod_revenue[p] for p in sp_list)
    total_prod_profit = sum(prod_profit[p] for p in sp_list)

    for table_title, data_type in [("제품별 ROAS","roas"),("제품별 순이익","profit"),("제품별 매출","revenue"),
        ("제품별 순이익율","profit_margin"),("제품별 예산","spend"),("제품별 예산 비중","spend_ratio")]:
        summary_data.append([""] * 25)
        r_tt = [""] * 25; r_tt[12] = table_title; summary_data.append(r_tt)
        r_ph = [""] * 25
        for i, p in enumerate(sp_list): r_ph[9 + i] = p
        r_ph[9 + len(sp_list)] = "합"; summary_data.append(r_ph)
        r_pv = [""] * 25
        for i, p in enumerate(sp_list):
            ps = prod_spend[p]; pr = prod_revenue[p]; pp = prod_profit[p]
            if data_type == "roas": r_pv[9+i] = pr/ps if ps > 0 else 0
            elif data_type == "profit": r_pv[9+i] = round(pp)
            elif data_type == "revenue": r_pv[9+i] = round(pr)
            elif data_type == "profit_margin": r_pv[9+i] = pp/pr if pr > 0 else 0
            elif data_type == "spend": r_pv[9+i] = round(ps)
            elif data_type == "spend_ratio": r_pv[9+i] = ps/total_prod_spend if total_prod_spend > 0 else 0
        if data_type == "roas": r_pv[20] = total_prod_revenue/total_prod_spend if total_prod_spend > 0 else 0
        elif data_type == "profit": r_pv[20] = round(total_prod_profit)
        elif data_type == "revenue": r_pv[20] = round(total_prod_revenue)
        elif data_type == "profit_margin": r_pv[20] = total_prod_profit/total_prod_revenue if total_prod_revenue > 0 else 0
        elif data_type == "spend": r_pv[20] = round(total_prod_spend)
        elif data_type == "spend_ratio": r_pv[20] = ""
        summary_data.append(r_pv)
    return summary_data


def format_date_tab_summary(sh, ws, data_row_count, summary_row_count):
    sid = ws.id; base = data_row_count + 1
    C_PINK = {"red":0.957,"green":0.8,"blue":0.8}; C_ORANGE = {"red":0.988,"green":0.898,"blue":0.804}
    C_LGREEN = {"red":0.851,"green":0.918,"blue":0.827}; C_GRAY = {"red":0.69,"green":0.7,"blue":0.698}
    C_DGRAY = {"red":0.6,"green":0.6,"blue":0.6}; C_BLACK = {"red":0,"green":0,"blue":0}
    C_WHITE = {"red":1,"green":1,"blue":1}; C_LYELLOW = {"red":1,"green":1,"blue":0.8}
    fmt_requests = []
    r = base + 2
    fmt_requests.append(create_format_request(sid, r, r+1, 14, 21, {"horizontalAlignment": "CENTER", "textFormat": {"bold": True}}))
    fmt_requests.append({"mergeCells": {"range": {"sheetId": sid, "startRowIndex": r, "endRowIndex": r+1, "startColumnIndex": 14, "endColumnIndex": 21}, "mergeType": "MERGE_ALL"}})
    r = base + 3
    hdr_fmt_base = {"textFormat": {"bold": True, "foregroundColor": C_BLACK}, "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}
    fmt_requests.append(create_format_request(sid, r, r+1, 13, 14, {**hdr_fmt_base, "backgroundColor": C_PINK}))
    fmt_requests.append(create_format_request(sid, r, r+1, 12, 13, {**hdr_fmt_base, "backgroundColor": C_ORANGE}))
    fmt_requests.append(create_format_request(sid, r, r+1, 13, 14, {**hdr_fmt_base, "backgroundColor": C_LGREEN}))
    fmt_requests.append(create_format_request(sid, r, r+1, 14, 20, {**hdr_fmt_base, "backgroundColor": C_GRAY}))
    fmt_requests.append(create_format_request(sid, r, r+1, 20, 21, {**hdr_fmt_base, "backgroundColor": C_DGRAY}))
    r = base + 4
    fmt_requests.append(create_number_format_request(sid, r, r+1, 11, 20, "NUMBER", "#,##0"))
    fmt_requests.append(create_number_format_request(sid, r, r+1, 19, 20, "NUMBER", "#,##0.0"))
    fmt_requests.append(create_number_format_request(sid, r, r+1, 20, 21, "NUMBER", "0.00%"))
    fmt_requests.append(create_border_request(sid, base+3, base+5, 11, 21))
    table_formats = [("ROAS","0.00%"),("순이익","[$₩-412]#,##0"),("매출","#,##0"),("순이익율","0.00%"),("예산","[$₩-412]#,##0"),("예산비중","0.00%")]
    offset = base + 5; prod_hdr_fmt = {**hdr_fmt_base, "backgroundColor": C_GRAY}
    for t_idx, (tname, nfmt) in enumerate(table_formats):
        t_start = offset + t_idx * 4
        fmt_requests.append(create_format_request(sid, t_start+1, t_start+2, 14, 21, {"horizontalAlignment": "CENTER", "textFormat": {"bold": True}}))
        fmt_requests.append({"mergeCells": {"range": {"sheetId": sid, "startRowIndex": t_start+1, "endRowIndex": t_start+2, "startColumnIndex": 14, "endColumnIndex": 20}, "mergeType": "MERGE_ALL"}})
        fmt_requests.append(create_format_request(sid, t_start+2, t_start+3, 9, 21, prod_hdr_fmt))
        fmt_requests.append(create_number_format_request(sid, t_start+3, t_start+4, 9, 21, "NUMBER", nfmt))
        if t_idx in [1, 2, 4]:
            fmt_requests.append(create_format_request(sid, t_start+3, t_start+4, 20, 21, {"backgroundColor": C_LYELLOW, "textFormat": {"bold": True}}))
            fmt_requests.append(create_number_format_request(sid, t_start+3, t_start+4, 20, 21, "NUMBER", "[$₩-412]#,##0"))
        fmt_requests.append(create_border_request(sid, t_start+2, t_start+4, 9, 21))
    return fmt_requests


# =============================================================================
# ★★★ 실행 시작 — 병렬의 병렬 처리 버전 ★★★
# =============================================================================

# =========================================================
# 1단계: 광고 계정 설정
# =========================================================
print("\n" + "="*60)
print("1단계: 광고 계정 설정")
print("="*60)
ALL_AD_ACCOUNTS = ["act_707835224206178", "act_1270614404675034", "act_1808141386564262"]
print(f"📋 광고 계정: {len(ALL_AD_ACCOUNTS)}개\n")

# =========================================================
# ★★★ 2~4단계: 기존 탭 읽기 + Meta + Mixpanel — 모두 병렬 ★★★
# =========================================================
print("="*60)
print("2~4단계: 기존 탭 읽기 + Meta + Mixpanel ★ 동시 병렬 실행 ★")
print("="*60)

ad_sets = defaultdict(lambda: {'campaign_name': '', 'adset_name': '', 'ad_name': '', 'adset_id': '', 'ad_id': '', 'dates': {}})
budget_by_date = defaultdict(lambda: defaultdict(lambda: {'spend': 0.0, 'revenue': 0.0}))
product_count = defaultdict(int)
date_objects = {}
date_tab_rows = defaultdict(list)
master_raw_data = []
master_headers = ["Date"] + DATE_TAB_HEADERS

refresh_date_keys = set()
for day_offset in range(REFRESH_DAYS):
    target_date = TODAY - timedelta(days=day_offset)
    dk = f"{target_date.year % 100:02d}/{target_date.month:02d}/{target_date.day:02d}"
    refresh_date_keys.add(dk)

print(f"  🔄 새로 수집할 날짜: {sorted(refresh_date_keys)}")

existing_sheets = sh.worksheets()

# --- (A) 기존 탭 병렬 읽기 함수 ---
def _read_existing_tab(ws_ex):
    tn = ws_ex.title
    dk = normalize_date_key(tn)
    dt_obj = parse_date_tab(tn)
    if dt_obj is None:
        return None
    if dk in refresh_date_keys:
        return {'skipped': True, 'tab_name': tn}

    try:
        with SHEETS_SEMAPHORE:
            all_values = with_retry(ws_ex.get_all_values)
            time.sleep(0.5)
        if not all_values or len(all_values) < 2:
            return {'empty': True, 'tab_name': tn}

        data_rows = []
        empty_count = 0
        for row in all_values[1:]:
            if not row or not str(row[0]).strip():
                empty_count += 1
                if empty_count >= 2: break
            else:
                empty_count = 0; data_rows.append(row)

        result_entries = []
        for row in data_rows:
            cn = str(row[0]) if len(row) > 0 else ""
            asn = str(row[1]) if len(row) > 1 else ""
            adn = str(row[2]) if len(row) > 2 else ""
            asid = str(row[3]) if len(row) > 3 else ""
            adi = clean_id(row[4]) if len(row) > 4 else ""
            sp = _to_num(row[5]) if len(row) > 5 else 0
            cpm = _to_num(row[8]) if len(row) > 8 else 0
            uc = _to_num(row[11]) if len(row) > 11 else 0
            mpv = _to_num(row[17]) if len(row) > 17 else 0
            pf = _to_num(row[18]) if len(row) > 18 else 0
            cvr_c = _to_num(row[20]) if len(row) > 20 else 0
            if not cn or sp == 0: continue
            tab_row = list(row[:25]) if len(row) >= 25 else list(row) + [""] * (25 - len(row))
            mrd = [dk] + tab_row
            while len(mrd) < len(master_headers): mrd.append("")
            result_entries.append({
                'cn': cn, 'asn': asn, 'adn': adn, 'asid': asid, 'adi': adi,
                'sp': sp, 'cpm': cpm, 'uc': uc, 'mpv': mpv, 'pf': pf, 'cvr_c': cvr_c,
                'tab_row': tab_row,
                'mrd': mrd[:len(master_headers)],
            })
        return {'dk': dk, 'dt_obj': dt_obj, 'tab_name': tn, 'entries': result_entries}
    except Exception as e:
        return {'error': str(e), 'tab_name': tn}


# --- (B) Meta: 날짜×계정 단위 병렬 호출 ---
def _fetch_meta_one(acc_id, target_date):
    target_str = target_date.strftime('%Y-%m-%d')
    date_key = f"{target_date.year % 100:02d}/{target_date.month:02d}/{target_date.day:02d}"
    with META_SEMAPHORE:
        rows = fetch_meta_insights_daily(acc_id, target_str, active_ids=None)
    parsed = parse_single_day_insights(rows, date_key, target_date) if rows else []
    return (date_key, target_date, acc_id, parsed)


# --- (C) Mixpanel 래퍼 ---
def _fetch_mixpanel():
    mp_from = (TODAY - timedelta(days=REFRESH_DAYS-1)).strftime('%Y-%m-%d')
    mp_to = TODAY.strftime('%Y-%m-%d')
    return fetch_mixpanel_data(mp_from, mp_to)


# =========================================================
# ★★★ 동시 실행: 탭 읽기 + Meta + Mixpanel ★★★
# =========================================================
_t_start = time.time()

tabs_to_read = [ws_ex for ws_ex in existing_sheets if parse_date_tab(ws_ex.title) is not None]
meta_jobs = []
for day_offset in range(REFRESH_DAYS):
    td = TODAY - timedelta(days=day_offset)
    for acc_id in ALL_AD_ACCOUNTS:
        meta_jobs.append((acc_id, td))

print(f"\n  📋 병렬 작업 제출:")
print(f"    - 기존 탭 읽기: {len(tabs_to_read)}개")
print(f"    - Meta API 호출: {len(meta_jobs)}개 (날짜{REFRESH_DAYS} × 계정{len(ALL_AD_ACCOUNTS)})")
print(f"    - Mixpanel: 1개 (이벤트: {MIXPANEL_EVENT_NAMES})")

all_futures = {}
with ThreadPoolExecutor(max_workers=12) as executor:
    for ws_ex in tabs_to_read:
        f = executor.submit(_read_existing_tab, ws_ex)
        all_futures[f] = ('tab', ws_ex.title)

    for acc_id, td in meta_jobs:
        f = executor.submit(_fetch_meta_one, acc_id, td)
        all_futures[f] = ('meta', f"{acc_id}/{td.strftime('%m-%d')}")

    f_mp = executor.submit(_fetch_mixpanel)
    all_futures[f_mp] = ('mixpanel', 'all')

    tab_results = []
    meta_results = []
    mp_raw = []

    for future in as_completed(all_futures):
        kind, label = all_futures[future]
        try:
            result = future.result()
            if kind == 'tab':
                tab_results.append(result)
            elif kind == 'meta':
                meta_results.append(result)
            elif kind == 'mixpanel':
                mp_raw = result or []
        except Exception as e:
            print(f"  ⚠️ {kind}/{label} 오류: {e}")

_elapsed = time.time() - _t_start
print(f"\n  ⏱️ 병렬 수집 완료: {_elapsed:.1f}초")

# --- 탭 읽기 결과 통합 ---
read_count = 0
for res in tab_results:
    if res is None: continue
    if res.get('skipped'): continue
    if res.get('empty') or res.get('error'):
        if res.get('error'): print(f"  ⚠️ {res['tab_name']} 읽기 오류: {res['error']}")
        continue
    dk = res['dk']; dt_obj = res['dt_obj']
    date_objects[dk] = dt_obj
    for e in res['entries']:
        date_tab_rows[dk].append(e['tab_row'])
        k = e['adi']
        if not ad_sets[k]['ad_id']:
            ad_sets[k] = {'campaign_name': e['cn'], 'adset_name': e['asn'], 'ad_name': e['adn'],
                          'adset_id': e['asid'], 'ad_id': e['adi'], 'dates': {}}
        ad_sets[k]['dates'][dk] = {'profit': e['pf'], 'revenue': e['mpv'], 'spend': e['sp'],
                                   'cpm': e['cpm'], 'cvr': e['cvr_c'], 'unique_clicks': e['uc']}
        p = extract_product(e['cn'])
        budget_by_date[dk][p]['spend'] += e['sp']
        budget_by_date[dk][p]['revenue'] += e['mpv']
        product_count[p] += 1
        master_raw_data.append({'date': dk, 'date_obj': dt_obj, 'spend': e['sp'], 'row_data': e['mrd']})
    read_count += 1

print(f"✅ 기존 탭: {read_count}개 읽기 완료")

# --- Meta 결과 통합 ---
meta_date_data = defaultdict(list)
meta_success_count = 0
for (date_key, target_date, acc_id, parsed) in meta_results:
    if parsed:
        meta_date_data[date_key].extend(parsed)
        meta_success_count += len(parsed)
        date_objects[date_key] = target_date

print(f"✅ Meta: {len(meta_date_data)}일, 총 {meta_success_count}건")

# --- Mixpanel 결과 처리 ---
df = pd.DataFrame(mp_raw)
print(f"✅ Mixpanel: {len(df)}건")
mp_value_map = {}; mp_count_map = {}
if len(df) > 0:
    df = df[df['utm_content'].notna() & (df['utm_content'] != '') & (df['utm_content'] != 'None')]
    df = df.sort_values('revenue', ascending=False)
    df_d = df.drop_duplicates(subset=['date', 'distinct_id', '서비스'], keep='first')
    total_revenue = df_d['revenue'].sum()
    print(f"  📊 매출 합계: ₩{int(total_revenue):,}")
    for (d, ut), v in df_d.groupby(['date', 'utm_content'])['revenue'].sum().items():
        if d and ut: mp_value_map[(d, str(ut))] = v
    for (d, ut), c in df_d.groupby(['date', 'utm_content']).size().items():
        if d and ut: mp_count_map[(d, str(ut))] = c
print()


# =========================================================
# 5단계: Meta + Mixpanel 병합
# =========================================================
print("="*60)
print("5단계: Meta + Mixpanel 병합")
print("="*60)

new_date_names = []
for dk in sorted(meta_date_data.keys(),
    key=lambda x: meta_date_data[x][0]['date_obj'] if meta_date_data[x] else datetime.min, reverse=True):
    rows = meta_date_data[dk]
    if not rows: continue
    dt = rows[0]['date_obj']; date_objects[dk] = dt; new_date_names.append(dk)

    for asi in list(ad_sets.keys()):
        if dk in ad_sets[asi]['dates']: del ad_sets[asi]['dates'][dk]
    budget_by_date[dk] = defaultdict(lambda: {'spend': 0.0, 'revenue': 0.0})
    date_tab_rows[dk] = []
    master_raw_data = [m for m in master_raw_data if m['date'] != dk]

    for mr in rows:
        adi = mr['ad_id']; cn = mr['campaign_name']; asn = mr['adset_name']
        adn = mr['ad_name']; asid = mr['adset_id']
        sp = mr['spend']; cm = mr['cpm']; uc = mr['unique_clicks']
        mpk = (dk, adi); mpv = mp_value_map.get(mpk, 0); mpc = mp_count_map.get(mpk, 0)
        rv = mpv; pf = rv - sp
        roas_c = (rv / sp * 100) if sp > 0 else 0
        cvr_c = (mpc / uc * 100) if uc > 0 else 0
        tab_row = [cn, asn, adn, asid, adi, sp, mr['cost_per_result'], mr['meta_roas'],
            cm, mr['reach'], mr['impressions'], uc, mr['unique_ctr'],
            mr['cost_per_unique_click'], mr['frequency'], mr['results'],
            mpc, mpv, round(pf, 0), round(roas_c, 1) if sp > 0 else 0,
            round(cvr_c, 2) if uc > 0 else 0, "", "", "", ""]
        date_tab_rows[dk].append(tab_row)
        k = adi
        if not ad_sets[k]['ad_id']:
            ad_sets[k] = {'campaign_name': cn, 'adset_name': asn, 'ad_name': adn, 'adset_id': asid, 'ad_id': adi, 'dates': {}}
        ad_sets[k]['dates'][dk] = {'profit': pf, 'revenue': rv, 'spend': sp, 'cpm': cm, 'cvr': cvr_c, 'unique_clicks': uc}
        p = extract_product(cn)
        budget_by_date[dk][p]['spend'] += sp; budget_by_date[dk][p]['revenue'] += rv; product_count[p] += 1
        mrd = [dk] + tab_row
        while len(mrd) < len(master_headers): mrd.append("")
        master_raw_data.append({'date': dk, 'date_obj': dt, 'spend': sp, 'row_data': mrd[:len(master_headers)]})

all_dk = sorted(set(date_objects.keys()), key=lambda x: date_objects[x])
date_names = all_dk
new_date_names.sort(key=lambda x: date_objects[x])

print(f"✅ 전체 광고: {len(ad_sets)}개, 전체 날짜: {len(date_names)}개")
print(f"📅 새로 생성할 탭: {new_date_names}")
print()


# =========================================================
# 5.5: 주간 집계 + 정렬
# =========================================================
print("5.5~6: 주간 집계 + 정렬")

week_groups = defaultdict(list); week_display_names = {}
for t in date_names:
    do = date_objects[t]; wk = get_week_range_short(do); wd = get_week_range(do)
    week_groups[wk].append(t); week_display_names[wk] = wd
week_ranges = []
for wr in week_groups:
    sd = week_groups[wr][0]; sdo = date_objects[sd]; m = get_week_monday(sdo)
    week_ranges.append((wr, m))
week_ranges.sort(key=lambda x: x[1]); week_keys = [wr[0] for wr in week_ranges]

ad_sets_weekly = defaultdict(lambda: {'campaign_name':'','ad_name':'','ad_id':'','weeks':{}})
for asi, d in ad_sets.items():
    ad_sets_weekly[asi]['campaign_name'] = d['campaign_name']
    ad_sets_weekly[asi]['ad_name'] = d['ad_name']
    ad_sets_weekly[asi]['ad_id'] = d['ad_id']
    for wk in week_keys:
        wp,wr_v,ws_v,wcs,wcc,wvs,wvc = 0,0,0,0,0,0,0
        for dn in week_groups[wk]:
            if dn in d['dates']:
                wp+=d['dates'][dn]['profit']; wr_v+=d['dates'][dn]['revenue']; ws_v+=d['dates'][dn]['spend']
                cv=d['dates'][dn].get('cpm',0)
                if cv>0: wcs+=cv; wcc+=1
                vv=d['dates'][dn].get('cvr',0)
                if vv>0: wvs+=vv; wvc+=1
        if ws_v > 0 or wr_v > 0:
            ad_sets_weekly[asi]['weeks'][wk] = {'profit':wp,'revenue':wr_v,'spend':ws_v,
                'cpm':(wcs/wcc) if wcc>0 else 0,'cvr':(wvs/wvc) if wvc>0 else 0}

latest_date = date_names[-1] if date_names else ""
sorted_list = []
for asi, d in ad_sets.items():
    sp = d['dates'].get(latest_date, {}).get('spend', 0)
    sorted_list.append({'campaign_name':d['campaign_name'],'ad_name':d['ad_name'],
                        'ad_id':d['ad_id'],'data':d,'spend':sp})
sorted_list.sort(key=lambda x: x['spend'], reverse=True)

product_spend = {p: budget_by_date[latest_date][p]['spend'] for p in PRODUCT_KEYWORDS + ["기타"]}
product_order = sorted(PRODUCT_KEYWORDS + ["기타"], key=lambda p: product_spend.get(p,0), reverse=True)
if "기타" in product_order and product_spend.get("기타",0) == 0: product_order.remove("기타")

chart_dn = list(reversed(date_names)); chart_wk = list(reversed(week_keys)); chart_sd = chart_dn[:7]
print(f"✅ 주차: {len(week_keys)}개, 제품 순서: {product_order}\n")


# =========================================================
# 7: 분석 시트 + 최근 7일 탭 삭제
# =========================================================
print("="*60)
print("7: 분석 시트 + 최근 7일 탭 삭제")
print("="*60)
analysis_sheets = ["마스터탭", "추이차트", "증감액", "추이차트(주간)", "주간종합", "주간종합_2", "주간종합_3", "예산", "_temp", "_temp_holder"]
all_to_delete = analysis_sheets + new_date_names

# 1차 삭제 (with_retry 적용)
for sn in all_to_delete:
    try:
        old = sh.worksheet(sn)
        if len(sh.worksheets()) <= 1:
            with_retry(sh.add_worksheet, title="_tmp", rows=1, cols=1); time.sleep(1)
        with_retry(sh.del_worksheet, old)
        print(f"  ✅ '{sn}' 삭제"); time.sleep(1.5)
    except gspread.exceptions.WorksheetNotFound: pass
    except Exception as e:
        print(f"  ⚠️ '{sn}' 1차 삭제 실패: {e}"); time.sleep(5)

# 2차 검증 — 삭제 안 된 탭 재시도
print("  🔄 삭제 검증 중...")
time.sleep(5)
for sn in all_to_delete:
    try:
        old = sh.worksheet(sn)
        print(f"  🔁 '{sn}' 아직 존재 — 재삭제 시도")
        time.sleep(3)
        with_retry(sh.del_worksheet, old)
        print(f"  ✅ '{sn}' 재삭제 완료"); time.sleep(2)
    except gspread.exceptions.WorksheetNotFound:
        pass
    except Exception as e:
        print(f"  ⚠️ '{sn}' 재삭제 실패: {e}"); time.sleep(5)

print("⏳ 5초 대기..."); time.sleep(5)


# =========================================================
# ★★★ 8~17: 데이터 병렬 준비 + 병렬 쓰기 ★★★
# =========================================================
print("\n" + "="*60)
print("8~17: ★ 데이터 병렬 준비 → ★병렬 쓰기★")
print("="*60)

# --- (A) 날짜 탭 데이터 병렬 준비 ---
def _prepare_date_tab(dk):
    rows = date_tab_rows.get(dk, [])
    if not rows: return None
    rows.sort(key=lambda r: _to_num(r[18]) if len(r) > 18 else 0, reverse=True)
    summary_rows = generate_date_tab_summary(rows)
    all_data = [DATE_TAB_HEADERS] + rows + summary_rows
    return {'dk': dk, 'rows': rows, 'summary_rows': summary_rows, 'all_data': all_data}


# --- (B) 추이차트 ---
def _prepare_trend_daily():
    dhw = []; sci = []
    for i, n in enumerate(chart_dn):
        do = date_objects[n]; wd = WEEKDAY_NAMES[do.weekday()]
        dhw.append(f"{n}({wd})")
        if do.weekday() == 6: sci.append(4+i)
    hdr_t = ['캠페인 이름','광고 이름','광고 ID','7일 평균'] + dhw

    sr = ["종합","",""]
    tp,tr,ts,tcs,tcc,tvs,tvc = 0,0,0,0,0,0,0
    for d in chart_sd:
        for it in sorted_list:
            if d in it['data']['dates']:
                tp+=it['data']['dates'][d]['profit']; tr+=it['data']['dates'][d]['revenue']; ts+=it['data']['dates'][d]['spend']
                cv=it['data']['dates'][d].get('cpm',0)
                if cv>0: tcs+=cv; tcc+=1
                vv=it['data']['dates'][d].get('cvr',0)
                if vv>0: tvs+=vv; tvc+=1
    sr.append(cell_text(tp,tr,ts,(tcs/tcc) if tcc>0 else 0,(tvs/tvc) if tvc>0 else 0))
    for d in chart_dn:
        dp,dr,ds_v,dcs,dcc,dvs,dvc = 0,0,0,0,0,0,0
        for it in sorted_list:
            if d in it['data']['dates']:
                dp+=it['data']['dates'][d]['profit']; dr+=it['data']['dates'][d]['revenue']; ds_v+=it['data']['dates'][d]['spend']
                cv=it['data']['dates'][d].get('cpm',0)
                if cv>0: dcs+=cv; dcc+=1
                vv=it['data']['dates'][d].get('cvr',0)
                if vv>0: dvs+=vv; dvc+=1
        sr.append(cell_text(dp,dr,ds_v,(dcs/dcc) if dcc>0 else 0,(dvs/dvc) if dvc>0 else 0))

    rt = []
    for it in sorted_list:
        r = [it['campaign_name'],it['ad_name'],it['ad_id']]
        tp,tr,ts,tcs,tcc,tvs,tvc = 0,0,0,0,0,0,0
        for d in chart_sd:
            if d in it['data']['dates']:
                tp+=it['data']['dates'][d]['profit']; tr+=it['data']['dates'][d]['revenue']; ts+=it['data']['dates'][d]['spend']
                cv=it['data']['dates'][d].get('cpm',0)
                if cv>0: tcs+=cv; tcc+=1
                vv=it['data']['dates'][d].get('cvr',0)
                if vv>0: tvs+=vv; tvc+=1
        r.append(cell_text(tp,tr,ts,(tcs/tcc) if tcc>0 else 0,(tvs/tvc) if tvc>0 else 0))
        for d in chart_dn:
            if d in it['data']['dates']:
                dt=it['data']['dates'][d]
                r.append(cell_text(dt['profit'],dt['revenue'],dt['spend'],dt.get('cpm',0),dt.get('cvr',0)))
            else: r.append('')
        rt.append(r)
    return {'hdr': hdr_t, 'summary_row': sr, 'rows': rt, 'sci': sci}


# --- (C) 추이차트(주간) ---
def _prepare_trend_weekly():
    wdl = [week_display_names[wk] for wk in chart_wk]
    hdr_w = ['캠페인 이름','광고 이름','광고 ID','전체 평균'] + wdl

    srw = ["종합","",""]
    tap,tar,tas,tacs,tacc,tavs,tavc = 0,0,0,0,0,0,0
    for wk in chart_wk:
        for it in sorted_list:
            wd = ad_sets_weekly[it['ad_id']]['weeks'].get(wk,{})
            if wd:
                tap+=wd['profit']; tar+=wd['revenue']; tas+=wd['spend']
                cv=wd.get('cpm',0)
                if cv>0: tacs+=cv; tacc+=1
                vv=wd.get('cvr',0)
                if vv>0: tavs+=vv; tavc+=1
    srw.append(cell_text(tap,tar,tas,(tacs/tacc) if tacc>0 else 0,(tavs/tavc) if tavc>0 else 0))
    for wk in chart_wk:
        wp,wr_v,ws_v,wcs,wcc,wvs,wvc = 0,0,0,0,0,0,0
        for it in sorted_list:
            wd = ad_sets_weekly[it['ad_id']]['weeks'].get(wk,{})
            if wd:
                wp+=wd['profit']; wr_v+=wd['revenue']; ws_v+=wd['spend']
                cv=wd.get('cpm',0)
                if cv>0: wcs+=cv; wcc+=1
                vv=wd.get('cvr',0)
                if vv>0: wvs+=vv; wvc+=1
        srw.append(cell_text(wp,wr_v,ws_v,(wcs/wcc) if wcc>0 else 0,(wvs/wvc) if wvc>0 else 0))

    rtw = []
    for it in sorted_list:
        r = [it['campaign_name'],it['ad_name'],it['ad_id']]
        tp,tr,ts,tcs,tcc,tvs,tvc = 0,0,0,0,0,0,0
        for wk in chart_wk:
            wd = ad_sets_weekly[it['ad_id']]['weeks'].get(wk,{})
            if wd:
                tp+=wd['profit']; tr+=wd['revenue']; ts+=wd['spend']
                cv=wd.get('cpm',0)
                if cv>0: tcs+=cv; tcc+=1
                vv=wd.get('cvr',0)
                if vv>0: tvs+=vv; tvc+=1
        r.append(cell_text(tp,tr,ts,(tcs/tcc) if tcc>0 else 0,(tvs/tvc) if tvc>0 else 0))
        for wk in chart_wk:
            wd = ad_sets_weekly[it['ad_id']]['weeks'].get(wk,{})
            r.append(cell_text(wd['profit'],wd['revenue'],wd['spend'],wd.get('cpm',0),wd.get('cvr',0)) if wd else '')
        rtw.append(r)
    return {'hdr': hdr_w, 'summary_row': srw, 'rows': rtw}


# --- (D) 증감액 ---
def _prepare_change():
    hdr_c = ['캠페인 이름','광고 이름','광고 ID','7일 평균'] + chart_dn
    src = ["종합","",""]
    t7r,t7s,t7cs,t7cc,t7vs,t7vc = 0,0,0,0,0,0
    for d in chart_sd:
        for it in sorted_list:
            if d in it['data']['dates']:
                t7r+=it['data']['dates'][d]['revenue']; t7s+=it['data']['dates'][d]['spend']
                cv=it['data']['dates'][d].get('cpm',0)
                if cv>0: t7cs+=cv; t7cc+=1
                vv=it['data']['dates'][d].get('cvr',0)
                if vv>0: t7vs+=vv; t7vc+=1
    t7roas = (t7r/t7s*100) if t7s>0 else 0
    fds = sum(it['data']['dates'].get(chart_sd[-1],{}).get('spend',0) for it in sorted_list) if len(chart_sd)>=2 else 0
    lds = sum(it['data']['dates'].get(chart_sd[0],{}).get('spend',0) for it in sorted_list) if len(chart_sd)>=2 else 0
    sdc = ((lds-fds)/fds*100) if fds>0 else 0
    src.append(cell_text_change(t7roas,sdc,t7s,(t7cs/t7cc) if t7cc>0 else 0,(t7vs/t7vc) if t7vc>0 else 0))
    for i,d in enumerate(chart_dn):
        dr,ds_v,dcs,dcc,dvs,dvc = 0,0,0,0,0,0
        for it in sorted_list:
            if d in it['data']['dates']:
                dr+=it['data']['dates'][d]['revenue']; ds_v+=it['data']['dates'][d]['spend']
                cv=it['data']['dates'][d].get('cpm',0)
                if cv>0: dcs+=cv; dcc+=1
                vv=it['data']['dates'][d].get('cvr',0)
                if vv>0: dvs+=vv; dvc+=1
        d_roas = (dr/ds_v*100) if ds_v>0 else 0
        if i < len(chart_dn)-1:
            pd_name = chart_dn[i+1]; ps = sum(it['data']['dates'].get(pd_name,{}).get('spend',0) for it in sorted_list)
            chg = ((ds_v-ps)/ps*100) if ps>0 else 0
        else: chg = 0
        src.append(cell_text_change(d_roas,chg,ds_v,(dcs/dcc) if dcc>0 else 0,(dvs/dvc) if dvc>0 else 0))

    rtc = []
    for it in sorted_list:
        r = [it['campaign_name'],it['ad_name'],it['ad_id']]
        tr_v,ts_v,tcpm,tcpc,tcvr,tcvc = 0,0,0,0,0,0
        for d in chart_sd:
            if d in it['data']['dates']:
                tr_v+=it['data']['dates'][d]['revenue']; ts_v+=it['data']['dates'][d]['spend']
                cv=it['data']['dates'][d].get('cpm',0)
                if cv>0: tcpm+=cv; tcpc+=1
                vv=it['data']['dates'][d].get('cvr',0)
                if vv>0: tcvr+=vv; tcvc+=1
        ar = (tr_v/ts_v*100) if ts_v>0 else 0
        fs = it['data']['dates'].get(chart_sd[-1],{}).get('spend',0) if len(chart_sd)>=2 else 0
        ls = it['data']['dates'].get(chart_sd[0],{}).get('spend',0) if len(chart_sd)>=2 else 0
        cp = ((ls-fs)/fs*100) if fs>0 else 0
        r.append(cell_text_change(ar,cp,ts_v,(tcpm/tcpc) if tcpc>0 else 0,(tcvr/tcvc) if tcvc>0 else 0))
        for i,d in enumerate(chart_dn):
            if d in it['data']['dates']:
                dt=it['data']['dates'][d]; roas=(dt['revenue']/dt['spend']*100) if dt['spend']>0 else 0
                if i<len(chart_dn)-1:
                    pd_name=chart_dn[i+1]; ps=it['data']['dates'].get(pd_name,{}).get('spend',0)
                    chg=((dt['spend']-ps)/ps*100) if ps>0 else 0
                else: chg=0
                r.append(cell_text_change(roas,chg,dt['spend'],dt.get('cpm',0),dt.get('cvr',0)))
            else: r.append('')
        rtc.append(r)
    return {'hdr': hdr_c, 'summary_row': src, 'rows': rtc}


# --- (E) 예산 ---
def _prepare_budget():
    br = [[""] + chart_dn]
    br.append(["전체 쓴돈"] + [sum(budget_by_date[d][p]['spend'] for p in product_order) for d in chart_dn])
    br.append(["전체 번돈"] + [sum(budget_by_date[d][p]['revenue'] for p in product_order) for d in chart_dn])
    br.append(["전체 순이익"] + [sum(budget_by_date[d][p]['revenue'] for p in product_order) - sum(budget_by_date[d][p]['spend'] for p in product_order) for d in chart_dn])
    br.append(["ROAS"] + [(sum(budget_by_date[d][p]['revenue'] for p in product_order)/sum(budget_by_date[d][p]['spend'] for p in product_order)*100) if sum(budget_by_date[d][p]['spend'] for p in product_order)>0 else 0 for d in chart_dn])
    br.append([""]*(len(chart_dn)+1))
    br.append(["쓴돈 - 제품별"]+[""]*len(chart_dn))
    for p in product_order: br.append([p]+[budget_by_date[d][p]['spend'] for d in chart_dn])
    br.append([""]*(len(chart_dn)+1))
    br.append(["번돈 - 제품별"]+[""]*len(chart_dn))
    for p in product_order: br.append([p]+[budget_by_date[d][p]['revenue'] for d in chart_dn])
    br.append([""]*(len(chart_dn)+1))
    br.append(["순이익 - 제품별"]+[""]*len(chart_dn))
    for p in product_order: br.append([p]+[budget_by_date[d][p]['revenue']-budget_by_date[d][p]['spend'] for d in chart_dn])
    return br


# --- (F) 마스터탭 ---
def _prepare_master():
    master_raw_data.sort(key=lambda x: (x['date_obj'], -x['spend']), reverse=True)
    for item in master_raw_data:
        while len(item['row_data']) < len(master_headers): item['row_data'].append("")
        item['row_data'] = item['row_data'][:len(master_headers)]
    return [master_headers] + [i['row_data'] for i in master_raw_data]


# --- (G) 주간종합 1/2/3 ---
products = ["솔로","재물","재회","29금궁합","자녀","29금","1%","별해"]
month_groups = defaultdict(list)
for t in date_names:
    do = date_objects[t]; mk = f"{do.year}년 {do.month}월"; month_groups[mk].append(t)
month_names_list = sorted(month_groups.keys(), key=lambda x: (int(x.split('년')[0]), int(x.split('년')[1].replace('월','').strip())), reverse=True)

daily_data = defaultdict(lambda: {'spend':0,'revenue':0,'profit':0})
daily_product_data = defaultdict(lambda: defaultdict(lambda: {'spend':0,'revenue':0,'profit':0}))
for t in date_names:
    for it in sorted_list:
        if t in it['data']['dates']:
            dt=it['data']['dates'][t]
            daily_data[t]['spend']+=dt['spend']; daily_data[t]['revenue']+=dt['revenue']; daily_data[t]['profit']+=dt['profit']
            p=extract_product(it['campaign_name'])
            daily_product_data[t][p]['spend']+=dt['spend']; daily_product_data[t][p]['revenue']+=dt['revenue']; daily_product_data[t][p]['profit']+=dt['profit']

wsd = {}; wps = {}
for wk in week_keys:
    wsd[wk] = {'spend':0,'revenue':0,'profit':0}; wps[wk] = defaultdict(lambda: {'spend':0,'revenue':0,'profit':0})
    for dn in week_groups[wk]:
        wsd[wk]['spend']+=daily_data[dn]['spend']; wsd[wk]['revenue']+=daily_data[dn]['revenue']; wsd[wk]['profit']+=daily_data[dn]['profit']
        for p in PRODUCT_KEYWORDS+["기타"]:
            wps[wk][p]['spend']+=daily_product_data[dn][p]['spend']; wps[wk][p]['revenue']+=daily_product_data[dn][p]['revenue']; wps[wk][p]['profit']+=daily_product_data[dn][p]['profit']

msd = {}; mps = {}
for mk in month_names_list:
    msd[mk] = {'spend':0,'revenue':0,'profit':0}; mps[mk] = defaultdict(lambda: {'spend':0,'revenue':0,'profit':0})
    for dn in month_groups[mk]:
        msd[mk]['spend']+=daily_data[dn]['spend']; msd[mk]['revenue']+=daily_data[dn]['revenue']; msd[mk]['profit']+=daily_data[dn]['profit']
        for p in PRODUCT_KEYWORDS+["기타"]:
            mps[mk][p]['spend']+=daily_product_data[dn][p]['spend']; mps[mk][p]['revenue']+=daily_product_data[dn][p]['revenue']; mps[mk][p]['profit']+=daily_product_data[dn][p]['profit']


def _prepare_weekly_summary_1():
    def ccb_data(pn, d, pd_map, im=False):
        rv = (d['revenue']/d['spend']) if d['spend']>0 else 0
        nc = len(products)+2
        block = [[pn]+[""]*(nc-1)]
        block.append(["지출 금액","구매(메타)","구매(믹스패널)","매출","이익","ROAS","CVR","전체"]+[""]*(nc-8))
        block.append([d['spend'],0,0,d['revenue'],d['profit'],rv,0,""]+[""]*(nc-8))
        block.append([""]+products+["합"])
        for lb, dk, ft, fp in [("제품별 ROAS","roas","PERCENT","0.00%"),("제품별 순이익","profit","NUMBER","#,##0"),
            ("제품별 매출","revenue","NUMBER","#,##0"),("제품별 예산","spend","NUMBER","#,##0"),("제품별 예산 비중","ratio","PERCENT","0.00%")]:
            r = [lb]; tsp = sum(pd_map[p]['spend'] for p in products); trv = sum(pd_map[p]['revenue'] for p in products)
            for p in products:
                if dk=="roas": r.append((pd_map[p]['revenue']/pd_map[p]['spend']) if pd_map[p]['spend']>0 else 0)
                elif dk=="ratio": r.append((pd_map[p]['spend']/tsp) if tsp>0 else 0)
                else: r.append(pd_map[p][dk])
            if dk=="roas": r.append((trv/tsp) if tsp>0 else 0)
            elif dk=="ratio": r.append(1.0)
            else: r.append(sum(pd_map[p][dk] for p in products))
            block.append(r)
        block.append([""]*nc)
        return block, im

    result_blocks = []
    for mk in month_names_list:
        yr=int(mk.split('년')[0]); mn=int(mk.split('년')[1].replace('월','').strip())
        dim=sorted(month_groups[mk],key=lambda x:date_objects[x])
        fd=date_objects[dim[0]]; ld=date_objects[dim[-1]]
        mr_d=get_month_range_display(fd,ld).replace("'","")
        result_blocks.append(ccb_data(f"'{mk} ({mr_d})", msd[mk], mps[mk], im=True))
        mw=[wk for wk in week_keys if any(date_objects[d].year==yr and date_objects[d].month==mn for d in week_groups[wk])]
        mw.reverse()
        for wk in mw:
            result_blocks.append(ccb_data(week_display_names[wk], wsd[wk], wps[wk], im=False))
    return result_blocks


def _prepare_weekly_summary_2():
    stl = []
    for mk in month_names_list:
        d = msd[mk]; roas = (d['revenue']/d['spend']) if d['spend']>0 else 0
        stl.append({'period':f"'{mk}",'type':'월별','spend':d['spend'],'revenue':d['revenue'],'profit':d['profit'],'roas':roas,'im':True,'mk':mk})
        yr=int(mk.split('년')[0]); mn=int(mk.split('년')[1].replace('월','').strip())
        mw=[wk for wk in week_keys if any(date_objects[dn].year==yr and date_objects[dn].month==mn for dn in week_groups[wk])]
        mw.reverse()
        for wk in mw:
            wd=wsd[wk]; wr=(wd['revenue']/wd['spend']) if wd['spend']>0 else 0
            stl.append({'period':week_display_names[wk],'type':'주간','spend':wd['spend'],'revenue':wd['revenue'],'profit':wd['profit'],'roas':wr,'im':False,'mk':None,'wk':wk})
    return stl


def _prepare_weekly_summary_3():
    dsr = []
    for t in reversed(date_names):
        do=date_objects[t]; d=daily_data[t]; roas=(d['revenue']/d['spend']) if d['spend']>0 else 0
        wd=WEEKDAY_NAMES[do.weekday()]
        dsr.append({'period':f"'{do.month}.{do.day}({wd})",'weekday':wd,'spend':d['spend'],'revenue':d['revenue'],'profit':d['profit'],'roas':roas,'tab_name':t})
    return dsr


# =========================================================
# ★★★ 병렬 데이터 준비 ★★★
# =========================================================
print("\n🔄 모든 탭 데이터 병렬 준비 중...")
_t_prep = time.time()

with ThreadPoolExecutor(max_workers=10) as executor:
    date_tab_futures = {executor.submit(_prepare_date_tab, dk): dk for dk in new_date_names}
    f_trend_daily = executor.submit(_prepare_trend_daily)
    f_trend_weekly = executor.submit(_prepare_trend_weekly)
    f_change = executor.submit(_prepare_change)
    f_budget = executor.submit(_prepare_budget)
    f_master = executor.submit(_prepare_master)
    f_ws1 = executor.submit(_prepare_weekly_summary_1)
    f_ws2 = executor.submit(_prepare_weekly_summary_2)
    f_ws3 = executor.submit(_prepare_weekly_summary_3)

    prepared_date_tabs = {}
    for future in as_completed(date_tab_futures):
        dk = date_tab_futures[future]
        result = future.result()
        if result: prepared_date_tabs[dk] = result

    trend_daily_data = f_trend_daily.result()
    trend_weekly_data = f_trend_weekly.result()
    change_data = f_change.result()
    budget_data = f_budget.result()
    master_data = f_master.result()
    ws1_blocks = f_ws1.result()
    ws2_stl = f_ws2.result()
    ws3_dsr = f_ws3.result()

print(f"  ⏱️ 데이터 준비 완료: {time.time()-_t_prep:.1f}초\n")


# =========================================================
# ★★★ 워크시트 일괄 생성 (순차 — 빠름) ★★★
# =========================================================
print("📝 워크시트 일괄 생성...")
_t_create = time.time()

ws_registry = {}  # name → worksheet object

def safe_add_worksheet(title, rows, cols):
    """이미 존재하면 삭제 후 재생성"""
    for attempt in range(3):
        try:
            ws = with_retry(sh.add_worksheet, title=title, rows=rows, cols=cols)
            time.sleep(0.8)
            return ws
        except APIError as e:
            if "already exists" in str(e):
                print(f"  🔁 '{title}' 이미 존재 — 삭제 후 재생성")
                try:
                    old = sh.worksheet(title)
                    with_retry(sh.del_worksheet, old)
                    time.sleep(2)
                except: time.sleep(3)
            elif "429" in str(e) or "Quota" in str(e):
                wait = 20 + attempt * 15
                print(f"  ⏳ 워크시트 생성 rate limit, {wait}초 대기")
                time.sleep(wait)
            else:
                raise
    # 최종 시도
    return with_retry(sh.add_worksheet, title=title, rows=rows, cols=cols)

# 날짜 탭 생성
for dk in new_date_names:
    prep = prepared_date_tabs.get(dk)
    if not prep: continue
    total_rows = len(prep['rows']) + len(prep['summary_rows']) + 5
    NUM_COLS = len(DATE_TAB_HEADERS)
    ws_d = safe_add_worksheet(dk, total_rows, NUM_COLS + 2)
    ws_registry[dk] = ws_d

# 분석 탭 생성
ws_registry['마스터탭'] = safe_add_worksheet("마스터탭", max(2000, len(master_data)+100), len(master_headers)+5)
ws_registry['추이차트'] = safe_add_worksheet("추이차트", 1000, 100)
ws_registry['추이차트(주간)'] = safe_add_worksheet("추이차트(주간)", 1000, 100)
ws_registry['증감액'] = safe_add_worksheet("증감액", 1000, 100)
ws_registry['예산'] = safe_add_worksheet("예산", 1000, 100)
ws_registry['주간종합'] = safe_add_worksheet("주간종합", 2000, 20)
ws_registry['주간종합_2'] = safe_add_worksheet("주간종합_2", 2000, 20)
ws_registry['주간종합_3'] = safe_add_worksheet("주간종합_3", 3000, 20)

print(f"  ⏱️ 워크시트 생성: {time.time()-_t_create:.1f}초 ({len(ws_registry)}개)\n")


# =========================================================
# ★★★ 병렬 쓰기 함수 정의 ★★★
# =========================================================

def _write_date_tab(dk):
    """날짜 탭 1개: 데이터 쓰기 + 서식"""
    prep = prepared_date_tabs.get(dk)
    if not prep: return f"{dk}: skip"
    ws_d = ws_registry.get(dk)
    if not ws_d: return f"{dk}: no ws"
    rows = prep['rows']; summary_rows = prep['summary_rows']; all_data = prep['all_data']
    sid_d = ws_d.id; data_end = len(rows) + 1
    NUM_COLS = len(DATE_TAB_HEADERS)

    with WRITE_SEMAPHORE:
        with_retry(ws_d.update, values=all_data, range_name="A1", value_input_option="USER_ENTERED")
        time.sleep(1)

    fmt_all = []
    fmt_all.append(create_format_request(sid_d, 0, 1, 0, 5, {"textFormat": {"bold": True}, "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE"}))
    fmt_all.append(create_format_request(sid_d, 0, 1, 5, 16, {"backgroundColor": {"red":0.937,"green":0.937,"blue":0.937}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE"}))
    fmt_all.append(create_format_request(sid_d, 0, 1, 16, 21, {"backgroundColor": {"red":0.6,"green":0.6,"blue":0.6}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP", "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}))
    fmt_all.append(create_format_request(sid_d, 0, 1, 21, 22, {"backgroundColor": {"red":0.851,"green":0.824,"blue":0.914}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP", "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}))
    fmt_all.append(create_format_request(sid_d, 0, 1, 22, 23, {"backgroundColor": {"red":0.706,"green":0.655,"blue":0.839}, "textFormat": {"bold": True, "foregroundColor": {"red":1,"green":1,"blue":1}}, "wrapStrategy": "WRAP", "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}))
    fmt_all.append(create_format_request(sid_d, 0, 1, 23, 24, {"backgroundColor": {"red":0.6,"green":0,"blue":1}, "textFormat": {"bold": True, "foregroundColor": {"red":1,"green":1,"blue":1}}, "wrapStrategy": "WRAP", "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}))
    fmt_all.append(create_format_request(sid_d, 0, 1, 24, 25, {"backgroundColor": {"red":1,"green":0.6,"blue":0}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE"}))
    fmt_all.append(create_number_format_request(sid_d, 1, data_end, 16, 17, "NUMBER", "#,##0"))
    fmt_all.append(create_number_format_request(sid_d, 1, data_end, 17, 18, "NUMBER", "#,##0"))
    fmt_all.append(create_number_format_request(sid_d, 1, data_end, 18, 19, "NUMBER", "#,##0"))
    fmt_all.append(create_number_format_request(sid_d, 1, data_end, 19, 20, "NUMBER", "#,##0"))
    fmt_all.append(create_number_format_request(sid_d, 1, data_end, 20, 21, "NUMBER", "#,##0.00"))
    fmt_all.append(create_number_format_request(sid_d, 0, 1, 22, 23, "NUMBER", "0%"))
    fmt_all.append(create_number_format_request(sid_d, 1, data_end, 22, 23, "NUMBER", "0%"))
    fmt_all.append(create_number_format_request(sid_d, 1, data_end, 23, 24, "NUMBER", "0.0"))
    col_widths = [(0,210),(1,207),(2,207),(3,96),(4,96)]
    for ci, w in col_widths:
        fmt_all.append({"updateDimensionProperties": {"range": {"sheetId": sid_d, "dimension": "COLUMNS", "startIndex": ci, "endIndex": ci+1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}})
    fmt_all.append({"setBasicFilter": {"filter": {"range": {"sheetId": sid_d, "startRowIndex": 0, "endRowIndex": data_end, "startColumnIndex": 0, "endColumnIndex": NUM_COLS}}}})
    fmt_all.append({"updateSheetProperties": {"properties": {"sheetId": sid_d, "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 5}}, "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}})

    profit_cond = [
        ("NUMBER_GREATER","0",{"red":1,"green":0.949,"blue":0.8},None),
        ("NUMBER_GREATER","50000",{"red":1,"green":0.898,"blue":0.6},None),
        ("NUMBER_GREATER","100000",{"red":0.576,"green":0.769,"blue":0.49},None),
        ("NUMBER_GREATER","200000",{"red":0,"green":1,"blue":0},None),
        ("NUMBER_GREATER","300000",{"red":0,"green":1,"blue":1},None),
        ("NUMBER_LESS","-10000",{"red":0.957,"green":0.8,"blue":0.8},None),
        ("NUMBER_LESS","-50000",{"red":0.878,"green":0.4,"blue":0.4},{"red":0,"green":0,"blue":0}),
    ]
    for ctype, val, bg, fc in profit_cond:
        rule_fmt = {"backgroundColor": bg}
        if fc: rule_fmt["textFormat"] = {"foregroundColor": fc}
        fmt_all.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sid_d, "startRowIndex": 1, "endRowIndex": data_end, "startColumnIndex": 18, "endColumnIndex": 19}], "booleanRule": {"condition": {"type": ctype, "values": [{"userEnteredValue": val}]}, "format": rule_fmt}}, "index": 0}})
    fmt_all.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sid_d, "startRowIndex": 1, "endRowIndex": data_end, "startColumnIndex": 18, "endColumnIndex": 19}], "gradientRule": {"minpoint": {"color": {"red":0.902,"green":0.486,"blue":0.451}, "type": "MIN"}, "midpoint": {"color": {"red":1,"green":1,"blue":1}, "type": "NUMBER", "value": "0"}, "maxpoint": {"color": {"red":0.341,"green":0.733,"blue":0.541}, "type": "MAX"}}}, "index": 0}})
    roas_cond = [("NUMBER_LESS","100",{"red":0.878,"green":0.4,"blue":0.4}),("NUMBER_GREATER_THAN_EQ","100",{"red":1,"green":0.851,"blue":0.4}),("NUMBER_GREATER_THAN_EQ","200",{"red":0.576,"green":0.769,"blue":0.49}),("NUMBER_GREATER_THAN_EQ","300",{"red":0,"green":1,"blue":1})]
    for ctype, val, bg in roas_cond:
        fmt_all.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sid_d, "startRowIndex": 1, "endRowIndex": data_end, "startColumnIndex": 19, "endColumnIndex": 20}], "booleanRule": {"condition": {"type": ctype, "values": [{"userEnteredValue": val}]}, "format": {"backgroundColor": bg}}}, "index": 0}})
    fmt_all.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sid_d, "startRowIndex": 1, "endRowIndex": data_end, "startColumnIndex": 20, "endColumnIndex": 21}], "gradientRule": {"minpoint": {"color": {"red":1,"green":1,"blue":1}, "type": "MIN"}, "maxpoint": {"color": {"red":0.341,"green":0.733,"blue":0.541}, "type": "MAX"}}}, "index": 0}})

    with FORMAT_SEMAPHORE:
        try:
            with_retry(sh.batch_update, body={"requests": fmt_all}); time.sleep(2)
        except Exception as e: print(f"    ⚠️ {dk} 서식 오류: {e}")
        try:
            fmt_reqs = format_date_tab_summary(sh, ws_d, len(rows), len(summary_rows))
            if fmt_reqs:
                for i in range(0, len(fmt_reqs), 50):
                    with_retry(sh.batch_update, body={"requests": fmt_reqs[i:i+50]}); time.sleep(1)
        except Exception as e: print(f"    ⚠️ {dk} 요약표 서식 오류: {e}")

    return f"{dk}: ✅ ({len(rows)}개 광고)"


def _write_master():
    """마스터탭 쓰기"""
    ws_m = ws_registry['마스터탭']
    with WRITE_SEMAPHORE:
        for i in range(0, len(master_data), 5000):
            with_retry(ws_m.update, values=master_data[i:i+5000], range_name=f"A{i+1}", value_input_option="USER_ENTERED"); time.sleep(2)
    with FORMAT_SEMAPHORE:
        try:
            with_retry(ws_m.format, 'A1:T1', {'backgroundColor':{'red':0.9,'green':0.9,'blue':0.9},'textFormat':{'bold':True}})
            with_retry(sh.batch_update, body={"requests":[{"updateSheetProperties":{"properties":{"sheetId":ws_m.id,"gridProperties":{"frozenRowCount":1}},"fields":"gridProperties.frozenRowCount"}}]})
        except: pass
    return "마스터탭: ✅"


def _write_trend_daily():
    """추이차트 쓰기 + 서식"""
    td = trend_daily_data
    ws_t = ws_registry['추이차트']
    with WRITE_SEMAPHORE:
        with_retry(ws_t.update, values=[td['hdr']]+[td['summary_row']]+td['rows'], range_name="A1", value_input_option="USER_ENTERED")
        time.sleep(3)
    with FORMAT_SEMAPHORE:
        apply_trend_chart_formatting(sh, ws_t, td['hdr'], len(td['rows']), sunday_col_indices=td['sci'])
        apply_c2_label_formatting(sh, ws_t)
    return "추이차트: ✅"


def _write_trend_weekly():
    """추이차트(주간) 쓰기 + 서식"""
    tw = trend_weekly_data
    ws_tw = ws_registry['추이차트(주간)']
    with WRITE_SEMAPHORE:
        with_retry(ws_tw.update, values=[tw['hdr']]+[tw['summary_row']]+tw['rows'], range_name="A1", value_input_option="USER_ENTERED")
        time.sleep(3)
    wfce = 3+1+WEEKLY_TREND_REFRESH_WEEKS
    with FORMAT_SEMAPHORE:
        apply_trend_chart_formatting(sh, ws_tw, tw['hdr'], len(tw['rows']), format_col_end=wfce)
        apply_c2_label_formatting(sh, ws_tw)
    return "추이차트(주간): ✅"


def _write_change():
    """증감액 쓰기 + 서식"""
    cd = change_data
    ws_c = ws_registry['증감액']
    with WRITE_SEMAPHORE:
        with_retry(ws_c.update, values=[cd['hdr']]+[cd['summary_row']]+cd['rows'], range_name="A1", value_input_option="USER_ENTERED")
        time.sleep(3)
    with FORMAT_SEMAPHORE:
        apply_trend_chart_formatting(sh, ws_c, cd['hdr'], len(cd['rows']), is_change_tab=True)
    return "증감액: ✅"


def _write_budget():
    """예산 쓰기"""
    bw = ws_registry['예산']
    with WRITE_SEMAPHORE:
        with_retry(bw.update, values=budget_data, range_name="A1", value_input_option="RAW")
    return "예산: ✅"


def _write_weekly_summary_1():
    """주간종합 쓰기 + 서식"""
    ws_ws = ws_registry['주간종합']
    sid_ws = ws_ws.id; fr_ws = []; ar_ws = []; cr_ws = 0

    for block_data, im in ws1_blocks:
        bs = cr_ws
        nc = len(products)+2
        hb = COLORS["dark_blue"] if im else COLORS["dark_gray"]; cb = COLORS["navy"] if im else COLORS["black"]
        fr_ws.append(create_format_request(sid_ws,cr_ws,cr_ws+1,0,nc,get_cell_format(hb,COLORS["white"],bold=True))); cr_ws+=1
        fr_ws.append(create_format_request(sid_ws,cr_ws,cr_ws+1,0,8,get_cell_format(cb,COLORS["white"],bold=True))); cr_ws+=1
        fr_ws.append(create_number_format_request(sid_ws,cr_ws,cr_ws+1,0,1,"NUMBER","#,##0"))
        fr_ws.append(create_number_format_request(sid_ws,cr_ws,cr_ws+1,3,5,"NUMBER","#,##0"))
        fr_ws.append(create_number_format_request(sid_ws,cr_ws,cr_ws+1,5,7,"PERCENT","0.00%")); cr_ws+=1
        fr_ws.append(create_format_request(sid_ws,cr_ws,cr_ws+1,1,len(products)+1,get_cell_format(cb,COLORS["white"],bold=True)))
        fr_ws.append(create_format_request(sid_ws,cr_ws,cr_ws+1,len(products)+1,len(products)+2,get_cell_format(COLORS["light_yellow"],bold=True))); cr_ws+=1
        for row_idx in range(5):
            fr_ws.append(create_format_request(sid_ws,cr_ws,cr_ws+1,0,1,get_cell_format(bold=True)))
            fr_ws.append(create_format_request(sid_ws,cr_ws,cr_ws+1,len(products)+1,len(products)+2,get_cell_format(COLORS["light_yellow"])))
            ft = "PERCENT" if row_idx in [0,4] else "NUMBER"; fp = "0.00%" if row_idx in [0,4] else "#,##0"
            fr_ws.append(create_number_format_request(sid_ws,cr_ws,cr_ws+1,1,len(products)+2,ft,fp)); cr_ws+=1
        fr_ws.append(create_border_request(sid_ws,bs,cr_ws,0,len(products)+2))
        cr_ws += 1
        ar_ws.extend(block_data)

    with WRITE_SEMAPHORE:
        with_retry(ws_ws.update, values=ar_ws, range_name="A1", value_input_option="USER_ENTERED"); time.sleep(3)
    with FORMAT_SEMAPHORE:
        try:
            for i in range(0,len(fr_ws),100): with_retry(sh.batch_update, body={"requests":fr_ws[i:i+100]}); time.sleep(1)
        except: pass
        try:
            cw=[{"updateDimensionProperties":{"range":{"sheetId":sid_ws,"dimension":"COLUMNS","startIndex":0,"endIndex":1},"properties":{"pixelSize":150},"fields":"pixelSize"}}]
            for ci in range(1,11): cw.append({"updateDimensionProperties":{"range":{"sheetId":sid_ws,"dimension":"COLUMNS","startIndex":ci,"endIndex":ci+1},"properties":{"pixelSize":100},"fields":"pixelSize"}})
            with_retry(sh.batch_update, body={"requests":cw})
        except: pass
    return "주간종합: ✅"


def _write_weekly_summary_2():
    """주간종합_2 쓰기 + 서식"""
    ws2 = ws_registry['주간종합_2']
    sid2=ws2.id; fr2=[]; ar2=[]; cr2=0; npc=len(products)+3

    ar2.append(["📊 기간별 전체 요약"]+[""]*6); fr2.append(create_format_request(sid2,cr2,cr2+1,0,7,get_cell_format(COLORS["navy"],COLORS["white"],bold=True))); cr2+=1
    ar2.append(["기간","유형","지출금액","매출","이익","ROAS","CVR"]); fr2.append(create_format_request(sid2,cr2,cr2+1,0,7,get_cell_format(COLORS["dark_blue"],COLORS["white"],bold=True))); cr2+=1
    t1s=cr2
    for rd in ws2_stl:
        ar2.append([rd['period'],rd['type'],rd['spend'],rd['revenue'],rd['profit'],rd['roas'],0])
        bg=COLORS["light_blue"] if rd['im'] else COLORS["white"]
        fr2.append(create_format_request(sid2,cr2,cr2+1,0,7,get_cell_format(bg)))
        fr2.append(create_number_format_request(sid2,cr2,cr2+1,2,5,"NUMBER","#,##0"))
        fr2.append(create_number_format_request(sid2,cr2,cr2+1,5,7,"PERCENT","0.00%")); cr2+=1
    fr2.append(create_border_request(sid2,t1s-1,cr2,0,7)); ar2+=[[""]*(npc),[""]*(npc)]; cr2+=2

    for tt,tc,dk in [("📈 제품별 ROAS",COLORS["dark_green"],"roas"),("💰 제품별 순이익",COLORS["dark_green"],"profit"),
        ("💵 제품별 매출",COLORS["orange"],"revenue"),("💸 제품별 예산",COLORS["purple"],"spend"),("📊 제품별 예산 비중",COLORS["purple"],"ratio")]:
        ar2.append([tt]+[""]*(npc-1)); fr2.append(create_format_request(sid2,cr2,cr2+1,0,npc,get_cell_format(tc,COLORS["white"],bold=True))); cr2+=1
        ar2.append(["기간","유형"]+products+["합계"]); fr2.append(create_format_request(sid2,cr2,cr2+1,0,npc,get_cell_format(COLORS["dark_gray"],COLORS["white"],bold=True))); cr2+=1
        tds=cr2
        for rd in ws2_stl:
            pd_r = mps.get(rd.get('mk',''),defaultdict(lambda:{'spend':0,'revenue':0,'profit':0})) if rd['im'] else wps.get(rd.get('wk',''),defaultdict(lambda:{'spend':0,'revenue':0,'profit':0}))
            r=[rd['period'],rd['type']]; tsp=sum(pd_r[p]['spend'] for p in products); trv=sum(pd_r[p]['revenue'] for p in products)
            for p in products:
                if dk=="roas": r.append((pd_r[p]['revenue']/pd_r[p]['spend']) if pd_r[p]['spend']>0 else 0)
                elif dk=="ratio": r.append((pd_r[p]['spend']/tsp) if tsp>0 else 0)
                else: r.append(pd_r[p][dk])
            if dk=="roas": r.append((trv/tsp) if tsp>0 else 0)
            elif dk=="ratio": r.append(1.0)
            else: r.append(sum(pd_r[p][dk] for p in products))
            ar2.append(r); bg=COLORS["light_blue"] if rd['im'] else COLORS["white"]
            fr2.append(create_format_request(sid2,cr2,cr2+1,0,npc,get_cell_format(bg)))
            ft="PERCENT" if dk in ["roas","ratio"] else "NUMBER"; fp="0.00%" if dk in ["roas","ratio"] else "#,##0"
            fr2.append(create_number_format_request(sid2,cr2,cr2+1,2,npc,ft,fp)); cr2+=1
        fr2.append(create_border_request(sid2,tds-1,cr2,0,npc)); ar2+=[[""]*(npc),[""]*(npc)]; cr2+=2

    with WRITE_SEMAPHORE:
        with_retry(ws2.update, values=ar2, range_name="A1", value_input_option="USER_ENTERED"); time.sleep(3)
    with FORMAT_SEMAPHORE:
        try:
            for i in range(0,len(fr2),100): with_retry(sh.batch_update, body={"requests":fr2[i:i+100]}); time.sleep(1)
        except: pass
    return "주간종합_2: ✅"


def _write_weekly_summary_3():
    """주간종합_3 쓰기 + 서식"""
    ws3 = ws_registry['주간종합_3']
    sid3=ws3.id; fr3=[]; ar3=[]; cr3=0; ndc=len(products)+4

    ar3.append(["📊 일별 전체 요약"]+[""]*7); fr3.append(create_format_request(sid3,cr3,cr3+1,0,8,get_cell_format(COLORS["navy"],COLORS["white"],bold=True))); cr3+=1
    ar3.append(["날짜","요일","지출금액","매출","이익","ROAS","CVR",""]); fr3.append(create_format_request(sid3,cr3,cr3+1,0,8,get_cell_format(COLORS["dark_blue"],COLORS["white"],bold=True))); cr3+=1
    t1s3=cr3
    for rd in ws3_dsr:
        ar3.append([rd['period'],rd['weekday'],rd['spend'],rd['revenue'],rd['profit'],rd['roas'],0,""])
        bg=COLORS["light_blue"] if rd['weekday'] in ['토','일'] else COLORS["white"]
        fr3.append(create_format_request(sid3,cr3,cr3+1,0,8,get_cell_format(bg)))
        fr3.append(create_number_format_request(sid3,cr3,cr3+1,2,5,"NUMBER","#,##0"))
        fr3.append(create_number_format_request(sid3,cr3,cr3+1,5,7,"PERCENT","0.00%")); cr3+=1
    fr3.append(create_border_request(sid3,t1s3-1,cr3,0,8)); ar3+=[[""]*(ndc),[""]*(ndc)]; cr3+=2

    for tt,tc,dk in [("📈 일별 제품별 ROAS",COLORS["dark_green"],"roas"),("💰 일별 제품별 순이익",COLORS["dark_green"],"profit"),
        ("💵 일별 제품별 매출",COLORS["orange"],"revenue"),("💸 일별 제품별 예산",COLORS["purple"],"spend"),("📊 일별 제품별 예산 비중",COLORS["purple"],"ratio")]:
        ar3.append([tt]+[""]*(ndc-1)); fr3.append(create_format_request(sid3,cr3,cr3+1,0,ndc,get_cell_format(tc,COLORS["white"],bold=True))); cr3+=1
        ar3.append(["날짜","요일"]+products+["합계"]); fr3.append(create_format_request(sid3,cr3,cr3+1,0,ndc,get_cell_format(COLORS["dark_gray"],COLORS["white"],bold=True))); cr3+=1
        tds=cr3
        for rd in ws3_dsr:
            pd_r=daily_product_data[rd['tab_name']]; r=[rd['period'],rd['weekday']]
            tsp=sum(pd_r[p]['spend'] for p in products); trv=sum(pd_r[p]['revenue'] for p in products)
            for p in products:
                if dk=="roas": r.append((pd_r[p]['revenue']/pd_r[p]['spend']) if pd_r[p]['spend']>0 else 0)
                elif dk=="ratio": r.append((pd_r[p]['spend']/tsp) if tsp>0 else 0)
                else: r.append(pd_r[p][dk])
            if dk=="roas": r.append((trv/tsp) if tsp>0 else 0)
            elif dk=="ratio": r.append(1.0)
            else: r.append(sum(pd_r[p][dk] for p in products))
            ar3.append(r); bg=COLORS["light_blue"] if rd['weekday'] in ['토','일'] else COLORS["white"]
            fr3.append(create_format_request(sid3,cr3,cr3+1,0,ndc,get_cell_format(bg)))
            ft="PERCENT" if dk in ["roas","ratio"] else "NUMBER"; fp="0.00%" if dk in ["roas","ratio"] else "#,##0"
            fr3.append(create_number_format_request(sid3,cr3,cr3+1,2,ndc,ft,fp)); cr3+=1
        fr3.append(create_border_request(sid3,tds-1,cr3,0,ndc)); ar3+=[[""]*(ndc),[""]*(ndc)]; cr3+=2

    with WRITE_SEMAPHORE:
        with_retry(ws3.update, values=ar3, range_name="A1", value_input_option="USER_ENTERED"); time.sleep(3)
    with FORMAT_SEMAPHORE:
        try:
            for i in range(0,len(fr3),100): with_retry(sh.batch_update, body={"requests":fr3[i:i+100]}); time.sleep(1)
        except: pass
    return "주간종합_3: ✅"


# =========================================================
# ★★★ 병렬의 병렬: 모든 탭 동시 쓰기 ★★★
# =========================================================
print("\n🚀 ★★★ 병렬의 병렬: 모든 탭 동시 쓰기 시작 ★★★")
_t_write = time.time()

write_futures = {}
with ThreadPoolExecutor(max_workers=6) as write_executor:
    # 날짜 탭 병렬 쓰기
    for dk in new_date_names:
        f = write_executor.submit(_write_date_tab, dk)
        write_futures[f] = f"날짜탭:{dk}"

    # 분석 탭 병렬 쓰기
    write_futures[write_executor.submit(_write_master)] = "마스터탭"
    write_futures[write_executor.submit(_write_budget)] = "예산"
    write_futures[write_executor.submit(_write_weekly_summary_1)] = "주간종합"
    write_futures[write_executor.submit(_write_weekly_summary_2)] = "주간종합_2"
    write_futures[write_executor.submit(_write_weekly_summary_3)] = "주간종합_3"

    # 추이차트 그룹 (서식이 무거우므로 별도 그룹)
    write_futures[write_executor.submit(_write_trend_daily)] = "추이차트"
    write_futures[write_executor.submit(_write_trend_weekly)] = "추이차트(주간)"
    write_futures[write_executor.submit(_write_change)] = "증감액"

    for future in as_completed(write_futures):
        label = write_futures[future]
        try:
            result = future.result()
            print(f"  ✅ {result}")
        except Exception as e:
            print(f"  ⚠️ {label} 오류: {e}")

_write_elapsed = time.time() - _t_write
print(f"\n  ⏱️ 병렬 쓰기 완료: {_write_elapsed:.1f}초")


# =========================================================
# 18: 탭 순서 정리
# =========================================================
print("\n📝 18: 탭 순서 정리")
try:
    tmp = sh.worksheet("_tmp"); sh.del_worksheet(tmp)
except: pass

try:
    all_ws = sh.worksheets()
    analysis_set = set(ANALYSIS_TAB_NAMES)
    analysis_tabs, date_tabs, other_tabs = [], [], []
    for ws in all_ws:
        tn = ws.title
        if tn in analysis_set: analysis_tabs.append(ws)
        elif parse_date_tab(tn) is not None: date_tabs.append(ws)
        else: other_tabs.append(ws)
    date_tabs.sort(key=lambda ws: parse_date_tab(ws.title))
    analysis_order = {name: i for i, name in enumerate(ANALYSIS_TAB_NAMES)}
    analysis_tabs.sort(key=lambda ws: analysis_order.get(ws.title, 999))
    final_order = other_tabs + date_tabs + analysis_tabs

    if date_tabs:
        print(f"  📅 날짜 탭: {len(date_tabs)}개 ({date_tabs[0].title} → {date_tabs[-1].title})")
    print(f"  📊 분석 탭: {len(analysis_tabs)}개")

    reorder_requests = [{"updateSheetProperties": {"properties": {"sheetId": ws.id, "index": idx}, "fields": "index"}} for idx, ws in enumerate(final_order)]
    if reorder_requests:
        with_retry(sh.batch_update, body={"requests": reorder_requests})
        print("  ✅ 탭 순서 정리 완료")
except Exception as e:
    print(f"  ⚠️ 탭 순서 오류: {e}")


# =========================================================
# 완료
# =========================================================
print("\n" + "="*60)
print("✅ 완료! (★ 병렬의 병렬 최적화 버전)")
print("="*60)
print()
print(f"📋 Meta: 최근 {REFRESH_DAYS}일 × {len(ALL_AD_ACCOUNTS)}계정 = {REFRESH_DAYS*len(ALL_AD_ACCOUNTS)}건 ★동시 호출★")
print(f"📋 Mixpanel: Meta와 ★동시 수집★ (이벤트: {MIXPANEL_EVENT_NAMES})")
print(f"📋 매출 필드: amount OR 결제금액 (OR fallback)")
print(f"📋 기존 탭 읽기: ★병렬 읽기★")
print(f"📋 데이터 준비: 날짜탭+분석탭 ★10개 동시 생성★")
print(f"📋 Sheets 쓰기: ★★★ 병렬의 병렬 — 날짜탭+분석탭 동시 쓰기 ★★★")
print(f"📋 탭 순서: [기타] → [날짜: 과거→최신] → [분석탭]")
print(f"\n📊 {SPREADSHEET_URL}")
