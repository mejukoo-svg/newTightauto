# -*- coding: utf-8 -*-
# v3-ad 통합 코드 (광고 세트 기준, 메모 보존, RIGHTMOST_TABS 지원)
# 탭 순서: [기타] + [날짜탭 과거→최신] + [분석탭] + [주간매출, 매출]

print("="*60)
print("\U0001f680 v3-ad 7일갱신 (광고세트 기준, utm_term 매핑, 메모보존) [FIX v21 - EVENT_OR]")
print("="*60)

import os, json, requests as req_lib, pandas as pd, time, random, math, re, calendar
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from decimal import Decimal

# =========================================================
# 인증
# =========================================================
if 'GCP_SERVICE_ACCOUNT_KEY' in os.environ:
    import gspread
    from google.oauth2.service_account import Credentials
    creds = Credentials.from_service_account_info(
        json.loads(os.environ['GCP_SERVICE_ACCOUNT_KEY']),
        scopes=['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds)
    print("\u2705 GitHub Actions 서비스 계정 인증 완료")
else:
    from google.colab import auth; auth.authenticate_user()
    import gspread
    from google.auth import default
    creds, _ = default(scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    print("\u2705 Google Colab 인증 완료")

from gspread.exceptions import APIError

SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL", "https://docs.google.com/spreadsheets/d/187gxptjGk6Bdhyp6_w14rldMSwBNve2QGDY7KzcIPTU/edit?usp=sharing")
sh = gc.open_by_url(SPREADSHEET_URL)
print(f"\u2705 스프레드시트: {sh.title}\n")

# =========================================================
# 환율
# =========================================================
FALLBACK_USD_KRW = 1450
FALLBACK_TWD_KRW = 45

def fetch_daily_exchange_rates(start_date, end_date, currency="USD"):
    rates = {}; pair = f"{currency}KRW=X"
    fallback = FALLBACK_USD_KRW if currency == "USD" else FALLBACK_TWD_KRW
    try:
        import yfinance as yf
    except ImportError:
        try: import subprocess; subprocess.check_call(['pip','install','yfinance','-q']); import yfinance as yf
        except: yf = None
    try:
        if yf:
            hist = yf.Ticker(pair).history(start=start_date.strftime('%Y-%m-%d'), end=(end_date+timedelta(days=3)).strftime('%Y-%m-%d'))
            if not hist.empty:
                for idx, row in hist.iterrows():
                    dt = idx.to_pydatetime().replace(tzinfo=None)
                    rates[f"{dt.year%100:02d}/{dt.month:02d}/{dt.day:02d}"] = round(float(row['Close']), 2)
                print(f"  \u2705 yfinance {currency}/KRW: {len(rates)}일")
    except Exception as e: print(f"  \u26a0\ufe0f yfinance {currency}/KRW 실패: {e}")
    if not rates:
        try:
            resp = req_lib.get(f"https://open.er-api.com/v6/latest/{currency}", timeout=10)
            if resp.status_code == 200:
                cr = resp.json().get('rates',{}).get('KRW', fallback)
                print(f"  \u2705 API 환율: 1 {currency} = \u20a9{cr:,.2f}")
                d = start_date
                while d <= end_date:
                    rates[f"{d.year%100:02d}/{d.month:02d}/{d.day:02d}"] = round(float(cr), 2); d += timedelta(days=1)
        except Exception as e: print(f"  \u26a0\ufe0f {currency}/KRW API 실패: {e}")
    if not rates: print(f"  \u26a0\ufe0f {currency}/KRW 폴백 \u20a9{fallback:,}")
    return rates

def get_rate_for_date(rates, dk, fallback=FALLBACK_USD_KRW):
    if dk in rates: return rates[dk]
    if rates:
        sk = sorted(rates.keys()); prev = [k for k in sk if k <= dk]
        return rates[prev[-1]] if prev else rates[sk[0]]
    return fallback

# =========================================================
# Meta Ads API
# =========================================================
META_TOKEN_DEFAULT = os.environ.get("META_TOKEN_1", "")
META_TOKEN_4 = os.environ.get("META_TOKEN_4", "")
META_TOKEN_ACT_2677 = META_TOKEN_4 or os.environ.get("META_TOKEN_3", "")
META_TOKENS = {"act_1054081590008088": os.environ.get("META_TOKEN_1",""), "act_2677707262628563": META_TOKEN_ACT_2677}
def get_token(acc_id): return META_TOKENS.get(acc_id, META_TOKEN_DEFAULT)
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

print("\U0001f511 Meta 토큰 상태:")
_ok = "\u2705"; _no = "\u274c"
print(f"  TOKEN_1 (act_1054): {_ok if META_TOKENS['act_1054081590008088'] else _no}")
print(f"  TOKEN_4 (act_2677): {_ok if META_TOKEN_4 else _no}")
print(f"  act_2677 최종: {_ok if META_TOKEN_ACT_2677 else _no}")

# =========================================================
# Mixpanel
# =========================================================
MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME = os.environ.get("MIXPANEL_USERNAME", "")
MIXPANEL_SECRET = os.environ.get("MIXPANEL_SECRET", "")
MIXPANEL_EVENT_NAMES = ["결제완료", "payment_complete"]

# =========================================================
# 기본 설정
# =========================================================
KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None)
CURRENT_YEAR, CURRENT_MONTH = TODAY.year, TODAY.month

FULL_REFRESH = os.environ.get("FULL_REFRESH","false").lower() == "true"
FULL_REFRESH_START = datetime(2026,1,1)
if FULL_REFRESH:
    REFRESH_DAYS = (TODAY - FULL_REFRESH_START).days + 1
    print(f"\U0001f525 FULL_REFRESH: {FULL_REFRESH_START.strftime('%Y-%m-%d')} ~ 오늘 ({REFRESH_DAYS}일)")
else:
    REFRESH_DAYS = 7
    print(f"\U0001f504 일반 모드: 최근 {REFRESH_DAYS}일")

META_COLLECT_DAYS = REFRESH_DAYS
DATA_REFRESH_START = TODAY - timedelta(days=REFRESH_DAYS - 1)
WEEKLY_TREND_REFRESH_WEEKS = 2

DATE_TAB_HEADERS = ["캠페인 이름","광고 세트 이름","광고 세트 ID","지출 금액 (KRW)","결과당 비용","구매 ROAS(광고 지출 대비 수익률)","CPM(1,000회 노출당 비용)","도달","노출","고유 아웃바운드 클릭","고유 아웃바운드 CTR(클릭률)","고유 아웃바운드 클릭당 비용","빈도","결과","결과(믹스패널)","매출","이익","ROAS","CVR","기존 예산","증액률","변동 예산","메모"]
OLD_DATE_TAB_HEADERS = ["캠페인 이름","광고 세트 이름","광고 세트 ID","광고 이름","광고 ID","지출 금액 (KRW)","결과당 비용","구매 ROAS(광고 지출 대비 수익률)","CPM(1,000회 노출당 비용)","도달","노출","고유 아웃바운드 클릭","고유 아웃바운드 CTR(클릭률)","고유 아웃바운드 클릭당 비용","빈도","결과","결과(믹스패널)","매출","이익","ROAS","CVR","기존 예산","증액률","변동 예산","메모"]

print(f"\U0001f4c5 현재: {TODAY.strftime('%Y-%m-%d')} | API: {DATA_REFRESH_START.strftime('%Y-%m-%d')}~오늘 | Mixpanel: {MIXPANEL_EVENT_NAMES}\n")

PRODUCT_KEYWORDS = ["starsun", "money", "solo"]
SKIP_WORDS = {"tw","kr","hk","my","sg","id","jp","th","vn","ph","asia","broad","interest","lookalike","retarget","retargeting","custom","asc","cbo","abo","dpa","daba","advantage","campaign","adset","ad","ads","set","purchase","conversion","traffic","reach","awareness","engagement","auto","manual","daily","lifetime","budget","v1","v2","v3","v4","v5","v6","v7","v8","v9","v10","test","new","old","copy","ver","final","draft","temp","sajutight","ttsaju","saju","tight","a","b","c","d","e","f","the","and","or","for","all","img","vid"}
WEEKDAY_NAMES = ['월','화','수','목','금','토','일']
COLORS = {"dark_gray":{"red":0.4,"green":0.4,"blue":0.4},"black":{"red":0.2,"green":0.2,"blue":0.2},"light_gray":{"red":0.9,"green":0.9,"blue":0.9},"light_gray2":{"red":0.95,"green":0.95,"blue":0.95},"white":{"red":1,"green":1,"blue":1},"green":{"red":0.56,"green":0.77,"blue":0.49},"light_yellow":{"red":1.0,"green":0.98,"blue":0.8},"light_blue":{"red":0.85,"green":0.92,"blue":0.98},"light_red":{"red":1.0,"green":0.85,"blue":0.85},"dark_blue":{"red":0.2,"green":0.3,"blue":0.5},"navy":{"red":0.15,"green":0.2,"blue":0.35},"dark_green":{"red":0.2,"green":0.5,"blue":0.3},"orange":{"red":0.9,"green":0.5,"blue":0.2},"purple":{"red":0.5,"green":0.3,"blue":0.6}}
SUMMARY_PRODUCTS = []

FINAL_ANALYSIS_ORDER = ["추이차트","추이차트(주간)","증감액","예산","주간종합","주간종합_2","주간종합_3","마스터탭"]
ANALYSIS_TABS_SET = set(FINAL_ANALYSIS_ORDER) | {"_temp","_temp_holder","_tmp"}

# ★ 맨 오른쪽에 배치할 탭
RIGHTMOST_TABS = ["주간매출", "매출"]

# =========================================================
# 유틸리티 함수
# =========================================================
OLD_TO_NEW_MAP = {0:0,1:1,2:2,5:3,6:4,7:5,8:6,9:7,10:8,11:9,12:10,13:11,14:12,15:13,16:14,17:15,18:16,19:17,20:18,21:19,22:20,23:21,24:22}

def detect_tab_structure(header_row):
    if not header_row or len(header_row) < 5: return "new"
    h = [str(x).strip().lower() for x in header_row]
    if len(h)>3 and ("광고 이름" in h[3] or "ad name" in h[3]): return "old"
    if len(h)>4 and ("광고 id" in h[4] or "ad id" in h[4]): return "old"
    if len(h)>=25 and len(h)>5 and "지출" in h[5]: return "old"
    return "new"

def normalize_row_to_new(row, structure):
    if structure == "new":
        r = list(row[:len(DATE_TAB_HEADERS)])
        while len(r) < len(DATE_TAB_HEADERS): r.append("")
        return r
    nr = [""]*len(DATE_TAB_HEADERS)
    for oi, ni in OLD_TO_NEW_MAP.items():
        if oi < len(row): nr[ni] = row[oi]
    return nr

def get_col_index(structure, new_col_idx):
    if structure == "new": return new_col_idx
    return new_col_idx if new_col_idx <= 2 else new_col_idx + 2

def get_col_letter(ci):
    r = ""; i = ci
    while True:
        r = chr(ord('A')+i%26)+r; i = i//26-1
        if i < 0: break
    return r

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
    n = re.sub(r'[^0-9]','',s)
    return n if n else s

def extract_product(adset_name):
    if not adset_name: return "기타"
    nl = str(adset_name).lower()
    for kw in PRODUCT_KEYWORDS:
        if kw in nl: return kw
    parts = nl.split('_')
    if len(parts) >= 3 and parts[2].strip() in PRODUCT_KEYWORDS: return parts[2].strip()
    return "기타"

def _to_num(x):
    try:
        v = str(x).replace(",","").replace("₩","").replace("%","").replace("\\","").replace("W","").replace("￦","").strip()
        return float(v) if v and v not in ["-","#DIV/0!"] else 0.0
    except: return 0.0

def money(n):
    try: return f"₩{int(round(float(n))):,}"
    except: return "₩0"

def with_retry(fn, *args, max_retries=8, **kwargs):
    for attempt in range(max_retries):
        try: return fn(*args, **kwargs)
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e):
                w = 20+(attempt*15)+random.random(); print(f"  \u23f3 Rate limit {w:.0f}s (#{attempt+1})"); time.sleep(w)
            elif attempt == max_retries-1: raise
            else: time.sleep(5+random.random())
    return None

def safe_add_worksheet(sh, title, rows, cols):
    try:
        ex = with_retry(sh.worksheet, title)
        if ex: with_retry(sh.del_worksheet, ex); print(f"  \U0001f5d1\ufe0f '{title}' 재생성"); time.sleep(2)
    except gspread.exceptions.WorksheetNotFound: pass
    except APIError as e:
        if "429" in str(e): time.sleep(30)
        else: print(f"  \u26a0\ufe0f safe_add: {e}")
    except Exception as e: print(f"  \u26a0\ufe0f safe_add: {e}")
    return with_retry(sh.add_worksheet, title=title, rows=rows, cols=cols)

def refresh_ws(sh, ws):
    try: return with_retry(sh.worksheet, ws.title)
    except: return ws

def clear_summary_conditional_formats(sh, ws, summary_start_row_0indexed):
    try:
        sid = ws.id
        try: metadata = sh.fetch_sheet_metadata(params={'fields':'sheets(properties.sheetId,conditionalFormats)'})
        except AttributeError:
            url = f"https://sheets.googleapis.com/v4/spreadsheets/{sh.id}"
            metadata = sh.client.request('get',url,params={'fields':'sheets(properties.sheetId,conditionalFormats)'}).json()
        sm = next((s for s in metadata.get('sheets',[]) if s.get('properties',{}).get('sheetId')==sid), None)
        if not sm: return
        cr = sm.get('conditionalFormats',[])
        if not cr: return
        di = [i for i,r in enumerate(cr) if any(rng.get('sheetId')==sid and rng.get('endRowIndex',999999)>summary_start_row_0indexed for rng in r.get('ranges',[]))]
        if di:
            with_retry(sh.batch_update, body={"requests":[{"deleteConditionalFormatRule":{"sheetId":sid,"index":i}} for i in sorted(di,reverse=True)]})
            time.sleep(0.5)
    except Exception as e: print(f"    \u26a0\ufe0f 조건부서식 삭제 오류: {e}")

# ★ 수정2: reorder_tabs - RIGHTMOST_TABS 지원
def reorder_tabs(sh):
    """탭 순서: [기타] + [날짜탭 과거→최신] + [분석탭] + [주간매출, 매출]"""
    try:
        all_ws = sh.worksheets()
        analysis_tabs, date_tabs, other_tabs, rightmost_tabs_list = [], [], [], []
        analysis_order_map = {name:i for i,name in enumerate(FINAL_ANALYSIS_ORDER)}
        rightmost_order_map = {name:i for i,name in enumerate(RIGHTMOST_TABS)}
        for ws in all_ws:
            tn = ws.title
            if tn in RIGHTMOST_TABS: rightmost_tabs_list.append(ws)
            elif tn in ANALYSIS_TABS_SET: analysis_tabs.append(ws)
            elif parse_date_tab(tn) is not None: date_tabs.append(ws)
            else: other_tabs.append(ws)
        date_tabs.sort(key=lambda ws: parse_date_tab(ws.title))
        analysis_tabs.sort(key=lambda ws: analysis_order_map.get(ws.title,999))
        rightmost_tabs_list.sort(key=lambda ws: rightmost_order_map.get(ws.title,999))
        final_order = other_tabs + date_tabs + analysis_tabs + rightmost_tabs_list
        print(f"  \U0001f4cb 기타:{len(other_tabs)} | 날짜:{len(date_tabs)} | 분석:{len(analysis_tabs)} | 맨끝:{len(rightmost_tabs_list)}")
        if date_tabs: print(f"  \U0001f4c5 {date_tabs[0].title} → {date_tabs[-1].title}")
        if analysis_tabs: print(f"  \U0001f4ca {' → '.join(ws.title for ws in analysis_tabs)}")
        if rightmost_tabs_list: print(f"  \u27a1\ufe0f {' → '.join(ws.title for ws in rightmost_tabs_list)}")
        with_retry(sh.batch_update, body={"requests":[
            {"updateSheetProperties":{"properties":{"sheetId":ws.id,"index":idx},"fields":"index"}}
            for idx, ws in enumerate(final_order)]})
        print("  \u2705 탭 순서 완료"); time.sleep(2)
    except Exception as e: print(f"  \u26a0\ufe0f 탭 순서 오류: {e}")

def cell_text(profit, revenue, spend, cpm=0, cvr=0):
    if spend == 0: return ""
    roas = (revenue/spend*100) if spend > 0 else 0
    return f"{roas:.0f}\n{money(profit)}\n-{money(spend)}\n{'₩'+str(int(round(cpm))) if cpm>0 else '₩0'}\n{cvr:.1f}%"

def cell_text_change(roas, chg, spend, cpm, cvr=0):
    if spend == 0: return ""
    cl = f"+{chg:.1f}%" if chg>0 else f"{chg:.1f}%" if chg<0 else "0.0%"
    return f"{roas:.0f}\n{cl}\n-{money(spend)}\n{'₩'+str(int(round(cpm))) if cpm>0 else '₩0'}\n{cvr:.1f}%"

def get_week_range(d):
    m = d-timedelta(days=d.weekday()); s = m+timedelta(days=6)
    return f"'{m.month}/{m.day}({WEEKDAY_NAMES[0]})~{s.month}/{s.day}({WEEKDAY_NAMES[6]})"
def get_week_range_short(d):
    m = d-timedelta(days=d.weekday()); s = m+timedelta(days=6)
    return f"{m.month}/{m.day}~{s.month}/{s.day}"
def get_month_range_display(f, l): return f"'{f.month}.{f.day}~{l.month}.{l.day}"
def get_week_monday(d): return d - timedelta(days=d.weekday())

def parse_date_tab(tab_name):
    if not tab_name or '/' not in tab_name: return None
    try:
        cn = tab_name.strip().strip("'").strip('\u200b\ufeff\xa0').strip()
        parts = [p.strip() for p in cn.split('/')]
        if len(parts) == 3: return datetime(2000+int(parts[0]),int(parts[1]),int(parts[2]))
        elif len(parts) == 2:
            m,d = int(parts[0]),int(parts[1]); now = datetime.now()
            c = datetime(now.year,m,d)
            return datetime(now.year-1,m,d) if c > now+timedelta(days=7) else c
        return None
    except: return None

def get_cell_format(bg=None, tc=None, bold=False, ha="CENTER"):
    f = {"horizontalAlignment":ha,"verticalAlignment":"MIDDLE"}
    if bg: f["backgroundColor"] = bg
    if tc or bold:
        f["textFormat"] = {}
        if tc: f["textFormat"]["foregroundColor"] = tc
        if bold: f["textFormat"]["bold"] = True
    return f

def create_format_request(sid,sr,er,sc,ec,fmt):
    return {"repeatCell":{"range":{"sheetId":sid,"startRowIndex":sr,"endRowIndex":er,"startColumnIndex":sc,"endColumnIndex":ec},"cell":{"userEnteredFormat":fmt},"fields":"userEnteredFormat"}}

def create_border_request(sid,sr,er,sc,ec):
    bs = {"style":"SOLID","width":1,"color":{"red":0.7,"green":0.7,"blue":0.7}}
    return {"updateBorders":{"range":{"sheetId":sid,"startRowIndex":sr,"endRowIndex":er,"startColumnIndex":sc,"endColumnIndex":ec},"top":bs,"bottom":bs,"left":bs,"right":bs,"innerHorizontal":bs,"innerVertical":bs}}

def create_number_format_request(sid,sr,er,sc,ec,ft="NUMBER",p="#,##0"):
    return {"repeatCell":{"range":{"sheetId":sid,"startRowIndex":sr,"endRowIndex":er,"startColumnIndex":sc,"endColumnIndex":ec},"cell":{"userEnteredFormat":{"numberFormat":{"type":ft,"pattern":p}}},"fields":"userEnteredFormat.numberFormat"}}

def apply_c2_label_formatting(sh, ws):
    sid = ws.id; cv = "ROAS\n순이익\n지출\nCPM\n전환율"
    lines = cv.split('\n'); indices = [0]; pos = 0
    for line in lines[:-1]: pos += len(line)+1; indices.append(pos)
    bk={"foregroundColor":{"red":0,"green":0,"blue":0}}; dg={"foregroundColor":{"red":0.22,"green":0.46,"blue":0.11}}
    rd={"foregroundColor":{"red":0.85,"green":0,"blue":0}}; bl={"foregroundColor":{"red":0,"green":0,"blue":0.85}}
    with_retry(sh.batch_update, body={"requests":[{"updateCells":{"range":{"sheetId":sid,"startRowIndex":1,"endRowIndex":2,"startColumnIndex":2,"endColumnIndex":3},
        "rows":[{"values":[{"userEnteredValue":{"stringValue":cv},"textFormatRuns":[
            {"startIndex":indices[0],"format":bk},{"startIndex":indices[1],"format":dg},
            {"startIndex":indices[2],"format":rd},{"startIndex":indices[3],"format":bl},
            {"startIndex":indices[4],"format":bk}]}]}],"fields":"userEnteredValue,textFormatRuns"}}]}); time.sleep(1)

def apply_trend_chart_formatting(sh, ws, headers, rows_count, is_change_tab=False, sunday_col_indices=None, format_col_end=None):
    sid = ws.id
    try: with_retry(ws.format,'A1:Z1',{'backgroundColor':{'red':0.9,'green':0.9,'blue':0.9},'textFormat':{'bold':True}}); time.sleep(1)
    except: pass
    if sunday_col_indices:
        try:
            sr = [create_format_request(sid,0,1,ci,ci+1,get_cell_format(COLORS["light_red"],bold=True)) for ci in sunday_col_indices]
            if sr: with_retry(sh.batch_update,body={"requests":sr}); time.sleep(1)
        except: pass
    try: with_retry(ws.format,'A2:Z2',{'textFormat':{'bold':True}}); time.sleep(1)
    except: pass
    try: with_retry(sh.batch_update,body={"requests":[{"updateSheetProperties":{"properties":{"sheetId":sid,"gridProperties":{"frozenRowCount":1}},"fields":"gridProperties.frozenRowCount"}}]}); time.sleep(2)
    except: pass
    try: with_retry(ws.format,'D2:Z1000',{'wrapStrategy':'WRAP','verticalAlignment':'TOP'}); time.sleep(1)
    except: pass
    try:
        cwr = [{"updateDimensionProperties":{"range":{"sheetId":sid,"dimension":"COLUMNS","startIndex":cn,"endIndex":cn+1},"properties":{"pixelSize":95},"fields":"pixelSize"}} for cn in range(3,min(len(headers),26))]
        if cwr: with_retry(sh.batch_update,body={"requests":cwr})
    except: pass
    print("  \U0001f3a8 ROAS 조건부 서식...")
    try:
        sc, ec = 3, len(headers)
        rules = [
            {'c':'=AND(NOT(ISBLANK(INDIRECT(ADDRESS(ROW(),COLUMN())))),LEN(TRIM(INDIRECT(ADDRESS(ROW(),COLUMN()))))>0,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))=0)','clr':{'red':1.0,'green':0.6,'blue':0.6}},
            {'c':'=AND(NOT(ISBLANK(INDIRECT(ADDRESS(ROW(),COLUMN())))),LEN(TRIM(INDIRECT(ADDRESS(ROW(),COLUMN()))))>0,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))>=300)','clr':{'red':0.6,'green':1.0,'blue':1.0}},
            {'c':'=AND(NOT(ISBLANK(INDIRECT(ADDRESS(ROW(),COLUMN())))),LEN(TRIM(INDIRECT(ADDRESS(ROW(),COLUMN()))))>0,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))>=200,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))<300)','clr':{'red':0.7,'green':1.0,'blue':0.7}},
            {'c':'=AND(NOT(ISBLANK(INDIRECT(ADDRESS(ROW(),COLUMN())))),LEN(TRIM(INDIRECT(ADDRESS(ROW(),COLUMN()))))>0,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))>=100,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))<200)','clr':{'red':1.0,'green':1.0,'blue':0.6}},
            {'c':'=AND(NOT(ISBLANK(INDIRECT(ADDRESS(ROW(),COLUMN())))),LEN(TRIM(INDIRECT(ADDRESS(ROW(),COLUMN()))))>0,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))<100,VALUE(LEFT(INDIRECT(ADDRESS(ROW(),COLUMN())),FIND(CHAR(10),INDIRECT(ADDRESS(ROW(),COLUMN()))&CHAR(10))-1))>0)','clr':{'red':1.0,'green':0.8,'blue':0.8}},
        ]
        fr = [{'addConditionalFormatRule':{'rule':{'ranges':[{'sheetId':sid,'startRowIndex':1,'endRowIndex':rows_count+2,'startColumnIndex':sc,'endColumnIndex':ec}],
            'booleanRule':{'condition':{'type':'CUSTOM_FORMULA','values':[{'userEnteredValue':r['c']}]},'format':{'backgroundColor':r['clr']}}},'index':0}} for r in rules]
        with_retry(sh.batch_update,body={'requests':fr}); time.sleep(3)
    except Exception as e: print(f"  \u26a0\ufe0f 조건부 서식 오류: {e}")
    tcs=3; tce=min(format_col_end or len(headers), len(headers), 20)
    print(f"  \U0001f3a8 텍스트 색상 (col {tcs}~{tce-1})...")
    try:
        fr = []
        bk={"foregroundColor":{"red":0,"green":0,"blue":0}}; dg3={"foregroundColor":{"red":0.22,"green":0.46,"blue":0.11}}
        rd={"foregroundColor":{"red":0.85,"green":0,"blue":0}}; gn={"foregroundColor":{"red":0,"green":0.7,"blue":0}}; bl={"foregroundColor":{"red":0,"green":0,"blue":0.85}}
        for ci in range(tcs, tce):
            try: cv = with_retry(ws.col_values, ci+1)
            except: continue
            if not cv or len(cv)<2: continue
            for ri in range(2, min(len(cv)+1, rows_count+3)):
                val = cv[ri-1] if ri-1<len(cv) else ""
                if not val or '\n' not in val: continue
                lines = val.split('\n')
                if len(lines)<4: continue
                l1e=len(lines[0]);l2e=l1e+1+len(lines[1]);l3e=l2e+1+len(lines[2]);l4s=l3e+1
                if is_change_tab:
                    cc = gn if lines[1].startswith('+') else rd if lines[1].startswith('-') else bk
                    tr=[{"startIndex":0,"format":bk},{"startIndex":l1e+1,"format":cc},{"startIndex":l2e+1,"format":rd},{"startIndex":l4s,"format":bl}]
                else:
                    tr=[{"startIndex":0,"format":bk},{"startIndex":l1e+1,"format":dg3},{"startIndex":l2e+1,"format":rd},{"startIndex":l4s,"format":bl}]
                if len(lines)>=5: l5s=l4s+len(lines[3])+1; tr.append({"startIndex":l5s,"format":bk})
                fr.append({"updateCells":{"range":{"sheetId":sid,"startRowIndex":ri-1,"endRowIndex":ri,"startColumnIndex":ci,"endColumnIndex":ci+1},"rows":[{"values":[{"userEnteredValue":{"stringValue":val},"textFormatRuns":tr}]}],"fields":"userEnteredValue,textFormatRuns"}})
                if len(fr)>=300:
                    try: with_retry(sh.batch_update,body={"requests":fr}); fr=[]; time.sleep(3)
                    except: fr=[]
        if fr:
            try: with_retry(sh.batch_update,body={"requests":fr}); time.sleep(2)
            except: pass
        print("  \u2705 텍스트 색상 완료")
    except Exception as e: print(f"  \u26a0\ufe0f 텍스트 색상 오류: {e}")


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
            elif resp.status_code == 400: print(f"  \u274c Meta 400: {resp.json().get('error',{}).get('message',resp.text[:200])}"); return None
            elif resp.status_code in [429,500,502,503]: w=30+attempt*30; print(f"  \u23f3 Meta {resp.status_code}, {w}s"); time.sleep(w)
            else:
                print(f"  \u274c Meta {resp.status_code}: {resp.text[:200]}")
                if attempt<4: time.sleep(15)
                else: return None
        except Exception as e:
            print(f"  \u274c Meta 요청 오류: {e}")
            if attempt<4: time.sleep(15)
            else: return None
    return None

def extract_action_value(al, types):
    if not al: return 0
    for a in al:
        if a.get('action_type','') in types:
            try: return float(a.get('value',0))
            except: return 0
    return 0

def fetch_meta_insights_daily(ad_account_id, single_date):
    url = f"{META_BASE_URL}/{ad_account_id}/insights"
    fields = 'campaign_name,adset_name,adset_id,spend,cpm,reach,impressions,frequency,actions,cost_per_action_type,purchase_roas,unique_outbound_clicks,unique_outbound_clicks_ctr,cost_per_unique_outbound_click'
    params = {'fields':fields,'level':'adset','time_increment':1,'time_range':json.dumps({'since':single_date,'until':single_date}),'limit':500,'filtering':json.dumps([{'field':'spend','operator':'GREATER_THAN','value':'0'}])}
    all_results = []; data = meta_api_get(url, params, token=get_token(ad_account_id))
    while data:
        all_results.extend(data.get('data',[])); nxt = data.get('paging',{}).get('next')
        if nxt:
            time.sleep(1)
            try: resp = req_lib.get(nxt,timeout=120); data = resp.json() if resp.status_code==200 else None
            except: data = None
        else: break
    return all_results

def parse_single_day_insights(rows, date_str, date_obj):
    pt = ['purchase','omni_purchase','offsite_conversion.fb_pixel_purchase']; ot = ['outbound_click']
    parsed = []
    for row in rows:
        sp=float(row.get('spend',0));cpm=float(row.get('cpm',0));reach=int(float(row.get('reach',0)));impr=int(float(row.get('impressions',0)));freq=float(row.get('frequency',0))
        results=extract_action_value(row.get('actions',[]),pt); cpr=extract_action_value(row.get('cost_per_action_type',[]),pt)
        uc=extract_action_value(row.get('unique_outbound_clicks',[]),ot); uctr=extract_action_value(row.get('unique_outbound_clicks_ctr',[]),ot)
        cpc=extract_action_value(row.get('cost_per_unique_outbound_click',[]),ot)
        parsed.append({'campaign_name':row.get('campaign_name',''),'adset_name':row.get('adset_name',''),'adset_id':row.get('adset_id',''),
            'spend':sp,'cost_per_result':cpr,'cpm':cpm,'reach':reach,'impressions':impr,'unique_clicks':uc,'unique_ctr':uctr,'cost_per_unique_click':cpc,'frequency':freq,'results':results,'date_obj':date_obj})
    return parsed

def fetch_adset_budgets(ad_account_id):
    url = f"{META_BASE_URL}/{ad_account_id}/adsets"
    params = {'fields':'id,daily_budget,campaign_id','limit':500,'filtering':json.dumps([{'field':'effective_status','operator':'IN','value':['ACTIVE']}])}
    ar = {}; nc = {}; data = meta_api_get(url, params, token=get_token(ad_account_id))
    while data:
        for row in data.get('data',[]):
            asid=row.get('id',''); b=row.get('daily_budget','0'); cid=row.get('campaign_id','')
            if not asid: continue
            try: bi=int(float(b)) if b else 0
            except: bi=0
            if bi>0: ar[asid]=bi
            else: ar[asid]=0; nc[asid]=cid if cid else None
        nxt = data.get('paging',{}).get('next')
        if nxt:
            time.sleep(1)
            try: resp=req_lib.get(nxt,timeout=120); data=resp.json() if resp.status_code==200 else None
            except: data=None
        else: break
    nc = {k:v for k,v in nc.items() if v}
    if nc:
        uq = set(nc.values()); cb = {}
        for cid in uq:
            try:
                cd = meta_api_get(f"{META_BASE_URL}/{cid}",{'fields':'id,daily_budget'},token=get_token(ad_account_id))
                if cd:
                    try: cb[cid]=int(float(cd.get('daily_budget','0')))
                    except: cb[cid]=0
                time.sleep(0.5)
            except: pass
        fc=0
        for asid,cid in nc.items():
            if cb.get(cid,0)>0: ar[asid]=cb[cid]; fc+=1
        if fc>0: print(f"    \U0001f4cc ASC폴백: {fc}개 (캠페인 {len(uq)}개)")
    return ar

# =========================================================
# Mixpanel
# =========================================================
def fetch_mixpanel_data(from_date, to_date):
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {'from_date':from_date,'to_date':to_date,'event':json.dumps(MIXPANEL_EVENT_NAMES),'project_id':MIXPANEL_PROJECT_ID}
    print(f"  \U0001f4e1 Mixpanel: {from_date}~{to_date}")
    try:
        resp = req_lib.get(url,params=params,auth=(MIXPANEL_USERNAME,MIXPANEL_SECRET),timeout=300)
        if resp.status_code != 200: print(f"  \u274c Mixpanel {resp.status_code}"); return []
        lines = [l for l in resp.text.split('\n') if l.strip()]; print(f"  \U0001f4ca 이벤트: {len(lines)}건")
        data = []; s_am=0;s_val=0;s_both=0;s_none=0;s_결=0;s_ev=defaultdict(int)
        for line in lines:
            try:
                ev=json.loads(line); props=ev.get('properties',{}); ts=props.get('time',0)
                s_ev[ev.get('event','')]+=1
                ds = None
                if ts: dt_kst=datetime.fromtimestamp(ts,tz=timezone.utc)+timedelta(hours=9); ds=f"{dt_kst.year%100:02d}/{dt_kst.month:02d}/{dt_kst.day:02d}"
                ut=None
                for k in ['utm_term','UTM_Term','UTM Term']:
                    if k in props and props[k]: ut=clean_id(str(props[k]).strip()); break
                r결=0.0; r_am=0.0; r_val=0.0
                try: r결=float(props.get('결제금액',0) or 0)
                except: pass
                try: r_am=float(props.get('amount',0) or 0)
                except: pass
                try: r_val=float(props.get('value',0) or 0)
                except: pass
                revenue = r결 if r결>0 else r_am if r_am>0 else r_val if r_val>0 else 0.0
                if r결>0 and not r_am and not r_val: s_결+=1
                elif r_am and r_val: s_both+=1
                elif r_am: s_am+=1
                elif r_val: s_val+=1
                elif not r결: s_none+=1
                data.append({'distinct_id':props.get('distinct_id'),'time':ts,'date':ds,'utm_term':ut or '','amount':r_am,'결제금액':r결,'value_raw':r_val,'revenue':revenue,'서비스':props.get('서비스','')})
            except: pass
        print(f"  \u2705 파싱: {len(data)}건 | 결제금액={s_결} amount={s_am} value={s_val} 둘다={s_both} 없음={s_none}")
        print(f"  \U0001f4ca 이벤트별: {dict(s_ev)}")
        return data
    except Exception as e: print(f"  \u274c Mixpanel 오류: {e}"); return []

# =========================================================
# find_last_data_row / read_all_date_tabs
# =========================================================
def find_last_data_row(all_values, structure):
    ac = get_col_index(structure, 2); last=1; dr=[]; ce=0
    for idx,row in enumerate(all_values[1:], start=2):
        if not row: ce+=1; (None if ce<3 else (_ for _ in ()).throw(StopIteration)); continue
        cn=str(row[0]).strip() if row else ""
        asid=str(row[ac]).strip() if len(row)>ac else ""
        if cn in ["캠페인 이름","전체","합계","Total"] or (not cn and not asid): ce+=1
        else: ce=0; dr.append(row); last=idx
        if ce>=3: break
    return last, dr

# ★ 수정3: read_all_date_tabs - RIGHTMOST_TABS 스킵
def read_all_date_tabs(sh, analysis_tab_names, mp_value_map=None, mp_count_map=None):
    print("\n"+"="*60); print("\u2605 8.5단계: 전체 날짜탭 읽기"); print("="*60)
    all_ad_sets = defaultdict(lambda:{'campaign_name':'','adset_name':'','adset_id':'','dates':{}})
    all_budget_by_date = defaultdict(lambda:defaultdict(lambda:{'spend':0.0,'revenue':0.0}))
    all_master_raw_data=[]; all_date_objects={}; all_date_names=[]
    master_headers_local = ["Date"]+DATE_TAB_HEADERS
    analysis_set=set(analysis_tab_names); all_ws=sh.worksheets(); dtf=[]
    print(f"\n  \U0001f4cb 전체 워크시트: {len(all_ws)}개")
    sk_a=[]; sk_n=[]; sk_f=[]
    for ws_ex in all_ws:
        tn = ws_ex.title
        if tn in analysis_set: sk_a.append(tn); continue
        # ★ RIGHTMOST_TABS도 스킵
        if tn in RIGHTMOST_TABS: sk_n.append(tn); continue
        dt_obj = parse_date_tab(tn)
        if dt_obj is None:
            (sk_f if '/' in tn else sk_n).append(tn); continue
        dtf.append((dt_obj, ws_ex, tn))
    dtf.sort(key=lambda x:x[0])
    print(f"  \u2705 날짜탭: {len(dtf)}개 | 분석스킵: {len(sk_a)} | 비날짜: {len(sk_n)}")
    if sk_f: print(f"  \u274c 파싱실패: {sk_f}")
    if dtf: print(f"  \U0001f4c5 {dtf[0][2]} ~ {dtf[-1][2]}")
    _mv = mp_value_map or {}; _mc = mp_count_map or {}
    for dt_obj, ws_ex, tn in dtf:
        dk = f"{dt_obj.year%100:02d}/{dt_obj.month:02d}/{dt_obj.day:02d}"
        all_date_objects[dk]=dt_obj; all_date_names.append(dk)
        try:
            print(f"  \U0001f4d6 {dk}...", end=" ", flush=True)
            av = with_retry(ws_ex.get_all_values); time.sleep(0.3)
            if not av or len(av)<2: print("빈탭"); continue
            st = detect_tab_structure(av[0]); print(f"[{'구' if st=='old' else '신'}] ", end="", flush=True)
            _, sr = find_last_data_row(av, st); rc=0
            for row in sr:
                nr=normalize_row_to_new(row,st); cn=str(nr[0]).strip()
                if not cn or cn in ["캠페인 이름","전체","합계","Total"]: continue
                asn=str(nr[1]).strip();asid=str(nr[2]).strip();spend=_to_num(nr[3]);cpm=_to_num(nr[6]);uc=_to_num(nr[9])
                mpc=_mc.get((dk,asid),0);rev=_mv.get((dk,asid),0.0)
                if mpc==0 and rev==0.0: mpc=_to_num(nr[14]);rev=_to_num(nr[15])
                profit=rev-spend;roas=(rev/spend*100) if spend>0 else 0;cvr=(mpc/uc*100) if uc>0 and mpc>0 else 0
                if asid:
                    if not all_ad_sets[asid]['adset_id']: all_ad_sets[asid]={'campaign_name':cn,'adset_name':asn,'adset_id':asid,'dates':{}}
                    all_ad_sets[asid]['dates'][dk]={'profit':profit,'revenue':rev,'spend':spend,'cpm':cpm,'cvr':cvr,'unique_clicks':uc,'mpc':mpc}
                p=extract_product(asn); all_budget_by_date[dk][p]['spend']+=spend; all_budget_by_date[dk][p]['revenue']+=rev
                mrd=[dk]+list(nr); mrd[15]=int(mpc) if mpc>0 else ""; mrd[16]=round(rev) if rev>0 else ""
                mrd[17]=round(profit,0) if spend>0 else ""; mrd[18]=round(roas,1) if spend>0 and rev>0 else ""; mrd[19]=round(cvr,2) if cvr>0 else ""
                while len(mrd)<len(master_headers_local): mrd.append("")
                all_master_raw_data.append({'date':dk,'date_obj':dt_obj,'spend':spend,'row_data':mrd[:len(master_headers_local)]}); rc+=1
            print(f"{rc}행")
        except Exception as e: print(f"오류: {e}")
    print(f"\n  \u2705 날짜탭:{len(dtf)} | adset:{len(all_ad_sets)} | 마스터:{len(all_master_raw_data)}행")
    return all_ad_sets, all_budget_by_date, all_master_raw_data, all_date_objects, all_date_names

def diagnose_chart_coverage(sh, date_names, ad_sets, analysis_tabs_set):
    print("\n"+"="*60); print("\u2605 진단: 추이차트 커버리지"); print("="*60)
    all_tab_names = [ws.title for ws in sh.worksheets()]
    slash_tabs = [t for t in all_tab_names if '/' in t]
    pt=[]; ft=[]
    for t in slash_tabs:
        dt = parse_date_tab(t)
        if dt: pt.append((t,f"{dt.year%100:02d}/{dt.month:02d}/{dt.day:02d}",dt))
        else: ft.append(t)
    dns = set(date_names)
    nic = [(t,dk) for t,dk,dt in pt if dk not in dns]
    print(f"  슬래시탭:{len(slash_tabs)} 파싱:{len(pt)} 차트포함:{len(pt)-len(nic)}")
    if nic: print(f"  \u274c 누락: {nic}")
    if ft: print(f"  \u274c 파싱실패: {ft}")
    if date_names: print(f"  범위: {date_names[0]}~{date_names[-1]} ({len(date_names)}일)")
    return ft


def generate_date_tab_summary(rows, structure="new"):
    ts=0.0;tmp=0.0;tmpp=0.0;tr=0.0;tp=0.0;tuc=0.0
    sba={"본계정":0.0,"부계정":0.0,"3rd계정":0.0}
    ps=defaultdict(float);pr=defaultdict(float);pp=defaultdict(float)
    for row in rows:
        nr=normalize_row_to_new(row,structure);cn=str(nr[0]);asn=str(nr[1]);aid=str(nr[2])
        sp=_to_num(nr[3]);mp=_to_num(nr[13]);mpp=_to_num(nr[14]);rv=_to_num(nr[15]);pf=_to_num(nr[16]);uc=_to_num(nr[9])
        if not cn or cn in ["전체","합계","Total"]: continue
        ts+=sp;tmp+=mp;tmpp+=mpp;tr+=rv;tp+=pf;tuc+=uc
        if aid.startswith("12023"): sba["본계정"]+=sp
        elif aid.startswith("12024"): sba["부계정"]+=sp
        elif aid.startswith("6"): sba["3rd계정"]+=sp
        p=extract_product(asn); ps[p]+=sp;pr[p]+=rv;pp[p]+=pf
    troas=(tr/ts*100) if ts>0 else 0; tcvr=(tmpp/tuc*100) if tuc>0 else 0
    sp_list = sorted([p for p in ps if p in PRODUCT_KEYWORDS and (ps[p]>0 or pr[p]>0)], key=lambda p:ps[p], reverse=True)
    np_count=len(sp_list); NC=25; sci=9+np_count
    sd=[]; sd+=[[""]*NC]*3
    rt=[""]*NC; rt[14]="전체"; sd.append(rt)
    rh=[""]*NC; rh[11]="본계정";rh[12]="부계정";rh[13]="3rd 계정";rh[14]="지출 금액 (KRW)";rh[15]="구매 (메타)";rh[16]="구매 (믹스패널)";rh[17]="매출";rh[18]="이익";rh[19]="ROAS";rh[20]="CVR"; sd.append(rh)
    rv=[""]*NC; rv[11]=round(sba["본계정"]);rv[12]=round(sba["부계정"]);rv[13]=round(sba["3rd계정"]);rv[14]=round(ts);rv[15]=round(tmp);rv[16]=round(tmpp);rv[17]=round(tr);rv[18]=round(tp);rv[19]=round(troas,1);rv[20]=tcvr/100 if tcvr else 0; sd.append(rv)
    if np_count==0: return sd, 0
    tps=sum(ps[p] for p in sp_list);tpr=sum(pr[p] for p in sp_list);tpp=sum(pp[p] for p in sp_list)
    for tt,dt in [("제품별 ROAS","roas"),("제품별 순이익","profit"),("제품별 매출","revenue"),("제품별 순이익율","profit_margin"),("제품별 예산","spend"),("제품별 예산 비중","spend_ratio")]:
        sd.append([""]*NC); r=[""]*NC; r[12]=tt; sd.append(r)
        ph=[""]*NC
        for i,p in enumerate(sp_list): ph[9+i]=p
        ph[sci]="합"; sd.append(ph)
        pv=[""]*NC
        for i,p in enumerate(sp_list):
            if dt=="roas": pv[9+i]=pr[p]/ps[p] if ps[p]>0 else 0
            elif dt=="profit": pv[9+i]=round(pp[p])
            elif dt=="revenue": pv[9+i]=round(pr[p])
            elif dt=="profit_margin": pv[9+i]=pp[p]/pr[p] if pr[p]>0 else 0
            elif dt=="spend": pv[9+i]=round(ps[p])
            elif dt=="spend_ratio": pv[9+i]=ps[p]/tps if tps>0 else 0
        if dt=="roas": pv[sci]=tpr/tps if tps>0 else 0
        elif dt=="profit": pv[sci]=round(tpp)
        elif dt=="revenue": pv[sci]=round(tpr)
        elif dt=="profit_margin": pv[sci]=tpp/tpr if tpr>0 else 0
        elif dt=="spend": pv[sci]=round(tps)
        elif dt=="spend_ratio": pv[sci]=""
        sd.append(pv)
    return sd, np_count

def format_date_tab_summary(sh, ws, ssr, src, num_products=0):
    sid=ws.id; base=ssr-1
    CP={"red":0.957,"green":0.8,"blue":0.8};CO={"red":0.988,"green":0.898,"blue":0.804};CL={"red":0.851,"green":0.918,"blue":0.827}
    CG={"red":0.69,"green":0.7,"blue":0.698};CD={"red":0.6,"green":0.6,"blue":0.6};CB={"red":0,"green":0,"blue":0};CW={"red":1,"green":1,"blue":1};CY={"red":1,"green":1,"blue":0.8}
    fr=[]; hf={"textFormat":{"bold":True,"foregroundColor":CB},"horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE"}
    r=base+3; fr.append(create_format_request(sid,r,r+1,14,21,{"horizontalAlignment":"CENTER","textFormat":{"bold":True}}))
    fr.append({"mergeCells":{"range":{"sheetId":sid,"startRowIndex":r,"endRowIndex":r+1,"startColumnIndex":14,"endColumnIndex":21},"mergeType":"MERGE_ALL"}})
    r=base+4
    fr.append(create_format_request(sid,r,r+1,11,12,{**hf,"backgroundColor":CP})); fr.append(create_format_request(sid,r,r+1,12,13,{**hf,"backgroundColor":CO}))
    fr.append(create_format_request(sid,r,r+1,13,14,{**hf,"backgroundColor":CL})); fr.append(create_format_request(sid,r,r+1,14,20,{**hf,"backgroundColor":CG}))
    fr.append(create_format_request(sid,r,r+1,20,21,{**hf,"backgroundColor":CD}))
    r=base+5; fr.append(create_number_format_request(sid,r,r+1,11,20,"NUMBER","#,##0")); fr.append(create_number_format_request(sid,r,r+1,19,20,"NUMBER","#,##0.0"))
    fr.append(create_number_format_request(sid,r,r+1,20,21,"NUMBER","0.00%")); fr.append(create_border_request(sid,base+4,base+6,11,21))
    if num_products==0: return fr
    sc=9+num_products; pec=sc+1; phf={**hf,"backgroundColor":CG}; off=base+6
    for ti,(tn,nf) in enumerate([("ROAS","0.00%"),("순이익","[$₩-412]#,##0"),("매출","#,##0"),("순이익율","0.00%"),("예산","[$₩-412]#,##0"),("예산비중","0.00%")]):
        ts=off+ti*4
        fr.append(create_format_request(sid,ts+1,ts+2,12,pec,{"horizontalAlignment":"CENTER","textFormat":{"bold":True}}))
        fr.append({"mergeCells":{"range":{"sheetId":sid,"startRowIndex":ts+1,"endRowIndex":ts+2,"startColumnIndex":12,"endColumnIndex":pec},"mergeType":"MERGE_ALL"}})
        fr.append(create_format_request(sid,ts+2,ts+3,9,pec,phf)); fr.append(create_number_format_request(sid,ts+3,ts+4,9,pec,"NUMBER",nf))
        if ti in [1,2,4]:
            fr.append(create_format_request(sid,ts+3,ts+4,sc,sc+1,{"backgroundColor":CY,"textFormat":{"bold":True}}))
            fr.append(create_number_format_request(sid,ts+3,ts+4,sc,sc+1,"NUMBER","[$₩-412]#,##0"))
        fr.append(create_border_request(sid,ts+2,ts+4,9,pec))
    return fr


# =============================================================================
# 실행
# =============================================================================
print("\n"+"="*60); print("1단계: 광고 계정 설정"); print("="*60)
ALL_AD_ACCOUNTS = ["act_1054081590008088","act_2677707262628563"]
print(f"\U0001f4cb 광고 계정: {ALL_AD_ACCOUNTS}\n")

# 2: Meta
print("="*60); print(f"2단계: Meta Insights ({REFRESH_DAYS}일)"); print("="*60)
meta_date_data = defaultdict(list); msc = 0
for do in range(META_COLLECT_DAYS):
    td=TODAY-timedelta(days=do);ts=td.strftime('%Y-%m-%d');dk=f"{td.year%100:02d}/{td.month:02d}/{td.day:02d}"
    print(f"\n\U0001f4c5 {ts} ({dk})..."); dr=[]
    for acc in ALL_AD_ACCOUNTS:
        rows=fetch_meta_insights_daily(acc,ts)
        if rows: p=parse_single_day_insights(rows,dk,td); dr.extend(p); print(f"  \u2705 {acc}: {len(p)}건")
        else: print(f"  \u26a0\ufe0f {acc}: 0건")
        time.sleep(2)
    if dr: meta_date_data[dk]=dr; msc+=len(dr)
print(f"\n\u2705 Meta: {len(meta_date_data)}일, {msc}건\n")

# 2.5: 예산
print("="*60); print("2.5단계: 예산"); print("="*60)
adset_budget_map = {}
for acc in ALL_AD_ACCOUNTS:
    b=fetch_adset_budgets(acc); adset_budget_map.update(b); print(f"  {acc}: {len(b)}개"); time.sleep(2)
print(f"\u2705 예산: {len(adset_budget_map)}개\n")

# 3: Mixpanel
print("="*60); print(f"3단계: Mixpanel ({REFRESH_DAYS}일)"); print("="*60)
YESTERDAY=TODAY-timedelta(days=1); mp_to_today=TODAY.strftime('%Y-%m-%d')
mp_raw=[]
if REFRESH_DAYS>14:
    cs=DATA_REFRESH_START
    while cs<=YESTERDAY:
        ce=min(cs+timedelta(days=6),YESTERDAY); cd=fetch_mixpanel_data(cs.strftime('%Y-%m-%d'),ce.strftime('%Y-%m-%d'))
        mp_raw.extend(cd); cs=ce+timedelta(days=1); time.sleep(3)
else:
    if DATA_REFRESH_START<=YESTERDAY:
        cd=fetch_mixpanel_data(DATA_REFRESH_START.strftime('%Y-%m-%d'),YESTERDAY.strftime('%Y-%m-%d')); mp_raw.extend(cd); time.sleep(2)
td=fetch_mixpanel_data(mp_to_today,mp_to_today)
if td: mp_raw.extend(td)
print(f"\u2705 Mixpanel: {len(mp_raw)}건")
df=pd.DataFrame(mp_raw); mp_value_map={};mp_count_map={}
if len(df)>0:
    df=df[df['utm_term'].notna()&(df['utm_term']!='')&(df['utm_term']!='None')]
    df=df.sort_values('revenue',ascending=False)
    df_d=df.drop_duplicates(subset=['date','distinct_id','서비스'],keep='first')
    print(f"  utm_term:{len(df)} → 중복제거:{len(df_d)} | 매출:₩{int(df_d['revenue'].sum()):,}")
    for (d,ut),v in df_d.groupby(['date','utm_term'])['revenue'].sum().items():
        if d and ut: mp_value_map[(d,str(ut))]=v
    for (d,ut),c in df_d.groupby(['date','utm_term']).size().items():
        if d and ut: mp_count_map[(d,str(ut))]=c
print()

# 4: 기존 탭
print("="*60); print(f"4단계: 기존 탭 파악"); print("="*60)
refresh_date_keys=set()
for do in range(REFRESH_DAYS):
    td=TODAY-timedelta(days=do); refresh_date_keys.add(f"{td.year%100:02d}/{td.month:02d}/{td.day:02d}")
existing_sheets=sh.worksheets(); existing_refresh_tabs={}; new_refresh_dates=set(refresh_date_keys)
for ws_ex in existing_sheets:
    dt_obj=parse_date_tab(ws_ex.title)
    if dt_obj is None: continue
    tab_dk=f"{dt_obj.year%100:02d}/{dt_obj.month:02d}/{dt_obj.day:02d}"
    if tab_dk in refresh_date_keys: existing_refresh_tabs[tab_dk]=ws_ex; new_refresh_dates.discard(tab_dk)
print(f"  기존:{len(existing_refresh_tabs)} | 신규:{len(new_refresh_dates)}\n")

# 4.5: 환율
print("="*60); print("4.5단계: 환율"); print("="*60)
rrs = DATA_REFRESH_START-timedelta(days=7)
usd_krw_rates = fetch_daily_exchange_rates(rrs, TODAY, "USD")
twd_krw_rates = fetch_daily_exchange_rates(rrs, TODAY, "TWD")
print()

# 5: 병합
print("="*60); print("5단계: 병합"); print("="*60)
date_tab_rows=defaultdict(list);date_mp_by_adsetid=defaultdict(dict);new_date_names=[];product_count=defaultdict(int)
dtr=0;dmr=0;dmrev=0; debug_기타=set()
for dk in sorted(meta_date_data.keys(),key=lambda x:meta_date_data[x][0]['date_obj'] if meta_date_data[x] else datetime.min,reverse=True):
    rows=meta_date_data[dk]
    if not rows: continue
    dt=rows[0]['date_obj']; new_date_names.append(dk)
    fu=get_rate_for_date(usd_krw_rates,dk); ft=get_rate_for_date(twd_krw_rates,dk,FALLBACK_TWD_KRW)
    for mr in rows:
        asid=mr['adset_id']
        if not asid: continue
        dtr+=1; sp=mr['spend']*fu; cpm=mr['cpm']*fu; cpr=mr['cost_per_result']*fu; cpc=mr['cost_per_unique_click']*fu
        mpc=mp_count_map.get((dk,asid),0);mpv=mp_value_map.get((dk,asid),0.0)
        if mpc>0 or mpv>0: dmr+=1;dmrev+=mpv
        rv=float(mpv)*ft; date_mp_by_adsetid[dk][asid]={'mpc':mpc,'mpv':rv}
        pf=rv-sp;roas_c=(rv/sp*100) if sp>0 else 0;cvr_c=(mpc/mr['unique_clicks']*100) if mr['unique_clicks']>0 and mpc>0 else 0
        br=adset_budget_map.get(asid,0); bv=round(br/100*fu) if br and br>0 else ""
        tab_row=[mr['campaign_name'],mr['adset_name'],asid,sp,cpr,0,round(cpm,0),mr['reach'],mr['impressions'],mr['unique_clicks'],round(mr['unique_ctr'],2),round(cpc,0),round(mr['frequency'],2),mr['results'],
            mpc if mpc>0 else "",round(rv) if rv>0 else "",round(pf,0) if sp>0 else "",round(roas_c,1) if sp>0 and rv>0 else "",round(cvr_c,2) if mr['unique_clicks']>0 and mpc>0 else "",bv,"","",""]
        date_tab_rows[dk].append(tab_row); p=extract_product(mr['adset_name']); product_count[p]+=1
        if p=="기타": debug_기타.add(mr['adset_name'])
print(f"\u2705 {len(new_date_names)}일 | {dict(product_count)} | 매칭:{dmr}/{dtr}")
if debug_기타: print(f"  \u26a0\ufe0f '기타': {sorted(debug_기타)}")
print()

# 6: 분석탭 삭제
print("="*60); print("6단계: 분석탭 삭제"); print("="*60)
for sn in ["마스터탭","추이차트","증감액","추이차트(주간)","주간종합","주간종합_2","주간종합_3","예산","_temp","_temp_holder"]:
    try:
        old=sh.worksheet(sn)
        if len(sh.worksheets())<=1: with_retry(sh.add_worksheet,title="_tmp",rows=1,cols=1); time.sleep(1)
        sh.del_worksheet(old); print(f"  \u2705 '{sn}'"); time.sleep(2)
    except gspread.exceptions.WorksheetNotFound: pass
    except Exception as e: print(f"  \u26a0\ufe0f '{sn}': {e}"); time.sleep(3)
time.sleep(5)

# 7-A: 기존 탭 업데이트
print("\n"+"="*60); print("7단계: 기존 탭 업데이트 + 새 탭 생성"); print("="*60)
for dk in sorted(existing_refresh_tabs.keys()):
    ws_ex=existing_refresh_tabs[dk]; mp_data=date_mp_by_adsetid.get(dk,{}); nrd=date_tab_rows.get(dk,[])
    print(f"\n  \U0001f4dd {dk} 업데이트...")
    try:
        ws_ex=refresh_ws(sh,ws_ex); av=with_retry(ws_ex.get_all_values); time.sleep(0.5)
        if not av or len(av)<2: continue
        st=detect_tab_structure(av[0]); sid_ex=ws_ex.id
        trc=max(len(av)+50,200); fcs=get_col_index(st,3)
        try: with_retry(sh.batch_update,body={"requests":[{"unmergeCells":{"range":{"sheetId":sid_ex,"startRowIndex":1,"endRowIndex":trc,"startColumnIndex":0,"endColumnIndex":30}}},{"repeatCell":{"range":{"sheetId":sid_ex,"startRowIndex":1,"endRowIndex":trc,"startColumnIndex":fcs,"endColumnIndex":30},"cell":{"userEnteredFormat":{}},"fields":"userEnteredFormat.numberFormat"}}]}); time.sleep(1)
        except: pass
        aac=get_col_index(st,2); aim={}; ldr=1
        for i,row in enumerate(av[1:],start=2):
            if not row: continue
            cn=str(row[0]).strip() if row else ""; asid=str(row[aac]).strip() if len(row)>aac else ""
            if cn in ["캠페인 이름","전체","합계","Total"] or (not cn and not asid): continue
            if asid: aim[asid]=i-1
            ldr=i
        dei=ldr; bu=[]; uc=0; nba={}
        for tr in nrd:
            ac=str(tr[2]).strip() if len(tr)>2 else ""
            if ac: nba[ac]=tr
        ucl="S" if st=="new" else "U"; dcc=19 if st=="new" else 21
        for asid,ri in aim.items():
            rn=ri+1
            if asid not in nba:
                if asid in mp_data:
                    er=av[ri]; asc=get_col_index(st,3);auc=get_col_index(st,9);amc=get_col_index(st,14);acv=get_col_index(st,18)
                    sp=_to_num(er[asc]) if len(er)>asc else 0; ukc=_to_num(er[auc]) if len(er)>auc else 0
                    mpc=mp_data[asid]['mpc'];mpv=mp_data[asid]['mpv'];rev=float(mpv)
                    pf=rev-sp;rv=(rev/sp*100) if sp>0 and rev>0 else 0;cv=(mpc/ukc*100) if ukc>0 and mpc>0 else 0
                    bu.append({'range':f'{get_col_letter(amc)}{rn}:{get_col_letter(acv)}{rn}','values':[[int(mpc) if mpc>0 else "",round(rev) if rev>0 else "",round(pf,0) if sp>0 else "",round(rv,1) if sp>0 and rev>0 else "",round(cv,2) if ukc>0 and mpc>0 else ""]]})
                continue
            ntr=nba[asid]
            if st=="new":
                ur=list(ntr[:dcc])
                while len(ur)<dcc: ur.append("")
            else:
                ur=list(ntr[:3])+["",""]+list(ntr[3:19])
                while len(ur)<dcc: ur.append("")
                ur=ur[:dcc]
            bu.append({'range':f'A{rn}:{ucl}{rn}','values':[ur]}); uc+=1
        if bu:
            for i in range(0,len(bu),100): with_retry(ws_ex.batch_update,bu[i:i+100],value_input_option="USER_ENTERED"); time.sleep(1)
        # 예산
        bup=[]; abc=get_col_index(st,19); abl=get_col_letter(abc); fb=get_rate_for_date(usd_krw_rates,dk)
        for asid,ri in aim.items():
            if asid in adset_budget_map and adset_budget_map[asid]>0:
                bup.append({'range':f'{abl}{ri+1}','values':[[round(adset_budget_map[asid]/100*fb)]]})
        if bup:
            for i in range(0,len(bup),100): with_retry(ws_ex.batch_update,bup[i:i+100],value_input_option="USER_ENTERED"); time.sleep(1)
        # 신규 행
        ea=set(aim.keys()); nra=[]
        for tr in nrd:
            ac=str(tr[2]).strip() if len(tr)>2 else ""
            if ac and ac not in ea:
                if st=="old": or_=list(tr[:3])+["",""]+list(tr[3:]); or_=(or_+[""]*(25-len(or_)))[:25]; nra.append(or_)
                else: nra.append(tr)
        if nra:
            apc=get_col_index(st,16); nra.sort(key=lambda r:_to_num(r[apc]) if len(r)>apc else 0,reverse=True)
            with_retry(ws_ex.update,values=nra,range_name=f"A{dei+1}",value_input_option="USER_ENTERED"); time.sleep(1); dei+=len(nra)
        # 요약표
        av=with_retry(ws_ex.get_all_values); time.sleep(0.5); st=detect_tab_structure(av[0])
        ldr2,drs=find_last_data_row(av,st); ssr=ldr2+1
        NCF=len(DATE_TAB_HEADERS) if st=="new" else len(OLD_DATE_TAB_HEADERS)
        try:
            try: with_retry(sh.batch_update,body={"requests":[{"clearBasicFilter":{"sheetId":ws_ex.id}}]})
            except: pass
            with_retry(sh.batch_update,body={"requests":[{"setBasicFilter":{"filter":{"range":{"sheetId":ws_ex.id,"startRowIndex":0,"endRowIndex":ldr2,"startColumnIndex":0,"endColumnIndex":NCF}}}}]}); time.sleep(0.5)
        except: pass
        srows,nprod=generate_date_tab_summary(drs,structure=st)
        nr=ssr+len(srows)+10; tsr=len(av)
        if nr>tsr:
            try: with_retry(ws_ex.resize,rows=nr); time.sleep(0.5)
            except: pass
        cer=max(tsr+50,nr)
        try:
            ws_ex=refresh_ws(sh,ws_ex); clear_summary_conditional_formats(sh,ws_ex,ssr-1)
            with_retry(sh.batch_update,body={"requests":[
                {"unmergeCells":{"range":{"sheetId":ws_ex.id,"startRowIndex":ssr-1,"endRowIndex":cer,"startColumnIndex":0,"endColumnIndex":30}}},
                {"repeatCell":{"range":{"sheetId":ws_ex.id,"startRowIndex":ssr-1,"endRowIndex":cer,"startColumnIndex":0,"endColumnIndex":30},"cell":{"userEnteredFormat":{},"userEnteredValue":{}},"fields":"userEnteredFormat,userEnteredValue"}},
                {"updateBorders":{"range":{"sheetId":ws_ex.id,"startRowIndex":ssr-1,"endRowIndex":cer,"startColumnIndex":0,"endColumnIndex":30},"top":{"style":"NONE"},"bottom":{"style":"NONE"},"left":{"style":"NONE"},"right":{"style":"NONE"},"innerHorizontal":{"style":"NONE"},"innerVertical":{"style":"NONE"}}}
            ]}); time.sleep(0.5)
        except: pass
        with_retry(ws_ex.update,values=srows,range_name=f"A{ssr}",value_input_option="USER_ENTERED"); time.sleep(1)
        try:
            fr=format_date_tab_summary(sh,ws_ex,ssr,len(srows),num_products=nprod)
            if fr:
                for i in range(0,len(fr),50): with_retry(sh.batch_update,body={"requests":fr[i:i+50]}); time.sleep(1)
        except: pass
    except Exception as e: print(f"    \u26a0\ufe0f {dk}: {e}")
print(f"\u2705 기존 {len(existing_refresh_tabs)}개 완료")

# 7-B: 새 탭
print(f"\n--- 7-B: 새 탭 ({len(new_refresh_dates)}개) ---")
for dk in sorted(new_refresh_dates):
    rows=date_tab_rows.get(dk,[])
    if not rows: continue
    rows.sort(key=lambda r:_to_num(r[16]) if len(r)>16 else 0,reverse=True)
    print(f"  \U0001f4c5 {dk} ({len(rows)}개)")
    srows,nprod=generate_date_tab_summary(rows,structure="new")
    try:
        NC=len(DATE_TAB_HEADERS); ws_d=safe_add_worksheet(sh,dk,rows=len(rows)+len(srows)+5,cols=NC+2); time.sleep(1)
        sid_d=ws_d.id; de=len(rows)+1
        with_retry(ws_d.update,values=[DATE_TAB_HEADERS]+rows+srows,range_name="A1",value_input_option="USER_ENTERED")
        fa=[]
        fa.append(create_format_request(sid_d,0,1,0,3,{"textFormat":{"bold":True},"wrapStrategy":"WRAP","verticalAlignment":"MIDDLE"}))
        fa.append(create_format_request(sid_d,0,1,3,14,{"backgroundColor":{"red":0.937,"green":0.937,"blue":0.937},"textFormat":{"bold":True},"wrapStrategy":"WRAP","verticalAlignment":"MIDDLE"}))
        fa.append(create_format_request(sid_d,0,1,14,19,{"backgroundColor":{"red":0.6,"green":0.6,"blue":0.6},"textFormat":{"bold":True},"wrapStrategy":"WRAP","horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE"}))
        fa.append(create_format_request(sid_d,0,1,19,20,{"backgroundColor":{"red":0.851,"green":0.824,"blue":0.914},"textFormat":{"bold":True},"wrapStrategy":"WRAP","horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE"}))
        fa.append(create_format_request(sid_d,0,1,20,21,{"backgroundColor":{"red":0.706,"green":0.655,"blue":0.839},"textFormat":{"bold":True,"foregroundColor":{"red":1,"green":1,"blue":1}},"wrapStrategy":"WRAP","horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE"}))
        fa.append(create_format_request(sid_d,0,1,21,22,{"backgroundColor":{"red":0.6,"green":0,"blue":1},"textFormat":{"bold":True,"foregroundColor":{"red":1,"green":1,"blue":1}},"wrapStrategy":"WRAP","horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE"}))
        fa.append(create_format_request(sid_d,0,1,22,23,{"backgroundColor":{"red":1,"green":0.6,"blue":0},"textFormat":{"bold":True},"wrapStrategy":"WRAP","verticalAlignment":"MIDDLE"}))
        for sc,ec,p in [(14,15,"#,##0"),(15,16,"#,##0"),(16,17,"#,##0"),(17,18,"#,##0.0"),(18,19,"#,##0.00"),(19,20,"#,##0")]:
            fa.append(create_number_format_request(sid_d,1,de,sc,ec,"NUMBER",p))
        fa.append(create_number_format_request(sid_d,0,1,20,21,"NUMBER","0%")); fa.append(create_number_format_request(sid_d,1,de,20,21,"NUMBER","0%"))
        for ci,w in [(0,210),(1,207),(2,130)]: fa.append({"updateDimensionProperties":{"range":{"sheetId":sid_d,"dimension":"COLUMNS","startIndex":ci,"endIndex":ci+1},"properties":{"pixelSize":w},"fields":"pixelSize"}})
        fa.append({"setBasicFilter":{"filter":{"range":{"sheetId":sid_d,"startRowIndex":0,"endRowIndex":de,"startColumnIndex":0,"endColumnIndex":NC}}}})
        fa.append({"updateSheetProperties":{"properties":{"sheetId":sid_d,"gridProperties":{"frozenRowCount":1,"frozenColumnCount":3}},"fields":"gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}})
        for ct,v,bg,fc in [("NUMBER_GREATER","0",{"red":1,"green":0.949,"blue":0.8},None),("NUMBER_GREATER","50000",{"red":1,"green":0.898,"blue":0.6},None),("NUMBER_GREATER","100000",{"red":0.576,"green":0.769,"blue":0.49},None),("NUMBER_GREATER","200000",{"red":0,"green":1,"blue":0},None),("NUMBER_GREATER","300000",{"red":0,"green":1,"blue":1},None),("NUMBER_LESS","-10000",{"red":0.957,"green":0.8,"blue":0.8},None),("NUMBER_LESS","-50000",{"red":0.878,"green":0.4,"blue":0.4},{"red":0,"green":0,"blue":0})]:
            rf={"backgroundColor":bg}
            if fc: rf["textFormat"]={"foregroundColor":fc}
            fa.append({"addConditionalFormatRule":{"rule":{"ranges":[{"sheetId":sid_d,"startRowIndex":1,"endRowIndex":de,"startColumnIndex":16,"endColumnIndex":17}],"booleanRule":{"condition":{"type":ct,"values":[{"userEnteredValue":v}]},"format":rf}},"index":0}})
        fa.append({"addConditionalFormatRule":{"rule":{"ranges":[{"sheetId":sid_d,"startRowIndex":1,"endRowIndex":de,"startColumnIndex":16,"endColumnIndex":17}],"gradientRule":{"minpoint":{"color":{"red":0.902,"green":0.486,"blue":0.451},"type":"MIN"},"midpoint":{"color":{"red":1,"green":1,"blue":1},"type":"NUMBER","value":"0"},"maxpoint":{"color":{"red":0.341,"green":0.733,"blue":0.541},"type":"MAX"}}},"index":0}})
        for ct,v,bg in [("NUMBER_LESS","100",{"red":0.878,"green":0.4,"blue":0.4}),("NUMBER_GREATER_THAN_EQ","100",{"red":1,"green":0.851,"blue":0.4}),("NUMBER_GREATER_THAN_EQ","200",{"red":0.576,"green":0.769,"blue":0.49}),("NUMBER_GREATER_THAN_EQ","300",{"red":0,"green":1,"blue":1})]:
            fa.append({"addConditionalFormatRule":{"rule":{"ranges":[{"sheetId":sid_d,"startRowIndex":1,"endRowIndex":de,"startColumnIndex":17,"endColumnIndex":18}],"booleanRule":{"condition":{"type":ct,"values":[{"userEnteredValue":v}]},"format":{"backgroundColor":bg}}},"index":0}})
        fa.append({"addConditionalFormatRule":{"rule":{"ranges":[{"sheetId":sid_d,"startRowIndex":1,"endRowIndex":de,"startColumnIndex":18,"endColumnIndex":19}],"gradientRule":{"minpoint":{"color":{"red":1,"green":1,"blue":1},"type":"MIN"},"maxpoint":{"color":{"red":0.341,"green":0.733,"blue":0.541},"type":"MAX"}}},"index":0}})
        try: with_retry(sh.batch_update,body={"requests":fa}); time.sleep(2)
        except Exception as e: print(f"    \u26a0\ufe0f 서식: {e}")
        try:
            fr=format_date_tab_summary(sh,ws_d,len(rows)+2,len(srows),num_products=nprod)
            if fr:
                for i in range(0,len(fr),50): with_retry(sh.batch_update,body={"requests":fr[i:i+50]}); time.sleep(1)
        except: pass
    except Exception as e: print(f"  \u26a0\ufe0f {dk}: {e}")
print(f"\u2705 완료"); time.sleep(3)

# 7.5: 탭순서
print("\n"+"="*60); print("7.5단계: 탭 순서"); print("="*60)
reorder_tabs(sh)

# 8.5: 전체 읽기
mp_value_map_krw = {(d,ut):v*get_rate_for_date(twd_krw_rates,d,FALLBACK_TWD_KRW) for (d,ut),v in mp_value_map.items()}
ad_sets, budget_by_date, master_raw_data, date_objects, date_names = read_all_date_tabs(sh, ANALYSIS_TABS_SET, mp_value_map=mp_value_map_krw, mp_count_map=mp_count_map)
failed_tabs = diagnose_chart_coverage(sh, date_names, ad_sets, ANALYSIS_TABS_SET)

print("\n\u23f3 60초 대기..."); time.sleep(60)
master_headers = ["Date"]+DATE_TAB_HEADERS
apib = set()
for dk in date_names:
    for p in budget_by_date[dk]:
        if budget_by_date[dk][p]['spend']>0 or budget_by_date[dk][p]['revenue']>0: apib.add(p)
product_order = sorted([p for p in apib if p in PRODUCT_KEYWORDS], key=lambda p:sum(budget_by_date[dk][p]['spend'] for dk in date_names), reverse=True)
chart_dn=list(reversed(date_names)); chart_sd=chart_dn[:7]

# 5.5: 주간집계
print("\n5.5단계: 주간 집계")
week_groups=defaultdict(list);week_display_names={}
for t in date_names: do=date_objects[t];wk=get_week_range_short(do);week_groups[wk].append(t);week_display_names[wk]=get_week_range(do)
week_ranges=[(wr,get_week_monday(date_objects[week_groups[wr][0]])) for wr in week_groups]
week_ranges.sort(key=lambda x:x[1]); week_keys=[wr[0] for wr in week_ranges]
adset_weekly=defaultdict(lambda:{'campaign_name':'','adset_name':'','adset_id':'','weeks':{}})
for asid,d in ad_sets.items():
    adset_weekly[asid].update({k:d[k] for k in ['campaign_name','adset_name','adset_id']})
    for wk in week_keys:
        wp,wr_v,ws_v,wcs,wcc,wvs,wvc=0,0,0,0,0,0,0
        for dn in week_groups[wk]:
            if dn in d['dates']:
                dd=d['dates'][dn]; wp+=dd['profit'];wr_v+=dd['revenue'];ws_v+=dd['spend']
                cv=dd.get('cpm',0);(wcs:=wcs+cv,wcc:=wcc+1) if cv>0 else None
                vv=dd.get('cvr',0);(wvs:=wvs+vv,wvc:=wvc+1) if vv>0 else None
        if ws_v>0 or wr_v>0: adset_weekly[asid]['weeks'][wk]={'profit':wp,'revenue':wr_v,'spend':ws_v,'cpm':wcs/wcc if wcc>0 else 0,'cvr':wvs/wvc if wvc>0 else 0}
sorted_list=[{'campaign_name':d['campaign_name'],'adset_name':d['adset_name'],'adset_id':d['adset_id'],'data':d} for asid,d in ad_sets.items()]
sorted_list.sort(key=lambda it:tuple(it['data']['dates'].get(d,{}).get('spend',0) for d in chart_dn), reverse=True)
chart_wk=list(reversed(week_keys))

def _agg(items, days, key='profit'):
    t=0
    for d in days:
        for it in items:
            if d in it['data']['dates']: t+=it['data']['dates'][d].get(key,0)
    return t

def _agg_avg(items, days, key='cpm'):
    s,c=0,0
    for d in days:
        for it in items:
            if d in it['data']['dates']:
                v=it['data']['dates'][d].get(key,0)
                if v>0: s+=v;c+=1
    return s/c if c>0 else 0

# 9: 마스터탭
print("\n9단계: 마스터탭")
ws_m=safe_add_worksheet(sh,"마스터탭",rows=max(2000,len(master_raw_data)+100),cols=len(master_headers)+5); time.sleep(3)
master_raw_data.sort(key=lambda x:(x['date_obj'],-x['spend']),reverse=True)
for it in master_raw_data:
    while len(it['row_data'])<len(master_headers): it['row_data'].append("")
    it['row_data']=it['row_data'][:len(master_headers)]
mr_all=[master_headers]+[i['row_data'] for i in master_raw_data]
for i in range(0,len(mr_all),5000): with_retry(ws_m.update,values=mr_all[i:i+5000],range_name=f"A{i+1}",value_input_option="USER_ENTERED"); time.sleep(2)
try:
    with_retry(ws_m.format,'A1:T1',{'backgroundColor':{'red':0.9,'green':0.9,'blue':0.9},'textFormat':{'bold':True}})
    with_retry(sh.batch_update,body={"requests":[{"updateSheetProperties":{"properties":{"sheetId":ws_m.id,"gridProperties":{"frozenRowCount":1}},"fields":"gridProperties.frozenRowCount"}}]})
except: pass
print(f"\u2705 마스터탭 ({len(master_raw_data)}행)"); time.sleep(2)

# 10: 추이차트
print("\n10단계: 추이차트")
ws_t=safe_add_worksheet(sh,"추이차트",rows=1000,cols=len(chart_dn)+10); time.sleep(3)
dhw=[];sci=[]
for i,n in enumerate(chart_dn):
    do=date_objects[n];wd=WEEKDAY_NAMES[do.weekday()];dhw.append(f"{n}({wd})")
    if do.weekday()==6: sci.append(4+i)
hdr_t=['캠페인 이름','광고 세트 이름','광고 세트 ID','7일 평균']+dhw
sr=["종합","","",cell_text(_agg(sorted_list,chart_sd,'profit'),_agg(sorted_list,chart_sd,'revenue'),_agg(sorted_list,chart_sd,'spend'),_agg_avg(sorted_list,chart_sd,'cpm'),_agg_avg(sorted_list,chart_sd,'cvr'))]
for d in chart_dn: sr.append(cell_text(_agg(sorted_list,[d],'profit'),_agg(sorted_list,[d],'revenue'),_agg(sorted_list,[d],'spend'),_agg_avg(sorted_list,[d],'cpm'),_agg_avg(sorted_list,[d],'cvr')))
rt=[]
for it in sorted_list:
    r=[it['campaign_name'],it['adset_name'],it['adset_id']]
    tp,tr_v,ts_v=0,0,0;tcs,tcc,tvs,tvc=0,0,0,0
    for d in chart_sd:
        if d in it['data']['dates']:
            dd=it['data']['dates'][d];tp+=dd['profit'];tr_v+=dd['revenue'];ts_v+=dd['spend']
            cv=dd.get('cpm',0);(tcs:=tcs+cv,tcc:=tcc+1) if cv>0 else None
            vv=dd.get('cvr',0);(tvs:=tvs+vv,tvc:=tvc+1) if vv>0 else None
    r.append(cell_text(tp,tr_v,ts_v,tcs/tcc if tcc>0 else 0,tvs/tvc if tvc>0 else 0))
    for d in chart_dn:
        if d in it['data']['dates']:
            dd=it['data']['dates'][d]; r.append(cell_text(dd['profit'],dd['revenue'],dd['spend'],dd.get('cpm',0),dd.get('cvr',0)))
        else: r.append('')
    rt.append(r)
with_retry(ws_t.update,values=[hdr_t]+[sr]+rt,range_name="A1",value_input_option="USER_ENTERED"); time.sleep(3)
ws_t=refresh_ws(sh,ws_t); apply_trend_chart_formatting(sh,ws_t,hdr_t,len(rt),sunday_col_indices=sci)
try: ws_t=refresh_ws(sh,ws_t); apply_c2_label_formatting(sh,ws_t)
except: pass
time.sleep(30)

# 11: 추이차트(주간)
print("\n11단계: 추이차트(주간)")
ws_tw=safe_add_worksheet(sh,"추이차트(주간)",rows=1000,cols=len(chart_wk)+10); time.sleep(3)
hdr_w=['캠페인 이름','광고 세트 이름','광고 세트 ID','전체 평균']+[week_display_names[wk] for wk in chart_wk]
srw=["종합","",""]
tap,tar,tas,tacs,tacc,tavs,tavc=0,0,0,0,0,0,0
for wk in chart_wk:
    for it in sorted_list:
        wd=adset_weekly[it['adset_id']]['weeks'].get(wk,{})
        if wd: tap+=wd['profit'];tar+=wd['revenue'];tas+=wd['spend'];cv=wd.get('cpm',0);(tacs:=tacs+cv,tacc:=tacc+1) if cv>0 else None;vv=wd.get('cvr',0);(tavs:=tavs+vv,tavc:=tavc+1) if vv>0 else None
srw.append(cell_text(tap,tar,tas,tacs/tacc if tacc>0 else 0,tavs/tavc if tavc>0 else 0))
for wk in chart_wk:
    wp,wr_v,ws_v,wcs,wcc,wvs,wvc=0,0,0,0,0,0,0
    for it in sorted_list:
        wd=adset_weekly[it['adset_id']]['weeks'].get(wk,{})
        if wd: wp+=wd['profit'];wr_v+=wd['revenue'];ws_v+=wd['spend'];cv=wd.get('cpm',0);(wcs:=wcs+cv,wcc:=wcc+1) if cv>0 else None;vv=wd.get('cvr',0);(wvs:=wvs+vv,wvc:=wvc+1) if vv>0 else None
    srw.append(cell_text(wp,wr_v,ws_v,wcs/wcc if wcc>0 else 0,wvs/wvc if wvc>0 else 0))
rtw=[]
for it in sorted_list:
    r=[it['campaign_name'],it['adset_name'],it['adset_id']]
    tp,tr_v,ts_v,tcs,tcc,tvs,tvc=0,0,0,0,0,0,0
    for wk in chart_wk:
        wd=adset_weekly[it['adset_id']]['weeks'].get(wk,{})
        if wd: tp+=wd['profit'];tr_v+=wd['revenue'];ts_v+=wd['spend'];cv=wd.get('cpm',0);(tcs:=tcs+cv,tcc:=tcc+1) if cv>0 else None;vv=wd.get('cvr',0);(tvs:=tvs+vv,tvc:=tvc+1) if vv>0 else None
    r.append(cell_text(tp,tr_v,ts_v,tcs/tcc if tcc>0 else 0,tvs/tvc if tvc>0 else 0))
    for wk in chart_wk:
        wd=adset_weekly[it['adset_id']]['weeks'].get(wk,{})
        r.append(cell_text(wd['profit'],wd['revenue'],wd['spend'],wd.get('cpm',0),wd.get('cvr',0)) if wd else '')
    rtw.append(r)
with_retry(ws_tw.update,values=[hdr_w]+[srw]+rtw,range_name="A1",value_input_option="USER_ENTERED"); time.sleep(3)
ws_tw=refresh_ws(sh,ws_tw); apply_trend_chart_formatting(sh,ws_tw,hdr_w,len(rtw),format_col_end=3+1+WEEKLY_TREND_REFRESH_WEEKS)
try: ws_tw=refresh_ws(sh,ws_tw); apply_c2_label_formatting(sh,ws_tw)
except: pass
time.sleep(30)

# 12: 증감액
print("\n12단계: 증감액")
ws_c=safe_add_worksheet(sh,"증감액",rows=1000,cols=len(chart_dn)+10); time.sleep(3)
hdr_c=['캠페인 이름','광고 세트 이름','광고 세트 ID','7일 평균']+chart_dn
t7r=_agg(sorted_list,chart_sd,'revenue');t7s=_agg(sorted_list,chart_sd,'spend');t7roas=(t7r/t7s*100) if t7s>0 else 0
fds=_agg(sorted_list,[chart_sd[-1]],'spend') if len(chart_sd)>=2 else 0; lds=_agg(sorted_list,[chart_sd[0]],'spend') if len(chart_sd)>=2 else 0
sdc=((lds-fds)/fds*100) if fds>0 else 0
src=["종합","","",cell_text_change(t7roas,sdc,t7s,_agg_avg(sorted_list,chart_sd,'cpm'),_agg_avg(sorted_list,chart_sd,'cvr'))]
for i,d in enumerate(chart_dn):
    dr=_agg(sorted_list,[d],'revenue');ds_v=_agg(sorted_list,[d],'spend');d_roas=(dr/ds_v*100) if ds_v>0 else 0
    chg=0
    if i<len(chart_dn)-1: ps=_agg(sorted_list,[chart_dn[i+1]],'spend'); chg=((ds_v-ps)/ps*100) if ps>0 else 0
    src.append(cell_text_change(d_roas,chg,ds_v,_agg_avg(sorted_list,[d],'cpm'),_agg_avg(sorted_list,[d],'cvr')))
rtc=[]
for it in sorted_list:
    r=[it['campaign_name'],it['adset_name'],it['adset_id']]
    tr_v,ts_v=0,0;tcpm,tcpc,tcvr,tcvc=0,0,0,0
    for d in chart_sd:
        if d in it['data']['dates']:
            dd=it['data']['dates'][d];tr_v+=dd['revenue'];ts_v+=dd['spend']
            cv=dd.get('cpm',0);(tcpm:=tcpm+cv,tcpc:=tcpc+1) if cv>0 else None
            vv=dd.get('cvr',0);(tcvr:=tcvr+vv,tcvc:=tcvc+1) if vv>0 else None
    ar=(tr_v/ts_v*100) if ts_v>0 else 0
    fs=it['data']['dates'].get(chart_sd[-1],{}).get('spend',0) if len(chart_sd)>=2 else 0
    ls=it['data']['dates'].get(chart_sd[0],{}).get('spend',0) if len(chart_sd)>=2 else 0
    cp=((ls-fs)/fs*100) if fs>0 else 0
    r.append(cell_text_change(ar,cp,ts_v,tcpm/tcpc if tcpc>0 else 0,tcvr/tcvc if tcvc>0 else 0))
    for i,d in enumerate(chart_dn):
        if d in it['data']['dates']:
            dd=it['data']['dates'][d];roas=(dd['revenue']/dd['spend']*100) if dd['spend']>0 else 0
            chg=0
            if i<len(chart_dn)-1: ps=it['data']['dates'].get(chart_dn[i+1],{}).get('spend',0); chg=((dd['spend']-ps)/ps*100) if ps>0 else 0
            r.append(cell_text_change(roas,chg,dd['spend'],dd.get('cpm',0),dd.get('cvr',0)))
        else: r.append('')
    rtc.append(r)
with_retry(ws_c.update,values=[hdr_c]+[src]+rtc,range_name="A1",value_input_option="USER_ENTERED"); time.sleep(3)
ws_c=refresh_ws(sh,ws_c); apply_trend_chart_formatting(sh,ws_c,hdr_c,len(rtc),is_change_tab=True); time.sleep(30)

# 13: 예산
print("\n13단계: 예산")
bw=safe_add_worksheet(sh,"예산",rows=1000,cols=len(chart_dn)+10); time.sleep(3)
br=[[""] + chart_dn]
br.append(["전체 쓴돈"]+[sum(budget_by_date[d][p]['spend'] for p in product_order) for d in chart_dn])
br.append(["전체 번돈"]+[sum(budget_by_date[d][p]['revenue'] for p in product_order) for d in chart_dn])
br.append(["전체 순이익"]+[sum(budget_by_date[d][p]['revenue']-budget_by_date[d][p]['spend'] for p in product_order) for d in chart_dn])
br.append(["ROAS"]+[(sum(budget_by_date[d][p]['revenue'] for p in product_order)/sum(budget_by_date[d][p]['spend'] for p in product_order)*100) if sum(budget_by_date[d][p]['spend'] for p in product_order)>0 else 0 for d in chart_dn])
for label,key in [("쓴돈","spend"),("번돈","revenue")]:
    br.append([""]*(len(chart_dn)+1));br.append([f"{label} - 제품별"]+[""]*len(chart_dn))
    for p in product_order: br.append([p]+[budget_by_date[d][p][key] for d in chart_dn])
br.append([""]*(len(chart_dn)+1));br.append(["순이익 - 제품별"]+[""]*len(chart_dn))
for p in product_order: br.append([p]+[budget_by_date[d][p]['revenue']-budget_by_date[d][p]['spend'] for d in chart_dn])
with_retry(bw.update,values=br,range_name="A1",value_input_option="RAW"); print("\u2705 예산"); time.sleep(3)

# 14~17: 주간종합
print("\n14단계: 주간종합 준비")
month_groups=defaultdict(list)
for t in date_names: do=date_objects[t]; month_groups[f"{do.year}년 {do.month}월"].append(t)
month_names_list=sorted(month_groups.keys(),key=lambda x:(int(x.split('년')[0]),int(x.split('년')[1].replace('월','').strip())),reverse=True)
daily_data=defaultdict(lambda:{'spend':0,'revenue':0,'profit':0});daily_product_data=defaultdict(lambda:defaultdict(lambda:{'spend':0,'revenue':0,'profit':0}))
for t in date_names:
    for it in sorted_list:
        if t in it['data']['dates']:
            dd=it['data']['dates'][t];daily_data[t]['spend']+=dd['spend'];daily_data[t]['revenue']+=dd['revenue'];daily_data[t]['profit']+=dd['profit']
            p=extract_product(it['adset_name']);daily_product_data[t][p]['spend']+=dd['spend'];daily_product_data[t][p]['revenue']+=dd['revenue'];daily_product_data[t][p]['profit']+=dd['profit']
wsd={};wps={}
for wk in week_keys:
    wsd[wk]={'spend':0,'revenue':0,'profit':0};wps[wk]=defaultdict(lambda:{'spend':0,'revenue':0,'profit':0})
    for dn in week_groups[wk]:
        for k in ['spend','revenue','profit']: wsd[wk][k]+=daily_data[dn][k]
        for p in daily_product_data[dn]:
            for k in ['spend','revenue','profit']: wps[wk][p][k]+=daily_product_data[dn][p][k]
msd={};mps={}
for mk in month_names_list:
    msd[mk]={'spend':0,'revenue':0,'profit':0};mps[mk]=defaultdict(lambda:{'spend':0,'revenue':0,'profit':0})
    for dn in month_groups[mk]:
        for k in ['spend','revenue','profit']: msd[mk][k]+=daily_data[dn][k]
        for p in daily_product_data[dn]:
            for k in ['spend','revenue','profit']: mps[mk][p][k]+=daily_product_data[dn][p][k]
afp=set()
for dn in date_names:
    for p in daily_product_data[dn]:
        if daily_product_data[dn][p]['spend']>0 or daily_product_data[dn][p]['revenue']>0: afp.add(p)
products = sorted([p for p in afp if p in PRODUCT_KEYWORDS], key=lambda p:sum(daily_product_data[dn][p]['spend'] for dn in date_names), reverse=True)
SUMMARY_PRODUCTS = products

print("\n15단계: 주간종합")
ws_ws=safe_add_worksheet(sh,"주간종합",rows=2000,cols=20); time.sleep(3)
sid_ws=ws_ws.id;fr_ws=[];ar_ws=[];cr_ws=0
def ccb(pn,d,pd,sid,cr,fr,im=False):
    block=[];bs=cr;rv=(d['revenue']/d['spend']) if d['spend']>0 else 0;hb=COLORS["dark_blue"] if im else COLORS["dark_gray"];cb=COLORS["navy"] if im else COLORS["black"];nc=len(products)+2
    block.append([pn]+[""]*(nc-1));fr.append(create_format_request(sid,cr,cr+1,0,nc,get_cell_format(hb,COLORS["white"],bold=True)));cr+=1
    block.append(["지출 금액","구매(메타)","구매(믹스패널)","매출","이익","ROAS","CVR","전체"]+[""]*(nc-8));fr.append(create_format_request(sid,cr,cr+1,0,8,get_cell_format(cb,COLORS["white"],bold=True)));cr+=1
    block.append([d['spend'],0,0,d['revenue'],d['profit'],rv,0,""]+[""]*(nc-8));fr.append(create_format_request(sid,cr,cr+1,0,8,get_cell_format(COLORS["light_gray"])))
    fr.append(create_number_format_request(sid,cr,cr+1,0,1,"NUMBER","#,##0"));fr.append(create_number_format_request(sid,cr,cr+1,3,5,"NUMBER","#,##0"));fr.append(create_number_format_request(sid,cr,cr+1,5,7,"PERCENT","0.00%"));cr+=1
    block.append([""]+products+["합"]);fr.append(create_format_request(sid,cr,cr+1,1,len(products)+1,get_cell_format(cb,COLORS["white"],bold=True)));fr.append(create_format_request(sid,cr,cr+1,len(products)+1,len(products)+2,get_cell_format(COLORS["light_yellow"],bold=True)));cr+=1
    for lb,dk,ft,fp in [("제품별 ROAS","roas","PERCENT","0.00%"),("제품별 순이익","profit","NUMBER","#,##0"),("제품별 매출","revenue","NUMBER","#,##0"),("제품별 예산","spend","NUMBER","#,##0"),("제품별 예산 비중","ratio","PERCENT","0.00%")]:
        r=[lb];tsp=sum(pd[p]['spend'] for p in products);trv=sum(pd[p]['revenue'] for p in products)
        for p in products:
            if dk=="roas":r.append((pd[p]['revenue']/pd[p]['spend']) if pd[p]['spend']>0 else 0)
            elif dk=="ratio":r.append((pd[p]['spend']/tsp) if tsp>0 else 0)
            else:r.append(pd[p][dk])
        r.append((trv/tsp if tsp>0 else 0) if dk=="roas" else 1.0 if dk=="ratio" else sum(pd[p][dk] for p in products))
        block.append(r);fr.append(create_format_request(sid,cr,cr+1,0,1,get_cell_format(COLORS["light_gray2"],bold=True)));fr.append(create_format_request(sid,cr,cr+1,len(products)+1,len(products)+2,get_cell_format(COLORS["light_yellow"])));fr.append(create_number_format_request(sid,cr,cr+1,1,len(products)+2,ft,fp));cr+=1
    fr.append(create_border_request(sid,bs,cr,0,len(products)+2));block.append([""]*nc);cr+=1
    return block,cr
for mk in month_names_list:
    yr=int(mk.split('년')[0]);mn=int(mk.split('년')[1].replace('월','').strip())
    dim=sorted(month_groups[mk],key=lambda x:date_objects[x])
    mr_d=get_month_range_display(date_objects[dim[0]],date_objects[dim[-1]]).replace("'","")
    b,cr_ws=ccb(f"'{mk} ({mr_d})",msd[mk],mps[mk],sid_ws,cr_ws,fr_ws,im=True);ar_ws.extend(b)
    mw=[wk for wk in week_keys if any(date_objects[d].year==yr and date_objects[d].month==mn for d in week_groups[wk])];mw.reverse()
    for wk in mw: b,cr_ws=ccb(week_display_names[wk],wsd[wk],wps[wk],sid_ws,cr_ws,fr_ws);ar_ws.extend(b)
with_retry(ws_ws.update,values=ar_ws,range_name="A1",value_input_option="USER_ENTERED"); time.sleep(3)
try:
    for i in range(0,len(fr_ws),100): with_retry(sh.batch_update,body={"requests":fr_ws[i:i+100]}); time.sleep(1)
except: pass
try:
    cw=[{"updateDimensionProperties":{"range":{"sheetId":sid_ws,"dimension":"COLUMNS","startIndex":0,"endIndex":1},"properties":{"pixelSize":150},"fields":"pixelSize"}}]
    for ci in range(1,11): cw.append({"updateDimensionProperties":{"range":{"sheetId":sid_ws,"dimension":"COLUMNS","startIndex":ci,"endIndex":ci+1},"properties":{"pixelSize":100},"fields":"pixelSize"}})
    with_retry(sh.batch_update,body={"requests":cw})
except: pass
print("\u2705 주간종합"); time.sleep(3)

print("\n16단계: 주간종합_2")
ws2=safe_add_worksheet(sh,"주간종합_2",rows=2000,cols=20); time.sleep(3)
sid2=ws2.id;fr2=[];ar2=[];cr2=0;npc=len(products)+3;stl=[]
for mk in month_names_list:
    yr=int(mk.split('년')[0]);mn=int(mk.split('년')[1].replace('월','').strip());d=msd[mk];roas=(d['revenue']/d['spend']) if d['spend']>0 else 0
    stl.append({'period':f"'{mk}",'type':'월별','spend':d['spend'],'revenue':d['revenue'],'profit':d['profit'],'roas':roas,'im':True,'mk':mk,'yr':yr,'mn':mn})
    mw=[wk for wk in week_keys if any(date_objects[dn].year==yr and date_objects[dn].month==mn for dn in week_groups[wk])];mw.reverse()
    for wk in mw: wd=wsd[wk];wr=(wd['revenue']/wd['spend']) if wd['spend']>0 else 0;stl.append({'period':week_display_names[wk],'type':'주간','spend':wd['spend'],'revenue':wd['revenue'],'profit':wd['profit'],'roas':wr,'im':False,'mk':None,'wk':wk})
ar2.append(["\U0001f4ca 기간별 전체 요약"]+[""]*6);fr2.append(create_format_request(sid2,cr2,cr2+1,0,7,get_cell_format(COLORS["navy"],COLORS["white"],bold=True)));cr2+=1
ar2.append(["기간","유형","지출금액","매출","이익","ROAS","CVR"]);fr2.append(create_format_request(sid2,cr2,cr2+1,0,7,get_cell_format(COLORS["dark_blue"],COLORS["white"],bold=True)));cr2+=1;t1s=cr2
for rd in stl:
    ar2.append([rd['period'],rd['type'],rd['spend'],rd['revenue'],rd['profit'],rd['roas'],0]);bg=COLORS["light_blue"] if rd['im'] else COLORS["light_gray"]
    fr2.append(create_format_request(sid2,cr2,cr2+1,0,7,get_cell_format(bg)));fr2.append(create_number_format_request(sid2,cr2,cr2+1,2,5,"NUMBER","#,##0"));fr2.append(create_number_format_request(sid2,cr2,cr2+1,5,7,"PERCENT","0.00%"));cr2+=1
fr2.append(create_border_request(sid2,t1s-1,cr2,0,7));ar2+=[[""]*(npc)]*2;cr2+=2
for tt,tc,dk in [("\U0001f4c8 제품별 ROAS",COLORS["dark_green"],"roas"),("\U0001f4b0 제품별 순이익",COLORS["dark_green"],"profit"),("\U0001f4b5 제품별 매출",COLORS["orange"],"revenue"),("\U0001f4b8 제품별 예산",COLORS["purple"],"spend"),("\U0001f4ca 제품별 예산 비중",COLORS["purple"],"ratio")]:
    ar2.append([tt]+[""]*(npc-1));fr2.append(create_format_request(sid2,cr2,cr2+1,0,npc,get_cell_format(tc,COLORS["white"],bold=True)));cr2+=1
    ar2.append(["기간","유형"]+products+["합계"]);fr2.append(create_format_request(sid2,cr2,cr2+1,0,npc,get_cell_format(COLORS["dark_gray"],COLORS["white"],bold=True)));cr2+=1;tds=cr2
    for rd in stl:
        pd_r=mps.get(rd['mk'],defaultdict(lambda:{'spend':0,'revenue':0,'profit':0})) if rd['im'] else wps.get(rd.get('wk',''),defaultdict(lambda:{'spend':0,'revenue':0,'profit':0}))
        r=[rd['period'],rd['type']];tsp=sum(pd_r[p]['spend'] for p in products);trv=sum(pd_r[p]['revenue'] for p in products)
        for p in products:
            if dk=="roas":r.append((pd_r[p]['revenue']/pd_r[p]['spend']) if pd_r[p]['spend']>0 else 0)
            elif dk=="ratio":r.append((pd_r[p]['spend']/tsp) if tsp>0 else 0)
            else:r.append(pd_r[p][dk])
        r.append((trv/tsp if tsp>0 else 0) if dk=="roas" else 1.0 if dk=="ratio" else sum(pd_r[p][dk] for p in products))
        ar2.append(r);bg=COLORS["light_blue"] if rd['im'] else COLORS["light_gray"];fr2.append(create_format_request(sid2,cr2,cr2+1,0,npc,get_cell_format(bg)))
        ft="PERCENT" if dk in ["roas","ratio"] else "NUMBER";fp="0.00%" if dk in ["roas","ratio"] else "#,##0"
        fr2.append(create_number_format_request(sid2,cr2,cr2+1,2,npc,ft,fp));cr2+=1
    fr2.append(create_border_request(sid2,tds-1,cr2,0,npc));ar2+=[[""]*(npc)]*2;cr2+=2
with_retry(ws2.update,values=ar2,range_name="A1",value_input_option="USER_ENTERED"); time.sleep(3)
try:
    for i in range(0,len(fr2),100): with_retry(sh.batch_update,body={"requests":fr2[i:i+100]}); time.sleep(1)
except: pass
print("\u2705 주간종합_2"); time.sleep(3)

print("\n17단계: 주간종합_3 (일별)")
ws3=safe_add_worksheet(sh,"주간종합_3",rows=3000,cols=20); time.sleep(3)
sid3=ws3.id;fr3=[];ar3=[];cr3=0;ndc=len(products)+4;dsr=[]
for t in reversed(date_names):
    do=date_objects[t];d=daily_data[t];roas=(d['revenue']/d['spend']) if d['spend']>0 else 0;wd=WEEKDAY_NAMES[do.weekday()]
    dsr.append({'period':f"'{do.month}.{do.day}({wd})",'weekday':wd,'spend':d['spend'],'revenue':d['revenue'],'profit':d['profit'],'roas':roas,'tab_name':t})
ar3.append(["\U0001f4ca 일별 전체 요약"]+[""]*7);fr3.append(create_format_request(sid3,cr3,cr3+1,0,8,get_cell_format(COLORS["navy"],COLORS["white"],bold=True)));cr3+=1
ar3.append(["날짜","요일","지출금액","매출","이익","ROAS","CVR",""]);fr3.append(create_format_request(sid3,cr3,cr3+1,0,8,get_cell_format(COLORS["dark_blue"],COLORS["white"],bold=True)));cr3+=1;t1s3=cr3
for rd in dsr:
    ar3.append([rd['period'],rd['weekday'],rd['spend'],rd['revenue'],rd['profit'],rd['roas'],0,""]);bg=COLORS["light_blue"] if rd['weekday'] in ['토','일'] else COLORS["light_gray"]
    fr3.append(create_format_request(sid3,cr3,cr3+1,0,8,get_cell_format(bg)));fr3.append(create_number_format_request(sid3,cr3,cr3+1,2,5,"NUMBER","#,##0"));fr3.append(create_number_format_request(sid3,cr3,cr3+1,5,7,"PERCENT","0.00%"));cr3+=1
fr3.append(create_border_request(sid3,t1s3-1,cr3,0,8));ar3+=[[""]*(ndc)]*2;cr3+=2
for tt,tc,dk in [("\U0001f4c8 일별 제품별 ROAS",COLORS["dark_green"],"roas"),("\U0001f4b0 일별 제품별 순이익",COLORS["dark_green"],"profit"),("\U0001f4b5 일별 제품별 매출",COLORS["orange"],"revenue"),("\U0001f4b8 일별 제품별 예산",COLORS["purple"],"spend"),("\U0001f4ca 일별 제품별 예산 비중",COLORS["purple"],"ratio")]:
    ar3.append([tt]+[""]*(ndc-1));fr3.append(create_format_request(sid3,cr3,cr3+1,0,ndc,get_cell_format(tc,COLORS["white"],bold=True)));cr3+=1
    ar3.append(["날짜","요일"]+products+["합계"]);fr3.append(create_format_request(sid3,cr3,cr3+1,0,ndc,get_cell_format(COLORS["dark_gray"],COLORS["white"],bold=True)));cr3+=1;tds=cr3
    for rd in dsr:
        pd_r=daily_product_data[rd['tab_name']];r=[rd['period'],rd['weekday']]
        tsp=sum(pd_r[p]['spend'] for p in products);trv=sum(pd_r[p]['revenue'] for p in products)
        for p in products:
            if dk=="roas":r.append((pd_r[p]['revenue']/pd_r[p]['spend']) if pd_r[p]['spend']>0 else 0)
            elif dk=="ratio":r.append((pd_r[p]['spend']/tsp) if tsp>0 else 0)
            else:r.append(pd_r[p][dk])
        r.append((trv/tsp if tsp>0 else 0) if dk=="roas" else 1.0 if dk=="ratio" else sum(pd_r[p][dk] for p in products))
        ar3.append(r);bg=COLORS["light_blue"] if rd['weekday'] in ['토','일'] else COLORS["light_gray"];fr3.append(create_format_request(sid3,cr3,cr3+1,0,ndc,get_cell_format(bg)))
        ft="PERCENT" if dk in ["roas","ratio"] else "NUMBER";fp="0.00%" if dk in ["roas","ratio"] else "#,##0"
        fr3.append(create_number_format_request(sid3,cr3,cr3+1,2,ndc,ft,fp));cr3+=1
    fr3.append(create_border_request(sid3,tds-1,cr3,0,ndc));ar3+=[[""]*(ndc)]*2;cr3+=2
with_retry(ws3.update,values=ar3,range_name="A1",value_input_option="USER_ENTERED"); time.sleep(3)
try:
    for i in range(0,len(fr3),100): with_retry(sh.batch_update,body={"requests":fr3[i:i+100]}); time.sleep(1)
except: pass
print("\u2705 주간종합_3"); time.sleep(3)

# 18: 최종 탭 순서
print("\n"+"="*60); print("18단계: 최종 탭 순서"); print("="*60)
reorder_tabs(sh)

print("\n"+"="*60); print("\u2705 완료!"); print("="*60)
print(f"\U0001f504 갱신: {REFRESH_DAYS}일 | 기존:{len(existing_refresh_tabs)} | 신규:{len(new_refresh_dates)}")
print(f"\U0001f4ca 분석: {len(date_names)}일 | 마스터:{len(master_raw_data)}행 | {len(week_keys)}주/{len(month_names_list)}월")
print(f"\n\U0001f4ca {SPREADSHEET_URL}")
