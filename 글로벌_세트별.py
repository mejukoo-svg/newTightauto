# -*- coding: utf-8 -*-
# v3-ad 통합 코드 (★ v26 - 나라 판별 강화: 캠페인/세트 이름 기반 서브스트링+한글 매칭):
#   - ★ v26: _detect_currency_from_name 강화 — 서브스트링 + 한글("일본","대만","홍콩","국내") 매칭
#   - ★ v26: 캠페인 이름에서도 나라 판별 (세트 이름에 없으면 캠페인 이름 검사)
#   - ★ v25: detect_currency()에 campaign_name 파라미터 추가 → 세트명에 없으면 캠페인명도 검사
#   - ★ v25: adset_id_to_campaign 맵 추가 → mp_value_map_krw 변환 시 campaign_name 참조
#   - ★ v25: generate_date_tab_summary에 campaign_name 전달 → 나라별 집계 정확도 향상
#   - ★ v24: 분석/매출탭이 맨 왼쪽, 날짜탭은 최신→과거 순, 기타가 맨 오른쪽
#   - ★ v24: 분석탭 생성 즉시 index=0 으로 이동
#   - ★ v23: 날짜탭 하단 요약표에 나라별 ROAS, 순이익, 매출 테이블 추가
#   - ★ v22: 광고 세트 이름 기준 통화 판별 (TWD/JPY/HKD/KRW) + 통화별 환율 적용
#   - Meta/Mixpanel: 최근 7일치만 새로 호출
#   - 광고 세트 이름/광고 세트 ID 기준으로 집계
#   - Mixpanel: utm_term = adset_id 기준으로 매핑
#   - 기존 날짜탭: Meta 데이터 + 매출/ROAS/순이익/CVR 모두 업데이트
#   - 없는 날짜탭만 새로 생성
#   - 마스터탭/추이차트 등 분석탭: 전체 날짜탭 데이터 읽어서 재구성
#   - 탭 순서: [매출/주간매출] + [분석탭] + [날짜탭 최신→과거] + [기타]
#   - 구 구조(25열) / 신 구조(23열) 자동 판별
#   - Mixpanel 전체 날짜탭 기간으로 조회, profit/ROAS/CVR 직접 계산
#   - ★ GitHub Actions 호환: 서비스 계정 인증

print("="*60)
print("🚀 v3-ad v26 (나라판별 강화: 캠페인/세트 이름 서브스트링+한글 매칭)")
print("="*60)

# =========================================================
# 인증 (GitHub Actions / Colab 자동 분기)
# =========================================================
import os
import json

if 'GCP_SERVICE_ACCOUNT_KEY' in os.environ:
    import gspread
    from google.oauth2.service_account import Credentials
    service_account_info = json.loads(os.environ['GCP_SERVICE_ACCOUNT_KEY'])
    scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    gc = gspread.authorize(creds)
    print("✅ GitHub Actions 서비스 계정 인증 완료")
else:
    from google.colab import auth
    auth.authenticate_user()
    import gspread
    from google.auth import default
    creds, _ = default(scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    print("✅ Google Colab 인증 완료")

SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL", "https://docs.google.com/spreadsheets/d/187gxptjGk6Bdhyp6_w14rldMSwBNve2QGDY7KzcIPTU/edit?usp=sharing")
sh = gc.open_by_url(SPREADSHEET_URL)
print(f"✅ 스프레드시트: {sh.title}\n")

import requests as req_lib
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
import random
import math
import re
import calendar
from collections import defaultdict
from gspread.exceptions import APIError
from decimal import Decimal

# =========================================================
# ★ v22: USD/TWD/JPY/HKD → KRW 일별 환율 조회
# =========================================================
FALLBACK_USD_KRW = 1450
FALLBACK_TWD_KRW = 45
FALLBACK_JPY_KRW = 10
FALLBACK_HKD_KRW = 185

def fetch_daily_exchange_rates(start_date, end_date, currency="USD"):
    rates = {}
    pair = f"{currency}KRW=X"
    fallback_map = {"USD": FALLBACK_USD_KRW, "TWD": FALLBACK_TWD_KRW, "JPY": FALLBACK_JPY_KRW, "HKD": FALLBACK_HKD_KRW}
    fallback = fallback_map.get(currency, FALLBACK_USD_KRW)
    try:
        import yfinance as yf
    except ImportError:
        try:
            import subprocess; subprocess.check_call(['pip', 'install', 'yfinance', '-q']); import yfinance as yf
        except: yf = None
    try:
        if yf:
            ticker = yf.Ticker(pair)
            hist = ticker.history(start=start_date.strftime('%Y-%m-%d'),
                                 end=(end_date + timedelta(days=3)).strftime('%Y-%m-%d'))
            if not hist.empty:
                for idx, row in hist.iterrows():
                    dt = idx.to_pydatetime().replace(tzinfo=None)
                    dk = f"{dt.year%100:02d}/{dt.month:02d}/{dt.day:02d}"
                    rates[dk] = round(float(row['Close']), 2)
                print(f"  ✅ yfinance {currency}/KRW 환율 조회 완료: {len(rates)}일")
    except Exception as e:
        print(f"  ⚠️ yfinance {currency}/KRW 실패: {e}")
    if not rates:
        try:
            resp = req_lib.get(f"https://open.er-api.com/v6/latest/{currency}", timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                current_rate = data.get('rates', {}).get('KRW', fallback)
                print(f"  ✅ API 현재 환율: 1 {currency} = ₩{current_rate:,.2f}")
                d = start_date
                while d <= end_date:
                    dk = f"{d.year%100:02d}/{d.month:02d}/{d.day:02d}"
                    rates[dk] = round(float(current_rate), 2)
                    d += timedelta(days=1)
        except Exception as e:
            print(f"  ⚠️ {currency}/KRW 환율 API 실패: {e}")
    if not rates:
        print(f"  ⚠️ {currency}/KRW 환율 조회 모두 실패 → 폴백 ₩{fallback:,} 사용")
    return rates

def get_rate_for_date(rates, dk, fallback=FALLBACK_USD_KRW):
    if dk in rates: return rates[dk]
    if rates:
        sorted_keys = sorted(rates.keys())
        prev = [k for k in sorted_keys if k <= dk]
        if prev: return rates[prev[-1]]
        return rates[sorted_keys[0]]
    return fallback

# =========================================================
# ★ v26: 통화 판별 — 캠페인/세트 이름 기반 (서브스트링+한글)
# =========================================================
CURRENCY_TO_COUNTRY = {"TWD": "대만", "JPY": "일본", "HKD": "홍콩", "KRW": "한국"}

def _detect_currency_from_name(name):
    """★ v26: 단일 이름에서 통화 감지 — 파트매칭 + 서브스트링 + 한글."""
    if not name:
        return None
    name_str = str(name)
    name_lower = name_str.lower()
    parts = re.split(r'[-_\s]', name_lower)
    # 1) 정확한 파트 매칭
    if "jp" in parts or "japan" in parts:
        return "JPY"
    if "hk" in parts or "hongkong" in parts:
        return "HKD"
    if "kr" in parts or "korea" in parts:
        return "KRW"
    if "tw" in parts or "taiwan" in parts:
        return "TWD"
    # 2) 서브스트링 매칭 (구분자 없이 붙어있는 경우: "솔로JP", "JP솔로" 등)
    if "japan" in name_lower:
        return "JPY"
    if re.search(r'(?:^|[^a-z])jp(?:[^a-z]|$)', name_lower):
        return "JPY"
    if "hongkong" in name_lower or "hong kong" in name_lower:
        return "HKD"
    if re.search(r'(?:^|[^a-z])hk(?:[^a-z]|$)', name_lower):
        return "HKD"
    # 3) 한글 매칭
    if "일본" in name_str:
        return "JPY"
    if "홍콩" in name_str:
        return "HKD"
    if "대만" in name_str or "타이완" in name_str or "台灣" in name_str or "台湾" in name_str:
        return "TWD"
    if "한국" in name_str or "국내" in name_str:
        return "KRW"
    return None

def detect_currency(adset_name, campaign_name=None):
    """★ v26: 세트명 → 캠페인명 순서로 통화 추론. 둘 다 없으면 TWD."""
    # 1) 광고 세트 이름에서 먼저 검사
    result = _detect_currency_from_name(adset_name)
    if result:
        return result
    # 2) 캠페인 이름에서 검사
    if campaign_name:
        result = _detect_currency_from_name(campaign_name)
        if result:
            return result
    # 3) 기본값
    return "TWD"

def get_revenue_fx(currency, dk):
    """통화별 KRW 환율 반환."""
    if currency == "KRW":
        return 1.0
    elif currency == "JPY":
        return get_rate_for_date(jpy_krw_rates, dk, fallback=FALLBACK_JPY_KRW)
    elif currency == "HKD":
        return get_rate_for_date(hkd_krw_rates, dk, fallback=FALLBACK_HKD_KRW)
    else:
        return get_rate_for_date(twd_krw_rates, dk, fallback=FALLBACK_TWD_KRW)


# =========================================================
# Meta Ads API 설정
# =========================================================
META_TOKEN_DEFAULT = os.environ.get("META_TOKEN_1", "")
META_TOKEN_GLOBAL = os.environ.get("META_TOKEN_GlobalTT", "")
META_TOKEN_4 = os.environ.get("META_TOKEN_4", "")
META_TOKEN_ACT_2677 = META_TOKEN_GLOBAL or META_TOKEN_4 or os.environ.get("META_TOKEN_3", "")

META_TOKENS = {
    "act_1054081590008088": os.environ.get("META_TOKEN_1", ""),
    "act_2677707262628563": META_TOKEN_ACT_2677,
}
def get_token(acc_id): return META_TOKENS.get(acc_id, META_TOKEN_DEFAULT)
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

print("🔑 Meta 토큰 상태:")
print(f"  TOKEN_1 (act_1054): {'✅ 설정됨' if META_TOKENS['act_1054081590008088'] else '❌ 비어있음'}")
print(f"  GlobalTT (act_2677 글로벌): {'✅ 설정됨' if META_TOKEN_GLOBAL else '❌ 비어있음'}")
print(f"  TOKEN_4 (act_2677 우선): {'✅ 설정됨' if META_TOKEN_4 else '❌ 비어있음'}")
print(f"  TOKEN_3 (act_2677 폴백): {'✅ 설정됨' if os.environ.get('META_TOKEN_3', '') else '❌ 비어있음'}")
print(f"  act_2677 최종 토큰: {'✅' if META_TOKEN_ACT_2677 else '❌'} {'(GlobalTT)' if META_TOKEN_GLOBAL else '(TOKEN_4)' if META_TOKEN_4 else '(TOKEN_3)' if os.environ.get('META_TOKEN_3','') else '(없음)'}")

# =========================================================
# Mixpanel 설정
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
CURRENT_YEAR = TODAY.year
CURRENT_MONTH = TODAY.month

FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
FULL_REFRESH_START = datetime(2026, 1, 1)

if FULL_REFRESH:
    REFRESH_DAYS = (TODAY - FULL_REFRESH_START).days + 1
    print(f"🔥 FULL_REFRESH 모드: {FULL_REFRESH_START.strftime('%Y-%m-%d')} ~ 오늘 ({REFRESH_DAYS}일)")
else:
    REFRESH_DAYS = 7
    print(f"🔄 일반 모드: 최근 {REFRESH_DAYS}일만 갱신")

META_COLLECT_DAYS = REFRESH_DAYS
DATA_REFRESH_START = TODAY - timedelta(days=REFRESH_DAYS - 1)
WEEKLY_TREND_REFRESH_WEEKS = 2

DATE_TAB_HEADERS = [
    "캠페인 이름", "광고 세트 이름", "광고 세트 ID",
    "지출 금액 (KRW)", "결과당 비용", "구매 ROAS(광고 지출 대비 수익률)",
    "CPM(1,000회 노출당 비용)", "도달", "노출",
    "고유 아웃바운드 클릭", "고유 아웃바운드 CTR(클릭률)", "고유 아웃바운드 클릭당 비용",
    "빈도", "결과",
    "결과(믹스패널)", "매출", "이익", "ROAS", "CVR",
    "기존 예산", "증액률", "변동 예산", "메모",
]

OLD_DATE_TAB_HEADERS = [
    "캠페인 이름", "광고 세트 이름", "광고 세트 ID", "광고 이름", "광고 ID",
    "지출 금액 (KRW)", "결과당 비용", "구매 ROAS(광고 지출 대비 수익률)",
    "CPM(1,000회 노출당 비용)", "도달", "노출",
    "고유 아웃바운드 클릭", "고유 아웃바운드 CTR(클릭률)", "고유 아웃바운드 클릭당 비용",
    "빈도", "결과",
    "결과(믹스패널)", "매출", "이익", "ROAS", "CVR",
    "기존 예산", "증액률", "변동 예산", "메모",
]

print(f"📅 현재 날짜: {TODAY.strftime('%Y-%m-%d')}")
print(f"🔄 API 갱신 범위: {DATA_REFRESH_START.strftime('%Y-%m-%d')} ~ 오늘 (최근 {REFRESH_DAYS}일)")
print(f"📊 분석탭: 스프레드시트의 모든 날짜탭 데이터 사용")
print(f"📡 Mixpanel 이벤트: {MIXPANEL_EVENT_NAMES} (OR 수집)")
print(f"💱 환율 지원: USD, TWD, JPY, HKD → KRW (★v26: 캠페인/세트 이름 기반 통화 판별)")
print()

PRODUCT_KEYWORDS = ["starsun", "money", "solo"]

SKIP_WORDS = {
    "tw", "kr", "hk", "my", "sg", "id", "jp", "th", "vn", "ph", "asia",
    "broad", "interest", "lookalike", "retarget", "retargeting", "custom", "asc", "cbo", "abo",
    "dpa", "daba", "advantage", "campaign", "adset", "ad", "ads", "set",
    "purchase", "conversion", "traffic", "reach", "awareness", "engagement",
    "auto", "manual", "daily", "lifetime", "budget",
    "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10",
    "test", "new", "old", "copy", "ver", "final", "draft", "temp",
    "sajutight", "ttsaju", "saju", "tight",
    "a", "b", "c", "d", "e", "f", "the", "and", "or", "for", "all", "img", "vid",
}
WEEKDAY_NAMES = ['월', '화', '수', '목', '금', '토', '일']

COLORS = {
    "dark_gray": {"red":0.4,"green":0.4,"blue":0.4}, "black": {"red":0.2,"green":0.2,"blue":0.2},
    "light_gray": {"red":0.9,"green":0.9,"blue":0.9}, "light_gray2": {"red":0.95,"green":0.95,"blue":0.95},
    "white": {"red":1,"green":1,"blue":1}, "green": {"red":0.56,"green":0.77,"blue":0.49},
    "light_yellow": {"red":1.0,"green":0.98,"blue":0.8}, "light_blue": {"red":0.85,"green":0.92,"blue":0.98},
    "light_red": {"red":1.0,"green":0.85,"blue":0.85}, "dark_blue": {"red":0.2,"green":0.3,"blue":0.5},
    "navy": {"red":0.15,"green":0.2,"blue":0.35}, "dark_green": {"red":0.2,"green":0.5,"blue":0.3},
    "orange": {"red":0.9,"green":0.5,"blue":0.2}, "purple": {"red":0.5,"green":0.3,"blue":0.6},
    "teal": {"red":0.0,"green":0.5,"blue":0.5},
}

SUMMARY_PRODUCTS = []

# ★ v24: 분석탭 순서 (맨 왼쪽에 배치)
LEFTMOST_TABS_ORDER = ["매출", "주간매출"]

FINAL_ANALYSIS_ORDER = [
    "추이차트", "추이차트(주간)", "증감액", "예산",
    "주간종합", "주간종합_2", "주간종합_3", "마스터탭"
]

ANALYSIS_TABS_SET = set(FINAL_ANALYSIS_ORDER) | set(LEFTMOST_TABS_ORDER) | {"_temp", "_temp_holder", "_tmp"}


def detect_tab_structure(header_row):
    if not header_row or len(header_row) < 5: return "new"
    headers = [str(h).strip().lower() for h in header_row]
    h3 = headers[3] if len(headers) > 3 else ""
    h4 = headers[4] if len(headers) > 4 else ""
    if "광고 이름" in h3 or "광고이름" in h3 or "ad name" in h3: return "old"
    if "광고 id" in h4 or "광고id" in h4 or "ad id" in h4: return "old"
    if len(headers) >= 25:
        h5 = headers[5] if len(headers) > 5 else ""
        if "지출" in h5: return "old"
    return "new"

OLD_TO_NEW_MAP = {0:0,1:1,2:2,5:3,6:4,7:5,8:6,9:7,10:8,11:9,12:10,13:11,14:12,15:13,16:14,17:15,18:16,19:17,20:18,21:19,22:20,23:21,24:22}

def normalize_row_to_new(row, structure):
    if structure == "new":
        result = list(row[:len(DATE_TAB_HEADERS)])
        while len(result) < len(DATE_TAB_HEADERS): result.append("")
        return result
    new_row = [""] * len(DATE_TAB_HEADERS)
    for old_idx, new_idx in OLD_TO_NEW_MAP.items():
        if old_idx < len(row): new_row[new_idx] = row[old_idx]
    return new_row

def get_col_index(structure, new_col_idx):
    if structure == "new": return new_col_idx
    if new_col_idx <= 2: return new_col_idx
    return new_col_idx + 2

def get_col_letter(col_idx_0based):
    result = ""; idx = col_idx_0based
    while True:
        result = chr(ord('A') + idx % 26) + result
        idx = idx // 26 - 1
        if idx < 0: break
    return result

def clean_id(val):
    if val is None: return ""
    s = str(val).strip()
    if not s: return ""
    if re.match(r'^\d+$', s): return s
    try:
        if ("E" in s or "e" in s) and re.match(r'^[\d.]+[eE][+\-]?\d+$', s): return str(int(Decimal(s)))
    except: pass
    try:
        if re.match(r'^\d+\.\d+$', s): return str(int(Decimal(s)))
    except: pass
    numeric_only = re.sub(r'[^0-9]', '', s)
    return numeric_only if numeric_only else s

def extract_product(adset_name):
    if not adset_name: return "기타"
    name_lower = str(adset_name).lower()
    for kw in PRODUCT_KEYWORDS:
        if kw in name_lower: return kw
    parts = name_lower.split('_')
    if len(parts) >= 3:
        candidate = parts[2].strip()
        if candidate in PRODUCT_KEYWORDS: return candidate
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
                wait_time = 20 + (attempt * 15) + random.random()
                print(f"  ⏳ Sheets Rate limit. {wait_time:.0f}초 대기 (시도 {attempt+1}/{max_retries})")
                time.sleep(wait_time)
            elif attempt == max_retries-1: raise
            else: time.sleep(5 + random.random())
    return None

def safe_add_worksheet(sh, title, rows, cols):
    try:
        existing = with_retry(sh.worksheet, title)
        if existing:
            with_retry(sh.del_worksheet, existing)
            print(f"  🗑️ 기존 '{title}' 삭제 후 재생성"); time.sleep(2)
    except gspread.exceptions.WorksheetNotFound: pass
    except APIError as e:
        if "429" in str(e) or "Quota exceeded" in str(e):
            print(f"  ⏳ safe_add_worksheet Rate Limit → 30초 대기 후 재시도"); time.sleep(30)
            try:
                existing = with_retry(sh.worksheet, title)
                if existing:
                    with_retry(sh.del_worksheet, existing); print(f"  🗑️ 기존 '{title}' 삭제 후 재생성"); time.sleep(2)
            except gspread.exceptions.WorksheetNotFound: pass
            except: pass
        else: print(f"  ⚠️ safe_add_worksheet 오류: {e}")
    except Exception as e: print(f"  ⚠️ safe_add_worksheet 기타 오류: {e}")
    return with_retry(sh.add_worksheet, title=title, rows=rows, cols=cols)

def refresh_ws(sh, ws):
    try: fresh = with_retry(sh.worksheet, ws.title); return fresh
    except Exception: return ws

def clear_summary_conditional_formats(sh, ws, summary_start_row_0indexed):
    try:
        sid = ws.id
        try: metadata = sh.fetch_sheet_metadata(params={'fields': 'sheets(properties.sheetId,conditionalFormats)'})
        except AttributeError:
            url = f"https://sheets.googleapis.com/v4/spreadsheets/{sh.id}"
            resp = sh.client.request('get', url, params={'fields': 'sheets(properties.sheetId,conditionalFormats)'}); metadata = resp.json()
        sheet_meta = None
        for s in metadata.get('sheets', []):
            if s.get('properties', {}).get('sheetId') == sid: sheet_meta = s; break
        if not sheet_meta: return
        cond_rules = sheet_meta.get('conditionalFormats', [])
        if not cond_rules: return
        delete_indices = []
        for idx, rule in enumerate(cond_rules):
            for rng in rule.get('ranges', []):
                if rng.get('sheetId') != sid: continue
                rule_end = rng.get('endRowIndex', 999999)
                if rule_end > summary_start_row_0indexed: delete_indices.append(idx); break
        if delete_indices:
            reqs = [{"deleteConditionalFormatRule":{"sheetId":sid,"index":i}} for i in sorted(delete_indices, reverse=True)]
            with_retry(sh.batch_update, body={"requests":reqs})
            print(f"    🗑️ 조건부 서식 {len(delete_indices)}개 삭제 (요약 영역)"); time.sleep(0.5)
    except Exception as e: print(f"    ⚠️ 조건부 서식 삭제 오류 (무시): {e}")


# =========================================================
# ★ v24: move_to_front — 워크시트를 index=0 (맨 왼쪽)으로 이동
# =========================================================
def move_to_front(sh, ws, target_index=0):
    """워크시트를 지정된 인덱스(기본 0, 맨 왼쪽)로 이동."""
    try:
        with_retry(sh.batch_update, body={"requests": [
            {"updateSheetProperties": {
                "properties": {"sheetId": ws.id, "index": target_index},
                "fields": "index"
            }}
        ]})
        time.sleep(1)
    except Exception as e:
        print(f"  ⚠️ 탭 이동 오류: {e}")


# =========================================================
# ★ v24: reorder_tabs — 분석/매출탭 맨 왼쪽 + 날짜탭 최신→과거 + 기타 맨 오른쪽
# =========================================================
def reorder_tabs(sh):
    """
    탭 순서: [매출/주간매출] + [분석탭] + [날짜탭 최신→과거] + [기타]
    """
    try:
        all_ws = sh.worksheets()
        leftmost_tabs, analysis_tabs, date_tabs, other_tabs = [], [], [], []
        analysis_order_map = {name: i for i, name in enumerate(FINAL_ANALYSIS_ORDER)}
        leftmost_order_map = {name: i for i, name in enumerate(LEFTMOST_TABS_ORDER)}

        for ws in all_ws:
            tn = ws.title
            if tn in leftmost_order_map:
                leftmost_tabs.append(ws)
            elif tn in analysis_order_map or tn in ANALYSIS_TABS_SET:
                analysis_tabs.append(ws)
            elif parse_date_tab(tn) is not None:
                date_tabs.append(ws)
            else:
                other_tabs.append(ws)

        # ★ v24: 날짜탭은 최신→과거 (reverse=True)
        date_tabs.sort(key=lambda ws: parse_date_tab(ws.title), reverse=True)
        analysis_tabs.sort(key=lambda ws: analysis_order_map.get(ws.title, 999))
        leftmost_tabs.sort(key=lambda ws: leftmost_order_map.get(ws.title, 999))

        # ★ v24: [매출/주간매출] + [분석탭] + [최신날짜→과거날짜] + [기타]
        final_order = leftmost_tabs + analysis_tabs + date_tabs + other_tabs

        print(f"  📈 매출탭: {len(leftmost_tabs)}개 | 📊 분석: {len(analysis_tabs)}개 | 📅 날짜: {len(date_tabs)}개 | 📋 기타: {len(other_tabs)}개")
        if leftmost_tabs: print(f"  📈 최좌측: {' → '.join(ws.title for ws in leftmost_tabs)}")
        if analysis_tabs: print(f"  📊 분석탭: {' → '.join(ws.title for ws in analysis_tabs)}")
        if date_tabs: print(f"  📅 날짜탭: {date_tabs[0].title} (최신) → {date_tabs[-1].title} (과거)")

        with_retry(sh.batch_update, body={"requests": [
            {"updateSheetProperties": {"properties": {"sheetId": ws.id, "index": idx}, "fields": "index"}}
            for idx, ws in enumerate(final_order)
        ]})
        print("  ✅ 탭 순서 정리 완료"); time.sleep(2)
    except Exception as e: print(f"  ⚠️ 탭 순서 정리 오류: {e}")


def cell_text(profit, revenue, spend, cpm=0, cvr=0):
    if spend == 0: return ""
    roas = (revenue / spend * 100) if spend > 0 else 0
    return f"{roas:.0f}\n{money(profit)}\n-{money(spend)}\n{'₩'+str(int(round(cpm)))+'' if cpm > 0 else '₩0'}\n{cvr:.1f}%"

def cell_text_change(roas, chg, spend, cpm, cvr=0):
    if spend == 0: return ""
    cl = f"+{chg:.1f}%" if chg > 0 else f"{chg:.1f}%" if chg < 0 else "0.0%"
    return f"{roas:.0f}\n{cl}\n-{money(spend)}\n{'₩'+str(int(round(cpm))) if cpm > 0 else '₩0'}\n{cvr:.1f}%"

def get_week_range(d):
    wd = d.weekday(); m = d - timedelta(days=wd); s = m + timedelta(days=6)
    return f"'{m.month}/{m.day}({WEEKDAY_NAMES[0]})~{s.month}/{s.day}({WEEKDAY_NAMES[6]})"

def get_week_range_short(d):
    wd = d.weekday(); m = d - timedelta(days=wd); s = m + timedelta(days=6)
    return f"{m.month}/{m.day}~{s.month}/{s.day}"

def get_month_range_display(f, l): return f"'{f.month}.{f.day}~{l.month}.{l.day}"
def get_week_monday(d): return d - timedelta(days=d.weekday())


def parse_date_tab(tab_name):
    if not tab_name or '/' not in tab_name:
        return None
    try:
        clean_name = tab_name.strip().strip("'").strip('\u200b\ufeff\xa0').strip()
        parts = clean_name.split('/')
        parts = [p.strip() for p in parts]
        if len(parts) == 3:
            y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            return datetime(2000 + y, m, d)
        elif len(parts) == 2:
            m, d = int(parts[0]), int(parts[1])
            now = datetime.now()
            candidate = datetime(now.year, m, d)
            if candidate > now + timedelta(days=7):
                candidate = datetime(now.year - 1, m, d)
            return candidate
        else:
            return None
    except:
        return None


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

def apply_c2_label_formatting(sh, ws):
    sid = ws.id; cv = "ROAS\n순이익\n지출\nCPM\n전환율"
    lines = cv.split('\n'); indices = [0]; pos = 0
    for line in lines[:-1]: pos += len(line) + 1; indices.append(pos)
    bk = {"foregroundColor":{"red":0,"green":0,"blue":0}}; dg = {"foregroundColor":{"red":0.22,"green":0.46,"blue":0.11}}
    rd = {"foregroundColor":{"red":0.85,"green":0.0,"blue":0.0}}; bl = {"foregroundColor":{"red":0.0,"green":0.0,"blue":0.85}}
    req = {"updateCells":{"range":{"sheetId":sid,"startRowIndex":1,"endRowIndex":2,"startColumnIndex":2,"endColumnIndex":3},
        "rows":[{"values":[{"userEnteredValue":{"stringValue":cv},"textFormatRuns":[
            {"startIndex":indices[0],"format":bk},{"startIndex":indices[1],"format":dg},
            {"startIndex":indices[2],"format":rd},{"startIndex":indices[3],"format":bl},
            {"startIndex":indices[4],"format":bk}]}]}],"fields":"userEnteredValue,textFormatRuns"}}
    with_retry(sh.batch_update, body={"requests":[req]}); time.sleep(1)

def apply_trend_chart_formatting(sh, ws, headers, rows_count, is_change_tab=False, sunday_col_indices=None, format_col_end=None):
    sid = ws.id
    try: with_retry(ws.format, 'A1:Z1', {'backgroundColor':{'red':0.9,'green':0.9,'blue':0.9},'textFormat':{'bold':True}}); time.sleep(1)
    except: pass
    if sunday_col_indices:
        try:
            sr = [create_format_request(sid,0,1,ci,ci+1,get_cell_format(COLORS["light_red"],bold=True)) for ci in sunday_col_indices]
            if sr: with_retry(sh.batch_update, body={"requests":sr}); time.sleep(1)
        except: pass
    try: with_retry(ws.format, 'A2:Z2', {'textFormat':{'bold':True}}); time.sleep(1)
    except: pass
    try: with_retry(sh.batch_update, body={"requests":[{"updateSheetProperties":{"properties":{"sheetId":sid,"gridProperties":{"frozenRowCount":1}},"fields":"gridProperties.frozenRowCount"}}]}); time.sleep(2)
    except: pass
    try: with_retry(ws.format, 'D2:Z1000', {'wrapStrategy':'WRAP','verticalAlignment':'TOP'}); time.sleep(1)
    except: pass
    try:
        cwr = [{"updateDimensionProperties":{"range":{"sheetId":sid,"dimension":"COLUMNS","startIndex":cn,"endIndex":cn+1},"properties":{"pixelSize":95},"fields":"pixelSize"}} for cn in range(3,min(len(headers),26))]
        if cwr: with_retry(sh.batch_update, body={"requests":cwr})
    except: pass
    print("  🎨 ROAS 조건부 서식...")
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
        with_retry(sh.batch_update, body={'requests':fr}); time.sleep(3)
    except Exception as e: print(f"  ⚠️ 조건부 서식 오류: {e}")
    tcs = 3; tce = min(format_col_end or len(headers), len(headers), 20)
    print(f"  🎨 텍스트 색상 (col {tcs}~{tce-1})...")
    try:
        fr = []
        bk={"foregroundColor":{"red":0,"green":0,"blue":0}}; dg3={"foregroundColor":{"red":0.22,"green":0.46,"blue":0.11}}
        rd={"foregroundColor":{"red":0.85,"green":0.0,"blue":0.0}}; gn={"foregroundColor":{"red":0.0,"green":0.7,"blue":0.0}}; bl={"foregroundColor":{"red":0.0,"green":0.0,"blue":0.85}}
        for ci in range(tcs, tce):
            try: cv = with_retry(ws.col_values, ci+1)
            except: continue
            if not cv or len(cv) < 2: continue
            for ri in range(2, min(len(cv)+1, rows_count+3)):
                val = cv[ri-1] if ri-1 < len(cv) else ""
                if not val or '\n' not in val: continue
                lines = val.split('\n')
                if len(lines) < 4: continue
                l1e=len(lines[0]);l2e=l1e+1+len(lines[1]);l3e=l2e+1+len(lines[2]);l4s=l3e+1
                if len(lines)>=5: l4e=l4s+len(lines[3]);l5s=l4e+1
                if is_change_tab:
                    cc = gn if lines[1].startswith('+') else rd if lines[1].startswith('-') else bk
                    tr=[{"startIndex":0,"format":bk},{"startIndex":l1e+1,"format":cc},{"startIndex":l2e+1,"format":rd},{"startIndex":l4s,"format":bl}]
                else:
                    tr=[{"startIndex":0,"format":bk},{"startIndex":l1e+1,"format":dg3},{"startIndex":l2e+1,"format":rd},{"startIndex":l4s,"format":bl}]
                if len(lines)>=5: tr.append({"startIndex":l5s,"format":bk})
                fr.append({"updateCells":{"range":{"sheetId":sid,"startRowIndex":ri-1,"endRowIndex":ri,"startColumnIndex":ci,"endColumnIndex":ci+1},"rows":[{"values":[{"userEnteredValue":{"stringValue":val},"textFormatRuns":tr}]}],"fields":"userEnteredValue,textFormatRuns"}})
                if len(fr) >= 300:
                    try: with_retry(sh.batch_update, body={"requests":fr}); fr=[]; time.sleep(3)
                    except: fr=[]
        if fr:
            try: with_retry(sh.batch_update, body={"requests":fr}); time.sleep(2)
            except: pass
        print("  ✅ 텍스트 색상 완료")
    except Exception as e: print(f"  ⚠️ 텍스트 색상 오류: {e}")


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
                err = resp.json().get('error',{}); print(f"  ❌ Meta 400: {err.get('message', resp.text[:200])}"); return None
            elif resp.status_code in [429,500,502,503]:
                wait = 30+attempt*30; print(f"  ⏳ Meta {resp.status_code}, {wait}초 대기 (시도 {attempt+1}/5)"); time.sleep(wait)
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

def fetch_meta_insights_daily(ad_account_id, single_date):
    url = f"{META_BASE_URL}/{ad_account_id}/insights"
    fields = 'campaign_name,adset_name,adset_id,spend,cpm,reach,impressions,frequency,actions,cost_per_action_type,purchase_roas,unique_outbound_clicks,unique_outbound_clicks_ctr,cost_per_unique_outbound_click'
    params = {'fields':fields,'level':'adset','time_increment':1,'time_range':json.dumps({'since':single_date,'until':single_date}),'limit':500,'filtering':json.dumps([{'field':'spend','operator':'GREATER_THAN','value':'0'}])}
    all_results = []
    data = meta_api_get(url, params, token=get_token(ad_account_id))
    while data:
        rows = data.get('data',[]); all_results.extend(rows)
        paging = data.get('paging',{}); next_url = paging.get('next')
        if next_url:
            time.sleep(1)
            try: resp = req_lib.get(next_url,timeout=120); data = resp.json() if resp.status_code==200 else None
            except: data = None
        else: break
    return all_results

def parse_single_day_insights(rows, date_str, date_obj):
    purchase_types = ['purchase','omni_purchase','offsite_conversion.fb_pixel_purchase']; outbound_types = ['outbound_click']
    parsed = []
    for row in rows:
        spend=float(row.get('spend',0)); cpm=float(row.get('cpm',0)); reach=int(float(row.get('reach',0))); impressions=int(float(row.get('impressions',0))); frequency=float(row.get('frequency',0))
        actions=row.get('actions',[]); results=extract_action_value(actions,purchase_types)
        cost_per_action=row.get('cost_per_action_type',[]); cost_per_result=extract_action_value(cost_per_action,purchase_types)
        uo=row.get('unique_outbound_clicks',[]); unique_clicks=extract_action_value(uo,outbound_types)
        uc=row.get('unique_outbound_clicks_ctr',[]); unique_ctr=extract_action_value(uc,outbound_types)
        cpu=row.get('cost_per_unique_outbound_click',[]); cost_per_click=extract_action_value(cpu,outbound_types)
        parsed.append({'campaign_name':row.get('campaign_name',''),'adset_name':row.get('adset_name',''),'adset_id':row.get('adset_id',''),
            'spend':spend,'cost_per_result':cost_per_result,'cpm':cpm,'reach':reach,'impressions':impressions,'unique_clicks':unique_clicks,
            'unique_ctr':unique_ctr,'cost_per_unique_click':cost_per_click,'frequency':frequency,'results':results,'date_obj':date_obj})
    return parsed

def fetch_adset_budgets(ad_account_id):
    url = f"{META_BASE_URL}/{ad_account_id}/adsets"
    params = {'fields':'id,daily_budget,campaign_id','limit':500,'filtering':json.dumps([{'field':'effective_status','operator':'IN','value':['ACTIVE']}])}
    adset_results = {}; needs_campaign = {}
    data = meta_api_get(url, params, token=get_token(ad_account_id))
    while data:
        for row in data.get('data',[]):
            asid = row.get('id',''); budget = row.get('daily_budget','0'); campaign_id = row.get('campaign_id','')
            if not asid: continue
            try: budget_int = int(float(budget)) if budget else 0
            except: budget_int = 0
            if budget_int > 0: adset_results[asid] = budget_int
            else:
                adset_results[asid] = 0
                if campaign_id: needs_campaign[asid] = campaign_id
        paging = data.get('paging',{}); next_url = paging.get('next')
        if next_url:
            time.sleep(1)
            try: resp = req_lib.get(next_url,timeout=120); data = resp.json() if resp.status_code==200 else None
            except: data = None
        else: break
    if needs_campaign:
        unique_campaigns = set(needs_campaign.values()); campaign_budgets = {}
        for cid in unique_campaigns:
            try:
                camp_url = f"{META_BASE_URL}/{cid}"; camp_params = {'fields':'id,daily_budget'}
                camp_data = meta_api_get(camp_url, camp_params, token=get_token(ad_account_id))
                if camp_data:
                    cb = camp_data.get('daily_budget','0')
                    try: campaign_budgets[cid] = int(float(cb)) if cb else 0
                    except: campaign_budgets[cid] = 0
                time.sleep(0.5)
            except: pass
        fallback_count = 0
        for asid, cid in needs_campaign.items():
            camp_budget = campaign_budgets.get(cid, 0)
            if camp_budget > 0: adset_results[asid] = camp_budget; fallback_count += 1
        if fallback_count > 0: print(f"    📌 ASC/캠페인 예산 폴백: {fallback_count}개 세트 (캠페인 {len(unique_campaigns)}개 조회)")
    return adset_results


def fetch_mixpanel_data(from_date, to_date):
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {'from_date':from_date,'to_date':to_date,'event':json.dumps(MIXPANEL_EVENT_NAMES),'project_id':MIXPANEL_PROJECT_ID}
    print(f"  📡 Mixpanel: {from_date} ~ {to_date} (이벤트: {MIXPANEL_EVENT_NAMES})")
    try:
        resp = req_lib.get(url,params=params,auth=(MIXPANEL_USERNAME,MIXPANEL_SECRET),timeout=300)
        if resp.status_code != 200: print(f"  ❌ Mixpanel {resp.status_code}: {resp.text[:300]}"); return []
        lines = [l for l in resp.text.split('\n') if l.strip()]; print(f"  📊 이벤트: {len(lines)}건")
        data = []
        stat_amount_only=0; stat_value_only=0; stat_both=0; stat_neither=0
        stat_결제금액_only=0; stat_event_counts=defaultdict(int)
        for line in lines:
            try:
                ev=json.loads(line); props=ev.get('properties',{}); ts=props.get('time',0)
                event_name = ev.get('event', '')
                stat_event_counts[event_name] += 1
                if ts: dt_kst=datetime.fromtimestamp(ts,tz=timezone.utc)+timedelta(hours=9); ds=f"{dt_kst.year%100:02d}/{dt_kst.month:02d}/{dt_kst.day:02d}"
                else: ds=None
                ut=None
                for k in ['utm_term','UTM_Term','UTM Term']:
                    if k in props and props[k]: ut=str(props[k]).strip(); break
                if ut: ut=clean_id(ut)
                raw_결제금액 = props.get('결제금액'); raw_amount = props.get('amount'); raw_value = props.get('value')
                결제금액_val = 0.0
                if raw_결제금액 is not None:
                    try: 결제금액_val = float(raw_결제금액)
                    except: 결제금액_val = 0.0
                amount_val = 0.0
                if raw_amount is not None:
                    try: amount_val = float(raw_amount)
                    except: amount_val = 0.0
                value_val = 0.0
                if raw_value is not None:
                    try: value_val = float(raw_value)
                    except: value_val = 0.0
                if 결제금액_val > 0: revenue = 결제금액_val
                elif amount_val > 0: revenue = amount_val
                elif value_val > 0: revenue = value_val
                else: revenue = 0.0
                has_결제금액 = 결제금액_val > 0; has_amount = amount_val > 0; has_value = value_val > 0
                if has_결제금액 and not has_amount and not has_value: stat_결제금액_only += 1
                elif has_amount and has_value: stat_both += 1
                elif has_amount: stat_amount_only += 1
                elif has_value: stat_value_only += 1
                elif has_결제금액: pass
                else: stat_neither += 1
                data.append({'distinct_id':props.get('distinct_id'),'time':ts,'date':ds,'utm_term':ut or '','amount':amount_val,'결제금액':결제금액_val,'value_raw':value_val,'revenue':revenue,'서비스':props.get('서비스','')})
            except: pass
        print(f"  ✅ 파싱: {len(data)}건")
        print(f"  📊 매출 출처: 결제금액={stat_결제금액_only}, amount={stat_amount_only}, value={stat_value_only}, 둘다={stat_both}, 없음={stat_neither}")
        print(f"  📊 이벤트별: {dict(stat_event_counts)}")
        return data
    except Exception as e: print(f"  ❌ Mixpanel 오류: {e}"); return []


def find_last_data_row(all_values, structure):
    actual_asid_col = get_col_index(structure, 2)
    last_data_sheet_row = 1; data_rows = []; consecutive_empty = 0
    EMPTY_THRESHOLD = 3
    for idx, row in enumerate(all_values[1:], start=2):
        if not row:
            consecutive_empty += 1
            if consecutive_empty >= EMPTY_THRESHOLD: break
            continue
        cn = str(row[0]).strip() if len(row) > 0 else ""
        asid = str(row[actual_asid_col]).strip() if len(row) > actual_asid_col else ""
        if cn in ["캠페인 이름","전체","합계","Total"]:
            consecutive_empty += 1
            if consecutive_empty >= EMPTY_THRESHOLD: break
            continue
        if not cn and not asid:
            consecutive_empty += 1
            if consecutive_empty >= EMPTY_THRESHOLD: break
            continue
        consecutive_empty = 0
        data_rows.append(row); last_data_sheet_row = idx
    return last_data_sheet_row, data_rows


def read_all_date_tabs(sh, analysis_tab_names, mp_value_map=None, mp_count_map=None):
    print("\n"+"="*60); print("★ 8.5단계: 전체 날짜탭 읽기"); print("="*60)
    all_ad_sets = defaultdict(lambda: {'campaign_name':'','adset_name':'','adset_id':'','dates':{}})
    all_budget_by_date = defaultdict(lambda: defaultdict(lambda: {'spend':0.0,'revenue':0.0}))
    all_master_raw_data = []; all_date_objects = {}; all_date_names = []
    master_headers_local = ["Date"] + DATE_TAB_HEADERS
    analysis_set = set(analysis_tab_names); all_ws = sh.worksheets(); date_tabs_found = []
    for ws_ex in all_ws:
        tn = ws_ex.title
        if tn in analysis_set: continue
        dt_obj = parse_date_tab(tn)
        if dt_obj is None: continue
        date_tabs_found.append((dt_obj, ws_ex, tn))
    date_tabs_found.sort(key=lambda x: x[0])
    print(f"  ✅ 날짜탭: {len(date_tabs_found)}개")
    if date_tabs_found: print(f"  📅 범위: {date_tabs_found[0][2]} ~ {date_tabs_found[-1][2]}")
    _mp_val = mp_value_map or {}; _mp_cnt = mp_count_map or {}
    for dt_obj, ws_ex, tn in date_tabs_found:
        dk = f"{dt_obj.year%100:02d}/{dt_obj.month:02d}/{dt_obj.day:02d}"
        all_date_objects[dk]=dt_obj; all_date_names.append(dk)
        try:
            print(f"  📖 {dk}...", end=" ", flush=True)
            all_values = with_retry(ws_ex.get_all_values); time.sleep(0.3)
            if not all_values or len(all_values) < 2: print("빈 탭"); continue
            structure = detect_tab_structure(all_values[0])
            _, scan_rows = find_last_data_row(all_values, structure); row_count = 0
            for row in scan_rows:
                norm_row = normalize_row_to_new(row, structure)
                cn=str(norm_row[0]).strip()
                if not cn or cn in ["캠페인 이름","전체","합계","Total"]: continue
                asn=str(norm_row[1]).strip();asid=str(norm_row[2]).strip();spend=_to_num(norm_row[3]);cpm=_to_num(norm_row[6]);unique_clicks=_to_num(norm_row[9])
                mpc=_mp_cnt.get((dk,asid),0);revenue=_mp_val.get((dk,asid),0.0)
                if mpc==0 and revenue==0.0: mpc=_to_num(norm_row[14]);revenue=_to_num(norm_row[15])
                profit=revenue-spend;roas=(revenue/spend*100) if spend>0 else 0;cvr=(mpc/unique_clicks*100) if unique_clicks>0 and mpc>0 else 0
                if asid:
                    if not all_ad_sets[asid]['adset_id']: all_ad_sets[asid]={'campaign_name':cn,'adset_name':asn,'adset_id':asid,'dates':{}}
                    all_ad_sets[asid]['dates'][dk]={'profit':profit,'revenue':revenue,'spend':spend,'cpm':cpm,'cvr':cvr,'unique_clicks':unique_clicks,'mpc':mpc}
                p=extract_product(asn); all_budget_by_date[dk][p]['spend']+=spend; all_budget_by_date[dk][p]['revenue']+=revenue
                mrd=[dk]+list(norm_row); mrd[14+1]=int(mpc) if mpc>0 else ""; mrd[15+1]=round(revenue) if revenue>0 else ""
                mrd[16+1]=round(profit,0) if spend>0 else ""; mrd[17+1]=round(roas,1) if spend>0 and revenue>0 else ""; mrd[18+1]=round(cvr,2) if cvr>0 else ""
                while len(mrd)<len(master_headers_local): mrd.append("")
                all_master_raw_data.append({'date':dk,'date_obj':dt_obj,'spend':spend,'row_data':mrd[:len(master_headers_local)]}); row_count+=1
            print(f"{row_count}행")
        except Exception as e: print(f"오류: {e}")
    print(f"  ✅ 광고 세트: {len(all_ad_sets)}개 | 마스터행: {len(all_master_raw_data)}개")
    return all_ad_sets, all_budget_by_date, all_master_raw_data, all_date_objects, all_date_names


def diagnose_chart_coverage(sh, date_names, ad_sets, analysis_tabs_set):
    all_ws = sh.worksheets(); slash_tabs = [ws.title for ws in all_ws if '/' in ws.title]
    parsed_tabs = []; failed_tabs = []
    for t in slash_tabs:
        dt = parse_date_tab(t)
        if dt:
            dk = f"{dt.year%100:02d}/{dt.month:02d}/{dt.day:02d}"; parsed_tabs.append((t, dk, dt))
        else: failed_tabs.append(t)
    if failed_tabs: print(f"  ⚠️ 파싱 실패 탭: {failed_tabs}")
    return failed_tabs


# =========================================================
# ★ v25: generate_date_tab_summary — 캠페인 이름도 활용하여 나라별 정확 판별
# =========================================================
def generate_date_tab_summary(rows, structure="new"):
    total_spend=0.0;total_meta_purchase=0.0;total_mp_purchase=0.0;total_revenue=0.0;total_profit=0.0;total_unique_clicks=0.0
    spend_by_account={"본계정":0.0,"부계정":0.0,"3rd계정":0.0}
    prod_spend=defaultdict(float);prod_revenue=defaultdict(float);prod_profit=defaultdict(float)
    # ★ v23/v25: 나라별 집계
    country_spend=defaultdict(float);country_revenue=defaultdict(float);country_profit=defaultdict(float)

    for row in rows:
        nr=normalize_row_to_new(row,structure);cn=str(nr[0]);asn=str(nr[1]);adset_id=str(nr[2]);spend=_to_num(nr[3]);meta_p=_to_num(nr[13]);mp_p=_to_num(nr[14]);rev=_to_num(nr[15]);prof=_to_num(nr[16]);uc=_to_num(nr[9])
        if not cn or cn in ["전체","합계","Total"]: continue
        total_spend+=spend;total_meta_purchase+=meta_p;total_mp_purchase+=mp_p;total_revenue+=rev;total_profit+=prof;total_unique_clicks+=uc
        if adset_id.startswith("12023"): spend_by_account["본계정"]+=spend
        elif adset_id.startswith("12024"): spend_by_account["부계정"]+=spend
        elif adset_id.startswith("6"): spend_by_account["3rd계정"]+=spend
        p = extract_product(asn)
        prod_spend[p]+=spend;prod_revenue[p]+=rev;prod_profit[p]+=prof
        # ★ v26: 캠페인 이름 + 광고 세트 이름 + adset_id로 나라 판별
        currency = detect_currency(asn, campaign_name=cn)
        country = CURRENCY_TO_COUNTRY.get(currency, "기타")
        country_spend[country]+=spend;country_revenue[country]+=rev;country_profit[country]+=prof

    total_roas=(total_revenue/total_spend*100) if total_spend>0 else 0
    total_cvr=(total_mp_purchase/total_unique_clicks*100) if total_unique_clicks>0 else 0
    sp = sorted([p for p in prod_spend if p in PRODUCT_KEYWORDS and (prod_spend[p] > 0 or prod_revenue[p] > 0)],
                key=lambda p: prod_spend[p], reverse=True)
    num_products = len(sp); NC = 25
    sum_col_idx = 9 + num_products

    # ★ v23: 나라 목록 (지출 기준 내림차순)
    countries = sorted([c for c in country_spend if country_spend[c] > 0 or country_revenue[c] > 0],
                       key=lambda c: country_spend[c], reverse=True)
    num_countries = len(countries)
    country_sum_col = 9 + num_countries

    summary_data = []; summary_data.append([""]*NC); summary_data.append([""]*NC); summary_data.append([""]*NC)
    r_title=[""]*NC; r_title[14]="전체"; summary_data.append(r_title)
    r_hdr=[""]*NC; r_hdr[11]="본계정";r_hdr[12]="부계정";r_hdr[13]="3rd 계정"
    r_hdr[14]="지출 금액 (KRW)";r_hdr[15]="구매 (메타)";r_hdr[16]="구매 (믹스패널)"
    r_hdr[17]="매출";r_hdr[18]="이익";r_hdr[19]="ROAS";r_hdr[20]="CVR"; summary_data.append(r_hdr)
    r_val=[""]*NC; r_val[11]=round(spend_by_account["본계정"]);r_val[12]=round(spend_by_account["부계정"]);r_val[13]=round(spend_by_account["3rd계정"])
    r_val[14]=round(total_spend);r_val[15]=round(total_meta_purchase);r_val[16]=round(total_mp_purchase)
    r_val[17]=round(total_revenue);r_val[18]=round(total_profit);r_val[19]=round(total_roas,1);r_val[20]=total_cvr/100 if total_cvr else 0
    summary_data.append(r_val)

    # --- 제품별 테이블 ---
    if num_products > 0:
        total_prod_spend=sum(prod_spend[p] for p in sp); total_prod_revenue=sum(prod_revenue[p] for p in sp); total_prod_profit=sum(prod_profit[p] for p in sp)
        tables=[("제품별 ROAS","roas"),("제품별 순이익","profit"),("제품별 매출","revenue"),("제품별 순이익율","profit_margin"),("제품별 예산","spend"),("제품별 예산 비중","spend_ratio")]
        for table_title, data_type in tables:
            summary_data.append([""]*NC); r_tt=[""]*NC; r_tt[12]=table_title; summary_data.append(r_tt)
            r_ph=[""]*NC
            for i,p in enumerate(sp): r_ph[9+i]=p
            r_ph[sum_col_idx]="합"; summary_data.append(r_ph)
            r_pv=[""]*NC
            for i,p in enumerate(sp):
                ps=prod_spend[p];pr=prod_revenue[p];pp=prod_profit[p]
                if data_type=="roas": r_pv[9+i]=pr/ps if ps>0 else 0
                elif data_type=="profit": r_pv[9+i]=round(pp)
                elif data_type=="revenue": r_pv[9+i]=round(pr)
                elif data_type=="profit_margin": r_pv[9+i]=pp/pr if pr>0 else 0
                elif data_type=="spend": r_pv[9+i]=round(ps)
                elif data_type=="spend_ratio": r_pv[9+i]=ps/total_prod_spend if total_prod_spend>0 else 0
            if data_type=="roas": r_pv[sum_col_idx]=total_prod_revenue/total_prod_spend if total_prod_spend>0 else 0
            elif data_type=="profit": r_pv[sum_col_idx]=round(total_prod_profit)
            elif data_type=="revenue": r_pv[sum_col_idx]=round(total_prod_revenue)
            elif data_type=="profit_margin": r_pv[sum_col_idx]=total_prod_profit/total_prod_revenue if total_prod_revenue>0 else 0
            elif data_type=="spend": r_pv[sum_col_idx]=round(total_prod_spend)
            elif data_type=="spend_ratio": r_pv[sum_col_idx]=""
            summary_data.append(r_pv)

    # ★ v23/v25: --- 나라별 테이블 ---
    if num_countries > 0:
        total_c_spend=sum(country_spend[c] for c in countries)
        total_c_revenue=sum(country_revenue[c] for c in countries)
        total_c_profit=sum(country_profit[c] for c in countries)
        country_tables=[("🌏 나라별 ROAS","roas"),("🌏 나라별 순이익","profit"),("🌏 나라별 매출","revenue")]
        for table_title, data_type in country_tables:
            summary_data.append([""]*NC)
            r_tt=[""]*NC; r_tt[12]=table_title; summary_data.append(r_tt)
            r_ch=[""]*NC
            for i,c in enumerate(countries): r_ch[9+i]=c
            r_ch[country_sum_col]="합"; summary_data.append(r_ch)
            r_cv=[""]*NC
            for i,c in enumerate(countries):
                cs=country_spend[c];cr_v=country_revenue[c];cp=country_profit[c]
                if data_type=="roas": r_cv[9+i]=cr_v/cs if cs>0 else 0
                elif data_type=="profit": r_cv[9+i]=round(cp)
                elif data_type=="revenue": r_cv[9+i]=round(cr_v)
            if data_type=="roas": r_cv[country_sum_col]=total_c_revenue/total_c_spend if total_c_spend>0 else 0
            elif data_type=="profit": r_cv[country_sum_col]=round(total_c_profit)
            elif data_type=="revenue": r_cv[country_sum_col]=round(total_c_revenue)
            summary_data.append(r_cv)

    return summary_data, num_products, countries


# =========================================================
# ★ v23: format_date_tab_summary — 나라별 테이블 서식 추가
# =========================================================
def format_date_tab_summary(sh, ws, summary_start_sheet_row, summary_row_count, num_products=0, countries=None):
    sid=ws.id; base=summary_start_sheet_row-1
    C_PINK={"red":0.957,"green":0.8,"blue":0.8}; C_ORANGE={"red":0.988,"green":0.898,"blue":0.804}
    C_LGREEN={"red":0.851,"green":0.918,"blue":0.827}; C_GRAY={"red":0.69,"green":0.7,"blue":0.698}
    C_DGRAY={"red":0.6,"green":0.6,"blue":0.6}; C_BLACK={"red":0,"green":0,"blue":0}
    C_WHITE={"red":1,"green":1,"blue":1}; C_LYELLOW={"red":1,"green":1,"blue":0.8}
    C_TEAL={"red":0.0,"green":0.5,"blue":0.5}
    fmt_requests=[]; r=base+3
    fmt_requests.append(create_format_request(sid,r,r+1,14,21,{"horizontalAlignment":"CENTER","textFormat":{"bold":True}}))
    fmt_requests.append({"mergeCells":{"range":{"sheetId":sid,"startRowIndex":r,"endRowIndex":r+1,"startColumnIndex":14,"endColumnIndex":21},"mergeType":"MERGE_ALL"}})
    r=base+4; hdr_fmt_base={"textFormat":{"bold":True,"foregroundColor":C_BLACK},"horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE"}
    fmt_requests.append(create_format_request(sid,r,r+1,11,12,{**hdr_fmt_base,"backgroundColor":C_PINK}))
    fmt_requests.append(create_format_request(sid,r,r+1,12,13,{**hdr_fmt_base,"backgroundColor":C_ORANGE}))
    fmt_requests.append(create_format_request(sid,r,r+1,13,14,{**hdr_fmt_base,"backgroundColor":C_LGREEN}))
    fmt_requests.append(create_format_request(sid,r,r+1,14,20,{**hdr_fmt_base,"backgroundColor":C_GRAY}))
    fmt_requests.append(create_format_request(sid,r,r+1,20,21,{**hdr_fmt_base,"backgroundColor":C_DGRAY}))
    r=base+5
    fmt_requests.append(create_number_format_request(sid,r,r+1,11,20,"NUMBER","#,##0"))
    fmt_requests.append(create_number_format_request(sid,r,r+1,19,20,"NUMBER","#,##0.0"))
    fmt_requests.append(create_number_format_request(sid,r,r+1,20,21,"NUMBER","0.00%"))
    fmt_requests.append(create_border_request(sid,base+4,base+6,11,21))

    # --- 제품별 서식 ---
    if num_products > 0:
        sum_col = 9 + num_products; prod_end_col = 9 + num_products + 1
        table_formats=[("ROAS","0.00%"),("순이익","[$₩-412]#,##0"),("매출","#,##0"),("순이익율","0.00%"),("예산","[$₩-412]#,##0"),("예산비중","0.00%")]
        prod_hdr_fmt={**hdr_fmt_base,"backgroundColor":C_GRAY}; offset=base+6
        for t_idx,(tname,nfmt) in enumerate(table_formats):
            t_start=offset+t_idx*4
            fmt_requests.append(create_format_request(sid,t_start+1,t_start+2,12,prod_end_col,{"horizontalAlignment":"CENTER","textFormat":{"bold":True}}))
            fmt_requests.append({"mergeCells":{"range":{"sheetId":sid,"startRowIndex":t_start+1,"endRowIndex":t_start+2,"startColumnIndex":12,"endColumnIndex":prod_end_col},"mergeType":"MERGE_ALL"}})
            fmt_requests.append(create_format_request(sid,t_start+2,t_start+3,9,prod_end_col,prod_hdr_fmt))
            fmt_requests.append(create_number_format_request(sid,t_start+3,t_start+4,9,prod_end_col,"NUMBER",nfmt))
            if t_idx in [1,2,4]:
                fmt_requests.append(create_format_request(sid,t_start+3,t_start+4,sum_col,sum_col+1,{"backgroundColor":C_LYELLOW,"textFormat":{"bold":True}}))
                fmt_requests.append(create_number_format_request(sid,t_start+3,t_start+4,sum_col,sum_col+1,"NUMBER","[$₩-412]#,##0"))
            fmt_requests.append(create_border_request(sid,t_start+2,t_start+4,9,prod_end_col))

    # ★ v23: --- 나라별 서식 ---
    if countries and len(countries) > 0:
        num_c = len(countries)
        c_sum_col = 9 + num_c; c_end_col = 9 + num_c + 1
        # 나라별 테이블 시작 오프셋 계산
        if num_products > 0:
            c_offset = base + 6 + num_products * 0 + 6 * 4  # 제품 6테이블 × 4행 후
        else:
            c_offset = base + 6  # 제품 없으면 바로

        country_table_formats = [("ROAS", "0.00%"), ("순이익", "[$₩-412]#,##0"), ("매출", "#,##0")]
        country_hdr_fmt = {**hdr_fmt_base, "backgroundColor": C_TEAL}
        for t_idx, (tname, nfmt) in enumerate(country_table_formats):
            t_start = c_offset + t_idx * 4
            fmt_requests.append(create_format_request(sid, t_start+1, t_start+2, 12, c_end_col, {"horizontalAlignment":"CENTER","textFormat":{"bold":True}}))
            fmt_requests.append({"mergeCells":{"range":{"sheetId":sid,"startRowIndex":t_start+1,"endRowIndex":t_start+2,"startColumnIndex":12,"endColumnIndex":c_end_col},"mergeType":"MERGE_ALL"}})
            fmt_requests.append(create_format_request(sid, t_start+2, t_start+3, 9, c_end_col, country_hdr_fmt))
            fmt_requests.append(create_number_format_request(sid, t_start+3, t_start+4, 9, c_end_col, "NUMBER", nfmt))
            if t_idx in [1, 2]:  # 순이익, 매출
                fmt_requests.append(create_format_request(sid, t_start+3, t_start+4, c_sum_col, c_sum_col+1, {"backgroundColor":C_LYELLOW,"textFormat":{"bold":True}}))
                fmt_requests.append(create_number_format_request(sid, t_start+3, t_start+4, c_sum_col, c_sum_col+1, "NUMBER", "[$₩-412]#,##0"))
            fmt_requests.append(create_border_request(sid, t_start+2, t_start+4, 9, c_end_col))

    return fmt_requests


# =============================================================================
# 실행 시작
# =============================================================================
print("\n"+"="*60); print("1단계: 광고 계정 설정"); print("="*60)
ALL_AD_ACCOUNTS = ["act_1054081590008088","act_2677707262628563"]
print(f"📋 광고 계정 ({len(ALL_AD_ACCOUNTS)}개):")
for acc in ALL_AD_ACCOUNTS: print(f"  - {acc}")
print()

# 2단계: Meta
print("="*60); print(f"2단계: Meta Insights 수집 (adset 레벨, 최근 {REFRESH_DAYS}일)"); print("="*60)
meta_date_data = defaultdict(list); meta_success_count = 0
for day_offset in range(META_COLLECT_DAYS):
    target_date=TODAY-timedelta(days=day_offset);target_str=target_date.strftime('%Y-%m-%d');date_key=f"{target_date.year%100:02d}/{target_date.month:02d}/{target_date.day:02d}"
    print(f"\n📅 {target_str} ({date_key}) 조회 중..."); day_rows=[]
    for acc_id in ALL_AD_ACCOUNTS:
        print(f"  📡 계정 {acc_id}..."); rows=fetch_meta_insights_daily(acc_id,target_str)
        if rows: parsed=parse_single_day_insights(rows,date_key,target_date); day_rows.extend(parsed); print(f"  ✅ {len(parsed)}건 (adset 레벨)")
        else: print(f"  ⚠️ 0건")
        time.sleep(2)
    if day_rows: meta_date_data[date_key]=day_rows; meta_success_count+=len(day_rows); print(f"  📊 {date_key} 합계: {len(day_rows)}개 광고 세트")
    else: print(f"  ⚠️ {date_key} 데이터 없음")
print(f"\n✅ Meta 수집 완료: {len(meta_date_data)}일, 총 {meta_success_count}건\n")

# 2.5단계: 예산
print("="*60); print("2.5단계: 광고 세트별 일일 예산 조회"); print("="*60)
adset_budget_map = {}
for acc_id in ALL_AD_ACCOUNTS:
    print(f"  📡 계정 {acc_id}..."); budgets = fetch_adset_budgets(acc_id); adset_budget_map.update(budgets)
    print(f"  ✅ {len(budgets)}개 활성 세트 예산 조회"); time.sleep(2)
print(f"\n✅ 전체 예산 조회 완료: {len(adset_budget_map)}개 세트\n")

# 3단계: Mixpanel
print("="*60); print(f"3단계: Mixpanel 수집 ({REFRESH_DAYS}일)"); print("="*60)
YESTERDAY=TODAY-timedelta(days=1)
mp_to_today=TODAY.strftime('%Y-%m-%d')
total_days=(TODAY-DATA_REFRESH_START).days+1
print(f"  📅 Mixpanel 범위: {DATA_REFRESH_START.strftime('%Y-%m-%d')} ~ {mp_to_today} ({total_days}일)")
mp_raw=[]
if REFRESH_DAYS > 14:
    CHUNK_SIZE = 7; chunk_start = DATA_REFRESH_START; chunk_num = 0
    while chunk_start <= YESTERDAY:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_SIZE - 1), YESTERDAY); chunk_num += 1
        print(f"\n  📦 청크 {chunk_num}: {chunk_start.strftime('%Y-%m-%d')} ~ {chunk_end.strftime('%Y-%m-%d')}")
        chunk_data = fetch_mixpanel_data(chunk_start.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d'))
        mp_raw.extend(chunk_data); chunk_start = chunk_end + timedelta(days=1); time.sleep(3)
else:
    mp_from=DATA_REFRESH_START.strftime('%Y-%m-%d'); mp_to_yesterday=YESTERDAY.strftime('%Y-%m-%d')
    if DATA_REFRESH_START <= YESTERDAY:
        chunk_data=fetch_mixpanel_data(mp_from, mp_to_yesterday); mp_raw.extend(chunk_data); time.sleep(2)
print(f"\n  ── 오늘({mp_to_today}) 별도 호출 ──")
today_data=fetch_mixpanel_data(mp_to_today,mp_to_today)
if today_data: mp_raw.extend(today_data); print(f"  ✅ 오늘: {len(today_data)}건 (누적: {len(mp_raw)}건)")
print(f"\n  ✅ Mixpanel 수집 완료: 총 {len(mp_raw)}건")
df=pd.DataFrame(mp_raw); mp_value_map={};mp_count_map={}
if len(df)>0:
    df=df[df['utm_term'].notna()&(df['utm_term']!='')&(df['utm_term']!='None')]
    df=df.sort_values('revenue',ascending=False); df_d=df.drop_duplicates(subset=['date','distinct_id','서비스'],keep='first')
    print(f"  utm_term 필터 후 중복제거: {len(df_d)}건 | 매출합: ₩{int(df_d['revenue'].sum()):,}")
    for (d,ut),v in df_d.groupby(['date','utm_term'])['revenue'].sum().items():
        if d and ut: mp_value_map[(d,str(ut))]=v
    for (d,ut),c in df_d.groupby(['date','utm_term']).size().items():
        if d and ut: mp_count_map[(d,str(ut))]=c
    print(f"  날짜+adset_id 조합: {len(mp_value_map)}개")
print()

# 4단계: 기존 탭 파악
print("="*60); print(f"4단계: 최근 {REFRESH_DAYS}일 기존 날짜탭 파악"); print("="*60)
refresh_date_keys=set()
for day_offset in range(REFRESH_DAYS):
    td=TODAY-timedelta(days=day_offset); dk=f"{td.year%100:02d}/{td.month:02d}/{td.day:02d}"; refresh_date_keys.add(dk)
existing_sheets=sh.worksheets(); existing_refresh_tabs={}; new_refresh_dates=set(refresh_date_keys)
for ws_ex in existing_sheets:
    tn=ws_ex.title; dt_obj=parse_date_tab(tn)
    if dt_obj is None: continue
    tab_dk=f"{dt_obj.year%100:02d}/{dt_obj.month:02d}/{dt_obj.day:02d}"
    if tab_dk in refresh_date_keys: existing_refresh_tabs[tab_dk]=ws_ex; new_refresh_dates.discard(tab_dk)
print(f"  📝 기존 탭 업데이트: {len(existing_refresh_tabs)}개 | 🆕 새로 생성: {len(new_refresh_dates)}개\n")

# 4.5단계: 환율 (USD/TWD/JPY/HKD)
print("="*60); print("4.5단계: USD/TWD/JPY/HKD → KRW 일별 환율 조회"); print("="*60)
rate_range_start = DATA_REFRESH_START - timedelta(days=7)
usd_krw_rates = fetch_daily_exchange_rates(rate_range_start, TODAY, currency="USD")
twd_krw_rates = fetch_daily_exchange_rates(rate_range_start, TODAY, currency="TWD")
jpy_krw_rates = fetch_daily_exchange_rates(rate_range_start, TODAY, currency="JPY")
hkd_krw_rates = fetch_daily_exchange_rates(rate_range_start, TODAY, currency="HKD")
for curr, rates, fb in [("USD",usd_krw_rates,FALLBACK_USD_KRW),("TWD",twd_krw_rates,FALLBACK_TWD_KRW),("JPY",jpy_krw_rates,FALLBACK_JPY_KRW),("HKD",hkd_krw_rates,FALLBACK_HKD_KRW)]:
    if rates:
        sample = list(rates.items())[-2:]
        for dk, rate in sample: print(f"  {curr} {dk}: ₩{rate:,.2f}")
    else: print(f"  ⚠️ {curr} 환율 조회 실패 → 폴백 ₩{fb:,}")
print()

# =========================================================
# ★ v25: 5단계 - Meta + Mixpanel 병합 (캠페인 이름도 활용한 통화 판별)
# =========================================================
print("="*60); print("5단계: Meta + Mixpanel 병합 (★v26: 서브스트링+한글+계정 기반 통화 판별)"); print("="*60)
date_tab_rows=defaultdict(list);date_mp_by_adsetid=defaultdict(dict);new_date_names=[];product_count=defaultdict(int)
debug_total_rows=0;debug_matched_rows=0;debug_matched_revenue=0
debug_기타_adset_names = set()
debug_currency_revenue = defaultdict(float); debug_currency_count = defaultdict(int)
adset_id_to_name = {}
# ★ v25: adset_id → campaign_name 매핑 추가
adset_id_to_campaign = {}

for dk in sorted(meta_date_data.keys(),key=lambda x:meta_date_data[x][0]['date_obj'] if meta_date_data[x] else datetime.min,reverse=True):
    rows=meta_date_data[dk]
    if not rows: continue
    dt=rows[0]['date_obj']; new_date_names.append(dk)
    fx_usd = get_rate_for_date(usd_krw_rates, dk)
    for mr in rows:
        asid=mr['adset_id']
        if not asid: continue
        debug_total_rows+=1
        adset_id_to_name[asid] = mr['adset_name']
        # ★ v25: campaign_name도 저장
        adset_id_to_campaign[asid] = mr['campaign_name']
        sp=mr['spend']*fx_usd;impr=mr['impressions'];reach=mr['reach'];uc=mr['unique_clicks'];results=mr['results']
        cpm=mr['cpm']*fx_usd;frequency=mr['frequency'];cost_per_result=mr['cost_per_result']*fx_usd;cost_per_click=mr['cost_per_unique_click']*fx_usd;unique_ctr=mr['unique_ctr']
        mpc=mp_count_map.get((dk,asid),0);mpv=mp_value_map.get((dk,asid),0.0)
        if mpc>0 or mpv>0: debug_matched_rows+=1;debug_matched_revenue+=mpv
        # ★ v26: campaign_name + adset_id도 전달하여 통화 판별
        currency = detect_currency(mr['adset_name'], campaign_name=mr['campaign_name'])
        fx_revenue = get_revenue_fx(currency, dk)
        rv = float(mpv) * fx_revenue
        if mpv > 0: debug_currency_revenue[currency] += rv; debug_currency_count[currency] += 1
        date_mp_by_adsetid[dk][asid]={'mpc':mpc,'mpv':rv}
        pf=rv-sp;roas_c=(rv/sp*100) if sp>0 else 0;cvr_c=(mpc/uc*100) if uc>0 and mpc>0 else 0
        cn=mr['campaign_name'];asn=mr['adset_name']
        budget_raw = adset_budget_map.get(asid, 0)
        budget_val = round(budget_raw / 100 * fx_usd) if budget_raw and budget_raw > 0 else ""
        tab_row=[cn,asn,asid,sp,cost_per_result,0,round(cpm,0),reach,impr,uc,round(unique_ctr,2),round(cost_per_click,0),round(frequency,2),results,
            mpc if mpc>0 else "",round(rv) if rv>0 else "",round(pf,0) if sp>0 else "",round(roas_c,1) if sp>0 and rv>0 else "",round(cvr_c,2) if uc>0 and mpc>0 else "",budget_val,"","",""]
        date_tab_rows[dk].append(tab_row); p=extract_product(asn); product_count[p]+=1
        if p == "기타": debug_기타_adset_names.add(asn)

print(f"✅ 날짜탭 구성 완료: {len(new_date_names)}개")
print(f"📦 제품별: {dict(product_count)}")
if debug_total_rows>0: print(f"🔍 매칭: {debug_matched_rows}/{debug_total_rows} ({debug_matched_rows/debug_total_rows*100:.1f}%)")
if debug_currency_revenue:
    print(f"\n💱 통화별 KRW 환산 매출:")
    for curr in sorted(debug_currency_revenue.keys()): print(f"  {curr} ({CURRENCY_TO_COUNTRY.get(curr,'?')}): ₩{int(debug_currency_revenue[curr]):,} ({debug_currency_count[curr]}건)")
# ★ v26: 통화 판별 디버그 — 캠페인명 기반 판별 건수 표시
debug_detected_by_campaign = 0
debug_undetected = 0
debug_currency_final = defaultdict(int)
for asid in adset_id_to_name:
    asn = adset_id_to_name[asid]
    cn = adset_id_to_campaign.get(asid, '')
    from_adset = _detect_currency_from_name(asn)
    from_campaign = _detect_currency_from_name(cn) if not from_adset else None
    if from_campaign:
        debug_detected_by_campaign += 1
    elif not from_adset and not from_campaign:
        debug_undetected += 1
    final_currency = detect_currency(asn, campaign_name=cn)
    debug_currency_final[final_currency] += 1
print(f"\n  ★ v26 통화 판별 결과:")
for curr in sorted(debug_currency_final.keys()):
    print(f"    {curr} ({CURRENCY_TO_COUNTRY.get(curr,'?')}): {debug_currency_final[curr]}개 세트")
if debug_detected_by_campaign > 0:
    print(f"    → 캠페인 이름으로 감지: {debug_detected_by_campaign}개 (세트명에는 나라 없었음)")
if debug_undetected > 0:
    print(f"    ⚠️ 이름에서 나라 미감지 → TWD 기본값: {debug_undetected}개")
if debug_기타_adset_names:
    print(f"\n  ⚠️ '기타' 광고 세트: {len(debug_기타_adset_names)}개")
    for name in sorted(debug_기타_adset_names)[:10]: print(f"     → '{name}'")
print()

# 6단계: 분석탭 삭제
print("="*60); print("6단계: 분석탭만 삭제"); print("="*60)
ANALYSIS_TAB_NAMES=["마스터탭","추이차트","증감액","추이차트(주간)","주간종합","주간종합_2","주간종합_3","예산","_temp","_temp_holder"]
for sn in ANALYSIS_TAB_NAMES:
    try:
        old=sh.worksheet(sn)
        if len(sh.worksheets())<=1: with_retry(sh.add_worksheet,title="_tmp",rows=1,cols=1); time.sleep(1)
        sh.del_worksheet(old); print(f"  ✅ '{sn}' 삭제"); time.sleep(2)
    except gspread.exceptions.WorksheetNotFound: pass
    except Exception as e: print(f"  ⚠️ '{sn}' 삭제 실패: {e}"); time.sleep(3)
print("⏳ 5초 대기..."); time.sleep(5)

# 7-A: 기존 날짜탭 업데이트
print("\n"+"="*60); print("7단계: 기존 탭 업데이트 + 새 탭 생성"); print("="*60)
print("\n--- 7-A: 기존 날짜탭 업데이트 ---")
for dk in sorted(existing_refresh_tabs.keys()):
    ws_ex=existing_refresh_tabs[dk]; mp_data=date_mp_by_adsetid.get(dk,{}); new_rows_for_date=date_tab_rows.get(dk,[])
    print(f"\n  📝 {dk} (기존 탭) 업데이트 중...")
    try:
        ws_ex = refresh_ws(sh, ws_ex); all_values=with_retry(ws_ex.get_all_values); time.sleep(0.5)
        if not all_values or len(all_values)<2: print(f"    ⚠️ 빈 탭"); continue
        structure=detect_tab_structure(all_values[0]); sid_ex=ws_ex.id
        total_rows_for_clear = max(len(all_values)+50, 200); fmt_clear_start_col = get_col_index(structure, 3)
        try:
            with_retry(sh.batch_update, body={"requests": [
                {"unmergeCells":{"range":{"sheetId":sid_ex,"startRowIndex":1,"endRowIndex":total_rows_for_clear,"startColumnIndex":0,"endColumnIndex":30}}},
                {"repeatCell":{"range":{"sheetId":sid_ex,"startRowIndex":1,"endRowIndex":total_rows_for_clear,"startColumnIndex":fmt_clear_start_col,"endColumnIndex":30},"cell":{"userEnteredFormat":{}},"fields":"userEnteredFormat.numberFormat"}}
            ]}); time.sleep(1)
        except: pass
        actual_asid_col=get_col_index(structure,2)
        adset_id_row_map={};last_data_sheet_row=1
        for i,row in enumerate(all_values[1:],start=2):
            if not row: continue
            cn=str(row[0]).strip() if len(row)>0 else ""; asid=str(row[actual_asid_col]).strip() if len(row)>actual_asid_col else ""
            if cn in ["캠페인 이름","전체","합계","Total"]: continue
            if not cn and not asid: continue
            if asid: adset_id_row_map[asid]=i-1
            last_data_sheet_row=i
        data_end_idx=last_data_sheet_row
        batch_updates=[];updated_count=0;new_row_by_asid={}
        for tab_row in new_rows_for_date:
            asid_check=str(tab_row[2]).strip() if len(tab_row)>2 else ""
            if asid_check: new_row_by_asid[asid_check]=tab_row
        if structure == "new": update_end_col_letter = "S"; data_col_count = 19
        else: update_end_col_letter = "U"; data_col_count = 21
        for asid, row_idx in adset_id_row_map.items():
            row_num=row_idx+1
            if asid not in new_row_by_asid:
                if asid in mp_data:
                    existing_row=all_values[row_idx]; actual_spend_col=get_col_index(structure,3);actual_uc_col=get_col_index(structure,9)
                    actual_mp_col=get_col_index(structure,14);actual_cvr_col=get_col_index(structure,18)
                    spend=_to_num(existing_row[actual_spend_col]) if len(existing_row)>actual_spend_col else 0
                    unique_clicks=_to_num(existing_row[actual_uc_col]) if len(existing_row)>actual_uc_col else 0
                    mpc=mp_data[asid]['mpc'];mpv=mp_data[asid]['mpv'];revenue=float(mpv)
                    profit=revenue-spend;roas_val=(revenue/spend*100) if spend>0 and revenue>0 else 0
                    cvr_val=(mpc/unique_clicks*100) if unique_clicks>0 and mpc>0 else 0
                    mp_col_letter=get_col_letter(actual_mp_col);cvr_col_letter=get_col_letter(actual_cvr_col)
                    batch_updates.append({'range':f'{mp_col_letter}{row_num}:{cvr_col_letter}{row_num}',
                        'values':[[int(mpc) if mpc>0 else "",round(revenue) if revenue>0 else "",round(profit,0) if spend>0 else "",round(roas_val,1) if spend>0 and revenue>0 else "",round(cvr_val,2) if unique_clicks>0 and mpc>0 else ""]]})
                continue
            new_tab_row=new_row_by_asid[asid]
            if structure == "new":
                update_row = list(new_tab_row[:data_col_count])
                while len(update_row) < data_col_count: update_row.append("")
                batch_updates.append({'range':f'A{row_num}:{update_end_col_letter}{row_num}','values':[update_row]})
            else:
                old_row = list(new_tab_row[:3]) + ["",""] + list(new_tab_row[3:19])
                while len(old_row) < data_col_count: old_row.append("")
                old_row = old_row[:data_col_count]
                batch_updates.append({'range':f'A{row_num}:{update_end_col_letter}{row_num}','values':[old_row]})
            updated_count+=1
        if batch_updates:
            for i in range(0,len(batch_updates),100):
                with_retry(ws_ex.batch_update, batch_updates[i:i+100], value_input_option="USER_ENTERED"); time.sleep(1)
            print(f"    ✅ {updated_count}개 업데이트")
        budget_updates = []; actual_budget_col = get_col_index(structure, 19); budget_col_letter = get_col_letter(actual_budget_col)
        fx_budget = get_rate_for_date(usd_krw_rates, dk)
        for asid, row_idx in adset_id_row_map.items():
            if asid in adset_budget_map and adset_budget_map[asid] > 0:
                row_num = row_idx + 1; budget_krw = round(adset_budget_map[asid] / 100 * fx_budget)
                budget_updates.append({'range': f'{budget_col_letter}{row_num}', 'values': [[budget_krw]]})
        if budget_updates:
            for i in range(0, len(budget_updates), 100):
                with_retry(ws_ex.batch_update, budget_updates[i:i+100], value_input_option="USER_ENTERED"); time.sleep(1)
        existing_asids=set(adset_id_row_map.keys());new_rows_to_add=[]
        for tab_row in new_rows_for_date:
            asid_check=str(tab_row[2]).strip() if len(tab_row)>2 else ""
            if asid_check and asid_check not in existing_asids:
                if structure=="old":
                    old_row=list(tab_row[:3])+["",""]+list(tab_row[3:])
                    while len(old_row)<25: old_row.append("")
                    new_rows_to_add.append(old_row[:25])
                else: new_rows_to_add.append(tab_row)
        if new_rows_to_add:
            actual_profit_col=get_col_index(structure,16)
            new_rows_to_add.sort(key=lambda r:_to_num(r[actual_profit_col]) if len(r)>actual_profit_col else 0,reverse=True)
            append_row=data_end_idx+1
            with_retry(ws_ex.update,values=new_rows_to_add,range_name=f"A{append_row}",value_input_option="USER_ENTERED")
            print(f"    ✅ {len(new_rows_to_add)}개 신규 행 추가"); time.sleep(1); data_end_idx+=len(new_rows_to_add)
        # 요약표 재생성
        all_values=with_retry(ws_ex.get_all_values); time.sleep(0.5); structure=detect_tab_structure(all_values[0])
        last_data_row, data_rows_for_summary = find_last_data_row(all_values, structure)
        summary_start_row=last_data_row+1
        NUM_COLS_FILTER = len(DATE_TAB_HEADERS) if structure == "new" else len(OLD_DATE_TAB_HEADERS)
        try:
            try: with_retry(sh.batch_update, body={"requests": [{"clearBasicFilter": {"sheetId": ws_ex.id}}]})
            except: pass
            with_retry(sh.batch_update, body={"requests": [{"setBasicFilter": {"filter": {"range": {"sheetId": ws_ex.id, "startRowIndex": 0, "endRowIndex": last_data_row, "startColumnIndex": 0, "endColumnIndex": NUM_COLS_FILTER}}}}]})
            time.sleep(0.5)
        except: pass
        summary_rows, num_products, countries=generate_date_tab_summary(data_rows_for_summary,structure=structure)
        needed_rows = summary_start_row + len(summary_rows) + 10; total_sheet_rows=len(all_values)
        if needed_rows > total_sheet_rows:
            try: with_retry(ws_ex.resize, rows=needed_rows); time.sleep(0.5)
            except: pass
        clear_end_row=max(total_sheet_rows+50,needed_rows)
        try:
            ws_ex = refresh_ws(sh, ws_ex); clear_summary_conditional_formats(sh, ws_ex, summary_start_row-1)
            with_retry(sh.batch_update, body={"requests":[
                {"unmergeCells":{"range":{"sheetId":ws_ex.id,"startRowIndex":summary_start_row-1,"endRowIndex":clear_end_row,"startColumnIndex":0,"endColumnIndex":30}}},
                {"repeatCell":{"range":{"sheetId":ws_ex.id,"startRowIndex":summary_start_row-1,"endRowIndex":clear_end_row,"startColumnIndex":0,"endColumnIndex":30},"cell":{"userEnteredFormat":{},"userEnteredValue":{}},"fields":"userEnteredFormat,userEnteredValue"}},
                {"updateBorders":{"range":{"sheetId":ws_ex.id,"startRowIndex":summary_start_row-1,"endRowIndex":clear_end_row,"startColumnIndex":0,"endColumnIndex":30},"top":{"style":"NONE"},"bottom":{"style":"NONE"},"left":{"style":"NONE"},"right":{"style":"NONE"},"innerHorizontal":{"style":"NONE"},"innerVertical":{"style":"NONE"}}}
            ]}); time.sleep(0.5)
        except: pass
        with_retry(ws_ex.update,values=summary_rows,range_name=f"A{summary_start_row}",value_input_option="USER_ENTERED"); time.sleep(1)
        try:
            fmt_reqs=format_date_tab_summary(sh,ws_ex,summary_start_row,len(summary_rows),num_products=num_products,countries=countries)
            if fmt_reqs:
                for i in range(0,len(fmt_reqs),50): with_retry(sh.batch_update,body={"requests":fmt_reqs[i:i+50]}); time.sleep(1)
        except Exception as e: print(f"    ⚠️ 요약표 서식 오류: {e}")
        print(f"    ✅ 요약표 완료 (상품 {num_products}개, 나라 {len(countries)}개)")
    except Exception as e: print(f"    ⚠️ {dk} 업데이트 오류: {e}")
print(f"\n✅ 기존 {len(existing_refresh_tabs)}개 탭 업데이트 완료")

# 7-B: 새 날짜탭 생성
print(f"\n--- 7-B: 새 날짜탭 생성 ({len(new_refresh_dates)}개) ---")
for dk in sorted(new_refresh_dates):
    rows=date_tab_rows.get(dk,[])
    if not rows: continue
    rows.sort(key=lambda r:_to_num(r[16]) if len(r)>16 else 0,reverse=True)
    print(f"  📅 {dk} ({len(rows)}개 광고 세트)")
    summary_rows, num_products, countries=generate_date_tab_summary(rows,structure="new")
    try:
        total_rows=len(rows)+len(summary_rows)+5;NUM_COLS=len(DATE_TAB_HEADERS)
        ws_d=safe_add_worksheet(sh,dk,rows=total_rows,cols=NUM_COLS+2); time.sleep(1)
        sid_d=ws_d.id; data_end=len(rows)+1
        all_data=[DATE_TAB_HEADERS]+rows+summary_rows
        with_retry(ws_d.update,values=all_data,range_name="A1",value_input_option="USER_ENTERED")
        fmt_all=[]
        fmt_all.append(create_format_request(sid_d,0,1,0,3,{"textFormat":{"bold":True},"wrapStrategy":"WRAP","verticalAlignment":"MIDDLE"}))
        fmt_all.append(create_format_request(sid_d,0,1,3,14,{"backgroundColor":{"red":0.937,"green":0.937,"blue":0.937},"textFormat":{"bold":True},"wrapStrategy":"WRAP","verticalAlignment":"MIDDLE"}))
        fmt_all.append(create_format_request(sid_d,0,1,14,19,{"backgroundColor":{"red":0.6,"green":0.6,"blue":0.6},"textFormat":{"bold":True},"wrapStrategy":"WRAP","horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE"}))
        fmt_all.append(create_format_request(sid_d,0,1,19,20,{"backgroundColor":{"red":0.851,"green":0.824,"blue":0.914},"textFormat":{"bold":True},"wrapStrategy":"WRAP","horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE"}))
        fmt_all.append(create_format_request(sid_d,0,1,20,21,{"backgroundColor":{"red":0.706,"green":0.655,"blue":0.839},"textFormat":{"bold":True,"foregroundColor":{"red":1,"green":1,"blue":1}},"wrapStrategy":"WRAP","horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE"}))
        fmt_all.append(create_format_request(sid_d,0,1,21,22,{"backgroundColor":{"red":0.6,"green":0,"blue":1},"textFormat":{"bold":True,"foregroundColor":{"red":1,"green":1,"blue":1}},"wrapStrategy":"WRAP","horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE"}))
        fmt_all.append(create_format_request(sid_d,0,1,22,23,{"backgroundColor":{"red":1,"green":0.6,"blue":0},"textFormat":{"bold":True},"wrapStrategy":"WRAP","verticalAlignment":"MIDDLE"}))
        fmt_all.append(create_number_format_request(sid_d,1,data_end,14,15,"NUMBER","#,##0"))
        fmt_all.append(create_number_format_request(sid_d,1,data_end,15,16,"NUMBER","#,##0"))
        fmt_all.append(create_number_format_request(sid_d,1,data_end,16,17,"NUMBER","#,##0"))
        fmt_all.append(create_number_format_request(sid_d,1,data_end,17,18,"NUMBER","#,##0.0"))
        fmt_all.append(create_number_format_request(sid_d,1,data_end,18,19,"NUMBER","#,##0.00"))
        fmt_all.append(create_number_format_request(sid_d,1,data_end,19,20,"NUMBER","#,##0"))
        fmt_all.append(create_number_format_request(sid_d,0,1,20,21,"NUMBER","0%"))
        fmt_all.append(create_number_format_request(sid_d,1,data_end,20,21,"NUMBER","0%"))
        fmt_all.append(create_number_format_request(sid_d,1,data_end,21,22,"NUMBER","0.0"))
        col_widths=[(0,210),(1,207),(2,130)]
        for ci,w in col_widths: fmt_all.append({"updateDimensionProperties":{"range":{"sheetId":sid_d,"dimension":"COLUMNS","startIndex":ci,"endIndex":ci+1},"properties":{"pixelSize":w},"fields":"pixelSize"}})
        fmt_all.append({"setBasicFilter":{"filter":{"range":{"sheetId":sid_d,"startRowIndex":0,"endRowIndex":data_end,"startColumnIndex":0,"endColumnIndex":NUM_COLS}}}})
        fmt_all.append({"updateSheetProperties":{"properties":{"sheetId":sid_d,"gridProperties":{"frozenRowCount":1,"frozenColumnCount":3}},"fields":"gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}})
        profit_cond=[("NUMBER_GREATER","0",{"red":1,"green":0.949,"blue":0.8},None),("NUMBER_GREATER","50000",{"red":1,"green":0.898,"blue":0.6},None),("NUMBER_GREATER","100000",{"red":0.576,"green":0.769,"blue":0.49},None),("NUMBER_GREATER","200000",{"red":0,"green":1,"blue":0},None),("NUMBER_GREATER","300000",{"red":0,"green":1,"blue":1},None),("NUMBER_LESS","-10000",{"red":0.957,"green":0.8,"blue":0.8},None),("NUMBER_LESS","-50000",{"red":0.878,"green":0.4,"blue":0.4},{"red":0,"green":0,"blue":0})]
        for ctype,val,bg,fc in profit_cond:
            rule_fmt={"backgroundColor":bg}
            if fc: rule_fmt["textFormat"]={"foregroundColor":fc}
            fmt_all.append({"addConditionalFormatRule":{"rule":{"ranges":[{"sheetId":sid_d,"startRowIndex":1,"endRowIndex":data_end,"startColumnIndex":16,"endColumnIndex":17}],"booleanRule":{"condition":{"type":ctype,"values":[{"userEnteredValue":val}]},"format":rule_fmt}},"index":0}})
        fmt_all.append({"addConditionalFormatRule":{"rule":{"ranges":[{"sheetId":sid_d,"startRowIndex":1,"endRowIndex":data_end,"startColumnIndex":16,"endColumnIndex":17}],"gradientRule":{"minpoint":{"color":{"red":0.902,"green":0.486,"blue":0.451},"type":"MIN"},"midpoint":{"color":{"red":1,"green":1,"blue":1},"type":"NUMBER","value":"0"},"maxpoint":{"color":{"red":0.341,"green":0.733,"blue":0.541},"type":"MAX"}}},"index":0}})
        roas_cond=[("NUMBER_LESS","100",{"red":0.878,"green":0.4,"blue":0.4}),("NUMBER_GREATER_THAN_EQ","100",{"red":1,"green":0.851,"blue":0.4}),("NUMBER_GREATER_THAN_EQ","200",{"red":0.576,"green":0.769,"blue":0.49}),("NUMBER_GREATER_THAN_EQ","300",{"red":0,"green":1,"blue":1})]
        for ctype,val,bg in roas_cond: fmt_all.append({"addConditionalFormatRule":{"rule":{"ranges":[{"sheetId":sid_d,"startRowIndex":1,"endRowIndex":data_end,"startColumnIndex":17,"endColumnIndex":18}],"booleanRule":{"condition":{"type":ctype,"values":[{"userEnteredValue":val}]},"format":{"backgroundColor":bg}}},"index":0}})
        fmt_all.append({"addConditionalFormatRule":{"rule":{"ranges":[{"sheetId":sid_d,"startRowIndex":1,"endRowIndex":data_end,"startColumnIndex":18,"endColumnIndex":19}],"gradientRule":{"minpoint":{"color":{"red":1,"green":1,"blue":1},"type":"MIN"},"maxpoint":{"color":{"red":0.341,"green":0.733,"blue":0.541},"type":"MAX"}}},"index":0}})
        try: with_retry(sh.batch_update,body={"requests":fmt_all}); time.sleep(2)
        except Exception as e: print(f"    ⚠️ 서식 오류: {e}")
        try:
            summary_start_for_new=len(rows)+2
            fmt_reqs=format_date_tab_summary(sh,ws_d,summary_start_for_new,len(summary_rows),num_products=num_products,countries=countries)
            if fmt_reqs:
                for i in range(0,len(fmt_reqs),50): with_retry(sh.batch_update,body={"requests":fmt_reqs[i:i+50]}); time.sleep(1)
        except Exception as e: print(f"    ⚠️ 요약표 서식 오류: {e}")
        time.sleep(1)
    except Exception as e: print(f"  ⚠️ {dk} 생성 오류: {e}")
print(f"\n✅ 기존 {len(existing_refresh_tabs)}개 업데이트 + 새 {len(new_refresh_dates)}개 생성 완료"); time.sleep(3)

# ★ v24: 7.5단계 — 날짜탭 생성 직후 바로 탭 순서 정리
print("\n"+"="*60); print("7.5단계: 날짜탭 순서 정리 (최신→과거)"); print("="*60)
reorder_tabs(sh)

# ★ v26: 8.5단계 — mp_value_map_krw 변환 시 campaign_name + adset_id 활용
mp_value_map_krw = {}
for (d, ut), v in mp_value_map.items():
    adset_name = adset_id_to_name.get(ut, '')
    campaign_name = adset_id_to_campaign.get(ut, '')  # ★ v25
    currency = detect_currency(adset_name, campaign_name=campaign_name)  # ★ v26
    fx = get_revenue_fx(currency, d)
    mp_value_map_krw[(d, ut)] = v * fx
ad_sets, budget_by_date, master_raw_data, date_objects, date_names = read_all_date_tabs(sh, ANALYSIS_TABS_SET, mp_value_map=mp_value_map_krw, mp_count_map=mp_count_map)
diagnose_chart_coverage(sh, date_names, ad_sets, ANALYSIS_TABS_SET)

print("\n⏳ 8.5→9단계 전환: 60초 대기..."); time.sleep(60)
master_headers = ["Date"] + DATE_TAB_HEADERS; latest_date = date_names[-1] if date_names else ""
all_products_in_budget = set()
for dk in date_names:
    for p in budget_by_date[dk]:
        if budget_by_date[dk][p]['spend'] > 0 or budget_by_date[dk][p]['revenue'] > 0: all_products_in_budget.add(p)
product_order = sorted([p for p in all_products_in_budget if p in PRODUCT_KEYWORDS], key=lambda p: sum(budget_by_date[dk][p]['spend'] for dk in date_names), reverse=True)
print(f"\n📦 제품 순서: {product_order}")
chart_dn = list(reversed(date_names)); chart_sd = chart_dn[:7]
print(f"📊 추이차트: 전체 {len(chart_dn)}개 날짜 사용")

# 5.5: 주간 집계
print("\n5.5단계: 주간 집계")
week_groups=defaultdict(list);week_display_names={}
for t in date_names: do=date_objects[t];wk=get_week_range_short(do);wd=get_week_range(do);week_groups[wk].append(t);week_display_names[wk]=wd
week_ranges=[]
for wr in week_groups: sd=week_groups[wr][0];sdo=date_objects[sd];m=get_week_monday(sdo);week_ranges.append((wr,m))
week_ranges.sort(key=lambda x:x[1]); week_keys=[wr[0] for wr in week_ranges]
print(f"✅ 주차: {len(week_keys)}개 | 날짜: {len(date_names)}개")
adset_weekly=defaultdict(lambda:{'campaign_name':'','adset_name':'','adset_id':'','weeks':{}})
for asid,d in ad_sets.items():
    adset_weekly[asid]['campaign_name']=d['campaign_name'];adset_weekly[asid]['adset_name']=d['adset_name'];adset_weekly[asid]['adset_id']=d['adset_id']
    for wk in week_keys:
        wp,wr_v,ws_v=0,0,0;wcs,wcc=0,0;wvs,wvc=0,0
        for dn in week_groups[wk]:
            if dn in d['dates']:
                wp+=d['dates'][dn]['profit'];wr_v+=d['dates'][dn]['revenue'];ws_v+=d['dates'][dn]['spend']
                cv=d['dates'][dn].get('cpm',0)
                if cv>0:wcs+=cv;wcc+=1
                vv=d['dates'][dn].get('cvr',0)
                if vv>0:wvs+=vv;wvc+=1
        if ws_v>0 or wr_v>0: adset_weekly[asid]['weeks'][wk]={'profit':wp,'revenue':wr_v,'spend':ws_v,'cpm':(wcs/wcc) if wcc>0 else 0,'cvr':(wvs/wvc) if wvc>0 else 0}
sorted_list=[]
for asid,d in ad_sets.items():
    sorted_list.append({'campaign_name':d['campaign_name'],'adset_name':d['adset_name'],'adset_id':d['adset_id'],'data':d})
def _multi_spend_key(item):
    return tuple(item['data']['dates'].get(d,{}).get('spend',0) for d in chart_dn)
sorted_list.sort(key=_multi_spend_key, reverse=True); chart_wk=list(reversed(week_keys))

# =========================================================
# ★ v24: 분석탭 생성 — 생성 즉시 맨 왼쪽(index=0)으로 이동
# =========================================================

# 9: 마스터탭
print("\n9단계: 마스터탭 생성")
ws_m=safe_add_worksheet(sh,"마스터탭",rows=max(2000,len(master_raw_data)+100),cols=len(master_headers)+5); time.sleep(3)
move_to_front(sh, ws_m)  # ★ v24: 즉시 맨 왼쪽
master_raw_data.sort(key=lambda x:(x['date_obj'],-x['spend']),reverse=True)
for item in master_raw_data:
    while len(item['row_data'])<len(master_headers): item['row_data'].append("")
    item['row_data']=item['row_data'][:len(master_headers)]
mr_all=[master_headers]+[i['row_data'] for i in master_raw_data]
for i in range(0,len(mr_all),5000): with_retry(ws_m.update,values=mr_all[i:i+5000],range_name=f"A{i+1}",value_input_option="USER_ENTERED"); time.sleep(2)
try:
    with_retry(ws_m.format,'A1:T1',{'backgroundColor':{'red':0.9,'green':0.9,'blue':0.9},'textFormat':{'bold':True}})
    with_retry(sh.batch_update,body={"requests":[{"updateSheetProperties":{"properties":{"sheetId":ws_m.id,"gridProperties":{"frozenRowCount":1}},"fields":"gridProperties.frozenRowCount"}}]})
except: pass
print(f"✅ 마스터탭 완료 ({len(master_raw_data)}행)"); time.sleep(2)

# 10: 추이차트
print("\n10단계: 추이차트")
ws_t=safe_add_worksheet(sh,"추이차트",rows=1000,cols=len(chart_dn)+10); time.sleep(3)
move_to_front(sh, ws_t)  # ★ v24: 즉시 맨 왼쪽
dhw=[];sci=[]
for i,n in enumerate(chart_dn): do=date_objects[n];wd=WEEKDAY_NAMES[do.weekday()];dhw.append(f"{n}({wd})"); (sci.append(4+i) if do.weekday()==6 else None)
hdr_t=['캠페인 이름','광고 세트 이름','광고 세트 ID','7일 평균']+dhw
sr=["종합","",""];tp,tr,ts=0,0,0;tcs,tcc=0,0;tvs,tvc=0,0
for d in chart_sd:
    for it in sorted_list:
        if d in it['data']['dates']:
                tp+=it['data']['dates'][d]['profit'];tr+=it['data']['dates'][d]['revenue'];ts+=it['data']['dates'][d]['spend']
                cv=it['data']['dates'][d].get('cpm',0)
                if cv>0: tcs+=cv; tcc+=1
                vv=it['data']['dates'][d].get('cvr',0)
                if vv>0: tvs+=vv; tvc+=1
sr.append(cell_text(tp,tr,ts,(tcs/tcc) if tcc>0 else 0,(tvs/tvc) if tvc>0 else 0))
for d in chart_dn:
    dp,dr,ds_v=0,0,0;dcs,dcc=0,0;dvs,dvc=0,0
    for it in sorted_list:
        if d in it['data']['dates']:
            dp+=it['data']['dates'][d]['profit'];dr+=it['data']['dates'][d]['revenue'];ds_v+=it['data']['dates'][d]['spend']
            cv=it['data']['dates'][d].get('cpm',0)
            if cv>0:dcs+=cv;dcc+=1
            vv=it['data']['dates'][d].get('cvr',0)
            if vv>0:dvs+=vv;dvc+=1
    sr.append(cell_text(dp,dr,ds_v,(dcs/dcc) if dcc>0 else 0,(dvs/dvc) if dvc>0 else 0))
rt=[]
for it in sorted_list:
    r=[it['campaign_name'],it['adset_name'],it['adset_id']];tp,tr,ts=0,0,0;tcs,tcc=0,0;tvs,tvc=0,0
    for d in chart_sd:
        if d in it['data']['dates']:
                tp+=it['data']['dates'][d]['profit'];tr+=it['data']['dates'][d]['revenue'];ts+=it['data']['dates'][d]['spend']
                cv=it['data']['dates'][d].get('cpm',0)
                if cv>0: tcs+=cv; tcc+=1
                vv=it['data']['dates'][d].get('cvr',0)
                if vv>0: tvs+=vv; tvc+=1
    r.append(cell_text(tp,tr,ts,(tcs/tcc) if tcc>0 else 0,(tvs/tvc) if tvc>0 else 0))
    for d in chart_dn:
        if d in it['data']['dates']:dt=it['data']['dates'][d];r.append(cell_text(dt['profit'],dt['revenue'],dt['spend'],dt.get('cpm',0),dt.get('cvr',0)))
        else:r.append('')
    rt.append(r)
with_retry(ws_t.update,values=[hdr_t]+[sr]+rt,range_name="A1",value_input_option="USER_ENTERED")
print(f"✅ 추이차트 완료"); time.sleep(3)
ws_t = refresh_ws(sh, ws_t); apply_trend_chart_formatting(sh,ws_t,hdr_t,len(rt),sunday_col_indices=sci)
try: ws_t = refresh_ws(sh, ws_t); apply_c2_label_formatting(sh,ws_t)
except: pass
print("⏳ 30초 대기..."); time.sleep(30)

# 11: 추이차트(주간)
print("\n11단계: 추이차트(주간)")
ws_tw=safe_add_worksheet(sh,"추이차트(주간)",rows=1000,cols=len(chart_wk)+10); time.sleep(3)
move_to_front(sh, ws_tw)  # ★ v24: 즉시 맨 왼쪽
wdl=[week_display_names[wk] for wk in chart_wk]; hdr_w=['캠페인 이름','광고 세트 이름','광고 세트 ID','전체 평균']+wdl
srw=["종합","",""];tap,tar,tas=0,0,0;tacs,tacc=0,0;tavs,tavc=0,0
for wk in chart_wk:
    for it in sorted_list:
        wd=adset_weekly[it['adset_id']]['weeks'].get(wk,{})
        if wd: tap+=wd['profit'];tar+=wd['revenue'];tas+=wd['spend'];cv=wd.get('cpm',0);(tacs:=tacs+cv,tacc:=tacc+1) if cv>0 else None;vv=wd.get('cvr',0);(tavs:=tavs+vv,tavc:=tavc+1) if vv>0 else None
srw.append(cell_text(tap,tar,tas,(tacs/tacc) if tacc>0 else 0,(tavs/tavc) if tavc>0 else 0))
for wk in chart_wk:
    wp,wr_v,ws_v=0,0,0;wcs,wcc=0,0;wvs,wvc=0,0
    for it in sorted_list:
        wd=adset_weekly[it['adset_id']]['weeks'].get(wk,{})
        if wd: wp+=wd['profit'];wr_v+=wd['revenue'];ws_v+=wd['spend'];cv=wd.get('cpm',0);(wcs:=wcs+cv,wcc:=wcc+1) if cv>0 else None;vv=wd.get('cvr',0);(wvs:=wvs+vv,wvc:=wvc+1) if vv>0 else None
    srw.append(cell_text(wp,wr_v,ws_v,(wcs/wcc) if wcc>0 else 0,(wvs/wvc) if wvc>0 else 0))
rtw=[]
for it in sorted_list:
    r=[it['campaign_name'],it['adset_name'],it['adset_id']];tp,tr,ts=0,0,0;tcs,tcc=0,0;tvs,tvc=0,0
    for wk in chart_wk:
        wd=adset_weekly[it['adset_id']]['weeks'].get(wk,{})
        if wd: tp+=wd['profit'];tr+=wd['revenue'];ts+=wd['spend'];cv=wd.get('cpm',0);(tcs:=tcs+cv,tcc:=tcc+1) if cv>0 else None;vv=wd.get('cvr',0);(tvs:=tvs+vv,tvc:=tvc+1) if vv>0 else None
    r.append(cell_text(tp,tr,ts,(tcs/tcc) if tcc>0 else 0,(tvs/tvc) if tvc>0 else 0))
    for wk in chart_wk:
        wd=adset_weekly[it['adset_id']]['weeks'].get(wk,{})
        r.append(cell_text(wd['profit'],wd['revenue'],wd['spend'],wd.get('cpm',0),wd.get('cvr',0)) if wd else '')
    rtw.append(r)
with_retry(ws_tw.update,values=[hdr_w]+[srw]+rtw,range_name="A1",value_input_option="USER_ENTERED")
print("✅ 추이차트(주간) 완료"); time.sleep(3)
wfce=3+1+WEEKLY_TREND_REFRESH_WEEKS; ws_tw = refresh_ws(sh, ws_tw)
apply_trend_chart_formatting(sh,ws_tw,hdr_w,len(rtw),format_col_end=wfce)
try: ws_tw = refresh_ws(sh, ws_tw); apply_c2_label_formatting(sh,ws_tw)
except: pass
print("⏳ 30초 대기..."); time.sleep(30)

# 12: 증감액
print("\n12단계: 증감액")
ws_c=safe_add_worksheet(sh,"증감액",rows=1000,cols=len(chart_dn)+10); time.sleep(3)
move_to_front(sh, ws_c)  # ★ v24: 즉시 맨 왼쪽
hdr_c=['캠페인 이름','광고 세트 이름','광고 세트 ID','7일 평균']+chart_dn
src=["종합","",""];t7r,t7s=0,0;t7cs,t7cc=0,0;t7vs,t7vc=0,0
for d in chart_sd:
    for it in sorted_list:
        if d in it['data']['dates']:
                t7r+=it['data']['dates'][d]['revenue'];t7s+=it['data']['dates'][d]['spend']
                cv=it['data']['dates'][d].get('cpm',0)
                if cv>0: t7cs+=cv; t7cc+=1
                vv=it['data']['dates'][d].get('cvr',0)
                if vv>0: t7vs+=vv; t7vc+=1
t7roas=(t7r/t7s*100) if t7s>0 else 0
fds=sum(it['data']['dates'].get(chart_sd[-1],{}).get('spend',0) for it in sorted_list) if len(chart_sd)>=2 else 0
lds=sum(it['data']['dates'].get(chart_sd[0],{}).get('spend',0) for it in sorted_list) if len(chart_sd)>=2 else 0
sdc=((lds-fds)/fds*100) if fds>0 else 0
src.append(cell_text_change(t7roas,sdc,t7s,(t7cs/t7cc) if t7cc>0 else 0,(t7vs/t7vc) if t7vc>0 else 0))
for i,d in enumerate(chart_dn):
    dr,ds_v=0,0;dcs,dcc=0,0;dvs,dvc=0,0
    for it in sorted_list:
        if d in it['data']['dates']:
                dr+=it['data']['dates'][d]['revenue'];ds_v+=it['data']['dates'][d]['spend']
                cv=it['data']['dates'][d].get('cpm',0)
                if cv>0: dcs+=cv; dcc+=1
                vv=it['data']['dates'][d].get('cvr',0)
                if vv>0: dvs+=vv; dvc+=1
    d_roas=(dr/ds_v*100) if ds_v>0 else 0
    if i<len(chart_dn)-1: pd_key=chart_dn[i+1];ps=sum(it['data']['dates'].get(pd_key,{}).get('spend',0) for it in sorted_list);chg=((ds_v-ps)/ps*100) if ps>0 else 0
    else: chg=0
    src.append(cell_text_change(d_roas,chg,ds_v,(dcs/dcc) if dcc>0 else 0,(dvs/dvc) if dvc>0 else 0))
rtc=[]
for it in sorted_list:
    r=[it['campaign_name'],it['adset_name'],it['adset_id']];tr_v,ts_v=0,0;tcpm,tcpc=0,0;tcvr,tcvc=0,0
    for d in chart_sd:
        if d in it['data']['dates']:
                tr_v+=it['data']['dates'][d]['revenue'];ts_v+=it['data']['dates'][d]['spend']
                cv=it['data']['dates'][d].get('cpm',0)
                if cv>0: tcpm+=cv; tcpc+=1
                vv=it['data']['dates'][d].get('cvr',0)
                if vv>0: tcvr+=vv; tcvc+=1
    ar=(tr_v/ts_v*100) if ts_v>0 else 0
    fs=it['data']['dates'].get(chart_sd[-1],{}).get('spend',0) if len(chart_sd)>=2 else 0
    ls=it['data']['dates'].get(chart_sd[0],{}).get('spend',0) if len(chart_sd)>=2 else 0
    cp=((ls-fs)/fs*100) if fs>0 else 0
    r.append(cell_text_change(ar,cp,ts_v,(tcpm/tcpc) if tcpc>0 else 0,(tcvr/tcvc) if tcvc>0 else 0))
    for i,d in enumerate(chart_dn):
        if d in it['data']['dates']:
            dt=it['data']['dates'][d];roas=(dt['revenue']/dt['spend']*100) if dt['spend']>0 else 0
            if i<len(chart_dn)-1:pd_key=chart_dn[i+1];ps=it['data']['dates'].get(pd_key,{}).get('spend',0);chg=((dt['spend']-ps)/ps*100) if ps>0 else 0
            else:chg=0
            r.append(cell_text_change(roas,chg,dt['spend'],dt.get('cpm',0),dt.get('cvr',0)))
        else:r.append('')
    rtc.append(r)
with_retry(ws_c.update,values=[hdr_c]+[src]+rtc,range_name="A1",value_input_option="USER_ENTERED")
print("✅ 증감액 완료"); time.sleep(3)
ws_c = refresh_ws(sh, ws_c); apply_trend_chart_formatting(sh,ws_c,hdr_c,len(rtc),is_change_tab=True); print("⏳ 30초 대기..."); time.sleep(30)

# 13: 예산
print("\n13단계: 예산")
bw=safe_add_worksheet(sh,"예산",rows=1000,cols=len(chart_dn)+10); time.sleep(3)
move_to_front(sh, bw)  # ★ v24: 즉시 맨 왼쪽
br=[[""] + chart_dn]
br.append(["전체 쓴돈"]+[sum(budget_by_date[d][p]['spend'] for p in product_order) for d in chart_dn])
br.append(["전체 번돈"]+[sum(budget_by_date[d][p]['revenue'] for p in product_order) for d in chart_dn])
br.append(["전체 순이익"]+[sum(budget_by_date[d][p]['revenue'] for p in product_order)-sum(budget_by_date[d][p]['spend'] for p in product_order) for d in chart_dn])
br.append(["ROAS"]+[(sum(budget_by_date[d][p]['revenue'] for p in product_order)/sum(budget_by_date[d][p]['spend'] for p in product_order)*100) if sum(budget_by_date[d][p]['spend'] for p in product_order)>0 else 0 for d in chart_dn])
br.append([""]*(len(chart_dn)+1));br.append(["쓴돈 - 제품별"]+[""]*len(chart_dn))
for p in product_order: br.append([p]+[budget_by_date[d][p]['spend'] for d in chart_dn])
br.append([""]*(len(chart_dn)+1));br.append(["번돈 - 제품별"]+[""]*len(chart_dn))
for p in product_order: br.append([p]+[budget_by_date[d][p]['revenue'] for d in chart_dn])
br.append([""]*(len(chart_dn)+1));br.append(["순이익 - 제품별"]+[""]*len(chart_dn))
for p in product_order: br.append([p]+[budget_by_date[d][p]['revenue']-budget_by_date[d][p]['spend'] for d in chart_dn])
with_retry(bw.update,values=br,range_name="A1",value_input_option="RAW"); print("✅ 예산 완료"); time.sleep(3)

# 14~17: 주간종합
print("\n14단계: 주간종합 데이터 준비")
month_groups=defaultdict(list)
for t in date_names: do=date_objects[t];mk=f"{do.year}년 {do.month}월";month_groups[mk].append(t)
month_names_list=sorted(month_groups.keys(),key=lambda x:(int(x.split('년')[0]),int(x.split('년')[1].replace('월','').strip())),reverse=True)
daily_data=defaultdict(lambda:{'spend':0,'revenue':0,'profit':0});daily_product_data=defaultdict(lambda:defaultdict(lambda:{'spend':0,'revenue':0,'profit':0}))
for t in date_names:
    for it in sorted_list:
        if t in it['data']['dates']:
            dt=it['data']['dates'][t];daily_data[t]['spend']+=dt['spend'];daily_data[t]['revenue']+=dt['revenue'];daily_data[t]['profit']+=dt['profit']
            p=extract_product(it['adset_name']);daily_product_data[t][p]['spend']+=dt['spend'];daily_product_data[t][p]['revenue']+=dt['revenue'];daily_product_data[t][p]['profit']+=dt['profit']
wsd={};wps={}
for wk in week_keys:
    wsd[wk]={'spend':0,'revenue':0,'profit':0};wps[wk]=defaultdict(lambda:{'spend':0,'revenue':0,'profit':0})
    for dn in week_groups[wk]:
        wsd[wk]['spend']+=daily_data[dn]['spend'];wsd[wk]['revenue']+=daily_data[dn]['revenue'];wsd[wk]['profit']+=daily_data[dn]['profit']
        for p in daily_product_data[dn]: wps[wk][p]['spend']+=daily_product_data[dn][p]['spend'];wps[wk][p]['revenue']+=daily_product_data[dn][p]['revenue'];wps[wk][p]['profit']+=daily_product_data[dn][p]['profit']
msd={};mps={}
for mk in month_names_list:
    msd[mk]={'spend':0,'revenue':0,'profit':0};mps[mk]=defaultdict(lambda:{'spend':0,'revenue':0,'profit':0})
    for dn in month_groups[mk]:
        msd[mk]['spend']+=daily_data[dn]['spend'];msd[mk]['revenue']+=daily_data[dn]['revenue'];msd[mk]['profit']+=daily_data[dn]['profit']
        for p in daily_product_data[dn]: mps[mk][p]['spend']+=daily_product_data[dn][p]['spend'];mps[mk][p]['revenue']+=daily_product_data[dn][p]['revenue'];mps[mk][p]['profit']+=daily_product_data[dn][p]['profit']
all_found_products = set()
for dn in date_names:
    for p in daily_product_data[dn]:
        if daily_product_data[dn][p]['spend'] > 0 or daily_product_data[dn][p]['revenue'] > 0: all_found_products.add(p)
products = sorted([p for p in all_found_products if p in PRODUCT_KEYWORDS], key=lambda p: sum(daily_product_data[dn][p]['spend'] for dn in date_names), reverse=True)
SUMMARY_PRODUCTS = products
print(f"📆 월:{len(month_names_list)}, 주:{len(week_keys)}, 일:{len(date_names)}")
print(f"📦 감지된 상품: {products}")

print("\n15단계: 주간종합")
ws_ws=safe_add_worksheet(sh,"주간종합",rows=2000,cols=20); time.sleep(3)
move_to_front(sh, ws_ws)  # ★ v24: 즉시 맨 왼쪽
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
        if dk=="roas":r.append((trv/tsp) if tsp>0 else 0)
        elif dk=="ratio":r.append(1.0)
        else:r.append(sum(pd[p][dk] for p in products))
        block.append(r);fr.append(create_format_request(sid,cr,cr+1,0,1,get_cell_format(COLORS["light_gray2"],bold=True)));fr.append(create_format_request(sid,cr,cr+1,len(products)+1,len(products)+2,get_cell_format(COLORS["light_yellow"])));fr.append(create_number_format_request(sid,cr,cr+1,1,len(products)+2,ft,fp));cr+=1
    fr.append(create_border_request(sid,bs,cr,0,len(products)+2));block.append([""]*nc);cr+=1
    return block,cr
for mk in month_names_list:
    yr=int(mk.split('년')[0]);mn=int(mk.split('년')[1].replace('월','').strip());dim=sorted(month_groups[mk],key=lambda x:date_objects[x]);fd=date_objects[dim[0]];ld=date_objects[dim[-1]];mr_d=get_month_range_display(fd,ld).replace("'","")
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
print("✅ 주간종합 완료"); time.sleep(3)

print("\n16단계: 주간종합_2")
ws2=safe_add_worksheet(sh,"주간종합_2",rows=2000,cols=20); time.sleep(3)
move_to_front(sh, ws2)  # ★ v24: 즉시 맨 왼쪽
sid2=ws2.id;fr2=[];ar2=[];cr2=0;npc=len(products)+3;stl=[]
for mk in month_names_list:
    yr=int(mk.split('년')[0]);mn=int(mk.split('년')[1].replace('월','').strip());d=msd[mk];roas=(d['revenue']/d['spend']) if d['spend']>0 else 0
    stl.append({'period':f"'{mk}",'type':'월별','spend':d['spend'],'revenue':d['revenue'],'profit':d['profit'],'roas':roas,'im':True,'mk':mk,'yr':yr,'mn':mn})
    mw=[wk for wk in week_keys if any(date_objects[dn].year==yr and date_objects[dn].month==mn for dn in week_groups[wk])];mw.reverse()
    for wk in mw: wd=wsd[wk];wr=(wd['revenue']/wd['spend']) if wd['spend']>0 else 0;stl.append({'period':week_display_names[wk],'type':'주간','spend':wd['spend'],'revenue':wd['revenue'],'profit':wd['profit'],'roas':wr,'im':False,'mk':None,'wk':wk})
ar2.append(["📊 기간별 전체 요약"]+[""]*6);fr2.append(create_format_request(sid2,cr2,cr2+1,0,7,get_cell_format(COLORS["navy"],COLORS["white"],bold=True)));cr2+=1
ar2.append(["기간","유형","지출금액","매출","이익","ROAS","CVR"]);fr2.append(create_format_request(sid2,cr2,cr2+1,0,7,get_cell_format(COLORS["dark_blue"],COLORS["white"],bold=True)));cr2+=1;t1s=cr2
for rd in stl:
    ar2.append([rd['period'],rd['type'],rd['spend'],rd['revenue'],rd['profit'],rd['roas'],0]);bg=COLORS["light_blue"] if rd['im'] else COLORS["light_gray"]
    fr2.append(create_format_request(sid2,cr2,cr2+1,0,7,get_cell_format(bg)));fr2.append(create_number_format_request(sid2,cr2,cr2+1,2,5,"NUMBER","#,##0"));fr2.append(create_number_format_request(sid2,cr2,cr2+1,5,7,"PERCENT","0.00%"));cr2+=1
fr2.append(create_border_request(sid2,t1s-1,cr2,0,7));ar2+=[[""]*(npc),[""]*(npc)];cr2+=2
for tt,tc,dk in [("📈 제품별 ROAS",COLORS["dark_green"],"roas"),("💰 제품별 순이익",COLORS["dark_green"],"profit"),("💵 제품별 매출",COLORS["orange"],"revenue"),("💸 제품별 예산",COLORS["purple"],"spend"),("📊 제품별 예산 비중",COLORS["purple"],"ratio")]:
    ar2.append([tt]+[""]*(npc-1));fr2.append(create_format_request(sid2,cr2,cr2+1,0,npc,get_cell_format(tc,COLORS["white"],bold=True)));cr2+=1
    ar2.append(["기간","유형"]+products+["합계"]);fr2.append(create_format_request(sid2,cr2,cr2+1,0,npc,get_cell_format(COLORS["dark_gray"],COLORS["white"],bold=True)));cr2+=1;tds=cr2
    for rd in stl:
        pd_r=mps.get(rd['mk'],defaultdict(lambda:{'spend':0,'revenue':0,'profit':0})) if rd['im'] else wps.get(rd.get('wk',''),defaultdict(lambda:{'spend':0,'revenue':0,'profit':0}))
        r=[rd['period'],rd['type']];tsp=sum(pd_r[p]['spend'] for p in products);trv=sum(pd_r[p]['revenue'] for p in products)
        for p in products:
            if dk=="roas":r.append((pd_r[p]['revenue']/pd_r[p]['spend']) if pd_r[p]['spend']>0 else 0)
            elif dk=="ratio":r.append((pd_r[p]['spend']/tsp) if tsp>0 else 0)
            else:r.append(pd_r[p][dk])
        if dk=="roas":r.append((trv/tsp) if tsp>0 else 0)
        elif dk=="ratio":r.append(1.0)
        else:r.append(sum(pd_r[p][dk] for p in products))
        ar2.append(r);bg=COLORS["light_blue"] if rd['im'] else COLORS["light_gray"];fr2.append(create_format_request(sid2,cr2,cr2+1,0,npc,get_cell_format(bg)))
        ft="PERCENT" if dk in ["roas","ratio"] else "NUMBER";fp="0.00%" if dk in ["roas","ratio"] else "#,##0"
        fr2.append(create_number_format_request(sid2,cr2,cr2+1,2,npc,ft,fp));cr2+=1
    fr2.append(create_border_request(sid2,tds-1,cr2,0,npc));ar2+=[[""]*(npc),[""]*(npc)];cr2+=2
with_retry(ws2.update,values=ar2,range_name="A1",value_input_option="USER_ENTERED"); time.sleep(3)
try:
    for i in range(0,len(fr2),100): with_retry(sh.batch_update,body={"requests":fr2[i:i+100]}); time.sleep(1)
except: pass
print("✅ 주간종합_2 완료"); time.sleep(3)

print("\n17단계: 주간종합_3 (일별)")
ws3=safe_add_worksheet(sh,"주간종합_3",rows=3000,cols=20); time.sleep(3)
move_to_front(sh, ws3)  # ★ v24: 즉시 맨 왼쪽
sid3=ws3.id;fr3=[];ar3=[];cr3=0;ndc=len(products)+4;dsr=[]
for t in reversed(date_names): do=date_objects[t];d=daily_data[t];roas=(d['revenue']/d['spend']) if d['spend']>0 else 0;wd=WEEKDAY_NAMES[do.weekday()];dsr.append({'period':f"'{do.month}.{do.day}({wd})",'weekday':wd,'spend':d['spend'],'revenue':d['revenue'],'profit':d['profit'],'roas':roas,'tab_name':t})
ar3.append(["📊 일별 전체 요약"]+[""]*7);fr3.append(create_format_request(sid3,cr3,cr3+1,0,8,get_cell_format(COLORS["navy"],COLORS["white"],bold=True)));cr3+=1
ar3.append(["날짜","요일","지출금액","매출","이익","ROAS","CVR",""]);fr3.append(create_format_request(sid3,cr3,cr3+1,0,8,get_cell_format(COLORS["dark_blue"],COLORS["white"],bold=True)));cr3+=1;t1s3=cr3
for rd in dsr:
    ar3.append([rd['period'],rd['weekday'],rd['spend'],rd['revenue'],rd['profit'],rd['roas'],0,""]);bg=COLORS["light_blue"] if rd['weekday'] in ['토','일'] else COLORS["light_gray"]
    fr3.append(create_format_request(sid3,cr3,cr3+1,0,8,get_cell_format(bg)));fr3.append(create_number_format_request(sid3,cr3,cr3+1,2,5,"NUMBER","#,##0"));fr3.append(create_number_format_request(sid3,cr3,cr3+1,5,7,"PERCENT","0.00%"));cr3+=1
fr3.append(create_border_request(sid3,t1s3-1,cr3,0,8));ar3+=[[""]*(ndc),[""]*(ndc)];cr3+=2
for tt,tc,dk in [("📈 일별 제품별 ROAS",COLORS["dark_green"],"roas"),("💰 일별 제품별 순이익",COLORS["dark_green"],"profit"),("💵 일별 제품별 매출",COLORS["orange"],"revenue"),("💸 일별 제품별 예산",COLORS["purple"],"spend"),("📊 일별 제품별 예산 비중",COLORS["purple"],"ratio")]:
    ar3.append([tt]+[""]*(ndc-1));fr3.append(create_format_request(sid3,cr3,cr3+1,0,ndc,get_cell_format(tc,COLORS["white"],bold=True)));cr3+=1
    ar3.append(["날짜","요일"]+products+["합계"]);fr3.append(create_format_request(sid3,cr3,cr3+1,0,ndc,get_cell_format(COLORS["dark_gray"],COLORS["white"],bold=True)));cr3+=1;tds=cr3
    for rd in dsr:
        pd_r=daily_product_data[rd['tab_name']];r=[rd['period'],rd['weekday']]
        tsp=sum(pd_r[p]['spend'] for p in products);trv=sum(pd_r[p]['revenue'] for p in products)
        for p in products:
            if dk=="roas":r.append((pd_r[p]['revenue']/pd_r[p]['spend']) if pd_r[p]['spend']>0 else 0)
            elif dk=="ratio":r.append((pd_r[p]['spend']/tsp) if tsp>0 else 0)
            else:r.append(pd_r[p][dk])
        if dk=="roas":r.append((trv/tsp) if tsp>0 else 0)
        elif dk=="ratio":r.append(1.0)
        else:r.append(sum(pd_r[p][dk] for p in products))
        ar3.append(r);bg=COLORS["light_blue"] if rd['weekday'] in ['토','일'] else COLORS["light_gray"];fr3.append(create_format_request(sid3,cr3,cr3+1,0,ndc,get_cell_format(bg)))
        ft="PERCENT" if dk in ["roas","ratio"] else "NUMBER";fp="0.00%" if dk in ["roas","ratio"] else "#,##0"
        fr3.append(create_number_format_request(sid3,cr3,cr3+1,2,ndc,ft,fp));cr3+=1
    fr3.append(create_border_request(sid3,tds-1,cr3,0,ndc));ar3+=[[""]*(ndc),[""]*(ndc)];cr3+=2
with_retry(ws3.update,values=ar3,range_name="A1",value_input_option="USER_ENTERED"); time.sleep(3)
try:
    for i in range(0,len(fr3),100): with_retry(sh.batch_update,body={"requests":fr3[i:i+100]}); time.sleep(1)
except: pass
print("✅ 주간종합_3 완료"); time.sleep(3)

# ★ v24: 18단계 — 최종 탭 순서 정리 (분석→최신날짜→과거→기타)
print("\n"+"="*60); print("18단계: 최종 탭 순서 정리 (분석→최신날짜→과거→기타)"); print("="*60)
reorder_tabs(sh)

print("\n"+"="*60); print("✅ 완료!"); print("="*60)
print()
print(f"🔄 Meta/Mixpanel 갱신: 최근 {REFRESH_DAYS}일")
print(f"📝 기존 탭 업데이트: {len(existing_refresh_tabs)}개 | 🆕 새 탭: {len(new_refresh_dates)}개")
print(f"📊 분석탭: 전체 {len(date_names)}일 | 마스터: {len(master_raw_data)}행 | 주간: {len(week_keys)}주")
print(f"💱 환율: USD/TWD/JPY/HKD → KRW (★v26: 캠페인/세트 이름 서브스트링+한글 매칭)")
print(f"🌏 나라별 요약: 날짜탭 하단 테이블에 포함 (★v26: 일본·홍콩·대만·한국 이름 기반 감지)")
print(f"📋 탭 순서: [매출/주간매출] → [분석탭] → [최신날짜→과거] → [기타]")
print(f"\n📊 {SPREADSHEET_URL}")
