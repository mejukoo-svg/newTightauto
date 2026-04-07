# -*- coding: utf-8 -*-
# v3-ad 국내 세트별 (★ v30f - 텍스트색상 최적화+대기 강화축소):
#   - ★ v30f: 텍스트 색상 컬럼 제한 (20→10), 배치 확대 (500→800), flush 1.5초
#   - ★ v30f: sleep(3)→sleep(2) 일괄 축소, 단계 간 대기 추가 축소
#   - ★ v30f: 주간종합 쿨다운 20초, 차트 대기 10초, 주간전 20초
#   - ★ v30e: 믹스패널 데이터 없으면 기존 시트 값 보존 (빈값 덮어쓰기 방지)
#   - ★ v30e: 주간종합 포맷 배치 크기 복원 (50→100), 대기 축소 (3→1.5초)
#   - ★ v30d: 7-A 기존 행 업데이트 시 cols 14-18(결과MP,매출,이익,ROAS,CVR)만 갱신
#   - ★ v30d: A~C 하이라이트, D~N 메타데이터, T~W 예산/메모 행 배열 모두 보존
#   - ★ v30d: 요약표+상품분류를 date_tab_rows(API 원본)로 생성 → 추이차트와 동일 소스
#   - ★ v30c: 15~17단계 주간종합 데이터 쓰기 청크 분할 (3000행)
#   - ★ v30c: 15~17단계 포맷 배치 크기 축소 (100→50) + 대기 시간 증가 (1→3초)
#   - ★ v30c: 14→15단계 전환 시 60초 쿨다운 추가
#   - ★ v30b: 마스터탭 날짜형식 수정, adset_id 정밀도, dedup개선
#   - ★ v30: 8.5단계 마스터탭 재활용 → 144개 탭 개별읽기 → 1개 탭 읽기로 대체
#   - ★ v30: Mixpanel dedup 시 utm_term 있는 이벤트 우선 선택
#   - ★ v30: Sheets 배치 크기 축소(8→4), 쿨다운 증가(12→25초), 워커 1개로 직렬화
#   - ★ v29b: extract_product → 캠페인명 첫 단어(이모지 제거) 기반 상품 추출
#   - ★ v29b: PRODUCT_KEYWORDS 필터 제거 → 모든 감지 상품 나열
#   - ★ v29: 추이차트 셀 CPM→매출 대체, C2 라벨 수정, 정렬=전날 지출 기준 내림차순
#   - Meta/Mixpanel: 최근 10일치만 새로 호출 (FULL_REFRESH 시 전체)
#   - 광고 세트 이름/광고 세트 ID 기준으로 집계
#   - Mixpanel: utm_term = adset_id 기준으로 매핑
#   - ★ v21: Mixpanel 이벤트 "결제완료" OR "payment_complete"
#   - ★ v21: 금액 필드 "amount" OR "결제금액"
#   - ★ v21: 병렬의 병렬 처리 (Meta 날짜×계정, Mixpanel 청크, 환율, 예산, 탭 읽기)
#   - 기존 날짜탭: Meta 데이터 + 매출/ROAS/순이익/CVR 모두 업데이트
#   - 없는 날짜탭만 새로 생성
#   - 분석탭: 전체 날짜탭 데이터 읽어서 재구성
#   - 탭 순서: [분석탭 맨 왼쪽] + [날짜탭 최신→과거] + [기타]
#   - 구 구조(25열) / 신 구조(23열) 자동 판별
#   - ★ v25: 분석탭 생성 즉시 맨 왼쪽으로 이동 (move_ws_to_front)
#   - ★ v26: extract_product가 adset_name에서 못 찾으면 campaign_name에서 재탐색
#   - ★ v27: 날짜탭 요약 아래 상품별 분류 테이블 (매출 기준 정렬)
#   - ★ v28: 추이차트_상품별 탭 (상품별 그룹핑, 7일평균매출순 정렬, 세트는 전날매출순)

print("="*60)
print("🚀 v3-ad v30h 국내 세트별 (마스터탭 우선+구조 재설계)")
print("="*60)

# =========================================================
# 인증 (GitHub Actions / Colab 자동 분기)
# =========================================================
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

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
import threading

# =========================================================
# ★ v21: 병렬 처리 설정  |  ★ v30f: 타임아웃 방지 강화
# =========================================================
META_DATE_WORKERS = 3
META_ACCOUNT_WORKERS = 3
MIXPANEL_CHUNK_WORKERS = 3
# ★ v30: Sheets 읽기 설정 완화 (rate limit 방지)
SHEETS_READ_WORKERS = 1          # v29c: 2 → v30: 1 (직렬화)
SHEETS_READ_BATCH_SIZE = 4       # v29c: 8 → v30: 4
SHEETS_READ_BATCH_DELAY = 25     # v29c: 12 → v30: 25초
EXCHANGE_RATE_WORKERS = 2
BUDGET_WORKERS = 3

# ★ v30f: 주간종합 쓰기 설정
WEEKLY_WRITE_CHUNK_SIZE = 3000   # 한 번에 쓸 최대 행 수
WEEKLY_FORMAT_BATCH_SIZE = 100   # ★ v30e: 50 → 100
WEEKLY_FORMAT_BATCH_DELAY = 1.5  # ★ v30e: 3 → 1.5초
WEEKLY_STEP_COOLDOWN = 2        # ★ v30h: 5 → 2초 (포맷 완전 스킵)

# ★ v30f: 단계 간 대기시간 상수 (더 공격적 축소)
CHART_STEP_SLEEP = 2             # ★ v30h: 5 → 2초
MAJOR_STEP_SLEEP = 2             # ★ v30h: 5 → 2초
PRE_WEEKLY_COOLDOWN = 5          # ★ v30h: 10 → 5초 (포맷 스킵으로 rate limit 여유)

# ★ v30g: 날짜탭 Mixpanel 업데이트 일수 (이전 날짜는 확정 → 최근만 갱신)
TAB_UPDATE_DAYS = 3              # 최근 3일만 7-A에서 업데이트 (나머지는 이전 실행 값 유지)

# ★ v30f: 텍스트 색상 포맷 설정 (핵심 병목 최적화)
TEXT_COLOR_MAX_COLS = 10         # ★ v30f: 20 → 10 (최근 ~6일만 색상 적용)
TEXT_COLOR_BATCH_SIZE = 800      # ★ v30f: 500 → 800 (배치 크기 확대)
TEXT_COLOR_FLUSH_SLEEP = 1.5     # ★ v30f: 3 → 1.5초 (flush 대기 축소)

_print_lock = threading.Lock()
def safe_print(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs)

# =========================================================
# USD → KRW 일별 환율 조회
# =========================================================
FALLBACK_USD_KRW = 1450

def fetch_daily_exchange_rates(start_date, end_date, currency="USD"):
    rates = {}
    pair = f"{currency}KRW=X"
    fallback = FALLBACK_USD_KRW
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
# Meta Ads API 설정 (국내 세트별: 3계정)
# =========================================================
META_TOKEN_A = os.environ.get("META_TOKEN_KR_A", os.environ.get("META_TOKEN_1", ""))
META_TOKEN_B = os.environ.get("META_TOKEN_KR_B", os.environ.get("META_TOKEN_2", ""))

META_TOKENS = {
    "act_1270614404675034": META_TOKEN_A,
    "act_707835224206178": META_TOKEN_A,
    "act_1808141386564262": META_TOKEN_B,
}
META_TOKEN_DEFAULT = META_TOKEN_A
def get_token(acc_id): return META_TOKENS.get(acc_id, META_TOKEN_DEFAULT)
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

print("🔑 Meta 토큰 상태 (국내 3계정):")
print(f"  TOKEN_A (act_1270 + act_7078): {'✅ 설정됨' if META_TOKEN_A else '❌ 비어있음'}")
print(f"  TOKEN_B (act_1808):            {'✅ 설정됨' if META_TOKEN_B else '❌ 비어있음'}")

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
    REFRESH_DAYS = 10
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
print(f"🔀 병렬 처리: Meta {META_DATE_WORKERS}×{META_ACCOUNT_WORKERS} | MP {MIXPANEL_CHUNK_WORKERS} | Sheets {SHEETS_READ_WORKERS}")
print()

# ★ v29b: PRODUCT_KEYWORDS는 참조용으로만 유지 (필터에 사용하지 않음)
PRODUCT_KEYWORDS_REF = ["집착", "결혼", "커리어", "0.01", "별해", "솔로", "재물", "재회", "29금궁합", "자녀", "29금"]

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

SUMMARY_PRODUCTS = []

FINAL_ANALYSIS_ORDER = [
    "추이차트", "추이차트_상품별", "추이차트(주간)", "증감액", "예산",
    "주간종합", "주간종합_2", "주간종합_3", "마스터탭"
]
ANALYSIS_TABS_SET = set(FINAL_ANALYSIS_ORDER) | {"_temp", "_temp_holder", "_tmp"}


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

# ★ v30f: 이익/ROAS/CVR 수식 생성 헬퍼
def _make_profit_formula(row_num, structure="new"):
    """이익 = 매출 - 지출"""
    spend_col = get_col_letter(get_col_index(structure, 3))
    rev_col = get_col_letter(get_col_index(structure, 15))
    return f'=IF({rev_col}{row_num}="","",{rev_col}{row_num}-{spend_col}{row_num})'

def _make_roas_formula(row_num, structure="new"):
    """ROAS = 매출/지출*100"""
    spend_col = get_col_letter(get_col_index(structure, 3))
    rev_col = get_col_letter(get_col_index(structure, 15))
    return f'=IF(AND({spend_col}{row_num}>0,{rev_col}{row_num}>0),ROUND({rev_col}{row_num}/{spend_col}{row_num}*100,1),"")'

def _make_cvr_formula(row_num, structure="new"):
    """CVR = 결과MP/고유클릭*100"""
    clicks_col = get_col_letter(get_col_index(structure, 9))
    mp_col = get_col_letter(get_col_index(structure, 14))
    return f'=IF(AND({clicks_col}{row_num}>0,{mp_col}{row_num}>0),ROUND({mp_col}{row_num}/{clicks_col}{row_num}*100,2),"")'


# ★ v30a: adset_id 정밀도 손실 대응 헬퍼
def _asid_match_key(asid_str):
    """15자리 prefix로 fuzzy 매칭 키 생성 (Sheets double 정밀도 = 유효숫자 15자리)"""
    s = clean_id(asid_str)
    return s[:15] if len(s) > 15 else s

def _prefix_asid_for_sheet(val):
    """Sheets 쓰기 시 adset_id를 텍스트로 강제 (' prefix → 정밀도 손실 방지)"""
    s = str(val).strip() if val else ""
    if s and re.match(r'^\d', s) and not s.startswith("'"):
        return "'" + s
    return s

def _make_rows_sheet_safe(rows, asid_col=2):
    """행 리스트의 adset_id 컬럼에 ' prefix 적용 (Sheets 쓰기용)"""
    safe = []
    for row in rows:
        r = list(row)
        if len(r) > asid_col and r[asid_col]:
            s = str(r[asid_col]).strip()
            if s and re.match(r'^\d', s) and s not in ["광고 세트 ID"]:
                r[asid_col] = "'" + s
        safe.append(r)
    return safe


# ★ v29b: 이모지 제거 헬퍼
def _strip_leading_emojis(text):
    """문자열 앞의 이모지/특수문자를 제거하고 첫 번째 의미있는 문자부터 반환"""
    i = 0
    while i < len(text):
        c = text[i]
        cp = ord(c)
        # 한글 음절/자모, 영숫자, 마침표, 퍼센트 → 의미 있는 문자
        if ('\uAC00' <= c <= '\uD7A3' or   # 한글 완성형
            '\u3131' <= c <= '\u3163' or    # 한글 자모
            c.isalnum() or c in '.%'):
            break
        i += 1
    return text[i:].strip()


# ★ v29b: 캠페인명 첫 단어 기반 상품 추출 (이모지 제거 후)
def extract_product(adset_name, campaign_name=""):
    """
    캠페인 이름의 첫 단어(이모지 제거 후)에서 상품명 추출.
    캠페인명 우선 → adset명 폴백.
    """
    for source in [campaign_name, adset_name]:
        if not source:
            continue
        name = str(source).strip()
        if not name:
            continue
        cleaned = _strip_leading_emojis(name)
        if not cleaned:
            continue
        # 구분자로 분할하여 첫 번째 토큰 추출
        first_word = re.split(r'[_\s\-/|,()\[\]]+', cleaned)[0].strip()
        if first_word:
            return first_word
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


# ★ v30f: 기존 분석탭 재사용 헬퍼
def get_or_create_ws(sh, title, rows, cols):
    """기존 탭 재사용 or 새로 생성. Returns (ws, is_new)"""
    try:
        ws = sh.worksheet(title)
        # 기존 탭 → resize if needed
        need_resize = False
        new_rows = max(rows, ws.row_count)
        new_cols = max(cols, ws.col_count)
        if rows > ws.row_count or cols > ws.col_count:
            with_retry(ws.resize, rows=new_rows, cols=new_cols)
            time.sleep(1)
        print(f"  ♻️ '{title}' 기존 탭 재사용 (값만 덮어쓰기)")
        return ws, False
    except gspread.exceptions.WorksheetNotFound:
        ws = with_retry(sh.add_worksheet, title=title, rows=rows, cols=cols)
        print(f"  🆕 '{title}' 새로 생성")
        return ws, True

# ★ v30f: 재사용 시 텍스트 색상 적용할 컬럼 범위 (7일평균 + 최근 N일)
REFRESH_FORMAT_COL_END = 4 + 10  # col3=7일평균, col4~col13=최근10일


def move_ws_to_front(sh, ws):
    try:
        with_retry(sh.batch_update, body={"requests":[
            {"updateSheetProperties":{"properties":{"sheetId":ws.id,"index":0},"fields":"index"}}
        ]})
        print(f"  ↪️ '{ws.title}' → 맨 왼쪽으로 이동")
        time.sleep(1)
    except Exception as e:
        print(f"  ⚠️ 탭 이동 오류: {e}")


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

def reorder_tabs(sh):
    try:
        all_ws = sh.worksheets()
        analysis_tabs, date_tabs, other_tabs = [], [], []
        analysis_order_map = {name: i for i, name in enumerate(FINAL_ANALYSIS_ORDER)}
        for ws in all_ws:
            tn = ws.title
            if tn in ANALYSIS_TABS_SET: analysis_tabs.append(ws)
            elif parse_date_tab(tn) is not None: date_tabs.append(ws)
            else: other_tabs.append(ws)
        date_tabs.sort(key=lambda ws: parse_date_tab(ws.title), reverse=True)
        analysis_tabs.sort(key=lambda ws: analysis_order_map.get(ws.title, 999))
        final_order = analysis_tabs + date_tabs + other_tabs
        print(f"  📊 분석: {len(analysis_tabs)}개 | 📅 날짜: {len(date_tabs)}개 | 📋 기타: {len(other_tabs)}개")
        if date_tabs: print(f"  📅 날짜탭: {date_tabs[0].title} (최신) → {date_tabs[-1].title} (과거)")
        if analysis_tabs: print(f"  📊 분석탭: {' → '.join(ws.title for ws in analysis_tabs)} (맨 왼쪽)")
        with_retry(sh.batch_update, body={"requests": [
            {"updateSheetProperties": {"properties": {"sheetId": ws.id, "index": idx}, "fields": "index"}}
            for idx, ws in enumerate(final_order)
        ]})
        print("  ✅ 탭 순서 정리 완료"); time.sleep(2)
    except Exception as e: print(f"  ⚠️ 탭 순서 정리 오류: {e}")

# ★ v29: cell_text - CPM → 매출(revenue) 대체
def cell_text(profit, revenue, spend, cpm=0, cvr=0):
    if spend == 0: return ""
    roas = (revenue / spend * 100) if spend > 0 else 0
    return f"{roas:.0f}\n{money(profit)}\n-{money(spend)}\n{money(revenue)}\n{cvr:.1f}%"

# ★ v29: cell_text_change - CPM → 매출(revenue) 대체
def cell_text_change(roas, chg, spend, revenue=0, cvr=0):
    if spend == 0: return ""
    cl = f"+{chg:.1f}%" if chg > 0 else f"{chg:.1f}%" if chg < 0 else "0.0%"
    return f"{roas:.0f}\n{cl}\n-{money(spend)}\n{money(revenue)}\n{cvr:.1f}%"

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

# ★ v29: C2 라벨 - CPM → 매출
def apply_c2_label_formatting(sh, ws):
    sid = ws.id; cv = "ROAS\n순이익\n지출\n매출\n전환율"
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

def apply_trend_chart_formatting(sh, ws, headers, rows_count, is_change_tab=False, sunday_col_indices=None, format_col_end=None, text_color_only=False):
    sid = ws.id
    # ★ v30f: text_color_only=True이면 조건부 서식/헤더 포맷 스킵 (기존 탭 재사용 시)
    if not text_color_only:
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
    # ★ v30h: text_color_only=True → 조건부 서식 스킵했지만, 텍스트 색상은 최근 컬럼만 적용
    tcs = 3; tce = min(format_col_end or len(headers), len(headers), TEXT_COLOR_MAX_COLS)
    print(f"  🎨 텍스트 색상 (col {tcs}~{tce-1})...")
    try:
        fr = []
        bk={"foregroundColor":{"red":0,"green":0,"blue":0}}; dg3={"foregroundColor":{"red":0.22,"green":0.46,"blue":0.11}}
        rd={"foregroundColor":{"red":0.85,"green":0.0,"blue":0.0}}; gn={"foregroundColor":{"red":0.0,"green":0.7,"blue":0.0}}; bl={"foregroundColor":{"red":0.0,"green":0.0,"blue":0.85}}
        # ★ v30b: col_values N회 → get_all_values 1회로 대체 (API 절약)
        all_vals = with_retry(ws.get_all_values); time.sleep(2)
        for ci in range(tcs, tce):
            for ri in range(2, min(len(all_vals)+1, rows_count+3)):
                val = all_vals[ri-1][ci] if ri-1 < len(all_vals) and ci < len(all_vals[ri-1]) else ""
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
                # ★ v30f: 배치 크기 800, 대기 1.5초 (v30e: 500/3초)
                if len(fr) >= TEXT_COLOR_BATCH_SIZE:
                    try: with_retry(sh.batch_update, body={"requests":fr}); fr=[]; time.sleep(TEXT_COLOR_FLUSH_SLEEP)
                    except: fr=[]
        if fr:
            try: with_retry(sh.batch_update, body={"requests":fr}); time.sleep(1)
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
                err = resp.json().get('error',{}); safe_print(f"  ❌ Meta 400: {err.get('message', resp.text[:200])}"); return None
            elif resp.status_code in [429,500,502,503]:
                wait = 30+attempt*30; safe_print(f"  ⏳ Meta {resp.status_code}, {wait}초 대기 (시도 {attempt+1}/5)"); time.sleep(wait)
            else:
                safe_print(f"  ❌ Meta {resp.status_code}: {resp.text[:200]}")
                if attempt < 4: time.sleep(15)
                else: return None
        except Exception as e:
            safe_print(f"  ❌ Meta 요청 오류: {e}")
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

def detect_account_currency(ad_account_id):
    url = f"{META_BASE_URL}/{ad_account_id}"
    params = {'fields': 'currency,name'}
    data = meta_api_get(url, params, token=get_token(ad_account_id))
    if data:
        currency = data.get('currency', 'USD')
        name = data.get('name', ad_account_id)
        return currency, name
    return 'USD', ad_account_id

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

def parse_single_day_insights(rows, date_str, date_obj, ad_account_id=""):
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
            'ad_account_id':ad_account_id,
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
        if fallback_count > 0: safe_print(f"    📌 ASC/캠페인 예산 폴백: {fallback_count}개 세트 (캠페인 {len(unique_campaigns)}개 조회)")
    return adset_results


# =========================================================
# Mixpanel
# =========================================================
def fetch_mixpanel_data(from_date, to_date):
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {'from_date':from_date,'to_date':to_date,'event':json.dumps(MIXPANEL_EVENT_NAMES),'project_id':MIXPANEL_PROJECT_ID}
    safe_print(f"  📡 Mixpanel: {from_date} ~ {to_date} (이벤트: {MIXPANEL_EVENT_NAMES})")
    try:
        resp = req_lib.get(url,params=params,auth=(MIXPANEL_USERNAME,MIXPANEL_SECRET),timeout=300)
        if resp.status_code != 200: safe_print(f"  ❌ Mixpanel {resp.status_code}: {resp.text[:300]}"); return []
        lines = [l for l in resp.text.split('\n') if l.strip()]; safe_print(f"  📊 이벤트: {len(lines)}건")
        data = []; stat_amount_only=0;stat_value_only=0;stat_both=0;stat_neither=0
        event_counts = defaultdict(int)
        for line in lines:
            try:
                ev=json.loads(line);props=ev.get('properties',{});ts=props.get('time',0)
                event_name = ev.get('event', '')
                event_counts[event_name] += 1
                if ts: dt_kst=datetime.fromtimestamp(ts,tz=timezone.utc)+timedelta(hours=9); ds=f"{dt_kst.year%100:02d}/{dt_kst.month:02d}/{dt_kst.day:02d}"
                else: ds=None
                ut=None
                for k in ['utm_term','UTM_Term','UTM Term']:
                    if k in props and props[k]: ut=str(props[k]).strip(); break
                if ut: ut=clean_id(ut)
                raw_amount = props.get('amount')
                if raw_amount is None:
                    raw_amount = props.get('결제금액')
                raw_value = props.get('value')
                amount_val=0.0
                if raw_amount is not None:
                    try: amount_val=float(raw_amount)
                    except: amount_val=0.0
                value_val=0.0
                if raw_value is not None:
                    try: value_val=float(raw_value)
                    except: value_val=0.0
                if amount_val > 0: revenue=amount_val
                elif value_val > 0: revenue=value_val
                else: revenue=0.0
                has_amount=amount_val>0;has_value=value_val>0
                if has_amount and has_value: stat_both+=1
                elif has_amount: stat_amount_only+=1
                elif has_value: stat_value_only+=1
                else: stat_neither+=1
                data.append({'distinct_id':props.get('distinct_id'),'time':ts,'date':ds,'utm_term':ut or '','amount':amount_val,'value_raw':value_val,'revenue':revenue,'서비스':props.get('서비스','')})
            except: pass
        safe_print(f"  ✅ 파싱: {len(data)}건")
        safe_print(f"  📊 이벤트별: {dict(event_counts)}")
        safe_print(f"  📊 매출 출처: amount/결제금액={stat_amount_only}, value={stat_value_only}, 둘다={stat_both}, 없음={stat_neither}")
        return data
    except Exception as e: safe_print(f"  ❌ Mixpanel 오류: {e}"); return []


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


# =========================================================
# ★ v27: 상품별 분류 테이블
# =========================================================
def generate_product_breakdown(rows, structure="new"):
    product_groups = defaultdict(list)
    for row in rows:
        nr = normalize_row_to_new(row, structure)
        cn = str(nr[0]).strip()
        if not cn or cn in ["캠페인 이름", "전체", "합계", "Total"]:
            continue
        asn = str(nr[1]).strip()
        p = extract_product(asn, cn)
        revenue = _to_num(nr[15])
        spend = _to_num(nr[3])
        mp_count = _to_num(nr[14])
        unique_clicks = _to_num(nr[9])
        product_groups[p].append({
            'row': list(nr), 'revenue': revenue, 'spend': spend,
            'mp_count': mp_count, 'unique_clicks': unique_clicks,
        })
    sorted_products = sorted(product_groups.keys(), key=lambda p: sum(item['revenue'] for item in product_groups[p]), reverse=True)
    breakdown_rows = []; product_meta = []; NC = len(DATE_TAB_HEADERS)
    breakdown_rows.append([""] * NC)
    title_row = [""] * NC; title_row[0] = "📊 상품별 분류"; breakdown_rows.append(title_row); breakdown_rows.append([""] * NC)
    for p in sorted_products:
        items = product_groups[p]; items.sort(key=lambda x: x['revenue'], reverse=True)
        total_spend = sum(i['spend'] for i in items); total_revenue = sum(i['revenue'] for i in items)
        total_profit = total_revenue - total_spend; total_roas = (total_revenue / total_spend * 100) if total_spend > 0 else 0
        total_mp = sum(i['mp_count'] for i in items); total_uc = sum(i['unique_clicks'] for i in items)
        total_cvr = (total_mp / total_uc * 100) if total_uc > 0 and total_mp > 0 else 0
        header_offset = len(breakdown_rows)
        product_header = [""] * NC
        product_header[0] = f"📦 {p}  |  매출 {money(total_revenue)}  |  이익 {money(total_profit)}  |  지출 {money(total_spend)}  |  ROAS {total_roas:.0f}%  |  {len(items)}개 세트"
        breakdown_rows.append(product_header)
        col_header_offset = len(breakdown_rows); breakdown_rows.append(list(DATE_TAB_HEADERS))
        data_start = len(breakdown_rows)
        for item in items: breakdown_rows.append(item['row'])
        data_end = len(breakdown_rows)
        subtotal_offset = len(breakdown_rows)
        subtotal = [""] * NC; subtotal[0] = f"▸ {p} 소계"; subtotal[3] = round(total_spend)
        subtotal[9] = round(total_uc) if total_uc > 0 else ""; subtotal[14] = int(total_mp) if total_mp > 0 else ""
        subtotal[15] = round(total_revenue); subtotal[16] = round(total_profit)
        subtotal[17] = round(total_roas, 1) if total_spend > 0 and total_revenue > 0 else ""
        subtotal[18] = round(total_cvr, 2) if total_cvr > 0 else ""
        breakdown_rows.append(subtotal)
        product_meta.append({'product': p, 'header_offset': header_offset, 'col_header_offset': col_header_offset,
            'data_start': data_start, 'data_end': data_end, 'subtotal_offset': subtotal_offset,
            'count': len(items), 'total_revenue': total_revenue})
        breakdown_rows.append([""] * NC)
    return breakdown_rows, product_meta


def format_product_breakdown(sh, ws, breakdown_start_sheet_row, product_meta):
    sid = ws.id; fmt_reqs = []; base = breakdown_start_sheet_row - 1; NC = len(DATE_TAB_HEADERS)
    fmt_reqs.append(create_format_request(sid, base + 1, base + 2, 0, NC, get_cell_format({"red": 0.15, "green": 0.15, "blue": 0.3}, COLORS["white"], bold=True)))
    fmt_reqs.append({"mergeCells": {"range": {"sheetId": sid, "startRowIndex": base + 1, "endRowIndex": base + 2, "startColumnIndex": 0, "endColumnIndex": NC}, "mergeType": "MERGE_ALL"}})
    PRODUCT_BG = [
        {"red": 0.22, "green": 0.42, "blue": 0.65}, {"red": 0.33, "green": 0.57, "blue": 0.33},
        {"red": 0.53, "green": 0.30, "blue": 0.58}, {"red": 0.68, "green": 0.42, "blue": 0.18},
        {"red": 0.58, "green": 0.22, "blue": 0.22}, {"red": 0.20, "green": 0.52, "blue": 0.52},
        {"red": 0.48, "green": 0.48, "blue": 0.25}, {"red": 0.38, "green": 0.25, "blue": 0.48},
        {"red": 0.30, "green": 0.55, "blue": 0.45}, {"red": 0.60, "green": 0.35, "blue": 0.40},
        {"red": 0.35, "green": 0.35, "blue": 0.55},
    ]
    for idx, info in enumerate(product_meta):
        color = PRODUCT_BG[idx % len(PRODUCT_BG)]
        hr = base + info['header_offset']
        fmt_reqs.append(create_format_request(sid, hr, hr + 1, 0, NC, get_cell_format(color, COLORS["white"], bold=True)))
        fmt_reqs.append({"mergeCells": {"range": {"sheetId": sid, "startRowIndex": hr, "endRowIndex": hr + 1, "startColumnIndex": 0, "endColumnIndex": NC}, "mergeType": "MERGE_ALL"}})
        chr_idx = base + info['col_header_offset']
        fmt_reqs.append(create_format_request(sid, chr_idx, chr_idx + 1, 0, 3, {"textFormat": {"bold": True}, "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE"}))
        fmt_reqs.append(create_format_request(sid, chr_idx, chr_idx + 1, 3, 14, {"backgroundColor": {"red": 0.937, "green": 0.937, "blue": 0.937}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE"}))
        fmt_reqs.append(create_format_request(sid, chr_idx, chr_idx + 1, 14, 19, {"backgroundColor": {"red": 0.6, "green": 0.6, "blue": 0.6}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP", "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}))
        fmt_reqs.append(create_format_request(sid, chr_idx, chr_idx + 1, 19, NC, {"backgroundColor": {"red": 0.75, "green": 0.75, "blue": 0.75}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP", "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}))
        ds = base + info['data_start']; de = base + info['data_end']
        fmt_reqs.append(create_number_format_request(sid, ds, de, 14, 15, "NUMBER", "#,##0"))
        fmt_reqs.append(create_number_format_request(sid, ds, de, 15, 16, "NUMBER", "#,##0"))
        fmt_reqs.append(create_number_format_request(sid, ds, de, 16, 17, "NUMBER", "#,##0"))
        fmt_reqs.append(create_number_format_request(sid, ds, de, 17, 18, "NUMBER", "#,##0.0"))
        fmt_reqs.append(create_number_format_request(sid, ds, de, 18, 19, "NUMBER", "#,##0.00"))
        fmt_reqs.append(create_number_format_request(sid, ds, de, 19, 20, "NUMBER", "#,##0"))
        profit_cond = [("NUMBER_GREATER","300000",{"red":0,"green":1,"blue":1}),("NUMBER_GREATER","200000",{"red":0.576,"green":0.769,"blue":0.49}),("NUMBER_GREATER","100000",{"red":1,"green":0.898,"blue":0.6}),("NUMBER_GREATER","0",{"red":1,"green":0.949,"blue":0.8}),("NUMBER_LESS","-50000",{"red":0.878,"green":0.4,"blue":0.4}),("NUMBER_LESS","-10000",{"red":0.957,"green":0.8,"blue":0.8})]
        for ctype, val, bg in profit_cond:
            fmt_reqs.append({"addConditionalFormatRule":{"rule":{"ranges":[{"sheetId":sid,"startRowIndex":ds,"endRowIndex":de,"startColumnIndex":16,"endColumnIndex":17}],"booleanRule":{"condition":{"type":ctype,"values":[{"userEnteredValue":val}]},"format":{"backgroundColor":bg}}},"index":0}})
        roas_cond = [("NUMBER_GREATER_THAN_EQ","300",{"red":0,"green":1,"blue":1}),("NUMBER_GREATER_THAN_EQ","200",{"red":0.576,"green":0.769,"blue":0.49}),("NUMBER_GREATER_THAN_EQ","100",{"red":1,"green":0.851,"blue":0.4}),("NUMBER_LESS","100",{"red":0.878,"green":0.4,"blue":0.4})]
        for ctype, val, bg in roas_cond:
            fmt_reqs.append({"addConditionalFormatRule":{"rule":{"ranges":[{"sheetId":sid,"startRowIndex":ds,"endRowIndex":de,"startColumnIndex":17,"endColumnIndex":18}],"booleanRule":{"condition":{"type":ctype,"values":[{"userEnteredValue":val}]},"format":{"backgroundColor":bg}}},"index":0}})
        sr = base + info['subtotal_offset']
        fmt_reqs.append(create_format_request(sid, sr, sr + 1, 0, NC, get_cell_format({"red": 0.93, "green": 0.93, "blue": 0.85}, bold=True)))
        fmt_reqs.append(create_number_format_request(sid, sr, sr + 1, 3, 4, "NUMBER", "#,##0"))
        fmt_reqs.append(create_number_format_request(sid, sr, sr + 1, 9, 10, "NUMBER", "#,##0"))
        for c in range(14, 19): fmt_reqs.append(create_number_format_request(sid, sr, sr + 1, c, c + 1, "NUMBER", "#,##0" if c < 17 else ("#,##0.0" if c == 17 else "#,##0.00")))
        fmt_reqs.append(create_border_request(sid, hr, sr + 1, 0, NC))
    return fmt_reqs


# =========================================================
# 병렬 탭 읽기 (★ v30: FULL_REFRESH 시 폴백용)
# =========================================================
def _read_single_date_tab(sh, ws_ex, tn, dt_obj, dk, _mp_val, _mp_cnt):
    try:
        all_values = with_retry(ws_ex.get_all_values); time.sleep(2)
        if not all_values or len(all_values) < 2:
            return {'dk': dk, 'dt_obj': dt_obj, 'status': 'empty'}
        structure = detect_tab_structure(all_values[0])
        _, scan_rows = find_last_data_row(all_values, structure)
        row_count = 0; tab_ad_sets = {}; tab_budget_by_product = defaultdict(lambda: {'spend': 0.0, 'revenue': 0.0})
        tab_master_rows = []; master_headers_local = ["Date"] + DATE_TAB_HEADERS
        for row in scan_rows:
            norm_row = normalize_row_to_new(row, structure)
            cn = str(norm_row[0]).strip()
            if not cn or cn in ["캠페인 이름", "전체", "합계", "Total"]: continue
            asn = str(norm_row[1]).strip(); asid = str(norm_row[2]).strip()
            spend = _to_num(norm_row[3]); cpm = _to_num(norm_row[6]); unique_clicks = _to_num(norm_row[9])
            mpc = _mp_cnt.get((dk, asid), 0); revenue = _mp_val.get((dk, asid), 0.0)
            if mpc == 0 and revenue == 0.0: mpc = _to_num(norm_row[14]); revenue = _to_num(norm_row[15])
            profit = revenue - spend; roas = (revenue / spend * 100) if spend > 0 else 0
            cvr = (mpc / unique_clicks * 100) if unique_clicks > 0 and mpc > 0 else 0
            if asid:
                tab_ad_sets[asid] = {'campaign_name': cn, 'adset_name': asn, 'adset_id': asid,
                    'date_data': {'profit': profit, 'revenue': revenue, 'spend': spend, 'cpm': cpm, 'cvr': cvr, 'unique_clicks': unique_clicks, 'mpc': mpc}}
            p = extract_product(asn, cn)
            tab_budget_by_product[p]['spend'] += spend; tab_budget_by_product[p]['revenue'] += revenue
            mrd = [dk] + list(norm_row)
            mrd[14+1] = int(mpc) if mpc > 0 else ""; mrd[15+1] = round(revenue) if revenue > 0 else ""
            mrd[16+1] = round(profit, 0) if spend > 0 else ""; mrd[17+1] = round(roas, 1) if spend > 0 and revenue > 0 else ""
            mrd[18+1] = round(cvr, 2) if cvr > 0 else ""
            while len(mrd) < len(master_headers_local): mrd.append("")
            tab_master_rows.append({'date': dk, 'date_obj': dt_obj, 'spend': spend, 'row_data': mrd[:len(master_headers_local)]})
            row_count += 1
        return {'dk': dk, 'dt_obj': dt_obj, 'status': 'ok', 'row_count': row_count,
            'structure': structure, 'ad_sets': tab_ad_sets, 'budget_by_product': dict(tab_budget_by_product), 'master_rows': tab_master_rows}
    except Exception as e:
        return {'dk': dk, 'dt_obj': dt_obj, 'status': 'error', 'error': str(e)}


# =========================================================
# ★ v30: 마스터탭에서 과거 데이터 읽기 (핵심 개선)
# =========================================================
def read_from_master_tab(sh, master_ws, preloaded_date_data, mp_value_map=None, mp_count_map=None):
    print("\n" + "=" * 60)
    print("★ 8.5단계: 마스터탭 재활용 (v30 - API 1회 읽기)")
    print("=" * 60)

    all_ad_sets = defaultdict(lambda: {'campaign_name': '', 'adset_name': '', 'adset_id': '', 'dates': {}})
    all_budget_by_date = defaultdict(lambda: defaultdict(lambda: {'spend': 0.0, 'revenue': 0.0}))
    all_master_raw_data = []
    all_date_objects = {}
    all_date_names_set = set()
    master_headers_local = ["Date"] + DATE_TAB_HEADERS

    _preloaded = preloaded_date_data or {}
    preloaded_dates = set(_preloaded.keys())
    _mp_val = mp_value_map or {}
    _mp_cnt = mp_count_map or {}

    print(f"  📖 마스터탭 읽기 중...")
    master_values = with_retry(master_ws.get_all_values)
    time.sleep(2)

    if not master_values or len(master_values) < 2:
        print("  ⚠️ 마스터탭 비어있음")
        all_ws = with_retry(sh.worksheets)
        return all_ad_sets, all_budget_by_date, all_master_raw_data, all_date_objects, sorted(list(all_date_names_set)), all_ws

    historical_count = 0
    skipped_count = 0

    for row in master_values[1:]:
        if not row or len(row) < 4:
            continue
        raw_dk = str(row[0]).strip()
        if not raw_dk:
            continue

        dt_parsed = parse_date_tab(raw_dk)
        if dt_parsed is None:
            try:
                cleaned = raw_dk.replace('.', '/').replace('-', '/').replace(' ', '')
                dt_parsed = parse_date_tab(cleaned)
            except:
                pass
            if dt_parsed is None:
                continue
        dk = f"{dt_parsed.year%100:02d}/{dt_parsed.month:02d}/{dt_parsed.day:02d}"

        if dk in preloaded_dates:
            skipped_count += 1
            continue

        dt_obj = parse_date_tab(dk)
        if not dt_obj:
            continue

        all_date_objects[dk] = dt_obj
        all_date_names_set.add(dk)
        historical_count += 1

        tab_row = row[1:]
        cn = str(tab_row[0]).strip() if len(tab_row) > 0 else ""
        if not cn or cn in ["캠페인 이름", "전체", "합계", "Total"]:
            continue

        asn = str(tab_row[1]).strip() if len(tab_row) > 1 else ""
        asid = clean_id(tab_row[2]) if len(tab_row) > 2 else ""
        spend = _to_num(tab_row[3]) if len(tab_row) > 3 else 0
        unique_clicks = _to_num(tab_row[9]) if len(tab_row) > 9 else 0
        cpm = _to_num(tab_row[6]) if len(tab_row) > 6 else 0
        mpc = _to_num(tab_row[14]) if len(tab_row) > 14 else 0
        revenue = _to_num(tab_row[15]) if len(tab_row) > 15 else 0
        profit = revenue - spend
        roas = (revenue / spend * 100) if spend > 0 else 0
        cvr = (mpc / unique_clicks * 100) if unique_clicks > 0 and mpc > 0 else 0

        if asid:
            if not all_ad_sets[asid]['adset_id']:
                all_ad_sets[asid] = {'campaign_name': cn, 'adset_name': asn, 'adset_id': asid, 'dates': {}}
            all_ad_sets[asid]['dates'][dk] = {
                'profit': profit, 'revenue': revenue, 'spend': spend,
                'cpm': cpm, 'cvr': cvr, 'unique_clicks': unique_clicks, 'mpc': mpc
            }

        p = extract_product(asn, cn)
        all_budget_by_date[dk][p]['spend'] += spend
        all_budget_by_date[dk][p]['revenue'] += revenue

        mrd = list(row)
        while len(mrd) < len(master_headers_local):
            mrd.append("")
        all_master_raw_data.append({
            'date': dk, 'date_obj': dt_obj, 'spend': spend,
            'row_data': mrd[:len(master_headers_local)]
        })

    print(f"  ✅ 마스터탭: 과거 {historical_count}행 읽기 | {skipped_count}행 스킵 (갱신 대상)")

    preloaded_row_count = 0
    for dk, tab_rows in _preloaded.items():
        dt_obj = parse_date_tab(dk)
        if not dt_obj:
            continue
        all_date_objects[dk] = dt_obj
        all_date_names_set.add(dk)

        for tab_row in tab_rows:
            cn = str(tab_row[0]).strip()
            if not cn or cn in ["캠페인 이름", "전체", "합계", "Total"]:
                continue
            asn = str(tab_row[1]).strip()
            asid = clean_id(tab_row[2])
            spend = _to_num(tab_row[3])
            unique_clicks = _to_num(tab_row[9])
            cpm = _to_num(tab_row[6])
            mpc = _to_num(tab_row[14])
            revenue = _to_num(tab_row[15])
            profit = revenue - spend
            roas = (revenue / spend * 100) if spend > 0 else 0
            cvr = (mpc / unique_clicks * 100) if unique_clicks > 0 and mpc > 0 else 0

            if asid:
                if not all_ad_sets[asid]['adset_id']:
                    all_ad_sets[asid] = {'campaign_name': cn, 'adset_name': asn, 'adset_id': asid, 'dates': {}}
                all_ad_sets[asid]['dates'][dk] = {
                    'profit': profit, 'revenue': revenue, 'spend': spend,
                    'cpm': cpm, 'cvr': cvr, 'unique_clicks': unique_clicks, 'mpc': mpc
                }

            p = extract_product(asn, cn)
            all_budget_by_date[dk][p]['spend'] += spend
            all_budget_by_date[dk][p]['revenue'] += revenue

            mrd = [dk] + list(tab_row)
            while len(mrd) < len(master_headers_local):
                mrd.append("")
            all_master_raw_data.append({
                'date': dk, 'date_obj': dt_obj, 'spend': spend,
                'row_data': mrd[:len(master_headers_local)]
            })
            preloaded_row_count += 1

    print(f"  ♻️ preloaded 갱신: {len(preloaded_dates)}일, {preloaded_row_count}행")

    all_date_names = sorted(list(all_date_names_set), key=lambda x: all_date_objects.get(x, datetime.min))
    all_ws = with_retry(sh.worksheets)

    print(f"\n  ✅ 총 날짜: {len(all_date_names)}개 | 광고세트: {len(all_ad_sets)}개 | 마스터행: {len(all_master_raw_data)}개")
    if all_date_names:
        print(f"  📅 범위: {all_date_names[0]} ~ {all_date_names[-1]}")

    return all_ad_sets, all_budget_by_date, all_master_raw_data, all_date_objects, all_date_names, all_ws


# =========================================================
# ★ v30: FULL_REFRESH 시 폴백 — 개별 탭 읽기 (기존 방식)
# =========================================================
def read_all_date_tabs_fallback(sh, analysis_tab_names, mp_value_map=None, mp_count_map=None, preloaded_date_data=None):
    print("\n" + "=" * 60)
    print("★ 8.5단계: 전체 날짜탭 읽기 (폴백 - 배치 처리)")
    print("=" * 60)
    all_ad_sets = defaultdict(lambda: {'campaign_name': '', 'adset_name': '', 'adset_id': '', 'dates': {}})
    all_budget_by_date = defaultdict(lambda: defaultdict(lambda: {'spend': 0.0, 'revenue': 0.0}))
    all_master_raw_data = []; all_date_objects = {}; all_date_names = []
    analysis_set = set(analysis_tab_names); all_ws = with_retry(sh.worksheets); date_tabs_found = []
    print(f"\n  📋 전체 워크시트: {len(all_ws)}개")

    for ws_ex in all_ws:
        tn = ws_ex.title
        if tn in analysis_set: continue
        dt_obj = parse_date_tab(tn)
        if dt_obj is None: continue
        date_tabs_found.append((dt_obj, ws_ex, tn))
    date_tabs_found.sort(key=lambda x: x[0])
    print(f"  ✅ 날짜탭 파싱 성공: {len(date_tabs_found)}개")
    if date_tabs_found: print(f"  📅 범위: {date_tabs_found[0][2]} ~ {date_tabs_found[-1][2]}")

    _mp_val = mp_value_map or {}; _mp_cnt = mp_count_map or {}
    _preloaded = preloaded_date_data or {}
    master_headers_local = ["Date"] + DATE_TAB_HEADERS

    preloaded_count = 0
    tabs_to_read = []
    for dt_obj, ws_ex, tn in date_tabs_found:
        dk = f"{dt_obj.year%100:02d}/{dt_obj.month:02d}/{dt_obj.day:02d}"
        all_date_objects[dk] = dt_obj
        all_date_names.append(dk)

        if dk in _preloaded and _preloaded[dk]:
            tab_rows = _preloaded[dk]
            for tab_row in tab_rows:
                cn = str(tab_row[0]).strip(); asn = str(tab_row[1]).strip(); asid = clean_id(tab_row[2])
                if not cn or cn in ["캠페인 이름", "전체", "합계", "Total"]: continue
                spend = _to_num(tab_row[3]); unique_clicks = _to_num(tab_row[9]); cpm = _to_num(tab_row[6])
                mpc = _to_num(tab_row[14]); revenue = _to_num(tab_row[15])
                profit = revenue - spend; roas = (revenue / spend * 100) if spend > 0 else 0
                cvr = (mpc / unique_clicks * 100) if unique_clicks > 0 and mpc > 0 else 0
                if asid:
                    if not all_ad_sets[asid]['adset_id']:
                        all_ad_sets[asid] = {'campaign_name': cn, 'adset_name': asn, 'adset_id': asid, 'dates': {}}
                    all_ad_sets[asid]['dates'][dk] = {'profit': profit, 'revenue': revenue, 'spend': spend, 'cpm': cpm, 'cvr': cvr, 'unique_clicks': unique_clicks, 'mpc': mpc}
                p = extract_product(asn, cn)
                all_budget_by_date[dk][p]['spend'] += spend
                all_budget_by_date[dk][p]['revenue'] += revenue
                mrd = [dk] + list(tab_row)
                while len(mrd) < len(master_headers_local): mrd.append("")
                all_master_raw_data.append({'date': dk, 'date_obj': dt_obj, 'spend': spend, 'row_data': mrd[:len(master_headers_local)]})
            preloaded_count += 1
            safe_print(f"  ♻️ {dk} → preloaded ({len(tab_rows)}행)")
        else:
            tabs_to_read.append((dt_obj, ws_ex, tn, dk))

    print(f"\n  ♻️ preloaded 재활용: {preloaded_count}개")
    print(f"  📖 시트에서 읽을 탭: {len(tabs_to_read)}개")

    total_batches = math.ceil(len(tabs_to_read) / SHEETS_READ_BATCH_SIZE)
    print(f"  🔀 배치 처리: {total_batches}개 배치 × {SHEETS_READ_BATCH_SIZE}탭 (워커: {SHEETS_READ_WORKERS}개)")

    for batch_idx in range(total_batches):
        batch_start = batch_idx * SHEETS_READ_BATCH_SIZE
        batch_end = min(batch_start + SHEETS_READ_BATCH_SIZE, len(tabs_to_read))
        batch = tabs_to_read[batch_start:batch_end]
        batch_results = []

        safe_print(f"\n  📦 배치 {batch_idx+1}/{total_batches} ({len(batch)}탭)...")

        with ThreadPoolExecutor(max_workers=SHEETS_READ_WORKERS) as pool:
            future_map = {}
            for dt_obj, ws_ex, tn, dk in batch:
                future = pool.submit(_read_single_date_tab, sh, ws_ex, tn, dt_obj, dk, _mp_val, _mp_cnt)
                future_map[future] = (dk, tn)
            for future in as_completed(future_map):
                dk, tn = future_map[future]
                try: result = future.result(); batch_results.append(result)
                except Exception as e:
                    safe_print(f"  ❌ {dk} 읽기 예외: {e}")
                    batch_results.append({'dk': dk, 'dt_obj': None, 'status': 'error', 'error': str(e)})

        for result in batch_results:
            dk = result['dk']; status = result['status']
            if status in ['empty', 'error']: continue
            row_count = result['row_count']
            safe_print(f"  📖 {dk} → {row_count}행")
            if row_count == 0: continue
            for asid, ad_data in result['ad_sets'].items():
                if not all_ad_sets[asid]['adset_id']:
                    all_ad_sets[asid] = {'campaign_name': ad_data['campaign_name'], 'adset_name': ad_data['adset_name'], 'adset_id': ad_data['adset_id'], 'dates': {}}
                all_ad_sets[asid]['dates'][dk] = ad_data['date_data']
            for p, bd in result['budget_by_product'].items():
                all_budget_by_date[dk][p]['spend'] += bd['spend']; all_budget_by_date[dk][p]['revenue'] += bd['revenue']
            all_master_raw_data.extend(result['master_rows'])

        if batch_idx < total_batches - 1:
            safe_print(f"  ⏳ 배치 쿨다운 {SHEETS_READ_BATCH_DELAY}초...")
            time.sleep(SHEETS_READ_BATCH_DELAY)

    print(f"\n  ✅ 날짜탭: {len(date_tabs_found)}개 | date_names: {len(all_date_names)}개 | 광고세트: {len(all_ad_sets)}개 | 마스터행: {len(all_master_raw_data)}개")
    return all_ad_sets, all_budget_by_date, all_master_raw_data, all_date_objects, all_date_names, all_ws


def diagnose_chart_coverage(sh, date_names, ad_sets, analysis_tabs_set, all_ws=None):
    print("\n" + "=" * 60); print("★ 진단: 추이차트 커버리지 확인"); print("=" * 60)
    if all_ws is None: all_ws = with_retry(sh.worksheets)
    all_tab_names = [ws.title for ws in all_ws]; slash_tabs = [t for t in all_tab_names if '/' in t]
    parsed_tabs = []; failed_tabs = []
    for t in slash_tabs:
        dt = parse_date_tab(t)
        if dt: dk = f"{dt.year%100:02d}/{dt.month:02d}/{dt.day:02d}"; parsed_tabs.append((t, dk, dt))
        else: failed_tabs.append(t)
    date_names_set = set(date_names)
    in_chart = [(t, dk) for t, dk, dt in parsed_tabs if dk in date_names_set]
    not_in_chart = [(t, dk) for t, dk, dt in parsed_tabs if dk not in date_names_set]
    print(f"  ✅ 추이차트 포함: {len(in_chart)}개 | 누락: {len(not_in_chart)}개 | 파싱실패: {len(failed_tabs)}개")
    if date_names: print(f"  📊 범위: {date_names[0]} ~ {date_names[-1]} ({len(date_names)}일)")
    return failed_tabs


def generate_date_tab_summary(rows, structure="new"):
    total_spend=0.0;total_meta_purchase=0.0;total_mp_purchase=0.0;total_revenue=0.0;total_profit=0.0;total_unique_clicks=0.0
    spend_by_account={"본계정":0.0,"부계정":0.0,"3rd계정":0.0}
    prod_spend=defaultdict(float);prod_revenue=defaultdict(float);prod_profit=defaultdict(float)
    for row in rows:
        nr=normalize_row_to_new(row,structure);cn=str(nr[0]);asn=str(nr[1]);adset_id=str(nr[2]);spend=_to_num(nr[3]);meta_p=_to_num(nr[13]);mp_p=_to_num(nr[14]);rev=_to_num(nr[15]);prof=_to_num(nr[16]);uc=_to_num(nr[9])
        if not cn or cn in ["전체","합계","Total"]: continue
        total_spend+=spend;total_meta_purchase+=meta_p;total_mp_purchase+=mp_p;total_revenue+=rev;total_profit+=prof;total_unique_clicks+=uc
        if adset_id.startswith("12023"): spend_by_account["본계정"]+=spend
        elif adset_id.startswith("12024"): spend_by_account["부계정"]+=spend
        elif adset_id.startswith("6"): spend_by_account["3rd계정"]+=spend
        p = extract_product(asn, cn)
        prod_spend[p]+=spend;prod_revenue[p]+=rev;prod_profit[p]+=prof
    total_roas=(total_revenue/total_spend*100) if total_spend>0 else 0
    total_cvr=(total_mp_purchase/total_unique_clicks*100) if total_unique_clicks>0 else 0
    sp = sorted([p for p in prod_spend if (prod_spend[p] > 0 or prod_revenue[p] > 0)],
                key=lambda p: prod_spend[p], reverse=True)
    num_products = len(sp); NC = max(25, 9 + num_products + 2)
    sum_col_idx = 9 + num_products
    summary_data = []; summary_data.append([""]*NC); summary_data.append([""]*NC); summary_data.append([""]*NC)
    r_title=[""]*NC; r_title[14]="전체"; summary_data.append(r_title)
    r_hdr=[""]*NC; r_hdr[11]="본계정";r_hdr[12]="부계정";r_hdr[13]="3rd 계정"
    r_hdr[14]="지출 금액 (KRW)";r_hdr[15]="구매 (메타)";r_hdr[16]="구매 (믹스패널)"
    r_hdr[17]="매출";r_hdr[18]="이익";r_hdr[19]="ROAS";r_hdr[20]="CVR"; summary_data.append(r_hdr)
    r_val=[""]*NC; r_val[11]=round(spend_by_account["본계정"]);r_val[12]=round(spend_by_account["부계정"]);r_val[13]=round(spend_by_account["3rd계정"])
    r_val[14]=round(total_spend);r_val[15]=round(total_meta_purchase);r_val[16]=round(total_mp_purchase)
    r_val[17]=round(total_revenue);r_val[18]=round(total_profit);r_val[19]=round(total_roas,1);r_val[20]=total_cvr/100 if total_cvr else 0
    summary_data.append(r_val)
    if num_products == 0: return summary_data, 0
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
    return summary_data, num_products

def format_date_tab_summary(sh, ws, summary_start_sheet_row, summary_row_count, num_products=0):
    sid=ws.id; base=summary_start_sheet_row-1
    C_PINK={"red":0.957,"green":0.8,"blue":0.8}; C_ORANGE={"red":0.988,"green":0.898,"blue":0.804}
    C_LGREEN={"red":0.851,"green":0.918,"blue":0.827}; C_GRAY={"red":0.69,"green":0.7,"blue":0.698}
    C_DGRAY={"red":0.6,"green":0.6,"blue":0.6}; C_BLACK={"red":0,"green":0,"blue":0}
    C_WHITE={"red":1,"green":1,"blue":1}; C_LYELLOW={"red":1,"green":1,"blue":0.8}
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
    if num_products == 0: return fmt_requests
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
    return fmt_requests


# =========================================================
# ★ v30c: 청크 쓰기 + 포맷 배치 헬퍼
# =========================================================
def chunked_sheet_write(ws, data_rows, range_start="A1", chunk_size=WEEKLY_WRITE_CHUNK_SIZE, label=""):
    """데이터를 chunk_size 행씩 나눠 쓰기 (대량 데이터 timeout 방지)"""
    total = len(data_rows)
    if total == 0:
        return
    total_chunks = math.ceil(total / chunk_size)
    for ci in range(total_chunks):
        start_idx = ci * chunk_size
        end_idx = min(start_idx + chunk_size, total)
        chunk = data_rows[start_idx:end_idx]
        rng = f"A{start_idx + 1}"
        with_retry(ws.update, values=chunk, range_name=rng, value_input_option="USER_ENTERED")
        if total_chunks > 1:
            print(f"  📝 {label} 청크 {ci+1}/{total_chunks} ({len(chunk)}행) 쓰기 완료")
            time.sleep(5)
        else:
            time.sleep(2)


def chunked_format_apply(sh, format_requests, batch_size=WEEKLY_FORMAT_BATCH_SIZE, delay=WEEKLY_FORMAT_BATCH_DELAY, label=""):
    """포맷 요청을 batch_size개씩 나눠 적용"""
    if not format_requests:
        return
    total_batches = math.ceil(len(format_requests) / batch_size)
    for bi in range(total_batches):
        start = bi * batch_size
        end = min(start + batch_size, len(format_requests))
        batch = format_requests[start:end]
        try:
            with_retry(sh.batch_update, body={"requests": batch})
        except Exception as e:
            print(f"  ⚠️ {label} 포맷 배치 {bi+1}/{total_batches} 오류: {e}")
        time.sleep(delay)
    if total_batches > 1:
        print(f"  ✅ {label} 포맷 적용 완료 ({len(format_requests)}개 → {total_batches}배치)")


# =============================================================================
# 실행 시작
# =============================================================================
print("\n"+"="*60); print("1단계: 광고 계정 설정"); print("="*60)
ALL_AD_ACCOUNTS = ["act_1270614404675034", "act_707835224206178", "act_1808141386564262"]
print(f"📋 광고 계정 ({len(ALL_AD_ACCOUNTS)}개):")
for acc in ALL_AD_ACCOUNTS: print(f"  - {acc}")
print()

# =========================================================
# 2단계 Meta - 병렬의 병렬
# =========================================================
print("="*60); print(f"2단계: Meta Insights 수집 (병렬: 날짜 {META_DATE_WORKERS}워커 × 계정 {META_ACCOUNT_WORKERS}워커)"); print("="*60)

def _fetch_single_account(acc_id, target_str, date_key, target_date):
    rows = fetch_meta_insights_daily(acc_id, target_str)
    if rows:
        parsed = parse_single_day_insights(rows, date_key, target_date, ad_account_id=acc_id)
        safe_print(f"    ✅ {date_key} {acc_id[-6:]}: {len(parsed)}건"); return parsed
    else:
        safe_print(f"    ⚠️ {date_key} {acc_id[-6:]}: 0건"); return []

def _fetch_single_date(day_offset):
    target_date = TODAY - timedelta(days=day_offset); target_str = target_date.strftime('%Y-%m-%d')
    date_key = f"{target_date.year%100:02d}/{target_date.month:02d}/{target_date.day:02d}"
    day_rows = []
    with ThreadPoolExecutor(max_workers=META_ACCOUNT_WORKERS) as inner_pool:
        inner_futures = {inner_pool.submit(_fetch_single_account, acc_id, target_str, date_key, target_date): acc_id for acc_id in ALL_AD_ACCOUNTS}
        for f in as_completed(inner_futures):
            try: result = f.result(); day_rows.extend(result)
            except Exception as e: safe_print(f"    ❌ {date_key} 계정 오류: {e}")
    return date_key, target_date, day_rows

meta_date_data = defaultdict(list); meta_success_count = 0
with ThreadPoolExecutor(max_workers=META_DATE_WORKERS) as date_pool:
    date_futures = {date_pool.submit(_fetch_single_date, day_offset): day_offset for day_offset in range(META_COLLECT_DAYS)}
    for f in as_completed(date_futures):
        try:
            date_key, target_date, day_rows = f.result()
            if day_rows: meta_date_data[date_key] = day_rows; meta_success_count += len(day_rows); safe_print(f"  📊 {date_key}: {len(day_rows)}개")
            else: safe_print(f"  ⚠️ {date_key} 데이터 없음")
        except Exception as e: safe_print(f"  ❌ 날짜 조회 오류: {e}")
print(f"\n✅ Meta 수집 완료: {len(meta_date_data)}일, 총 {meta_success_count}건")

ADSET_TO_ACCOUNT = {}
for dk, rows in meta_date_data.items():
    for mr in rows:
        asid = mr.get('adset_id', ''); acc = mr.get('ad_account_id', '')
        if asid and acc: ADSET_TO_ACCOUNT[asid] = acc
print(f"  📦 adset→account 매핑: {len(ADSET_TO_ACCOUNT)}개\n")

# 2.5단계 예산
print("="*60); print(f"2.5단계: 예산 조회 (병렬: {BUDGET_WORKERS}워커)"); print("="*60)
adset_budget_map = {}
def _fetch_budget_for_account(acc_id):
    safe_print(f"  📡 계정 {acc_id}..."); budgets = fetch_adset_budgets(acc_id); safe_print(f"  ✅ {acc_id[-6:]}: {len(budgets)}개"); return budgets
with ThreadPoolExecutor(max_workers=BUDGET_WORKERS) as budget_pool:
    budget_futures = {budget_pool.submit(_fetch_budget_for_account, acc): acc for acc in ALL_AD_ACCOUNTS}
    for f in as_completed(budget_futures):
        try: budgets = f.result(); adset_budget_map.update(budgets)
        except Exception as e: safe_print(f"  ❌ 예산 조회 오류: {e}")
print(f"\n✅ 예산 조회 완료: {len(adset_budget_map)}개 세트\n")

# 3단계 Mixpanel
print("="*60); print(f"3단계: Mixpanel 수집 ({REFRESH_DAYS}일)"); print("="*60)
YESTERDAY = TODAY - timedelta(days=1); mp_to_today = TODAY.strftime('%Y-%m-%d')
total_days = (TODAY - DATA_REFRESH_START).days + 1
print(f"  📅 범위: {DATA_REFRESH_START.strftime('%Y-%m-%d')} ~ {mp_to_today} ({total_days}일)")
mp_raw = []
if REFRESH_DAYS > 14:
    CHUNK_SIZE = 7; chunks = []; chunk_start = DATA_REFRESH_START
    while chunk_start <= YESTERDAY:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_SIZE - 1), YESTERDAY)
        chunks.append((chunk_start.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d'))); chunk_start = chunk_end + timedelta(days=1)
    with ThreadPoolExecutor(max_workers=MIXPANEL_CHUNK_WORKERS) as mp_pool:
        mp_futures = {mp_pool.submit(fetch_mixpanel_data, c_from, c_to): (c_from, c_to) for c_from, c_to in chunks}
        for f in as_completed(mp_futures):
            try: chunk_data = f.result(); mp_raw.extend(chunk_data)
            except Exception as e: safe_print(f"  ❌ 청크 오류: {e}")
else:
    mp_from = DATA_REFRESH_START.strftime('%Y-%m-%d'); mp_to_yesterday = YESTERDAY.strftime('%Y-%m-%d')
    if DATA_REFRESH_START <= YESTERDAY:
        chunk_data = fetch_mixpanel_data(mp_from, mp_to_yesterday); mp_raw.extend(chunk_data); time.sleep(2)
print(f"\n  ── 오늘({mp_to_today}) 별도 호출 ──")
today_data = fetch_mixpanel_data(mp_to_today, mp_to_today)
if today_data: mp_raw.extend(today_data)
print(f"\n  ✅ Mixpanel 수집 완료: 총 {len(mp_raw)}건")
df = pd.DataFrame(mp_raw); mp_value_map = {}; mp_count_map = {}
if len(df) > 0:
    df = df[df['utm_term'].notna() & (df['utm_term'] != '') & (df['utm_term'] != 'None')]
    df['_has_utm'] = df['utm_term'].apply(lambda x: 0 if (x and str(x).strip() and str(x).strip() != 'None') else 1)
    df = df.sort_values(['_has_utm', 'revenue'], ascending=[True, False])
    df_d = df.drop_duplicates(subset=['date', 'distinct_id', '서비스'], keep='first')
    df = df.drop(columns=['_has_utm'])
    total_revenue = df_d['revenue'].sum(); print(f"  📊 매출 합계: ₩{int(total_revenue):,}")
    for (d, ut), v in df_d.groupby(['date', 'utm_term'])['revenue'].sum().items():
        if d and ut: mp_value_map[(d, str(ut))] = v
    for (d, ut), c in df_d.groupby(['date', 'utm_term']).size().items():
        if d and ut: mp_count_map[(d, str(ut))] = c
print()

# 4단계: 기존 탭 파악
print("="*60); print(f"4단계: 최근 {REFRESH_DAYS}일 기존 날짜탭 파악"); print("="*60)
refresh_date_keys = set()
for day_offset in range(REFRESH_DAYS):
    td = TODAY - timedelta(days=day_offset); dk = f"{td.year%100:02d}/{td.month:02d}/{td.day:02d}"; refresh_date_keys.add(dk)
existing_sheets = sh.worksheets(); existing_refresh_tabs = {}; new_refresh_dates = set(refresh_date_keys)
for ws_ex in existing_sheets:
    tn = ws_ex.title; dt_obj = parse_date_tab(tn)
    if dt_obj is None: continue
    tab_dk = f"{dt_obj.year%100:02d}/{dt_obj.month:02d}/{dt_obj.day:02d}"
    if tab_dk in refresh_date_keys: existing_refresh_tabs[tab_dk] = ws_ex; new_refresh_dates.discard(tab_dk)
print(f"  📝 기존 탭 업데이트: {len(existing_refresh_tabs)}개 | 🆕 새로 생성: {len(new_refresh_dates)}개\n")

# 4.5단계 - 통화 감지 + 환율
print("="*60); print("4.5단계: 통화 감지 + 환율"); print("="*60)
ACCOUNT_CURRENCY = {}; has_usd_account = False
for acc_id in ALL_AD_ACCOUNTS:
    currency, acc_name = detect_account_currency(acc_id); ACCOUNT_CURRENCY[acc_id] = currency
    if currency != 'KRW': has_usd_account = True
    print(f"  💱 {acc_id[-6:]}: {currency}"); time.sleep(0.5)
usd_krw_rates = {}
if has_usd_account:
    rate_range_start = DATA_REFRESH_START - timedelta(days=7)
    usd_krw_rates = fetch_daily_exchange_rates(rate_range_start, TODAY, currency="USD")
else: print("  ✅ 모든 계정 KRW")
def get_fx_for_account(acc_id, dk):
    cur = ACCOUNT_CURRENCY.get(acc_id, 'KRW')
    if cur == 'KRW': return 1.0
    return get_rate_for_date(usd_krw_rates, dk)
def get_budget_divisor(acc_id):
    cur = ACCOUNT_CURRENCY.get(acc_id, 'KRW'); return 100 if cur != 'KRW' else 1
print()

# 5단계: 병합
print("="*60); print("5단계: Meta + Mixpanel 병합"); print("="*60)
date_tab_rows = defaultdict(list); date_mp_by_adsetid = defaultdict(dict); new_date_names = []; product_count = defaultdict(int)
debug_total_rows = 0; debug_matched_rows = 0; debug_matched_revenue = 0
for dk in sorted(meta_date_data.keys(), key=lambda x: meta_date_data[x][0]['date_obj'] if meta_date_data[x] else datetime.min, reverse=True):
    rows = meta_date_data[dk]
    if not rows: continue
    dt = rows[0]['date_obj']; new_date_names.append(dk)
    for mr in rows:
        asid = mr['adset_id']
        if not asid: continue
        debug_total_rows += 1; acc_id = mr.get('ad_account_id', '')
        fx = get_fx_for_account(acc_id, dk); budget_div = get_budget_divisor(acc_id)
        sp = mr['spend'] * fx; uc = mr['unique_clicks']; results = mr['results']
        cpm = mr['cpm'] * fx; frequency = mr['frequency']; cost_per_result = mr['cost_per_result'] * fx
        cost_per_click = mr['cost_per_unique_click'] * fx; unique_ctr = mr['unique_ctr']
        mpc = mp_count_map.get((dk, asid), 0); mpv = mp_value_map.get((dk, asid), 0.0)
        if mpc > 0 or mpv > 0: debug_matched_rows += 1; debug_matched_revenue += mpv
        rv = float(mpv); date_mp_by_adsetid[dk][asid] = {'mpc': mpc, 'mpv': rv}
        pf = rv - sp; roas_c = (rv / sp * 100) if sp > 0 else 0; cvr_c = (mpc / uc * 100) if uc > 0 and mpc > 0 else 0
        cn = mr['campaign_name']; asn = mr['adset_name']
        budget_raw = adset_budget_map.get(asid, 0); budget_val = round(budget_raw / budget_div * fx) if budget_raw and budget_raw > 0 else ""
        tab_row = [cn, asn, asid, sp, cost_per_result, 0, round(cpm, 0), mr['reach'], mr['impressions'], uc, round(unique_ctr, 2), round(cost_per_click, 0), round(frequency, 2), results,
            mpc if mpc > 0 else "", round(rv) if rv > 0 else "", round(pf, 0) if sp > 0 else "", round(roas_c, 1) if sp > 0 and rv > 0 else "", round(cvr_c, 2) if uc > 0 and mpc > 0 else "", budget_val, "", "", ""]
        date_tab_rows[dk].append(tab_row); p = extract_product(asn, cn); product_count[p] += 1
print(f"✅ 날짜탭: {len(new_date_names)}개 | 📦 제품별: {dict(product_count)}")
if debug_total_rows > 0: print(f"🔍 매칭: {debug_matched_rows}/{debug_total_rows} ({debug_matched_rows/debug_total_rows*100:.1f}%) | 매출: ₩{int(debug_matched_revenue):,}")
print()

# =========================================================
# ★ v30: 6단계 — 마스터탭 보존, 나머지 분석탭만 삭제
# =========================================================
print("="*60); print("6단계: 분석탭 삭제 (★ v30: 마스터탭 보존)"); print("="*60)

_existing_master_ws = None
if not FULL_REFRESH:
    try:
        _existing_master_ws = sh.worksheet("마스터탭")
        print(f"  💾 마스터탭 발견 → 보존 (8.5단계에서 재활용)")
    except gspread.exceptions.WorksheetNotFound:
        print(f"  ⚠️ 마스터탭 없음 → preloaded 모드 (10일 데이터로 진행)")

# ★ v30f: 모든 분석탭 재사용 (삭제하지 않음), 임시탭만 삭제
ANALYSIS_TAB_NAMES_TO_DELETE = ["_temp", "_temp_holder"]
if FULL_REFRESH:
    ANALYSIS_TAB_NAMES_TO_DELETE.extend(["추이차트", "추이차트_상품별", "증감액", "추이차트(주간)", "예산", "주간종합", "주간종합_2", "주간종합_3", "마스터탭"])
    print("  🔥 FULL_REFRESH: 모든 분석탭 삭제 후 재생성")

for sn in ANALYSIS_TAB_NAMES_TO_DELETE:
    try:
        old = sh.worksheet(sn)
        if len(sh.worksheets()) <= 1: with_retry(sh.add_worksheet, title="_tmp", rows=1, cols=1); time.sleep(1)
        sh.del_worksheet(old); print(f"  ✅ '{sn}' 삭제"); time.sleep(2)
    except gspread.exceptions.WorksheetNotFound: pass
    except Exception as e: print(f"  ⚠️ '{sn}' 삭제 실패: {e}"); time.sleep(2)
print("⏳ 2초 대기..."); time.sleep(2)

# =========================================================
# ★ v30h: 7단계 → 마스터탭 먼저 (8.5+9를 앞으로 이동)
# =========================================================
print("\n"+"="*60); print("7단계: 마스터탭 갱신 (먼저 처리)"); print("="*60)
master_headers = ["Date"] + DATE_TAB_HEADERS

if _existing_master_ws and not FULL_REFRESH:
    _preloaded_only = False
    ad_sets, budget_by_date, master_raw_data, date_objects, date_names, cached_ws = read_from_master_tab(
        sh, _existing_master_ws, date_tab_rows, mp_value_map=mp_value_map, mp_count_map=mp_count_map
    )
elif FULL_REFRESH:
    _preloaded_only = False
    print("  🔥 FULL_REFRESH → 개별 탭 읽기 모드")
    ad_sets, budget_by_date, master_raw_data, date_objects, date_names, cached_ws = read_all_date_tabs_fallback(
        sh, ANALYSIS_TABS_SET, mp_value_map=mp_value_map, mp_count_map=mp_count_map, preloaded_date_data=date_tab_rows
    )
else:
    _preloaded_only = True
    print("  ⚠️ 마스터탭 없음 → preloaded 10일 데이터로 진행 (폴백 스킵)")
    ad_sets = defaultdict(lambda: {'campaign_name': '', 'adset_name': '', 'adset_id': '', 'dates': {}})
    budget_by_date = defaultdict(lambda: defaultdict(lambda: {'spend': 0.0, 'revenue': 0.0}))
    master_raw_data = []; date_objects = {}; date_names = []
    master_headers_local = ["Date"] + DATE_TAB_HEADERS
    for dk, tab_rows in date_tab_rows.items():
        dt_obj = parse_date_tab(dk)
        if not dt_obj: continue
        date_objects[dk] = dt_obj; date_names.append(dk)
        for tab_row in tab_rows:
            cn = str(tab_row[0]).strip(); asn = str(tab_row[1]).strip(); asid = clean_id(tab_row[2])
            if not cn or cn in ["캠페인 이름", "전체", "합계", "Total"]: continue
            spend = _to_num(tab_row[3]); unique_clicks = _to_num(tab_row[9]); cpm = _to_num(tab_row[6])
            mpc = _to_num(tab_row[14]); revenue = _to_num(tab_row[15])
            profit = revenue - spend; roas = (revenue / spend * 100) if spend > 0 else 0
            cvr = (mpc / unique_clicks * 100) if unique_clicks > 0 and mpc > 0 else 0
            if asid:
                if not ad_sets[asid]['adset_id']:
                    ad_sets[asid] = {'campaign_name': cn, 'adset_name': asn, 'adset_id': asid, 'dates': {}}
                ad_sets[asid]['dates'][dk] = {'profit': profit, 'revenue': revenue, 'spend': spend, 'cpm': cpm, 'cvr': cvr, 'unique_clicks': unique_clicks, 'mpc': mpc}
            p = extract_product(asn, cn)
            budget_by_date[dk][p]['spend'] += spend; budget_by_date[dk][p]['revenue'] += revenue
            mrd = [dk] + list(tab_row)
            while len(mrd) < len(master_headers_local): mrd.append("")
            master_raw_data.append({'date': dk, 'date_obj': dt_obj, 'spend': spend, 'row_data': mrd[:len(master_headers_local)]})
    date_names = sorted(date_names, key=lambda x: date_objects.get(x, datetime.min))
    cached_ws = with_retry(sh.worksheets)
    print(f"  ✅ preloaded: {len(date_names)}일 | 광고세트: {len(ad_sets)}개 | 마스터행: {len(master_raw_data)}개")

# 마스터탭 쓰기
print("\n  📝 마스터탭 쓰기...")
ws_m, _is_new_m = get_or_create_ws(sh, "마스터탭", rows=max(2000, len(master_raw_data) + 100), cols=len(master_headers) + 5); time.sleep(2)
if not _is_new_m:
    try: with_retry(ws_m.clear); time.sleep(1)
    except: pass
move_ws_to_front(sh, ws_m)
master_raw_data.sort(key=lambda x: (x['date_obj'], -x['spend']), reverse=True)
for item in master_raw_data:
    while len(item['row_data']) < len(master_headers): item['row_data'].append("")
    item['row_data'] = item['row_data'][:len(master_headers)]
mr_all_raw = [i['row_data'] for i in master_raw_data]
mr_safe = _make_rows_sheet_safe(mr_all_raw, asid_col=3)
for row in mr_safe:
    if len(row) > 0 and row[0] and str(row[0]).strip():
        dk_str = str(row[0]).strip()
        if not dk_str.startswith("'") and '/' in dk_str:
            row[0] = "'" + dk_str
mr_all = [master_headers] + mr_safe
for i in range(0, len(mr_all), 5000): with_retry(ws_m.update, values=mr_all[i:i+5000], range_name=f"A{i+1}", value_input_option="USER_ENTERED"); time.sleep(2)
try:
    with_retry(ws_m.format, 'A1:T1', {'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}, 'textFormat': {'bold': True}})
    with_retry(sh.batch_update, body={"requests": [{"updateSheetProperties": {"properties": {"sheetId": ws_m.id, "gridProperties": {"frozenRowCount": 1}}, "fields": "gridProperties.frozenRowCount"}}]})
except: pass
print(f"  ✅ 마스터탭 ({len(master_raw_data)}행)")

# ★ v30h: preloaded 모드에서는 마스터탭만 생성하고 종료
if _preloaded_only:
    print("\n" + "=" * 60)
    print("⏩ preloaded 모드: 마스터탭 생성 완료, 차트/주간종합 스킵")
    print("   → 다음 실행에서 마스터탭 기반 전체 차트 복구")
    print("=" * 60)
    reorder_tabs(sh)
    print("\n" + "=" * 60); print("✅ 완료! (preloaded 모드 - 마스터탭 복구)"); print("=" * 60)
    print(f"\n📊 {SPREADSHEET_URL}")
    import sys; sys.exit(0)

# =========================================================
# 8단계: 날짜탭 업데이트 (마스터탭 데이터 기반)
# =========================================================
print("\n"+"="*60); print("8단계: 날짜탭 업데이트 + 새 탭 생성"); print("="*60)
# ★ v30g: 최근 TAB_UPDATE_DAYS일만 업데이트 (이전 날짜는 확정 → 스킵)
_all_existing_keys = sorted(existing_refresh_tabs.keys())
if FULL_REFRESH:
    _update_keys = _all_existing_keys
else:
    _update_keys = _all_existing_keys[-TAB_UPDATE_DAYS:] if len(_all_existing_keys) > TAB_UPDATE_DAYS else _all_existing_keys
_skip_count = len(_all_existing_keys) - len(_update_keys)
print(f"\n--- 8-A: 기존 날짜탭 업데이트 ({len(_update_keys)}개 갱신, {_skip_count}개 스킵) ---")
for dk in _update_keys:
    ws_ex = existing_refresh_tabs[dk]; mp_data = date_mp_by_adsetid.get(dk, {}); new_rows_for_date = date_tab_rows.get(dk, [])
    print(f"\n  📝 {dk} 업데이트 중...")
    try:
        ws_ex = refresh_ws(sh, ws_ex)
        # ★ v30g: get_all_values(23컬럼) → A:C만 읽기 (3컬럼, 행 위치 파악용)
        col_ac = with_retry(ws_ex.get, 'A:C'); time.sleep(0.5)
        if not col_ac or len(col_ac) < 2: continue
        structure = "new"  # 최근 탭은 항상 new 구조
        actual_asid_col = 2  # column C (0-based)
        adset_id_row_map = {}; last_data_sheet_row = 1
        for i, row in enumerate(col_ac[1:], start=2):
            if not row: continue
            cn = str(row[0]).strip() if len(row) > 0 else ""; asid = clean_id(row[actual_asid_col]) if len(row) > actual_asid_col else ""
            if cn in ["캠페인 이름", "전체", "합계", "Total"]: continue
            if not cn and not asid: continue
            if asid: adset_id_row_map[asid] = i - 1
            last_data_sheet_row = i
        data_end_idx = last_data_sheet_row; batch_updates = []; updated_count = 0; new_row_by_asid = {}
        for tab_row in new_rows_for_date:
            asid_check = clean_id(tab_row[2]) if len(tab_row) > 2 else ""
            if asid_check: new_row_by_asid[asid_check] = tab_row
        _fuzzy_to_full = {}
        for full_asid in new_row_by_asid:
            fk = _asid_match_key(full_asid)
            if fk: _fuzzy_to_full[fk] = full_asid
        _fuzzy_mp = {}
        for mp_asid in mp_data:
            fk = _asid_match_key(mp_asid)
            if fk: _fuzzy_mp[fk] = mp_asid
        # ★ v30h: D~S 전체 덮어쓰기 (Meta D~N + Mixpanel O~P + 수식 Q~S)
        actual_spend_col = get_col_index(structure, 3)   # D열
        actual_cvr_col = get_col_index(structure, 18)     # S열
        spend_col_letter = get_col_letter(actual_spend_col)
        cvr_col_letter = get_col_letter(actual_cvr_col)
        for sheet_asid, row_idx in adset_id_row_map.items():
            row_num = row_idx + 1
            matched_asid = sheet_asid if sheet_asid in new_row_by_asid else _fuzzy_to_full.get(_asid_match_key(sheet_asid))
            if not matched_asid:
                # Meta 매칭 안 되는 행 → Mixpanel만이라도 업데이트
                mp_asid = sheet_asid if sheet_asid in mp_data else _fuzzy_mp.get(_asid_match_key(sheet_asid))
                if mp_asid and mp_asid in mp_data:
                    actual_mp_col = get_col_index(structure, 14)
                    mp_col_letter = get_col_letter(actual_mp_col)
                    mpc = mp_data[mp_asid]['mpc']; mpv = mp_data[mp_asid]['mpv']; revenue = float(mpv)
                    batch_updates.append({'range': f'{mp_col_letter}{row_num}:{cvr_col_letter}{row_num}',
                        'values': [[int(mpc) if mpc > 0 else "", round(revenue) if revenue > 0 else "",
                            _make_profit_formula(row_num, structure),
                            _make_roas_formula(row_num, structure),
                            _make_cvr_formula(row_num, structure)]]})
                continue
            new_tab_row = new_row_by_asid[matched_asid]
            # D~N (Meta: 지출~결과) + O~P (Mixpanel: 결과MP, 매출) + Q~S (수식)
            row_data = list(new_tab_row[3:16]) + [
                _make_profit_formula(row_num, structure),
                _make_roas_formula(row_num, structure),
                _make_cvr_formula(row_num, structure)
            ]
            batch_updates.append({'range': f'{spend_col_letter}{row_num}:{cvr_col_letter}{row_num}',
                'values': [row_data]})
            updated_count += 1
        # ★ v30g: 예산도 같은 batch에 합치기 (별도 API 호출 제거)
        actual_budget_col = get_col_index(structure, 19); budget_col_letter = get_col_letter(actual_budget_col)
        for sheet_asid, row_idx in adset_id_row_map.items():
            budget_asid = sheet_asid if sheet_asid in adset_budget_map else _fuzzy_to_full.get(_asid_match_key(sheet_asid), sheet_asid)
            if budget_asid in adset_budget_map and adset_budget_map[budget_asid] > 0:
                acc = ADSET_TO_ACCOUNT.get(budget_asid, ''); fx = get_fx_for_account(acc, dk); div = get_budget_divisor(acc)
                batch_updates.append({'range': f'{budget_col_letter}{row_idx+1}', 'values': [[round(adset_budget_map[budget_asid] / div * fx)]]})
        if batch_updates:
            for i in range(0, len(batch_updates), 200): with_retry(ws_ex.batch_update, batch_updates[i:i+200], value_input_option="USER_ENTERED"); time.sleep(0.5)
            print(f"    ✅ {updated_count}개 업데이트")
        # 신규 행 추가
        existing_asids = set(adset_id_row_map.keys())
        existing_fuzzy_keys = set(_asid_match_key(a) for a in existing_asids)
        new_rows_to_add = []
        for tab_row in new_rows_for_date:
            asid_check = clean_id(tab_row[2]) if len(tab_row) > 2 else ""
            if asid_check and asid_check not in existing_asids and _asid_match_key(asid_check) not in existing_fuzzy_keys:
                if structure == "old":
                    old_row = list(tab_row[:3]) + ["", ""] + list(tab_row[3:])
                    old_row[2] = _prefix_asid_for_sheet(old_row[2])
                    while len(old_row) < 25: old_row.append(""); new_rows_to_add.append(old_row[:25])
                else:
                    safe_row = list(tab_row)
                    safe_row[2] = _prefix_asid_for_sheet(safe_row[2])
                    new_rows_to_add.append(safe_row)
        if new_rows_to_add:
            actual_profit_col = get_col_index(structure, 16)
            new_rows_to_add.sort(key=lambda r: _to_num(r[actual_profit_col]) if len(r) > actual_profit_col else 0, reverse=True)
            # ★ v30f: 신규 행에도 Q/R/S 수식 적용
            for ri, row in enumerate(new_rows_to_add):
                sheet_row = data_end_idx + 1 + ri  # 실제 시트 행 번호
                pc = get_col_index(structure, 16)
                if len(row) > pc + 2:
                    row[pc] = _make_profit_formula(sheet_row, structure)
                    row[pc + 1] = _make_roas_formula(sheet_row, structure)
                    row[pc + 2] = _make_cvr_formula(sheet_row, structure)
            with_retry(ws_ex.update, values=new_rows_to_add, range_name=f"A{data_end_idx+1}", value_input_option="USER_ENTERED")
            print(f"    ✅ {len(new_rows_to_add)}개 신규 행 추가"); time.sleep(1); data_end_idx += len(new_rows_to_add)
        # ★ v30h: 요약표 데이터만 쓰기 (포맷 스킵 → API 1회 추가)
        api_rows = date_tab_rows.get(dk, [])
        _has_mp = any((_to_num(r[14]) > 0 or _to_num(r[15]) > 0) for r in api_rows) if api_rows else False
        if _has_mp and api_rows:
            summary_rows, num_products = generate_date_tab_summary(api_rows, structure="new")
            breakdown_rows, product_meta = generate_product_breakdown(api_rows, structure="new")
            combined_extra = summary_rows + breakdown_rows
            summary_start = data_end_idx + 1
            with_retry(ws_ex.update, values=combined_extra, range_name=f"A{summary_start}", value_input_option="USER_ENTERED"); time.sleep(0.5)
            print(f"    ✅ 요약표 갱신 ({num_products}개 상품, 포맷 스킵)")
    except Exception as e: print(f"    ⚠️ {dk} 오류: {e}")

# 8-B: 새 날짜탭 생성
print(f"\n--- 8-B: 새 날짜탭 생성 ({len(new_refresh_dates)}개) ---")
for dk in sorted(new_refresh_dates):
    rows = date_tab_rows.get(dk, [])
    if not rows: continue
    rows.sort(key=lambda r: _to_num(r[16]) if len(r) > 16 else 0, reverse=True)
    print(f"  📅 {dk} ({len(rows)}개) - 새로 생성")
    summary_rows, num_products = generate_date_tab_summary(rows, structure="new")
    breakdown_rows, product_meta = generate_product_breakdown(rows, structure="new")
    try:
        total_rows = len(rows) + len(summary_rows) + len(breakdown_rows) + 5
        NUM_COLS = len(DATE_TAB_HEADERS)
        ws_d = safe_add_worksheet(sh, dk, rows=total_rows, cols=NUM_COLS + 2); time.sleep(1)
        sid_d = ws_d.id; data_end = len(rows) + 1
        safe_rows = _make_rows_sheet_safe(rows)
        # ★ v30f: Q(이익), R(ROAS), S(CVR) 수식으로 교체
        for ri, row in enumerate(safe_rows):
            sheet_row = ri + 2  # header가 1행이므로 데이터는 2행부터
            if len(row) > 18:
                row[16] = _make_profit_formula(sheet_row, "new")
                row[17] = _make_roas_formula(sheet_row, "new")
                row[18] = _make_cvr_formula(sheet_row, "new")
        safe_breakdown = _make_rows_sheet_safe(breakdown_rows)
        all_data = [DATE_TAB_HEADERS] + safe_rows + summary_rows + safe_breakdown
        with_retry(ws_d.update, values=all_data, range_name="A1", value_input_option="USER_ENTERED")
        fmt_all = []
        fmt_all.append(create_format_request(sid_d, 0, 1, 0, 3, {"textFormat": {"bold": True}, "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE"}))
        fmt_all.append(create_format_request(sid_d, 0, 1, 3, 14, {"backgroundColor": {"red": 0.937, "green": 0.937, "blue": 0.937}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE"}))
        fmt_all.append(create_format_request(sid_d, 0, 1, 14, 19, {"backgroundColor": {"red": 0.6, "green": 0.6, "blue": 0.6}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP", "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}))
        fmt_all.append(create_format_request(sid_d, 0, 1, 19, 20, {"backgroundColor": {"red": 0.851, "green": 0.824, "blue": 0.914}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP", "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}))
        fmt_all.append(create_format_request(sid_d, 0, 1, 20, 21, {"backgroundColor": {"red": 0.706, "green": 0.655, "blue": 0.839}, "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}, "wrapStrategy": "WRAP", "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}))
        fmt_all.append(create_format_request(sid_d, 0, 1, 21, 22, {"backgroundColor": {"red": 0.6, "green": 0, "blue": 1}, "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}, "wrapStrategy": "WRAP", "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}))
        fmt_all.append(create_format_request(sid_d, 0, 1, 22, 23, {"backgroundColor": {"red": 1, "green": 0.6, "blue": 0}, "textFormat": {"bold": True}, "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE"}))
        for sc, ec, p in [(14,15,"#,##0"),(15,16,"#,##0"),(16,17,"#,##0"),(17,18,"#,##0.0"),(18,19,"#,##0.00"),(19,20,"#,##0")]:
            fmt_all.append(create_number_format_request(sid_d, 1, data_end, sc, ec, "NUMBER", p))
        fmt_all.append(create_number_format_request(sid_d, 0, 1, 20, 21, "NUMBER", "0%"))
        fmt_all.append(create_number_format_request(sid_d, 1, data_end, 20, 21, "NUMBER", "0%"))
        for ci, w in [(0, 210), (1, 207), (2, 130)]: fmt_all.append({"updateDimensionProperties": {"range": {"sheetId": sid_d, "dimension": "COLUMNS", "startIndex": ci, "endIndex": ci + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}})
        fmt_all.append({"setBasicFilter": {"filter": {"range": {"sheetId": sid_d, "startRowIndex": 0, "endRowIndex": data_end, "startColumnIndex": 0, "endColumnIndex": NUM_COLS}}}})
        fmt_all.append({"updateSheetProperties": {"properties": {"sheetId": sid_d, "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 3}}, "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}})
        profit_cond = [("NUMBER_GREATER","0",{"red":1,"green":0.949,"blue":0.8},None),("NUMBER_GREATER","50000",{"red":1,"green":0.898,"blue":0.6},None),("NUMBER_GREATER","100000",{"red":0.576,"green":0.769,"blue":0.49},None),("NUMBER_GREATER","200000",{"red":0,"green":1,"blue":0},None),("NUMBER_GREATER","300000",{"red":0,"green":1,"blue":1},None),("NUMBER_LESS","-10000",{"red":0.957,"green":0.8,"blue":0.8},None),("NUMBER_LESS","-50000",{"red":0.878,"green":0.4,"blue":0.4},{"red":0,"green":0,"blue":0})]
        for ctype, val, bg, fc in profit_cond:
            rule_fmt = {"backgroundColor": bg}
            if fc: rule_fmt["textFormat"] = {"foregroundColor": fc}
            fmt_all.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sid_d, "startRowIndex": 1, "endRowIndex": data_end, "startColumnIndex": 16, "endColumnIndex": 17}], "booleanRule": {"condition": {"type": ctype, "values": [{"userEnteredValue": val}]}, "format": rule_fmt}}, "index": 0}})
        roas_cond = [("NUMBER_LESS","100",{"red":0.878,"green":0.4,"blue":0.4}),("NUMBER_GREATER_THAN_EQ","100",{"red":1,"green":0.851,"blue":0.4}),("NUMBER_GREATER_THAN_EQ","200",{"red":0.576,"green":0.769,"blue":0.49}),("NUMBER_GREATER_THAN_EQ","300",{"red":0,"green":1,"blue":1})]
        for ctype, val, bg in roas_cond:
            fmt_all.append({"addConditionalFormatRule": {"rule": {"ranges": [{"sheetId": sid_d, "startRowIndex": 1, "endRowIndex": data_end, "startColumnIndex": 17, "endColumnIndex": 18}], "booleanRule": {"condition": {"type": ctype, "values": [{"userEnteredValue": val}]}, "format": {"backgroundColor": bg}}}, "index": 0}})
        try: with_retry(sh.batch_update, body={"requests": fmt_all}); time.sleep(2)
        except: pass
        try:
            fmt_reqs = format_date_tab_summary(sh, ws_d, len(rows) + 2, len(summary_rows), num_products=num_products)
            if fmt_reqs:
                for i in range(0, len(fmt_reqs), 200): with_retry(sh.batch_update, body={"requests": fmt_reqs[i:i+200]}); time.sleep(0.5)
        except: pass
        if product_meta:
            try:
                bd_fmt = format_product_breakdown(sh, ws_d, len(rows) + 1 + len(summary_rows) + 1, product_meta)
                if bd_fmt:
                    for i in range(0, len(bd_fmt), 200): with_retry(sh.batch_update, body={"requests": bd_fmt[i:i+200]}); time.sleep(0.5)
            except: pass
        time.sleep(1)
    except Exception as e: print(f"  ⚠️ {dk} 생성 오류: {e}")
print(f"\n✅ 기존 {len(existing_refresh_tabs)}개 + 새 {len(new_refresh_dates)}개 완료"); time.sleep(2)

# 8.5: 탭 순서
print("\n"+"="*60); print("8.5단계: 탭 순서 정리"); print("="*60)
reorder_tabs(sh)

failed_tabs = diagnose_chart_coverage(sh, date_names, ad_sets, ANALYSIS_TABS_SET, all_ws=cached_ws)

# 차트 데이터 준비
all_products_in_budget = set()
for dk in date_names:
    for p in budget_by_date[dk]:
        if budget_by_date[dk][p]['spend'] > 0 or budget_by_date[dk][p]['revenue'] > 0: all_products_in_budget.add(p)
product_order = sorted(list(all_products_in_budget), key=lambda p: sum(budget_by_date[dk][p]['spend'] for dk in date_names), reverse=True)
print(f"\n📦 제품 순서 (전체): {product_order}")
chart_dn = list(reversed(date_names)); chart_sd = chart_dn[:7]

# 주간 집계
print("\n주간 집계")
week_groups = defaultdict(list); week_display_names = {}
for t in date_names: do = date_objects[t]; wk = get_week_range_short(do); wd = get_week_range(do); week_groups[wk].append(t); week_display_names[wk] = wd
week_ranges = []
for wr in week_groups: sd = week_groups[wr][0]; sdo = date_objects[sd]; m = get_week_monday(sdo); week_ranges.append((wr, m))
week_ranges.sort(key=lambda x: x[1]); week_keys = [wr[0] for wr in week_ranges]
print(f"✅ 주차: {len(week_keys)}개 | 날짜: {len(date_names)}개")
adset_weekly = defaultdict(lambda: {'campaign_name': '', 'adset_name': '', 'adset_id': '', 'weeks': {}})
for asid, d in ad_sets.items():
    adset_weekly[asid]['campaign_name'] = d['campaign_name']; adset_weekly[asid]['adset_name'] = d['adset_name']; adset_weekly[asid]['adset_id'] = d['adset_id']
    for wk in week_keys:
        wp, wr_v, ws_v = 0, 0, 0; wvs, wvc = 0, 0
        for dn in week_groups[wk]:
            if dn in d['dates']:
                wp += d['dates'][dn]['profit']; wr_v += d['dates'][dn]['revenue']; ws_v += d['dates'][dn]['spend']
                vv = d['dates'][dn].get('cvr', 0)
                if vv > 0: wvs += vv; wvc += 1
        if ws_v > 0 or wr_v > 0: adset_weekly[asid]['weeks'][wk] = {'profit': wp, 'revenue': wr_v, 'spend': ws_v, 'cvr': (wvs / wvc) if wvc > 0 else 0}
sorted_list = []
for asid, d in ad_sets.items():
    sorted_list.append({'campaign_name': d['campaign_name'], 'adset_name': d['adset_name'], 'adset_id': d['adset_id'], 'data': d})

def _multi_spend_key(item):
    return tuple(item['data']['dates'].get(d, {}).get('spend', 0) for d in chart_dn[1:])
sorted_list.sort(key=_multi_spend_key, reverse=True); chart_wk = list(reversed(week_keys))

# 10단계: 추이차트
print("\n10단계: 추이차트")
ws_t, _is_new_t = get_or_create_ws(sh, "추이차트", rows=1000, cols=len(chart_dn) + 10); time.sleep(2)
move_ws_to_front(sh, ws_t)
dhw = []; sci = []
for i, n in enumerate(chart_dn): do = date_objects[n]; wd = WEEKDAY_NAMES[do.weekday()]; dhw.append(f"{n}({wd})"); (sci.append(4 + i) if do.weekday() == 6 else None)
hdr_t = ['캠페인 이름', '광고 세트 이름', '광고 세트 ID', '7일 평균'] + dhw
sr = ["종합", "", ""]; tp, tr, ts = 0, 0, 0; tvs, tvc = 0, 0
for d in chart_sd:
    for it in sorted_list:
        if d in it['data']['dates']:
            tp += it['data']['dates'][d]['profit']; tr += it['data']['dates'][d]['revenue']; ts += it['data']['dates'][d]['spend']
            vv = it['data']['dates'][d].get('cvr', 0)
            if vv > 0: tvs += vv; tvc += 1
sr.append(cell_text(tp, tr, ts, 0, (tvs / tvc) if tvc > 0 else 0))
for d in chart_dn:
    dp, dr, ds_v = 0, 0, 0; dvs, dvc = 0, 0
    for it in sorted_list:
        if d in it['data']['dates']:
            dp += it['data']['dates'][d]['profit']; dr += it['data']['dates'][d]['revenue']; ds_v += it['data']['dates'][d]['spend']
            vv = it['data']['dates'][d].get('cvr', 0)
            if vv > 0: dvs += vv; dvc += 1
    sr.append(cell_text(dp, dr, ds_v, 0, (dvs / dvc) if dvc > 0 else 0))
rt = []
for it in sorted_list:
    r = [it['campaign_name'], it['adset_name'], it['adset_id']]; tp, tr, ts = 0, 0, 0; tvs, tvc = 0, 0
    for d in chart_sd:
        if d in it['data']['dates']:
            tp += it['data']['dates'][d]['profit']; tr += it['data']['dates'][d]['revenue']; ts += it['data']['dates'][d]['spend']
            vv = it['data']['dates'][d].get('cvr', 0)
            if vv > 0: tvs += vv; tvc += 1
    r.append(cell_text(tp, tr, ts, 0, (tvs / tvc) if tvc > 0 else 0))
    for d in chart_dn:
        if d in it['data']['dates']:
            dt = it['data']['dates'][d]; r.append(cell_text(dt['profit'], dt['revenue'], dt['spend'], 0, dt.get('cvr', 0)))
        else: r.append('')
    rt.append(r)
with_retry(ws_t.update, values=[hdr_t] + [sr] + rt, range_name="A1", value_input_option="USER_ENTERED")
print(f"✅ 추이차트 ({len(chart_dn)}일)"); time.sleep(2)
ws_t = refresh_ws(sh, ws_t)
if _is_new_t:
    apply_trend_chart_formatting(sh, ws_t, hdr_t, len(rt), sunday_col_indices=sci)
else:
    # ★ v30f: 기존 탭 → 텍스트 색상만 최근 컬럼 적용
    apply_trend_chart_formatting(sh, ws_t, hdr_t, len(rt), sunday_col_indices=sci, format_col_end=REFRESH_FORMAT_COL_END, text_color_only=True)
try: ws_t = refresh_ws(sh, ws_t); apply_c2_label_formatting(sh, ws_t)
except: pass
# ★ v30e: 30→15초
print(f"⏳ {CHART_STEP_SLEEP}초 대기..."); time.sleep(CHART_STEP_SLEEP)

# 10.5단계: 추이차트_상품별
print("\n10.5단계: 추이차트_상품별")
_product_groups_chart = defaultdict(list)
for it in sorted_list: p = extract_product(it['adset_name'], it['campaign_name']); _product_groups_chart[p].append(it)

# ★ v30g: 전날 매출 기준 정렬 (yesterday = chart_dn[1])
_yesterday = chart_dn[1] if len(chart_dn) >= 2 else (chart_dn[0] if chart_dn else None)
_product_yesterday_revenue = {}
for p, items in _product_groups_chart.items():
    rev = 0
    if _yesterday:
        for it in items:
            rev += it['data']['dates'].get(_yesterday, {}).get('revenue', 0)
    _product_yesterday_revenue[p] = rev

_sorted_products_chart = sorted(_product_groups_chart.keys(), key=lambda p: _product_yesterday_revenue.get(p, 0), reverse=True)
_second_recent = _yesterday
for p in _sorted_products_chart:
    _product_groups_chart[p].sort(key=lambda it: it['data']['dates'].get(_second_recent, {}).get('spend', 0) if _second_recent else 0, reverse=True)
hdr_tp = ['캠페인 이름', '광고 세트 이름', '광고 세트 ID', '7일 평균'] + dhw

# ★ v30g: 상단에 상품별 종합 성과 요약
_summary_sorted_products = list(_sorted_products_chart)  # 같은 정렬 기준 사용

# 종합 성과 행 생성
rows_tp = []
_summary_header_idx = len(rows_tp)
rows_tp.append(["📊 상품별 종합 성과"] + [""] * (len(hdr_tp) - 1))
for p in _summary_sorted_products:
    items = _product_groups_chart[p]
    psr = [f"{p}", f"({len(items)}개 세트)", ""]; _tp, _tr, _ts = 0, 0, 0; _tvs, _tvc = 0, 0
    for d in chart_sd:
        for it in items:
            if d in it['data']['dates']:
                _tp += it['data']['dates'][d]['profit']; _tr += it['data']['dates'][d]['revenue']; _ts += it['data']['dates'][d]['spend']
                vv = it['data']['dates'][d].get('cvr', 0)
                if vv > 0: _tvs += vv; _tvc += 1
    psr.append(cell_text(_tp, _tr, _ts, 0, (_tvs / _tvc) if _tvc > 0 else 0))
    for d in chart_dn:
        dp, dr, ds_v = 0, 0, 0; dvs, dvc = 0, 0
        for it in items:
            if d in it['data']['dates']:
                dp += it['data']['dates'][d]['profit']; dr += it['data']['dates'][d]['revenue']; ds_v += it['data']['dates'][d]['spend']
                vv = it['data']['dates'][d].get('cvr', 0)
                if vv > 0: dvs += vv; dvc += 1
        psr.append(cell_text(dp, dr, ds_v, 0, (dvs / dvc) if dvc > 0 else 0))
    rows_tp.append(psr)
_summary_end_idx = len(rows_tp)
rows_tp.append([""] * len(hdr_tp))  # 구분 빈 행

# 상세 상품별 세트 목록 (기존 구조, 전날 매출순 정렬)
_product_header_indices = []
for p_idx, p in enumerate(_sorted_products_chart):
    items = _product_groups_chart[p]; p_yd_rev = _product_yesterday_revenue.get(p, 0)
    _product_header_indices.append(len(rows_tp))
    rows_tp.append([f"📦 {p}  |  전날 매출 {money(p_yd_rev)}  |  {len(items)}개 세트"] + [""] * (len(hdr_tp) - 1))
    psr = [f"{p} 종합", "", ""]; _tp, _tr, _ts = 0, 0, 0; _tvs, _tvc = 0, 0
    for d in chart_sd:
        for it in items:
            if d in it['data']['dates']:
                _tp += it['data']['dates'][d]['profit']; _tr += it['data']['dates'][d]['revenue']; _ts += it['data']['dates'][d]['spend']
                vv = it['data']['dates'][d].get('cvr', 0)
                if vv > 0: _tvs += vv; _tvc += 1
    psr.append(cell_text(_tp, _tr, _ts, 0, (_tvs / _tvc) if _tvc > 0 else 0))
    for d in chart_dn:
        dp, dr, ds_v = 0, 0, 0; dvs, dvc = 0, 0
        for it in items:
            if d in it['data']['dates']:
                dp += it['data']['dates'][d]['profit']; dr += it['data']['dates'][d]['revenue']; ds_v += it['data']['dates'][d]['spend']
                vv = it['data']['dates'][d].get('cvr', 0)
                if vv > 0: dvs += vv; dvc += 1
        psr.append(cell_text(dp, dr, ds_v, 0, (dvs / dvc) if dvc > 0 else 0))
    rows_tp.append(psr)
    for it in items:
        r = [it['campaign_name'], it['adset_name'], it['adset_id']]; _tp2, _tr2, _ts2 = 0, 0, 0; _tvs2, _tvc2 = 0, 0
        for d in chart_sd:
            if d in it['data']['dates']:
                _tp2 += it['data']['dates'][d]['profit']; _tr2 += it['data']['dates'][d]['revenue']; _ts2 += it['data']['dates'][d]['spend']
                vv = it['data']['dates'][d].get('cvr', 0)
                if vv > 0: _tvs2 += vv; _tvc2 += 1
        r.append(cell_text(_tp2, _tr2, _ts2, 0, (_tvs2 / _tvc2) if _tvc2 > 0 else 0))
        for d in chart_dn:
            if d in it['data']['dates']:
                dt = it['data']['dates'][d]; r.append(cell_text(dt['profit'], dt['revenue'], dt['spend'], 0, dt.get('cvr', 0)))
            else: r.append('')
        rows_tp.append(r)
    rows_tp.append([""] * len(hdr_tp))
ws_tp, _is_new_tp = get_or_create_ws(sh, "추이차트_상품별", rows=len(rows_tp) + 100, cols=len(chart_dn) + 10); time.sleep(2)
move_ws_to_front(sh, ws_tp)
# ★ v30f: 기존 탭 재사용 시 이전 머지 해제 (상품 헤더 위치 변동 대응)
if not _is_new_tp:
    try:
        with_retry(sh.batch_update, body={"requests": [
            {"unmergeCells": {"range": {"sheetId": ws_tp.id, "startRowIndex": 0, "endRowIndex": ws_tp.row_count, "startColumnIndex": 0, "endColumnIndex": min(len(hdr_tp), 26)}}}
        ]}); time.sleep(1)
    except: pass
with_retry(ws_tp.update, values=[hdr_tp] + rows_tp, range_name="A1", value_input_option="USER_ENTERED")
print(f"✅ 추이차트_상품별 ({len(_sorted_products_chart)}개 상품)"); time.sleep(2)
ws_tp = refresh_ws(sh, ws_tp)
if _is_new_tp:
    apply_trend_chart_formatting(sh, ws_tp, hdr_tp, len(rows_tp), sunday_col_indices=sci)
else:
    apply_trend_chart_formatting(sh, ws_tp, hdr_tp, len(rows_tp), sunday_col_indices=sci, format_col_end=REFRESH_FORMAT_COL_END, text_color_only=True)
try: ws_tp = refresh_ws(sh, ws_tp); apply_c2_label_formatting(sh, ws_tp)
except: pass
try:
    tp_fmt_reqs = []; sid_tp = ws_tp.id
    # ★ v30g: 상단 종합 성과 헤더 포맷
    tp_fmt_reqs.append(create_format_request(sid_tp, _summary_header_idx + 1, _summary_header_idx + 2, 0, len(hdr_tp),
        get_cell_format({"red": 0.15, "green": 0.15, "blue": 0.3}, COLORS["white"], bold=True)))
    tp_fmt_reqs.append({"mergeCells": {"range": {"sheetId": sid_tp, "startRowIndex": _summary_header_idx + 1, "endRowIndex": _summary_header_idx + 2,
        "startColumnIndex": 0, "endColumnIndex": min(len(hdr_tp), 26)}, "mergeType": "MERGE_ALL"}})
    # 종합 성과 행 볼드
    tp_fmt_reqs.append(create_format_request(sid_tp, _summary_header_idx + 2, _summary_end_idx + 1, 0, 1,
        get_cell_format(bold=True, ha="LEFT")))
    # 상세 상품별 헤더 포맷
    PRODUCT_CHART_BG = [{"red":0.22,"green":0.42,"blue":0.65},{"red":0.33,"green":0.57,"blue":0.33},{"red":0.53,"green":0.30,"blue":0.58},{"red":0.68,"green":0.42,"blue":0.18},{"red":0.58,"green":0.22,"blue":0.22},{"red":0.20,"green":0.52,"blue":0.52},{"red":0.48,"green":0.48,"blue":0.25},{"red":0.38,"green":0.25,"blue":0.48},{"red":0.30,"green":0.55,"blue":0.45},{"red":0.60,"green":0.35,"blue":0.40},{"red":0.35,"green":0.35,"blue":0.55}]
    for p_idx, row_idx in enumerate(_product_header_indices):
        sheet_row = row_idx + 1; color = PRODUCT_CHART_BG[p_idx % len(PRODUCT_CHART_BG)]
        tp_fmt_reqs.append(create_format_request(sid_tp, sheet_row, sheet_row + 1, 0, len(hdr_tp), get_cell_format(color, COLORS["white"], bold=True)))
        tp_fmt_reqs.append({"mergeCells": {"range": {"sheetId": sid_tp, "startRowIndex": sheet_row, "endRowIndex": sheet_row + 1, "startColumnIndex": 0, "endColumnIndex": min(len(hdr_tp), 26)}, "mergeType": "MERGE_ALL"}})
    if tp_fmt_reqs:
        for i in range(0, len(tp_fmt_reqs), 50): with_retry(sh.batch_update, body={"requests": tp_fmt_reqs[i:i+50]}); time.sleep(1)
except: pass
# ★ v30e: 30→15초
print(f"⏳ {CHART_STEP_SLEEP}초 대기..."); time.sleep(CHART_STEP_SLEEP)

# 11단계: 추이차트(주간)
print("\n11단계: 추이차트(주간)")
ws_tw, _is_new_tw = get_or_create_ws(sh, "추이차트(주간)", rows=1000, cols=len(chart_wk) + 10); time.sleep(2)
move_ws_to_front(sh, ws_tw)
wdl = [week_display_names[wk] for wk in chart_wk]; hdr_w = ['캠페인 이름', '광고 세트 이름', '광고 세트 ID', '전체 평균'] + wdl
srw = ["종합", "", ""]; tap, tar, tas = 0, 0, 0; tavs, tavc = 0, 0
for wk in chart_wk:
    for it in sorted_list:
        wd = adset_weekly[it['adset_id']]['weeks'].get(wk, {})
        if wd: tap += wd['profit']; tar += wd['revenue']; tas += wd['spend']; vv = wd.get('cvr', 0); (tavs := tavs + vv, tavc := tavc + 1) if vv > 0 else None
srw.append(cell_text(tap, tar, tas, 0, (tavs / tavc) if tavc > 0 else 0))
for wk in chart_wk:
    wp, wr_v, ws_v = 0, 0, 0; wvs, wvc = 0, 0
    for it in sorted_list:
        wd = adset_weekly[it['adset_id']]['weeks'].get(wk, {})
        if wd: wp += wd['profit']; wr_v += wd['revenue']; ws_v += wd['spend']; vv = wd.get('cvr', 0); (wvs := wvs + vv, wvc := wvc + 1) if vv > 0 else None
    srw.append(cell_text(wp, wr_v, ws_v, 0, (wvs / wvc) if wvc > 0 else 0))
rtw = []
for it in sorted_list:
    r = [it['campaign_name'], it['adset_name'], it['adset_id']]; tp, tr, ts = 0, 0, 0; tvs, tvc = 0, 0
    for wk in chart_wk:
        wd = adset_weekly[it['adset_id']]['weeks'].get(wk, {})
        if wd: tp += wd['profit']; tr += wd['revenue']; ts += wd['spend']; vv = wd.get('cvr', 0); (tvs := tvs + vv, tvc := tvc + 1) if vv > 0 else None
    r.append(cell_text(tp, tr, ts, 0, (tvs / tvc) if tvc > 0 else 0))
    for wk in chart_wk:
        wd = adset_weekly[it['adset_id']]['weeks'].get(wk, {})
        r.append(cell_text(wd['profit'], wd['revenue'], wd['spend'], 0, wd.get('cvr', 0)) if wd else '')
    rtw.append(r)
with_retry(ws_tw.update, values=[hdr_w] + [srw] + rtw, range_name="A1", value_input_option="USER_ENTERED")
print("✅ 추이차트(주간)"); time.sleep(2)
wfce = 3 + 1 + WEEKLY_TREND_REFRESH_WEEKS; ws_tw = refresh_ws(sh, ws_tw)
if _is_new_tw:
    apply_trend_chart_formatting(sh, ws_tw, hdr_w, len(rtw), format_col_end=wfce)
else:
    print("  ⏩ 기존 탭 → 텍스트 색상 스킵 (주간은 컬럼 적음)")
try: ws_tw = refresh_ws(sh, ws_tw); apply_c2_label_formatting(sh, ws_tw)
except: pass
# ★ v30e: 30→15초
print(f"⏳ {CHART_STEP_SLEEP}초 대기..."); time.sleep(CHART_STEP_SLEEP)

# 12단계: 증감액
print("\n12단계: 증감액")
ws_c, _is_new_c = get_or_create_ws(sh, "증감액", rows=1000, cols=len(chart_dn) + 10); time.sleep(2)
move_ws_to_front(sh, ws_c)
hdr_c = ['캠페인 이름', '광고 세트 이름', '광고 세트 ID', '7일 평균'] + chart_dn
src = ["종합", "", ""]; t7r, t7s = 0, 0; t7vs, t7vc = 0, 0
for d in chart_sd:
    for it in sorted_list:
        if d in it['data']['dates']:
            t7r += it['data']['dates'][d]['revenue']; t7s += it['data']['dates'][d]['spend']
            vv = it['data']['dates'][d].get('cvr', 0)
            if vv > 0: t7vs += vv; t7vc += 1
t7roas = (t7r / t7s * 100) if t7s > 0 else 0
fds = sum(it['data']['dates'].get(chart_sd[-1], {}).get('spend', 0) for it in sorted_list) if len(chart_sd) >= 2 else 0
lds = sum(it['data']['dates'].get(chart_sd[0], {}).get('spend', 0) for it in sorted_list) if len(chart_sd) >= 2 else 0
sdc = ((lds - fds) / fds * 100) if fds > 0 else 0
src.append(cell_text_change(t7roas, sdc, t7s, t7r, (t7vs / t7vc) if t7vc > 0 else 0))
for i, d in enumerate(chart_dn):
    dr, ds_v = 0, 0; dvs, dvc = 0, 0
    for it in sorted_list:
        if d in it['data']['dates']:
            dr += it['data']['dates'][d]['revenue']; ds_v += it['data']['dates'][d]['spend']
            vv = it['data']['dates'][d].get('cvr', 0)
            if vv > 0: dvs += vv; dvc += 1
    d_roas = (dr / ds_v * 100) if ds_v > 0 else 0
    if i < len(chart_dn) - 1: pd_key = chart_dn[i + 1]; ps = sum(it['data']['dates'].get(pd_key, {}).get('spend', 0) for it in sorted_list); chg = ((ds_v - ps) / ps * 100) if ps > 0 else 0
    else: chg = 0
    src.append(cell_text_change(d_roas, chg, ds_v, dr, (dvs / dvc) if dvc > 0 else 0))
rtc = []
for it in sorted_list:
    r = [it['campaign_name'], it['adset_name'], it['adset_id']]; tr_v, ts_v = 0, 0; tcvr, tcvc = 0, 0
    for d in chart_sd:
        if d in it['data']['dates']:
            tr_v += it['data']['dates'][d]['revenue']; ts_v += it['data']['dates'][d]['spend']
            vv = it['data']['dates'][d].get('cvr', 0)
            if vv > 0: tcvr += vv; tcvc += 1
    ar = (tr_v / ts_v * 100) if ts_v > 0 else 0
    fs = it['data']['dates'].get(chart_sd[-1], {}).get('spend', 0) if len(chart_sd) >= 2 else 0
    ls = it['data']['dates'].get(chart_sd[0], {}).get('spend', 0) if len(chart_sd) >= 2 else 0
    cp = ((ls - fs) / fs * 100) if fs > 0 else 0
    r.append(cell_text_change(ar, cp, ts_v, tr_v, (tcvr / tcvc) if tcvc > 0 else 0))
    for i, d in enumerate(chart_dn):
        if d in it['data']['dates']:
            dt = it['data']['dates'][d]; roas = (dt['revenue'] / dt['spend'] * 100) if dt['spend'] > 0 else 0
            if i < len(chart_dn) - 1: pd_key = chart_dn[i + 1]; ps = it['data']['dates'].get(pd_key, {}).get('spend', 0); chg = ((dt['spend'] - ps) / ps * 100) if ps > 0 else 0
            else: chg = 0
            r.append(cell_text_change(roas, chg, dt['spend'], dt.get('revenue', 0), dt.get('cvr', 0)))
        else: r.append('')
    rtc.append(r)
with_retry(ws_c.update, values=[hdr_c] + [src] + rtc, range_name="A1", value_input_option="USER_ENTERED")
print("✅ 증감액"); time.sleep(2)
ws_c = refresh_ws(sh, ws_c)
if _is_new_c:
    apply_trend_chart_formatting(sh, ws_c, hdr_c, len(rtc), is_change_tab=True)
else:
    print("  ⏩ 기존 탭 → 텍스트 색상 스킵")
# ★ v30e: 30→15초
print(f"⏳ {CHART_STEP_SLEEP}초 대기..."); time.sleep(CHART_STEP_SLEEP)

# 13단계: 예산
print("\n13단계: 예산")
bw, _is_new_bw = get_or_create_ws(sh, "예산", rows=1000, cols=len(chart_dn) + 10); time.sleep(2)
move_ws_to_front(sh, bw)
br = [[""] + chart_dn]
br.append(["전체 쓴돈"] + [sum(budget_by_date[d][p]['spend'] for p in product_order) for d in chart_dn])
br.append(["전체 번돈"] + [sum(budget_by_date[d][p]['revenue'] for p in product_order) for d in chart_dn])
br.append(["전체 순이익"] + [sum(budget_by_date[d][p]['revenue'] for p in product_order) - sum(budget_by_date[d][p]['spend'] for p in product_order) for d in chart_dn])
br.append(["ROAS"] + [(sum(budget_by_date[d][p]['revenue'] for p in product_order) / sum(budget_by_date[d][p]['spend'] for p in product_order) * 100) if sum(budget_by_date[d][p]['spend'] for p in product_order) > 0 else 0 for d in chart_dn])
br.append([""] * (len(chart_dn) + 1)); br.append(["쓴돈 - 제품별"] + [""] * len(chart_dn))
for p in product_order: br.append([p] + [budget_by_date[d][p]['spend'] for d in chart_dn])
br.append([""] * (len(chart_dn) + 1)); br.append(["번돈 - 제품별"] + [""] * len(chart_dn))
for p in product_order: br.append([p] + [budget_by_date[d][p]['revenue'] for d in chart_dn])
br.append([""] * (len(chart_dn) + 1)); br.append(["순이익 - 제품별"] + [""] * len(chart_dn))
for p in product_order: br.append([p] + [budget_by_date[d][p]['revenue'] - budget_by_date[d][p]['spend'] for d in chart_dn])
with_retry(bw.update, values=br, range_name="A1", value_input_option="RAW"); print("✅ 예산"); time.sleep(2)

# 14~17: 주간종합
print("\n14단계: 주간종합 준비")
month_groups = defaultdict(list)
for t in date_names: do = date_objects[t]; mk = f"{do.year}년 {do.month}월"; month_groups[mk].append(t)
month_names_list = sorted(month_groups.keys(), key=lambda x: (int(x.split('년')[0]), int(x.split('년')[1].replace('월', '').strip())), reverse=True)
daily_data = defaultdict(lambda: {'spend': 0, 'revenue': 0, 'profit': 0}); daily_product_data = defaultdict(lambda: defaultdict(lambda: {'spend': 0, 'revenue': 0, 'profit': 0}))
for t in date_names:
    for it in sorted_list:
        if t in it['data']['dates']:
            dt = it['data']['dates'][t]; daily_data[t]['spend'] += dt['spend']; daily_data[t]['revenue'] += dt['revenue']; daily_data[t]['profit'] += dt['profit']
            p = extract_product(it['adset_name'], it['campaign_name'])
            daily_product_data[t][p]['spend'] += dt['spend']; daily_product_data[t][p]['revenue'] += dt['revenue']; daily_product_data[t][p]['profit'] += dt['profit']
wsd = {}; wps = {}
for wk in week_keys:
    wsd[wk] = {'spend': 0, 'revenue': 0, 'profit': 0}; wps[wk] = defaultdict(lambda: {'spend': 0, 'revenue': 0, 'profit': 0})
    for dn in week_groups[wk]:
        wsd[wk]['spend'] += daily_data[dn]['spend']; wsd[wk]['revenue'] += daily_data[dn]['revenue']; wsd[wk]['profit'] += daily_data[dn]['profit']
        for p in daily_product_data[dn]: wps[wk][p]['spend'] += daily_product_data[dn][p]['spend']; wps[wk][p]['revenue'] += daily_product_data[dn][p]['revenue']; wps[wk][p]['profit'] += daily_product_data[dn][p]['profit']
msd = {}; mps = {}
for mk in month_names_list:
    msd[mk] = {'spend': 0, 'revenue': 0, 'profit': 0}; mps[mk] = defaultdict(lambda: {'spend': 0, 'revenue': 0, 'profit': 0})
    for dn in month_groups[mk]:
        msd[mk]['spend'] += daily_data[dn]['spend']; msd[mk]['revenue'] += daily_data[dn]['revenue']; msd[mk]['profit'] += daily_data[dn]['profit']
        for p in daily_product_data[dn]: mps[mk][p]['spend'] += daily_product_data[dn][p]['spend']; mps[mk][p]['revenue'] += daily_product_data[dn][p]['revenue']; mps[mk][p]['profit'] += daily_product_data[dn][p]['profit']
all_found_products = set()
for dn in date_names:
    for p in daily_product_data[dn]:
        if daily_product_data[dn][p]['spend'] > 0 or daily_product_data[dn][p]['revenue'] > 0: all_found_products.add(p)
products = sorted(list(all_found_products), key=lambda p: sum(daily_product_data[dn][p]['spend'] for dn in date_names), reverse=True)
SUMMARY_PRODUCTS = products
print(f"📆 월:{len(month_names_list)}, 주:{len(week_keys)}, 일:{len(date_names)}")
print(f"📦 감지된 상품 (전체): {products}")

# ★ v30e: 60→30초
print(f"\n⏳ 14→15단계 전환: {PRE_WEEKLY_COOLDOWN}초 대기 (rate limit 해소)...")
time.sleep(PRE_WEEKLY_COOLDOWN)

# 15단계: 주간종합
print("\n15단계: 주간종합")
ws_ws, _is_new_ws = get_or_create_ws(sh, "주간종합", rows=2000, cols=20); time.sleep(2)
move_ws_to_front(sh, ws_ws)
# ★ v30f: 기존 탭 재사용 시 기존 데이터 클리어
if not _is_new_ws:
    try: with_retry(ws_ws.clear); time.sleep(1)
    except: pass
sid_ws = ws_ws.id; fr_ws = []; ar_ws = []; cr_ws = 0
def ccb(pn, d, pd, sid, cr, fr, im=False):
    block = []; bs = cr; rv = (d['revenue'] / d['spend']) if d['spend'] > 0 else 0; hb = COLORS["dark_blue"] if im else COLORS["dark_gray"]; cb = COLORS["navy"] if im else COLORS["black"]; nc = len(products) + 2
    block.append([pn] + [""] * (nc - 1)); fr.append(create_format_request(sid, cr, cr + 1, 0, nc, get_cell_format(hb, COLORS["white"], bold=True))); cr += 1
    block.append(["지출 금액", "구매(메타)", "구매(믹스패널)", "매출", "이익", "ROAS", "CVR", "전체"] + [""] * (nc - 8)); fr.append(create_format_request(sid, cr, cr + 1, 0, 8, get_cell_format(cb, COLORS["white"], bold=True))); cr += 1
    block.append([d['spend'], 0, 0, d['revenue'], d['profit'], rv, 0, ""] + [""] * (nc - 8)); fr.append(create_format_request(sid, cr, cr + 1, 0, 8, get_cell_format(COLORS["light_gray"])))
    fr.append(create_number_format_request(sid, cr, cr + 1, 0, 1, "NUMBER", "#,##0")); fr.append(create_number_format_request(sid, cr, cr + 1, 3, 5, "NUMBER", "#,##0")); fr.append(create_number_format_request(sid, cr, cr + 1, 5, 7, "PERCENT", "0.00%")); cr += 1
    block.append([""] + products + ["합"]); fr.append(create_format_request(sid, cr, cr + 1, 1, len(products) + 1, get_cell_format(cb, COLORS["white"], bold=True))); fr.append(create_format_request(sid, cr, cr + 1, len(products) + 1, len(products) + 2, get_cell_format(COLORS["light_yellow"], bold=True))); cr += 1
    for lb, dk, ft, fp in [("제품별 ROAS", "roas", "PERCENT", "0.00%"), ("제품별 순이익", "profit", "NUMBER", "#,##0"), ("제품별 매출", "revenue", "NUMBER", "#,##0"), ("제품별 예산", "spend", "NUMBER", "#,##0"), ("제품별 예산 비중", "ratio", "PERCENT", "0.00%")]:
        r = [lb]; tsp = sum(pd[p]['spend'] for p in products); trv = sum(pd[p]['revenue'] for p in products)
        for p in products:
            if dk == "roas": r.append((pd[p]['revenue'] / pd[p]['spend']) if pd[p]['spend'] > 0 else 0)
            elif dk == "ratio": r.append((pd[p]['spend'] / tsp) if tsp > 0 else 0)
            else: r.append(pd[p][dk])
        if dk == "roas": r.append((trv / tsp) if tsp > 0 else 0)
        elif dk == "ratio": r.append(1.0)
        else: r.append(sum(pd[p][dk] for p in products))
        block.append(r); fr.append(create_format_request(sid, cr, cr + 1, 0, 1, get_cell_format(COLORS["light_gray2"], bold=True))); fr.append(create_format_request(sid, cr, cr + 1, len(products) + 1, len(products) + 2, get_cell_format(COLORS["light_yellow"]))); fr.append(create_number_format_request(sid, cr, cr + 1, 1, len(products) + 2, ft, fp)); cr += 1
    fr.append(create_border_request(sid, bs, cr, 0, len(products) + 2)); block.append([""] * nc); cr += 1
    return block, cr
for mk in month_names_list:
    yr = int(mk.split('년')[0]); mn = int(mk.split('년')[1].replace('월', '').strip()); dim = sorted(month_groups[mk], key=lambda x: date_objects[x]); fd = date_objects[dim[0]]; ld = date_objects[dim[-1]]; mr_d = get_month_range_display(fd, ld).replace("'", "")
    b, cr_ws = ccb(f"'{mk} ({mr_d})", msd[mk], mps[mk], sid_ws, cr_ws, fr_ws, im=True); ar_ws.extend(b)
    mw = [wk for wk in week_keys if any(date_objects[d].year == yr and date_objects[d].month == mn for d in week_groups[wk])]; mw.reverse()
    for wk in mw: b, cr_ws = ccb(week_display_names[wk], wsd[wk], wps[wk], sid_ws, cr_ws, fr_ws); ar_ws.extend(b)

print(f"  📝 주간종합 데이터: {len(ar_ws)}행 → 청크 쓰기...")
chunked_sheet_write(ws_ws, ar_ws, label="주간종합")
time.sleep(2)

# 주간종합 포맷 생략
print("  ⏩ 포맷 스킵 (데이터만 쓰기)")
print("✅ 주간종합")

# ★ v30e: 45→25초
print(f"⏳ 15→16단계 전환: {WEEKLY_STEP_COOLDOWN}초 대기...")
time.sleep(WEEKLY_STEP_COOLDOWN)

# 16단계: 주간종합_2
print("\n16단계: 주간종합_2")
ws2, _is_new_ws2 = get_or_create_ws(sh, "주간종합_2", rows=2000, cols=20); time.sleep(2)
move_ws_to_front(sh, ws2)
if not _is_new_ws2:
    try: with_retry(ws2.clear); time.sleep(1)
    except: pass
sid2 = ws2.id; fr2 = []; ar2 = []; cr2 = 0; npc = len(products) + 3; stl = []
for mk in month_names_list:
    yr = int(mk.split('년')[0]); mn = int(mk.split('년')[1].replace('월', '').strip()); d = msd[mk]; roas = (d['revenue'] / d['spend']) if d['spend'] > 0 else 0
    stl.append({'period': f"'{mk}", 'type': '월별', 'spend': d['spend'], 'revenue': d['revenue'], 'profit': d['profit'], 'roas': roas, 'im': True, 'mk': mk})
    mw = [wk for wk in week_keys if any(date_objects[dn].year == yr and date_objects[dn].month == mn for dn in week_groups[wk])]; mw.reverse()
    for wk in mw: wd = wsd[wk]; wr = (wd['revenue'] / wd['spend']) if wd['spend'] > 0 else 0; stl.append({'period': week_display_names[wk], 'type': '주간', 'spend': wd['spend'], 'revenue': wd['revenue'], 'profit': wd['profit'], 'roas': wr, 'im': False, 'mk': None, 'wk': wk})
ar2.append(["📊 기간별 전체 요약"] + [""] * 6); fr2.append(create_format_request(sid2, cr2, cr2 + 1, 0, 7, get_cell_format(COLORS["navy"], COLORS["white"], bold=True))); cr2 += 1
ar2.append(["기간", "유형", "지출금액", "매출", "이익", "ROAS", "CVR"]); fr2.append(create_format_request(sid2, cr2, cr2 + 1, 0, 7, get_cell_format(COLORS["dark_blue"], COLORS["white"], bold=True))); cr2 += 1; t1s = cr2
for rd in stl:
    ar2.append([rd['period'], rd['type'], rd['spend'], rd['revenue'], rd['profit'], rd['roas'], 0]); bg = COLORS["light_blue"] if rd['im'] else COLORS["light_gray"]
    fr2.append(create_format_request(sid2, cr2, cr2 + 1, 0, 7, get_cell_format(bg))); fr2.append(create_number_format_request(sid2, cr2, cr2 + 1, 2, 5, "NUMBER", "#,##0")); fr2.append(create_number_format_request(sid2, cr2, cr2 + 1, 5, 7, "PERCENT", "0.00%")); cr2 += 1
fr2.append(create_border_request(sid2, t1s - 1, cr2, 0, 7)); ar2 += [[""] * npc, [""] * npc]; cr2 += 2
for tt, tc, dk in [("📈 제품별 ROAS", COLORS["dark_green"], "roas"), ("💰 제품별 순이익", COLORS["dark_green"], "profit"), ("💵 제품별 매출", COLORS["orange"], "revenue"), ("💸 제품별 예산", COLORS["purple"], "spend"), ("📊 제품별 예산 비중", COLORS["purple"], "ratio")]:
    ar2.append([tt] + [""] * (npc - 1)); fr2.append(create_format_request(sid2, cr2, cr2 + 1, 0, npc, get_cell_format(tc, COLORS["white"], bold=True))); cr2 += 1
    ar2.append(["기간", "유형"] + products + ["합계"]); fr2.append(create_format_request(sid2, cr2, cr2 + 1, 0, npc, get_cell_format(COLORS["dark_gray"], COLORS["white"], bold=True))); cr2 += 1; tds = cr2
    for rd in stl:
        pd_r = mps.get(rd['mk'], defaultdict(lambda: {'spend': 0, 'revenue': 0, 'profit': 0})) if rd['im'] else wps.get(rd.get('wk', ''), defaultdict(lambda: {'spend': 0, 'revenue': 0, 'profit': 0}))
        r = [rd['period'], rd['type']]; tsp = sum(pd_r[p]['spend'] for p in products); trv = sum(pd_r[p]['revenue'] for p in products)
        for p in products:
            if dk == "roas": r.append((pd_r[p]['revenue'] / pd_r[p]['spend']) if pd_r[p]['spend'] > 0 else 0)
            elif dk == "ratio": r.append((pd_r[p]['spend'] / tsp) if tsp > 0 else 0)
            else: r.append(pd_r[p][dk])
        if dk == "roas": r.append((trv / tsp) if tsp > 0 else 0)
        elif dk == "ratio": r.append(1.0)
        else: r.append(sum(pd_r[p][dk] for p in products))
        ar2.append(r); bg = COLORS["light_blue"] if rd['im'] else COLORS["light_gray"]; fr2.append(create_format_request(sid2, cr2, cr2 + 1, 0, npc, get_cell_format(bg)))
        ft = "PERCENT" if dk in ["roas", "ratio"] else "NUMBER"; fp = "0.00%" if dk in ["roas", "ratio"] else "#,##0"
        fr2.append(create_number_format_request(sid2, cr2, cr2 + 1, 2, npc, ft, fp)); cr2 += 1
    fr2.append(create_border_request(sid2, tds - 1, cr2, 0, npc)); ar2 += [[""] * npc, [""] * npc]; cr2 += 2

print(f"  📝 주간종합_2 데이터: {len(ar2)}행 → 청크 쓰기...")
chunked_sheet_write(ws2, ar2, label="주간종합_2")
time.sleep(2)

# 주간종합_2 포맷 생략
print("  ⏩ 포맷 스킵 (데이터만 쓰기)")
print("✅ 주간종합_2")

# ★ v30e: 45→25초
print(f"⏳ 16→17단계 전환: {WEEKLY_STEP_COOLDOWN}초 대기...")
time.sleep(WEEKLY_STEP_COOLDOWN)

# 17단계: 주간종합_3 (일별)
print("\n17단계: 주간종합_3")
ws3, _is_new_ws3 = get_or_create_ws(sh, "주간종합_3", rows=3000, cols=20); time.sleep(2)
move_ws_to_front(sh, ws3)
if not _is_new_ws3:
    try: with_retry(ws3.clear); time.sleep(1)
    except: pass
sid3 = ws3.id; fr3 = []; ar3 = []; cr3 = 0; ndc = len(products) + 4; dsr = []
for t in reversed(date_names): do = date_objects[t]; d = daily_data[t]; roas = (d['revenue'] / d['spend']) if d['spend'] > 0 else 0; wd = WEEKDAY_NAMES[do.weekday()]; dsr.append({'period': f"'{do.month}.{do.day}({wd})", 'weekday': wd, 'spend': d['spend'], 'revenue': d['revenue'], 'profit': d['profit'], 'roas': roas, 'tab_name': t})
ar3.append(["📊 일별 전체 요약"] + [""] * 7); fr3.append(create_format_request(sid3, cr3, cr3 + 1, 0, 8, get_cell_format(COLORS["navy"], COLORS["white"], bold=True))); cr3 += 1
ar3.append(["날짜", "요일", "지출금액", "매출", "이익", "ROAS", "CVR", ""]); fr3.append(create_format_request(sid3, cr3, cr3 + 1, 0, 8, get_cell_format(COLORS["dark_blue"], COLORS["white"], bold=True))); cr3 += 1; t1s3 = cr3
for rd in dsr:
    ar3.append([rd['period'], rd['weekday'], rd['spend'], rd['revenue'], rd['profit'], rd['roas'], 0, ""]); bg = COLORS["light_blue"] if rd['weekday'] in ['토', '일'] else COLORS["light_gray"]
    fr3.append(create_format_request(sid3, cr3, cr3 + 1, 0, 8, get_cell_format(bg))); fr3.append(create_number_format_request(sid3, cr3, cr3 + 1, 2, 5, "NUMBER", "#,##0")); fr3.append(create_number_format_request(sid3, cr3, cr3 + 1, 5, 7, "PERCENT", "0.00%")); cr3 += 1
fr3.append(create_border_request(sid3, t1s3 - 1, cr3, 0, 8)); ar3 += [[""] * ndc, [""] * ndc]; cr3 += 2
for tt, tc, dk in [("📈 일별 제품별 ROAS", COLORS["dark_green"], "roas"), ("💰 일별 제품별 순이익", COLORS["dark_green"], "profit"), ("💵 일별 제품별 매출", COLORS["orange"], "revenue"), ("💸 일별 제품별 예산", COLORS["purple"], "spend"), ("📊 일별 제품별 예산 비중", COLORS["purple"], "ratio")]:
    ar3.append([tt] + [""] * (ndc - 1)); fr3.append(create_format_request(sid3, cr3, cr3 + 1, 0, ndc, get_cell_format(tc, COLORS["white"], bold=True))); cr3 += 1
    ar3.append(["날짜", "요일"] + products + ["합계"]); fr3.append(create_format_request(sid3, cr3, cr3 + 1, 0, ndc, get_cell_format(COLORS["dark_gray"], COLORS["white"], bold=True))); cr3 += 1; tds = cr3
    for rd in dsr:
        pd_r = daily_product_data[rd['tab_name']]; r = [rd['period'], rd['weekday']]
        tsp = sum(pd_r[p]['spend'] for p in products); trv = sum(pd_r[p]['revenue'] for p in products)
        for p in products:
            if dk == "roas": r.append((pd_r[p]['revenue'] / pd_r[p]['spend']) if pd_r[p]['spend'] > 0 else 0)
            elif dk == "ratio": r.append((pd_r[p]['spend'] / tsp) if tsp > 0 else 0)
            else: r.append(pd_r[p][dk])
        if dk == "roas": r.append((trv / tsp) if tsp > 0 else 0)
        elif dk == "ratio": r.append(1.0)
        else: r.append(sum(pd_r[p][dk] for p in products))
        ar3.append(r); bg = COLORS["light_blue"] if rd['weekday'] in ['토', '일'] else COLORS["light_gray"]; fr3.append(create_format_request(sid3, cr3, cr3 + 1, 0, ndc, get_cell_format(bg)))
        ft = "PERCENT" if dk in ["roas", "ratio"] else "NUMBER"; fp = "0.00%" if dk in ["roas", "ratio"] else "#,##0"
        fr3.append(create_number_format_request(sid3, cr3, cr3 + 1, 2, ndc, ft, fp)); cr3 += 1
    fr3.append(create_border_request(sid3, tds - 1, cr3, 0, ndc)); ar3 += [[""] * ndc, [""] * ndc]; cr3 += 2

print(f"  📝 주간종합_3 데이터: {len(ar3)}행 → 청크 쓰기...")
chunked_sheet_write(ws3, ar3, label="주간종합_3")
time.sleep(2)

# 주간종합_3 포맷 생략
print("  ⏩ 포맷 스킵 (데이터만 쓰기)")
print("✅ 주간종합_3"); time.sleep(2)

# 18: 최종 탭 순서
print("\n" + "=" * 60); print("18단계: 최종 탭 순서 정리"); print("=" * 60)
reorder_tabs(sh)

print("\n" + "=" * 60); print("✅ 완료!"); print("=" * 60)
print()
print(f"🔄 Meta: 최근 {REFRESH_DAYS}일 | Mixpanel: {REFRESH_DAYS}일")
print(f"📝 기존 탭: {len(existing_refresh_tabs)}개 | 🆕 새 탭: {len(new_refresh_dates)}개")
print(f"📊 분석탭: {len(date_names)}일 기반")
print(f"   → 마스터탭: {len(master_raw_data)}행 | 추이차트: {len(date_names)}일")
print(f"   → 추이차트_상품별: {len(_sorted_products_chart)}개 상품")
print(f"   → 주간종합: {len(week_keys)}주 / {len(month_names_list)}개월")
print(f"📦 ★ v30f: 텍스트색상 {TEXT_COLOR_MAX_COLS}col/{TEXT_COLOR_BATCH_SIZE}배치/{TEXT_COLOR_FLUSH_SLEEP}초, 대기: 차트{CHART_STEP_SLEEP}s/주간전{PRE_WEEKLY_COOLDOWN}s/주간간{WEEKLY_STEP_COOLDOWN}s")
print(f"📦 ★ v30f: sleep(3)→sleep(2) 일괄축소, 포맷 배치 {WEEKLY_FORMAT_BATCH_SIZE}개/{WEEKLY_FORMAT_BATCH_DELAY}초")
print(f"📦 ★ v30d: 기존 행 cols14-18만 갱신(하이라이트 보존), 요약표 API소스(추이차트 동기화)")
print(f"📦 ★ v30b: 마스터탭 날짜 텍스트 강제, 날짜 정규화, adset_id 정밀도 보존, dedup 개선")
print(f"📦 ★ v29b: 상품=캠페인 첫 단어(이모지 제거), 모든 상품 나열")
print(f"📦 ★ v29: CPM→매출, 정렬=전날지출순")
print(f"\n📊 {SPREADSHEET_URL}")
