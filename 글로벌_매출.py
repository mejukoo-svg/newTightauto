"""
============================================================
타이트사주 - Stripe 매출 → Google Sheets
============================================================
두 가지 모드:
  1) daily   - 국가별(대만/홍콩/일본) 일별 매출 → "매출" 시트
               ★ 최신 10일만 Stripe API 호출, 기존 시트 데이터와 병합
               ★ 순이익 = 종합 매출 - 마스터탭 지출금액 합산
  2) weekly  - 월별 블록 + 주차별 달러/원화 매출 → "주간매출" 시트
  3) all     - 위 두 가지 모두 실행

[사용법]
  python 글로벌_매출.py daily
  python 글로벌_매출.py weekly
  python 글로벌_매출.py all

환경변수:
  STRIPE_API_KEY             - Stripe Restricted API Key
  SPREADSHEET_URL_TW_ADSET   - Google Sheets 문서 ID 또는 URL
  GCP_SERVICE_ACCOUNT_KEY    - Service Account JSON
============================================================
"""

import os, sys, json, time, stripe, gspread, requests, threading, re
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==================== CONFIG ====================

STRIPE_API_KEY = os.environ.get("STRIPE_API_KEY", "")

_raw_sheet = os.environ.get("SPREADSHEET_URL_TW_ADSET", "")
if "/d/" in _raw_sheet:
    SPREADSHEET_ID = _raw_sheet.split("/d/")[1].split("/")[0]
else:
    SPREADSHEET_ID = _raw_sheet.strip()

DAILY_SHEET_NAME = "매출"
WEEKLY_SHEET_NAME = "주간매출"
MASTER_SHEET_NAME = "마스터탭"

# ★ daily 모드: 최신 10일만 API 호출
DAYS_BACK = 10

START_YEAR = 2025
START_MONTH = 12

MONTHLY_TARGETS = {
    (2025, 12): 100_000_000,
    (2026, 1): 150_000_000,
    (2026, 2): 150_000_000,
    (2026, 3): 500_000_000,
}
DEFAULT_TARGET = 500_000_000

TARGET_COUNTRIES = {"TW": "대만", "HK": "홍콩", "JP": "일본"}
CURRENCY_TO_COUNTRY = {"twd": "TW", "hkd": "HK", "jpy": "JP"}
COUNTRY_ORDER = ["대만", "홍콩", "일본"]

DAILY_ALLOWED_CURRENCIES = {"twd", "hkd", "jpy", "usd"}
WEEKLY_ALLOWED_CURRENCIES = {"twd", "hkd", "jpy"}

CURRENCY_DIVISOR = {"jpy": 1, "twd": 100, "hkd": 100, "usd": 100}

KST = timezone(timedelta(hours=9))
DAY_NAMES_KR = ["월", "화", "수", "목", "금", "토", "일"]

MAX_WORKERS = 16

LEFT_TABS_ORDER = ["매출", "주간매출"]

# ==================== 재시도 로직 ====================

def retry_on_quota(fn, label="", max_retries=5, base_wait=30):
    for attempt in range(max_retries):
        try:
            return fn()
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429 and attempt < max_retries - 1:
                wait = base_wait * (attempt + 1)
                print(f"  ⏳ [{label}] 할당량 초과, {wait}초 대기 후 재시도 ({attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                raise

# ==================== 인증 ====================

def get_gspread_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    if 'GCP_SERVICE_ACCOUNT_KEY' in os.environ:
        creds = Credentials.from_service_account_info(
            json.loads(os.environ['GCP_SERVICE_ACCOUNT_KEY']), scopes=scopes)
        print("✅ GitHub Actions 서비스 계정 인증")
        return gspread.authorize(creds)
    try:
        creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
        return gspread.authorize(creds)
    except FileNotFoundError:
        pass
    try:
        from google.colab import auth; auth.authenticate_user()
        import google.auth
        creds, _ = google.auth.default(scopes=scopes)
        print("✅ Colab 인증")
        return gspread.authorize(creds)
    except Exception:
        pass
    raise RuntimeError("인증 실패")

# ==================== 환율 (병렬 조회) ====================

_rate_cache = {}
_rate_lock = threading.Lock()


def _fetch_single_rate(date_str):
    with _rate_lock:
        if date_str in _rate_cache:
            return date_str, _rate_cache[date_str]

    url = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date_str}/v1/currencies/usd.json"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            usd = resp.json().get("usd", {})
            rates = {
                "krw": usd.get("krw", 1480),
                "twd": usd.get("twd", 32),
                "hkd": usd.get("hkd", 7.8),
                "jpy": usd.get("jpy", 150),
            }
            with _rate_lock:
                _rate_cache[date_str] = rates
            return date_str, rates
    except Exception:
        pass

    with _rate_lock:
        if _rate_cache:
            last = list(_rate_cache.values())[-1]
            _rate_cache[date_str] = last
            return date_str, last

    default = {"krw": 1480, "twd": 32, "hkd": 7.8, "jpy": 150}
    with _rate_lock:
        _rate_cache[date_str] = default
    return date_str, default


def get_rate(date_str):
    with _rate_lock:
        if date_str in _rate_cache:
            return _rate_cache[date_str]
    _, rates = _fetch_single_rate(date_str)
    return rates


def prefetch_rates(start_date, end_date):
    print(f"\n💱 환율 병렬 조회 중...")
    dates = []
    d = start_date
    while d <= end_date:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_single_rate, ds): ds for ds in dates}
        done = 0
        for future in as_completed(futures):
            future.result()
            done += 1
            if done % 20 == 0:
                print(f"  💱 {done}/{len(dates)} 완료...")

    print(f"✅ {len(dates)}일 환율 병렬 조회 완료")


def convert_to_krw(amount, currency, rates):
    krw_rate = rates["krw"]
    if currency == "usd":
        return round(amount * krw_rate)
    usd_to_local = rates.get(currency, 0)
    if usd_to_local == 0:
        return 0
    return round((amount / usd_to_local) * krw_rate)

# ==================== Stripe 데이터 수집 ====================

def fetch_all_charges(start_date, end_date):
    stripe.api_key = STRIPE_API_KEY
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())
    all_charges = []; has_more = True; starting_after = None

    print(f"📅 수집: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    while has_more:
        params = {"limit": 100, "created": {"gte": start_ts, "lte": end_ts}, "status": "succeeded"}
        if starting_after: params["starting_after"] = starting_after
        response = stripe.Charge.list(**params)
        all_charges.extend(response.data)
        has_more = response.has_more
        if response.data: starting_after = response.data[-1].id
        print(f"  💳 {len(all_charges)}건...")
    print(f"✅ 총 {len(all_charges)}건")
    return all_charges

# ==================== [모드 1] 일별 매출 (매출 탭) ====================

def _process_charge_daily(charge):
    currency = (getattr(charge, "currency", "") or "").lower()
    if currency not in DAILY_ALLOWED_CURRENCIES:
        return None

    charge_dt = datetime.fromtimestamp(charge.created, tz=KST)
    date_str = charge_dt.strftime("%Y-%m-%d")

    country_code = None
    bd = getattr(charge, "billing_details", None)
    if bd:
        addr = getattr(bd, "address", None)
        if addr:
            country_code = getattr(addr, "country", None)
    if not country_code and currency:
        country_code = CURRENCY_TO_COUNTRY.get(currency)
    if country_code not in TARGET_COUNTRIES:
        return None

    country_name = TARGET_COUNTRIES[country_code]
    divisor = CURRENCY_DIVISOR.get(currency, 100)
    amount = charge.amount / divisor
    rates = get_rate(date_str)
    amount_krw = convert_to_krw(amount, currency, rates)
    return country_name, date_str, amount_krw


def aggregate_revenue_daily_mode(charges, start_date, end_date):
    revenue = defaultdict(lambda: defaultdict(float))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(_process_charge_daily, ch) for ch in charges]
        for future in as_completed(futures):
            result = future.result()
            if result:
                country_name, date_str, amount_krw = result
                revenue[country_name][date_str] += amount_krw

    all_dates = []
    d = start_date
    while d <= end_date:
        all_dates.append(d.strftime("%Y-%m-%d")); d += timedelta(days=1)
    all_dates.sort(reverse=True)

    all_countries = [c for c in COUNTRY_ORDER if c in revenue]
    return revenue, all_dates, all_countries


def _cell_format_req(sheet_id, start_row, end_row, start_col, end_col, fmt, fields):
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            },
            "cell": {"userEnteredFormat": fmt},
            "fields": fields,
        }
    }


def _single_cell_format_req(sheet_id, row, col, fmt, fields):
    return _cell_format_req(sheet_id, row, row + 1, col, col + 1, fmt, fields)


def _parse_header_date(cell_text):
    """헤더 셀 'MM-DD(요일)' → 'YYYY-MM-DD' 변환 (연도 추정)"""
    m = re.match(r"(\d{2})-(\d{2})", str(cell_text).strip())
    if not m:
        return None
    month, day = int(m.group(1)), int(m.group(2))
    now = datetime.now(KST)
    # 현재 월보다 크면 작년으로 추정
    year = now.year if month <= now.month else now.year - 1
    try:
        return date(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _read_existing_daily_sheet(gc):
    """기존 매출 시트에서 국가별/날짜별 데이터 읽기"""
    try:
        ss = gc.open_by_key(SPREADSHEET_ID)
        ws = ss.worksheet(DAILY_SHEET_NAME)
    except (gspread.exceptions.WorksheetNotFound, gspread.exceptions.SpreadsheetNotFound):
        return None

    all_values = retry_on_quota(lambda: ws.get_all_values(), label="기존 시트 읽기")
    if not all_values or len(all_values) < 2:
        return None

    header = all_values[0]
    if header[0] != "국가":
        return None

    # 헤더에서 날짜 파싱
    date_map = {}  # col_idx → YYYY-MM-DD
    for col_idx in range(1, len(header)):
        ds = _parse_header_date(header[col_idx])
        if ds:
            date_map[col_idx] = ds

    if not date_map:
        return None

    existing_dates = set(date_map.values())
    existing_revenue = defaultdict(lambda: defaultdict(float))
    existing_rates = {}

    for row in all_values[1:]:
        label = row[0] if row else ""
        if label in COUNTRY_ORDER:
            for col_idx, ds in date_map.items():
                if col_idx < len(row) and row[col_idx]:
                    try:
                        val = float(str(row[col_idx]).replace(",", ""))
                        existing_revenue[label][ds] = val
                    except ValueError:
                        pass
        elif label == "USD/KRW":
            for col_idx, ds in date_map.items():
                if col_idx < len(row) and row[col_idx]:
                    try:
                        existing_rates[ds] = float(str(row[col_idx]).replace(",", ""))
                    except ValueError:
                        pass

    existing_countries = [c for c in COUNTRY_ORDER if c in existing_revenue]
    print(f"📖 기존 시트: {len(existing_dates)}일, 국가: {existing_countries}")
    return existing_revenue, existing_dates, existing_countries, existing_rates


def _read_master_spending(gc, all_dates):
    """마스터탭에서 Date + 지출금액 컬럼을 읽어 날짜별 지출 합산 → {YYYY-MM-DD: 지출액}"""
    spending = defaultdict(float)
    try:
        ss = gc.open_by_key(SPREADSHEET_ID)
        ws = ss.worksheet(MASTER_SHEET_NAME)
    except (gspread.exceptions.WorksheetNotFound, gspread.exceptions.SpreadsheetNotFound):
        print(f"⚠️ '{MASTER_SHEET_NAME}' 탭을 찾을 수 없습니다. 순이익은 매출과 동일하게 표시됩니다.")
        return spending

    all_values = retry_on_quota(lambda: ws.get_all_values(), label="마스터탭 읽기")
    if not all_values or len(all_values) < 2:
        print(f"⚠️ 마스터탭에 데이터가 없습니다.")
        return spending

    # 헤더에서 Date, 지출금액 컬럼 찾기
    header = all_values[0]
    date_col = None
    spend_col = None
    for i, h in enumerate(header):
        h_lower = str(h).strip().lower()
        if h_lower in ("date", "날짜"):
            date_col = i
        if "지출" in str(h).strip():
            spend_col = i

    if date_col is None or spend_col is None:
        print(f"⚠️ 마스터탭 헤더에서 Date({date_col}) 또는 지출금액({spend_col}) 컬럼을 찾을 수 없습니다.")
        print(f"   헤더: {header[:10]}...")
        return spending

    print(f"📖 마스터탭: Date=col{date_col}, 지출금액=col{spend_col}, 데이터 {len(all_values)-1}행")

    # 날짜별 지출금액 합산
    for row in all_values[1:]:
        if len(row) <= max(date_col, spend_col):
            continue
        raw_date = str(row[date_col]).strip()
        raw_spend = str(row[spend_col]).strip().replace(",", "").replace("₩", "").replace("\\", "")

        # 날짜 파싱: YYYY-MM-DD, YYYY/MM/DD, MM/DD 등
        ds = None
        if re.match(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", raw_date):
            ds = raw_date.replace("/", "-")
            # 월/일 zero-pad
            parts = ds.split("-")
            if len(parts) == 3:
                ds = f"{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}"
        elif re.match(r"\d{1,2}/\d{1,2}", raw_date):
            parts = raw_date.split("/")
            now = datetime.now(KST)
            m, d = int(parts[0]), int(parts[1])
            year = now.year if m <= now.month else now.year - 1
            ds = f"{year}-{m:02d}-{d:02d}"

        if not ds:
            continue

        try:
            val = float(raw_spend) if raw_spend and raw_spend not in ("-", "", "0", "#DIV/0!") else 0.0
            spending[ds] += val
        except ValueError:
            pass

    matched = sum(1 for d in all_dates if d in spending)
    total_spend = sum(spending.values())
    print(f"💰 마스터탭 지출: {len(spending)}일, 총 {total_spend:,.0f}원, 매출 탭과 {matched}일 매칭")
    return spending


def write_daily_sheet(revenue, fresh_dates, all_countries):
    """기존 시트 읽기 → 최신 10일 병합 → 전체 쓰기"""
    gc = get_gspread_client()
    ss = gc.open_by_key(SPREADSHEET_ID)

    # ── 1. 기존 시트 데이터 읽기 ──
    existing = _read_existing_daily_sheet(gc)

    if existing:
        old_revenue, old_dates, old_countries, old_rates = existing
        merged_dates_set = old_dates | set(fresh_dates)
        merged_revenue = defaultdict(lambda: defaultdict(float))

        for country in COUNTRY_ORDER:
            for ds, val in old_revenue[country].items():
                merged_revenue[country][ds] = val
        for country in COUNTRY_ORDER:
            for ds, val in revenue[country].items():
                merged_revenue[country][ds] = val

        merged_countries = [c for c in COUNTRY_ORDER
                           if c in set(old_countries) | set(all_countries)]

        all_dates = sorted(merged_dates_set, reverse=True)
        all_countries = merged_countries
        revenue = merged_revenue

        for ds, rate_krw in old_rates.items():
            with _rate_lock:
                if ds not in _rate_cache:
                    _rate_cache[ds] = {"krw": rate_krw, "twd": 32, "hkd": 7.8, "jpy": 150}

        print(f"🔀 병합 완료: {len(all_dates)}일 (기존 {len(old_dates)}일 + 신규 {len(fresh_dates)}일)")
    else:
        all_dates = fresh_dates
        print(f"🆕 신규 시트 생성: {len(all_dates)}일")

    # ── 1.5. 마스터탭에서 날짜별 지출 데이터 읽기 ──
    spending = _read_master_spending(gc, all_dates)

    # ── 2. 시트 준비 ──
    try:
        ws = ss.worksheet(DAILY_SHEET_NAME)
        retry_on_quota(lambda: ws.clear(), label="매출 시트 초기화")
        print(f"📋 시트 초기화: {DAILY_SHEET_NAME}")
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=DAILY_SHEET_NAME, rows=100, cols=len(all_dates)+3)
        print(f"📋 새 시트 생성: {DAILY_SHEET_NAME}")

    # ── 3. 데이터 빌드 ──
    date_objects = [datetime.strptime(ds, "%Y-%m-%d") for ds in all_dates]
    weekdays = [d.weekday() for d in date_objects]

    rows = []
    header = ["국가"]
    for ds, dt_obj, wd in zip(all_dates, date_objects, weekdays):
        header.append(f"{dt_obj.strftime('%m-%d')}({DAY_NAMES_KR[wd]})")
    header.append("합계")
    rows.append(header)

    for country in all_countries:
        row = [country]; total = 0
        for ds in all_dates:
            val = round(revenue[country].get(ds, 0)); row.append(val); total += val
        row.append(total); rows.append(row)

    rate_row = ["USD/KRW"] + [round(get_rate(ds).get("krw", 0), 2) for ds in all_dates] + [""]
    rows.append(rate_row)

    total_row = ["종합"]; gt = 0
    for ds in all_dates:
        ds_sum = sum(round(revenue[c].get(ds, 0)) for c in all_countries)
        total_row.append(ds_sum); gt += ds_sum
    total_row.append(gt)
    rows.append(total_row)

    # ── 순이익 row (종합 - 지출) ──
    net_profit_row = ["순이익"]; np_total = 0
    for ds in all_dates:
        ds_revenue = sum(round(revenue[c].get(ds, 0)) for c in all_countries)
        ds_spend = spending.get(ds, 0)
        np_val = round(ds_revenue - ds_spend)
        net_profit_row.append(np_val)
        np_total += np_val
    net_profit_row.append(np_total)
    rows.append(net_profit_row)

    # ── 주간 총합 row ──
    total_values = total_row[1:-1]
    week_totals = defaultdict(float)
    sunday_col_idx = {}
    first_col_idx = {}

    for i, (dt_obj, wd) in enumerate(zip(date_objects, weekdays)):
        monday = dt_obj - timedelta(days=wd)
        mk = monday.strftime("%Y-%m-%d")
        week_totals[mk] += total_values[i]
        if mk not in first_col_idx:
            first_col_idx[mk] = i
        if wd == 6:
            sunday_col_idx[mk] = i

    show_at = {}
    for mk in week_totals:
        show_at[mk] = sunday_col_idx.get(mk, first_col_idx[mk])

    weekly_row = ["주간합계"]
    for i, (dt_obj, wd) in enumerate(zip(date_objects, weekdays)):
        monday = dt_obj - timedelta(days=wd)
        mk = monday.strftime("%Y-%m-%d")
        if show_at[mk] == i:
            weekly_row.append(round(week_totals[mk]))
        else:
            weekly_row.append("")
    weekly_row.append("")
    rows.append(weekly_row)

    # ── 4. 시트 기록 ──
    num_rows = len(rows); num_cols = len(rows[0])
    if ws.row_count < num_rows: ws.resize(rows=num_rows)
    if ws.col_count < num_cols: ws.resize(cols=num_cols)

    range_name = f"A1:{gspread.utils.rowcol_to_a1(num_rows, num_cols)}"
    retry_on_quota(
        lambda: ws.update(values=rows, range_name=range_name, value_input_option="USER_ENTERED"),
        label="매출 데이터 기록"
    )
    print(f"📊 {len(all_countries)}국가 × {len(all_dates)}일 기록")

    # ── 5. 서식 ──
    sid = ws.id
    num_countries = len(all_countries)
    data_last_row = 1 + num_countries          # 국가 데이터 끝 (0-indexed)
    rate_row_idx = data_last_row               # USD/KRW
    total_row_idx = data_last_row + 1          # 종합
    net_profit_row_idx = data_last_row + 2     # 순이익
    weekly_row_idx = data_last_row + 3         # 주간합계

    reqs = [
        _cell_format_req(sid, 0, 1, 0, num_cols, {
            "backgroundColor": {"red": 0.267, "green": 0.447, "blue": 0.769},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER",
        }, "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"),
    ]

    for i, wd in enumerate(weekdays):
        if wd == 6:
            col_idx = i + 1
            reqs.append(_single_cell_format_req(sid, 0, col_idx, {
                "backgroundColor": {"red": 0.957, "green": 0.78, "blue": 0.78},
                "textFormat": {"bold": True, "foregroundColor": {"red": 0.6, "green": 0.0, "blue": 0.0}},
                "horizontalAlignment": "CENTER",
            }, "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"))

    if num_countries > 0:
        reqs.append(_cell_format_req(sid, 1, data_last_row, 1, num_cols, {
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
            "horizontalAlignment": "RIGHT",
        }, "userEnteredFormat(numberFormat,horizontalAlignment)"))

    reqs.append(_cell_format_req(sid, rate_row_idx, rate_row_idx + 1, 0, num_cols, {
        "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95},
    }, "userEnteredFormat(backgroundColor)"))
    reqs.append(_cell_format_req(sid, rate_row_idx, rate_row_idx + 1, 1, num_cols, {
        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
        "horizontalAlignment": "RIGHT",
    }, "userEnteredFormat(numberFormat,horizontalAlignment)"))

    reqs.append(_cell_format_req(sid, total_row_idx, total_row_idx + 1, 0, num_cols, {
        "textFormat": {"bold": True},
        "borders": {"top": {"style": "SOLID", "color": {"red": 0, "green": 0, "blue": 0}}},
    }, "userEnteredFormat(textFormat,borders)"))
    reqs.append(_cell_format_req(sid, total_row_idx, total_row_idx + 1, 1, num_cols, {
        "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
        "horizontalAlignment": "RIGHT",
    }, "userEnteredFormat(numberFormat,horizontalAlignment)"))

    # ── 순이익 서식: 녹색 배경, 볼드 ──
    reqs.append(_cell_format_req(sid, net_profit_row_idx, net_profit_row_idx + 1, 0, num_cols, {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85},
    }, "userEnteredFormat(textFormat,backgroundColor)"))
    reqs.append(_cell_format_req(sid, net_profit_row_idx, net_profit_row_idx + 1, 1, num_cols, {
        "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
        "horizontalAlignment": "RIGHT",
    }, "userEnteredFormat(numberFormat,horizontalAlignment)"))

    reqs.append(_cell_format_req(sid, weekly_row_idx, weekly_row_idx + 1, 0, num_cols, {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.8},
        "horizontalAlignment": "RIGHT",
    }, "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"))
    reqs.append(_cell_format_req(sid, weekly_row_idx, weekly_row_idx + 1, 1, num_cols, {
        "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
    }, "userEnteredFormat(numberFormat)"))

    reqs.append(_cell_format_req(sid, 1, num_rows, 0, 1, {
        "textFormat": {"bold": True},
        "horizontalAlignment": "CENTER",
    }, "userEnteredFormat(textFormat,horizontalAlignment)"))

    reqs.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sid,
                "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 1},
            },
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
        }
    })

    retry_on_quota(
        lambda: ss.batch_update({"requests": reqs}),
        label="매출 서식 적용"
    )
    print("🎨 서식 적용 완료")
    return ws


def run_daily(charges=None):
    """일별 매출 모드 - 최신 10일만 Stripe API → 기존 시트와 병합"""
    print("\n" + "=" * 50)
    print("💳 [daily] 국가별 일별 매출 → 매출 탭 (최신 10일)")
    print("=" * 50)

    end_date = datetime.now(KST)
    start_date = (end_date - timedelta(days=DAYS_BACK)).replace(hour=0, minute=0, second=0, microsecond=0)

    if charges is None:
        prefetch_rates(start_date, end_date)
        charges = fetch_all_charges(start_date, end_date)

    if not charges:
        print("⚠️ 수집된 결제 내역이 없습니다 (기존 시트 데이터는 유지됩니다)")
        revenue = defaultdict(lambda: defaultdict(float))
        fresh_dates = []
        all_countries = []
    else:
        revenue, fresh_dates, all_countries = aggregate_revenue_daily_mode(charges, start_date, end_date)

        print(f"\n📊 신규 수집 국가: {all_countries}")
        for country in all_countries:
            total = sum(revenue[country].values())
            print(f"  {country}: {total:,.0f} KRW")

    write_daily_sheet(revenue, fresh_dates, all_countries)
    print("✅ daily 완료!")

# ==================== [모드 2] 주간 매출 (주간매출 탭) ====================

def aggregate_daily_for_weekly(charges):
    daily = defaultdict(lambda: {"usd": 0.0, "krw": 0.0})
    skipped = 0

    for ch in charges:
        currency = (getattr(ch, "currency", "") or "").lower()
        if currency not in WEEKLY_ALLOWED_CURRENCIES:
            skipped += 1
            continue

        charge_dt = datetime.fromtimestamp(ch.created, tz=KST)
        date_str = charge_dt.strftime("%Y-%m-%d")

        divisor = CURRENCY_DIVISOR.get(currency, 100)
        amount = ch.amount / divisor

        rates = get_rate(date_str)
        usd_to_local = rates.get(currency, 1)
        amount_usd = amount / usd_to_local if usd_to_local else 0
        amount_krw = amount_usd * rates.get("krw", 1480)

        daily[date_str]["usd"] += amount_usd
        daily[date_str]["krw"] += amount_krw

    if skipped:
        print(f"  ⏭️ 허용 외 통화 {skipped}건 제외 (USD 등)")

    return daily


def get_weeks_for_month(year, month):
    first_day = date(year, month, 1)
    last_day = date(year, month, monthrange(year, month)[1])
    mon = first_day - timedelta(days=first_day.weekday())
    weeks = []
    while mon <= last_day:
        sun = mon + timedelta(days=6)
        weeks.append((mon, sun))
        mon += timedelta(days=7)
    return weeks


def build_weekly_sheet_data(daily, start_year, start_month):
    rows = []
    now = datetime.now(KST)
    current_year, current_month = now.year, now.month
    y, m = start_year, start_month

    while (y, m) <= (current_year, current_month):
        month_total_krw = 0
        month_total_usd = 0
        for ds, vals in daily.items():
            d = datetime.strptime(ds, "%Y-%m-%d").date()
            if d.year == y and d.month == m:
                month_total_krw += vals["krw"]
                month_total_usd += vals["usd"]

        target = MONTHLY_TARGETS.get((y, m), DEFAULT_TARGET)
        achievement = month_total_krw / target if target else 0
        month_name = f"{y}년 {m}월"

        DAY_NAMES = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]

        r1 = ["실제매출", round(month_total_krw, 2), f"{month_name} Stripe 매출 통계"]
        r1 += [""] * 7
        rows.append(r1 + [""] * (10 - len(r1)))

        r2 = ["목표 매출", f"{target:,.0f}"] + [""] * 8 + [f"{m}월 달성률"]
        rows.append(r2)

        r3 = ["", ""] + DAY_NAMES + ["주간 매출", "달성", round(achievement, 4)]
        rows.append(r3)

        weeks = get_weeks_for_month(y, m)
        for wi, (mon_day, sun_day) in enumerate(weeks, 1):
            week_usd = 0
            week_krw = 0
            daily_usd = []
            daily_krw = []
            date_cells = []

            for i in range(7):
                day = mon_day + timedelta(days=i)
                ds = day.strftime("%Y-%m-%d")
                d_usd = round(daily.get(ds, {}).get("usd", 0), 2)
                d_krw = round(daily.get(ds, {}).get("krw", 0), 2)
                daily_usd.append(d_usd)
                daily_krw.append(d_krw)
                week_usd += d_usd
                week_krw += d_krw
                date_cells.append(day.strftime("%Y-%m-%d"))

            rows.append([f"{m}월 {wi}주차", ""] + date_cells + [f"{m}월 {wi}주차"])
            rows.append([round(week_usd, 2), "달러"] + daily_usd + [round(week_usd, 2)])
            rows.append(["", "원화"] + daily_krw + [round(week_krw, 2)])

        rows.append([""] * 11)

        if m == 12:
            y += 1; m = 1
        else:
            m += 1

    return rows


def write_weekly_sheet(rows):
    gc = get_gspread_client()
    ss = gc.open_by_key(SPREADSHEET_ID)

    try:
        ws = ss.worksheet(WEEKLY_SHEET_NAME)
        retry_on_quota(lambda: ws.clear(), label="주간매출 시트 초기화")
        print(f"📋 기존 시트 초기화: {WEEKLY_SHEET_NAME}")
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=WEEKLY_SHEET_NAME, rows=len(rows) + 50, cols=15)
        print(f"📋 새 시트 생성: {WEEKLY_SHEET_NAME}")

    if ws.row_count < len(rows) + 10:
        ws.resize(rows=len(rows) + 10)
    if ws.col_count < 15:
        ws.resize(cols=15)

    num_rows = len(rows)
    num_cols = max(len(r) for r in rows)
    for r in rows:
        while len(r) < num_cols:
            r.append("")

    cell_range = f"A1:{gspread.utils.rowcol_to_a1(num_rows, num_cols)}"
    retry_on_quota(
        lambda: ws.update(cell_range, rows, value_input_option="USER_ENTERED"),
        label="주간매출 데이터 기록"
    )
    print(f"📊 {num_rows}행 기록 완료")

    _apply_weekly_formatting(ws, ss, rows, num_cols)
    return ws


def _apply_weekly_formatting(ws, ss, rows, num_cols):
    sid = ws.id
    reqs = []

    for i, row in enumerate(rows):
        a_val = str(row[0]) if row[0] else ""
        b_val = str(row[1]) if len(row) > 1 and row[1] else ""

        if a_val == "실제매출":
            reqs.append(_cell_format_req(sid, i, i+1, 0, num_cols, {
                "backgroundColor": {"red": 0.267, "green": 0.447, "blue": 0.769},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            }, "userEnteredFormat(backgroundColor,textFormat)"))
        elif a_val == "목표 매출":
            reqs.append(_cell_format_req(sid, i, i+1, 0, num_cols, {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.93, "green": 0.93, "blue": 0.93},
            }, "userEnteredFormat(textFormat,backgroundColor)"))
        elif "주차" in a_val:
            reqs.append(_cell_format_req(sid, i, i+1, 0, num_cols, {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 1.0},
            }, "userEnteredFormat(textFormat,backgroundColor)"))
        elif b_val == "달러":
            reqs.append(_cell_format_req(sid, i, i+1, 0, num_cols, {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
            }, "userEnteredFormat(numberFormat)"))
        elif b_val == "원화":
            reqs.append(_cell_format_req(sid, i, i+1, 0, num_cols, {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
            }, "userEnteredFormat(numberFormat)"))

    reqs.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sid,
                "gridProperties": {"frozenColumnCount": 2},
            },
            "fields": "gridProperties.frozenColumnCount",
        }
    })

    if reqs:
        retry_on_quota(
            lambda: ss.batch_update({"requests": reqs}),
            label="주간매출 서식 적용"
        )
    print("🎨 서식 적용 완료")


def run_weekly(charges=None):
    print("\n" + "=" * 50)
    print("💳 [weekly] 주간 매출 통계 → 주간매출 탭")
    print("=" * 50)

    start_date = datetime(START_YEAR, START_MONTH, 1, tzinfo=KST)
    end_date = datetime.now(KST)

    if charges is None:
        prefetch_rates(start_date, end_date)
        charges = fetch_all_charges(start_date, end_date)

    if not charges:
        print("⚠️ 수집된 결제 내역이 없습니다.")
        return

    daily = aggregate_daily_for_weekly(charges)
    print(f"\n📊 일별 데이터: {len(daily)}일")

    monthly_summary = defaultdict(float)
    for ds, vals in daily.items():
        d = datetime.strptime(ds, "%Y-%m-%d").date()
        monthly_summary[(d.year, d.month)] += vals["krw"]
    for (y, m), total in sorted(monthly_summary.items()):
        print(f"  {y}년 {m}월: {total:,.0f} KRW")

    rows = build_weekly_sheet_data(daily, START_YEAR, START_MONTH)
    write_weekly_sheet(rows)
    print("✅ weekly 완료!")

# ==================== 탭 순서 정렬 ====================

def reorder_tabs_to_left():
    gc = get_gspread_client()
    ss = gc.open_by_key(SPREADSHEET_ID)
    ws_map = {w.title: w for w in ss.worksheets()}

    reqs = []
    for idx, tab_name in enumerate(LEFT_TABS_ORDER):
        if tab_name in ws_map:
            reqs.append({
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": ws_map[tab_name].id,
                        "index": idx,
                    },
                    "fields": "index",
                }
            })

    if reqs:
        retry_on_quota(
            lambda: ss.batch_update({"requests": reqs}),
            label="탭 왼쪽 이동"
        )
    print(f"📌 탭 맨 왼쪽 이동 완료: {LEFT_TABS_ORDER}")

# ==================== 메인 ====================

def main():
    if len(sys.argv) < 2:
        mode = "all"
    else:
        mode = sys.argv[1].lower()

    print("=" * 50)
    print(f"💳 Stripe 매출 → Google Sheets (모드: {mode})")
    print("=" * 50)

    if mode == "daily":
        run_daily()
        reorder_tabs_to_left()
    elif mode == "weekly":
        run_weekly()
        reorder_tabs_to_left()
    elif mode == "all":
        end_date = datetime.now(KST)

        # ── daily: 최신 10일만 ──
        start_date_daily = (end_date - timedelta(days=DAYS_BACK)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        prefetch_rates(start_date_daily, end_date)
        daily_charges = fetch_all_charges(start_date_daily, end_date)
        run_daily(daily_charges)

        # ── weekly: 전체 기간 ──
        start_date_weekly = datetime(START_YEAR, START_MONTH, 1, tzinfo=KST)
        prefetch_rates(start_date_weekly, end_date)
        weekly_charges = fetch_all_charges(start_date_weekly, end_date)
        run_weekly(weekly_charges)

        reorder_tabs_to_left()
        print("\n🎉 all 모드 완료!")
    else:
        print(f"❌ 알 수 없는 모드: {mode}")
        print("사용법: python 글로벌_매출.py [daily|weekly|all]")
        sys.exit(1)

    print("\n✅ 완료!")


if __name__ == "__main__":
    main()
