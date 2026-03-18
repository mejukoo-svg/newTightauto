"""
============================================================
타이트사주 - Stripe 매출 → Google Sheets
============================================================
두 가지 모드:
  1) daily   - 국가별(대만/홍콩/일본) 일별 매출 → "매출" 시트
  2) weekly  - 월별 블록 + 주차별 달러/원화 매출 → "주간매출" 시트
  3) all     - 위 두 가지 모두 실행

[사용법]
  python 글로벌_매출.py daily
  python 글로벌_매출.py weekly
  python 글로벌_매출.py all

환경변수:
  STRIPE_API_KEY          - Stripe Restricted API Key
  SPREADSHEET_URL_TW_ADSET   - Google Sheets 문서 ID
  GCP_SERVICE_ACCOUNT_KEY - Service Account JSON (파일 경로 또는 JSON 문자열)
============================================================
"""

import os
import sys
import json
import tempfile
import stripe
import gspread
import requests
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from calendar import monthrange

# ==================== CONFIG ====================

STRIPE_API_KEY = os.environ["STRIPE_API_KEY"]

_raw_sheet = os.environ["SPREADSHEET_URL_TW_ADSET"]
# URL 전체든 ID만이든 자동 처리
if "/d/" in _raw_sheet:
    SPREADSHEET_ID = _raw_sheet.split("/d/")[1].split("/")[0]
else:
    SPREADSHEET_ID = _raw_sheet.strip()

# Service account: 파일 경로 또는 JSON 문자열 모두 지원
_sa_raw = os.environ.get("GCP_SERVICE_ACCOUNT_KEY", "service_account.json")

DAILY_SHEET_NAME = "매출"
WEEKLY_SHEET_NAME = "주간매출"

DAYS_BACK = 45

# 주간매출 데이터 시작 월
START_YEAR = 2025
START_MONTH = 12

# 월별 목표 매출 (KRW) — 주간매출 시트에서 사용
MONTHLY_TARGETS = {
    (2025, 12): 100_000_000,
    (2026, 1): 150_000_000,
    (2026, 2): 150_000_000,
    (2026, 3): 500_000_000,
}
DEFAULT_TARGET = 500_000_000

# 국가/통화 설정
TARGET_COUNTRIES = {"TW": "대만", "HK": "홍콩", "JP": "일본"}
CURRENCY_TO_COUNTRY = {"twd": "TW", "hkd": "HK", "jpy": "JP"}
COUNTRY_ORDER = ["대만", "홍콩", "일본"]

DAILY_ALLOWED_CURRENCIES = {"twd", "hkd", "jpy", "usd"}
WEEKLY_ALLOWED_CURRENCIES = {"twd", "hkd", "jpy"}

CURRENCY_DIVISOR = {"jpy": 1, "twd": 100, "hkd": 100, "usd": 100}

KST = timezone(timedelta(hours=9))
DAY_NAMES = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]


# ==================== Google Auth ====================

def _resolve_service_account_path():
    """환경변수가 JSON 문자열이면 임시 파일로 저장, 파일 경로면 그대로 반환"""
    raw = _sa_raw.strip()
    if raw.startswith("{"):
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        tmp.write(raw)
        tmp.close()
        return tmp.name
    return raw


def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    sa_path = _resolve_service_account_path()
    try:
        creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
        return gspread.authorize(creds)
    except FileNotFoundError:
        pass

    # Colab 환경 폴백
    try:
        from google.colab import auth
        auth.authenticate_user()
        import google.auth
        creds, _ = google.auth.default(scopes=scopes)
        return gspread.authorize(creds)
    except Exception:
        pass

    raise RuntimeError("Google 인증 실패: GCP_SERVICE_ACCOUNT_KEY 환경변수를 확인하세요.")


# ==================== 환율 ====================

_rate_cache = {}


def get_rate(date_str):
    """날짜별 환율 조회 (캐싱, USD 기준)"""
    if date_str in _rate_cache:
        return _rate_cache[date_str]

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
            _rate_cache[date_str] = rates
            return rates
    except Exception:
        pass

    if _rate_cache:
        last = list(_rate_cache.values())[-1]
        _rate_cache[date_str] = last
        return last

    default = {"krw": 1480, "twd": 32, "hkd": 7.8, "jpy": 150}
    _rate_cache[date_str] = default
    return default


def prefetch_rates(start_date, end_date):
    """날짜 범위 환율 일괄 조회"""
    print(f"\n💱 환율 일괄 조회 중...")
    d = start_date
    count = 0
    while d <= end_date:
        get_rate(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
        count += 1
    print(f"✅ {count}일 환율 조회 완료")


def convert_to_krw(amount, currency, rates):
    """현지 통화 금액 → KRW 변환"""
    krw_rate = rates["krw"]
    if currency == "usd":
        return round(amount * krw_rate)
    usd_to_local = rates.get(currency, 0)
    if usd_to_local == 0:
        return 0
    return round((amount / usd_to_local) * krw_rate)


# ==================== Stripe 데이터 수집 ====================

def fetch_all_charges(start_date, end_date):
    """Stripe에서 성공한 결제 내역 전체 수집"""
    stripe.api_key = STRIPE_API_KEY
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())

    all_charges = []
    has_more = True
    starting_after = None

    print(f"📅 수집 범위: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

    while has_more:
        params = {
            "limit": 100,
            "created": {"gte": start_ts, "lte": end_ts},
            "status": "succeeded",
        }
        if starting_after:
            params["starting_after"] = starting_after

        response = stripe.Charge.list(**params)
        charges = response.data
        all_charges.extend(charges)
        has_more = response.has_more
        if charges:
            starting_after = charges[-1].id
        print(f"  💳 {len(all_charges)}건 수집중...")

    print(f"✅ 총 {len(all_charges)}건 수집 완료")
    return all_charges


# ==================== [모드 1] 일별 매출 (매출 탭) ====================

def aggregate_revenue_daily_mode(charges, start_date, end_date):
    """국가별/날짜별 매출 집계 (매출 탭용)"""
    revenue = defaultdict(lambda: defaultdict(float))
    skipped_country = 0
    skipped_currency = 0
    skipped_currencies = defaultdict(int)

    for charge in charges:
        currency = (charge.get("currency") or "").lower()
        if currency not in DAILY_ALLOWED_CURRENCIES:
            skipped_currency += 1
            skipped_currencies[currency.upper()] += 1
            continue

        charge_dt = datetime.fromtimestamp(charge.created, tz=KST)
        date_str = charge_dt.strftime("%Y-%m-%d")

        country_code = None
        bd = charge.get("billing_details")
        if bd and bd.get("address") and bd["address"].get("country"):
            country_code = bd["address"]["country"]
        elif currency:
            country_code = CURRENCY_TO_COUNTRY.get(currency)

        if country_code not in TARGET_COUNTRIES:
            skipped_country += 1
            continue

        country_name = TARGET_COUNTRIES[country_code]
        divisor = CURRENCY_DIVISOR.get(currency, 100)
        amount = charge.amount / divisor
        rates = get_rate(date_str)
        amount_krw = convert_to_krw(amount, currency, rates)
        revenue[country_name][date_str] += amount_krw

    all_dates = []
    d = start_date
    while d <= end_date:
        all_dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    all_dates.sort(reverse=True)

    all_countries = [c for c in COUNTRY_ORDER if c in revenue]

    if skipped_country:
        print(f"  ⏭️ 대만/홍콩/일본 외 {skipped_country}건 제외")
    if skipped_currency:
        print(f"  ⏭️ 허용 통화 외 {skipped_currency}건 제외: {dict(skipped_currencies)}")

    return revenue, all_dates, all_countries


def _cell_format_req(sheet_id, start_row, end_row, start_col, end_col, fmt, fields):
    """batchUpdate용 repeatCell 요청 헬퍼"""
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


def write_daily_sheet(revenue, all_dates, all_countries):
    """Google Sheets '매출' 탭에 일별 매출 기록"""
    gc = get_gspread_client()
    ss = gc.open_by_key(SPREADSHEET_ID)

    all_worksheets = ss.worksheets()
    total_sheets = len(all_worksheets)

    try:
        ws = ss.worksheet(DAILY_SHEET_NAME)
        ws.clear()
        print(f"📋 기존 시트 초기화: {DAILY_SHEET_NAME}")
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(
            title=DAILY_SHEET_NAME, rows=100, cols=len(all_dates) + 3, index=total_sheets,
        )
        print(f"📋 새 시트 생성: {DAILY_SHEET_NAME}")

    # 데이터 구성
    rows = []
    header = ["국가"]
    for date_str in all_dates:
        header.append(datetime.strptime(date_str, "%Y-%m-%d").strftime("%m-%d"))
    header.append("합계")
    rows.append(header)

    for country in all_countries:
        row = [country]
        total = 0
        for date_str in all_dates:
            val = round(revenue[country].get(date_str, 0))
            row.append(val)
            total += val
        row.append(total)
        rows.append(row)

    rate_row = ["USD/KRW"]
    for date_str in all_dates:
        rates = _rate_cache.get(date_str, {})
        rate_row.append(round(rates.get("krw", 0), 2))
    rate_row.append("")
    rows.append(rate_row)

    total_row = ["종합"]
    grand_total = 0
    for date_str in all_dates:
        day_sum = sum(round(revenue[c].get(date_str, 0)) for c in all_countries)
        total_row.append(day_sum)
        grand_total += day_sum
    total_row.append(grand_total)
    rows.append(total_row)

    # 일괄 업데이트
    num_rows = len(rows)
    num_cols = len(rows[0])
    if ws.row_count < num_rows:
        ws.resize(rows=num_rows)
    if ws.col_count < num_cols:
        ws.resize(cols=num_cols)

    cell_range = f"A1:{gspread.utils.rowcol_to_a1(num_rows, num_cols)}"
    ws.update(values=rows, range_name=cell_range, value_input_option="USER_ENTERED")
    print(f"📊 {len(all_countries)}개 국가 × {len(all_dates)}일 데이터 기록 완료")

    # 서식
    _apply_daily_formatting(ss, ws, num_rows, num_cols, len(all_countries))

    # 맨 오른쪽 이동
    all_worksheets = ss.worksheets()
    others = [w for w in all_worksheets if w.id != ws.id]
    ss.reorder_worksheets(others + [ws])
    print(f"📌 '{ws.title}' 탭을 맨 오른쪽으로 이동 완료")

    return ws


def _apply_daily_formatting(ss, ws, num_rows, num_cols, num_countries):
    """매출 탭 서식 (batch_update 1회)"""
    sid = ws.id
    data_last_row = 1 + num_countries
    rate_row_idx = data_last_row
    total_row_idx = data_last_row + 1

    reqs = []

    # 헤더
    reqs.append(_cell_format_req(sid, 0, 1, 0, num_cols, {
        "backgroundColor": {"red": 0.267, "green": 0.447, "blue": 0.769},
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    }, "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"))

    # 매출 데이터 숫자
    if num_countries > 0:
        reqs.append(_cell_format_req(sid, 1, data_last_row, 1, num_cols, {
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
            "horizontalAlignment": "RIGHT",
        }, "userEnteredFormat(numberFormat,horizontalAlignment)"))

    # 환율 행
    reqs.append(_cell_format_req(sid, rate_row_idx, rate_row_idx + 1, 0, num_cols, {
        "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95},
    }, "userEnteredFormat(backgroundColor)"))
    reqs.append(_cell_format_req(sid, rate_row_idx, rate_row_idx + 1, 1, num_cols, {
        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
        "horizontalAlignment": "RIGHT",
    }, "userEnteredFormat(numberFormat,horizontalAlignment)"))

    # 종합 행
    reqs.append(_cell_format_req(sid, total_row_idx, total_row_idx + 1, 0, num_cols, {
        "textFormat": {"bold": True},
        "borders": {"top": {"style": "SOLID", "color": {"red": 0, "green": 0, "blue": 0}}},
    }, "userEnteredFormat(textFormat,borders)"))
    reqs.append(_cell_format_req(sid, total_row_idx, total_row_idx + 1, 1, num_cols, {
        "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
        "horizontalAlignment": "RIGHT",
    }, "userEnteredFormat(numberFormat,horizontalAlignment)"))

    # A열 라벨
    reqs.append(_cell_format_req(sid, 1, num_rows, 0, 1, {
        "textFormat": {"bold": True},
        "horizontalAlignment": "CENTER",
    }, "userEnteredFormat(textFormat,horizontalAlignment)"))

    # 고정 행/열
    reqs.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sid,
                "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 1},
            },
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
        }
    })

    ss.batch_update({"requests": reqs})
    print("🎨 서식 적용 완료 (batch_update 1회)")


def run_daily():
    """일별 매출 모드 실행"""
    print("\n" + "=" * 50)
    print("💳 [daily] 국가별 일별 매출 → 매출 탭")
    print("=" * 50)

    end_date = datetime.now(KST)
    start_date = (end_date - timedelta(days=DAYS_BACK)).replace(hour=0, minute=0, second=0, microsecond=0)

    prefetch_rates(start_date, end_date)
    charges = fetch_all_charges(start_date, end_date)

    if not charges:
        print("⚠️ 수집된 결제 내역이 없습니다.")
        return

    revenue, all_dates, all_countries = aggregate_revenue_daily_mode(charges, start_date, end_date)

    print(f"\n📊 국가: {all_countries}")
    print(f"📊 날짜: {all_dates[-1]} ~ {all_dates[0]}")
    for country in all_countries:
        total = sum(revenue[country].values())
        print(f"  {country}: {total:,.0f} KRW")
    grand_total = sum(sum(revenue[c].values()) for c in all_countries)
    print(f"  종합: {grand_total:,.0f} KRW")

    write_daily_sheet(revenue, all_dates, all_countries)
    print("✅ daily 완료!")


# ==================== [모드 2] 주간 매출 (주간매출 탭) ====================

def aggregate_daily_for_weekly(charges):
    """일별 USD/KRW 집계 (주간매출 탭용)"""
    daily = defaultdict(lambda: {"usd": 0.0, "krw": 0.0})
    skipped = 0

    for ch in charges:
        currency = (ch.get("currency") or "").lower()
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
        print(f"  ⏭️ 허용 외 통화 {skipped}건 제외 (USD, IDR 등)")

    return daily


def get_weeks_for_month(year, month):
    """해당 월에 속하는 주차 리스트: [(월요일, 일요일), ...]"""
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
    """주간매출 시트용 2D 배열 생성"""
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

        r1 = ["실제매출", round(month_total_krw, 2), f"{month_name} Stripe 매출 통계"]
        r1 += [""] * 7
        rows.append(r1 + [""] * (10 - len(r1)))

        r2 = ["목표 매출", f"{target:,.0f}"] + [""] * 8 + [f"{m}월 달성률"]
        rows.append(r2)

        r3 = ["", ""] + DAY_NAMES + ["주간 매출", "달성", round(achievement, 4)]
        rows.append(r3)

        weeks = get_weeks_for_month(y, m)
        for wi, (mon, sun) in enumerate(weeks, 1):
            week_usd = 0
            week_krw = 0
            daily_usd = []
            daily_krw = []
            date_cells = []

            for i in range(7):
                day = mon + timedelta(days=i)
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
            y += 1
            m = 1
        else:
            m += 1

    return rows


def write_weekly_sheet(rows):
    """Google Sheets '주간매출' 탭에 기록"""
    gc = get_gspread_client()
    ss = gc.open_by_key(SPREADSHEET_ID)

    all_worksheets = ss.worksheets()
    total_sheets = len(all_worksheets)

    try:
        ws = ss.worksheet(WEEKLY_SHEET_NAME)
        ws.clear()
        print(f"📋 기존 시트 초기화: {WEEKLY_SHEET_NAME}")
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=WEEKLY_SHEET_NAME, rows=len(rows) + 50, cols=15, index=total_sheets)
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
    ws.update(cell_range, rows, value_input_option="USER_ENTERED")
    print(f"📊 {num_rows}행 기록 완료")

    _apply_weekly_formatting(ws, rows, num_cols)

    # 맨 오른쪽 이동
    all_worksheets = ss.worksheets()
    others = [w for w in all_worksheets if w.id != ws.id]
    ss.reorder_worksheets(others + [ws])
    print(f"📌 '{ws.title}' 탭을 맨 오른쪽으로 이동 완료")

    return ws


def _apply_weekly_formatting(ws, rows, num_cols):
    """주간매출 탭 서식"""
    for i, row in enumerate(rows):
        row_num = i + 1
        a_val = str(row[0]) if row[0] else ""
        b_val = str(row[1]) if row[1] else ""

        if a_val == "실제매출":
            ws.format(f"A{row_num}:{gspread.utils.rowcol_to_a1(row_num, num_cols)}", {
                "backgroundColor": {"red": 0.267, "green": 0.447, "blue": 0.769},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            })
        elif a_val == "목표 매출":
            ws.format(f"A{row_num}:{gspread.utils.rowcol_to_a1(row_num, num_cols)}", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.93, "green": 0.93, "blue": 0.93},
            })
        elif "주차" in a_val:
            ws.format(f"A{row_num}:{gspread.utils.rowcol_to_a1(row_num, num_cols)}", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 1.0},
            })
        elif b_val == "달러":
            ws.format(f"A{row_num}:{gspread.utils.rowcol_to_a1(row_num, num_cols)}", {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"},
            })
        elif b_val == "원화":
            ws.format(f"A{row_num}:{gspread.utils.rowcol_to_a1(row_num, num_cols)}", {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
            })

    ws.freeze(rows=0, cols=2)
    print("🎨 서식 적용 완료")


def run_weekly():
    """주간 매출 모드 실행"""
    print("\n" + "=" * 50)
    print("💳 [weekly] 주간 매출 통계 → 주간매출 탭")
    print("=" * 50)

    start_date = datetime(START_YEAR, START_MONTH, 1, tzinfo=KST)
    end_date = datetime.now(KST)

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


# ==================== 메인 ====================

def main():
    if len(sys.argv) < 2:
        mode = "all"
    else:
        mode = sys.argv[1].lower()

    if mode == "daily":
        run_daily()
    elif mode == "weekly":
        run_weekly()
    elif mode == "all":
        run_daily()
        # 환율 캐시는 유지되므로 두 번째 실행은 더 빠름
        run_weekly()
    else:
        print(f"❌ 알 수 없는 모드: {mode}")
        print("사용법: python 글로벌_매출.py [daily|weekly|all]")
        sys.exit(1)


if __name__ == "__main__":
    main()
