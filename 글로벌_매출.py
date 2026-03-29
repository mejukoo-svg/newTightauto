"""
============================================================
타이트사주 - Stripe 매출 → Google Sheets (매출 탭)
============================================================
GitHub Actions / Colab 자동 분기
환경변수: STRIPE_API_KEY, SPREADSHEET_URL_TW_ADSET, GCP_SERVICE_ACCOUNT_KEY
============================================================
"""

import os, json, time, stripe, gspread, requests
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ==================== CONFIG ====================

STRIPE_API_KEY = os.environ.get("STRIPE_API_KEY", "")

_raw_sheet = os.environ.get("SPREADSHEET_URL_TW_ADSET", "")
if "/d/" in _raw_sheet:
    SPREADSHEET_ID = _raw_sheet.split("/d/")[1].split("/")[0]
else:
    SPREADSHEET_ID = _raw_sheet.strip()

SHEET_NAME = "매출"
DAYS_BACK = 45

TARGET_COUNTRIES = {"TW": "대만", "HK": "홍콩", "JP": "일본"}
CURRENCY_TO_COUNTRY = {"twd": "TW", "hkd": "HK", "jpy": "JP"}
COUNTRY_ORDER = ["대만", "홍콩", "일본"]
ALLOWED_CURRENCIES = {"twd", "hkd", "jpy", "usd"}
CURRENCY_DIVISOR = {"jpy": 1, "twd": 100, "hkd": 100, "usd": 100}

KST = timezone(timedelta(hours=9))

# ==================== 재시도 로직 ====================

def retry_on_quota(fn, label="", max_retries=5, base_wait=30):
    """429 Quota 초과 시 exponential backoff 재시도"""
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

# ==================== 환율 ====================

def fetch_daily_rates(all_dates):
    daily_rates = {}; fallback = None
    print(f"\n💱 {len(all_dates)}일 환율 조회...")
    for ds in sorted(all_dates):
        url = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{ds}/v1/currencies/usd.json"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                usd = resp.json().get("usd", {})
                rates = {"krw": usd.get("krw",1480), "twd": usd.get("twd",32), "hkd": usd.get("hkd",7.8), "jpy": usd.get("jpy",150)}
                daily_rates[ds] = rates; fallback = rates
            elif fallback:
                daily_rates[ds] = fallback
        except:
            if fallback: daily_rates[ds] = fallback
    default = {"krw":1480, "twd":32, "hkd":7.8, "jpy":150}
    for ds in all_dates:
        if ds not in daily_rates: daily_rates[ds] = fallback or default
    print(f"✅ 환율 완료")
    return daily_rates

def convert_to_krw(amount, currency, rates):
    krw_rate = rates["krw"]
    if currency == "usd": return round(amount * krw_rate)
    usd_to_local = rates.get(currency, 0)
    return round((amount / usd_to_local) * krw_rate) if usd_to_local else 0

# ==================== Stripe ====================

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

# ==================== 집계 ====================

def aggregate_revenue(charges, start_date, end_date, daily_rates):
    revenue = defaultdict(lambda: defaultdict(float))
    for charge in charges:
        currency = (getattr(charge, "currency", "") or "").lower()
        if currency not in ALLOWED_CURRENCIES: continue
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
        if country_code not in TARGET_COUNTRIES: continue
        country_name = TARGET_COUNTRIES[country_code]
        divisor = CURRENCY_DIVISOR.get(currency, 100)
        amount = charge.amount / divisor
        rates = daily_rates.get(date_str, daily_rates.get(sorted(daily_rates.keys())[-1]))
        revenue[country_name][date_str] += convert_to_krw(amount, currency, rates)

    all_dates = []
    d = start_date
    while d <= end_date:
        all_dates.append(d.strftime("%Y-%m-%d")); d += timedelta(days=1)
    all_dates.sort(reverse=True)
    all_countries = [c for c in COUNTRY_ORDER if c in revenue]
    return revenue, all_dates, all_countries

# ==================== Sheets 기록 ====================

def _cell_fmt(sid, sr, er, sc, ec, fmt, fields):
    return {"repeatCell":{"range":{"sheetId":sid,"startRowIndex":sr,"endRowIndex":er,"startColumnIndex":sc,"endColumnIndex":ec},"cell":{"userEnteredFormat":fmt},"fields":fields}}

def write_to_sheet(revenue, all_dates, all_countries, daily_rates):
    gc = get_gspread_client()
    ss = gc.open_by_key(SPREADSHEET_ID)
    all_worksheets = ss.worksheets()

    try:
        ws = ss.worksheet(SHEET_NAME)
        retry_on_quota(lambda: ws.clear(), label="시트 초기화")
        print(f"📋 기존 시트 초기화: {SHEET_NAME}")
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=SHEET_NAME, rows=100, cols=len(all_dates)+3, index=len(all_worksheets))
        print(f"📋 새 시트 생성: {SHEET_NAME}")

    rows = []
    header = ["국가"] + [datetime.strptime(ds,"%Y-%m-%d").strftime("%m-%d") for ds in all_dates] + ["합계"]
    rows.append(header)

    for country in all_countries:
        row = [country]; total = 0
        for ds in all_dates:
            val = round(revenue[country].get(ds, 0)); row.append(val); total += val
        row.append(total); rows.append(row)

    rate_row = ["USD/KRW"] + [round(daily_rates.get(ds,{}).get("krw",0),2) for ds in all_dates] + [""]
    rows.append(rate_row)

    total_row = ["종합"]; gt = 0
    for ds in all_dates:
        ds_sum = sum(round(revenue[c].get(ds,0)) for c in all_countries)
        total_row.append(ds_sum); gt += ds_sum
    total_row.append(gt); rows.append(total_row)

    nr = len(rows); nc = len(rows[0])
    if ws.row_count < nr: ws.resize(rows=nr)
    if ws.col_count < nc: ws.resize(cols=nc)

    range_name = f"A1:{gspread.utils.rowcol_to_a1(nr,nc)}"
    retry_on_quota(
        lambda: ws.update(values=rows, range_name=range_name, value_input_option="USER_ENTERED"),
        label="데이터 기록"
    )
    print(f"📊 {len(all_countries)}국가 × {len(all_dates)}일 기록")

    # 서식
    sid = ws.id; dlr = 1+len(all_countries); rri = dlr; tri = dlr+1
    reqs = [
        _cell_fmt(sid,0,1,0,nc,{"backgroundColor":{"red":0.267,"green":0.447,"blue":0.769},"textFormat":{"bold":True,"foregroundColor":{"red":1,"green":1,"blue":1}},"horizontalAlignment":"CENTER"},"userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"),
        _cell_fmt(sid,1,dlr,1,nc,{"numberFormat":{"type":"NUMBER","pattern":"#,##0"},"horizontalAlignment":"RIGHT"},"userEnteredFormat(numberFormat,horizontalAlignment)"),
        _cell_fmt(sid,rri,rri+1,0,nc,{"backgroundColor":{"red":0.95,"green":0.95,"blue":0.95}},"userEnteredFormat(backgroundColor)"),
        _cell_fmt(sid,rri,rri+1,1,nc,{"numberFormat":{"type":"NUMBER","pattern":"#,##0.00"},"horizontalAlignment":"RIGHT"},"userEnteredFormat(numberFormat,horizontalAlignment)"),
        _cell_fmt(sid,tri,tri+1,0,nc,{"textFormat":{"bold":True},"borders":{"top":{"style":"SOLID","color":{"red":0,"green":0,"blue":0}}}},"userEnteredFormat(textFormat,borders)"),
        _cell_fmt(sid,tri,tri+1,1,nc,{"numberFormat":{"type":"NUMBER","pattern":"#,##0"},"horizontalAlignment":"RIGHT"},"userEnteredFormat(numberFormat,horizontalAlignment)"),
        _cell_fmt(sid,1,nr,0,1,{"textFormat":{"bold":True},"horizontalAlignment":"CENTER"},"userEnteredFormat(textFormat,horizontalAlignment)"),
        {"updateSheetProperties":{"properties":{"sheetId":sid,"gridProperties":{"frozenRowCount":1,"frozenColumnCount":1}},"fields":"gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}}
    ]
    retry_on_quota(
        lambda: ss.batch_update({"requests": reqs}),
        label="서식 적용"
    )
    print("🎨 서식 완료")

    # 맨 오른쪽 이동
    all_ws = ss.worksheets()
    others = [w for w in all_ws if w.id != ws.id]
    retry_on_quota(
        lambda: ss.reorder_worksheets(others + [ws]),
        label="탭 순서 이동"
    )
    print(f"📌 '{SHEET_NAME}' 맨 오른쪽 이동")
    return ws

# ==================== 메인 ====================

def main():
    print("="*50)
    print("💳 Stripe 매출 → Google Sheets (매출 탭)")
    print("="*50)

    end_date = datetime.now(KST)
    start_date = (end_date - timedelta(days=DAYS_BACK)).replace(hour=0, minute=0, second=0, microsecond=0)

    all_date_strs = []
    d = start_date
    while d <= end_date:
        all_date_strs.append(d.strftime("%Y-%m-%d")); d += timedelta(days=1)

    daily_rates = fetch_daily_rates(all_date_strs)
    charges = fetch_all_charges(start_date, end_date)
    if not charges: print("⚠️ 결제 내역 없음"); return

    revenue, all_dates, all_countries = aggregate_revenue(charges, start_date, end_date, daily_rates)

    print(f"\n📊 국가: {all_countries}")
    for c in all_countries: print(f"  {c}: {sum(revenue[c].values()):,.0f} KRW")
    print(f"  종합: {sum(sum(revenue[c].values()) for c in all_countries):,.0f} KRW")

    write_to_sheet(revenue, all_dates, all_countries, daily_rates)
    print("\n✅ 완료!")

if __name__ == "__main__":
    main()
