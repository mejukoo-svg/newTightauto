#!/usr/bin/env python3
# ============================================================
# 📊 매출 대시보드 자동화 v2 - GitHub Actions용
# TossPay 매출 + Meta Ads 비용 → Google Sheets 자동 입력
# ============================================================

import gspread
from google.oauth2.service_account import Credentials
import requests
import base64
import re
import json
import os
import time
from datetime import datetime, timedelta

# ─── Google 인증 (서비스 계정) ───
gcp_key = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
creds = Credentials.from_service_account_info(gcp_key, scopes=[
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
])
gc = gspread.authorize(creds)
print("✅ Google 인증 완료")

# ─── API 설정값 ───
META_ACCOUNTS = [
    {"account_id": "707835224206178",    "access_token": os.environ["META_TOKEN_1"]},
    {"account_id": "1270614404675034",   "access_token": os.environ["META_TOKEN_2"]},
    {"account_id": "25183853061243175",  "access_token": os.environ["META_TOKEN_3"]},
    {"account_id": "1808141386564262",   "access_token": os.environ["META_TOKEN_4"]},
]
META_API_VERSION = "v21.0"

TOSS_SECRET_KEY = os.environ["TOSS_SECRET_KEY"]

SPREADSHEET_ID = "1uiMN2bBNOt4qU9H86JzxMs_D6PeaIWOtz3IMtJqu4aI"
SHEET_NAME = "2604_매출대시보드"

print("✅ 설정값 로드 완료")

# ─── 시트에서 날짜 읽기 & 자동 기간 감지 ───
def parse_sheet_date(raw_date, year=2026):
    if not raw_date or not raw_date.strip():
        return None
    raw = raw_date.strip()

    m = re.match(r'^(\d{1,2})/(\d{1,2})\s*\(', raw)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        return f"{year}-{month:02d}-{day:02d}"

    m = re.match(r'^(\d{1,2})/(\d{1,2})$', raw)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        return f"{year}-{month:02d}-{day:02d}"

    for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y. %m. %d.", "%Y/%m/%d"]:
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

sh = gc.open_by_key(SPREADSHEET_ID)
ws = sh.worksheet(SHEET_NAME)
all_values = ws.get_all_values()

print(f"📋 헤더: {all_values[0]}")
print(f"📋 날짜 샘플: {[row[0] for row in all_values[1:4]]}")

sheet_year = 2026
m = re.match(r'(\d{2})(\d{2})', SHEET_NAME)
if m:
    sheet_year = 2000 + int(m.group(1))
    sheet_month = int(m.group(2))
    print(f"📅 시트명에서 감지: {sheet_year}년 {sheet_month}월")

date_row_map = {}
for idx, row in enumerate(all_values[1:], start=2):
    date_key = parse_sheet_date(row[0], year=sheet_year)
    if date_key:
        date_row_map[date_key] = idx

dates_sorted = sorted(date_row_map.keys())
if not dates_sorted:
    raise ValueError(f"❌ 날짜 파싱 실패! A열 샘플: {[row[0] for row in all_values[1:6]]}")

START_DATE = dates_sorted[0]
END_DATE = dates_sorted[-1]
TODAY = datetime.now().strftime("%Y-%m-%d")
API_END_DATE = min(END_DATE, TODAY)
print(f"\n✅ 날짜 매핑 완료: {len(date_row_map)}개")
print(f"   시트 기간: {START_DATE} ~ {END_DATE}")
print(f"   API 조회: {START_DATE} ~ {API_END_DATE} (오늘까지)")

# ─── TossPay 매출 데이터 ───
def fetch_toss_revenue(secret_key, start_date, end_date):
    auth_string = base64.b64encode(f"{secret_key}:".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_string}",
        "Content-Type": "application/json"
    }
    REVENUE_STATUSES = {"DONE", "PARTIAL_CANCELED"}
    daily_revenue = {}
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        params = {
            "startDate": f"{date_str}T00:00:00+09:00",
            "endDate": f"{date_str}T23:59:59+09:00",
            "limit": 100,
        }
        total_amount = 0
        page_count = 0

        while True:
            page_count += 1
            try:
                resp = requests.get(
                    "https://api.tosspayments.com/v1/transactions",
                    headers=headers, params=params, timeout=30
                )
                if resp.status_code != 200:
                    print(f"  ⚠️ API 오류 ({date_str}, 페이지{page_count}): {resp.status_code} - {resp.text[:200]}")
                    break

                data = resp.json()

                if isinstance(data, list):
                    transactions = data
                    has_more = len(data) >= 100
                    cursor_value = data[-1].get('transactionKey') if data else None
                elif isinstance(data, dict):
                    transactions = data.get('data', data.get('transactions', []))
                    has_more = data.get('hasMore', False)
                    cursor_value = data.get('lastCursor') or (transactions[-1].get('transactionKey') if transactions else None)
                else:
                    break

                if not transactions:
                    break

                for txn in transactions:
                    if txn.get("status", "") in REVENUE_STATUSES:
                        total_amount += txn.get("amount", 0)

                if (has_more or len(transactions) >= 100) and cursor_value:
                    params = {
                        "startDate": f"{date_str}T00:00:00+09:00",
                        "endDate": f"{date_str}T23:59:59+09:00",
                        "limit": 100,
                        "startingAfter": cursor_value,
                    }
                    time.sleep(0.2)
                else:
                    break
            except Exception as e:
                print(f"  ❌ 요청 실패 ({date_str}, 페이지{page_count}): {e}")
                break

        daily_revenue[date_str] = total_amount
        page_info = f" ({page_count}페이지)" if page_count > 1 else ""
        print(f"  📅 {date_str}: ₩{total_amount:,.0f}{page_info}")
        current += timedelta(days=1)
        time.sleep(0.3)

    return daily_revenue

print(f"\n🔄 토스페이 매출 조회 중... ({START_DATE} ~ {API_END_DATE})")
toss_revenue = fetch_toss_revenue(TOSS_SECRET_KEY, START_DATE, API_END_DATE)
print(f"\n✅ 토스페이 조회 완료 - 총 {len(toss_revenue)}일")
print(f"   합계: ₩{sum(toss_revenue.values()):,.0f}")

# ─── Meta Ads 비용 데이터 ───
def fetch_meta_spend(accounts, start_date, end_date, api_version="v21.0"):
    daily_spend = {}
    success_count = 0
    fail_count = 0

    for i, account in enumerate(accounts):
        account_id = account["account_id"]
        access_token = account["access_token"]
        print(f"\n  📊 계정 {i+1}/{len(accounts)}: act_{account_id}")

        url = f"https://graph.facebook.com/{api_version}/act_{account_id}/insights"
        params = {
            "fields": "spend",
            "time_range": json.dumps({"since": start_date, "until": end_date}),
            "time_increment": 1,
            "level": "account",
            "limit": 100,
            "access_token": access_token
        }

        has_next = True
        account_total = 0
        while has_next:
            try:
                resp = requests.get(url, params=params, timeout=60)
                if resp.status_code == 200:
                    result = resp.json()
                    for row in result.get("data", []):
                        date_str = row.get("date_start", "")
                        spend = float(row.get("spend", 0))
                        daily_spend[date_str] = daily_spend.get(date_str, 0) + spend
                        account_total += spend

                    next_url = result.get("paging", {}).get("next")
                    if next_url:
                        url = next_url
                        params = {}
                    else:
                        has_next = False
                    success_count += 1
                else:
                    error_msg = resp.json().get("error", {}).get("message", resp.text[:200])
                    print(f"    ❌ API 오류: {resp.status_code} - {error_msg}")
                    fail_count += 1
                    has_next = False
            except Exception as e:
                print(f"    ❌ 요청 실패: {e}")
                fail_count += 1
                has_next = False

        if account_total > 0:
            print(f"    ✅ 계정 합계: ₩{account_total:,.0f}")
        time.sleep(1)

    print(f"\n  📊 계정 성공: {success_count - fail_count}개 / 실패: {fail_count}개")
    if fail_count > 0:
        print(f"  ⚠️ 실패한 계정의 비용은 합산에서 빠져 있습니다!")

    for date_str in sorted(daily_spend.keys()):
        print(f"  📅 {date_str}: ₩{daily_spend[date_str]:,.0f}")

    return daily_spend

print(f"\n🔄 Meta Ads 비용 조회 중... ({START_DATE} ~ {API_END_DATE})")
meta_spend = fetch_meta_spend(META_ACCOUNTS, START_DATE, API_END_DATE, META_API_VERSION)
print(f"\n✅ Meta 조회 완료 - 총 {len(meta_spend)}일")
print(f"   합계: ₩{sum(meta_spend.values()):,.0f}")

# ─── Google Sheets에 데이터 입력 ───
def write_to_sheets(ws, date_row_map, toss_data, meta_data):
    updates = []
    for date_str, row_num in date_row_map.items():
        if date_str in toss_data:
            updates.append({"range": f"C{row_num}", "values": [[toss_data[date_str]]]})
        if date_str in meta_data:
            updates.append({"range": f"E{row_num}", "values": [[round(meta_data[date_str])]]})

    if updates:
        # 배치 분할 (API rate limit 방지)
        BATCH_SIZE = 50
        for i in range(0, len(updates), BATCH_SIZE):
            batch = updates[i:i + BATCH_SIZE]
            ws.batch_update(batch, value_input_option="RAW")
            if i + BATCH_SIZE < len(updates):
                time.sleep(1)
        print(f"\n✅ 총 {len(updates)}개 셀 업데이트 완료!")
    else:
        print("\n⚠️ 업데이트할 데이터가 없습니다.")

    toss_count = sum(1 for d in date_row_map if d in toss_data)
    meta_count = sum(1 for d in date_row_map if d in meta_data)
    print(f"   토스페이 매출 입력: {toss_count}일")
    print(f"   Meta 비용 입력: {meta_count}일")

print("🔄 Google Sheets에 데이터 입력 중...")
write_to_sheets(ws, date_row_map, toss_revenue, meta_spend)

# ── 요약 영역 수식 입력 ──
last_row = max(date_row_map.values())
summary_formulas = [
    {"range": "K3", "values": [[f"=SUM(B2:B{last_row})"]]},
    {"range": "K4", "values": [[f"=IFERROR(K3/SUMPRODUCT((B2:B{last_row}>0)*1)*7, 0)"]]},
    {"range": "K5", "values": [[f"=IFERROR(K3/SUMPRODUCT((B2:B{last_row}>0)*1)*30, 0)"]]},
    {"range": "K6", "values": [[f"=IFERROR(K5/K2, 0)"]]},
    {"range": "J7", "values": [["목표까지 남은 금액"]]},
    {"range": "K7", "values": [[f"=MAX(K2-K3, 0)"]]},
    {"range": "J8", "values": [["필요 일매출 (남은 일수 기준)"]]},
    {"range": "K8", "values": [[f"=IFERROR(K7/(COUNTA(A2:A{last_row})-SUMPRODUCT((B2:B{last_row}>0)*1)), 0)"]]},
]
ws.batch_update(summary_formulas, value_input_option="USER_ENTERED")
print("✅ 요약 영역 수식 입력 완료!")

print("\n🎉 매출 대시보드 업데이트 완료!")
