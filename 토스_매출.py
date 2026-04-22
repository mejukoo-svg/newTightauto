"""
Toss Payments API → toss_daily_revenue 동기화
─────────────────────────────────────────────────────────
  ㆍ대상 테이블: public.toss_daily_revenue
  ㆍ컬럼:        date, total_amount, total_count,
                 cancel_amount, cancel_count,
                 net_amount,    net_count
  ㆍToss /v1/transactions 엔드포인트 사용 (cursor 페이지네이션)

  requirements.txt:
    requests
    supabase

  필수 환경변수:
    TOSS_SECRET_KEY            # 토스페이먼츠 시크릿 키 (라이브)
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY  # RLS 우회

  선택 환경변수:
    REFRESH_DAYS=10            # 최근 N일 (기본 10)
    DRY_RUN=true               # 쓰기 스킵, 로그만
"""
import os, sys, base64, time
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)


def require_env(name):
    v = os.environ.get(name)
    if not v:
        print(f"❌ 환경변수 '{name}' 미설정")
        sys.exit(1)
    return v


TOSS_SECRET               = require_env("TOSS_SECRET_KEY")
SUPABASE_URL              = require_env("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = require_env("SUPABASE_SERVICE_ROLE_KEY")
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))
DRY_RUN      = os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes")

KST = timezone(timedelta(hours=9))

try:
    from supabase import create_client, Client
except ImportError:
    print("❌ supabase 패키지 미설치: pip install supabase")
    sys.exit(1)

sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ── Toss Basic Auth ──
auth_b64 = base64.b64encode((TOSS_SECRET + ":").encode()).decode()
TOSS_HEADERS = {
    "Authorization": f"Basic {auth_b64}",
    "Content-Type": "application/json",
}
TOSS_URL = "https://api.tosspayments.com/v1/transactions"

# ── 조회 기간 (KST) ──
end_kst   = datetime.now(KST).date()
start_kst = end_kst - timedelta(days=REFRESH_DAYS)

print("=" * 60)
print("🚀 Toss → Supabase (toss_daily_revenue)")
print(f"   기간: {start_kst} ~ {end_kst} (최근 {REFRESH_DAYS}일)")
print(f"   DRY_RUN: {DRY_RUN}")
print("=" * 60)


# ══════════════════════════════════════════════════════════
# Toss 거래 수집 (cursor 페이지네이션)
# ══════════════════════════════════════════════════════════
def fetch_toss_transactions(start_d, end_d):
    all_tx = []
    starting_after = None
    page = 0

    while True:
        params = {
            "startDate": start_d.isoformat(),
            "endDate":   end_d.isoformat(),
            "limit": 10000,
        }
        if starting_after:
            params["startingAfter"] = starting_after

        r = requests.get(TOSS_URL, headers=TOSS_HEADERS, params=params, timeout=120)
        if r.status_code != 200:
            print(f"❌ Toss API 오류 {r.status_code}: {r.text[:300]}")
            sys.exit(1)

        data = r.json()
        if not data:
            break

        all_tx.extend(data)
        page += 1
        print(f"  페이지 {page}: +{len(data)}건 (누적 {len(all_tx)})")

        if len(data) < 10000:
            break

        starting_after = data[-1].get("transactionKey")
        if not starting_after:
            break
        time.sleep(0.3)

    return all_tx


print("\n🔍 Toss 거래 수집 중...")
all_tx = fetch_toss_transactions(start_kst, end_kst)
print(f"✅ 총 {len(all_tx)}건 수집")

if not all_tx:
    print("⚠️ 조회 기간에 거래 없음 — 종료")
    sys.exit(0)

# 샘플 로그 (구조 확인용)
print(f"\n🔎 샘플 거래 (첫 1건):")
if all_tx:
    sample = all_tx[0]
    print(f"  status:        {sample.get('status')}")
    print(f"  amount:        {sample.get('amount')}")
    print(f"  transactedAt:  {sample.get('transactedAt')}")
    print(f"  method:        {sample.get('method')}")


# ══════════════════════════════════════════════════════════
# KST 날짜별 집계
# ══════════════════════════════════════════════════════════
print("\n📊 일별 집계 중...")
daily = defaultdict(lambda: {
    "total_amount":  0,
    "total_count":   0,
    "cancel_amount": 0,
    "cancel_count":  0,
})

status_counts = defaultdict(int)
skipped = 0

for tx in all_tx:
    ts = tx.get("transactedAt") or tx.get("approvedAt")
    if not ts:
        skipped += 1
        continue

    try:
        # Toss는 ISO 8601 (2026-04-22T15:32:11+09:00) 으로 반환
        dt_utc = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        dt_kst = dt_utc.astimezone(KST).date()
    except Exception:
        skipped += 1
        continue

    dstr   = dt_kst.isoformat()
    amount = int(tx.get("amount", 0) or 0)
    status = (tx.get("status") or "").upper()
    status_counts[status] += 1

    if status == "DONE":
        daily[dstr]["total_amount"] += amount
        daily[dstr]["total_count"]  += 1
    elif status in ("CANCELED", "CANCELLED", "PARTIAL_CANCELED"):
        daily[dstr]["cancel_amount"] += amount
        daily[dstr]["cancel_count"]  += 1
    # 나머지 (READY, IN_PROGRESS, WAITING_FOR_DEPOSIT, EXPIRED, ABORTED) → 스킵

print(f"  거래 status 분포: {dict(status_counts)}")
if skipped:
    print(f"  ⚠️ 스킵: {skipped}건 (timestamp 파싱 실패)")


# ══════════════════════════════════════════════════════════
# Supabase upsert
# ══════════════════════════════════════════════════════════
records = []
for date_str in sorted(daily.keys()):
    d = daily[date_str]
    net_amount = d["total_amount"] - d["cancel_amount"]
    net_count  = d["total_count"]  - d["cancel_count"]
    records.append({
        "date":          date_str,
        "total_amount":  d["total_amount"],
        "total_count":   d["total_count"],
        "cancel_amount": d["cancel_amount"],
        "cancel_count":  d["cancel_count"],
        "net_amount":    net_amount,
        "net_count":     net_count,
    })

print(f"\n📅 {len(records)}일 집계 결과:")
for r in records:
    tot = r["total_amount"]
    can = r["cancel_amount"]
    net = r["net_amount"]
    print(f"  {r['date']}: 총매출 ₩{tot:>12,}  취소 ₩{can:>10,}  순매출 ₩{net:>12,}  ({r['net_count']}건)")

if DRY_RUN:
    print("\n[DRY_RUN] Supabase 쓰기 스킵")
    sys.exit(0)

if not records:
    print("\n⚠️ 업서트할 데이터 없음")
    sys.exit(0)

print("\n📤 toss_daily_revenue 업서트 중...")
try:
    sb.table("toss_daily_revenue").upsert(
        records,
        on_conflict="date",
    ).execute()
    print(f"✅ {len(records)}건 업서트 완료")
except Exception as e:
    print(f"❌ 업서트 실패: {e}")
    sys.exit(1)

# ── 요약 ──
grand_net = sum(r["net_amount"] for r in records)
grand_cnt = sum(r["net_count"] for r in records)
print("\n" + "=" * 60)
print("🎉 완료")
print(f"   기간 합산 순매출:  ₩{grand_net:,}")
print(f"   기간 합산 거래수:  {grand_cnt}건")
print("=" * 60)
