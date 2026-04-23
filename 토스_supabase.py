# -*- coding: utf-8 -*-
"""
토스_supabase.py
================
Toss Payments API → toss_daily_revenue 직통 파이프라인

Toss Payments `/v1/transactions` endpoint로 일별 결제완료/취소 집계:
  - total_amount  : DONE + PARTIAL_CANCELED status 합
  - cancel_amount : CANCELED + PARTIAL_CANCELED 취소분
  - net_amount    : total - cancel
  - total_count   : 결제 건수

환경변수:
  TOSS_SECRET_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY
  REFRESH_DAYS (기본 10), FULL_REFRESH
"""

import os, re, sys, time, math, logging, base64
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict

import requests as req_lib

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

TOSS_SECRET_KEY = os.environ["TOSS_SECRET_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))
START = date(2025, 1, 1) if FULL_REFRESH else TODAY - timedelta(days=REFRESH_DAYS - 1)
END = TODAY
log.info(f"📅 Toss 수집: {START} ~ {END}")

# ============================================================
def fetch_toss_day(d: date):
    """하루치 transactions 전부 페이지네이션 조회"""
    auth = base64.b64encode(f"{TOSS_SECRET_KEY}:".encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
    dstr = d.strftime("%Y-%m-%d")
    total_amount = 0
    total_count = 0
    cancel_amount = 0
    cancel_count = 0
    cursor = None
    page = 0

    while True:
        page += 1
        params = {
            "startDate": f"{dstr}T00:00:00+09:00",
            "endDate":   f"{dstr}T23:59:59+09:00",
            "limit": 100,
        }
        if cursor:
            params["startingAfter"] = cursor
        try:
            resp = req_lib.get("https://api.tosspayments.com/v1/transactions",
                               headers=headers, params=params, timeout=30)
            if resp.status_code != 200:
                log.error(f"  ❌ {dstr} p{page} HTTP {resp.status_code}: {resp.text[:200]}")
                break
            data = resp.json()
            if isinstance(data, list):
                txns = data; has_more = len(data) >= 100
                cursor = data[-1].get('transactionKey') if data else None
            else:
                txns = data.get('data', data.get('transactions', []))
                has_more = data.get('hasMore', False)
                cursor = data.get('lastCursor') or (txns[-1].get('transactionKey') if txns else None)
            if not txns: break
            for t in txns:
                st = t.get("status", "")
                amt = float(t.get("amount", 0))
                if st in ("DONE", "PARTIAL_CANCELED"):
                    total_amount += amt
                    total_count += 1
                if st in ("CANCELED", "PARTIAL_CANCELED"):
                    # cancel amount — PARTIAL_CANCELED의 경우 amount가 남은 금액이므로 별도 계산 필요
                    # 간단히: CANCELED는 전액 취소로 간주
                    if st == "CANCELED":
                        cancel_amount += amt
                        cancel_count += 1
                    # PARTIAL_CANCELED 취소분은 API 응답에 별도 필드 (canceledAmount) 있으면 반영
                    canceled_amt = t.get("canceledAmount")
                    if canceled_amt is not None:
                        try:
                            cancel_amount += float(canceled_amt)
                        except: pass
            if not (has_more or len(txns) >= 100) or not cursor:
                break
            time.sleep(0.2)
        except Exception as e:
            log.error(f"  ❌ {dstr} p{page} 예외: {e}")
            break

    net_amount = total_amount - cancel_amount
    net_count = total_count - cancel_count
    return {
        "date": dstr,
        "total_amount": round(total_amount, 2),
        "total_count": total_count,
        "cancel_amount": round(cancel_amount, 2),
        "cancel_count": cancel_count,
        "net_amount": round(net_amount, 2),
        "net_count": net_count,
    }

# ============================================================
class SupabaseClient:
    def __init__(self, url, key):
        clean = re.sub(r'[^\x20-\x7E]', '', url).strip().rstrip("/")
        if not clean.startswith("http"):
            clean = "https://" + clean
        self.base = clean
        self.headers = {
            "apikey": key.strip(),
            "Authorization": f"Bearer {key.strip()}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }
    def upsert(self, table, records, chunk=500):
        if not records: return 0
        url = f"{self.base}/rest/v1/{table}"
        ok = 0
        for i in range(0, len(records), chunk):
            batch = records[i:i+chunk]
            resp = req_lib.post(url, headers=self.headers, json=batch, timeout=60)
            if resp.status_code in (200, 201):
                ok += len(batch)
                log.info(f"  ✅ upsert {ok}/{len(records)}")
            else:
                log.error(f"  ❌ HTTP {resp.status_code}: {resp.text[:300]}")
            time.sleep(0.3)
        return ok

# ============================================================
def main():
    log.info("=" * 60)
    log.info("🚀 Toss → Supabase (toss_daily_revenue)")
    log.info("=" * 60)
    records = []
    d = START
    while d <= END:
        r = fetch_toss_day(d)
        log.info(f"  📅 {r['date']}: 총 ₩{r['total_amount']:,.0f} ({r['total_count']}건) | 취소 ₩{r['cancel_amount']:,.0f} | 순매출 ₩{r['net_amount']:,.0f}")
        records.append(r)
        d += timedelta(days=1)
        time.sleep(0.3)
    log.info(f"📦 records: {len(records)}")
    SupabaseClient(SUPABASE_URL, SUPABASE_KEY).upsert("toss_daily_revenue", records)
    log.info("✅ 완료")

if __name__ == "__main__":
    main()
