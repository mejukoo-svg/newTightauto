# -*- coding: utf-8 -*-
"""
backfill_thailand.py
--------------------
태국 캠페인 country/currency 라벨 보정 일회성 스크립트.
이전 detect_currency()에 태국 케이스가 없어 TWD/대만으로 잘못 분류된 레코드를 정정.

사용법:
    $env:SUPABASE_URL="https://qkvqiorazdrhtuicnpec.supabase.co"
    $env:SUPABASE_SERVICE_KEY="<service_role_key>"
    python backfill_thailand.py            # DRY RUN (조회만)
    python backfill_thailand.py --apply    # 실제 UPDATE
"""
import os, sys, json, requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://qkvqiorazdrhtuicnpec.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
if not SUPABASE_KEY:
    print("ERROR: SUPABASE_SERVICE_KEY 환경변수가 필요합니다.")
    sys.exit(1)

APPLY = "--apply" in sys.argv
TWD_RATE = 31.7
THB_RATE = 35.5
RATIO = TWD_RATE / THB_RATE  # ~0.893

H = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# 1) 조회
url = f"{SUPABASE_URL}/rest/v1/global_ad_performance_daily"
q = "select=date,campaign_name,adset_name,country,currency,spend_usd,revenue_usd,profit_usd,roas,results_mp&or=(campaign_name.ilike.%25태국%25,adset_name.ilike.%25태국%25,campaign_name.ilike.%25thailand%25)&order=date.desc"
r = requests.get(f"{url}?{q}", headers=H)
rows = r.json()
if not isinstance(rows, list):
    print("Query failed:", rows); sys.exit(1)

print(f"[조회] 영향 레코드: {len(rows)}건")
for row in rows:
    print(f"  {row['date']} {row['campaign_name'][:30]:30} country={row['country']:4} currency={row['currency']:4} spend=${row['spend_usd']} rev=${row['revenue_usd']}")

if not APPLY:
    print("\n--apply 옵션 없이 실행됨. 실제 UPDATE는 발생하지 않았습니다.")
    sys.exit(0)

# 2) 실제 UPDATE — TWD/대만으로 잘못 분류된 것만
target = [r for r in rows if r['currency'] == 'TWD']
print(f"\n[적용] currency='TWD' 인 {len(target)}건을 country='태국', currency='THB' 로 보정 + revenue 재환산")

for row in target:
    new_rev = round(row['revenue_usd'] * RATIO, 2)
    new_profit = round(new_rev - row['spend_usd'], 2)
    new_roas = round((new_rev / row['spend_usd'] * 100), 2) if row['spend_usd'] > 0 else 0
    patch = {
        "country": "태국",
        "currency": "THB",
        "revenue_usd": new_rev,
        "profit_usd": new_profit,
        "roas": new_roas,
    }
    # adset_id + date 가 PK (또는 unique)일 것
    pr = requests.patch(
        f"{url}?date=eq.{row['date']}&campaign_name=eq.{requests.utils.quote(row['campaign_name'])}",
        headers={**H, "Content-Type": "application/json", "Prefer": "return=minimal"},
        data=json.dumps(patch)
    )
    status = "OK" if pr.status_code in (200, 204) else f"FAIL({pr.status_code})"
    print(f"  {status}  {row['date']}  ${row['revenue_usd']} → ${new_rev}  ROAS {row['roas']}% → {new_roas}%")

print("\n완료. 다음 파이프라인 실행 시 정확한 일별 환율로 자동 재계산됩니다.")
