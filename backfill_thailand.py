# -*- coding: utf-8 -*-
"""
backfill_thailand.py
--------------------
태국 캠페인 country/currency/product 라벨 보정 일회성 스크립트.

이전 detect_currency()/extract_product()/SKIP_WORDS에 태국 케이스가 없어
- country: '대만' (잘못)
- currency: 'TWD' (잘못)
- product: '태국' (잘못 — 실제 상품은 캠페인명 두번째 토큰. 예: 무당)
- revenue_usd: TWD 환율로 환산되어 약 10% 과대 계상
로 분류되어 있던 레코드를 정정.

사용법:
    $env:SUPABASE_URL="https://qkvqiorazdrhtuicnpec.supabase.co"
    $env:SUPABASE_SERVICE_KEY="<service_role_key>"
    py backfill_thailand.py            # DRY RUN (조회만)
    py backfill_thailand.py --apply    # 실제 UPDATE
"""
import os, sys, json, re, requests

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

# 메인 파이프라인 글로벌_세트별_supabase.py 와 동일 (태국 추가됨)
SKIP_WORDS = {"tw","kr","hk","my","sg","id","jp","th","vn","ph","asia","taiwan","japan","hongkong","korea",
    "singapore","malaysia","thailand","broad","interest","lookalike","retarget","custom","asc","cbo","abo",
    "dpa","advantage","campaign","adset","ad","ads","set","purchase","conversion","traffic",
    "v1","v2","v3","v4","v5","test","new","old","copy","sajutight","ttsaju","saju","tight",
    "대만","일본","홍콩","한국","국내","글로벌","태국","台灣","台湾","日本","香港"}

def extract_product(adset_name, campaign_name=None):
    """글로벌_세트별_supabase.py 의 동일 함수 복제 (태국 SKIP 반영된 버전)"""
    for name in [campaign_name, adset_name]:
        if not name: continue
        parts = re.split(r'[-_\s]+', str(name).lower().strip())
        candidates = [p for p in parts if p and p not in SKIP_WORDS and len(p) > 1 and not re.match(r'^\d+$', p)]
        if candidates: return candidates[0]
    return "기타"


# 1) 조회 — 영향 받을 레코드
url = f"{SUPABASE_URL}/rest/v1/global_ad_performance_daily"
q = ("select=date,campaign_name,adset_name,country,currency,product,spend_usd,revenue_usd,profit_usd,roas,results_mp"
     "&or=(campaign_name.ilike.%25태국%25,adset_name.ilike.%25태국%25,campaign_name.ilike.%25thailand%25)"
     "&order=date.desc")
r = requests.get(f"{url}?{q}", headers=H)
rows = r.json()
if not isinstance(rows, list):
    print("Query failed:", rows); sys.exit(1)

print(f"[조회] 영향 레코드: {len(rows)}건\n")
for row in rows:
    new_product = extract_product(row['adset_name'], row['campaign_name'])
    print(f"  {row['date']}  {row['campaign_name'][:30]:30}")
    print(f"    country  : {row['country']:4} -> 태국")
    print(f"    currency : {row['currency']:4} -> THB")
    print(f"    product  : {row['product']:6} -> {new_product}")
    print(f"    rev      : ${row['revenue_usd']:.2f} -> ${row['revenue_usd']*RATIO:.2f}")

if not APPLY:
    print("\n--apply 옵션 없이 실행됨. 실제 UPDATE는 발생하지 않았습니다.")
    sys.exit(0)

# 2) 실제 UPDATE — TWD/대만으로 잘못 분류된 것만
target = [r for r in rows if r['currency'] == 'TWD']
print(f"\n[적용] currency='TWD' 인 {len(target)}건 보정 시작\n")

for row in target:
    new_product = extract_product(row['adset_name'], row['campaign_name'])
    new_rev = round(row['revenue_usd'] * RATIO, 2)
    new_profit = round(new_rev - row['spend_usd'], 2)
    new_roas = round((new_rev / row['spend_usd'] * 100), 2) if row['spend_usd'] > 0 else 0
    patch = {
        "country": "태국",
        "currency": "THB",
        "product": new_product,
        "revenue_usd": new_rev,
        "profit_usd": new_profit,
        "roas": new_roas,
    }
    pr = requests.patch(
        f"{url}?date=eq.{row['date']}&campaign_name=eq.{requests.utils.quote(row['campaign_name'])}",
        headers={**H, "Content-Type": "application/json", "Prefer": "return=minimal"},
        data=json.dumps(patch)
    )
    status = "OK" if pr.status_code in (200, 204) else f"FAIL({pr.status_code})"
    print(f"  {status}  {row['date']}  product={new_product}  rev ${row['revenue_usd']}->${new_rev}  ROAS {row['roas']}%->{new_roas}%")

print("\n완료. 다음 파이프라인 실행 시 정확한 일별 환율로 자동 재계산됩니다.")
