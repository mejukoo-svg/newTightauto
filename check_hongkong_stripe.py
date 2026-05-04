"""Diagnose: why Hong Kong revenue is ~95% lower than expected.

Hypothesis: HKD is pegged to USD (1USD=7.8HKD) so many HK customers pay in USD.
Current pipeline filters by currency only, so USD charges with HK billing
address are dropped entirely.

Run after `STRIPE_API_KEY` is exported.
"""
import os, sys
import requests
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

SB_URL = "https://qkvqiorazdrhtuicnpec.supabase.co"
SB_KEY = "sb_publishable_43NkUJjcYzcBUiJhnKVHXw_eWTuZU2g"
H = {"apikey": SB_KEY, "Authorization": "Bearer " + SB_KEY}

KST = timezone(timedelta(hours=9))

# 1) Show current Supabase HK aggregates
print("=== global_stripe_daily — 홍콩 (last 30 days) ===")
since = (datetime.now(KST) - timedelta(days=30)).strftime("%Y-%m-%d")
r = requests.get(
    f"{SB_URL}/rest/v1/global_stripe_daily"
    f"?country=eq.%ED%99%8D%EC%BD%A9&date=gte.{since}&order=date.desc",
    headers=H,
)
rows = r.json()
hk_total_krw = sum(row.get("revenue_krw", 0) for row in rows)
print(f"  rows: {len(rows)}, total: ₩{hk_total_krw:,}")
for row in rows[:10]:
    print(f"  {row['date']}  ₩{row.get('revenue_krw',0):>10,}  USD ${row.get('revenue_usd',0)}")

# 2) Raw Stripe analysis — billing country vs currency
api_key = os.environ.get("STRIPE_API_KEY")
if not api_key:
    print("\n  ⚠️ STRIPE_API_KEY not set — set it to run raw analysis")
    sys.exit(0)

try:
    import stripe
except ImportError:
    print("  stripe lib not installed (pip install stripe)")
    sys.exit(0)

stripe.api_key = api_key
end = datetime.now(KST)
start = end - timedelta(days=30)
start_ts = int(start.timestamp()); end_ts = int(end.timestamp())

print(f"\n=== Raw Stripe charges {start:%Y-%m-%d} ~ {end:%Y-%m-%d} ===")
charges = []; has_more = True; starting_after = None
while has_more:
    params = {"limit": 100, "created": {"gte": start_ts, "lte": end_ts}, "status": "succeeded"}
    if starting_after: params["starting_after"] = starting_after
    resp = stripe.Charge.list(**params)
    charges.extend(resp.data)
    has_more = resp.has_more
    if resp.data: starting_after = resp.data[-1].id

print(f"  Total succeeded charges: {len(charges)}")

# Breakdown by (currency, billing_country)
breakdown = Counter()
hk_addr_charges = []  # billing address = HK
hk_addr_by_currency = defaultdict(lambda: {"count": 0, "amount_local": 0.0})

DIVISOR = {"jpy": 1, "twd": 100, "hkd": 100, "usd": 100}

for ch in charges:
    currency = (ch.currency or "").lower()
    bd = getattr(ch, "billing_details", None)
    addr_country = None
    if bd:
        addr = getattr(bd, "address", None)
        if addr: addr_country = getattr(addr, "country", None)
    breakdown[(currency, addr_country)] += 1

    if addr_country == "HK":
        amt_local = ch.amount / DIVISOR.get(currency, 100)
        hk_addr_charges.append({
            "id": ch.id, "currency": currency, "amount": amt_local,
            "date": datetime.fromtimestamp(ch.created, tz=KST).strftime("%Y-%m-%d %H:%M"),
            "description": (ch.description or "")[:60],
        })
        hk_addr_by_currency[currency]["count"] += 1
        hk_addr_by_currency[currency]["amount_local"] += amt_local

print("\n  Top 20 (currency, billing_country):")
for (c, cc), n in breakdown.most_common(20):
    print(f"    currency={c!r:8} addr={cc!r:8}  -> {n} charges")

print(f"\n  📍 Charges with billing address country='HK': {len(hk_addr_charges)}")
print("  HK billing — currency breakdown:")
for cur, stats in sorted(hk_addr_by_currency.items(), key=lambda x: -x[1]["count"]):
    print(f"    {cur.upper():4}  {stats['count']:>4}건  amount_local={stats['amount_local']:,.2f}")

# Show recent HK charges
print("\n  Recent HK billing charges (top 20):")
for c in sorted(hk_addr_charges, key=lambda x: x["date"], reverse=True)[:20]:
    print(f"    {c['date']}  {c['currency'].upper():4} {c['amount']:>10,.2f}  {c['id']}")

# Estimate impact
hkd_only = hk_addr_by_currency.get("hkd", {}).get("count", 0)
usd_hk = hk_addr_by_currency.get("usd", {}).get("count", 0)
total_hk = sum(s["count"] for s in hk_addr_by_currency.values())
if total_hk:
    print(f"\n  📊 HK 거주자 결제 중 currency 분포:")
    print(f"      HKD: {hkd_only}건 ({hkd_only/total_hk*100:.1f}%) — 현재 로직으로 잡힘")
    print(f"      USD: {usd_hk}건  ({usd_hk/total_hk*100:.1f}%) — 현재 로직 누락")
    print(f"      기타: {total_hk-hkd_only-usd_hk}건")
    print(f"\n  → billing-address 우선 분류 적용 시 누락 {usd_hk}건 복구 예상")
