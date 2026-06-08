# -*- coding: utf-8 -*-
"""
메타_시간대분석.py  (읽기 전용 — Supabase/시트 미수정)
======================================================
Meta Marketing API 로 전체 광고 데이터를 받아 **상품별 × 시간대(0~23시)별**
판매수(구매수)와 구매당비용(CPA)을 집계한다.

기간: 2026-05-01 ~ 2026-06-08 (고정, --since/--until 로 override 가능)
시간대: Meta breakdowns=hourly_stats_aggregated_by_advertiser_time_zone (광고주 TZ 기준 0~23시)
상품: 캠페인/광고세트 이름에서 추출 (국내_세트별_supabase.py 의 extract_product 와 동일 규칙)
판매수: actions 중 purchase 류 합
구매당비용(CPA): spend / 판매수  (통화는 계정 통화 — 국내 KRW, 글로벌 USD)

환경변수(토큰):
  META_TOKEN_1   (국내 act_1270614404675034/act_707835224206178, 글로벌 act_1054081590008088)
  META_TOKEN_2   (국내 act_1808141386564262)
  META_TOKEN_GlobalTT / META_TOKEN_4 / META_TOKEN_3  (글로벌 act_2677.../act_1335...)
  META_TOKEN_ACT_9937  (대만 act_993712016404855)

옵션:
  --scope kr|global|all   (기본 all — 토큰 있는 계정만 자동 시도)
  --since YYYY-MM-DD --until YYYY-MM-DD
  --level adset|campaign  (기본 adset)
"""
import os, re, sys, time, json
from collections import defaultdict
import requests

API = "v21.0"
BASE = f"https://graph.facebook.com/{API}"

# 계정 → (토큰env, 통화, 권역)
ACCOUNTS = [
    ("act_1270614404675034", "META_TOKEN_1",      "KRW", "kr"),
    ("act_707835224206178",  "META_TOKEN_1",      "KRW", "kr"),
    ("act_1808141386564262", "META_TOKEN_2",      "KRW", "kr"),
    ("act_1054081590008088", "META_TOKEN_1",      "USD", "global"),
    ("act_2677707262628563", "META_TOKEN_GlobalTT","USD","global"),
    ("act_1335040608536838", "META_TOKEN_GlobalTT","USD","global"),
    ("act_993712016404855",  "META_TOKEN_ACT_9937","USD","global"),
]

def argval(flag, default=None):
    return sys.argv[sys.argv.index(flag)+1] if flag in sys.argv else default

SCOPE = argval("--scope", "all")
SINCE = argval("--since", "2026-05-01")
UNTIL = argval("--until", "2026-06-08")
LEVEL = argval("--level", "adset")
PURCHASE_TYPES = {"purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase"}

def token_for(env_name):
    # GlobalTT fallback chain
    if env_name == "META_TOKEN_GlobalTT":
        return os.environ.get("META_TOKEN_GlobalTT") or os.environ.get("META_TOKEN_4") or os.environ.get("META_TOKEN_3") or ""
    return os.environ.get(env_name, "")

def _strip_leading_emojis(text):
    i = 0
    while i < len(text):
        c = text[i]
        if ("가" <= c <= "힣" or "ㄱ" <= c <= "ㅣ" or c.isalnum() or c in ".%"):
            break
        i += 1
    return text[i:].strip()

def extract_product(adset_name, campaign_name=""):
    for source in [campaign_name, adset_name]:
        if not source: continue
        cleaned = _strip_leading_emojis(str(source).strip())
        if not cleaned: continue
        for token in re.split(r"[_\s\-/|,()\[\]]+", cleaned):
            token = token.strip()
            if not token: continue
            if re.match(r"^\d+$", token): continue
            return token
    return "기타"

def fetch_insights(acc, token):
    url = f"{BASE}/{acc}/insights"
    params = {
        "access_token": token,
        "level": LEVEL,
        "time_range": json.dumps({"since": SINCE, "until": UNTIL}),
        "breakdowns": "hourly_stats_aggregated_by_advertiser_time_zone",
        "fields": "campaign_name,adset_name,spend,actions",
        "limit": "500",
    }
    rows, page = [], 0
    while url:
        r = requests.get(url, params=params if page == 0 else None, timeout=120)
        if r.status_code != 200:
            print(f"  ❌ {acc} HTTP {r.status_code}: {r.text[:200]}")
            break
        j = r.json()
        rows.extend(j.get("data", []))
        nxt = j.get("paging", {}).get("next")
        url = nxt; params = None; page += 1
        if page > 200: break
    return rows

def hour_of(bucket):
    # "00:00:00 - 00:59:59" → 0
    m = re.match(r"^(\d{1,2})", str(bucket or ""))
    return int(m.group(1)) if m else -1

def purchases_of(actions):
    tot = 0.0
    for a in actions or []:
        if a.get("action_type") in PURCHASE_TYPES:
            try: tot += float(a.get("value", 0))
            except: pass
    return tot

def main():
    accts = [a for a in ACCOUNTS if SCOPE in ("all", a[3])]
    # (product, hour) -> {spend, purchases}; product->currency
    agg = defaultdict(lambda: {"spend": 0.0, "purch": 0.0})
    prod_cur = {}
    used = 0
    for acc, envn, cur, region in accts:
        tok = token_for(envn)
        if not tok:
            print(f"  ⏭  {acc} ({region}) — 토큰({envn}) 없음, 스킵"); continue
        print(f"  📡 {acc} ({region}, {cur}) fetch…")
        rows = fetch_insights(acc, tok)
        print(f"     rows={len(rows)}")
        used += 1
        for row in rows:
            prod = extract_product(row.get("adset_name",""), row.get("campaign_name",""))
            prod_cur.setdefault(prod, cur)
            h = hour_of(row.get("hourly_stats_aggregated_by_advertiser_time_zone"))
            sp = float(row.get("spend", 0) or 0)
            pu = purchases_of(row.get("actions"))
            agg[(prod, h)]["spend"] += sp
            agg[(prod, h)]["purch"] += pu
    if not used:
        print("\n❌ 사용 가능한 Meta 토큰이 없습니다. META_TOKEN_1 등 env 를 설정하세요.")
        return

    # 상품별 합계로 정렬
    prod_tot = defaultdict(lambda: {"spend":0.0,"purch":0.0})
    for (p,h),v in agg.items():
        prod_tot[p]["spend"]+=v["spend"]; prod_tot[p]["purch"]+=v["purch"]
    prods = sorted(prod_tot, key=lambda p:-prod_tot[p]["purch"])

    print(f"\n{'='*100}\n기간 {SINCE}~{UNTIL} · 상품별×시간대(0~23시) 판매수 / 구매당비용(CPA)\n{'='*100}")
    for p in prods:
        cur = prod_cur.get(p,"")
        t = prod_tot[p]
        if t["purch"] < 1 and t["spend"] < 1: continue
        cpa = t["spend"]/t["purch"] if t["purch"]>0 else 0
        print(f"\n■ {p}  ({cur})  — 총 판매 {t['purch']:.0f}건 · 지출 {t['spend']:,.0f} · CPA {cpa:,.0f}")
        print(f"   {'시':>3} {'판매수':>7} {'지출':>12} {'구매당비용':>12}")
        for h in range(24):
            v = agg.get((p,h))
            if not v or (v["purch"]<1 and v["spend"]<1): continue
            cpa_h = v["spend"]/v["purch"] if v["purch"]>0 else 0
            print(f"   {h:>3} {v['purch']:>7.0f} {v['spend']:>12,.0f} {cpa_h:>12,.0f}")

    # 전체 상품 합산 시간대 요약
    print(f"\n{'='*100}\n[전체 합산] 시간대별 (통화 혼재 주의 — KRW/USD 별도 보는 게 정확)\n{'='*100}")
    by_h = defaultdict(lambda: {"spend":0.0,"purch":0.0})
    for (p,h),v in agg.items():
        by_h[h]["spend"]+=v["spend"]; by_h[h]["purch"]+=v["purch"]
    print(f"   {'시':>3} {'판매수':>8} {'지출(혼재)':>14}")
    for h in range(24):
        v=by_h.get(h)
        if not v: continue
        print(f"   {h:>3} {v['purch']:>8.0f} {v['spend']:>14,.0f}")

if __name__ == "__main__":
    main()
