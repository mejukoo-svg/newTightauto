# -*- coding: utf-8 -*-
"""
네이버_mp_supabase.py
=====================
Mixpanel 결제완료 이벤트 → utm_source에 'naver' 포함된 것만 필터 →
일자별 매출/건수 집계 → naver_daily_mp 테이블 upsert

네이버 광고 매출 attribution:
  - 네이버 SA API의 convAmt는 네이버 자체 전환추적이라 Mixpanel(실제 결제)과 차이날 수 있음
  - Mixpanel 기준으로 '실제 결제완료 이벤트 중 utm_source가 naver인 것'을 합산

필터 규칙 (대소문자 무시):
  properties.utm_source ∈ {naver, Naver, NAVER, 네이버}
  OR properties.$initial_utm_source ∈ 위와 동일

환경변수:
  MIXPANEL_PROJECT_ID / MIXPANEL_USERNAME / MIXPANEL_SECRET
  SUPABASE_URL / SUPABASE_SERVICE_KEY
  REFRESH_DAYS (기본 10), FULL_REFRESH
"""

import os, re, sys, json, time, math, logging
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict

import requests as req_lib

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME   = os.environ["MIXPANEL_USERNAME"]
MIXPANEL_SECRET     = os.environ["MIXPANEL_SECRET"]
MIXPANEL_EVENTS     = ["결제완료", "payment_complete"]

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))
START = date(2025, 1, 1) if FULL_REFRESH else TODAY - timedelta(days=REFRESH_DAYS - 1)
END = TODAY
log.info(f"📅 Mixpanel(naver) 수집: {START} ~ {END}")

NAVER_UTM_VALUES = {"naver", "네이버"}   # lowercase 비교용

def is_naver_event(props: dict) -> bool:
    """utm_source 또는 initial_utm_source가 naver인지"""
    for key in ["utm_source", "$initial_utm_source",
                "UTM_Source", "utm_Source", "UTM Source"]:
        v = props.get(key)
        if v and str(v).strip().lower() in NAVER_UTM_VALUES:
            return True
    return False

def fetch_mixpanel(from_date: date, to_date: date):
    """Mixpanel export API로 기간 내 결제완료 이벤트 일괄 다운로드"""
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "event": json.dumps(MIXPANEL_EVENTS),
        "project_id": MIXPANEL_PROJECT_ID,
    }
    log.info(f"  📡 Mixpanel export {params['from_date']} ~ {params['to_date']}")
    for attempt in range(4):
        try:
            resp = req_lib.get(url, params=params,
                               auth=(MIXPANEL_USERNAME, MIXPANEL_SECRET), timeout=300)
            if resp.status_code == 429:
                w = 30 + attempt * 30
                log.warning(f"  ⏳ 429 → {w}s"); time.sleep(w); continue
            if resp.status_code != 200:
                log.error(f"  ❌ HTTP {resp.status_code}: {resp.text[:300]}"); return []
            lines = [l for l in resp.text.splitlines() if l.strip()]
            log.info(f"  📊 total events: {len(lines)}")
            return lines
        except Exception as e:
            log.error(f"  ❌ Mixpanel 예외: {e}"); time.sleep(5)
    return []

def aggregate(lines):
    """events → daily aggregate (date, revenue, count) for naver-attributed only"""
    agg = defaultdict(lambda: {"revenue":0.0, "count":0})
    sample_seen = 0
    debug_keys_logged = False
    utm_source_seen = defaultdict(int)
    referrer_seen = defaultdict(int)
    initial_referrer_seen = defaultdict(int)
    first_event_props = None

    for line in lines:
        try:
            ev = json.loads(line)
        except:
            continue
        props = ev.get("properties", {})

        # 첫 이벤트의 전체 properties 키 덤프
        if first_event_props is None:
            first_event_props = list(props.keys())

        # 분포 수집
        v = props.get("utm_source");
        if v: utm_source_seen[str(v)[:30]] += 1
        v = props.get("$initial_utm_source")
        if v: utm_source_seen[str(v)[:30]] += 1
        v = props.get("$initial_referring_domain")
        if v: initial_referrer_seen[str(v)[:50]] += 1
        v = props.get("$referrer") or props.get("referrer")
        if v: referrer_seen[str(v)[:50]] += 1

        # 네이버 필터 - 기존 + 확장된 매칭
        is_naver = is_naver_event(props)
        # 보조 매칭: referring_domain 에 naver.com 포함
        if not is_naver:
            for k in ("$initial_referring_domain", "referring_domain"):
                rd = props.get(k)
                if rd and "naver" in str(rd).lower():
                    is_naver = True; break
        # 보조 매칭: $initial_referrer 에 naver 포함
        if not is_naver:
            for k in ("$initial_referrer", "referrer", "$referrer"):
                rf = props.get(k)
                if rf and "naver.com" in str(rf).lower():
                    is_naver = True; break

        if not is_naver:
            continue
        # 일자 (KST)
        ts = props.get("time", 0)
        if not ts: continue
        dt_kst = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)
        d_iso = dt_kst.date().isoformat()
        # 금액
        amt = 0.0
        for k in ("amount", "결제금액", "value"):
            v = props.get(k)
            if v is not None:
                try: amt = float(v); break
                except: pass
        if amt <= 0: continue
        agg[d_iso]["revenue"] += amt
        agg[d_iso]["count"]   += 1
        if sample_seen < 3:
            log.info(f"    📍 sample: date={d_iso} amt=₩{amt:,.0f} "
                     f"utm_source={props.get('utm_source')!r} "
                     f"init_ref_dom={props.get('$initial_referring_domain')!r}")
            sample_seen += 1

    log.info(f"  🔍 첫 이벤트 properties keys ({len(first_event_props or [])}개):")
    if first_event_props:
        # 네이버 관련일 것 같은 키만 출력
        for k in first_event_props:
            if any(tok in k.lower() for tok in ("utm","ref","source","camp","medium","네이버","naver")):
                log.info(f"    - {k}")
        # 전체 키 일부 출력 (처음 30개)
        log.info(f"    (전체: {first_event_props[:30]})")

    log.info(f"  🔍 utm_source 분포: {dict(sorted(utm_source_seen.items(), key=lambda x:-x[1])[:15])}")
    log.info(f"  🔍 initial_referring_domain 분포: {dict(sorted(initial_referrer_seen.items(), key=lambda x:-x[1])[:15])}")
    log.info(f"  🔍 referrer 분포: {dict(sorted(referrer_seen.items(), key=lambda x:-x[1])[:15])}")
    return dict(agg)

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
        return ok

def main():
    log.info("=" * 60)
    log.info("🚀 Mixpanel(naver) → Supabase (naver_daily_mp)")
    log.info("=" * 60)
    lines = fetch_mixpanel(START, END)
    if not lines:
        log.warning("  ⚠️ 빈 결과"); return
    agg = aggregate(lines)
    log.info(f"📦 날짜수={len(agg)}")
    records = [{
        "date": d,
        "revenue": round(v["revenue"], 2),
        "purchase_count": v["count"],
    } for d, v in sorted(agg.items())]
    for r in records:
        log.info(f"   {r['date']}: ₩{r['revenue']:,.0f} ({r['purchase_count']}건)")
    # 기간 내 빈 날짜도 0으로 채워서 upsert (대시보드 일관성 위해)
    existing_dates = {r["date"] for r in records}
    d = START
    while d <= END:
        iso = d.isoformat()
        if iso not in existing_dates:
            records.append({"date": iso, "revenue": 0, "purchase_count": 0})
        d += timedelta(days=1)
    SupabaseClient(SUPABASE_URL, SUPABASE_KEY).upsert("naver_daily_mp", records)
    log.info("✅ 완료")

if __name__ == "__main__":
    main()
