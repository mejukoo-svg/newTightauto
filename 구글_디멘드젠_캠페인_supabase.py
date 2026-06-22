# -*- coding: utf-8 -*-
"""
구글_디멘드젠_캠페인_supabase.py
================================
구글 디멘드젠 **[Tight] 캠페인**의 캠페인 id × 일자 성과를
google_demandgen_campaign_daily 테이블에 upsert.

  · 지출 = 구글 Ads API (campaign, advertising_channel_type=DEMAND_GEN,
           campaign.name 에 '[Tight]' 포함) → campaign.id × segments.date × cost
  · 매출 = Mixpanel export (payment_complete) properties.utm_campaign(=구글 campaign.id)
           매칭 → $insert_id + order_id dedup 후 (date_KST, campaign_id) 별 매출/건수
  ※ Mixpanel 결제 이벤트의 utm_campaign 값이 구글 캠페인 숫자 id 와 동일.
    (utm_content = 광고 id) — 캠페인 id 로 지출↔매출 직접 귀속.

index.html '🟢 구글 디멘드젠'(국내 탭, renderGgdgTight)이 이 테이블 단독으로
  캠페인 × 일자 추이를 렌더 (지출/매출 한 테이블에 함께 적재).

자격증명 (.env 또는 GitHub Secrets):
  G_ADS_DEV_TOKEN / G_ADS_CLIENT_ID / G_ADS_CLIENT_SECRET /
  G_ADS_REFRESH_TOKEN / G_ADS_LOGIN_ID  (필수)
  G_ADS_CUSTOMER_ID  (권장 — 미지정 시 MCC 하위 KRW 운영계정 자동탐색)
  MIXPANEL_PROJECT_ID / MIXPANEL_USERNAME / MIXPANEL_SECRET
  SUPABASE_URL / SUPABASE_SERVICE_KEY

기간/옵션 (다른 *_supabase.py 와 동일 규약):
  REFRESH_DAYS (기본 10) / FULL_REFRESH=true (2025-01-01부터)
  --dry      : Supabase 미적재, 집계만 출력
  --replace  : 기간 전체 삭제 후 재삽입
  --any-tight: '[Tight]' 대신 'tight' 부분일치 (검색캠페인 포함)

실행: GitHub Actions(supabase.yml google-dg-tight job) 매시 cron 자동.
로컬: py 구글_디멘드젠_캠페인_supabase.py [--dry] [--replace]
의존성: pip install google-ads requests
"""

import os, re, sys, json, logging
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict

import requests as req_lib

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
except ImportError:
    sys.stderr.write("\n❌ google-ads 라이브러리가 없습니다.  pip install google-ads\n\n")
    raise

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ── .env 로드 (로컬용; Actions 는 env 로 주입) ─ 반드시 환경변수 상수 정의 전에 ──
def _load_env():
    from pathlib import Path
    p = Path(__file__).parent / ".env"
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            val = v.strip().strip('"').strip("'")
            if val:
                os.environ.setdefault(k.strip(), val)


_load_env()

DRY = "--dry" in sys.argv
REPLACE = "--replace" in sys.argv
ANY_TIGHT = "--any-tight" in sys.argv
TIGHT_KEY = "tight" if ANY_TIGHT else "[tight]"   # 소문자 비교

TABLE = "google_demandgen_campaign_daily"

MP_PID    = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MP_USER   = os.environ.get("MIXPANEL_USERNAME")
MP_SECRET = os.environ.get("MIXPANEL_SECRET")
MP_EVENTS = ["결제완료", "payment_complete"]

DEFAULT_CURRENCY = os.environ.get("G_ADS_CURRENCY", "KRW").upper()
KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))
START = date(2025, 1, 1) if FULL_REFRESH else TODAY - timedelta(days=REFRESH_DAYS - 1)
END = TODAY
START_ISO, END_ISO = START.isoformat(), END.isoformat()

digits = lambda s: re.sub(r"\D", "", str(s or ""))


# ── 구글 Ads ──────────────────────────────────────────────────────────────────
def build_client():
    cfg = {
        "developer_token": os.environ["G_ADS_DEV_TOKEN"].strip(),
        "client_id":       os.environ["G_ADS_CLIENT_ID"].strip(),
        "client_secret":   os.environ["G_ADS_CLIENT_SECRET"].strip(),
        "refresh_token":   os.environ["G_ADS_REFRESH_TOKEN"].strip(),
        "login_customer_id": digits(os.environ["G_ADS_LOGIN_ID"]),
        "use_proto_plus": True,
    }
    missing = [k for k in ("developer_token", "client_id", "client_secret",
                           "refresh_token", "login_customer_id") if not cfg.get(k)]
    if missing:
        raise SystemExit(f"❌ 구글 Ads 자격증명 누락: {missing}")
    return GoogleAdsClient.load_from_dict(cfg)


def discover_customer_ids(client):
    forced = digits(os.environ.get("G_ADS_CUSTOMER_ID", ""))
    if forced:
        log.info(f"  🎯 지정 운영 계정 CID={forced}")
        return [forced]
    log.info(f"  🔍 MCC 하위 {DEFAULT_CURRENCY} 운영 계정 자동 탐색")
    mcc = digits(os.environ["G_ADS_LOGIN_ID"])
    ga = client.get_service("GoogleAdsService")
    q = ("SELECT customer_client.id, customer_client.descriptive_name, "
         "customer_client.currency_code, customer_client.manager, customer_client.status "
         "FROM customer_client WHERE customer_client.status='ENABLED'")
    found = []
    for batch in ga.search_stream(customer_id=mcc, query=q):
        for row in batch.results:
            cc = row.customer_client
            if cc.manager:
                continue
            if cc.currency_code and cc.currency_code.upper() != DEFAULT_CURRENCY:
                continue
            found.append(str(cc.id))
            log.info(f"    · {cc.id}  {cc.descriptive_name!r}  {cc.currency_code}")
    return found


def fetch_tight_names(client, cids):
    """[Tight] 디멘드젠 캠페인 전체 id→name (날짜 무관, 매출만 있는 캠페인도 포함 위함)."""
    ga = client.get_service("GoogleAdsService")
    names = {}
    q = ("SELECT campaign.id, campaign.name, campaign.advertising_channel_type "
         "FROM campaign WHERE campaign.name LIKE '%Tight%' "
         "AND campaign.advertising_channel_type = 'DEMAND_GEN'")
    for cid in cids:
        try:
            for b in ga.search_stream(customer_id=cid, query=q):
                for r in b.results:
                    cnm = r.campaign.name
                    if TIGHT_KEY in cnm.lower():
                        names[str(r.campaign.id)] = cnm
        except GoogleAdsException as e:
            log.error(f"  ❌ name 조회 CID={cid}")
            for err in getattr(e.failure, "errors", []):
                log.error(f"     · {err.message}")
            raise
    return names


def fetch_tight_spend(client, cids, tight_ids):
    """[Tight] 캠페인의 (campaign_id, date) 별 지출."""
    ga = client.get_service("GoogleAdsService")
    spend = defaultdict(float)   # spend[(cid_camp, date)]
    q = f"""
        SELECT campaign.id, segments.date, metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{START_ISO}' AND '{END_ISO}'
          AND campaign.name LIKE '%Tight%'
          AND campaign.advertising_channel_type = 'DEMAND_GEN'
    """
    for cid in cids:
        try:
            for b in ga.search_stream(customer_id=cid, query=q):
                for r in b.results:
                    camp = str(r.campaign.id)
                    if camp not in tight_ids:
                        continue
                    spend[(camp, r.segments.date)] += r.metrics.cost_micros / 1_000_000.0
        except GoogleAdsException as e:
            log.error(f"  ❌ spend 조회 CID={cid}")
            for err in getattr(e.failure, "errors", []):
                log.error(f"     · {err.message}")
            raise
    return spend


# ── Mixpanel ──────────────────────────────────────────────────────────────────
def fetch_mp(tight_ids):
    """utm_campaign ∈ tight_ids 결제만 (campaign_id, date) 별 매출/건수.
    $insert_id 로 export 중복 제거, order_id 로 구매 고유화."""
    if not (MP_USER and MP_SECRET):
        log.warning("  ⏭  Mixpanel 자격증명 없음 — 매출측 스킵")
        return defaultdict(float), defaultdict(int)
    r = req_lib.get("https://data.mixpanel.com/api/2.0/export",
                    params={"from_date": START_ISO, "to_date": END_ISO,
                            "event": json.dumps(MP_EVENTS), "project_id": MP_PID},
                    auth=(MP_USER, MP_SECRET), timeout=600)
    r.raise_for_status()
    rev = defaultdict(float)   # rev[(camp, date)]
    cnt = defaultdict(int)
    seen_insert, seen_order = set(), set()
    dup_ins = dup_ord = matched = 0
    for ln in r.text.splitlines():
        if not ln.strip():
            continue
        try: ev = json.loads(ln)
        except Exception: continue
        p = ev.get("properties", {})
        iid = p.get("$insert_id")
        if iid is not None:
            k = (ev.get("event"), iid)
            if k in seen_insert: dup_ins += 1; continue
            seen_insert.add(k)
        camp = digits(p.get("utm_campaign"))
        if camp not in tight_ids:
            continue
        oid = p.get("order_id") or iid
        if oid is not None:
            if oid in seen_order: dup_ord += 1; continue
            seen_order.add(oid)
        ts = p.get("time", 0)
        if not ts: continue
        d = (datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)).date().isoformat()
        if d < START_ISO or d > END_ISO:
            continue
        amt = 0.0
        for kk in ("amount", "결제금액", "value"):
            v = p.get(kk)
            if v is not None:
                try: amt = float(v); break
                except Exception: pass
        rev[(camp, d)] += amt
        cnt[(camp, d)] += 1
        matched += 1
    log.info(f"  📥 Mixpanel [Tight]매칭 {matched}건 "
             f"(insert중복 {dup_ins}/order중복 {dup_ord} 제거)")
    return rev, cnt


# ── Supabase ──────────────────────────────────────────────────────────────────
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
        if not records:
            return 0
        url = f"{self.base}/rest/v1/{table}"
        ok = 0
        for i in range(0, len(records), chunk):
            batch = records[i:i+chunk]
            resp = req_lib.post(url, headers=self.headers, json=batch, timeout=60)
            if resp.status_code in (200, 201):
                ok += len(batch); log.info(f"  ✅ upsert {ok}/{len(records)} → {table}")
            else:
                log.error(f"  ❌ HTTP {resp.status_code}: {resp.text[:300]}")
        return ok

    def delete_range(self, table, date_from, date_to):
        url = f"{self.base}/rest/v1/{table}?date=gte.{date_from}&date=lte.{date_to}"
        try:
            resp = req_lib.delete(url, headers=self.headers, timeout=60)
            log.info(f"  🗑  {table} {date_from}~{date_to} 삭제 HTTP {resp.status_code}")
        except Exception as e:
            log.warning(f"  ⚠️ delete 예외: {e}")


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 64)
    log.info(f"🚀 구글 디멘드젠 [Tight] 캠페인 → Supabase ({TABLE})")
    log.info(f"   기간 {START} ~ {END} | 필터 '{TIGHT_KEY}' | DRY={DRY} REPLACE={REPLACE}")
    log.info("=" * 64)

    client = build_client()
    cids = discover_customer_ids(client)
    if not cids:
        log.error("  ❌ 조회할 운영 계정 없음"); return

    names = fetch_tight_names(client, cids)
    if not names:
        log.warning("  ⚠️ [Tight] 디멘드젠 캠페인을 찾지 못함 — 스킵"); return
    tight_ids = set(names)
    log.info(f"  🎯 [Tight] 캠페인 {len(names)}개")
    for c, nm in names.items():
        log.info(f"     · {c}  {nm}")

    spend = fetch_tight_spend(client, cids, tight_ids)
    rev, cnt = fetch_mp(tight_ids)

    # (campaign_id, date) 합집합 — 지출/매출/구매 중 하나라도 있으면 적재
    keys = set(spend) | set(rev) | set(cnt)
    records = []
    for (camp, d) in sorted(keys):
        s = round(spend.get((camp, d), 0.0), 2)
        rv = round(rev.get((camp, d), 0.0), 2)
        c = int(cnt.get((camp, d), 0))
        if not s and not rv and not c:
            continue
        records.append({
            "date": d,
            "campaign_id": camp,
            "campaign_name": names.get(camp, "")[:300],
            "spend": s,
            "revenue": rv,
            "purchase_count": c,
        })

    if not records:
        log.warning("  ⚠️ 적재할 행 없음 (지출/매출 모두 0) — 스킵"); return

    # 캠페인별 기간합계 로그
    by_c = defaultdict(lambda: {"s": 0.0, "r": 0.0, "c": 0})
    for r in records:
        b = by_c[r["campaign_id"]]
        b["s"] += r["spend"]; b["r"] += r["revenue"]; b["c"] += r["purchase_count"]
    log.info(f"📦 행 {len(records)}개 / 캠페인 {len(by_c)}개")
    for c, b in sorted(by_c.items(), key=lambda x: -x[1]["s"]):
        roas = f"{b['r']/b['s']*100:.0f}%" if b["s"] else "-"
        log.info(f"   · {names.get(c,c)[:34]:34s} 지출 ₩{b['s']:>11,.0f} 매출 ₩{b['r']:>11,.0f} "
                 f"구매 {b['c']:>3} ROAS {roas}")

    if DRY:
        log.info("  🟡 DRY RUN — Supabase 미적재. 샘플 5건:")
        for r in records[:5]:
            log.info(f"    {r}")
        return

    sb = SupabaseClient(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    dates = sorted({r["date"] for r in records})
    if REPLACE:
        log.warning("  🧨 --replace: 기간 전체 삭제 후 재삽입")
        sb.delete_range(TABLE, dates[0], dates[-1])
    sb.upsert(TABLE, records)
    log.info("✅ 완료")


if __name__ == "__main__":
    main()
