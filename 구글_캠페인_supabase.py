# -*- coding: utf-8 -*-
"""
구글_캠페인_supabase.py
=======================
구글 Ads **전 캠페인**(유형 무관) × 일자 성과를 google_campaign_daily 에 upsert.
매출탭의 구글 채널 분할(국내/대만 × 검색/디멘드젠 + PMAX + 기타)이 이 테이블 하나를 읽는다.

  · 지출/클릭/노출 = Google Ads API — campaign × segments.date (계정=KRW 운영계정, 기본 5912047700)
       유형 필터 없음: SEARCH / DEMAND_GEN / PERFORMANCE_MAX / DISPLAY / VIDEO 전부 적재.
  · 매출/구매수    = Mixpanel export(결제완료) properties.utm_campaign(=구글 campaign.id) 매칭
       계정 추적템플릿이 utm_campaign={campaignid} 라 모든 유형이 캠페인 id 로 붙는다
       (2026-08-11 실측: 구글 결제 867건 중 캠페인 id 미매칭 0건).
       $insert_id + order_id dedup, 비KRW 결제(대만 TWD 등)는 KRW 환산 — 지출이 KRW라 통일.
  · country = 캠페인명 TW 태그(토큰 단위 tw/taiwan/대만/台) → 'TW', 그 외 'KR'
  · owner   = 캠페인명에 '[Tight]' 포함 → 'tight'(우리), 그 외 'vanced'

※ 기존 테이블과의 관계
   - google_demandgen_campaign_daily([Tight] DG 캠페인×세트) : 국내탭 🟢구글 디멘드젠 탭 전용, 유지.
   - google_ads_daily(시트 검색광고 일합계) : 매출탭에서는 이 테이블로 대체됨(검색 KR/TW 분리 불가).

기간/옵션 (다른 *_supabase.py 와 동일 규약):
  REFRESH_DAYS (기본 10) / FULL_REFRESH=true (2025-01-01부터)
  --dry      : Supabase 미적재, 집계만 출력
  --replace  : 기간 전체 삭제 후 재삽입
  --from/--to YYYY-MM-DD : 기간 직접 지정(백필용)

자격증명(.env 또는 GitHub Secrets):
  G_ADS_DEV_TOKEN / G_ADS_CLIENT_ID / G_ADS_CLIENT_SECRET / G_ADS_REFRESH_TOKEN / G_ADS_LOGIN_ID
  G_ADS_CUSTOMER_ID(권장) / MIXPANEL_PROJECT_ID / MIXPANEL_USERNAME / MIXPANEL_SECRET
  SUPABASE_URL / SUPABASE_SECRET_KEY(없으면 SUPABASE_SERVICE_KEY 폴백) / SUPABASE_DB_SCHEMA

실행: py 구글_캠페인_supabase.py [--dry] [--replace] [--from 2026-06-01 --to 2026-06-30]
의존성: pip install google-ads requests
"""

import os, re, sys, json, logging
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict

import requests as req_lib

try:
    from google.ads.googleads.client import GoogleAdsClient
except ImportError:
    sys.stderr.write("\n❌ google-ads 라이브러리가 없습니다.  pip install google-ads\n\n")
    raise

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def _load_env():
    from pathlib import Path
    p = Path(__file__).parent / ".env"
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            val = v.strip().strip('"').strip("'")
            if val:
                os.environ.setdefault(k.strip(), val)


_load_env()

DRY = "--dry" in sys.argv
REPLACE = "--replace" in sys.argv
TABLE = "google_campaign_daily"

MP_PID    = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MP_USER   = os.environ.get("MIXPANEL_USERNAME")
MP_SECRET = os.environ.get("MIXPANEL_SECRET")
MP_EVENTS = ["결제완료", "payment_complete"]

DEFAULT_CURRENCY = os.environ.get("G_ADS_CURRENCY", "KRW").upper()
KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))


def _argdate(flag):
    if flag in sys.argv:
        try: return date.fromisoformat(sys.argv[sys.argv.index(flag) + 1])
        except Exception: raise SystemExit(f"❌ {flag} 뒤에 YYYY-MM-DD 를 주세요")
    return None


START = _argdate("--from") or (date(2025, 1, 1) if FULL_REFRESH else TODAY - timedelta(days=REFRESH_DAYS - 1))
END   = _argdate("--to") or TODAY
START_ISO, END_ISO = START.isoformat(), END.isoformat()

# ★ Mixpanel export 는 to_date 가 프로젝트(UTC) 기준 미래면 400 → UTC 오늘로 클램프.
#   (KST 00:00~09:00 에 매일 죽던 문제. 다른 *_supabase.py 와 동일 가드)
MP_END_ISO = min(END, datetime.now(timezone.utc).date()).isoformat()
# ★ KST 경계 절단 방지: MP 조회창 첫날 00~09시(KST) 결제가 UTC 기준 전날로 잡혀 누락된다 →
#   조회는 2일 앞에서 시작하고, 집계 때 START~END 로 다시 자른다.
MP_START_ISO = (START - timedelta(days=2)).isoformat()

digits = lambda s: re.sub(r"\D", "", str(s or ""))


# ── 분류 규칙 ────────────────────────────────────────────────────────────────
def is_tw_campaign(name: str) -> bool:
    """캠페인명에 대만 태그가 있으면 True. 토큰 단위 매칭이라 연속문자에 묻힌 tw 는 무시.
    예: '[Vanced] SA_TW_General', '[Vanced] DG_MZmoodang_VT_TW_260612'
    (구글_디멘드젠_api_supabase.py _is_tw_campaign 과 동일 규칙)"""
    n = str(name or "")
    parts = re.split(r"[-_\s\[\]().]+", n.lower())
    if "tw" in parts or "taiwan" in parts:
        return True
    return ("대만" in n) or ("台灣" in n) or ("台湾" in n)


def owner_of(name: str) -> str:
    """'[Tight]' 포함 = 우리 운영 캠페인, 그 외 = 밴스드(대행사) 운영."""
    return "tight" if "[tight]" in str(name or "").lower() else "vanced"


# ── 결제 통화 판별 + KRW 환산 (구글_디멘드젠_mp_supabase.py 와 동일 규칙) ─────
#   지출(Ads cost)이 KRW인데 대만 매출이 TWD 로 적재되면 ROAS 가 ~48배 과소(가짜 적자)가 된다.
KNOWN_NONKRW = {"TWD", "HKD", "THB", "JPY", "USD"}
SUFFIX_CURRENCY = {"tw": "TWD", "th": "THB", "jp": "JPY", "hk": "HKD"}
FALLBACK_KRW_PER = {"TWD": 48.0, "HKD": 197.0, "THB": 45.0, "JPY": 10.3, "USD": 1540.0, "KRW": 1.0}


def currency_from_suffix(svc):
    m = re.search(r"-([a-z]{2,3})$", str(svc or "").strip().lower())
    return SUFFIX_CURRENCY.get(m.group(1)) if m else None


def event_currency(props):
    """통화필드 → 서비스 접미사(-tw 등) → mp_country_code → KRW(기본).
    해외 신호가 명확할 때만 비KRW 로 판정(국내 결제를 해외로 오판해 ×수십배 부풀리는 것 방지)."""
    c = str(props.get("통화") or "").strip().upper()
    if c in KNOWN_NONKRW:
        return c
    if c == "KRW":
        return "KRW"
    sc = currency_from_suffix(props.get("서비스"))
    if sc:
        return sc
    cc = str(props.get("mp_country_code") or "").strip().upper()
    return {"TW": "TWD", "HK": "HKD", "TH": "THB", "JP": "JPY"}.get(cc, "KRW")


_krw_rates = None
def get_krw_rates():
    global _krw_rates
    if _krw_rates is not None:
        return _krw_rates
    rates = dict(FALLBACK_KRW_PER)
    try:
        r = req_lib.get("https://open.er-api.com/v6/latest/USD", timeout=15)
        if r.status_code == 200:
            usd = r.json().get("rates", {})
            krw = usd.get("KRW")
            if krw:
                for cur in ("TWD", "HKD", "THB", "JPY"):
                    per = usd.get(cur)
                    if per:
                        rates[cur] = krw / per
                rates["USD"] = krw
                rates["KRW"] = 1.0
                log.info(f"  💱 환율(KRW/단위): TWD={rates['TWD']:.2f} JPY={rates['JPY']:.3f}")
    except Exception as e:
        log.warning(f"  ⚠️ 환율 조회 실패 → 폴백 사용: {e}")
    _krw_rates = rates
    return rates


# ── 구글 Ads ────────────────────────────────────────────────────────────────
def build_client():
    cfg = {
        "developer_token": os.environ["G_ADS_DEV_TOKEN"].strip(),
        "client_id":       os.environ["G_ADS_CLIENT_ID"].strip(),
        "client_secret":   os.environ["G_ADS_CLIENT_SECRET"].strip(),
        "refresh_token":   os.environ["G_ADS_REFRESH_TOKEN"].strip(),
        "login_customer_id": digits(os.environ["G_ADS_LOGIN_ID"]),
        "use_proto_plus": True,
    }
    missing = [k for k, v in cfg.items() if k != "use_proto_plus" and not v]
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
    if not found:
        log.warning("  ⚠️ 자동 탐색 결과 없음 — G_ADS_CUSTOMER_ID 를 직접 지정하세요.")
    return found


def fetch_spend(client, cids):
    """(campaign_id, date) → 지출/클릭/노출 + 캠페인 메타. 유형 필터 없음(전 캠페인)."""
    ga = client.get_service("GoogleAdsService")
    rows = {}
    meta = {}
    q = ("SELECT campaign.id, campaign.name, campaign.advertising_channel_type, "
         "segments.date, metrics.cost_micros, metrics.clicks, metrics.impressions "
         "FROM campaign "
         f"WHERE segments.date BETWEEN '{START_ISO}' AND '{END_ISO}'")
    for cid in cids:
        try:
            for b in ga.search_stream(customer_id=cid, query=q):
                for r in b.results:
                    camp_id = str(r.campaign.id)
                    d = r.segments.date
                    cost = r.metrics.cost_micros / 1e6
                    clicks = int(r.metrics.clicks or 0)
                    imps = int(r.metrics.impressions or 0)
                    if cost <= 0 and clicks == 0 and imps == 0:
                        continue          # 완전 무활동 행은 적재하지 않음(테이블 비대 방지)
                    k = (d, camp_id)
                    o = rows.setdefault(k, [0.0, 0, 0])
                    o[0] += cost; o[1] += clicks; o[2] += imps
                    meta[camp_id] = (r.campaign.name, r.campaign.advertising_channel_type.name)
        except Exception as e:
            log.error(f"  ❌ [{cid}] 지출 조회 실패: {str(e)[:200]}")
    log.info(f"  📊 구글 지출 {len(rows)}행 / 캠페인 {len(meta)}개")
    return rows, meta


# ── Mixpanel ────────────────────────────────────────────────────────────────
def fetch_mp(camp_ids):
    """utm_campaign ∈ camp_ids 결제를 (campaign_id, date_KST) 별 매출(KRW)/건수로 집계."""
    if not (MP_USER and MP_SECRET):
        log.warning("  ⏭  Mixpanel 자격증명 없음 — 매출측 스킵")
        return defaultdict(float), defaultdict(int)
    r = req_lib.get("https://data.mixpanel.com/api/2.0/export",
                    params={"from_date": MP_START_ISO, "to_date": MP_END_ISO,
                            "event": json.dumps(MP_EVENTS), "project_id": MP_PID},
                    auth=(MP_USER, MP_SECRET), timeout=900)
    r.raise_for_status()
    rates = get_krw_rates()
    rev, cnt = defaultdict(float), defaultdict(int)
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
        if camp not in camp_ids:
            continue
        oid = p.get("order_id") or iid
        if oid is not None:
            if oid in seen_order: dup_ord += 1; continue
            seen_order.add(oid)
        ts = p.get("time", 0)
        if not ts: continue
        d = (datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)).date().isoformat()
        if d < START_ISO or d > END_ISO:
            continue                      # 버퍼 2일분은 여기서 잘라낸다
        amt = 0.0
        for kk in ("amount", "결제금액", "value"):
            v = p.get(kk)
            if v is not None:
                try: amt = float(v); break
                except Exception: pass
        cur = event_currency(p)
        amt *= rates.get(cur, 1.0)
        rev[(d, camp)] += amt
        cnt[(d, camp)] += 1
        matched += 1
    log.info(f"  📥 Mixpanel 매칭 {matched}건 (insert중복 {dup_ins}/order중복 {dup_ord} 제거)")
    return rev, cnt


# ── Supabase ────────────────────────────────────────────────────────────────
class SupabaseClient:
    def __init__(self, url, key):
        clean = re.sub(r"[^\x20-\x7E]", "", url).strip().rstrip("/")
        if not clean.startswith("http"):
            clean = "https://" + clean
        self.base = clean
        self.headers = {
            "apikey": key.strip(),
            "Authorization": f"Bearer {key.strip()}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }
        _sc = os.environ.get("SUPABASE_DB_SCHEMA", "").strip()
        if _sc:
            self.headers["Accept-Profile"] = _sc
            self.headers["Content-Profile"] = _sc

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


def main():
    log.info(f"🎯 구글 전 캠페인 적재  {START_ISO} ~ {END_ISO}"
             f"{'  [DRY]' if DRY else ''}{'  [REPLACE]' if REPLACE else ''}")
    client = build_client()
    cids = discover_customer_ids(client)
    if not cids:
        raise SystemExit("❌ 운영 계정을 찾지 못했습니다.")
    spend, meta = fetch_spend(client, cids)
    if not spend:
        log.warning("  ⚠️ 지출 데이터 없음 — 종료")
        return
    rev, cnt = fetch_mp(set(meta.keys()))

    records = []
    keys = set(spend.keys()) | set(rev.keys())      # 지출 0·매출만 있는 날도 보존
    for (d, camp_id) in sorted(keys):
        name, ctype = meta.get(camp_id, ("", ""))
        s = spend.get((d, camp_id), [0.0, 0, 0])
        records.append({
            "date": d,
            "campaign_id": camp_id,
            "campaign_name": name,
            "channel_type": ctype,
            "country": "TW" if is_tw_campaign(name) else "KR",
            "owner": owner_of(name),
            "spend": round(s[0], 2),
            "revenue": round(rev.get((d, camp_id), 0.0), 2),
            "purchase_count": cnt.get((d, camp_id), 0),
            "clicks": s[1],
            "impressions": s[2],
        })

    # 요약 (매출탭 행 단위와 동일한 버킷)
    agg = defaultdict(lambda: [0.0, 0.0])
    for r in records:
        key = (r["country"], r["channel_type"],
               r["owner"] if r["channel_type"] == "DEMAND_GEN" and r["country"] == "KR" else "")
        agg[key][0] += r["spend"]; agg[key][1] += r["revenue"]
    log.info(f"  📦 {len(records)}행")
    for k, v in sorted(agg.items(), key=lambda z: -z[1][0]):
        roas = (v[1] / v[0] * 100) if v[0] > 0 else 0
        log.info(f"     {'/'.join([x for x in k if x]):<32} 지출 {round(v[0]):>12,}  "
                 f"매출 {round(v[1]):>12,}  ROAS {roas:>6.0f}%")

    if DRY:
        log.info("  🧪 --dry: 적재 생략")
        return
    _sb_key = os.environ.get("SUPABASE_SECRET_KEY") or os.environ["SUPABASE_SERVICE_KEY"]
    sb = SupabaseClient(os.environ["SUPABASE_URL"], _sb_key)
    if REPLACE:
        sb.delete_range(TABLE, START_ISO, END_ISO)
    sb.upsert(TABLE, records)
    log.info("✅ 완료")


if __name__ == "__main__":
    main()
