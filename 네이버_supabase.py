# -*- coding: utf-8 -*-
"""
네이버_supabase.py (v2 - StatReport)
====================================
네이버 검색광고 StatReport API → naver_sa_daily upsert

v1의 /stats 엔드포인트가 "지원하지 않는 기능" 에러 (code=11001) 로 실패.
StatReport는 폴링 기반이지만 지원 필드가 더 많고 (conversion 포함) 안정적.

흐름:
  1) GET  /ncc/campaigns, /ncc/adgroups  ← 메타데이터
  2) POST /stat-reports { reportTp: AD_CONVERSION_DETAIL, statDt: YYYYMMDD } (날짜마다)
  3) GET  /stat-reports/{id} 폴링 (status=BUILT 될 때까지)
  4) GET  downloadUrl (TSV) → 파싱
  5) 광고그룹 단위로 집계 후 upsert

reportTp=AD_CONVERSION_DETAIL 컬럼:
  0: 일자(YYYYMMDD)   1: 광고주 ID   2: 캠페인 ID   3: 비즈머니 ID
  4: 광고그룹 ID      5: 키워드 ID   6: 광고 ID     7: 비즈채널 ID
  8: 매체 9: PC/모바일구분 10: 노출수 11: 클릭수 12: 비용(VAT포함)
  13: 전환수(클릭+간접) 14: 전환금액 15: 이월전환수 16: 이월전환금액
  (네이버 SA API StatReport 공식 스키마)

환경변수:
  NAVER_API_KEY, NAVER_SECRET_KEY, NAVER_CUSTOMER_ID
  SUPABASE_URL, SUPABASE_SERVICE_KEY
  REFRESH_DAYS (기본 10), FULL_REFRESH
"""

import os, sys, time, hmac, hashlib, base64, json, math, logging, re, io
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict

import requests as req_lib

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ============================================================
NAVER_API_KEY     = os.environ["NAVER_API_KEY"]
NAVER_SECRET_KEY  = os.environ["NAVER_SECRET_KEY"]
NAVER_CUSTOMER_ID = os.environ["NAVER_CUSTOMER_ID"]

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))
START = date(2025, 1, 1) if FULL_REFRESH else TODAY - timedelta(days=REFRESH_DAYS - 1)
END = TODAY - timedelta(days=1)   # StatReport는 어제까지만 확정
log.info(f"📅 네이버SA 수집: {START} ~ {END}")

NAVER_BASE = "https://api.searchad.naver.com"

# ============================================================
def _sign(timestamp: str, method: str, uri: str) -> str:
    msg = f"{timestamp}.{method}.{uri}"
    sig = hmac.new(bytes(NAVER_SECRET_KEY, "utf-8"),
                   bytes(msg, "utf-8"), hashlib.sha256).digest()
    return base64.b64encode(sig).decode()

def _headers(method: str, uri: str) -> dict:
    ts = str(round(time.time() * 1000))
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": NAVER_API_KEY,
        "X-Customer": str(NAVER_CUSTOMER_ID),
        "X-Signature": _sign(ts, method, uri),
    }

def naver_request(method: str, uri: str, params=None, body=None, raw=False):
    url = NAVER_BASE + uri
    for attempt in range(4):
        try:
            resp = req_lib.request(method, url, headers=_headers(method, uri),
                                   params=params, json=body, timeout=60)
            if resp.status_code == 200:
                return resp.content if raw else resp.json()
            if resp.status_code == 429:
                time.sleep(5 + attempt * 5); continue
            log.error(f"  ❌ {method} {uri} HTTP {resp.status_code}: {resp.text[:300]}")
            return None
        except Exception as e:
            log.error(f"  ❌ {method} {uri} 예외: {e}")
            time.sleep(2 + attempt)
    return None

# ============================================================
def list_campaigns():
    log.info("📋 캠페인 목록 조회...")
    data = naver_request("GET", "/ncc/campaigns")
    if not data: return []
    log.info(f"  → {len(data)}개 캠페인")
    return data

def list_adgroups(campaign_id: str):
    return naver_request("GET", "/ncc/adgroups",
                         params={"nccCampaignId": campaign_id}) or []

# ============================================================
# StatReport: per-date report, polling, TSV download, parse
# ============================================================
REPORT_TP = "AD_CONVERSION_DETAIL"    # 광고그룹×키워드 단위 일별 전환포함

def build_report(stat_dt: date) -> dict | None:
    body = {"reportTp": REPORT_TP, "statDt": stat_dt.strftime("%Y%m%d")}
    return naver_request("POST", "/stat-reports", body=body)

def wait_report(job_id: str, max_wait: int = 180) -> dict | None:
    """polling until BUILT or FAIL"""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        r = naver_request("GET", f"/stat-reports/{job_id}")
        if not r:
            time.sleep(3); continue
        status = r.get("status")
        if status == "BUILT":
            return r
        if status in ("FAILED", "CANCELED"):
            log.error(f"  ❌ report {job_id} status={status}")
            return None
        time.sleep(3)
    log.error(f"  ❌ report {job_id} timeout")
    return None

def download_tsv(download_url: str) -> str | None:
    """downloadUrl은 Naver API 경로(상대) — 헤더 서명 필요"""
    # downloadUrl 은 풀 URL일 수도, 상대경로일 수도 있음
    if download_url.startswith("http"):
        # 풀 URL — 경로 부분만 발췌해서 서명
        from urllib.parse import urlparse
        uri = urlparse(download_url).path
        if urlparse(download_url).query:
            uri += "?" + urlparse(download_url).query
        url = download_url
    else:
        uri = download_url
        url = NAVER_BASE + uri
    try:
        # 다운로드는 HMAC 서명한 헤더로 직접 GET
        method = "GET"
        # 서명용 uri는 경로+쿼리 전체
        signing_uri = uri if uri.startswith("/") else "/" + uri
        resp = req_lib.get(url, headers=_headers(method, signing_uri), timeout=60)
        if resp.status_code == 200:
            return resp.text
        log.error(f"  ❌ download HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        log.error(f"  ❌ download 예외: {e}")
    return None

def parse_tsv(tsv: str, stat_dt: date):
    """
    AD_CONVERSION_DETAIL 컬럼 인덱스:
      0:statDt 1:advertiserId 2:campaignId 3:bizmoneyId 4:adgroupId 5:keywordId
      6:adId 7:bizChannelId 8:media 9:pcMobile 10:impCnt 11:clkCnt 12:salesAmt
      13:ccnt 14:convAmt 15:crtoCnt 16:crtoSalesAmt
    """
    agg = defaultdict(lambda: {"impCnt":0,"clkCnt":0,"salesAmt":0.0,"ccnt":0,"convAmt":0.0})
    for line in tsv.splitlines():
        cols = line.split("\t")
        if len(cols) < 15: continue
        ag_id = cols[4].strip()
        if not ag_id or not ag_id.isdigit() and not ag_id.startswith("grp-"):
            # 네이버 adgroup_id는 "grp-..." 형태
            pass
        a = agg[ag_id]
        try:
            a["impCnt"]   += int(cols[10] or 0)
            a["clkCnt"]   += int(cols[11] or 0)
            a["salesAmt"] += float(cols[12] or 0)
            a["ccnt"]     += int(float(cols[13] or 0))
            a["convAmt"]  += float(cols[14] or 0)
        except (ValueError, IndexError):
            continue
    return dict(agg)

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
            batch = [self._clean(r) for r in records[i:i+chunk]]
            resp = req_lib.post(url, headers=self.headers, json=batch, timeout=60)
            if resp.status_code in (200, 201):
                ok += len(batch)
                log.info(f"  ✅ upsert {ok}/{len(records)}")
            else:
                log.error(f"  ❌ HTTP {resp.status_code}: {resp.text[:300]}")
            time.sleep(0.3)
        return ok
    @staticmethod
    def _clean(r):
        out = {}
        for k, v in r.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): v = 0
            out[k] = v
        return out

# ============================================================
PRODUCT_KEYWORDS = ["사주", "솔로", "무당", "타로", "신점", "관상", "로또", "운세"]
def extract_product(name: str) -> str | None:
    if not name: return None
    for kw in PRODUCT_KEYWORDS:
        if kw in name: return kw
    return None

# ============================================================
def main():
    log.info("=" * 60)
    log.info("🚀 네이버SA → Supabase (StatReport)")
    log.info("=" * 60)

    # 1. 메타데이터
    campaigns = list_campaigns()
    all_adgroups = []
    for c in campaigns:
        ags = list_adgroups(c["nccCampaignId"])
        for ag in ags:
            ag["_campaign_name"] = c.get("name", "")
            ag["_campaign_id"] = c["nccCampaignId"]
        all_adgroups.extend(ags)
        time.sleep(0.1)
    adgroup_meta = {ag["nccAdgroupId"]: ag for ag in all_adgroups}
    log.info(f"📋 광고그룹 {len(adgroup_meta)}개")

    # 2. 날짜별 StatReport 빌드 + 다운로드
    all_records = []
    d = START
    while d <= END:
        log.info(f"  📊 StatReport {d}...")
        rpt = build_report(d)
        if not rpt or "reportJobId" not in rpt:
            log.warning(f"    빌드 실패 — 스킵")
            d += timedelta(days=1); continue
        job_id = rpt["reportJobId"]
        status = rpt.get("status")
        # 상태에 따라 폴링
        if status != "BUILT":
            rpt = wait_report(job_id)
        if not rpt:
            d += timedelta(days=1); continue
        dl = rpt.get("downloadUrl")
        if not dl:
            log.warning(f"    downloadUrl 없음 — 스킵")
            d += timedelta(days=1); continue
        tsv = download_tsv(dl)
        if not tsv:
            d += timedelta(days=1); continue
        agg = parse_tsv(tsv, d)
        log.info(f"    {d}: adgroups={len(agg)}, lines={tsv.count(chr(10))}")

        for ag_id, a in agg.items():
            meta = adgroup_meta.get(ag_id, {})
            cost = a["salesAmt"]
            rev  = a["convAmt"]
            clk  = a["clkCnt"]
            conv = a["ccnt"]
            roas = (rev / cost * 100) if cost > 0 else 0
            cvr  = (conv / clk * 100) if clk > 0 else 0
            ctr  = (clk / a["impCnt"] * 100) if a["impCnt"] > 0 else 0
            cpc  = (cost / clk) if clk > 0 else 0
            all_records.append({
                "date": d.isoformat(),
                "adgroup_id": ag_id,
                "campaign_id": meta.get("_campaign_id", ""),
                "campaign_name": meta.get("_campaign_name", ""),
                "adgroup_name": meta.get("name", ""),
                "product": extract_product(meta.get("name", "")),
                "cost_vat": round(cost, 2),
                "impressions": a["impCnt"],
                "clicks": clk,
                "ctr": round(ctr, 4),
                "cpc": round(cpc, 2),
                "conversions": conv,
                "revenue": round(rev, 2),
                "profit": round(rev - cost, 2),
                "roas": round(roas, 2),
                "cvr": round(cvr, 4),
            })
        d += timedelta(days=1)
        time.sleep(1)

    log.info(f"📦 records: {len(all_records)}")
    if not all_records:
        log.warning("  ⚠️ 빈 결과")
        return
    # 샘플 출력
    for r in all_records[:3]:
        log.info(f"   {r['date']}  {r['adgroup_name'][:20]:20s}  cost=₩{r['cost_vat']:,.0f}  rev=₩{r['revenue']:,.0f}  ROAS={r['roas']:.0f}%")
    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    sb.upsert("naver_sa_daily", all_records)
    log.info("✅ 완료")

if __name__ == "__main__":
    main()
