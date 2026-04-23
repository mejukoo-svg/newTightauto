# -*- coding: utf-8 -*-
"""
네이버_supabase.py
==================
네이버 검색광고 API → Supabase(naver_sa_daily) 직통 파이프라인

구조:
  1) Campaign 목록 조회
  2) Campaign → AdGroup 목록 수집
  3) /stats API로 adgroup별 일자별 성과 조회 (배치)
  4) naver_sa_daily UPSERT

환경변수:
  NAVER_API_KEY        : 네이버 검색광고 API 액세스 라이선스
  NAVER_SECRET_KEY     : 네이버 검색광고 API 시크릿키
  NAVER_CUSTOMER_ID    : 광고주 고객번호
  SUPABASE_URL         : https://xxx.supabase.co
  SUPABASE_SERVICE_KEY : service role key
  REFRESH_DAYS         : 기본 10일
  FULL_REFRESH         : "true" 시 2025-01-01부터 전체 갱신
"""

import os, sys, time, hmac, hashlib, base64, json, math, logging, re
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import requests as req_lib

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ============================================================
# 환경변수
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

if FULL_REFRESH:
    START = datetime(2025, 1, 1).date()
else:
    START = TODAY - timedelta(days=REFRESH_DAYS - 1)
END = TODAY

log.info(f"📅 네이버SA 수집: {START} ~ {END}")

# ============================================================
# 네이버 SA API - 서명/요청
# ============================================================
NAVER_BASE = "https://api.searchad.naver.com"

def _sign(timestamp: str, method: str, uri: str) -> str:
    msg = f"{timestamp}.{method}.{uri}"
    sig = hmac.new(bytes(NAVER_SECRET_KEY, "utf-8"),
                   bytes(msg, "utf-8"), hashlib.sha256).digest()
    return base64.b64encode(sig).decode()

def _naver_headers(method: str, uri: str) -> dict:
    ts = str(round(time.time() * 1000))
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": NAVER_API_KEY,
        "X-Customer": str(NAVER_CUSTOMER_ID),
        "X-Signature": _sign(ts, method, uri),
    }

def naver_get(uri: str, params: dict | None = None):
    url = NAVER_BASE + uri
    for attempt in range(4):
        try:
            resp = req_lib.get(url, headers=_naver_headers("GET", uri),
                               params=params or {}, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                time.sleep(5 + attempt * 5)
                continue
            log.error(f"  ❌ GET {uri} HTTP {resp.status_code}: {resp.text[:200]}")
            return None
        except Exception as e:
            log.error(f"  ❌ GET {uri} 예외: {e}")
            time.sleep(2 + attempt)
    return None

# ============================================================
# 1) 캠페인 → 광고그룹 목록
# ============================================================
def list_campaigns():
    log.info("📋 캠페인 목록 조회...")
    data = naver_get("/ncc/campaigns")
    if not data:
        return []
    log.info(f"  → {len(data)}개 캠페인")
    return data

def list_adgroups(campaign_id: str):
    data = naver_get("/ncc/adgroups", {"nccCampaignId": campaign_id})
    return data or []

# ============================================================
# 2) /stats - adgroup별 일자별 성과
# ============================================================
STAT_FIELDS = ["impCnt", "clkCnt", "salesAmt", "ctr", "cpc",
               "ccnt", "crnt", "convAmt"]
# impCnt: 노출수, clkCnt: 클릭수, salesAmt: 광고비(VAT 포함), ctr: CTR,
# cpc: CPC, ccnt: 전환수(클릭/간접 합계), convAmt: 전환금액

def fetch_adgroup_stats(adgroup_ids: list[str], date_from, date_to):
    """최대 100개 ID를 한 번에 호출하는 /stats 엔드포인트 (일별)."""
    out = []
    if not adgroup_ids:
        return out
    for i in range(0, len(adgroup_ids), 50):
        chunk = adgroup_ids[i:i+50]
        params = {
            "ids": ",".join(chunk),
            "fields": json.dumps(STAT_FIELDS),
            "timeRange": json.dumps({
                "since": date_from.strftime("%Y-%m-%d"),
                "until": date_to.strftime("%Y-%m-%d"),
            }),
            "datePreset": "custom",
            "timeIncrement": "1",   # 일별
        }
        resp = naver_get("/stats", params)
        if not resp:
            continue
        # 응답 구조: { "data": [ { "id":..., "impCnt":..., ... } ] }
        for row in resp.get("data", []):
            out.append(row)
        time.sleep(0.3)
    return out

# ============================================================
# 3) Supabase 클라이언트
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
                log.error(f"  ❌ upsert HTTP {resp.status_code}: {resp.text[:300]}")
            time.sleep(0.3)
        return ok

    @staticmethod
    def _clean(r):
        out = {}
        for k, v in r.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                v = 0
            out[k] = v
        return out

# ============================================================
# 4) 상품 추출 (adgroup_name에서 사주/솔로 등 상품 키워드)
# ============================================================
PRODUCT_KEYWORDS = ["사주", "솔로", "무당", "타로", "신점", "관상", "로또", "운세"]

def extract_product(name: str) -> str | None:
    if not name: return None
    for kw in PRODUCT_KEYWORDS:
        if kw in name: return kw
    return None

# ============================================================
# 메인
# ============================================================
def main():
    log.info("=" * 60)
    log.info("🚀 네이버SA → Supabase 파이프라인")
    log.info("=" * 60)

    # 1. 캠페인 + 광고그룹 메타데이터
    campaigns = list_campaigns()
    camp_meta = {c["nccCampaignId"]: c for c in campaigns}

    all_adgroups = []
    for c in campaigns:
        ags = list_adgroups(c["nccCampaignId"])
        for ag in ags:
            ag["_campaign_name"] = c.get("name", "")
        all_adgroups.extend(ags)
        time.sleep(0.2)
    log.info(f"📋 광고그룹 {len(all_adgroups)}개")

    adgroup_meta = {ag["nccAdgroupId"]: ag for ag in all_adgroups}

    # 2. 기간 내 /stats 조회 (adgroup 단위 일별)
    adgroup_ids = list(adgroup_meta.keys())
    log.info(f"📊 /stats 호출: {len(adgroup_ids)}개 광고그룹, {START} ~ {END}")
    raw_stats = fetch_adgroup_stats(adgroup_ids, START, END)
    log.info(f"  → raw rows: {len(raw_stats)}")

    # 3. 레코드 구성
    records = []
    for row in raw_stats:
        ag_id = str(row.get("id") or "")
        meta = adgroup_meta.get(ag_id, {})
        dt_str = row.get("date") or row.get("dt")
        if not dt_str:
            continue
        # 네이버 /stats의 date는 "YYYYMMDD" 또는 "YYYY-MM-DD"
        try:
            if "-" in dt_str:
                dt = datetime.strptime(dt_str[:10], "%Y-%m-%d").date()
            else:
                dt = datetime.strptime(dt_str[:8], "%Y%m%d").date()
        except ValueError:
            continue

        cost = float(row.get("salesAmt") or 0)
        imp  = int(row.get("impCnt") or 0)
        clk  = int(row.get("clkCnt") or 0)
        conv = int(row.get("ccnt") or 0)
        rev  = float(row.get("convAmt") or 0)
        ctr  = float(row.get("ctr") or 0)
        cpc  = float(row.get("cpc") or 0)
        profit = rev - cost
        roas = (rev / cost * 100) if cost > 0 else 0
        cvr  = (conv / clk * 100) if clk > 0 else 0

        records.append({
            "date": dt.isoformat(),
            "adgroup_id": ag_id,
            "campaign_id": meta.get("nccCampaignId", ""),
            "campaign_name": meta.get("_campaign_name", ""),
            "adgroup_name": meta.get("name", ""),
            "product": extract_product(meta.get("name", "")),
            "cost_vat": round(cost, 2),
            "impressions": imp,
            "clicks": clk,
            "ctr": round(ctr, 4),
            "cpc": round(cpc, 2),
            "conversions": conv,
            "revenue": round(rev, 2),
            "profit": round(profit, 2),
            "roas": round(roas, 2),
            "cvr": round(cvr, 4),
        })

    log.info(f"📦 records: {len(records)}")
    if not records:
        log.warning("  ⚠️ 빈 결과 — 인증/기간/광고그룹 확인")
        return

    # 4. Supabase upsert
    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    sb.upsert("naver_sa_daily", records)
    log.info("✅ 완료")

if __name__ == "__main__":
    main()
