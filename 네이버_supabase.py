# -*- coding: utf-8 -*-
"""
네이버_supabase.py (v3 - StatReport + Keyword level)
=====================================================
네이버 검색광고 StatReport API → Supabase
  · naver_sa_daily          : 광고그룹 단위 (기존)
  · naver_sa_keyword_daily  : 키워드 단위 (신규)

흐름:
  1) GET  /ncc/campaigns, /ncc/adgroups, /ncc/keywords  ← 메타데이터 (키워드 텍스트 매핑 포함)
  2) POST /stat-reports { reportTp: AD | AD_CONVERSION_DETAIL, statDt: YYYYMMDD } (날짜마다)
  3) GET  /stat-reports/{id} 폴링 (status=BUILT 까지)
  4) GET  downloadUrl (TSV) → 파싱
  5) 광고그룹 단위 집계 + 키워드 단위 집계 → 두 테이블에 각각 upsert

reportTp=AD_CONVERSION_DETAIL 컬럼:
  0: 일자(YYYYMMDD)   1: 광고주 ID   2: 캠페인 ID   3: 비즈머니 ID
  4: 광고그룹 ID      5: 키워드 ID   6: 광고 ID     7: 비즈채널 ID
  ...

reportTp=AD 컬럼 (run #81):
  0:statDt 1:customerId 2:campaignId 3:adgroupId 4:keywordId 5:adId 6:bizChannelId
  7:hourOrRank 8:pcMobile 9:impCnt 10:clkCnt 11:salesAmt 12:? 13:?

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

def list_keywords(adgroup_id: str):
    """광고그룹의 키워드 메타데이터 (nccKeywordId, keyword text 등)"""
    return naver_request("GET", "/ncc/keywords",
                         params={"nccAdgroupId": adgroup_id}) or []

# ============================================================
# StatReport: per-date report, polling, TSV download, parse
# ============================================================
REPORT_TP_STAT = "AD"                     # cost/imp/click
REPORT_TP_CONV = "AD_CONVERSION_DETAIL"   # conversions/revenue

def build_report(stat_dt: date, report_tp: str) -> dict | None:
    body = {"reportTp": report_tp, "statDt": stat_dt.strftime("%Y%m%d")}
    return naver_request("POST", "/stat-reports", body=body)

def wait_report(job_id: str, max_wait: int = 300) -> dict | None:
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
    from urllib.parse import urlparse
    url = download_url if download_url.startswith("http") else NAVER_BASE + download_url
    parsed = urlparse(url)
    path_only = parsed.path
    # Try 1: plain GET (pre-signed URL)
    try:
        resp = req_lib.get(url, timeout=60)
        if resp.status_code == 200:
            return resp.text
        if resp.status_code != 403:
            log.error(f"  ❌ download(plain) HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        log.error(f"  ❌ download(plain) 예외: {e}")
    # Try 2: HMAC 서명
    try:
        resp = req_lib.get(url, headers=_headers("GET", path_only), timeout=60)
        if resp.status_code == 200:
            return resp.text
        log.error(f"  ❌ download(signed) HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        log.error(f"  ❌ download(signed) 예외: {e}")
    return None

# ── 광고그룹 단위 파서 (기존 유지) ─────────────────────
def parse_tsv_stat(tsv: str, label: str = "AD"):
    lines = [l for l in tsv.splitlines() if l.strip()]
    if not lines: return {}
    log.info(f"    🔍 {label} TSV sample ({len(lines)} lines):")
    for i, line in enumerate(lines[:3]):
        cols = line.split("\t")
        log.info(f"      line{i} ({len(cols)} cols): {[c[:15] for c in cols]}")
    AG_IDX = 3
    IMP_IDX, CLK_IDX, COST_IDX = 9, 10, 11
    agg = defaultdict(lambda: {"impCnt":0,"clkCnt":0,"salesAmt":0.0})
    for line in lines:
        cols = line.split("\t")
        if len(cols) <= COST_IDX: continue
        ag_id = cols[AG_IDX].strip()
        if not ag_id: continue
        a = agg[ag_id]
        try:
            a["impCnt"]   += int(float(cols[IMP_IDX] or 0))
            a["clkCnt"]   += int(float(cols[CLK_IDX] or 0))
            a["salesAmt"] += float(cols[COST_IDX] or 0)
        except (ValueError, IndexError):
            continue
    return dict(agg)

def parse_tsv_conv(tsv: str, label: str = "CONV"):
    lines = [l for l in tsv.splitlines() if l.strip()]
    if not lines: return {}
    log.info(f"    🔍 {label} TSV sample ({len(lines)} lines):")
    for i, line in enumerate(lines[:3]):
        cols = line.split("\t")
        log.info(f"      line{i} ({len(cols)} cols): {[c[:15] for c in cols]}")
    AG_IDX = 3
    CONV_CNT_IDX, CONV_AMT_IDX = 13, 14
    agg = defaultdict(lambda: {"ccnt":0, "convAmt":0.0})
    for line in lines:
        cols = line.split("\t")
        if len(cols) <= CONV_AMT_IDX: continue
        ag_id = cols[AG_IDX].strip()
        if not ag_id: continue
        a = agg[ag_id]
        try:
            a["ccnt"]    += int(float(cols[CONV_CNT_IDX] or 0))
            a["convAmt"] += float(cols[CONV_AMT_IDX] or 0)
        except (ValueError, IndexError):
            continue
    return dict(agg)

# ── 키워드 단위 파서 (신규: (adgroup_id, keyword_id) 키) ──
def parse_tsv_stat_kw(tsv: str):
    lines = [l for l in tsv.splitlines() if l.strip()]
    if not lines: return {}
    AG_IDX, KW_IDX = 3, 4
    IMP_IDX, CLK_IDX, COST_IDX = 9, 10, 11
    agg = defaultdict(lambda: {"impCnt":0,"clkCnt":0,"salesAmt":0.0})
    for line in lines:
        cols = line.split("\t")
        if len(cols) <= COST_IDX: continue
        ag_id = cols[AG_IDX].strip()
        kw_id = cols[KW_IDX].strip() if KW_IDX < len(cols) else ""
        if not ag_id: continue
        key = (ag_id, kw_id)
        a = agg[key]
        try:
            a["impCnt"]   += int(float(cols[IMP_IDX] or 0))
            a["clkCnt"]   += int(float(cols[CLK_IDX] or 0))
            a["salesAmt"] += float(cols[COST_IDX] or 0)
        except (ValueError, IndexError):
            continue
    return dict(agg)

def parse_tsv_conv_kw(tsv: str):
    lines = [l for l in tsv.splitlines() if l.strip()]
    if not lines: return {}
    AG_IDX, KW_IDX = 3, 4
    CONV_CNT_IDX, CONV_AMT_IDX = 13, 14
    agg = defaultdict(lambda: {"ccnt":0, "convAmt":0.0})
    for line in lines:
        cols = line.split("\t")
        if len(cols) <= CONV_AMT_IDX: continue
        ag_id = cols[AG_IDX].strip()
        kw_id = cols[KW_IDX].strip() if KW_IDX < len(cols) else ""
        if not ag_id: continue
        key = (ag_id, kw_id)
        a = agg[key]
        try:
            a["ccnt"]    += int(float(cols[CONV_CNT_IDX] or 0))
            a["convAmt"] += float(cols[CONV_AMT_IDX] or 0)
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
                log.info(f"  ✅ upsert {ok}/{len(records)} → {table}")
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
    log.info("🚀 네이버SA → Supabase (StatReport + Keyword level)")
    log.info("=" * 60)

    # 1. 캠페인/광고그룹 메타데이터
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

    # 1.5. 키워드 메타데이터: keyword_id → {text, adgroup_id}
    log.info("📋 키워드 메타데이터 수집 중 (광고그룹별 /ncc/keywords)...")
    keyword_meta = {}
    for i, ag in enumerate(all_adgroups):
        agid = ag["nccAdgroupId"]
        kws = list_keywords(agid)
        for kw in kws:
            kid = kw.get("nccKeywordId")
            if not kid: continue
            keyword_meta[kid] = {
                "text": (kw.get("keyword") or "").strip(),
                "adgroup_id": agid,
            }
        if (i+1) % 20 == 0:
            log.info(f"    진행 {i+1}/{len(all_adgroups)} | 누적 키워드={len(keyword_meta)}")
        time.sleep(0.12)
    log.info(f"  → 키워드 메타 {len(keyword_meta)}개")

    # 2. 날짜별 StatReport (TSV 한 번 받아서 광고그룹/키워드 두 단위로 파싱)
    def fetch_tsv(d, report_tp, label):
        rpt = build_report(d, report_tp)
        if not rpt or "reportJobId" not in rpt:
            log.warning(f"    {label} 빌드 실패")
            return None
        if rpt.get("status") != "BUILT":
            rpt = wait_report(rpt["reportJobId"])
        if not rpt: return None
        dl = rpt.get("downloadUrl")
        if not dl:
            log.warning(f"    {label} downloadUrl 없음")
            return None
        return download_tsv(dl)

    ag_records = []
    kw_records = []
    d = START
    while d <= END:
        log.info(f"  📊 StatReport {d} (AD + CONV)...")
        stat_tsv = fetch_tsv(d, REPORT_TP_STAT, "AD")
        conv_tsv = fetch_tsv(d, REPORT_TP_CONV, "CONV")

        # 광고그룹 단위 (기존)
        stat_agg_ag = parse_tsv_stat(stat_tsv or "", "AD") if stat_tsv else {}
        conv_agg_ag = parse_tsv_conv(conv_tsv or "", "CONV") if conv_tsv else {}
        # 키워드 단위 (신규)
        stat_agg_kw = parse_tsv_stat_kw(stat_tsv or "") if stat_tsv else {}
        conv_agg_kw = parse_tsv_conv_kw(conv_tsv or "") if conv_tsv else {}

        log.info(f"    {d}: AD ag={len(stat_agg_ag)}/kw={len(stat_agg_kw)},  "
                 f"CONV ag={len(conv_agg_ag)}/kw={len(conv_agg_kw)}")

        # ── 광고그룹 단위 record (기존 호환) ─────────────
        all_ag_ids = set(stat_agg_ag.keys()) | set(conv_agg_ag.keys())
        for ag_id in all_ag_ids:
            meta = adgroup_meta.get(ag_id, {})
            s = stat_agg_ag.get(ag_id, {"impCnt":0,"clkCnt":0,"salesAmt":0.0})
            c = conv_agg_ag.get(ag_id, {"ccnt":0,"convAmt":0.0})
            cost = s["salesAmt"]; rev  = c["convAmt"]
            clk  = s["clkCnt"];  imp  = s["impCnt"]; conv = c["ccnt"]
            roas = (rev / cost * 100) if cost > 0 else 0
            cvr  = (conv / clk * 100) if clk > 0 else 0
            ctr  = (clk / imp * 100) if imp > 0 else 0
            cpc  = (cost / clk) if clk > 0 else 0
            ag_records.append({
                "date": d.isoformat(),
                "adgroup_id": ag_id,
                "campaign_id": meta.get("_campaign_id", ""),
                "campaign_name": meta.get("_campaign_name", ""),
                "adgroup_name": meta.get("name", ""),
                "product": extract_product(meta.get("name", "")),
                "cost_vat": round(cost, 2),
                "impressions": imp, "clicks": clk,
                "ctr": round(ctr, 4), "cpc": round(cpc, 2),
                "conversions": conv,
                "revenue": round(rev, 2),
                "profit": round(rev - cost, 2),
                "roas": round(roas, 2),
                "cvr": round(cvr, 4),
            })

        # ── 키워드 단위 record (신규) ────────────────────
        all_kw_keys = set(stat_agg_kw.keys()) | set(conv_agg_kw.keys())
        for (ag_id, kw_id) in all_kw_keys:
            ag_m = adgroup_meta.get(ag_id, {})
            kw_m = keyword_meta.get(kw_id, {})
            kw_text = kw_m.get("text", "") if kw_id else ""
            s = stat_agg_kw.get((ag_id, kw_id), {"impCnt":0,"clkCnt":0,"salesAmt":0.0})
            c = conv_agg_kw.get((ag_id, kw_id), {"ccnt":0,"convAmt":0.0})
            cost = s["salesAmt"]; rev  = c["convAmt"]
            clk  = s["clkCnt"];  imp  = s["impCnt"]; conv = c["ccnt"]
            roas = (rev / cost * 100) if cost > 0 else 0
            cvr  = (conv / clk * 100) if clk > 0 else 0
            ctr  = (clk / imp * 100) if imp > 0 else 0
            cpc  = (cost / clk) if clk > 0 else 0
            kw_records.append({
                "date": d.isoformat(),
                "adgroup_id": ag_id,
                "keyword_id": kw_id or "_empty_",
                "keyword_text": kw_text or "(미매핑)",
                "campaign_id": ag_m.get("_campaign_id", ""),
                "campaign_name": ag_m.get("_campaign_name", ""),
                "adgroup_name": ag_m.get("name", ""),
                "product": extract_product(ag_m.get("name", "")),
                "cost_vat": round(cost, 2),
                "impressions": imp, "clicks": clk,
                "ctr": round(ctr, 4), "cpc": round(cpc, 2),
                "conversions": conv,
                "revenue": round(rev, 2),
                "profit": round(rev - cost, 2),
                "roas": round(roas, 2),
                "cvr": round(cvr, 4),
            })

        d += timedelta(days=1)
        time.sleep(1)

    log.info(f"📦 광고그룹 records: {len(ag_records)}  /  키워드 records: {len(kw_records)}")
    if not ag_records and not kw_records:
        log.warning("  ⚠️ 빈 결과")
        return

    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)

    # 샘플 출력
    for r in ag_records[:3]:
        log.info(f"   AG {r['date']}  {r['adgroup_name'][:20]:20s}  cost=₩{r['cost_vat']:,.0f}  rev=₩{r['revenue']:,.0f}  ROAS={r['roas']:.0f}%")
    if ag_records:
        sb.upsert("naver_sa_daily", ag_records)

    # 키워드 Top10 (기간 합계)
    if kw_records:
        kw_totals = defaultdict(lambda: {"cost":0,"rev":0,"clk":0})
        for r in kw_records:
            k = r["keyword_text"]
            kw_totals[k]["cost"] += r["cost_vat"]
            kw_totals[k]["rev"]  += r["revenue"]
            kw_totals[k]["clk"]  += r["clicks"]
        top = sorted(kw_totals.items(), key=lambda x: -x[1]["cost"])[:10]
        log.info("  🏆 검색어 Top10 (기간 cost 합계):")
        for kw, v in top:
            roas = (v["rev"]/v["cost"]*100) if v["cost"]>0 else 0
            log.info(f"    · {kw[:30]:30s}  지출 ₩{v['cost']:>11,.0f}  매출 ₩{v['rev']:>11,.0f}  ROAS={roas:.0f}%")
        sb.upsert("naver_sa_keyword_daily", kw_records)

    log.info("✅ 완료")

if __name__ == "__main__":
    main()
