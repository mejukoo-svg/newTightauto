# -*- coding: utf-8 -*-
"""
구글_supabase.py
================
구글 검색광고 일별 집계 → Supabase(google_ads_daily) 직통

Colab 참고 스크립트와 동일한 출처 사용:
  "00. 네이버/구글 Daily" 시트의 구글 섹션만 (키워드 단 행은 제외,
  일별 '총 지출' / '총 구매전환값' 집계 한 행만 사용)

환경변수:
  GCP_SERVICE_ACCOUNT_KEY : 서비스 계정 JSON (문자열)
  NAV_GOO_SHEET_URL       : 입력 스프레드시트 URL (기본값 내장)
  NAV_GOO_TAB             : "00. 네이버/구글 Daily"
  SUPABASE_URL / SUPABASE_SERVICE_KEY
  REFRESH_DAYS            : 기본 10
  FULL_REFRESH            : "true" 시 2025-01-01부터
"""

import os, re, sys, json, time, math, logging
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict

import gspread
import requests as req_lib
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ============================================================
# 환경변수
# ============================================================
NAV_GOO_SHEET_URL = os.environ.get(
    "NAV_GOO_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1tJ1iv6oi7y-tOmrsXY7pk-chYDBQV__j6gaRNhsGW-Q/edit"
)
NAV_GOO_TAB = os.environ.get("NAV_GOO_TAB", "00. 네이버/구글 Daily")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))
START = date(2025, 1, 1) if FULL_REFRESH else TODAY - timedelta(days=REFRESH_DAYS - 1)
END = TODAY

log.info(f"📅 구글 Ads 수집: {START} ~ {END}")

# ============================================================
# Google Sheets 인증
# ============================================================
def authenticate():
    gcp_key = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
    creds = Credentials.from_service_account_info(gcp_key, scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ])
    return gspread.authorize(creds)

# ============================================================
# 파서
# ============================================================
def _num(x):
    try:
        s = str(x).replace(",", "").replace("₩", "").replace("%", "") \
                  .replace("\\", "").replace("W", "").replace("￦", "") \
                  .replace("+", "").strip()
        return float(s) if s and s not in ["-", "#DIV/0!", "nan", "None", "NaN"] else 0.0
    except:
        return 0.0

def _parse_date(raw):
    raw = str(raw).strip().split("\n")[0].strip()
    if not raw or raw in ["", "nan", "None"]:
        return None
    m = re.match(r"^(\d{2})/(\d{1,2})/(\d{1,2})", raw)
    if m:
        try: return date(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except: pass
    m = re.match(r"^(\d{1,2})/(\d{1,2})", raw)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        year = 2026 if month <= 3 else 2025
        try: return date(year, month, day)
        except: return None
    for fmt in ["%Y-%m-%d", "%Y/%m/%d"]:
        try: return datetime.strptime(raw[:10], fmt).date()
        except: continue
    return None

def load_google_section(gc):
    """구글 섹션의 일자별 총 지출·총 구매전환값·노출·클릭·전환수를 추출"""
    ss = gc.open_by_url(NAV_GOO_SHEET_URL)
    ws = None
    for s in ss.worksheets():
        if NAV_GOO_TAB in s.title or s.title.strip() == NAV_GOO_TAB:
            ws = s; break
    if ws is None:
        for s in ss.worksheets():
            if "네이버" in s.title and "Daily" in s.title:
                ws = s; break
    if ws is None:
        raise RuntimeError("네이버/구글 Daily 탭 못 찾음")
    log.info(f"  📖 시트: {ws.title}")
    vals = ws.get_all_values()
    time.sleep(0.5)
    if not vals or len(vals) < 3:
        return []

    gh = [str(v).strip() for v in vals[0]]
    ch = [str(v).strip() for v in vals[1]]

    # 그룹(1행) 범위 산출
    groups = {}; current = None
    for ci, val in enumerate(gh):
        if val and val not in ["", "nan"]:
            current = val
            groups[current] = {"start": ci, "end": len(ch)}
    sg = sorted(groups.items(), key=lambda x: x[1]["start"])
    for i, (name, g) in enumerate(sg):
        if i + 1 < len(sg):
            g["end"] = sg[i + 1][1]["start"]

    # 구글 섹션 컬럼 탐색
    cols = {"cost_vat": None, "revenue": None, "impressions": None,
            "clicks": None, "conversions": None}
    label_map = {
        "cost_vat":    ["총 지출", "지출"],
        "revenue":     ["구매전환값", "총 구매전환값"],
        "impressions": ["노출수", "총 노출"],
        "clicks":      ["클릭수", "총 클릭"],
        "conversions": ["전환수", "구매전환수"],
    }
    for name, g in groups.items():
        if not ("구글" in name or "검색" in name):
            continue
        for ci in range(g["start"], min(g["end"], len(ch))):
            h = ch[ci]
            for key, labels in label_map.items():
                if cols[key] is None and h in labels:
                    cols[key] = ci
    log.info(f"  → 구글 컬럼: {cols}")

    # 일자별 합산 (같은 날짜 중복 행이 있을 수 있음)
    agg = defaultdict(lambda: defaultdict(float))
    for ri in range(2, len(vals)):
        row = vals[ri]
        if not row: continue
        dt = _parse_date(row[0])
        if dt is None or dt < START or dt > END: continue
        for key, ci in cols.items():
            if ci is None or ci >= len(row): continue
            agg[dt][key] += _num(row[ci])

    records = []
    for dt, d in sorted(agg.items()):
        cost = d.get("cost_vat", 0)
        rev  = d.get("revenue", 0)
        imp  = int(d.get("impressions", 0))
        clk  = int(d.get("clicks", 0))
        conv = int(d.get("conversions", 0))
        profit = rev - cost
        roas = (rev / cost * 100) if cost > 0 else 0
        ctr  = (clk / imp * 100) if imp > 0 else 0
        cpc  = (cost / clk) if clk > 0 else 0
        cvr  = (conv / clk * 100) if clk > 0 else 0
        if cost == 0 and rev == 0 and imp == 0:
            continue
        records.append({
            "date": dt.isoformat(),
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
    return records

# ============================================================
# Supabase
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
            batch = records[i:i+chunk]
            resp = req_lib.post(url, headers=self.headers, json=batch, timeout=60)
            if resp.status_code in (200, 201):
                ok += len(batch)
                log.info(f"  ✅ upsert {ok}/{len(records)}")
            else:
                log.error(f"  ❌ HTTP {resp.status_code}: {resp.text[:300]}")
            time.sleep(0.3)
        return ok

# ============================================================
# 메인
# ============================================================
def main():
    log.info("=" * 60)
    log.info("🚀 구글Ads(시트) → Supabase")
    log.info("=" * 60)
    gc = authenticate()
    records = load_google_section(gc)
    log.info(f"📦 records: {len(records)}")
    if not records:
        log.warning("  ⚠️ 빈 결과")
        return
    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    sb.upsert("google_ads_daily", records)
    log.info("✅ 완료")

if __name__ == "__main__":
    main()
