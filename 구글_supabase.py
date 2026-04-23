# -*- coding: utf-8 -*-
"""
구글_supabase.py (v2)
=====================
"00. 네이버/구글 Daily" 시트의 **구글 섹션**만 파싱 → google_ads_daily upsert

실제 시트 구조 (2026-04 확인):
  Row 0: 단일 헤더 (그룹헤더 없음)
    col0: 일                         → 날짜 (YY-MM-DD)
    col1: 주차                       → 주차
    col2: 요일
    col3: 네이버 파워링크 브랜드 지출
    col4: 일반 지출                    ← 네이버 일반지출
    col5: 총 구매전환값                 ← 네이버 revenue
    col6: 브랜드 구매전환값
    col7: 일반 구매전환값
    col8~11: 네이버 ROAS/D3 COST
    col12: 구글 검색광고 브랜드 지출
    col13: 일반 지출                    ← 구글 일반지출
    col14: 구매전환값                   ← 구글 revenue
    col15~20: 구글 ROAS/D3 COST

  Row 1부터 데이터 (한 행 = 하루)

환경변수:
  GCP_SERVICE_ACCOUNT_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY
  NAV_GOO_SHEET_URL, NAV_GOO_TAB (기본값 내장)
  REFRESH_DAYS, FULL_REFRESH
"""

import os, re, sys, json, time, math, logging
from datetime import datetime, timedelta, timezone, date

import gspread
import requests as req_lib
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

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
    # YY-MM-DD (e.g. 26-04-22)
    m = re.match(r"^(\d{2})-(\d{1,2})-(\d{1,2})$", raw)
    if m:
        try: return date(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except: return None
    # YY/M/D(요일)
    m = re.match(r"^(\d{2})/(\d{1,2})/(\d{1,2})", raw)
    if m:
        try: return date(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except: pass
    for fmt in ["%Y-%m-%d", "%Y/%m/%d"]:
        try: return datetime.strptime(raw[:10], fmt).date()
        except: continue
    return None

# ============================================================
def authenticate():
    gcp_key = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
    creds = Credentials.from_service_account_info(gcp_key, scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ])
    return gspread.authorize(creds)

# ============================================================
def load_google_section(gc):
    """
    시트 구조 (gspread 기준 — 병합셀은 top-left만 값 채워짐):
      Row 0: 그룹헤더 — col3='네이버 파워링크' (col3~11 병합), col12='구글 검색광고' (col12~20 병합)
      Row 1: 컬럼헤더 — '일','주차','요일','브랜드 지출','일반 지출','총 구매전환값',...
      Row 2+: 데이터

    구글 섹션 컬럼 매칭 (Row1 기준, col >= google_start):
      - cost_brand  : '브랜드 지출'
      - cost_general: '일반 지출'
      - revenue     : '구매전환값' 또는 '총 구매전환값'
    """
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

    gh = [str(v).strip() for v in vals[0]]   # 그룹헤더
    ch = [str(v).strip() for v in vals[1]]   # 컬럼헤더

    # 1) 그룹헤더에서 구글 섹션 시작 컬럼
    google_start = None
    for ci, v in enumerate(gh):
        if v and "구글" in v:
            google_start = ci; break
    if google_start is None:
        log.error(f"  ❌ '구글' 그룹헤더 못 찾음. Row0={gh}")
        return []
    # 다음 그룹(네이버 재등장/기타)까지가 구글 범위
    google_end = len(ch)
    for ci in range(google_start + 1, len(gh)):
        v = gh[ci]
        if v and "구글" not in v:
            google_end = ci; break
    log.info(f"  → 구글 섹션 col{google_start}~{google_end-1}: Row0[{google_start}]={gh[google_start]!r}")

    cost_brand_ci = cost_general_ci = revenue_ci = None
    for ci in range(google_start, min(google_end, len(ch))):
        h = ch[ci]
        if cost_brand_ci is None and ("브랜드" in h and "지출" in h):
            cost_brand_ci = ci
        elif cost_general_ci is None and ("일반" in h and "지출" in h):
            cost_general_ci = ci
        elif revenue_ci is None and (h == "구매전환값" or h == "총 구매전환값"):
            revenue_ci = ci
    log.info(f"  → 컬럼: cost_brand={cost_brand_ci}, cost_general={cost_general_ci}, revenue={revenue_ci}")
    log.info(f"  → Row1(컬럼헤더): {ch}")
    if cost_brand_ci is None or revenue_ci is None:
        log.error("  ❌ 필수 컬럼 못 찾음")
        return []

    records = []
    for ri in range(2, len(vals)):   # Row 2부터 데이터
        row = vals[ri]
        if not row:
            continue
        dt = _parse_date(row[0])
        if dt is None or dt < START or dt > END:
            continue
        cost_brand = _num(row[cost_brand_ci]) if cost_brand_ci < len(row) else 0
        cost_general = _num(row[cost_general_ci]) if (cost_general_ci is not None and cost_general_ci < len(row)) else 0
        revenue = _num(row[revenue_ci]) if (revenue_ci is not None and revenue_ci < len(row)) else 0
        cost = cost_brand + cost_general
        if cost == 0 and revenue == 0:
            continue
        profit = revenue - cost
        roas = (revenue / cost * 100) if cost > 0 else 0
        records.append({
            "date": dt.isoformat(),
            "cost_vat": round(cost, 2),
            "revenue": round(revenue, 2),
            "profit": round(profit, 2),
            "roas": round(roas, 2),
            # 시트에 impressions/clicks/conversions 없음 → 0
            "impressions": 0, "clicks": 0, "ctr": 0, "cpc": 0,
            "conversions": 0, "cvr": 0,
        })
    return records

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
    # 샘플 출력
    for r in records[:3]:
        log.info(f"   {r['date']}  cost=₩{r['cost_vat']:,.0f}  rev=₩{r['revenue']:,.0f}  ROAS={r['roas']:.0f}%")
    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    sb.upsert("google_ads_daily", records)
    log.info("✅ 완료")

if __name__ == "__main__":
    main()
