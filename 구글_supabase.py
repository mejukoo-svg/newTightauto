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
    구글 섹션 컬럼 탐색 규칙:
      - 'cost_brand'   : 헤더에 '구글' AND '브랜드' AND '지출'
      - 'cost_general' : cost_brand 컬럼 바로 오른쪽 + 헤더에 '일반' AND '지출'
      - 'revenue_total': 헤더가 '구매전환값' 이면서 cost_brand보다 오른쪽의 첫 매치
                         + '브랜드' 미포함 + '일반' 미포함 (= 총액)
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
    if not vals or len(vals) < 2:
        return []

    hdr = [str(v).strip() for v in vals[0]]
    log.info(f"  → 헤더 {len(hdr)}개")

    cost_brand_ci = None
    for ci, h in enumerate(hdr):
        if "구글" in h and "브랜드" in h and "지출" in h:
            cost_brand_ci = ci; break
    if cost_brand_ci is None:
        log.error("  ❌ 구글 브랜드지출 컬럼 못 찾음")
        log.error(f"  헤더 전체: {hdr}")
        return []

    # 일반 지출 = 브랜드지출 바로 오른쪽 (일반적으로)
    cost_general_ci = None
    for ci in range(cost_brand_ci + 1, min(cost_brand_ci + 4, len(hdr))):
        h = hdr[ci]
        if "일반" in h and "지출" in h:
            cost_general_ci = ci; break
    # revenue: 구글 섹션 안에서 '구매전환값' (총액) 찾기 — 브랜드/일반 suffix 없는 것
    revenue_ci = None
    for ci in range(cost_brand_ci + 1, len(hdr)):
        h = hdr[ci].strip()
        if h == "구매전환값" or h == "총 구매전환값":
            revenue_ci = ci; break
        # 다음 섹션 만나면 중단 (구글 섹션 끝)
        if "네이버" in h: break

    log.info(f"  → 구글 컬럼: cost_brand={cost_brand_ci}({hdr[cost_brand_ci]!r}), "
             f"cost_general={cost_general_ci}"
             + (f"({hdr[cost_general_ci]!r})" if cost_general_ci is not None else "")
             + f", revenue={revenue_ci}"
             + (f"({hdr[revenue_ci]!r})" if revenue_ci is not None else ""))

    records = []
    for ri in range(1, len(vals)):   # Row 1부터 데이터
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
