# -*- coding: utf-8 -*-
"""
구글_supabase.py (v3 - gviz CSV 직접 fetch)
===========================================
"00. 네이버/구글 Daily" 시트의 **구글 섹션**만 파싱 → google_ads_daily upsert

gspread는 병합셀을 불완전하게 읽으므로 (Row 0에 그룹라벨만 남고 Row 1이 비어보임),
Google Sheets gviz 엔드포인트를 HTTP로 직접 호출해서 CSV 받음.

gviz 기본 CSV는 Row 0을 결합 헤더로 제공:
  col3:  '네이버 파워링크 브랜드 지출'
  col4:  '일반 지출'
  col12: '구글 검색광고 브랜드 지출'
  col13: '일반 지출'
  col14: '구매전환값'
  col15: '브랜드 구매전환값'
  col16: '일반 구매전환값'

환경변수:
  SUPABASE_URL / SUPABASE_SERVICE_KEY
  NAV_GOO_SHEET_ID (기본 내장) / NAV_GOO_TAB (기본 '00. 네이버/구글 Daily')
  REFRESH_DAYS / FULL_REFRESH
  (GCP_SERVICE_ACCOUNT_KEY 불필요 — 시트가 '링크있는 사람 보기'여야 함)
"""

import os, re, sys, io, csv, time, math, logging, urllib.parse
from datetime import datetime, timedelta, timezone, date

import requests as req_lib

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

NAV_GOO_SHEET_ID = os.environ.get("NAV_GOO_SHEET_ID",
    "1tJ1iv6oi7y-tOmrsXY7pk-chYDBQV__j6gaRNhsGW-Q")
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
    m = re.match(r"^(\d{2})-(\d{1,2})-(\d{1,2})$", raw)
    if m:
        try: return date(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except: return None
    m = re.match(r"^(\d{2})/(\d{1,2})/(\d{1,2})", raw)
    if m:
        try: return date(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except: pass
    for fmt in ["%Y-%m-%d", "%Y/%m/%d"]:
        try: return datetime.strptime(raw[:10], fmt).date()
        except: continue
    return None

# ============================================================
def fetch_gviz_csv():
    url = (f"https://docs.google.com/spreadsheets/d/{NAV_GOO_SHEET_ID}/gviz/tq"
           f"?tqx=out:csv&sheet={urllib.parse.quote(NAV_GOO_TAB)}")
    log.info(f"  🌐 GET gviz CSV ({len(NAV_GOO_TAB)}자 tab)")
    resp = req_lib.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    resp.raise_for_status()
    return list(csv.reader(resp.text.splitlines()))

def load_google_section():
    rows = fetch_gviz_csv()
    if not rows or len(rows) < 2:
        log.error("  ❌ 빈 시트")
        return []
    log.info(f"  📊 rows={len(rows)}")

    hdr = [str(v).strip() for v in rows[0]]
    log.info(f"  → Row0 헤더 {len([h for h in hdr if h])}개 non-empty")
    for ci, h in enumerate(hdr):
        if h: log.info(f"    col{ci}: {repr(h)[:60]}")

    # 구글 섹션 컬럼 찾기
    cost_brand_ci = cost_general_ci = revenue_ci = None
    # cost_brand: 헤더에 '구글' AND '브랜드' AND '지출'
    for ci, h in enumerate(hdr):
        if "구글" in h and "브랜드" in h and "지출" in h:
            cost_brand_ci = ci; break
    if cost_brand_ci is None:
        log.error("  ❌ '구글 브랜드 지출' 컬럼 못 찾음"); return []

    # cost_general: cost_brand 바로 뒤 1~3 col 내에 '일반' AND '지출'
    for ci in range(cost_brand_ci + 1, min(cost_brand_ci + 4, len(hdr))):
        h = hdr[ci]
        if "일반" in h and "지출" in h:
            cost_general_ci = ci; break

    # revenue: cost_brand 이후 '구매전환값' (브랜드/일반 suffix 없는 '총'성격)
    for ci in range(cost_brand_ci + 1, len(hdr)):
        h = hdr[ci].strip()
        if h in ("구매전환값", "총 구매전환값"):
            revenue_ci = ci; break

    log.info(f"  → cost_brand={cost_brand_ci}({hdr[cost_brand_ci]!r})")
    if cost_general_ci is not None:
        log.info(f"  → cost_general={cost_general_ci}({hdr[cost_general_ci]!r})")
    if revenue_ci is not None:
        log.info(f"  → revenue={revenue_ci}({hdr[revenue_ci]!r})")
    if revenue_ci is None:
        log.error("  ❌ '구매전환값' 컬럼 못 찾음"); return []

    # Row 1+ 데이터
    records = []
    for ri in range(1, len(rows)):
        row = rows[ri]
        if not row:
            continue
        dt = _parse_date(row[0])
        if dt is None or dt < START or dt > END:
            continue
        cost_brand = _num(row[cost_brand_ci]) if cost_brand_ci < len(row) else 0
        cost_general = _num(row[cost_general_ci]) if (cost_general_ci is not None and cost_general_ci < len(row)) else 0
        revenue = _num(row[revenue_ci]) if revenue_ci < len(row) else 0
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
    log.info("🚀 구글Ads(gviz CSV) → Supabase")
    log.info("=" * 60)
    records = load_google_section()
    log.info(f"📦 records: {len(records)}")
    if not records:
        log.warning("  ⚠️ 빈 결과"); return
    for r in records[:3]:
        log.info(f"   {r['date']}  cost=₩{r['cost_vat']:,.0f}  rev=₩{r['revenue']:,.0f}  ROAS={r['roas']:.0f}%")
    SupabaseClient(SUPABASE_URL, SUPABASE_KEY).upsert("google_ads_daily", records)
    log.info("✅ 완료")

if __name__ == "__main__":
    main()
