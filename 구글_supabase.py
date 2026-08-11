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
    empty = {"search": [], "dg": [], "naver": []}
    rows = fetch_gviz_csv()
    if not rows or len(rows) < 2:
        log.error("  ❌ 빈 시트")
        return empty
    log.info(f"  📊 rows={len(rows)}")

    hdr = [str(v).strip() for v in rows[0]]
    log.info(f"  → Row0 헤더 {len([h for h in hdr if h])}개 non-empty")
    for ci, h in enumerate(hdr):
        if h: log.info(f"    col{ci}: {repr(h)[:60]}")

    # ── 섹션 단위 컬럼 탐지 (2026-08-11 재작성) ────────────────────────────
    # 시트가 '네이버 파워링크 / 구글 국내 검색광고 / 구글 대만 검색광고 / 구글 국내·대만 디멘드젠 /
    # 구글 PMAX' 섹션 구조로 바뀌면서, 섹션명은 각 섹션 첫 컬럼('… 총 지출')에만 붙고
    # 뒤 컬럼은 '브랜드 지출'·'일반 구매전환값'처럼 섹션명 없이 온다.
    # 구버전은 '구글 AND 브랜드 AND 지출' 한 셀을 찾아서 전 섹션이 통째로 스킵됐다 →
    # 섹션 앵커(섹션명 포함 헤더)로 구간을 자르고, 그 안에서 하위 컬럼을 찾는다.
    _SEC = re.compile(r"(네이버|구글|디멘드젠|PMAX)")
    anchors = [ci for ci, h in enumerate(hdr) if h and _SEC.search(h)]

    def _section(pred):
        """섹션 앵커를 찾아 (start, end) 반환. 못 찾으면 (None, None)."""
        for i, ci in enumerate(anchors):
            if pred(hdr[ci]):
                end = anchors[i + 1] if i + 1 < len(anchors) else len(hdr)
                return ci, end
        return None, None

    def _col(s, e, test):
        if s is None:
            return None
        for ci in range(s, e):
            if test(hdr[ci]):
                return ci
        return None

    _is_total_rev = lambda h: h.strip() in ("구매전환값", "총 구매전환값")
    _brand_cost   = lambda h: "브랜드" in h and "지출" in h
    _gen_cost     = lambda h: "일반" in h and "지출" in h
    _brand_rev    = lambda h: "브랜드" in h and "구매전" in h
    _gen_rev      = lambda h: "일반" in h and "구매전" in h

    # 구글 국내 검색광고 (대만은 별도 섹션이라 제외 — 국내 채널 테이블이다)
    g_s, g_e = _section(lambda h: "구글" in h and "검색광고" in h and "대만" not in h)
    if g_s is None:
        g_s, g_e = _section(lambda h: "구글" in h and "브랜드" in h and "지출" in h)  # 구 레이아웃 호환
    cost_brand_ci = _col(g_s, g_e, _brand_cost)
    cost_general_ci = _col(g_s, g_e, _gen_cost)
    revenue_ci = _col(g_s, g_e, _is_total_rev)
    if cost_brand_ci is None or revenue_ci is None:
        log.error("  ❌ 구글 국내 검색광고 섹션 컬럼 못 찾음 — 시트 헤더 확인 필요"); return empty
    log.info(f"  → search brand={cost_brand_ci}({hdr[cost_brand_ci]!r}) general={cost_general_ci} rev={revenue_ci}({hdr[revenue_ci]!r})")

    # 구글 디멘드젠(국내) — 레거시 google_demandgen_daily 용. 지출 + 구매전환값(시트 오타 '구매전홤값' 허용)
    d_s, d_e = _section(lambda h: "디멘드젠" in h and "대만" not in h)
    dg_cost_ci = d_s if (d_s is not None and "지출" in hdr[d_s]) else _col(d_s, d_e, lambda h: "지출" in h)
    dg_rev_ci = _col(d_s, d_e, lambda h: "구매전" in h)
    if dg_cost_ci is not None and dg_rev_ci is not None:
        log.info(f"  → demandgen cost={dg_cost_ci}({hdr[dg_cost_ci]!r}) rev={dg_rev_ci}({hdr[dg_rev_ci]!r})")
    else:
        log.warning("  ⚠️ 구글 디멘드젠 컬럼 못 찾음 — 디멘드젠 스킵")

    # 네이버 파워링크 — 브랜드/일반을 지출·매출 모두 분리해 적재(매출탭 채널 2개로 쪼갬)
    n_s, n_e = _section(lambda h: "네이버" in h)
    nv_brand_ci = _col(n_s, n_e, _brand_cost)
    nv_gen_ci = _col(n_s, n_e, _gen_cost)
    nv_rev_ci = _col(n_s, n_e, _is_total_rev)
    nv_brand_rev_ci = _col(n_s, n_e, _brand_rev)
    nv_gen_rev_ci = _col(n_s, n_e, _gen_rev)
    if nv_brand_ci is not None and nv_rev_ci is not None:
        log.info(f"  → naver brand={nv_brand_ci} general={nv_gen_ci} rev={nv_rev_ci} "
                 f"brandRev={nv_brand_rev_ci} genRev={nv_gen_rev_ci}")
    else:
        log.warning("  ⚠️ 네이버 파워링크 컬럼 못 찾음 — 네이버 스킵")

    # Row 1+ 데이터
    records = []
    dg_records = []
    nv_records = []
    for ri in range(1, len(rows)):
        row = rows[ri]
        if not row:
            continue
        dt = _parse_date(row[0])
        if dt is None or dt < START or dt > END:
            continue
        # 검색광고
        cost_brand = _num(row[cost_brand_ci]) if cost_brand_ci < len(row) else 0
        cost_general = _num(row[cost_general_ci]) if (cost_general_ci is not None and cost_general_ci < len(row)) else 0
        revenue = _num(row[revenue_ci]) if revenue_ci < len(row) else 0
        cost = cost_brand + cost_general
        if cost != 0 or revenue != 0:
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
        # 디멘드젠
        if dg_cost_ci is not None and dg_rev_ci is not None:
            dg_cost = _num(row[dg_cost_ci]) if dg_cost_ci < len(row) else 0
            dg_rev = _num(row[dg_rev_ci]) if dg_rev_ci < len(row) else 0
            if dg_cost != 0 or dg_rev != 0:
                dg_profit = dg_rev - dg_cost
                dg_roas = (dg_rev / dg_cost * 100) if dg_cost > 0 else 0
                dg_records.append({
                    "date": dt.isoformat(),
                    "cost_vat": round(dg_cost, 2),
                    "revenue": round(dg_rev, 2),
                    "profit": round(dg_profit, 2),
                    "roas": round(dg_roas, 2),
                    "impressions": 0, "clicks": 0, "ctr": 0, "cpc": 0,
                    "conversions": 0, "cvr": 0,
                })
        # 네이버 파워링크
        if nv_brand_ci is not None and nv_rev_ci is not None:
            nv_brand = _num(row[nv_brand_ci]) if nv_brand_ci < len(row) else 0
            nv_gen = _num(row[nv_gen_ci]) if (nv_gen_ci is not None and nv_gen_ci < len(row)) else 0
            nv_rev = _num(row[nv_rev_ci]) if nv_rev_ci < len(row) else 0
            nv_brand_rev = _num(row[nv_brand_rev_ci]) if (nv_brand_rev_ci is not None and nv_brand_rev_ci < len(row)) else None
            nv_gen_rev = _num(row[nv_gen_rev_ci]) if (nv_gen_rev_ci is not None and nv_gen_rev_ci < len(row)) else None
            nv_cost = nv_brand + nv_gen
            if nv_cost != 0 or nv_rev != 0:
                nv_profit = nv_rev - nv_cost
                nv_roas = (nv_rev / nv_cost * 100) if nv_cost > 0 else 0
                nv_records.append({
                    "date": dt.isoformat(),
                    "cost_vat": round(nv_cost, 2),
                    "revenue": round(nv_rev, 2),
                    "profit": round(nv_profit, 2),
                    "roas": round(nv_roas, 2),
                    "brand_cost": round(nv_brand, 2),
                    "general_cost": round(nv_gen, 2),
                    # 브랜드/일반 구매전환값 — 매출탭이 '네이버 브랜드검색'·'네이버 일반검색어'로 쪼개 쓴다.
                    "brand_revenue": round(nv_brand_rev, 2) if nv_brand_rev is not None else None,
                    "general_revenue": round(nv_gen_rev, 2) if nv_gen_rev is not None else None,
                })
    return {"search": records, "dg": dg_records, "naver": nv_records}

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
        # new-tightauto: SUPABASE_DB_SCHEMA 설정 시에만 스키마 프로파일 헤더 (미설정=기존 public)
        _sc = os.environ.get('SUPABASE_DB_SCHEMA', '').strip()
        if _sc:
            self.headers['Accept-Profile'] = _sc
            self.headers['Content-Profile'] = _sc

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
    data = load_google_section()
    records = data["search"]; dg_records = data["dg"]; nv_records = data["naver"]
    log.info(f"📦 검색광고: {len(records)} / 디멘드젠: {len(dg_records)} / 네이버: {len(nv_records)}")
    client = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    if records:
        for r in records[:3]:
            log.info(f"   [검색]   {r['date']}  cost=₩{r['cost_vat']:,.0f}  rev=₩{r['revenue']:,.0f}  ROAS={r['roas']:.0f}%")
        client.upsert("google_ads_daily", records)
    else:
        log.warning("  ⚠️ 검색광고 빈 결과")
    if dg_records:
        for r in dg_records[:3]:
            log.info(f"   [디멘드젠] {r['date']}  cost=₩{r['cost_vat']:,.0f}  rev=₩{r['revenue']:,.0f}  ROAS={r['roas']:.0f}%")
        client.upsert("google_demandgen_daily", dg_records)
    else:
        log.warning("  ⚠️ 디멘드젠 빈 결과 (테이블 미생성 시 upsert 404 — CREATE TABLE 필요)")
    if nv_records:
        for r in nv_records[:3]:
            log.info(f"   [네이버]  {r['date']}  cost=₩{r['cost_vat']:,.0f}  rev=₩{r['revenue']:,.0f}  ROAS={r['roas']:.0f}%")
        client.upsert("naver_powerlink_daily", nv_records)
    else:
        log.warning("  ⚠️ 네이버 빈 결과 (테이블 미생성 시 upsert 404 — CREATE TABLE 필요)")
    log.info("✅ 완료")

if __name__ == "__main__":
    main()
