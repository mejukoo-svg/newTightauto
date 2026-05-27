# -*- coding: utf-8 -*-
"""
kpi_supabase.py — 지표 하이아라키 KPI 직통 파이프라인 → kpi_metrics
====================================================================
시트(지표 하이아라키 KPI)를 채우던 로직을 그대로 자동화하여 Supabase에 직접 적재.

소스 (전부 기존 Supabase 테이블 + Mixpanel 1콜 — Toss/Meta API 재호출 없음):
  예산(budget)   = ad_performance_daily.spend  (KR 전용, 일별 합)
  매출(revenue)  = toss_daily_revenue.total_amount
  판매수(sales)  = toss_daily_revenue.total_count
  PV             = Mixpanel pv_onboarding (전 구간 1콜 → 일별 버킷팅)
계산식 (시트와 동일):
  순이익 = 매출 − 예산        객단가 = 매출 / 판매수      ROAS = 매출 / 예산
  CPP   = 예산 / 판매수       CAC   = 예산 / 판매수       결제율 = 판매수 / PV

주간 = 월~일 (시트 주차 경계와 동일), 월간 = 달력월. 최신이 먼저.
예산은 KR 전용(매출이 Toss=국내라 스코프 일치). 글로벌 지출은 budget_global_usd 참고 컬럼.
upsert PK = (period, period_start) → 재실행 시 갱신(idempotent).

환경변수:
  SUPABASE_URL, SUPABASE_SERVICE_KEY,
  MIXPANEL_USERNAME, MIXPANEL_SECRET, MIXPANEL_PROJECT_ID
  WEEKS_BACK(기본 30), MONTH_START(기본 2026-01)
옵션:  --dry  (upsert 생략, 계산 결과만 출력)

로컬 실행 시 ./.env 또는 ../meta_scraper/.env 자동 로드.
"""
import os, sys, json, time, base64, logging, urllib.request, urllib.parse
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


def _load_env():
    for p in [Path(__file__).parent / ".env",
              Path(__file__).parent.parent / "meta_scraper" / ".env"]:
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

SB_URL = os.environ["SUPABASE_URL"].rstrip("/")
SB_KEY = os.environ["SUPABASE_SERVICE_KEY"]
SBH = {"apikey": SB_KEY, "Authorization": "Bearer " + SB_KEY, "Content-Type": "application/json"}
MP_USER = os.environ.get("MIXPANEL_USERNAME", "")
MP_SECRET = os.environ.get("MIXPANEL_SECRET", "")
MP_PROJECT = int(os.environ.get("MIXPANEL_PROJECT_ID", "3390233"))

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
WEEKS_BACK = int(os.environ.get("WEEKS_BACK", "30"))
MONTH_START = os.environ.get("MONTH_START", "2026-01")
DRY = "--dry" in sys.argv


# ───────────────────── Supabase REST ─────────────────────
def sb_get(table, select_query):
    out, off = [], 0
    while True:
        q = f"{select_query}&limit=1000&offset={off}"
        req = urllib.request.Request(f"{SB_URL}/rest/v1/{table}?{q}", headers=SBH)
        chunk = json.loads(urllib.request.urlopen(req, timeout=60).read().decode("utf-8"))
        out += chunk
        if len(chunk) < 1000:
            break
        off += 1000
    return out


def sb_upsert(table, records, chunk=500):
    h = dict(SBH); h["Prefer"] = "resolution=merge-duplicates"
    ok = 0
    for i in range(0, len(records), chunk):
        batch = records[i:i + chunk]
        req = urllib.request.Request(f"{SB_URL}/rest/v1/{table}",
                                     data=json.dumps(batch).encode("utf-8"),
                                     headers=h, method="POST")
        try:
            urllib.request.urlopen(req, timeout=60).read()
            ok += len(batch)
            log.info(f"  ✅ upsert {ok}/{len(records)}")
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8")[:400]
            log.error(f"  ❌ upsert HTTP {e.code}: {body}")
            if e.code == 404:
                log.error("  → kpi_metrics 테이블이 없습니다. sql/kpi_metrics.sql 을 Supabase SQL 편집기에서 먼저 실행하세요.")
            raise
        time.sleep(0.2)
    return ok


# ───────────────────── Mixpanel pv_onboarding (1콜) ─────────────────────
def mp_pv_daily(start, end, retries=6):
    auth = base64.b64encode(f"{MP_USER}:{MP_SECRET}".encode()).decode()
    params = {"project_id": MP_PROJECT, "event": json.dumps(["pv_onboarding"]),
              "type": "general", "unit": "day", "from_date": start, "to_date": end}
    url = "https://mixpanel.com/api/2.0/events?" + urllib.parse.urlencode(params)
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"Authorization": f"Basic {auth}",
                                                       "Accept": "application/json"})
            data = json.loads(urllib.request.urlopen(req, timeout=90).read().decode("utf-8"))
            series = data.get("data", {}).get("values", {}).get("pv_onboarding", {})
            return {d[:10]: c for d, c in series.items()}
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(3 * (i + 1)); continue
            raise
    raise RuntimeError("pv_onboarding rate-limited")


# ───────────────────── period generators ─────────────────────
def gen_weeks(n):
    """최근 n개 월~일 주차 (최신 먼저). (nominal_monday, nominal_sunday)"""
    mon = TODAY - timedelta(days=TODAY.weekday())
    return [(mon - timedelta(days=7 * i), mon - timedelta(days=7 * i) + timedelta(days=6))
            for i in range(n)]


def gen_months(start_ym):
    y, m = map(int, start_ym.split("-"))
    out = []
    while (y, m) <= (TODAY.year, TODAY.month):
        s = date(y, m, 1)
        nm = date(y + (m // 12), (m % 12) + 1, 1)
        out.append((s, nm - timedelta(days=1)))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return list(reversed(out))


# ───────────────────── compute ─────────────────────
def build_daily():
    earliest_week = gen_weeks(WEEKS_BACK)[-1][0]
    earliest_month = date(*map(int, MONTH_START.split("-")), 1)
    win = min(earliest_week, earliest_month).isoformat()
    log.info(f"📅 윈도우 시작 {win} ~ {TODAY}")

    toss = {}
    for r in sb_get("toss_daily_revenue", f"select=date,total_amount,total_count&date=gte.{win}"):
        toss[r["date"]] = (float(r.get("total_amount") or 0), int(r.get("total_count") or 0))
    kr = {}
    for r in sb_get("ad_performance_daily", f"select=date,spend&date=gte.{win}"):
        kr[r["date"]] = kr.get(r["date"], 0.0) + float(r.get("spend") or 0)
    gl = {}
    for r in sb_get("global_ad_performance_daily", f"select=date,spend_usd&date=gte.{win}"):
        gl[r["date"]] = gl.get(r["date"], 0.0) + float(r.get("spend_usd") or 0)
    pv = mp_pv_daily(win, TODAY.isoformat())
    log.info(f"📦 toss {len(toss)}일 · KR예산 {len(kr)}일 · pv {len(pv)}일")
    return toss, kr, gl, pv


def metric(start, end, toss, kr, gl, pv):
    e = min(end, TODAY)
    budget = rev = sales = pvv = glusd = 0.0
    d = start
    while d <= e:
        k = d.isoformat()
        if k in toss:
            rev += toss[k][0]; sales += toss[k][1]
        budget += kr.get(k, 0.0); glusd += gl.get(k, 0.0); pvv += pv.get(k, 0)
        d += timedelta(days=1)

    def div(a, b):
        return round(a / b, 6) if b else None
    return {
        "budget": round(budget), "revenue": round(rev), "pv": int(pvv), "sales": int(sales),
        "net_profit": round(rev - budget), "aov": div(rev, sales), "roas": div(rev, budget),
        "cpp": div(budget, sales), "pay_rate": div(sales, pvv), "cac": div(budget, sales),
        "budget_global_usd": round(glusd, 2),
    }


def build_records():
    toss, kr, gl, pv = build_daily()
    now = datetime.now(timezone.utc).isoformat()
    recs = []
    for s, sun in gen_weeks(WEEKS_BACK):
        recs.append({"period": "weekly", "period_label": f"{s:%m.%d}~{sun:%m.%d}",
                     "period_start": s.isoformat(), "period_end": min(sun, TODAY).isoformat(),
                     "updated_at": now, **metric(s, sun, toss, kr, gl, pv)})
    for s, e in gen_months(MONTH_START):
        recs.append({"period": "monthly", "period_label": f"{s:%Y-%m}",
                     "period_start": s.isoformat(), "period_end": min(e, TODAY).isoformat(),
                     "updated_at": now, **metric(s, e, toss, kr, gl, pv)})
    return recs


def fmt(n):
    return f"{n:,}" if isinstance(n, (int, float)) else "-"


def main():
    log.info("=" * 64)
    log.info("🚀 KPI → Supabase (kpi_metrics)" + ("  [DRY RUN]" if DRY else ""))
    log.info("=" * 64)
    recs = build_records()
    for kind in ("weekly", "monthly"):
        rows = [r for r in recs if r["period"] == kind]
        print(f"\n=== {kind} ({len(rows)}) ===")
        print(f"  {'기간':<14}{'예산':>13}{'매출':>14}{'판매수':>8}{'PV':>9}{'ROAS':>8}{'결제율':>8}{'객단가':>9}")
        for r in rows:
            roas = f"{r['roas']*100:.1f}%" if r["roas"] else "-"
            pr = f"{r['pay_rate']*100:.2f}%" if r["pay_rate"] else "-"
            aov = fmt(round(r["aov"])) if r["aov"] else "-"
            print(f"  {r['period_label']:<14}{fmt(r['budget']):>13}{fmt(r['revenue']):>14}"
                  f"{fmt(r['sales']):>8}{fmt(r['pv']):>9}{roas:>8}{pr:>8}{aov:>9}")
    log.info(f"\n📊 총 {len(recs)} records")
    if DRY:
        log.info("[DRY RUN] upsert 생략. 실제 적재하려면 --dry 빼고 실행.")
        return
    sb_upsert("kpi_metrics", recs)
    log.info("✅ 완료")


if __name__ == "__main__":
    main()
