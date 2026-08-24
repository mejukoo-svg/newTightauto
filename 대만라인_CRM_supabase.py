# -*- coding: utf-8 -*-
"""
============================================================
대만 LINE CRM 채널 매출 → Supabase(line_crm_daily_campaign)
============================================================
목적: index.html '📊 매출' 탭(chrev)에 글로벌·우리 카테고리 채널
      '대만 LINE CRM' 을 추가하기 위한 일자×캠페인(ct) 귀속 매출 집계.

소스   : Mixpanel export 결제완료/payment_complete (properties.time = UTC epoch)
필터   : properties.ch == 'line'  AND  properties.ct 가 'crm' 으로 시작
         · ct = LINE OA 발송 메시지의 랜딩 URL 파라미터(?ch=line&ct=crm_YYMMDD_...)
         · 제외 ct: crm_test(테스트), result_link/share/menu_* (리포트 링크 공유·메뉴 = 오가닉)
           → '대만 LINE CRM 트래킹' 시트 '지표 정의' 탭의 집계 규칙과 동일
이중계상 방지:
         utm_term(=adset_id)이 이미 다른 채널에 귀속된 세트(국내메타/밴스드/대만밴스드/글로벌
         타이트)면 스킵. 그 결제는 세트별 파이프라인이 해당 채널 매출로 이미 잡고 있다.
         (전체의 약 6.5% — 대만밴스드 3.7% / 글로벌 타이트 2.6% / 국내메타 0.2%)
통화   : TWD → TWD_KRW_RATE(밴스드·대만 파이프라인과 동일 기본 47.85) 환산.
         HKD/SGD/USD 등 소액 혼입분은 실시간 환율(get_krw_rates)로 KRW 환산해 함께 집계
         (대시보드 글로벌 종합 매출 = Stripe 실결제 KRW 이므로 통화 무관하게 다 들어가야
          '채널 합 = 종합' 이 유지된다). 시트 대조용 TWD 원값은 revenue_twd 로 따로 저장.
dedup  : order_id(utm_term우선·max revenue) → $insert_id → (date,distinct_id,서비스)
         ※ MP export 는 $insert_id 중복을 제거해주지 않는다(raw 3,942 → dedup 1,284 실측)
가드   : only-raise — 새 집계가 기존 저장값보다 낮으면(=transient MP fetch 실패) 기존값 보존.
         (memory: mixpanel-fetch-fail-zeroes-revenue)

검증(2026-08-24, 2026-07-01~08-24 구간):
    crm_260808_ninetail_tw  ~08-13 누적 108,490 TWD / 95건  ← 시트 값과 정확히 일치
    crm_260725_carousel     ~08-13 누적 122,131 TWD /109건  (시트 120,479/109)
    crm_260717_newproduct   ~08-13 누적 404,172 TWD /253건  (시트 401,521/252)

환경변수: MIXPANEL_PROJECT_ID, MIXPANEL_USERNAME, MIXPANEL_SECRET,
          SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_DB_SCHEMA,
          REFRESH_DAYS(기본 10), FULL_REFRESH(기본 false), TWD_KRW_RATE, DRY_RUN

[사용법]  python 대만라인_CRM_supabase.py
============================================================
"""
import os, sys, json, time, re, logging
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal
from collections import defaultdict

import requests as req_lib

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("linecrm")


# ---- .env 로컬 로드 (GitHub Actions 는 env 주입) ----
def _load_env():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(p):
        return
    for line in open(p, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


_load_env()

MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME = os.environ.get("MIXPANEL_USERNAME", "")
MIXPANEL_SECRET = os.environ.get("MIXPANEL_SECRET", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

MIXPANEL_EVENTS = ["결제완료", "payment_complete"]
TABLE = "line_crm_daily_campaign"

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))
MP_FETCH_BUFFER_DAYS = 2      # KST 새벽 경계분 확보용(export 는 UTC 날짜 필터)

# LINE CRM 첫 발송(=첫 매출) 2026-07-18. 그 이전은 데이터 자체가 없다.
FIRST_DAY = date(2026, 7, 1)
START = FIRST_DAY if FULL_REFRESH else max(FIRST_DAY, TODAY - timedelta(days=REFRESH_DAYS - 1))
END = TODAY

VN_TW_ACC = "act_1286632473622244"    # 대만 밴스드 계정

# 대만 밴스드 매출 환산과 동일 환율(밴스드 파이프라인 관례)
TWD_KRW_RATE = float(os.environ.get("TWD_KRW_RATE") or 47.85)
FALLBACK_KRW_PER = {"TWD": TWD_KRW_RATE, "HKD": 197.0, "THB": 45.0,
                    "JPY": 10.3, "USD": 1540.0, "SGD": 1080.0, "KRW": 1.0}

META_SRC = {"ig", "fb", "an", "threads", "instagram", "facebook",
            "audience_network", "meta", "msg"}
# ct 가 crm 으로 시작해도 집계에서 빼는 것들(테스트 발송)
EXCLUDE_CT_SUBSTR = ["test"]


# =========================================================
# 헬퍼
# =========================================================
def clean_id(val):
    """1.2E+17 같은 지수표기 adset_id 를 정수 문자열로 정규화(다른 파이프라인과 동일)."""
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    if re.match(r"^\d+$", s):
        return s
    try:
        if ("E" in s or "e" in s) and re.match(r"^[\d.]+[eE][+\-]?\d+$", s):
            return str(int(Decimal(s)))
    except Exception:
        pass
    try:
        if re.match(r"^\d+\.\d+$", s):
            return str(int(Decimal(s)))
    except Exception:
        pass
    return s


def is_crm_ct(ct):
    c = str(ct or "").strip().lower()
    if not c.startswith("crm"):
        return False
    return not any(s in c for s in EXCLUDE_CT_SUBSTR)


SUFFIX_CURRENCY = {"tw": "TWD", "th": "THB", "jp": "JPY", "hk": "HKD", "sg": "SGD"}
KNOWN_NONKRW = {"TWD", "HKD", "THB", "JPY", "USD", "SGD"}


def event_currency(props):
    c = str(props.get("통화") or "").strip().upper()
    if c in KNOWN_NONKRW:
        return c
    if c == "KRW":
        return "KRW"
    m = re.search(r"-([a-z]{2,3})$", str(props.get("서비스") or "").strip().lower())
    if m and m.group(1) in SUFFIX_CURRENCY:
        return SUFFIX_CURRENCY[m.group(1)]
    cc = str(props.get("mp_country_code") or "").strip().upper()
    return {"TW": "TWD", "HK": "HKD", "TH": "THB", "JP": "JPY", "SG": "SGD"}.get(cc, "TWD")


_krw_rates = None


def get_krw_rates():
    """실시간 USD 기준 환율 → KRW/1단위. TWD 는 파이프라인 고정 환율을 우선한다."""
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
                for cur in ("HKD", "THB", "JPY", "SGD"):
                    per = usd.get(cur)
                    if per:
                        rates[cur] = krw / per
                rates["USD"] = krw
    except Exception as e:
        log.warning(f"  ⚠️ 환율 조회 실패 → 폴백 사용: {e}")
    rates["TWD"] = TWD_KRW_RATE   # 대만밴스드 행과 동일 환율로 고정
    rates["KRW"] = 1.0
    _krw_rates = rates
    return rates


# =========================================================
# Supabase
# =========================================================
class SupabaseClient:
    def __init__(self, url, key):
        self.base = url.rstrip("/")
        self.h = {"apikey": key, "Authorization": f"Bearer {key}",
                  "Content-Type": "application/json"}
        _sc = os.environ.get("SUPABASE_DB_SCHEMA", "").strip()
        if _sc:
            self.h["Accept-Profile"] = _sc
            self.h["Content-Profile"] = _sc

    def select_rows(self, table, select, flt=""):
        """페이지네이션 GET (Range 1000/페이지) — 1000행 캡 구멍 방지."""
        out, step, start = [], 1000, 0
        while True:
            headers = dict(self.h)
            headers["Range-Unit"] = "items"
            headers["Range"] = f"{start}-{start+step-1}"
            url = f"{self.base}/rest/v1/{table}?select={select}{flt}"
            r = req_lib.get(url, headers=headers, timeout=60)
            if r.status_code not in (200, 206):
                log.warning(f"  ⚠️ select {table} HTTP {r.status_code}: {r.text[:200]}")
                break
            rows = r.json()
            out.extend(rows)
            if len(rows) < step:
                break
            start += step
        return out

    def upsert(self, table, records, on_conflict):
        if not records:
            return
        url = f"{self.base}/rest/v1/{table}?on_conflict={on_conflict}"
        headers = dict(self.h)
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        for i in range(0, len(records), 500):
            chunk = records[i:i + 500]
            r = req_lib.post(url, headers=headers, data=json.dumps(chunk), timeout=120)
            if r.status_code not in (200, 201, 204):
                log.error(f"  ❌ upsert {table} HTTP {r.status_code}: {r.text[:300]}")
                raise RuntimeError(f"upsert 실패: {r.status_code}")


def load_attributed_adsets(sb):
    """이미 다른 채널에 귀속된 adset_id 집합(국내메타·밴스드·대만밴스드·글로벌 타이트)."""
    cutoff = (TODAY - timedelta(days=120)).isoformat()
    ids = set()
    for tbl, sel in (("ad_performance_daily", "adset_id"),
                     ("vanced_ad_performance_daily", "adset_id"),
                     ("global_ad_performance_daily", "adset_id")):
        n0 = len(ids)
        for row in sb.select_rows(tbl, sel, f"&date=gte.{cutoff}"):
            aid = clean_id(row.get("adset_id"))
            if aid:
                ids.add(aid)
        log.info(f"  📇 {tbl}: +{len(ids)-n0} adset")
    log.info(f"  📇 귀속 제외 대상 세트 총 {len(ids)}개")
    return ids


# =========================================================
# Mixpanel
# =========================================================
def fetch_mixpanel(from_date, to_date):
    """ch=line 결제 이벤트만 export. where 조건은 짧게 유지(길면 날짜창이 무시된다)."""
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "event": json.dumps(MIXPANEL_EVENTS),
        "project_id": MIXPANEL_PROJECT_ID,
        "where": 'properties["ch"] == "line"',
    }
    log.info(f"  📡 Mixpanel export {params['from_date']} ~ {params['to_date']} (ch=line)")
    for attempt in range(4):
        try:
            resp = req_lib.get(url, params=params,
                               auth=(MIXPANEL_USERNAME, MIXPANEL_SECRET), timeout=300)
            if resp.status_code == 429:
                w = 30 + attempt * 30
                log.warning(f"  ⏳ 429 → {w}s")
                time.sleep(w)
                continue
            if resp.status_code != 200:
                log.error(f"  ❌ HTTP {resp.status_code}: {resp.text[:300]}")
                return None
            lines = [l for l in resp.text.splitlines() if l.strip()]
            log.info(f"  📊 raw events: {len(lines)}")
            return lines
        except Exception as e:
            log.error(f"  ❌ Mixpanel 예외: {e}")
            time.sleep(5)
    return None


def parse_events(lines):
    out = []
    for ln in lines:
        try:
            ev = json.loads(ln)
        except Exception:
            continue
        p = ev.get("properties", {}) or {}
        try:
            ts = int(p.get("time", 0) or 0)
        except Exception:
            continue
        if ts <= 0:
            continue
        # where 가 무시되는 사고에 대비한 클라이언트 재필터
        if str(p.get("ch") or "").strip().lower() != "line":
            continue
        kst = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)

        raw_amount = p.get("amount")
        raw_alt = p.get("결제금액")
        raw_value = p.get("value")
        rev = 0.0
        for cand in (raw_amount, raw_alt, raw_value):   # 해외는 amount=실청구액(우선)
            try:
                v = float(cand)
            except (TypeError, ValueError):
                continue
            if v > 0:
                rev = v
                break

        out.append({
            "date": kst.date().isoformat(),
            "ts": ts,
            "ct": str(p.get("ct") or "").strip(),
            "utm_term": clean_id(p.get("utm_term") or p.get("UTM_Term") or ""),
            "utm_source": str(p.get("utm_source") or p.get("UTM_Source") or "").strip().lower(),
            "currency": event_currency(p),
            "revenue": rev,
            "서비스": p.get("서비스", ""),
            "distinct_id": p.get("distinct_id"),
            "insert_id": p.get("$insert_id") or p.get("insert_id") or "",
            "order_id": str(p.get("order_id") or "").strip(),
        })
    log.info(f"  ✅ 파싱(ch=line): {len(out)}건")
    return out


def dedup_events(events):
    """order_id(utm_term우선·max revenue) → $insert_id → (date,distinct_id,서비스)."""
    with_oid = [e for e in events if e["order_id"]]
    no_oid = [e for e in events if not e["order_id"]]
    by_oid = {}
    for e in with_oid:
        key = (1 if e["utm_term"] else 0, e["revenue"])
        cur = by_oid.get(e["order_id"])
        if cur is None or key > cur[0]:
            by_oid[e["order_id"]] = (key, e)
    kept = [v[1] for v in by_oid.values()]
    seen_ins, seen_dds = set(), set()
    for e in no_oid:
        ins = str(e["insert_id"]).strip()
        if ins:
            if ins in seen_ins:
                continue
            seen_ins.add(ins)
        else:
            k = (e["date"], e["distinct_id"], e["서비스"])
            if k in seen_dds:
                continue
            seen_dds.add(k)
        kept.append(e)
    log.info(f"  🧹 dedup: {len(events)} → {len(kept)}건")
    return kept


# =========================================================
# 메인
# =========================================================
def main():
    for name, val in [("MIXPANEL_USERNAME", MIXPANEL_USERNAME), ("MIXPANEL_SECRET", MIXPANEL_SECRET),
                      ("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_SERVICE_KEY", SUPABASE_KEY)]:
        if not val:
            log.error(f"❌ 환경변수 없음: {name}")
            sys.exit(1)

    log.info("=" * 60)
    log.info(f"💬 대만 LINE CRM 매출 → {TABLE}  ({START} ~ {END})")
    log.info("=" * 60)

    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    attributed = load_attributed_adsets(sb)

    lines = fetch_mixpanel(START - timedelta(days=MP_FETCH_BUFFER_DAYS), END)
    if lines is None:
        log.error("⚠️ Mixpanel fetch 실패 — 기존값 보존(업로드 스킵)")
        sys.exit(1)
    if not lines:
        log.error("⚠️ Mixpanel 이벤트 0건 — 기존값 보존(업로드 스킵)")
        return

    events = dedup_events(parse_events(lines))

    start_s, end_s = START.isoformat(), END.isoformat()
    rates = get_krw_rates()
    agg = defaultdict(lambda: {"krw": 0.0, "twd": 0.0, "cnt": 0})
    skipped_ct = defaultdict(int)
    skipped_attr = {"cnt": 0, "twd": 0.0}

    for e in events:
        if not (start_s <= e["date"] <= end_s):
            continue
        if not is_crm_ct(e["ct"]):
            skipped_ct[e["ct"] or "(빈값)"] += 1
            continue
        # 이미 다른 채널(세트)에 귀속된 결제 → 이중계상 방지
        if e["utm_term"] and e["utm_term"] in attributed and e["utm_source"] in META_SRC:
            skipped_attr["cnt"] += 1
            skipped_attr["twd"] += e["revenue"] if e["currency"] == "TWD" else 0
            continue
        g = agg[(e["date"], e["ct"])]
        g["krw"] += e["revenue"] * rates.get(e["currency"], 1.0)
        g["twd"] += e["revenue"] if e["currency"] == "TWD" else 0.0
        g["cnt"] += 1

    log.info(f"  🔎 CRM 귀속: {sum(v['cnt'] for v in agg.values())}건 → {len(agg)} (date,ct) 셀")
    if skipped_ct:
        top = sorted(skipped_ct.items(), key=lambda x: -x[1])[:6]
        log.info("  ⏭️ 비CRM ct 제외: " + ", ".join(f"{k}={v}" for k, v in top))
    if skipped_attr["cnt"]:
        log.info(f"  ⏭️ 세트 귀속 중복 제외: {skipped_attr['cnt']}건 (TWD {skipped_attr['twd']:,.0f})")

    # only-raise 가드
    existing = {}
    for row in sb.select_rows(TABLE, "date,ct,revenue,revenue_twd,purchase_count",
                              f"&date=gte.{start_s}&date=lte.{end_s}"):
        existing[(row["date"], row["ct"])] = (float(row.get("revenue") or 0),
                                              float(row.get("revenue_twd") or 0),
                                              int(row.get("purchase_count") or 0))

    records, lowered = [], 0
    for (ds, ct) in set(agg.keys()) | set(existing.keys()):
        g = agg.get((ds, ct), {"krw": 0.0, "twd": 0.0, "cnt": 0})
        krw, twd, cnt = round(g["krw"], 2), round(g["twd"], 2), g["cnt"]
        old = existing.get((ds, ct))
        if old and old[0] > krw:
            krw, twd, cnt = old[0], max(twd, old[1]), max(cnt, old[2])
            lowered += 1
        records.append({"date": ds, "ct": ct, "revenue": krw,
                        "revenue_twd": twd, "purchase_count": cnt})
    if lowered:
        log.info(f"  🛡️ only-raise: {lowered}셀 기존값 보존")

    per_ct = defaultdict(lambda: [0.0, 0.0, 0])
    for r in records:
        v = per_ct[r["ct"]]
        v[0] += r["revenue"]; v[1] += r["revenue_twd"]; v[2] += r["purchase_count"]
    log.info("  📋 캠페인(ct)별 합계")
    for ct in sorted(per_ct):
        v = per_ct[ct]
        log.info(f"    {ct:28s} ₩{v[0]:>12,.0f}  (TWD {v[1]:>9,.0f} / {v[2]:>4d}건)")

    per_d = defaultdict(float)
    for r in records:
        per_d[r["date"]] += r["revenue"]
    for ds in sorted(per_d)[-14:]:
        log.info(f"    📅 {ds}: ₩{per_d[ds]:,.0f}")

    if os.environ.get("DRY_RUN", "").lower() == "true":
        log.info(f"  🧪 DRY_RUN — 업로드 스킵 ({len(records)}행)")
        return

    sb.upsert(TABLE, records, on_conflict="date,ct")
    log.info(f"✅ 업로드 완료: {len(records)}행")


if __name__ == "__main__":
    main()
