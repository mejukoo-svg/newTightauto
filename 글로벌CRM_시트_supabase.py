# -*- coding: utf-8 -*-
"""
============================================================
글로벌 CRM(대만 LINE OA) 매출 → Supabase(global_crm_daily)
============================================================
목적: index.html '📊 매출' 탭(chrev)에 글로벌·우리 카테고리 채널
      '글로벌 CRM' 을 채우기 위한 일자 × 캠페인 매출/구매건수 적재.

소스   : 구글시트 'LINE CRM tracking' 자동갱신 탭
         https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit?gid={GID}
         · 시트 쪽에서 이미 ch=line · ct=crm_* · 통화 TWD · KST 기준으로 집계해 둔 값이다.
           (귀속 규칙은 그 시트의 '지표 정의' 탭이 정본 — 여기서 다시 판정하지 않는다)
         · 탭 상단 안내대로 매 실행마다 전 기간이 재작성되므로, 여기서도 시트를 정본으로
           보고 전 기간을 통째로 재적재한다(증분·only-raise 가드 없음).
         · 링크 공개 시트라 인증 없이 CSV export 로 읽는다(GitHub Actions 에 자격증명 불필요).

시트 형식(파서가 의존하는 부분):
         '매출(TWD)' 로 시작하는 헤더행 → 날짜행들 → '합계' 행
         '구매건수'  로 시작하는 헤더행 → 날짜행들 → '합계' 행
         두 블록의 컬럼(캠페인) 구성은 동일. 마지막 '합계' 컬럼은 저장하지 않는다.
         ※ 헤더 문구가 바뀌면 파싱 0건 → 아래 가드가 종료코드 1 로 알람.

통화   : revenue_twd = 시트 원값(TWD)
         revenue     = revenue_twd × TWD_KRW_RATE (기본 47.85 — 대만밴스드·대만 파이프라인과
                       동일 고정 환율. 시트가 TWD 단일 통화라 실시간 환율 조회는 하지 않는다)
지출   : 없음(0). LINE OA 발송비용 원천이 수기라 ROAS 를 계산하면 틀린 값이 된다.
         대시보드도 '글로벌 CRM' 을 매출 전용 행(revOnly)으로 그린다.

이중계상: 이 매출은 Stripe 실결제(글로벌 종합) 안에 이미 들어있다. 대시보드가
         '글로벌(밴스드 제외)' 잔여 행에서 같은 값을 빼서 '채널 합 = 글로벌 종합' 을 유지한다.
         (app.js _chrevChannels 의 glExcRow 참고)

가드   : ① CSV 가 비었거나 날짜행 0건이면 아무것도 쓰지 않고 종료코드 1
         ② 파싱한 매출 총합이 0 이면 동일 — 시트 구조 변경/일시 장애를 조용히 0으로 덮지 않는다
         ③ 적재 후, 시트 날짜 범위 안에서 시트에 없는 (date,campaign) 행은 삭제(캠페인 리네임 대응)
         ④ 시트 2행의 '자동갱신 YYYY-MM-DD HH:MM'(KST) 이 STALE_HOURS(기본 24) 보다 낡으면
            **적재는 정상 수행한 뒤** 종료코드 1 로 알람. 시트를 채우는 잡은 이 레포 밖에 있어
            (2026-09-04 전수검색 결과 로컬에 없음 — Apps Script 등 외부) 그쪽이 멈추면
            값이 '있는데 낡은' 상태로 굳는다. ①②는 그걸 못 잡으므로 별도 가드가 필요하다.
            낡았어도 값 자체는 유효하니 쓰기는 막지 않고, Actions 를 빨갛게 만들어 알린다.

환경변수: SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_DB_SCHEMA,
          GLOBAL_CRM_SHEET_ID, GLOBAL_CRM_SHEET_GID, TWD_KRW_RATE, STALE_HOURS, DRY_RUN

[사용법]  python 글로벌CRM_시트_supabase.py
============================================================
"""
import os, sys, csv, json, io, re, logging
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import requests as req_lib

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("glcrm")


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

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
TABLE = "global_crm_daily"

SHEET_ID = os.environ.get("GLOBAL_CRM_SHEET_ID") or "1z6gaUS0D4nLkMDfr6MQDcUI_iYzO7_rdI5PdVmDxHgQ"
SHEET_GID = os.environ.get("GLOBAL_CRM_SHEET_GID") or "290165057"
CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"

# 대만밴스드 매출 환산과 동일 환율(대만 파이프라인 관례)
TWD_KRW_RATE = float(os.environ.get("TWD_KRW_RATE") or 47.85)
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
# 시트 '자동갱신' 스탬프가 이보다 낡으면 알람(적재는 하고 종료코드 1).
#   평상시 시트 갱신 ≈13:44 KST / 이 잡 16:00 KST → 정상 나이 ≈2시간. 하루를 통째로 거르면 ≈26시간.
STALE_HOURS = float(os.environ.get("STALE_HOURS") or 24)

KST = timezone(timedelta(hours=9))
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# 시트 2행: '자동갱신 2026-09-04 13:44 · 이 탭은 매 실행마다 전체 재작성되니 수기 입력 금지'
STAMP_RE = re.compile(r"자동갱신\s*(\d{4}-\d{2}-\d{2})[ T]+(\d{1,2}):(\d{2})")

REV_HEADER = "매출"        # '매출(TWD)' — 통화 표기가 바뀌어도 걸리도록 접두만 본다
CNT_HEADER = "구매건수"
TOTAL_LABEL = "합계"


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

    def delete_one(self, table, date_str, campaign):
        url = f"{self.base}/rest/v1/{table}?date=eq.{date_str}&campaign=eq.{quote(campaign)}"
        r = req_lib.delete(url, headers=self.h, timeout=60)
        if r.status_code not in (200, 204):
            log.warning(f"  ⚠️ delete {table} {date_str}/{campaign} "
                        f"HTTP {r.status_code}: {r.text[:200]}")


# =========================================================
# 시트 파싱
# =========================================================
def fetch_csv():
    r = req_lib.get(CSV_URL, timeout=60)
    r.raise_for_status()
    r.encoding = "utf-8"
    return list(csv.reader(io.StringIO(r.text)))


def _num(cell):
    """'12,345' / '' / '-' → float. 숫자로 못 읽으면 0."""
    s = (cell or "").strip().replace(",", "").replace("₩", "").replace("NT$", "")
    if not s or not re.match(r"^-?\d+(\.\d+)?$", s):
        return 0.0
    return float(s)


def parse_stamp(rows):
    """시트 상단 안내줄의 '자동갱신 …' 시각(KST) → datetime. 못 찾으면 None."""
    for row in rows[:5]:
        for cell in row:
            m = STAMP_RE.search(cell or "")
            if m:
                d, hh, mm = m.group(1), int(m.group(2)), int(m.group(3))
                y, mo, dd = (int(x) for x in d.split("-"))
                return datetime(y, mo, dd, hh, mm, tzinfo=KST)
    return None


def parse_block(rows, header_prefix):
    """header_prefix 로 시작하는 헤더행을 찾아 {date: {campaign: value}} 반환."""
    for i, row in enumerate(rows):
        if not row:
            continue
        head = (row[0] or "").strip()
        if not head.startswith(header_prefix):
            continue
        # 캠페인 컬럼 = 헤더 1번째~ , 마지막 '합계' 컬럼은 제외
        cols = [(j, (c or "").strip()) for j, c in enumerate(row) if j > 0 and (c or "").strip()]
        cols = [(j, c) for j, c in cols if c != TOTAL_LABEL]
        if not cols:
            continue
        out = {}
        for row2 in rows[i + 1:]:
            if not row2:
                continue
            d = (row2[0] or "").strip()
            if not DATE_RE.match(d):
                if not out:
                    continue          # 헤더 바로 밑 빈 줄 등은 건너뛴다
                break                 # 날짜행이 끝나면 블록 종료('합계' 행 / 다음 블록)
            vals = {}
            for j, name in cols:
                v = _num(row2[j]) if j < len(row2) else 0.0
                if v:
                    vals[name] = v
            if vals:
                out[d] = vals
        return out
    return {}


def warn_if_stale(stamp, age_h):
    """가드 ④ — 적재를 마친 뒤 호출. 시트가 낡았으면 종료코드 1(의도된 알람)."""
    if age_h is None or age_h <= STALE_HOURS:
        return
    log.error(f"❌ 시트가 낡았다 — 자동갱신 {stamp:%Y-%m-%d %H:%M} KST, "
              f"{age_h:.1f}시간 전(임계 {STALE_HOURS:g}h). 적재는 마쳤지만 값이 갱신되지 않고 있다.")
    log.error("   시트를 채우는 잡은 이 레포 밖(Apps Script 등)에 있다 — 그쪽이 멈췄는지 확인할 것.")
    sys.exit(1)


# =========================================================
def main():
    log.info("=" * 60)
    log.info("글로벌 CRM(대만 LINE OA) 시트 → Supabase")
    log.info("=" * 60)
    for name, val in [("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_SERVICE_KEY", SUPABASE_KEY)]:
        if not val:
            log.error(f"❌ 환경변수 {name} 없음")
            sys.exit(1)

    log.info(f"📄 시트: {CSV_URL}")
    rows = fetch_csv()
    for r0 in rows[:2]:
        note = " ".join((c or "").strip() for c in r0 if (c or "").strip())
        if note:
            log.info(f"   {note}")

    # 가드 ④ 준비 — 시트를 채우는 잡은 이 레포 밖이라, 그쪽이 멈추면 값이 '낡은 채로' 남는다.
    #   여기선 판정만 하고 알람은 적재를 마친 뒤에 낸다(값 자체는 유효하므로 쓰기는 막지 않는다).
    stamp = parse_stamp(rows)
    stamp_age_h = None
    if stamp:
        stamp_age_h = (datetime.now(KST) - stamp).total_seconds() / 3600
        log.info(f"   ↳ 시트 자동갱신 {stamp:%Y-%m-%d %H:%M} KST ({stamp_age_h:.1f}시간 전)")
    else:
        log.warning("   ⚠️ 시트에서 '자동갱신' 스탬프를 못 찾음 — 신선도 판정 생략")

    rev = parse_block(rows, REV_HEADER)
    cnt = parse_block(rows, CNT_HEADER)

    # 가드 ① 날짜행 0건 = 시트 구조 변경/장애 → 쓰지 않고 알람
    if not rev:
        log.error("❌ 매출 블록에서 날짜행을 못 찾음 — 시트 헤더 구조 변경 여부 확인 필요. 적재 중단.")
        sys.exit(1)
    total_twd = sum(sum(v.values()) for v in rev.values())
    # 가드 ② 총합 0 = 위와 동일 취급(조용한 0 방지)
    if total_twd <= 0:
        log.error("❌ 파싱된 매출 총합이 0 — 적재 중단(기존 값 보존).")
        sys.exit(1)

    dates = sorted(rev.keys())
    campaigns = sorted({c for v in rev.values() for c in v})
    total_cnt = sum(sum(v.values()) for v in cnt.values())
    log.info(f"📊 파싱: {len(dates)}일 ({dates[0]} ~ {dates[-1]}) · 캠페인 {len(campaigns)}개 "
             f"· 매출 {total_twd:,.0f} TWD · 건수 {total_cnt:,.0f}")

    now = datetime.now(timezone.utc).isoformat()
    records = []
    for d in dates:
        for c, twd in rev[d].items():
            records.append({
                "date": d,
                "campaign": c,
                "revenue_twd": round(twd, 2),
                "revenue": round(twd * TWD_KRW_RATE),
                "purchase_count": int(cnt.get(d, {}).get(c, 0)),
                "updated_at": now,
            })
    # 매출 0 인데 건수만 있는 칸(환불 등)도 살린다
    for d, v in cnt.items():
        for c, n in v.items():
            if c not in rev.get(d, {}):
                records.append({"date": d, "campaign": c, "revenue_twd": 0, "revenue": 0,
                                "purchase_count": int(n), "updated_at": now})

    krw = sum(r["revenue"] for r in records)
    log.info(f"💾 upsert 대상 {len(records)}행 · 환율 TWD→KRW {TWD_KRW_RATE} (≈ ₩{krw:,})")
    if DRY_RUN:
        log.info("🧪 DRY_RUN — 쓰지 않고 종료")
        for r in records[-5:]:
            log.info(f"   {r}")
        warn_if_stale(stamp, stamp_age_h)
        return

    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    sb.upsert(TABLE, records, "date,campaign")
    log.info("✅ upsert 완료")

    # 가드 ③ 시트 날짜 범위 안에서 시트에 없는 행 삭제(캠페인 리네임·삭제 반영)
    keep = {(r["date"], r["campaign"]) for r in records}
    existing = sb.select_rows(TABLE, "date,campaign",
                              f"&date=gte.{dates[0]}&date=lte.{dates[-1]}")
    stale = [(r["date"], r["campaign"]) for r in existing
             if (r["date"], r["campaign"]) not in keep]
    for d, c in stale:
        sb.delete_one(TABLE, d, c)
    if stale:
        log.info(f"🧹 시트에 없는 {len(stale)}행 삭제")
    log.info("🏁 완료")
    warn_if_stale(stamp, stamp_age_h)


if __name__ == "__main__":
    main()
