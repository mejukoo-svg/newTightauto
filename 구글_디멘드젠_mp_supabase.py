# -*- coding: utf-8 -*-
"""
구글_디멘드젠_mp_supabase.py
============================
Mixpanel 결제완료 이벤트 → properties.ch == 'google' 인 것만 필터 →
콘텐츠(properties.ct)별 × 일자별 매출/건수 집계 →
google_demandgen_content_mp_daily 테이블 upsert

구글 디멘드젠 성과를 콘텐츠(ct) 단위로 본다:
  - ch  = 채널 (google / meta / naver / dable ...) — 'google' 만 사용
  - ct  = 콘텐츠 이름 (예: dg_ugc_moodang_260512_blankit)
  - cr  = 제작자 (집계엔 미사용, 향후 확장용)
  ※ ch/ct 는 랜딩 URL 파라미터가 Mixpanel 이벤트 속성으로 저장된 값.
    결제완료/payment_complete 이벤트에도 실려 있어 콘텐츠별 매출 attribution 가능.

집계 단위: (date_KST, ct) → revenue(결제금액 합), purchase_count(건수)
※ Mixpanel 에는 콘텐츠별 광고비가 없으므로 매출/건수만 적재 (ROAS/지출 없음).
  구글 디멘드젠 '전체' 지출/ROAS 는 별도 테이블 google_demandgen_daily(시트 기반) 참고.

네이버_mp_supabase.py 와 동일한 구조/Supabase 클라이언트 사용.

환경변수:
  MIXPANEL_PROJECT_ID / MIXPANEL_USERNAME / MIXPANEL_SECRET
  SUPABASE_URL / SUPABASE_SERVICE_KEY
  REFRESH_DAYS (기본 10), FULL_REFRESH
"""

import os, re, sys, json, time, logging
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict

import requests as req_lib

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME   = os.environ["MIXPANEL_USERNAME"]
MIXPANEL_SECRET     = os.environ["MIXPANEL_SECRET"]
MIXPANEL_EVENTS     = ["결제완료", "payment_complete"]

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))
START = date(2025, 1, 1) if FULL_REFRESH else TODAY - timedelta(days=REFRESH_DAYS - 1)
END = TODAY
log.info(f"📅 Mixpanel(구글 디멘드젠/ch=google) 수집: {START} ~ {END}")

GOOGLE_CH_VALUES = {"google"}   # lowercase 비교용 (필요시 'youtube','yt' 등 추가 가능)


def is_google_event(props: dict) -> bool:
    """properties.ch 가 google 인지 (대소문자 무시)"""
    v = props.get("ch")
    if v is not None and str(v).strip().lower() in GOOGLE_CH_VALUES:
        return True
    return False


def extract_content(props: dict):
    """properties.ct (콘텐츠 이름) 정규화. 빈/undefined 는 None"""
    v = props.get("ct")
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in ("undefined", "null", "none", "{ct}", "{content}"):
        return None
    return s


def fetch_mixpanel(from_date: date, to_date: date):
    """Mixpanel export API 로 기간 내 결제완료 이벤트 일괄 다운로드"""
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "event": json.dumps(MIXPANEL_EVENTS),
        "project_id": MIXPANEL_PROJECT_ID,
    }
    log.info(f"  📡 Mixpanel export {params['from_date']} ~ {params['to_date']}")
    for attempt in range(4):
        try:
            resp = req_lib.get(url, params=params,
                               auth=(MIXPANEL_USERNAME, MIXPANEL_SECRET), timeout=300)
            if resp.status_code == 429:
                w = 30 + attempt * 30
                log.warning(f"  ⏳ 429 → {w}s"); time.sleep(w); continue
            if resp.status_code != 200:
                log.error(f"  ❌ HTTP {resp.status_code}: {resp.text[:300]}"); return []
            lines = [l for l in resp.text.splitlines() if l.strip()]
            log.info(f"  📊 total events: {len(lines)}")
            return lines
        except Exception as e:
            log.error(f"  ❌ Mixpanel 예외: {e}"); time.sleep(5)
    return []


def aggregate(lines):
    """events → (date, ct) 별 revenue/count 집계 (ch=google 인 것만)"""
    agg = defaultdict(lambda: {"revenue": 0.0, "count": 0})
    ch_seen = defaultdict(int)
    google_total = 0
    no_ct_count = 0
    sample_seen = 0
    seen_ids = set()   # $insert_id 중복 제거용
    dup_skipped = 0

    for line in lines:
        try:
            ev = json.loads(line)
        except Exception:
            continue
        props = ev.get("properties", {})

        # ⚠️ Mixpanel export API 는 $insert_id 중복을 제거하지 않음 (insights/분석은 자동 dedup).
        # 같은 결제가 여러 번 적재돼 있어 dedup 안 하면 매출/건수가 수배 과대집계됨.
        # (event_name, $insert_id) 단위로 1건만 인정 — insights 와 동일 기준.
        iid = props.get("$insert_id")
        if iid is not None:
            dk = (ev.get("event"), iid)
            if dk in seen_ids:
                dup_skipped += 1
                continue
            seen_ids.add(dk)

        # ch 분포 수집 (디버그)
        chv = props.get("ch")
        if chv:
            ch_seen[str(chv)[:30]] += 1

        # 구글 채널 필터
        if not is_google_event(props):
            continue
        google_total += 1

        # 일자 (KST)
        ts = props.get("time", 0)
        if not ts:
            continue
        dt_kst = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)
        d_iso = dt_kst.date().isoformat()

        # 금액
        amt = 0.0
        for k in ("amount", "결제금액", "value"):
            v = props.get(k)
            if v is not None:
                try:
                    amt = float(v); break
                except Exception:
                    pass
        if amt <= 0:
            continue

        # 콘텐츠(ct)
        ct = extract_content(props)
        if not ct:
            no_ct_count += 1
            ct = "(미지정)"   # ct 없는 구글 결제도 별도 버킷으로 보존

        agg[(d_iso, ct)]["revenue"] += amt
        agg[(d_iso, ct)]["count"]   += 1

        if sample_seen < 5:
            log.info(f"    📍 sample: date={d_iso} ct={ct!r} amt=₩{amt:,.0f}")
            sample_seen += 1

    log.info(f"  🔍 $insert_id 중복 제거: {dup_skipped}건 스킵")
    log.info(f"  🔍 ch 분포(Top15, dedup후): {dict(sorted(ch_seen.items(), key=lambda x:-x[1])[:15])}")
    log.info(f"  🔍 google 이벤트 {google_total}건 / ct 미지정 {no_ct_count}건 / "
             f"(date,ct)조합 {len(agg)}개")
    return dict(agg)


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
        if not records:
            return 0
        url = f"{self.base}/rest/v1/{table}"
        ok = 0
        for i in range(0, len(records), chunk):
            batch = records[i:i+chunk]
            resp = req_lib.post(url, headers=self.headers, json=batch, timeout=60)
            if resp.status_code in (200, 201):
                ok += len(batch)
                log.info(f"  ✅ upsert {ok}/{len(records)} → {table}")
            else:
                log.error(f"  ❌ HTTP {resp.status_code}: {resp.text[:300]}")
        return ok

    def delete_range(self, table, date_from, date_to):
        """갱신 시 (date,content) 조합이 사라질 수 있으므로 기간 단위 선삭제 후 재삽입"""
        url = f"{self.base}/rest/v1/{table}?date=gte.{date_from}&date=lte.{date_to}"
        try:
            resp = req_lib.delete(url, headers=self.headers, timeout=60)
            if resp.status_code in (200, 204):
                log.info(f"  🗑  {table} {date_from}~{date_to} 기존 행 삭제")
            else:
                log.warning(f"  ⚠️ delete {table}: HTTP {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            log.warning(f"  ⚠️ delete {table} 예외: {e}")


def main():
    log.info("=" * 60)
    log.info("🚀 Mixpanel(ch=google) → Supabase (google_demandgen_content_mp_daily)")
    log.info("=" * 60)
    lines = fetch_mixpanel(START, END)
    if not lines:
        log.warning("  ⚠️ 빈 결과"); return
    agg = aggregate(lines)
    if not agg:
        log.warning("  ⚠️ ch=google 결제 집계 결과 없음 — 스킵"); return
    log.info(f"📦 (date,content)조합 {len(agg)}개")

    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)

    # PRIMARY KEY (date, content) — 갱신 시점에 더이상 발생 안 하는 조합 잔존 방지 위해
    # 기간 단위 선삭제 후 재삽입 (naver_keyword_mp_daily 와 동일 패턴)
    sb.delete_range("google_demandgen_content_mp_daily", START.isoformat(), END.isoformat())

    records = []
    for (d_iso, ct), v in sorted(agg.items()):
        records.append({
            "date": d_iso,
            "content": ct[:300],   # 너무 긴 값 방어
            "revenue": round(v["revenue"], 2),
            "purchase_count": v["count"],
        })

    # 콘텐츠 Top10 (기간 합계) 로그
    by_ct = defaultdict(lambda: {"revenue": 0.0, "count": 0})
    for (d_iso, ct), v in agg.items():
        by_ct[ct]["revenue"] += v["revenue"]
        by_ct[ct]["count"]   += v["count"]
    top = sorted(by_ct.items(), key=lambda x: -x[1]["revenue"])[:10]
    log.info("  🏆 콘텐츠 Top10 (기간 합계):")
    for ct, v in top:
        log.info(f"    · {ct[:40]:40s}  ₩{v['revenue']:>12,.0f}  ({v['count']}건)")

    sb.upsert("google_demandgen_content_mp_daily", records)
    log.info("✅ 완료")


if __name__ == "__main__":
    main()
