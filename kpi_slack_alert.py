# -*- coding: utf-8 -*-
"""
kpi_slack_alert.py — Supabase 데이터 조건 검사 → Slack 알람 (중복 방지)
======================================================================
현재 조건:
  [일ROAS 경고] 어제 국내 광고 일ROAS < 100% (KR 지출 > 매출)  · ad_performance_daily

중복 방지: alert_log 테이블에 alert_key 기록 → 이미 보낸 키는 skip
  (파이프라인이 하루 4회 돌아도 같은 알람은 1회만 전송).
  alert_key 에 "대상 날짜"가 들어가므로 날짜가 바뀌면 다시 검사·전송.

env: SUPABASE_URL, SUPABASE_SERVICE_KEY, SLACK_WEBHOOK_URL
옵션: --dry (Slack 전송/기록 생략, 판정만 출력)
로컬 실행 시 ./.env 또는 ../meta_scraper/.env 자동 로드 (SLACK_WEBHOOK_URL 은 직접 설정).

새 조건 추가: check_* 함수 만들고 ALERT_CHECKS 에 등록. (key, message) 반환 or None.
"""
import os, sys, json, logging, urllib.request, urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path


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
SLACK = os.environ.get("SLACK_WEBHOOK_URL", "")
SBH = {"apikey": SB_KEY, "Authorization": "Bearer " + SB_KEY, "Content-Type": "application/json"}
KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
DRY = "--dry" in sys.argv


# ───────────────────── Supabase ─────────────────────
def sb_get(table, q):
    req = urllib.request.Request(f"{SB_URL}/rest/v1/{table}?{q}", headers=SBH)
    return json.loads(urllib.request.urlopen(req, timeout=30).read().decode("utf-8"))


def sb_insert(table, rec):
    req = urllib.request.Request(f"{SB_URL}/rest/v1/{table}", data=json.dumps(rec).encode("utf-8"),
                                 headers={**SBH, "Prefer": "return=minimal"}, method="POST")
    urllib.request.urlopen(req, timeout=30).read()


def already_sent(key):
    try:
        rows = sb_get("alert_log", f"alert_key=eq.{urllib.parse.quote(key)}&select=alert_key&limit=1")
        return len(rows) > 0
    except Exception as e:
        log.warning(f"alert_log 조회 실패(전송 진행): {e}")
        return False


def mark_sent(key, detail):
    try:
        sb_insert("alert_log", {"alert_key": key, "detail": detail})
    except Exception as e:
        log.warning(f"alert_log 기록 실패: {e}")


# ───────────────────── Slack ─────────────────────
def slack_send(blocks, fallback):
    if not SLACK:
        log.error("SLACK_WEBHOOK_URL 미설정 — 전송 불가")
        return
    body = json.dumps({"text": fallback, "blocks": blocks}).encode("utf-8")
    req = urllib.request.Request(SLACK, data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    urllib.request.urlopen(req, timeout=15).read()


def won(n):
    return "₩" + format(round(n), ",")


# ───────────────────── 조건 ─────────────────────
def check_daily_roas():
    """어제 국내 광고 일ROAS < 100% (지출 > 매출)."""
    y = (TODAY - timedelta(days=1)).isoformat()
    rows = sb_get("ad_performance_daily", f"date=eq.{y}&select=spend,revenue")
    spend = sum(float(r.get("spend") or 0) for r in rows)
    rev = sum(float(r.get("revenue") or 0) for r in rows)
    if spend <= 0:
        return None
    roas = rev / spend
    if rev < spend:
        msg = (f"🔴 *어제({y}) 국내 광고 일ROAS {roas*100:.0f}%*\n"
               f"지출 {won(spend)} > 매출 {won(rev)}  (손실 {won(spend - rev)})")
        return (f"daily_roas:{y}", msg)
    log.info(f"일ROAS {roas*100:.0f}% — 정상({y})")
    return None


ALERT_CHECKS = [check_daily_roas]


def main():
    log.info("=" * 56)
    log.info("🔔 KPI Slack 알람 검사" + ("  [DRY]" if DRY else ""))
    sent = 0
    for chk in ALERT_CHECKS:
        try:
            res = chk()
        except Exception as e:
            log.error(f"{chk.__name__} 검사 오류: {e}")
            continue
        if not res:
            continue
        key, msg = res
        if already_sent(key):
            log.info(f"이미 전송됨, skip: {key}")
            continue
        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": msg}},
            {"type": "context", "elements": [{"type": "mrkdwn",
             "text": f"검사 {TODAY} (KST) · <https://new-tightauto.vercel.app|📊 대시보드 열기>"}]},
        ]
        if DRY:
            log.info(f"[DRY] 전송 대상: {key}\n{msg}")
        else:
            slack_send(blocks, msg.splitlines()[0])
            mark_sent(key, msg.replace("\n", " | "))
            log.info(f"✅ 전송: {key}")
        sent += 1
    if sent == 0:
        log.info("조건 미해당 — 알람 없음")


if __name__ == "__main__":
    main()
