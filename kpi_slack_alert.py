# -*- coding: utf-8 -*-
"""
kpi_slack_alert.py — Supabase 데이터 조건 검사 → Slack 알람 (중복 방지)
======================================================================
조건:
  [고예산 저ROAS 세트] 오늘(실시간) 국내 광고세트 중
        일예산(budget) >= 800,000  AND  ROAS < 110%  (지출 기준)
        + 오전 오알람 방지: 오늘 지출이 일예산의 SPEND_MATURITY(기본 15%) 이상인 세트만
        → ROAS 낮은 순 목록(세트ID 포함). 조건 지속 시 매시간 재알림  · ad_performance_daily
  [일ROAS 경고] 어제 국내 광고 일ROAS < 100% (KR 지출 > 매출)   · ad_performance_daily

중복 방지: alert_log(alert_key PK).
  - 고예산세트 key = 날짜+시각(HH)+세트id → 매시간 재검사·재알림(같은 시각 중복 실행만 차단)
  - 일ROAS key = 날짜 → 하루 1회(어제 기준이라 불변)

env: SUPABASE_URL, SUPABASE_SERVICE_KEY, SLACK_WEBHOOK_URL
옵션: --dry (전송/기록 생략, 판정만 출력)
로컬 실행 시 ./.env 또는 ../meta_scraper/.env 자동 로드.

새 조건 추가: check_* 함수 만들어 ALERT_CHECKS 에 등록.
  반환 형식: {"title": str, "items": [(alert_key, mrkdwn_line), ...]}  또는 None.
"""
import os, sys, json, logging, urllib.request, urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── 임계값 (필요시 여기만 수정) ──
ADSET_BUDGET_MIN = 800_000   # 일예산 하한 (KRW)
ADSET_ROAS_MAX   = 110       # ROAS 상한 (% 미만이면 알람)
SPEND_MATURITY   = 0.15      # 오늘 지출이 일예산의 이 비율 이상인 세트만 판정(새벽 빈데이터 오알람 방지). 0=끔
ADSET_MAX_LINES  = 30        # 한 메시지 최대 세트 수


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
NOW = datetime.now(KST).replace(tzinfo=None)
TODAY = NOW.date()
DRY = "--dry" in sys.argv


# ───────────────────── Supabase ─────────────────────
def sb_get(table, q):
    out, off = [], 0
    while True:
        req = urllib.request.Request(f"{SB_URL}/rest/v1/{table}?{q}&limit=1000&offset={off}", headers=SBH)
        chunk = json.loads(urllib.request.urlopen(req, timeout=40).read().decode("utf-8"))
        out += chunk
        if len(chunk) < 1000:
            return out
        off += 1000


def sb_insert(table, rec):
    req = urllib.request.Request(f"{SB_URL}/rest/v1/{table}", data=json.dumps(rec).encode("utf-8"),
                                 headers={**SBH, "Prefer": "return=minimal"}, method="POST")
    urllib.request.urlopen(req, timeout=30).read()


def already_sent(key):
    try:
        rows = sb_get("alert_log", f"alert_key=eq.{urllib.parse.quote(key)}&select=alert_key")
        return len(rows) > 0
    except Exception as e:
        log.warning(f"alert_log 조회 실패(전송 진행): {e}")
        return False


def mark_sent(key, detail):
    try:
        sb_insert("alert_log", {"alert_key": key, "detail": detail[:500]})
    except Exception as e:
        log.warning(f"alert_log 기록 실패: {e}")


# ───────────────────── Slack ─────────────────────
def slack_send(blocks, fallback):
    if not SLACK:
        log.error("SLACK_WEBHOOK_URL 미설정 — 전송 불가")
        return False
    body = json.dumps({"text": fallback, "blocks": blocks}).encode("utf-8")
    req = urllib.request.Request(SLACK, data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    urllib.request.urlopen(req, timeout=15).read()
    return True


def won(n):
    """가독성을 위해 만원 단위로 축약 (1만 미만은 원 단위)."""
    n = round(n)
    if abs(n) >= 10_000:
        man = n / 10_000
        return f"₩{man:,.0f}만" if abs(man - round(man)) < 0.05 else f"₩{man:,.1f}만"
    return "₩" + format(n, ",")


def sev_emoji(roas):
    """ROAS 심각도: 🔴<70 · 🟠<90 · 🟡 그 외(90~110)."""
    return "🔴" if roas < 70 else ("🟠" if roas < 90 else "🟡")


# ───────────────────── 조건 ─────────────────────
def check_adset_low_roas():
    """오늘(실시간) 일예산 800k↑ & ROAS<110% 광고세트 (지출 성숙도 가드)."""
    d = TODAY.isoformat()
    rows = sb_get("ad_performance_daily",
                  f"date=eq.{d}&select=adset_id,adset_name,product,campaign_name,budget,spend,revenue")
    qual = []
    for r in rows:
        budget = float(r.get("budget") or 0)
        spend = float(r.get("spend") or 0)
        rev = float(r.get("revenue") or 0)
        if budget < ADSET_BUDGET_MIN or spend <= 0:
            continue
        if SPEND_MATURITY > 0 and spend < budget * SPEND_MATURITY:
            continue  # 아직 일예산 대비 충분히 안 써서 판정 보류(오전 오알람 방지)
        roas = rev / spend * 100
        if roas < ADSET_ROAS_MAX:
            qual.append((roas, r, budget, spend, rev))
    qual.sort(key=lambda x: x[0])  # ROAS 낮은 순
    items = []
    hh = f"{NOW:%H}"  # KST 시 — 매시간 재알림용 dedup 키 (조건 지속 시 매시간 재전송)
    for roas, r, budget, spend, rev in qual[:ADSET_MAX_LINES]:
        name = (r.get("adset_name") or r.get("adset_id") or "?")[:46]
        prod = r.get("product") or "-"
        aid = r.get("adset_id") or "?"
        key = f"adset_low_roas:{d}T{hh}:{aid}"   # 세트별 시간당 1회 → 매시간 재알림
        loss = spend - rev
        pl = f"적자 *{won(loss)}*" if loss > 0 else f"이익 {won(-loss)}"
        line = (f"{sev_emoji(roas)} *{name}*  ·  {prod}\n"
                f"↳ ROAS *{roas:.0f}%*  ·  지출 {won(spend)} → 매출 {won(rev)}  ·  {pl}\n"
                f"↳ 예산 {won(budget)}  ·  세트ID `{aid}`")
        items.append((key, line))
    extra = len(qual) - ADSET_MAX_LINES
    if extra > 0:
        items.append((f"adset_low_roas_more:{d}T{hh}", f"_…외 {extra}개 세트 더 있음_"))
    if not items:
        log.info(f"고예산 저ROAS 세트 없음 ({d})")
        return None
    return {"title": f"🚨 고예산 저ROAS 세트  ·  {len(qual)}건",
            "subtitle": f"오늘 {d} · 일예산 {won(ADSET_BUDGET_MIN)}↑ & ROAS<{ADSET_ROAS_MAX}% · ROAS 낮은 순",
            "items": items}


def check_daily_roas():
    """어제 국내 광고 일ROAS < 100% (지출 > 매출)."""
    y = (TODAY - timedelta(days=1)).isoformat()
    rows = sb_get("ad_performance_daily", f"date=eq.{y}&select=spend,revenue")
    spend = sum(float(r.get("spend") or 0) for r in rows)
    rev = sum(float(r.get("revenue") or 0) for r in rows)
    if spend <= 0:
        return None
    roas = rev / spend * 100
    if rev < spend:
        line = (f"🔴 어제(*{y}*) 국내 전체 일ROAS *{roas:.0f}%*\n"
                f"↳ 지출 {won(spend)} > 매출 {won(rev)}  ·  손실 *{won(spend - rev)}*")
        return {"title": "🔴 국내 일ROAS 경고", "subtitle": "어제 KR 광고 지출 > 매출", "items": [(f"daily_roas:{y}", line)]}
    log.info(f"국내 일ROAS {roas:.0f}% — 정상({y})")
    return None


ALERT_CHECKS = [check_adset_low_roas, check_daily_roas]


def main():
    log.info("=" * 56)
    log.info("🔔 광고 알람 검사" + ("  [DRY]" if DRY else ""))
    sections, keys = [], []
    for chk in ALERT_CHECKS:
        try:
            res = chk()
        except Exception as e:
            log.error(f"{chk.__name__} 오류: {e}")
            continue
        if not res:
            continue
        new = [(k, l) for k, l in res["items"] if not already_sent(k)]
        if not new:
            log.info(f"[{chk.__name__}] 해당 항목 이미 전송됨")
            continue
        sections.append((res["title"], res.get("subtitle", ""), [l for _, l in new]))
        keys += [k for k, _ in new]

    if not sections:
        log.info("전송할 새 알람 없음")
        return

    # ── Slack Block Kit 조립 (헤더 → 섹션별 제목/부제 → 카드 라인(분할) → 구분선 → 푸터) ──
    blocks = [{"type": "header",
               "text": {"type": "plain_text", "text": "🚨 Tight 광고 퍼포먼스 알람", "emoji": True}}]
    for i, (title, subtitle, lines) in enumerate(sections):
        if i > 0:
            blocks.append({"type": "divider"})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*{title}*"}})
        if subtitle:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": subtitle}]})
        # Slack section 텍스트 3000자 제한 → 여러 블록으로 분할 (6줄/2600자 단위)
        buf = []
        for l in lines:
            if buf and (len(buf) >= 6 or sum(len(x) for x in buf) + len(l) > 2600):
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(buf)}})
                buf = []
            buf.append(l)
        if buf:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(buf)}})
    blocks.append({"type": "divider"})
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn",
                   "text": f"🕒 {NOW:%Y-%m-%d %H:%M} KST   ·   <https://new-tightauto.vercel.app|📊 대시보드 열기>"}]})

    fallback = sections[0][0]
    if DRY:
        log.info("[DRY] 전송 예정:")
        for title, subtitle, lines in sections:
            print(f"\n■ {title}" + (f"   ({subtitle})" if subtitle else ""))
            for l in lines:
                print("   " + l.replace("*", "").replace("\n", "\n   "))
    else:
        if slack_send(blocks, fallback):
            for k in keys:
                mark_sent(k, "sent")
            log.info(f"✅ 전송 완료 ({len(keys)}개 항목)")


if __name__ == "__main__":
    main()
