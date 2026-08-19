# -*- coding: utf-8 -*-
"""
dashboard_bot.py — 대시보드(Supabase) 지표를 가공해 마케팅 채널에 매일 11시 게시하는 봇.

  · 국내 메시지 → 국내 마케팅 채널 (₩)
  · 글로벌 메시지 → 글로벌 마케팅 채널 ($, 환율 표기)

비교 구간: (오늘-DAYS_BACK-1) → (오늘-DAYS_BACK)
  기본 DAYS_BACK=0 → 어제 vs 오늘 (예: 오늘 6/30 → 6/29 vs 6/30).
  오전 실행 시 '오늘'은 부분일 데이터이므로, 완결일끼리 비교하려면 DAYS_BACK=1.

전송: 봇 1개의 Slack Bot 토큰(chat.postMessage)으로 채널에 게시.
  필요한 .env 키:
    SUPABASE_URL, SUPABASE_SERVICE_KEY, SLACK_BOT_TOKEN
    SLACK_CH_KR_MARKETING   (국내 메시지 게시 채널 ID)
    SLACK_CH_GL_MARKETING   (글로벌 메시지 게시 채널 ID; 같은 채널이면 동일값)

사용:
  py dashboard_bot.py                 # 실제 전송
  py dashboard_bot.py --dry-run       # 전송 없이 메시지 + 구성요소 출력
  py dashboard_bot.py --dry-run --dates 2026-06-27,2026-06-28   # 특정 두 날짜로 검증
  py dashboard_bot.py --kr-only | --gl-only
  py dashboard_bot.py --dry-run --exp-only   # 🧪 실험 현황 알림만 미리보기
  py dashboard_bot.py --no-exp               # 실험 현황 알림 끄기

※ '전체 종합' 공식은 calc_kr_total / calc_gl_total 에 분리해 두었다(아래 주석 참고).
   예시 숫자와 대조해 이 두 함수만 조정하면 된다.
※ 채널에 나가는 메시지는 3개다: ①퍼포먼스 표 ②그 스레드 댓글(증감액 조언)
   ③🧪 실험 현황 · 오늘의 변화(별도 메시지, 변화 있는 날만).
"""
import os, sys, json, re, datetime, urllib.request, urllib.parse
from pathlib import Path

BASE = Path(__file__).parent
DRY = "--dry-run" in sys.argv
KR_ONLY = "--kr-only" in sys.argv
GL_ONLY = "--gl-only" in sys.argv
NO_EXP = "--no-exp" in sys.argv    # 🧪 실험 현황 알림 끄기
EXP_ONLY = "--exp-only" in sys.argv  # 실험 현황 알림만 (퍼포먼스 표·조언 생략)
DAYS_BACK = 1  # 0=어제vs오늘, 1=그제vs어제(완결일) ← 11시 전송은 완결일 비교 권장

# 특정 날짜 강제 (검증용): --dates D_PREV,D_CUR
FORCE_DATES = None
if "--dates" in sys.argv:
    i = sys.argv.index("--dates")
    FORCE_DATES = sys.argv[i + 1].split(",")

# ---------- 설정 로드: 로컬 .env + 환경변수(GitHub Actions secrets) ----------
def load_env():
    env = {}
    p = BASE / ".env"
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip().lstrip("﻿")
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'").replace("\r", "")
    # .env에 없는 키는 환경변수에서 보강 (GitHub Actions에서는 .env 없이 secrets 주입)
    for k in ("SUPABASE_URL", "SUPABASE_SERVICE_KEY", "SLACK_BOT_TOKEN",
              "SLACK_CH_KR_MARKETING", "SLACK_CH_GL_MARKETING", "ANTHROPIC_API_KEY"):
        if not env.get(k) and os.environ.get(k):
            env[k] = os.environ[k].strip()
    return env

ENV = load_env()
SB_URL = ENV["SUPABASE_URL"].rstrip("/")
SB_KEY = ENV["SUPABASE_SERVICE_KEY"]
SBH = {"apikey": SB_KEY, "Authorization": "Bearer " + SB_KEY}
# new-tightauto: SUPABASE_DB_SCHEMA 설정 시 스키마 프로파일 헤더 (미설정=기존 public)
_sc = os.environ.get('SUPABASE_DB_SCHEMA', '').strip()
if _sc:
    SBH['Accept-Profile'] = _sc
    SBH['Content-Profile'] = _sc
BOT = ENV.get("SLACK_BOT_TOKEN", "")          # 봇 1개 (양쪽 채널에 게시)
CH_KR = ENV.get("SLACK_CH_KR_MARKETING", "")
CH_GL = ENV.get("SLACK_CH_GL_MARKETING", "")

# ── 조언(스레드 댓글) 설정 ──
ANTHROPIC_KEY = ENV.get("ANTHROPIC_API_KEY", "")
NO_ADVICE = "--no-advice" in sys.argv
NO_HL = "--no-hl" in sys.argv  # 조언→추이차트 하이라이트 자동표기 끄기
DISTILL = "--distill-lessons" in sys.argv  # 월간 교훈 증류 모드(조언/게시 안 함)
ADVICE_DAYS = 7  # 세트 분석에 사용할 최근 일수
LESSON_WINDOW = 90  # 교훈 증류 시 훑는 최근 일수(14일 조언창을 넘는 장기 패턴 학습)
# 메타 퍼포먼스 증감액 플레이북 (구글 문서 → txt export, 실패 시 로컬 캐시)
GDOC_URL = "https://docs.google.com/document/d/1mH5_iDCqEXQbrt4dVCAJbKI-q1raYMeP5MtGskWBzu4/export?format=txt"
PLAYBOOK_CACHE = BASE / "dashboard_bot_playbook.txt"

KST = datetime.timezone(datetime.timedelta(hours=9))
WD = ["월", "화", "수", "목", "금", "토", "일"]

def wd(dstr):
    y, m, d = map(int, dstr.split("-"))
    return WD[datetime.date(y, m, d).weekday()]

def md(dstr):  # 06-29
    return dstr[5:]

# ---------- Supabase ----------
def sb(table, q):
    out, off = [], 0
    while True:
        req = urllib.request.Request(f"{SB_URL}/rest/v1/{table}?{q}&limit=1000&offset={off}", headers=SBH)
        chunk = json.loads(urllib.request.urlopen(req, timeout=60).read().decode("utf-8"))
        out += chunk
        if len(chunk) < 1000:
            return out
        off += 1000

def sb_upsert(table, rows):
    """adset_id 충돌 시 병합(merge-duplicates)하는 upsert. rows=dict 또는 dict 리스트.
    payload에 넣은 컬럼만 갱신 → memo 등 미포함 컬럼은 보존(index.html saveHL과 동일 규칙)."""
    data = json.dumps(rows).encode("utf-8")
    req = urllib.request.Request(
        f"{SB_URL}/rest/v1/{table}", data=data,
        headers={**SBH, "Content-Type": "application/json",
                 "Prefer": "resolution=merge-duplicates,return=minimal"}, method="POST")
    urllib.request.urlopen(req, timeout=30).read()

def col_sum(table, field, date, extra=""):
    """table에서 date의 field 합계 (다중 행 합산)."""
    rows = sb(table, f"date=eq.{date}&select={field}{extra}")
    return sum((r.get(field) or 0) for r in rows)

# =====================================================================
# 국내(KR) 지표
# =====================================================================
def calc_kr(date):
    """반환: dict(meta_rev, meta_spend, total_rev, total_spend, comp=구성요소)."""
    meta_rev = col_sum("ad_performance_daily", "revenue", date)
    meta_spend = col_sum("ad_performance_daily", "spend", date)

    # ── 전체 종합 정의 ───────────────────────────────────────────
    #  전체 매출 = 토스 매출(net)만. (사용자 정의: 글로벌 Stripe 미포함)
    #  전체 지출 = 국내 광고비 전체 (메타 + 구글Ads + 네이버SA/PL + 구글DG).
    comp = {
        "토스_net": col_sum("toss_daily_revenue", "net_amount", date),
        "메타_지출": meta_spend,
        "구글ads_비용": col_sum("google_ads_daily", "cost_vat", date),
        "네이버SA_비용": col_sum("naver_sa_daily", "cost_vat", date),
        "네이버PL_비용": col_sum("naver_powerlink_daily", "cost_vat", date),
        "구글DG_비용": col_sum("google_demandgen_daily", "cost_vat", date),
    }
    total_rev = comp["토스_net"]
    total_spend = (comp["메타_지출"] + comp["구글ads_비용"] + comp["네이버SA_비용"] +
                   comp["네이버PL_비용"] + comp["구글DG_비용"])
    return {"meta_rev": meta_rev, "meta_spend": meta_spend,
            "total_rev": total_rev, "total_spend": total_spend, "comp": comp}

# =====================================================================
# 글로벌(GL) 지표 — USD
# =====================================================================
VN_TW_ACC = "act_1286632473622244"  # 밴스드 대만 (글로벌 귀속)

def gl_rate(date):
    rows = sb("global_stripe_daily", f"date=eq.{date}&select=usd_krw_rate")
    rates = [r["usd_krw_rate"] for r in rows if r.get("usd_krw_rate")]
    return max(rates) if rates else 0

def calc_gl(date):
    meta_rev = col_sum("global_ad_performance_daily", "revenue_usd", date)
    meta_spend = col_sum("global_ad_performance_daily", "spend_usd", date)
    rate = gl_rate(date) or 1
    # 밴스드(대만) — KRW 적재 → USD 환산
    vn_rev_krw = col_sum("vanced_ad_performance_daily", "revenue", date, f"&ad_account_id=eq.{VN_TW_ACC}")
    vn_spend_krw = col_sum("vanced_ad_performance_daily", "spend", date, f"&ad_account_id=eq.{VN_TW_ACC}")
    stripe_usd = col_sum("global_stripe_daily", "revenue_usd", date)
    comp = {
        "글로벌메타_매출": meta_rev, "글로벌메타_지출": meta_spend,
        "Stripe총매출": stripe_usd,
        "밴스드_매출KRW": vn_rev_krw, "밴스드_지출KRW": vn_spend_krw,
        "환율": rate,
    }
    # 전체 매출 = Stripe 총액만 (대만/밴스드 결제가 이미 Stripe에 포함 → 중복합산 금지).
    # 전체 메타지출 = 글로벌 메타 + 밴스드(대만) 지출 USD환산.
    total_rev = stripe_usd
    total_meta_spend = meta_spend + (vn_spend_krw / rate)
    return {"meta_rev": meta_rev, "meta_spend": meta_spend,
            "total_rev": total_rev, "total_meta_spend": total_meta_spend,
            "rate": rate, "comp": comp}

# =====================================================================
# 메시지 포맷
# =====================================================================
def won(n):
    return "₩" + format(int(round(n)), ",")

def usd(n):
    return "$" + format(int(round(n)), ",")

def roas(rev, spend):
    return round(rev / spend * 100) if spend else 0

# 표시 폭 기준 정렬 (한글/전각=2칸) — 코드블록 표 정렬용
def dw(s):
    w = 0
    for ch in s:
        w += 2 if ("가" <= ch <= "힣" or "㄰" <= ch <= "㆏"
                   or "＀" <= ch <= "￯" or "一" <= ch <= "鿿") else 1
    return w

def ljust(s, n):
    return s + " " * max(0, n - dw(s))

def rjust(s, n):
    return " " * max(0, n - dw(s)) + s

ARROW = "→"

def row(label, v1, v2, lw, vw):
    """라벨 + 그제값 → 어제값 (값은 우측정렬)."""
    return f"{ljust(label, lw)}{rjust(v1, vw)} {ARROW} {rjust(v2, vw)}"

def fmt_kr(dp, dc, p, c):
    wp, wc = wd(dp), wd(dc)
    mp, mc = p["meta_rev"] - p["meta_spend"], c["meta_rev"] - c["meta_spend"]
    tp, tc = p["total_rev"] - p["total_spend"], c["total_rev"] - c["total_spend"]
    LW, VW = 9, 13
    body = "\n".join([
        "〈ROAS〉",
        row("메타", f"{roas(p['meta_rev'],p['meta_spend'])}%", f"{roas(c['meta_rev'],c['meta_spend'])}%", LW, VW),
        row("전체종합", f"{roas(p['total_rev'],p['total_spend'])}%", f"{roas(c['total_rev'],c['total_spend'])}%", LW, VW),
        "",
        "〈메타 · 추이차트 기준〉",
        row("매출", won(p["meta_rev"]), won(c["meta_rev"]), LW, VW),
        row("지출", won(p["meta_spend"]), won(c["meta_spend"]), LW, VW),
        row("순이익", won(mp), won(mc), LW, VW),
        "",
        "〈전체 종합 · 토스페이 기준〉",
        row("매출", won(p["total_rev"]), won(c["total_rev"]), LW, VW),
        row("지출", won(p["total_spend"]), won(c["total_spend"]), LW, VW),
        row("순이익", won(tp), won(tc), LW, VW),
    ])
    head = f"🇰🇷 *국내 메타 & 전체 종합*   {md(dp)}({wp}) {ARROW} {md(dc)}({wc})"
    return f"{head}\n```\n{body}\n```"

def fmt_gl(dp, dc, p, c):
    wp, wc = wd(dp), wd(dc)
    mp, mc = p["meta_rev"] - p["meta_spend"], c["meta_rev"] - c["meta_spend"]
    tp, tc = p["total_rev"] - p["total_meta_spend"], c["total_rev"] - c["total_meta_spend"]
    LW, VW = 9, 9
    body = "\n".join([
        "〈ROAS〉",
        row("메타", f"{roas(p['meta_rev'],p['meta_spend'])}%", f"{roas(c['meta_rev'],c['meta_spend'])}%", LW, VW),
        row("전체종합", f"{roas(p['total_rev'],p['total_meta_spend'])}%", f"{roas(c['total_rev'],c['total_meta_spend'])}%", LW, VW),
        "",
        "〈메타 · 추이차트 기준〉",
        row("매출", usd(p["meta_rev"]), usd(c["meta_rev"]), LW, VW),
        row("지출", usd(p["meta_spend"]), usd(c["meta_spend"]), LW, VW),
        row("순이익", usd(mp), usd(mc), LW, VW),
        "",
        "〈전체 종합 · 밴스드 포함〉",
        f"환율 {c['rate']:,.2f}원/USD (양일 동일 적용)",
        row("매출", usd(p["total_rev"]), usd(c["total_rev"]), LW, VW),
        row("메타지출", usd(p["total_meta_spend"]), usd(c["total_meta_spend"]), LW, VW),
        row("순이익", usd(tp), usd(tc), LW, VW),
    ])
    head = f"🌏 *글로벌 메타 & 전체 종합*   {md(dp)}({wp}) {ARROW} {md(dc)}({wc})  (USD)"
    return f"{head}\n```\n{body}\n```"

# =====================================================================
# Slack 전송
# =====================================================================
def slack_post(channel, text, thread_ts=None):
    payload = {"channel": channel, "text": text, "unfurl_links": False}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request("https://slack.com/api/chat.postMessage", data=body,
                                 headers={"Authorization": f"Bearer {BOT}",
                                          "Content-Type": "application/json; charset=utf-8"}, method="POST")
    r = json.loads(urllib.request.urlopen(req, timeout=30).read().decode("utf-8"))
    return r.get("ok", False), (r.get("ts") if r.get("ok") else r.get("error"))

def slack_get(method, params):
    qs = urllib.parse.urlencode(params)
    req = urllib.request.Request(f"https://slack.com/api/{method}?{qs}",
                                 headers={"Authorization": f"Bearer {BOT}"})
    return json.loads(urllib.request.urlopen(req, timeout=30).read().decode("utf-8"))

def fetch_thread_context(channel, region, days=ADVICE_DAYS, max_chars=4000):
    """과거 부모글(같은 지역)의 스레드 댓글(내 지난 조언 + 사람들의 토론)을 모은다.
    필요한 봇 스코프: channels:history (공개채널) / groups:history (비공개)."""
    if not BOT or not channel:
        return ""
    # Slack은 텍스트에서 이모지를 :kr:/:earth_asia: 숏코드로 반환 → 한글 라벨로 식별
    marker = "국내 메타" if region == "kr" else "글로벌 메타"
    oldest = (datetime.datetime.now(KST) - datetime.timedelta(days=days)).timestamp()
    hist = slack_get("conversations.history", {"channel": channel, "oldest": f"{oldest:.0f}", "limit": 200})
    if not hist.get("ok"):
        print(f"  [조언] 스레드 히스토리 조회 불가: {hist.get('error')} (channels:history 권한 필요)")
        return ""
    # 같은 지역의 부모 퍼포먼스 글 중 스레드 있는 것 (오래된→최근 순으로 정렬해 맥락 누적)
    parents = [m for m in hist.get("messages", [])
               if marker in (m.get("text") or "") and m.get("reply_count", 0) > 0]
    parents.sort(key=lambda m: float(m.get("ts", 0)))
    blocks = []
    for pm in parents:
        rep = slack_get("conversations.replies", {"channel": channel, "ts": pm["ts"], "limit": 50})
        if not rep.get("ok"):
            continue
        day = (pm.get("text") or "").split("\n")[0]
        for m in rep.get("messages", [])[1:]:  # 부모(표) 제외
            who = "봇" if m.get("bot_id") or m.get("app_id") else "사람"
            t = (m.get("text") or "").strip()
            if t:
                blocks.append(f"〔{who}〕{t}")
    ctx = "\n".join(blocks)
    return ctx[-max_chars:] if ctx else ""

# =====================================================================
# 조언 (플레이북 + 세트/메모/증감액표시 + 과거 스레드 토론 → Claude → 스레드 댓글)
# =====================================================================
def fetch_playbook():
    """구글 문서(플레이북)를 실시간 로드. 성공 시 캐시 갱신, 실패 시 캐시 사용."""
    try:
        req = urllib.request.Request(GDOC_URL, headers={"User-Agent": "Mozilla/5.0"})
        txt = urllib.request.urlopen(req, timeout=30).read().decode("utf-8").lstrip("﻿")
        if len(txt) > 500:
            try:
                PLAYBOOK_CACHE.write_text(txt, encoding="utf-8")
            except Exception:
                pass
            return txt, "live"
    except Exception as e:
        print(f"  [조언] 플레이북 실시간 로드 실패({e}) → 캐시 사용")
    if PLAYBOOK_CACHE.exists():
        return PLAYBOOK_CACHE.read_text(encoding="utf-8").lstrip("﻿"), "cache"
    return "", "none"

# region별 테이블/필드 맵: (일별테이블, 하이라이트테이블, 지출필드, 매출필드, 예산필드, 통화)
ADV_SRC = {
    "kr": ("ad_performance_daily", "adset_highlights", "spend", "revenue", "budget", "₩"),
    "gl": ("global_ad_performance_daily", "global_adset_highlights", "spend_usd", "revenue_usd", "budget_usd", "$"),
}

HIST_DAYS = 14  # 증감액 액션 이력 조회 창 (7일 성과요약보다 길게 봐야 '그 조치가 먹혔는지' 판단 가능)

# =====================================================================
# Meta effective_status (현재 활성/중단) — 하이라이트는 '활성' 세트만 대상.
#   중단(PAUSED 계열)된 세트에 증감액·OFF 하이라이트를 달지 않도록, 조언 프롬프트에
#   상태를 실어주고 마크 적용 단계에서 '중단 확정' 세트를 하드 필터한다.
#   계정→토큰 맵은 세트별 파이프라인(국내_세트별_supabase.py / 글로벌_세트별_supabase.py)과
#   동일 — 그쪽이 정본이므로 계정이 바뀌면 양쪽을 함께 맞춘다.
# =====================================================================
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
def _mtok(k):
    # .env(로컬, ENV엔 .env 전체 키가 담김) → os.environ(GitHub Actions secrets) 순으로 읽는다.
    return (ENV.get(k) or os.environ.get(k, "")).strip()
_MT1 = _mtok("META_TOKEN_1")
_MT2 = _mtok("META_TOKEN_2")
_MT_GL = _mtok("META_TOKEN_GlobalTT") or _mtok("META_TOKEN_4") or _mtok("META_TOKEN_3")
_MT_9937 = _mtok("META_TOKEN_ACT_9937")
META_TOKENS = {
    # 국내 3계정 (국내_세트별_supabase.py)
    "act_1270614404675034": _MT1,
    "act_707835224206178": _MT1,
    "act_1808141386564262": _MT2,
    # 글로벌 (글로벌_세트별_supabase.py)
    "act_1054081590008088": _MT1,
    "act_2677707262628563": _MT_GL,
    "act_1335040608536838": _MT_GL,
    "act_993712016404855": _MT_9937,
    "act_1021437716898605": _MT1,
}
META_ACCOUNTS = {
    "kr": ["act_1270614404675034", "act_707835224206178", "act_1808141386564262"],
    "gl": ["act_1054081590008088", "act_2677707262628563", "act_1335040608536838",
           "act_993712016404855", "act_1021437716898605"],
}
# '활성화중'으로 볼 상태 — 이것만 하이라이트 대상. 나머지(PAUSED·CAMPAIGN_PAUSED·ADSET_PAUSED·
# DISAPPROVED·PENDING_* 등)는 '중단'으로 보고 하이라이트에서 제외한다.
ACTIVE_STATUSES = {"ACTIVE"}
# effective_status 조회 시 넓게 요청(ARCHIVED/DELETED만 자동 제외) → 여기 없는 세트는 보관/삭제 = 비활성.
_STATUS_FILTER = json.dumps([{"field": "effective_status", "operator": "IN", "value": [
    "ACTIVE", "PAUSED", "CAMPAIGN_PAUSED", "ADSET_PAUSED", "IN_PROCESS",
    "WITH_ISSUES", "PENDING_REVIEW", "PENDING_BILLING_INFO", "DISAPPROVED", "PREAPPROVED"]}])

def fetch_active_status(region):
    """region 계정들의 adset effective_status를 조회해 {adset_id: status} 반환.
    토큰이 하나도 없거나 전부 실패하면 {} → 상태 미상으로 두어 하이라이트 필터를 적용하지 않는다
    (기존 동작으로 안전 폴백; '중단 확정'일 때만 제외)."""
    out = {}
    for acc in META_ACCOUNTS.get(region, []):
        tok = META_TOKENS.get(acc, "")
        if not tok:
            continue
        url = f"{META_BASE_URL}/{acc}/adsets"
        params = {"fields": "id,effective_status", "limit": 500,
                  "filtering": _STATUS_FILTER, "access_token": tok}
        try:
            nxt = f"{url}?{urllib.parse.urlencode(params)}"
            while nxt:
                data = json.loads(urllib.request.urlopen(nxt, timeout=60).read().decode("utf-8"))
                for row in data.get("data", []):
                    aid = row.get("id")
                    if aid:
                        out[aid] = row.get("effective_status") or "UNKNOWN"
                nxt = data.get("paging", {}).get("next")
        except Exception as e:
            print(f"  [active_status] {acc} 조회 실패(무시): {e}")
    return out

def _load_human_marks(region, since, dc):
    """durable 사람 마킹 로드 {adset_id: {date: tag}}. 글로벌은 perfTbl.highlight 유실이 잦아
    (daily 늦은 적재) 이 테이블이 사람 조치의 신뢰 소스. 국내도 보강(마킹 유실 방지)."""
    out = {}
    try:
        for r in (sb("human_advice_marks", f"region=eq.{region}&date=gte.{since}&date=lte.{dc}"
                                           f"&select=date,adset_id,tag") or []):
            aid = r.get("adset_id")
            if aid and r.get("tag"):
                out.setdefault(aid, {})[r["date"]] = r["tag"]
    except Exception:
        pass
    return out

def gather_sets(region, dc, days=ADVICE_DAYS):
    """세트별 최근 7일 성과 요약 + 최근 14일 증감액 액션 이력(액션 시점 ROAS 포함) 수집.
    이력(acts)으로 '과거 증감액이 실제로 먹혔는지'를 추세와 대조해 판단할 수 있게 한다."""
    table, hl_table, sf, rf, bf, cur = ADV_SRC[region]
    win = max(days, HIST_DAYS)
    since = (datetime.date.fromisoformat(dc) - datetime.timedelta(days=win - 1)).isoformat()
    rows = sb(table, f"date=gte.{since}&date=lte.{dc}"
                     f"&select=date,adset_id,adset_name,product,{bf},{sf},{rf},highlight,memo"
                     f"&order=date.asc")
    agg = {}
    for r in rows:
        aid = r.get("adset_id") or "?"
        a = agg.setdefault(aid, {"name": r.get("adset_name") or aid, "product": r.get("product") or "",
                                 "budget": 0, "days": {}, "acts": {}, "hl": "", "memo": ""})
        a["name"] = r.get("adset_name") or a["name"]
        a["product"] = r.get("product") or a["product"]
        a["budget"] = max(a["budget"], r.get(bf) or 0)
        a["days"][r["date"]] = (r.get(sf) or 0, r.get(rf) or 0)
        if r.get("highlight"):
            a["acts"][r["date"]] = r["highlight"]  # 날짜별 증감액 액션(중복행 대비 date로 dedup)
            a["hl"] = r["highlight"]
        if r.get("memo"):
            a["memo"] = r["memo"]
    # 세트별 현재 메모/하이라이트 보강 (adset_highlights)
    for r in (sb(hl_table, "select=adset_id,highlight,memo") or []):
        aid = r.get("adset_id")
        if aid in agg:
            if r.get("highlight"):
                agg[aid]["hl"] = r["highlight"]
            if r.get("memo"):
                agg[aid]["memo"] = r["memo"]
    # 사람 조치 durable 병합 (글로벌은 perfTbl.highlight 유실 잦음 → 여기서 채워 국내와 동일하게 이력 확보)
    for aid, dm in _load_human_marks(region, since, dc).items():
        if aid in agg:
            agg[aid]["acts"].update(dm)
    # AI 과거 추천 이력 (ai_advice_marks): 학습용 — 그날 내가(AI) 권한 증감액 vs 사람이 실제 선택한 하이라이트 비교
    ai_marks = {}
    try:
        for r in (sb("ai_advice_marks", f"region=eq.{region}&date=gte.{since}&date=lte.{dc}"
                                        f"&select=date,adset_id,tag") or []):
            aid = r.get("adset_id")
            if aid and r.get("tag"):
                ai_marks.setdefault(aid, {})[r["date"]] = r["tag"]
    except Exception:
        ai_marks = {}   # 테이블 미생성 등 → 비교 생략(무해)
    # 현재 활성/중단 상태(Meta effective_status) — 하이라이트를 활성 세트로 한정하기 위함.
    # 실패 시 {} → 아래에서 active=None(미상)로 두어 필터를 걸지 않는다(안전 폴백).
    status_map = fetch_active_status(region)
    # 요약 라인 생성 (지출 큰 순, 최대 40세트) — 성과는 최근 7일, 이력은 최근 14일
    items = []
    for aid, a in agg.items():
        dts = sorted(a["days"])
        last7 = dts[-ADVICE_DAYS:]
        sp = sum(a["days"][d][0] for d in last7)
        rv = sum(a["days"][d][1] for d in last7)
        if sp <= 0:
            continue
        roas7 = round(rv / sp * 100)
        last3 = dts[-3:]
        trend = "→".join(f"{round(a['days'][d][1]/a['days'][d][0]*100) if a['days'][d][0] else 0}" for d in last3)
        # 증감액 액션 이력: 'MMDD액션@그날ROAS' 시간순 (조치가 먹혔는지 = 이후 추세와 대조)
        hist = []
        for d in sorted(a["acts"]):
            hl = a["acts"][d]
            sp_d, rv_d = a["days"].get(d, (0, 0))
            roas_d = round(rv_d / sp_d * 100) if sp_d else 0
            hist.append(f"{d[5:]}{HL_SHORT.get(hl, hl)}@{roas_d}%")
        # AI 권고 vs 사람 선택 비교: 'MMDD AI{권고}(사람:{그날 사람선택 or —})' — 내 조언의 적중/빗나감 학습용
        aim = ai_marks.get(aid, {})
        airec = []
        for d in sorted(aim):
            hs = a["acts"].get(d)
            airec.append(f"{d[5:]}AI{HL_SHORT.get(aim[d], aim[d])}(사람:{HL_SHORT.get(hs, hs) if hs else '—'})")
        # active: True=현재 활성(하이라이트 대상) / False=중단 확정(제외) / None=상태 미상(필터 안 함)
        st = status_map.get(aid)
        active = (st in ACTIVE_STATUSES) if status_map else None
        # 이틀 연속 증감액 금지: 마지막 조치가 기준일(dc=어제)의 증액·감액이면 오늘은 조정 대상에서 뺀다
        act_dates = sorted(a["acts"])
        last_act = a["acts"][act_dates[-1]] if act_dates else ""
        just_adj = bool(act_dates) and act_dates[-1] == dc and last_act in ADJ_TAGS
        # ROAS 보호선: 기준일(dc=어제) 일간 ROAS가 120%↑면 하락폭과 무관하게 감액·OFF 금지.
        # 기준일 지출이 없으면(데이터 없음) roas_dc=None → 가드 미적용(종전대로 판단).
        sp_dc, rv_dc = a["days"].get(dc, (0, 0))
        roas_dc = round(rv_dc / sp_dc * 100) if sp_dc else None
        keep_floor = roas_dc is not None and roas_dc >= KEEP_ROAS_FLOOR
        items.append({"id": aid, "name": a["name"][:40], "product": a["product"], "budget": round(a["budget"]),
                      "sp": round(sp), "rv": round(rv), "roas7": roas7, "trend": trend,
                      "hl": a["hl"], "memo": a["memo"], "hist": " → ".join(hist),
                      "airec": " → ".join(airec), "ndays": len(last7), "_sp": sp,
                      "active": active, "status": st or "",
                      "just_adj": just_adj, "last_act": last_act,
                      "roas_dc": roas_dc, "keep_floor": keep_floor})
    # 활성(및 상태미상) 먼저, 그 안에서 지출 큰 순 → 40칸을 조언 대상 세트가 우선 차지한다.
    # (중단 세트는 sets_to_text에서 목록 제외되지만, 하이라이트 하드 가드용으로 뒤에 남겨둔다)
    items.sort(key=lambda x: (x.get("active") is False, -x["_sp"]))
    return items[:40], cur

HL_KO = {"up10": "증액10%", "up20": "증액20%", "up": "증액", "down10": "감액10%",
         "down20": "감액20%", "down": "감액", "off": "OFF", "watch": "관찰"}
# 이력용 축약 라벨 (14일 액션 타임라인, 짧게)
HL_SHORT = {"up10": "증10", "up20": "증20", "up": "증", "down10": "감10",
            "down20": "감20", "down": "감", "off": "OFF", "watch": "관찰"}

# 조언→추이차트 하이라이트로 자동 표기할 태그 (관찰=watch은 제외, 사용자 결정 2026-07-03)
HL_TAGS_OK = {"up10", "up20", "down10", "down20", "off"}
# 예산 '조정' 액션 (증액·감액) — 이틀 연속 금지 대상. OFF는 조정이 아니므로 제외(적자 방어는 언제든 가능)
ADJ_TAGS = {"up10", "up20", "up", "down10", "down20", "down"}
# 하방 액션(감액·OFF) — ROAS 보호선 가드 대상
CUT_TAGS = {"down10", "down20", "down", "off"}
# ROAS 보호선(사용자 결정 2026-08-03): 기준일(dc=어제) 일간 ROAS가 이 값 이상이면
# 하락이 아무리 가팔라도 감액·OFF 금지 — 여전히 남는 장사라 끄면 매출만 깎인다.
KEEP_ROAS_FLOOR = 120
# 봇 응답 끝에 붙일 기계용 하이라이트 블록 지시 (ADV_SYSTEM이 아닌 봇 user 프롬프트에만 → perf-advice 스킬과 무관)
ADV_MARKS_HINT = (
    "\n\n[하이라이트 출력 — 본문 맨 끝에 반드시 추가]\n"
    "위에서 실제로 증액/감액/OFF를 권한 세트만 골라, 대시보드 추이차트가 읽을 수 있게 아래 코드블록으로 정확히 출력하라. "
    "관찰·보류·특이없음은 넣지 마라. **'⏸ …상태:중단'으로 표시된 비활성 세트는 절대 넣지 마라(현재 활성 세트만 하이라이트한다).** "
    "id는 [세트 데이터]에 주어진 세트ID 숫자를 그대로 쓴다. "
    "tag는 다음 중 하나: 증액10%→up10, 증액20%→up20, 감액10%→down10, 감액20%→down20, OFF→off, 복제증액(복증)→up20.\n"
    "```marks\n[{\"id\":\"120xxxxxxxxxxxxxxx\",\"tag\":\"up10\"}]\n```")

def _extract_marks(txt):
    """봇 응답에서 ```marks [...]``` 블록을 떼어내 (게시용 텍스트, marks리스트) 반환.
    블록은 Slack 댓글에 노출하지 않는다(맨 끝 기계용)."""
    m = re.search(r"```marks\s*(\[.*?\])\s*```", txt, re.DOTALL)
    if not m:
        return txt.strip(), []
    marks = []
    try:
        for it in json.loads(m.group(1)):
            aid, tag = str(it.get("id", "")).strip(), str(it.get("tag", "")).strip()
            if aid and tag in HL_TAGS_OK:
                marks.append({"id": aid, "tag": tag})
    except Exception:
        marks = []
    return txt[:m.start()].rstrip(), marks  # 블록 및 그 뒤 제거

def apply_advice_highlights(region, marks):
    """조언에서 뽑은 [{id,tag}]를 추이차트 하이라이트 테이블(adset_highlights류)에 upsert.
    ★ ad_performance_daily(봇이 읽는 14일 '조치 이력')는 건드리지 않는다 → 조언 인과판단 오염 방지.
    정책: '항상 봇 조언으로 덮어쓰기' — 언급된 세트는 사람 마킹이 있어도 덮어씀(memo는 payload 미포함이라 보존).
    updated_at=지금(UTC) → 대시보드는 '오늘' 마킹으로 렌더, 자정 지나면 기존대로 자동 삭제."""
    hl_table = ADV_SRC[region][1]
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    rows = [{"adset_id": str(m["id"]), "highlight": m["tag"], "updated_at": now, "source": "ai"}
            for m in marks if m.get("tag") in HL_TAGS_OK and m.get("id")]
    if rows:
        try:
            sb_upsert(hl_table, rows)          # source='ai' → 추이차트에서 테두리 표시
        except Exception:
            for r in rows:                     # source 컬럼 미생성 등 → source 빼고 재시도(하이라이트는 최소 적용)
                r.pop("source", None)
            sb_upsert(hl_table, rows)
    return len(rows)

def record_ai_marks(region, marks, mark_date):
    """AI가 그날 권한 증감액을 durable하게 기록(ai_advice_marks) → 후일 '사람 선택 vs AI 권고' 학습 비교용.
    adset_highlights(오늘 시각화·purge됨)·ad_performance_daily(사람 조치이력)와 별개 테이블이라 아무것도 오염 안 함.
    mark_date=행동일(dc+1=보통 오늘) → 사람 saveHL이 그날로 찍는 highlight와 날짜 정렬."""
    rows = [{"date": mark_date, "adset_id": str(m["id"]), "region": region, "tag": m["tag"]}
            for m in marks if m.get("tag") in HL_TAGS_OK and m.get("id")]
    if not rows:
        return 0
    try:
        sb_upsert("ai_advice_marks", rows)     # PK(date,adset_id) 병합
    except Exception as e:
        print(f"  [ai_marks] 기록 실패(테이블 미생성?): {e}")
        return 0
    return len(rows)

def sets_to_text(items, cur):
    """조언용 세트 목록. 중단(비활성) 세트는 아예 빼고 건수만 알린다 — 조언 대상은 활성 세트뿐."""
    lines, skipped = [], 0
    for s in items:
        if s.get("active") is False:  # 이미 정지됨 → 조언에서 다루지 않음(목록에서 제외)
            skipped += 1
            continue
        tag = []
        if s.get("keep_floor"):  # 기준일 ROAS가 보호선 이상 → 하락해도 감액·OFF 금지
            tag.append(f"기준일ROAS {s['roas_dc']}%≥{KEEP_ROAS_FLOOR}(감액·OFF 금지)")
        if s.get("just_adj"):  # 어제 증감액함 → 오늘 또 조정하면 이틀 연속(효과 측정 불가)
            tag.append("어제조정:" + HL_KO.get(s.get("last_act"), s.get("last_act")) + "(오늘 증감액 금지)")
        if s["hl"]:
            tag.append("조치:" + HL_KO.get(s["hl"], s["hl"]))
        if s["memo"]:
            tag.append("메모:" + s["memo"][:50])
        if s.get("hist"):
            tag.append("이력:" + s["hist"])  # 최근 14일 증감액 액션@그날ROAS (조치 효과 판단용)
        if s.get("airec"):
            tag.append("AI권고이력:" + s["airec"])  # 과거 AI권고 vs 그날 사람선택 (조언 자체 보정용)
        tagstr = (" | " + " · ".join(tag)) if tag else ""
        rdc = f"{s['roas_dc']}%" if s.get("roas_dc") is not None else "—"
        lines.append(f"- {s['name']} (ID {s['id']}) [{s['product']}] 예산{cur}{s['budget']:,} · "
                     f"{ADVICE_DAYS}일ROAS {s['roas7']}%(지출{cur}{s['sp']:,}) · "
                     f"최근3일 {s['trend']}% · 기준일ROAS {rdc} · {s['ndays']}일{tagstr}")
    if skipped:  # 조언 대상에서 빠졌음을 명시 (세트 수가 적어 보이는 이유 + 되살리지 말라는 신호)
        lines.append(f"(이미 정지된 세트 {skipped}개는 조언 대상이 아니므로 목록에서 제외 — 언급하지 말 것)")
    return "\n".join(lines)

ADV_SYSTEM = """너는 메타 퍼포먼스 마케팅 어드바이저다. 아래 [플레이북]의 기준을 그대로 적용해,
[세트 데이터]와 각 세트의 '조치(증감액 표시)·메모'를 보고 '오늘의 증감액 조언'을 한국어로 작성한다.

전체 스탠스(먼저 판단해 첫 줄에 모드를 밝힌다):
- 종합 ROAS 전일 대비 변화로 모드를 정한다. 변화가 ±2%p 이내면 '보합'으로 진단하고 과장하지 마라(보합을 '뚜렷한 하락'으로 몰지 말 것).
- 계정이 하락 흐름(특히 플레이북 6-3의 화~목 하락 구간)이면 '방어 모드'다. 단, 방어 모드는 '가만히 있기'가 아니다 → 증액은 억제하되 감액·OFF는 오히려 더 적극적으로 발굴해 하방을 방어한다(플레이북 '화~목 하락 → 적극적 OFF·감액').
- 상승 흐름(목~일)이면 '공격 모드' → 증액·복증을 적극 발굴한다.

규칙:
- 형식(이모지 헤더만, 굵게 없이 간결한 텍스트, Slack 스레드 댓글용):
  · 한 줄 전체 흐름 진단 (어제 종합 ROAS·추세 + 모드(보합/방어/공격)를 명시)
  · 🔺 점진 증액 후보: 세트명 + 근거(ROAS·추세·예산) + 폭(플레이북: 150%대 안정 +10% / 150~200% 상승 +20%, 일예산 40만원↑ 대형은 +10%로 하향). 방어·보합기엔 질 우선으로 선별.
  · 🔁 복제증액(복증) 후보: 7일 ROAS 200%+ (또는 190%+ & 확실한 상승) **그리고** 최근 3~4일 연속 안정 세트만. 200%+는 그냥 % 증액하면 효율이 무너지므로 복제로 스케일한다. 폭은 스윗스팟 경로(처음 2배 → 안정되면 3·4배 순차). 반드시 신중히: **복증 22개 중 20개가 원본보다 효율 하락(구조적)** → 하루 스파이크·0%가 섞인 변동 세트는 제외, 2일차 데이터만으론 금지, 즉흥 실행 말고 '회의 후 실행'으로 제안한다. 방어·하락기엔 즉시 말고 흐름 컨펌 후(금·토 공격일 권장)로 타이밍을 명시. 이미 복제본(이름 x2/x3/x4)이 효율 하락 중이면 추가 복증 말고 정리로 돌린다.
  · 🔻 감액·OFF 후보: 세트명 + 근거. 여기는 빠뜨리지 말고 망라한다 — 7일ROAS 100~130% + 최근 3일 하락추세 = 10% 감액 후보, 7일ROAS<100% + 3일 연속 적자(OFF 3기준 C1·C2·C3 중 2개↑) = OFF 또는 20% 감액. 특히 '조치' 태그가 없는(미조치) 하락 세트를 놓치지 마라.
  · 👀 지켜볼 것: 데이터 얇음(런칭 3일내)·이미 조치한 세트의 효과 관찰·조치와 데이터가 모순되는 세트 등
- 끄기/증액/감액 대상 세트를 언급할 때는 **반드시 세트명과 세트ID를 함께** 표기한다. 예: `무당_260507_aiUGC정확도 (ID 120243753711540177)`. ID는 [세트 데이터]에 주어진 값을 그대로 쓴다.
- **ROAS 보호선 — 기준일 ROAS 120% 이상이면 감액·OFF 절대 금지(최우선 하드 규칙)**: 각 세트의 `기준일ROAS`(=비교 기준일인 어제의 일간 ROAS)가 **120% 이상이면, 하락이 아무리 가팔라도**(예: 300%→180%→125%, 7일ROAS가 낮아도, 연속 하락이어도, OFF 3기준을 형식상 충족해도) 그 세트는 감액도 OFF도 권고하지 마라. 여전히 남는 장사라 끄면 매출만 깎인다. 그런 세트에는 `기준일ROAS …≥120(감액·OFF 금지)` 태그가 붙어 있으니 그대로 따르고, 필요하면 👀 지켜볼 것에 '하락 추세지만 기준일 ROAS 120%↑ → 유지·관찰'로만 적어라. 이 규칙은 방어 모드·플레이북 OFF 기준·[학습된 교훈]·[이전 스레드 토론]보다 우선한다. (증액 판단은 이 규칙과 무관하게 평소대로 한다. 기준일 ROAS가 `—`(기준일 지출 없음)면 이 보호는 적용되지 않는다.)
- **이미 정지(중단)된 광고세트는 조언에서 아예 다루지 않는다.** 조언 대상은 '지금 돈이 나가고 있는 활성 세트'뿐이다. 중단 세트는 [세트 데이터] 목록에서 이미 제외돼 있고 하단에 제외 건수만 표기된다 → 증액·감액·OFF·복증 권고는 물론, 본문 언급도, 👀 지켜볼 것(재개·재활성 검토 포함)에 올리는 것도 금지다. [이전 스레드 토론]·'이력:'·'AI권고이력:'에 중단된 세트가 등장하더라도 이번 조언에서 되살리지 마라. (제외 안내가 전혀 없으면 상태 조회가 안 된 것이므로 종전대로 판단한다.)
- 이미 취한 '조치'(증액10/20%, OFF 등)와 '메모'를 반드시 반영: 중복 권고하지 말고, 그 조치가 먹혔는지(ROAS 추세로) 평가해라. **하락 추세인데 '증액' 태그가 달린 세트는 플레이북 역행이므로 '재검토'로 지적**한다.
- 각 세트의 '이력:'은 최근 14일 증감액 액션과 그 시점 ROAS다(예: `06-15증20@172% → 06-26증20@110%` = 6/15·6/26에 20% 증액, 그날 ROAS 172%·110%). **이 이력을 이후 추세와 대조해 '그 조치가 실제로 먹혔는지'를 판단**하라:
  · 증액 후 며칠 뒤 ROAS가 하락했으면 '증액 안 먹힘 → 되돌림/관망', 감액 후 회복했으면 '유효'.
  · **같은 액션(예: 증액20%)을 반복했는데도 계속 하락하면** 그 패턴을 명시적으로 지적하고, 증감액 손장난 대신 다른 처방(소재 수혈·타겟 제외·OFF 등 플레이북 5·9장)을 권하라.
  · 과거에 실패한 액션을 그대로 반복 권고하지 마라. 근거로 이력의 날짜·ROAS를 인용하라.
- **이틀 연속 증감액 금지(원칙)**: 예산을 조정한 다음 날은 결과를 최소 하루 지켜본다. 이력의 날짜는 '조치를 실행한 날'이고 [세트 데이터]의 기준일은 어제이므로, **이력의 가장 최근 조치가 어제이고 그것이 증액 또는 감액이면 그 세트는 오늘 증액·감액 후보에서 제외**한다. 그런 세트에는 `어제조정:…(오늘 증감액 금지)` 태그가 붙어 있으니 그대로 따르라. 같은 방향 재조정(증액→증액)도, 방향을 뒤집는 조정(증액→감액)도 안 된다 — 이틀 연속 손대면 어느 조치가 먹혔는지 측정이 불가능해진다. 대신 👀 지켜볼 것으로 돌려 '어제 OO 조정 → 효과 관찰 중'으로만 적어라. 예외: OFF(끄기)는 증감액이 아니므로 이 제한을 받지 않는다(OFF 3기준을 명백히 충족하는 적자 세트는 어제 조정했더라도 OFF 권고 가능).
- 각 세트의 'AI권고이력:'은 **과거에 내가(AI) 그날 권한 증감액과, 그날 사람이 실제 선택한 하이라이트를 나란히** 보여준다(예: `07-01AI OFF(사람:—) → 07-02AI증20(사람:증10)`). `(사람:—)`=사람이 내 권고를 안 따랐거나 미표기, `(사람:증10)`=내가 증20을 권했으나 사람이 증10으로 하향 조정. **이 AI↔사람 차이를 이후 ROAS 추세와 대조해 내 조언 기준 자체를 채점·보정하라(핵심 학습 루프)**:
  · 사람이 내 권고를 반복적으로 하향/무시했고 그게 옳았으면(이후 ROAS가 사람 선택을 지지), 내 기준이 과했음을 인정하고 이번 권고의 강도·폭을 그 방향으로 조정하라.
  · 반대로 사람이 안 따랐는데 이후 ROAS가 나빠졌으면, 근거(그날 AI권고·이후 ROAS)를 들어 이번에 다시 설득하라.
  · 나와 사람이 일치했고 결과가 좋았던 패턴은 계속 신뢰하라. 요지: 내 과거 권고의 적중/빗나감을 스스로 채점해 조언을 발전시킨다(사람 선택을 무조건 추종하지도, 무시하지도 말고 결과로 판단).
- [보정 지표]가 주어지면 이는 위 AI↔사람 채점을 **계정 전체로 정량 집계한 최근 요약**이다(개별 세트 airec의 상위 통계). 이번 권고 강도를 여기에 맞춰 **먼저 캘리브레이션**하라:
  · 'AI 증액 권고 중 인간 하향/취소 비율'이 높고(예: 절반↑) '인간이 하향한 건의 직후 3일 ROAS 하락'이 다수면 = **내 증액 권고가 구조적으로 과하다는 신호** → 이번엔 증액 폭을 한 단계 낮추고(증20→증10) 후보 선별을 더 보수적으로.
  · 반대로 '인간이 상향한 건의 직후 ROAS 상승'이 다수면 = 내 감액이 과했다는 신호 → 감액·OFF를 약간 완화하고 증액 후보를 놓치지 마라.
  · 표본이 적으면(건수 작음) 약한 신호로만 취급하고 과적합하지 마라. 이 지표는 방향 보정용이지 개별 세트 판단을 대체하지 않는다.
- [학습된 교훈]이 주어지면, 이 계정에서 장기간 결과로 검증된 규칙이므로 **플레이북 다음으로 강하게 반영**하라. 일반 플레이북과 이 계정 특화 교훈이 충돌하면 이 계정 교훈을 우선한다. 단, 최근 14일 [세트 데이터]가 교훈과 명백히 어긋나면 최근 데이터를 우선하고 그 사실을 짚어라.
- [이전 스레드 토론]이 주어지면 반드시 참고: 내가 지난 번에 한 조언과 그 뒤 사람들의 코멘트·결정을 이어받아라.
  지난 권고가 실행/반박/보류됐는지 추적하고, 사람의 피드백과 충돌하면 그 의견을 우선 존중하며, 같은 말 반복하지 말고 후속 관점을 더해라.
- 추세로 판단(하루 반등/적자에 속지 말 것). 단정과 추정을 구분. 세트명은 실제 이름 그대로.
- 증액은 선별적으로(질 우선), 감액·OFF는 후보를 빠뜨리지 말고 충분히(하방 방어) 담는다. 스캔 가능한 선에서 대략 20줄 이내로 하되, 후보를 억지로 줄여 누락시키지 마라. 진짜 후보가 없을 때만 '특이 없음'."""

# =====================================================================
# 보정 지표 (매일 자동계산): AI 권고 vs 인간 실제선택 차이 + 직후 ROAS로 '누가 옳았나'
#   개별 세트 airec를 계정 전체로 정량 집계 → 조언 강도를 매일 먼저 캘리브레이션.
#   새 테이블/워크플로 없이 기존 ai_advice_marks·highlight·human_advice_marks·daily만 사용.
# =====================================================================
CALIB_DAYS = 30  # 보정 집계 창(14일 조언창보다 길게 봐야 AI↔인간 표본 확보)
# aggressiveness 스케일: OFF(끄기)=가장 보수적 … 증20=가장 공격적. 인간vs AI 강도 비교용.
CALIB_VAL = {"off": -3, "down20": -2, "down10": -1, "watch": 0, "up10": 1, "up20": 2}

def compute_calibration(region, dc, window_days=CALIB_DAYS):
    """최근 window_days에서 'AI 권고 vs 인간 실제선택'의 차이와 그 직후 3일 ROAS 결과를
    정량 집계한 보정 지표 텍스트를 반환(표본 얇으면 ''). ADV_SYSTEM이 이걸로 권고 강도를 보정."""
    table, hl_table, sf, rf, bf, cur = ADV_SRC[region]
    since = (datetime.date.fromisoformat(dc) - datetime.timedelta(days=window_days - 1)).isoformat()
    days, hact = {}, {}
    try:
        rows = sb(table, f"date=gte.{since}&date=lte.{dc}"
                         f"&select=date,adset_id,{sf},{rf},highlight&order=date.asc")
    except Exception:
        return ""
    for r in rows:
        aid = r.get("adset_id") or "?"
        days.setdefault(aid, {})[r["date"]] = (r.get(sf) or 0, r.get(rf) or 0)
        if r.get("highlight"):
            hact.setdefault(aid, {})[r["date"]] = r["highlight"]
    for aid, dm in _load_human_marks(region, since, dc).items():
        hact.setdefault(aid, {}).update(dm)  # 인간 실제선택 durable 병합(글로벌 유실 보완)
    ai = {}
    try:
        for r in (sb("ai_advice_marks", f"region=eq.{region}&date=gte.{since}&date=lte.{dc}"
                                        f"&select=date,adset_id,tag") or []):
            aid = r.get("adset_id")
            if aid and r.get("tag"):
                ai.setdefault(aid, {})[r["date"]] = r["tag"]
    except Exception:
        return ""  # AI 이력 테이블 없음 → 보정 생략(무해)
    if not ai:
        return ""

    def roas_on(aid, d):
        sp, rv = days.get(aid, {}).get(d, (0, 0))
        return round(rv / sp * 100) if sp else None

    def roas_next(aid, d, n=3):
        base = datetime.date.fromisoformat(d)
        sp = rv = 0
        for k in range(1, n + 1):
            s, r = days.get(aid, {}).get((base + datetime.timedelta(days=k)).isoformat(), (0, 0))
            sp += s
            rv += r
        return round(rv / sp * 100) if sp else None

    n_ai = match = softer = harder = none = 0
    up_reco = up_softened = 0
    softer_val = softer_tot = harder_val = harder_tot = 0
    for aid, dm in ai.items():
        for d, tag_ai in dm.items():
            va = CALIB_VAL.get(tag_ai)
            if va is None:
                continue
            n_ai += 1
            if tag_ai in ("up10", "up20"):
                up_reco += 1
            tag_h = hact.get(aid, {}).get(d)
            if tag_h is None:
                none += 1
                continue
            vh = CALIB_VAL.get(tag_h, 0)
            if vh == va:
                match += 1
            elif vh < va:  # 인간이 AI보다 더 보수적(하향/OFF)
                softer += 1
                if tag_ai in ("up10", "up20"):
                    up_softened += 1
            else:          # 인간이 AI보다 더 공격적
                harder += 1
            r0, r3 = roas_on(aid, d), roas_next(aid, d)
            if r0 is not None and r3 is not None:
                if vh < va:      # 보수 선택 → 직후 ROAS 하락이면 '보수가 옳았다(AI 과함)'
                    softer_tot += 1
                    softer_val += 1 if r3 < r0 else 0
                elif vh > va:    # 공격 선택 → 직후 ROAS 상승이면 '공격이 옳았다(AI 감액 과함)'
                    harder_tot += 1
                    harder_val += 1 if r3 > r0 else 0
    if n_ai < 4:
        return ""  # 표본 얇음 → 억지 보정 금지
    lines = [f"최근 {window_days}일 AI 권고 {n_ai}건 → 인간 반응: 일치 {match} · 하향(더 보수적) {softer} · 상향 {harder} · 미실행/미표기 {none}"]
    if up_reco:
        lines.append(f"AI 증액 권고 {up_reco}건 중 인간이 하향/취소 {up_softened}건({round(up_softened / up_reco * 100)}%)")
    if softer_tot:
        lines.append(f"인간이 하향한 건 직후 3일 ROAS 하락 {softer_val}/{softer_tot} (하락 다수 = AI 증액이 과했다는 신호)")
    if harder_tot:
        lines.append(f"인간이 상향한 건 직후 3일 ROAS 상승 {harder_val}/{harder_tot} (상승 다수 = AI 감액이 과했다는 신호)")
    return "\n".join(lines)

def compose_advice(label, region, playbook, items, p, c, dp, dc, thread_ctx=""):
    if not ANTHROPIC_KEY or not playbook:
        return None, []
    try:
        import anthropic
    except Exception:
        return None, []
    cur = ADV_SRC[region][-1]
    total_roas_p = roas(p["total_rev"], p.get("total_spend") or p.get("total_meta_spend"))
    total_roas_c = roas(c["total_rev"], c.get("total_spend") or c.get("total_meta_spend"))
    meta_roas_p = roas(p["meta_rev"], p["meta_spend"])
    meta_roas_c = roas(c["meta_rev"], c["meta_spend"])
    ctx_block = f"\n\n[이전 스레드 토론 — 과거 조언 및 사람들의 코멘트(오래된→최근)]\n{thread_ctx}" if thread_ctx else ""
    lessons = fetch_lessons(region)
    lessons_block = f"\n\n[학습된 교훈 — 이 계정에서 결과로 검증된 규칙(플레이북 특화)]\n{lessons}" if lessons else ""
    try:
        calib = compute_calibration(region, dc)
    except Exception as e:
        print(f"  [조언] 보정 지표 계산 실패(무시): {e}")
        calib = ""
    calib_block = f"\n\n[보정 지표 — 최근 {CALIB_DAYS}일 내 권고 vs 인간 실제선택·결과 정량요약]\n{calib}" if calib else ""
    user = (f"[기간] {dp} → {dc} ({label})\n"
            f"[종합] 메타 ROAS {meta_roas_p}%→{meta_roas_c}% · 전체종합 ROAS {total_roas_p}%→{total_roas_c}%\n\n"
            f"[세트 데이터 · 최근 {ADVICE_DAYS}일 · 지출 큰 순]\n{sets_to_text(items, cur)}"
            f"{ctx_block}\n\n[플레이북]\n{playbook}{lessons_block}{calib_block}"
            f"{ADV_MARKS_HINT}")
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    # max_tokens는 thinking+본문을 함께 덮는 하드 상한. adaptive thinking이 수천 토큰을
    # 쓰므로 상한이 낮으면 thinking 도중 잘려 본문이 빈다(→조언 미게시, 2026-07-07 국내 재발).
    # 프롬프트가 커질수록(교훈·이력 레이어) thinking이 길어져 여유가 더 필요 → 32000.
    # stop_reason이 max_tokens면 thinking에 다 먹혀 본문이 빈 것이므로 한 번 더 키워 재시도한다.
    # max_tokens가 크면 SDK가 비스트리밍 호출을 거부한다("Streaming is required …
    # longer than 10 minutes"). 스트리밍으로 받으면 그 가드가 없어 높은 상한을 그대로 쓴다.
    def _call(max_toks):
        with client.messages.stream(
            model="claude-opus-4-8", max_tokens=max_toks,
            thinking={"type": "adaptive"}, output_config={"effort": "medium"},
            system=ADV_SYSTEM, messages=[{"role": "user", "content": user}]) as stream:
            resp = stream.get_final_message()
        txt = "".join(b.text for b in resp.content if b.type == "text").strip()
        return txt, resp.stop_reason
    txt, stop = _call(32000)
    if not txt:
        # 본문 0자 = thinking이 상한을 다 먹음(stop=max_tokens) 또는 빈 응답 → 상한 키워 1회 재시도
        print(f"  [조언] {label} 본문 비어 재시도 (stop={stop})")
        txt, stop = _call(48000)
    body, marks = _extract_marks(txt)  # 기계용 marks 블록 분리 (Slack엔 body만)
    if not body:
        # 여기까지 비면 조용히 넘기지 말고 남긴다(예전엔 무로그 스킵 → 원인 추적 불가했음)
        print(f"  [조언] {label} 본문 생성 실패 — 조언 미게시 (stop={stop}, txt={len(txt)}자)")
    advice = f"🧠 *오늘의 증감액 조언* ({label} · 플레이북 기준)\n{body}" if body else None
    return advice, marks

# =====================================================================
# 학습된 교훈 (월간 증류 → advice_lessons 테이블 → 매일 조언에 항상 주입)
#   14일 조언창을 넘어서는 장기 패턴을, 압축된 '검증 규칙'으로 매일 반영해 복리 학습.
# =====================================================================
def fetch_lessons(region):
    """advice_lessons에서 이 지역의 최신 교훈 텍스트를 읽는다(없으면 '')."""
    try:
        rows = sb("advice_lessons", f"region=eq.{region}&select=content&limit=1")
        return (rows[0].get("content") or "").strip() if rows else ""
    except Exception:
        return ""

def gather_learning_data(region, dc, window_days=LESSON_WINDOW):
    """학습용: 최근 window_days 세트별 '조치(사람/AI)→직후 ROAS 변화' 이벤트 목록.
    조치가 있던 세트만(학습 신호), 지출 큰 순 상위 30. distill_lessons가 이걸 감사한다."""
    table, hl_table, sf, rf, bf, cur = ADV_SRC[region]
    since = (datetime.date.fromisoformat(dc) - datetime.timedelta(days=window_days - 1)).isoformat()
    rows = sb(table, f"date=gte.{since}&date=lte.{dc}"
                     f"&select=date,adset_id,adset_name,product,{sf},{rf},highlight&order=date.asc")
    agg = {}
    for r in rows:
        aid = r.get("adset_id") or "?"
        a = agg.setdefault(aid, {"name": r.get("adset_name") or aid, "product": r.get("product") or "",
                                 "days": {}, "hacts": {}})
        a["name"] = r.get("adset_name") or a["name"]
        a["product"] = r.get("product") or a["product"]
        a["days"][r["date"]] = (r.get(sf) or 0, r.get(rf) or 0)
        if r.get("highlight"):
            a["hacts"][r["date"]] = r["highlight"]
    # 사람 조치 durable 병합 (글로벌 유실 보완 → 학습에도 국내와 동일하게 반영)
    for aid, dm in _load_human_marks(region, since, dc).items():
        if aid in agg:
            agg[aid]["hacts"].update(dm)
    ai = {}
    try:
        for r in (sb("ai_advice_marks", f"region=eq.{region}&date=gte.{since}&date=lte.{dc}"
                                        f"&select=date,adset_id,tag") or []):
            aid = r.get("adset_id")
            if aid and r.get("tag"):
                ai.setdefault(aid, {})[r["date"]] = r["tag"]
    except Exception:
        ai = {}

    def roas_on(a, d):
        sp, rv = a["days"].get(d, (0, 0))
        return round(rv / sp * 100) if sp else None

    def roas_next(a, d, n=3):
        base = datetime.date.fromisoformat(d)
        sp = rv = 0
        for k in range(1, n + 1):
            s, r = a["days"].get((base + datetime.timedelta(days=k)).isoformat(), (0, 0))
            sp += s
            rv += r
        return round(rv / sp * 100) if sp else None

    order = sorted(agg.items(), key=lambda kv: -sum(sp for sp, _ in kv[1]["days"].values()))
    blocks = []
    for aid, a in order:
        tot_sp = sum(sp for sp, _ in a["days"].values())
        if tot_sp <= 0:
            continue
        events = sorted(set(a["hacts"]) | set(ai.get(aid, {})))
        if not events:
            continue  # 조치 없는 세트 = 학습 신호 없음
        ev = []
        for d in events:
            h, m = a["hacts"].get(d), ai.get(aid, {}).get(d)
            parts = ([("AI" + HL_SHORT.get(m, m))] if m else []) + ([("사람" + HL_SHORT.get(h, h))] if h else [])
            r0, r3 = roas_on(a, d), roas_next(a, d)
            ev.append(f"{d[5:]}{'·'.join(parts)}@{r0 if r0 is not None else '?'}%→3일후{r3 if r3 is not None else '?'}%")
        blocks.append(f"- {a['name'][:40]} (ID {aid}) [{a['product']}] 총지출{cur}{round(tot_sp):,}\n    " + " ; ".join(ev))
        if len(blocks) >= 30:
            break
    return "\n".join(blocks)

LESSONS_SYSTEM = """너는 이 계정의 증감액 의사결정 이력을 감사하는 분석가다.
입력은 최근 기간 세트별 '조치(사람/AI 증감액 표시)와 그 직후 3일 ROAS 변화'다.
여기서 이 계정에 **반복적으로 검증된 교훈만** 뽑아, 앞으로의 증감액 조언을 날카롭게 하는 규칙으로 정리하라.

원칙:
- 일반 플레이북을 이 계정/상품/예산대에 맞게 **특화·보정**하는 형태로 쓴다(예: "무당 상품은 증20 직후 3일 ROAS가 반복 하락 → 무당은 +10% 상한", "일예산 80만↑ 대형은 감액이 늦으면 회복 안 됨 → 하락 2일차에 즉시 감액").
- 사람과 AI가 갈린 지점이 있으면 **이후 ROAS로 누가 옳았는지** 판정해 규칙에 반영(사람이 반복적으로 옳았던 방향은 그쪽으로 기준을 옮긴다).
- 각 규칙은 **근거(빈도·ROAS 수치)를 짧게** 포함. 예: "(6건 중 5건 하락)".
- 표본이 얇거나 노이즈면 억지 규칙 만들지 말고 '아직 결론 이름'으로 남긴다. 우연을 패턴으로 과적합하지 마라.
- 5~15개 불릿, 간결한 한국어. 세트명 나열 말고 **일반화된 규칙**으로."""

def distill_lessons():
    """월간: 계정 이력을 감사해 검증 교훈을 뽑아 advice_lessons에 저장(region별)."""
    if not ANTHROPIC_KEY:
        print("[교훈] ANTHROPIC_API_KEY 없음 — 생략")
        return
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    today = datetime.datetime.now(KST).date()
    dc = FORCE_DATES[1] if FORCE_DATES else (today - datetime.timedelta(days=1)).isoformat()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for region, label in (("kr", "국내"), ("gl", "글로벌")):
        try:
            data = gather_learning_data(region, dc, LESSON_WINDOW)
        except Exception as e:
            print(f"[교훈] {label}: 데이터 수집 실패 {e}")
            continue
        if not data.strip():
            print(f"[교훈] {label}: 조치 이력 없음 — 생략")
            continue
        user = f"[계정] {label} · 최근 {LESSON_WINDOW}일\n[세트별 조치→직후 ROAS]\n{data}"
        resp = client.messages.create(
            model="claude-opus-4-8", max_tokens=16000,
            thinking={"type": "adaptive"}, output_config={"effort": "high"},
            system=LESSONS_SYSTEM, messages=[{"role": "user", "content": user}])
        txt = "".join(b.text for b in resp.content if b.type == "text").strip()
        if not txt:
            print(f"[교훈] {label}: 빈 응답 — 생략")
            continue
        content = f"(생성 {today.isoformat()} · 최근 {LESSON_WINDOW}일 기준)\n{txt}"
        sb_upsert("advice_lessons", {"region": region, "content": content,
                                     "window_days": LESSON_WINDOW, "updated_at": now})
        print(f"[교훈] {label}: {len(txt):,}자 저장 → advice_lessons({region})")

# =====================================================================
# 🧪 실험 현황 · 오늘의 변화  (index.html '📋 실험 현황' 탭의 파이썬 이식)
#
#   대시보드 실험현황 탭은 '원본 + 그 파생(복제·tROAS 등)' 가족을 자동으로 찾아
#   가족마다 [헤더 + 기간 평균표]를 보여준다. 이 봇은 그중 **기준일에 실제로
#   변화가 생긴 가족만** 골라, 그 헤더 + 기간 평균 내용을 게시한다.
#
#   가족 판정 = _dv_classify(app.js dvClassify 이식) → 이름의 복제·변형 마커를 걷어낸
#   '계보 키'로 묶고, 파생이 하나라도 있는 가족만 실험으로 본다. 판정 규칙이 바뀌면
#   app.js 의 DV_DUP/DV_VAR 와 **양쪽을 함께** 고쳐야 한다(정본은 app.js).
#
#   변화 판정(기준일 dc = 메시지의 '어제' 완결일)
#     🆕 새 변형 시작 : 파생 세트의 첫 지출일 == dc
#     ⏹️ 지출 중단    : dc-1 까지 돌던 세트가 dc 에 지출 0
#     🔄 우열 역전    : 원본 대비 누적 ROAS 격차의 부호가 dc 에서 뒤집힘
#     🏁 격차 확정    : |누적 ROAS 격차| 가 dc 에 처음 ES_GAP%p 돌파
#     📈📉 급변       : 세트의 dc 일간 ROAS 가 직전 7일 대비 ±ES_SPIKE%p
#     💰 지출 급변    : 세트의 dc 지출이 직전 7일 평균 대비 ES_SPEND_UP↑ / ES_SPEND_DN↓
# =====================================================================
ES_WINDOW = 14            # 집계 기간(실험현황 탭 기본값과 동일)
ES_MAX_CARDS = 6          # 한 메시지에 담을 가족 수 상한(슬랙 길이 보호)
ES_PRIOR = 7              # 급변 판정에 쓰는 '직전 며칠'
ES_FLIP_MIN = 20          # 역전으로 인정할 최소 격차(%p) — 0 근처 잡음 제거
ES_GAP = 50               # '격차 확정' 임계(%p)
ES_SPIKE = 100            # 일간 ROAS 급변 임계(%p, 전체흐름 보정 후) — 하루 ROAS는 원래 출렁여서 높게 잡음
ES_MIN_BUY_PRIOR = 5      # 급변 판정 최소 표본: 직전 7일 구매수
ES_MIN_BUY_DAY = 3        # 급변 판정 최소 표본: 기준일 기대 구매수(평소 구매당비용 기준)
ES_SPEND_UP, ES_SPEND_DN = 2.0, 0.4   # 지출 급변 배수(전체흐름 보정 후)
# 통화별 하한 — 이보다 작으면 잡음으로 보고 판정하지 않는다(fam=14일 누적, day=하루)
ES_TH = {"kr": {"fam": 1_000_000, "day": 50_000},
         "gl": {"fam": 700, "day": 40}}
# 글로벌 상품명 통합 — app.js GL_PRODUCT_CANON 과 동일(대시보드 배지와 같은 이름이 찍히도록)
ES_GL_CANON = {"솔로": "solo", "solo": "solo",
               "무당": "shaman", "mudang": "shaman", "shaman": "shaman",
               "무녀": "mzpian", "mzpian": "mzpian",
               "집착": "possessive", "possessive": "possessive",
               "커리어": "job", "job": "job"}

# ── 계보 분류 (app.js dvClassify 이식) ────────────────────────────────
_DV_DUP = [(re.compile(r"\[\s*복제\s*\]"), "복제"),
           (re.compile(r"\s*-\s*(사본|복사본|copy)\s*$", re.I), "사본"),
           (re.compile(r"\s*-\s*복제증액\s*$"), "복제증액")]
_DV_VAR = [(re.compile(r"[_\-\s]+troas(실험)?$", re.I), "tROAS"),
           (re.compile(r"[_\-\s]+구매당(비용)?(변경|전환)?$"), "구매당비용"),
           (re.compile(r"[_\-\s]+결과당비용(전환|변경)?$"), "결과당비용"),
           (re.compile(r"[_\-\s]+(기존)?구매자\s*제외(실험)?$"), "구매자제외"),
           (re.compile(r"[_\-\s]+(테스트|test)$", re.I), "테스트"),
           (re.compile(r"[_\-\s]+전세계중국어$"), "전세계중국어")]
_DV_EMOJI = re.compile(u'[🀀-🫿☀-➿️‍]')
_DV_DATE = re.compile(r"^\d{4}(\d{2})?(\d{2})?$")
_DV_XN = re.compile(r"\s*[xX]\s*\d+")
_DV_KEY = re.compile(r"[^0-9a-z가-힣%]")

def _dv_strip_all(s, rx):
    while True:
        n = rx.sub("", s, count=1)
        if n == s:
            return s
        s = n

def _dv_classify(name):
    """세트 이름 → {kind:'orig'|'dup'|'var', tags:[...], key}. app.js dvClassify 와 동일 규칙."""
    s = str(name or "").strip()
    tags, kind = [], "orig"

    def strip_var(w):
        nonlocal kind
        for _ in range(4):
            hit = None
            for rx, tag in _DV_VAR:
                if rx.search(w):
                    w = rx.sub("", w, count=1)
                    hit = tag
                    break
            if not hit:
                break
            if hit not in tags:
                tags.append(hit)
            kind = "var"
        return w

    work = strip_var(s)
    dup = False
    for rx, tag in _DV_DUP:
        if rx.search(work):
            dup = True
            if tag not in tags:
                tags.append(tag)
            work = _dv_strip_all(work, rx)
    if dup:
        # 배수 표기(x2/x4)는 복제 마커가 있을 때만 제거 — 일반 이름의 'x2' 오제거 방지
        work = _DV_XN.sub("", work)
        work = _dv_strip_all(_dv_strip_all(work, _DV_DUP[0][0]), _DV_DUP[1][0])
        work = strip_var(work)   # xN 제거로 꼬리에 드러난 변형 마커 재수거
        kind = "dup"             # 복제+변형 동시 보유 → '복제'로 센다
    toks = [t for t in re.split(r"[_\s]+", _DV_EMOJI.sub("", work)) if t and not _DV_DATE.match(t)]
    label = "_".join(toks)
    return {"kind": kind, "tags": tags, "key": _DV_KEY.sub("", label.lower())}

# ── 수집·묶기 (app.js _esCollect / _esFamilies 이식) ──────────────────
def es_collect(region, dc, days=ES_WINDOW):
    """세트 단위 기간 집계. 원소 = {id,name,product,byDate,s,r,kind,tags,ckey}"""
    table, _hl, sf, rf, _bf, _cur = ADV_SRC[region]
    since = (datetime.date.fromisoformat(dc) - datetime.timedelta(days=days - 1)).isoformat()
    rows = sb(table, f"date=gte.{since}&date=lte.{dc}"
                     f"&select=date,adset_id,adset_name,product,{sf},{rf},"
                     f"unique_clicks,results_mp,impressions,reach")
    sets = {}
    for r in rows:
        aid = str(r.get("adset_id") or "")
        d = r.get("date")
        if not aid or not d:
            continue
        o = sets.get(aid)
        if o is None:
            prod = r.get("product") or "기타"
            if region == "gl":
                prod = ES_GL_CANON.get(str(prod).strip().lower(), prod)
            o = sets[aid] = {"id": aid, "name": r.get("adset_name") or "",
                             "product": prod, "byDate": {}, "s": 0.0, "r": 0.0}
        if r.get("adset_name"):
            o["name"] = r["adset_name"]
        b = o["byDate"].get(d)
        if b is None:
            b = o["byDate"][d] = {"s": 0.0, "r": 0.0, "uc": 0, "mp": 0, "imp": 0, "rch": 0}
        sp, rv = (r.get(sf) or 0), (r.get(rf) or 0)
        b["s"] += sp
        b["r"] += rv
        b["uc"] += (r.get("unique_clicks") or 0)
        b["mp"] += (r.get("results_mp") or 0)
        b["imp"] += (r.get("impressions") or 0)
        b["rch"] += (r.get("reach") or 0)
        o["s"] += sp
        o["r"] += rv
    out = []
    for o in sets.values():
        c = _dv_classify(o["name"])
        o["kind"], o["tags"], o["ckey"] = c["kind"], c["tags"], (c["key"] or o["id"])
        out.append(o)
    return out

def es_families(sets):
    """계보로 묶어 '파생이 있는' 가족만 남긴다(=실험이 걸린 원본)."""
    fam = {}
    for o in sets:
        f = fam.get(o["ckey"])
        if f is None:
            f = fam[o["ckey"]] = {"key": o["ckey"], "product": o["product"],
                                  "mem": [], "s": 0.0, "r": 0.0}
        f["mem"].append(o)
        f["s"] += o["s"]
        f["r"] += o["r"]
        if o["kind"] == "orig" and o["product"]:
            f["product"] = o["product"]
    return [f for f in fam.values()
            if len(f["mem"]) > 1 and any(m["kind"] != "orig" for m in f["mem"])]

# ── 변화 판정 ────────────────────────────────────────────────────────
def _es_money(n, cur):
    return usd(n) if cur == "$" else won(n)

def _es_cum(m, upto):
    """upto(포함)까지 누적 (지출, 매출)."""
    s = r = 0.0
    for d, o in m["byDate"].items():
        if d <= upto:
            s += o["s"]
            r += o["r"]
    return s, r

def _es_roas(s, r):
    return (r / s * 100) if s > 0 else None

def _es_prior_days(m, dc, n=ES_PRIOR):
    """dc 직전의 '지출 있는 날' 최대 n일."""
    ds = sorted((d for d, o in m["byDate"].items() if d < dc and o["s"] > 0), reverse=True)
    return [m["byDate"][d] for d in ds[:n]]

def es_market(sets, dc):
    """지역 전체의 '그날 분위기'. 주말 지출 램프·전사 매출 부진처럼 모든 세트에 똑같이 걸리는
    움직임은 실험의 변화가 아니므로, 급변 판정에서 이만큼을 빼고 본다.
      spend: dc 총지출 / 직전 ES_PRIOR일 하루평균 총지출   (1.0=평소)
      roas : dc ROAS - 직전 ES_PRIOR일 ROAS (%p)          (0=평소)"""
    ds = sorted({d for m in sets for d in m["byDate"]})
    prior = [d for d in ds if d < dc][-ES_PRIOR:]
    ts = tr = ps = pr = 0.0
    for m in sets:
        o = m["byDate"].get(dc)
        if o:
            ts += o["s"]
            tr += o["r"]
        for d in prior:
            o = m["byDate"].get(d)
            if o:
                ps += o["s"]
                pr += o["r"]
    pm = ps / len(prior) if prior else 0
    return {"spend": (ts / pm) if pm > 0 else 1.0,
            "roas": ((_es_roas(ts, tr) or 0) - (_es_roas(ps, pr) or 0)) if (ts > 0 and ps > 0) else 0.0}

def es_changes(fam, dc, th, mkt=None):
    """가족 하나에서 기준일 dc 에 생긴 변화 목록(문자열). 비었으면 '변화 없음'.
    mkt = es_market() 결과 — 전 지역 공통 움직임을 뺀 '이 세트만의 변화'로 판정한다."""
    dp = (datetime.date.fromisoformat(dc) - datetime.timedelta(days=1)).isoformat()
    mkt = mkt or {"spend": 1.0, "roas": 0.0}
    cur = th["cur"]
    out = []
    mem = fam["mem"]
    orig = next((m for m in mem if m["kind"] == "orig"), None)
    for m in mem:
        L = m.get("_lab", "")
        today = m["byDate"].get(dc)
        ts = today["s"] if today else 0.0
        spend_days = sorted(d for d, o in m["byDate"].items() if o["s"] > 0)
        # 🆕 새 변형 시작
        if m["kind"] != "orig" and spend_days and spend_days[0] == dc and ts >= th["day"]:
            out.append(f"🆕 {L} 새 변형 시작 (첫 지출 {_es_money(ts, cur)})")
        # ⏹️ 지출 중단
        prev = m["byDate"].get(dp)
        if prev and prev["s"] >= th["day"] and ts <= 0 and len(spend_days) >= 3:
            out.append(f"⏹️ {L} 지출 중단 (어제 {_es_money(prev['s'], cur)} → 0)")
        prior = _es_prior_days(m, dc)
        if today and ts >= th["day"] and len(prior) >= 3:
            ps = sum(o["s"] for o in prior)
            pr = sum(o["r"] for o in prior)
            base = _es_roas(ps, pr)
            now = _es_roas(ts, today["r"])
            # 구매 몇 건으로 하루 ROAS 가 100%p 씩 튀는 소액 세트는 급변 판정에서 제외한다.
            # '평소 구매당비용으로 오늘 지출을 돌렸다면 몇 건이 나왔어야 하는가' = 기대 구매수.
            pmp = sum(o["mp"] for o in prior)
            exp_buy = (ts / (ps / pmp)) if pmp > 0 else 0
            if base is not None and now is not None and pmp >= ES_MIN_BUY_PRIOR and exp_buy >= ES_MIN_BUY_DAY:
                exc = (now - base) - mkt["roas"]
                if abs(exc) >= ES_SPIKE and (now >= base * 2 or now <= base * 0.5):
                    arrow = "📈" if exc > 0 else "📉"
                    out.append(f"{arrow} {L} ROAS {base:.0f}% → {now:.0f}%"
                               f" (직전 {len(prior)}일 대비 {now - base:+.0f}%p"
                               f" · 전체흐름 보정 {exc:+.0f}%p)")
            # 💰 지출 급변 — 마찬가지로 지역 전체 증감 배수로 나눠 '이 세트만의 조정'만 잡는다
            pm = ps / len(prior)
            if pm >= th["day"]:
                rt = (ts / pm) / (mkt["spend"] or 1.0)
                if rt >= ES_SPEND_UP or rt <= ES_SPEND_DN:
                    out.append(f"💰 {L} 지출 {_es_money(pm, cur)} → {_es_money(ts, cur)}"
                               f" (전체흐름 보정 {(rt - 1) * 100:+.0f}%)")
    # 🔄 우열 역전 / 🏁 격차 확정 — 원본 대비 누적 ROAS 격차의 어제→오늘 변화
    if orig:
        bR_c = _es_roas(*_es_cum(orig, dc))
        bR_p = _es_roas(*_es_cum(orig, dp))
        for m in mem:
            if m is orig:
                continue
            L = m.get("_lab", "")
            vs_c, vr_c = _es_cum(m, dc)
            vR_c = _es_roas(vs_c, vr_c)
            vR_p = _es_roas(*_es_cum(m, dp))
            if None in (bR_c, bR_p, vR_c, vR_p) or vs_c < th["day"]:
                continue
            gc, gp = vR_c - bR_c, vR_p - bR_p
            if gc * gp < 0 and abs(gc) >= ES_FLIP_MIN:
                who = "원본을 앞섬" if gc > 0 else "원본에 뒤집힘"
                out.append(f"🔄 {L} 우열 역전 — {who} ({gp:+.0f}%p → {gc:+.0f}%p)")
            elif abs(gp) < ES_GAP <= abs(gc):
                who = "변형 우세" if gc > 0 else "원본 우세"
                out.append(f"🏁 {L} 격차 확정 — {who} ({gc:+.0f}%p)")
    return out

# ── 표시: 실험현황 카드의 헤더 + 기간 평균 ────────────────────────────
def _es_side(m, i, cur):
    """세트 하나 → 기간 평균 한 줄에 필요한 값(app.js _esSide/_esAvgTable 과 동일 계산)."""
    tot = {"s": 0.0, "r": 0.0, "uc": 0, "mp": 0, "imp": 0, "rch": 0}
    ks = sorted(m["byDate"])
    for d in ks:
        o = m["byDate"][d]
        for k in tot:
            tot[k] += o[k]
    cal = ((datetime.date.fromisoformat(ks[-1]) - datetime.date.fromisoformat(ks[0])).days + 1) if ks else 0
    if cal < 1:
        cal = len(ks)
    role = "원본" if m["kind"] == "orig" else ("/".join(m["tags"]) if m["tags"] else "변형")
    # 일별 ROAS 표준편차(지출 있는 날) — 변동성
    rd = [o["r"] / o["s"] * 100 for o in m["byDate"].values() if o["s"] > 0]
    sd = None
    if len(rd) >= 2:
        mn = sum(rd) / len(rd)
        sd = (sum((x - mn) ** 2 for x in rd) / len(rd)) ** 0.5
    elif len(rd) == 1:
        sd = 0.0
    return {"lab": chr(65 + i), "role": role, "name": m["name"] or "-",
            "days": cal, "tot": tot, "sd": sd}

def fmt_exp_card(fam, sides, reasons, cur):
    """카드 1개 = 헤더(상품·원본이름·가족 지출/매출/ROAS) + 변화 사유 + 세트별 기간 평균."""
    fr = _es_roas(fam["s"], fam["r"]) or 0
    orig = sides[0]
    varn = sum(1 for s in sides if s["role"] != "원본")
    head = (f"*[{fam['product']}] {orig['name']}*\n"
            f"　실험 {varn}개 · 가족 지출 {_es_money(fam['s'], cur)}"
            f" · 매출 {_es_money(fam['r'], cur)} · ROAS {fr:.0f}%")
    why = "\n".join("　" + r for r in reasons)
    base = sides[0]
    bR = _es_roas(base["tot"]["s"], base["tot"]["r"])
    body = []
    for S in sides:
        t, days = S["tot"], S["days"]
        body.append(f"{S['lab']} [{S['role']}] {S['name']}")
        if not days or t["s"] <= 0:
            body.append("   데이터 없음")
            continue
        R = _es_roas(t["s"], t["r"]) or 0
        dlt = (R - bR) if (S is not base and bR) else None
        cvr = (t["mp"] / t["uc"] * 100) if (t["uc"] > 0 and t["mp"] > 0) else 0
        ctr = (t["uc"] / t["imp"] * 100) if t["imp"] > 0 else 0
        cpm = (t["s"] / t["imp"] * 1000) if t["imp"] > 0 else 0
        freq = (t["imp"] / t["rch"]) if t["rch"] > 0 else 0
        cpa = (t["s"] / t["mp"]) if t["mp"] > 0 else 0
        prof = (t["r"] - t["s"]) / days
        rs = (f"ROAS {R:.0f}%" + (f"({dlt:+.0f}p)" if dlt is not None else "")
              + (f" ±{S['sd']:.0f}" if S["sd"] is not None else ""))
        body.append(f"   {days}일 · {ljust(rs + ' ', 23)}· 순이익/일 {_es_money(prof, cur)}"
                    f" · 지출/일 {_es_money(t['s'] / days, cur)}"
                    f" · 매출/일 {_es_money(t['r'] / days, cur)}")
        body.append(f"        · CVR {cvr:.1f}% · CTR {ctr:.1f}% · 빈도 {freq:.2f}"
                    f" · 구매당 {_es_money(cpa, cur) if cpa else '-'} · CPM {_es_money(cpm, cur)}")
    return head + "\n" + why + "\n```\n" + "\n".join(body) + "\n```"

def build_exp_message(label, region, dc):
    """기준일에 변화가 있는 실험 가족만 골라 메시지 문자열 반환. 없으면 None."""
    cur = ADV_SRC[region][-1]
    th = dict(ES_TH[region])
    th["cur"] = cur
    sets = es_collect(region, dc)
    mkt = es_market(sets, dc)      # 지역 전체의 그날 분위기 — 급변 판정에서 이만큼 뺀다
    fams = es_families(sets)
    total = len(fams)
    hits = []
    for f in fams:
        if f["s"] < th["fam"]:
            continue
        # 원본 먼저, 그다음 매출 큰 순(실험현황 탭과 동일) → 라벨 A,B,C…
        mem = sorted(f["mem"], key=lambda m: (0 if m["kind"] == "orig" else 1, -m["r"], -m["s"]))
        f["mem"] = mem
        sides = [_es_side(m, i, cur) for i, m in enumerate(mem)]
        for m, S in zip(mem, sides):
            m["_lab"] = S["lab"]
        rs = es_changes(f, dc, th, mkt)
        if rs:
            hits.append((f, sides, rs))
    print(f"  [실험] 실험 {total}건 중 변화 {len(hits)}건"
          f"  (전체흐름: 지출 {mkt['spend']:.2f}배 · ROAS {mkt['roas']:+.0f}%p)")
    if not hits:
        return None
    hits.sort(key=lambda x: -x[0]["s"])
    shown = hits[:ES_MAX_CARDS]
    head = (f"🧪 *{label} 실험 현황 · 오늘의 변화*   {md(dc)}({wd(dc)}) 기준"
            f"   ·  실험 {total}건 중 변화 {len(hits)}건")
    if len(hits) > len(shown):
        head += f" (지출 상위 {len(shown)}건만 표시)"
    return head + "\n\n" + "\n\n".join(fmt_exp_card(f, s, r, cur) for f, s, r in shown)


# =====================================================================
# main
# =====================================================================
def main():
    if DISTILL:
        distill_lessons()
        return
    if FORCE_DATES:
        dp, dc = FORCE_DATES[0], FORCE_DATES[1]
    else:
        today = datetime.datetime.now(KST).date()
        dc = (today - datetime.timedelta(days=DAYS_BACK)).isoformat()
        dp = (today - datetime.timedelta(days=DAYS_BACK + 1)).isoformat()
    print(f"[구간] {dp} -> {dc}" + ("  [DRY-RUN]" if DRY else ""))
    # AI 권고 기록 날짜 = 행동일(dc+1, 라이브에선 오늘) → 사람 highlight(그날 찍힘)와 정렬
    mark_date = (datetime.date.fromisoformat(dc) + datetime.timedelta(days=1)).isoformat()

    jobs = []
    if not GL_ONLY:
        if EXP_ONLY:
            jobs.append(("국내", "kr", CH_KR, None, None, None))
        else:
            kp, kc = calc_kr(dp), calc_kr(dc)
            jobs.append(("국내", "kr", CH_KR, fmt_kr(dp, dc, kp, kc), kp, kc))
    if not KR_ONLY:
        if EXP_ONLY:
            jobs.append(("글로벌", "gl", CH_GL, None, None, None))
        else:
            gp, gc = calc_gl(dp), calc_gl(dc)
            jobs.append(("글로벌", "gl", CH_GL, fmt_gl(dp, dc, gp, gc), gp, gc))

    playbook, src = ("", "off")
    if not NO_ADVICE and not EXP_ONLY:
        playbook, src = fetch_playbook()
        print(f"[조언] 플레이북 로드: {src} ({len(playbook):,}자)")

    for label, region, ch, msg, p, c in jobs:
        print("\n" + "=" * 60 + f"\n■ {label} 메시지" + (f" → {ch or '(채널ID 미설정)'}" if not DRY else "") + "\n" + "=" * 60)
        if msg:
            print(msg)

        # 🧪 실험 현황 · 오늘의 변화 (별도 메시지). 변화가 없는 날은 아예 보내지 않는다.
        exp_msg = None
        if not NO_EXP:
            try:
                exp_msg = build_exp_message(label, region, dc)
            except Exception as e:
                print(f"  [실험] 생성 실패: {e}")

        # 조언 생성 (플레이북 + 세트/메모/증감액표시 → Claude). marks=조언에서 뽑은 추이차트 하이라이트
        advice, adv_marks = None, []
        if not NO_ADVICE and playbook:
            try:
                items, _ = gather_sets(region, dc)
                thread_ctx = fetch_thread_context(ch, region)
                advice, adv_marks = compose_advice(label, region, playbook, items, p, c, dp, dc, thread_ctx)
                # 하드 가드: 프롬프트 지시와 무관하게 '중단 확정' 세트는 하이라이트/기록에서 제외.
                # (active is False인 세트만 제거 — 상태 미상(None)은 종전대로 통과)
                inactive_ids = {str(it["id"]) for it in items if it.get("active") is False}
                if inactive_ids and adv_marks:
                    kept = [m for m in adv_marks if str(m.get("id")) not in inactive_ids]
                    dropped = len(adv_marks) - len(kept)
                    if dropped:
                        print(f"  [하이라이트] 중단 세트 {dropped}건 제외 (활성 세트만 마킹)")
                    adv_marks = kept
                # 하드 가드2: 어제 증감액한 세트는 오늘 또 증감액 금지(이틀 연속 조정 → 효과 측정 불가).
                # OFF는 조정이 아니므로 통과시킨다.
                adj_ids = {str(it["id"]) for it in items if it.get("just_adj")}
                if adj_ids and adv_marks:
                    kept = [m for m in adv_marks
                            if not (str(m.get("id")) in adj_ids and m.get("tag") in ADJ_TAGS)]
                    dropped = len(adv_marks) - len(kept)
                    if dropped:
                        print(f"  [하이라이트] 어제 증감액한 세트 {dropped}건 제외 (이틀 연속 조정 금지)")
                    adv_marks = kept
                # 하드 가드3: 기준일 ROAS가 보호선(120%) 이상인 세트는 감액·OFF 금지(하락폭 무관).
                # 증액 마킹은 그대로 통과시킨다.
                keep_ids = {str(it["id"]) for it in items if it.get("keep_floor")}
                if keep_ids and adv_marks:
                    kept = [m for m in adv_marks
                            if not (str(m.get("id")) in keep_ids and m.get("tag") in CUT_TAGS)]
                    dropped = len(adv_marks) - len(kept)
                    if dropped:
                        print(f"  [하이라이트] 기준일ROAS {KEEP_ROAS_FLOOR}%↑ 세트 감액·OFF {dropped}건 제외 (ROAS 보호선)")
                    adv_marks = kept
            except Exception as e:
                print(f"  [조언] 생성 실패: {e}")

        if DRY:
            if exp_msg:
                print("\n  ── 🧪 실험 현황 알림 미리보기 ──\n" + exp_msg)
            if p and c:
                print("\n  ── 구성요소 (전체 종합 보정용) ──")
                for tag, d in [(dp, p), (dc, c)]:
                    print(f"  · {tag}: " + "  ".join(f"{k}={v:,.0f}" for k, v in d["comp"].items()))
            if advice:
                print("\n  ── 스레드 댓글(조언) 미리보기 ──\n" + advice)
            if adv_marks and not NO_HL:
                print(f"\n  ── 추이차트 하이라이트 {len(adv_marks)}건 (미적용, 대상: {ADV_SRC[region][1]}) ──")
                for m in adv_marks:
                    print(f"    {m['id']}  →  {HL_KO.get(m['tag'], m['tag'])}")
            if adv_marks:
                print(f"  ── AI권고 기록(미적용): {len(adv_marks)}건 → ai_advice_marks(date={mark_date}) ──")
            continue

        if not BOT:
            print("  [SKIP] SLACK_BOT_TOKEN 미설정 — 전송 불가")
            continue
        if not ch:
            print(f"  [SKIP] 채널 ID 미설정 ({label})")
            continue
        ts = None
        if msg:
            ok, ts = slack_post(ch, msg)
            print(f"  전송: {'성공' if ok else '실패(' + str(ts) + ')'}")
            if not ok:
                ts = None
            if ok and advice:
                ok2, info2 = slack_post(ch, advice, thread_ts=ts)
                print(f"  조언 댓글: {'성공' if ok2 else '실패(' + str(info2) + ')'}")
        # 🧪 실험 현황 — 퍼포먼스 표와 주제가 다르므로 별도 메시지로 게시
        if exp_msg:
            ok3, info3 = slack_post(ch, exp_msg)
            print(f"  실험 현황 알림: {'성공' if ok3 else '실패(' + str(info3) + ')'}")
        # 조언의 증감액을 추이차트 하이라이트로 자동 표기 (adset_highlights류만, 영구 조치이력은 불변)
        if adv_marks and not NO_HL:
            try:
                n = apply_advice_highlights(region, adv_marks)
                print(f"  추이차트 하이라이트: {n}건 적용 → {ADV_SRC[region][1]}")
            except Exception as e:
                print(f"  추이차트 하이라이트 적용 실패: {e}")
        # AI 권고를 durable 기록 (학습용: 후일 '사람 최종선택 vs AI 권고' 비교) — NO_HL과 무관
        if adv_marks:
            nr = record_ai_marks(region, adv_marks, mark_date)
            if nr:
                print(f"  AI권고 기록: {nr}건 → ai_advice_marks(date={mark_date})")

if __name__ == "__main__":
    main()
