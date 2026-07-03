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

※ '전체 종합' 공식은 calc_kr_total / calc_gl_total 에 분리해 두었다(아래 주석 참고).
   예시 숫자와 대조해 이 두 함수만 조정하면 된다.
"""
import os, sys, json, re, datetime, urllib.request, urllib.parse
from pathlib import Path

BASE = Path(__file__).parent
DRY = "--dry-run" in sys.argv
KR_ONLY = "--kr-only" in sys.argv
GL_ONLY = "--gl-only" in sys.argv
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
BOT = ENV.get("SLACK_BOT_TOKEN", "")          # 봇 1개 (양쪽 채널에 게시)
CH_KR = ENV.get("SLACK_CH_KR_MARKETING", "")
CH_GL = ENV.get("SLACK_CH_GL_MARKETING", "")

# ── 조언(스레드 댓글) 설정 ──
ANTHROPIC_KEY = ENV.get("ANTHROPIC_API_KEY", "")
NO_ADVICE = "--no-advice" in sys.argv
NO_HL = "--no-hl" in sys.argv  # 조언→추이차트 하이라이트 자동표기 끄기
ADVICE_DAYS = 7  # 세트 분석에 사용할 최근 일수
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
        items.append({"id": aid, "name": a["name"][:40], "product": a["product"], "budget": round(a["budget"]),
                      "sp": round(sp), "rv": round(rv), "roas7": roas7, "trend": trend,
                      "hl": a["hl"], "memo": a["memo"], "hist": " → ".join(hist), "ndays": len(last7), "_sp": sp})
    items.sort(key=lambda x: -x["_sp"])
    return items[:40], cur

HL_KO = {"up10": "증액10%", "up20": "증액20%", "up": "증액", "down10": "감액10%",
         "down20": "감액20%", "down": "감액", "off": "OFF", "watch": "관찰"}
# 이력용 축약 라벨 (14일 액션 타임라인, 짧게)
HL_SHORT = {"up10": "증10", "up20": "증20", "up": "증", "down10": "감10",
            "down20": "감20", "down": "감", "off": "OFF", "watch": "관찰"}

# 조언→추이차트 하이라이트로 자동 표기할 태그 (관찰=watch은 제외, 사용자 결정 2026-07-03)
HL_TAGS_OK = {"up10", "up20", "down10", "down20", "off"}
# 봇 응답 끝에 붙일 기계용 하이라이트 블록 지시 (ADV_SYSTEM이 아닌 봇 user 프롬프트에만 → perf-advice 스킬과 무관)
ADV_MARKS_HINT = (
    "\n\n[하이라이트 출력 — 본문 맨 끝에 반드시 추가]\n"
    "위에서 실제로 증액/감액/OFF를 권한 세트만 골라, 대시보드 추이차트가 읽을 수 있게 아래 코드블록으로 정확히 출력하라. "
    "관찰·보류·특이없음은 넣지 마라. id는 [세트 데이터]에 주어진 세트ID 숫자를 그대로 쓴다. "
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
    rows = [{"adset_id": str(m["id"]), "highlight": m["tag"], "updated_at": now}
            for m in marks if m.get("tag") in HL_TAGS_OK and m.get("id")]
    if rows:
        sb_upsert(hl_table, rows)
    return len(rows)

def sets_to_text(items, cur):
    lines = []
    for s in items:
        tag = []
        if s["hl"]:
            tag.append("조치:" + HL_KO.get(s["hl"], s["hl"]))
        if s["memo"]:
            tag.append("메모:" + s["memo"][:50])
        if s.get("hist"):
            tag.append("이력:" + s["hist"])  # 최근 14일 증감액 액션@그날ROAS (조치 효과 판단용)
        tagstr = (" | " + " · ".join(tag)) if tag else ""
        lines.append(f"- {s['name']} (ID {s['id']}) [{s['product']}] 예산{cur}{s['budget']:,} · "
                     f"{ADVICE_DAYS}일ROAS {s['roas7']}%(지출{cur}{s['sp']:,}) · "
                     f"최근3일 {s['trend']}% · {s['ndays']}일{tagstr}")
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
- 이미 취한 '조치'(증액10/20%, OFF 등)와 '메모'를 반드시 반영: 중복 권고하지 말고, 그 조치가 먹혔는지(ROAS 추세로) 평가해라. **하락 추세인데 '증액' 태그가 달린 세트는 플레이북 역행이므로 '재검토'로 지적**한다.
- 각 세트의 '이력:'은 최근 14일 증감액 액션과 그 시점 ROAS다(예: `06-15증20@172% → 06-26증20@110%` = 6/15·6/26에 20% 증액, 그날 ROAS 172%·110%). **이 이력을 이후 추세와 대조해 '그 조치가 실제로 먹혔는지'를 판단**하라:
  · 증액 후 며칠 뒤 ROAS가 하락했으면 '증액 안 먹힘 → 되돌림/관망', 감액 후 회복했으면 '유효'.
  · **같은 액션(예: 증액20%)을 반복했는데도 계속 하락하면** 그 패턴을 명시적으로 지적하고, 증감액 손장난 대신 다른 처방(소재 수혈·타겟 제외·OFF 등 플레이북 5·9장)을 권하라.
  · 과거에 실패한 액션을 그대로 반복 권고하지 마라. 근거로 이력의 날짜·ROAS를 인용하라.
- [이전 스레드 토론]이 주어지면 반드시 참고: 내가 지난 번에 한 조언과 그 뒤 사람들의 코멘트·결정을 이어받아라.
  지난 권고가 실행/반박/보류됐는지 추적하고, 사람의 피드백과 충돌하면 그 의견을 우선 존중하며, 같은 말 반복하지 말고 후속 관점을 더해라.
- 추세로 판단(하루 반등/적자에 속지 말 것). 단정과 추정을 구분. 세트명은 실제 이름 그대로.
- 증액은 선별적으로(질 우선), 감액·OFF는 후보를 빠뜨리지 말고 충분히(하방 방어) 담는다. 스캔 가능한 선에서 대략 20줄 이내로 하되, 후보를 억지로 줄여 누락시키지 마라. 진짜 후보가 없을 때만 '특이 없음'."""

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
    user = (f"[기간] {dp} → {dc} ({label})\n"
            f"[종합] 메타 ROAS {meta_roas_p}%→{meta_roas_c}% · 전체종합 ROAS {total_roas_p}%→{total_roas_c}%\n\n"
            f"[세트 데이터 · 최근 {ADVICE_DAYS}일 · 지출 큰 순]\n{sets_to_text(items, cur)}"
            f"{ctx_block}\n\n[플레이북]\n{playbook}"
            f"{ADV_MARKS_HINT}")
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    # max_tokens는 thinking+본문을 함께 덮는 하드 상한. adaptive thinking이 수천 토큰을
    # 쓰므로 1500은 thinking 도중 잘려 본문이 빈다(→조언 미게시). 넉넉히 16000.
    resp = client.messages.create(
        model="claude-opus-4-8", max_tokens=16000,
        thinking={"type": "adaptive"}, output_config={"effort": "medium"},
        system=ADV_SYSTEM, messages=[{"role": "user", "content": user}])
    txt = "".join(b.text for b in resp.content if b.type == "text").strip()
    body, marks = _extract_marks(txt)  # 기계용 marks 블록 분리 (Slack엔 body만)
    advice = f"🧠 *오늘의 증감액 조언* ({label} · 플레이북 기준)\n{body}" if body else None
    return advice, marks

# =====================================================================
# main
# =====================================================================
def main():
    if FORCE_DATES:
        dp, dc = FORCE_DATES[0], FORCE_DATES[1]
    else:
        today = datetime.datetime.now(KST).date()
        dc = (today - datetime.timedelta(days=DAYS_BACK)).isoformat()
        dp = (today - datetime.timedelta(days=DAYS_BACK + 1)).isoformat()
    print(f"[구간] {dp} -> {dc}" + ("  [DRY-RUN]" if DRY else ""))

    jobs = []
    if not GL_ONLY:
        kp, kc = calc_kr(dp), calc_kr(dc)
        jobs.append(("국내", "kr", CH_KR, fmt_kr(dp, dc, kp, kc), kp, kc))
    if not KR_ONLY:
        gp, gc = calc_gl(dp), calc_gl(dc)
        jobs.append(("글로벌", "gl", CH_GL, fmt_gl(dp, dc, gp, gc), gp, gc))

    playbook, src = ("", "off")
    if not NO_ADVICE:
        playbook, src = fetch_playbook()
        print(f"[조언] 플레이북 로드: {src} ({len(playbook):,}자)")

    for label, region, ch, msg, p, c in jobs:
        print("\n" + "=" * 60 + f"\n■ {label} 메시지" + (f" → {ch or '(채널ID 미설정)'}" if not DRY else "") + "\n" + "=" * 60)
        print(msg)

        # 조언 생성 (플레이북 + 세트/메모/증감액표시 → Claude). marks=조언에서 뽑은 추이차트 하이라이트
        advice, adv_marks = None, []
        if not NO_ADVICE and playbook:
            try:
                items, _ = gather_sets(region, dc)
                thread_ctx = fetch_thread_context(ch, region)
                advice, adv_marks = compose_advice(label, region, playbook, items, p, c, dp, dc, thread_ctx)
            except Exception as e:
                print(f"  [조언] 생성 실패: {e}")

        if DRY:
            print("\n  ── 구성요소 (전체 종합 보정용) ──")
            for tag, d in [(dp, p), (dc, c)]:
                print(f"  · {tag}: " + "  ".join(f"{k}={v:,.0f}" for k, v in d["comp"].items()))
            if advice:
                print("\n  ── 스레드 댓글(조언) 미리보기 ──\n" + advice)
            if adv_marks and not NO_HL:
                print(f"\n  ── 추이차트 하이라이트 {len(adv_marks)}건 (미적용, 대상: {ADV_SRC[region][1]}) ──")
                for m in adv_marks:
                    print(f"    {m['id']}  →  {HL_KO.get(m['tag'], m['tag'])}")
            continue

        if not BOT:
            print("  [SKIP] SLACK_BOT_TOKEN 미설정 — 전송 불가")
            continue
        if not ch:
            print(f"  [SKIP] 채널 ID 미설정 ({label})")
            continue
        ok, ts = slack_post(ch, msg)
        print(f"  전송: {'성공' if ok else '실패(' + str(ts) + ')'}")
        if ok and advice:
            ok2, info2 = slack_post(ch, advice, thread_ts=ts)
            print(f"  조언 댓글: {'성공' if ok2 else '실패(' + str(info2) + ')'}")
        # 조언의 증감액을 추이차트 하이라이트로 자동 표기 (adset_highlights류만, 영구 조치이력은 불변)
        if adv_marks and not NO_HL:
            try:
                n = apply_advice_highlights(region, adv_marks)
                print(f"  추이차트 하이라이트: {n}건 적용 → {ADV_SRC[region][1]}")
            except Exception as e:
                print(f"  추이차트 하이라이트 적용 실패: {e}")

if __name__ == "__main__":
    main()
