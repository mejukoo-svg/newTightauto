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
import os, sys, json, datetime, urllib.request, urllib.parse
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

def gather_sets(region, dc, days=ADVICE_DAYS):
    """최근 days일 세트별 성과 + 메모/증감액표시(highlight) 수집."""
    table, hl_table, sf, rf, bf, cur = ADV_SRC[region]
    since = (datetime.date.fromisoformat(dc) - datetime.timedelta(days=days - 1)).isoformat()
    rows = sb(table, f"date=gte.{since}&date=lte.{dc}"
                     f"&select=date,adset_id,adset_name,product,{bf},{sf},{rf},highlight,memo"
                     f"&order=date.asc")
    agg = {}
    for r in rows:
        aid = r.get("adset_id") or "?"
        a = agg.setdefault(aid, {"name": r.get("adset_name") or aid, "product": r.get("product") or "",
                                 "budget": 0, "days": {}, "hl": "", "memo": ""})
        a["name"] = r.get("adset_name") or a["name"]
        a["product"] = r.get("product") or a["product"]
        a["budget"] = max(a["budget"], r.get(bf) or 0)
        a["days"][r["date"]] = (r.get(sf) or 0, r.get(rf) or 0)
        if r.get("highlight"):
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
    # 요약 라인 생성 (지출 큰 순, 최대 40세트)
    items = []
    for aid, a in agg.items():
        dts = sorted(a["days"])
        sp = sum(a["days"][d][0] for d in dts)
        rv = sum(a["days"][d][1] for d in dts)
        if sp <= 0:
            continue
        roas7 = round(rv / sp * 100)
        last3 = dts[-3:]
        trend = "→".join(f"{round(a['days'][d][1]/a['days'][d][0]*100) if a['days'][d][0] else 0}" for d in last3)
        items.append({"id": aid, "name": a["name"][:40], "product": a["product"], "budget": round(a["budget"]),
                      "sp": round(sp), "rv": round(rv), "roas7": roas7, "trend": trend,
                      "hl": a["hl"], "memo": a["memo"], "ndays": len(dts), "_sp": sp})
    items.sort(key=lambda x: -x["_sp"])
    return items[:40], cur

HL_KO = {"up10": "증액10%", "up20": "증액20%", "up": "증액", "down10": "감액10%",
         "down20": "감액20%", "down": "감액", "off": "OFF"}

def sets_to_text(items, cur):
    lines = []
    for s in items:
        tag = []
        if s["hl"]:
            tag.append("조치:" + HL_KO.get(s["hl"], s["hl"]))
        if s["memo"]:
            tag.append("메모:" + s["memo"][:50])
        tagstr = (" | " + " · ".join(tag)) if tag else ""
        lines.append(f"- {s['name']} (ID {s['id']}) [{s['product']}] 예산{cur}{s['budget']:,} · "
                     f"{ADVICE_DAYS}일ROAS {s['roas7']}%(지출{cur}{s['sp']:,}) · "
                     f"최근3일 {s['trend']}% · {s['ndays']}일{tagstr}")
    return "\n".join(lines)

ADV_SYSTEM = """너는 메타 퍼포먼스 마케팅 어드바이저다. 아래 [플레이북]의 기준을 그대로 적용해,
[세트 데이터]와 각 세트의 '조치(증감액 표시)·메모'를 보고 '오늘의 증감액 조언'을 한국어로 작성한다.

규칙:
- 형식(이모지/굵게 없이 간결한 텍스트, Slack 스레드 댓글용):
  · 한 줄 전체 흐름 진단 (어제 종합 ROAS·추세 기반, 플레이북 '흐름 우선' 원칙)
  · 🔺 증액 후보: 세트명 + 근거(ROAS·추세·예산) + 권고 폭(플레이북 기준: 안정 10%/상승 20%, 200%+는 복증 고려)
  · 🔻 감액·OFF 후보: 세트명 + 근거(7일ROAS<100%+연속적자 등 OFF 3기준)
- 끄기/증액/감액 대상 세트를 언급할 때는 **반드시 세트명과 세트ID를 함께** 표기한다. 예: `무당_260507_aiUGC정확도 (ID 120243753711540177)`. ID는 [세트 데이터]에 주어진 값을 그대로 쓴다.
  · 👀 지켜볼 것: 데이터 얇음(런칭 3일내)·이미 조치한 세트의 효과 관찰 등
- 이미 취한 '조치'(증액10/20%, OFF 등)와 '메모'를 반드시 반영: 중복 권고하지 말고, 그 조치가 먹혔는지(ROAS 추세로) 평가해라.
- [이전 스레드 토론]이 주어지면 반드시 참고: 내가 지난 번에 한 조언과 그 뒤 사람들의 코멘트·결정을 이어받아라.
  지난 권고가 실행/반박/보류됐는지 추적하고, 사람의 피드백과 충돌하면 그 의견을 우선 존중하며, 같은 말 반복하지 말고 후속 관점을 더해라.
- 추세로 판단(하루 반등/적자에 속지 말 것). 단정과 추정을 구분. 세트명은 실제 이름 그대로.
- 후보가 없으면 '특이 없음'. 전체를 12줄 이내로, 스캔 가능하게."""

def compose_advice(label, region, playbook, items, p, c, dp, dc, thread_ctx=""):
    if not ANTHROPIC_KEY or not playbook:
        return None
    try:
        import anthropic
    except Exception:
        return None
    cur = ADV_SRC[region][-1]
    total_roas_p = roas(p["total_rev"], p.get("total_spend") or p.get("total_meta_spend"))
    total_roas_c = roas(c["total_rev"], c.get("total_spend") or c.get("total_meta_spend"))
    meta_roas_p = roas(p["meta_rev"], p["meta_spend"])
    meta_roas_c = roas(c["meta_rev"], c["meta_spend"])
    ctx_block = f"\n\n[이전 스레드 토론 — 과거 조언 및 사람들의 코멘트(오래된→최근)]\n{thread_ctx}" if thread_ctx else ""
    user = (f"[기간] {dp} → {dc} ({label})\n"
            f"[종합] 메타 ROAS {meta_roas_p}%→{meta_roas_c}% · 전체종합 ROAS {total_roas_p}%→{total_roas_c}%\n\n"
            f"[세트 데이터 · 최근 {ADVICE_DAYS}일 · 지출 큰 순]\n{sets_to_text(items, cur)}"
            f"{ctx_block}\n\n[플레이북]\n{playbook}")
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    resp = client.messages.create(
        model="claude-opus-4-8", max_tokens=1500,
        thinking={"type": "adaptive"}, output_config={"effort": "medium"},
        system=ADV_SYSTEM, messages=[{"role": "user", "content": user}])
    txt = "".join(b.text for b in resp.content if b.type == "text").strip()
    return f"🧠 *오늘의 증감액 조언* ({label} · 플레이북 기준)\n{txt}" if txt else None

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

        # 조언 생성 (플레이북 + 세트/메모/증감액표시 → Claude)
        advice = None
        if not NO_ADVICE and playbook:
            try:
                items, _ = gather_sets(region, dc)
                thread_ctx = fetch_thread_context(ch, region)
                advice = compose_advice(label, region, playbook, items, p, c, dp, dc, thread_ctx)
            except Exception as e:
                print(f"  [조언] 생성 실패: {e}")

        if DRY:
            print("\n  ── 구성요소 (전체 종합 보정용) ──")
            for tag, d in [(dp, p), (dc, c)]:
                print(f"  · {tag}: " + "  ".join(f"{k}={v:,.0f}" for k, v in d["comp"].items()))
            if advice:
                print("\n  ── 스레드 댓글(조언) 미리보기 ──\n" + advice)
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

if __name__ == "__main__":
    main()
