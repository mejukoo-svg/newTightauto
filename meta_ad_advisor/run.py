"""
Tight Saju — Meta 광고 세트 일일 증감액 자문 봇.

매일 KST 00:30 (UTC 15:30) 실행 → 어제(KST D-1) 데이터를 분석해 시트에 추천 행 prepend.

흐름
  1. Supabase REST API로 ad_performance_daily에서 D, D-1, 7일치 raw 데이터 조회
  2. 베이스라인 분류기 (CLONE_30 / INC_20 / DEC_30 / DEC_50 / DEC_20_FATIGUE)
  3. 시트 G컬럼의 최근 14일 피드백 수집
  4. Claude (sonnet-4-6)가 후보 + 피드백을 받아 최종 추천 확정
  5. 시트 row 2 위치에 추천 prepend (최신이 위로)

환경변수 (GitHub Secrets — newTightauto 레포 기존 시크릿 재사용)
  - SUPABASE_URL              : https://<ref>.supabase.co
  - SUPABASE_SERVICE_KEY      : service-role JWT
  - GCP_SERVICE_ACCOUNT_KEY   : 서비스 계정 JSON 전체
  - ANTHROPIC_API_KEY         : Claude API key (신규 등록 필요)
  - AI_ADVISOR_META           : Google Sheet URL or bare key
  - DRY_RUN (optional)        : "1"이면 시트 쓰기 스킵
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, date, timedelta, timezone
from typing import Any

import anthropic
import gspread
import requests
from google.oauth2.service_account import Credentials


def _extract_sheet_key(raw: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", raw)
    return m.group(1) if m else raw.strip()


KST = timezone(timedelta(hours=9))
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
SHEET_ID = _extract_sheet_key(os.environ["AI_ADVISOR_META"])
DRY_RUN = os.environ.get("DRY_RUN") == "1"

CLAUDE_MODEL = "claude-sonnet-4-6"
SUPA_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept": "application/json",
}

# ───────────────────────────── baseline rules (system prompt) ─────────────────────────────

BASELINE_RULES = """\
You are the Meta ads optimizer for Tight Saju (Korean fortune-telling, KRW market).
You receive (a) candidate ad-set recommendations produced by a deterministic Python classifier
and (b) recent user feedback from the G column of the recommendation sheet.

Apply the user's accumulated feedback as durable preferences and return the FINAL list.

Baseline rules the classifier already enforces (do not relitigate the math):
  - +20% (증액): D ROAS ≥ 1.5× & D-1 ≥ 1.2× & 7d ≥ 1.3× & freq ≤ 1.4 & 예산소진 ≥ 70%
                (Meta 공식 Significant Edits — 한 번에 20% 초과 변경 시 학습 단계 재진입 회피)
  - +30% (복제증액): 7d ≥ 2.0× & 결과 ≥ 5건 & 5일 데이터 & D, D-1 모두 ≥ 1.5×
                    OR D ≥ 2.5× & D-1 ≥ 2.0× & freq ≥ 1.2 (천장 신호)
                    (r/PPC, r/FacebookAds 합의 — 위닝 직접증액 대신 복제로 학습 보호)
  - −20% (피로 감액): freq ≥ 1.5 & D ROAS < D-1×0.85 & 7d ≥ 1.3× — 소재 리프레시 권장
  - −30% (저조 감액): D < 0.8× & D-1 < 1.0× & 7d < 1.0×
  - −50% (심각 감액): D, D-1 모두 < 0.5×; 또는 D, D-1 모두 ₩50k+ 지출 & 결과 0건

Your job:
  1. For EACH candidate, decide: keep / suppress / modify (rec_pct or reason).
  2. Reflect the feedback. Examples:
     - "이미 복제했음" for adset X → suppress CLONE on X for next ~7 days.
     - "freq 기준 너무 낮음 1.5→1.7" → mention you applied looser threshold.
     - "무당 시리즈 클론 그만" → suppress CLONE on 무당 series.
     - "소량 데이터 빼" → suppress recs where 7d_results < 5 unless very strong.
  3. Do NOT add adsets that weren't in the candidates list (no new promotions).
  4. Reasons should be 1-2 short Korean sentences citing concrete numbers (D / D-1 / 7d ROAS, freq, 결과).
     If feedback influenced the decision, briefly note it (e.g., "(피드백 반영)").
  5. Return rec_pct strings exactly like "+20% (증액)", "+30% (복제증액)", "-30% (감액)", "-50% (감액)", "-20% (소재 리프레시)".
  6. ROAS values in the input are decimal ratios (1.5 = 150%).

Use the submit_recommendations tool. Return ONLY the tool call.
"""

REC_PCT_LABEL = {
    "CLONE_30": "+30% (복제증액)",
    "INC_20":   "+20% (증액)",
    "DEC_20_FATIGUE": "-20% (소재 리프레시)",
    "DEC_30":   "-30% (감액)",
    "DEC_50":   "-50% (감액)",
}


# ───────────────────────────── data fetch (Supabase REST) ─────────────────────────────

def _fetch_range(start: date, end: date) -> list[dict[str, Any]]:
    """Fetch ad_performance_daily rows in [start, end] inclusive. Paginated."""
    url = f"{SUPABASE_URL}/rest/v1/ad_performance_daily"
    cols = "date,adset_id,campaign_name,adset_name,spend,revenue,frequency,results_mp,budget"
    out: list[dict] = []
    offset = 0
    page = 1000
    while True:
        params = {
            "date": f"gte.{start.isoformat()}",
            "select": cols,
            "limit": str(page),
            "offset": str(offset),
        }
        # PostgREST allows multiple filters on same column via repeated query: append &date=lte.X
        full = f"{url}?date=gte.{start.isoformat()}&date=lte.{end.isoformat()}&select={cols}&limit={page}&offset={offset}"
        r = requests.get(full, headers=SUPA_HEADERS, timeout=30)
        r.raise_for_status()
        chunk = r.json()
        out.extend(chunk)
        if len(chunk) < page:
            break
        offset += page
    return out


def _to_float(v: Any) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def fetch_candidates(target_date: date) -> list[dict[str, Any]]:
    """Replicates the SQL classifier in Python over REST-fetched rows."""
    start = target_date - timedelta(days=6)
    rows = _fetch_range(start, target_date)
    if not rows:
        return []

    # Index by adset_id + date
    by_adset: dict[str, dict[date, dict]] = {}
    for r in rows:
        aid = r["adset_id"]
        d = datetime.fromisoformat(r["date"]).date() if isinstance(r["date"], str) else r["date"]
        by_adset.setdefault(aid, {})[d] = r

    out: list[dict[str, Any]] = []
    dm1_date = target_date - timedelta(days=1)
    seven_dates = [target_date - timedelta(days=i) for i in range(7)]

    for aid, day_map in by_adset.items():
        d_row = day_map.get(target_date)
        if not d_row:
            continue  # no D data → not actionable for this run

        dm1_row = day_map.get(dm1_date, {})

        d_spend = _to_float(d_row.get("spend"))
        d_revenue = _to_float(d_row.get("revenue"))
        d_roas = (d_revenue / d_spend) if d_spend > 0 else 0.0
        d_freq = _to_float(d_row.get("frequency"))
        d_results = int(_to_float(d_row.get("results_mp")))
        d_budget = _to_float(d_row.get("budget"))

        dm1_spend = _to_float(dm1_row.get("spend"))
        dm1_revenue = _to_float(dm1_row.get("revenue"))
        dm1_roas = (dm1_revenue / dm1_spend) if dm1_spend > 0 else 0.0
        dm1_results = int(_to_float(dm1_row.get("results_mp")))

        # 7-day window (only days actually present)
        s7_spend = sum(_to_float(day_map.get(d, {}).get("spend")) for d in seven_dates)
        s7_revenue = sum(_to_float(day_map.get(d, {}).get("revenue")) for d in seven_dates)
        s7_roas = (s7_revenue / s7_spend) if s7_spend > 0 else 0.0
        s7_results = sum(int(_to_float(day_map.get(d, {}).get("results_mp"))) for d in seven_dates)
        present_days = [d for d in seven_dates if d in day_map]
        s7_days = len(present_days)
        s7_freqs = [_to_float(day_map[d].get("frequency")) for d in present_days
                    if _to_float(day_map[d].get("frequency")) > 0]
        s7_avg_freq = (sum(s7_freqs) / len(s7_freqs)) if s7_freqs else 0.0

        # Classifier (mirrors SQL CASE WHEN, evaluated top-down)
        rec = "HOLD"
        if (s7_roas >= 2.0 and s7_results >= 5 and s7_days >= 5
                and d_roas >= 1.5 and dm1_roas >= 1.5):
            rec = "CLONE_30"
        elif d_roas >= 2.5 and dm1_roas >= 2.0 and d_freq >= 1.2:
            rec = "CLONE_30"
        elif d_roas < 0.5 and dm1_roas < 0.5 and d_spend > 0 and dm1_spend > 0:
            rec = "DEC_50"
        elif d_spend >= 50000 and d_results == 0 and dm1_spend >= 50000 and dm1_results == 0:
            rec = "DEC_50"
        elif d_freq >= 1.5 and dm1_roas > 0 and d_roas < dm1_roas * 0.85 and s7_roas >= 1.3:
            rec = "DEC_20_FATIGUE"
        elif d_roas < 0.8 and dm1_roas < 1.0 and s7_roas < 1.0 and d_spend > 0:
            rec = "DEC_30"
        elif (d_roas >= 1.5 and dm1_roas >= 1.2 and s7_roas >= 1.3 and d_freq <= 1.4
              and (d_budget == 0 or (d_budget > 0 and d_spend / d_budget >= 0.7))):
            rec = "INC_20"

        out.append({
            "d_date": target_date.isoformat(),
            "adset_id": aid,
            "campaign_name": d_row.get("campaign_name"),
            "adset_name": d_row.get("adset_name"),
            "d_spend": d_spend, "d_revenue": d_revenue, "d_roas": d_roas,
            "d_freq": d_freq, "d_results": d_results, "d_budget": d_budget,
            "dm1_spend": dm1_spend, "dm1_roas": dm1_roas, "dm1_results": dm1_results,
            "s7_spend": s7_spend, "s7_roas": s7_roas, "s7_results": s7_results,
            "s7_days": s7_days, "s7_avg_freq": s7_avg_freq,
            "rec": rec,
        })
    return out


# ───────────────────────────── sheet I/O ─────────────────────────────

def init_sheet():
    sa_json = os.environ["GCP_SERVICE_ACCOUNT_KEY"]
    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID).sheet1


def fetch_recent_feedback(ws, lookback_days: int = 14) -> list[dict[str, str]]:
    all_rows = ws.get_all_values()
    if len(all_rows) < 2:
        return []
    cutoff = date.today() - timedelta(days=lookback_days + 7)
    out = []
    for row in all_rows[1:]:
        cells = (row + [""] * 7)[:7]
        d_str, camp, adset, adset_id, pct, reason, fb = cells
        if not fb.strip():
            continue
        try:
            d = datetime.strptime(d_str.strip(), "%Y-%m-%d").date()
        except Exception:
            continue
        if d < cutoff:
            continue
        out.append({
            "date": d_str, "campaign_name": camp, "adset_name": adset,
            "adset_id": adset_id, "prior_rec": pct, "feedback": fb.strip(),
        })
    return out


# ───────────────────────────── Claude call ─────────────────────────────

def call_claude(target_date: date, candidates: list[dict], feedback: list[dict]) -> list[dict]:
    if not candidates:
        return []

    presented_candidates = []
    for c in candidates:
        if c["rec"] == "HOLD":
            continue
        presented_candidates.append({
            "campaign_name": c["campaign_name"], "adset_name": c["adset_name"], "adset_id": c["adset_id"],
            "default_rec_pct": REC_PCT_LABEL[c["rec"]],
            "signals": {
                "d_spend": round(c["d_spend"]), "d_roas": round(c["d_roas"], 2),
                "d_freq": round(c["d_freq"], 2), "d_results": int(c["d_results"]),
                "d_budget": int(c["d_budget"]),
                "dm1_spend": round(c["dm1_spend"]), "dm1_roas": round(c["dm1_roas"], 2),
                "dm1_results": int(c["dm1_results"]),
                "s7_spend": round(c["s7_spend"]), "s7_roas": round(c["s7_roas"], 2),
                "s7_results": int(c["s7_results"]), "s7_days": int(c["s7_days"]),
                "s7_avg_freq": round(c["s7_avg_freq"], 2),
            },
        })

    if not presented_candidates:
        return []

    user_payload = {
        "target_date": target_date.isoformat(),
        "candidates": presented_candidates,
        "recent_feedback": feedback,
    }

    tool = {
        "name": "submit_recommendations",
        "description": "Submit final ad-set recommendations after applying user feedback.",
        "input_schema": {
            "type": "object",
            "properties": {
                "recommendations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "campaign_name": {"type": "string"},
                            "adset_name": {"type": "string"},
                            "adset_id": {"type": "string"},
                            "rec_pct": {"type": "string"},
                            "reason": {"type": "string"},
                        },
                        "required": ["campaign_name", "adset_name", "adset_id", "rec_pct", "reason"],
                    },
                }
            },
            "required": ["recommendations"],
        },
    }

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=8000,
        system=[{"type": "text", "text": BASELINE_RULES, "cache_control": {"type": "ephemeral"}}],
        tools=[tool],
        tool_choice={"type": "tool", "name": "submit_recommendations"},
        messages=[{"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)}],
    )

    for block in resp.content:
        if block.type == "tool_use" and block.name == "submit_recommendations":
            return list(block.input.get("recommendations", []))
    return []


# ───────────────────────────── output ─────────────────────────────

def format_reason(reason: str) -> str:
    """Insert line breaks at sentence boundaries for readability in Sheets cells."""
    s = reason.strip()
    parts = re.split(r"(?<=[.])\s+", s)
    return "\n".join(p.strip() for p in parts if p.strip())


def write_to_sheet(ws, target_date: date, recs: list[dict]):
    if not recs:
        print(f"[{target_date}] no recommendations — sheet untouched.")
        return
    rows = [
        [target_date.isoformat(), r["campaign_name"], r["adset_name"], r["adset_id"],
         r["rec_pct"], format_reason(r["reason"]), ""]
        for r in recs
    ]
    if DRY_RUN:
        print("[DRY_RUN] rows that would be inserted:")
        for r in rows:
            print(" | ".join(r))
        return
    # RAW so leading "+" in rec_pct (e.g. "+20% (증액)") is stored as text, not parsed as formula.
    ws.insert_rows(rows, row=2, value_input_option="RAW")
    print(f"[{target_date}] inserted {len(rows)} rows at row 2.")


def main():
    now_kst = datetime.now(KST)
    override = os.environ.get("TARGET_DATE", "").strip()
    if override:
        target_date = datetime.strptime(override, "%Y-%m-%d").date()
        print(f"=== Run @ KST {now_kst.isoformat(timespec='seconds')} → TARGET_DATE override={target_date} ===")
    else:
        target_date = (now_kst - timedelta(days=1)).date()
        print(f"=== Run @ KST {now_kst.isoformat(timespec='seconds')} → target_date={target_date} (D-1) ===")

    candidates = fetch_candidates(target_date)
    if not candidates:
        print(f"[{target_date}] no rows in ad_performance_daily — aborting.")
        return
    actionable = [c for c in candidates if c["rec"] != "HOLD"]
    print(f"candidates: {len(candidates)} total, {len(actionable)} actionable.")

    ws = init_sheet()
    feedback = fetch_recent_feedback(ws)
    print(f"feedback rows: {len(feedback)}")

    final_recs = call_claude(target_date, candidates, feedback)
    print(f"final recs from Claude: {len(final_recs)}")

    write_to_sheet(ws, target_date, final_recs)


if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        print(f"missing env var: {e}", file=sys.stderr)
        sys.exit(2)
