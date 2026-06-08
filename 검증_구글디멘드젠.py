# -*- coding: utf-8 -*-
"""
검증_구글디멘드젠.py  (읽기 전용 — 어떤 테이블도 수정하지 않음)
=================================================================
구글 디멘드젠 콘텐츠별 데이터(매출/지출)를 독립 소스와 교차검증한다.

검증 3종:
  ① 일별 종합 (디멘드젠 dg_ 기준): 지출/매출/순이익/ROAS/건수
       - ROAS 가 비상식(수천%)이면 지출 누락, 음수/0이면 데이터 누락 의심
       - 매출만 있고 지출 0 인 날 = 광고보고서 엑셀 미적재 (표시됨)
  ② 지출 교차검증: Supabase 콘텐츠지출 일합  vs  google_demandgen_daily(구글시트 채널합)
       - 완전히 다른 파이프라인(시트)과 대조. VAT 차이로 엑셀이 시트의 ~91% 면 정상
  ③ 매출 교차검증: Supabase 매출  vs  Mixpanel export+$insert_id dedup 재계산
       - production 스크립트와 동일 방식으로 원본에서 재계산해 일치 확인
       - Mixpanel 인증정보(env) 없으면 이 검증은 건너뜀
       - ※ Mixpanel insights UI 는 dedup 기준이 달라 10~20% 차이날 수 있음(정상).
            대시보드/이 스크립트는 export+dedup 기준.

환경변수:
  SUPABASE_URL        (기본: 대시보드 프로젝트)
  SUPABASE_KEY        (읽기용 anon publishable, 기본: index.html 공개키)
  MIXPANEL_PROJECT_ID / MIXPANEL_USERNAME / MIXPANEL_SECRET  (③ 용, 없으면 스킵)

사용:
  py 검증_구글디멘드젠.py            # 최근 16일
  py 검증_구글디멘드젠.py --days 30  # 기간 지정
"""

import os, sys, json
from datetime import datetime, timezone, timedelta, date
from collections import defaultdict

import requests

# ── 설정 ────────────────────────────────────────────────────────────────
SB_URL = os.environ.get("SUPABASE_URL", "https://qkvqiorazdrhtuicnpec.supabase.co").rstrip("/")
# 읽기 전용: index.html 에 이미 공개된 anon publishable 키 기본값 (override 가능)
SB_KEY = os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_SERVICE_KEY") \
    or "sb_publishable_43NkUJjcYzcBUiJhnKVHXw_eWTuZU2g"
SBH = {"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}"}

MP_PID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MP_USER = os.environ.get("MIXPANEL_USERNAME")
MP_SECRET = os.environ.get("MIXPANEL_SECRET")

MP_EVENTS = ["결제완료", "payment_complete"]
EXCLUDE_CT_SUBSTR = ["moodang_260529"]   # 구글 디멘드젠 매출 스크립트와 동일한 phantom 제외

DAYS = 16
if "--days" in sys.argv:
    try: DAYS = int(sys.argv[sys.argv.index("--days") + 1])
    except Exception: pass

TODAY = (datetime.now(timezone.utc) + timedelta(hours=9)).date()
START = TODAY - timedelta(days=DAYS - 1)
START_ISO, END_ISO = START.isoformat(), TODAY.isoformat()

KRW = lambda n: f"₩{n:,.0f}"
isdg = lambda c: (c or "").lower().startswith("dg_")
excluded = lambda c: any(s in (c or "").lower() for s in EXCLUDE_CT_SUBSTR)


def sb_all(table, cols="*"):
    out, off = [], 0
    while True:
        r = requests.get(f"{SB_URL}/rest/v1/{table}",
                         headers=SBH,
                         params={"select": cols, "date": f"gte.{START_ISO}",
                                 "order": "date.asc", "limit": "1000", "offset": str(off)},
                         timeout=60)
        if r.status_code != 200:
            print(f"  ⚠️ {table} 읽기 실패 HTTP {r.status_code}: {r.text[:200]}")
            return out
        b = r.json()
        out += b
        if len(b) < 1000: break
        off += 1000
    return [x for x in out if x.get("date", "") <= END_ISO]


def section1_summary(rev, sp):
    print("=" * 74)
    print(f"① 일별 종합 (디멘드젠 dg_ 기준)  {START_ISO} ~ {END_ISO}")
    print("=" * 74)
    R, C, S = defaultdict(float), defaultdict(int), defaultdict(float)
    for r in rev:
        if isdg(r["content"]): R[r["date"]] += r["revenue"] or 0; C[r["date"]] += r["purchase_count"] or 0
    for r in sp:
        if isdg(r["content"]): S[r["date"]] += r["spend"] or 0
    dates = sorted(set(R) | set(S))
    print(f"  {'날짜':12}{'지출':>13}{'매출':>13}{'순이익':>13}{'ROAS':>7}{'건수':>6}  비고")
    tR = tS = tC = 0
    for d in dates:
        s, r, c = S.get(d, 0), R.get(d, 0), C.get(d, 0)
        tR += r; tS += s; tC += c
        roas = f"{r/s*100:.0f}%" if s > 0 else "-"
        note = ""
        if s == 0 and r > 0: note = "지출없음(엑셀미적재)"
        elif s > 0 and r/s*100 > 1000: note = "⚠️ROAS과대(지출누락?)"
        print(f"  {d:12}{KRW(s):>13}{KRW(r):>13}{KRW(r-s):>13}{roas:>7}{c:>6}  {note}")
    roas = f"{tR/tS*100:.0f}%" if tS > 0 else "-"
    print(f"  {'합계':12}{KRW(tS):>13}{KRW(tR):>13}{KRW(tR-tS):>13}{roas:>7}{tC:>6}")


def section2_spend_vs_sheet(sp):
    print("\n" + "=" * 74)
    print("② 지출 교차검증: 콘텐츠지출 일합(엑셀)  vs  google_demandgen_daily(구글시트)")
    print("   독립 소스 대조. 엑셀이 시트의 85~100% 면 정상(VAT 등).")
    print("=" * 74)
    S = defaultdict(float)
    for r in sp:
        S[r["date"]] += r["spend"] or 0     # sa_ 포함 전체 (시트도 채널전체라 동일 기준)
    sheet = {r["date"]: r for r in sb_all("google_demandgen_daily", "date,cost_vat")}
    print(f"  {'날짜':12}{'엑셀합(콘텐츠)':>16}{'시트채널지출':>15}{'비율':>8}")
    ratios = []
    for d in sorted(S):
        sh = sheet.get(d, {}).get("cost_vat")
        if sh:
            ratio = S[d] / sh * 100; ratios.append(ratio)
            flag = "✅" if 80 <= ratio <= 105 else "⚠️"
            print(f"  {d:12}{KRW(S[d]):>16}{KRW(sh):>15}{ratio:>7.0f}% {flag}")
        else:
            print(f"  {d:12}{KRW(S[d]):>16}{'(시트값없음)':>15}")
    if ratios:
        print(f"  → 평균 비율 {sum(ratios)/len(ratios):.0f}%  (VAT 보정 시 ~91% 가 기대값)")


def mp_export(frm, to):
    r = requests.get("https://data.mixpanel.com/api/2.0/export",
                     params={"from_date": frm, "to_date": to,
                             "event": json.dumps(MP_EVENTS), "project_id": MP_PID},
                     auth=(MP_USER, MP_SECRET), timeout=600)
    if r.status_code != 200:
        print(f"  ⚠️ Mixpanel export HTTP {r.status_code}: {r.text[:200]}")
        return None
    return [l for l in r.text.splitlines() if l.strip()]


def section3_revenue_vs_mixpanel(rev):
    print("\n" + "=" * 74)
    print("③ 매출 교차검증: Supabase  vs  Mixpanel export+$insert_id dedup 재계산")
    print("=" * 74)
    if not (MP_USER and MP_SECRET):
        print("  ⏭  MIXPANEL_USERNAME/SECRET 환경변수 없음 → 매출 교차검증 스킵")
        print("     (export+dedup 재계산하려면 서비스 계정 인증정보를 env 로 주세요)")
        return
    lines = mp_export(START_ISO, END_ISO)
    if not lines:
        return
    seen = set(); dup = 0
    mp = defaultdict(float)   # dg_ 기준, phantom 제외 — Supabase 와 동일 기준
    for ln in lines:
        try: ev = json.loads(ln)
        except Exception: continue
        p = ev.get("properties", {})
        iid = p.get("$insert_id")
        if iid is not None:
            k = (ev.get("event"), iid)
            if k in seen: dup += 1; continue
            seen.add(k)
        if str(p.get("ch", "")).strip().lower() != "google": continue
        ct = str(p.get("ct", "")).strip()
        if not isdg(ct) or excluded(ct): continue
        ts = p.get("time", 0)
        if not ts: continue
        d = (datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)).date().isoformat()
        amt = 0.0
        for kk in ("amount", "결제금액", "value"):
            v = p.get(kk)
            if v is not None:
                try: amt = float(v); break
                except Exception: pass
        if amt > 0: mp[d] += amt
    print(f"  raw {len(lines)}건 / 중복제거 {dup}건")
    sbv = defaultdict(float)
    for r in rev:
        if isdg(r["content"]): sbv[r["date"]] += r["revenue"] or 0
    print(f"  {'날짜':12}{'Supabase매출':>15}{'MP재계산':>15}{'차이':>12}")
    for d in sorted(set(sbv) | set(mp)):
        diff = sbv.get(d, 0) - mp.get(d, 0)
        flag = "✅" if abs(diff) < 1 else ("~" if abs(diff) < max(mp.get(d, 1), 1) * 0.02 else "⚠️")
        print(f"  {d:12}{KRW(sbv.get(d,0)):>15}{KRW(mp.get(d,0)):>15}{diff:>12,.0f} {flag}")
    print("  ※ ⚠️ 인 날은 cron 갱신창 밖이거나 적재 후 데이터 변동. 재적재로 해소.")


def main():
    print(f"\n🔎 구글 디멘드젠 데이터 검증  (최근 {DAYS}일: {START_ISO}~{END_ISO})")
    print(f"   Supabase: {SB_URL}")
    rev = sb_all("google_demandgen_content_mp_daily", "date,content,revenue,purchase_count")
    sp = sb_all("google_demandgen_content_spend_daily", "date,content,spend")
    print(f"   매출행 {len(rev)} / 지출행 {len(sp)}\n")
    section1_summary(rev, sp)
    section2_spend_vs_sheet(sp)
    section3_revenue_vs_mixpanel(rev)
    print("\n완료.")


if __name__ == "__main__":
    main()
