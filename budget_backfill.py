# -*- coding: utf-8 -*-
"""
budget_backfill.py
==================
예산 이력 '교정' 공용 모듈 (날짜탭 예산 컬럼 자가교정 + 1회 백필).

배경: 파이프라인은 activities(예산 변경이력) 창을 REFRESH_DAYS+2(≈12일)로만 조회해,
      마지막 예산변경이 그 창 밖으로 밀려난 세트는 has_events_for=False 로 판정되어
      재구성을 건너뛰고 '현재값 평탄화/기존값 보존' 폴백으로 빠졌다. 그 결과 과거 날짜
      예산이 현재값으로 평탄화되거나 잘못된 스냅샷으로 고착됐다(대조 결과 세트의 ~28%).

해결(2단):
  A) activities 창을 표시범위(≈180일)로 확대 → raw_on / has_events_for 가 항상 옳음.
  B) reconcile_budget(): 표시범위 전체 저장행의 예산 컬럼을 activities 재구성값으로
     맞춘다. 부분 업서트(PK+예산컬럼만, Prefer: resolution=merge-duplicates)라 다른 컬럼
     (spend/revenue 등)은 절대 건드리지 않는다(프로덕션 1행 검증 완료).

standalone 실행: 국내(ad_performance_daily.budget) + 글로벌(global_ad_performance_daily.
budget_usd) 두 테이블을 즉시 백필한다.  `python budget_backfill.py [--apply] [--days 180]`
(기본 dry-run. --apply 지정 시 실제 교정.)
"""
import os, sys, json, time
from datetime import datetime, timedelta, timezone
from pathlib import Path

BUDGET_HIST_DAYS_DEFAULT = 180


def reconcile_budget(sb_base_url, sb_headers, table, budget_col, bud_hist, budget_map,
                     transform, start_iso, end_iso, req_lib, log=None,
                     id_col="adset_id", tol=0.5, dry_run=False, extra_cols=()):
    """[start_iso, end_iso] 구간 저장행의 budget_col 을 activities 재구성값으로 교정.
       - has_events_for(세트)=True → raw_on 재구성값
       - False & 현재예산>0        → 현재값(변경 없음 = 평탄)
       - False & 현재예산 미상(0)   → 기존값 보존(교정 안 함)
       extra_cols: 부분 업서트 시 충돌키를 맞추기 위해 함께 읽어 되돌려 보낼 컬럼
                   (예: 글로벌 테이블 PK 에 포함된 'country'). 값은 갱신하지 않고 그대로 echo.
       반환: 교정(예정) 행 리스트."""
    read_headers = {**sb_headers, "Prefer": ""}
    _sel = ",".join(["date", id_col, budget_col, *extra_cols])
    rows = []
    off = 0
    while True:
        u = (f"{sb_base_url}/rest/v1/{table}?select={_sel}"
             f"&date=gte.{start_iso}&date=lte.{end_iso}&order=date.asc&limit=1000&offset={off}")
        try:
            chunk = req_lib.get(u, headers=read_headers, timeout=60).json()
        except Exception as e:
            if log: log.warning(f"  ⚠️ 예산교정 읽기 실패({table}): {e}")
            break
        if not isinstance(chunk, list) or not chunk:
            break
        rows += chunk
        if len(chunk) < 1000:
            break
        off += 1000

    updates = []
    for r in rows:
        aid = str(r.get(id_col))
        d = r.get("date")
        stored = r.get(budget_col)
        cur = budget_map.get(aid, 0) or 0
        if bud_hist.has_events_for(aid):
            raw = bud_hist.raw_on(aid, d, cur)
            val = transform(raw) if raw and raw > 0 else 0
        elif cur > 0:
            val = transform(cur)
        else:
            continue  # 현재예산 미상 + 이력 없음 → 판단 불가, 보존
        try:
            sv = float(stored) if stored is not None else 0.0
        except (TypeError, ValueError):
            sv = 0.0
        if abs(float(val) - sv) > tol:
            _u = {"date": d, id_col: aid, budget_col: val}
            for _c in extra_cols:      # 충돌키 echo (country 등)
                _u[_c] = r.get(_c)
            updates.append(_u)

    if log:
        log.info(f"  🔧 예산 교정 {len(updates)}/{len(rows)}행 ({table}, {start_iso}~{end_iso})"
                 + (" [DRY-RUN]" if dry_run else ""))
    if dry_run or not updates:
        return updates

    up_headers = {**sb_headers, "Prefer": "resolution=merge-duplicates"}
    url = f"{sb_base_url}/rest/v1/{table}"
    ok = 0
    for i in range(0, len(updates), 500):
        ch = updates[i:i + 500]
        try:
            resp = req_lib.post(url, headers=up_headers, json=ch, timeout=60)
            if resp.status_code in (200, 201):
                ok += len(ch)
            elif log:
                log.warning(f"   ⚠️ 예산교정 upsert 실패 {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            if log: log.warning(f"   ⚠️ 예산교정 upsert 예외: {e}")
        time.sleep(0.3)
    if log:
        log.info(f"  ✅ 예산 교정 반영 {ok}/{len(updates)}행 ({table})")
    return updates


# ============================ standalone 백필 ============================
def _standalone():
    import logging
    import requests as req_lib
    from budget_history import fetch_budget_events, BudgetHistory

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger("budbackfill")

    for line in (Path(__file__).parent / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); v = v.strip().strip('"').strip("'")
            if v: os.environ.setdefault(k.strip(), v)

    APPLY = "--apply" in sys.argv
    DAYS = BUDGET_HIST_DAYS_DEFAULT
    if "--days" in sys.argv:
        DAYS = int(sys.argv[sys.argv.index("--days") + 1])
    KST = timezone(timedelta(hours=9))
    VER = "v21.0"
    BASE = f"https://graph.facebook.com/{VER}"
    now = datetime.now(KST)
    since = int((now - timedelta(days=DAYS)).timestamp())
    until = int((now + timedelta(days=1)).timestamp())
    start_iso = (now - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    end_iso = now.strftime("%Y-%m-%d")

    SB_URL = os.environ["SUPABASE_URL"]
    SB_KEY = os.environ["SUPABASE_SERVICE_KEY"]
    SCHEMA = os.environ.get("SUPABASE_DB_SCHEMA", "new-tightauto").strip()
    SBH = {"apikey": SB_KEY, "Authorization": "Bearer " + SB_KEY,
           "Content-Type": "application/json",
           "Accept-Profile": SCHEMA, "Content-Profile": SCHEMA}

    def build_hist(accounts_tokens):
        bh = BudgetHistory(KST)
        cur = {}
        a2c = {}
        for acc, tok in accounts_tokens.items():
            if not tok:
                log.warning(f"  ⚠️ 토큰 없음: {acc}"); continue
            evs = fetch_budget_events(BASE, acc, tok, since, until, req_lib, log, max_pages=120)
            bh.add_events(evs)
            # 현재 예산 + 세트→캠페인
            url = f"{BASE}/{acc}/adsets?" + _qs({"fields": "id,daily_budget,campaign_id",
                                                 "limit": 500, "access_token": tok})
            pg = 0
            while url and pg < 30:
                j = req_lib.get(url, timeout=60).json()
                for a in j.get("data", []):
                    aid = str(a["id"]); a2c[aid] = str(a.get("campaign_id") or "")
                    db = a.get("daily_budget"); cur[aid] = int(float(db)) if db else 0
                url = j.get("paging", {}).get("next"); pg += 1
            log.info(f"  📈 {acc[-6:]}: 이벤트 {len(evs)}건")
        bh.set_adset_campaign(a2c)
        return bh, cur

    # 국내 (KRW → transform=반올림 정수)
    KR = {"act_1270614404675034": os.environ.get("META_TOKEN_1", ""),
          "act_707835224206178": os.environ.get("META_TOKEN_1", ""),
          "act_1808141386564262": os.environ.get("META_TOKEN_2", "")}
    log.info("=== 국내 activities 수집 ===")
    kr_bh, kr_cur = build_hist(KR)
    reconcile_budget(SB_URL, SBH, "ad_performance_daily", "budget",
                     kr_bh, kr_cur, lambda raw: int(round(raw)),
                     start_iso, end_iso, req_lib, log, tol=0.5, dry_run=not APPLY)

    # 글로벌 (raw cents → budget_usd = raw/100)
    _g1 = os.environ.get("META_TOKEN_1", "")
    _g2677 = os.environ.get("META_TOKEN_GlobalTT", "") or os.environ.get("META_TOKEN_4", "") or os.environ.get("META_TOKEN_3", "")
    _g9937 = os.environ.get("META_TOKEN_ACT_9937", "")
    GL = {"act_1054081590008088": _g1,
          "act_2677707262628563": _g2677,
          "act_1335040608536838": _g2677,
          "act_993712016404855": _g9937,
          "act_1021437716898605": _g1}
    log.info("=== 글로벌 activities 수집 ===")
    gl_bh, gl_cur = build_hist(GL)
    reconcile_budget(SB_URL, SBH, "global_ad_performance_daily", "budget_usd",
                     gl_bh, gl_cur, lambda raw: round(raw / 100, 2),
                     start_iso, end_iso, req_lib, log, tol=0.01, dry_run=not APPLY,
                     extra_cols=("country",))

    log.info("완료." + ("" if APPLY else "  (dry-run — 실제 반영하려면 --apply)"))


def _qs(d):
    import urllib.parse
    return urllib.parse.urlencode(d)


if __name__ == "__main__":
    _standalone()
