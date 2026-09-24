# -*- coding: utf-8 -*-
"""
budget_resolve.py
=================
'현재 일예산' 해석 공용 모듈 (국내·글로벌 파이프라인 공유).

배경 — 추이차트(국내·글로벌) '예산' 컬럼이 일부 세트에서 비어 있던 이유 (2026-09-23 실측):

  ① 일정(예약 노출) 세트 — '심야만' · '오전시간제외테스트' 류
     메타는 노출 일정을 쓰는 세트에 daily_budget 을 허용하지 않는다. 대신
     lifetime_budget(기간 총예산) + start_time/end_time 만 있다.
     기존 코드는 daily_budget 만 읽어 0 → 예산 칸이 비었다.
       예) 무당_260419_aiUGC이불_기여증대_심야만: daily=0, lifetime=600,000,
           09-22 18:10 ~ 09-27 23:59(5.24일) → 일예산 환산 ≈ 114,500 (실제 일 지출 117k)

  ② 캠페인 예산(ASC·CBO)
     예산이 캠페인에 달려 세트에는 아무 예산도 없다. 국내엔 캠페인 daily_budget
     폴백이 있었지만 글로벌엔 폴백 자체가 없었고, 양쪽 다 캠페인 lifetime_budget 은
     못 읽었다.
       예) 대만_외모정병_ASC: 세트 예산 없음 / 캠페인 daily_budget 20,000(=$200)
           무당_260916_심야캠페인: 캠페인 lifetime_budget 1,500,000

  ③ ARCHIVED 세트
     계정 세트 목록 조회는 effective_status 필터로 ARCHIVED/DELETED 를 제외한다.
     그런데 최근에 지출이 있던 세트는 보관 처리 뒤에도 추이차트에 행이 남는다
     → 행은 보이는데 예산만 빈 상태. 세트를 id 로 직접 조회하면 예산이 그대로 있다.
       예) 홍콩_무녀_..._260914: ARCHIVED, daily_budget 8,000 보유

해결(이 모듈) — 계정 스윕이 끝난 뒤 '아직 0인, 대시보드에 실제로 뜨는 세트'만 골라
  1) ids= 배치로 세트를 직접 조회(③ 해결, 총예산·일정도 함께 읽음)
  2) 그래도 0이면 캠페인 예산으로 폴백(② 해결)
  3) 총예산(lifetime)은 일정 기간으로 나눠 '일예산 환산값'으로 바꾼다(① 해결)

값 단위는 daily_budget 과 같은 raw 그대로 반환한다
(국내 KRW=원 / 글로벌 USD=cents) — 호출부의 기존 환산식이 그대로 먹도록.

★ 대상은 relevant_ids(최근 insights 가 있어 실제로 표에 뜨는 세트)로 한정한다.
  계정 전체를 다시 훑지 않으므로 추가 API 호출은 보통 한 자릿수에 그친다.
"""
from datetime import datetime, timezone

# ids= 배치 1회당 오브젝트 수 (메타 권장 상한 50)
BATCH = 50
# 세트/캠페인에서 읽을 필드
ADSET_FIELDS = "id,daily_budget,lifetime_budget,start_time,end_time,campaign_id"
CAMPAIGN_FIELDS = "id,daily_budget,lifetime_budget,start_time,stop_time"


def _num(x):
    """'8000' · 8000.0 · None · '' → int 또는 0."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0
    return int(round(v)) if v > 0 else 0


def _dt(s):
    """메타 시각 문자열('2026-09-22T18:10:03+0900') → aware datetime. 실패 시 None."""
    if not s:
        return None
    try:
        return datetime.strptime(str(s), "%Y-%m-%dT%H:%M:%S%z")
    except (TypeError, ValueError):
        return None


def lifetime_to_daily(lifetime, start_time, end_time, now=None):
    """총예산 → 일예산 환산. 단위는 입력 그대로(raw).

       일정 세트의 총예산은 '그 기간 동안 하루 얼마'를 쓰겠다는 뜻이므로
       기간(일)으로 나눠야 추이차트의 다른 세트(일예산)와 같은 축에 놓인다.
       그냥 총예산을 넣으면 정렬(💸 예산순)과 증감 테두리가 몇 배로 부풀려진다.

       기간 산정: end 가 있으면 end-start, 없으면 start~지금(진행 중). 최소 1일.
       start 조차 없으면 기간을 모르므로 0 을 돌려 '모름'으로 둔다 —
       확실하지 않은 큰 숫자를 넣는 것보다 비워두는 편이 덜 해롭다."""
    lt = _num(lifetime)
    if lt <= 0:
        return 0
    s, e = _dt(start_time), _dt(end_time)
    if s is None:
        return 0
    if e is not None and e > s:
        days = (e - s).total_seconds() / 86400.0
    else:
        ref = now or datetime.now(timezone.utc)
        days = (ref - s).total_seconds() / 86400.0
    return int(round(lt / max(days, 1.0)))


def _batch_get(api_get, base_url, token, ids, fields, log=None):
    """ids= 배치 조회. 배치 하나가 통째로 실패하면(잘못된 id 하나에 전체가 400)
       반으로 쪼개 재시도하고, 1개짜리까지 실패하면 그 id만 버린다."""
    out = {}
    if not ids:
        return out
    data = api_get(base_url.rstrip("/") + "/",
                   {"ids": ",".join(ids), "fields": fields}, token=token)
    if isinstance(data, dict) and not data.get("error"):
        for k, v in data.items():
            if isinstance(v, dict) and v.get("id"):
                out[str(k)] = v
        return out
    if len(ids) == 1:
        if log:
            log.warning(f"  ⚠️ 예산 직접조회 실패(무시): {ids[0]}")
        return out
    mid = len(ids) // 2
    out.update(_batch_get(api_get, base_url, token, ids[:mid], fields, log))
    out.update(_batch_get(api_get, base_url, token, ids[mid:], fields, log))
    return out


def enrich_budgets(base_url, api_get, token, results, relevant_ids,
                   adset_campaign=None, log=None, label="", lifetime_ids=None):
    """계정 스윕 결과(results: 세트id→raw 예산)를 제자리 보강한다.

       results        : 세트 목록 조회로 이미 채운 dict (0 = 아직 모름)
       relevant_ids   : 이 계정에서 실제로 표에 뜨는 세트 id 집합 (보강 대상 한정)
       adset_campaign : 세트→캠페인 맵. 주면 여기서 알아낸 매핑도 채워 넣는다
                        (activities CBO 이벤트 적용에 쓰인다)
       lifetime_ids   : set 을 주면 '총예산을 환산해 값을 낸 세트' id 를 담아 돌려준다.
                        ★ 이 세트들은 activities(예산 변경이력) 재구성을 쓰면 안 된다 —
                          이벤트 값이 일예산이 아니라 총예산이라 그대로 쓰면 기간 배수만큼
                          부풀려진다(실측: 5일 일정 720,000 총예산이 720,000 일예산으로 표시).
       반환: {'direct': n, 'lifetime': n, 'campaign': n} — 무엇으로 몇 개를 채웠는지"""
    stat = {"direct": 0, "lifetime": 0, "campaign": 0}
    if not relevant_ids:
        return stat

    pending = [str(a) for a in relevant_ids if _num(results.get(str(a))) <= 0]
    if not pending:
        return stat

    # 1) 세트 직접 조회 — ARCHIVED 로 목록에서 빠진 세트, 총예산(일정) 세트를 함께 건진다.
    need_camp = {}
    sched = {}      # 세트id → (start_time, end_time) — 캠페인 총예산 환산의 기간 기준
    for i in range(0, len(pending), BATCH):
        got = _batch_get(api_get, base_url, token, pending[i:i + BATCH], ADSET_FIELDS, log)
        for aid, o in got.items():
            cid = str(o.get("campaign_id") or "")
            if cid and adset_campaign is not None:
                adset_campaign[aid] = cid
            d = _num(o.get("daily_budget"))
            if d > 0:
                results[aid] = d
                stat["direct"] += 1
                continue
            d = lifetime_to_daily(o.get("lifetime_budget"), o.get("start_time"), o.get("end_time"))
            if d > 0:
                results[aid] = d
                if lifetime_ids is not None:
                    lifetime_ids.add(aid)
                stat["lifetime"] += 1
                continue
            if cid:
                need_camp[aid] = cid
                # 세트 자신의 일정은 캠페인 총예산을 일예산으로 나눌 때 쓴다 (아래 참고)
                sched[aid] = (o.get("start_time"), o.get("end_time"))
    # 목록 조회에서 캠페인 id 를 이미 알고 있던(=ARCHIVED 가 아닌) 0원 세트도 폴백 대상
    if adset_campaign is not None:
        for aid in pending:
            if _num(results.get(aid)) <= 0 and aid not in need_camp and adset_campaign.get(aid):
                need_camp[aid] = adset_campaign[aid]

    # 2) 캠페인 예산 폴백 (ASC·CBO) — 일예산이 없으면 캠페인 총예산도 일예산으로 환산.
    if need_camp:
        cids = sorted(set(need_camp.values()))
        camps = {}
        for i in range(0, len(cids), BATCH):
            camps.update(_batch_get(api_get, base_url, token, cids[i:i + BATCH],
                                    CAMPAIGN_FIELDS, log))
        for aid, cid in need_camp.items():
            o = camps.get(str(cid))
            if not o:
                continue
            b = _num(o.get("daily_budget"))
            _from_life = False
            if b <= 0:
                # 캠페인 총예산(CBO lifetime) → 일예산 환산.
                #   ★ 기간은 세트 자신의 일정을 먼저 쓴다. 캠페인의 start/stop_time 은
                #     '마지막으로 손댄 시각'이 들어오는 일이 있어(실측: 4일짜리 심야캠페인의
                #     stop_time 이 시작 다음날) 그대로 나누면 일예산이 몇 배로 튄다.
                st, en = sched.get(aid, (None, None))
                b = lifetime_to_daily(o.get("lifetime_budget"), st, en)
                if b <= 0:
                    b = lifetime_to_daily(o.get("lifetime_budget"),
                                          o.get("start_time"), o.get("stop_time"))
                _from_life = b > 0
            if b > 0:
                if _from_life and lifetime_ids is not None:
                    lifetime_ids.add(aid)
                # CBO 는 한 캠페인 예산을 소속 세트가 나눠 쓴다 — 대시보드 관례대로
                # 세트마다 같은 값을 반복해 넣는다(세로로 더하지 말 것: 예산 컬럼 툴팁 참고).
                results[aid] = b
                stat["campaign"] += 1

    if log and any(stat.values()):
        log.info(f"  🩹 예산 보강{(' ' + label) if label else ''}: "
                 f"세트직접 {stat['direct']} · 총예산환산 {stat['lifetime']} · 캠페인예산 {stat['campaign']}"
                 f" (대상 {len(pending)}개)")
    return stat
