# -*- coding: utf-8 -*-
"""
budget_history.py
=================
메타 activities(계정 변경이력) 기반 '일자별 예산 재구성' 공용 모듈.
국내/글로벌 파이프라인이 공유한다.

배경: 파이프라인은 예산을 '현재값(daily_budget)' 한 번만 조회해 리프레시 윈도우
      전체 날짜에 덮어써 왔다. 그 결과 최근 구간 예산이 평탄화돼 전일 대비 증감이
      사라지고, 대시보드 추이차트의 '증감액 테두리'가 안 그려졌다.
해결: activities 의 update_ad_set_budget / update_campaign_budget 이벤트로
      각 세트의 '그 날짜에 실제로 설정돼 있던 예산'을 재구성한다.

핵심 포맷(프로브 확인):
  event_type: update_ad_set_budget(ABO, object_id=세트id) |
              update_campaign_budget(CBO, object_id=캠페인id → 하위 세트에 적용)
  extra_data(JSON): {"old_value":{"old_value":74100,...},
                     "new_value":{"new_value":81500,...}, ...}
    값 단위는 endpoint daily_budget 와 동일 raw(국내 KRW=원, 글로벌 USD=cents).
  event_time: UTC. KST(+9)로 변환해 날짜 매핑.

★ 메타 activities 의 '넓은 창 샘플링' (2026-08-11 실측):
  같은 계정·같은 시점에 since 만 바꿔 조회하면 결과가 서로의 부분집합이 아니다.
    since -20일 → 4페이지 / 전체이벤트 1925건 / 예산이벤트 194건 (가장 오래된 07-23)
    since -90일 → 5페이지 / 전체이벤트 2453건 / 예산이벤트 199건 (가장 오래된 05-12)
  90일 창이 20일 창보다 페이지·이벤트가 거의 안 늘었다 = 넓은 창은 로그를 건너뛴다.
  실제로 세트 120252298092520721 의 08-01·08-02 예산 변경은 20일 창엔 있고 90일 창엔 없다.
  오류·레이트리밋이 아니라(오류 None) 메타가 조용히 누락시키는 것이라 재시도로도 못 고친다.
  → ① 창 하나를 믿지 말고 '지금'을 끝점으로 고정한 중첩 창 여러 개를 합집합으로 모은다.
    ② RELIABLE_DAYS(기본 20일) 이내만 '완전하다'고 보고, 그 밖의 날짜는 재구성으로
       기존 저장값(일일 스냅샷)을 덮어쓰지 않는다 — has_event_on() 참고.
  이 규칙을 어기면(=넓은 창 재구성으로 과거를 덮어쓰면) 파이프라인이 매일 모아둔
  올바른 스냅샷이 누락투성이 재구성으로 지워져 증감액 테두리가 사라진다. 실제로 그랬다.
"""
import json
from collections import defaultdict
from datetime import datetime, timedelta

BUDGET_EVENT_TYPES = ("update_ad_set_budget", "update_campaign_budget")

# activities 가 '완전하다'고 믿을 수 있는 최근 일수(위 실측 기준). 이 밖의 과거는
# 재구성값으로 기존 저장값을 덮어쓰지 않는다.
RELIABLE_DAYS = 20
# 합집합으로 조회할 중첩 창(일). 끝점은 항상 until_epoch('지금') 고정.
PROBE_WINDOWS = (14, 20, 45, 90)


def _num(x):
    try:
        return int(round(float(x)))
    except Exception:
        return None


def fetch_budget_events(base_url, acc_id, token, since_epoch, until_epoch,
                        req_lib, log=None, max_pages=300):
    """계정의 예산 변경 이벤트를 [{ts, level, obj_id, old, new, actor} ...] 로 반환.
       level: 'adset'(update_ad_set_budget) | 'campaign'(update_campaign_budget).
       실패 시 [] (호출부에서 스냅샷 보존 폴백으로 자연 처리).

       ★ 단일 창은 메타가 조용히 샘플링하므로(모듈 docstring 참고) 요청 창 + 중첩
         단기 창들을 각각 조회해 합집합을 만든다. 중복은 (ts, obj_id, old, new) 로 제거."""
    if not token:
        return []
    spans = [int(until_epoch) - int(since_epoch)]
    spans += [d * 86400 for d in PROBE_WINDOWS if d * 86400 < spans[0]]
    merged = {}
    for sp in spans:
        for e in _fetch_window(base_url, acc_id, token, int(until_epoch) - sp,
                               int(until_epoch), req_lib, log, max_pages):
            merged[(e["ts"], e["level"], e["obj_id"], e["old"], e["new"])] = e
    return sorted(merged.values(), key=lambda e: e["ts"])


def _fetch_window(base_url, acc_id, token, since_epoch, until_epoch,
                  req_lib, log=None, max_pages=300):
    """창 하나를 페이지 끝까지 훑어 예산 이벤트만 뽑는다(내부용)."""
    url = f"{base_url}/{acc_id}/activities"
    params = {
        "access_token": token,
        "since": int(since_epoch), "until": int(until_epoch),
        "limit": 500,   # 페이지 수 축소(창 확대 비용 완화). 데이터 동일.
        "fields": "event_type,event_time,object_id,extra_data,actor_name",
    }
    out = []
    page = 0
    while url and page < max_pages:
        try:
            r = req_lib.get(url, params=params if page == 0 else None, timeout=60)
            j = r.json()
        except Exception as e:
            if log:
                log.warning(f"  ⚠️ activities fetch 실패({acc_id}, p{page}): {e}")
            break
        if not isinstance(j, dict):
            break
        if j.get("error"):
            if log:
                log.warning(f"  ⚠️ activities API 오류({acc_id}): {j['error'].get('message')}")
            break
        for e in j.get("data", []):
            et = e.get("event_type", "")
            if et not in BUDGET_EVENT_TYPES:
                continue
            try:
                ed = json.loads(e.get("extra_data") or "{}")
            except Exception:
                continue
            ov, nv = ed.get("old_value"), ed.get("new_value")
            if not isinstance(ov, dict) or not isinstance(nv, dict):
                continue  # scheduling_state 등 금액 없는 예산 이벤트 제외
            old, new = _num(ov.get("old_value")), _num(nv.get("new_value"))
            if old is None or new is None:
                continue
            try:
                ts = datetime.strptime(e.get("event_time"), "%Y-%m-%dT%H:%M:%S%z").timestamp()
            except Exception:
                continue
            out.append({
                "ts": ts,
                "level": "adset" if et == "update_ad_set_budget" else "campaign",
                "obj_id": str(e.get("object_id") or ""),
                "old": old, "new": new,
                "actor": e.get("actor_name") or "",
            })
        url = j.get("paging", {}).get("next")
        params = None
        page += 1
    return out


class BudgetHistory:
    """예산 재구성기.
       add_events()로 이벤트를, set_adset_campaign()로 세트→캠페인 매핑을 채운 뒤
       raw_on(adset_id, 'YYYY-MM-DD', cur_raw)로 그 날짜의 예산 raw값을 얻는다."""

    def __init__(self, kst_tz):
        self.kst = kst_tz
        self.adset_ev = defaultdict(list)   # 세트id → [(ts, old, new)]
        self.camp_ev = defaultdict(list)    # 캠페인id → [(ts, old, new)]
        self.adset_camp = {}                # 세트id → 캠페인id (CBO 이벤트 적용용)
        self._merged_cache = {}
        self._dayend_cache = {}
        self._daystart_cache = {}

    def add_events(self, events):
        for e in events:
            if e["level"] == "adset":
                self.adset_ev[e["obj_id"]].append((e["ts"], e["old"], e["new"]))
            else:
                self.camp_ev[e["obj_id"]].append((e["ts"], e["old"], e["new"]))

    def set_adset_campaign(self, mapping):
        for k, v in (mapping or {}).items():
            if k and v:
                self.adset_camp[str(k)] = str(v)

    def has_events_for(self, adset_id):
        aid = str(adset_id)
        if self.adset_ev.get(aid):
            return True
        cid = self.adset_camp.get(aid)
        return bool(cid and self.camp_ev.get(cid))

    def has_event_on(self, adset_id, date_str):
        """그 세트에 'date_str(KST) 당일' 예산 변경 이벤트가 있었나.
           있으면 그 날의 종료시점 값(raw_on)이 확실한 정답이므로 저장값을 덮어써도 된다.
           없으면 파이프라인이 그날 찍어둔 스냅샷이 더 믿을 만하다 —
           activities 는 오래된 구간에서 이벤트를 누락하기 때문(모듈 docstring)."""
        evs = self._merged(str(adset_id))
        if not evs:
            return False
        d0 = self._day_start(date_str)
        de = self._day_end(date_str)
        return any(d0 <= ts <= de for ts, _o, _n in evs)

    def reliable_from(self, today):
        """'재구성이 완전하다'고 볼 수 있는 시작일(YYYY-MM-DD). today 는 date/datetime."""
        return (today - timedelta(days=RELIABLE_DAYS)).strftime("%Y-%m-%d")

    def _day_start(self, date_str):
        ts = self._daystart_cache.get(date_str)
        if ts is None:
            ts = datetime.strptime(date_str + " 00:00:00", "%Y-%m-%d %H:%M:%S") \
                .replace(tzinfo=self.kst).timestamp()
            self._daystart_cache[date_str] = ts
        return ts

    def _merged(self, adset_id):
        c = self._merged_cache.get(adset_id)
        if c is not None:
            return c
        evs = list(self.adset_ev.get(adset_id, []))
        cid = self.adset_camp.get(adset_id)
        if cid:
            evs += self.camp_ev.get(cid, [])
        evs.sort(key=lambda x: x[0])
        self._merged_cache[adset_id] = evs
        return evs

    def _day_end(self, date_str):
        ts = self._dayend_cache.get(date_str)
        if ts is None:
            ts = datetime.strptime(date_str + " 23:59:59", "%Y-%m-%d %H:%M:%S") \
                .replace(tzinfo=self.kst).timestamp()
            self._dayend_cache[date_str] = ts
        return ts

    def raw_on(self, adset_id, date_str, cur_raw):
        """date_str(KST 'YYYY-MM-DD') 종료 시점에 설정돼 있던 예산 raw값 반환.
           - 이벤트 없으면 cur_raw(현재값).
           - 해당일 이전 이벤트의 마지막 new(=그 날 유효값). 첫 이벤트보다 이전이면 first.old."""
        evs = self._merged(str(adset_id))
        if not evs:
            return cur_raw
        de = self._day_end(date_str)
        val = None
        for ts, old, new in evs:
            if ts <= de:
                val = new
            else:
                break
        if val is None:
            val = evs[0][1]  # 첫 이벤트의 old = 그 이전(=해당일) 예산
        return val
