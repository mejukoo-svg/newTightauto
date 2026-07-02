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
"""
import json
from collections import defaultdict
from datetime import datetime

BUDGET_EVENT_TYPES = ("update_ad_set_budget", "update_campaign_budget")


def _num(x):
    try:
        return int(round(float(x)))
    except Exception:
        return None


def fetch_budget_events(base_url, acc_id, token, since_epoch, until_epoch,
                        req_lib, log=None, max_pages=300):
    """계정의 예산 변경 이벤트를 [{ts, level, obj_id, old, new, actor} ...] 로 반환.
       level: 'adset'(update_ad_set_budget) | 'campaign'(update_campaign_budget).
       실패 시 [] (호출부에서 스냅샷 보존 폴백으로 자연 처리)."""
    if not token:
        return []
    url = f"{base_url}/{acc_id}/activities"
    params = {
        "access_token": token,
        "since": int(since_epoch), "until": int(until_epoch),
        "limit": 200,
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
