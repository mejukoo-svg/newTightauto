# -*- coding: utf-8 -*-
"""
_backfill_mp_kst.py — KST 윈도우 버그로 과소집계된 과거 Mixpanel 귀속분 백필.

이미 동결된 과거 날짜의 ad_performance_daily.revenue/results_mp 가 export 첫날
새벽 누락으로 ~25~35% 낮게 굳어있다. 이 스크립트는:
  - 기존 ad_performance_daily 행을 읽고(spend/budget/results_meta/memo/highlight 보존)
  - Mixpanel 을 [start-2, end] 로 다시 받아(버퍼 2일 → 모든 대상일 '내부') KST 재버킷
  - 프로덕션과 동일한 dedup + utm_term backfill + (date,utm_term) 집계
  - revenue/results_mp/profit/roas/cvr 만 재계산해 동일 형태로 upsert
  - ad_daily_product_summary 도 해당 날짜 재집계
기본은 dry-run (쓰기 없음). 실제 반영은 --apply.

사용:
  py _backfill_mp_kst.py 2026-04-01 2026-05-24            # dry-run
  py _backfill_mp_kst.py 2026-04-01 2026-05-24 --apply    # 실제 upsert
"""
import os, sys, io, importlib.util
from datetime import datetime, timedelta
from collections import defaultdict, OrderedDict

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
MODULE_PATH = os.path.join(HERE, "국내_세트별_supabase.py")
ENV_PATH = r"C:\Users\gram\meta_scraper\.env"

# ---- .env 로드 (모듈 import 전에 SUPABASE_URL 등 필수 env 주입) ----
def load_env(path):
    if not os.path.exists(path):
        sys.exit(f"❌ .env 없음: {path}")
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip(); v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)

load_env(ENV_PATH)

# ---- 프로덕션 모듈 import (fetch_mixpanel_data / clean_id / SupabaseClient 재사용) ----
spec = importlib.util.spec_from_file_location("kr_pipeline", MODULE_PATH)
P = importlib.util.module_from_spec(spec)
spec.loader.exec_module(P)

fetch_mixpanel_data = P.fetch_mixpanel_data
clean_id = P.clean_id
SupabaseClient = P.SupabaseClient
SB_URL = P.SUPABASE_URL
SB_KEY = P.SUPABASE_KEY

BUFFER_DAYS = 2

# 백필 대상에서 보존(읽어서 그대로 다시 씀)하는 컬럼 — 프로덕션 record 형태와 동일
PRESERVE_COLS = [
    "date", "adset_id", "campaign_name", "adset_name", "ad_account_id", "product",
    "spend", "cost_per_result", "purchase_roas_meta", "cpm", "reach", "impressions",
    "unique_clicks", "unique_ctr", "cost_per_click", "frequency", "results_meta", "budget",
]
READ_COLS = PRESERVE_COLS + ["results_mp", "revenue"]  # results_mp/revenue 는 비교용


def iso_to_dk(iso):
    p = iso.split("-")
    return f"{p[0][2:]}/{p[1]}/{p[2]}"  # 2026-05-14 -> 26/05/14


def fetch_existing(sb, start, end):
    rows, off = [], 0
    sel = ",".join(READ_COLS)
    while True:
        params = {"select": sel, "date": f"gte.{start}", "limit": "1000", "offset": str(off)}
        # date 두 조건은 params dict로 안 되므로 직접 URL
        url = f"{sb.base_url}/rest/v1/ad_performance_daily?select={sel}&date=gte.{start}&date=lte.{end}&order=date.asc&limit=1000&offset={off}"
        import requests
        r = requests.get(url, headers={**sb.headers, "Prefer": ""}, timeout=60)
        chunk = r.json()
        if not isinstance(chunk, list):
            sys.exit(f"❌ 읽기 실패: {chunk}")
        rows += chunk
        if len(chunk) < 1000:
            break
        off += 1000
    return rows


def _fetch_chunk_safe(ff, ct):
    """빈 결과=네트워크 실패로 간주(7~9일 윈도우엔 결제가 항상 수천 건).
    재시도 후에도 비면 RuntimeError 로 전체 중단 → 0으로 덮어쓰는 사고 방지."""
    import time as _t
    for attempt in range(5):
        data = fetch_mixpanel_data(ff, ct)
        if data:
            return data
        wait = 20 * (attempt + 1)
        print(f"  ⚠️ 청크 {ff}~{ct} 0건(실패 추정) — {wait}s 후 재시도 {attempt+1}/5")
        _t.sleep(wait)
    raise RuntimeError(f"Mixpanel 청크 {ff}~{ct} 가 반복 실패(빈 결과). 네트워크 확인 후 재실행 필요 — 중단.")


def build_mp_maps(start, end):
    """프로덕션과 동일 로직(순수 파이썬)으로 (dk, utm)->revenue/count 맵 구축."""
    # 7일 청크, 각 from_date 를 BUFFER_DAYS 앞당겨 fetch
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    mp_raw = []
    cs = s
    while cs <= e:
        ce = min(cs + timedelta(days=6), e)
        ff = (cs - timedelta(days=BUFFER_DAYS)).strftime("%Y-%m-%d")
        mp_raw.extend(_fetch_chunk_safe(ff, ce.strftime("%Y-%m-%d")))
        cs = ce + timedelta(days=1)

    # 1) dedup: insert_id 우선, 없으면 (date,distinct_id,서비스,utm_term,revenue)
    seen_iid, seen_no = set(), set()
    deduped = []
    for r in mp_raw:
        iid = str(r.get("insert_id") or "")
        if iid:
            if iid in seen_iid:
                continue
            seen_iid.add(iid)
        else:
            key = (r.get("date"), r.get("distinct_id"), r.get("서비스"), r.get("utm_term"), r.get("revenue"))
            if key in seen_no:
                continue
            seen_no.add(key)
        deduped.append(r)

    # 1.5) order_id 주문단위 dedup (결제완료/payment_complete 이중발화 방지)
    #   같은 order_id 그룹에서 utm_term-set & 최대 revenue 행 1건만 보존(귀속 유실 방지).
    by_oid = OrderedDict()
    no_oid = []
    for r in deduped:
        oid = str(r.get("order_id") or "").strip()
        if not oid:
            no_oid.append(r)
            continue
        cur = by_oid.get(oid)
        if cur is None:
            by_oid[oid] = r
            continue
        cur_has = bool(str(cur.get("utm_term") or ""))
        r_has = bool(str(r.get("utm_term") or ""))
        if (r_has and not cur_has) or (
            r_has == cur_has and float(r.get("revenue") or 0) > float(cur.get("revenue") or 0)
        ):
            by_oid[oid] = r
    deduped = list(by_oid.values()) + no_oid

    # 2) utm_term backfill: (date,distinct_id) 그룹의 첫 non-empty utm 으로 빈 값 채움
    bf_map = {}
    for r in deduped:
        ut = str(r.get("utm_term") or "")
        if ut:
            k = (r.get("date"), r.get("distinct_id"))
            if k not in bf_map:
                bf_map[k] = ut
    # 3) 채운 뒤 non-empty 만 집계
    value_map = defaultdict(float)
    count_map = defaultdict(int)
    for r in deduped:
        ut = str(r.get("utm_term") or "")
        if not ut:
            ut = bf_map.get((r.get("date"), r.get("distinct_id")), "")
        if not ut:
            continue
        d = r.get("date")
        if not d:
            continue
        value_map[(d, ut)] += float(r.get("revenue") or 0)
        count_map[(d, ut)] += 1
    return value_map, count_map, len(mp_raw), len(deduped)


def monday(iso):
    x = datetime.strptime(iso, "%Y-%m-%d")
    return (x - timedelta(days=x.weekday())).strftime("%Y-%m-%d")


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    apply = "--apply" in sys.argv
    force = "--force" in sys.argv  # only-raise 가드 우회: 하향(과대계상 보정)도 덮어씀
    if len(args) < 2:
        sys.exit("사용: py _backfill_mp_kst.py START END [--apply]")
    start, end = args[0], args[1]

    sb = SupabaseClient(SB_URL, SB_KEY)
    print(f"\n{'='*70}\n백필 {start} ~ {end}  (버퍼 {BUFFER_DAYS}일)  "
          f"mode={'APPLY' if apply else 'DRY-RUN'}{' +FORCE(하향허용)' if force else ''}\n{'='*70}")

    existing = fetch_existing(sb, start, end)
    print(f"기존 ad_performance_daily 행: {len(existing)}")

    value_map, count_map, raw_n, dd_n = build_mp_maps(start, end)
    print(f"Mixpanel fetch: {raw_n}건 → dedup {dd_n}건 → 귀속키 {len(count_map)}개\n")

    # 재계산 + 레코드 생성
    # ★ only-raise 가드: 재계산 results_mp 가 기존보다 클 때만 갱신(undercount 보정).
    #   기존이 더 크거나 같으면(이미 정상이거나 fetch 누락) 건드리지 않는다.
    to_update = []          # 실제 upsert 할(=상향된) ad_performance_daily 레코드
    effective = []          # 모든 행의 '최종' (date,product,spend,uc,revenue,mp) — 요약 재집계용
    per_day = defaultdict(lambda: {"rows": 0, "spend": 0.0, "old_rev": 0.0, "new_rev": 0.0,
                                   "old_mp": 0, "new_mp": 0, "meta": 0, "chg": 0})
    for row in existing:
        iso = row["date"]; dk = iso_to_dk(iso)
        asid = str(row["adset_id"])
        spend = float(row.get("spend") or 0)
        uc = float(row.get("unique_clicks") or 0)
        old_mp = int(row.get("results_mp") or 0)
        old_rev = float(row.get("revenue") or 0)
        mpc = int(count_map.get((dk, asid), 0))
        mpv = float(value_map.get((dk, asid), 0.0))

        raises = force or (mpc > old_mp)  # 평소 상향만; --force 시 하향(과대계상 보정)도 갱신
        if raises:
            revenue = mpv
            profit = revenue - spend
            roas = (revenue / spend * 100) if spend > 0 else 0
            cvr = (mpc / uc * 100) if uc > 0 and mpc > 0 else 0
            rec = {c: row.get(c) for c in PRESERVE_COLS}
            rec["results_mp"] = mpc
            rec["revenue"] = round(revenue, 2)
            rec["profit"] = round(profit, 2)
            rec["roas"] = round(roas, 2)
            rec["cvr"] = round(cvr, 4)
            to_update.append(rec)
            eff_rev, eff_mp = revenue, mpc
        else:
            eff_rev, eff_mp = old_rev, old_mp  # 보존

        effective.append({"date": iso, "product": row.get("product"), "spend": spend,
                          "unique_clicks": uc, "revenue": eff_rev, "results_mp": eff_mp})

        d = per_day[iso]
        d["rows"] += 1; d["spend"] += spend
        d["old_rev"] += old_rev; d["new_rev"] += eff_rev
        d["old_mp"] += old_mp; d["new_mp"] += eff_mp
        d["meta"] += int(row.get("results_meta") or 0)
        d["chg"] += 1 if raises else 0

    # 일별 before/after
    print(f"{'date':11} {'rows':>4} {'chg':>3} {'spend':>12} {'old_rev':>12} {'new_rev':>12} "
          f"{'oROAS':>6} {'nROAS':>6} {'old_mp':>6} {'new_mp':>6} {'meta':>6}")
    wk = defaultdict(lambda: {"spend": 0.0, "old_rev": 0.0, "new_rev": 0.0})
    tot = {"spend": 0.0, "old_rev": 0.0, "new_rev": 0.0}
    for iso in sorted(per_day):
        d = per_day[iso]
        oR = d["old_rev"]/d["spend"]*100 if d["spend"] else 0
        nR = d["new_rev"]/d["spend"]*100 if d["spend"] else 0
        flag = "" if d["chg"] else " ·"  # 변경 없는 날 표시
        print(f"{iso:11} {d['rows']:>4} {d['chg']:>3} {d['spend']:>12,.0f} {d['old_rev']:>12,.0f} {d['new_rev']:>12,.0f} "
              f"{oR:>5.0f}% {nR:>5.0f}% {d['old_mp']:>6} {d['new_mp']:>6} {d['meta']:>6}{flag}")
        w = wk[monday(iso)]
        w["spend"] += d["spend"]; w["old_rev"] += d["old_rev"]; w["new_rev"] += d["new_rev"]
        for k in tot: tot[k] += d[k]

    print(f"\n{'주(월~일)':11} {'spend':>14} {'old_rev→new_rev':>28} {'oROAS→nROAS':>16}")
    for mon in sorted(wk):
        w = wk[mon]
        oR = w["old_rev"]/w["spend"]*100 if w["spend"] else 0
        nR = w["new_rev"]/w["spend"]*100 if w["spend"] else 0
        print(f"{mon:11} {w['spend']:>14,.0f}   {w['old_rev']:>12,.0f} → {w['new_rev']:>12,.0f}   {oR:>5.0f}% → {nR:>5.0f}%")
    oRt = tot["old_rev"]/tot["spend"]*100 if tot["spend"] else 0
    nRt = tot["new_rev"]/tot["spend"]*100 if tot["spend"] else 0
    print(f"\n총합  지출 {tot['spend']:,.0f} | 매출 {tot['old_rev']:,.0f} → {tot['new_rev']:,.0f} "
          f"(+{tot['new_rev']-tot['old_rev']:,.0f}) | ROAS {oRt:.0f}% → {nRt:.0f}%")
    print(f"갱신 대상(상향) 행: {len(to_update)} / 전체 {len(existing)}  (· = 변경없는 날)")

    if not apply:
        print("\n[DRY-RUN] 쓰기 없음. 반영하려면 --apply 추가.")
        return

    if not to_update:
        print("\n갱신할 행이 없습니다(모두 이미 정상). 종료.")
        return

    # ---- 실제 upsert (상향된 행만) ----
    print(f"\n[APPLY] ad_performance_daily upsert {len(to_update)}행 (상향분만)...")
    n1 = sb.upsert("ad_performance_daily", to_update, chunk_size=500)
    print(f"  ✅ {n1}/{len(to_update)}")

    # ad_daily_product_summary 재집계 — '최종(effective)' 값 기준(상향분 반영 + 나머지 보존)
    #   단, 변경이 있었던 날짜만 다시 upsert (안 바뀐 날은 그대로 둠)
    changed_dates = {r["date"] for r in to_update}
    agg = defaultdict(lambda: {"spend": 0.0, "revenue": 0.0, "profit": 0.0, "results_mp": 0, "unique_clicks": 0})
    for r in effective:
        if r["date"] not in changed_dates:
            continue
        a = agg[(r["date"], r["product"])]
        a["spend"] += float(r["spend"] or 0); a["revenue"] += r["revenue"]
        a["profit"] += (r["revenue"] - float(r["spend"] or 0))
        a["results_mp"] += r["results_mp"]; a["unique_clicks"] += int(r.get("unique_clicks") or 0)
    agg_records = []
    for (d, p), v in agg.items():
        roas = (v["revenue"]/v["spend"]*100) if v["spend"] > 0 else 0
        cvr = (v["results_mp"]/v["unique_clicks"]*100) if v["unique_clicks"] > 0 and v["results_mp"] > 0 else 0
        agg_records.append({"date": d, "product": p, "spend": round(v["spend"], 2),
                            "revenue": round(v["revenue"], 2), "profit": round(v["profit"], 2),
                            "roas": round(roas, 2), "cvr": round(cvr, 4),
                            "results_mp": v["results_mp"], "unique_clicks": v["unique_clicks"]})
    print(f"[APPLY] ad_daily_product_summary upsert {len(agg_records)}행...")
    n2 = sb.upsert("ad_daily_product_summary", agg_records, chunk_size=500)
    print(f"  ✅ {n2}/{len(agg_records)}")
    print("\n완료.")


if __name__ == "__main__":
    main()
