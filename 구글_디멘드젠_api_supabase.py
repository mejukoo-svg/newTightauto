# -*- coding: utf-8 -*-
"""
구글_디멘드젠_api_supabase.py
=============================
구글 Ads API 로 **디멘드젠 캠페인의 콘텐츠(ct)별 × 일자별 지출**을 직접 가져와
google_demandgen_content_spend_daily 테이블에 upsert.

  ※ 기존 구글_디멘드젠_예산_엑셀.py(바탕화면 "광고 보고서*.xlsx" 수동 다운로드)를
    대체한다. 출력 테이블/컬럼/집계 키가 동일하므로 index.html '🟢 구글 디멘드젠' 탭
    (밴스드 모드)은 수정 없이 그대로 동작.
    매출은 그대로 Mixpanel(구글_디멘드젠_mp_supabase.py)에서, 지출은 이 스크립트에서.
    index.html 이 (date, content) 로 두 소스를 조인해 ROAS/순이익 계산.

콘텐츠(ct) attribution 방식 (엑셀 보고서와 동일 근거):
  광고 최종 URL 에 ?ch=google&ct=<콘텐츠>&cr=<제작자> 파라미터가 박혀 있다.
  ad_group_ad 리소스의 ad.final_urls 에서 ct 를 파싱해 (date, ct) 로 비용/클릭/노출/전환 합산.
  - ch 가 google 이 아닌 다른 채널로 명시된 URL 은 제외 (ch 없으면 디멘드젠이므로 포함).
  - 디멘드젠 캠페인(advertising_channel_type IN DEMAND_GEN/DISCOVERY)만 조회.

자격증명 (구글 Ads API OAuth — 첨부 이미지의 키 네임 그대로):
  G_ADS_DEV_TOKEN       개발자 토큰
  G_ADS_CLIENT_ID       클라이언트 ID
  G_ADS_CLIENT_SECRET   클라이언트 보안 비밀
  G_ADS_REFRESH_TOKEN   리프레시 토큰
  G_ADS_LOGIN_ID        관리자(MCC) 계정 ID (하이픈 제외 숫자 10자리)
  G_ADS_CUSTOMER_ID     (권장) 실제 운영 계정 ID — 디멘드젠 캠페인이 있는 광고 계정.
                        미지정 시 MCC 하위에서 통화=KRW 인 운영(비매니저) 계정을 자동 탐색.

Supabase:
  SUPABASE_URL / SUPABASE_SERVICE_KEY

기간/옵션 (다른 *_supabase.py 와 동일 규약):
  REFRESH_DAYS (기본 10)  — 최근 N일
  FULL_REFRESH=true       — 2025-01-01 부터 전체
  --dry      : Supabase 미적재, 집계만 출력
  --replace  : 기간 전체 삭제 후 재삽입 (이 배치에 없는 콘텐츠 지출도 삭제)
               기본은 merge upsert (부분 조회로 돌려도 다른 콘텐츠 지출 보존)

실행: GitHub Actions(.github/workflows/supabase.yml 의 google-dg-spend job)에서
  매시 cron 자동 실행. G_ADS_* 시크릿이 repo Secrets 에 등록돼 있어야 함.
  (API 라 엑셀처럼 수동 다운로드가 필요 없어 cron 자동화 가능 — 교체의 핵심 이유.)

로컬 테스트:
  py 구글_디멘드젠_api_supabase.py            # 최근 10일, merge upsert
  py 구글_디멘드젠_api_supabase.py --dry
  FULL_REFRESH=true py 구글_디멘드젠_api_supabase.py --replace

의존성: pip install google-ads requests
"""

import os, re, sys, time, logging
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from urllib.parse import urlparse, parse_qs

import requests as req_lib

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
except ImportError:
    sys.stderr.write(
        "\n❌ google-ads 라이브러리가 없습니다.  pip install google-ads\n\n")
    raise

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

DRY = "--dry" in sys.argv
REPLACE = "--replace" in sys.argv

TABLE = "google_demandgen_content_spend_daily"

# 디멘드젠 콘텐츠 ct 는 'dg_' prefix. (sa_=검색광고 등 다른 채널 콘텐츠는 디멘드젠 캠페인
#  필터로 이미 걸러지지만, 혹시 섞여 들어와도 매출측(dg_만 표시)과 정합 위해 prefix 확인은 안 함:
#  지출 테이블은 디멘드젠 캠페인 전체 콘텐츠를 담고, index.html 이 dg_ 만 표시.)
DEFAULT_CURRENCY = os.environ.get("G_ADS_CURRENCY", "KRW").upper()

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))
START = date(2025, 1, 1) if FULL_REFRESH else TODAY - timedelta(days=REFRESH_DAYS - 1)
END = TODAY


# ───────────────────────── 구글 Ads 클라이언트 ─────────────────────────
def digits(s: str) -> str:
    return re.sub(r"\D", "", str(s or ""))


def build_client() -> GoogleAdsClient:
    cfg = {
        "developer_token": os.environ["G_ADS_DEV_TOKEN"].strip(),
        "client_id":       os.environ["G_ADS_CLIENT_ID"].strip(),
        "client_secret":   os.environ["G_ADS_CLIENT_SECRET"].strip(),
        "refresh_token":   os.environ["G_ADS_REFRESH_TOKEN"].strip(),
        "login_customer_id": digits(os.environ["G_ADS_LOGIN_ID"]),
        "use_proto_plus": True,
    }
    missing = [k for k in ("developer_token", "client_id", "client_secret",
                           "refresh_token", "login_customer_id") if not cfg.get(k)]
    if missing:
        raise SystemExit(f"❌ 구글 Ads 자격증명 누락: {missing}")
    return GoogleAdsClient.load_from_dict(cfg)


def discover_customer_ids(client) -> list[str]:
    """G_ADS_CUSTOMER_ID 지정 시 그것만. 미지정 시 MCC 하위에서
    통화=DEFAULT_CURRENCY 인 운영(비매니저, ENABLED) 계정을 자동 탐색."""
    forced = digits(os.environ.get("G_ADS_CUSTOMER_ID", ""))
    if forced:
        log.info(f"  🎯 지정 운영 계정 CID={forced}")
        return [forced]

    log.info(f"  🔍 G_ADS_CUSTOMER_ID 미지정 → MCC 하위 {DEFAULT_CURRENCY} 운영 계정 자동 탐색")
    mcc = digits(os.environ["G_ADS_LOGIN_ID"])
    ga = client.get_service("GoogleAdsService")
    q = """
        SELECT
          customer_client.id,
          customer_client.descriptive_name,
          customer_client.currency_code,
          customer_client.manager,
          customer_client.status
        FROM customer_client
        WHERE customer_client.status = 'ENABLED'
    """
    found = []
    for batch in ga.search_stream(customer_id=mcc, query=q):
        for row in batch.results:
            cc = row.customer_client
            if cc.manager:
                continue
            if cc.currency_code and cc.currency_code.upper() != DEFAULT_CURRENCY:
                continue
            found.append(str(cc.id))
            log.info(f"    · {cc.id}  {cc.descriptive_name!r}  {cc.currency_code}")
    if not found:
        log.warning("  ⚠️ 자동 탐색 결과 없음 — G_ADS_CUSTOMER_ID 를 직접 지정하세요.")
    return found


# ───────────────────────── ct 파싱 ─────────────────────────
def extract_ch_ct(final_urls):
    """final_urls(list) 중 ct 파라미터가 있는 첫 URL → (ch, ct).
       ch 가 google 아닌 다른 채널로 명시되면 (ch, None) 반환해 호출측이 스킵."""
    for u in final_urls:
        if not u:
            continue
        q = parse_qs(urlparse(str(u)).query)
        ch = (q.get("ch") or [""])[0].strip().lower()
        ct = (q.get("ct") or [""])[0].strip()
        if ct and ct.lower() not in ("undefined", "null", "none", "{ct}", "{content}"):
            return ch, ct
        if ch:  # ct 없지만 ch 만 있는 경우 — ch 만 반환
            return ch, None
    return "", None


# ───────────────────────── 디멘드젠 지출 조회 ─────────────────────────
def fetch_demandgen_spend(client, customer_id: str, agg: dict):
    """customer_id 의 디멘드젠 광고를 (date, ct) 로 비용/클릭/노출/전환 합산해 agg 에 누적."""
    ga = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          segments.date,
          ad_group_ad.ad.final_urls,
          metrics.cost_micros,
          metrics.clicks,
          metrics.impressions,
          metrics.conversions
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{START.isoformat()}' AND '{END.isoformat()}'
          AND campaign.advertising_channel_type = 'DEMAND_GEN'
    """
    # ※ 구 Discovery 캠페인은 Google Ads API v24 에서 enum 'DISCOVERY' 가 제거되고
    #   모두 DEMAND_GEN 으로 통합됨. 'DISCOVERY' 를 WHERE 에 넣으면 INVALID_ARGUMENT.
    rows = skipped_ch = no_ct = 0
    ch_seen = defaultdict(int)
    try:
        stream = ga.search_stream(customer_id=customer_id, query=query)
        for batch in stream:
            for row in batch.results:
                rows += 1
                d_iso = row.segments.date  # 계정 타임존(=KR 계정이면 KST) 기준 일자
                final_urls = list(row.ad_group_ad.ad.final_urls)
                ch, ct = extract_ch_ct(final_urls)
                if ch:
                    ch_seen[ch] += 1
                # 다른 채널로 명시된 URL 은 제외 (ch 없음=디멘드젠이므로 포함)
                if ch and ch != "google":
                    skipped_ch += 1
                    continue
                if not ct:
                    no_ct += 1
                    ct = "(미지정)"   # ct 없는 디멘드젠 지출도 버킷 보존
                m = row.metrics
                a = agg[(d_iso, ct)]
                a["spend"]       += m.cost_micros / 1_000_000.0
                a["clicks"]      += m.clicks
                a["impressions"] += m.impressions
                a["conversions"] += m.conversions
    except GoogleAdsException as e:
        log.error(f"  ❌ GoogleAdsException (CID={customer_id}): "
                  f"{e.error.code().name if hasattr(e,'error') else ''}")
        for err in getattr(e.failure, "errors", []):
            log.error(f"     · {err.message}")
        raise
    log.info(f"  📊 CID={customer_id}: 광고행 {rows} / ch≠google 스킵 {skipped_ch} / "
             f"ct미지정 {no_ct} / ch분포 {dict(sorted(ch_seen.items(), key=lambda x:-x[1])[:8])}")


# ───────────────────────── Supabase ─────────────────────────
class SupabaseClient:
    def __init__(self, url, key):
        clean = re.sub(r'[^\x20-\x7E]', '', url).strip().rstrip("/")
        if not clean.startswith("http"):
            clean = "https://" + clean
        self.base = clean
        self.headers = {
            "apikey": key.strip(),
            "Authorization": f"Bearer {key.strip()}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }

    def upsert(self, table, records, chunk=500):
        if not records:
            return 0
        url = f"{self.base}/rest/v1/{table}"
        ok = 0
        for i in range(0, len(records), chunk):
            batch = records[i:i+chunk]
            resp = req_lib.post(url, headers=self.headers, json=batch, timeout=60)
            if resp.status_code in (200, 201):
                ok += len(batch); log.info(f"  ✅ upsert {ok}/{len(records)} → {table}")
            else:
                log.error(f"  ❌ HTTP {resp.status_code}: {resp.text[:300]}")
        return ok

    def delete_range(self, table, date_from, date_to):
        url = f"{self.base}/rest/v1/{table}?date=gte.{date_from}&date=lte.{date_to}"
        try:
            resp = req_lib.delete(url, headers=self.headers, timeout=60)
            log.info(f"  🗑  {table} {date_from}~{date_to} 삭제 HTTP {resp.status_code}")
        except Exception as e:
            log.warning(f"  ⚠️ delete 예외: {e}")


# ───────────────────────── main ─────────────────────────
def main():
    log.info("=" * 60)
    log.info(f"🚀 구글 Ads API(디멘드젠) → Supabase ({TABLE})")
    log.info(f"   기간 {START} ~ {END}  | DRY={DRY} REPLACE={REPLACE}")
    log.info("=" * 60)

    client = build_client()
    cids = discover_customer_ids(client)
    if not cids:
        log.error("  ❌ 조회할 운영 계정이 없습니다."); return

    agg = defaultdict(lambda: {"spend": 0.0, "clicks": 0.0,
                               "impressions": 0.0, "conversions": 0.0})
    for cid in cids:
        fetch_demandgen_spend(client, cid, agg)

    if not agg:
        log.warning("  ⚠️ 디멘드젠 지출 집계 결과 없음 — 스킵 "
                    "(디멘드젠 캠페인 없음? 기간 밖? CID 확인)"); return

    dates = sorted({d for d, _ in agg})
    log.info(f"📦 (date,content)조합 {len(agg)}개 / 기간 {dates[0]}~{dates[-1]}")
    by_date = defaultdict(float)
    for (d, ct), v in agg.items():
        by_date[d] += v["spend"]
    for d in dates:
        log.info(f"   {d}: 지출 ₩{by_date[d]:,.0f}")
    by_ct = defaultdict(float)
    for (d, ct), v in agg.items():
        by_ct[ct] += v["spend"]
    log.info("  🏆 콘텐츠 Top10 (기간 합계 지출):")
    for ct, sp in sorted(by_ct.items(), key=lambda x: -x[1])[:10]:
        log.info(f"    · {ct[:42]:42s}  ₩{sp:>12,.0f}")

    records = []
    for (d_iso, ct), v in sorted(agg.items()):
        records.append({
            "date": d_iso,
            "content": ct[:300],
            "spend": round(v["spend"], 2),
            "clicks": int(round(v["clicks"])),
            "impressions": int(round(v["impressions"])),
            "conversions": round(v["conversions"], 2),
        })

    if DRY:
        log.info("  🟡 DRY RUN — Supabase 미적재. 샘플 5건:")
        for r in records[:5]:
            log.info(f"    {r}")
        return

    sb = SupabaseClient(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    # 기본은 merge upsert (부분 조회로 돌려도 다른 콘텐츠 지출 보존).
    # --replace 면 기간 전체 삭제 후 재삽입 (이 배치에 없는 콘텐츠 지출도 삭제됨).
    if REPLACE:
        log.warning("  🧨 --replace: 기간 전체 삭제 후 재삽입")
        sb.delete_range(TABLE, dates[0], dates[-1])
    sb.upsert(TABLE, records)
    log.info("✅ 완료 (지출 소스 = 구글 Ads API)")


if __name__ == "__main__":
    main()
