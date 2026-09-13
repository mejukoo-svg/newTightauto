# -*- coding: utf-8 -*-
"""
구글_DGTT_소재_supabase.py
==========================
구글 **디멘드젠** 중 캠페인명 또는 광고그룹(세트)명에 `DG_TT` 가 붙은 광고만 골라
**소재 내용 + 광고별 일자 성과 + 에셋별 일자 성과** 를 Supabase 3개 테이블에 upsert.

  1) google_dgtt_ad_creative  (ad_id)             — 소재 내용 스냅샷(항상 전 기간 전체 광고)
       헤드라인/롱헤드라인/설명/비즈니스명/CTA 텍스트, 유튜브 영상 id·제목, 로고 이미지 URL,
       최종 URL(+?ct=), 광고·세트·캠페인 상태, 정책 승인상태
  2) google_dgtt_ad_daily     (date, ad_id)       — 광고별 일자 성과(Ads API 기준)
       지출/노출/클릭/전환수/전환가치/참여수 + 영상 25·50·75·100% 시청률
  3) google_dgtt_asset_daily  (date, ad_id, asset_id) — 광고 안 에셋(헤드라인·영상·로고…)별 일자 성과
       field_type / performance_label(BEST·GOOD·LOW·LEARNING…) / 노출·클릭·지출

  · 계정 = KRW 운영계정(기본 5912047700 '사이버네틱스'), 구글_캠페인_supabase.py 와 동일 자격증명.
  · 필터 = campaign.advertising_channel_type='DEMAND_GEN' 을 GAQL 로 걸고,
    `DG_TT` 포함 여부는 클라이언트에서 판정(GAQL 은 campaign.name OR ad_group.name 을 못 쓴다).
  · 매출(Mixpanel utm_content 귀속)은 여기서 다루지 않는다 — 기존 google_demandgen_ad_daily 를
    ad_id 로 조인하면 된다(같은 [Tight] DG 광고를 적재하고 있음).
  · 영상 실물은 API 로 못 받는다 — youtube_video_id 만 저장(https://youtu.be/<id>).
    ※ 'metrics.video_views' 는 v24 ad_group_ad 에서 인식되지 않아 quartile 률만 저장.

기간/옵션 (다른 *_supabase.py 와 동일 규약):
  REFRESH_DAYS (기본 10) / FULL_REFRESH=true (2025-01-01부터)
  --dry      : Supabase 미적재, 집계만 출력
  --replace  : 기간 전체 삭제 후 재삽입 (daily 2개 테이블만; creative 는 항상 upsert)
  --from/--to YYYY-MM-DD : 기간 직접 지정(백필용)
  --no-assets : 에셋별 테이블 생략(빠른 실행)

자격증명(.env 또는 GitHub Secrets):
  G_ADS_DEV_TOKEN / G_ADS_CLIENT_ID / G_ADS_CLIENT_SECRET / G_ADS_REFRESH_TOKEN / G_ADS_LOGIN_ID
  G_ADS_CUSTOMER_ID(권장) / SUPABASE_URL / SUPABASE_SERVICE_KEY / SUPABASE_DB_SCHEMA

실행: py 구글_DGTT_소재_supabase.py [--dry] [--replace] [--from 2026-06-01 --to 2026-06-30]
의존성: pip install google-ads requests
"""

import os, re, sys, logging
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from urllib.parse import urlparse, parse_qs

import requests as req_lib

try:
    from google.ads.googleads.client import GoogleAdsClient
except ImportError:
    sys.stderr.write("\n❌ google-ads 라이브러리가 없습니다.  pip install google-ads\n\n")
    raise

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def _load_env():
    from pathlib import Path
    p = Path(__file__).parent / ".env"
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            val = v.strip().strip('"').strip("'")
            if val:
                os.environ.setdefault(k.strip(), val)


_load_env()

DRY = "--dry" in sys.argv
REPLACE = "--replace" in sys.argv
NO_ASSETS = "--no-assets" in sys.argv
TABLE_CREATIVE = "google_dgtt_ad_creative"
TABLE_DAILY    = "google_dgtt_ad_daily"
TABLE_ASSET    = "google_dgtt_asset_daily"

NAME_TAG = os.environ.get("G_DGTT_TAG", "DG_TT").upper()   # 캠페인/세트명 필터 토큰

DEFAULT_CURRENCY = os.environ.get("G_ADS_CURRENCY", "KRW").upper()
KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None).date()
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))


def _argdate(flag):
    if flag in sys.argv:
        try: return date.fromisoformat(sys.argv[sys.argv.index(flag) + 1])
        except Exception: raise SystemExit(f"❌ {flag} 뒤에 YYYY-MM-DD 를 주세요")
    return None


START = _argdate("--from") or (date(2025, 1, 1) if FULL_REFRESH else TODAY - timedelta(days=REFRESH_DAYS - 1))
END   = _argdate("--to") or TODAY
START_ISO, END_ISO = START.isoformat(), END.isoformat()

digits = lambda s: re.sub(r"\D", "", str(s or ""))


def is_target(campaign_name: str, ad_group_name: str) -> bool:
    """캠페인명 또는 광고그룹명에 DG_TT(대소문자 무시)가 있으면 수집 대상."""
    return NAME_TAG in f"{campaign_name or ''}\n{ad_group_name or ''}".upper()


def _ct_of(final_urls):
    """광고 최종 URL 의 ?ct=<콘텐츠> 토큰. 없으면 ''. (구글_디멘드젠_캠페인_supabase.py 와 동일)"""
    for u in final_urls or []:
        try:
            v = parse_qs(urlparse(u).query).get("ct", [""])[0]
            if v:
                return v
        except Exception:
            pass
    return ""


# ── 구글 Ads ────────────────────────────────────────────────────────────────
def build_client():
    cfg = {
        "developer_token": os.environ["G_ADS_DEV_TOKEN"].strip(),
        "client_id":       os.environ["G_ADS_CLIENT_ID"].strip(),
        "client_secret":   os.environ["G_ADS_CLIENT_SECRET"].strip(),
        "refresh_token":   os.environ["G_ADS_REFRESH_TOKEN"].strip(),
        "login_customer_id": digits(os.environ["G_ADS_LOGIN_ID"]),
        "use_proto_plus": True,
    }
    missing = [k for k, v in cfg.items() if k != "use_proto_plus" and not v]
    if missing:
        raise SystemExit(f"❌ 구글 Ads 자격증명 누락: {missing}")
    return GoogleAdsClient.load_from_dict(cfg)


def discover_customer_ids(client):
    forced = digits(os.environ.get("G_ADS_CUSTOMER_ID", ""))
    if forced:
        log.info(f"  🎯 지정 운영 계정 CID={forced}")
        return [forced]
    log.info(f"  🔍 MCC 하위 {DEFAULT_CURRENCY} 운영 계정 자동 탐색")
    mcc = digits(os.environ["G_ADS_LOGIN_ID"])
    ga = client.get_service("GoogleAdsService")
    q = ("SELECT customer_client.id, customer_client.descriptive_name, "
         "customer_client.currency_code, customer_client.manager, customer_client.status "
         "FROM customer_client WHERE customer_client.status='ENABLED'")
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


def _asset_id(resource_name: str) -> str:
    return resource_name.rsplit("/", 1)[-1] if resource_name else ""


def fetch_creatives(client, cid):
    """DG_TT 광고 전체(기간 무관)의 소재 내용. ad_id → dict. 자산 resource 는 id 만 남긴다."""
    ga = client.get_service("GoogleAdsService")
    q = """
        SELECT
          campaign.id, campaign.name, campaign.status,
          ad_group.id, ad_group.name, ad_group.status,
          ad_group_ad.status,
          ad_group_ad.policy_summary.approval_status,
          ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type, ad_group_ad.ad.final_urls,
          ad_group_ad.ad.demand_gen_video_responsive_ad.headlines,
          ad_group_ad.ad.demand_gen_video_responsive_ad.long_headlines,
          ad_group_ad.ad.demand_gen_video_responsive_ad.descriptions,
          ad_group_ad.ad.demand_gen_video_responsive_ad.business_name,
          ad_group_ad.ad.demand_gen_video_responsive_ad.call_to_actions,
          ad_group_ad.ad.demand_gen_video_responsive_ad.videos,
          ad_group_ad.ad.demand_gen_video_responsive_ad.logo_images,
          ad_group_ad.ad.demand_gen_multi_asset_ad.headlines,
          ad_group_ad.ad.demand_gen_multi_asset_ad.descriptions,
          ad_group_ad.ad.demand_gen_multi_asset_ad.business_name,
          ad_group_ad.ad.demand_gen_multi_asset_ad.call_to_action_text,
          ad_group_ad.ad.demand_gen_multi_asset_ad.marketing_images,
          ad_group_ad.ad.demand_gen_multi_asset_ad.square_marketing_images,
          ad_group_ad.ad.demand_gen_multi_asset_ad.portrait_marketing_images,
          ad_group_ad.ad.demand_gen_multi_asset_ad.logo_images
        FROM ad_group_ad
        WHERE campaign.advertising_channel_type = 'DEMAND_GEN'
    """
    out = {}
    total = 0
    for b in ga.search_stream(customer_id=cid, query=q):
        for r in b.results:
            total += 1
            if not is_target(r.campaign.name, r.ad_group.name):
                continue
            ad = r.ad_group_ad.ad
            atype = ad.type_.name
            if atype == "DEMAND_GEN_VIDEO_RESPONSIVE_AD":
                a = ad.demand_gen_video_responsive_ad
                texts = {
                    "headlines":      [t.text for t in a.headlines],
                    "long_headlines": [t.text for t in a.long_headlines],
                    "descriptions":   [t.text for t in a.descriptions],
                    "business_name":  a.business_name.text,
                    "call_to_actions": [c.call_to_action.name for c in a.call_to_actions],
                }
                video_ids = [_asset_id(v.asset) for v in a.videos]
                image_ids = []
                logo_ids  = [_asset_id(v.asset) for v in a.logo_images]
            elif atype == "DEMAND_GEN_MULTI_ASSET_AD":
                a = ad.demand_gen_multi_asset_ad
                texts = {
                    "headlines":      [t.text for t in a.headlines],
                    "long_headlines": [],
                    "descriptions":   [t.text for t in a.descriptions],
                    "business_name":  a.business_name,
                    "call_to_actions": [a.call_to_action_text] if a.call_to_action_text else [],
                }
                video_ids = []
                image_ids = [_asset_id(v.asset) for v in
                             list(a.marketing_images) + list(a.square_marketing_images) + list(a.portrait_marketing_images)]
                logo_ids  = [_asset_id(v.asset) for v in a.logo_images]
            else:
                texts = {"headlines": [], "long_headlines": [], "descriptions": [], "business_name": "", "call_to_actions": []}
                video_ids = image_ids = logo_ids = []
            final_urls = list(ad.final_urls)
            out[str(ad.id)] = {
                "ad_id": str(ad.id),
                "ad_name": ad.name,
                "ad_type": atype,
                "ad_status": r.ad_group_ad.status.name,
                "approval_status": r.ad_group_ad.policy_summary.approval_status.name,
                "ad_group_id": str(r.ad_group.id),
                "ad_group_name": r.ad_group.name,
                "ad_group_status": r.ad_group.status.name,
                "campaign_id": str(r.campaign.id),
                "campaign_name": r.campaign.name,
                "campaign_status": r.campaign.status.name,
                "final_url": final_urls[0] if final_urls else "",
                "ct": _ct_of(final_urls),
                **texts,
                "_video_ids": video_ids, "_image_ids": image_ids, "_logo_ids": logo_ids,
            }
    log.info(f"  🎨 디멘드젠 광고 {total}개 중 {NAME_TAG} 대상 {len(out)}개")
    return out


def fetch_assets(client, cid, asset_ids):
    """asset.id → {type, youtube_video_id, youtube_title, image_url, text, name}"""
    ga = client.get_service("GoogleAdsService")
    ids = sorted({i for i in asset_ids if i})
    out = {}
    for i in range(0, len(ids), 500):
        chunk = ids[i:i+500]
        q = f"""
            SELECT asset.id, asset.type, asset.name,
                   asset.youtube_video_asset.youtube_video_id, asset.youtube_video_asset.youtube_video_title,
                   asset.image_asset.full_size.url, asset.image_asset.full_size.width_pixels,
                   asset.image_asset.full_size.height_pixels, asset.text_asset.text
            FROM asset WHERE asset.id IN ({",".join(chunk)})
        """
        for b in ga.search_stream(customer_id=cid, query=q):
            for r in b.results:
                a = r.asset
                out[str(a.id)] = {
                    "type": a.type_.name, "name": a.name,
                    "youtube_video_id": a.youtube_video_asset.youtube_video_id,
                    "youtube_title": a.youtube_video_asset.youtube_video_title,
                    "image_url": a.image_asset.full_size.url,
                    "width": a.image_asset.full_size.width_pixels,
                    "height": a.image_asset.full_size.height_pixels,
                    "text": a.text_asset.text,
                }
    log.info(f"  🖼  에셋 메타 {len(out)}/{len(ids)}개 조회")
    return out


def fetch_ad_daily(client, cid, target_ad_ids):
    """(date, ad_id) → 광고별 일자 성과. 무활동 행은 버린다."""
    ga = client.get_service("GoogleAdsService")
    q = f"""
        SELECT segments.date, campaign.name, ad_group.name, ad_group_ad.ad.id,
               metrics.cost_micros, metrics.impressions, metrics.clicks,
               metrics.conversions, metrics.conversions_value, metrics.engagements,
               metrics.video_quartile_p25_rate, metrics.video_quartile_p50_rate,
               metrics.video_quartile_p75_rate, metrics.video_quartile_p100_rate
        FROM ad_group_ad
        WHERE campaign.advertising_channel_type = 'DEMAND_GEN'
          AND segments.date BETWEEN '{START_ISO}' AND '{END_ISO}'
    """
    rows = {}
    for b in ga.search_stream(customer_id=cid, query=q):
        for r in b.results:
            ad_id = str(r.ad_group_ad.ad.id)
            if ad_id not in target_ad_ids and not is_target(r.campaign.name, r.ad_group.name):
                continue
            m = r.metrics
            cost = m.cost_micros / 1e6
            if cost <= 0 and not m.impressions and not m.clicks:
                continue
            rows[(r.segments.date, ad_id)] = {
                "spend": round(cost, 2),
                "impressions": int(m.impressions or 0),
                "clicks": int(m.clicks or 0),
                "conversions": round(float(m.conversions or 0), 4),
                "conversions_value": round(float(m.conversions_value or 0), 2),
                "engagements": int(m.engagements or 0),
                "video_p25_rate": round(float(m.video_quartile_p25_rate or 0), 4),
                "video_p50_rate": round(float(m.video_quartile_p50_rate or 0), 4),
                "video_p75_rate": round(float(m.video_quartile_p75_rate or 0), 4),
                "video_p100_rate": round(float(m.video_quartile_p100_rate or 0), 4),
            }
    log.info(f"  📊 광고별 일자 성과 {len(rows)}행")
    return rows


def fetch_asset_daily(client, cid, target_ad_ids):
    """(date, ad_id, asset_id) → 에셋별 일자 성과 + field_type/performance_label."""
    ga = client.get_service("GoogleAdsService")
    q = f"""
        SELECT segments.date, campaign.name, ad_group.name, ad_group_ad.ad.id,
               ad_group_ad_asset_view.field_type, ad_group_ad_asset_view.performance_label,
               ad_group_ad_asset_view.enabled,
               asset.id, asset.type, asset.text_asset.text, asset.youtube_video_asset.youtube_video_id,
               metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions
        FROM ad_group_ad_asset_view
        WHERE campaign.advertising_channel_type = 'DEMAND_GEN'
          AND segments.date BETWEEN '{START_ISO}' AND '{END_ISO}'
    """
    rows = {}
    for b in ga.search_stream(customer_id=cid, query=q):
        for r in b.results:
            ad_id = str(r.ad_group_ad.ad.id)
            if ad_id not in target_ad_ids and not is_target(r.campaign.name, r.ad_group.name):
                continue
            m = r.metrics
            cost = m.cost_micros / 1e6
            if cost <= 0 and not m.impressions and not m.clicks:
                continue
            v = r.ad_group_ad_asset_view
            a = r.asset
            rows[(r.segments.date, ad_id, str(a.id))] = {
                "field_type": v.field_type.name,
                "performance_label": v.performance_label.name,
                "enabled": bool(v.enabled),
                "asset_type": a.type_.name,
                "asset_text": a.text_asset.text or "",
                "youtube_video_id": a.youtube_video_asset.youtube_video_id or "",
                "spend": round(cost, 2),
                "impressions": int(m.impressions or 0),
                "clicks": int(m.clicks or 0),
                "conversions": round(float(m.conversions or 0), 4),
            }
    log.info(f"  🧩 에셋별 일자 성과 {len(rows)}행")
    return rows


# ── Supabase ────────────────────────────────────────────────────────────────
class SupabaseClient:
    def __init__(self, url, key):
        clean = re.sub(r"[^\x20-\x7E]", "", url).strip().rstrip("/")
        if not clean.startswith("http"):
            clean = "https://" + clean
        self.base = clean
        self.headers = {
            "apikey": key.strip(),
            "Authorization": f"Bearer {key.strip()}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }
        _sc = os.environ.get("SUPABASE_DB_SCHEMA", "").strip()
        if _sc:
            self.headers["Accept-Profile"] = _sc
            self.headers["Content-Profile"] = _sc

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


def main():
    log.info(f"🎯 구글 디멘드젠 {NAME_TAG} 소재·성과 적재  {START_ISO} ~ {END_ISO}"
             f"{'  [DRY]' if DRY else ''}{'  [REPLACE]' if REPLACE else ''}")
    client = build_client()
    cids = discover_customer_ids(client)
    if not cids:
        raise SystemExit("❌ 운영 계정을 찾지 못했습니다.")

    creatives, ad_daily, asset_daily = {}, {}, {}
    for cid in cids:
        try:
            c = fetch_creatives(client, cid)
            creatives.update(c)
            ad_daily.update(fetch_ad_daily(client, cid, set(c.keys())))
            if not NO_ASSETS:
                asset_daily.update(fetch_asset_daily(client, cid, set(c.keys())))
        except Exception as e:
            log.error(f"  ❌ [{cid}] 조회 실패: {str(e)[:300]}")
            raise

    if not creatives:
        log.warning(f"  ⚠️ {NAME_TAG} 광고 없음 — 종료")
        return

    # 에셋 메타(유튜브 id·제목, 이미지 URL) 붙이기
    need = set()
    for c in creatives.values():
        need.update(c["_video_ids"]); need.update(c["_image_ids"]); need.update(c["_logo_ids"])
    assets = fetch_assets(client, cids[0], need) if need else {}

    now_iso = datetime.now(timezone.utc).isoformat()
    creative_records = []
    for c in creatives.values():
        vids = [assets.get(i, {}) for i in c["_video_ids"]]
        creative_records.append({
            k: v for k, v in c.items() if not k.startswith("_")
        } | {
            "videos": [{"asset_id": i, "youtube_video_id": a.get("youtube_video_id", ""),
                        "title": a.get("youtube_title", "")} for i, a in zip(c["_video_ids"], vids)],
            "images": [{"asset_id": i, "url": assets.get(i, {}).get("image_url", ""),
                        "w": assets.get(i, {}).get("width", 0), "h": assets.get(i, {}).get("height", 0)}
                       for i in c["_image_ids"]],
            "logos":  [{"asset_id": i, "url": assets.get(i, {}).get("image_url", "")} for i in c["_logo_ids"]],
            "youtube_video_id": next((a.get("youtube_video_id") for a in vids if a.get("youtube_video_id")), ""),
            "updated_at": now_iso,
        })

    daily_records = []
    for (d, ad_id), m in sorted(ad_daily.items()):
        c = creatives.get(ad_id, {})
        daily_records.append({
            "date": d, "ad_id": ad_id,
            "ad_group_id": c.get("ad_group_id", ""), "campaign_id": c.get("campaign_id", ""),
            "ad_name": c.get("ad_name", ""), **m,
        })

    asset_records = []
    for (d, ad_id, asset_id), m in sorted(asset_daily.items()):
        asset_records.append({"date": d, "ad_id": ad_id, "asset_id": asset_id, **m})

    # 요약
    by_ad = defaultdict(lambda: [0.0, 0.0, 0])
    for r in daily_records:
        o = by_ad[r["ad_id"]]; o[0] += r["spend"]; o[1] += r["conversions_value"]; o[2] += r["clicks"]
    log.info(f"  📦 소재 {len(creative_records)}개 / 광고일자 {len(daily_records)}행 / 에셋일자 {len(asset_records)}행")
    for ad_id, v in sorted(by_ad.items(), key=lambda z: -z[1][0])[:15]:
        c = creatives.get(ad_id, {})
        roas = (v[1] / v[0] * 100) if v[0] > 0 else 0
        log.info(f"     {c.get('ad_name','')[:38]:<38} 지출 {round(v[0]):>10,}  전환가치 {round(v[1]):>10,}"
                 f"  ROAS {roas:>5.0f}%  yt={c.get('_video_ids', [''])[:1]}  "
                 f"H={c.get('headlines', [''])[:1]}")

    if DRY:
        log.info("  🧪 --dry: 적재 생략")
        return
    sb = SupabaseClient(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    sb.upsert(TABLE_CREATIVE, creative_records)
    if REPLACE:
        sb.delete_range(TABLE_DAILY, START_ISO, END_ISO)
        if not NO_ASSETS:
            sb.delete_range(TABLE_ASSET, START_ISO, END_ISO)
    sb.upsert(TABLE_DAILY, daily_records)
    if not NO_ASSETS:
        sb.upsert(TABLE_ASSET, asset_records)
    log.info("✅ 완료")


if __name__ == "__main__":
    main()
