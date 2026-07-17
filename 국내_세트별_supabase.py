# -*- coding: utf-8 -*-
"""
국내_세트별_supabase.py
======================
Meta Ads + Mixpanel → Supabase 직통 파이프라인

기존 국내_세트별.py의 스프레드시트 마스터탭 구조를 그대로 따르되,
Google Sheets 의존 없이 API → Supabase로 바로 적재.

GitHub Actions cron으로 하루 2회 실행.

환경변수:
  - META_TOKEN_1 / META_TOKEN_2  (Meta 광고 토큰)
  - MIXPANEL_PROJECT_ID / MIXPANEL_USERNAME / MIXPANEL_SECRET
  - SUPABASE_URL / SUPABASE_SERVICE_KEY
  - REFRESH_DAYS (기본 10)
  - FULL_REFRESH (true면 전체 기간)
"""

import os
import json
import time
import re
import math
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import requests as req_lib

from budget_history import fetch_budget_events, BudgetHistory

# =========================================================
# 로깅 설정
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# =========================================================
# 환경변수 로드
# =========================================================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

META_TOKEN_A = os.environ.get("META_TOKEN_1", "")
META_TOKEN_B = os.environ.get("META_TOKEN_2", "")

META_TOKENS = {
    "act_1270614404675034": META_TOKEN_A,
    "act_707835224206178": META_TOKEN_A,
    "act_1808141386564262": META_TOKEN_B,
}
META_TOKEN_DEFAULT = META_TOKEN_A

# 계정별 확정 통화 (Meta에서 직접 확인된 값 — 셋 다 KRW).
# 통화 오판(특히 KRW를 USD로 가정)은 지출에 환율(~1450)이 곱해져 ROAS 알람을
# 오발시킨다. API 조회가 실패해도 여기 값이 있으면 환율 오적용을 차단한다.
ACCOUNT_CURRENCY = {
    "act_1270614404675034": "KRW",  # 1분꿀잼썰
    "act_707835224206178": "KRW",   # 2비즈니스계정 hksong
    "act_1808141386564262": "KRW",  # 타이트사주3rd원화새계정
}
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
ALL_AD_ACCOUNTS = list(META_TOKENS.keys())

# 세트→캠페인 매핑 (fetch_adset_budgets 가 채움). activities 의 캠페인예산(CBO) 이벤트를
# 하위 세트에 적용할 때 사용.
ADSET_CAMPAIGN = {}

MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME = os.environ.get("MIXPANEL_USERNAME", "")
MIXPANEL_SECRET = os.environ.get("MIXPANEL_SECRET", "")
MIXPANEL_EVENT_NAMES = ["결제완료", "payment_complete"]

# =========================================================
# Meta 채널 판별 (utm_source 화이트리스트)
# =========================================================
# 타 채널(google/tiktok 등) 결제가 직전 Meta 방문에서 남은 stale utm_term(세트 id)을
# 그대로 달고 들어와 Meta 세트 매출로 잘못 합산되는 문제를 차단한다.
# → 마지막 터치가 Meta(ig/fb/an 등)인 결제만 세트에 귀속한다.
META_UTM_SOURCES = {"ig", "fb", "an", "msg", "instagram", "facebook", "threads", "th"}


def is_meta_source(src):
    s = str(src).strip().lower() if src is not None else ""
    if not s:
        return False
    if s in META_UTM_SOURCES:
        return True
    # 미치환 매크로/변형: ig_text_*, fb-SiteLink, {{site_source_name}} 등
    if s.startswith("ig") or s.startswith("fb") or "instagram" in s or "facebook" in s or "site_source_name" in s:
        return True
    return False

# 실행 설정
KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None)
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
FULL_REFRESH_START = datetime(2025, 1, 1)
REFRESH_DAYS = int(os.environ.get("REFRESH_DAYS", "10"))

if FULL_REFRESH:
    REFRESH_DAYS = (TODAY - FULL_REFRESH_START).days + 1
    log.info(f"🔥 FULL_REFRESH: {FULL_REFRESH_START:%Y-%m-%d} ~ 오늘 ({REFRESH_DAYS}일)")
else:
    log.info(f"🔄 일반 모드: 최근 {REFRESH_DAYS}일 갱신")

DATA_REFRESH_START = TODAY - timedelta(days=REFRESH_DAYS - 1)
FALLBACK_USD_KRW = 1450

# 병렬 워커 수
META_DATE_WORKERS = 3
META_ACCOUNT_WORKERS = 3
MIXPANEL_CHUNK_WORKERS = 1  # sequential to avoid 429
BUDGET_WORKERS = 3


# =========================================================
# 유틸리티
# =========================================================
def clean_id(val):
    """광고 세트 ID 정규화 (float→int 변환, 불필요 문자 제거)"""
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    if re.match(r"^\d+$", s):
        return s
    try:
        if ("E" in s or "e" in s) and re.match(r"^[\d.]+[eE][+\-]?\d+$", s):
            return str(int(Decimal(s)))
    except Exception:
        pass
    try:
        if re.match(r"^\d+\.\d+$", s):
            return str(int(Decimal(s)))
    except Exception:
        pass
    numeric_only = re.sub(r"[^0-9]", "", s)
    return numeric_only if numeric_only else s


def _strip_leading_emojis(text):
    i = 0
    while i < len(text):
        c = text[i]
        if (
            "\uAC00" <= c <= "\uD7A3"
            or "\u3131" <= c <= "\u3163"
            or c.isalnum()
            or c in ".%"
        ):
            break
        i += 1
    return text[i:].strip()


def extract_product(adset_name, campaign_name=""):
    """캠페인 이름에서 상품명 추출. 순수 숫자(날짜)는 건너뛰기."""
    for source in [campaign_name, adset_name]:
        if not source:
            continue
        cleaned = _strip_leading_emojis(str(source).strip())
        if not cleaned:
            continue
        tokens = re.split(r"[_\s\-/|,()\[\]]+", cleaned)
        for token in tokens:
            token = token.strip()
            if not token:
                continue
            # 순수 숫자(0626, 1003, 260213 등 날짜)는 건너뛰기
            if re.match(r"^\d+$", token):
                continue
            return token
    return "기타"


def get_token(acc_id):
    return META_TOKENS.get(acc_id, META_TOKEN_DEFAULT)


def make_date_key(dt):
    """datetime → 'YY/MM/DD' 형식"""
    return f"{dt.year % 100:02d}/{dt.month:02d}/{dt.day:02d}"


def make_iso_date(dt):
    """datetime → 'YYYY-MM-DD'"""
    return dt.strftime("%Y-%m-%d")


# =========================================================
# 환율
# =========================================================
def fetch_exchange_rates(start_date, end_date, currency="USD"):
    rates = {}
    try:
        import yfinance as yf

        pair = f"{currency}KRW=X"
        ticker = yf.Ticker(pair)
        hist = ticker.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=(end_date + timedelta(days=3)).strftime("%Y-%m-%d"),
        )
        if not hist.empty:
            for idx, row in hist.iterrows():
                dt = idx.to_pydatetime().replace(tzinfo=None)
                dk = make_date_key(dt)
                rates[dk] = round(float(row["Close"]), 2)
            log.info(f"  ✅ yfinance {currency}/KRW: {len(rates)}일")
    except Exception as e:
        log.warning(f"  ⚠️ yfinance 실패: {e}")

    if not rates:
        try:
            resp = req_lib.get(
                f"https://open.er-api.com/v6/latest/{currency}", timeout=10
            )
            if resp.status_code == 200:
                current_rate = resp.json().get("rates", {}).get("KRW", FALLBACK_USD_KRW)
                d = start_date
                while d <= end_date:
                    rates[make_date_key(d)] = round(float(current_rate), 2)
                    d += timedelta(days=1)
                log.info(f"  ✅ API 환율: ₩{current_rate:,.0f}")
        except Exception as e:
            log.warning(f"  ⚠️ 환율 API 실패: {e}")

    if not rates:
        log.warning(f"  ⚠️ 환율 조회 모두 실패 → 폴백 ₩{FALLBACK_USD_KRW:,}")
    return rates


def get_rate_for_date(rates, dk):
    if dk in rates:
        return rates[dk]
    if rates:
        sorted_keys = sorted(rates.keys())
        prev = [k for k in sorted_keys if k <= dk]
        if prev:
            return rates[prev[-1]]
        return rates[sorted_keys[0]]
    return FALLBACK_USD_KRW


# =========================================================
# 통화 감지
# =========================================================
def detect_account_currency(ad_account_id):
    # 확정 통화가 있으면 그대로 사용(이름만 best-effort 조회) → 조회 실패에 면역.
    forced = ACCOUNT_CURRENCY.get(ad_account_id)
    url = f"{META_BASE_URL}/{ad_account_id}"
    data = meta_api_get(url, {"fields": "currency,name"}, token=get_token(ad_account_id))
    name = data.get("name", ad_account_id) if data else ad_account_id
    if forced:
        if data and data.get("currency") and data.get("currency") != forced:
            log.warning(f"  ⚠️ {ad_account_id} 통화 불일치: API={data.get('currency')} ≠ 확정={forced} → 확정값 사용")
        return forced, name
    if data:
        return data.get("currency", "KRW"), name
    # 조회 실패 시 국내 파이프라인 안전 기본값은 KRW.
    # (USD로 가정하면 원화 지출에 환율 ~1450배가 곱해져 ROAS 알람을 오발시킨다.)
    log.warning(f"  ⚠️ 통화 조회 실패 {ad_account_id} → KRW 가정(USD 오적용 방지)")
    return "KRW", ad_account_id


# =========================================================
# Meta Ads API
# =========================================================
def meta_api_get(url, params=None, token=None):
    if params is None:
        params = {}
    params["access_token"] = token or META_TOKEN_DEFAULT

    for attempt in range(5):
        try:
            resp = req_lib.get(url, params=params, timeout=120)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 400:
                err = resp.json().get("error", {})
                log.error(f"  ❌ Meta 400: {err.get('message', resp.text[:200])}")
                return None
            if resp.status_code in [429, 500, 502, 503]:
                wait = 30 + attempt * 30
                log.warning(f"  ⏳ Meta {resp.status_code}, {wait}초 대기 ({attempt+1}/5)")
                time.sleep(wait)
            else:
                log.error(f"  ❌ Meta {resp.status_code}: {resp.text[:200]}")
                if attempt < 4:
                    time.sleep(15)
                else:
                    return None
        except Exception as e:
            log.error(f"  ❌ Meta 요청 오류: {e}")
            if attempt < 4:
                time.sleep(15)
            else:
                return None
    return None


def _extract_action_value(action_list, types):
    if not action_list:
        return 0
    for a in action_list:
        if a.get("action_type", "") in types:
            try:
                return float(a.get("value", 0))
            except Exception:
                return 0
    return 0


def _extract_action_window(action_list, types, window_key):
    """특정 어트리뷰션 윈도우(예: '7d_click') 값만 추출. 뷰스루/클릭 분리용."""
    if not action_list:
        return 0
    for a in action_list:
        if a.get("action_type", "") in types:
            try:
                return float(a.get(window_key, 0) or 0)
            except Exception:
                return 0
    return 0


def fetch_meta_insights_daily(ad_account_id, single_date):
    """단일 계정/단일 날짜의 adset-level 인사이트"""
    url = f"{META_BASE_URL}/{ad_account_id}/insights"
    fields = (
        "campaign_name,adset_name,adset_id,spend,cpm,reach,impressions,frequency,"
        "actions,cost_per_action_type,purchase_roas,"
        "unique_outbound_clicks,unique_outbound_clicks_ctr,cost_per_unique_outbound_click"
    )
    params = {
        "fields": fields,
        "level": "adset",
        "time_increment": 1,
        "time_range": json.dumps({"since": single_date, "until": single_date}),
        # 계정 기본 윈도우와 동일(7일클릭+1일조회) → actions.value(=results_meta)는 불변,
        # 추가로 7d_click 키가 붙어 클릭만 구매수를 분리 추출 가능.
        "action_attribution_windows": json.dumps(["7d_click", "1d_view"]),
        "limit": 500,
        "filtering": json.dumps(
            [{"field": "spend", "operator": "GREATER_THAN", "value": "0"}]
        ),
    }

    all_results = []
    data = meta_api_get(url, params, token=get_token(ad_account_id))
    while data:
        all_results.extend(data.get("data", []))
        next_url = data.get("paging", {}).get("next")
        if next_url:
            time.sleep(1)
            try:
                resp = req_lib.get(next_url, timeout=120)
                data = resp.json() if resp.status_code == 200 else None
            except Exception:
                data = None
        else:
            break
    return all_results


def parse_insights(rows, date_str, date_obj, ad_account_id=""):
    """Meta API 응답 → 정규화된 dict 리스트"""
    purchase_types = [
        "purchase",
        "omni_purchase",
        "offsite_conversion.fb_pixel_purchase",
    ]
    outbound_types = ["outbound_click"]
    parsed = []

    for row in rows:
        spend = float(row.get("spend", 0))
        cpm = float(row.get("cpm", 0))
        reach = int(float(row.get("reach", 0)))
        impressions = int(float(row.get("impressions", 0)))
        frequency = float(row.get("frequency", 0))

        actions = row.get("actions", [])
        results = _extract_action_value(actions, purchase_types)
        results_click = _extract_action_window(actions, purchase_types, "7d_click")

        cost_per_action = row.get("cost_per_action_type", [])
        cost_per_result = _extract_action_value(cost_per_action, purchase_types)

        uo = row.get("unique_outbound_clicks", [])
        unique_clicks = _extract_action_value(uo, outbound_types)

        uc = row.get("unique_outbound_clicks_ctr", [])
        unique_ctr = _extract_action_value(uc, outbound_types)

        cpu = row.get("cost_per_unique_outbound_click", [])
        cost_per_click = _extract_action_value(cpu, outbound_types)

        # purchase_roas from Meta
        roas_list = row.get("purchase_roas", [])
        meta_roas = _extract_action_value(roas_list, purchase_types)

        parsed.append(
            {
                "campaign_name": row.get("campaign_name", ""),
                "adset_name": row.get("adset_name", ""),
                "adset_id": row.get("adset_id", ""),
                "ad_account_id": ad_account_id,
                "spend": spend,
                "cost_per_result": cost_per_result,
                "meta_roas": meta_roas,
                "cpm": cpm,
                "reach": reach,
                "impressions": impressions,
                "unique_clicks": unique_clicks,
                "unique_ctr": unique_ctr,
                "cost_per_click": cost_per_click,
                "frequency": frequency,
                "results_meta": results,
                "results_meta_click": results_click,
                "date_obj": date_obj,
                "date_key": date_str,
            }
        )
    return parsed


def fetch_adset_budgets(ad_account_id):
    """광고 세트별 일 예산 조회 (ASC 캠페인 예산 폴백 포함).
       부수효과: ADSET_CAMPAIGN(세트→캠페인) 맵을 채운다 → activities CBO 이벤트 적용용."""
    url = f"{META_BASE_URL}/{ad_account_id}/adsets"
    params = {
        "fields": "id,daily_budget,campaign_id",
        "limit": 500,
        "filtering": json.dumps(
            [{"field": "effective_status", "operator": "IN", "value": ["ACTIVE"]}]
        ),
    }

    adset_results = {}
    needs_campaign = {}

    data = meta_api_get(url, params, token=get_token(ad_account_id))
    while data:
        for row in data.get("data", []):
            asid = row.get("id", "")
            budget = row.get("daily_budget", "0")
            campaign_id = row.get("campaign_id", "")
            if not asid:
                continue
            if campaign_id:
                ADSET_CAMPAIGN[asid] = campaign_id
            try:
                budget_int = int(float(budget)) if budget else 0
            except Exception:
                budget_int = 0
            if budget_int > 0:
                adset_results[asid] = budget_int
            else:
                adset_results[asid] = 0
                if campaign_id:
                    needs_campaign[asid] = campaign_id
        next_url = data.get("paging", {}).get("next")
        if next_url:
            time.sleep(1)
            try:
                resp = req_lib.get(next_url, timeout=120)
                data = resp.json() if resp.status_code == 200 else None
            except Exception:
                data = None
        else:
            break

    # ASC 캠페인 예산 폴백
    if needs_campaign:
        unique_campaigns = set(needs_campaign.values())
        campaign_budgets = {}
        for cid in unique_campaigns:
            try:
                camp_data = meta_api_get(
                    f"{META_BASE_URL}/{cid}",
                    {"fields": "id,daily_budget"},
                    token=get_token(ad_account_id),
                )
                if camp_data:
                    cb = camp_data.get("daily_budget", "0")
                    campaign_budgets[cid] = int(float(cb)) if cb else 0
                time.sleep(0.5)
            except Exception:
                pass
        for asid, cid in needs_campaign.items():
            camp_budget = campaign_budgets.get(cid, 0)
            if camp_budget > 0:
                adset_results[asid] = camp_budget

    return adset_results


# =========================================================
# Mixpanel
# =========================================================
def fetch_mixpanel_data(from_date, to_date):
    url = "https://data.mixpanel.com/api/2.0/export"
    params = {
        "from_date": from_date,
        "to_date": to_date,
        "event": json.dumps(MIXPANEL_EVENT_NAMES),
        "project_id": MIXPANEL_PROJECT_ID,
    }
    log.info(f"  📡 Mixpanel: {from_date} ~ {to_date}")

    for attempt in range(4):
        try:
            resp = req_lib.get(
                url, params=params, auth=(MIXPANEL_USERNAME, MIXPANEL_SECRET), timeout=300
            )
            if resp.status_code == 429:
                wait = 30 + attempt * 30
                log.warning(f"  ⏳ Mixpanel 429 rate limit → {wait}초 대기 ({attempt+1}/4)")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                log.error(f"  ❌ Mixpanel {resp.status_code}: {resp.text[:300]}")
                return []

            lines = [l for l in resp.text.split("\n") if l.strip()]
            log.info(f"  📊 이벤트: {len(lines)}건")

            data = []
            for line in lines:
                try:
                    ev = json.loads(line)
                    props = ev.get("properties", {})
                    ts = props.get("time", 0)

                    if ts:
                        dt_kst = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=9)
                        ds = f"{dt_kst.year % 100:02d}/{dt_kst.month:02d}/{dt_kst.day:02d}"
                    else:
                        ds = None

                    ut = None
                    for k in ["utm_term", "UTM_Term", "UTM Term"]:
                        if k in props and props[k]:
                            ut = clean_id(str(props[k]).strip())
                            break

                    # 채널 판별용 utm_source (Meta 결제만 귀속하기 위함)
                    us = ""
                    for k in ["utm_source", "UTM_Source", "UTM Source"]:
                        if k in props and props[k]:
                            us = str(props[k]).strip()
                            break

                    raw_amount = props.get("amount") or props.get("결제금액")
                    raw_value = props.get("value")

                    amount_val = 0.0
                    if raw_amount is not None:
                        try:
                            amount_val = float(raw_amount)
                        except Exception:
                            pass

                    value_val = 0.0
                    if raw_value is not None:
                        try:
                            value_val = float(raw_value)
                        except Exception:
                            pass

                    revenue = amount_val if amount_val > 0 else (value_val if value_val > 0 else 0.0)

                    data.append({
                        "distinct_id": props.get("distinct_id"),
                        "date": ds,
                        "ts": int(ts) if ts else 0,
                        "utm_term": ut or "",
                        "utm_source": us or "",
                        "amount": amount_val,
                        "value_raw": value_val,
                        "revenue": revenue,
                        "서비스": props.get("서비스", ""),
                        "insert_id": props.get("$insert_id") or props.get("insert_id") or "",
                        "order_id": props.get("order_id") or "",
                    })
                except Exception:
                    pass

            log.info(f"  ✅ 파싱: {len(data)}건")
            return data
        except Exception as e:
            log.error(f"  ❌ Mixpanel 오류: {e}")
            return []
    log.error(f"  ❌ Mixpanel 재시도 소진: {from_date}~{to_date}")
    return []


# =========================================================
# Supabase 클라이언트
# =========================================================
class SupabaseClient:
    """supabase-py 없이 REST API 직접 호출 (의존성 최소화)"""

    def __init__(self, url, key):
        # 보이지 않는 문자 제거 (BOM, zero-width space, 줄바꿈 등)
        clean_url = re.sub(r'[^\x20-\x7E]', '', url).strip().rstrip("/")
        if not clean_url.startswith("http"):
            clean_url = f"https://{clean_url}"
        self.base_url = clean_url
        self.key = key.strip()
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }
        # new-tightauto: SUPABASE_DB_SCHEMA 설정 시에만 스키마 프로파일 헤더 (미설정=기존 public)
        _sc = os.environ.get('SUPABASE_DB_SCHEMA', '').strip()
        if _sc:
            self.headers['Accept-Profile'] = _sc
            self.headers['Content-Profile'] = _sc
        log.info(f"  🔗 Supabase URL length={len(self.base_url)}, repr={repr(self.base_url[:50])}")

    def _test_connection(self):
        """연결 테스트 (빈 SELECT)"""
        url = f"{self.base_url}/rest/v1/ad_performance_daily?limit=1"
        log.info(f"  🧪 연결 테스트: {url[:60]}...")
        try:
            resp = req_lib.get(url, headers={**self.headers, "Prefer": ""}, timeout=15)
            log.info(f"  🧪 응답: {resp.status_code} {resp.text[:200]}")
            return resp.status_code in [200, 406]
        except Exception as e:
            log.error(f"  ❌ 연결 테스트 실패: {e}")
            return False

    def _sanitize(self, records):
        """numpy/pandas 타입 → Python 기본 타입 변환"""
        clean = []
        for rec in records:
            row = {}
            for k, v in rec.items():
                if hasattr(v, 'item'):
                    v = v.item()
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    v = 0
                row[k] = v
            clean.append(row)
        return clean

    def upsert(self, table, records, chunk_size=500):
        """records를 chunk_size씩 나눠 upsert"""
        url = f"{self.base_url}/rest/v1/{table}"
        total = len(records)
        success = 0

        # 첫 번째 레코드 디버깅
        if records:
            sample = self._sanitize(records[:1])[0]
            log.info(f"  🔍 샘플 레코드 키: {list(sample.keys())}")
            log.info(f"  🔍 샘플 date={sample.get('date')}, adset_id={sample.get('adset_id','')[:10]}...")

        for i in range(0, total, chunk_size):
            chunk = self._sanitize(records[i : i + chunk_size])
            try:
                resp = req_lib.post(url, headers=self.headers, json=chunk, timeout=60)
                if resp.status_code in [200, 201]:
                    success += len(chunk)
                    log.info(f"  ✅ upsert {success}/{total}")
                else:
                    log.error(
                        f"  ❌ upsert 실패 (행 {i}~{i+len(chunk)}): "
                        f"HTTP {resp.status_code} | {resp.text[:500]}"
                    )
                    # 첫 실패 시 단건 테스트
                    if i == 0:
                        log.info("  🧪 단건 테스트...")
                        try:
                            test_resp = req_lib.post(url, headers=self.headers, json=[chunk[0]], timeout=15)
                            log.info(f"  🧪 단건 결과: HTTP {test_resp.status_code} | {test_resp.text[:500]}")
                        except Exception as te:
                            log.error(f"  🧪 단건 예외: {te}")
            except Exception as e:
                log.error(f"  ❌ upsert 예외 (행 {i}~{i+len(chunk)}): {type(e).__name__}: {e}")
            time.sleep(0.5)

        return success

    def query(self, table, params=None):
        """간단한 SELECT 쿼리"""
        url = f"{self.base_url}/rest/v1/{table}"
        headers = {**self.headers, "Prefer": ""}
        try:
            resp = req_lib.get(url, headers=headers, params=params or {}, timeout=30)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            log.error(f"  ❌ 쿼리 오류: {e}")
        return []


# =========================================================
# 메인 파이프라인
# =========================================================
def main():
    log.info("=" * 60)
    log.info("🚀 Meta + Mixpanel → Supabase 직통 파이프라인")
    log.info("=" * 60)
    log.info(f"📅 오늘: {TODAY:%Y-%m-%d} | 갱신: {DATA_REFRESH_START:%Y-%m-%d} ~ 오늘 ({REFRESH_DAYS}일)")
    log.info(f"🔑 Meta TOKEN_1: {'✅' if META_TOKEN_A else '❌'} | TOKEN_2: {'✅' if META_TOKEN_B else '❌'}")
    log.info(f"🔑 Mixpanel: {'✅' if MIXPANEL_USERNAME else '❌'} | Supabase: {'✅' if SUPABASE_KEY else '❌'}")

    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    if not sb._test_connection():
        log.error("❌ Supabase 연결 실패 — URL/KEY 확인 필요")
        log.error(f"   URL repr: {repr(SUPABASE_URL[:50])}")
        log.error(f"   KEY length: {len(SUPABASE_KEY)}")

    # =======================================================
    # 1) 통화 감지 + 환율
    # =======================================================
    log.info("\n1단계: 통화 감지 + 환율")
    account_currency = {}
    has_usd = False
    for acc_id in ALL_AD_ACCOUNTS:
        currency, acc_name = detect_account_currency(acc_id)
        account_currency[acc_id] = currency
        if currency != "KRW":
            has_usd = True
        log.info(f"  💱 {acc_id[-6:]}: {currency} ({acc_name})")
        time.sleep(0.5)

    usd_krw_rates = {}
    if has_usd:
        usd_krw_rates = fetch_exchange_rates(
            DATA_REFRESH_START - timedelta(days=7), TODAY
        )

    def get_fx(acc_id, dk):
        if account_currency.get(acc_id, "KRW") == "KRW":
            return 1.0
        return get_rate_for_date(usd_krw_rates, dk)

    def get_budget_divisor(acc_id):
        return 100 if account_currency.get(acc_id, "KRW") != "KRW" else 1

    # =======================================================
    # 2) Meta Ads 수집 (병렬: 날짜 × 계정)
    # =======================================================
    log.info(f"\n2단계: Meta Insights 수집 ({REFRESH_DAYS}일 × {len(ALL_AD_ACCOUNTS)}계정)")

    def _fetch_account(acc_id, target_str, dk, target_date):
        rows = fetch_meta_insights_daily(acc_id, target_str)
        if rows:
            return parse_insights(rows, dk, target_date, ad_account_id=acc_id)
        return []

    def _fetch_date(day_offset):
        td = TODAY - timedelta(days=day_offset)
        target_str = td.strftime("%Y-%m-%d")
        dk = make_date_key(td)
        day_rows = []
        with ThreadPoolExecutor(max_workers=META_ACCOUNT_WORKERS) as pool:
            futs = {
                pool.submit(_fetch_account, acc, target_str, dk, td): acc
                for acc in ALL_AD_ACCOUNTS
            }
            for f in as_completed(futs):
                try:
                    day_rows.extend(f.result())
                except Exception as e:
                    log.error(f"  ❌ {dk} 계정 오류: {e}")
        return dk, td, day_rows

    meta_date_data = defaultdict(list)
    adset_to_account = {}

    with ThreadPoolExecutor(max_workers=META_DATE_WORKERS) as pool:
        futs = {
            pool.submit(_fetch_date, d): d for d in range(REFRESH_DAYS)
        }
        for f in as_completed(futs):
            try:
                dk, td, day_rows = f.result()
                if day_rows:
                    meta_date_data[dk] = day_rows
                    log.info(f"  📊 {dk}: {len(day_rows)}건")
                    for mr in day_rows:
                        if mr["adset_id"] and mr["ad_account_id"]:
                            adset_to_account[mr["adset_id"]] = mr["ad_account_id"]
            except Exception as e:
                log.error(f"  ❌ 날짜 오류: {e}")

    total_meta = sum(len(v) for v in meta_date_data.values())
    log.info(f"✅ Meta 완료: {len(meta_date_data)}일, {total_meta}건")

    # Meta rate limit 쿨다운
    log.info("⏳ Meta rate limit 쿨다운 60초...")
    time.sleep(60)

    # =======================================================
    # 3) 예산 조회 (병렬)
    # =======================================================
    log.info(f"\n3단계: 예산 조회")
    budget_map = {}

    with ThreadPoolExecutor(max_workers=BUDGET_WORKERS) as pool:
        futs = {
            pool.submit(fetch_adset_budgets, acc): acc for acc in ALL_AD_ACCOUNTS
        }
        for f in as_completed(futs):
            try:
                budget_map.update(f.result())
            except Exception as e:
                log.error(f"  ❌ 예산 오류: {e}")

    log.info(f"✅ 예산: {len(budget_map)}개 세트")

    # =======================================================
    # 3.5) 예산 변경이력(activities) → 일자별 예산 재구성기
    #   현재값만 조회하면 최근 예산이 평탄화돼 증감액 테두리가 사라진다.
    #   activities 로 '그 날짜에 설정돼 있던 실제 예산'을 복원한다(ABO+CBO).
    # =======================================================
    log.info(f"\n3.5단계: 예산 변경이력(activities) 수집")
    bud_hist = BudgetHistory(KST)
    bud_hist.set_adset_campaign(ADSET_CAMPAIGN)
    _act_since = (DATA_REFRESH_START - timedelta(days=2)).replace(tzinfo=KST).timestamp()
    _act_until = (TODAY + timedelta(days=1)).replace(tzinfo=KST).timestamp()
    _act_total = 0
    for acc in ALL_AD_ACCOUNTS:
        evs = fetch_budget_events(META_BASE_URL, acc, get_token(acc),
                                  _act_since, _act_until, req_lib, log)
        bud_hist.add_events(evs)
        _act_total += len(evs)
        log.info(f"  📈 {acc}: 예산변경 {len(evs)}건")
    log.info(f"✅ 예산 변경이력: 총 {_act_total}건 (세트예산 {sum(len(v) for v in bud_hist.adset_ev.values())} · 캠페인예산 {sum(len(v) for v in bud_hist.camp_ev.values())})")

    # =======================================================
    # 4) Mixpanel 수집
    # =======================================================
    log.info(f"\n4단계: Mixpanel 수집 ({REFRESH_DAYS}일)")
    YESTERDAY = TODAY - timedelta(days=1)
    mp_raw = []

    # ★ KST 경계 보정 버퍼:
    #   Mixpanel raw export 는 from_date 를 프로젝트(UTC) 날짜로 필터하지만,
    #   parse 단계에서 이벤트 time 을 KST(UTC+9)로 재버킷팅한다. 그래서 fetch
    #   윈도우의 '첫 KST 날짜'는 새벽분(KST 00:00~09:00 = 전날 UTC)이 from_date
    #   미만으로 빠져 ~25~35% 과소집계된다. from_date 를 MP_FETCH_BUFFER_DAYS 만큼
    #   앞당겨 실제 시작일을 '내부 날짜'로 만든다. records 는 meta 윈도우
    #   (>= DATA_REFRESH_START) 날짜에만 생성되고, 청크 겹침 중복은 insert_id
    #   기준 dedup(아래)으로 제거되므로 안전하다.
    MP_FETCH_BUFFER_DAYS = 2

    if REFRESH_DAYS > 14:
        # 7일 단위 청크 (각 청크 from_date 를 버퍼만큼 앞당겨 겹쳐 fetch)
        chunks = []
        chunk_start = DATA_REFRESH_START
        while chunk_start <= YESTERDAY:
            chunk_end = min(chunk_start + timedelta(days=6), YESTERDAY)
            fetch_from = (chunk_start - timedelta(days=MP_FETCH_BUFFER_DAYS)).strftime("%Y-%m-%d")
            chunks.append((fetch_from, chunk_end.strftime("%Y-%m-%d")))
            chunk_start = chunk_end + timedelta(days=1)

        with ThreadPoolExecutor(max_workers=MIXPANEL_CHUNK_WORKERS) as pool:
            futs = {
                pool.submit(fetch_mixpanel_data, c_from, c_to): (c_from, c_to)
                for c_from, c_to in chunks
            }
            for f in as_completed(futs):
                try:
                    mp_raw.extend(f.result())
                except Exception as e:
                    log.error(f"  ❌ 청크 오류: {e}")
    else:
        if DATA_REFRESH_START <= YESTERDAY:
            # 단일 호출이므로 겹침 중복 없음 — from_date 만 버퍼만큼 앞당기면 됨
            mp_from = (DATA_REFRESH_START - timedelta(days=MP_FETCH_BUFFER_DAYS)).strftime("%Y-%m-%d")
            mp_raw.extend(
                fetch_mixpanel_data(
                    mp_from,
                    YESTERDAY.strftime("%Y-%m-%d"),
                )
            )
            time.sleep(2)

    # 오늘 데이터 별도 호출
    utc_today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    mp_today_str = TODAY.strftime("%Y-%m-%d")
    if mp_today_str <= utc_today:
        log.info(f"  ── 오늘({mp_today_str}) 별도 호출 ──")
        today_data = fetch_mixpanel_data(mp_today_str, mp_today_str)
        if today_data:
            mp_raw.extend(today_data)

    log.info(f"✅ Mixpanel 총: {len(mp_raw)}건")

    # Mixpanel 집계: (date, adset_id) → revenue, count
    import pandas as pd

    mp_value_map = {}
    mp_count_map = {}

    if mp_raw:
        df = pd.DataFrame(mp_raw)

        # utm_term 정규화: undefined/None 등을 빈 문자열로
        def _norm_utm(x):
            s = str(x).strip() if x is not None else ""
            return "" if s.lower() in ("", "none", "undefined", "null") else s
        df["utm_term"] = df["utm_term"].apply(_norm_utm)

        # ▼ 채널 분류 (meta / organic / other)
        #   - other(google 등 명시된 타채널): 결제가 직전 Meta 방문의 stale utm_term(세트 id)을
        #     달고 들어와 Meta 세트로 오귀속되는 것 차단 → 통째로 제외(기존 Meta 소스필터 취지 유지).
        #   - organic(utm_source 빈값): 크로스셀로 utm 소실돼 오가닉처럼 찍힌 결제. 자기 utm_term은
        #     신뢰 안 함(스테일 가능) → 비운 뒤 같은 유저의 직전 Meta 결제(라스트터치·24h)에서만 상속.
        #   - meta: 자기 utm_term(=adset_id) 그대로 사용.
        def _src_class(us):
            if is_meta_source(us):
                return "meta"
            return "organic" if str(us).strip() == "" else "other"
        df["_src"] = df["utm_source"].apply(_src_class)
        _n_all = len(df)
        _n_other = int((df["_src"] == "other").sum())
        df = df[df["_src"] != "other"].copy()
        df.loc[df["_src"] == "organic", "utm_term"] = ""  # organic 자기 utm 불신 → Meta 라스트터치에서만 상속
        log.info(f"  🔵 채널 분류: 전체 {_n_all} → meta+organic {len(df)}건 (other 타채널 {_n_other}건 제외)")

        # 1) Mixpanel canonical dedup: $insert_id 기준 (true duplicates만 제거)
        #    insert_id 가 없는 이벤트는 청크 2일 겹침 fetch 시 중복될 수 있어
        #    파싱 필드 조합으로 추가 dedup 한다(단일 호출 시엔 제거 대상이 거의 없음).
        if "insert_id" in df.columns:
            df_iid = df[df["insert_id"].astype(str).str.len() > 0]
            df_no_iid = df[df["insert_id"].astype(str).str.len() == 0]
            df_iid = df_iid.drop_duplicates(subset=["insert_id"], keep="first")
            df_no_iid = df_no_iid.drop_duplicates(
                subset=["date", "distinct_id", "서비스", "utm_term", "revenue"], keep="first"
            )
            df_d = pd.concat([df_iid, df_no_iid], ignore_index=True)
        else:
            df_d = df.drop_duplicates(subset=["date", "distinct_id", "서비스"], keep="first")

        # 1.5) order_id 기준 주문 단위 dedup (결제완료/payment_complete 이중발화 방지)
        #   한 주문이 두 이벤트명 + 재시도로 평균 ~3.3회 발화하는데, 발화마다 insert_id
        #   가 달라 위 insert_id dedup 으로는 안 걸러진다 → utm_term backfill 후 groupby
        #   sum 단계에서 같은 주문 매출이 2~3배 중복 합산되어 특정 날짜 귀속이 과대계상된다
        #   (원본 order_id dedup 검증: 일 총매출이 Toss 와 ±0.5% 일치).
        #   같은 order_id 그룹에서 utm_term-set & 최대 revenue 행을 1건만 보존(귀속 유실 방지).
        if "order_id" in df_d.columns:
            df_d["_oid"] = df_d["order_id"].astype(str).str.strip()
            _has_oid = df_d["_oid"].str.len() > 0
            _with = df_d[_has_oid].copy()
            _without = df_d[~_has_oid]
            _with["_hasu"] = (_with["utm_term"].astype(str).str.len() > 0).astype(int)
            _with = (_with.sort_values(["_oid", "_hasu", "revenue"], ascending=[True, False, False])
                          .drop_duplicates(subset=["_oid"], keep="first")
                          .drop(columns=["_hasu"]))
            df_d = pd.concat([_with, _without], ignore_index=True).drop(columns=["_oid"])
            log.info(f"  🧹 order_id 주문단위 dedup 후: {len(df_d)}건")

        # 2) utm_term 백필 — 크로스셀 회수 (라스트터치 · 1일창 · 세트 그레인)
        #   같은 유저(distinct_id)가 Meta 광고로 산 뒤(utm_term 보유) 인앱/추천 등 UTM 없는
        #   경로로 연달아 산 결제(organic)는 utm_term 이 비어 오가닉처럼 찍힌다. 이 빈 결제에
        #   '직전 24h 내 마지막 Meta 결제'의 utm_term(=adset_id)을 상속시켜 세트 매출로 회수한다.
        #   · 라스트터치: 시간상 가장 최근 Meta 접점의 세트.
        #   · 1일창(86400s): 접점~결제 간격 24h 이내만(과거 광고 과귀속 방지).
        #   · 백필된 행은 다음 결제의 접점으로 쓰지 않음(체이닝 24h 초과 확장 방지).
        #   과거 글로벌은 무제한 백필로 Stripe 초과(over-attribution)해 비활성화됨(2026-05-06) →
        #   여기선 라스트터치+24h 로 좁혀 재도입. 세트 전용(utm_term=adset_id)이라 소재별엔 무영향.
        BACKFILL_WINDOW_SEC = 86400
        if "ts" not in df_d.columns:
            df_d["ts"] = 0
        df_d = df_d.reset_index(drop=True)
        df_d["_ismeta"] = (df_d["_src"] == "meta") & (df_d["utm_term"].astype(str).str.len() > 0)
        _s = df_d.sort_values(["distinct_id", "ts"], kind="mergesort").reset_index(drop=False)
        _terms = _s["utm_term"].astype(str).tolist()
        _tsl = _s["ts"].fillna(0).astype("int64").tolist()
        _didl = _s["distinct_id"].astype(str).tolist()
        _metal = _s["_ismeta"].tolist()
        _idxl = _s["index"].tolist()
        _last_did = None; _last_term = None; _last_ts = None
        _recovered = {}
        for _i in range(len(_s)):
            _d = _didl[_i]
            if _d != _last_did:
                _last_did = _d; _last_term = None; _last_ts = None
            if _d in ("", "None", "nan", "null"):
                continue  # 식별 불가 유저는 상속/접점 대상 제외
            if _terms[_i]:
                if _metal[_i] and _tsl[_i] > 0:
                    _last_term = _terms[_i]; _last_ts = _tsl[_i]  # 라스트터치 갱신
            else:
                if _last_term and _tsl[_i] > 0 and _last_ts and 0 <= _tsl[_i] - _last_ts <= BACKFILL_WINDOW_SEC:
                    _recovered[_idxl[_i]] = _last_term
        if _recovered:
            _rev_rec = float(df_d.loc[list(_recovered.keys()), "revenue"].sum())
            for _oi, _t in _recovered.items():
                df_d.at[_oi, "utm_term"] = _t
            log.info(f"  🔗 크로스셀 백필(라스트터치·24h): {len(_recovered)}건 회수 · 매출 ₩{int(_rev_rec):,}")
        else:
            log.info("  🔗 크로스셀 백필: 회수 대상 없음")

        # 3) utm_term 채워진 결제만 귀속 (미회수 organic 은 오가닉으로 남겨 제외)
        _pre_n = len(df_d); _pre_rev = float(df_d["revenue"].sum())
        df_d = df_d[df_d["utm_term"].astype(str).str.len() > 0]
        _drop_rev = _pre_rev - float(df_d["revenue"].sum())
        log.info(f"  📊 귀속 {len(df_d)}건 · 미회수 organic 제외 {_pre_n - len(df_d)}건(매출 ₩{int(_drop_rev):,})")

        total_revenue = df_d["revenue"].sum()
        log.info(f"  📊 매출 합계 (크로스셀 백필 적용): ₩{int(total_revenue):,}")

        for (d, ut), v in df_d.groupby(["date", "utm_term"])["revenue"].sum().items():
            if d and ut:
                mp_value_map[(d, str(ut))] = v
        for (d, ut), c in df_d.groupby(["date", "utm_term"]).size().items():
            if d and ut:
                mp_count_map[(d, str(ut))] = c

    # ── only-raise 가드용: 현재 저장된 귀속(results_mp/revenue) 미리 읽기 ──
    #   부실/부분 실패한 Mixpanel fetch 가 이미 정상인 과거 귀속을 '낮추지' 못하게 한다.
    #   (spend/meta 등 Meta-side 지표는 항상 최신값으로 갱신됨)
    #   ※ budget 은 예외 — 아래 '예산 스냅샷 보존' 참고. 과거 날짜 예산은 그날 값으로 고정.
    prev_attr = {}
    prev_budget = {}  # (date, adset_id) → 기존 저장된 일예산 (증감액 테두리용 일자별 스냅샷 보존)
    _ps = DATA_REFRESH_START.strftime("%Y-%m-%d")
    _pe = TODAY.strftime("%Y-%m-%d")
    _off = 0
    while True:
        _u = (f"{sb.base_url}/rest/v1/ad_performance_daily?select=date,adset_id,results_mp,revenue,budget"
              f"&date=gte.{_ps}&date=lte.{_pe}&order=date.asc,adset_id.asc&limit=1000&offset={_off}")
        try:
            _chunk = req_lib.get(_u, headers={**sb.headers, "Prefer": ""}, timeout=60).json()
        except Exception as _e:
            log.warning(f"  ⚠️ 기존 귀속 읽기 실패(가드 비활성화): {_e}")
            _chunk = []
        if not isinstance(_chunk, list) or not _chunk:
            break
        for _row in _chunk:
            _k = (_row.get("date"), str(_row.get("adset_id")))
            prev_attr[_k] = (
                int(_row.get("results_mp") or 0), float(_row.get("revenue") or 0.0))
            prev_budget[_k] = int(_row.get("budget") or 0)
        if len(_chunk) < 1000:
            break
        _off += 1000
    log.info(f"  🛡️ only-raise 가드: 기존 귀속 {len(prev_attr)}건 로드")

    # =======================================================
    # 5) 병합 → Supabase 레코드 생성
    # =======================================================
    log.info(f"\n5단계: Meta + Mixpanel + 예산 병합")

    records = []
    product_stats = defaultdict(lambda: {"spend": 0, "revenue": 0, "count": 0})

    for dk, rows in meta_date_data.items():
        # dk → ISO date
        parts = dk.split("/")
        iso_date = f"20{parts[0]}-{parts[1]}-{parts[2]}"

        for mr in rows:
            asid = mr["adset_id"]
            if not asid:
                continue

            acc_id = mr["ad_account_id"]
            fx = get_fx(acc_id, dk)
            bdiv = get_budget_divisor(acc_id)

            # FX 적용
            spend = mr["spend"] * fx
            cpm = mr["cpm"] * fx
            cost_per_result = mr["cost_per_result"] * fx
            cost_per_click = mr["cost_per_click"] * fx

            # Mixpanel 매칭
            mpc = mp_count_map.get((dk, asid), 0)
            mpv = mp_value_map.get((dk, asid), 0.0)
            # ★ only-raise 가드: 새 귀속이 기존 저장값보다 낮으면(부실 fetch 등) 기존 보존
            _prev = prev_attr.get((iso_date, str(asid)))
            if _prev and mpc < _prev[0]:
                mpc = _prev[0]
                mpv = _prev[1]
            revenue = float(mpv)

            # 파생 지표
            profit = revenue - spend
            roas = (revenue / spend * 100) if spend > 0 else 0
            cvr = (mpc / mr["unique_clicks"] * 100) if mr["unique_clicks"] > 0 and mpc > 0 else 0

            # 예산 — 증감액 테두리를 위해 '그 날짜에 실제 설정돼 있던 값'으로 채운다.
            #   1순위: activities 재구성(해당 세트에 예산 변경이력이 있으면 그 날짜값 복원).
            #   2순위(폴백): 변경이력 없음/activities 실패 → 일자별 스냅샷 보존
            #     (오늘만 현재값, 과거는 기존 저장값 유지 → 평탄화 방지).
            budget_raw_cur = budget_map.get(asid, 0)
            if bud_hist.has_events_for(asid):
                b_raw = bud_hist.raw_on(asid, iso_date, budget_raw_cur)
                budget_val = round(b_raw / bdiv * fx) if b_raw > 0 else 0
            else:
                budget_cur = round(budget_raw_cur / bdiv * fx) if budget_raw_cur > 0 else 0
                if iso_date == _pe:
                    budget_val = budget_cur
                else:
                    _pb = prev_budget.get((iso_date, str(asid)))
                    budget_val = _pb if _pb else budget_cur

            # 상품 추출
            product = extract_product(mr["adset_name"], mr["campaign_name"])
            product_stats[product]["spend"] += spend
            product_stats[product]["revenue"] += revenue
            product_stats[product]["count"] += 1

            records.append(
                {
                    "date": iso_date,
                    "adset_id": asid,
                    "campaign_name": mr["campaign_name"],
                    "adset_name": mr["adset_name"],
                    "ad_account_id": acc_id,
                    "product": product,
                    "spend": round(spend, 2),
                    "cost_per_result": round(cost_per_result, 2),
                    "purchase_roas_meta": round(mr["meta_roas"], 4),
                    "cpm": round(cpm, 2),
                    "reach": mr["reach"],
                    "impressions": mr["impressions"],
                    "unique_clicks": int(mr["unique_clicks"]),
                    "unique_ctr": round(mr["unique_ctr"], 4),
                    "cost_per_click": round(cost_per_click, 2),
                    "frequency": round(mr["frequency"], 4),
                    "results_meta": int(mr["results_meta"]),
                    "results_meta_click": int(mr.get("results_meta_click", 0)),
                    "results_mp": mpc,
                    "revenue": round(revenue, 2),
                    "profit": round(profit, 2),
                    "roas": round(roas, 2),
                    "cvr": round(cvr, 4),
                    "budget": budget_val,
                }
            )

    log.info(f"✅ 레코드: {len(records)}개")
    log.info(f"📦 상품별:")
    for p in sorted(product_stats, key=lambda x: product_stats[x]["spend"], reverse=True):
        s = product_stats[p]
        r = (s["revenue"] / s["spend"] * 100) if s["spend"] > 0 else 0
        log.info(f"  {p}: {s['count']}건 | 지출 ₩{int(s['spend']):,} | 매출 ₩{int(s['revenue']):,} | ROAS {r:.0f}%")

    # =======================================================
    # 6) Supabase Upsert
    # =======================================================
    log.info(f"\n6단계: Supabase upsert ({len(records)}행)")
    if records:
        success = sb.upsert("ad_performance_daily", records, chunk_size=500)
        log.info(f"✅ Supabase 완료: {success}/{len(records)}행")
    else:
        log.warning("⚠️ upsert할 레코드 없음")

    # =======================================================
    # 7) 일별 집계 테이블
    # =======================================================
    log.info(f"\n7단계: 일별/상품별 집계 테이블")

    daily_agg = defaultdict(lambda: defaultdict(lambda: {
        "spend": 0, "revenue": 0, "profit": 0, "results_mp": 0, "unique_clicks": 0,
    }))

    for rec in records:
        d = rec["date"]
        p = rec["product"]
        daily_agg[d][p]["spend"] += rec["spend"]
        daily_agg[d][p]["revenue"] += rec["revenue"]
        daily_agg[d][p]["profit"] += rec["profit"]
        daily_agg[d][p]["results_mp"] += rec["results_mp"]
        daily_agg[d][p]["unique_clicks"] += rec["unique_clicks"]

    agg_records = []
    for d, products in daily_agg.items():
        for p, vals in products.items():
            roas = (vals["revenue"] / vals["spend"] * 100) if vals["spend"] > 0 else 0
            cvr = (
                (vals["results_mp"] / vals["unique_clicks"] * 100)
                if vals["unique_clicks"] > 0 and vals["results_mp"] > 0
                else 0
            )
            agg_records.append(
                {
                    "date": d,
                    "product": p,
                    "spend": round(vals["spend"], 2),
                    "revenue": round(vals["revenue"], 2),
                    "profit": round(vals["profit"], 2),
                    "roas": round(roas, 2),
                    "cvr": round(cvr, 4),
                    "results_mp": vals["results_mp"],
                    "unique_clicks": vals["unique_clicks"],
                }
            )

    if agg_records:
        agg_success = sb.upsert("ad_daily_product_summary", agg_records, chunk_size=500)
        log.info(f"✅ 일별 집계: {agg_success}/{len(agg_records)}행")

    # =======================================================
    # 완료
    # =======================================================
    log.info("\n" + "=" * 60)
    log.info("✅ 파이프라인 완료!")
    log.info("=" * 60)
    log.info(f"  Meta: {len(meta_date_data)}일, {total_meta}건")
    log.info(f"  Mixpanel: {len(mp_raw)}건")
    log.info(f"  예산: {len(budget_map)}개 세트")
    log.info(f"  → Supabase ad_performance_daily: {len(records)}행")
    log.info(f"  → Supabase ad_daily_product_summary: {len(agg_records)}행")


if __name__ == "__main__":
    main()
