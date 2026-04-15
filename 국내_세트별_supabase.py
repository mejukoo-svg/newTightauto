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
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
ALL_AD_ACCOUNTS = list(META_TOKENS.keys())

MIXPANEL_PROJECT_ID = os.environ.get("MIXPANEL_PROJECT_ID", "3390233")
MIXPANEL_USERNAME = os.environ.get("MIXPANEL_USERNAME", "")
MIXPANEL_SECRET = os.environ.get("MIXPANEL_SECRET", "")
MIXPANEL_EVENT_NAMES = ["결제완료", "payment_complete"]

# 실행 설정
KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).replace(tzinfo=None)
FULL_REFRESH = os.environ.get("FULL_REFRESH", "false").lower() == "true"
FULL_REFRESH_START = datetime(2026, 1, 1)
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
MIXPANEL_CHUNK_WORKERS = 3
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
    """캠페인 이름의 첫 단어(이모지 제거)에서 상품명 추출"""
    for source in [campaign_name, adset_name]:
        if not source:
            continue
        cleaned = _strip_leading_emojis(str(source).strip())
        if not cleaned:
            continue
        first_word = re.split(r"[_\s\-/|,()\[\]]+", cleaned)[0].strip()
        if first_word:
            return first_word
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
    url = f"{META_BASE_URL}/{ad_account_id}"
    data = meta_api_get(url, {"fields": "currency,name"}, token=get_token(ad_account_id))
    if data:
        return data.get("currency", "USD"), data.get("name", ad_account_id)
    return "USD", ad_account_id


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
                "date_obj": date_obj,
                "date_key": date_str,
            }
        )
    return parsed


def fetch_adset_budgets(ad_account_id):
    """광고 세트별 일 예산 조회 (ASC 캠페인 예산 폴백 포함)"""
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

    try:
        resp = req_lib.get(
            url, params=params, auth=(MIXPANEL_USERNAME, MIXPANEL_SECRET), timeout=300
        )
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
                    dt_kst = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(
                        hours=9
                    )
                    ds = f"{dt_kst.year % 100:02d}/{dt_kst.month:02d}/{dt_kst.day:02d}"
                else:
                    ds = None

                # utm_term = adset_id
                ut = None
                for k in ["utm_term", "UTM_Term", "UTM Term"]:
                    if k in props and props[k]:
                        ut = clean_id(str(props[k]).strip())
                        break

                # 매출 금액
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

                data.append(
                    {
                        "distinct_id": props.get("distinct_id"),
                        "date": ds,
                        "utm_term": ut or "",
                        "amount": amount_val,
                        "value_raw": value_val,
                        "revenue": revenue,
                        "서비스": props.get("서비스", ""),
                    }
                )
            except Exception:
                pass

        log.info(f"  ✅ 파싱: {len(data)}건")
        return data
    except Exception as e:
        log.error(f"  ❌ Mixpanel 오류: {e}")
        return []


# =========================================================
# Supabase 클라이언트
# =========================================================
class SupabaseClient:
    """supabase-py 없이 REST API 직접 호출 (의존성 최소화)"""

    def __init__(self, url, key):
        self.base_url = url.strip().rstrip("/")
        if not self.base_url.startswith("http"):
            self.base_url = f"https://{self.base_url}"
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",  # upsert
        }

    def _sanitize(self, records):
        """numpy/pandas 타입 → Python 기본 타입 변환"""
        clean = []
        for rec in records:
            row = {}
            for k, v in rec.items():
                if hasattr(v, 'item'):  # numpy int64, float64 등
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

        for i in range(0, total, chunk_size):
            chunk = self._sanitize(records[i : i + chunk_size])
            try:
                resp = req_lib.post(
                    url,
                    headers=self.headers,
                    json=chunk,
                    timeout=60,
                )
                if resp.status_code in [200, 201]:
                    success += len(chunk)
                    log.info(f"  ✅ upsert {success}/{total}")
                else:
                    log.error(
                        f"  ❌ upsert 실패 (행 {i}~{i+len(chunk)}): "
                        f"{resp.status_code} {resp.text[:500]}"
                    )
            except Exception as e:
                log.error(f"  ❌ upsert 오류 (행 {i}~{i+len(chunk)}): {e}")
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
    # 4) Mixpanel 수집
    # =======================================================
    log.info(f"\n4단계: Mixpanel 수집 ({REFRESH_DAYS}일)")
    YESTERDAY = TODAY - timedelta(days=1)
    mp_raw = []

    if REFRESH_DAYS > 14:
        # 7일 단위 청크
        chunks = []
        chunk_start = DATA_REFRESH_START
        while chunk_start <= YESTERDAY:
            chunk_end = min(chunk_start + timedelta(days=6), YESTERDAY)
            chunks.append((chunk_start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
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
            mp_raw.extend(
                fetch_mixpanel_data(
                    DATA_REFRESH_START.strftime("%Y-%m-%d"),
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
        df = df[df["utm_term"].notna() & (df["utm_term"] != "") & (df["utm_term"] != "None")]

        # utm_term 있는 이벤트 우선, 중복 제거
        df["_has_utm"] = df["utm_term"].apply(
            lambda x: 0 if (x and str(x).strip() and str(x).strip() != "None") else 1
        )
        df = df.sort_values(["_has_utm", "revenue"], ascending=[True, False])
        df_d = df.drop_duplicates(subset=["date", "distinct_id", "서비스"], keep="first")

        total_revenue = df_d["revenue"].sum()
        log.info(f"  📊 매출 합계: ₩{int(total_revenue):,}")

        for (d, ut), v in df_d.groupby(["date", "utm_term"])["revenue"].sum().items():
            if d and ut:
                mp_value_map[(d, str(ut))] = v
        for (d, ut), c in df_d.groupby(["date", "utm_term"]).size().items():
            if d and ut:
                mp_count_map[(d, str(ut))] = c

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
            revenue = float(mpv)

            # 파생 지표
            profit = revenue - spend
            roas = (revenue / spend * 100) if spend > 0 else 0
            cvr = (mpc / mr["unique_clicks"] * 100) if mr["unique_clicks"] > 0 and mpc > 0 else 0

            # 예산
            budget_raw = budget_map.get(asid, 0)
            budget_val = round(budget_raw / bdiv * fx) if budget_raw > 0 else 0

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
