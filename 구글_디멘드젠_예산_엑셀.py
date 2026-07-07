# -*- coding: utf-8 -*-
"""
구글_디멘드젠_예산_엑셀.py
=========================
바탕화면(또는 지정 폴더)의 구글 "광고 보고서" 엑셀(*.xlsx)들을 읽어
콘텐츠(ct)별 × 일자별 **지출(비용)** 을 집계 → google_demandgen_content_spend_daily upsert

광고 보고서 엑셀 구조:
  Row0: '광고 보고서'
  Row1: 기간 (예: '2026년 6월 6일 - 2026년 6월 6일')  ← 파일당 하루치
  Row2: 헤더 ('광고 상태','최종 URL',...,'비용',...,'전환수',...)
  Row3+: 데이터, 마지막 '전체:...' 합계행은 스킵
  '최종 URL' 에 ?ch=google&ct=<콘텐츠>&cr=<제작자> 파라미터 포함

ch=google 인 행만, ct 별로 비용/클릭/노출/전환 합산.
※ 매출은 Mixpanel(구글_디멘드젠_mp_supabase.py)에서, 지출은 이 스크립트(엑셀)에서.
  index.html 이 (date, content) 로 두 소스를 조인해 ROAS/순이익 계산.

이 스크립트는 로컬 수동 실행용 (엑셀이 수동 다운로드라 GitHub Actions cron 불가).
  사용: python 구글_디멘드젠_예산_엑셀.py [엑셀폴더]  (생략 시 바탕화면)
  --dry 옵션: Supabase 적재 없이 집계만 출력

환경변수: SUPABASE_URL / SUPABASE_SERVICE_KEY  (--dry 면 불필요)
"""

import os, re, sys, glob, time, logging
from datetime import date
from collections import defaultdict
from urllib.parse import urlparse, parse_qs

import openpyxl
import requests as req_lib

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

DRY = "--dry" in sys.argv
args = [a for a in sys.argv[1:] if not a.startswith("--")]
DEFAULT_DIR = os.path.join(os.path.expanduser("~"), "Desktop")
SRC_DIR = args[0] if args else DEFAULT_DIR
GLOB = os.path.join(SRC_DIR, "광고 보고서*.xlsx")
GOOGLE_CH = {"google"}
TABLE = "google_demandgen_content_spend_daily"

_date_re = re.compile(r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일")


def parse_period_start(cell) -> date | None:
    """Row1 '2026년 6월 6일 - 2026년 6월 6일' → date(2026,6,6) (시작일)"""
    if not cell:
        return None
    m = _date_re.search(str(cell))
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _num(x):
    try:
        s = str(x).replace(",", "").strip()
        return float(s) if s not in ("", "--", "None", "nan") else 0.0
    except Exception:
        return 0.0


def header_index(hdr_row):
    """헤더명 → 컬럼 인덱스 (정확 일치). '전환수'(col), '비용'(col) 등."""
    idx = {}
    for i, name in enumerate(hdr_row):
        if name is None:
            continue
        idx.setdefault(str(name).strip(), i)
    return idx


def load_excels(src_glob):
    files = sorted(glob.glob(src_glob))
    if not files:
        log.error(f"  ❌ 엑셀 없음: {src_glob}")
        return {}
    log.info(f"  📂 {len(files)}개 파일 발견")
    agg = defaultdict(lambda: {"spend": 0.0, "clicks": 0.0, "impressions": 0.0, "conversions": 0.0})
    skipped_ch = defaultdict(int)
    multiday = 0
    for f in files:
        try:
            wb = openpyxl.load_workbook(f, data_only=True)
        except Exception as e:
            log.warning(f"  ⚠️ 열기 실패 {os.path.basename(f)}: {e}"); continue
        ws = wb.worksheets[0]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 4:
            continue
        # 기간(시작일/종료일) — 하루치 가정. 멀티데이면 경고(일자 분할 불가).
        m = _date_re.findall(str(rows[1][0] or ""))
        start = parse_period_start(rows[1][0])
        if start is None:
            log.warning(f"  ⚠️ 기간 파싱 실패 {os.path.basename(f)}: {rows[1][0]!r}"); continue
        if len(m) >= 2 and m[0] != m[1]:
            multiday += 1
            log.warning(f"  ⚠️ {os.path.basename(f)} 기간이 여러 날({rows[1][0]}) — 시작일로만 적재(일자 분할 불가)")
        hidx = header_index(rows[2])
        ci_url = hidx.get("최종 URL")
        ci_cost = hidx.get("비용")
        ci_clk = hidx.get("클릭수")
        ci_imp = hidx.get("노출수")
        ci_conv = hidx.get("전환수")
        if ci_url is None or ci_cost is None:
            log.warning(f"  ⚠️ 헤더 미발견 {os.path.basename(f)}"); continue
        d_iso = start.isoformat()
        for r in rows[3:]:
            if not r:
                continue
            if r[0] and str(r[0]).startswith("전체"):
                continue
            url = r[ci_url] if ci_url < len(r) else None
            if not url:
                continue
            q = parse_qs(urlparse(str(url)).query)
            ch = (q.get("ch") or [""])[0].strip().lower()
            ct = (q.get("ct") or [""])[0].strip()
            if ch not in GOOGLE_CH:
                skipped_ch[ch or "(none)"] += 1
                continue
            if not ct:
                continue
            cell = lambda ci: (r[ci] if (ci is not None and ci < len(r)) else None)
            a = agg[(d_iso, ct)]
            a["spend"]       += _num(cell(ci_cost))
            a["clicks"]      += _num(cell(ci_clk))
            a["impressions"] += _num(cell(ci_imp))
            a["conversions"] += _num(cell(ci_conv))
    if skipped_ch:
        log.info(f"  🔍 ch!=google 스킵: {dict(skipped_ch)}")
    if multiday:
        log.warning(f"  ⚠️ 멀티데이 파일 {multiday}개 — 가능하면 하루치로 다운로드 권장")
    return dict(agg)


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
        # new-tightauto: SUPABASE_DB_SCHEMA 설정 시에만 스키마 프로파일 헤더 (미설정=기존 public)
        _sc = os.environ.get('SUPABASE_DB_SCHEMA', '').strip()
        if _sc:
            self.headers['Accept-Profile'] = _sc
            self.headers['Content-Profile'] = _sc

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
    log.info("=" * 60)
    log.info(f"🚀 구글 디멘드젠 예산(엑셀) → Supabase ({TABLE})")
    log.info(f"   폴더: {SRC_DIR}  | DRY={DRY}")
    log.info("=" * 60)
    agg = load_excels(GLOB)
    if not agg:
        log.warning("  ⚠️ 집계 결과 없음"); return

    dates = sorted({d for d, _ in agg})
    log.info(f"📦 (date,content)조합 {len(agg)}개 / 기간 {dates[0]}~{dates[-1]}")
    # 일자별 총지출 요약
    by_date = defaultdict(float)
    for (d, ct), v in agg.items():
        by_date[d] += v["spend"]
    for d in dates:
        log.info(f"   {d}: 지출 ₩{by_date[d]:,.0f}")
    # 콘텐츠 Top10 (기간 합계 지출)
    by_ct = defaultdict(float)
    for (d, ct), v in agg.items():
        by_ct[ct] += v["spend"]
    log.info("  🏆 콘텐츠 Top10 (지출):")
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
    # ⚠️ 기본은 upsert(merge)만 — 부분 다운로드(일부 광고그룹만)로 돌려도 다른 콘텐츠 지출 보존.
    #    (date,content) PK 기준 덮어쓰기/추가. 전체 기간을 싹 지우고 다시 넣으려면 --replace.
    if "--replace" in sys.argv:
        log.warning("  🧨 --replace: 기간 전체 삭제 후 재삽입 (이 배치에 없는 콘텐츠 지출도 삭제됨)")
        sb.delete_range(TABLE, dates[0], dates[-1])
    sb.upsert(TABLE, records)
    log.info("✅ 완료 (merge upsert — 기존 다른 콘텐츠 지출 보존)")


if __name__ == "__main__":
    main()
