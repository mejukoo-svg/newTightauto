#   - 기존 날짜탭: Meta 데이터 + 매출/ROAS/순이익/CVR 모두 업데이트
#   - 없는 날짜탭만 새로 생성
#   - 마스터탭/추이차트 등 분석탭: 전체 날짜탭 데이터 읽어서 재구성
#   - 탭 순서: [기타] + [날짜탭 과거→최신] + [분석탭]
#   - 탭 순서: [기타] + [날짜탭 과거→최신] + [분석탭] + [주간매출, 매출]
#   - 구 구조(25열) / 신 구조(23열) 자동 판별
#   - Mixpanel 전체 날짜탭 기간으로 조회, profit/ROAS/CVR 직접 계산
#   - ★ FIX v21: Mixpanel 이벤트명/필드명 OR 처리 (결제완료↔payment_complete, amount↔결제금액)
@@ -226,6 +226,10 @@ def get_token(acc_id): return META_TOKENS.get(acc_id, META_TOKEN_DEFAULT)
"추이차트", "추이차트(주간)", "증감액", "예산",
"주간종합", "주간종합_2", "주간종합_3", "마스터탭"
]

# ★ 맨 오른쪽에 배치할 탭 (순서대로)
RIGHTMOST_TABS = ["주간매출", "매출"]

ANALYSIS_TABS_SET = set(FINAL_ANALYSIS_ORDER) | {"_temp", "_temp_holder", "_tmp"}


@@ -363,21 +367,26 @@ def clear_summary_conditional_formats(sh, ws, summary_start_row_0indexed):
except Exception as e: print(f"    ⚠️ 조건부 서식 삭제 오류 (무시): {e}")

def reorder_tabs(sh):
    """탭 순서 정리: [기타] + [날짜탭 과거→최신] + [분석탭] + [주간매출, 매출]"""
try:
all_ws = sh.worksheets()
        analysis_tabs, date_tabs, other_tabs = [], [], []
        analysis_tabs, date_tabs, other_tabs, rightmost_tabs_list = [], [], [], []
analysis_order_map = {name: i for i, name in enumerate(FINAL_ANALYSIS_ORDER)}
        rightmost_order_map = {name: i for i, name in enumerate(RIGHTMOST_TABS)}
for ws in all_ws:
tn = ws.title
            if tn in ANALYSIS_TABS_SET: analysis_tabs.append(ws)
            if tn in RIGHTMOST_TABS: rightmost_tabs_list.append(ws)
            elif tn in ANALYSIS_TABS_SET: analysis_tabs.append(ws)
elif parse_date_tab(tn) is not None: date_tabs.append(ws)
else: other_tabs.append(ws)
date_tabs.sort(key=lambda ws: parse_date_tab(ws.title))
analysis_tabs.sort(key=lambda ws: analysis_order_map.get(ws.title, 999))
        final_order = other_tabs + date_tabs + analysis_tabs
        print(f"  📋 기타: {len(other_tabs)}개 | 📅 날짜: {len(date_tabs)}개 | 📊 분석: {len(analysis_tabs)}개")
        rightmost_tabs_list.sort(key=lambda ws: rightmost_order_map.get(ws.title, 999))
        final_order = other_tabs + date_tabs + analysis_tabs + rightmost_tabs_list
        print(f"  📋 기타: {len(other_tabs)}개 | 📅 날짜: {len(date_tabs)}개 | 📊 분석: {len(analysis_tabs)}개 | ➡️ 맨끝: {len(rightmost_tabs_list)}개")
if date_tabs: print(f"  📅 날짜탭: {date_tabs[0].title} (과거) → {date_tabs[-1].title} (최신)")
if analysis_tabs: print(f"  📊 분석탭: {' → '.join(ws.title for ws in analysis_tabs)}")
        if rightmost_tabs_list: print(f"  ➡️ 맨끝탭: {' → '.join(ws.title for ws in rightmost_tabs_list)}")
with_retry(sh.batch_update, body={"requests": [
{"updateSheetProperties": {"properties": {"sheetId": ws.id, "index": idx}, "fields": "index"}}
for idx, ws in enumerate(final_order)
@@ -771,6 +780,9 @@ def read_all_date_tabs(sh, analysis_tab_names, mp_value_map=None, mp_count_map=N
tn = ws_ex.title
if tn in analysis_set:
skipped_analysis.append(tn); continue
        # ★ RIGHTMOST_TABS도 스킵
        if tn in RIGHTMOST_TABS:
            skipped_no_slash.append(tn); continue
dt_obj = parse_date_tab(tn)
if dt_obj is None:
if '/' in tn: skipped_parse_fail.append(tn)
