# ================================================================
# v3-ad FIX v20 — 패치 가이드 (원본 v18에서 2곳만 수정)
#
# 문제: REFRESH_DAYS=7이라 최근 7일만 갱신, 1~2월 탭이 업데이트 안 됨
# 해결: FULL_REFRESH 모드 추가 → 1월1일부터 전체 기간 갱신
#
# 최초 1회: FULL_REFRESH=true 로 실행 (~30분, 72일치 전체 호출)
# 이후 매일: FULL_REFRESH=false 로 실행 (~5분, 7일치만)
# ================================================================


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 수정 1/2: REFRESH_DAYS 설정 (약 128~133행)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# ▼▼▼ 기존 코드 (이 3줄을 찾으세요) ▼▼▼
#
# REFRESH_DAYS = 7
# META_COLLECT_DAYS = REFRESH_DAYS
# DATA_REFRESH_START = TODAY - timedelta(days=REFRESH_DAYS - 1)
#
# ▲▲▲ 위 3줄을 아래로 교체 ▲▲▲

# ★ v20: FULL_REFRESH 모드
FULL_REFRESH = os.environ.get("FULL_REFRESH", "true").lower() == "true"
FULL_REFRESH_START = datetime(2026, 1, 1)

if FULL_REFRESH:
    REFRESH_DAYS = (TODAY - FULL_REFRESH_START).days + 1
    print(f"🔥 FULL_REFRESH 모드: {FULL_REFRESH_START.strftime('%Y-%m-%d')} ~ 오늘 ({REFRESH_DAYS}일)")
else:
    REFRESH_DAYS = 7

META_COLLECT_DAYS = REFRESH_DAYS
DATA_REFRESH_START = TODAY - timedelta(days=REFRESH_DAYS - 1)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 수정 2/2: Mixpanel 3단계 전체 교체 (약 285~315행)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# ▼▼▼ 기존 코드 (이 블록 전체를 찾으세요) ▼▼▼
# 시작: print("="*60); print(f"3단계: Mixpanel 수집 (최근 {REFRESH_DAYS}일만)"); print("="*60)
# 끝:   print()  (mp_value_map/mp_count_map 처리 후 빈 줄)
#
# ▲▲▲ 위 블록 전체를 아래로 교체 ▲▲▲

print("="*60); print(f"3단계: Mixpanel 수집 ({REFRESH_DAYS}일)"); print("="*60)
YESTERDAY = TODAY - timedelta(days=1)
mp_to_today = TODAY.strftime('%Y-%m-%d')
total_days = (TODAY - DATA_REFRESH_START).days + 1
print(f"  📅 Mixpanel 범위: {DATA_REFRESH_START.strftime('%Y-%m-%d')} ~ {mp_to_today} ({total_days}일)")
mp_raw = []

# ★ v20: 14일 초과 시 7일 단위 청크 분할 (Mixpanel 타임아웃 방지)
if REFRESH_DAYS > 14:
    CHUNK_SIZE = 7
    chunk_start = DATA_REFRESH_START
    chunk_num = 0
    while chunk_start <= YESTERDAY:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_SIZE - 1), YESTERDAY)
        chunk_num += 1
        print(f"\n  📦 청크 {chunk_num}: {chunk_start.strftime('%Y-%m-%d')} ~ {chunk_end.strftime('%Y-%m-%d')}")
        chunk_data = fetch_mixpanel_data(chunk_start.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d'))
        mp_raw.extend(chunk_data)
        print(f"  ✅ {len(chunk_data)}건 (누적: {len(mp_raw)}건)")
        chunk_start = chunk_end + timedelta(days=1)
        time.sleep(3)
else:
    mp_from = DATA_REFRESH_START.strftime('%Y-%m-%d')
    mp_to_yesterday = YESTERDAY.strftime('%Y-%m-%d')
    if DATA_REFRESH_START <= YESTERDAY:
        chunk_data = fetch_mixpanel_data(mp_from, mp_to_yesterday)
        mp_raw.extend(chunk_data)
        print(f"  ✅ 어제까지: {len(chunk_data)}건")
        time.sleep(2)

print(f"\n  ── 오늘({mp_to_today}) 별도 호출 ──")
today_data = fetch_mixpanel_data(mp_to_today, mp_to_today)
if today_data:
    mp_raw.extend(today_data)
    print(f"  ✅ 오늘: {len(today_data)}건 (누적: {len(mp_raw)}건)")
else:
    print(f"  ⚠️ 오늘 데이터 없음")
print(f"\n  ✅ Mixpanel 수집 완료: 총 {len(mp_raw)}건")

df = pd.DataFrame(mp_raw); print(f"  원본: {len(df)}건")
mp_value_map = {}; mp_count_map = {}
if len(df) > 0:
    df = df[df['utm_term'].notna() & (df['utm_term'] != '') & (df['utm_term'] != 'None')]
    print(f"  utm_term 있는 것: {len(df)}건")
    df = df.sort_values('revenue', ascending=False); bf = len(df)
    df_d = df.drop_duplicates(subset=['date', 'distinct_id', '서비스'], keep='first')
    print(f"  중복 제거: {bf} → {len(df_d)}건")
    total_revenue = df_d['revenue'].sum()
    print(f"  📊 매출 합계: ₩{int(total_revenue):,}")
    for (d, ut), v in df_d.groupby(['date', 'utm_term'])['revenue'].sum().items():
        if d and ut: mp_value_map[(d, str(ut))] = v
    for (d, ut), c in df_d.groupby(['date', 'utm_term']).size().items():
        if d and ut: mp_count_map[(d, str(ut))] = c
    print(f"  날짜+adset_id 조합: {len(mp_value_map)}개")
print()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# (선택) Meta 2단계 sleep 조정
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Meta 루프 안의 time.sleep(2) → time.sleep(3 if FULL_REFRESH else 2)
# FULL_REFRESH 시 72일 × 2계정 = 144회 호출이므로 Rate limit 방어


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 사용법
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# [최초 실행] 1~2월 수동 탭을 API 데이터로 덮어쓰기:
#   환경변수: FULL_REFRESH=true (또는 코드에서 직접 "true")
#   → Meta: 1/1~오늘 전체 호출 (~72일 × 2계정)
#   → Mixpanel: 7일 단위 청크로 전체 호출
#   → 기존 1~2월 탭: Meta 데이터로 업데이트 (7-A 단계)
#   → 없는 날짜: 새 탭 생성 (7-B 단계)
#   → 추이차트: 1/1부터 전체 날짜 포함
#   → 소요 시간: 약 30~60분
#
# [이후 매일] 일상 운영:
#   환경변수: FULL_REFRESH=false
#   → 최근 7일만 갱신 (기존과 동일, ~5분)
#
# [GitHub Actions]:
#   env:
#     FULL_REFRESH: "false"    # 일상
#     # FULL_REFRESH: "true"   # 전체 재갱신 필요 시
