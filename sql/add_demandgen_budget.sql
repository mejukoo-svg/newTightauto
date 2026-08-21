-- 구글 디멘드젠 [Tight] 세트 테이블에 budget 컬럼 추가
-- 목적: 국내탭 '🟢 구글 디멘드젠' 표의 세트ID 오른쪽 '예산' 컬럼.
-- 값: 구글 Ads 의 **캠페인 예산**(campaign_budget.amount_micros / 1e6, KRW).
--     디멘드젠은 예산이 캠페인 단위라 같은 캠페인의 광고그룹 행에 같은 값이 반복된다 → 세로합 금지.
-- 적재 규칙: 구글_디멘드젠_캠페인_supabase.py 가 **광고그룹별 최신 날짜 행에만** 현재값을 쓴다.
--     (지금 값의 스냅샷일 뿐 날짜별 이력이 아니다 — 과거 행을 채우면 예산 이력으로 오해된다.)
-- 실행: Supabase SQL Editor 또는 Management API 에서 1회 실행 (idempotent).
--     ⚠️ 실제 테이블은 스키마 "new-tightauto" 에 있다(sql/ 의 옛 파일들이 쓰는 public 이 아니다 —
--        2026-07 회사 프로젝트로 이관하며 옮겨졌다). 스키마를 명시하지 않으면 relation does not exist.

alter table "new-tightauto".google_demandgen_campaign_daily add column if not exists budget numeric;

-- 기존 행은 NULL → 대시보드는 세트별 '예산이 있는 가장 최근 행'을 읽으므로 다음 파이프라인 실행 시 채워짐.
