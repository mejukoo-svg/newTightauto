-- 뷰스루/클릭 분리용 컬럼
-- results_meta        = 메타 구매수(클릭 7일 + 조회 1일, 기존값 그대로)
-- results_meta_click  = 클릭(7d_click) 어트리뷰션 구매수만
-- 뷰% = (results_meta - results_meta_click) / results_meta
--
-- ⚠️ 파이프라인(국내/글로벌_세트별_supabase.py) 배포 '전에' 먼저 실행해야 함.
--    (컬럼 없는 상태로 파이프라인이 results_meta_click 을 upsert 하면 실패)
-- Supabase 대시보드 → SQL Editor 에서 실행.

ALTER TABLE ad_performance_daily
  ADD COLUMN IF NOT EXISTS results_meta_click integer DEFAULT 0;

ALTER TABLE global_ad_performance_daily
  ADD COLUMN IF NOT EXISTS results_meta_click integer DEFAULT 0;
