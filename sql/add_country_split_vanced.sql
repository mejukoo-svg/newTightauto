-- =====================================================================
-- 밴스드: country 분할(grain 변경) 마이그레이션
--   기존 grain (date, adset_id)  →  (date, adset_id, country)
--   country = Mixpanel mp_country_code (ISO alpha-2: TW/HK/TH/JP/US...,
--             mp_country_code 누락 = 국내 KR)
--   currency = 서비스 접미사 기준 통화(TWD/THB/KRW...) — 디버깅/검증용
-- 실행 순서: ① 이 파일 STEP1~3 실행 → ② 파이프라인 FULL_REFRESH=true 재적재
--            → ③ STEP4(이전 grain 잔여행 정리) 실행
-- =====================================================================

-- STEP 1) 컬럼 추가 (idempotent)
ALTER TABLE vanced_ad_performance_daily ADD COLUMN IF NOT EXISTS country  text;
ALTER TABLE vanced_ad_performance_daily ADD COLUMN IF NOT EXISTS currency text;

-- STEP 2) PK 변경 전, 기존 행을 충돌 안 나는 sentinel 로 백필
UPDATE vanced_ad_performance_daily SET country = '_PRE' WHERE country IS NULL;
ALTER TABLE vanced_ad_performance_daily ALTER COLUMN country SET NOT NULL;

-- STEP 3) PK 를 (date, adset_id, country) 로 교체 (기존 PK 이름 자동 탐지)
DO $$
DECLARE pk text;
BEGIN
  SELECT conname INTO pk FROM pg_constraint
   WHERE conrelid = 'vanced_ad_performance_daily'::regclass AND contype = 'p';
  IF pk IS NOT NULL THEN
    EXECUTE format('ALTER TABLE vanced_ad_performance_daily DROP CONSTRAINT %I', pk);
  END IF;
END $$;
ALTER TABLE vanced_ad_performance_daily
  ADD CONSTRAINT vanced_ad_performance_daily_pkey PRIMARY KEY (date, adset_id, country);

-- =====================================================================
-- ② 여기서 멈추고 파이프라인을 FULL_REFRESH=true 로 1회 재적재한다.
--    (밴스드_세트별_supabase.py — country 분할 버전이 (date,adset_id,country) 행을 upsert)
-- =====================================================================

-- STEP 4) 재적재 완료 후, 마이그레이션 이전 잔여행(_PRE) 정리 — 한 번만 실행
-- DELETE FROM vanced_ad_performance_daily WHERE country = '_PRE';

-- 검증 쿼리 (참고)
-- SELECT country, count(*), round(sum(spend))  AS spend, round(sum(revenue)) AS revenue
--   FROM vanced_ad_performance_daily
--  WHERE date >= (current_date - 7)::text
--  GROUP BY country ORDER BY spend DESC;
