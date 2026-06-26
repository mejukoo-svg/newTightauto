-- =====================================================================
-- 글로벌: country 분할(grain 변경) 마이그레이션
--   기존 grain (date, adset_id) — adset당 country 1개(캠페인명 detect_currency, 한글값)
--   신규 grain (date, adset_id, country) — country = mp_country_code(ISO), 한국(KR) 제외
--   currency 는 서비스 접미사 기준(TWD/THB...)으로 파이프라인이 채움
-- 실행 순서: ① STEP1~2 → ② 파이프라인 FULL_REFRESH=true 재적재 → ③ STEP3(한글 잔여행 정리)
-- =====================================================================

-- STEP 1) country 는 이미 존재. NOT NULL 보장 (혹시 NULL 행이 있으면 sentinel)
UPDATE global_ad_performance_daily SET country = '_PRE' WHERE country IS NULL OR country = '';
ALTER TABLE global_ad_performance_daily ALTER COLUMN country SET NOT NULL;

-- STEP 2) PK 를 (date, adset_id, country) 로 교체 (기존 PK 이름 자동 탐지)
DO $$
DECLARE pk text;
BEGIN
  SELECT conname INTO pk FROM pg_constraint
   WHERE conrelid = 'global_ad_performance_daily'::regclass AND contype = 'p';
  IF pk IS NOT NULL THEN
    EXECUTE format('ALTER TABLE global_ad_performance_daily DROP CONSTRAINT %I', pk);
  END IF;
END $$;
ALTER TABLE global_ad_performance_daily
  ADD CONSTRAINT global_ad_performance_daily_pkey PRIMARY KEY (date, adset_id, country);

-- =====================================================================
-- ② 여기서 멈추고 파이프라인을 FULL_REFRESH=true 로 1회 재적재한다.
--    (글로벌_세트별_supabase.py — country=mp_country_code(ISO) 분할 버전)
-- =====================================================================

-- STEP 3) 재적재 완료 후, 마이그레이션 이전 한글 country 잔여행 정리 — 한 번만 실행
--   ISO 코드(A-Z 2글자 등)만 남기고 기존 한글 라벨('대만'/'일본'/'태국'/'_PRE') 제거
-- DELETE FROM global_ad_performance_daily WHERE country ~ '[가-힣]' OR country = '_PRE';

-- 검증 쿼리 (참고)
-- SELECT country, count(*), round(sum(spend_usd)) AS spend_usd, round(sum(revenue_usd)) AS rev_usd
--   FROM global_ad_performance_daily
--  WHERE date >= (current_date - 7)::text
--  GROUP BY country ORDER BY spend_usd DESC;
