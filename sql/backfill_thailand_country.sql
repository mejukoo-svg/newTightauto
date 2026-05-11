-- 태국 캠페인 country/currency/product 라벨 보정 (2026-05-11)
-- 이전 코드 버그로 잘못 분류된 레코드 정정:
--   detect_currency():  태국 케이스 없음 → currency='TWD', country='대만'
--   SKIP_WORDS:         '태국' 빠짐 → product='태국' (실제로는 캠페인명 2번째 토큰, 예: 무당)
-- revenue/profit/roas 도 TWD 환율 기준이라 약 10% 과대 계상 (THB:35.5 vs TWD:31.7)

-- 1) 영향 받을 레코드 확인 (DRY RUN)
SELECT date, campaign_name, adset_name, country, currency, product, spend_usd, revenue_usd, roas
FROM global_ad_performance_daily
WHERE (campaign_name ILIKE '%태국%' OR adset_name ILIKE '%태국%' OR campaign_name ILIKE '%thailand%')
  AND (country != '태국' OR currency != 'THB' OR product = '태국')
ORDER BY date DESC;

-- 2) 라벨만 보정 (금액은 다음 파이프라인 실행 시 정확한 환율로 재계산됨)
-- UPDATE global_ad_performance_daily
-- SET country = '태국',
--     currency = 'THB',
--     product = LOWER(SPLIT_PART(campaign_name, '_', 3))  -- '태국_th_무당_xxx' -> '무당'
-- WHERE (campaign_name ILIKE '%태국%' OR adset_name ILIKE '%태국%' OR campaign_name ILIKE '%thailand%')
--   AND (country != '태국' OR currency != 'THB' OR product = '태국');

-- 3) 금액까지 즉시 보정 (TWD 31.7 / THB 35.5 환율 가정, 한번만 실행)
-- UPDATE global_ad_performance_daily
-- SET country     = '태국',
--     currency    = 'THB',
--     product     = LOWER(SPLIT_PART(campaign_name, '_', 3)),
--     revenue_usd = ROUND((revenue_usd * 31.7 / 35.5)::numeric, 2),
--     profit_usd  = ROUND((revenue_usd * 31.7 / 35.5)::numeric, 2) - spend_usd,
--     roas        = CASE WHEN spend_usd > 0
--                        THEN ROUND(((revenue_usd * 31.7 / 35.5) / spend_usd * 100)::numeric, 2)
--                        ELSE 0 END
-- WHERE (campaign_name ILIKE '%태국%' OR adset_name ILIKE '%태국%' OR campaign_name ILIKE '%thailand%')
--   AND currency = 'TWD';   -- 가드: 재실행 방지
