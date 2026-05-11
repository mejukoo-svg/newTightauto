-- 태국 캠페인 country/currency 라벨 보정 (2026-05-11)
-- 이전 detect_currency()에 태국 케이스가 없어 TWD/대만으로 잘못 분류된 레코드 정정
-- revenue_usd 등 금액 값은 다음 파이프라인 실행 시 정확한 THB→USD 환율로 재계산됨
--   (대략 stored * 0.89 정도가 정확한 값. 약 10% 과대 계상)

-- 1) 영향 받을 레코드 확인 (DRY RUN)
SELECT date, campaign_name, adset_name, country, currency, spend_usd, revenue_usd, results_mp
FROM global_ad_performance_daily
WHERE (campaign_name ILIKE '%태국%' OR adset_name ILIKE '%태국%' OR campaign_name ILIKE '%thailand%')
  AND (country != '태국' OR currency != 'THB')
ORDER BY date DESC;

-- 2) 실행 (위 결과 확인 후 주석 해제하여 실행)
-- UPDATE global_ad_performance_daily
-- SET country = '태국', currency = 'THB'
-- WHERE (campaign_name ILIKE '%태국%' OR adset_name ILIKE '%태국%' OR campaign_name ILIKE '%thailand%')
--   AND (country != '태국' OR currency != 'THB');

-- 3) 금액까지 즉시 보정하고 싶다면 (TWD 31.7 / THB 35.5 가정):
-- UPDATE global_ad_performance_daily
-- SET country = '태국',
--     currency = 'THB',
--     revenue_usd = ROUND((revenue_usd * 31.7 / 35.5)::numeric, 2),
--     profit_usd  = ROUND((revenue_usd * 31.7 / 35.5)::numeric, 2) - spend_usd,
--     roas        = CASE WHEN spend_usd > 0
--                        THEN ROUND(((revenue_usd * 31.7 / 35.5) / spend_usd * 100)::numeric, 2)
--                        ELSE 0 END
-- WHERE (campaign_name ILIKE '%태국%' OR adset_name ILIKE '%태국%' OR campaign_name ILIKE '%thailand%')
--   AND currency = 'TWD';
--   -- 주의: 한번만 실행해야 함 (currency='TWD' 가드로 재실행 방지)
