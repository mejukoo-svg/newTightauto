-- ============================================================
-- STEP 2 — anon(publishable 키) 접근 회수.
--          이 시점부터 공개된 anon 키만으로는 데이터를 읽을 수 없다.
--          (테이블 권한이 없으면 RLS 정책과 무관하게 거부됨)
--
-- ⚠ 반드시 먼저 확인할 것:
--   1) 새 index.html 이 배포돼 있고
--   2) Supabase Auth 공유계정으로 로그인이 되며
--   3) 하이라이트/메모 저장까지 정상 동작
--   위 3개를 검증한 뒤에만 실행.
--
-- ⚠ 부수효과: anon 키로 읽던 로컬 분석 스크립트(.py)는 깨진다.
--   → 그 스크립트들은 SUPABASE_SERVICE_KEY 로 전환해 둘 것(별도 처리됨).
--   → 프로덕션 파이프라인은 SERVICE_KEY 사용이라 영향 없음.
--
-- 검증 팁(실행 후): 로그아웃/시크릿창에서 대시보드를 열어
--   로그인 없이 데이터가 안 보이면(또는 콘솔에 401/permission denied) 성공.
-- ============================================================
do $$
declare
  t text;
  allt text[] := array[
    'global_ad_creative_daily','naver_sa_daily','naver_sa_keyword_daily','global_stripe_daily',
    'google_ads_daily','google_demandgen_campaign_daily','google_demandgen_content_mp_daily',
    'google_demandgen_content_spend_daily','google_demandgen_daily','kpi_metrics','kpi_product_metrics',
    'naver_daily_mp','naver_keyword_mp_daily','naver_powerlink_daily','toss_daily_revenue',
    'ad_performance_daily','global_ad_performance_daily','ad_creative_daily','vanced_ad_performance_daily',
    'adset_highlights','global_adset_highlights','ad_creative_highlights','vanced_adset_highlights'
  ];
begin
  foreach t in array allt loop
    -- anon 의 모든 테이블 권한 회수 (public 경유 권한이 있을 경우 대비해 public 도 함께)
    execute format('revoke all on public.%I from anon', t);
    execute format('revoke all on public.%I from public', t);
  end loop;
end $$;

-- 참고: 기존 "*anon read" 정책들은 남겨둬도 권한이 없으면 무해하지만,
-- 깔끔히 지우려면 Supabase SQL Editor에서 아래로 목록 확인 후 개별 drop 가능:
--   select schemaname, tablename, policyname, roles
--   from pg_policies where schemaname='public' and 'anon' = any(roles);
