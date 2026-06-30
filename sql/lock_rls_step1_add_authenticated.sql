-- ============================================================
-- STEP 1 — authenticated(로그인) 역할에 접근 권한 부여
--          anon(publishable 키) 권한은 그대로 유지 → 무중단.
-- 이 단계만으로는 아무것도 깨지지 않는다.
-- 새 index.html 배포 + 공유계정 생성 + 로그인/하이라이트 저장 검증을
-- 모두 마친 뒤에 STEP 2(anon 회수)를 실행할 것.
--
-- 실행: Supabase 대시보드 → SQL Editor 에 붙여넣고 Run.
-- 멱등(여러 번 실행해도 안전).
-- ============================================================
do $$
declare
  t text;
  -- 읽기 전용 (SELECT 만)
  ro text[] := array[
    'global_ad_creative_daily','naver_sa_daily','naver_sa_keyword_daily',
    'global_stripe_daily','google_ads_daily','google_demandgen_campaign_daily',
    'google_demandgen_content_mp_daily','google_demandgen_content_spend_daily',
    'google_demandgen_daily','kpi_metrics','kpi_product_metrics','naver_daily_mp',
    'naver_keyword_mp_daily','naver_powerlink_daily','toss_daily_revenue'
  ];
  -- 성과 테이블 (SELECT + UPDATE — 하이라이트/메모 PATCH)
  perf text[] := array[
    'ad_performance_daily','global_ad_performance_daily',
    'ad_creative_daily','vanced_ad_performance_daily'
  ];
  -- 하이라이트 테이블 (SELECT+INSERT+UPDATE+DELETE — upsert/삭제)
  hl text[] := array[
    'adset_highlights','global_adset_highlights',
    'ad_creative_highlights','vanced_adset_highlights'
  ];
begin
  -- 읽기 전용
  foreach t in array ro loop
    execute format('alter table public.%I enable row level security', t);
    execute format('drop policy if exists %I on public.%I', t||'_auth_select', t);
    execute format('create policy %I on public.%I for select to authenticated using (true)', t||'_auth_select', t);
    execute format('grant select on public.%I to authenticated', t);
  end loop;

  -- 성과 테이블: SELECT + UPDATE
  foreach t in array perf loop
    execute format('alter table public.%I enable row level security', t);
    execute format('drop policy if exists %I on public.%I', t||'_auth_select', t);
    execute format('create policy %I on public.%I for select to authenticated using (true)', t||'_auth_select', t);
    execute format('drop policy if exists %I on public.%I', t||'_auth_update', t);
    execute format('create policy %I on public.%I for update to authenticated using (true) with check (true)', t||'_auth_update', t);
    execute format('grant select, update on public.%I to authenticated', t);
  end loop;

  -- 하이라이트 테이블: 전체 CRUD
  foreach t in array hl loop
    execute format('alter table public.%I enable row level security', t);
    execute format('drop policy if exists %I on public.%I', t||'_auth_all', t);
    execute format('create policy %I on public.%I for all to authenticated using (true) with check (true)', t||'_auth_all', t);
    execute format('grant select, insert, update, delete on public.%I to authenticated', t);
  end loop;
end $$;
