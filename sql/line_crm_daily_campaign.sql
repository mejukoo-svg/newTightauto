-- ============================================================
-- line_crm_daily_campaign : 대만 LINE CRM 채널 매출 (일자 × 캠페인 ct)
--   프로젝트 tightsaju-vibe / grtglwavqhvlqcocahao, 스키마 "new-tightauto"
-- ============================================================
-- 적재: 대만라인_CRM_supabase.py (GitHub Actions cron)
--   소스   : Mixpanel export 결제완료/payment_complete, properties.ch='line' & ct like 'crm%'
--   제외   : crm_test / result_link / share / menu_* (오가닉·테스트)
--            + utm_term(adset)이 이미 국내메타·밴스드·대만밴스드·글로벌 세트에 귀속된 결제
--   통화   : revenue = 전 통화 KRW 환산(TWD는 TWD_KRW_RATE=47.85 고정, 나머지 실시간 환율)
--            revenue_twd = TWD 결제 원값 합(구글시트 'LINE CRM tracking' 대조용)
--   dedup  : order_id → $insert_id → (date,distinct_id,서비스)
-- 사용처: index.html '📊 매출' 탭(chrev) — 글로벌·우리 카테고리 채널 '대만 LINE CRM'.
--   그 매출은 Stripe 실결제(글로벌 종합)의 일부라, 대시보드는 '글로벌(밴스드 제외)' 잔여
--   행에서 이 값을 빼서 표시한다(채널 합 = 글로벌 종합 유지).
-- Supabase SQL Editor 에 붙여넣어 1회 실행.
-- ============================================================

create table if not exists "new-tightauto".line_crm_daily_campaign (
  date           date    not null,
  ct             text    not null,          -- 랜딩 URL 파라미터 ct (예: crm_260808_ninetail_tw)
  revenue        numeric not null default 0,-- 귀속 매출(KRW 환산)
  revenue_twd    numeric not null default 0,-- TWD 결제분 원값 합(시트 대조용)
  purchase_count integer not null default 0,
  updated_at     timestamptz not null default now(),
  primary key (date, ct)
);

create index if not exists idx_line_crm_date
  on "new-tightauto".line_crm_daily_campaign(date desc);

-- 권한 + RLS (기존 new-tightauto 테이블과 동일 태세: authenticated 만, anon 차단)
grant select, insert, update, delete
  on "new-tightauto".line_crm_daily_campaign to authenticated, service_role;
revoke all on "new-tightauto".line_crm_daily_campaign from anon;
alter table "new-tightauto".line_crm_daily_campaign enable row level security;
drop policy if exists auth_all on "new-tightauto".line_crm_daily_campaign;
create policy auth_all on "new-tightauto".line_crm_daily_campaign
  as permissive for all to authenticated using (true) with check (true);
