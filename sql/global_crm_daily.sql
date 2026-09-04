-- ============================================================
-- global_crm_daily : 글로벌 CRM(대만 LINE OA) 일자 × 캠페인 매출
--   프로젝트 tightsaju-vibe / grtglwavqhvlqcocahao, 스키마 "new-tightauto"
-- ============================================================
-- 적재: 글로벌CRM_시트_supabase.py (GitHub Actions cron, 하루 1회)
--   소스 : 구글시트 'LINE CRM tracking' 의 자동갱신 탭(gid=290165057)
--          — ch=line · ct=crm_* · 통화 TWD · KST 기준으로 시트 쪽에서 이미 집계된 값.
--          시트가 매 실행마다 전 기간을 재작성하므로 여기도 전 기간 재적재(=시트가 정본).
--   통화 : revenue_twd = 시트 원값(TWD), revenue = revenue_twd × TWD_KRW_RATE(기본 47.85,
--          대만밴스드·대만 파이프라인과 동일 환율) 로 환산한 KRW.
--   지출 : 없음. LINE OA 발송비용 원천이 수기라 매출만 싣는다(대시보드도 매출 전용 행).
-- 사용처: index.html '📊 매출' 탭(chrev) — 글로벌·우리 카테고리 채널 '글로벌 CRM'.
--   이 매출은 Stripe 실결제(글로벌 종합)의 일부이므로, 대시보드는 '글로벌(밴스드 제외)'
--   잔여 행에서 같은 값을 빼서 표시한다 → '채널 합 = 글로벌 종합' 유지.
-- Supabase SQL Editor 에 붙여넣어 1회 실행.
-- ============================================================

create table if not exists "new-tightauto".global_crm_daily (
  date           date    not null,
  campaign       text    not null,          -- 시트 컬럼명 (예: 260808_ninetail)
  revenue_twd    numeric not null default 0,-- 시트 원값(TWD)
  revenue        numeric not null default 0,-- KRW 환산
  purchase_count integer not null default 0,
  updated_at     timestamptz not null default now(),
  primary key (date, campaign)
);

create index if not exists idx_global_crm_date
  on "new-tightauto".global_crm_daily(date desc);

-- 권한 + RLS (기존 new-tightauto 테이블과 동일 태세: authenticated 만, anon 차단)
grant select, insert, update, delete
  on "new-tightauto".global_crm_daily to authenticated, service_role;
revoke all on "new-tightauto".global_crm_daily from anon;
alter table "new-tightauto".global_crm_daily enable row level security;
drop policy if exists auth_all on "new-tightauto".global_crm_daily;
create policy auth_all on "new-tightauto".global_crm_daily
  as permissive for all to authenticated using (true) with check (true);
