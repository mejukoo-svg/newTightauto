-- 날짜탭 '메타에 예산 적용' 실행 로그  (스키마: new-tightauto)
--
-- 목적: 대시보드 날짜탭의 증감액 마킹(+20/+10/-10/-20/OFF)을 Edge Function(apply-budget)이
--       메타에 실제로 반영할 때, 무엇을 어떻게 바꿨는지 되돌릴 수 있게 남긴다.
--       실제 광고비를 움직이는 쓰기라서 before/after 를 반드시 기록한다 —
--       메타 Ads Manager 변경이력만으로는 '누가 대시보드에서 눌렀는지'를 알 수 없다.
--
-- 쓰기: Edge Function (service_role)  /  읽기: index.html (authenticated)
-- 실행: Supabase SQL Editor 에서 1회 실행 (idempotent).

create table if not exists "new-tightauto".budget_apply_log (
  id             bigserial   primary key,
  applied_at     timestamptz not null default now(),
  actor          text,                  -- 실행한 로그인 계정 이메일
  region         text,                  -- 'kr' | 'gl' | 'vn'
  adset_id       text        not null,  -- 마킹된 광고세트
  adset_name     text,
  ad_account_id  text,
  tag            text,                  -- up20 | up10 | down10 | down20 | off
  scope          text,                  -- 'adset' | 'campaign'  (campaign = CBO)
  target_id      text,                  -- 실제로 수정한 객체 id (CBO면 캠페인 id)
  field          text,                  -- daily_budget | lifetime_budget | status
  before_value   text,                  -- 메타 원본값(최소통화단위 또는 상태문자열)
  after_value    text,
  currency       text,
  ok             boolean     not null default false,
  error          text
);

create index if not exists budget_apply_log_at_idx
  on "new-tightauto".budget_apply_log (applied_at desc);
create index if not exists budget_apply_log_adset_idx
  on "new-tightauto".budget_apply_log (adset_id, applied_at desc);

alter table "new-tightauto".budget_apply_log enable row level security;

-- 이 스키마는 신규 테이블에 authenticated 전권을 주는 기본권한이 걸려 있다.
-- RLS 정책이 SELECT 뿐이라 실제 쓰기는 막히지만, 테이블 권한도 의도대로 좁힌다.
revoke insert, update, delete, truncate, references, trigger
  on "new-tightauto".budget_apply_log from authenticated;

grant select on "new-tightauto".budget_apply_log to authenticated;
grant usage, select on sequence "new-tightauto".budget_apply_log_id_seq to service_role;
grant select, insert on "new-tightauto".budget_apply_log to service_role;

-- 대시보드는 읽기만 (기록 위조 방지 — 쓰기는 Edge Function/service_role 전용)
drop policy if exists budget_apply_log_auth_select on "new-tightauto".budget_apply_log;
create policy budget_apply_log_auth_select on "new-tightauto".budget_apply_log
  for select to authenticated using (true);

notify pgrst, 'reload schema';
