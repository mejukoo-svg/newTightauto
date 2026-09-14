-- 소재별 탭 'ASC' 하이라이트 → 같은 상품의 ASC 세트로 소재 복사 실행 로그  (스키마: new-tightauto)
--
-- 목적: 소재별 탭에서 소재를 'ASC'(보라) 로 마킹하면 Edge Function(asc-copy)이 그 광고(ad)를
--       같은 상품명의 모든 ASC 캠페인 세트에 복사(POST /{ad_id}/copies)한다. 원본은 그대로 두고
--       ASC 세트에 새 광고(copied_ad_id)가 생기므로, 어느 소재가 어디로 복사됐는지 남겨야
--       ① 같은 소재를 또 넣지 않고(중복 방지) ② 잘못 들어간 광고를 Ads Manager 에서 찾아 지울 수 있다.
--
-- 쓰기: Edge Function (service_role)  /  읽기: index.html (authenticated)
-- 실행: Supabase SQL Editor 또는 Management API /database/query 에서 1회 실행 (idempotent).

create table if not exists "new-tightauto".asc_copy_log (
  id                   bigserial   primary key,
  applied_at           timestamptz not null default now(),
  actor                text,                  -- 실행한 로그인 계정 이메일
  region               text,                  -- 'cr' (국내 소재별)
  ad_id                text        not null,  -- 원본 광고(소재) id
  ad_name              text,
  ad_account_id        text,
  product              text,                  -- 캠페인명에서 추출한 상품명 (extract_product 규칙)
  src_campaign_name    text,
  src_adset_id         text,
  target_campaign_id   text,                  -- 복사해 넣은 ASC 캠페인
  target_campaign_name text,
  target_adset_id      text        not null,  -- 복사해 넣은 ASC 세트
  target_adset_name    text,
  copied_ad_id         text,                  -- 새로 생긴 광고 id (성공 시)
  status_option        text,                  -- ACTIVE | PAUSED | INHERITED_FROM_SOURCE
  ok                   boolean     not null default false,
  error                text
);

create index if not exists asc_copy_log_at_idx
  on "new-tightauto".asc_copy_log (applied_at desc);
create index if not exists asc_copy_log_ad_idx
  on "new-tightauto".asc_copy_log (ad_id, target_adset_id, applied_at desc);

alter table "new-tightauto".asc_copy_log enable row level security;

-- 이 스키마는 신규 테이블에 authenticated 전권을 주는 기본권한이 걸려 있다 → 의도대로 좁힌다.
revoke insert, update, delete, truncate, references, trigger
  on "new-tightauto".asc_copy_log from authenticated;

grant select on "new-tightauto".asc_copy_log to authenticated;
grant usage, select on sequence "new-tightauto".asc_copy_log_id_seq to service_role;
grant select, insert on "new-tightauto".asc_copy_log to service_role;

-- 대시보드는 읽기만 (기록 위조 방지 — 쓰기는 Edge Function/service_role 전용)
drop policy if exists asc_copy_log_auth_select on "new-tightauto".asc_copy_log;
create policy asc_copy_log_auth_select on "new-tightauto".asc_copy_log
  for select to authenticated using (true);

notify pgrst, 'reload schema';
