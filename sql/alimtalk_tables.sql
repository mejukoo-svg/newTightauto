-- ============================================================
-- 알림톡(CRM) 대시보드 데이터 적재용 테이블 (tightsaju-vibe / grtglwavqhvlqcocahao)
-- ⚠️ 대시보드(index.html)가 읽는 스키마는 public 이 아니라 "new-tightauto".
--    기존 테이블과 동일하게: RLS ON + authenticated 롤 auth_all(using true), anon 접근 없음(=anon 401).
-- Supabase SQL Editor에 붙여넣어 1회 실행 (또는 Management API query 엔드포인트).
-- ============================================================

-- 1) 캠페인 메타 (crm_upsell / reviewcoupon / couponremind)
create table if not exists "new-tightauto".alimtalk_campaign (
  key          text primary key,
  label        text,
  intent_label text,
  color        text
);

-- 2) 일자별 전체 지표 (스토어 전체 발송수·매출)
create table if not exists "new-tightauto".alimtalk_daily_total (
  date       date primary key,
  n          integer not null default 0,
  rev        bigint  not null default 0,
  updated_at timestamptz not null default now()
);

-- 3) 일자 x 캠페인 지표
create table if not exists "new-tightauto".alimtalk_daily_campaign (
  date         date    not null,
  campaign_key text    not null,
  n            integer not null default 0,   -- 귀속 구매수
  rev          bigint  not null default 0,   -- 귀속 매출
  intent_n     integer not null default 0,   -- 의도(추천상품/쿠폰사용) 구매수
  intent_rev   bigint  not null default 0,   -- 의도 매출
  sent         integer not null default 0,   -- 발송수
  cost         bigint  not null default 0,   -- 발송비용 = sent × cost_krw(현재 13원/건)
  updated_at   timestamptz not null default now(),
  primary key (date, campaign_key)
);

-- 4) 일자 x 캠페인 x 제품 지표
create table if not exists "new-tightauto".alimtalk_daily_product (
  date         date    not null,
  campaign_key text    not null,
  product      text    not null,
  n            integer not null default 0,
  rev          bigint  not null default 0,
  updated_at   timestamptz not null default now(),
  primary key (date, campaign_key, product)
);

-- 조회 편의 인덱스
create index if not exists idx_alimtalk_camp_date on "new-tightauto".alimtalk_daily_campaign(date);
create index if not exists idx_alimtalk_prod_date on "new-tightauto".alimtalk_daily_product(date);

-- 권한 + RLS (기존 new-tightauto 테이블과 동일 태세)
do $$
declare t text;
begin
  foreach t in array array['alimtalk_campaign','alimtalk_daily_total','alimtalk_daily_campaign','alimtalk_daily_product']
  loop
    execute format('grant select,insert,update,delete on "new-tightauto".%I to authenticated, service_role', t);
    execute format('revoke all on "new-tightauto".%I from anon', t);
    execute format('alter table "new-tightauto".%I enable row level security', t);
    execute format('drop policy if exists auth_all on "new-tightauto".%I', t);
    execute format('create policy auth_all on "new-tightauto".%I as permissive for all to authenticated using (true) with check (true)', t);
  end loop;
end $$;
