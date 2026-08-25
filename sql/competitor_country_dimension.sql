-- 경쟁사 지표에 국가(country) 차원 추가 — 글로벌 경쟁사 탭용 (2026-08-25)
--
-- 왜 필요한가: 같은 Meta 페이지를 KR 기준과 TW 기준으로 각각 관측한다.
--   용용사주 990105674178850 / 1081705355035955 는 국내 탭에도, 글로벌 탭에도 잡힌다.
--   현재 PK 가 (date, page_id) 라서 TW 관측이 같은 날 KR 관측을 덮어써 버린다.
--
-- 기존 행은 전부 country='KR' 로 채워진다 (지금까지 수집이 전부 COUNTRY="KR" 기준).
-- 대시보드 국내 탭 쿼리에는 country=eq.KR 필터를 추가해야 한다.

alter table "new-tightauto".competitor_ad_daily
  add column if not exists country text not null default 'KR';
alter table "new-tightauto".competitor_ad_daily
  drop constraint if exists competitor_ad_daily_pkey;
alter table "new-tightauto".competitor_ad_daily
  add constraint competitor_ad_daily_pkey primary key (date, page_id, country);

alter table "new-tightauto".competitor_ad_weekly
  add column if not exists country text not null default 'KR';
alter table "new-tightauto".competitor_ad_weekly
  drop constraint if exists competitor_ad_weekly_pkey;
alter table "new-tightauto".competitor_ad_weekly
  add constraint competitor_ad_weekly_pkey primary key (week_start, company, country);

alter table "new-tightauto".competitor_product_count
  add column if not exists country text not null default 'KR';
alter table "new-tightauto".competitor_product_count
  drop constraint if exists competitor_product_count_pkey;
alter table "new-tightauto".competitor_product_count
  add constraint competitor_product_count_pkey primary key (date, site, country);

alter table "new-tightauto".competitor_product_count_weekly
  add column if not exists country text not null default 'KR';
alter table "new-tightauto".competitor_product_count_weekly
  drop constraint if exists competitor_product_count_weekly_pkey;
alter table "new-tightauto".competitor_product_count_weekly
  add constraint competitor_product_count_weekly_pkey primary key (week_start, site, country);
