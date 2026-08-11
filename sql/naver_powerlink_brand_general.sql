-- naver_powerlink_daily: 브랜드/일반 구매전환값 컬럼 추가 (2026-08-11)
-- 지출은 이미 brand_cost / general_cost 로 분리돼 있었고, 매출(구매전환값)만 총액뿐이었다.
-- 시트 '00. 네이버/구글 Daily' 네이버 파워링크 섹션에 '브랜드 구매전환값'·'일반 구매전환값'이
-- 있어 그대로 적재 → 매출탭이 '네이버 브랜드검색'·'네이버 일반검색어' 2개 채널로 나눠 쓴다.
-- 적재: 구글_supabase.py (Actions google-ads 잡)
alter table "new-tightauto".naver_powerlink_daily
  add column if not exists brand_revenue   numeric,
  add column if not exists general_revenue numeric;

comment on column "new-tightauto".naver_powerlink_daily.brand_revenue is
  '브랜드검색 구매전환값(시트 00. 네이버/구글 Daily). 지출은 brand_cost.';
comment on column "new-tightauto".naver_powerlink_daily.general_revenue is
  '일반검색어 구매전환값(시트). 지출은 general_cost.';
