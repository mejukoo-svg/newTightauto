-- 하이라이트 출처(source) 컬럼 추가
-- 목적: AI(오늘의퍼포먼스봇)가 조언으로 자동 표기한 하이라이트를 사람이 직접 친 것과 구분.
--       추이차트에서 source='ai' 인 하이라이트 네모에는 테두리(.hl-ai)를 둘러 표시한다.
--       사람이 셀을 클릭해 마킹하면 source='user' 로 바뀌어 테두리가 사라진다.
-- 실행: Supabase SQL Editor 에서 1회 실행 (idempotent).

alter table adset_highlights         add column if not exists source text;
alter table global_adset_highlights  add column if not exists source text;
alter table ad_creative_highlights   add column if not exists source text;
alter table vanced_adset_highlights  add column if not exists source text;
