-- 추이차트 전용 메모 컬럼 추가
-- 목적: 추이차트의 메모를 "하이라이트 테이블"에 저장 → '하이라이트 전체 삭제' 시 메모도 함께 삭제.
--       날짜탭 메모(ad_performance_daily.memo 등 perfTbl.memo)는 그대로 영구 보존.
-- 실행: Supabase SQL Editor 에서 1회 실행 (idempotent).

-- 1) memo 컬럼 추가 (4개 하이라이트 테이블) -----------------------------------
alter table adset_highlights         add column if not exists memo text;
alter table global_adset_highlights  add column if not exists memo text;
alter table ad_creative_highlights   add column if not exists memo text;
alter table vanced_adset_highlights  add column if not exists memo text;

-- 2) (선택) 기존 날짜탭 메모를 추이차트 메모로 1회 백필 --------------------------
--    이미 적어둔 메모가 추이차트에서도 그대로 보이게 하려면 아래 4개 블록을 실행.
--    실행 안 하면 추이차트 메모는 비어서 시작하고, 새로 입력하는 것부터 채워짐.
--    (날짜탭 메모는 어느 경우든 안전하게 유지됨)

insert into adset_highlights (adset_id, memo)
select distinct on (adset_id) adset_id, memo
from ad_performance_daily
where memo is not null and memo <> ''
order by adset_id, date desc
on conflict (adset_id) do update set memo = excluded.memo;

insert into global_adset_highlights (adset_id, memo)
select distinct on (adset_id) adset_id, memo
from global_ad_performance_daily
where memo is not null and memo <> ''
order by adset_id, date desc
on conflict (adset_id) do update set memo = excluded.memo;

insert into ad_creative_highlights (ad_id, memo)
select distinct on (ad_id) ad_id, memo
from ad_creative_daily
where memo is not null and memo <> ''
order by ad_id, date desc
on conflict (ad_id) do update set memo = excluded.memo;

insert into vanced_adset_highlights (adset_id, memo)
select distinct on (adset_id) adset_id, memo
from vanced_ad_performance_daily
where memo is not null and memo <> ''
order by adset_id, date desc
on conflict (adset_id) do update set memo = excluded.memo;
