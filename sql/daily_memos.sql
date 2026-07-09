-- 날짜탭 ↔ 추이차트 메모 동기화용 durable 저장소  (스키마: new-tightauto)
-- 문제: 추이차트 메모(saveTrendMemo)는 날짜탭 반영을 perfTbl(ad_performance_daily 류)의
--       (날짜, adset_id) 행에 memo를 PATCH해서 했는데, 글로벌은 daily 적재가 늦어
--       그 행이 아직 없으면 0행 갱신 → 조용히 유실 → 날짜탭에 안 뜸(동기화 깨짐).
--       (human_advice_marks 가 하이라이트에서 같은 문제를 우회한 것과 동일 원인)
-- 해결: 메모를 (날짜, entity_id, region) 로 여기에 upsert → daily 행 존재와 무관하게 durable.
--       날짜탭은 이 테이블을 우선 읽고, 추이차트도 이 값을 폴백으로 읽음 → 양방향 동기화(B+C).
-- 접근: index.html(로그인=authenticated)이 쓰고 읽음 → adset_highlights 와 동일 권한.
-- 실행: Supabase SQL Editor 또는 Management API 로 1회 실행 (idempotent).

create table if not exists "new-tightauto".daily_memos (
  date       date        not null,
  entity_id  text        not null,   -- adset_id(kr/gl/vn) 또는 ad_id(cr)
  region     text        not null,   -- 'kr' | 'gl' | 'cr' | 'vn'
  memo       text,
  updated_at timestamptz not null default now(),
  primary key (date, entity_id, region)
);

alter table "new-tightauto".daily_memos enable row level security;
grant select, insert, update, delete on "new-tightauto".daily_memos to authenticated, service_role;
drop policy if exists daily_memos_auth_all on "new-tightauto".daily_memos;
create policy daily_memos_auth_all on "new-tightauto".daily_memos
  for all to authenticated using (true) with check (true);

-- 기존 날짜탭 메모(perfTbl.memo)를 durable 저장소로 1회 백필 → 기존 메모도 그대로 동기화 표시.
insert into "new-tightauto".daily_memos (date, entity_id, region, memo)
select date, adset_id, 'kr', memo from "new-tightauto".ad_performance_daily
where memo is not null and memo <> ''
on conflict (date, entity_id, region) do update set memo = excluded.memo;

insert into "new-tightauto".daily_memos (date, entity_id, region, memo)
select date, adset_id, 'gl', memo from "new-tightauto".global_ad_performance_daily
where memo is not null and memo <> ''
on conflict (date, entity_id, region) do update set memo = excluded.memo;

insert into "new-tightauto".daily_memos (date, entity_id, region, memo)
select date, adset_id, 'vn', memo from "new-tightauto".vanced_ad_performance_daily
where memo is not null and memo <> ''
on conflict (date, entity_id, region) do update set memo = excluded.memo;

insert into "new-tightauto".daily_memos (date, entity_id, region, memo)
select date, ad_id, 'cr', memo from "new-tightauto".ad_creative_daily
where memo is not null and memo <> ''
on conflict (date, entity_id, region) do update set memo = excluded.memo;

-- PostgREST 스키마 캐시 리로드 → 새 테이블 즉시 쿼리 가능
notify pgrst, 'reload schema';
