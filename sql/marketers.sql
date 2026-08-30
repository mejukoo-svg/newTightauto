-- 마케터탭(mktDash) 마케터 목록 — 대시보드에서 직접 추가/삭제 (스키마: new-tightauto)
-- 배경: app.js 의 KR_MARKETERS / GL_MARKETERS 하드코딩 배열이라 사람이 들어오고 나갈 때마다
--       코드 수정 → 커밋 → 배포가 필요했다. 목록을 DB로 옮겨 대시보드에서 바로 관리한다.
-- 동작: app.js 가 부팅 시 이 테이블을 읽어 목록을 대체한다(행이 0개면 코드의 기본값 유지).
--       읽기 실패(테이블 없음·오프라인)면 localStorage 캐시 → 코드 기본값 순으로 폴백.
-- 접근: index.html(로그인=authenticated)이 읽고 쓴다 → daily_memos 와 동일 권한.
-- 실행: Supabase SQL Editor 또는 Management API 로 1회 실행 (idempotent).

create table if not exists "new-tightauto".marketers (
  region     text        not null,               -- 'kr' | 'gl'
  name       text        not null,               -- 소재명/세트명에 들어가는 제작자 태그(부분일치용)
  sort_order int         not null default 999,   -- 드롭다운 표시 순서
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (region, name)
);

alter table "new-tightauto".marketers enable row level security;
grant select, insert, update, delete on "new-tightauto".marketers to authenticated, service_role;
drop policy if exists marketers_auth_all on "new-tightauto".marketers;
create policy marketers_auth_all on "new-tightauto".marketers
  for all to authenticated using (true) with check (true);

-- 현재 app.js 하드코딩 목록을 그대로 시드(순서 보존). 이미 있으면 순서만 맞춘다.
insert into "new-tightauto".marketers (region, name, sort_order) values
  ('kr','수연',1),('kr','희상',2),('kr','혜린',3),('kr','본걸',4),('kr','지은',5),
  ('kr','휘동',6),('kr','지연',7),('kr','정헌',8),('kr','연희',9),('kr','지영',10),
  ('kr','하루',11),('kr','훤기',12),('kr','베스',13),('kr','기쁨',14),
  ('gl','본걸',1),('gl','지은',2),('gl','훤기',3),('gl','채채',4),('gl','지영',5),('gl','하루',6)
on conflict (region, name) do update set sort_order = excluded.sort_order;

-- PostgREST 스키마 캐시 리로드 → 새 테이블 즉시 쿼리 가능
notify pgrst, 'reload schema';
