-- 사람 증감액 선택 durable 기록 테이블
-- 문제: 사람이 추이차트에서 증감액을 마킹하면 saveHL이 perfTbl(ad_performance_daily 류)의
--       (오늘 날짜, adset_id) 행에 highlight를 PATCH하는데, 글로벌은 daily 데이터가 시간별로
--       늦게 적재돼 '오늘 행'이 없을 때 마킹하면 0행 갱신 → 조용히 유실. 결과: 글로벌은
--       조치 이력(gather_sets '이력' + 월간 학습)이 텅 빔(국내는 데이터가 일찍 있어 정상).
-- 해결: 마킹 시 daily 행 존재와 무관하게 여기에 upsert → 국내·글로벌 동일하게 사람 선택이 durable.
--       gather_sets·gather_learning_data가 이걸 읽어 조치 이력에 병합.
-- 접근: index.html(로그인=authenticated)이 쓰고 봇(service_role)이 읽음 → adset_highlights와 동일 권한.
-- 실행: Supabase SQL Editor 에서 1회 실행 (idempotent).

create table if not exists human_advice_marks (
  date       date        not null,      -- 마킹한 날(사람 행동일)
  adset_id   text        not null,
  region     text,                       -- 'kr' | 'gl'
  tag        text,                        -- up10 | up20 | down10 | down20 | off (취소=null)
  updated_at timestamptz not null default now(),
  primary key (date, adset_id)
);

alter table human_advice_marks enable row level security;
grant select, insert, update, delete on human_advice_marks to authenticated, service_role;
drop policy if exists human_advice_marks_auth_all on human_advice_marks;
create policy human_advice_marks_auth_all on human_advice_marks
  for all to authenticated using (true) with check (true);
