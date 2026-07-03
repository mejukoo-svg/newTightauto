-- 학습된 교훈 저장 테이블 (월간 증류 → 매일 조언에 항상 주입)
-- 목적: 14일 조언창을 넘어서는 장기 패턴을, 봇이 월 1회 계정 이력(조치→ROAS)을 감사해
--       '이 계정에서 검증된 규칙'으로 증류한 뒤 여기에 저장한다. 매일 조언(봇+perf-advice
--       스킬)이 fetch_lessons(region)로 읽어 [학습된 교훈] 블록으로 항상 반영 → 복리 학습.
-- 왜 파일 아닌 DB: GitHub Actions 러너는 파일이 휘발 → 월간 잡이 self-commit해야 하는 번거로움.
--                DB면 월간 잡(쓰기)·매일 봇(읽기)이 service key로 바로 공유. index.html은 안 씀.
-- 실행: Supabase SQL Editor 에서 1회 실행 (idempotent).

create table if not exists advice_lessons (
  region      text        primary key,       -- 'kr' | 'gl'
  content     text,                           -- 증류된 교훈(불릿 텍스트)
  window_days int,                            -- 증류에 쓴 최근 일수
  updated_at  timestamptz not null default now()
);

alter table advice_lessons enable row level security;
-- 정책 없음 → service_role(봇)만 접근.
