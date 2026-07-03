-- AI 증감액 추천 이력 테이블
-- 목적: 오늘의퍼포먼스봇(AI)이 매일 권한 증감액을 durable하게 기록한다.
--       후일 gather_sets가 '그날 AI가 뭘 권했나(tag)' vs '사람이 실제 선택한 하이라이트
--       (ad_performance_daily.highlight)'를 세트·날짜로 비교 → 조언이 자기 권고의 적중/빗나감을
--       ROAS로 채점하며 발전하는 학습 루프에 쓴다.
-- 특징: adset_highlights(오늘 시각화·매일 purge)·ad_performance_daily(사람 조치이력)와 별개 →
--       기존 데이터/조치이력을 전혀 오염시키지 않는다.
-- 접근: 봇/스킬만 service key로 읽고 쓴다(index.html은 안 읽음) → anon SELECT 정책 불필요.
-- 실행: Supabase SQL Editor 에서 1회 실행 (idempotent).

create table if not exists ai_advice_marks (
  date       date        not null,          -- 행동일(봇 실행일 = 사람이 그날 highlight 찍는 날)
  adset_id   text        not null,
  region     text,                          -- 'kr' | 'gl'
  tag        text        not null,          -- up10 | up20 | down10 | down20 | off
  created_at timestamptz not null default now(),
  primary key (date, adset_id)
);

alter table ai_advice_marks enable row level security;
-- 정책 없음 → service_role(봇)만 접근. anon/authenticated 불가(대시보드는 이 테이블 안 씀).
