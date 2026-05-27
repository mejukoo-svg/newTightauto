-- 지표 하이아라키 KPI 주간/월간 집계 테이블
-- kpi_supabase.py 가 upsert. index.html(지표KPI 탭)이 publishable(anon) 키로 읽음.
-- Supabase SQL 편집기에서 1회 실행.

create table if not exists public.kpi_metrics (
  period            text not null,           -- 'weekly' | 'monthly'
  period_label      text not null,           -- weekly '05.18~05.24' / monthly '2026-05'
  period_start      date not null,           -- 주차 월요일 / 월 1일 (정렬·PK용)
  period_end        date not null,           -- 데이터 종료일 (진행 중 주/월은 today)
  budget            numeric,                 -- 예산 (KR 메타 지출, KRW)
  revenue           numeric,                 -- 매출 (Toss total_amount, KRW)
  pv                numeric,                 -- PV (Mixpanel pv_onboarding)
  sales             numeric,                 -- 판매수 (Toss total_count)
  net_profit        numeric,                 -- 순이익 = revenue - budget
  aov               numeric,                 -- 객단가 = revenue / sales
  roas              numeric,                 -- ROAS = revenue / budget (비율)
  cpp               numeric,                 -- CPP = budget / sales
  pay_rate          numeric,                 -- 결제율 = sales / pv (비율)
  cac               numeric,                 -- CAC = budget / sales
  budget_global_usd numeric,                 -- 참고: 글로벌 지출(USD), 예산엔 미포함
  updated_at        timestamptz default now(),
  primary key (period, period_start)
);

-- 인덱스 (기간 조회)
create index if not exists kpi_metrics_period_start_idx
  on public.kpi_metrics (period, period_start desc);

-- RLS: anon(publishable 키) 읽기 허용
alter table public.kpi_metrics enable row level security;
drop policy if exists "kpi_metrics_anon_read" on public.kpi_metrics;
create policy "kpi_metrics_anon_read"
  on public.kpi_metrics for select to anon using (true);
grant select on public.kpi_metrics to anon;
