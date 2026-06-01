-- kpi_product_metrics : KPI 탭 Top10 상품별 실매출 지표 (광고DB qkvqiorazdrhtuicnpec)
-- 소스: 제품DB(wvgwlwaqlhewhobzauda) payments(status='paid') → kpi_supabase.py 가 적재
-- ⚠️ 이 광고DB는 MCP/secret키로 DDL 불가 → Supabase SQL 편집기에서 1회 실행 필요.
create table if not exists public.kpi_product_metrics (
  period        text    not null,          -- 'weekly' | 'monthly'
  period_label  text    not null,
  period_start  date    not null,
  period_end    date    not null,
  product       text    not null,
  revenue       bigint  not null default 0, -- 실매출(원) sum(amount)
  sales         integer not null default 0, -- 판매수 count
  aov           numeric,                     -- 객단가 = revenue/sales
  rank          integer not null,           -- 기간 내 매출 순위(1=Top)
  updated_at    timestamptz not null default now(),
  primary key (period, period_start, product)
);

create index if not exists idx_kpi_product_metrics_lookup
  on public.kpi_product_metrics (period, period_start, rank);

-- 대시보드(브라우저 publishable 키)에서 읽기 허용 (다른 kpi/ad 테이블과 동일 정책 가정)
alter table public.kpi_product_metrics enable row level security;
do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname='public' and tablename='kpi_product_metrics' and policyname='kpi_product_metrics_read'
  ) then
    create policy kpi_product_metrics_read on public.kpi_product_metrics for select using (true);
  end if;
end $$;

grant select on public.kpi_product_metrics to anon, authenticated;
