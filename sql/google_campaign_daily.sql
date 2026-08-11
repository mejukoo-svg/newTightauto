-- google_campaign_daily
-- 구글 Ads 전 캠페인(유형 무관) × 일자 성과. 매출탭의 구글 채널 5분할(국내/대만 × 검색/디멘드젠 + PMAX)용.
--   · 지출/클릭/노출 = Google Ads API (계정 5912047700, KRW) campaign × segments.date
--   · 매출/구매수    = Mixpanel 결제완료 utm_campaign(=구글 campaign.id) 귀속
--                     ($insert_id + order_id dedup, 비KRW 결제는 KRW 환산)
--   · country = 캠페인명 TW 태그(토큰 단위 tw/taiwan/대만/台) → 'TW', 그 외 'KR'
--   · owner   = 캠페인명에 '[Tight]' 포함 → 'tight'(우리), 그 외 'vanced'
-- 적재: 구글_캠페인_supabase.py (GitHub Actions supabase.yml google-campaign job)
create table if not exists "new-tightauto".google_campaign_daily (
  date            date        not null,
  campaign_id     text        not null,
  campaign_name   text,
  channel_type    text,                  -- SEARCH / DEMAND_GEN / PERFORMANCE_MAX / DISPLAY / VIDEO ...
  country         text        not null default 'KR',
  owner           text        not null default 'vanced',
  spend           numeric     not null default 0,
  revenue         numeric     not null default 0,
  purchase_count  integer     not null default 0,
  clicks          integer     not null default 0,
  impressions     bigint      not null default 0,
  updated_at      timestamptz not null default now(),
  primary key (date, campaign_id)
);

create index if not exists google_campaign_daily_date_idx
  on "new-tightauto".google_campaign_daily (date desc);
create index if not exists google_campaign_daily_type_idx
  on "new-tightauto".google_campaign_daily (channel_type, country);

alter table "new-tightauto".google_campaign_daily enable row level security;

-- 대시보드(로그인 사용자)만 접근 — 다른 테이블(auth_all)과 동일 정책
drop policy if exists auth_all on "new-tightauto".google_campaign_daily;
create policy auth_all on "new-tightauto".google_campaign_daily
  for all to authenticated using (true) with check (true);
