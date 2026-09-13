-- ============================================================================
-- 구글 디멘드젠 DG_TT 소재 · 성과 3테이블
-- ============================================================================
-- 적재: 구글_DGTT_소재_supabase.py (supabase.yml google-dgtt job, 매시)
-- 대상: 계정 5912047700 디멘드젠 광고 중 캠페인명 또는 광고그룹명에 'DG_TT' 포함
--   · google_dgtt_ad_creative : 소재 내용 스냅샷(광고 1행) — 텍스트·유튜브id·이미지URL·상태
--   · google_dgtt_ad_daily    : 광고 × 일자 성과(Ads API) — 지출·노출·클릭·전환·영상 시청률
--   · google_dgtt_asset_daily : 광고 × 에셋 × 일자 — 헤드라인/영상/로고별 성과 + performance_label
-- 매출(Mixpanel utm_content 귀속)은 기존 google_demandgen_ad_daily 를 ad_id 로 조인.
-- ============================================================================

create table if not exists "new-tightauto".google_dgtt_ad_creative (
  ad_id            text primary key,
  ad_name          text not null default '',
  ad_type          text not null default '',   -- DEMAND_GEN_VIDEO_RESPONSIVE_AD / DEMAND_GEN_MULTI_ASSET_AD
  ad_status        text not null default '',   -- ENABLED / PAUSED / REMOVED
  approval_status  text not null default '',   -- APPROVED / DISAPPROVED / ...
  ad_group_id      text not null default '',
  ad_group_name    text not null default '',
  ad_group_status  text not null default '',
  campaign_id      text not null default '',
  campaign_name    text not null default '',
  campaign_status  text not null default '',
  final_url        text not null default '',
  ct               text not null default '',   -- final_url 의 ?ct=
  headlines        jsonb not null default '[]'::jsonb,
  long_headlines   jsonb not null default '[]'::jsonb,
  descriptions     jsonb not null default '[]'::jsonb,
  business_name    text not null default '',
  call_to_actions  jsonb not null default '[]'::jsonb,
  videos           jsonb not null default '[]'::jsonb,   -- [{asset_id, youtube_video_id, title}]
  images           jsonb not null default '[]'::jsonb,   -- [{asset_id, url, w, h}]
  logos            jsonb not null default '[]'::jsonb,   -- [{asset_id, url}]
  youtube_video_id text not null default '',             -- 첫 영상 id (https://youtu.be/<id>)
  updated_at       timestamptz not null default now()
);
create index if not exists google_dgtt_ad_creative_group_idx
  on "new-tightauto".google_dgtt_ad_creative (ad_group_id);
create index if not exists google_dgtt_ad_creative_campaign_idx
  on "new-tightauto".google_dgtt_ad_creative (campaign_id);

create table if not exists "new-tightauto".google_dgtt_ad_daily (
  date              date    not null,
  ad_id             text    not null,
  ad_group_id       text    not null default '',
  campaign_id       text    not null default '',
  ad_name           text    not null default '',
  spend             numeric not null default 0,
  impressions       bigint  not null default 0,
  clicks            integer not null default 0,
  conversions       numeric not null default 0,   -- 구글 전환수(전체구매)
  conversions_value numeric not null default 0,   -- 구글 전환가치(계정통화 KRW)
  engagements       integer not null default 0,
  video_p25_rate    numeric not null default 0,   -- 영상 25% 시청률(0~1)
  video_p50_rate    numeric not null default 0,
  video_p75_rate    numeric not null default 0,
  video_p100_rate   numeric not null default 0,
  updated_at        timestamptz not null default now(),
  primary key (date, ad_id)
);
create index if not exists google_dgtt_ad_daily_date_idx
  on "new-tightauto".google_dgtt_ad_daily (date desc);
create index if not exists google_dgtt_ad_daily_group_idx
  on "new-tightauto".google_dgtt_ad_daily (ad_group_id, date desc);

create table if not exists "new-tightauto".google_dgtt_asset_daily (
  date              date    not null,
  ad_id             text    not null,
  asset_id          text    not null,
  field_type        text    not null default '',   -- HEADLINE / LONG_HEADLINE / DESCRIPTION / YOUTUBE_VIDEO / LOGO / BUSINESS_NAME ...
  performance_label text    not null default '',   -- BEST / GOOD / LOW / LEARNING / PENDING / UNSPECIFIED
  enabled           boolean not null default true,
  asset_type        text    not null default '',
  asset_text        text    not null default '',
  youtube_video_id  text    not null default '',
  spend             numeric not null default 0,
  impressions       bigint  not null default 0,
  clicks            integer not null default 0,
  conversions       numeric not null default 0,
  updated_at        timestamptz not null default now(),
  primary key (date, ad_id, asset_id)
);
create index if not exists google_dgtt_asset_daily_date_idx
  on "new-tightauto".google_dgtt_asset_daily (date desc);
create index if not exists google_dgtt_asset_daily_ad_idx
  on "new-tightauto".google_dgtt_asset_daily (ad_id, date desc);

-- RLS: 대시보드(로그인 사용자)만 — 다른 테이블(auth_all)과 동일 정책
alter table "new-tightauto".google_dgtt_ad_creative enable row level security;
alter table "new-tightauto".google_dgtt_ad_daily    enable row level security;
alter table "new-tightauto".google_dgtt_asset_daily enable row level security;

drop policy if exists auth_all on "new-tightauto".google_dgtt_ad_creative;
create policy auth_all on "new-tightauto".google_dgtt_ad_creative
  for all to authenticated using (true) with check (true);
drop policy if exists auth_all on "new-tightauto".google_dgtt_ad_daily;
create policy auth_all on "new-tightauto".google_dgtt_ad_daily
  for all to authenticated using (true) with check (true);
drop policy if exists auth_all on "new-tightauto".google_dgtt_asset_daily;
create policy auth_all on "new-tightauto".google_dgtt_asset_daily
  for all to authenticated using (true) with check (true);

grant select, insert, update, delete on "new-tightauto".google_dgtt_ad_creative to authenticated;
grant select, insert, update, delete on "new-tightauto".google_dgtt_ad_daily    to authenticated;
grant select, insert, update, delete on "new-tightauto".google_dgtt_asset_daily to authenticated;
grant all on "new-tightauto".google_dgtt_ad_creative to service_role;
grant all on "new-tightauto".google_dgtt_ad_daily    to service_role;
grant all on "new-tightauto".google_dgtt_asset_daily to service_role;
