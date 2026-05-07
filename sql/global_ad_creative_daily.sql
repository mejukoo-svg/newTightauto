-- ============================================================================
-- global_ad_creative_daily : 글로벌 Meta Ads 소재(ad)별 일별 성과
-- ============================================================================
-- 글로벌_소재별_supabase.py 가 (date, ad_id) 단위로 upsert
-- index.html 의 글로벌 추이차트에서 화살표 펼침으로 소재 단위 추이 표시용
-- 스키마 참고: ad_creative_daily(KR) + global_ad_performance_daily(USD 필드)
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.global_ad_creative_daily (
  date                date    NOT NULL,
  ad_id               text    NOT NULL,
  campaign_name       text,
  adset_name          text,
  adset_id            text,
  ad_name             text,
  ad_account_id       text,
  product             text,
  country             text,
  currency            text,
  spend_usd           numeric NOT NULL DEFAULT 0,
  cost_per_result     numeric NOT NULL DEFAULT 0,
  purchase_roas_meta  numeric NOT NULL DEFAULT 0,
  cpm                 numeric NOT NULL DEFAULT 0,
  reach               integer NOT NULL DEFAULT 0,
  impressions         integer NOT NULL DEFAULT 0,
  frequency           numeric NOT NULL DEFAULT 0,
  unique_clicks       integer NOT NULL DEFAULT 0,
  unique_ctr          numeric NOT NULL DEFAULT 0,
  cost_per_click      numeric NOT NULL DEFAULT 0,
  results_meta        integer NOT NULL DEFAULT 0,
  results_mp          integer NOT NULL DEFAULT 0,
  revenue_usd         numeric NOT NULL DEFAULT 0,
  profit_usd          numeric NOT NULL DEFAULT 0,
  roas                numeric NOT NULL DEFAULT 0,
  cvr                 numeric NOT NULL DEFAULT 0,
  budget_usd          numeric NOT NULL DEFAULT 0,
  updated_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (date, ad_id)
);

CREATE INDEX IF NOT EXISTS idx_global_creative_date_desc
  ON public.global_ad_creative_daily (date DESC);

CREATE INDEX IF NOT EXISTS idx_global_creative_adset
  ON public.global_ad_creative_daily (adset_id, date DESC);

CREATE INDEX IF NOT EXISTS idx_global_creative_spend
  ON public.global_ad_creative_daily (spend_usd DESC);

-- index.html 은 anon publishable key 로 읽으므로 RLS SELECT 정책 필요
ALTER TABLE public.global_ad_creative_daily ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "global_ad_creative_daily anon read"
  ON public.global_ad_creative_daily;

CREATE POLICY "global_ad_creative_daily anon read"
  ON public.global_ad_creative_daily
  FOR SELECT
  TO anon, authenticated
  USING (true);
