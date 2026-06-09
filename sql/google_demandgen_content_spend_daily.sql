-- ============================================================================
-- google_demandgen_content_spend_daily : 구글 디멘드젠 콘텐츠(ct)별 지출 (구글 Ads API 기반)
-- ============================================================================
-- 구글_디멘드젠_api_supabase.py 가 (date, content) 단위로 upsert
--   소스: 구글 Ads API (ad_group_ad, advertising_channel_type IN DEMAND_GEN/DISCOVERY)
--   ad.final_urls 의 ?ch=google&ct=<콘텐츠> 에서 ct 별 비용/클릭/노출/전환 합산
-- index.html 이 google_demandgen_content_mp_daily(매출) 와 (date,content) 로 조인 →
--   콘텐츠별 ROAS/순이익/지출/매출 표시
-- (API 라 수동 다운로드 불필요 — supabase.yml google-dg-spend job 으로 매시 cron 자동 적재)
-- ※ 과거: 바탕화면 "광고 보고서*.xlsx" 수동 적재(구글_디멘드젠_예산_엑셀.py) — Ads API 로 대체됨
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.google_demandgen_content_spend_daily (
  date            date    NOT NULL,
  content         text    NOT NULL,
  spend           numeric NOT NULL DEFAULT 0,
  clicks          integer NOT NULL DEFAULT 0,
  impressions     integer NOT NULL DEFAULT 0,
  conversions     numeric NOT NULL DEFAULT 0,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (date, content)
);

CREATE INDEX IF NOT EXISTS idx_ggdg_spend_date_desc
  ON public.google_demandgen_content_spend_daily (date DESC);

-- index.html 은 anon publishable key 로 읽으므로 RLS SELECT 정책 필요
ALTER TABLE public.google_demandgen_content_spend_daily ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "google_demandgen_content_spend_daily anon read"
  ON public.google_demandgen_content_spend_daily;

CREATE POLICY "google_demandgen_content_spend_daily anon read"
  ON public.google_demandgen_content_spend_daily
  FOR SELECT
  TO anon, authenticated
  USING (true);
