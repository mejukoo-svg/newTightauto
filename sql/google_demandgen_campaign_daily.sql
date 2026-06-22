-- ============================================================================
-- google_demandgen_campaign_daily : 구글 디멘드젠 [Tight] 캠페인별 일자 성과
-- ============================================================================
-- 구글_디멘드젠_캠페인_supabase.py 가 (date, campaign_id) 단위로 upsert
--   지출 : 구글 Ads API (campaign, advertising_channel_type=DEMAND_GEN,
--          campaign.name 에 '[Tight]' 포함) — campaign.id × segments.date × cost
--   매출 : Mixpanel export (payment_complete) properties.utm_campaign(=구글 campaign.id)
--          매칭 → $insert_id + order_id dedup 후 (date_KST, campaign_id) 별 매출/건수
--   ※ Mixpanel 결제 이벤트의 utm_campaign 이 구글 캠페인 숫자 id 와 동일하여
--     캠페인 id 로 지출↔매출 직접 귀속 가능 (utm_content = 광고 id).
-- index.html '🟢 구글 디멘드젠'(국내 탭, renderGgdgTight)이 이 테이블 단독으로
--   캠페인 × 일자 ROAS/순이익/지출/매출/구매 추이 렌더.
-- (supabase.yml google-dg-tight job 으로 매시 cron 자동 적재)
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.google_demandgen_campaign_daily (
  date            date    NOT NULL,
  campaign_id     text    NOT NULL,
  campaign_name   text    NOT NULL DEFAULT '',
  spend           numeric NOT NULL DEFAULT 0,
  revenue         numeric NOT NULL DEFAULT 0,
  purchase_count  integer NOT NULL DEFAULT 0,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (date, campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_ggdg_camp_date_desc
  ON public.google_demandgen_campaign_daily (date DESC);

-- index.html 은 anon publishable key 로 읽으므로 RLS SELECT 정책 필요
ALTER TABLE public.google_demandgen_campaign_daily ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "google_demandgen_campaign_daily anon read"
  ON public.google_demandgen_campaign_daily;

CREATE POLICY "google_demandgen_campaign_daily anon read"
  ON public.google_demandgen_campaign_daily
  FOR SELECT
  TO anon, authenticated
  USING (true);
