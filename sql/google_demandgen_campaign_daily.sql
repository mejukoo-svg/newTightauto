-- ============================================================================
-- google_demandgen_campaign_daily : 구글 디멘드젠 [Tight] 세트(광고그룹)별 일자 성과
-- ============================================================================
-- 구글_디멘드젠_캠페인_supabase.py 가 (date, ad_group_id) 단위로 upsert
--   지출 : 구글 Ads API (ad_group, advertising_channel_type=DEMAND_GEN,
--          campaign.name 에 '[Tight]' 포함) — ad_group.id × segments.date × cost
--   매출 : Mixpanel export (payment_complete) properties.utm_campaign(=구글 campaign.id)
--          ∈ [Tight] 캠페인 → utm_content(=광고 id)을 광고그룹(세트) id 로 매핑
--          → $insert_id + order_id dedup 후 (date_KST, ad_group_id) 별 매출/건수
--   ※ Mixpanel 결제 이벤트엔 세트 id 가 없어 utm_content(광고id)→ad_group 매핑(Ads API)으로 귀속.
-- index.html '🟢 구글 디멘드젠'(국내 탭, renderGgdgTight)이 이 테이블 단독으로
--   캠페인이름 / 세트이름 / 세트id × 일자 ROAS/순이익/지출/매출/구매 추이 렌더.
-- (supabase.yml google-dg-tight job 으로 매시 cron 자동 적재)
-- ============================================================================

DROP TABLE IF EXISTS public.google_demandgen_campaign_daily;

CREATE TABLE public.google_demandgen_campaign_daily (
  date            date    NOT NULL,
  ad_group_id     text    NOT NULL,            -- 세트(광고그룹) id
  ad_group_name   text    NOT NULL DEFAULT '', -- 세트이름
  campaign_id     text    NOT NULL DEFAULT '',
  campaign_name   text    NOT NULL DEFAULT '', -- 캠페인이름
  spend           numeric NOT NULL DEFAULT 0,
  revenue         numeric NOT NULL DEFAULT 0,
  purchase_count  integer NOT NULL DEFAULT 0,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (date, ad_group_id)
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
