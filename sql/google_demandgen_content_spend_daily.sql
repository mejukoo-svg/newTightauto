-- ============================================================================
-- google_demandgen_content_spend_daily : 구글 디멘드젠 콘텐츠(ct)별 지출 (구글 광고 보고서 엑셀 기반)
-- ============================================================================
-- 구글_디멘드젠_예산_엑셀.py 가 (date, content) 단위로 upsert
--   소스: 바탕화면의 "광고 보고서*.xlsx" (구글 Ads 광고 보고서, 하루치)
--   최종 URL 의 ?ch=google&ct=<콘텐츠> 에서 ch=google 만, ct 별 비용/클릭/노출/전환 합산
-- index.html 이 google_demandgen_content_mp_daily(매출) 와 (date,content) 로 조인 →
--   콘텐츠별 ROAS/순이익/지출/매출 표시
-- (엑셀이 수동 다운로드라 cron 불가 — 로컬 수동 적재)
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
