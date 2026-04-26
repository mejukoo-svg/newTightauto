-- ============================================================================
-- naver_keyword_mp_daily : 네이버 SA 검색어별 매출 (Mixpanel cr 기반)
-- ============================================================================
-- 네이버_mp_supabase.py 가 (date, keyword) 단위로 upsert
-- index.html 의 네이버SA_추이차트 패널에 검색어 추이 섹션 표시용
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.naver_keyword_mp_daily (
  date            date    NOT NULL,
  keyword         text    NOT NULL,
  revenue         numeric NOT NULL DEFAULT 0,
  purchase_count  integer NOT NULL DEFAULT 0,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (date, keyword)
);

CREATE INDEX IF NOT EXISTS idx_naver_kw_date_desc
  ON public.naver_keyword_mp_daily (date DESC);

CREATE INDEX IF NOT EXISTS idx_naver_kw_revenue
  ON public.naver_keyword_mp_daily (revenue DESC);

-- index.html 은 anon publishable key 로 읽으므로 RLS SELECT 정책 필요
ALTER TABLE public.naver_keyword_mp_daily ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "naver_keyword_mp_daily anon read"
  ON public.naver_keyword_mp_daily;

CREATE POLICY "naver_keyword_mp_daily anon read"
  ON public.naver_keyword_mp_daily
  FOR SELECT
  TO anon, authenticated
  USING (true);
