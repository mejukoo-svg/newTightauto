-- ============================================================================
-- google_demandgen_content_mp_daily : 구글 디멘드젠 콘텐츠(ct)별 매출 (Mixpanel 기반)
-- ============================================================================
-- 구글_디멘드젠_mp_supabase.py 가 (date, content) 단위로 upsert
--   필터: Mixpanel 결제완료/payment_complete 이벤트 중 properties.ch == 'google'
--   그룹: properties.ct (콘텐츠 이름)
--   집계: revenue(결제금액 합), purchase_count(결제 건수)
-- index.html 의 국내(KR) 모드 '🟢 구글 디멘드젠' 탭에서 콘텐츠별 추이 표시용
-- (Mixpanel 에는 콘텐츠별 광고비가 없으므로 매출/건수만 — ROAS/지출 컬럼 없음)
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.google_demandgen_content_mp_daily (
  date            date    NOT NULL,
  content         text    NOT NULL,
  revenue         numeric NOT NULL DEFAULT 0,
  purchase_count  integer NOT NULL DEFAULT 0,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (date, content)
);

CREATE INDEX IF NOT EXISTS idx_ggdg_ct_date_desc
  ON public.google_demandgen_content_mp_daily (date DESC);

CREATE INDEX IF NOT EXISTS idx_ggdg_ct_revenue
  ON public.google_demandgen_content_mp_daily (revenue DESC);

-- index.html 은 anon publishable key 로 읽으므로 RLS SELECT 정책 필요
ALTER TABLE public.google_demandgen_content_mp_daily ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "google_demandgen_content_mp_daily anon read"
  ON public.google_demandgen_content_mp_daily;

CREATE POLICY "google_demandgen_content_mp_daily anon read"
  ON public.google_demandgen_content_mp_daily
  FOR SELECT
  TO anon, authenticated
  USING (true);
