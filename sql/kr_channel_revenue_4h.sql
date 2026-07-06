-- ============================================================================
-- kr_channel_revenue_4h : 국내 채널별 매출 — 4시간(intra-day) 버킷 (Mixpanel 기반)
-- ============================================================================
-- 국내_시간대매출_supabase.py 가 (date, bucket, channel) 단위로 upsert.
--   소스: Mixpanel export 결제완료/payment_complete 이벤트 (properties.time = UTC epoch)
--   시간: time + 9h(KST) 후 4시간 floor → bucket ∈ {0,4,8,12,16,20} (해당 구간 시작시각)
--   채널 분류(우선순위):
--     ① utm_term(=adset_id) ∈ 국내 메타 세트  & utm_source=Meta계열  → '국내 메타'
--     ② utm_term(=adset_id) ∈ 밴스드(국내) 세트 & utm_source=Meta계열 → '밴스드'
--     ③ is_naver_event(utm_source/referrer=naver)                    → '네이버'
--     ④ properties.ch == 'google'                                    → '구글디멘드젠'
--     ⑤ 그 외 → 미집계(스킵)
--   집계: revenue(KRW, 해외통화는 KRW 환산), purchase_count(dedup 후 결제 건수)
--   dedup: order_id(utm_term우선·max revenue) → $insert_id → (date,distinct_id,서비스)
-- index.html 국내(KR) 모드 '📊 매출' 탭의 '시간별' 보기(chrView=hourly)에서 사용.
--   ⚠ 시트 기반 채널(구글 검색·네이버 파워링크·디멘드젠 전체 시트값)은 일 단위라 4시간 버킷 불가 →
--     시간별 뷰는 Mixpanel 귀속 채널만 표시(일별 뷰의 시트기반 매출과 값이 다를 수 있음).
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.kr_channel_revenue_4h (
  date            date     NOT NULL,
  bucket          smallint NOT NULL,          -- 4시간 구간 시작시각(KST): 0,4,8,12,16,20
  channel         text     NOT NULL,          -- '국내 메타' | '밴스드' | '네이버' | '구글디멘드젠'
  revenue         numeric  NOT NULL DEFAULT 0,
  purchase_count  integer  NOT NULL DEFAULT 0,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (date, bucket, channel)
);

CREATE INDEX IF NOT EXISTS idx_kr_rev4h_date_desc
  ON public.kr_channel_revenue_4h (date DESC);

-- index.html 은 로그인(authenticated) 세션으로 읽는다. anon 은 잠금(revoke) 상태 유지.
ALTER TABLE public.kr_channel_revenue_4h ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "kr_channel_revenue_4h auth read"
  ON public.kr_channel_revenue_4h;

CREATE POLICY "kr_channel_revenue_4h auth read"
  ON public.kr_channel_revenue_4h
  FOR SELECT
  TO authenticated
  USING (true);

GRANT SELECT ON public.kr_channel_revenue_4h TO authenticated;
