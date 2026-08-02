-- ============================================================================
-- google_demandgen_ad_daily : 구글 디멘드젠 [Tight] 소재(광고)별 일자 성과
-- ============================================================================
-- 구글_디멘드젠_캠페인_supabase.py 가 (date, ad_group_id, ad_id) 단위로 upsert.
--   (세트 단위 google_demandgen_campaign_daily 와 같은 실행에서 함께 적재 —
--    Ads API 호출 1회 + Mixpanel export 1회를 공유하므로 추가 비용이 거의 없다.)
--   지출 : 구글 Ads API ad_group_ad × segments.date × metrics.cost_micros
--          (campaign.advertising_channel_type=DEMAND_GEN, campaign.name 에 '[Tight]')
--   매출 : Mixpanel export(결제완료) properties.utm_content(=구글 광고 id) 매칭
--          → $insert_id + order_id dedup 후 (date_KST, ad_id) 별 매출/건수
--          ※ utm_content 가 없거나 매핑에 없는 결제는 세트별 테이블에만 남고
--            (소재미상) 이 되므로, 소재 합계 < 세트 매출 이 될 수 있다.
-- index.html '🟢 구글 디멘드젠'(국내 탭, renderGgdgTight)에서 캠페인/세트 행의
--   ▶ 캐럿을 누르면 이 테이블을 ad_group_id 로 조회해 소재 행을 펼친다.
-- (supabase.yml google-dg-tight job 으로 매시 cron 자동 적재)
-- ============================================================================

CREATE TABLE IF NOT EXISTS "new-tightauto".google_demandgen_ad_daily (
  date            date    NOT NULL,
  ad_group_id     text    NOT NULL,            -- 세트(광고그룹) id — 펼침 조회 키
  ad_id           text    NOT NULL,            -- 소재(광고) id
  ad_name         text    NOT NULL DEFAULT '', -- 소재(광고) 이름
  ct              text    NOT NULL DEFAULT '', -- 최종 URL 의 ?ct= 콘텐츠 토큰(있으면)
  ad_group_name   text    NOT NULL DEFAULT '',
  campaign_id     text    NOT NULL DEFAULT '',
  campaign_name   text    NOT NULL DEFAULT '',
  spend           numeric NOT NULL DEFAULT 0,
  revenue         numeric NOT NULL DEFAULT 0,
  purchase_count  integer NOT NULL DEFAULT 0,
  clicks          integer NOT NULL DEFAULT 0,
  impressions     bigint  NOT NULL DEFAULT 0,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (date, ad_group_id, ad_id)
);

CREATE INDEX IF NOT EXISTS idx_ggdg_ad_group_date
  ON "new-tightauto".google_demandgen_ad_daily (ad_group_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_ggdg_ad_date_desc
  ON "new-tightauto".google_demandgen_ad_daily (date DESC);

-- index.html 은 로그인(authenticated) 세션으로 읽는다. anon 은 잠금 상태 유지.
ALTER TABLE "new-tightauto".google_demandgen_ad_daily ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS auth_all ON "new-tightauto".google_demandgen_ad_daily;
CREATE POLICY auth_all
  ON "new-tightauto".google_demandgen_ad_daily
  FOR ALL
  TO authenticated
  USING (true) WITH CHECK (true);

GRANT SELECT, INSERT, UPDATE, DELETE
  ON "new-tightauto".google_demandgen_ad_daily TO authenticated;
GRANT ALL ON "new-tightauto".google_demandgen_ad_daily TO service_role;
