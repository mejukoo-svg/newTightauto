-- 구글 디멘드젠 콘텐츠 지출 테이블에 country 컬럼 추가
-- 목적: 밴스드 구글 디멘드젠 탭에서 대만(TW)/국내(KR) 구분.
--       캠페인명 TW 태그('[Vanced] DG_..._TW_...')로 구글_디멘드젠_api_supabase.py 가 content별 country 적재.
-- 실행: Supabase SQL Editor 에서 1회 실행 (idempotent).

alter table google_demandgen_content_spend_daily add column if not exists country text;

-- 기존 행은 NULL → 대시보드에서 국내(KR)로 폴백 처리. 다음 파이프라인 실행 시 채워짐.
