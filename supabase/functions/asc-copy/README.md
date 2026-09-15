# asc-copy — 소재별 탭 'ASC' 마킹 → 같은 상품의 모든 ASC 세트에 소재 복사

대시보드 **국내 소재별 탭**에서 소재(광고)를 보라색 `ASC` 로 마킹하면 호출되는 Edge Function.
그 광고를 **같은 상품명**의 모든 ASC 캠페인 세트에 **복사**한다(이동 아님 — 원본은 그대로).

## 흐름 (대시보드)

1. 소재 칸 클릭 → 색상 피커의 `ASC`(보라) 선택. 세 곳에서 가능:
   국내 소재별 탭 / 국내 세트 탭에서 ▶ 로 펼친 하위 소재 / 글로벌 세트 탭에서 ▶ 로 펼친 하위 소재(`region:'gl'`)
2. 마킹이 `ad_creative_highlights` 에 저장된 뒤 비밀번호 게이트 → dry-run 계획 모달
3. 모달에 "소재 → 대상 ASC 캠페인/세트/상태/비고" 가 나열된다
   - 같은 소재가 이미 있는 세트 → `건너뜀`
   - 중단(PAUSED)된 ASC 에도 기본 포함 — 광고만 추가하고 **ASC 는 켜지 않는다**(status 는 읽기만). 주황색으로 표시
4. `확인 — N개 세트에 복사` → 실제 `POST /act_X/ads`(원본 creative_id 참조) → 결과 표 + `asc_copy_log` 기록

## 서버 동작

1. 로그인 JWT 검증 (비로그인 = 401)
2. `ad_creative_highlights` 의 현재 값이 `asc` 인지 대조 — 낡은 화면에서의 오실행 차단
3. 원본 광고를 메타에서 읽어 소속 계정 대조 + **캠페인명에서 상품 키 추출** (`region` 별)
   - `kr`: `국내_소재별_supabase.py` 의 `extract_product` 와 같은 규칙(구분자로 쪼갠 첫 비숫자 토큰, 앞 이모지 제거) → `집착`
   - `gl`: **국가 + 상품** (`app.js` 의 `GL_NON_PRODUCT_TOKENS`/`GL_PRODUCT_CANON` 과 같은 표) → `TW|shaman`.
     글로벌은 같은 상품 ASC 가 국가별로 따로 있고 언어가 다르므로 `대만_무당` 소재는 `대만_무당_ASC*` 에만 간다
     (`미국_무당_ASC_미국` 제외). `shaman`↔`무당` 같은 한↔영 표기는 canon 으로 합친다.
4. 같은 계정의 캠페인 중 이름에 `ASC` 가 들어가고 상품 키가 같은 것 → 하위 세트 전부가 대상
   - 실측: 우리 ASC 캠페인은 `smart_promotion_type=GUIDED_CREATION` 으로 나와 API 필드로는 못 가른다 → 이름 규칙
5. 대상 세트의 기존 광고(삭제·보관 제외)와 **소재 지문** 비교 — creative id / effective_object_story_id /
   video_id / image_hash 중 하나라도 겹치면 "이미 있음" 으로 건너뜀. `asc_copy_log` 의 성공 기록도 대조.
6. `dryRun:false` 면 `POST /act_X/ads { name:원본이름, adset_id, creative:{creative_id:원본}, status:ACTIVE }`
   - **`/{ad_id}/copies` 는 쓰지 않는다** — 크리에이티브를 새로 만들다 "기본 개선 사항(standard enhancements)
     필드 지원 중단"(subcode 3858504)으로 전부 거부됐다(2026-09-15 실측). 기존 크리에이티브를 id 로
     참조해 광고만 만들면 통과한다.
   - `tracking_specs` 는 넘기지 않는다 — 원본의 게시물 참여 추적이 원본 post 를 가리켜 #200 이 난다.
     비우면 메타가 세트 픽셀 기준 기본값을 채운다.
   - ACTIVE 로 만드는 이유: 죽은 소재를 ASC 에서 되살리는 용도라 원본이 꺼져 있어도 바로 게재
   - 이름을 바꾸지 않아 소재별 탭에서 원본과 같은 이름으로 ASC 세트 행에 나타난다(ad_id 는 다름)
   - 종료일이 지난 예약 세트(일정 실험 ASC 등)는 메타가 광고 추가를 거부한다 → 그 행만 오류로 표시
7. `asc_copy_log` 에 (원본 ad_id, 대상 세트, copied_ad_id, ok/error) 기록

UTM 은 광고 URL 의 `{{adset.id}}`/`{{ad.id}}` 매크로가 그대로 복사되므로 새 광고의 성과는
다음 소재별 파이프라인 실행 후 ASC 세트 소속 행으로 자동 귀속된다.

## 안전장치

- 한 번에 소재 50개 / 복사 200건까지
- 마킹 불일치·계정 불일치·DELETED/ARCHIVED 원본·상품명 추출 실패 → 거부
- 같은 소재 중복 투입 차단(지문 + 로그)
- 대시보드는 로그 읽기만(쓰기는 service_role 전용)

## 사전 준비 / 배포

1. `sql/asc_copy_log.sql` 실행 (SQL Editor 또는 Management API `/database/query`)
2. Edge Secret 은 apply-budget 과 공유(`META_TOKEN_1 / META_TOKEN_2_1 / …`) — 추가 등록 없음
3. 배포 — **git push 로는 배포되지 않는다**
   ```
   POST https://api.supabase.com/v1/projects/{ref}/functions/deploy?slug=asc-copy
     metadata: {"entrypoint_path":"index.ts","name":"asc-copy","verify_jwt":true}
     file:     index.ts
   ```
   또는 `npx supabase functions deploy asc-copy --project-ref grtglwavqhvlqcocahao`

## 로컬 검증

```bash
node --experimental-strip-types _verify.mjs   # Node 22+, 실제 메타·Supabase 호출 없음
```

같은 상품 ASC 만 대상 / 같은 영상 이미 있음 → skip / 마킹 없음 거부 / ASC 없는 상품 오류 /
dry-run 쓰기 0건 / select 로 고른 세트만 `/copies` / 로그 1건.

## 되돌리기

```sql
select applied_at, actor, ad_name, product, target_campaign_name, target_adset_name, copied_ad_id, ok, error
from "new-tightauto".asc_copy_log order by applied_at desc limit 50;
```
`copied_ad_id` 를 Ads Manager 에서 찾아 삭제하면 된다(원본은 건드린 적 없음).

## 계정 → 토큰 매핑

`ACC_TOKEN_ENV` 는 `apply-budget/index.ts` 와 동일하게 유지할 것(계정 추가 시 함께 수정).
